"""CCCC 协作闭环 (CCCC-CORE-10).

实现 Foreman 协调流程：a3 发送任务 → a1 执行 → a1 返回 → a3 决定下一步。
完整链路可追踪。

开发内容：
1. 保存消息关联（task_message 链：instruction ↔ result，parent_message_id）。
2. 保存 Run 关系（Run ↔ message_id ↔ delivery_id ↔ session_id）。
3. 支持继续 Session（复用 session_manager，上下文继续）。
4. 区分用户回复和 Actor 指令（user_reply vs actor_instruction，sender 来源决定）。

设计：
- 复用 coordination_store（投递/Run/Session/产物）、mailbox（可靠投递消费）、
  route_validator（foreman_managed 路由校验）、session_manager、one_shot_runner、
  output_capture、output_adapter。本模块只编排闭环，不重写存储。
- TaskMessage 作为 ledger 之上的标准化视图：实际消息存储仍以 ledger 事件为准，
  本模块在 coordination 目录维护 task_messages.jsonl 关联索引（message_id 链）。
- 完整链路可追踪：instruction.message_id → result.parent_message_id →
  result.run_id → run.message_id → run.session_id → session。任一节点可双向追溯。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..contracts.v1.coordination import (
    Delivery,
    Run,
    RuntimeSession,
    StandardEvent,
    TaskMessage,
    TaskMessageKind,
)
from ..util.fs import atomic_write_bytes
from ..util.time import utc_now_iso
from . import coordination_store as store
from .group import Group
from .mailbox import ack_delivery, enqueue_delivery, reserve_delivery
from .route_validator import RouteValidator
from .session_manager import (
    acquire_session_for_run,
    create_session,
    release_session,
    resume_session,
)

_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# TaskMessage 关联索引（消息链）
# ---------------------------------------------------------------------------


def _messages_path(group: Group):
    return store.coordination_dir(group) / "task_messages.jsonl"


def _append_task_message(group: Group, tm: TaskMessage) -> None:
    store.ensure_coordination_layout(group)
    path = _messages_path(group)
    with _LOCK:
        with path.open("a", encoding="utf-8") as f:
            f.write(tm.model_dump_json() + "\n")


def _read_task_messages(group: Group) -> List[TaskMessage]:
    path = _messages_path(group)
    if not path.exists():
        return []
    out: List[TaskMessage] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(TaskMessage.model_validate_json(line))
        except Exception:
            continue
    return out


def list_task_messages(
    group: Group,
    *,
    sender_actor_id: Optional[str] = None,
    recipient_actor_id: Optional[str] = None,
    kind: Optional[TaskMessageKind] = None,
) -> List[TaskMessage]:
    items = _read_task_messages(group)
    if sender_actor_id is not None:
        items = [m for m in items if m.sender_actor_id == sender_actor_id]
    if recipient_actor_id is not None:
        items = [m for m in items if m.recipient_actor_id == recipient_actor_id]
    if kind is not None:
        items = [m for m in items if m.kind == kind]
    return items


def get_task_message(group: Group, message_id: str) -> Optional[TaskMessage]:
    for m in _read_task_messages(group):
        if m.message_id == message_id:
            return m
    return None


# ---------------------------------------------------------------------------
# 闭环编排
# ---------------------------------------------------------------------------


@dataclass
class InstructionReceipt:
    """Foreman 下发任务指令的结果：消息 + 投递（进入 peer 队列）。"""

    task_message: TaskMessage
    delivery: Delivery


@dataclass
class ExecutionReceipt:
    """peer 执行任务的结果：Run + Session + 产物事件。"""

    run: Run
    session: RuntimeSession
    events: List[StandardEvent]


@dataclass
class ResultReceipt:
    """peer 返回任务结果：消息（链回 instruction）+ 投递（进入 foreman 队列）。"""

    task_message: TaskMessage
    delivery: Delivery


def send_instruction(
    group: Group,
    *,
    foreman_actor_id: str,
    peer_actor_id: str,
    message_id: str,
    payload: Optional[Dict[str, Any]] = None,
    coordination_mode: str = "foreman_managed",
) -> InstructionReceipt:
    """Foreman 向 peer 下发任务指令（task_instruction）。

    1. 路由校验（foreman_managed：foreman→peer 单接收者）。
    2. 保存 TaskMessage 关联（消息链）。
    3. 入队投递（进入 peer 独立邮箱，foreman 不可见他人队列）。
    """
    validator = RouteValidator(group, coordination_mode=coordination_mode)  # type: ignore[arg-type]
    validator.validate(
        sender_actor_id=foreman_actor_id,
        kind="task_instruction",
        recipient_tokens=[peer_actor_id],
    )
    tm = TaskMessage(
        group_id=group.group_id,
        message_id=message_id,
        sender_actor_id=foreman_actor_id,
        recipient_actor_id=peer_actor_id,
        kind="task_instruction",
        payload=payload or {},
    )
    _append_task_message(group, tm)
    delivery = enqueue_delivery(
        group,
        delivery_id=f"dv.{message_id}",
        message_id=message_id,
        recipient_actor_id=peer_actor_id,
    )
    return InstructionReceipt(task_message=tm, delivery=delivery)


def consume_instruction(
    group: Group,
    *,
    peer_actor_id: str,
    consumer_id: str,
) -> Optional[TaskMessage]:
    """peer 从独立邮箱取出一条待执行指令（预留投递锁）。

    返回对应 TaskMessage；无待执行指令返回 None。
    """
    from .mailbox import queued_deliveries

    for d in queued_deliveries(group, peer_actor_id):
        tm = get_task_message(group, d.message_id)
        if tm is None or tm.kind != "task_instruction":
            continue
        try:
            reserve_delivery(group, d.delivery_id, consumer_id=consumer_id)
        except Exception:
            continue
        return tm
    return None


def execute_instruction(
    group: Group,
    *,
    peer_actor_id: str,
    task_message: TaskMessage,
    run_id: str,
    command: List[str],
    session_id: str = "",
    runtime: str = "",
    cwd: str = "",
    env: Optional[Dict[str, str]] = None,
    stdin_text: str = "",
    timeout_seconds: Optional[float] = None,
    consumer_id: str = "",
) -> ExecutionReceipt:
    """peer 执行一条任务指令：占用/创建 Session → 启动 Run → 采集输出 → 释放 Session。

    支持继续 Session：传入已存在的 session_id 则复用上下文；为空则创建新会话。
    Run 关系保存：run.message_id = task_message.message_id，run.session_id = session_id。
    """
    from .one_shot_runner import OneShotRunInput, OneShotRunner
    from .output_capture import capture_run_output

    # 1. Session：继续或新建
    if session_id:
        session = resume_session(group, session_id=session_id, actor_id=peer_actor_id, runtime=runtime, work_dir=cwd)
    else:
        session_id = f"ses.{run_id}"
        session = create_session(
            group, session_id=session_id, owner_actor_id=peer_actor_id,
            runtime=runtime, work_dir=cwd, execution_mode="one_shot",
        )
    acquire_session_for_run(group, session_id=session_id, run_id=run_id, actor_id=peer_actor_id, runtime=runtime, work_dir=cwd)

    # 2. Run
    runner = OneShotRunner(group, runtime=runtime or "one_shot")
    run_input = OneShotRunInput(
        run_id=run_id,
        message_id=task_message.message_id,
        actor_id=peer_actor_id,
        command=command,
        cwd=cwd,
        env=env or {},
        stdin_text=stdin_text,
        timeout_seconds=timeout_seconds,
        session_id=session_id,
    )
    result = runner.start(run_input)
    run = store.get_run(group, run_id)
    assert run is not None

    # 3. 采集输出（生成有序标准事件，可追溯到原始产物）
    stdout_blob = _read_blob(group, run, "raw.stdout")
    stderr_blob = _read_blob(group, run, "raw.stderr")
    captured = capture_run_output(group, run, stdout=stdout_blob, stderr=stderr_blob)

    # 4. 释放 Session（失败标记 failed）
    release_session(group, session_id=session_id, failed=(run.state == "failed"))

    # 5. ack 投递（消费完成）
    delivery_id = f"dv.{task_message.message_id}"
    if store.get_delivery(group, delivery_id) is not None and consumer_id:
        try:
            ack_delivery(group, delivery_id, consumer_id=consumer_id)
        except Exception:
            pass

    return ExecutionReceipt(run=run, session=session, events=captured.events)


def send_result(
    group: Group,
    *,
    peer_actor_id: str,
    foreman_actor_id: str,
    instruction_message_id: str,
    result_message_id: str,
    run_id: str,
    payload: Optional[Dict[str, Any]] = None,
    coordination_mode: str = "foreman_managed",
) -> ResultReceipt:
    """peer 向 Foreman 返回任务结果（task_result），链回 instruction。

    1. 路由校验（peer→foreman 单接收者）。
    2. 保存 TaskMessage，payload 含 parent_message_id（链回指令）与 run_id（链回 Run）。
    3. 入队投递（进入 foreman 邮箱）。
    """
    validator = RouteValidator(group, coordination_mode=coordination_mode)  # type: ignore[arg-type]
    validator.validate(
        sender_actor_id=peer_actor_id,
        kind="task_result",
        recipient_tokens=[foreman_actor_id],
    )
    body = dict(payload or {})
    body.setdefault("parent_message_id", instruction_message_id)
    body.setdefault("run_id", run_id)
    tm = TaskMessage(
        group_id=group.group_id,
        message_id=result_message_id,
        sender_actor_id=peer_actor_id,
        recipient_actor_id=foreman_actor_id,
        kind="task_result",
        payload=body,
    )
    _append_task_message(group, tm)
    delivery = enqueue_delivery(
        group,
        delivery_id=f"dv.{result_message_id}",
        message_id=result_message_id,
        recipient_actor_id=foreman_actor_id,
    )
    return ResultReceipt(task_message=tm, delivery=delivery)


# ---------------------------------------------------------------------------
# 区分用户回复与 Actor 指令
# ---------------------------------------------------------------------------


def classify_sender(group: Group, sender_actor_id: str) -> str:
    """区分用户回复和 Actor 指令。

    返回 "user_reply"（来自 user/system，非 Actor）或 "actor_instruction"（来自 Actor）。
    路由校验已强制任务消息发送者必须是 enabled Actor；非 Actor 发送者视为用户回复，
    不进入 task_instruction/task_result 闭环（由上层 chat 语义处理）。
    """
    from .actors import find_actor, is_internal_actor

    sid = str(sender_actor_id or "").strip()
    if not sid:
        return "user_reply"
    actor = find_actor(group, sid)
    if actor is None:
        return "user_reply"
    if is_internal_actor(actor):
        return "user_reply"
    return "actor_instruction"


# ---------------------------------------------------------------------------
# 链路追踪
# ---------------------------------------------------------------------------


def trace_chain(group: Group, instruction_message_id: str) -> Dict[str, Any]:
    """完整链路可追踪：从 instruction 追溯 result → run → session → events。

    返回结构化链路快照，任一节点缺失则以 None 表示，不抛错。
    """
    instruction = get_task_message(group, instruction_message_id)
    chain: Dict[str, Any] = {"instruction": instruction}
    if instruction is None:
        return chain

    # result：payload.parent_message_id == instruction_message_id
    result: Optional[TaskMessage] = None
    run: Optional[Run] = None
    session: Optional[RuntimeSession] = None
    for m in _read_task_messages(group):
        if m.kind == "task_result" and m.payload.get("parent_message_id") == instruction_message_id:
            result = m
            run_id = m.payload.get("run_id")
            if run_id:
                run = store.get_run(group, str(run_id))
                if run and run.session_id:
                    session = store.get_session(group, run.session_id)
            break

    events: List[StandardEvent] = []
    if run is not None:
        events = store.read_standard_events(group, run.run_id)

    chain["result"] = result
    chain["run"] = run
    chain["session"] = session
    chain["events"] = events
    return chain


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------


def _read_blob(group: Group, run: Run, kind: str) -> bytes:
    artifact = store.get_artifact(group, f"{run.run_id}.{kind}")
    if artifact is None or not artifact.path:
        return b""
    blob_path = group.path / artifact.path
    if not blob_path.exists():
        return b""
    return blob_path.read_bytes()
