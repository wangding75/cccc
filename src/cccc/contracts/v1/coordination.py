"""CCCC One-shot 协作契约 (CCCC-CORE-01 契约冻结).

本模块冻结 CCCC-CORE-02 ~ CCCC-CORE-12 共同遵守的数据与业务规则。

设计原则：兼容演进。
- 保留现有公开契约：Group、ActorRole=foreman|peer、ChatMessageData.to=list[str]
  （含广播 `[]`、`@all`、`@peers`、`@foreman`）、PTY/headless/app-server 运行方式。
- 新增两个独立维度：``coordination_mode`` 与 ``execution_mode``，默认值保持当前行为兼容。
- 只有显式启用 ``foreman_managed`` 时才应用严格的任务消息路由规则。
- 只有显式启用 ``one_shot`` 时才使用一次任务一次进程的执行模式。
- 普通消息（chat）继续支持单接收者、多接收者、空 to 广播、@all、@peers。
- 任务消息（task_instruction / task_result）必须单接收者，禁止广播/多接收者/peer→peer。

本模块只定义契约（模型、枚举、错误码、纯路由校验），不实现运行时能力
（One-shot Runner / Session resume CLI / 输出采集器 / 队列 / Adapter 属于后续任务）。

术语映射（任务书 §9 → 现有契约）：
    Workspace → Group
    Worker    → peer
    Foreman   → foreman
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ...util.time import utc_now_iso

# ---------------------------------------------------------------------------
# 维度一：协调模式
# ---------------------------------------------------------------------------

CoordinationMode = Literal["legacy", "foreman_managed"]
"""协调模式。

``legacy``：默认值，保持现有 CCCC 行为。普通消息支持广播与多接收者，
不强制任务消息路由规则，不启用可靠投递/Run/Session 契约。

``foreman_managed``：显式启用后，任务消息（task_instruction / task_result）
必须满足严格单接收者路由规则，并启用 Delivery/Run/RuntimeSession 契约。
普通消息（chat）行为不变。
"""

DEFAULT_COORDINATION_MODE: CoordinationMode = "legacy"

# ---------------------------------------------------------------------------
# 维度二：执行模式
# ---------------------------------------------------------------------------

ExecutionMode = Literal["pty", "headless", "one_shot"]
"""执行模式。

``pty`` / ``headless``：保持现有 runner 语义（长期交互进程 / MCP 驱动）。

``one_shot``：新增执行模式。每条任务消息创建一个 Run，启动一次智能体进程，
保存全部输出，进程退出后 Run 结束。Actor 仍是逻辑角色，不绑定长期进程。
仅当显式启用时生效，不替换现有 PTY/headless/app-server 代码。
"""

DEFAULT_EXECUTION_MODE: ExecutionMode = "pty"
"""默认执行模式保持现有 Runtime 行为（PTY 优先），不强制 one_shot。"""

# ---------------------------------------------------------------------------
# 任务消息语义
# ---------------------------------------------------------------------------

TaskMessageKind = Literal["chat", "task_instruction", "task_result", "system"]
"""消息语义类别。

``chat``：普通消息，保持现有 ChatMessageData 全部语义（含广播、多接收者）。

``task_instruction``：Foreman 向指定 peer 下发任务。
    必须满足：sender.role=foreman，recipient 数量=1，recipient.role=peer，
    recipient actor id 明确存在，sender 与 recipient 属于同一 Group。

``task_result``：peer 向 Foreman 返回任务结果。
    必须满足：sender.role=peer，recipient 数量=1，recipient=本 Group foreman，
    sender 与 recipient 属于同一 Group。

``system``：系统消息，保留扩展位。

task_instruction / task_result 禁止：广播、多接收者、空接收者、peer→peer、
自动选择空闲 peer、根据 Runtime 模糊匹配接收者。CCCC 必须依据明确 Actor ID 投递。
"""

DEFAULT_TASK_MESSAGE_KIND: TaskMessageKind = "chat"

# ---------------------------------------------------------------------------
# 状态机枚举（冻结）
# ---------------------------------------------------------------------------

DeliveryState = Literal["queued", "reserved", "delivered", "failed"]
"""投递状态机：queued → reserved → delivered | failed（reserved 可回退 queued）。"""

RunState = Literal["pending", "running", "succeeded", "failed", "cancelled", "timed_out"]
"""Run 状态机：pending → running → succeeded | failed | cancelled | timed_out。"""

RuntimeSessionState = Literal["idle", "busy", "closed", "failed"]
"""Session 状态机：idle ↔ busy → closed | failed（busy 期间禁止并发 Run）。"""

StandardEventStream = Literal["stdout", "stderr", "event", "system"]
"""标准事件流来源。"""

StandardEventType = Literal[
    "raw.stdout",
    "raw.stderr",
    "tool_call",
    "tool_result",
    "assistant_message",
    "user_message",
    "system",
    "run_start",
    "run_end",
    "unknown",
]
"""标准事件类型。无法识别的输出封装为 raw.stdout / raw.stderr，不丢弃、不摘要替代。"""

RunArtifactKind = Literal["raw.stdout", "raw.stderr", "artifact"]
"""Run 产物类型。原始 stdout/stderr 分开保存，不修改内容、不删除重复、不以摘要替代。"""

# ---------------------------------------------------------------------------
# 错误码（冻结，语义完整保留）
# ---------------------------------------------------------------------------

# 注：错误码名称遵循项目 UPPER_SNAKE_CASE 规范，语义与任务书 §10 一致。

ERR_GROUP_FOREMAN_REQUIRED = "GROUP_FOREMAN_REQUIRED"
ERR_GROUP_MULTIPLE_FOREMEN = "GROUP_MULTIPLE_FOREMEN"
ERR_TASK_RECIPIENT_REQUIRED = "TASK_RECIPIENT_REQUIRED"
ERR_TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED = "TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED"
ERR_TASK_ROUTE_NOT_ALLOWED = "TASK_ROUTE_NOT_ALLOWED"
ERR_ACTOR_NOT_FOUND = "ACTOR_NOT_FOUND"
ERR_ACTOR_DISABLED = "ACTOR_DISABLED"
ERR_SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
ERR_SESSION_ACTOR_MISMATCH = "SESSION_ACTOR_MISMATCH"
ERR_SESSION_RUNTIME_MISMATCH = "SESSION_RUNTIME_MISMATCH"
ERR_SESSION_GROUP_MISMATCH = "SESSION_GROUP_MISMATCH"
ERR_SESSION_WORKSPACE_PATH_MISMATCH = "SESSION_WORKSPACE_PATH_MISMATCH"
ERR_SESSION_BUSY = "SESSION_BUSY"
ERR_DELIVERY_ALREADY_EXISTS = "DELIVERY_ALREADY_EXISTS"
ERR_RUN_ALREADY_EXISTS = "RUN_ALREADY_EXISTS"
ERR_RUN_CANCELLED = "RUN_CANCELLED"
ERR_RUN_TIMED_OUT = "RUN_TIMED_OUT"
ERR_SESSION_RESUME_FAILED = "SESSION_RESUME_FAILED"
ERR_OUTPUT_CAPTURE_FAILED = "OUTPUT_CAPTURE_FAILED"

ERROR_CODES: tuple[str, ...] = (
    ERR_GROUP_FOREMAN_REQUIRED,
    ERR_GROUP_MULTIPLE_FOREMEN,
    ERR_TASK_RECIPIENT_REQUIRED,
    ERR_TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED,
    ERR_TASK_ROUTE_NOT_ALLOWED,
    ERR_ACTOR_NOT_FOUND,
    ERR_ACTOR_DISABLED,
    ERR_SESSION_NOT_FOUND,
    ERR_SESSION_ACTOR_MISMATCH,
    ERR_SESSION_RUNTIME_MISMATCH,
    ERR_SESSION_GROUP_MISMATCH,
    ERR_SESSION_WORKSPACE_PATH_MISMATCH,
    ERR_SESSION_BUSY,
    ERR_DELIVERY_ALREADY_EXISTS,
    ERR_RUN_ALREADY_EXISTS,
    ERR_RUN_CANCELLED,
    ERR_RUN_TIMED_OUT,
    ERR_SESSION_RESUME_FAILED,
    ERR_OUTPUT_CAPTURE_FAILED,
)

_ERROR_MESSAGES: Dict[str, str] = {
    ERR_GROUP_FOREMAN_REQUIRED: "group requires a foreman actor",
    ERR_GROUP_MULTIPLE_FOREMEN: "group must have exactly one foreman",
    ERR_TASK_RECIPIENT_REQUIRED: "task message requires a single explicit recipient actor id",
    ERR_TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED: "task message must not have multiple recipients",
    ERR_TASK_ROUTE_NOT_ALLOWED: "task message route is not allowed",
    ERR_ACTOR_NOT_FOUND: "actor not found",
    ERR_ACTOR_DISABLED: "actor is disabled",
    ERR_SESSION_NOT_FOUND: "session not found",
    ERR_SESSION_ACTOR_MISMATCH: "session owner actor mismatch",
    ERR_SESSION_RUNTIME_MISMATCH: "session runtime mismatch",
    ERR_SESSION_GROUP_MISMATCH: "session group mismatch",
    ERR_SESSION_WORKSPACE_PATH_MISMATCH: "session work directory mismatch",
    ERR_SESSION_BUSY: "session is busy with another run",
    ERR_DELIVERY_ALREADY_EXISTS: "delivery already exists for this message and recipient",
    ERR_RUN_ALREADY_EXISTS: "run already exists for this message",
    ERR_RUN_CANCELLED: "run was cancelled",
    ERR_RUN_TIMED_OUT: "run timed out",
    ERR_SESSION_RESUME_FAILED: "session resume failed",
    ERR_OUTPUT_CAPTURE_FAILED: "output capture failed",
}


def error_message(code: str) -> str:
    """Return the frozen human-readable message for an error code."""
    return _ERROR_MESSAGES.get(str(code or "").strip(), str(code or ""))


# ---------------------------------------------------------------------------
# 契约模型（foreman_managed / one_shot 模式下使用）
# ---------------------------------------------------------------------------


class TaskMessage(BaseModel):
    """任务消息：foreman_managed 模式下 Foreman↔peer 之间的有向任务通信。

    复用现有 Message 与 Ledger，不建立重复消息系统。本模型描述任务消息的
    标准化视图，实际存储仍以 ledger 事件为准。
    """

    group_id: str
    message_id: str
    sender_actor_id: str
    recipient_actor_id: str
    kind: Literal["task_instruction", "task_result"]
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now_iso)

    model_config = ConfigDict(extra="forbid")


class Delivery(BaseModel):
    """投递记录：可靠邮箱模型，每个 Actor 独立逻辑队列。

    消费条件必须包含明确的 group_id 与 recipient_actor_id。发给 a1 的任务
    只进入 a1 的队列，a2 不得消费、查询或触发该任务的 Run。
    """

    delivery_id: str
    group_id: str
    message_id: str
    recipient_actor_id: str
    state: DeliveryState = "queued"
    attempts: int = 0
    locked_by: Optional[str] = None
    locked_at: Optional[str] = None
    last_error: Optional[str] = None
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: str = Field(default_factory=utc_now_iso)

    model_config = ConfigDict(extra="forbid")


class Run(BaseModel):
    """Run：一次任务一次进程的生命周期。

    一条任务消息对一个 Actor 最多创建一个 Run。守护进程重启后不能重复启动
    同一任务。
    """

    run_id: str
    group_id: str
    actor_id: str
    message_id: str
    delivery_id: str = ""
    session_id: str = ""
    runtime: str = ""
    execution_mode: ExecutionMode = "one_shot"
    state: RunState = "pending"
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    exit_code: Optional[int] = None
    error_code: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class RuntimeSession(BaseModel):
    """Session：上下文继续机制，绑定 Group/Actor/Runtime/工作目录。

    校验：session.owner_actor_id = run.actor_id。禁止 a1 使用 a2 的 Session、
    同一 Session 并发执行多个 Run、Session 恢复失败后静默创建新会话。
    消息路由只依据明确 recipient Actor ID，不依据 session_id 决定接收者。
    """

    session_id: str
    group_id: str
    owner_actor_id: str
    runtime: str = ""
    work_dir: str = ""
    execution_mode: ExecutionMode = "one_shot"
    state: RuntimeSessionState = "idle"
    current_run_id: Optional[str] = None
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: str = Field(default_factory=utc_now_iso)

    model_config = ConfigDict(extra="forbid")


class StandardEvent(BaseModel):
    """标准事件：统一事件格式，标准化结构、不修改内容。

    Web、Ledger 上层视图和其他 Actor 优先消费标准事件，不直接消费杂乱的
    Runtime 原始格式。无法识别的输出封装为 raw.stdout / raw.stderr。
    """

    event_id: str
    group_id: str
    actor_id: str
    run_id: str
    session_id: str = ""
    runtime: str = ""
    sequence: int = 0
    stream: StandardEventStream = "event"
    event_type: StandardEventType = "unknown"
    content: str = ""
    received_at: str = Field(default_factory=utc_now_iso)
    source_reference: str = ""

    model_config = ConfigDict(extra="forbid")


class RunArtifact(BaseModel):
    """Run 产物：完整保存智能体输出。

    原始 stdout/stderr 分开保存、不修改内容、不删除重复、不以摘要替代、
    不因解析失败而丢弃。无法识别的输出封装为 raw.stdout / raw.stderr。
    """

    artifact_id: str
    run_id: str
    kind: RunArtifactKind
    path: str = ""
    sha256: str = ""
    bytes: int = 0
    mime_type: str = ""

    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------
# 纯路由校验（无副作用，可独立测试）
# ---------------------------------------------------------------------------


class RouteValidationError(ValueError):
    """路由校验失败。``code`` 为冻结错误码之一。"""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        msg = f"{code}: {detail}" if detail else error_message(code)
        super().__init__(msg)


def _is_broadcast_tokens(tokens: Any) -> bool:
    """判断接收者列表是否为广播语义（空、@all、@peers）。"""
    if not isinstance(tokens, list):
        return False
    if not tokens:
        return True
    cleaned = [str(t or "").strip() for t in tokens if isinstance(t, str) and str(t).strip()]
    if not cleaned:
        return True
    return all(t in ("@all", "@peers") for t in cleaned)


def validate_task_route(
    *,
    coordination_mode: CoordinationMode,
    kind: TaskMessageKind,
    sender_role: str,
    recipient_tokens: List[str],
    resolved_recipient_ids: List[str],
    recipient_roles: List[str],
    same_group: bool = True,
) -> None:
    """校验任务消息路由规则（纯函数，无副作用）。

    仅在 ``foreman_managed`` 模式下对 ``task_instruction`` / ``task_result``
    强制严格规则。``legacy`` 模式与 ``chat`` / ``system`` 消息不做限制，
    保持现有广播/多接收者兼容语义。

    参数：
        coordination_mode: 协调模式。
        kind: 消息语义类别。
        sender_role: 发送者角色（``foreman`` / ``peer``）。
        recipient_tokens: 原始接收者 token（用于检测广播）。
        resolved_recipient_ids: 解析后的明确 Actor ID 列表。
        recipient_roles: 与 resolved_recipient_ids 对应的角色列表。
        same_group: 发送者与接收者是否属于同一 Group。

    抛出：
        RouteValidationError: 校验失败，携带冻结错误码。
    """
    if coordination_mode != "foreman_managed":
        return
    if kind not in ("task_instruction", "task_result"):
        return

    # 禁止广播 / 空接收者
    if _is_broadcast_tokens(recipient_tokens) or not resolved_recipient_ids:
        raise RouteValidationError(ERR_TASK_RECIPIENT_REQUIRED, "task message must not broadcast")

    # 禁止多接收者
    if len(resolved_recipient_ids) != 1:
        raise RouteValidationError(
            ERR_TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED,
            f"expected 1 recipient, got {len(resolved_recipient_ids)}",
        )

    if not same_group:
        raise RouteValidationError(ERR_TASK_ROUTE_NOT_ALLOWED, "sender and recipient must be in the same group")

    recipient_role = recipient_roles[0] if recipient_roles else ""

    if kind == "task_instruction":
        # sender.role=foreman, recipient.role=peer
        if sender_role != "foreman":
            raise RouteValidationError(ERR_TASK_ROUTE_NOT_ALLOWED, "task_instruction sender must be foreman")
        if recipient_role != "peer":
            raise RouteValidationError(ERR_TASK_ROUTE_NOT_ALLOWED, "task_instruction recipient must be a peer")
    else:  # task_result
        # sender.role=peer, recipient=foreman
        if sender_role != "peer":
            raise RouteValidationError(ERR_TASK_ROUTE_NOT_ALLOWED, "task_result sender must be a peer")
        if recipient_role != "foreman":
            raise RouteValidationError(ERR_TASK_ROUTE_NOT_ALLOWED, "task_result recipient must be the foreman")


def validate_session_owner(
    *,
    session_owner_actor_id: str,
    run_actor_id: str,
    session_group_id: str,
    run_group_id: str,
    session_runtime: str,
    run_runtime: str,
    session_work_dir: str,
    run_work_dir: str,
) -> None:
    """校验 Session 所有权与一致性（纯函数，无副作用）。

    禁止 a1 使用 a2 的 Session；禁止跨 Group/Runtime/工作目录复用 Session。
    """
    if session_owner_actor_id != run_actor_id:
        raise RouteValidationError(ERR_SESSION_ACTOR_MISMATCH)
    if session_group_id != run_group_id:
        raise RouteValidationError(ERR_SESSION_GROUP_MISMATCH)
    if session_runtime and run_runtime and session_runtime != run_runtime:
        raise RouteValidationError(ERR_SESSION_RUNTIME_MISMATCH)
    if session_work_dir and run_work_dir and session_work_dir != run_work_dir:
        raise RouteValidationError(ERR_SESSION_WORKSPACE_PATH_MISMATCH)


def is_run_terminal(state: RunState) -> bool:
    """Run 是否处于终态。"""
    return state in ("succeeded", "failed", "cancelled", "timed_out")


def is_delivery_terminal(state: DeliveryState) -> bool:
    """Delivery 是否处于终态。"""
    return state in ("delivered", "failed")
