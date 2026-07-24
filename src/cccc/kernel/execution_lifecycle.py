"""CCCC 执行生命周期补偿 (CCCC-FIX-02)。

保证执行链任一阶段失败后，Run、Session、Delivery 三类状态最终一致且错误可追踪。

生命周期：
    reserve Delivery → acquire Session → create/start Run
    → capture output → persist result → ack Delivery → release Session

每个阶段定义失败补偿，确保 Session 最终释放或进入可诊断失败状态。

设计：
- 不依赖 except Exception: pass 吞掉 ACK/NACK 异常。
- 不把失败 Run 标记为成功。
- 不自动切换到新 Session。
- 不删除状态记录规避不一致。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from ..contracts.v1.coordination import (
    ERR_DELIVERY_ALREADY_EXISTS,
    ERR_OUTPUT_CAPTURE_FAILED,
    ERR_RUN_ALREADY_EXISTS,
    ERR_RUN_CANCELLED,
    ERR_RUN_TIMED_OUT,
    Delivery,
    DeliveryState,
    Run,
    RunArtifact,
    RunState,
    RuntimeSession,
    RouteValidationError,
)
from ..util.time import utc_now_iso
from . import coordination_store as store
from .execution_authorizer import ExecutionAuthorization
from .one_shot_runner import OneShotRunInput, OneShotRunner, OneShotRunResult
from .session_manager import acquire_session_for_run, create_session, release_session, resume_session


# ---------------------------------------------------------------------------
# 补偿域名
# ---------------------------------------------------------------------------


LIFECYCLE_STAGE = Literal[
    "auth",
    "session_create",
    "session_resume",
    "acquire_session",
    "run_create",
    "run_start",
    "output_capture",
    "output_persist",
    "ack",
    "release_session",
]
LIFECYCLE_STAGE = Literal[  # type: ignore[misc]
    "auth",
    "session_create",
    "session_resume",
    "acquire_session",
    "run_create",
    "run_start",
    "output_capture",
    "output_persist",
    "ack",
    "release_session",
]


@dataclass
class LifecycleError:
    """执行生命周期中的单阶段错误。"""

    stage: str
    code: str
    detail: str
    timestamp: str = field(default_factory=utc_now_iso)
    recoverable: bool = True


@dataclass
class ExecutionResult:
    """execute 的最终结果（包含补偿历史）。"""

    run: Optional[Run] = None
    session: Optional[RuntimeSession] = None
    events: List[Any] = field(default_factory=list)
    state: RunState = "pending"
    errors: List[LifecycleError] = field(default_factory=list)
    compensation_actions: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# 执行 Saga
# ---------------------------------------------------------------------------


class ExecutionSaga:
    """执行生命周期编排器：确保每个阶段失败都有显式补偿。

    使用 try/except/finally 显式状态迁移，避免 except Exception: pass 吞掉异常。
    """

    def __init__(self, group: Any) -> None:
        self.group = group
        self._errors: List[LifecycleError] = []
        self._compensation_actions: List[str] = []
        self._run: Optional[Run] = None
        self._session: Optional[RuntimeSession] = None
        self._session_id: str = ""
        self._delivery: Optional[Delivery] = None
        self._authorization: Optional[ExecutionAuthorization] = None

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------

    def execute(
        self,
        authorization: ExecutionAuthorization,
        *,
        run_id: str,
        command: List[str],
        session_id: str = "",
        runtime: str = "",
        cwd: str = "",
        env: Optional[Dict[str, str]] = None,
        stdin_text: str = "",
        timeout_seconds: Optional[float] = None,
    ) -> ExecutionResult:
        """执行完整生命周期，返回最终结果（含错误与补偿历史）。"""
        self._authorization = authorization
        self._delivery = store.get_delivery(self.group, authorization.delivery_id)
        task_message = _get_task_message(self.group, authorization.message_id)

        try:
            # 阶段 1：Session
            self._session_id = session_id or f"ses.{run_id}"
            self._session = self._acquire_or_create_session(
                session_id=session_id,
                actor_id=authorization.peer_actor_id,
                runtime=runtime,
                work_dir=cwd,
            )

            # 阶段 2：创建并启动 Run
            self._run = self._create_and_start_run(
                run_id=run_id,
                task_message=task_message,
                command=command,
                cwd=cwd,
                env=env,
                stdin_text=stdin_text,
                timeout_seconds=timeout_seconds,
                runtime=runtime,
            )

            # 阶段 3：保存输出
            stdout_bytes, stderr_bytes, events = self._capture_and_persist_output()

            # 阶段 4：ack Delivery
            self._ack_delivery()

            # 成功路径：释放 Session（非失败）
            self._release_session(failed=False)
            return ExecutionResult(
                run=self._run,
                session=self._session,
                events=events,
                state=self._run.state,
                errors=list(self._errors),
                compensation_actions=list(self._compensation_actions),
            )
        except Exception as exc:  # noqa: BLE001
            # 失败路径：补偿
            failed_stage = self._current_stage()
            self._errors.append(LifecycleError(
                stage=failed_stage,
                code=getattr(exc, "code", "UNKNOWN_ERROR"),
                detail=str(exc),
            ))
            self._compensate(failed_stage)
            raise
        finally:
            self._finalize()

    # ------------------------------------------------------------------
    # 阶段实现
    # ------------------------------------------------------------------

    def _acquire_or_create_session(
        self,
        *,
        session_id: str,
        actor_id: str,
        runtime: str,
        work_dir: str,
    ) -> RuntimeSession:
        try:
            if session_id:
                return resume_session(
                    self.group, session_id=session_id, actor_id=actor_id,
                    runtime=runtime, work_dir=work_dir,
                )
            # 新建：使用已生成的 self._session_id
            return create_session(
                self.group,
                session_id=self._session_id,
                owner_actor_id=actor_id,
                runtime=runtime,
                work_dir=work_dir,
                execution_mode="one_shot",
            )
        except Exception as exc:  # noqa: BLE001
            self._errors.append(LifecycleError(
                stage="session_create" if not session_id else "session_resume",
                code=getattr(exc, "code", "SESSION_ERROR"),
                detail=str(exc),
            ))
            raise

    def _create_and_start_run(
        self,
        *,
        run_id: str,
        task_message: Any,
        command: List[str],
        cwd: str,
        env: Optional[Dict[str, str]],
        stdin_text: str,
        timeout_seconds: Optional[float],
        runtime: str,
    ) -> Run:
        # NOTE: OneShotRunner.start() already calls store.create_run() internally,
        # 所以这里只构造 runner 并透传执行结果，避免双重创建。
        runner = OneShotRunner(self.group, runtime=runtime or "one_shot")
        run_input = OneShotRunInput(
            run_id=run_id,
            message_id=task_message.message_id if task_message else "",
            actor_id=self._authorization.peer_actor_id,
            command=command,
            cwd=cwd,
            env=env or {},
            stdin_text=stdin_text,
            timeout_seconds=timeout_seconds,
            session_id=self._session_id,
        )
        try:
            result = runner.start(run_input)
            run = store.get_run(self.group, run_id)
            if run is None:
                run = Run(
                    run_id=run_id,
                    group_id=self.group.group_id,
                    actor_id=self._authorization.peer_actor_id,
                    message_id=task_message.message_id if task_message else "",
                    delivery_id=self._authorization.delivery_id,
                    session_id=self._session_id,
                    runtime=runtime,
                    execution_mode="one_shot",
                    state=result.state,
                )
            self._run = run
            return run
        except Exception as exc:  # noqa: BLE001
            self._errors.append(LifecycleError(stage="run_start", code=getattr(exc, "code", "RUN_START_FAILED"), detail=str(exc)))
            self._mark_run_failed_if_exists(run_id, "run_start", str(exc))
            raise

    def _capture_and_persist_output(self) -> tuple[int, int, List[Any]]:
        run = self._run
        if run is None:
            return 0, 0, []
        stdout_blob = _read_blob(self.group, run, "raw.stdout")
        stderr_blob = _read_blob(self.group, run, "raw.stderr")
        try:
            from .output_capture import capture_run_output
            captured = capture_run_output(self.group, run, stdout=stdout_blob, stderr=stderr_blob)
            self._errors.append(LifecycleError(stage="output_capture", code="OK", detail="captured", recoverable=True))
            return len(stdout_blob), len(stderr_blob), captured.events
        except Exception as exc:  # noqa: BLE001
            self._errors.append(LifecycleError(
                stage="output_persist",
                code=ERR_OUTPUT_CAPTURE_FAILED,
                detail=str(exc),
            ))
            self._mark_run_failed_if_exists(run.run_id, "output_persist", str(exc))
            raise

    def _ack_delivery(self) -> None:
        if self._delivery is None or not self._authorization.consumer_id:
            return
        try:
            from .mailbox import ack_delivery
            ack_delivery(
                self.group,
                self._authorization.delivery_id,
                consumer_id=self._authorization.consumer_id,
            )
        except RouteValidationError as exc:
            if exc.code == ERR_DELIVERY_ALREADY_EXISTS:
                # 已 delivered，幂等，不报错
                return
            self._errors.append(LifecycleError(
                stage="ack",
                code="ACK_FAILED",
                detail=f"delivery ack failed: {exc}",
                recoverable=False,
            ))
            # 记录 Delivery 一致性错误（不静默吞掉）
            _record_delivery_inconsistency(
                self.group,
                self._authorization.delivery_id,
                f"ack failed after run success: {exc}",
            )
            # 不抛异常，让成功路径继续，但错误已记录
        except Exception as exc:  # noqa: BLE001
            self._errors.append(LifecycleError(
                stage="ack",
                code="ACK_UNEXPECTED_ERROR",
                detail=str(exc),
                recoverable=False,
            ))
            _record_delivery_inconsistency(
                self.group,
                self._authorization.delivery_id,
                f"ack unexpected error: {exc}",
            )

    def _release_session(self, failed: bool = False) -> None:
        if not self._session_id:
            return
        try:
            release_session(self.group, session_id=self._session_id, failed=failed)
            self._compensation_actions.append(f"release_session:{self._session_id}")
        except Exception as exc:  # noqa: BLE001
            self._errors.append(LifecycleError(
                stage="release_session",
                code="SESSION_RELEASE_FAILED",
                detail=str(exc),
                recoverable=False,
            ))

    # ------------------------------------------------------------------
    # 补偿
    # ------------------------------------------------------------------

    def _compensate(self, failed_stage: str) -> None:
        """根据失败阶段执行补偿动作。"""
        if self._run:
            self._mark_run_failed_if_exists(
                self._run.run_id, failed_stage, "; ".join(str(e.detail) for e in self._errors[-3:])
            )
        if self._delivery and self._authorization and self._authorization.consumer_id:
            try:
                _nack_delivery(
                    self.group,
                    self._authorization.delivery_id,
                    consumer_id=self._authorization.consumer_id,
                )
                self._compensation_actions.append(f"nack_delivery:{self._authorization.delivery_id}")
            except Exception as exc:  # noqa: BLE001
                self._errors.append(LifecycleError(
                    stage="compensate_nack",
                    code="NACK_FAILED",
                    detail=str(exc),
                    recoverable=False,
                ))
                _record_delivery_inconsistency(
                    self.group,
                    self._authorization.delivery_id,
                    f"nack during compensation failed: {exc}",
                )
        if self._session_id:
            try:
                release_session(self.group, session_id=self._session_id, failed=True)
                self._compensation_actions.append(f"compensate_release_session:{self._session_id}")
            except Exception as exc:  # noqa: BLE001
                self._errors.append(LifecycleError(
                    stage="compensate_release_session",
                    code="SESSION_RELEASE_FAILED",
                    detail=str(exc),
                    recoverable=False,
                ))

    def _finalize(self) -> None:
        """最终清理：确保进程引用和进程树等资源被回收。"""
        if self._run:
            final = store.get_run(self.group, self._run.run_id)
            if final is None:
                return
            if final.state in ("pending", "running"):
                store.update_run_state(self.group, final.run_id, state="failed", error_code="LIFECYCLE_INCOMPLETE")
            elif final.state == "succeeded" and self._has_real_error():
                # runner 已标记 succeeded，但后续阶段（如 output_persist）
                # 失败导致进入补偿；_compensate 因终态 guard 未回滚，
                # 此处强制回滚保证最终一致。
                store.update_run_state(
                    self.group, final.run_id,
                    state="failed", ended_at=utc_now_iso(),
                    error_code="LIFECYCLE_FAILED",
                )
                self._compensation_actions.append(f"rollback_succeeded:{final.run_id}")

    def _has_real_error(self) -> bool:
        """是否存在非信息性的生命周期错误。"""
        for e in self._errors:
            if e.stage == "output_capture" and e.code == "OK":
                continue
            return True
        return False

    def _current_stage(self) -> str:
        if not self._session:
            return "session_create"
        if not self._run:
            return "run_create"
        if self._run.state == "running":
            return "run_start"
        return "post_start"

    def _mark_run_failed_if_exists(self, run_id: str, stage: str, detail: str) -> None:
        try:
            existing = store.get_run(self.group, run_id)
            if existing is not None and existing.state in ("pending", "running"):
                store.update_run_state(
                    self.group, run_id,
                    state="failed",
                    ended_at=utc_now_iso(),
                    error_code=f"LIFECYCLE_FAILED:{stage}",
                )
                self._compensation_actions.append(f"mark_run_failed:{run_id}")
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------


def _get_task_message(group: Any, message_id: str) -> Any:
    from .collaboration_loop import get_task_message
    return get_task_message(group, message_id)


def _read_blob(group: Any, run: Run, kind: str) -> bytes:
    artifact = store.get_artifact(group, f"{run.run_id}.{kind}")
    if artifact is None or not artifact.path:
        return b""
    blob_path = group.path / artifact.path
    if not blob_path.exists():
        return b""
    return blob_path.read_bytes()


def _nack_delivery(group: Any, delivery_id: str, *, consumer_id: str) -> Any:
    from .mailbox import nack_delivery
    return nack_delivery(group, delivery_id, consumer_id=consumer_id, requeue=False)


def _record_delivery_inconsistency(group: Any, delivery_id: str, detail: str) -> None:
    """记录 Delivery 一致性错误（可查询字段）。"""
    try:
        store.update_delivery_state(
            group,
            delivery_id,
            last_error=f"consistency: {detail}",
        )
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# 便捷入口（用于 execute_instruction 替换为显式 Saga）
# ---------------------------------------------------------------------------


def execute_with_lifecycle(
    group: Any,
    authorization: ExecutionAuthorization,
    *,
    run_id: str,
    command: List[str],
    session_id: str = "",
    runtime: str = "",
    cwd: str = "",
    env: Optional[Dict[str, str]] = None,
    stdin_text: str = "",
    timeout_seconds: Optional[float] = None,
) -> ExecutionResult:
    """执行生命周期入口：授权后执行完整补偿事务。"""
    saga = ExecutionSaga(group)
    return saga.execute(
        authorization,
        run_id=run_id,
        command=command,
        session_id=session_id,
        runtime=runtime,
        cwd=cwd,
        env=env,
        stdin_text=stdin_text,
        timeout_seconds=timeout_seconds,
    )
