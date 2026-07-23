"""CCCC Session 管理 (CCCC-CORE-06).

支持上下文继续：无 session_id 创建新会话，有 session_id 继续执行。
保存 session 归属，校验 Actor 所有权，防止 Session 串用。

规则：
- session.owner_actor_id = run.actor_id（所有权校验）。
- 禁止 a1 使用 a2 的 Session。
- 同一 Session 并发执行多个 Run 被拒绝（busy → SESSION_BUSY）。
- Session 恢复失败不得静默创建新会话（显式 SESSION_RESUME_FAILED）。
- 消息路由只依据明确 recipient Actor ID，不依据 session_id 决定接收者。

复用 CCCC-CORE-02 的 coordination_store 持久化。本模块只实现 Session 生命周期与
所有权校验，不实现具体 Agent CLI 的 resume（属后续 Runtime Adapter）。
"""

from __future__ import annotations

import threading
from typing import Optional

from ..contracts.v1.coordination import (
    ERR_SESSION_BUSY,
    ERR_SESSION_NOT_FOUND,
    ERR_SESSION_RESUME_FAILED,
    ExecutionMode,
    RouteValidationError,
    RuntimeSession,
    RuntimeSessionState,
    validate_session_owner,
)
from ..util.time import utc_now_iso
from . import coordination_store as store
from .actors import find_actor
from .group import Group

_LOCK = threading.Lock()
"""进程内串行化 Session 占用状态迁移（daemon 单写者前提下额外防护并发）。"""


def create_session(
    group: Group,
    *,
    session_id: str,
    owner_actor_id: str,
    runtime: str = "",
    work_dir: str = "",
    execution_mode: ExecutionMode = "one_shot",
) -> RuntimeSession:
    """创建新会话（无 session_id 场景由调用方生成 id 后调用）。保存归属。"""
    _require_actor(group, owner_actor_id)
    session = RuntimeSession(
        session_id=session_id,
        group_id=group.group_id,
        owner_actor_id=owner_actor_id,
        runtime=runtime,
        work_dir=work_dir,
        execution_mode=execution_mode,
        state="idle",
    )
    return store.create_session(group, session)


def resume_session(
    group: Group,
    *,
    session_id: str,
    actor_id: str,
    runtime: str = "",
    work_dir: str = "",
) -> RuntimeSession:
    """继续已有会话。校验所有权与一致性；恢复失败显式抛 SESSION_RESUME_FAILED。

    禁止 a1 使用 a2 的 Session。禁止跨 Group/Runtime/工作目录复用。
    """
    session = store.get_session(group, session_id)
    if session is None:
        # 恢复失败：不得静默创建新会话
        raise RouteValidationError(ERR_SESSION_NOT_FOUND, f"session not found: {session_id}")
    # 所有权 + 一致性校验（复用契约层纯校验）
    validate_session_owner(
        session_owner_actor_id=session.owner_actor_id,
        run_actor_id=actor_id,
        session_group_id=session.group_id,
        run_group_id=group.group_id,
        session_runtime=session.runtime,
        run_runtime=runtime,
        session_work_dir=session.work_dir,
        run_work_dir=work_dir,
    )
    return session


def acquire_session_for_run(
    group: Group,
    *,
    session_id: str,
    run_id: str,
    actor_id: str,
    runtime: str = "",
    work_dir: str = "",
) -> RuntimeSession:
    """在启动 Run 前占用 Session：idle → busy。同一 Session 并发 Run 被拒绝（SESSION_BUSY）。"""
    with _LOCK:
        session = resume_session(group, session_id=session_id, actor_id=actor_id, runtime=runtime, work_dir=work_dir)
        if session.state == "busy":
            # 同一 Session 并发执行多个 Run
            raise RouteValidationError(ERR_SESSION_BUSY, f"session busy: {session_id}")
        if session.state in ("closed", "failed"):
            raise RouteValidationError(ERR_SESSION_RESUME_FAILED, f"session {session.state}: {session_id}")
        return store.update_session_state(group, session_id, state="busy", current_run_id=run_id)


def release_session(group: Group, *, session_id: str, failed: bool = False) -> RuntimeSession:
    """Run 结束后释放 Session：busy → idle（或 failed）。"""
    session = store.get_session(group, session_id)
    if session is None:
        raise RouteValidationError(ERR_SESSION_NOT_FOUND, f"session not found: {session_id}")
    new_state: RuntimeSessionState = "failed" if failed else "idle"
    return store.update_session_state(group, session_id, state=new_state, clear_current_run_id=True)


def close_session(group: Group, *, session_id: str) -> RuntimeSession:
    """关闭会话（终态 closed）。"""
    return store.update_session_state(group, session_id, state="closed", clear_current_run_id=True)


def get_session(group: Group, session_id: str) -> Optional[RuntimeSession]:
    return store.get_session(group, session_id)


def list_sessions_for_actor(group: Group, actor_id: str):
    """列出某 Actor 的 Session（按 owner 过滤，防止跨 Actor 查询）。"""
    return store.list_sessions(group, owner_actor_id=actor_id)


def _require_actor(group: Group, actor_id: str) -> None:
    if not actor_id:
        raise RouteValidationError("ACTOR_NOT_FOUND", "missing actor_id")
    if find_actor(group, actor_id) is None:
        raise RouteValidationError("ACTOR_NOT_FOUND", f"actor not found: {actor_id}")
