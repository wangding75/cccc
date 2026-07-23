"""CCCC-CORE-11 协调控制 API.

提供统一控制接口，UI 无需读取内部文件即可完成操作：
1. Workspace（Group）协调设置 API（coordination_mode / execution_mode）。
2. Actor API（foreman/peer 角色、可见性）。
3. Message API（task_message 链查询、投递状态）。
4. Run API（Run 状态、产物）。
5. Session API（Session 生命周期与归属）。
6. Event 查询 API（标准事件流）。

只暴露已有 kernel 模块的查询/只读视图，避免 UI 直读内部文件。写操作（下发指令/执行）
复用 collaboration_loop，受路由校验约束。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query

from ....kernel import collaboration_loop as loop
from ....kernel import coordination_store as store
from ....kernel.actors import find_actor, find_foreman, get_effective_role
from ....kernel.group import load_group
from ..schemas import RouteContext, require_group


def _group_or_404(group_id: str):
    group = load_group(group_id)
    if group is None:
        raise HTTPException(status_code=404, detail={"code": "group_not_found", "message": "group not found", "details": {"group_id": group_id}})
    return group


def _actor_or_404(group, actor_id: str):
    actor = find_actor(group, actor_id)
    if actor is None:
        raise HTTPException(status_code=404, detail={"code": "actor_not_found", "message": "actor not found", "details": {"actor_id": actor_id}})
    return actor


def create_coordination_routers(ctx: RouteContext) -> List[APIRouter]:
    """构建协调控制 API 路由（group 作用域）。"""
    group_router = APIRouter(prefix="/api/v1/groups/{group_id}", dependencies=[Depends(require_group)])

    # ------------------------------------------------------------------
    # Workspace（Group）协调设置
    # ------------------------------------------------------------------

    @group_router.get("/coordination/settings")
    async def get_coordination_settings(group_id: str) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        settings = store.load_settings_doc(group)
        return {"group_id": group_id, "settings": settings}

    # ------------------------------------------------------------------
    # Actor API（协调视图：角色、是否 foreman）
    # ------------------------------------------------------------------

    @group_router.get("/coordination/actors")
    async def list_coordination_actors(group_id: str) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        foreman = find_foreman(group)
        foreman_id = foreman.get("id") if foreman else None
        actors_doc = group.doc.get("actors") or []
        items: List[Dict[str, Any]] = []
        for a in actors_doc:
            aid = a.get("id") or a.get("actor_id") or ""
            if not aid:
                continue
            items.append({
                "actor_id": aid,
                "title": a.get("title") or aid,
                "role": get_effective_role(group, aid),
                "is_foreman": aid == foreman_id,
                "enabled": bool(a.get("enabled", True)),
                "runtime": a.get("runtime") or "",
            })
        return {"group_id": group_id, "foreman_actor_id": foreman_id, "actors": items}

    @group_router.get("/coordination/actors/{actor_id}")
    async def get_coordination_actor(group_id: str, actor_id: str) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        actor = _actor_or_404(group, actor_id)
        foreman = find_foreman(group)
        foreman_id = foreman.get("id") if foreman else None
        return {
            "group_id": group_id,
            "actor_id": actor_id,
            "title": actor.get("title") or actor_id,
            "role": get_effective_role(group, actor_id),
            "is_foreman": actor_id == foreman_id,
            "enabled": bool(actor.get("enabled", True)),
            "runtime": actor.get("runtime") or "",
        }

    # ------------------------------------------------------------------
    # Message API（task_message 链 + 投递状态）
    # ------------------------------------------------------------------

    @group_router.get("/coordination/messages")
    async def list_coordination_messages(
        group_id: str,
        kind: Optional[str] = Query(default=None),
        sender: Optional[str] = Query(default=None),
        recipient: Optional[str] = Query(default=None),
    ) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        items = loop.list_task_messages(
            group,
            sender_actor_id=sender,
            recipient_actor_id=recipient,
            kind=kind if kind in ("task_instruction", "task_result") else None,  # type: ignore[arg-type]
        )
        return {
            "group_id": group_id,
            "messages": [m.model_dump() for m in items],
        }

    @group_router.get("/coordination/messages/{message_id}")
    async def get_coordination_message(group_id: str, message_id: str) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        tm = loop.get_task_message(group, message_id)
        if tm is None:
            raise HTTPException(status_code=404, detail={"code": "message_not_found", "message": "task message not found", "details": {"message_id": message_id}})
        return tm.model_dump()

    @group_router.get("/coordination/messages/{message_id}/chain")
    async def get_coordination_message_chain(group_id: str, message_id: str) -> Dict[str, Any]:
        # 完整链路可追踪：instruction -> result -> run -> session -> events
        group = _group_or_404(group_id)
        chain = loop.trace_chain(group, message_id)
        return {
            "group_id": group_id,
            "instruction": chain["instruction"].model_dump() if chain.get("instruction") else None,
            "result": chain["result"].model_dump() if chain.get("result") else None,
            "run": chain["run"].model_dump() if chain.get("run") else None,
            "session": chain["session"].model_dump() if chain.get("session") else None,
            "events": [e.model_dump() for e in chain.get("events", [])],
        }

    @group_router.get("/coordination/deliveries")
    async def list_coordination_deliveries(
        group_id: str,
        recipient: Optional[str] = Query(default=None),
        message_id: Optional[str] = Query(default=None),
    ) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        items = store.list_deliveries(group, recipient_actor_id=recipient, message_id=message_id)
        return {"group_id": group_id, "deliveries": [d.model_dump() for d in items]}

    # ------------------------------------------------------------------
    # Run API
    # ------------------------------------------------------------------

    @group_router.get("/coordination/runs")
    async def list_coordination_runs(
        group_id: str,
        actor_id: Optional[str] = Query(default=None),
    ) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        runs = store.list_runs(group, actor_id=actor_id) if actor_id else store.list_runs(group)
        return {"group_id": group_id, "runs": [r.model_dump() for r in runs]}

    @group_router.get("/coordination/runs/{run_id}")
    async def get_coordination_run(group_id: str, run_id: str) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        run = store.get_run(group, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail={"code": "run_not_found", "message": "run not found", "details": {"run_id": run_id}})
        return run.model_dump()

    @group_router.get("/coordination/runs/{run_id}/artifacts")
    async def list_coordination_run_artifacts(group_id: str, run_id: str) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        arts = store.list_artifacts(group, run_id=run_id)
        return {"group_id": group_id, "run_id": run_id, "artifacts": [a.model_dump() for a in arts]}

    # ------------------------------------------------------------------
    # Session API（归属过滤，防跨 Actor 查询）
    # ------------------------------------------------------------------

    @group_router.get("/coordination/sessions")
    async def list_coordination_sessions(
        group_id: str,
        owner_actor_id: Optional[str] = Query(default=None),
    ) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        items = store.list_sessions(group, owner_actor_id=owner_actor_id)
        return {"group_id": group_id, "sessions": [s.model_dump() for s in items]}

    @group_router.get("/coordination/sessions/{session_id}")
    async def get_coordination_session(group_id: str, session_id: str) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        s = store.get_session(group, session_id)
        if s is None:
            raise HTTPException(status_code=404, detail={"code": "session_not_found", "message": "session not found", "details": {"session_id": session_id}})
        return s.model_dump()

    # ------------------------------------------------------------------
    # Event 查询 API（标准事件流）
    # ------------------------------------------------------------------

    @group_router.get("/coordination/runs/{run_id}/events")
    async def list_coordination_run_events(group_id: str, run_id: str) -> Dict[str, Any]:
        group = _group_or_404(group_id)
        events = store.read_standard_events(group, run_id)
        return {"group_id": group_id, "run_id": run_id, "events": [e.model_dump() for e in events]}

    return [group_router]


def register_coordination_routes(app: FastAPI, *, ctx: RouteContext) -> None:
    for router in create_coordination_routers(ctx):
        app.include_router(router)
