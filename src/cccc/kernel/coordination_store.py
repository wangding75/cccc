"""CCCC 协调数据模型持久化 (CCCC-CORE-02).

为 CCCC-CORE-01 冻结的 coordination 契约提供文件型 JSON 持久化基础：
coordination 设置、Delivery、Run、RuntimeSession、RunArtifact 的存储、
外键关系、唯一约束、查询索引、幂等迁移与基础 CRUD。

设计原则（与现有架构一致）：
- 文件型持久化，无 SQL。位置：``~/.cccc/groups/<gid>/state/coordination/``。
- 原子写（``atomic_write_json``），单写者由 daemon 保证。
- 向后兼容：旧 group 缺少 ``state/coordination`` 视为空。
- 不实现运行时行为（路由强制、Runner 进程、Session resume CLI 属后续任务）。

布局::

    groups/<gid>/state/coordination/
        settings.json
        deliveries/<delivery_id>.json
        deliveries_index.json
        runs/<run_id>.json
        runs_index.json
        sessions/<session_id>.json
        sessions_index.json
        artifacts/<artifact_id>.json
        artifacts_index.json
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..contracts.v1.coordination import (
    DEFAULT_COORDINATION_MODE,
    DEFAULT_EXECUTION_MODE,
    ERR_DELIVERY_ALREADY_EXISTS,
    ERR_GROUP_FOREMAN_REQUIRED,
    ERR_RUN_ALREADY_EXISTS,
    ERR_SESSION_BUSY,
    CoordinationMode,
    Delivery,
    DeliveryState,
    ExecutionMode,
    Run,
    RunArtifact,
    RunState,
    RouteValidationError,
    RuntimeSession,
    RuntimeSessionState,
    StandardEvent,
)
from ..util.fs import atomic_write_json, read_json
from ..util.time import utc_now_iso
from .actors import find_actor, find_foreman, list_actors
from .group import Group

SETTINGS_VERSION = 1
INDEX_VERSION = 1

_LOCK = threading.Lock()
"""进程内串行化存储写操作（daemon 单写者前提下额外防护并发测试）。"""


# ---------------------------------------------------------------------------
# 路径与布局
# ---------------------------------------------------------------------------


def coordination_dir(group: Group) -> Path:
    return group.path / "state" / "coordination"


def _settings_path(group: Group) -> Path:
    return coordination_dir(group) / "settings.json"


def _deliveries_dir(group: Group) -> Path:
    return coordination_dir(group) / "deliveries"


def _runs_dir(group: Group) -> Path:
    return coordination_dir(group) / "runs"


def _sessions_dir(group: Group) -> Path:
    return coordination_dir(group) / "sessions"


def _artifacts_dir(group: Group) -> Path:
    return coordination_dir(group) / "artifacts"


def _delivery_path(group: Group, delivery_id: str) -> Path:
    return _deliveries_dir(group) / f"{_safe_id(delivery_id)}.json"


def _run_path(group: Group, run_id: str) -> Path:
    return _runs_dir(group) / f"{_safe_id(run_id)}.json"


def _session_path(group: Group, session_id: str) -> Path:
    return _sessions_dir(group) / f"{_safe_id(session_id)}.json"


def _artifact_path(group: Group, artifact_id: str) -> Path:
    return _artifacts_dir(group) / f"{_safe_id(artifact_id)}.json"


def _safe_id(value: str) -> str:
    """Sanitize an id for use as a filename (defense-in-depth against path traversal)."""
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("empty id")
    # Reject any path separator or parent-dir component outright.
    if "/" in raw or "\\" in raw or raw in (".", "..") or raw.startswith("."):
        raise ValueError(f"unsafe id: {raw!r}")
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in raw)
    if not cleaned or cleaned in (".", ".."):
        raise ValueError(f"unsafe id: {raw!r}")
    return cleaned


def ensure_coordination_layout(group: Group) -> None:
    """幂等创建 coordination 目录结构。"""
    base = coordination_dir(group)
    base.mkdir(parents=True, exist_ok=True)
    _deliveries_dir(group).mkdir(parents=True, exist_ok=True)
    _runs_dir(group).mkdir(parents=True, exist_ok=True)
    _sessions_dir(group).mkdir(parents=True, exist_ok=True)
    _artifacts_dir(group).mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def default_settings_doc() -> Dict[str, Any]:
    return {
        "v": SETTINGS_VERSION,
        "coordination_mode": DEFAULT_COORDINATION_MODE,
        "execution_mode": DEFAULT_EXECUTION_MODE,
        "default_runtime": "",
        "default_work_dir": "",
        "updated_at": utc_now_iso(),
    }


def load_settings_doc(group: Group) -> Dict[str, Any]:
    """读取 coordination 设置，缺失则返回默认（向后兼容，不落盘）。"""
    ensure_coordination_layout(group)
    raw = read_json(_settings_path(group))
    if not isinstance(raw, dict) or not raw:
        return default_settings_doc()
    base = default_settings_doc()
    base["coordination_mode"] = _as_coordination_mode(raw.get("coordination_mode"))
    base["execution_mode"] = _as_execution_mode(raw.get("execution_mode"))
    base["default_runtime"] = str(raw.get("default_runtime") or "").strip()
    base["default_work_dir"] = str(raw.get("default_work_dir") or "").strip()
    base["updated_at"] = str(raw.get("updated_at") or base["updated_at"])
    return base


def save_settings_doc(group: Group, doc: Dict[str, Any]) -> Dict[str, Any]:
    ensure_coordination_layout(group)
    normalized = {
        "v": SETTINGS_VERSION,
        "coordination_mode": _as_coordination_mode(doc.get("coordination_mode")),
        "execution_mode": _as_execution_mode(doc.get("execution_mode")),
        "default_runtime": str(doc.get("default_runtime") or "").strip(),
        "default_work_dir": str(doc.get("default_work_dir") or "").strip(),
        "updated_at": utc_now_iso(),
    }
    with _LOCK:
        atomic_write_json(_settings_path(group), normalized)
    return normalized


def update_settings(
    group: Group,
    *,
    coordination_mode: Optional[CoordinationMode] = None,
    execution_mode: Optional[ExecutionMode] = None,
    default_runtime: Optional[str] = None,
    default_work_dir: Optional[str] = None,
) -> Dict[str, Any]:
    doc = load_settings_doc(group)
    if coordination_mode is not None:
        doc["coordination_mode"] = _as_coordination_mode(coordination_mode)
    if execution_mode is not None:
        doc["execution_mode"] = _as_execution_mode(execution_mode)
    if default_runtime is not None:
        doc["default_runtime"] = str(default_runtime).strip()
    if default_work_dir is not None:
        doc["default_work_dir"] = str(default_work_dir).strip()
    return save_settings_doc(group, doc)


def require_foreman_managed_settings(group: Group) -> Dict[str, Any]:
    """读取 foreman_managed 模式设置；若启用但缺少 foreman 则抛 GROUP_FOREMAN_REQUIRED。"""
    doc = load_settings_doc(group)
    if doc["coordination_mode"] != "foreman_managed":
        return doc
    if find_foreman(group) is None:
        raise RouteValidationError(ERR_GROUP_FOREMAN_REQUIRED, "foreman_managed mode requires a foreman actor")
    return doc


# ---------------------------------------------------------------------------
# 索引
# ---------------------------------------------------------------------------


def _empty_index() -> Dict[str, Any]:
    return {"v": INDEX_VERSION, "by_recipient": {}, "by_message": {}, "by_actor": {}, "by_session": {}, "by_run": {}}


def _index_path(group: Group, kind: str) -> Path:
    return coordination_dir(group) / f"{kind}_index.json"


def _load_index(group: Group, kind: str) -> Dict[str, Any]:
    raw = read_json(_index_path(group, kind))
    if not isinstance(raw, dict) or not raw:
        return _empty_index()
    base = _empty_index()
    for key in ("by_recipient", "by_message", "by_actor", "by_session", "by_run"):
        val = raw.get(key)
        if isinstance(val, dict):
            base[key] = {str(k): list(v) if isinstance(v, list) else [] for k, v in val.items()}
    return base


def _save_index(group: Group, kind: str, index: Dict[str, Any]) -> None:
    atomic_write_json(_index_path(group, kind), index)


def _idx_add(index: Dict[str, Any], field: str, key: str, value: str) -> None:
    bucket = index.setdefault(field, {})
    lst = bucket.setdefault(str(key), [])
    if value not in lst:
        lst.append(value)


def _idx_remove(index: Dict[str, Any], field: str, key: str, value: str) -> None:
    bucket = index.get(field)
    if not isinstance(bucket, dict):
        return
    lst = bucket.get(str(key))
    if not isinstance(lst, list):
        return
    if value in lst:
        lst.remove(value)
    if not lst:
        bucket.pop(str(key), None)


# ---------------------------------------------------------------------------
# Delivery CRUD
# ---------------------------------------------------------------------------


def create_delivery(group: Group, delivery: Delivery) -> Delivery:
    """创建投递记录。唯一约束：(message_id, recipient_actor_id) → DELIVERY_ALREADY_EXISTS。"""
    ensure_coordination_layout(group)
    _validate_actor_exists(group, delivery.recipient_actor_id)
    with _LOCK:
        existing = _find_delivery_by_message_recipient(group, delivery.message_id, delivery.recipient_actor_id)
        if existing is not None:
            raise RouteValidationError(
                ERR_DELIVERY_ALREADY_EXISTS,
                f"message={delivery.message_id} recipient={delivery.recipient_actor_id}",
            )
        path = _delivery_path(group, delivery.delivery_id)
        if path.exists():
            raise RouteValidationError(ERR_DELIVERY_ALREADY_EXISTS, f"delivery_id={delivery.delivery_id}")
        payload = delivery.model_dump()
        atomic_write_json(path, payload)
        index = _load_index(group, "deliveries")
        _idx_add(index, "by_recipient", delivery.recipient_actor_id, delivery.delivery_id)
        _idx_add(index, "by_message", delivery.message_id, delivery.delivery_id)
        _save_index(group, "deliveries", index)
    return delivery


def get_delivery(group: Group, delivery_id: str) -> Optional[Delivery]:
    raw = read_json(_delivery_path(group, delivery_id))
    if not isinstance(raw, dict) or not raw:
        return None
    return Delivery.model_validate(raw)


def list_deliveries(
    group: Group,
    *,
    recipient_actor_id: Optional[str] = None,
    message_id: Optional[str] = None,
) -> List[Delivery]:
    ensure_coordination_layout(group)
    if recipient_actor_id is None and message_id is None:
        out: List[Delivery] = []
        for p in sorted(_deliveries_dir(group).glob("*.json")):
            d = get_delivery(group, p.stem)
            if d is not None:
                out.append(d)
        return out
    index = _load_index(group, "deliveries")
    ids: List[str] = []
    if recipient_actor_id is not None:
        ids.extend(index.get("by_recipient", {}).get(recipient_actor_id, []))
    if message_id is not None:
        msg_ids = set(index.get("by_message", {}).get(message_id, []))
        if recipient_actor_id is not None:
            ids = [i for i in ids if i in msg_ids]
        else:
            ids = list(msg_ids)
    out = []
    for did in ids:
        d = get_delivery(group, did)
        if d is not None:
            out.append(d)
    return out


def update_delivery_state(
    group: Group,
    delivery_id: str,
    *,
    state: Optional[DeliveryState] = None,
    attempts: Optional[int] = None,
    locked_by: Optional[str] = None,
    locked_at: Optional[str] = None,
    last_error: Optional[str] = None,
) -> Delivery:
    d = get_delivery(group, delivery_id)
    if d is None:
        raise ValueError(f"delivery not found: {delivery_id}")
    if state is not None:
        d.state = state
    if attempts is not None:
        d.attempts = attempts
    if locked_by is not None:
        d.locked_by = locked_by or None
    if locked_at is not None:
        d.locked_at = locked_at or None
    if last_error is not None:
        d.last_error = last_error or None
    d.updated_at = utc_now_iso()
    with _LOCK:
        atomic_write_json(_delivery_path(group, delivery_id), d.model_dump())
    return d


def _find_delivery_by_message_recipient(group: Group, message_id: str, recipient_actor_id: str) -> Optional[Delivery]:
    index = _load_index(group, "deliveries")
    recipient_ids = index.get("by_recipient", {}).get(recipient_actor_id, [])
    message_ids = set(index.get("by_message", {}).get(message_id, []))
    for did in recipient_ids:
        if did in message_ids:
            return get_delivery(group, did)
    return None


# ---------------------------------------------------------------------------
# Run CRUD
# ---------------------------------------------------------------------------


def create_run(group: Group, run: Run) -> Run:
    """创建 Run。唯一约束：(message_id, actor_id) → RUN_ALREADY_EXISTS。"""
    ensure_coordination_layout(group)
    _validate_actor_exists(group, run.actor_id)
    with _LOCK:
        existing = _find_run_by_message_actor(group, run.message_id, run.actor_id)
        if existing is not None:
            raise RouteValidationError(
                ERR_RUN_ALREADY_EXISTS,
                f"message={run.message_id} actor={run.actor_id}",
            )
        path = _run_path(group, run.run_id)
        if path.exists():
            raise RouteValidationError(ERR_RUN_ALREADY_EXISTS, f"run_id={run.run_id}")
        if run.session_id:
            _validate_session_owner_for_run(group, run)
        payload = run.model_dump()
        atomic_write_json(path, payload)
        index = _load_index(group, "runs")
        _idx_add(index, "by_actor", run.actor_id, run.run_id)
        _idx_add(index, "by_message", run.message_id, run.run_id)
        if run.session_id:
            _idx_add(index, "by_session", run.session_id, run.run_id)
        _save_index(group, "runs", index)
    return run


def get_run(group: Group, run_id: str) -> Optional[Run]:
    raw = read_json(_run_path(group, run_id))
    if not isinstance(raw, dict) or not raw:
        return None
    return Run.model_validate(raw)


def list_runs(
    group: Group,
    *,
    actor_id: Optional[str] = None,
    message_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> List[Run]:
    ensure_coordination_layout(group)
    if actor_id is None and message_id is None and session_id is None:
        out: List[Run] = []
        for p in sorted(_runs_dir(group).glob("*.json")):
            r = get_run(group, p.stem)
            if r is not None:
                out.append(r)
        return out
    index = _load_index(group, "runs")
    candidate_sets: List[set[str]] = []
    if actor_id is not None:
        candidate_sets.append(set(index.get("by_actor", {}).get(actor_id, [])))
    if message_id is not None:
        candidate_sets.append(set(index.get("by_message", {}).get(message_id, [])))
    if session_id is not None:
        candidate_sets.append(set(index.get("by_session", {}).get(session_id, [])))
    ids = set.intersection(*candidate_sets) if candidate_sets else set()
    out = []
    for rid in sorted(ids):
        r = get_run(group, rid)
        if r is not None:
            out.append(r)
    return out


def update_run_state(
    group: Group,
    run_id: str,
    *,
    state: Optional[RunState] = None,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    exit_code: Optional[int] = None,
    error_code: Optional[str] = None,
) -> Run:
    r = get_run(group, run_id)
    if r is None:
        raise ValueError(f"run not found: {run_id}")
    if state is not None:
        r.state = state
    if started_at is not None:
        r.started_at = started_at or None
    if ended_at is not None:
        r.ended_at = ended_at or None
    if exit_code is not None:
        r.exit_code = exit_code
    if error_code is not None:
        r.error_code = error_code or None
    with _LOCK:
        atomic_write_json(_run_path(group, run_id), r.model_dump())
    return r


def _find_run_by_message_actor(group: Group, message_id: str, actor_id: str) -> Optional[Run]:
    index = _load_index(group, "runs")
    actor_ids = index.get("by_actor", {}).get(actor_id, [])
    message_ids = set(index.get("by_message", {}).get(message_id, []))
    for rid in actor_ids:
        if rid in message_ids:
            return get_run(group, rid)
    return None


# ---------------------------------------------------------------------------
# RuntimeSession CRUD
# ---------------------------------------------------------------------------


def create_session(group: Group, session: RuntimeSession) -> RuntimeSession:
    ensure_coordination_layout(group)
    _validate_actor_exists(group, session.owner_actor_id)
    with _LOCK:
        path = _session_path(group, session.session_id)
        if path.exists():
            raise ValueError(f"session already exists: {session.session_id}")
        # 同一 owner 最多一个 busy session（运行时由调用方在启动 Run 前校验）
        payload = session.model_dump()
        atomic_write_json(path, payload)
        index = _load_index(group, "sessions")
        _idx_add(index, "by_actor", session.owner_actor_id, session.session_id)
        _save_index(group, "sessions", index)
    return session


def get_session(group: Group, session_id: str) -> Optional[RuntimeSession]:
    raw = read_json(_session_path(group, session_id))
    if not isinstance(raw, dict) or not raw:
        return None
    return RuntimeSession.model_validate(raw)


def list_sessions(group: Group, *, owner_actor_id: Optional[str] = None) -> List[RuntimeSession]:
    ensure_coordination_layout(group)
    if owner_actor_id is None:
        out: List[RuntimeSession] = []
        for p in sorted(_sessions_dir(group).glob("*.json")):
            s = get_session(group, p.stem)
            if s is not None:
                out.append(s)
        return out
    index = _load_index(group, "sessions")
    ids = index.get("by_actor", {}).get(owner_actor_id, [])
    out = []
    for sid in ids:
        s = get_session(group, sid)
        if s is not None:
            out.append(s)
    return out


def update_session_state(
    group: Group,
    session_id: str,
    *,
    state: Optional[RuntimeSessionState] = None,
    current_run_id: Optional[str] = None,
) -> RuntimeSession:
    s = get_session(group, session_id)
    if s is None:
        raise ValueError(f"session not found: {session_id}")
    if state is not None:
        s.state = state
    if current_run_id is not None:
        s.current_run_id = current_run_id or None
    s.updated_at = utc_now_iso()
    with _LOCK:
        atomic_write_json(_session_path(group, session_id), s.model_dump())
    return s


def _validate_session_owner_for_run(group: Group, run: Run) -> None:
    """外键校验：若 Run 指定 session_id，session.owner_actor_id 必须 = run.actor_id。"""
    s = get_session(group, run.session_id)
    if s is None:
        return  # session 存在性由运行时校验；存储层仅校验归属一致性
    if s.owner_actor_id != run.actor_id:
        raise RouteValidationError(
            "SESSION_ACTOR_MISMATCH",
            f"session owner={s.owner_actor_id} run actor={run.actor_id}",
        )


# ---------------------------------------------------------------------------
# RunArtifact CRUD
# ---------------------------------------------------------------------------


def create_artifact(group: Group, artifact: RunArtifact) -> RunArtifact:
    ensure_coordination_layout(group)
    with _LOCK:
        path = _artifact_path(group, artifact.artifact_id)
        if path.exists():
            raise ValueError(f"artifact already exists: {artifact.artifact_id}")
        atomic_write_json(path, artifact.model_dump())
        index = _load_index(group, "artifacts")
        _idx_add(index, "by_run", artifact.run_id, artifact.artifact_id)
        _save_index(group, "artifacts", index)
    return artifact


def get_artifact(group: Group, artifact_id: str) -> Optional[RunArtifact]:
    raw = read_json(_artifact_path(group, artifact_id))
    if not isinstance(raw, dict) or not raw:
        return None
    return RunArtifact.model_validate(raw)


def list_artifacts(group: Group, *, run_id: Optional[str] = None) -> List[RunArtifact]:
    ensure_coordination_layout(group)
    if run_id is None:
        out: List[RunArtifact] = []
        for p in sorted(_artifacts_dir(group).glob("*.json")):
            a = get_artifact(group, p.stem)
            if a is not None:
                out.append(a)
        return out
    index = _load_index(group, "artifacts")
    ids = index.get("by_run", {}).get(run_id, [])
    out = []
    for aid in ids:
        a = get_artifact(group, aid)
        if a is not None:
            out.append(a)
    return out


# ---------------------------------------------------------------------------
# StandardEvent 追加流（轻量；完整采集器属 CCCC-CORE-08）
# ---------------------------------------------------------------------------


def events_path(group: Group, run_id: str) -> Path:
    return _runs_dir(group) / f"{_safe_id(run_id)}.events.jsonl"


def append_standard_event(group: Group, event: StandardEvent) -> None:
    """追加标准事件到 run 的 events.jsonl（有序，不修改内容）。"""
    ensure_coordination_layout(group)
    path = events_path(group, event.run_id)
    with _LOCK:
        with path.open("a", encoding="utf-8") as f:
            f.write(event.model_dump_json() + "\n")


def read_standard_events(group: Group, run_id: str) -> List[StandardEvent]:
    path = events_path(group, run_id)
    if not path.exists():
        return []
    out: List[StandardEvent] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(StandardEvent.model_validate_json(line))
        except Exception:
            continue
    return out


# ---------------------------------------------------------------------------
# 校验与工具
# ---------------------------------------------------------------------------


def _validate_actor_exists(group: Group, actor_id: str) -> None:
    if not actor_id:
        raise ValueError("missing actor_id")
    if find_actor(group, actor_id) is None:
        raise RouteValidationError("ACTOR_NOT_FOUND", f"actor not found: {actor_id}")


def _as_coordination_mode(value: Any) -> CoordinationMode:
    s = str(value or "").strip().lower()
    return "foreman_managed" if s == "foreman_managed" else "legacy"


def _as_execution_mode(value: Any) -> ExecutionMode:
    s = str(value or "").strip().lower()
    if s in ("pty", "headless", "one_shot"):
        return s  # type: ignore[return-value]
    return DEFAULT_EXECUTION_MODE
