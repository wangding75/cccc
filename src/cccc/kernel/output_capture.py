"""CCCC 输出采集 (CCCC-CORE-08).

完整保存智能体输出：
1. 保存 stdout（原始内容，不修改、不摘要）。
2. 保存 stderr（原始内容，分开保存）。
3. 记录接收顺序（递增 sequence）。
4. 生成 stream 记录（StandardEvent，按 stdout/stderr 流归类）。
5. 保留原始内容（RunArtifact 指向 blob，可追溯）。

验收：任何标准事件可以追溯原始输出——每个 StandardEvent 的 source_reference 指向
对应的 RunArtifact（artifact_id），RunArtifact 的 path 指向未修改的原始 blob。

本模块只做采集与落盘，不做语义解析（解析属 CCCC-CORE-09 Adapter）。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import List

from ..contracts.v1.coordination import (
    Run,
    RunArtifact,
    StandardEvent,
    StandardEventStream,
)
from ..util.fs import atomic_write_bytes
from ..util.time import utc_now_iso
from . import coordination_store as store
from .group import Group


@dataclass
class CapturedOutput:
    """一次 Run 的采集结果：产物 + 有序标准事件。"""

    artifacts: List[RunArtifact]
    events: List[StandardEvent]


def _outputs_dir(group: Group):
    return group.path / "state" / "coordination" / "outputs"


def _save_blob(group: Group, artifact_id: str, data: bytes) -> str:
    """保存原始内容到 blob，返回相对路径（不修改、不摘要替代）。"""
    blob_dir = _outputs_dir(group)
    blob_dir.mkdir(parents=True, exist_ok=True)
    rel = f"state/coordination/outputs/{artifact_id}.bin"
    atomic_write_bytes(group.path / rel, data)
    return rel


def _next_sequence(group: Group, run_id: str) -> int:
    """获取下一个 sequence（基于已有事件数，保证全局递增、记录接收顺序）。"""
    existing = store.read_standard_events(group, run_id)
    return len(existing)


def capture_run_output(
    group: Group,
    run: Run,
    *,
    stdout: bytes,
    stderr: bytes,
) -> CapturedOutput:
    """采集一次 Run 的完整输出：保存原始 stdout/stderr + 生成有序 stream 记录。

    - 原始 stdout/stderr 分开保存为 blob（RunArtifact 元信息 + 原始内容）。
    - 生成 StandardEvent：stream=stdout/stderr、event_type=raw.stdout/raw.stderr、
      sequence 递增记录接收顺序、source_reference 指向 artifact_id（可追溯原始输出）。
    - 不修改内容、不删除重复、不以摘要替代。
    """
    artifacts: List[RunArtifact] = []
    events: List[StandardEvent] = []
    seq = _next_sequence(group, run.run_id)
    now = utc_now_iso()

    for kind, stream, data in (
        ("raw.stdout", "stdout", stdout),
        ("raw.stderr", "stderr", stderr),
    ):
        artifact_id = f"{run.run_id}.{kind}"
        sha256 = hashlib.sha256(data).hexdigest()
        existing = store.get_artifact(group, artifact_id)
        if existing is None:
            rel_path = _save_blob(group, artifact_id, data)
            artifact = RunArtifact(
                artifact_id=artifact_id,
                run_id=run.run_id,
                kind=kind,  # type: ignore[arg-type]
                path=rel_path,
                sha256=sha256,
                bytes=len(data),
                mime_type="text/plain",
            )
            store.create_artifact(group, artifact)
            artifacts.append(artifact)
        else:
            # 已有产物（如 Runner 已保存）：复用，不重复创建，保持原始内容可追溯
            artifact = existing
            artifacts.append(artifact)

        event = StandardEvent(
            event_id=f"{run.run_id}.{kind}.{seq}",
            group_id=run.group_id,
            actor_id=run.actor_id,
            run_id=run.run_id,
            session_id=run.session_id,
            runtime=run.runtime,
            sequence=seq,
            stream=stream,  # type: ignore[arg-type]
            event_type=kind,  # type: ignore[arg-type]
            content=data.decode("utf-8", errors="replace"),
            received_at=now,
            source_reference=artifact_id,
        )
        store.append_standard_event(group, event)
        events.append(event)
        seq += 1

    return CapturedOutput(artifacts=artifacts, events=events)


def append_stream_event(
    group: Group,
    run: Run,
    *,
    stream: StandardEventStream,
    event_type: str,
    content: str,
    source_reference: str = "",
) -> StandardEvent:
    """追加一条 stream 记录（保持接收顺序）。用于流式/增量采集场景。"""
    seq = _next_sequence(group, run.run_id)
    event = StandardEvent(
        event_id=f"{run.run_id}.{stream}.{seq}",
        group_id=run.group_id,
        actor_id=run.actor_id,
        run_id=run.run_id,
        session_id=run.session_id,
        runtime=run.runtime,
        sequence=seq,
        stream=stream,
        event_type=event_type,  # type: ignore[arg-type]
        content=content,
        received_at=utc_now_iso(),
        source_reference=source_reference,
    )
    store.append_standard_event(group, event)
    return event


def trace_original_output(group: Group, event: StandardEvent) -> bytes:
    """验收：任何标准事件可以追溯原始输出。

    通过 event.source_reference（artifact_id）找到 RunArtifact，再按其 path 读取
    未修改的原始 blob。若无可追溯来源，返回空字节。
    """
    if not event.source_reference:
        return b""
    artifact = store.get_artifact(group, event.source_reference)
    if artifact is None or not artifact.path:
        return b""
    blob_path = group.path / artifact.path
    if not blob_path.exists():
        return b""
    return blob_path.read_bytes()


def read_run_events(group: Group, run_id: str) -> List[StandardEvent]:
    """读取一次 Run 的全部标准事件（按 sequence 有序）。"""
    return store.read_standard_events(group, run_id)
