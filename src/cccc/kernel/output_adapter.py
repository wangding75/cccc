"""CCCC 输出标准化 (CCCC-CORE-09).

不同智能体输出统一展示：定义 StandardEvent（已在 CCCC-CORE-01 冻结）、建立 Adapter 接口、
第一阶段支持原样透传、后续支持智能体专用解析。

原则：只包装结构，不修改内容。

Adapter 接口：
    OutputAdapter.runtime -> 适配的 runtime 名（如 "codex"/"claude"/"codex_cli"）。
    OutputAdapter.parse(stream, raw_chunk) -> List[StandardEvent]：将一段原始输出解析为
        0..N 条标准事件。原样透传 Adapter 将整段封装为 raw.stdout/raw.stderr，不解析、
        不修改、不摘要。

第一阶段：PassthroughAdapter 作为默认 Adapter，所有 runtime 原样透传。
后续：注册专用 Adapter（如 CodexAdapter 解析 tool_call/tool_result/assistant_message），
      未注册的 runtime 回退到 PassthroughAdapter。
"""

from __future__ import annotations

from typing import Dict, List, Optional, Protocol, runtime_checkable

from ..contracts.v1.coordination import (
    Run,
    StandardEvent,
    StandardEventStream,
    StandardEventType,
)
from ..util.time import utc_now_iso


@runtime_checkable
class OutputAdapter(Protocol):
    """智能体输出适配器接口：把原始输出包装为标准事件，只改结构不改内容。"""

    runtime: str

    def parse(
        self,
        run: Run,
        stream: StandardEventStream,
        raw_chunk: str,
        *,
        start_sequence: int,
    ) -> List[StandardEvent]:
        """解析一段原始输出为 0..N 条标准事件。

        Args:
            run: 所属 Run（提供 group_id/actor_id/run_id/session_id/runtime）。
            stream: 输出流来源（stdout/stderr/event/system）。
            raw_chunk: 原始输出片段（不修改）。
            start_sequence: 起始 sequence（调用方保证全局递增）。
        """
        ...


def _event_type_for_stream(stream: StandardEventStream) -> StandardEventType:
    if stream == "stdout":
        return "raw.stdout"
    if stream == "stderr":
        return "raw.stderr"
    return "unknown"


class PassthroughAdapter:
    """原样透传 Adapter：整段封装为 raw.stdout/raw.stderr，不解析、不修改、不摘要。"""

    runtime = "passthrough"

    def parse(
        self,
        run: Run,
        stream: StandardEventStream,
        raw_chunk: str,
        *,
        start_sequence: int,
    ) -> List[StandardEvent]:
        if raw_chunk == "":
            return []
        event_type = _event_type_for_stream(stream)
        return [
            StandardEvent(
                event_id=f"{run.run_id}.{stream}.{start_sequence}",
                group_id=run.group_id,
                actor_id=run.actor_id,
                run_id=run.run_id,
                session_id=run.session_id,
                runtime=run.runtime,
                sequence=start_sequence,
                stream=stream,
                event_type=event_type,
                content=raw_chunk,
                received_at=utc_now_iso(),
                source_reference="",
            )
        ]


# ---------------------------------------------------------------------------
# Adapter 注册表
# ---------------------------------------------------------------------------

_REGISTRY: Dict[str, OutputAdapter] = {}
_DEFAULT_ADAPTER: OutputAdapter = PassthroughAdapter()


def register_adapter(adapter: OutputAdapter) -> None:
    """注册一个 runtime 专用 Adapter。后续解析按 runtime 查找。"""
    _REGISTRY[adapter.runtime] = adapter


def get_adapter(runtime: str) -> OutputAdapter:
    """按 runtime 查找 Adapter；未注册则回退到 PassthroughAdapter（原样透传）。"""
    return _REGISTRY.get(runtime, _DEFAULT_ADAPTER)


def list_registered_runtimes() -> List[str]:
    return sorted(_REGISTRY.keys())


def normalize_output(
    run: Run,
    stream: StandardEventStream,
    raw_chunk: str,
    *,
    start_sequence: int,
    runtime: Optional[str] = None,
) -> List[StandardEvent]:
    """将一段原始输出标准化为标准事件。

    第一阶段：按 runtime 查找 Adapter，未注册则 PassthroughAdapter 原样透传。
    只包装结构，不修改内容。
    """
    adapter = get_adapter(runtime or run.runtime or "")
    return adapter.parse(run, stream, raw_chunk, start_sequence=start_sequence)


def normalize_run_output(
    run: Run,
    *,
    stdout: str,
    stderr: str,
    start_sequence: int = 0,
    runtime: Optional[str] = None,
) -> List[StandardEvent]:
    """便捷：标准化一次 Run 的完整 stdout/stderr，按接收顺序拼接事件列表。"""
    events: List[StandardEvent] = []
    seq = start_sequence
    for chunk in (stdout, stderr):
        stream: StandardEventStream = "stdout" if chunk is stdout else "stderr"
        produced = normalize_output(run, stream, chunk, start_sequence=seq, runtime=runtime)
        events.extend(produced)
        seq += len(produced)
    return events
