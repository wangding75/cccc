"""CCCC-CORE-09 输出标准化测试.

验证：StandardEvent 已定义、Adapter 接口、第一阶段原样透传、后续专用解析回退。
原则：只包装结构，不修改内容。
"""

from __future__ import annotations

import os
import tempfile
import unittest

from cccc.contracts.v1.coordination import Run, StandardEvent
from cccc.kernel import output_adapter as oa
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group
from cccc.kernel.registry import load_registry


def _setup_group():
    reg = load_registry()
    reg.save()
    group = create_group(reg, title="wg")
    add_actor(group, actor_id="a2", title="a2", runtime="codex")
    return group


def _run(group, run_id="r1", runtime="codex"):
    return Run(
        run_id=run_id, group_id=group.group_id, actor_id="a2", message_id="m1",
        runtime=runtime, execution_mode="one_shot", state="succeeded",
    )


class _HomeTestCase(unittest.TestCase):
    def _with_home(self) -> tempfile.TemporaryDirectory:
        self._old_home = os.environ.get("CCCC_HOME")
        self._td = tempfile.TemporaryDirectory()
        os.environ["CCCC_HOME"] = self._td.name
        return self._td

    def tearDown(self) -> None:
        td = getattr(self, "_td", None)
        if td is not None:
            td.cleanup()
        old = getattr(self, "_old_home", None)
        if old is None:
            os.environ.pop("CCCC_HOME", None)
        else:
            os.environ["CCCC_HOME"] = old


class TestPassthroughAdapter(_HomeTestCase):
    def test_standard_event_defined(self) -> None:
        # StandardEvent 已在契约冻结，可直接构造
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            ev = oa.normalize_output(run, "stdout", "hello", start_sequence=0)
            self.assertEqual(len(ev), 1)
            self.assertIsInstance(ev[0], StandardEvent)
            self.assertEqual(ev[0].stream, "stdout")
            self.assertEqual(ev[0].event_type, "raw.stdout")

    def test_passthrough_preserves_content(self) -> None:
        # 原样透传：内容不修改、不摘要
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            raw = "line1\nline2\n  indented\n"
            ev = oa.normalize_output(run, "stdout", raw, start_sequence=3)
            self.assertEqual(ev[0].content, raw)
            self.assertEqual(ev[0].sequence, 3)
            self.assertEqual(ev[0].event_type, "raw.stdout")

    def test_passthrough_stderr(self) -> None:
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            ev = oa.normalize_output(run, "stderr", "boom", start_sequence=0)
            self.assertEqual(ev[0].stream, "stderr")
            self.assertEqual(ev[0].event_type, "raw.stderr")
            self.assertEqual(ev[0].content, "boom")

    def test_empty_chunk_produces_no_event(self) -> None:
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            self.assertEqual(oa.normalize_output(run, "stdout", "", start_sequence=0), [])

    def test_event_metadata_from_run(self) -> None:
        with self._with_home():
            group = _setup_group()
            run = _run(group, run_id="r9", runtime="codex")
            ev = oa.normalize_output(run, "stdout", "x", start_sequence=0)
            self.assertEqual(ev[0].run_id, "r9")
            self.assertEqual(ev[0].group_id, group.group_id)
            self.assertEqual(ev[0].actor_id, "a2")
            self.assertEqual(ev[0].runtime, "codex")


class TestAdapterRegistry(_HomeTestCase):
    def test_unknown_runtime_falls_back_to_passthrough(self) -> None:
        # 未注册的 runtime 回退到原样透传
        with self._with_home():
            group = _setup_group()
            run = _run(group, runtime="some-future-agent")
            ev = oa.normalize_output(run, "stdout", "raw-out", start_sequence=0)
            self.assertEqual(ev[0].event_type, "raw.stdout")
            self.assertEqual(ev[0].content, "raw-out")

    def test_registered_adapter_used(self) -> None:
        # 后续专用解析：注册一个 codex Adapter，解析 tool_call
        class CodexAdapter:
            runtime = "codex"

            def parse(self, run, stream, raw_chunk, *, start_sequence):
                # 模拟解析：以 >>> tool_call: 开头识别为 tool_call 事件，其余透传
                if raw_chunk.startswith(">>> tool_call:"):
                    return [StandardEvent(
                        event_id=f"{run.run_id}.{stream}.{start_sequence}",
                        group_id=run.group_id, actor_id=run.actor_id, run_id=run.run_id,
                        session_id=run.session_id, runtime=run.runtime,
                        sequence=start_sequence, stream=stream, event_type="tool_call",
                        content=raw_chunk[len(">>> tool_call:"):],
                        received_at=oa.utc_now_iso(), source_reference="",
                    )]
                return oa.PassthroughAdapter().parse(run, stream, raw_chunk, start_sequence=start_sequence)

        oa.register_adapter(CodexAdapter())
        try:
            with self._with_home():
                group = _setup_group()
                run = _run(group, runtime="codex")
                ev = oa.normalize_output(run, "stdout", ">>> tool_call:ls -la", start_sequence=0)
                self.assertEqual(ev[0].event_type, "tool_call")
                self.assertEqual(ev[0].content, "ls -la")
                # 非工具调用仍透传
                ev2 = oa.normalize_output(run, "stdout", "plain text", start_sequence=1)
                self.assertEqual(ev2[0].event_type, "raw.stdout")
        finally:
            # 清理注册表，避免污染后续测试
            oa._REGISTRY.pop("codex", None)

    def test_list_registered_runtimes(self) -> None:
        class A1:
            runtime = "agent-one"

            def parse(self, run, stream, raw_chunk, *, start_sequence):
                return []

        class A2:
            runtime = "agent-two"

            def parse(self, run, stream, raw_chunk, *, start_sequence):
                return []

        oa.register_adapter(A1())
        oa.register_adapter(A2())
        try:
            self.assertIn("agent-one", oa.list_registered_runtimes())
            self.assertIn("agent-two", oa.list_registered_runtimes())
        finally:
            oa._REGISTRY.pop("agent-one", None)
            oa._REGISTRY.pop("agent-two", None)


class TestNormalizeRunOutput(_HomeTestCase):
    def test_normalize_full_output_ordered(self) -> None:
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            events = oa.normalize_run_output(run, stdout="out", stderr="err")
            self.assertEqual(len(events), 2)
            self.assertEqual([e.sequence for e in events], [0, 1])
            self.assertEqual([e.stream for e in events], ["stdout", "stderr"])
            self.assertEqual(events[0].content, "out")
            self.assertEqual(events[1].content, "err")


if __name__ == "__main__":
    unittest.main()
