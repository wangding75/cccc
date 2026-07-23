"""CCCC-CORE-08 输出采集测试.

验证完整保存智能体输出：保存 stdout/stderr、记录接收顺序、生成 stream 记录、
保留原始内容；任何标准事件可以追溯原始输出。
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

from cccc.contracts.v1.coordination import Run, RunArtifact, StandardEvent
from cccc.kernel import coordination_store as store
from cccc.kernel import output_capture as cap
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group
from cccc.kernel.one_shot_runner import OneShotRunInput, OneShotRunner
from cccc.kernel.registry import load_registry


def _setup_group(actor_ids=("a1", "a2")):
    reg = load_registry()
    reg.save()
    group = create_group(reg, title="wg")
    for aid in actor_ids:
        add_actor(group, actor_id=aid, title=aid, runtime="codex")
    return group


def _run(group, run_id="r1", actor_id="a2", message_id="m1"):
    return Run(
        run_id=run_id,
        group_id=group.group_id,
        actor_id=actor_id,
        message_id=message_id,
        runtime="codex",
        execution_mode="one_shot",
        state="succeeded",
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


class TestOutputCapture(_HomeTestCase):
    def test_save_stdout_and_stderr_separately(self) -> None:
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            out = cap.capture_run_output(group, run, stdout=b"hello-out", stderr=b"hello-err")
            kinds = sorted(a.kind for a in out.artifacts)
            self.assertEqual(kinds, ["raw.stderr", "raw.stdout"])
            # 原始内容分开保存且未修改
            stdout_art = next(a for a in out.artifacts if a.kind == "raw.stdout")
            stderr_art = next(a for a in out.artifacts if a.kind == "raw.stderr")
            self.assertEqual(stdout_art.bytes, len("hello-out"))
            self.assertEqual(stderr_art.bytes, len("hello-err"))
            blob_out = (group.path / stdout_art.path).read_bytes()
            blob_err = (group.path / stderr_art.path).read_bytes()
            self.assertEqual(blob_out, b"hello-out")
            self.assertEqual(blob_err, b"hello-err")

    def test_records_receiving_order_with_sequence(self) -> None:
        # 记录接收顺序：sequence 递增
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            out = cap.capture_run_output(group, run, stdout=b"a", stderr=b"b")
            seqs = [e.sequence for e in out.events]
            self.assertEqual(seqs, sorted(seqs))
            self.assertEqual(len(set(seqs)), len(seqs))
            # stdout 先于 stderr（采集顺序）
            streams = [e.stream for e in out.events]
            self.assertEqual(streams, ["stdout", "stderr"])

    def test_generates_stream_records(self) -> None:
        # 生成 stream 记录：stream 与 event_type 正确
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            out = cap.capture_run_output(group, run, stdout=b"x", stderr=b"y")
            stdout_evt = next(e for e in out.events if e.stream == "stdout")
            stderr_evt = next(e for e in out.events if e.stream == "stderr")
            self.assertEqual(stdout_evt.event_type, "raw.stdout")
            self.assertEqual(stderr_evt.event_type, "raw.stderr")
            self.assertEqual(stdout_evt.content, "x")
            self.assertEqual(stderr_evt.content, "y")

    def test_preserves_original_content_binary(self) -> None:
        # 保留原始内容：二进制/非 UTF-8 不被破坏
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            payload = bytes(range(256))
            out = cap.capture_run_output(group, run, stdout=payload, stderr=b"")
            stdout_art = next(a for a in out.artifacts if a.kind == "raw.stdout")
            blob = (group.path / stdout_art.path).read_bytes()
            self.assertEqual(blob, payload)

    def test_any_event_traces_back_to_original(self) -> None:
        # 验收：任何标准事件可以追溯原始输出
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            out = cap.capture_run_output(group, run, stdout=b"trace-me", stderr=b"err-trace")
            for event in out.events:
                original = cap.trace_original_output(group, event)
                self.assertNotEqual(original, b"")
                self.assertIn(event.source_reference, [a.artifact_id for a in out.artifacts])
            # stdout 事件追溯到 stdout 原始内容
            stdout_evt = next(e for e in out.events if e.stream == "stdout")
            self.assertEqual(cap.trace_original_output(group, stdout_evt), b"trace-me")
            stderr_evt = next(e for e in out.events if e.stream == "stderr")
            self.assertEqual(cap.trace_original_output(group, stderr_evt), b"err-trace")

    def test_read_events_ordered(self) -> None:
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            cap.capture_run_output(group, run, stdout=b"1", stderr=b"2")
            events = cap.read_run_events(group, run.run_id)
            self.assertEqual(len(events), 2)
            self.assertEqual([e.sequence for e in events], [0, 1])

    def test_append_stream_event_increments_sequence(self) -> None:
        with self._with_home():
            group = _setup_group()
            run = _run(group)
            cap.capture_run_output(group, run, stdout=b"a", stderr=b"b")
            e = cap.append_stream_event(
                group, run, stream="event", event_type="tool_call", content="call",
            )
            self.assertEqual(e.sequence, 2)
            events = cap.read_run_events(group, run.run_id)
            self.assertEqual(len(events), 3)
            self.assertEqual(events[-1].sequence, 2)

    def test_integration_with_one_shot_runner(self) -> None:
        # 端到端：Runner 已保存原始产物，输出采集直接复用既有 artifact 生成事件并追溯
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            runner.start(OneShotRunInput(
                run_id="r2", message_id="m2", actor_id="a2",
                command=[sys.executable, "-c", "import sys; sys.stdout.write('out'); sys.stderr.write('er')"],
            ))
            run = store.get_run(group, "r2")
            # Runner 已保存 raw stdout/stderr 产物；直接基于既有 artifact 生成 stream 事件
            stdout_art = store.get_artifact(group, "r2.raw.stdout")
            stderr_art = store.get_artifact(group, "r2.raw.stderr")
            self.assertIsNotNone(stdout_art)
            self.assertTrue(stdout_art.path)
            stdout_blob = (group.path / stdout_art.path).read_bytes()
            stderr_blob = (group.path / stderr_art.path).read_bytes()
            # 采集器对未采集过的 run 生成事件（artifact 已存在则复用，不重复创建）
            out = cap.capture_run_output(group, run, stdout=stdout_blob, stderr=stderr_blob)
            stdout_evt = next(e for e in out.events if e.stream == "stdout")
            self.assertEqual(cap.trace_original_output(group, stdout_evt), b"out")


if __name__ == "__main__":
    unittest.main()
