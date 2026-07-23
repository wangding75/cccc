"""CCCC-CORE-12 测试验收.

验证可靠性，形成完整测试报告：
1. 消息串线测试（a3 发给 a1 时 a2 不可见，独立邮箱无串扰）。
2. 重复投递测试（同 message+recipient 唯一约束；重复消费被锁拒绝）。
3. Session 错误测试（a1 不能用 a2 Session、并发 Run 拒绝、恢复失败不静默创建）。
4. 服务重启测试（进程内重启后 Run 不重复启动、状态可恢复）。
5. 大输出测试（大 stdout/stderr 完整保存、可追溯、顺序保留）。
6. 取消测试（取消后无孤儿进程、状态 cancelled）。
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
import unittest

from cccc.contracts.v1.coordination import RouteValidationError
from cccc.kernel import collaboration_loop as loop
from cccc.kernel import coordination_store as store
from cccc.kernel import mailbox as mb
from cccc.kernel import session_manager as sm
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group, load_group
from cccc.kernel.one_shot_runner import OneShotRunInput, OneShotRunner
from cccc.kernel.registry import load_registry


def _setup_group(actor_ids=("a1", "a2", "a3")):
    reg = load_registry()
    reg.save()
    group = create_group(reg, title="wg")
    for aid in actor_ids:
        add_actor(group, actor_id=aid, title=aid, runtime="codex")
    return group


def _echo_cmd(text: str):
    return [sys.executable, "-c", f"import sys; sys.stdout.write({text!r}); sys.stderr.write('err')"]


class _HomeTestCase(unittest.TestCase):
    def _with_home(self):
        self._old_home = os.environ.get("CCCC_HOME")
        self._td = tempfile.TemporaryDirectory()
        os.environ["CCCC_HOME"] = self._td.name
        self.addCleanup(self._cleanup_home)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def _cleanup_home(self) -> None:
        td = getattr(self, "_td", None)
        if td is not None:
            td.cleanup()
        old = getattr(self, "_old_home", None)
        if old is None:
            os.environ.pop("CCCC_HOME", None)
        else:
            os.environ["CCCC_HOME"] = old


class TestCrossTalk(_HomeTestCase):
    """1. 消息串线测试：a3 发给 a1 时 a2 不可见。"""

    def test_no_cross_talk_between_mailboxes(self) -> None:
        with self._with_home():
            group = _setup_group()
            # a1(foreman) 下发给 a3，a2 不应看到
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a3", message_id="m1")
            # a2 队列空，a3 队列有 1
            self.assertEqual(mb.queued_deliveries(group, "a2"), [])
            self.assertEqual(len(mb.queued_deliveries(group, "a3")), 1)
            # a3 取出指令后 a2 仍无
            tm = loop.consume_instruction(group, peer_actor_id="a3", consumer_id="a3")
            self.assertIsNotNone(tm)
            self.assertIsNone(loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2"))
            # a2 即使尝试 reserve a3 的投递也被拒
            a3_delivery = store.list_deliveries(group, recipient_actor_id="a3")[0]
            from cccc.kernel import mailbox as mbox
            with self.assertRaises(RouteValidationError):
                mbox.reserve_delivery(group, a3_delivery.delivery_id, consumer_id="a2")


class TestDuplicateDelivery(_HomeTestCase):
    """2. 重复投递测试：唯一约束 + 重复消费拒绝。"""

    def test_duplicate_enqueue_rejected(self) -> None:
        with self._with_home():
            group = _setup_group()
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a2", message_id="m1")
            # 同 message+recipient 再次入队被拒
            with self.assertRaises(RouteValidationError) as exc:
                mb.enqueue_delivery(group, delivery_id="dv.dupe", message_id="m1", recipient_actor_id="a2")
            self.assertEqual(exc.exception.code, "DELIVERY_ALREADY_EXISTS")

    def test_double_consume_rejected(self) -> None:
        with self._with_home():
            group = _setup_group()
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a2", message_id="m1")
            d = store.list_deliveries(group, recipient_actor_id="a2")[0]
            mb.reserve_delivery(group, d.delivery_id, consumer_id="a2")
            # 同一投递被另一 consumer 再次预留被拒
            with self.assertRaises(RouteValidationError):
                mb.reserve_delivery(group, d.delivery_id, consumer_id="other")


class TestSessionErrors(_HomeTestCase):
    """3. Session 错误测试。"""

    def test_actor_cannot_use_others_session(self) -> None:
        with self._with_home():
            group = _setup_group()
            sm.create_session(group, session_id="s1", owner_actor_id="a2")
            with self.assertRaises(RouteValidationError) as exc:
                sm.resume_session(group, session_id="s1", actor_id="a1")
            self.assertEqual(exc.exception.code, "SESSION_ACTOR_MISMATCH")

    def test_concurrent_run_rejected(self) -> None:
        with self._with_home():
            group = _setup_group()
            sm.create_session(group, session_id="s1", owner_actor_id="a2")
            sm.acquire_session_for_run(group, session_id="s1", run_id="r1", actor_id="a2")
            with self.assertRaises(RouteValidationError) as exc:
                sm.acquire_session_for_run(group, session_id="s1", run_id="r2", actor_id="a2")
            self.assertEqual(exc.exception.code, "SESSION_BUSY")

    def test_resume_missing_no_silent_create(self) -> None:
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                sm.resume_session(group, session_id="ghost", actor_id="a2")
            self.assertEqual(exc.exception.code, "SESSION_NOT_FOUND")
            self.assertIsNone(store.get_session(group, "ghost"))


class TestRestart(_HomeTestCase):
    """4. 服务重启测试：重启后 Run 不重复启动、状态可恢复。"""

    def test_run_not_restarted_after_reload(self) -> None:
        with self._with_home():
            group = _setup_group()
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a2", message_id="m1")
            tm = loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2")
            loop.execute_instruction(
                group, peer_actor_id="a2", task_message=tm, run_id="r1",
                command=_echo_cmd("done"), runtime="codex", consumer_id="a2",
            )
            gid = group.group_id
            # 模拟重启：重新加载 group，Run 已终态，不可再创建同 Run
            reloaded = load_group(gid)
            self.assertIsNotNone(reloaded)
            run = store.get_run(reloaded, "r1")
            self.assertEqual(run.state, "succeeded")
            # 同 (message, actor) 不可重复创建 Run
            from cccc.contracts.v1.coordination import Run as RunModel
            with self.assertRaises(RouteValidationError) as exc:
                store.create_run(reloaded, RunModel(
                    run_id="r1dup", group_id=gid, actor_id="a2", message_id="m1",
                    runtime="codex", execution_mode="one_shot", state="pending",
                ))
            self.assertEqual(exc.exception.code, "RUN_ALREADY_EXISTS")


class TestLargeOutput(_HomeTestCase):
    """5. 大输出测试：完整保存、可追溯、顺序保留。"""

    def test_large_output_preserved_and_traceable(self) -> None:
        with self._with_home():
            group = _setup_group()
            # 生成 ~1MB 输出：通过脚本循环写，避免 argv 长度限制
            size = 1024 * 1024
            script = (
                "import sys\n"
                f"chunk = 'A' * 65536\n"
                f"for _ in range({size // 65536}):\n"
                "    sys.stdout.write(chunk)\n"
                "sys.stderr.write('E' * 1024)\n"
            )
            cmd = [sys.executable, "-c", script]
            runner = OneShotRunner(group, runtime="codex")
            result = runner.start(OneShotRunInput(run_id="r1", message_id="m1", actor_id="a2", command=cmd))
            self.assertEqual(result.state, "succeeded")
            self.assertEqual(result.stdout_bytes, size)
            # 原始内容完整保留
            stdout_art = store.get_artifact(group, "r1.raw.stdout")
            blob = (group.path / stdout_art.path).read_bytes()
            self.assertEqual(len(blob), size)
            self.assertEqual(blob, b"A" * size)
            # 采集为事件并追溯
            from cccc.kernel.output_capture import capture_run_output, trace_original_output
            run = store.get_run(group, "r1")
            captured = capture_run_output(group, run, stdout=blob, stderr=b"E" * 1024)
            stdout_evt = next(e for e in captured.events if e.stream == "stdout")
            self.assertEqual(trace_original_output(group, stdout_evt), blob)
            # 顺序保留
            seqs = [e.sequence for e in captured.events]
            self.assertEqual(seqs, sorted(seqs))


class TestCancel(_HomeTestCase):
    """6. 取消测试：取消后无孤儿进程、状态 cancelled。"""

    def test_cancel_no_orphan_and_state_cancelled(self) -> None:
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            results = []
            thread = runner.start_async(
                OneShotRunInput(
                    run_id="r1", message_id="m1", actor_id="a2",
                    command=[sys.executable, "-c", "import time; time.sleep(60)"],
                ),
                on_done=results.append,
            )
            # 等待进程启动
            for _ in range(50):
                with runner._lock:
                    running = "r1" in runner._processes and runner._processes["r1"].poll() is None
                if running:
                    break
                time.sleep(0.1)
            triggered = runner.cancel("r1")
            self.assertTrue(triggered)
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(results[0].state, "cancelled")
            self.assertEqual(results[0].error_code, "RUN_CANCELLED")
            run = store.get_run(group, "r1")
            self.assertEqual(run.state, "cancelled")
            # 进程引用已释放，无孤儿
            self.assertEqual(len(runner._processes), 0)

    def test_cancel_unknown_run_no_op(self) -> None:
        with self._with_home():
            group = _setup_group()
            runner = OneShotRunner(group, runtime="codex")
            # 不存在的 run：cancel 返回 False，不抛错
            self.assertFalse(runner.cancel("ghost"))


class TestFullAcceptance(_HomeTestCase):
    """端到端验收：完整闭环 + 可追踪 + API 可得。"""

    def test_end_to_end_loop_trackable(self) -> None:
        with self._with_home():
            group = _setup_group()
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a2", message_id="i1", payload={"task": "do"})
            tm = loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2")
            exe = loop.execute_instruction(group, peer_actor_id="a2", task_message=tm, run_id="r1", command=_echo_cmd("ok"), runtime="codex", consumer_id="a2")
            self.assertEqual(exe.run.state, "succeeded")
            loop.send_result(group, peer_actor_id="a2", foreman_actor_id="a1", instruction_message_id="i1", result_message_id="res1", run_id="r1")
            chain = loop.trace_chain(group, "i1")
            self.assertIsNotNone(chain["instruction"])
            self.assertIsNotNone(chain["result"])
            self.assertEqual(chain["run"].run_id, "r1")
            self.assertIsNotNone(chain["session"])
            self.assertTrue(chain["events"])


if __name__ == "__main__":
    unittest.main()
