"""CCCC-CORE-10 协作闭环测试.

流程：a3 发送任务 → a1 执行 → a1 返回 → a3 决定下一步。
验收：完整链路可追踪；区分用户回复和 Actor 指令；支持继续 Session。
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

from cccc.kernel import collaboration_loop as loop
from cccc.kernel import coordination_store as store
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group
from cccc.kernel.registry import load_registry


def _setup_group():
    # a1=foreman, a2/a3=peer；任务书流程 a3 发送 → a1 执行 → a1 返回 → a3 决定
    # 这里用 foreman=a1 下发给 peer=a2，再 a2 返回 a1，验证闭环通用性。
    reg = load_registry()
    reg.save()
    group = create_group(reg, title="wg")
    add_actor(group, actor_id="a1", title="a1", runtime="codex")
    add_actor(group, actor_id="a2", title="a2", runtime="codex")
    add_actor(group, actor_id="a3", title="a3", runtime="codex")
    return group


def _echo_cmd(text: str):
    return [sys.executable, "-c", f"import sys; sys.stdout.write({text!r}); sys.stderr.write('e')"]


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


class TestCollaborationLoop(_HomeTestCase):
    def test_full_loop_trackable(self) -> None:
        # 验收：完整链路可追踪 a1 下发 → a2 执行 → a2 返回 → a1 可见
        with self._with_home():
            group = _setup_group()
            # 1. foreman a1 下发指令给 peer a2
            instr = loop.send_instruction(
                group, foreman_actor_id="a1", peer_actor_id="a2",
                message_id="m.instr.1", payload={"task": "do X"},
            )
            self.assertEqual(instr.task_message.kind, "task_instruction")
            # 投递进入 a2 队列，a1/a3 不可见
            self.assertEqual(len(store.list_deliveries(group, recipient_actor_id="a2")), 1)
            self.assertEqual(len(store.list_deliveries(group, recipient_actor_id="a1")), 0)

            # 2. a2 取出并执行
            tm = loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2")
            self.assertIsNotNone(tm)
            self.assertEqual(tm.message_id, "m.instr.1")
            exe = loop.execute_instruction(
                group, peer_actor_id="a2", task_message=tm,
                run_id="r1", command=_echo_cmd("done"), runtime="codex", consumer_id="a2",
            )
            self.assertEqual(exe.run.state, "succeeded")
            self.assertEqual(exe.run.message_id, "m.instr.1")
            self.assertTrue(exe.events)

            # 3. a2 返回结果给 a1
            res = loop.send_result(
                group, peer_actor_id="a2", foreman_actor_id="a1",
                instruction_message_id="m.instr.1", result_message_id="m.result.1",
                run_id="r1", payload={"summary": "X done"},
            )
            self.assertEqual(res.task_message.kind, "task_result")
            # 结果投递进入 a1 队列
            self.assertEqual(len(store.list_deliveries(group, recipient_actor_id="a1")), 1)

            # 4. 完整链路可追踪
            chain = loop.trace_chain(group, "m.instr.1")
            self.assertIsNotNone(chain["instruction"])
            self.assertIsNotNone(chain["result"])
            self.assertEqual(chain["result"].payload.get("parent_message_id"), "m.instr.1")
            self.assertEqual(chain["run"].run_id, "r1")
            self.assertIsNotNone(chain["session"])
            self.assertTrue(chain["events"])

    def test_message_association_chain(self) -> None:
        # 保存消息关联：instruction ↔ result 通过 parent_message_id 链接
        with self._with_home():
            group = _setup_group()
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a2", message_id="i1")
            tm = loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2")
            loop.execute_instruction(group, peer_actor_id="a2", task_message=tm, run_id="r1", command=_echo_cmd("x"), runtime="codex", consumer_id="a2")
            loop.send_result(group, peer_actor_id="a2", foreman_actor_id="a1", instruction_message_id="i1", result_message_id="res1", run_id="r1")
            result = loop.get_task_message(group, "res1")
            self.assertEqual(result.payload["parent_message_id"], "i1")
            self.assertEqual(result.payload["run_id"], "r1")

    def test_run_relations_saved(self) -> None:
        # 保存 Run 关系：Run ↔ message_id ↔ session_id
        with self._with_home():
            group = _setup_group()
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a2", message_id="i1")
            tm = loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2")
            exe = loop.execute_instruction(group, peer_actor_id="a2", task_message=tm, run_id="r1", command=_echo_cmd("x"), runtime="codex", consumer_id="a2")
            run = store.get_run(group, "r1")
            self.assertEqual(run.message_id, "i1")
            self.assertTrue(run.session_id)
            self.assertEqual(run.session_id, exe.session.session_id)

    def test_continue_session(self) -> None:
        # 支持继续 Session：第二次执行复用同一 session_id
        with self._with_home():
            group = _setup_group()
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a2", message_id="i1")
            tm1 = loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2")
            exe1 = loop.execute_instruction(group, peer_actor_id="a2", task_message=tm1, run_id="r1", command=_echo_cmd("a"), runtime="codex", consumer_id="a2")
            sid = exe1.session.session_id
            # 第二条指令复用同一 session
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a2", message_id="i2")
            tm2 = loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2")
            exe2 = loop.execute_instruction(group, peer_actor_id="a2", task_message=tm2, run_id="r2", command=_echo_cmd("b"), runtime="codex", session_id=sid, consumer_id="a2")
            self.assertEqual(exe2.session.session_id, sid)
            # 同一 owner，a2 的 session 列表含该 session
            sessions = store.list_sessions(group, owner_actor_id="a2")
            self.assertEqual(len(sessions), 1)

    def test_classify_user_vs_actor(self) -> None:
        # 区分用户回复和 Actor 指令
        with self._with_home():
            group = _setup_group()
            self.assertEqual(loop.classify_sender(group, "a1"), "actor_instruction")
            self.assertEqual(loop.classify_sender(group, "user"), "user_reply")
            self.assertEqual(loop.classify_sender(group, ""), "user_reply")
            self.assertEqual(loop.classify_sender(group, "ghost"), "user_reply")

    def test_cross_actor_invisible(self) -> None:
        # foreman 发给 a3 时 a2 不可见：任务投递只进入明确接收者队列
        with self._with_home():
            group = _setup_group()
            # a1(foreman) 下发给 a3
            loop.send_instruction(group, foreman_actor_id="a1", peer_actor_id="a3", message_id="i1")
            self.assertEqual(len(store.list_deliveries(group, recipient_actor_id="a3")), 1)
            self.assertEqual(len(store.list_deliveries(group, recipient_actor_id="a2")), 0)
            # a2 队列为空，consume 返回 None
            self.assertIsNone(loop.consume_instruction(group, peer_actor_id="a2", consumer_id="a2"))

    def test_task_route_enforced_foreman_managed(self) -> None:
        # foreman_managed 模式：peer→peer 任务消息被拒绝（TASK_ROUTE_NOT_ALLOWED）
        with self._with_home():
            group = _setup_group()
            from cccc.contracts.v1.coordination import RouteValidationError
            with self.assertRaises(RouteValidationError):
                loop.send_instruction(group, foreman_actor_id="a2", peer_actor_id="a3", message_id="i1")


if __name__ == "__main__":
    unittest.main()
