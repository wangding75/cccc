"""CCCC 执行授权校验专项测试.

覆盖 CCCC-FIX-01 场景：
1. a3 的任务不能通过 execute_instruction 让 a2 执行。
2. 跨 Group 的 Delivery 与 TaskMessage 组合必须失败。
3. 错误 message kind 必须失败。
4. consumer_id 与 reservation owner 不一致必须失败。
5. 未 reserve、已 delivered、已 dead-letter 的 Delivery 不得执行。
6. 重复调用同一 Delivery 不得创建第二个 Run。
7. 正常 a3 → a1 执行路径通过。
8. 发送者不是 foreman 被拒绝。
9. 接收者不是 peer 被拒绝。
10. 没有 Delivery 被拒绝。
11. 验证失败时不创建 Session/Run.
"""

from __future__ import annotations

import sys
import unittest
from dataclasses import dataclass
from typing import List, Optional

from cccc.contracts.v1.coordination import (
    ERR_DELIVERY_NOT_EXECUTABLE,
    ERR_DELIVERY_NOT_FOUND,
    ERR_DELIVERY_OWNER_MISMATCH,
    ERR_RUN_ALREADY_EXECUTED,
    ERR_TASK_MESSAGE_KIND_INVALID,
    ERR_TASK_RECIPIENT_MISMATCH,
    ERR_TASK_SENDER_NOT_FOREMAN,
    ERR_TASK_RECIPIENT_NOT_PEER,
    RouteValidationError,
    TaskMessage,
    TaskMessageKind,
)
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group
from cccc.kernel.registry import load_registry
from cccc.kernel.execution_authorizer import authorize_delivery_execution


@dataclass
class _Env:
    group: object
    foreman_id: str = "a1"
    peer_id: str = "a2"
    third_id: str = "a3"


class _HomeTestCase(unittest.TestCase):
    def _with_home(self) -> "TempDirWrapper":
        import os
        import tempfile
        self._old_home = os.environ.get("CCCC_HOME")
        self._td = tempfile.TemporaryDirectory()
        os.environ["CCCC_HOME"] = self._td.name
        return self

    def tearDown(self) -> None:
        import os
        td = getattr(self, "_td", None)
        if td is not None:
            td.cleanup()
        old = getattr(self, "_old_home", None)
        if old is None:
            os.environ.pop("CCCC_HOME", None)
        else:
            os.environ["CCCC_HOME"] = old


def _setup_env() -> _Env:
    reg = load_registry()
    reg.save()
    group = create_group(reg, title="wg")
    # add_actor 以稳定位置决定角色：第一个可见 actor 为 foreman，其余为 peer
    add_actor(group, actor_id="a1", title="a1", runtime="codex", enabled=True)
    add_actor(group, actor_id="a2", title="a2", runtime="codex", enabled=True)
    add_actor(group, actor_id="a3", title="a3", runtime="codex", enabled=True)
    return _Env(group=group)


class _DeliveryBuilder:
    """Helper that fabricates a persisted delivery and task message.

    All writes go to the same coordination directory as the real store.
    """

    def __init__(self, group, tmp_home):
        from pathlib import Path
        self._group = group
        self._base = Path(tmp_home) / "state" / "groups" / group.group_id / "coordination"

    def write(
        self,
        *,
        delivery_id: str,
        message_id: str,
        recipient_actor_id: str,
        sender_actor_id: str = "a1",
        state: str = "queued",
        locked_by: str = "",
    ) -> None:
        from cccc.kernel.coordination_store import _delivery_path
        from cccc.kernel.collaboration_loop import _messages_path
        from cccc.contracts.v1.coordination import Delivery, TaskMessage
        d = Delivery(
            delivery_id=delivery_id,
            message_id=message_id,
            group_id=self._group.group_id,
            recipient_actor_id=recipient_actor_id,
            state=state,
            attempts=0,
            locked_by=locked_by or None,
            locked_at=None,
            last_error=None,
            created_at="2026-07-23T10:00:00Z",
            updated_at="2026-07-23T10:00:00Z",
        )
        dpath = _delivery_path(self._group, delivery_id)
        dpath.parent.mkdir(parents=True, exist_ok=True)
        dpath.write_text(d.model_dump_json())
        tm = TaskMessage(
            group_id=self._group.group_id,
            message_id=message_id,
            sender_actor_id=sender_actor_id,
            recipient_actor_id=recipient_actor_id,
            kind="task_instruction",
            payload={},
            created_at="2026-07-23T10:00:00Z",
        )
        mpath = _messages_path(self._group)
        with mpath.open("a", encoding="utf-8") as f:
            f.write(tm.model_dump_json() + "\n")


def _make_delivery(
    env: _Env,
    *,
    tmp_td: str,
    message_id: str = "m.1",
    delivery_id: str = "dv.m.1",
    recipient_actor_id: str = "a2",
    sender_actor_id: str = "a1",
    state: str = "reserved",
    locked_by: str = "a2",
) -> None:
    b = _DeliveryBuilder(env.group, tmp_td)
    b.write(
        delivery_id=delivery_id,
        message_id=message_id,
        recipient_actor_id=recipient_actor_id,
        sender_actor_id=sender_actor_id,
        state=state,
        locked_by=locked_by,
    )


def _make_delivery_kind(
    env: _Env,
    *,
    tmp_td: str,
    message_id: str,
    delivery_id: str,
    recipient_actor_id: str,
    kind: TaskMessageKind,
    sender_actor_id: str = "a1",
    state: str = "reserved",
    locked_by: str = "a2",
) -> None:
    from cccc.kernel.coordination_store import _delivery_path
    from cccc.kernel.collaboration_loop import _messages_path
    from cccc.contracts.v1.coordination import Delivery, TaskMessage
    from pathlib import Path
    d = Delivery(
        delivery_id=delivery_id,
        message_id=message_id,
        group_id=env.group.group_id,
        recipient_actor_id=recipient_actor_id,
        state=state,
        attempts=0,
        locked_by=locked_by,
        locked_at=None,
        last_error=None,
        created_at="2026-07-23T10:00:00Z",
        updated_at="2026-07-23T10:00:00Z",
    )
    dpath = _delivery_path(env.group, delivery_id)
    dpath.parent.mkdir(parents=True, exist_ok=True)
    dpath.write_text(d.model_dump_json())
    tm = TaskMessage(
        group_id=env.group.group_id,
        message_id=message_id,
        sender_actor_id=sender_actor_id,
        recipient_actor_id=recipient_actor_id,
        kind=kind,
        payload={},
        created_at="2026-07-23T10:00:00Z",
    )
    mpath = _messages_path(env.group)
    with mpath.open("a", encoding="utf-8") as f:
        f.write(tm.model_dump_json() + "\n")


def _delivery_exists(env, tmp_td, message_id: str = "m.1") -> bool:
    from cccc.kernel.coordination_store import list_deliveries
    try:
        items = list_deliveries(env.group)
        return any(d.message_id == message_id for d in items)
    except Exception:
        return False


class TestExecutionAuthorizer(_HomeTestCase):
    """验证 deliver 授权边界：execute_instruction 入口授权校验"""

    def _env_and_td(self):
        import tempfile, os
        td = tempfile.TemporaryDirectory()
        os.environ["CCCC_HOME"] = td.name
        return _setup_env(), td

    def test_a3_task_cannot_be_executed_by_a2(self):
        """a3 的任务不能让 a2 执行：recipient 不匹配应失败."""
        env, td = self._env_and_td()
        try:
            _make_delivery(env, tmp_td=td.name, message_id="m.x", delivery_id="dv.m.x",
                           recipient_actor_id="a3", state="reserved", locked_by="a2")
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.x",
                    actor_id="a2",       # actor 欲执行 a3 的 delivery
                    consumer_id="a2",
                )
            self.assertEqual(ctx.exception.code, ERR_TASK_RECIPIENT_MISMATCH)
        finally:
            td.cleanup()

    def test_cross_group_delivery_task_message_rejected(self):
        """跨 Group 的 Delivery 必须失败：SESSION_GROUP_MISMATCH. """
        env, td = self._env_and_td()
        try:
            # 用另一个 group，保证校验会触发 group mismatch
            reg = load_registry()
            other = create_group(reg, title="wg2")
            add_actor(other, actor_id="a1", title="a1", runtime="codex", enabled=True)
            add_actor(other, actor_id="a2", title="a2", runtime="codex", enabled=True)
            # 在 other 中创建合法 delivery 和 tm
            b = _DeliveryBuilder(other, td.name)
            b.write(
                delivery_id="dv.cross.1",
                message_id="m.cross.1",
                recipient_actor_id="a2",
                sender_actor_id="a1",
                state="reserved",
                locked_by="a2",
            )
            with self.assertRaises(RouteValidationError):
                # 把 other 的 delivery 带到 env.group 里校验
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.cross.1",
                    actor_id="a2",
                    consumer_id="a2",
                )
        finally:
            td.cleanup()

    def test_wrong_message_kind_rejected(self):
        """非 task_instruction 类型的 TaskMessage 必须失败."""
        env, td = self._env_and_td()
        try:
            _make_delivery_kind(env, tmp_td=td.name, message_id="m.bad", delivery_id="dv.m.bad",
                                recipient_actor_id="a2", kind="task_result",
                                sender_actor_id="a2", state="reserved", locked_by="a2")
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.bad",
                    actor_id="a2",
                    consumer_id="a2",
                )
            self.assertEqual(ctx.exception.code, ERR_TASK_MESSAGE_KIND_INVALID)
        finally:
            td.cleanup()

    def test_consumer_id_mismatch_rejected(self):
        """consumer 不是 delivery lock 的 owner 被拒绝."""
        env, td = self._env_and_td()
        try:
            _make_delivery(env, tmp_td=td.name, message_id="m.lock", delivery_id="dv.m.lock",
                           recipient_actor_id="a2", state="reserved", locked_by="other")
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.lock",
                    actor_id="a2",
                    consumer_id="a2",  # 不匹配 locked_by="other"
                )
            self.assertEqual(ctx.exception.code, ERR_DELIVERY_OWNER_MISMATCH)
        finally:
            td.cleanup()

    def test_non_reserved_delivery_not_executable(self):
        """queued 状态（未 reserve）的 Delivery 不得执行."""
        env, td = self._env_and_td()
        try:
            _make_delivery(env, tmp_td=td.name, message_id="m.q", delivery_id="dv.m.q",
                           recipient_actor_id="a2", state="queued")
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.q",
                    actor_id="a2",
                    consumer_id="a2",
                )
            self.assertEqual(ctx.exception.code, ERR_DELIVERY_NOT_EXECUTABLE)
        finally:
            td.cleanup()

    def test_delivered_delivery_not_executable(self):
        """已经 delivered 的 Delivery 不得执行."""
        env, td = self._env_and_td()
        try:
            _make_delivery(env, tmp_td=td.name, message_id="m.d", delivery_id="dv.m.d",
                           recipient_actor_id="a2", state="delivered", locked_by="a2")
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.d",
                    actor_id="a2",
                    consumer_id="a2",
                )
            self.assertEqual(ctx.exception.code, ERR_DELIVERY_NOT_EXECUTABLE)
        finally:
            td.cleanup()

    def test_failed_delivery_not_executable(self):
        """failed/dead-letter 的 Delivery 不得执行."""
        env, td = self._env_and_td()
        try:
            _make_delivery(env, tmp_td=td.name, message_id="m.f", delivery_id="dv.m.f",
                           recipient_actor_id="a2", state="failed", locked_by="a2")
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.f",
                    actor_id="a2",
                    consumer_id="a2",
                )
            self.assertEqual(ctx.exception.code, ERR_DELIVERY_NOT_EXECUTABLE)
        finally:
            td.cleanup()

    def test_duplicate_execution_no_second_run(self):
        """同一 message+actor 已有 run 时，重复执行失败."""
        env, td = self._env_and_td()
        try:
            # 先写入合法的 delivery 和 task message 和 existing run
            _make_delivery(env, tmp_td=td.name, message_id="m.dup", delivery_id="dv.m.dup",
                           recipient_actor_id="a2", state="reserved", locked_by="a2")
            # 写一个已完成/运行中的 run
            from cccc.kernel.coordination_store import create_run
            from cccc.contracts.v1.coordination import Run
            run = Run(
                run_id="r.dup",
                group_id=env.group.group_id,
                message_id="m.dup",
                actor_id="a2",
                state="succeeded",
            )
            create_run(env.group, run)
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.dup",
                    actor_id="a2",
                    consumer_id="a2",
                )
            self.assertEqual(ctx.exception.code, ERR_RUN_ALREADY_EXECUTED)
        finally:
            td.cleanup()

    def test_normal_path_passes(self):
        """正常路径：合法的 foreman→peer、reserved、recipient 匹配通过."""
        env, td = self._env_and_td()
        try:
            _make_delivery(env, tmp_td=td.name, message_id="m.ok", delivery_id="dv.m.ok",
                           recipient_actor_id="a2", state="reserved", locked_by="a2")
            ctx = authorize_delivery_execution(
                group=env.group,
                delivery_id="dv.m.ok",
                actor_id="a2",
                consumer_id="a2",
            )
            self.assertEqual(ctx.delivery_id, "dv.m.ok")
        finally:
            td.cleanup()

    def test_sender_not_foreman_rejected(self):
        """sender 不是 foreman 被拒绝."""
        env, td = self._env_and_td()
        try:
            # peer a2 发送 task_instruction（违反 sender role）
            _make_delivery_kind(env, tmp_td=td.name, message_id="m.s", delivery_id="dv.m.s",
                                recipient_actor_id="a2", kind="task_instruction",
                                sender_actor_id="a2", state="reserved", locked_by="a2")
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.s",
                    actor_id="a2",
                    consumer_id="a2",
                )
            self.assertEqual(ctx.exception.code, ERR_TASK_SENDER_NOT_FOREMAN)
        finally:
            td.cleanup()

    def test_recipient_not_peer_rejected(self):
        """recipient 是 foreman（a1）而不是 peer 被拒绝."""
        env, td = self._env_and_td()
        try:
            # foreman a1 自己作为 recipient，但 foreman 不是 peer
            _make_delivery(env, tmp_td=td.name, message_id="m.ip",
                           delivery_id="dv.m.ip",
                           recipient_actor_id="a1", state="reserved", locked_by="a1")
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.ip",
                    actor_id="a1",
                    consumer_id="a1",
                )
            self.assertEqual(ctx.exception.code, ERR_TASK_RECIPIENT_NOT_PEER)
        finally:
            td.cleanup()

    def test_delivery_not_found_rejected(self):
        """不存在对应投递的 message_id 失败: DELIVERY_NOT_FOUND."""
        env, td = self._env_and_td()
        try:
            with self.assertRaises(RouteValidationError) as ctx:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.missing",
                    actor_id="a2",
                    consumer_id="a2",
                )
            self.assertEqual(ctx.exception.code, ERR_DELIVERY_NOT_FOUND)
        finally:
            td.cleanup()

    def test_no_state_created_on_failure(self):
        """校验失败时不创建 Session/Run。"""
        env, td = self._env_and_td()
        try:
            _make_delivery(env, tmp_td=td.name, message_id="m.bad", delivery_id="dv.m.bad",
                           recipient_actor_id="a2", state="queued")
            try:
                authorize_delivery_execution(
                    group=env.group,
                    delivery_id="dv.m.bad",
                    actor_id="a2",
                    consumer_id="a2",
                )
            except RouteValidationError:
                pass
            # 验证没有 run 被写入
            from cccc.kernel.coordination_store import list_runs
            self.assertEqual(len(list_runs(env.group)), 0)
        finally:
            td.cleanup()


if __name__ == "__main__":
    unittest.main()
