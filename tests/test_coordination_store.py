"""CCCC-CORE-02 数据模型持久化测试.

验证 coordination_store：coordination 设置、Delivery/Run/RuntimeSession/RunArtifact 的 CRUD、
唯一约束、查询索引、幂等迁移、向后兼容、重启不丢失/不重复。
"""

from __future__ import annotations

import os
import tempfile
import unittest

from cccc.contracts.v1.coordination import (
    Delivery,
    Run,
    RunArtifact,
    RouteValidationError,
    RuntimeSession,
    StandardEvent,
)
from cccc.kernel import coordination_store as cs
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group
from cccc.kernel.registry import load_registry


def _setup_group(home: str, *, title: str = "wg", actor_ids=("a1", "a2")):
    """创建 group 并添加 actors（a1=foreman, 其余=peer）。"""
    reg = load_registry()
    reg.save()
    group = create_group(reg, title=title)
    for aid in actor_ids:
        add_actor(group, actor_id=aid, title=aid, runtime="codex")
    return group


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


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


class TestSettings(_HomeTestCase):
    def test_defaults_are_legacy_and_pty(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            doc = cs.load_settings_doc(group)
            self.assertEqual(doc["coordination_mode"], "legacy")
            self.assertEqual(doc["execution_mode"], "pty")

    def test_backward_compat_missing_file(self) -> None:
        # 旧 group 没有 state/coordination 目录，读取应返回默认且不报错
        with self._with_home():
            group = _setup_group(self._td.name)
            self.assertFalse(cs.coordination_dir(group).exists())
            doc = cs.load_settings_doc(group)
            self.assertEqual(doc["coordination_mode"], "legacy")
            # 读取后不强制落盘
            self.assertFalse(cs._settings_path(group).exists())

    def test_update_settings_persists(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.update_settings(group, coordination_mode="foreman_managed", execution_mode="one_shot")
            doc = cs.load_settings_doc(group)
            self.assertEqual(doc["coordination_mode"], "foreman_managed")
            self.assertEqual(doc["execution_mode"], "one_shot")

    def test_invalid_values_fall_back_to_defaults(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.save_settings_doc(group, {"coordination_mode": "bogus", "execution_mode": "bogus"})
            doc = cs.load_settings_doc(group)
            self.assertEqual(doc["coordination_mode"], "legacy")
            self.assertEqual(doc["execution_mode"], "pty")

    def test_require_foreman_managed_settings_requires_foreman(self) -> None:
        with self._with_home():
            # 无 actor 的 group
            reg = load_registry()
            reg.save()
            group = create_group(reg, title="empty")
            cs.update_settings(group, coordination_mode="foreman_managed")
            with self.assertRaises(RouteValidationError) as exc:
                cs.require_foreman_managed_settings(group)
            self.assertEqual(exc.exception.code, "GROUP_FOREMAN_REQUIRED")

    def test_require_foreman_managed_ok_with_foreman(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.update_settings(group, coordination_mode="foreman_managed")
            doc = cs.require_foreman_managed_settings(group)
            self.assertEqual(doc["coordination_mode"], "foreman_managed")


# ---------------------------------------------------------------------------
# Migration / layout
# ---------------------------------------------------------------------------


class TestLayout(_HomeTestCase):
    def test_ensure_layout_idempotent(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.ensure_coordination_layout(group)
            self.assertTrue(cs._deliveries_dir(group).exists())
            self.assertTrue(cs._runs_dir(group).exists())
            self.assertTrue(cs._sessions_dir(group).exists())
            self.assertTrue(cs._artifacts_dir(group).exists())
            # 再次调用不报错
            cs.ensure_coordination_layout(group)

    def test_safe_id_rejects_traversal(self) -> None:
        with self.assertRaises(ValueError):
            cs._safe_id("../etc")
        with self.assertRaises(ValueError):
            cs._safe_id("")


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


class TestDelivery(_HomeTestCase):
    def test_create_and_get(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            d = cs.create_delivery(
                group,
                Delivery(delivery_id="d1", group_id=group.group_id, message_id="m1", recipient_actor_id="a2"),
            )
            self.assertEqual(d.state, "queued")
            got = cs.get_delivery(group, "d1")
            self.assertIsNotNone(got)
            self.assertEqual(got.recipient_actor_id, "a2")

    def test_unique_constraint_message_recipient(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_delivery(
                group,
                Delivery(delivery_id="d1", group_id=group.group_id, message_id="m1", recipient_actor_id="a2"),
            )
            with self.assertRaises(RouteValidationError) as exc:
                cs.create_delivery(
                    group,
                    Delivery(delivery_id="d2", group_id=group.group_id, message_id="m1", recipient_actor_id="a2"),
                )
            self.assertEqual(exc.exception.code, "DELIVERY_ALREADY_EXISTS")

    def test_actor_must_exist(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            with self.assertRaises(RouteValidationError) as exc:
                cs.create_delivery(
                    group,
                    Delivery(delivery_id="d1", group_id=group.group_id, message_id="m1", recipient_actor_id="ghost"),
                )
            self.assertEqual(exc.exception.code, "ACTOR_NOT_FOUND")

    def test_index_by_recipient_and_message(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_delivery(group, Delivery(delivery_id="d1", group_id=group.group_id, message_id="m1", recipient_actor_id="a2"))
            cs.create_delivery(group, Delivery(delivery_id="d2", group_id=group.group_id, message_id="m2", recipient_actor_id="a2"))
            cs.create_delivery(group, Delivery(delivery_id="d3", group_id=group.group_id, message_id="m1", recipient_actor_id="a1"))
            by_recipient = [d.delivery_id for d in cs.list_deliveries(group, recipient_actor_id="a2")]
            self.assertEqual(sorted(by_recipient), ["d1", "d2"])
            by_message = [d.delivery_id for d in cs.list_deliveries(group, message_id="m1")]
            self.assertEqual(sorted(by_message), ["d1", "d3"])
            both = [d.delivery_id for d in cs.list_deliveries(group, recipient_actor_id="a2", message_id="m1")]
            self.assertEqual(both, ["d1"])

    def test_update_state(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_delivery(group, Delivery(delivery_id="d1", group_id=group.group_id, message_id="m1", recipient_actor_id="a2"))
            d = cs.update_delivery_state(group, "d1", state="reserved", attempts=1, locked_by="worker")
            self.assertEqual(d.state, "reserved")
            self.assertEqual(d.attempts, 1)
            self.assertEqual(d.locked_by, "worker")


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


class TestRun(_HomeTestCase):
    def test_create_and_get(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            r = cs.create_run(group, Run(run_id="r1", group_id=group.group_id, actor_id="a2", message_id="m1"))
            self.assertEqual(r.state, "pending")
            self.assertEqual(r.execution_mode, "one_shot")
            self.assertIsNotNone(cs.get_run(group, "r1"))

    def test_unique_constraint_message_actor(self) -> None:
        # 同一消息不能重复创建 Run
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_run(group, Run(run_id="r1", group_id=group.group_id, actor_id="a2", message_id="m1"))
            with self.assertRaises(RouteValidationError) as exc:
                cs.create_run(group, Run(run_id="r2", group_id=group.group_id, actor_id="a2", message_id="m1"))
            self.assertEqual(exc.exception.code, "RUN_ALREADY_EXISTS")

    def test_same_message_different_actor_ok(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name, actor_ids=("a1", "a2", "a3"))
            cs.create_run(group, Run(run_id="r1", group_id=group.group_id, actor_id="a2", message_id="m1"))
            cs.create_run(group, Run(run_id="r2", group_id=group.group_id, actor_id="a3", message_id="m1"))
            self.assertEqual(len(cs.list_runs(group, message_id="m1")), 2)

    def test_index_by_actor_message_session(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_run(group, Run(run_id="r1", group_id=group.group_id, actor_id="a2", message_id="m1", session_id="s1"))
            cs.create_run(group, Run(run_id="r2", group_id=group.group_id, actor_id="a2", message_id="m2"))
            self.assertEqual([r.run_id for r in cs.list_runs(group, actor_id="a2")], ["r1", "r2"])
            self.assertEqual([r.run_id for r in cs.list_runs(group, session_id="s1")], ["r1"])

    def test_run_with_session_owner_mismatch_blocked(self) -> None:
        # a1 不能用 a2 的 session 创建 run
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_session(group, RuntimeSession(session_id="s1", group_id=group.group_id, owner_actor_id="a2"))
            with self.assertRaises(RouteValidationError) as exc:
                cs.create_run(
                    group,
                    Run(run_id="r1", group_id=group.group_id, actor_id="a1", message_id="m1", session_id="s1"),
                )
            self.assertEqual(exc.exception.code, "SESSION_ACTOR_MISMATCH")

    def test_update_state_terminal(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_run(group, Run(run_id="r1", group_id=group.group_id, actor_id="a2", message_id="m1"))
            r = cs.update_run_state(group, "r1", state="succeeded", exit_code=0)
            self.assertEqual(r.state, "succeeded")
            self.assertEqual(r.exit_code, 0)


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


class TestSession(_HomeTestCase):
    def test_create_and_list_by_owner(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_session(group, RuntimeSession(session_id="s1", group_id=group.group_id, owner_actor_id="a2"))
            cs.create_session(group, RuntimeSession(session_id="s2", group_id=group.group_id, owner_actor_id="a2"))
            cs.create_session(group, RuntimeSession(session_id="s3", group_id=group.group_id, owner_actor_id="a1"))
            self.assertEqual(len(cs.list_sessions(group, owner_actor_id="a2")), 2)

    def test_duplicate_session_id_rejected(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_session(group, RuntimeSession(session_id="s1", group_id=group.group_id, owner_actor_id="a2"))
            with self.assertRaises(ValueError):
                cs.create_session(group, RuntimeSession(session_id="s1", group_id=group.group_id, owner_actor_id="a2"))

    def test_update_state(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_session(group, RuntimeSession(session_id="s1", group_id=group.group_id, owner_actor_id="a2"))
            s = cs.update_session_state(group, "s1", state="busy", current_run_id="r1")
            self.assertEqual(s.state, "busy")
            self.assertEqual(s.current_run_id, "r1")


# ---------------------------------------------------------------------------
# Artifact
# ---------------------------------------------------------------------------


class TestArtifact(_HomeTestCase):
    def test_create_and_list_by_run(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_artifact(group, RunArtifact(artifact_id="ar1", run_id="r1", kind="raw.stdout", path="out.log", sha256="x"))
            cs.create_artifact(group, RunArtifact(artifact_id="ar2", run_id="r1", kind="raw.stderr", path="err.log"))
            cs.create_artifact(group, RunArtifact(artifact_id="ar3", run_id="r2", kind="artifact"))
            self.assertEqual(len(cs.list_artifacts(group, run_id="r1")), 2)


# ---------------------------------------------------------------------------
# StandardEvent 流
# ---------------------------------------------------------------------------


class TestStandardEvents(_HomeTestCase):
    def test_append_and_read_preserves_order_and_content(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            for i in range(3):
                cs.append_standard_event(
                    group,
                    StandardEvent(
                        event_id=f"e{i}",
                        group_id=group.group_id,
                        actor_id="a2",
                        run_id="r1",
                        sequence=i,
                        stream="stdout",
                        event_type="raw.stdout",
                        content=f"line{i}",
                    ),
                )
            events = cs.read_standard_events(group, "r1")
            self.assertEqual(len(events), 3)
            self.assertEqual([e.sequence for e in events], [0, 1, 2])
            self.assertEqual(events[0].content, "line0")  # 不修改内容


# ---------------------------------------------------------------------------
# 重启不丢失、不重复
# ---------------------------------------------------------------------------


class TestRestartDurability(_HomeTestCase):
    def test_reload_preserves_data(self) -> None:
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.update_settings(group, coordination_mode="foreman_managed", execution_mode="one_shot")
            cs.create_delivery(group, Delivery(delivery_id="d1", group_id=group.group_id, message_id="m1", recipient_actor_id="a2"))
            cs.create_run(group, Run(run_id="r1", group_id=group.group_id, actor_id="a2", message_id="m1"))
            cs.create_session(group, RuntimeSession(session_id="s1", group_id=group.group_id, owner_actor_id="a2"))

            # 模拟重启：重新加载同一 group
            from cccc.kernel.group import load_group

            group2 = load_group(group.group_id)
            self.assertIsNotNone(group2)
            self.assertEqual(cs.load_settings_doc(group2)["coordination_mode"], "foreman_managed")
            self.assertIsNotNone(cs.get_delivery(group2, "d1"))
            self.assertIsNotNone(cs.get_run(group2, "r1"))
            self.assertIsNotNone(cs.get_session(group2, "s1"))
            # 索引重建后查询仍可用
            self.assertEqual(len(cs.list_deliveries(group2, recipient_actor_id="a2")), 1)
            self.assertEqual(len(cs.list_runs(group2, actor_id="a2")), 1)

    def test_no_duplicate_run_after_reload(self) -> None:
        # 重启后再次创建相同 (message, actor) Run 仍被唯一约束拒绝
        with self._with_home():
            group = _setup_group(self._td.name)
            cs.create_run(group, Run(run_id="r1", group_id=group.group_id, actor_id="a2", message_id="m1"))
            from cccc.kernel.group import load_group

            group2 = load_group(group.group_id)
            with self.assertRaises(RouteValidationError) as exc:
                cs.create_run(group2, Run(run_id="r2", group_id=group.group_id, actor_id="a2", message_id="m1"))
            self.assertEqual(exc.exception.code, "RUN_ALREADY_EXISTS")


if __name__ == "__main__":
    unittest.main()
