"""CCCC-CORE-04 消息投递系统测试.

验证 Actor 独立 Mailbox、queued/reserved/delivered 状态、消息锁、防止重复消费、
投递日志、重启不丢失不重复。
"""

from __future__ import annotations

import os
import tempfile
import unittest

from cccc.contracts.v1.coordination import RouteValidationError
from cccc.kernel import coordination_store as store
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group, load_group
from cccc.kernel.mailbox import (
    ack_delivery,
    can_consume,
    delivery_log,
    enqueue_delivery,
    mailbox_deliveries,
    nack_delivery,
    queued_deliveries,
    release_expired_locks,
    reserve_delivery,
)
from cccc.kernel.registry import load_registry


def _setup_group(actor_ids=("a1", "a2", "a3")):
    reg = load_registry()
    reg.save()
    group = create_group(reg, title="wg")
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


class TestMailbox(_HomeTestCase):
    def test_actor_independent_mailbox(self) -> None:
        # 发给 a1 的投递只属于 a1；a2 看不到
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a1")
            enqueue_delivery(group, delivery_id="d2", message_id="m2", recipient_actor_id="a2")
            self.assertEqual([d.delivery_id for d in mailbox_deliveries(group, "a1")], ["d1"])
            self.assertEqual([d.delivery_id for d in mailbox_deliveries(group, "a2")], ["d2"])
            self.assertFalse(can_consume(group, "a2", "d1"))
            self.assertTrue(can_consume(group, "a1", "d1"))

    def test_duplicate_enqueue_rejected(self) -> None:
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            with self.assertRaises(RouteValidationError):
                enqueue_delivery(group, delivery_id="d2", message_id="m1", recipient_actor_id="a2")


class TestDeliveryLifecycle(_HomeTestCase):
    def test_reserve_ack_flow(self) -> None:
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            d = reserve_delivery(group, "d1", consumer_id="worker-a2")
            self.assertEqual(d.state, "reserved")
            self.assertEqual(d.locked_by, "worker-a2")
            self.assertEqual(d.attempts, 1)
            d = ack_delivery(group, "d1", consumer_id="worker-a2")
            self.assertEqual(d.state, "delivered")

    def test_reserve_prevents_double_consume(self) -> None:
        # 防止重复消费：a2 锁定后，a3 不能再 reserve
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            reserve_delivery(group, "d1", consumer_id="worker-a2")
            with self.assertRaises(RouteValidationError):
                reserve_delivery(group, "d1", consumer_id="worker-a3")

    def test_ack_idempotent_when_delivered(self) -> None:
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            reserve_delivery(group, "d1", consumer_id="w")
            ack_delivery(group, "d1", consumer_id="w")
            # 再次 ack 不报错（幂等）
            d = ack_delivery(group, "d1", consumer_id="w")
            self.assertEqual(d.state, "delivered")

    def test_ack_wrong_owner_rejected(self) -> None:
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            reserve_delivery(group, "d1", consumer_id="w1")
            with self.assertRaises(RouteValidationError):
                ack_delivery(group, "d1", consumer_id="w2")

    def test_nack_requeues(self) -> None:
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            reserve_delivery(group, "d1", consumer_id="w")
            d = nack_delivery(group, "d1", consumer_id="w", error="boom", requeue=True)
            self.assertEqual(d.state, "queued")
            self.assertEqual(d.last_error, "boom")
            # 重新入队后可再次 reserve
            d2 = reserve_delivery(group, "d1", consumer_id="w")
            self.assertEqual(d2.attempts, 2)

    def test_nack_fail_terminal(self) -> None:
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            reserve_delivery(group, "d1", consumer_id="w")
            d = nack_delivery(group, "d1", consumer_id="w", error="fatal", requeue=False)
            self.assertEqual(d.state, "failed")
            with self.assertRaises(RouteValidationError):
                reserve_delivery(group, "d1", consumer_id="w")


class TestLockExpiry(_HomeTestCase):
    def test_expired_lock_can_be_preempted(self) -> None:
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            reserve_delivery(group, "d1", consumer_id="w1", lock_ttl_seconds=1.0, now_iso="2026-01-01T00:00:00Z")
            # TTL=1s，now 推进 10s → 锁过期，w2 可抢占
            d = reserve_delivery(group, "d1", consumer_id="w2", lock_ttl_seconds=1.0, now_iso="2026-01-01T00:00:10Z")
            self.assertEqual(d.locked_by, "w2")

    def test_release_expired_locks_on_restart(self) -> None:
        # 重启后过期的 reserved 锁回退 queued
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            reserve_delivery(group, "d1", consumer_id="w", lock_ttl_seconds=1.0, now_iso="2026-01-01T00:00:00Z")
            released = release_expired_locks(group, lock_ttl_seconds=1.0, now_iso="2026-01-01T00:00:10Z")
            self.assertEqual(released, ["d1"])
            d = store.get_delivery(group, "d1")
            self.assertEqual(d.state, "queued")
            self.assertIsNone(d.locked_by)


class TestRestartDurability(_HomeTestCase):
    def test_restart_preserves_and_no_duplicate(self) -> None:
        # 重启不丢失、不重复
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            reserve_delivery(group, "d1", consumer_id="w", lock_ttl_seconds=1.0, now_iso="2026-01-01T00:00:00Z")
            # 模拟重启
            group2 = load_group(group.group_id)
            self.assertEqual(len(mailbox_deliveries(group2, "a2")), 1)
            d = store.get_delivery(group2, "d1")
            self.assertEqual(d.state, "reserved")
            # 重启后过期锁释放，可重新 reserve，但投递记录只有一条
            release_expired_locks(group2, lock_ttl_seconds=1.0, now_iso="2026-01-01T00:00:10Z")
            self.assertEqual(len(queued_deliveries(group2, "a2")), 1)


class TestDeliveryLog(_HomeTestCase):
    def test_delivery_log_snapshot(self) -> None:
        with self._with_home():
            group = _setup_group()
            enqueue_delivery(group, delivery_id="d1", message_id="m1", recipient_actor_id="a2")
            reserve_delivery(group, "d1", consumer_id="w")
            log = delivery_log(group, "d1")
            self.assertEqual(log.state, "reserved")
            self.assertEqual(log.attempts, 1)
            self.assertEqual(log.locked_by, "w")


if __name__ == "__main__":
    unittest.main()
