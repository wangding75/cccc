"""CCCC-CORE-03 路由控制测试.

验证 RouteValidator：foreman_managed 模式下任务消息的发送者/接收者身份校验、
角色通信规则、禁止广播/多接收者/peer→peer；普通 chat 消息保持兼容语义。

验收语义：a3 发给 a1 时，a2 不可见（任务消息解析结果只含唯一接收者）。
"""

from __future__ import annotations

import os
import tempfile
import unittest

from cccc.contracts.v1.coordination import RouteValidationError
from cccc.kernel.actors import add_actor
from cccc.kernel.group import create_group
from cccc.kernel.registry import load_registry
from cccc.kernel.route_validator import (
    RouteValidator,
    is_recipient_visible,
    validate_message_route,
)


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


class TestTaskRouteForemanManaged(_HomeTestCase):
    def test_task_instruction_foreman_to_peer_ok(self) -> None:
        with self._with_home():
            group = _setup_group()
            decision = validate_message_route(
                group,
                coordination_mode="foreman_managed",
                sender_actor_id="a1",
                kind="task_instruction",
                recipient_tokens=["a3"],
            )
            self.assertTrue(decision.is_task_message)
            self.assertEqual(decision.recipient_actor_ids, ["a3"])
            self.assertEqual(decision.sender_role, "foreman")
            self.assertEqual(decision.recipient_roles, ["peer"])

    def test_task_result_peer_to_foreman_ok(self) -> None:
        with self._with_home():
            group = _setup_group()
            decision = validate_message_route(
                group,
                coordination_mode="foreman_managed",
                sender_actor_id="a3",
                kind="task_result",
                recipient_tokens=["a1"],
            )
            self.assertEqual(decision.recipient_actor_ids, ["a1"])
            self.assertEqual(decision.sender_role, "peer")
            self.assertEqual(decision.recipient_roles, ["foreman"])

    def test_a3_to_a1_a2_not_visible(self) -> None:
        # 验收语义：a3 发给 a1 时，a2 不可见
        with self._with_home():
            group = _setup_group()
            decision = validate_message_route(
                group,
                coordination_mode="foreman_managed",
                sender_actor_id="a3",
                kind="task_result",
                recipient_tokens=["a1"],
            )
            self.assertTrue(is_recipient_visible("a1", decision))
            self.assertFalse(is_recipient_visible("a2", decision))
            self.assertFalse(is_recipient_visible("a3", decision))

    def test_task_instruction_rejects_peer_sender(self) -> None:
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                validate_message_route(
                    group,
                    coordination_mode="foreman_managed",
                    sender_actor_id="a2",
                    kind="task_instruction",
                    recipient_tokens=["a3"],
                )
            self.assertEqual(exc.exception.code, "TASK_ROUTE_NOT_ALLOWED")

    def test_task_result_rejects_foreman_sender(self) -> None:
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                validate_message_route(
                    group,
                    coordination_mode="foreman_managed",
                    sender_actor_id="a1",
                    kind="task_result",
                    recipient_tokens=["a1"],
                )
            self.assertEqual(exc.exception.code, "TASK_ROUTE_NOT_ALLOWED")

    def test_peer_to_peer_blocked(self) -> None:
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                validate_message_route(
                    group,
                    coordination_mode="foreman_managed",
                    sender_actor_id="a2",
                    kind="task_result",
                    recipient_tokens=["a3"],
                )
            self.assertEqual(exc.exception.code, "TASK_ROUTE_NOT_ALLOWED")

    def test_task_message_rejects_broadcast(self) -> None:
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                validate_message_route(
                    group,
                    coordination_mode="foreman_managed",
                    sender_actor_id="a1",
                    kind="task_instruction",
                    recipient_tokens=[],
                )
            self.assertEqual(exc.exception.code, "TASK_RECIPIENT_REQUIRED")

    def test_task_message_rejects_all_mention(self) -> None:
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                validate_message_route(
                    group,
                    coordination_mode="foreman_managed",
                    sender_actor_id="a1",
                    kind="task_instruction",
                    recipient_tokens=["@all"],
                )
            self.assertEqual(exc.exception.code, "TASK_RECIPIENT_REQUIRED")

    def test_task_message_rejects_multiple_recipients(self) -> None:
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                validate_message_route(
                    group,
                    coordination_mode="foreman_managed",
                    sender_actor_id="a1",
                    kind="task_instruction",
                    recipient_tokens=["a2", "a3"],
                )
            self.assertEqual(exc.exception.code, "TASK_MULTIPLE_RECIPIENTS_NOT_ALLOWED")

    def test_task_message_rejects_hash_token(self) -> None:
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                validate_message_route(
                    group,
                    coordination_mode="foreman_managed",
                    sender_actor_id="a1",
                    kind="task_instruction",
                    recipient_tokens=["#other-group"],
                )
            self.assertEqual(exc.exception.code, "TASK_RECIPIENT_REQUIRED")

    def test_task_message_sender_must_exist(self) -> None:
        with self._with_home():
            group = _setup_group()
            with self.assertRaises(RouteValidationError) as exc:
                validate_message_route(
                    group,
                    coordination_mode="foreman_managed",
                    sender_actor_id="ghost",
                    kind="task_instruction",
                    recipient_tokens=["a2"],
                )
            self.assertEqual(exc.exception.code, "ACTOR_NOT_FOUND")

    def test_task_message_rejects_disabled_sender(self) -> None:
        with self._with_home():
            group = _setup_group()
            from cccc.kernel.actors import update_actor

            update_actor(group, "a1", {"enabled": False})
            with self.assertRaises(RouteValidationError) as exc:
                validate_message_route(
                    group,
                    coordination_mode="foreman_managed",
                    sender_actor_id="a1",
                    kind="task_instruction",
                    recipient_tokens=["a2"],
                )
            self.assertEqual(exc.exception.code, "ACTOR_DISABLED")


class TestLegacyAndChatCompatibility(_HomeTestCase):
    def test_legacy_mode_task_instruction_not_enforced(self) -> None:
        # legacy 模式不强制任务消息规则
        with self._with_home():
            group = _setup_group()
            decision = validate_message_route(
                group,
                coordination_mode="legacy",
                sender_actor_id="a2",
                kind="task_instruction",
                recipient_tokens=["a3"],
            )
            # legacy 下 peer→peer 不被拒绝
            self.assertEqual(decision.recipient_actor_ids, ["a3"])

    def test_foreman_managed_chat_allows_broadcast(self) -> None:
        # foreman_managed 模式下，普通 chat 仍保持广播兼容
        with self._with_home():
            group = _setup_group()
            decision = validate_message_route(
                group,
                coordination_mode="foreman_managed",
                sender_actor_id="a2",
                kind="chat",
                recipient_tokens=[],
            )
            self.assertFalse(decision.is_task_message)
            self.assertEqual(decision.recipient_actor_ids, [])

    def test_foreman_managed_chat_allows_multi_recipient(self) -> None:
        with self._with_home():
            group = _setup_group()
            decision = validate_message_route(
                group,
                coordination_mode="foreman_managed",
                sender_actor_id="a1",
                kind="chat",
                recipient_tokens=["a2", "a3"],
            )
            self.assertFalse(decision.is_task_message)
            self.assertEqual(decision.recipient_actor_ids, ["a2", "a3"])

    def test_chat_all_mention_resolves(self) -> None:
        with self._with_home():
            group = _setup_group()
            decision = validate_message_route(
                group,
                coordination_mode="foreman_managed",
                sender_actor_id="a1",
                kind="chat",
                recipient_tokens=["@all"],
            )
            self.assertEqual(decision.recipient_actor_ids, ["@all"])


class TestValidatorDirect(_HomeTestCase):
    def test_route_validator_class(self) -> None:
        with self._with_home():
            group = _setup_group()
            v = RouteValidator(group, coordination_mode="foreman_managed")
            decision = v.validate(sender_actor_id="a1", kind="task_instruction", recipient_tokens=["a2"])
            self.assertEqual(decision.recipient_actor_ids, ["a2"])
            self.assertTrue(decision.is_task_message)


if __name__ == "__main__":
    unittest.main()
