"""CCCC 路由控制 (CCCC-CORE-03).

实现 RouteValidator：在 foreman_managed 模式下校验任务消息（task_instruction /
task_result）的路由规则，保证消息不会投递错误。

校验内容：
1. 发送者身份（存在、enabled、role）。
2. 接收者身份（存在、明确 actor id，非广播）。
3. Workspace（Group）关系（同 Group）。
4. 角色通信规则（foreman↔peer 定向，禁止 peer→peer、广播、多接收者）。

普通 chat 消息保持现有广播/多接收者兼容语义，不受此限。

验收语义：a3 发给 a1 时，a2 不可见——任务消息的解析结果只含唯一明确接收者，
不扩展为广播或多接收者，a2 不会出现在投递目标中。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from ..contracts.v1.coordination import (
    ERR_ACTOR_DISABLED,
    ERR_ACTOR_NOT_FOUND,
    ERR_TASK_RECIPIENT_REQUIRED,
    RouteValidationError,
    TaskMessageKind,
    CoordinationMode,
    validate_task_route,
)
from .actors import find_actor, find_foreman, get_effective_role, is_internal_actor, resolve_recipient_tokens
from .group import Group
from .recipient_syntax import has_hash_recipient_token


@dataclass(frozen=True)
class RouteDecision:
    """路由校验结果。

    ``recipient_actor_ids`` 为最终投递目标。对任务消息，长度必为 1 且为明确 actor id；
    对普通 chat 消息，保持现有解析语义（可为空=广播、多接收者、@all 等）。
    """

    kind: TaskMessageKind
    recipient_actor_ids: List[str]
    sender_role: str
    recipient_roles: List[str]
    is_task_message: bool


class RouteValidator:
    """任务消息路由校验器。

    仅在 ``foreman_managed`` 协调模式下对任务消息强制严格规则。
    ``legacy`` 模式与 chat/system 消息直接返回现有解析结果，不做限制。
    """

    def __init__(self, group: Group, *, coordination_mode: CoordinationMode = "legacy") -> None:
        self.group = group
        self.coordination_mode = coordination_mode

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------

    def validate(
        self,
        *,
        sender_actor_id: str,
        kind: TaskMessageKind,
        recipient_tokens: Sequence[str],
    ) -> RouteDecision:
        """校验并解析路由。返回 RouteDecision；校验失败抛 RouteValidationError。"""
        sender_role = self._sender_role(sender_actor_id)
        tokens_list = list(recipient_tokens)

        # 任务消息：在解析前先拦截非法 token（#group、广播），避免 resolve 抛 ValueError
        if kind in ("task_instruction", "task_result"):
            self._precheck_task_tokens(tokens_list)

        try:
            resolved = resolve_recipient_tokens(self.group, tokens_list)
        except ValueError as exc:
            # 解析失败（未知接收者等）：任务消息要求明确接收者，转为 TASK_RECIPIENT_REQUIRED
            if kind in ("task_instruction", "task_result"):
                raise RouteValidationError(ERR_TASK_RECIPIENT_REQUIRED, str(exc)) from exc
            raise
        recipient_roles = [get_effective_role(self.group, aid) for aid in resolved]

        # 任务消息：强制单接收者、禁止广播/多接收者/peer→peer
        if kind in ("task_instruction", "task_result"):
            self._validate_task_message(
                sender_actor_id=sender_actor_id,
                sender_role=sender_role,
                kind=kind,
                recipient_tokens=tokens_list,
                resolved=resolved,
                recipient_roles=recipient_roles,
            )
            return RouteDecision(
                kind=kind,
                recipient_actor_ids=resolved,
                sender_role=sender_role,
                recipient_roles=recipient_roles,
                is_task_message=True,
            )

        # 普通 chat / system：保持现有兼容语义
        return RouteDecision(
            kind=kind,
            recipient_actor_ids=resolved,
            sender_role=sender_role,
            recipient_roles=recipient_roles,
            is_task_message=False,
        )

    def _precheck_task_tokens(self, recipient_tokens: List[str]) -> None:
        """任务消息解析前拦截非法 token（#group 哈希），避免 resolve 抛 ValueError。"""
        if has_hash_recipient_token(recipient_tokens):
            raise RouteValidationError(
                ERR_TASK_RECIPIENT_REQUIRED, "task message must not use #group recipient tokens"
            )

    # ------------------------------------------------------------------
    # 内部校验
    # ------------------------------------------------------------------

    def _sender_role(self, sender_actor_id: str) -> str:
        sid = str(sender_actor_id or "").strip()
        if not sid:
            # user / system 发送者不在 actor 列表中；任务消息要求 actor 发送者，
            # 由 _validate_task_message 进一步拒绝。这里返回 peer 作中性默认。
            return "peer"
        actor = find_actor(self.group, sid)
        if actor is None:
            # 非 Actor 发送者（user/system）——任务消息校验会拒绝
            return "peer"
        return get_effective_role(self.group, sid)

    def _validate_task_message(
        self,
        *,
        sender_actor_id: str,
        sender_role: str,
        kind: TaskMessageKind,
        recipient_tokens: List[str],
        resolved: List[str],
        recipient_roles: List[str],
    ) -> None:
        # 任务消息发送者必须是明确的 enabled actor
        sid = str(sender_actor_id or "").strip()
        if not sid:
            raise RouteValidationError(ERR_ACTOR_NOT_FOUND, "task message sender must be an actor")
        sender_actor = find_actor(self.group, sid)
        if sender_actor is None:
            raise RouteValidationError(ERR_ACTOR_NOT_FOUND, f"sender not found: {sid}")
        if is_internal_actor(sender_actor):
            raise RouteValidationError(ERR_ACTOR_NOT_FOUND, "internal actor cannot send task messages")
        # enabled 校验（actor dict 中 enabled 字段）
        if not bool(sender_actor.get("enabled", True)):
            raise RouteValidationError(ERR_ACTOR_DISABLED, f"sender disabled: {sid}")

        # 广播 / 空接收者检测（resolve_recipient_tokens 对空/广播返回 []）
        if not resolved:
            raise RouteValidationError(ERR_TASK_RECIPIENT_REQUIRED, "task message requires an explicit recipient")

        # 委托给契约层的纯路由校验（多接收者、角色、跨 Group）
        validate_task_route(
            coordination_mode=self.coordination_mode,
            kind=kind,
            sender_role=sender_role,
            recipient_tokens=recipient_tokens,
            resolved_recipient_ids=resolved,
            recipient_roles=recipient_roles,
            same_group=True,  # 同一 Group 内解析，跨 Group 由 group_bridge 处理
        )


def validate_message_route(
    group: Group,
    *,
    coordination_mode: CoordinationMode,
    sender_actor_id: str,
    kind: TaskMessageKind,
    recipient_tokens: Sequence[str],
) -> RouteDecision:
    """便捷函数：构造 RouteValidator 并校验。"""
    return RouteValidator(group, coordination_mode=coordination_mode).validate(
        sender_actor_id=sender_actor_id,
        kind=kind,
        recipient_tokens=recipient_tokens,
    )


def is_recipient_visible(actor_id: str, decision: RouteDecision) -> bool:
    """验收辅助：判断某 actor 是否在路由决策的可见接收者中。

    对任务消息，只有唯一明确接收者可见（a3 发给 a1 时，a2 不可见）。
    对普通 chat 消息，按现有解析结果判断（广播时所有 actor 可见，由上层投递决定）。
    """
    return actor_id in decision.recipient_actor_ids
