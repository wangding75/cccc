"""CCCC 执行授权校验。

负责在 execute_instruction 入口强制依据持久化层 Delivery、TaskMessage、Actor
和锁所有权执行校验。任何校验失败发生在创建 Session、Run 或进程之前。

调用约定：
    authorize_delivery_execution(group, delivery_id, actor_id, consumer_id)
        → ExecutionAuthorization
        → 失败时抛 RouteValidationError
"""

from __future__ import annotations

from typing import Optional

from ..contracts.v1.coordination import (
    ERR_DELIVERY_NOT_EXECUTABLE,
    ERR_DELIVERY_NOT_FOUND,
    ERR_DELIVERY_OWNER_MISMATCH,
    ERR_RUN_ALREADY_EXECUTED,
    ERR_TASK_MESSAGE_KIND_INVALID,
    ERR_TASK_MESSAGE_NOT_FOUND,
    ERR_TASK_RECIPIENT_MISMATCH,
    ERR_TASK_SENDER_NOT_FOREMAN,
    ERR_TASK_RECIPIENT_NOT_PEER,
    ERR_ACTOR_NOT_FOUND,
    ERR_ACTOR_DISABLED,
    RouteValidationError,
)
from .coordination_store import coordination_dir, get_delivery, list_runs
from .collaboration_loop import get_task_message
from .actors import find_actor, get_effective_role, find_foreman, is_internal_actor


EXECUTABLE_DELIVERY_STATES = frozenset({"reserved"})
"""仅 reserved 状态允许执行。"""


class ExecutionAuthorization:
    """执行授权结果：携带必要引用供调用方使用。"""

    def __init__(
        self,
        delivery_id: str,
        group_id: str,
        message_id: str,
        peer_actor_id: str,
        consumer_id: str,
    ) -> None:
        self.delivery_id = delivery_id
        self.group_id = group_id
        self.message_id = message_id
        self.peer_actor_id = peer_actor_id
        self.consumer_id = consumer_id


def authorize_delivery_execution(
    group: object,
    *,
    delivery_id: str,
    actor_id: str,
    consumer_id: str,
) -> ExecutionAuthorization:
    """从持久化层重新读取并校验执行授权。

    输入：
        group: Group。
        delivery_id: 要执行的投递 ID。
        actor_id: 调用方声称的执行者（必须与 recipient 及锁 owner 一致）。
        consumer_id: 当前消费者标识（用于锁 owner 校验）。

    返回：
        ExecutionAuthorization：授权通过的权威引用。

    抛出：
        RouteValidationError：任一校验失败。
    """
    if not actor_id or not consumer_id or not delivery_id:
        raise RouteValidationError(
            ERR_DELIVERY_NOT_FOUND,
            "delivery_id, actor_id and consumer_id are required",
        )

    delivery = get_delivery(group, delivery_id)
    if delivery is None:
        raise RouteValidationError(ERR_DELIVERY_NOT_FOUND, f"delivery not found: {delivery_id}")

    message_id = delivery.message_id
    task_message = get_task_message(group, message_id)
    if task_message is None:
        raise RouteValidationError(
            ERR_TASK_MESSAGE_NOT_FOUND, f"task message not found: {message_id}"
        )

    # 3. kind 必须为 task_instruction
    if task_message.kind != "task_instruction":
        raise RouteValidationError(
            ERR_TASK_MESSAGE_KIND_INVALID,
            f"expected task_instruction, got {task_message.kind}",
        )

    # 4. group_id 一致
    if task_message.group_id != delivery.group_id or task_message.group_id != group.group_id:
        raise RouteValidationError(
            ERR_SESSION_GROUP_MISMATCH,
            "task message, delivery and group must share the same group_id",
        )

    # 5. recipient_actor_id == actor_id
    if task_message.recipient_actor_id != actor_id:
        raise RouteValidationError(
            ERR_TASK_RECIPIENT_MISMATCH,
            f"task recipient={task_message.recipient_actor_id} != actor={actor_id}",
        )

    # 6. 发送者为 foreman，接收者为 enabled peer
    sender_actor = find_actor(group, task_message.sender_actor_id)
    if sender_actor is None or is_internal_actor(sender_actor):
        raise RouteValidationError(
            ERR_TASK_SENDER_NOT_FOREMAN,
            f"sender actor not found or internal: {task_message.sender_actor_id}",
        )
    sender_role = get_effective_role(group, task_message.sender_actor_id)
    if sender_role != "foreman":
        raise RouteValidationError(
            ERR_TASK_SENDER_NOT_FOREMAN,
            f"sender role={sender_role} is not foreman: {task_message.sender_actor_id}",
        )

    recipient_actor = find_actor(group, actor_id)
    if recipient_actor is None:
        raise RouteValidationError(ERR_ACTOR_NOT_FOUND, f"recipient actor not found: {actor_id}")
    if is_internal_actor(recipient_actor):
        # 不把内部 actor 作为任务执行者
        recipient_role = "peer"
    else:
        recipient_role = get_effective_role(group, actor_id)
    if not recipient_actor.get("enabled", True):
        raise RouteValidationError(ERR_ACTOR_DISABLED, f"recipient actor disabled: {actor_id}")
    if recipient_role != "peer":
        raise RouteValidationError(
            ERR_TASK_RECIPIENT_NOT_PEER,
            f"recipient role={recipient_role} is not peer: {actor_id}",
        )

    # 7. delivery.recipient_actor_id == actor_id
    if delivery.recipient_actor_id != actor_id:
        raise RouteValidationError(
            ERR_TASK_RECIPIENT_MISMATCH,
            f"delivery recipient={delivery.recipient_actor_id} != actor={actor_id}",
        )

    # 8. delivery.state == reserved
    if delivery.state not in EXECUTABLE_DELIVERY_STATES:
        raise RouteValidationError(
            ERR_DELIVERY_NOT_EXECUTABLE,
            f"delivery state={delivery.state} is not executable",
        )

    # 9. locked_by == consumer_id
    locked_by = str(delivery.locked_by or "").strip()
    if not locked_by or locked_by != consumer_id:
        raise RouteValidationError(
            ERR_DELIVERY_OWNER_MISMATCH,
            f"delivery locked_by={locked_by} != consumer={consumer_id}",
        )

    # 10. 重复执行检查：同一 message + actor 尚未创建可执行/终态 Run
    existing_runs = list_runs(group, actor_id=actor_id, message_id=message_id)
    for run in existing_runs:
        if run.state in {
            "pending",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            "timed_out",
        }:
            raise RouteValidationError(
                ERR_RUN_ALREADY_EXECUTED,
                f"existing run={run.run_id} state={run.state} for message={message_id} actor={actor_id}",
            )

    return ExecutionAuthorization(
        delivery_id=delivery.delivery_id,
        group_id=group.group_id,
        message_id=message_id,
        peer_actor_id=actor_id,
        consumer_id=consumer_id,
    )
