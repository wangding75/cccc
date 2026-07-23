"""CCCC 消息投递系统 (CCCC-CORE-04).

建立 Actor 独立 Mailbox 与可靠投递模型：queued/reserved/delivered/failed 状态、
消息锁、防止重复消费、投递日志。复用 CCCC-CORE-02 的 coordination_store 持久化。

设计要点：
- 每个 Actor 独立逻辑队列：发给 a1 的投递只进入 a1 的 mailbox，a2 不得消费。
- 消息锁：reserved 状态绑定 consumer id + locked_at，锁过期可回退 queued。
- 防止重复消费：reserve 幂等（同 delivery 已 reserved/终态则拒绝），ack 推进到 delivered。
- 投递日志：每次状态迁移记录 attempt（last_error / attempts / updated_at）。
- 重启不丢失、不重复：状态持久化于文件，重启后 reserved 锁过期者回退 queued。

本模块只实现投递状态机与邮箱语义，不实现实际投递传输（Runner/进程属后续任务）。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from ..contracts.v1.coordination import (
    ERR_DELIVERY_ALREADY_EXISTS,
    Delivery,
    DeliveryState,
    RouteValidationError,
)
from ..util.time import parse_utc_iso, utc_now_iso
from . import coordination_store as store
from .actors import find_actor
from .group import Group

DEFAULT_LOCK_TTL_SECONDS = 300.0
"""消息锁默认有效期；超过则视为过期可回退。"""


# ---------------------------------------------------------------------------
# Mailbox
# ---------------------------------------------------------------------------


def mailbox_deliveries(group: Group, actor_id: str) -> List[Delivery]:
    """返回某 Actor 的全部投递（独立逻辑队列视角）。

    发给 a1 的投递只属于 a1；a2 调用此函数不会看到 a1 的投递。
    """
    return store.list_deliveries(group, recipient_actor_id=actor_id)


def queued_deliveries(group: Group, actor_id: str) -> List[Delivery]:
    """返回某 Actor 当前可消费的 queued 投递（按 updated_at 排序）。"""
    items = [d for d in mailbox_deliveries(group, actor_id) if d.state == "queued"]
    items.sort(key=lambda d: d.updated_at or d.created_at)
    return items


# ---------------------------------------------------------------------------
# 投递生命周期
# ---------------------------------------------------------------------------


def enqueue_delivery(
    group: Group,
    *,
    delivery_id: str,
    message_id: str,
    recipient_actor_id: str,
) -> Delivery:
    """入队一条投递。唯一约束：(message_id, recipient) → DELIVERY_ALREADY_EXISTS。"""
    delivery = Delivery(
        delivery_id=delivery_id,
        group_id=group.group_id,
        message_id=message_id,
        recipient_actor_id=recipient_actor_id,
        state="queued",
    )
    return store.create_delivery(group, delivery)


def reserve_delivery(
    group: Group,
    delivery_id: str,
    *,
    consumer_id: str,
    lock_ttl_seconds: float = DEFAULT_LOCK_TTL_SECONDS,
    now_iso: Optional[str] = None,
) -> Delivery:
    """预留投递（消息锁）。防止重复消费：已 reserved/终态则拒绝。

    返回更新后的 Delivery；若投递不存在抛 ValueError；若不可预留抛 RouteValidationError。
    """
    d = store.get_delivery(group, delivery_id)
    if d is None:
        raise ValueError(f"delivery not found: {delivery_id}")

    # 终态不可预留
    if d.state in ("delivered", "failed"):
        raise RouteValidationError(ERR_DELIVERY_ALREADY_EXISTS, f"delivery already terminal: {d.state}")

    now = now_iso or utc_now_iso()
    # 已被他人锁定且未过期 → 拒绝（防止重复消费）
    if d.state == "reserved" and d.locked_by and d.locked_by != consumer_id:
        if not _lock_expired(d, lock_ttl_seconds, now):
            raise RouteValidationError(ERR_DELIVERY_ALREADY_EXISTS, f"delivery locked by {d.locked_by}")

    # 过期的 reserved 锁可被抢占（回退后重新预留）
    attempts = d.attempts + 1
    return store.update_delivery_state(
        group,
        delivery_id,
        state="reserved",
        attempts=attempts,
        locked_by=consumer_id,
        locked_at=now,
        last_error=None,
    )


def ack_delivery(group: Group, delivery_id: str, *, consumer_id: str) -> Delivery:
    """确认投递完成 → delivered。只有持锁 consumer 可确认。"""
    d = store.get_delivery(group, delivery_id)
    if d is None:
        raise ValueError(f"delivery not found: {delivery_id}")
    if d.state == "delivered":
        return d  # 幂等
    if d.state != "reserved":
        raise RouteValidationError(ERR_DELIVERY_ALREADY_EXISTS, f"cannot ack from state {d.state}")
    if d.locked_by and d.locked_by != consumer_id:
        raise RouteValidationError(ERR_DELIVERY_ALREADY_EXISTS, f"not owner: {consumer_id}")
    return store.update_delivery_state(
        group,
        delivery_id,
        state="delivered",
        locked_by=consumer_id,
        last_error=None,
    )


def nack_delivery(
    group: Group,
    delivery_id: str,
    *,
    consumer_id: str,
    error: str,
    requeue: bool = True,
) -> Delivery:
    """否定确认：失败时回退 queued（可重试）或置 failed。"""
    d = store.get_delivery(group, delivery_id)
    if d is None:
        raise ValueError(f"delivery not found: {delivery_id}")
    if d.state in ("delivered", "failed"):
        raise RouteValidationError(ERR_DELIVERY_ALREADY_EXISTS, f"delivery already terminal: {d.state}")
    if d.locked_by and d.locked_by != consumer_id:
        raise RouteValidationError(ERR_DELIVERY_ALREADY_EXISTS, f"not owner: {consumer_id}")
    new_state: DeliveryState = "queued" if requeue else "failed"
    return store.update_delivery_state(
        group,
        delivery_id,
        state=new_state,
        clear_lock=True,
        last_error=error,
    )


def release_expired_locks(
    group: Group,
    *,
    lock_ttl_seconds: float = DEFAULT_LOCK_TTL_SECONDS,
    now_iso: Optional[str] = None,
) -> List[str]:
    """重启/清理：将过期的 reserved 锁回退 queued。返回被释放的 delivery_id 列表。"""
    now = now_iso or utc_now_iso()
    released: List[str] = []
    for d in store.list_deliveries(group):
        if d.state == "reserved" and _lock_expired(d, lock_ttl_seconds, now):
            store.update_delivery_state(
                group,
                d.delivery_id,
                state="queued",
                clear_lock=True,
                last_error="lock expired",
            )
            released.append(d.delivery_id)
    return released


# ---------------------------------------------------------------------------
# 投递日志
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DeliveryLogEntry:
    delivery_id: str
    state: DeliveryState
    attempts: int
    locked_by: Optional[str]
    last_error: Optional[str]
    updated_at: str


def delivery_log(group: Group, delivery_id: str) -> DeliveryLogEntry:
    """读取投递当前状态作为日志快照（完整历史由 ledger/事件流记录，本任务提供当前视图）。"""
    d = store.get_delivery(group, delivery_id)
    if d is None:
        raise ValueError(f"delivery not found: {delivery_id}")
    return DeliveryLogEntry(
        delivery_id=d.delivery_id,
        state=d.state,
        attempts=d.attempts,
        locked_by=d.locked_by,
        last_error=d.last_error,
        updated_at=d.updated_at,
    )


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------


def _lock_expired(d: Delivery, ttl_seconds: float, now_iso: str) -> bool:
    if not d.locked_at:
        return True
    locked = parse_utc_iso(d.locked_at)
    now = parse_utc_iso(now_iso)
    if locked is None or now is None:
        return False
    return (now - locked).total_seconds() > ttl_seconds


def can_consume(group: Group, actor_id: str, delivery_id: str) -> bool:
    """校验该投递是否属于该 Actor 的 mailbox（a2 不能消费 a1 的投递）。"""
    d = store.get_delivery(group, delivery_id)
    if d is None:
        return False
    return d.recipient_actor_id == actor_id
