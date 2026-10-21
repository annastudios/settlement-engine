"""Crash recovery: finds payments that got stuck and puts them back in line.

Two ways a payment gets stuck:

1. Its worker stopped (crashed, was killed, or stalled) while holding it,
   so the lease expires. The payment may be 'processing' (the worker died
   mid-send), still 'received'/'retrying' (died right after claiming), or
   already final (died just before releasing the lease).
2. Redis lost track of it: Redis was down when it was submitted, or Redis
   was wiped. Postgres says it is waiting, Redis has no trace of it.

In both cases Postgres decides what happens, and the payment is re-queued
only if it is still waiting for an attempt. A payment that was mid-send is
resent with the same UETR, which the rail dedupes, so recovering it can't
move the money twice, even if the "dead" worker's send actually went through.

Workers run this every RECOVERY_INTERVAL_SECONDS. Several workers running it
at once is safe: each expired lease goes to exactly one of them (atomic Lua),
and a payment queued twice is skipped by the Postgres guard.
"""

import logging
import uuid
from datetime import timedelta

import redis
from sqlalchemy import func, select, update

from settlement import config, connections, work_queue
from settlement.models import Payment
from settlement.schemas import PaymentStatus

log = logging.getLogger("settlement.recovery")

_WAITING = (PaymentStatus.RECEIVED, PaymentStatus.RETRYING)
_STOPPED_ERROR = "worker stopped while processing (lease expired)"


def recover(r: redis.Redis) -> int:
    """Run one recovery pass. Returns how many payments were re-queued."""
    requeued = 0
    for uetr in work_queue.reclaim_expired(r):
        if _requeue(r, uuid.UUID(uetr)):
            log.warning("payment %s recovered: its worker stopped holding it", uetr)
            requeued += 1
    for uetr in _orphans(r):
        if _requeue(r, uetr, unchanged_for=timedelta(seconds=config.ORPHAN_AFTER_SECONDS)):
            log.warning("payment %s recovered: it was missing from the queue", uetr)
            requeued += 1
    return requeued


def _requeue(r: redis.Redis, uetr: uuid.UUID, unchanged_for: timedelta | None = None) -> bool:
    """Put the payment back in the queue if it is still waiting for an attempt.

    A payment left in 'processing' counts that attempt as used: it goes back
    to 'retrying', or to the repair queue if it is out of attempts. That
    stops a payment that crashes the worker every time from looping forever.
    With unchanged_for, only a payment unchanged for at least that long is
    touched, so one a worker is handling right now is left alone.
    """
    abandoned = [Payment.uetr == uetr, Payment.status == PaymentStatus.PROCESSING]
    if unchanged_for is not None:
        abandoned.append(Payment.updated_at < func.now() - unchanged_for)

    with connections.SessionLocal() as session:
        session.execute(
            update(Payment)
            .where(*abandoned, Payment.attempts < config.MAX_ATTEMPTS)
            .values(status=PaymentStatus.RETRYING, last_error=_STOPPED_ERROR)
        )
        session.execute(
            update(Payment)
            .where(*abandoned, Payment.attempts >= config.MAX_ATTEMPTS)
            .values(status=PaymentStatus.IN_REPAIR, last_error=_STOPPED_ERROR)
        )
        session.commit()
        status = session.scalar(select(Payment.status).where(Payment.uetr == uetr))

    if status not in _WAITING:
        return False  # final, in repair, or being processed right now
    work_queue.enqueue(r, str(uetr))
    return True


def _orphans(r: redis.Redis) -> list[uuid.UUID]:
    """Payments Postgres says are in flight, unchanged for ORPHAN_AFTER_SECONDS,
    that Redis has no trace of."""
    with connections.SessionLocal() as session:
        candidates = session.scalars(
            select(Payment.uetr)
            .where(
                Payment.status.in_((*_WAITING, PaymentStatus.PROCESSING)),
                Payment.updated_at
                < func.now() - timedelta(seconds=config.ORPHAN_AFTER_SECONDS),
            )
            .order_by(Payment.updated_at)
            .limit(500)
        ).all()
    return [uetr for uetr in candidates if not work_queue.is_queued(r, str(uetr))]
