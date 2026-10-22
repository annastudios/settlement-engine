"""A worker: claims payments from the Redis queue and processes them.

Run one (or several, in separate terminals) with:
    python -m settlement.worker
"""

import logging
import os
import signal
import socket
import threading
import time
import uuid
from collections.abc import Callable

import redis
from sqlalchemy import select
from sqlalchemy.orm import Session

from settlement import audit, config, connections, rate_limit, recovery, work_queue
from settlement.backoff import backoff_seconds
from settlement.models import Payment
from settlement.rail_client import RailRejected, RailUnavailable, send_to_rail
from settlement.schemas import PaymentStatus

log = logging.getLogger("settlement.worker")


def process_one(
    r: redis.Redis,
    worker_id: str,
    send: Callable[[Payment], None] = send_to_rail,
) -> str | None:
    """Claim and process the next payment. Returns its UETR, or None if
    nothing is ready.

    Settled, rejected and in_repair are final for now, so the lease is
    released. A payment to retry is parked in Redis until its retry is due.
    Anything unexpected (a bug, Postgres down) raises and the lease is NOT
    released, so the payment is not lost: once the lease expires, recovery
    puts it back in line.
    """
    uetr = work_queue.claim(r, worker_id)
    if uetr is None:
        return None
    with connections.SessionLocal() as session:
        slot_in = _rail_slot(r, session, uetr)
        if slot_in > 0:
            # Over the rail's rate limit: park it until its booked slot. Not
            # an attempt (nothing was sent) and its status doesn't change.
            work_queue.retry_later(r, uetr, worker_id, slot_in)
            return uetr
        retry_in = _process(session, uuid.UUID(uetr), send, actor=f"worker {worker_id}")
    if retry_in is None:
        work_queue.ack(r, uetr, worker_id)
    else:
        work_queue.retry_later(r, uetr, worker_id, retry_in)
    return uetr


def _rail_slot(r: redis.Redis, session: Session, uetr: str) -> float:
    """Seconds until this payment may be sent under its rail's rate limit."""
    if r.srem(rate_limit.BOOKED, uetr):
        return 0.0  # parked earlier with a slot booked, and the slot is here
    rail = session.scalar(select(Payment.rail).where(Payment.uetr == uuid.UUID(uetr)))
    if rail is None:
        return 0.0  # unknown payment: _process will skip it
    slot_in = rate_limit.book_slot(r, rail)
    if slot_in > 0:
        r.sadd(rate_limit.BOOKED, uetr)
    return slot_in


def _process(
    session: Session, uetr: uuid.UUID, send: Callable[[Payment], None], *, actor: str
) -> float | None:
    """Send the payment to the rail and record the outcome. Returns how many
    seconds to wait before retrying, or None if there is nothing to retry."""
    # Postgres guard: only a payment waiting for an attempt may start.
    # Redis decides who works on a payment; this makes sure it happens once,
    # even if the same UETR was somehow queued or handed out twice.
    started = audit.move(
        session, uetr, (PaymentStatus.RECEIVED, PaymentStatus.RETRYING),
        PaymentStatus.PROCESSING, actor=actor, reason="sending to the rail",
        attempts=Payment.attempts + 1,
    )
    if not started:
        log.info("payment %s is not waiting to be processed; skipping", uetr)
        return None

    payment = session.get(Payment, uetr, populate_existing=True)
    try:
        send(payment)
    except RailRejected as exc:
        audit.move(
            session, uetr, (PaymentStatus.PROCESSING,), PaymentStatus.REJECTED,
            actor=actor, reason=f"rejected by the rail: {exc.reason}",
            rejection_reason=exc.reason,
        )
        log.info("payment %s rejected by the rail: %s", uetr, exc.reason)
        return None
    except RailUnavailable as exc:
        # Outcome unknown: the next attempt resends the same UETR, which the
        # rail dedupes, so it can't move the money twice.
        return _after_unavailable(session, payment, str(exc), actor=actor)

    audit.move(
        session, uetr, (PaymentStatus.PROCESSING,), PaymentStatus.SETTLED,
        actor=actor, reason="settled by the rail", last_error=None,
    )
    log.info("payment %s settled (attempt %d)", uetr, payment.attempts)
    return None


def _after_unavailable(
    session: Session, payment: Payment, error: str, *, actor: str
) -> float | None:
    if payment.attempts >= config.MAX_ATTEMPTS:
        audit.move(
            session, payment.uetr, (PaymentStatus.PROCESSING,), PaymentStatus.IN_REPAIR,
            actor=actor, reason=f"out of attempts: {error}", last_error=error,
        )
        log.warning(
            "payment %s moved to the repair queue after %d attempts: %s",
            payment.uetr, payment.attempts, error,
        )
        return None

    delay = backoff_seconds(
        payment.attempts, config.RETRY_BASE_SECONDS, config.RETRY_MAX_DELAY_SECONDS
    )
    audit.move(
        session, payment.uetr, (PaymentStatus.PROCESSING,), PaymentStatus.RETRYING,
        actor=actor, reason=f"{error}; retrying in {delay:.1f}s", last_error=error,
    )
    log.warning(
        "rail unavailable for payment %s (attempt %d of %d), retrying in %.1fs: %s",
        payment.uetr, payment.attempts, config.MAX_ATTEMPTS, delay, error,
    )
    return delay


def run(
    worker_id: str | None = None,
    poll_seconds: float = 0.2,
    stop: threading.Event | None = None,
) -> None:
    """Process payments until stop is set. Waits poll_seconds when nothing is
    ready, and runs crash recovery every RECOVERY_INTERVAL_SECONDS."""
    worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
    stop = stop or threading.Event()
    next_recovery = 0.0
    log.info("worker %s started", worker_id)
    while not stop.is_set():
        if time.monotonic() >= next_recovery:
            next_recovery = time.monotonic() + config.RECOVERY_INTERVAL_SECONDS
            try:
                recovery.recover(connections.redis_client, actor=f"recovery by worker {worker_id}")
            except Exception:
                log.exception("crash recovery failed; will try again")
        try:
            if process_one(connections.redis_client, worker_id) is None:
                stop.wait(poll_seconds)
        except Exception:
            # One bad payment or a brief Redis/Postgres outage must not kill
            # the worker. Log it, pause, carry on.
            log.exception("error while processing; continuing")
            stop.wait(poll_seconds)
    log.info("worker %s stopped", worker_id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is noise
    stop_event = threading.Event()
    # Ctrl+C or `docker stop`: finish the current payment, then exit.
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    run(stop=stop_event)
