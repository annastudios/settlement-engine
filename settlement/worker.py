"""A worker: claims payments from the Redis queue and processes them.

Run one (or several, in separate terminals) with:
    python -m settlement.worker
"""

import logging
import os
import signal
import socket
import threading
import uuid
from collections.abc import Callable

import redis
from sqlalchemy import update
from sqlalchemy.orm import Session

from settlement import config, connections, work_queue
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
    released, so the payment is not lost.
    """
    uetr = work_queue.claim(r, worker_id)
    if uetr is None:
        return None
    with connections.SessionLocal() as session:
        retry_in = _process(session, uuid.UUID(uetr), send)
    if retry_in is None:
        work_queue.ack(r, uetr, worker_id)
    else:
        work_queue.retry_later(r, uetr, worker_id, retry_in)
    return uetr


def _process(
    session: Session, uetr: uuid.UUID, send: Callable[[Payment], None]
) -> float | None:
    """Send the payment to the rail and record the outcome. Returns how many
    seconds to wait before retrying, or None if there is nothing to retry."""
    # Postgres guard: only a payment waiting for an attempt may start.
    # Redis decides who works on a payment; this makes sure it happens once,
    # even if the same UETR was somehow queued or handed out twice.
    started = _move(
        session, uetr, (PaymentStatus.RECEIVED, PaymentStatus.RETRYING),
        PaymentStatus.PROCESSING, attempts=Payment.attempts + 1,
    )
    if not started:
        log.info("payment %s is not waiting to be processed; skipping", uetr)
        return None

    payment = session.get(Payment, uetr, populate_existing=True)
    try:
        send(payment)
    except RailRejected as exc:
        _move(
            session, uetr, (PaymentStatus.PROCESSING,), PaymentStatus.REJECTED,
            rejection_reason=exc.reason,
        )
        log.info("payment %s rejected by the rail: %s", uetr, exc.reason)
        return None
    except RailUnavailable as exc:
        # Outcome unknown: the next attempt resends the same UETR, which the
        # rail dedupes, so it can't move the money twice.
        return _after_unavailable(session, payment, str(exc))

    _move(session, uetr, (PaymentStatus.PROCESSING,), PaymentStatus.SETTLED, last_error=None)
    log.info("payment %s settled (attempt %d)", uetr, payment.attempts)
    return None


def _after_unavailable(session: Session, payment: Payment, error: str) -> float | None:
    if payment.attempts >= config.MAX_ATTEMPTS:
        _move(
            session, payment.uetr, (PaymentStatus.PROCESSING,), PaymentStatus.IN_REPAIR,
            last_error=error,
        )
        log.warning(
            "payment %s moved to the repair queue after %d attempts: %s",
            payment.uetr, payment.attempts, error,
        )
        return None

    _move(
        session, payment.uetr, (PaymentStatus.PROCESSING,), PaymentStatus.RETRYING,
        last_error=error,
    )
    delay = backoff_seconds(
        payment.attempts, config.RETRY_BASE_SECONDS, config.RETRY_MAX_DELAY_SECONDS
    )
    log.warning(
        "rail unavailable for payment %s (attempt %d of %d), retrying in %.1fs: %s",
        payment.uetr, payment.attempts, config.MAX_ATTEMPTS, delay, error,
    )
    return delay


def _move(
    session: Session,
    uetr: uuid.UUID,
    from_statuses: tuple[str, ...],
    to_status: str,
    **changes,
) -> bool:
    """Change the status (and any other columns in changes) only if it is
    currently one of from_statuses. True if it changed."""
    result = session.execute(
        update(Payment)
        .where(Payment.uetr == uetr, Payment.status.in_(from_statuses))
        .values(status=to_status, **changes)
    )
    session.commit()
    return result.rowcount == 1


def run(
    worker_id: str | None = None,
    poll_seconds: float = 0.2,
    stop: threading.Event | None = None,
) -> None:
    """Process payments until stop is set. Waits poll_seconds when nothing is ready."""
    worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
    stop = stop or threading.Event()
    log.info("worker %s started", worker_id)
    while not stop.is_set():
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
