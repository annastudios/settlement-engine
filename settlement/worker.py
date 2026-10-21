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

from settlement import connections, work_queue
from settlement.models import Payment
from settlement.schemas import PaymentStatus

log = logging.getLogger("settlement.worker")


def send_to_rail(payment: Payment) -> None:
    """Placeholder: always succeeds. Step 3 sends to the rail simulator."""


def process_one(
    r: redis.Redis,
    worker_id: str,
    send: Callable[[Payment], None] = send_to_rail,
) -> str | None:
    """Claim and process the next payment. Returns its UETR, or None if the
    queue is empty.

    If processing raises, the lease is not released, so the payment is not
    lost: it stays leased until recovery (step 5) hands it out again.
    """
    uetr = work_queue.claim(r, worker_id)
    if uetr is None:
        return None
    with connections.SessionLocal() as session:
        _process(session, uuid.UUID(uetr), send)
    work_queue.ack(r, uetr, worker_id)
    return uetr


def _process(session: Session, uetr: uuid.UUID, send: Callable[[Payment], None]) -> None:
    # Postgres guard: only a payment that is still 'received' may start.
    # Redis decides who works on a payment; this makes sure it happens once,
    # even if the same UETR was somehow queued or handed out twice.
    started = _move(session, uetr, PaymentStatus.RECEIVED, PaymentStatus.PROCESSING)
    if not started:
        log.info("payment %s is not waiting to be processed; skipping", uetr)
        return

    send(session.get(Payment, uetr))

    _move(session, uetr, PaymentStatus.PROCESSING, PaymentStatus.SETTLED)
    log.info("payment %s settled", uetr)


def _move(session: Session, uetr: uuid.UUID, from_status: str, to_status: str) -> bool:
    """Change the status only if it is currently from_status. True if it changed."""
    result = session.execute(
        update(Payment)
        .where(Payment.uetr == uetr, Payment.status == from_status)
        .values(status=to_status)
    )
    session.commit()
    return result.rowcount == 1


def run(
    worker_id: str | None = None,
    poll_seconds: float = 0.2,
    stop: threading.Event | None = None,
) -> None:
    """Process payments until stop is set. Waits poll_seconds when the queue is empty."""
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
    stop_event = threading.Event()
    # Ctrl+C or `docker stop`: finish the current payment, then exit.
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    run(stop=stop_event)
