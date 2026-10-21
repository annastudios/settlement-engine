import logging
import uuid
from contextlib import asynccontextmanager

import redis
from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from settlement import connections, work_queue
from settlement.models import Base, Payment
from settlement.payments import UetrConflict, submit_payment
from settlement.schemas import PaymentRequest, PaymentResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Creates missing tables. It does not change existing ones: after a schema
    # change, reset the dev database with `docker compose down -v`.
    Base.metadata.create_all(connections.engine)
    yield


app = FastAPI(title="settlement-engine", lifespan=lifespan)
log = logging.getLogger("settlement.api")


def get_session():
    with connections.SessionLocal() as session:
        yield session


def _check_postgres() -> str:
    try:
        with connections.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return "ok"
    except Exception as exc:
        return f"error: {type(exc).__name__}"


def _check_redis() -> str:
    try:
        connections.redis_client.ping()
        return "ok"
    except Exception as exc:
        return f"error: {type(exc).__name__}"


@app.get("/health")
def health(response: Response):
    """Reports whether Postgres and Redis are reachable. 503 if either is not."""
    checks = {"postgres": _check_postgres(), "redis": _check_redis()}
    healthy = all(result == "ok" for result in checks.values())
    if not healthy:
        response.status_code = 503
    return {"status": "ok" if healthy else "degraded", "checks": checks}


@app.post("/payments", response_model=PaymentResponse, status_code=201)
def create_payment(
    request: PaymentRequest,
    response: Response,
    session: Session = Depends(get_session),
):
    """Submit a payment. 201 when new, 200 when this exact payment was
    already submitted, 409 when the UETR belongs to a different payment."""
    try:
        payment, created = submit_payment(session, request)
    except UetrConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not created:
        response.status_code = 200
        return payment

    # The payment is already safely in Postgres, so a Redis failure here must
    # not fail the request. The payment then waits in 'received' until
    # recovery (step 5) re-queues payments that never reached the queue.
    try:
        work_queue.enqueue(connections.redis_client, str(payment.uetr))
    except redis.RedisError:
        log.exception("payment %s stored but not queued", payment.uetr)
    return payment


@app.get("/payments/{uetr}", response_model=PaymentResponse)
def get_payment(uetr: uuid.UUID, session: Session = Depends(get_session)):
    payment = session.get(Payment, uetr)
    if payment is None:
        raise HTTPException(status_code=404, detail="payment not found")
    return payment
