"""A live dashboard for watching payments move, plus the demo controls behind
its buttons. Open http://127.0.0.1:8000/dashboard while the rail simulator and
at least one worker are running.

The /demo endpoints generate test payments and change the rail simulator's
behaviour. They are for local development and demos only.
"""

import random
import uuid
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import redis
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from settlement import connections, rail_client, rate_limit, work_queue
from settlement.connections import get_session
from settlement.models import Payment
from settlement.payments import submit_payment
from settlement.schemas import PaymentRequest, PaymentResponse, PaymentStatus

router = APIRouter()

_PAGE = Path(__file__).parent / "static" / "dashboard.html"

_DEBTORS = ["Alice Smith", "Ravi Patel", "Chen Wei", "Maria Garcia", "Tom Brown"]
_CREDITORS = ["Bob's Bakery Ltd", "Northwind Energy", "City Council Tax", "Acme Rentals"]
_DEBTOR_IBAN = "GB33BUKB20201555555555"
_CREDITOR_IBAN = {"FPS": "GB82WEST12345698765432", "SEPA": "DE89370400440532013000"}
_CLOSED_ACCOUNT = "GB56CLOS00000000000001"  # the rail rejects it with AC04
# Window for measuring each rail's actual speed.
_SPEED_WINDOW_SECONDS = 5


@router.get("/dashboard", include_in_schema=False)
def dashboard_page():
    return FileResponse(_PAGE)


@router.get("/stats")
def stats(session: Session = Depends(get_session)):
    """Everything the dashboard shows, in one call."""
    counts = dict(
        session.execute(select(Payment.status, func.count()).group_by(Payment.status)).all()
    )
    recent = session.scalars(select(Payment).order_by(Payment.updated_at.desc()).limit(25)).all()
    repair = session.scalars(
        select(Payment)
        .where(Payment.status == PaymentStatus.IN_REPAIR)
        .order_by(Payment.created_at)
        .limit(25)
    ).all()
    return {
        "counts": {status.value: counts.get(status.value, 0) for status in PaymentStatus},
        "queue": _queue_state(connections.redis_client),
        "rate_limits": _rate_limits(session, connections.redis_client),
        "rail": _rail_state(),
        "recent": [_as_json(p) for p in recent],
        "repair_queue": [_as_json(p) for p in repair],
    }


def _as_json(payment: Payment) -> dict:
    return PaymentResponse.model_validate(payment).model_dump(mode="json")


def _queue_state(r: redis.Redis) -> dict:
    try:
        seconds, micros = r.time()
        now_ms = seconds * 1000 + micros // 1000
        owners = r.hgetall(work_queue.LEASE_OWNERS)
        leases = [
            {
                "uetr": uetr,
                "worker": worker,
                "seconds_left": round(((r.zscore(work_queue.LEASES, uetr) or now_ms) - now_ms) / 1000, 1),
            }
            for uetr, worker in owners.items()
        ]
        return {
            "ready": r.llen(work_queue.READY),
            # From the same read as the table, so the two always agree.
            "in_progress": len(leases),
            "waiting_to_retry": r.zcard(work_queue.DELAYED),
            "leases": leases,
        }
    except redis.RedisError:
        return {"error": "Redis unreachable"}


def _rate_limits(session: Session, r: redis.Redis) -> dict:
    """Each rail's limit next to its actual speed: payments that reached a
    final answer from the rail per second, over the last few seconds."""
    finished = dict(
        session.execute(
            select(Payment.rail, func.count())
            .where(
                Payment.status.in_((PaymentStatus.SETTLED, PaymentStatus.REJECTED)),
                Payment.updated_at > func.now() - timedelta(seconds=_SPEED_WINDOW_SECONDS),
            )
            .group_by(Payment.rail)
        ).all()
    )
    try:
        in_force = rate_limit.limits(r)
    except redis.RedisError:
        return {}
    return {
        rail: {
            "limit_per_second": limit,
            "actual_per_second": round(finished.get(rail, 0) / _SPEED_WINDOW_SECONDS, 1),
        }
        for rail, limit in in_force.items()
    }


def _rail_state() -> dict:
    try:
        return {"reachable": True, "chaos": rail_client.get_chaos()}
    except httpx.HTTPError:
        return {"reachable": False, "chaos": None}


@router.post("/demo/payments")
def send_demo_payments(
    count: int = Query(default=10, ge=1, le=200),
    session: Session = Depends(get_session),
):
    """Submit `count` random test payments (about 1 in 10 to a closed account)."""
    for _ in range(count):
        payment, _created = submit_payment(session, _random_payment())
        work_queue.enqueue(connections.redis_client, str(payment.uetr))
    return {"submitted": count}


def _random_payment() -> PaymentRequest:
    rail = random.choice(["FPS", "FPS", "SEPA"])
    creditor_iban = _CREDITOR_IBAN[rail]
    if rail == "FPS" and random.random() < 0.1:
        creditor_iban = _CLOSED_ACCOUNT
    return PaymentRequest(
        uetr=uuid.uuid4(),
        rail=rail,
        amount=Decimal(random.randint(100, 500_000)) / 100,
        currency="GBP" if rail == "FPS" else "EUR",
        debtor_name=random.choice(_DEBTORS),
        debtor_iban=_DEBTOR_IBAN,
        creditor_name=random.choice(_CREDITORS),
        creditor_iban=creditor_iban,
        remittance_info=f"Demo {random.randint(1000, 9999)}",
    )


class RateLimitChange(BaseModel):
    rail: str
    per_second: float = Field(gt=0, le=1000)


@router.put("/demo/rate-limits")
def set_rate_limit(change: RateLimitChange):
    """Change a rail's rate limit for every worker at once."""
    try:
        rate_limit.set_limit(connections.redis_client, change.rail, change.per_second)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return rate_limit.limits(connections.redis_client)


@router.put("/demo/chaos")
def set_rail_chaos(settings: dict):
    """Change how the rail simulator behaves (healthy, flaky, down, ...)."""
    try:
        return rail_client.set_chaos(settings)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=422, detail=exc.response.json()) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="rail simulator unreachable") from exc
