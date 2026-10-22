"""A live dashboard for watching payments move, plus the demo controls behind
its buttons. Open http://127.0.0.1:8000/dashboard while the rail simulator and
at least one worker are running.

The /demo endpoints generate test payments and change the rail simulator's
behaviour. They are for local development and demos only.
"""

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import redis
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from settlement import connections, demo_story, rail_client, rate_limit, reconciliation, work_queue
from settlement.connections import get_session
from settlement.models import Payment
from settlement.demo_data import random_payment
from settlement.payments import submit_payment
from settlement.schemas import PaymentResponse, PaymentStatus

router = APIRouter()

_PAGE = Path(__file__).parent / "static" / "dashboard.html"

# Window for measuring each rail's actual speed.
_SPEED_WINDOW_SECONDS = 5
# An in-flight payment unchanged this long, that nothing will pick up, is stuck.
# (Crash recovery normally acts within about 40 seconds.)
STUCK_AFTER_SECONDS = 120
_IN_FLIGHT = (PaymentStatus.RECEIVED, PaymentStatus.PROCESSING, PaymentStatus.RETRYING)


@router.get("/dashboard", include_in_schema=False)
def dashboard_page():
    # no-cache: browsers must check for a newer page each time, so an
    # updated dashboard shows up on reload instead of a stale cached copy.
    return FileResponse(_PAGE, headers={"Cache-Control": "no-cache"})


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


@router.get("/scoreboard")
def scoreboard(session: Session = Depends(get_session)):
    """The system's promises, checked live.

    lost:       payments where we and the network disagree (a live
                reconciliation of today), leaving out payments still in
                flight here: a worker may be about to record the answer,
                or a retry will ask the network again
    sent_twice: payments the network settled more than once, from the
                network's own records
    stuck:      in-flight payments unchanged for STUCK_AFTER_SECONDS that
                nothing will pick up (no live claim, not waiting in Redis)
    None means it can't be checked right now (network or Redis unreachable).
    """
    needs_person = session.scalar(
        select(func.count()).where(Payment.status == PaymentStatus.IN_REPAIR)
    )
    result = {
        "lost": None, "sent_twice": None, "resends_absorbed": None,
        "stuck": _stuck(session, connections.redis_client),
        "needs_person": needs_person,
        "stuck_after_seconds": STUCK_AFTER_SECONDS,
    }
    try:
        ledger = rail_client.fetch_ledger()
    except httpx.HTTPError:
        return result
    settled = [e["uetr"] for e in ledger if e["status"] == PaymentStatus.SETTLED]
    report = reconciliation.run_for_day(session, datetime.now(UTC).date(), network_ledger=ledger)
    lost = [b for b in report["breaks"] if b["our_status"] not in _IN_FLIGHT]
    return result | {
        "lost": len(lost),
        "sent_twice": len(settled) - len(set(settled)),
        "resends_absorbed": sum(max(0, e.get("received_count", 1) - 1) for e in ledger),
    }


def _stuck(session: Session, r: redis.Redis) -> int | None:
    candidates = session.scalars(
        select(Payment.uetr)
        .where(
            Payment.status.in_(_IN_FLIGHT),
            Payment.updated_at < func.now() - timedelta(seconds=STUCK_AFTER_SECONDS),
        )
        .limit(500)
    ).all()
    if not candidates:
        return 0
    try:
        seconds, micros = r.time()
        now_ms = seconds * 1000 + micros // 1000
        stuck = 0
        for uetr in map(str, candidates):
            lease_deadline = r.zscore(work_queue.LEASES, uetr)
            if lease_deadline is not None:
                stuck += lease_deadline < now_ms  # its worker is gone
            elif not work_queue.is_queued(r, uetr):
                stuck += 1  # nothing knows about it
        return stuck
    except redis.RedisError:
        return None


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
        booked = list(r.smembers(rate_limit.BOOKED))
    except redis.RedisError:
        return {}
    # Payments parked until their booked rate-limit slot, per rail.
    waiting = dict(
        session.execute(
            select(Payment.rail, func.count())
            .where(Payment.uetr.in_([uuid.UUID(u) for u in booked]))
            .group_by(Payment.rail)
        ).all()
    ) if booked else {}
    return {
        rail: {
            "limit_per_second": limit,
            "actual_per_second": round(finished.get(rail, 0) / _SPEED_WINDOW_SECONDS, 1),
            "waiting_for_slot": waiting.get(rail, 0),
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
        payment, _created = submit_payment(session, random_payment(), actor="dashboard demo")
        work_queue.enqueue(connections.redis_client, str(payment.uetr))
    return {"submitted": count}


# The one-click demo (see settlement/demo_story.py). One at a time.
story = demo_story.DemoStory()


@router.post("/demo/run", status_code=202)
def run_demo():
    """Start the ~90-second demo story. 409 if it is already running."""
    if not story.start():
        raise HTTPException(status_code=409, detail="the demo is already running")
    return story.status()


@router.get("/demo/run")
def demo_status():
    """Where the demo story is: its acts, what it has narrated, and the result."""
    return story.status()


@router.post("/demo/run/stop")
def stop_demo():
    story.stop()
    return story.status()


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
