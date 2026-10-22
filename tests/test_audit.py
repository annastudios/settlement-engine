import threading
import uuid

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from settlement import config, recovery, worker
from settlement.connections import SessionLocal, engine
from settlement.connections import redis_client as r
from settlement.models import Payment, PaymentEvent
from tests.factories import fps_payment


def submit(client, **overrides):
    payment = fps_payment(**overrides)
    assert client.post("/payments", json=payment).status_code == 201
    return payment["uetr"]


def history(client, uetr):
    response = client.get(f"/payments/{uetr}/history")
    assert response.status_code == 200
    return response.json()


def steps(client, uetr):
    """The trail as (from, to) pairs, the easiest way to read it in a test."""
    return [(e["from_status"], e["to_status"]) for e in history(client, uetr)]


@pytest.fixture
def instant_retries(monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_SECONDS", 0)


# --- what gets recorded --------------------------------------------------------


def test_submission_is_the_first_entry(client):
    uetr = submit(client)

    [event] = history(client, uetr)

    assert event["from_status"] is None
    assert event["to_status"] == "received"
    assert event["actor"] == "api"
    assert event["reason"] == "submitted"
    assert event["attempt"] == 0


def test_submitting_the_same_payment_again_adds_nothing(client):
    payment = fps_payment()
    client.post("/payments", json=payment)
    client.post("/payments", json=payment)  # 200: already have it

    assert len(history(client, payment["uetr"])) == 1


def test_a_settled_payment_has_its_whole_journey(client):
    uetr = submit(client)

    worker.process_one(r, "worker-a")

    trail = history(client, uetr)
    assert [(e["from_status"], e["to_status"]) for e in trail] == [
        (None, "received"),
        ("received", "processing"),
        ("processing", "settled"),
    ]
    assert [e["actor"] for e in trail[1:]] == ["worker worker-a", "worker worker-a"]
    assert trail[2]["reason"] == "settled by the rail"
    assert [e["attempt"] for e in trail] == [0, 1, 1]
    times = [e["at"] for e in trail]
    assert times == sorted(times)


def test_a_rejection_records_the_reason_code(client):
    uetr = submit(client, creditor_iban="GB56CLOS00000000000001")

    worker.process_one(r, "worker-a")

    assert history(client, uetr)[-1]["reason"] == "rejected by the rail: AC04"


def test_retries_and_the_repair_queue_are_all_recorded(
    client, rail, instant_retries, monkeypatch
):
    monkeypatch.setattr(config, "MAX_ATTEMPTS", 2)
    uetr = submit(client)
    rail.put("/chaos", json={"failure_rate": 1})

    while worker.process_one(r, "worker-a") is not None:
        pass

    trail = history(client, uetr)
    assert [(e["from_status"], e["to_status"]) for e in trail] == [
        (None, "received"),
        ("received", "processing"),
        ("processing", "retrying"),
        ("retrying", "processing"),
        ("processing", "in_repair"),
    ]
    assert "503" in trail[2]["reason"] and "retrying in" in trail[2]["reason"]
    assert trail[4]["reason"].startswith("out of attempts")


def test_resubmitting_is_recorded_as_done_by_ops(client, rail, instant_retries, monkeypatch):
    monkeypatch.setattr(config, "MAX_ATTEMPTS", 1)
    uetr = submit(client)
    rail.put("/chaos", json={"failure_rate": 1})
    worker.process_one(r, "worker-a")  # straight to the repair queue

    client.post(f"/payments/{uetr}/resubmit")

    last = history(client, uetr)[-1]
    assert (last["from_status"], last["to_status"]) == ("in_repair", "retrying")
    assert last["actor"] == "ops"
    assert last["reason"] == "resubmitted from the repair queue"


def test_crash_recovery_is_recorded(client, monkeypatch):
    monkeypatch.setattr(config, "LEASE_SECONDS", 0)  # leases expire at once
    uetr = submit(client)

    def worker_dies(payment):
        raise RuntimeError("worker died")

    with pytest.raises(RuntimeError):
        worker.process_one(r, "worker-a", send=worker_dies)
    recovery.recover(r, actor="recovery by worker worker-b")

    last = history(client, uetr)[-1]
    assert (last["from_status"], last["to_status"]) == ("processing", "retrying")
    assert last["actor"] == "recovery by worker worker-b"
    assert "its worker stopped while holding it" in last["reason"]


def test_putting_a_lost_payment_back_in_the_queue_is_noted(client, monkeypatch):
    monkeypatch.setattr(config, "ORPHAN_AFTER_SECONDS", 0)
    uetr = submit(client)
    r.flushdb()  # Redis forgets it; the status doesn't change

    recovery.recover(r)

    last = history(client, uetr)[-1]
    assert (last["from_status"], last["to_status"]) == ("received", "received")
    assert last["reason"] == "put back in the queue: it was missing from the queue"


# --- the trail and the payments always agree ---------------------------------


def test_after_a_messy_run_every_payment_matches_its_trail(
    client, rail, instant_retries
):
    # 20 payments through a flaky rail with 3 workers at once. Whatever
    # happened to each one, its last audit entry must match its status, and
    # it must have one "processing" entry per attempt.
    uetrs = [submit(client) for _ in range(20)]
    rail.put("/chaos", json={"failure_rate": 0.4})
    stop = threading.Event()
    threads = [
        threading.Thread(
            target=worker.run, kwargs={"worker_id": f"w{n}", "poll_seconds": 0.01, "stop": stop}
        )
        for n in range(3)
    ]
    for thread in threads:
        thread.start()
    try:
        for _ in range(300):
            statuses = [client.get(f"/payments/{u}").json()["status"] for u in uetrs]
            if all(s in ("settled", "in_repair") for s in statuses):
                break
            stop.wait(0.05)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=5)

    for uetr in uetrs:
        payment = client.get(f"/payments/{uetr}").json()
        trail = history(client, uetr)
        assert trail[-1]["to_status"] == payment["status"]
        assert trail[-1]["attempt"] == payment["attempts"]
        sends = [e for e in trail if e["to_status"] == "processing"]
        assert len(sends) == payment["attempts"]


# --- nobody can rewrite history ------------------------------------------------


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE payment_events SET to_status = 'settled'",
        "DELETE FROM payment_events",
    ],
)
def test_the_database_refuses_to_change_or_delete_history(client, statement):
    uetr = submit(client)

    with pytest.raises(DBAPIError, match="append-only"):
        with engine.begin() as conn:
            conn.execute(text(statement))

    assert len(history(client, uetr)) == 1  # untouched


def test_history_of_an_unknown_payment_is_404(client):
    assert client.get(f"/payments/{uuid.uuid4()}/history").status_code == 404


def test_no_status_change_without_an_audit_entry(client):
    # Every payment's latest event agrees with its status, straight from SQL.
    submit(client)
    submit(client)
    worker.process_one(r, "worker-a")

    with SessionLocal() as session:
        latest = (
            select(PaymentEvent.uetr, func.max(PaymentEvent.id).label("id"))
            .group_by(PaymentEvent.uetr)
            .subquery()
        )
        mismatches = session.scalar(
            select(func.count())
            .select_from(Payment)
            .join(latest, latest.c.uetr == Payment.uetr)
            .join(PaymentEvent, PaymentEvent.id == latest.c.id)
            .where(PaymentEvent.to_status != Payment.status)
        )
    assert mismatches == 0
