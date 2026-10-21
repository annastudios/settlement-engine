import threading

import pytest

from settlement import config, work_queue, worker
from settlement.connections import redis_client as r
from tests.factories import fps_payment, payment_status


def submit(client, **overrides):
    payment = fps_payment(**overrides)
    assert client.post("/payments", json=payment).status_code == 201
    return payment["uetr"]


@pytest.fixture
def instant_retries(monkeypatch):
    """Retries become due immediately, so tests don't wait for backoff."""
    monkeypatch.setattr(config, "RETRY_BASE_SECONDS", 0)


def drain(worker_id="worker-a"):
    """Process until nothing is ready (retries included, if they are due)."""
    while worker.process_one(r, worker_id) is not None:
        pass


def test_worker_settles_a_submitted_payment(client, rail):
    uetr = submit(client)

    processed = worker.process_one(r, "worker-a")

    assert processed == uetr
    body = client.get(f"/payments/{uetr}").json()
    assert body["status"] == "settled"
    assert body["attempts"] == 1
    assert r.zscore(work_queue.LEASES, uetr) is None  # lease released
    assert rail.get(f"/payments/{uetr}").json()["status"] == "settled"


def test_rejected_payment_is_final_with_its_reason(client):
    uetr = submit(client, creditor_iban="GB56CLOS00000000000001")

    worker.process_one(r, "worker-a")

    body = client.get(f"/payments/{uetr}").json()
    assert body["status"] == "rejected"
    assert body["rejection_reason"] == "AC04"
    # A rejection is a final answer, so the lease is released.
    assert r.zscore(work_queue.LEASES, uetr) is None


def test_empty_queue_returns_none():
    assert worker.process_one(r, "worker-a") is None


def test_payment_queued_twice_is_sent_to_the_rail_once(client):
    uetr = submit(client)
    work_queue.enqueue(r, uetr)  # a duplicate job
    sent = []

    worker.process_one(r, "worker-a", send=sent.append)
    worker.process_one(r, "worker-b", send=sent.append)

    assert len(sent) == 1
    assert payment_status(uetr) == "settled"


def test_rail_outage_schedules_a_retry_with_backoff(client, rail):
    uetr = submit(client)
    rail.put("/chaos", json={"failure_rate": 1})

    worker.process_one(r, "worker-a")

    body = client.get(f"/payments/{uetr}").json()
    assert body["status"] == "retrying"
    assert body["attempts"] == 1
    assert "503" in body["last_error"]
    # Lease released; parked in the delayed set instead. With the default
    # 1s base, the first retry is due in 0.5-1s.
    assert r.zscore(work_queue.LEASES, uetr) is None
    assert r.zscore(work_queue.DELAYED, uetr) is not None
    assert worker.process_one(r, "worker-a") is None  # not due yet


def test_retry_settles_once_the_rail_recovers(client, rail, instant_retries):
    uetr = submit(client)
    rail.put("/chaos", json={"failure_rate": 1})
    worker.process_one(r, "worker-a")  # attempt 1 fails

    rail.put("/chaos", json={})
    worker.process_one(r, "worker-a")  # attempt 2

    body = client.get(f"/payments/{uetr}").json()
    assert body["status"] == "settled"
    assert body["attempts"] == 2
    assert body["last_error"] is None


def test_lost_answer_is_resolved_by_the_retry_without_paying_twice(
    client, rail, instant_retries
):
    # Attempt 1 settles at the rail but the answer is lost. Attempt 2 resends
    # the same UETR; the rail dedupes and simply confirms "settled".
    uetr = submit(client)
    rail.put("/chaos", json={"drop_response_rate": 1, "hang_seconds": 0})
    worker.process_one(r, "worker-a")
    assert payment_status(uetr) == "retrying"

    rail.put("/chaos", json={})
    worker.process_one(r, "worker-a")

    assert payment_status(uetr) == "settled"
    ledger = rail.get(f"/payments/{uetr}").json()
    assert ledger["received_count"] == 2  # arrived twice, money moved once


def test_payment_goes_to_repair_queue_after_max_attempts(
    client, rail, instant_retries, monkeypatch
):
    monkeypatch.setattr(config, "MAX_ATTEMPTS", 3)
    uetr = submit(client)
    rail.put("/chaos", json={"failure_rate": 1})

    drain()

    body = client.get(f"/payments/{uetr}").json()
    assert body["status"] == "in_repair"
    assert body["attempts"] == 3
    assert "503" in body["last_error"]
    # Nothing left in Redis: it waits for a person now.
    assert r.zscore(work_queue.LEASES, uetr) is None
    assert r.zscore(work_queue.DELAYED, uetr) is None
    assert r.llen(work_queue.READY) == 0


def test_unexpected_error_keeps_the_lease_so_the_payment_is_not_lost(client):
    # A bug is not a rail outage: no retry is scheduled, the lease stays, and
    # recovery of expired leases (step 5) deals with it.
    uetr = submit(client)

    def buggy_send(payment):
        raise RuntimeError("bug")

    with pytest.raises(RuntimeError):
        worker.process_one(r, "worker-a", send=buggy_send)

    assert payment_status(uetr) == "processing"
    assert r.hget(work_queue.LEASE_OWNERS, uetr) == "worker-a"


def test_run_loop_processes_payments_until_stopped(client):
    uetrs = [submit(client) for _ in range(5)]
    stop = threading.Event()
    thread = threading.Thread(
        target=worker.run, kwargs={"worker_id": "loop", "poll_seconds": 0.05, "stop": stop}
    )

    thread.start()
    try:
        for _ in range(100):
            if all(payment_status(u) == "settled" for u in uetrs):
                break
            stop.wait(0.05)
    finally:
        stop.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert all(payment_status(u) == "settled" for u in uetrs)
