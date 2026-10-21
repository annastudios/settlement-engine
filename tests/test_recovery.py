import threading
import uuid

import pytest
from sqlalchemy import update

from settlement import config, connections, recovery, work_queue, worker
from settlement.connections import SessionLocal
from settlement.connections import redis_client as r
from settlement.models import Payment
from settlement.rail_client import send_to_rail
from tests.factories import dead_redis, fps_payment, payment_status


class WorkerCrashed(Exception):
    """Stands in for a worker process dying partway through."""


def crash_before_sending(payment):
    raise WorkerCrashed


def crash_after_sending(payment):
    send_to_rail(payment)  # the money moves...
    raise WorkerCrashed  # ...but the worker dies before recording it


@pytest.fixture
def expired_leases(monkeypatch):
    """Leases expire the moment they are taken, so a stopped worker can be
    simulated without waiting LEASE_SECONDS."""
    monkeypatch.setattr(config, "LEASE_SECONDS", 0)


@pytest.fixture
def orphans_immediately(monkeypatch):
    monkeypatch.setattr(config, "ORPHAN_AFTER_SECONDS", 0)


def submit(client):
    payment = fps_payment()
    assert client.post("/payments", json=payment).status_code == 201
    return payment["uetr"]


def submit_while_redis_is_down(client, monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(connections, "redis_client", dead_redis())
        return submit(client)


def worker_dies(uetr_send):
    with pytest.raises(WorkerCrashed):
        worker.process_one(r, "worker-that-dies", send=uetr_send)


# --- 1. the worker stopped while holding a payment -------------------------


def test_payment_of_a_stopped_worker_is_retried(client, expired_leases):
    uetr = submit(client)
    worker_dies(crash_before_sending)

    assert recovery.recover(r) == 1

    body = client.get(f"/payments/{uetr}").json()
    assert body["status"] == "retrying"
    assert "lease expired" in body["last_error"]
    assert r.zscore(work_queue.LEASES, uetr) is None
    assert r.lrange(work_queue.READY, 0, -1) == [uetr]

    worker.process_one(r, "healthy-worker")
    body = client.get(f"/payments/{uetr}").json()
    assert body["status"] == "settled"
    assert body["attempts"] == 2  # the crashed attempt counts


def test_send_that_went_through_before_the_crash_is_not_paid_twice(
    client, rail, expired_leases
):
    uetr = submit(client)
    worker_dies(crash_after_sending)

    recovery.recover(r)
    worker.process_one(r, "healthy-worker")

    assert payment_status(uetr) == "settled"
    assert rail.get(f"/payments/{uetr}").json()["received_count"] == 2  # moved once


def test_payment_finished_before_the_crash_is_not_resent(client, expired_leases):
    # The worker recorded "settled" in Postgres, then died before releasing
    # its lease. Recovery must clean up the lease and NOT send it again.
    uetr = submit(client)
    work_queue.claim(r, "worker-that-dies")
    with SessionLocal() as session:
        session.execute(
            update(Payment).where(Payment.uetr == uuid.UUID(uetr)).values(status="settled")
        )
        session.commit()

    assert recovery.recover(r) == 0

    assert r.zscore(work_queue.LEASES, uetr) is None
    assert r.llen(work_queue.READY) == 0
    assert payment_status(uetr) == "settled"


def test_live_lease_is_left_alone(client):
    # Lease not expired yet: the worker may still be working on it.
    uetr = submit(client)
    worker_dies(crash_before_sending)

    assert recovery.recover(r) == 0

    assert payment_status(uetr) == "processing"
    assert r.hget(work_queue.LEASE_OWNERS, uetr) == "worker-that-dies"


def test_payment_that_keeps_crashing_the_worker_ends_in_repair(
    client, expired_leases, monkeypatch
):
    # A "poison" payment must not loop forever: each crash uses an attempt.
    monkeypatch.setattr(config, "MAX_ATTEMPTS", 2)
    uetr = submit(client)

    worker_dies(crash_before_sending)
    recovery.recover(r)
    worker_dies(crash_before_sending)
    recovery.recover(r)

    body = client.get(f"/payments/{uetr}").json()
    assert body["status"] == "in_repair"
    assert body["attempts"] == 2
    assert "lease expired" in body["last_error"]
    assert r.llen(work_queue.READY) == 0


def test_expired_lease_goes_to_exactly_one_recovering_worker(client, expired_leases):
    submit(client)
    worker_dies(crash_before_sending)
    start_together = threading.Barrier(5)
    results = []

    def recover():
        start_together.wait()
        results.append(recovery.recover(r))

    threads = [threading.Thread(target=recover) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(results) == [0, 0, 0, 0, 1]
    assert r.llen(work_queue.READY) == 1


# --- 2. Redis lost track of a payment ---------------------------------------


def test_payment_never_queued_is_recovered_from_postgres(
    client, monkeypatch, orphans_immediately
):
    uetr = submit_while_redis_is_down(client, monkeypatch)
    assert payment_status(uetr) == "received"

    assert recovery.recover(r) == 1

    worker.process_one(r, "worker-a")
    assert payment_status(uetr) == "settled"


def test_recently_submitted_payment_is_not_treated_as_lost(client, monkeypatch):
    # Default ORPHAN_AFTER_SECONDS (60s): give the system time before acting.
    submit_while_redis_is_down(client, monkeypatch)

    assert recovery.recover(r) == 0
    assert r.llen(work_queue.READY) == 0


def test_payment_already_in_the_queue_is_not_queued_again(client, orphans_immediately):
    submit(client)

    assert recovery.recover(r) == 0
    assert r.llen(work_queue.READY) == 1


def test_whole_queue_is_rebuilt_from_postgres_after_redis_is_wiped(
    client, rail, orphans_immediately
):
    uetrs = [submit(client) for _ in range(5)]
    r.flushdb()  # Redis loses everything

    assert recovery.recover(r) == 5
    while worker.process_one(r, "worker-a") is not None:
        pass

    assert all(payment_status(u) == "settled" for u in uetrs)
    assert all(rail.get(f"/payments/{u}").json()["received_count"] == 1 for u in uetrs)


def test_processing_payment_whose_lease_vanished_is_recovered(client, orphans_immediately):
    uetr = submit(client)
    worker_dies(crash_before_sending)  # 'processing', with a live lease...
    r.flushdb()  # ...which Redis then loses

    assert recovery.recover(r) == 1
    assert payment_status(uetr) == "retrying"
    assert r.lrange(work_queue.READY, 0, -1) == [uetr]


# --- 3. workers run recovery on their own -----------------------------------


def test_worker_loop_runs_recovery(client, monkeypatch, orphans_immediately):
    uetr = submit_while_redis_is_down(client, monkeypatch)
    monkeypatch.setattr(config, "RECOVERY_INTERVAL_SECONDS", 0.05)
    stop = threading.Event()
    thread = threading.Thread(
        target=worker.run, kwargs={"worker_id": "loop", "poll_seconds": 0.05, "stop": stop}
    )

    thread.start()
    try:
        for _ in range(100):
            if payment_status(uetr) == "settled":
                break
            stop.wait(0.05)
    finally:
        stop.set()
        thread.join(timeout=5)

    assert payment_status(uetr) == "settled"
