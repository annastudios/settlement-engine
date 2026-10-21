import threading

import pytest

from settlement import work_queue, worker
from settlement.connections import redis_client as r
from settlement.rail_client import RailUnavailable
from tests.factories import fps_payment, payment_status


def submit(client, **overrides):
    payment = fps_payment(**overrides)
    assert client.post("/payments", json=payment).status_code == 201
    return payment["uetr"]


def test_worker_settles_a_submitted_payment(client, rail):
    uetr = submit(client)

    processed = worker.process_one(r, "worker-a")

    assert processed == uetr
    assert payment_status(uetr) == "settled"
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


def test_settled_status_is_visible_through_the_api(client):
    uetr = submit(client)
    worker.process_one(r, "worker-a")

    assert client.get(f"/payments/{uetr}").json()["status"] == "settled"


def test_empty_queue_returns_none():
    assert worker.process_one(r, "worker-a") is None


def test_payment_queued_twice_is_sent_to_the_rail_once(client):
    uetr = submit(client)
    work_queue.enqueue(r, uetr)  # a duplicate job, e.g. from a retry
    sent = []

    worker.process_one(r, "worker-a", send=sent.append)
    worker.process_one(r, "worker-b", send=sent.append)

    assert len(sent) == 1
    assert payment_status(uetr) == "settled"


def test_rail_outage_keeps_the_lease_so_the_payment_is_not_lost(client, rail):
    uetr = submit(client)
    rail.put("/chaos", json={"failure_rate": 1})

    with pytest.raises(RailUnavailable):
        worker.process_one(r, "worker-a")

    # Not settled, and still leased, so it can be retried.
    assert payment_status(uetr) == "processing"
    assert r.hget(work_queue.LEASE_OWNERS, uetr) == "worker-a"


def test_lost_answer_leaves_payment_processing_although_the_rail_settled(client, rail):
    # We must not mark it settled (we don't know that) nor rejected (it
    # wasn't). It stays processing until a retry with the same UETR asks
    # the rail again.
    uetr = submit(client)
    rail.put("/chaos", json={"drop_response_rate": 1, "hang_seconds": 0})

    with pytest.raises(RailUnavailable):
        worker.process_one(r, "worker-a")

    assert payment_status(uetr) == "processing"
    assert rail.get(f"/payments/{uetr}").json()["status"] == "settled"


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
