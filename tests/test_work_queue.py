import threading
import time
from concurrent.futures import ThreadPoolExecutor

from settlement import work_queue
from settlement.connections import redis_client as r


def test_claim_from_empty_queue_returns_none():
    assert work_queue.claim(r, "worker-a") is None


def test_queue_is_first_in_first_out():
    for uetr in ["first", "second", "third"]:
        work_queue.enqueue(r, uetr)

    claimed = [work_queue.claim(r, "worker-a") for _ in range(3)]

    assert claimed == ["first", "second", "third"]


def test_claim_records_lease_deadline_and_owner():
    work_queue.enqueue(r, "p1")
    before_ms = time.time() * 1000

    work_queue.claim(r, "worker-a", lease_seconds=30)

    assert r.llen(work_queue.READY) == 0
    deadline_ms = r.zscore(work_queue.LEASES, "p1")
    # About 30s from now (Redis's clock; allow a little slack).
    assert before_ms + 29_000 < deadline_ms < before_ms + 31_000
    assert r.hget(work_queue.LEASE_OWNERS, "p1") == "worker-a"


def test_ack_by_owner_releases_lease():
    work_queue.enqueue(r, "p1")
    work_queue.claim(r, "worker-a")

    assert work_queue.ack(r, "p1", "worker-a") is True

    assert r.zscore(work_queue.LEASES, "p1") is None
    assert r.hget(work_queue.LEASE_OWNERS, "p1") is None


def test_ack_by_another_worker_is_refused():
    work_queue.enqueue(r, "p1")
    work_queue.claim(r, "worker-a")

    assert work_queue.ack(r, "p1", "worker-b") is False

    # worker-a's lease is untouched.
    assert r.zscore(work_queue.LEASES, "p1") is not None
    assert r.hget(work_queue.LEASE_OWNERS, "p1") == "worker-a"


def test_concurrent_workers_never_claim_the_same_payment():
    uetrs = [f"p{i}" for i in range(300)]
    for uetr in uetrs:
        work_queue.enqueue(r, uetr)
    start_together = threading.Barrier(8)

    def drain(worker_number):
        worker_id = f"worker-{worker_number}"
        start_together.wait()
        mine = []
        while (uetr := work_queue.claim(r, worker_id)) is not None:
            mine.append(uetr)
        return mine

    with ThreadPoolExecutor(max_workers=8) as pool:
        per_worker = list(pool.map(drain, range(8)))

    all_claimed = [uetr for mine in per_worker for uetr in mine]
    assert len(all_claimed) == len(set(all_claimed)), "a payment was claimed twice"
    assert sorted(all_claimed) == sorted(uetrs), "a payment was never claimed"
    assert r.zcard(work_queue.LEASES) == 300
