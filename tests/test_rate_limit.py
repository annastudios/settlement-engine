import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from settlement import config, rate_limit, work_queue, worker
from settlement.connections import redis_client as r
from tests.factories import fps_payment, payment_status, sepa_payment


@pytest.fixture
def limits(monkeypatch):
    """Set the configured per-second limits for one test."""

    def set_limits(fps, sepa=100):
        monkeypatch.setattr(config, "RATE_LIMITS", {"FPS": fps, "SEPA": sepa})

    return set_limits


# --- booking slots ------------------------------------------------------------


def test_a_burst_up_to_the_limit_goes_straight_through(limits):
    limits(fps=5)

    waits = [rate_limit.book_slot(r, "FPS") for _ in range(5)]

    assert waits == [0, 0, 0, 0, 0]


def test_over_the_limit_each_payment_gets_the_next_slot(limits):
    limits(fps=5)  # one slot every 200ms
    for _ in range(5):
        rate_limit.book_slot(r, "FPS")

    waits = [rate_limit.book_slot(r, "FPS") for _ in range(3)]

    # About 0.2s, 0.4s, 0.6s: evenly spaced at the rail's pace.
    assert [round(w, 1) for w in waits] == [0.2, 0.4, 0.6]


def test_slots_free_up_as_time_passes(limits):
    limits(fps=10)
    for _ in range(10):
        rate_limit.book_slot(r, "FPS")
    assert rate_limit.book_slot(r, "FPS") > 0

    time.sleep(0.35)

    assert rate_limit.book_slot(r, "FPS") == 0


def test_rails_have_separate_limits(limits):
    limits(fps=1, sepa=1)
    rate_limit.book_slot(r, "FPS")

    assert rate_limit.book_slot(r, "FPS") > 0
    assert rate_limit.book_slot(r, "SEPA") == 0


def test_rail_without_a_limit_is_never_held_back(limits):
    limits(fps=1)

    assert all(rate_limit.book_slot(r, "SWIFT") == 0 for _ in range(10))


def test_concurrent_workers_never_book_the_same_slot(limits):
    limits(fps=10)
    start_together = threading.Barrier(8)

    def book(_):
        start_together.wait()
        return [rate_limit.book_slot(r, "FPS") for _ in range(10)]

    seconds, micros = r.time()
    started_ms = seconds * 1000 + micros / 1000
    with ThreadPoolExecutor(max_workers=8) as pool:
        waits = [w for batch in pool.map(book, range(8)) for w in batch]

    # 80 bookings at 10/s: exactly 10 go now (a one-second burst) and the
    # other 70 wait for later slots.
    assert waits.count(0) == 10
    # Every booking moves the rail's next free slot on by exactly 100ms, in
    # one atomic step. So after 80 bookings it is 8 seconds after the first
    # one; if any two had shared a slot it would fall short. Read from
    # Redis's own clock and records, not timed from here: with 8 threads,
    # timing each booking from the test is too noisy to be reliable.
    next_free_ms = float(r.get("ratelimit:next:FPS"))
    assert 7_950 < next_free_ms - started_ms < 8_300


# --- changing the limit while running ----------------------------------------


def test_limit_can_be_changed_while_running(limits):
    limits(fps=10)

    rate_limit.set_limit(r, "FPS", 2)

    assert rate_limit.limits(r)["FPS"] == 2
    # 2/s: a burst of 2, then the next slot is about half a second away.
    waits = [rate_limit.book_slot(r, "FPS") for _ in range(3)]
    assert [round(w, 1) for w in waits] == [0, 0, 0.5]


@pytest.mark.parametrize("rail, per_second", [("SWIFT", 5), ("FPS", 0), ("FPS", -1)])
def test_invalid_limit_changes_are_refused(rail, per_second):
    with pytest.raises(ValueError):
        rate_limit.set_limit(r, rail, per_second)


# --- the worker respects the limit -------------------------------------------


def test_payment_over_the_limit_is_parked_not_failed(client, limits):
    limits(fps=1)
    first, second = fps_payment(), fps_payment()
    client.post("/payments", json=first)
    client.post("/payments", json=second)

    worker.process_one(r, "worker-a")  # uses the one slot available now
    worker.process_one(r, "worker-a")  # must wait about a second

    assert payment_status(first["uetr"]) == "settled"
    body = client.get(f"/payments/{second['uetr']}").json()
    assert body["status"] == "received"  # untouched...
    assert body["attempts"] == 0  # ...and not counted as an attempt
    assert r.zscore(work_queue.DELAYED, second["uetr"]) is not None
    assert r.sismember(rate_limit.BOOKED, second["uetr"])


def test_parked_payment_is_sent_in_its_slot_without_booking_again(client, limits):
    limits(fps=1)
    first, second = fps_payment(), fps_payment()
    client.post("/payments", json=first)
    client.post("/payments", json=second)
    worker.process_one(r, "worker-a")
    worker.process_one(r, "worker-a")  # parked until its slot
    next_slot_before = r.get("ratelimit:next:FPS")

    time.sleep(1.1)
    worker.process_one(r, "worker-a")

    assert payment_status(second["uetr"]) == "settled"
    assert r.get("ratelimit:next:FPS") == next_slot_before  # no second booking
    assert not r.sismember(rate_limit.BOOKED, second["uetr"])


def test_busy_rail_does_not_hold_up_the_other_rail(client, limits):
    limits(fps=1, sepa=100)
    for _ in range(5):
        client.post("/payments", json=fps_payment())
    sepa = sepa_payment()
    client.post("/payments", json=sepa)

    for _ in range(6):
        worker.process_one(r, "worker-a")

    # The FPS backlog is parked; the SEPA payment went straight through.
    assert payment_status(sepa["uetr"]) == "settled"


def test_several_workers_together_stay_within_the_limit(client, limits):
    # 40 payments at 20/s with 4 workers: the first 20 go as a burst, the
    # other 20 at 20/s, so it takes about a second. Without the limit, 4
    # workers would finish in well under half a second.
    limits(fps=20)
    uetrs = []
    for _ in range(40):
        payment = fps_payment()
        client.post("/payments", json=payment)
        uetrs.append(payment["uetr"])
    stop = threading.Event()
    threads = [
        threading.Thread(
            target=worker.run,
            kwargs={"worker_id": f"w{n}", "poll_seconds": 0.01, "stop": stop},
        )
        for n in range(4)
    ]

    start = time.monotonic()
    for thread in threads:
        thread.start()
    try:
        while not all(payment_status(u) == "settled" for u in uetrs):
            assert time.monotonic() - start < 10, "payments did not finish"
            time.sleep(0.05)
        elapsed = time.monotonic() - start
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=5)

    assert elapsed >= 0.9
