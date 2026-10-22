import httpx
import pytest

from settlement import config, connections, dashboard, rail_client, rate_limit, worker
from settlement.connections import redis_client as r
from tests.factories import dead_redis, fps_payment


def drain():
    while worker.process_one(r, "worker-a") is not None:
        pass


def test_all_clear_on_a_normal_day(client):
    client.post("/payments", json=fps_payment())
    client.post("/payments", json=fps_payment(creditor_iban="GB56CLOS00000000000001"))
    drain()

    board = client.get("/scoreboard").json()

    assert board["lost"] == 0
    assert board["sent_twice"] == 0
    assert board["stuck"] == 0
    assert board["needs_person"] == 0


def test_money_moved_but_we_gave_up_counts_as_lost(client, rail, monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_SECONDS", 0)
    monkeypatch.setattr(config, "MAX_ATTEMPTS", 2)
    payment = fps_payment()
    client.post("/payments", json=payment)
    rail.put("/chaos", json={"drop_response_rate": 1, "hang_seconds": 0})
    drain()  # the network settled it twice over, but we put it in repair

    board = client.get("/scoreboard").json()

    assert board["lost"] == 1
    assert board["needs_person"] == 1
    assert board["sent_twice"] == 0  # received twice, settled once
    assert board["resends_absorbed"] == 1

    rail.put("/chaos", json={})
    client.post(f"/payments/{payment['uetr']}/resubmit")
    drain()
    assert client.get("/scoreboard").json()["lost"] == 0


def test_a_payment_still_in_flight_is_not_lost(client, rail):
    # A lost answer that a retry will fix by itself isn't counted.
    client.post("/payments", json=fps_payment())
    rail.put("/chaos", json={"drop_response_rate": 1, "hang_seconds": 0})
    worker.process_one(r, "worker-a")  # now 'retrying'; the network settled it

    assert client.get("/scoreboard").json()["lost"] == 0


def test_a_payment_nothing_will_pick_up_is_stuck(client, monkeypatch):
    monkeypatch.setattr(dashboard, "STUCK_AFTER_SECONDS", 0)
    client.post("/payments", json=fps_payment())
    r.flushdb()  # Redis forgets it, and no recovery has run yet

    assert client.get("/scoreboard").json()["stuck"] == 1


def test_a_payment_waiting_in_the_queue_is_not_stuck(client, monkeypatch):
    monkeypatch.setattr(dashboard, "STUCK_AFTER_SECONDS", 0)
    client.post("/payments", json=fps_payment())

    assert client.get("/scoreboard").json()["stuck"] == 0


def test_a_payment_held_by_a_dead_worker_is_stuck(client, monkeypatch):
    monkeypatch.setattr(dashboard, "STUCK_AFTER_SECONDS", 0)
    monkeypatch.setattr(config, "LEASE_SECONDS", 0)  # its lease has already run out
    client.post("/payments", json=fps_payment())

    def worker_dies(payment):
        raise RuntimeError("worker died")

    with pytest.raises(RuntimeError):
        worker.process_one(r, "worker-a", send=worker_dies)

    assert client.get("/scoreboard").json()["stuck"] == 1


def test_unknown_when_the_network_or_redis_cannot_be_reached(client, monkeypatch):
    def nobody_there(request):
        raise httpx.ConnectError("refused", request=request)

    fake = httpx.Client(transport=httpx.MockTransport(nobody_there), base_url="http://rail")
    monkeypatch.setattr(rail_client, "_client", fake)
    monkeypatch.setattr(dashboard, "STUCK_AFTER_SECONDS", 0)
    client.post("/payments", json=fps_payment())
    monkeypatch.setattr(connections, "redis_client", dead_redis())

    board = client.get("/scoreboard").json()

    assert board["lost"] is None
    assert board["sent_twice"] is None
    assert board["stuck"] is None


def test_rate_limits_show_payments_waiting_for_a_slot(client, monkeypatch):
    monkeypatch.setattr(config, "RATE_LIMITS", {"FPS": 1, "SEPA": 100})
    for _ in range(3):
        client.post("/payments", json=fps_payment())
    drain()  # one goes now; two are parked for later slots

    limits = client.get("/stats").json()["rate_limits"]

    assert limits["FPS"]["waiting_for_slot"] == 2
    assert limits["SEPA"]["waiting_for_slot"] == 0
    assert r.scard(rate_limit.BOOKED) == 2
