import httpx
import pytest

from settlement import connections, rail_client, work_queue, worker
from settlement.connections import redis_client as r
from tests.factories import count_payments, dead_redis, fps_payment


def test_dashboard_page_is_served(client):
    response = client.get("/dashboard")

    assert response.status_code == 200
    assert "Settlement Engine" in response.text


def test_stats_on_an_empty_system(client):
    body = client.get("/stats").json()

    assert body["counts"] == {
        "received": 0, "processing": 0, "retrying": 0,
        "settled": 0, "rejected": 0, "in_repair": 0,
    }
    assert body["queue"] == {"ready": 0, "in_progress": 0, "waiting_to_retry": 0, "leases": []}
    assert body["rail"]["reachable"] is True
    assert body["recent"] == []
    assert body["repair_queue"] == []


def test_stats_count_payments_by_status(client):
    client.post("/payments", json=fps_payment())
    client.post("/payments", json=fps_payment())
    worker.process_one(r, "worker-a")

    body = client.get("/stats").json()

    assert body["counts"]["settled"] == 1
    assert body["counts"]["received"] == 1
    assert body["queue"]["ready"] == 1
    assert len(body["recent"]) == 2
    assert body["recent"][0]["status"] == "settled"  # most recently changed first


def test_stats_show_who_holds_a_lease(client):
    client.post("/payments", json=fps_payment())
    uetr = work_queue.claim(r, "worker-a", lease_seconds=30)

    lease = client.get("/stats").json()["queue"]["leases"][0]

    assert lease["uetr"] == uetr
    assert lease["worker"] == "worker-a"
    assert 29 <= lease["seconds_left"] <= 30


def test_stats_still_work_when_redis_is_down(client, monkeypatch):
    monkeypatch.setattr(connections, "redis_client", dead_redis())

    body = client.get("/stats").json()

    assert body["queue"] == {"error": "Redis unreachable"}


def test_stats_report_an_unreachable_rail(client, monkeypatch):
    def nobody_there(request):
        raise httpx.ConnectError("refused", request=request)

    fake = httpx.Client(transport=httpx.MockTransport(nobody_there), base_url="http://rail")
    monkeypatch.setattr(rail_client, "_client", fake)

    assert client.get("/stats").json()["rail"] == {"reachable": False, "chaos": None}


def test_demo_payments_are_submitted_and_queued(client):
    response = client.post("/demo/payments?count=20")

    assert response.json() == {"submitted": 20}
    assert count_payments() == 20
    assert r.llen(work_queue.READY) == 20


@pytest.mark.parametrize("count", [0, 201])
def test_demo_batch_size_is_limited(client, count):
    assert client.post(f"/demo/payments?count={count}").status_code == 422


def test_demo_chaos_changes_the_rail(client, rail):
    response = client.put("/demo/chaos", json={"failure_rate": 1})

    assert response.status_code == 200
    assert rail.get("/chaos").json()["failure_rate"] == 1


def test_demo_chaos_rejects_invalid_settings(client):
    assert client.put("/demo/chaos", json={"failure_rate": 5}).status_code == 422
