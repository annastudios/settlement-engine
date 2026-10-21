import uuid

import pytest

from settlement import config, work_queue, worker
from settlement.connections import redis_client as r
from tests.factories import fps_payment


@pytest.fixture
def payment_in_repair(client, rail, monkeypatch):
    """A payment that used up its attempts while the rail was down."""
    monkeypatch.setattr(config, "RETRY_BASE_SECONDS", 0)
    monkeypatch.setattr(config, "MAX_ATTEMPTS", 2)
    payment = fps_payment()
    client.post("/payments", json=payment)
    rail.put("/chaos", json={"failure_rate": 1})
    while worker.process_one(r, "worker-a") is not None:
        pass
    rail.put("/chaos", json={})
    return payment["uetr"]


def test_repair_queue_lists_payments_in_repair(client, payment_in_repair):
    client.post("/payments", json=fps_payment())  # a healthy one, not listed

    response = client.get("/repair-queue")

    assert response.status_code == 200
    assert [p["uetr"] for p in response.json()] == [payment_in_repair]
    assert response.json()[0]["status"] == "in_repair"


def test_empty_repair_queue(client):
    assert client.get("/repair-queue").json() == []


def test_resubmit_gives_a_fresh_set_of_attempts(client, payment_in_repair):
    response = client.post(f"/payments/{payment_in_repair}/resubmit")

    assert response.status_code == 200
    assert response.json()["status"] == "retrying"
    assert response.json()["attempts"] == 0
    assert r.lrange(work_queue.READY, 0, -1) == [payment_in_repair]


def test_resubmitted_payment_settles(client, payment_in_repair):
    client.post(f"/payments/{payment_in_repair}/resubmit")

    worker.process_one(r, "worker-a")

    body = client.get(f"/payments/{payment_in_repair}").json()
    assert body["status"] == "settled"
    assert client.get("/repair-queue").json() == []


def test_resubmitting_a_payment_not_in_repair_is_409(client):
    payment = fps_payment()
    client.post("/payments", json=payment)

    response = client.post(f"/payments/{payment['uetr']}/resubmit")

    assert response.status_code == 409
    assert "received" in response.json()["detail"]


def test_resubmitting_an_unknown_payment_is_404(client):
    assert client.post(f"/payments/{uuid.uuid4()}/resubmit").status_code == 404
