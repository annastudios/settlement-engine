from prometheus_client.parser import text_string_to_metric_families

from settlement import config, connections, recovery, worker
from settlement.connections import redis_client as r
from tests.factories import dead_redis, fps_payment, sepa_payment


def scrape(client):
    """Read /metrics the way Prometheus does: {(name, labels): value}."""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    values = {}
    for family in text_string_to_metric_families(response.text):
        for sample in family.samples:
            values[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return values


def entered(values, status, rail):
    return values.get(
        ("settlement_payments_entered_total", (("rail", rail), ("status", status))), 0
    )


def test_counts_payments_entering_each_status_by_rail(client):
    client.post("/payments", json=fps_payment())
    client.post("/payments", json=fps_payment(creditor_iban="GB56CLOS00000000000001"))
    client.post("/payments", json=sepa_payment())
    while worker.process_one(r, "worker-a") is not None:
        pass

    values = scrape(client)

    assert entered(values, "received", "FPS") == 2
    assert entered(values, "processing", "FPS") == 2
    assert entered(values, "settled", "FPS") == 1
    assert entered(values, "rejected", "FPS") == 1
    assert entered(values, "settled", "SEPA") == 1


def test_counts_retries_and_the_repair_queue(client, rail, monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_SECONDS", 0)
    monkeypatch.setattr(config, "MAX_ATTEMPTS", 3)
    client.post("/payments", json=fps_payment())
    rail.put("/chaos", json={"failure_rate": 1})
    while worker.process_one(r, "worker-a") is not None:
        pass

    values = scrape(client)

    assert entered(values, "processing", "FPS") == 3  # three sends
    assert entered(values, "retrying", "FPS") == 2  # two of them retried
    assert entered(values, "in_repair", "FPS") == 1


def test_current_status_and_queue_gauges(client):
    client.post("/payments", json=fps_payment())
    client.post("/payments", json=fps_payment())
    worker.process_one(r, "worker-a")

    values = scrape(client)

    assert values[("settlement_payments", (("status", "settled"),))] == 1
    assert values[("settlement_payments", (("status", "received"),))] == 1
    assert values[("settlement_payments", (("status", "in_repair"),))] == 0
    assert values[("settlement_queue_payments", (("state", "ready"),))] == 1
    assert values[("settlement_rate_limit_per_second", (("rail", "FPS"),))] == 10


def test_counts_recoveries(client, monkeypatch):
    monkeypatch.setattr(config, "ORPHAN_AFTER_SECONDS", 0)
    client.post("/payments", json=fps_payment())
    r.flushdb()

    recovery.recover(r)

    assert scrape(client)[("settlement_recoveries_total", ())] == 1


def test_still_works_when_redis_is_down(client, monkeypatch):
    monkeypatch.setattr(connections, "redis_client", dead_redis())

    values = scrape(client)

    assert ("settlement_payments", (("status", "settled"),)) in values
    assert not any(name == "settlement_queue_payments" for name, _ in values)


def test_metrics_are_not_in_the_api_docs(client):
    assert "/metrics" not in client.get("/openapi.json").json()["paths"]
