from settlement import connections
from tests.factories import dead_redis


def test_health_ok_when_postgres_and_redis_are_up(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {"postgres": "ok", "redis": "ok"},
    }


def test_health_degraded_when_redis_is_down(client, monkeypatch):
    monkeypatch.setattr(connections, "redis_client", dead_redis())

    response = client.get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded"
    assert body["checks"]["postgres"] == "ok"
    assert body["checks"]["redis"].startswith("error")
