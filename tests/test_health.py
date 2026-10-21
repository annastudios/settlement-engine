import redis

from settlement import connections


def test_health_ok_when_postgres_and_redis_are_up(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {"postgres": "ok", "redis": "ok"},
    }


def test_health_degraded_when_redis_is_down(client, monkeypatch):
    # Nothing listens on port 1, so the connection is refused straight away.
    dead_redis = redis.Redis(host="127.0.0.1", port=1, socket_connect_timeout=1)
    monkeypatch.setattr(connections, "redis_client", dead_redis)

    response = client.get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded"
    assert body["checks"]["postgres"] == "ok"
    assert body["checks"]["redis"].startswith("error")
