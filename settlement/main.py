from fastapi import FastAPI, Response
from sqlalchemy import text

from settlement import connections

app = FastAPI(title="settlement-engine")


def _check_postgres() -> str:
    try:
        with connections.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return "ok"
    except Exception as exc:
        return f"error: {type(exc).__name__}"


def _check_redis() -> str:
    try:
        connections.redis_client.ping()
        return "ok"
    except Exception as exc:
        return f"error: {type(exc).__name__}"


@app.get("/health")
def health(response: Response):
    """Reports whether Postgres and Redis are reachable. 503 if either is not."""
    checks = {"postgres": _check_postgres(), "redis": _check_redis()}
    healthy = all(result == "ok" for result in checks.values())
    if not healthy:
        response.status_code = 503
    return {"status": "ok" if healthy else "degraded", "checks": checks}
