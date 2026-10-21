import pytest
from fastapi.testclient import TestClient

from settlement.main import app

# Tests use the real Postgres and Redis (docker compose locally, service
# containers in CI). Later steps rely on Redis Lua scripts and Postgres
# constraints, which fakes would not test faithfully.


@pytest.fixture
def client():
    return TestClient(app)
