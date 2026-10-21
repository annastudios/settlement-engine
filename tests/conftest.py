import os

# Tests get their own database and Redis DB number, so they never wipe dev
# data. This must run before settlement.config is imported.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://settlement:settlement-local-dev@127.0.0.1:5432/settlement_test",
)
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/1")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine, make_url, text  # noqa: E402

from settlement.connections import engine  # noqa: E402
from settlement.main import app  # noqa: E402
from settlement.models import Base  # noqa: E402

# Tests use the real Postgres and Redis (docker compose locally, service
# containers in CI). Later steps rely on Redis Lua scripts and Postgres
# constraints, which fakes would not test faithfully.


def _ensure_test_database():
    url = make_url(os.environ["DATABASE_URL"])
    admin = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        exists = conn.scalar(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": url.database},
        )
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{url.database}"'))
    admin.dispose()


@pytest.fixture(scope="session", autouse=True)
def _schema():
    # Rebuilt every run, so schema changes always apply to tests.
    _ensure_test_database()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


@pytest.fixture(autouse=True)
def _empty_tables():
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE payments"))


@pytest.fixture
def client():
    return TestClient(app)
