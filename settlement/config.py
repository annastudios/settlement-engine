import os

from dotenv import load_dotenv

load_dotenv()

# Defaults match docker-compose.yml, so local dev works without a .env file.
# 127.0.0.1, not localhost: on Windows "localhost" tries IPv6 first, which
# Docker does not listen on, so every connection waits for a timeout.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://settlement:settlement-local-dev@127.0.0.1:5432/settlement",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
RAIL_URL = os.getenv("RAIL_URL", "http://127.0.0.1:9000")
# How long to wait for the rail before treating the outcome as unknown.
RAIL_TIMEOUT_SECONDS = float(os.getenv("RAIL_TIMEOUT_SECONDS", "2"))
