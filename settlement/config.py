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

# Retries when the rail is unavailable: wait about 1s, 2s, 4s, ... (capped),
# and after MAX_ATTEMPTS sends in total, move the payment to the repair queue.
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))
RETRY_BASE_SECONDS = float(os.getenv("RETRY_BASE_SECONDS", "1"))
RETRY_MAX_DELAY_SECONDS = float(os.getenv("RETRY_MAX_DELAY_SECONDS", "60"))

# Crash recovery. A worker may hold a payment for LEASE_SECONDS; after that
# it counts as stopped and the payment is recovered. Workers run recovery
# every RECOVERY_INTERVAL_SECONDS. A payment that is waiting in Postgres but
# missing from Redis is re-queued once it has been unchanged for
# ORPHAN_AFTER_SECONDS.
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "30"))
RECOVERY_INTERVAL_SECONDS = float(os.getenv("RECOVERY_INTERVAL_SECONDS", "10"))
ORPHAN_AFTER_SECONDS = float(os.getenv("ORPHAN_AFTER_SECONDS", "60"))
