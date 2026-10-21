import redis
from sqlalchemy import create_engine

from settlement import config

# Postgres holds the money truth; Redis only coordinates work.
# Short timeouts on both so a dead dependency fails fast instead of hanging.
engine = create_engine(
    config.DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 5},
)

redis_client = redis.Redis.from_url(
    config.REDIS_URL,
    socket_connect_timeout=2,
    socket_timeout=2,
    decode_responses=True,
)
