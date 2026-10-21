"""The work queue in Redis: which payments are waiting and who is working on what.

Keys:
  payments:ready         LIST  UETRs waiting to be processed (LPUSH in, RPOP out: FIFO)
  payments:leases        ZSET  UETR -> lease deadline (ms since epoch) while a worker has it
  payments:lease_owners  HASH  UETR -> id of the worker holding the lease

Redis only decides who works on a payment. Postgres stays the source of
truth, so losing these keys loses no money, only the order of work.
"""

import redis

READY = "payments:ready"
LEASES = "payments:leases"
LEASE_OWNERS = "payments:lease_owners"

# How long a worker may hold a payment before it counts as abandoned
# (a crashed worker never finishes). Recovering abandoned payments is step 5.
LEASE_SECONDS = 30

# Take the next payment AND record the lease in one atomic step, so two
# workers can never claim the same payment. The deadline uses Redis's own
# clock, so workers with wrong clocks can't give themselves longer leases.
_CLAIM = """
local uetr = redis.call('RPOP', KEYS[1])
if not uetr then
  return nil
end
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call('ZADD', KEYS[2], now_ms + tonumber(ARGV[1]) * 1000, uetr)
redis.call('HSET', KEYS[3], uetr, ARGV[2])
return uetr
"""

# Release the lease, but only if this worker still holds it. If the lease
# expired and another worker took over, the slow worker must not remove
# the new worker's lease.
_ACK = """
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""


def enqueue(r: redis.Redis, uetr: str) -> None:
    r.lpush(READY, uetr)


def claim(r: redis.Redis, worker_id: str, lease_seconds: int = LEASE_SECONDS) -> str | None:
    """Take the next payment, or None if the queue is empty."""
    return r.register_script(_CLAIM)(
        keys=[READY, LEASES, LEASE_OWNERS], args=[lease_seconds, worker_id]
    )


def ack(r: redis.Redis, uetr: str, worker_id: str) -> bool:
    """Mark this worker's payment as finished. False if it no longer holds the lease."""
    return r.register_script(_ACK)(keys=[LEASES, LEASE_OWNERS], args=[uetr, worker_id]) == 1
