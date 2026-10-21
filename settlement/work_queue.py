"""The work queue in Redis: which payments are waiting and who is working on what.

Keys:
  payments:ready         LIST  UETRs waiting to be processed (LPUSH in, RPOP out: FIFO)
  payments:leases        ZSET  UETR -> lease deadline (ms since epoch) while a worker has it
  payments:lease_owners  HASH  UETR -> id of the worker holding the lease
  payments:delayed       ZSET  UETR -> when it may be retried (ms since epoch)

Redis only decides who works on a payment. Postgres stays the source of
truth, so losing these keys loses no money, only the order of work.
"""

import redis

from settlement import config

READY = "payments:ready"
LEASES = "payments:leases"
LEASE_OWNERS = "payments:lease_owners"
DELAYED = "payments:delayed"

# Shared by the Lua scripts here and in rate_limit.py: the current time in ms
# from Redis's own clock, so workers with wrong clocks can't skew leases,
# retry times or rate limits.
LUA_NOW_MS = """
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
"""

# First move retries whose wait is over back onto the ready list, then take
# the next payment AND record the lease, all in one atomic step: two workers
# can never claim the same payment.
_CLAIM = LUA_NOW_MS + """
local due = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now_ms, 'LIMIT', 0, 100)
for _, due_uetr in ipairs(due) do
  redis.call('ZREM', KEYS[4], due_uetr)
  redis.call('LPUSH', KEYS[1], due_uetr)
end

local uetr = redis.call('RPOP', KEYS[1])
if not uetr then
  return nil
end
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

# Take every lease whose deadline has passed (its worker stopped or stalled)
# and remove it, in one atomic step: if several workers run recovery at the
# same time, each expired lease is handed to exactly one of them.
_RECLAIM_EXPIRED = LUA_NOW_MS + """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms, 'LIMIT', 0, 100)
for _, uetr in ipairs(expired) do
  redis.call('ZREM', KEYS[1], uetr)
  redis.call('HDEL', KEYS[2], uetr)
end
return expired
"""

# Like ack, then park the payment in the delayed set until its retry is due.
_RETRY_LATER = LUA_NOW_MS + """
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], now_ms + tonumber(ARGV[3]), ARGV[1])
return 1
"""


def enqueue(r: redis.Redis, uetr: str) -> None:
    r.lpush(READY, uetr)


def claim(r: redis.Redis, worker_id: str, lease_seconds: float | None = None) -> str | None:
    """Take the next payment, or None if nothing is ready (or due for retry).
    The lease lasts config.LEASE_SECONDS unless lease_seconds is given."""
    if lease_seconds is None:
        lease_seconds = config.LEASE_SECONDS
    return r.register_script(_CLAIM)(
        keys=[READY, LEASES, LEASE_OWNERS, DELAYED], args=[lease_seconds, worker_id]
    )


def ack(r: redis.Redis, uetr: str, worker_id: str) -> bool:
    """Mark this worker's payment as finished. False if it no longer holds the lease."""
    return r.register_script(_ACK)(keys=[LEASES, LEASE_OWNERS], args=[uetr, worker_id]) == 1


def reclaim_expired(r: redis.Redis) -> list[str]:
    """Remove up to 100 expired leases and return their UETRs. The caller
    decides what happens to each payment (see settlement.recovery)."""
    return r.register_script(_RECLAIM_EXPIRED)(keys=[LEASES, LEASE_OWNERS])


def is_queued(r: redis.Redis, uetr: str) -> bool:
    """Whether Redis knows about this payment at all: leased, delayed or ready.

    Not atomic, so it can miss a payment that is moving between keys at
    that instant. Recovery only re-queues payments that have been unchanged
    for a while, and a duplicate in the queue is harmless (the Postgres
    guard skips it), so that is fine. Checking the ready list is O(n).
    """
    return (
        r.zscore(LEASES, uetr) is not None
        or r.zscore(DELAYED, uetr) is not None
        or r.lpos(READY, uetr) is not None
    )


def retry_later(r: redis.Redis, uetr: str, worker_id: str, delay_seconds: float) -> bool:
    """Release this worker's lease and make the payment claimable again after
    delay_seconds. False if this worker no longer holds the lease."""
    return r.register_script(_RETRY_LATER)(
        keys=[LEASES, LEASE_OWNERS, DELAYED],
        args=[uetr, worker_id, int(delay_seconds * 1000)],
    ) == 1
