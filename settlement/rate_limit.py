"""Rate limiting per rail, shared by every worker through Redis.

Each rail accepts at most N payments per second from us (config.RATE_LIMITS,
changeable while running). Instead of "try, and if refused try again", every
payment books the next free time slot on its rail (the GCRA algorithm, a
close cousin of the token bucket):

- Within the limit, its slot is now: send straight away.
- Over the limit, its slot is in the future: the worker parks the payment
  in the delayed set until then, and when it comes back it is sent without
  booking again. Parked payments come back exactly at the rail's pace, and
  no worker waits around for a slot (so a busy FPS doesn't hold up SEPA).

Keys:
  ratelimit:next:{rail}  when the rail's next slot after the booked ones starts (ms)
  ratelimit:limits       HASH rail -> per-second limit set while running
  ratelimit:booked       SET  UETRs parked with a slot already booked
"""

import redis

from settlement import config
from settlement.work_queue import LUA_NOW_MS

LIMITS = "ratelimit:limits"
BOOKED = "ratelimit:booked"


def _next_slot_key(rail: str) -> str:
    return f"ratelimit:next:{rail}"


# Book the next slot on a rail and return how many ms until it starts (0 =
# now). One atomic step, so concurrent workers can never book the same slot.
# A burst of up to one second's worth of payments goes through at once.
_BOOK = LUA_NOW_MS + """
local per_second = tonumber(redis.call('HGET', KEYS[2], ARGV[1]) or ARGV[2])
local interval = 1000 / per_second
local burst = interval * math.max(1, per_second)

local next_free = math.max(tonumber(redis.call('GET', KEYS[1]) or 0), now_ms)
local after_this = next_free + interval
redis.call('SET', KEYS[1], tostring(after_this), 'PX', math.ceil(after_this - now_ms) + 60000)
return math.max(0, math.ceil(after_this - burst - now_ms))
"""


def book_slot(r: redis.Redis, rail: str) -> float:
    """Book the next slot on `rail`. Returns how many seconds until it starts:
    0 means send now. Rails without a configured limit are unlimited."""
    default = config.RATE_LIMITS.get(rail)
    if default is None:
        return 0.0
    wait_ms = r.register_script(_BOOK)(keys=[_next_slot_key(rail), LIMITS], args=[rail, default])
    return wait_ms / 1000


def limits(r: redis.Redis) -> dict[str, float]:
    """The per-second limit in force for each rail."""
    changed = r.hgetall(LIMITS)
    return {rail: float(changed.get(rail, default)) for rail, default in config.RATE_LIMITS.items()}


def set_limit(r: redis.Redis, rail: str, per_second: float) -> None:
    """Change a rail's limit for every worker at once. Slots already booked
    keep their times; new bookings use the new limit."""
    if rail not in config.RATE_LIMITS:
        raise ValueError(f"unknown rail {rail!r}")
    if per_second <= 0:
        raise ValueError("the limit must be above 0")
    r.hset(LIMITS, rail, per_second)
