import random
from collections.abc import Callable


def backoff_seconds(
    attempt: int,
    base: float,
    cap: float,
    rand: Callable[[], float] = random.random,
) -> float:
    """How long to wait before the next attempt, after `attempt` failed tries.

    Exponential: about base, 2*base, 4*base, ... never more than cap. Then
    jitter: a random 50-100% of that. Without jitter, workers that failed at
    the same moment (the rail went down) would all retry at the same moment
    too, and could knock the recovering rail straight back over.
    """
    delay = min(cap, base * 2 ** (attempt - 1))
    return delay * (0.5 + rand() / 2)
