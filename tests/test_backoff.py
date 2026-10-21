import pytest

from settlement.backoff import backoff_seconds


def no_jitter():
    return 1.0  # the top of the jitter range: the full exponential delay


def most_jitter():
    return 0.0  # the bottom of the jitter range: half the delay


def test_delay_doubles_each_attempt():
    delays = [backoff_seconds(n, base=1, cap=60, rand=no_jitter) for n in range(1, 6)]

    assert delays == [1, 2, 4, 8, 16]


def test_delay_never_exceeds_the_cap():
    assert backoff_seconds(20, base=1, cap=60, rand=no_jitter) == 60


def test_jitter_can_halve_the_delay():
    assert backoff_seconds(4, base=1, cap=60, rand=most_jitter) == 4


@pytest.mark.parametrize("attempt", [1, 3, 7])
def test_real_jitter_stays_between_half_and_full(attempt):
    full = backoff_seconds(attempt, base=1, cap=60, rand=no_jitter)

    for _ in range(200):
        assert full / 2 <= backoff_seconds(attempt, base=1, cap=60) <= full


def test_jitter_spreads_retries_out():
    delays = {backoff_seconds(3, base=1, cap=60) for _ in range(50)}

    assert len(delays) > 40  # workers that failed together retry at different times
