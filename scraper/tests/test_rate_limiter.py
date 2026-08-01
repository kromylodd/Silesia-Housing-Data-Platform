import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from itertools import pairwise

from rate_limiter import GlobalRateLimiter


@pytest.mark.asyncio
async def test_enforces_min_interval_across_concurrent_callers():
    """5 'requests' fired at once should still be spaced >= min_interval apart,
    proving the pacing is global (shared) rather than per-caller."""
    limiter = GlobalRateLimiter(max_concurrent=5, min_interval=(0.05, 0.05))
    dispatch_times = []

    async def fake_request():
        async with limiter:
            dispatch_times.append(time.monotonic())

    await asyncio.gather(*(fake_request() for _ in range(5)))

    dispatch_times.sort()
    gaps = [b - a for a, b in pairwise(dispatch_times)]
    assert all(gap >= 0.045 for gap in gaps), f"gaps too small: {gaps}"


@pytest.mark.asyncio
async def test_respects_max_concurrent():
    """No more than max_concurrent requests should be 'in flight' at once."""
    limiter = GlobalRateLimiter(max_concurrent=2, min_interval=(0.0, 0.0))
    in_flight = 0
    peak_in_flight = 0
    lock = asyncio.Lock()

    async def fake_slow_request():
        nonlocal in_flight, peak_in_flight
        async with limiter:
            async with lock:
                in_flight += 1
                peak_in_flight = max(peak_in_flight, in_flight)
            await asyncio.sleep(0.05)
            async with lock:
                in_flight -= 1

    await asyncio.gather(*(fake_slow_request() for _ in range(6)))

    assert peak_in_flight <= 2


def test_rejects_invalid_config():
    with pytest.raises(ValueError):
        GlobalRateLimiter(max_concurrent=0)
    with pytest.raises(ValueError):
        GlobalRateLimiter(min_interval=(3.0, 1.5))
