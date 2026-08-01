"""Global, cross-city request rate limiter for the async scraper.

Why this exists: as the target city list grows from 8 to 30-40+ (Stage 2),
scraping cities one after another (the old sync scrapper.py behavior) makes
the daily Cloud Run Job runtime scale roughly linearly with city count and
risks hitting the job's task timeout.

Running cities concurrently fixes the runtime problem, but naive concurrency
(e.g. N cities each independently sleeping 1.5-3.5s between requests) would
multiply the *actual* request rate hitting OLX by N — the opposite of what
"ethical, rate-limited scraping" is supposed to mean.

GlobalRateLimiter decouples the two concerns:
  - max_concurrent: how many requests may be in flight at once (bounds
    parallelism / memory / connection count)
  - min_interval: the minimum wall-clock gap enforced between dispatching
    any two requests, regardless of which city's coroutine is asking

The result: OLX sees roughly the same request cadence as the old sequential
scraper (~1 request every 1.5-3.5s), no matter how many cities are being
scraped "at the same time" — concurrency here buys parallel *waiting* on
network I/O, not a higher hit rate on the source.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Self


class GlobalRateLimiter:
    """Async context manager enforcing a shared concurrency + pacing budget.

    Usage:
        limiter = GlobalRateLimiter(max_concurrent=4, min_interval=(1.5, 3.0))
        async with limiter:
            response = await client.post(...)
    """

    def __init__(
        self,
        max_concurrent: int = 4,
        min_interval: tuple[float, float] = (1.5, 3.0),
    ):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        if min_interval[0] < 0 or min_interval[1] < min_interval[0]:
            raise ValueError("min_interval must be a valid (low, high) pair with low <= high")

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._min_interval = min_interval
        self._pacing_lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def __aenter__(self) -> Self:
        # Concurrency gate: at most `max_concurrent` requests in flight,
        # summed across every city being scraped right now.
        await self._semaphore.acquire()

        # Pacing gate: serialize *dispatch timing* across all callers so the
        # gap between any two requests leaving the process is never smaller
        # than min_interval, even if several coroutines are ready at once.
        async with self._pacing_lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next_allowed = now + random.uniform(*self._min_interval)

        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        self._semaphore.release()
        return False
