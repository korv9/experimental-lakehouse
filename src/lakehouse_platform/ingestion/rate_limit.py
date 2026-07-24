"""Cooperative client-side rate limiting for external source APIs."""
from __future__ import annotations

import threading
import time
from collections.abc import Callable


class RateLimiter:
    """Space requests across threads using a shared monotonic clock."""

    def __init__(
        self,
        requests_per_second: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be greater than zero")
        self.minimum_interval = 1.0 / requests_per_second
        self._clock = clock
        self._sleep = sleeper
        self._lock = threading.Lock()
        self._next_request_at = 0.0

    def wait(self) -> float:
        """Block until the next request slot and return the slept duration."""
        with self._lock:
            now = self._clock()
            delay = max(0.0, self._next_request_at - now)
            if delay:
                self._sleep(delay)
                now = self._clock()
            self._next_request_at = max(now, self._next_request_at) + self.minimum_interval
            return delay
