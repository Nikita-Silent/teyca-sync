"""Generic async circuit breaker guarding calls to a flaky upstream dependency."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

STATE_CLOSED = "closed"
STATE_OPEN = "open"
STATE_HALF_OPEN = "half_open"


class CircuitBreakerOpenError(Exception):
    """Raised when a call is rejected because the circuit is open."""

    def __init__(self, name: str, *, retry_after_seconds: float) -> None:
        super().__init__(
            f"Circuit breaker '{name}' is open: too many recent failures, "
            f"retry after {retry_after_seconds:.1f}s"
        )
        self.name = name
        self.retry_after_seconds = retry_after_seconds


@dataclass
class CircuitBreaker:
    """Closed/open/half-open breaker: opens after N consecutive failures,
    rejects calls without attempting them until a cooldown elapses, then lets a
    single probe call through to decide whether to close again or reopen."""

    name: str
    failure_threshold: int = 5
    cooldown_seconds: float = 30.0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    _state: str = field(default=STATE_CLOSED, repr=False)
    _consecutive_failures: int = field(default=0, repr=False)
    _opened_at: float | None = field(default=None, repr=False)
    _half_open_probe_in_flight: bool = field(default=False, repr=False)

    @property
    def state(self) -> str:
        return self._state

    async def before_call(self) -> None:
        """Raise CircuitBreakerOpenError if the call must be rejected without trying."""
        async with self._lock:
            if self._state == STATE_CLOSED:
                return
            if self._state == STATE_OPEN:
                remaining = self.cooldown_seconds - (time.monotonic() - (self._opened_at or 0.0))
                if remaining > 0:
                    raise CircuitBreakerOpenError(self.name, retry_after_seconds=remaining)
                self._state = STATE_HALF_OPEN
                self._half_open_probe_in_flight = True
                return
            # STATE_HALF_OPEN: only one probe call is allowed in flight at a time.
            if self._half_open_probe_in_flight:
                raise CircuitBreakerOpenError(
                    self.name, retry_after_seconds=self.cooldown_seconds
                )
            self._half_open_probe_in_flight = True

    async def record_success(self) -> None:
        async with self._lock:
            self._state = STATE_CLOSED
            self._consecutive_failures = 0
            self._opened_at = None
            self._half_open_probe_in_flight = False

    async def record_failure(self) -> None:
        async with self._lock:
            self._half_open_probe_in_flight = False
            if self._state == STATE_HALF_OPEN:
                # Probe failed: reopen immediately and restart the cooldown.
                self._state = STATE_OPEN
                self._opened_at = time.monotonic()
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= max(1, self.failure_threshold):
                self._state = STATE_OPEN
                self._opened_at = time.monotonic()
