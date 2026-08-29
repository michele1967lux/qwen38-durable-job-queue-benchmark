"""Clock seam (D007): all time-sensitive logic goes through Clock.now().

Production uses SystemClock (wall clock). Tests inject a deterministic
clock and advance it manually — no real sleeps for lease/retry semantics.
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """A source of 'now' in unix seconds (float)."""

    def now(self) -> float: ...


class SystemClock:
    """Wall-clock implementation (time.time)."""

    def now(self) -> float:
        return time.time()
