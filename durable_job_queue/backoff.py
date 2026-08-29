"""Deterministic exponential backoff (D005).

delay(n) = min(base * 2**(n-1), max_delay)

where n is the 1-based index of the attempt that just failed:

    attempt 1 fails -> 1s
    attempt 2 fails -> 2s
    attempt 3 fails -> 4s
    attempt 4 fails -> 8s
    ...
    attempt 10 fails -> min(512s, 300s) = 300s (capped)
"""

from __future__ import annotations

DEFAULT_BASE_SECONDS = 1.0
DEFAULT_MAX_DELAY_SECONDS = 300.0


def backoff_delay(
    attempt: int,
    base_seconds: float = DEFAULT_BASE_SECONDS,
    max_delay_seconds: float = DEFAULT_MAX_DELAY_SECONDS,
) -> float:
    """Return the retry delay (seconds) after the given failed attempt.

    `attempt` is 1-based: the number of the attempt that just failed.
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    if base_seconds <= 0:
        raise ValueError("base_seconds must be > 0")
    if max_delay_seconds <= 0:
        raise ValueError("max_delay_seconds must be > 0")
    delay = base_seconds * (2 ** (attempt - 1))
    return min(delay, max_delay_seconds)
