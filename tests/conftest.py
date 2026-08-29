"""Shared test fixtures: temp database paths and a FakeClock.

The FakeClock is the time seam (D007): tests advance it manually instead of
sleeping, so lease expiry and retry backoff are deterministic.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest


class FakeClock:
    """Deterministic clock: now() returns a controlled value."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self._now = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("cannot advance clock backwards")
        self._now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "jobs.db"


@pytest.fixture
def make_queue(db_path: Path, clock: FakeClock):
    """Factory returning a JobQueue bound to the shared db file and clock."""
    from durable_job_queue import JobQueue

    created: list[JobQueue] = []

    def _make() -> JobQueue:
        q = JobQueue(str(db_path), clock=clock)
        created.append(q)
        return q

    yield _make
    for q in created:
        q.close()
