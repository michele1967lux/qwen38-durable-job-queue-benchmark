"""Job model: JobStatus enum and Job dataclass (public API, D008)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class JobStatus(str, Enum):
    """Durable job states.

    - PENDING:   durable, not owned; claimable when now >= available_at.
    - RUNNING:   owned by exactly one lease (worker_id + claim token).
    - COMPLETED: terminal; execution succeeded.
    - DEAD:      terminal; attempts exhausted.
    - FAILED:    API-completeness value; never stored (D005: a failed
                 attempt with retries left re-pends the job in the same
                 transaction, so no row is ever persisted as FAILED).
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


TERMINAL_STATUSES = frozenset({JobStatus.COMPLETED, JobStatus.DEAD})


@dataclass(frozen=True)
class Job:
    """Immutable snapshot of a job as read from (or written to) the queue.

    `token` is the claim token for the lease *this instance* holds on the
    job (set by claim()). It is None for jobs read via get()/list() and for
    jobs that are not running. It is the ownership proof required by
    complete()/fail() (D002).
    """

    id: str
    job_type: str
    payload: dict
    status: JobStatus
    created_at: float
    updated_at: float
    attempts: int
    max_attempts: int
    available_at: float
    claimed_at: float | None
    lease_until: float | None
    worker_id: str | None
    last_error: str | None
    idempotency_key: str | None
    token: str | None = None
