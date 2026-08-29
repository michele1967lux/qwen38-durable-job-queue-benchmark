"""JobQueue: the public API (D008).

One JobQueue instance owns one SQLite connection. Instances are not
thread-safe; for concurrent access use one instance per thread/process
(see tests/test_concurrent_claim.py).
"""

from __future__ import annotations

import os
import uuid

from .backoff import (
    DEFAULT_BASE_SECONDS,
    DEFAULT_MAX_DELAY_SECONDS,
    backoff_delay,
)
from .clock import Clock, SystemClock
from .errors import (
    InvalidPayload,
    QueueConfigurationError,
)
from .job import Job, JobStatus
from .repository import Repository
from .schema import init_schema, open_connection

DEFAULT_MAX_ATTEMPTS = 3


class JobQueue:
    """Durable local job queue backed by a single SQLite file.

    Example:
        queue = JobQueue("jobs.db")
        job = queue.enqueue(job_type="resize-image", payload={"file": "a.jpg"})
        claimed = queue.claim(worker_id="w1", lease_seconds=30)
        queue.complete(claimed.id, token=claimed.token)
    """

    def __init__(
        self,
        path: str,
        *,
        clock: Clock | None = None,
        base_backoff_seconds: float = DEFAULT_BASE_SECONDS,
        max_backoff_seconds: float = DEFAULT_MAX_DELAY_SECONDS,
    ) -> None:
        self._path = os.path.abspath(path)
        self._clock = clock or SystemClock()
        self._base_backoff = base_backoff_seconds
        self._max_backoff = max_backoff_seconds
        try:
            conn = open_connection(self._path)
            init_schema(conn)
        except Exception as e:
            raise QueueConfigurationError(
                f"cannot open queue database at {self._path}: {e}"
            ) from e
        self._repo = Repository(conn)
        self._closed = False

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """Close the underlying database connection. Idempotent."""
        if not self._closed:
            self._repo.close()
            self._closed = True

    def __enter__(self) -> "JobQueue":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # enqueue
    # ------------------------------------------------------------------ #

    def enqueue(
        self,
        job_type: str,
        payload: dict,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        idempotency_key: str | None = None,
    ) -> Job:
        """Durably create a pending job and return it (with its id).

        - `payload` must be a JSON object (dict) with JSON-serializable
          values; otherwise InvalidPayload is raised and nothing is written.
        - `max_attempts` must be a positive int (default 3).
        - `idempotency_key` (optional): enqueuing again with the same key
          returns the existing job instead of creating a new one (D004).
        """
        self._ensure_open()
        if not isinstance(job_type, str) or not job_type:
            raise InvalidPayload(
                f"job_type must be a non-empty string, got {job_type!r}"
            )
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) \
                or max_attempts < 1:
            raise InvalidPayload(
                f"max_attempts must be a positive int, got {max_attempts!r}"
            )
        if idempotency_key is not None and (
            not isinstance(idempotency_key, str) or not idempotency_key
        ):
            raise InvalidPayload(
                "idempotency_key must be a non-empty string when provided"
            )
        # Validate payload before touching the database (T13).
        from .repository import _canonical_json

        _canonical_json(payload)

        job_id = uuid.uuid4().hex
        now = self._clock.now()
        return self._repo.enqueue(
            job_id=job_id,
            job_type=job_type,
            payload=payload,
            max_attempts=max_attempts,
            now=now,
            idempotency_key=idempotency_key,
        )

    # ------------------------------------------------------------------ #
    # claim / complete / fail
    # ------------------------------------------------------------------ #

    def claim(
        self, worker_id: str, lease_seconds: float = 30.0
    ) -> Job | None:
        """Atomically claim one job for this worker.

        Returns the claimed Job (status RUNNING, with `.token` set) or None
        if nothing is claimable. Claimable means:
          - pending with available_at <= now, or
          - running with an expired lease (lease_until <= now, D006).

        The token on the returned Job is the ownership proof: it must be
        passed back to complete()/fail().
        """
        self._ensure_open()
        if not isinstance(worker_id, str) or not worker_id:
            raise ValueError("worker_id must be a non-empty string")
        if not isinstance(lease_seconds, (int, float)) \
                or isinstance(lease_seconds, bool) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive number")
        now = self._clock.now()
        return self._repo.claim(worker_id, float(lease_seconds), now)

    def complete(self, job_id: str, *, token: str) -> Job:
        """Mark a running job completed. Requires the current claim token.

        Raises:
          JobNotFound: unknown id.
          InvalidStateTransition: job is not running (e.g. pending,
            completed, dead).
          LeaseLost: the job is running under a different (newer) claim —
            the caller's lease is stale (D002).
        """
        self._ensure_open()
        now = self._clock.now()
        return self._repo.complete(job_id, token, now)

    def fail(
        self, job_id: str, *, token: str, error: str = ""
    ) -> Job:
        """Record a failed execution of a running job.

        - attempts < max_attempts: job returns to PENDING with
          available_at = now + backoff(attempts) (D005).
        - attempts >= max_attempts: job becomes DEAD.

        Raises the same errors as complete().
        """
        self._ensure_open()
        if not isinstance(error, str):
            raise InvalidPayload("error must be a string")
        now = self._clock.now()
        job = self._repo.get(job_id)  # raises JobNotFound if unknown
        delay = backoff_delay(
            job.attempts, self._base_backoff, self._max_backoff
        )
        make_dead = job.attempts >= job.max_attempts
        return self._repo.fail(
            job_id,
            token,
            error,
            now,
            next_available_at=now + delay,
            make_dead=make_dead,
        )

    # ------------------------------------------------------------------ #
    # lookup / listing / recovery
    # ------------------------------------------------------------------ #

    def get(self, job_id: str) -> Job:
        """Return the current state of a job. Raises JobNotFound."""
        self._ensure_open()
        return self._repo.get(job_id)

    def list(self, status: JobStatus | str | None = None) -> list[Job]:
        """List jobs, optionally filtered by status.

        `status` may be a JobStatus or its string value ("pending", ...).
        Order: created_at, then id.
        """
        self._ensure_open()
        if status is None:
            return self._repo.list()
        if isinstance(status, str):
            try:
                status = JobStatus(status)
            except ValueError:
                valid = ", ".join(s.value for s in JobStatus)
                raise ValueError(
                    f"unknown status {status!r}; expected one of: {valid}"
                ) from None
        return self._repo.list(status)

    def recover(self) -> int:
        """Re-pend all running jobs whose lease has expired (D006).

        Returns the number of jobs recovered. Deterministic: depends only
        on the clock and stored lease_until values. No background thread.
        """
        self._ensure_open()
        return self._repo.recover(self._clock.now())

    # ------------------------------------------------------------------ #

    def _ensure_open(self) -> None:
        if self._closed:
            raise QueueConfigurationError(
                "queue is closed; open a new JobQueue instance"
            )
