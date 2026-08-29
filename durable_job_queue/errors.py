"""Domain exceptions for durable_job_queue.

These are the controlled error surface (spec §23). Raw sqlite3 errors are
never raised for expected domain failures; they are translated here.
"""

from __future__ import annotations


class JobQueueError(Exception):
    """Base class for all durable_job_queue domain errors."""


class JobNotFound(JobQueueError):
    """The requested job id does not exist in this database."""


class InvalidStateTransition(JobQueueError):
    """An operation is not valid for the job's current status.

    Examples: completing a pending job, completing a completed/dead job,
    failing a job that is not running.
    """


class LeaseLost(JobQueueError):
    """The presented claim token does not own the job's current lease.

    Raised by complete()/fail() when the job is running under a *different*
    (newer) claim. The caller's execution is stale; its result must be
    discarded.
    """


class InvalidPayload(JobQueueError):
    """The payload or job_type is not acceptable.

    Raised before any database write: the database is unchanged.
    """


class QueueConfigurationError(JobQueueError):
    """The queue cannot be opened or configured (bad path, locked file, ...)."""
