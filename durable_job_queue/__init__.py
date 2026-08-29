"""durable_job_queue — durable local job queue backed by SQLite.

Public API (D008):
    JobQueue, Job, JobStatus, Clock, SystemClock,
    JobQueueError, JobNotFound, InvalidStateTransition, LeaseLost,
    InvalidPayload, QueueConfigurationError
"""

from .clock import Clock, SystemClock
from .errors import (
    InvalidPayload,
    InvalidStateTransition,
    JobNotFound,
    JobQueueError,
    LeaseLost,
    QueueConfigurationError,
)
from .job import Job, JobStatus
from .queue import JobQueue

__version__ = "0.1.0"

__all__ = [
    "JobQueue",
    "Job",
    "JobStatus",
    "Clock",
    "SystemClock",
    "JobQueueError",
    "JobNotFound",
    "InvalidStateTransition",
    "LeaseLost",
    "InvalidPayload",
    "QueueConfigurationError",
    "__version__",
]
