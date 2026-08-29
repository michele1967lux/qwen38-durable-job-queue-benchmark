"""T4 — valid completion: the current lease owner completes successfully.

And T14 — invalid state transition: completing a pending (unclaimed) job
must not silently succeed.
"""

from __future__ import annotations

import pytest

from durable_job_queue import (
    InvalidStateTransition,
    JobQueue,
    LeaseLost,
)


def test_valid_completion(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={"msg": "done"})
    claimed = q.claim(worker_id="w1", lease_seconds=30)

    completed = q.complete(claimed.id, token=claimed.token)
    assert completed.id == job.id
    assert completed.status.value == "completed"
    assert completed.worker_id == "w1"

    # I4: completed is terminal — cannot be claimed again.
    assert q.claim(worker_id="w2", lease_seconds=30) is None
    q.close()


def test_complete_pending_job_rejected(db_path, clock):
    """T14: complete on a job that was never claimed must raise."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={})

    with pytest.raises(InvalidStateTransition):
        q.complete(job.id, token="bogus-token")

    # State unchanged: still pending, claimable.
    state = q.get(job.id)
    assert state.status.value == "pending"
    claimed = q.claim(worker_id="w", lease_seconds=30)
    assert claimed.id == job.id
    q.close()


def test_complete_with_wrong_token_rejected(db_path, clock):
    """A token that was never issued for this claim must not work."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={})
    q.claim(worker_id="w1", lease_seconds=30)

    with pytest.raises(LeaseLost):
        q.complete(job.id, token="forged-token")

    # Job still running under w1's lease.
    state = q.get(job.id)
    assert state.status.value == "running"
    assert state.worker_id == "w1"
    q.close()


def test_complete_twice_rejected(db_path, clock):
    """Second completion attempt (same or different token) must fail."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={})
    claimed = q.claim(worker_id="w1", lease_seconds=30)
    q.complete(job.id, token=claimed.token)

    with pytest.raises(InvalidStateTransition):
        q.complete(job.id, token=claimed.token)
    q.close()
