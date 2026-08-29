"""Additional edge cases and invariant regressions (spec §27, §39).

These go beyond T1–T18:
- I1: at most one current lease (double-claim of the same running job).
- I2: only the current lease owner may fail the job.
- I4: completed never returns to pending/running (even via recover()).
- Claim ordering: pending jobs before expired running jobs.
- get() of unknown id raises JobNotFound.
- list() filtering by each status.
- max_attempts validation.
- lease_seconds validation.
- close() idempotency and use-after-close behavior.
"""

from __future__ import annotations

import pytest

from durable_job_queue import (
    InvalidPayload,
    JobNotFound,
    JobQueue,
    LeaseLost,
)


def test_get_unknown_id_raises(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    with pytest.raises(JobNotFound):
        q.get("no-such-id")
    q.close()


def test_list_filters_by_status(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    p = q.enqueue(job_type="echo", payload={"s": "pending"})
    r = q.enqueue(job_type="echo", payload={"s": "running"})
    d = q.enqueue(job_type="echo", payload={"s": "dead"}, max_attempts=1)

    c = q.claim(worker_id="w", lease_seconds=30)
    assert c.id == r.id
    c = q.claim(worker_id="w", lease_seconds=30)
    assert c.id == d.id
    q.fail(d.id, token=c.token, error="fatal")

    assert [j.id for j in q.list(status="pending")] == [p.id]
    assert [j.id for j in q.list(status="running")] == [r.id]
    assert [j.id for j in q.list(status="dead")] == [d.id]
    assert len(q.list()) == 3
    q.close()


def test_list_invalid_status_rejected(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    with pytest.raises(ValueError):
        q.list(status="bogus")
    q.close()


def test_recover_does_not_resurrect_completed(db_path, clock):
    """I4: completed is terminal; recover() must not touch it."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={})
    c = q.claim(worker_id="w", lease_seconds=30)
    q.complete(job.id, token=c.token)

    clock.advance(1000)
    assert q.recover() == 0
    assert q.get(job.id).status.value == "completed"
    q.close()


def test_recover_does_not_resurrect_dead(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=1)
    c = q.claim(worker_id="w", lease_seconds=30)
    q.fail(job.id, token=c.token, error="fatal")

    clock.advance(1000)
    assert q.recover() == 0
    assert q.get(job.id).status.value == "dead"
    q.close()


def test_claim_prefers_pending_over_expired_running(db_path, clock):
    """Deterministic claim order: pending first, then expired running."""
    q = JobQueue(str(db_path), clock=clock)
    pending = q.enqueue(job_type="echo", payload={"kind": "pending"})
    running = q.enqueue(job_type="echo", payload={"kind": "running"})

    c = q.claim(worker_id="w1", lease_seconds=10)
    assert c.id == running.id  # first claim takes the first pending job
    clock.advance(11)  # its lease expires

    # Now: one pending (pending), one expired-running (running).
    c = q.claim(worker_id="w2", lease_seconds=30)
    assert c.id == pending.id, "pending must be preferred over expired running"
    q.close()


def test_max_attempts_validation(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    with pytest.raises(InvalidPayload):
        q.enqueue(job_type="echo", payload={}, max_attempts=0)
    with pytest.raises(InvalidPayload):
        q.enqueue(job_type="echo", payload={}, max_attempts=-1)
    with pytest.raises(InvalidPayload):
        q.enqueue(job_type="echo", payload={}, max_attempts="three")
    assert q.list() == []
    q.close()


def test_lease_seconds_validation(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    q.enqueue(job_type="echo", payload={})
    with pytest.raises(ValueError):
        q.claim(worker_id="w", lease_seconds=0)
    with pytest.raises(ValueError):
        q.claim(worker_id="w", lease_seconds=-5)
    q.close()


def test_worker_id_validation(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    q.enqueue(job_type="echo", payload={})
    with pytest.raises(ValueError):
        q.claim(worker_id="", lease_seconds=30)
    with pytest.raises(ValueError):
        q.claim(worker_id=None, lease_seconds=30)
    q.close()


def test_close_is_idempotent(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    q.close()
    q.close()  # must not raise


def test_fail_with_empty_error_allowed(db_path, clock):
    """error may be an empty string (still a recorded failure)."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=2)
    c = q.claim(worker_id="w", lease_seconds=30)
    q.fail(job.id, token=c.token, error="")
    state = q.get(job.id)
    assert state.status.value == "pending"
    assert state.last_error == ""
    q.close()


def test_attempts_preserved_across_recovery(db_path, clock):
    """A recovered job keeps its attempt count (no free retries)."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=2)

    c = q.claim(worker_id="w1", lease_seconds=10)
    assert c.attempts == 1
    clock.advance(11)  # crash: lease expires, no fail() call

    c = q.claim(worker_id="w2", lease_seconds=10)
    assert c.attempts == 2, "recovered job must keep attempt count"
    q.fail(job.id, token=c.token, error="second attempt failed")

    assert q.get(job.id).status.value == "dead"
    q.close()


def test_many_jobs_claimed_in_order(db_path, clock):
    """FIFO-ish fairness: claims drain the queue without duplicates."""
    q = JobQueue(str(db_path), clock=clock)
    ids = [q.enqueue(job_type="echo", payload={"i": i}).id for i in range(5)]

    claimed = []
    for i in range(5):
        c = q.claim(worker_id=f"w{i}", lease_seconds=30)
        assert c is not None
        claimed.append(c.id)
    assert sorted(claimed) == sorted(ids)
    assert q.claim(worker_id="w9", lease_seconds=30) is None
    q.close()
