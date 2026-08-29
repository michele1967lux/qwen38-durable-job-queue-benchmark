"""T9 — dead transition: exhausted job becomes dead and is not claimable.

I5: a dead job is never returned by normal claim operations.
"""

from __future__ import annotations

from durable_job_queue import JobQueue


def test_dead_after_max_attempts(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=3)

    # Attempt 1 -> pending (retry)
    c = q.claim(worker_id="w1", lease_seconds=30)
    q.fail(job.id, token=c.token, error="e1")
    assert q.get(job.id).status.value == "pending"
    clock.advance(2)

    # Attempt 2 -> pending (retry)
    c = q.claim(worker_id="w2", lease_seconds=30)
    q.fail(job.id, token=c.token, error="e2")
    assert q.get(job.id).status.value == "pending"
    clock.advance(3)

    # Attempt 3 (== max_attempts) -> dead
    c = q.claim(worker_id="w3", lease_seconds=30)
    assert c.attempts == 3
    q.fail(job.id, token=c.token, error="e3")

    state = q.get(job.id)
    assert state.status.value == "dead"
    assert state.last_error == "e3"
    assert state.attempts == 3

    # I5: not claimable, even after a long time.
    clock.advance(10_000)
    assert q.claim(worker_id="w4", lease_seconds=30) is None
    q.close()


def test_dead_job_not_in_list_pending_or_running(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=1)
    c = q.claim(worker_id="w", lease_seconds=30)
    q.fail(job.id, token=c.token, error="fatal")

    assert q.get(job.id).status.value == "dead"
    assert q.list(status="pending") == []
    assert q.list(status="running") == []
    dead = q.list(status="dead")
    assert [j.id for j in dead] == [job.id]
    q.close()


def test_dead_job_cannot_be_completed(db_path, clock):
    """I4/I5: terminal states are terminal."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=1)
    c = q.claim(worker_id="w", lease_seconds=30)
    q.fail(job.id, token=c.token, error="fatal")

    from durable_job_queue import InvalidStateTransition

    try:
        q.complete(job.id, token=c.token)
        raise AssertionError("complete on dead job must raise")
    except InvalidStateTransition:
        pass
    q.close()
