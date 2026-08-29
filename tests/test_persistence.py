"""T1 — persistence: enqueue, close, reopen, job still exists.

Also covers invariant I8 (committed state survives reopening the database).
"""

from __future__ import annotations

from durable_job_queue import JobQueue


def test_persistence_across_reopen(db_path, clock):
    q1 = JobQueue(str(db_path), clock=clock)
    job = q1.enqueue(job_type="resize-image", payload={"file": "photo.jpg"})
    q1.close()

    q2 = JobQueue(str(db_path), clock=clock)
    loaded = q2.get(job.id)
    assert loaded.id == job.id
    assert loaded.job_type == "resize-image"
    assert loaded.payload == {"file": "photo.jpg"}
    assert loaded.status.value == "pending"
    assert loaded.attempts == 0
    q2.close()


def test_persistence_preserves_running_state(db_path, clock):
    """A running job (with lease) survives a restart (T15 precondition)."""
    q1 = JobQueue(str(db_path), clock=clock)
    job = q1.enqueue(job_type="echo", payload={"msg": "hi"})
    claimed = q1.claim(worker_id="w1", lease_seconds=30)
    assert claimed.id == job.id
    q1.close()

    q2 = JobQueue(str(db_path), clock=clock)
    loaded = q2.get(job.id)
    assert loaded.status.value == "running"
    assert loaded.worker_id == "w1"
    assert loaded.lease_until == clock.now() + 30
    q2.close()
