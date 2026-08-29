"""T2 — basic claim: a pending available job can be claimed.

Claiming makes the job running with a finite lease and a claim token.
"""

from __future__ import annotations

from durable_job_queue import JobQueue


def test_claim_pending_job(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={"msg": "hello"})

    claimed = q.claim(worker_id="worker-1", lease_seconds=30)
    assert claimed is not None
    assert claimed.id == job.id
    assert claimed.status.value == "running"
    assert claimed.worker_id == "worker-1"
    assert claimed.claimed_at == clock.now()
    assert claimed.lease_until == clock.now() + 30
    assert claimed.token is not None  # ownership proof (D002)
    q.close()


def test_claim_returns_none_when_no_jobs(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    assert q.claim(worker_id="worker-1", lease_seconds=30) is None
    q.close()


def test_claim_increments_attempts(db_path, clock):
    """D005: attempts is incremented at claim time (execution in flight)."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={})
    claimed = q.claim(worker_id="w", lease_seconds=30)
    assert claimed.attempts == 1
    q.close()
