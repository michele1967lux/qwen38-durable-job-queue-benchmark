"""T5 — stale completion: the critical stale-worker race (spec §10).

Timeline (driven by FakeClock, no real sleeps):
  T0: Worker A claims Job X
  T1: A stops responding (simulated: A simply does nothing)
  T2: A's lease expires (clock advances past lease_until)
  T3: Worker B claims Job X (recovery path, D006)
  T4: Worker A returns late
  T5: Worker A tries to mark X completed -> must be REJECTED
  B remains authoritative.
"""

from __future__ import annotations

import pytest

from durable_job_queue import InvalidStateTransition, JobQueue, LeaseLost


def test_stale_worker_cannot_complete(db_path, clock):
    q_a = JobQueue(str(db_path), clock=clock)
    q_b = JobQueue(str(db_path), clock=clock)
    job = q_a.enqueue(job_type="echo", payload={"msg": "race"})

    # T0: A claims.
    a_claim = q_a.claim(worker_id="worker-A", lease_seconds=30)
    assert a_claim is not None
    a_token = a_claim.token

    # T2: A's lease expires.
    clock.advance(31)

    # T3: B reclaims the expired job.
    b_claim = q_b.claim(worker_id="worker-B", lease_seconds=30)
    assert b_claim is not None
    assert b_claim.id == job.id
    assert b_claim.worker_id == "worker-B"
    assert b_claim.token != a_token  # new ownership proof

    # T5: A returns late and tries to complete with its stale token.
    with pytest.raises(LeaseLost):
        q_a.complete(job.id, token=a_token)

    # B remains authoritative: job still running under B's lease.
    state = q_b.get(job.id)
    assert state.status.value == "running"
    assert state.worker_id == "worker-B"

    # B completes successfully.
    b_done = q_b.complete(job.id, token=b_claim.token)
    assert b_done.status.value == "completed"
    assert b_done.worker_id == "worker-B"

    # A still cannot fail it: the job is now completed (terminal), so the
    # precise error is InvalidStateTransition. (While the job was still
    # running under B's lease, A's stale token raised LeaseLost — proven
    # above. I3 holds either way: A cannot overwrite B's outcome.)
    with pytest.raises(InvalidStateTransition):
        q_a.fail(job.id, token=a_token, error="late failure")

    final = q_b.get(job.id)
    assert final.status.value == "completed"
    assert final.worker_id == "worker-B"

    q_a.close()
    q_b.close()


def test_stale_worker_cannot_fail(db_path, clock):
    """Same race, fail() instead of complete()."""
    q_a = JobQueue(str(db_path), clock=clock)
    q_b = JobQueue(str(db_path), clock=clock)
    job = q_a.enqueue(job_type="echo", payload={}, max_attempts=5)

    a_claim = q_a.claim(worker_id="worker-A", lease_seconds=10)
    a_token = a_claim.token

    clock.advance(11)  # lease expires

    b_claim = q_b.claim(worker_id="worker-B", lease_seconds=10)
    assert b_claim is not None

    with pytest.raises(LeaseLost):
        q_a.fail(job.id, token=a_token, error="stale failure")

    state = q_b.get(job.id)
    assert state.status.value == "running"
    assert state.worker_id == "worker-B"
    q_a.close()
    q_b.close()


def test_stale_token_after_reclaim_cycle(db_path, clock):
    """Two full expiry cycles: each old token is dead, only the newest lives."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=10)

    c1 = q.claim(worker_id="w1", lease_seconds=5)
    clock.advance(6)
    c2 = q.claim(worker_id="w2", lease_seconds=5)
    clock.advance(6)
    c3 = q.claim(worker_id="w3", lease_seconds=5)

    assert c1.token != c2.token != c3.token

    with pytest.raises(LeaseLost):
        q.complete(job.id, token=c1.token)
    with pytest.raises(LeaseLost):
        q.complete(job.id, token=c2.token)

    done = q.complete(job.id, token=c3.token)
    assert done.status.value == "completed"
    assert done.worker_id == "w3"
    q.close()
