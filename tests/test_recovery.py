"""T6 — expired lease recovery; T15 — restart during a running job.

An expired running job becomes claimable again (by claim-time recovery,
D006) and via the explicit recover() operation.
"""

from __future__ import annotations

from durable_job_queue import JobQueue


def test_expired_lease_becomes_claimable(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={})
    q.claim(worker_id="crashed-worker", lease_seconds=30)

    # Before expiry: not claimable by another worker.
    assert q.claim(worker_id="w2", lease_seconds=30) is None

    # Lease expires.
    clock.advance(31)

    # Another worker can now claim it.
    reclaimed = q.claim(worker_id="w2", lease_seconds=30)
    assert reclaimed is not None
    assert reclaimed.id == job.id
    assert reclaimed.worker_id == "w2"
    assert reclaimed.lease_until == clock.now() + 30
    q.close()


def test_recover_re_pends_expired_running_jobs(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    j1 = q.enqueue(job_type="echo", payload={"n": 1})
    j2 = q.enqueue(job_type="echo", payload={"n": 2})
    q.claim(worker_id="dead-1", lease_seconds=10)
    q.claim(worker_id="dead-2", lease_seconds=10)

    clock.advance(11)
    recovered = q.recover()
    assert recovered == 2

    s1 = q.get(j1.id)
    s2 = q.get(j2.id)
    assert s1.status.value == "pending"
    assert s2.status.value == "pending"
    # Attempts preserved (D005): the executions still count.
    assert s1.attempts == 1
    assert s2.attempts == 1
    q.close()


def test_recover_does_not_touch_valid_leases(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    j1 = q.enqueue(job_type="echo", payload={})
    j2 = q.enqueue(job_type="echo", payload={})
    q.claim(worker_id="alive", lease_seconds=100)
    q.claim(worker_id="dead", lease_seconds=1)

    clock.advance(2)
    recovered = q.recover()
    assert recovered == 1

    s1 = q.get(j1.id)
    assert s1.status.value == "running"  # valid lease untouched
    assert s1.worker_id == "alive"
    s2 = q.get(j2.id)
    assert s2.status.value == "pending"
    q.close()


def test_restart_during_running_job_recovers(db_path, clock):
    """T15: process dies mid-job; after restart the expired lease recovers."""
    q1 = JobQueue(str(db_path), clock=clock)
    job = q1.enqueue(job_type="echo", payload={"msg": "dying"})
    q1.claim(worker_id="worker-that-died", lease_seconds=30)
    q1.close()  # simulate process termination

    clock.advance(31)  # time passes while the process is down

    q2 = JobQueue(str(db_path), clock=clock)  # restart
    reclaimed = q2.claim(worker_id="worker-reborn", lease_seconds=30)
    assert reclaimed is not None
    assert reclaimed.id == job.id
    assert reclaimed.worker_id == "worker-reborn"

    done = q2.complete(job.id, token=reclaimed.token)
    assert done.status.value == "completed"
    q2.close()
