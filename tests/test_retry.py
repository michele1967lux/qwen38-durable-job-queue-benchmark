"""T7/T8 — retry scheduling with deterministic exponential backoff.

D005: delay after failed attempt n = min(1s * 2**(n-1), 300s).
attempts is incremented at claim time.
"""

from __future__ import annotations

from durable_job_queue import JobQueue


def test_retry_attempt1_schedules_1s(db_path, clock):
    """T7: first failure schedules retry at now + 1s."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=3)
    t0 = clock.now()

    claimed = q.claim(worker_id="w", lease_seconds=30)
    assert claimed.attempts == 1
    q.fail(job.id, token=claimed.token, error="connection timeout")

    state = q.get(job.id)
    assert state.status.value == "pending"
    assert state.available_at == t0 + 1
    assert state.last_error == "connection timeout"
    assert state.attempts == 1
    q.close()


def test_retry_progression_1_2_4_8(db_path, clock):
    """T8: delays are 1s, 2s, 4s, 8s for attempts 1..4."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=10)

    expected = [1, 2, 4, 8]
    for n, delay in enumerate(expected, start=1):
        t0 = clock.now()
        claimed = q.claim(worker_id=f"w{n}", lease_seconds=30)
        assert claimed is not None, f"attempt {n}: job not claimable"
        assert claimed.attempts == n
        q.fail(job.id, token=claimed.token, error=f"err-{n}")

        state = q.get(job.id)
        assert state.status.value == "pending"
        assert state.available_at == t0 + delay, (
            f"attempt {n}: expected available_at={t0 + delay}, "
            f"got {state.available_at}"
        )
        # Advance past the backoff so the next claim succeeds.
        clock.advance(delay + 0.5)

    q.close()


def test_backoff_is_capped(db_path, clock):
    """Delays never exceed max_delay (300s default)."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=20)

    # Attempt 10 would be 1s * 2**9 = 512s -> capped at 300s.
    for n in range(1, 10):
        t0 = clock.now()
        claimed = q.claim(worker_id=f"w{n}", lease_seconds=30)
        q.fail(job.id, token=claimed.token, error=f"err-{n}")
        clock.advance(301)

    t0 = clock.now()
    claimed = q.claim(worker_id="w10", lease_seconds=30)
    q.fail(job.id, token=claimed.token, error="err-10")
    state = q.get(job.id)
    assert state.available_at == t0 + 300
    q.close()


def test_future_retry_not_claimable(db_path, clock):
    """T16: a job scheduled for future retry is not claimable early (I6)."""
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={}, max_attempts=5)

    claimed = q.claim(worker_id="w", lease_seconds=30)
    q.fail(job.id, token=claimed.token, error="boom")

    # available_at is now + 1s; we are still before it.
    assert q.claim(worker_id="w2", lease_seconds=30) is None

    clock.advance(1.0)
    reclaimed = q.claim(worker_id="w2", lease_seconds=30)
    assert reclaimed is not None
    assert reclaimed.id == job.id
    q.close()
