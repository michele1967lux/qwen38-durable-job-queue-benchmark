"""T10/T11/T12 — idempotent enqueue.

D004: idempotency_key is unique per database file. Duplicate enqueue
returns the existing job (same id); concurrent duplicates still yield
exactly one durable job (I7).
"""

from __future__ import annotations

import threading

from durable_job_queue import JobQueue


def test_sequential_duplicate_idempotency_key(db_path, clock):
    """T10: same key twice -> one job, same id returned both times."""
    q = JobQueue(str(db_path), clock=clock)
    first = q.enqueue(
        job_type="invoice",
        payload={"invoice_id": 123},
        idempotency_key="invoice-123",
    )
    second = q.enqueue(
        job_type="invoice",
        payload={"invoice_id": 123},
        idempotency_key="invoice-123",
    )
    assert first.id == second.id
    assert len(q.list()) == 1
    q.close()


def test_concurrent_duplicate_idempotency_key(db_path, clock):
    """T11: concurrent enqueues with the same key -> exactly one job (I7)."""
    q_a = JobQueue(str(db_path), clock=clock)
    q_b = JobQueue(str(db_path), clock=clock)

    results: list = [None, None]
    errors: list = []
    barrier = threading.Barrier(2)

    def enqueue(idx: int, q: JobQueue) -> None:
        barrier.wait()
        try:
            results[idx] = q.enqueue(
                job_type="invoice",
                payload={"invoice_id": 456},
                idempotency_key="invoice-456",
            )
        except Exception as e:  # pragma: no cover - defensive
            errors.append(e)

    threads = [
        threading.Thread(target=enqueue, args=(0, q_a)),
        threading.Thread(target=enqueue, args=(1, q_b)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"concurrent enqueue raised: {errors}"
    assert results[0] is not None and results[1] is not None
    assert results[0].id == results[1].id
    assert len(q_a.list()) == 1
    q_a.close()
    q_b.close()


def test_different_idempotency_keys_distinct_jobs(db_path, clock):
    """T12: different keys -> distinct jobs."""
    q = JobQueue(str(db_path), clock=clock)
    a = q.enqueue(job_type="invoice", payload={"id": 1}, idempotency_key="k-1")
    b = q.enqueue(job_type="invoice", payload={"id": 2}, idempotency_key="k-2")
    assert a.id != b.id
    assert len(q.list()) == 2
    q.close()


def test_jobs_without_key_are_never_deduplicated(db_path, clock):
    """Enqueue without a key always creates a new job."""
    q = JobQueue(str(db_path), clock=clock)
    a = q.enqueue(job_type="echo", payload={"n": 1})
    b = q.enqueue(job_type="echo", payload={"n": 1})
    assert a.id != b.id
    assert len(q.list()) == 2
    q.close()


def test_idempotent_enqueue_returns_existing_state(db_path, clock):
    """A duplicate enqueue reflects the job's current (advanced) state."""
    q = JobQueue(str(db_path), clock=clock)
    first = q.enqueue(
        job_type="echo", payload={}, idempotency_key="dup-state"
    )
    c = q.claim(worker_id="w", lease_seconds=30)
    q.complete(first.id, token=c.token)

    again = q.enqueue(
        job_type="echo", payload={}, idempotency_key="dup-state"
    )
    assert again.id == first.id
    assert again.status.value == "completed"
    assert len(q.list()) == 1
    q.close()
