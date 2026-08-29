"""T3 — concurrent claim: two workers compete for one job; exactly one wins.

Runs in threads with separate JobQueue instances (separate SQLite
connections, same file) — the real concurrency path (D001/D003).
"""

from __future__ import annotations

import threading

from durable_job_queue import JobQueue


def test_concurrent_claim_single_winner(db_path, clock):
    q_a = JobQueue(str(db_path), clock=clock)
    q_b = JobQueue(str(db_path), clock=clock)
    job = q_a.enqueue(job_type="echo", payload={"msg": "race"})

    results: list = [None, None]
    barrier = threading.Barrier(2)

    def claimer(idx: int, q: JobQueue, wid: str) -> None:
        barrier.wait()
        results[idx] = q.claim(worker_id=wid, lease_seconds=30)

    threads = [
        threading.Thread(target=claimer, args=(0, q_a, "worker-A")),
        threading.Thread(target=claimer, args=(1, q_b, "worker-B")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [r for r in results if r is not None]
    assert len(winners) == 1, f"expected exactly one winner, got {results}"
    assert winners[0].id == job.id
    assert winners[0].worker_id in ("worker-A", "worker-B")

    # The job is running exactly once; the loser sees it as running.
    state = q_a.get(job.id)
    assert state.status.value == "running"
    q_a.close()
    q_b.close()


def test_concurrent_claim_many_workers_one_job(db_path, clock):
    """8 workers, 1 job: still exactly one winner (I1)."""
    queues = [JobQueue(str(db_path), clock=clock) for _ in range(8)]
    job = queues[0].enqueue(job_type="echo", payload={})

    results: list = [None] * 8
    barrier = threading.Barrier(8)

    def claimer(idx: int) -> None:
        barrier.wait()
        results[idx] = queues[idx].claim(worker_id=f"w{idx}", lease_seconds=30)

    threads = [threading.Thread(target=claimer, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert winners[0].id == job.id
    for q in queues:
        q.close()
