"""Worker loop: claim -> execute -> complete/fail (reusable by CLI/tests)."""

from __future__ import annotations

import time

from .handlers import Handler
from .queue import JobQueue


def run_worker(
    queue: JobQueue,
    worker_id: str,
    *,
    handler: Handler,
    lease_seconds: float = 30.0,
    max_jobs: int | None = None,
    poll_seconds: float = 0.5,
    idle_polls: int | None = None,
) -> int:
    """Process jobs until `max_jobs` are done or `idle_polls` consecutive
    empty polls occur (None = run forever).

    Returns the number of jobs this worker claimed.
    """
    processed = 0
    idle = 0
    while max_jobs is None or processed < max_jobs:
        job = queue.claim(worker_id=worker_id, lease_seconds=lease_seconds)
        if job is None:
            idle += 1
            if idle_polls is not None and idle >= idle_polls:
                break
            time.sleep(poll_seconds)
            continue
        idle = 0
        processed += 1
        try:
            handler(job.payload)
        except Exception as e:  # noqa: BLE001 - worker boundary
            queue.fail(job.id, token=job.token, error=f"{type(e).__name__}: {e}")
            print(f"[{worker_id}] job {job.id} failed: {e}", flush=True)
        else:
            queue.complete(job.id, token=job.token)
            print(f"[{worker_id}] job {job.id} completed", flush=True)
    return processed
