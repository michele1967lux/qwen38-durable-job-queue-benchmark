# durable-job-queue

A durable local job queue backed by SQLite. Jobs survive process
termination and restart. Multiple workers operate concurrently without
executing the same lease simultaneously.

## Features

- **Durable**: SQLite is the source of truth; no in-memory state.
- **Concurrent**: multiple workers can claim jobs safely (atomic claims).
- **Lease-based**: each claim has a finite lease; expired leases are
  recoverable by other workers.
- **Stale-worker safe**: a worker whose lease expired cannot complete or
  fail a job now owned by another worker (claim token, D002).
- **Retry with backoff**: deterministic exponential backoff (1s, 2s, 4s,
  8s, ... capped at 300s).
- **Dead jobs**: jobs that exhaust `max_attempts` move to `dead` and are
  never claimable again.
- **Idempotent enqueue**: optional `idempotency_key` prevents duplicate
  jobs (race-safe, D004).
- **CLI**: `jobq` command for enqueue, list, status, worker, recover.
- **No external dependencies**: stdlib only (Python 3.12+, SQLite).

## Installation

```bash
pip install -e .
```

Or from a release tarball:

```bash
pip install durable-job-queue
```

## Quick start

```python
from durable_job_queue import JobQueue

queue = JobQueue("jobs.db")

# Enqueue a job.
job = queue.enqueue(
    job_type="resize-image",
    payload={"file": "photo.jpg"},
)
print(job.id)

# Claim a job (worker loop).
claimed = queue.claim(worker_id="worker-1", lease_seconds=30)
if claimed:
    # Do the work.
    ...
    # Report success.
    queue.complete(claimed.id, token=claimed.token)
    # Or report failure (retry or dead per policy).
    # queue.fail(claimed.id, token=claimed.token, error="timeout")

queue.close()
```

## Public API

| Name | Description |
|------|-------------|
| `JobQueue` | The queue. One instance per thread/process. |
| `Job` | Immutable job snapshot (id, status, payload, ...). |
| `JobStatus` | Enum: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `DEAD`. |
| `Clock` | Protocol for time injection (test seam). |
| `SystemClock` | Wall-clock implementation (default). |
| `JobQueueError` | Base exception. |
| `JobNotFound` | Unknown job id. |
| `InvalidStateTransition` | Operation not valid for current status. |
| `LeaseLost` | Stale claim token (worker lost the lease). |
| `InvalidPayload` | Payload/job_type not acceptable. |
| `QueueConfigurationError` | Cannot open/configure the queue. |

### JobQueue methods

| Method | Description |
|--------|-------------|
| `enqueue(job_type, payload, *, max_attempts=3, idempotency_key=None)` | Durably create a pending job. |
| `claim(worker_id, lease_seconds=30)` | Atomically claim one job (or `None`). |
| `complete(job_id, *, token)` | Mark a running job completed. |
| `fail(job_id, *, token, error="")` | Record a failed execution (retry or dead). |
| `get(job_id)` | Return the current state of a job. |
| `list(status=None)` | List jobs, optionally filtered by status. |
| `recover()` | Re-pend all expired leases. Returns count. |
| `close()` | Close the database connection (idempotent). |

## CLI

```bash
# Enqueue a job.
jobq enqueue --db jobs.db --type echo --payload '{"msg":"hello"}'

# List jobs (optionally filtered by status).
jobq list --db jobs.db
jobq list --db jobs.db --status pending

# Show one job.
jobq status --db jobs.db <job_id>

# Run a worker (processes jobs until idle).
jobq worker --db jobs.db --worker-id w1 --handler echo --max-jobs 10

# Re-pend expired leases.
jobq recover --db jobs.db
```

### Demo handlers

| Handler | Description |
|---------|-------------|
| `echo` | Print the payload. |
| `sleep` | Sleep for `payload["seconds"]` (default 1). |
| `write-file` | Write `payload["text"]` to `payload["path"]`. |
| `always-fail` | Always raises (demonstrates retry → dead). |

## Job lifecycle

```
pending -> running -> completed
   ^          |
   |          v
   +---- pending (retry)
   |          |
   |          v
   +-------- dead (attempts exhausted)
```

- **pending**: durable, not owned; claimable when `now >= available_at`.
- **running**: owned by exactly one lease (worker_id + claim token).
- **completed**: terminal; execution succeeded.
- **dead**: terminal; attempts exhausted.
- **failed**: API-completeness value; never stored (D005).

## Lease semantics

- Each claim generates a fresh random `claim_token` (128-bit).
- `complete()`/`fail()` must present the token received at claim time.
- The UPDATE is guarded by `WHERE id=? AND status='running' AND claim_token=?`.
- Zero rows affected → `LeaseLost` (stale worker).
- Expired leases (`lease_until <= now`) are reclaimable by other workers.
- Recovery is pull-based: `claim()` considers expired running jobs, and
  `recover()` re-pends all expired leases. No background thread.

## Retry semantics

- `attempts` is incremented at claim time (execution in flight, D005).
- `fail()` with `attempts < max_attempts`: job returns to `pending` with
  `available_at = now + backoff(attempts)`.
- `fail()` with `attempts >= max_attempts`: job becomes `dead`.
- Backoff: `delay = min(1s * 2**(attempt-1), 300s)`.
  - attempt 1 fails → 1s
  - attempt 2 fails → 2s
  - attempt 3 fails → 4s
  - attempt 4 fails → 8s
  - attempt 10 fails → min(512s, 300s) = 300s (capped)

## Idempotency

- `idempotency_key` is optional. Scope: unique per database file (D004).
- Enqueue with an existing key returns the existing job (same id).
- Concurrent duplicates are race-safe (partial unique index + IntegrityError
  fallback).
- Jobs without a key are never deduplicated.

## SQLite file location

The database file is created at the path you pass to `JobQueue(path)`.
WAL side files (`-wal`, `-shm`) are created alongside it. All are
git-ignored (`.gitignore`).

## Limitations

- **Single-writer**: SQLite serializes writes; only one writer at a time.
  `busy_timeout=5000` handles contention.
- **No distributed coordination**: the queue is local to one machine.
- **No at-most-once delivery**: the model is at-least-once with leases.
  A job may be executed more than once if the worker crashes mid-execution.
- **No background reaper**: recovery is pull-based (on claim or via
  `recover()`). If no worker claims for a long time, expired leases stay
  expired until the next claim/recover.
- **Clock skew**: `SystemClock` uses `time.time()`; NTP steps can cause
  non-monotonic time. Acceptable for a local queue (documented).

## Testing

```bash
pip install -e .[test]
pytest
```

53 tests cover T1–T18 (spec §26) plus additional invariants (I1–I8, spec
§39). Time is driven by an injected `FakeClock` (D007) — no real sleeps for
lease/retry semantics.

## License

MIT
