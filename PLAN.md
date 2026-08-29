# PLAN — Durable Local Job Queue

Status: initial plan (committed before any production code exists).
Branch: `bench/qwen38-durable-job-queue` (created from empty repo; no prior commits existed).

## Goal

A production-quality, local, durable job queue backed by SQLite:

```
client -> enqueue -> SQLite durable queue -> worker A / worker B
claim -> execute -> complete / retry / dead
```

Jobs survive process termination and restart. Multiple workers operate
concurrently without executing the same lease simultaneously.

## Scope

- Python 3.12+, SQLite (stdlib `sqlite3`), no external runtime services.
- Installable package `durable-job-queue`, import name `durable_job_queue`.
- Public API: `JobQueue`, `Job`, `JobStatus`, domain exceptions.
- CLI: `jobq enqueue --type TYPE --payload JSON`, `jobq list`,
  `jobq status JOB_ID`, `jobq worker`.
- Small built-in demo handler registry (`echo`, `sleep`, `write-file`).
- Deterministic exponential backoff retry, `max_attempts`, dead jobs.
- Idempotent enqueue via `idempotency_key`.
- Lease-based claiming with stale-worker safety (claim token).
- Recovery of expired leases without a background scheduler.
- Behavioral tests T1–T18 plus additional edge cases.

## Non-goals

- No Redis/PostgreSQL/RabbitMQ/Celery/Docker.
- No plugin framework, no query language, no web UI.
- No distributed coordination beyond a single SQLite file.
- No at-most-once delivery guarantee (at-least-once with leases is the model).
- No background daemon; recovery is pull-based (on claim) + explicit `recover()`.

## Proposed architecture

```
durable_job_queue/
  __init__.py        # public exports
  errors.py          # domain exceptions
  clock.py           # Clock protocol + SystemClock + FakeClock (test seam)
  schema.py          # DDL + migration version
  repository.py      # SQLite access, transactions (internal)
  job.py             # Job dataclass + JobStatus enum
  queue.py           # JobQueue public API
  backoff.py         # deterministic exponential backoff
  cli.py             # argparse CLI
  handlers.py        # demo handler registry (echo/sleep/write-file)
```

- `JobQueue` is the only public entry point; it owns one SQLite connection
  per instance (one connection per process/thread of use).
- `repository.py` is internal; all SQL lives there.
- Time comes from an injectable `Clock` (default `SystemClock`), so tests
  never sleep for real seconds.

## SQLite schema proposal

```sql
CREATE TABLE jobs (
  id              TEXT PRIMARY KEY,          -- uuid4 hex
  job_type        TEXT NOT NULL,
  payload         TEXT NOT NULL,             -- canonical JSON
  status          TEXT NOT NULL,             -- pending|running|completed|failed|dead
  created_at      REAL NOT NULL,             -- unix seconds (float)
  updated_at      REAL NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 3,
  available_at    REAL NOT NULL,             -- claimable only when now >= available_at
  claimed_at      REAL,
  lease_until     REAL,
  worker_id       TEXT,
  claim_token     TEXT,                      -- random token per claim (ownership proof)
  last_error      TEXT,
  idempotency_key TEXT
);
CREATE UNIQUE INDEX idx_jobs_idempotency ON jobs(idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_jobs_claim ON jobs(status, available_at);
```

Notes:
- `claim_token` is a fresh random value generated inside the claim
  transaction. `complete`/`fail` must present the token they received at
  claim time; the UPDATE is guarded by `claim_token = ? AND status='running'`.
  This is the stale-worker defense (worker_id alone is insufficient because
  a worker could reuse an id; the token is unguessable and single-use).
- `failed` is a transient state used only inside the fail transaction when
  attempts remain (job is re-pended with future `available_at`); the durable
  terminal states are `completed` and `dead`. (See D005.)
- Partial unique index on `idempotency_key` gives one logical job per key
  (scope: the whole database file — documented).

## State machine

States: `pending`, `running`, `completed`, `failed`, `dead`.

- `pending`: durable, not currently owned. Claimable iff `now >= available_at`.
- `running`: owned by exactly one lease (worker_id + claim_token + lease_until).
- `completed`: terminal. Success recorded.
- `failed`: transient bookkeeping state after a failed attempt that still has
  retries left; the job is immediately re-scheduled to `pending` with a future
  `available_at` in the same transaction. (If we keep `failed` as a durable
  state instead, the transition table below still holds; decision D005.)
- `dead`: terminal. Attempts exhausted. Never claimable.

Valid transitions:

```
pending   -> running      (claim: now >= available_at, atomic)
running   -> completed    (complete: valid current claim token)
running   -> pending      (fail with attempts < max_attempts: backoff schedule)
running   -> dead         (fail with attempts >= max_attempts)
running   -> pending      (lease expiry recovery: now >= lease_until)
```

Invalid (must raise, never silently succeed):
- `complete`/`fail` on a job not in `running` (e.g. completing a pending job).
- `complete`/`fail` with a stale/unknown claim token.
- `claim` of a `completed`/`dead` job.
- Any transition out of `completed` or `dead`.

## Lease strategy

- Claim: single transaction, `BEGIN IMMEDIATE`, select one claimable job
  (`status='pending' AND available_at <= now`), set
  `status='running'`, `worker_id`, `claimed_at=now`, `lease_until=now+lease_seconds`,
  `claim_token=<random>`. Commit. Return job + token.
- Ownership proof: the claim token. `complete`/`fail` UPDATEs are guarded by
  `WHERE id=? AND status='running' AND claim_token=?`. Zero rows affected =>
  `LeaseLost` (or `InvalidStateTransition` if the job is not running at all).
- Expiry: a running job with `lease_until <= now` is reclaimable. Recovery is
  pull-based: `claim()` also considers expired running jobs, and an explicit
  `queue.recover(now)` re-pends all expired running jobs. No background thread.
- Two workers cannot both hold a valid lease: the claim transaction is
  serialized by SQLite's write lock; the token makes stale commits impossible.

## Transaction strategy

- Every mutating operation is one transaction:
  - `enqueue`: `BEGIN IMMEDIATE` (idempotency check + insert atomically).
  - `claim`: `BEGIN IMMEDIATE` (select + update atomically).
  - `complete`/`fail`: `BEGIN IMMEDIATE` (guarded update; raise if 0 rows).
  - `recover`: `BEGIN IMMEDIATE` (bulk re-pend of expired leases).
- Reads (`get`, `list`) are plain autocommit reads.
- `PRAGMA journal_mode=WAL` (set once at open) so readers do not block the
  writer and vice versa; `PRAGMA busy_timeout=5000` for lock contention.
- `PRAGMA synchronous=NORMAL` (WAL-safe; durability tradeoff documented).
- One connection per `JobQueue` instance; connections are not shared across
  threads (documented; tests use separate processes/instances for concurrency).

## Idempotency strategy

- `idempotency_key` is optional. Scope: unique per database file (global).
- Enqueue with a key: `BEGIN IMMEDIATE`; if a row with that key exists, return
  the existing job (no new row); otherwise insert. The partial unique index is
  the backstop: a concurrent duplicate insert fails with IntegrityError and is
  converted to "return existing job".
- The returned job id is stable across duplicate enqueues.

## Retry strategy

- Deterministic exponential backoff: `delay = base * 2**(attempt-1)` where
  `attempt` is the number of the attempt that just failed (1-based),
  `base = 1s`, capped at `max_delay = 300s` (documented, configurable).
  - attempt 1 fails -> available_at = now + 1s
  - attempt 2 fails -> now + 2s
  - attempt 3 fails -> now + 4s
  - attempt 4 fails -> now + 8s
- `attempts` is incremented at claim time (the attempt is "in flight"), so a
  job with `max_attempts=3` is executed at most 3 times, then `dead`.
  (Documented; alternative is increment-on-fail — see D005.)
- `fail` with `attempts < max_attempts`: status back to `pending`,
  `available_at = now + delay`, `last_error` set.
- `fail` with `attempts >= max_attempts`: status `dead`, `last_error` set.
- Jobs with `available_at > now` are not claimable (I6).

## Test strategy

- `pytest` + stdlib only. `FakeClock` (manual `now()`/`advance()`) injected
  via `JobQueue(..., clock=...)` — no real sleeps for lease/retry semantics.
- Concurrency tests use separate `JobQueue` instances (separate connections,
  same file) in threads, and a subprocess-based test for true process-level
  races (T3, T11). SQLite WAL + busy_timeout makes this safe.
- T1–T18 mapped 1:1 to test functions; additional invariants I1–I8 covered.
- Stale-worker race (T5) driven by clock advancement, not sleeps.
- CLI tested via `subprocess` against the installed entry point.
- External-consumer test (T17) runs `python -c "import durable_job_queue"`
  from a directory outside the repo after `pip install -e .`.

## Known risks

- SQLite write serialization: only one writer at a time; busy_timeout must be
  large enough for the test concurrency. Mitigation: short transactions.
- `claim_token` must be unguessable: use `secrets.token_hex(16)`.
- Clock seam: `SystemClock` uses `time.time()`; monotonicity not guaranteed
  across NTP steps — acceptable for a local queue (documented).
- WAL file (`-wal`, `-shm`) must not be committed; `.gitignore` covers `*.db*`.
- `attempts` increment-at-claim vs at-fail is a semantic choice; must be
  documented and tested consistently (D005).

## Uncertainties

- Whether `failed` should be a durable state or transient (D005 decides).
- Exact `max_attempts` semantics (count of executions vs count of failures) —
  decided: count of executions (attempts), documented.
- Whether the judge's "attempt 1 -> retry after 1 second" means delay after
  the 1st failed attempt — yes, that is the interpretation used.
