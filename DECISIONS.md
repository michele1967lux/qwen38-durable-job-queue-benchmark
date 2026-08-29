# DECISIONS — Durable Local Job Queue

Material architectural decisions with stable IDs.
Status: `accepted` | `superseded by Dxxx` | `open`.

---

## D001 — SQLite persistence model

- **Context**: the queue must be durable, local, and dependency-free.
- **Decision**: single SQLite file is the source of truth. One connection per
  `JobQueue` instance. `PRAGMA journal_mode=WAL`, `busy_timeout=5000`,
  `synchronous=NORMAL`. All mutations in `BEGIN IMMEDIATE` transactions.
- **Alternatives**: append-only log + index (more code, no benefit at this
  scale); Postgres/Redis (forbidden by spec); in-memory + snapshot (violates
  durability requirement).
- **Reason**: SQLite gives ACID writes, file durability, and zero services.
- **Consequences**: single-writer serialization (busy_timeout handles
  contention); WAL side files must be git-ignored; no cross-machine sharing.
- **Status**: accepted.

## D002 — Lease ownership mechanism (claim token)

- **Context**: stale-worker safety (spec §10): a worker whose lease expired
  must not be able to complete/fail a job now owned by another worker.
  `worker_id` alone is insufficient (ids can be reused or guessed).
- **Decision**: each claim generates a fresh random `claim_token`
  (`secrets.token_hex(16)`) stored on the job. `complete`/`fail` must present
  the token received at claim; the UPDATE is guarded by
  `WHERE id=? AND status='running' AND claim_token=?`. Zero rows => `LeaseLost`.
- **Alternatives**: worker_id guard only (insufficient); lease generation
  counter (works, but token is simpler and unguessable); fencing via
  `claimed_at` timestamp (breaks on clock skew).
- **Reason**: token is single-use, unguessable, and checked atomically in the
  same transaction as the state change.
- **Consequences**: `Job` must carry the token (internal field, not part of
  the public identity); stale commits fail safely with a domain error.
- **Status**: accepted.

## D003 — Claim transaction

- **Context**: two workers claiming the same pending job concurrently.
- **Decision**: `claim()` runs in one `BEGIN IMMEDIATE` transaction:
  select one row matching `status='pending' AND available_at <= now` (or an
  expired running row, see D006), update it to `running` with worker_id,
  claimed_at, lease_until, claim_token; commit. SQLite's write lock serializes
  concurrent claims; the second claimer either sees the job already running
  (skips it) or blocks on the lock and then re-evaluates.
- **Alternatives**: optimistic locking with retry loop (more code, same
  outcome); `SELECT ... FOR UPDATE` (not in SQLite).
- **Reason**: single atomic read-modify-write; no window for double claim.
- **Consequences**: claim latency bounded by transaction length; busy_timeout
  must exceed worst-case contention (5s chosen).
- **Status**: accepted.

## D004 — Idempotency scope

- **Context**: spec §15 requires idempotent enqueue; scope must be defined.
- **Decision**: `idempotency_key` is unique **per database file** (global
  scope). Enqueue with an existing key returns the existing job (same id),
  never creates a second row. Enforced by a partial unique index
  `WHERE idempotency_key IS NOT NULL` plus an `INSERT ... ` inside
  `BEGIN IMMEDIATE` with IntegrityError fallback to "select existing".
- **Alternatives**: per-queue-name scope (no queue names exist); per-job_type
  scope (too narrow, surprising); no DB enforcement (race-unsafe).
- **Reason**: simplest scope that is correct and race-safe; documented.
- **Consequences**: two different databases may each hold a job with the same
  key (acceptable: different durability domains); key reuse across job types
  is the caller's responsibility.
- **Status**: accepted.

## D005 — Retry backoff and attempts semantics

- **Context**: spec §12–13 require deterministic exponential backoff
  (1s, 2s, 4s, 8s, ...) and a dead transition when attempts are exhausted.
- **Decision**:
  - `delay = min(base * 2**(n-1), max_delay)` with `base=1s`, `max_delay=300s`,
    where `n` is the 1-based index of the attempt that just failed.
  - `attempts` is incremented **at claim time** (the execution is in flight).
    A job with `max_attempts=3` executes at most 3 times; the 3rd failure
    moves it to `dead`.
  - `failed` is **not** a durable state: a failed attempt with retries left
    re-pends the job (`status='pending'`, future `available_at`) in the same
    transaction; the durable states are `pending`, `running`, `completed`,
    `dead`. `JobStatus.FAILED` exists in the enum for API completeness and is
    never observed on a stored row.
- **Alternatives**: increment-at-fail (a crashed worker would never consume
  its attempt — bad); `failed` as durable state with a separate re-pend step
  (two transactions, window where job is stuck); linear backoff (spec asks
  for exponential).
- **Reason**: increment-at-claim bounds total executions even if the worker
  crashes mid-execution; single-transaction fail keeps the job consistent.
- **Consequences**: a job that crashes on every attempt still reaches `dead`
  after `max_attempts` claims; `list(status="failed")` returns empty by
  design (documented in README).
- **Status**: accepted.

## D006 — Recovery mechanism

- **Context**: spec §18: recover expired leases without a background
  scheduler; deterministic and testable.
- **Decision**: pull-based recovery, two complementary paths:
  1. `claim()` considers expired running jobs (`status='running' AND
     lease_until <= now`) as claimable, in the same transaction as normal
     claims (expired-running rows are preferred after pending rows).
  2. Explicit `queue.recover()` re-pends all expired running jobs
     (`status='pending'`, `available_at=now`, attempts preserved) in one
     transaction.
  No threads, no timers.
- **Alternatives**: background reaper thread (forbidden by spec §18);
  recovery only in `recover()` (a worker that only calls `claim()` would
  never see expired jobs — weaker).
- **Reason**: claim-time recovery is the minimal mechanism that guarantees
  "another worker must eventually be able to recover and claim the job"
  (spec §9) with zero extra infrastructure.
- **Consequences**: recovery latency = time until next claim/recover call;
  documented.
- **Status**: accepted.

## D007 — Clock seam

- **Context**: spec §21: time-sensitive behavior must be testable without
  long real sleeps.
- **Decision**: `Clock` protocol with `now() -> float`. `SystemClock`
  (default) wraps `time.time()`. `FakeClock` (in `tests/`, also exported for
  embedding) supports `advance(seconds)`. `JobQueue` accepts `clock=` and
  uses it for all `now` computations (claim, lease, backoff, recovery).
- **Alternatives**: monkeypatching `time.time` (fragile, global); passing
  `now` into every method (API pollution).
- **Reason**: single seam, no global state, tests are deterministic.
- **Consequences**: production code never calls `time.time()` directly
  outside `SystemClock`.
- **Status**: accepted.

## D008 — Public API surface

- **Context**: spec §24: keep the public surface small.
- **Decision**: `durable_job_queue` exports exactly: `JobQueue`, `Job`,
  `JobStatus`, `Clock`, `SystemClock`, and the exceptions `JobNotFound`,
  `InvalidStateTransition`, `LeaseLost`, `InvalidPayload`,
  `QueueConfigurationError`. `Job` is a frozen dataclass with a `token`
  field (the claim token, `None` unless the job was just claimed by this
  instance). `JobQueue` methods: `enqueue`, `claim`, `complete`, `fail`,
  `get`, `list`, `recover`, `close`.
- **Alternatives**: expose repository/SQL (forbidden by spec); separate
  `Worker` class (CLI implements the loop instead — smaller surface).
- **Reason**: minimal, testable, matches the spec's conceptual API.
- **Consequences**: the worker loop lives in `cli.py` (and is reusable via
  `durable_job_queue.worker.run_worker` for the demo).
- **Status**: accepted.
