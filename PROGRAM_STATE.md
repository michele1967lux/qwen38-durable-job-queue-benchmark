# PROGRAM_STATE — Durable Local Job Queue

Factual state of the project. Every claim is classified:

- **VERIFIED** — proven by executed evidence (test run, command output).
- **CHARACTERIZED** — observed behavior, not yet covered by a regression test.
- **INFERRED** — reasoned from documentation/design, not directly executed.
- **UNPROVEN** — assumed, no evidence either way.

## Current state

- Repository: initialized 2026-07-09 (session date), branch
  `bench/qwen38-durable-job-queue`, no prior commits existed (greenfield).
- Governance files (PLAN.md, PROGRAM_STATE.md, DECISIONS.md) committed as the
  baseline commit.
- No production code exists yet.

## Claims

| # | Claim | Class | Evidence |
|---|-------|-------|----------|
| 1 | Python 3.12.3 and SQLite 3.45.1 are available in this environment | VERIFIED | `python3 --version`, `sqlite3.sqlite_version` executed at session start |
| 2 | `git init` produced a fresh repository with no prior history | VERIFIED | `git rev-parse HEAD` failed with "unknown revision" before first commit |
| 3 | WAL mode improves reader/writer concurrency on this SQLite build | INFERRED | SQLite docs; not yet measured |
| 4 | Behavior under abrupt OS power loss | UNPROVEN | not tested (out of scope; WAL + synchronous=NORMAL is the documented posture) |
| 5 | Concurrent claim yields exactly one lease owner | UNPROVEN | test T3 written but not yet executed |
| 6 | Stale worker cannot commit over a newer claim | UNPROVEN | test T5 written but not yet executed |
| 7 | Idempotent enqueue is race-safe | UNPROVEN | tests T10/T11 written but not yet executed |

## Findings log

(Empty so far. Defects, spec ambiguities, and stop-condition events are
recorded here with dates and links to commits.)

## Stop conditions

None triggered.
