"""SQLite schema and connection setup (D001).

The schema is created idempotently on open. All SQL lives in
repository.py; this module only owns DDL and pragmas.
"""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS jobs (
  id              TEXT PRIMARY KEY,
  job_type        TEXT NOT NULL,
  payload         TEXT NOT NULL,
  status          TEXT NOT NULL CHECK (status IN
                    ('pending','running','completed','failed','dead')),
  created_at      REAL NOT NULL,
  updated_at      REAL NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 3,
  available_at    REAL NOT NULL,
  claimed_at      REAL,
  lease_until     REAL,
  worker_id       TEXT,
  claim_token     TEXT,
  last_error      TEXT,
  idempotency_key TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
  ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_claim
  ON jobs(status, available_at);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""


def open_connection(path: str) -> sqlite3.Connection:
    """Open a SQLite connection with the queue's concurrency posture.

    - WAL: readers do not block the writer and vice versa (D001).
    - busy_timeout=5000: concurrent writers wait up to 5s for the lock
      instead of failing immediately (D003).
    - synchronous=NORMAL: safe under WAL; slightly faster than FULL.
    - check_same_thread=False: a JobQueue instance may be used from the
      thread that created it or handed to another thread (one connection
      per instance; SQLite itself serializes all writes, and each
      transaction is short). This is what makes the documented
      "one instance per thread" concurrency model work.
    """
    conn = sqlite3.connect(path, timeout=5.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Create tables/indexes if absent and record the schema version."""
    conn.executescript(DDL)
    conn.execute(
        "INSERT INTO meta(key, value) VALUES ('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()
