"""SQLite repository: all SQL and transaction boundaries (D001/D003).

Internal module — not part of the public API (D008).

Transaction strategy (PLAN §transaction strategy):
- enqueue:  BEGIN IMMEDIATE  (idempotency check + insert atomically)
- claim:    BEGIN IMMEDIATE  (select + update atomically)
- complete: BEGIN IMMEDIATE  (guarded update; 0 rows -> domain error)
- fail:     BEGIN IMMEDIATE  (guarded update; retry or dead)
- recover:  BEGIN IMMEDIATE  (bulk re-pend of expired leases)
- get/list: plain autocommit reads

sqlite3 autocommit mode is used (isolation_level=None) so that BEGIN
IMMEDIATE is explicit and every transaction boundary is visible in code.
"""

from __future__ import annotations

import json
import secrets
import sqlite3

from .errors import (
    InvalidPayload,
    InvalidStateTransition,
    JobNotFound,
    LeaseLost,
)
from .job import Job, JobStatus

# Columns selected in the canonical order of the Job dataclass (minus token).
_ROW_COLUMNS = (
    "id", "job_type", "payload", "status", "created_at", "updated_at",
    "attempts", "max_attempts", "available_at", "claimed_at", "lease_until",
    "worker_id", "last_error", "idempotency_key",
)


def _row_to_job(row: sqlite3.Row, token: str | None = None) -> Job:
    data = {c: row[c] for c in _ROW_COLUMNS}
    data["payload"] = json.loads(data["payload"])
    data["status"] = JobStatus(data["status"])
    data["token"] = token
    return Job(**data)


def _canonical_json(payload: dict) -> str:
    """Deterministic JSON: sorted keys, compact separators, no ASCII escapes.

    Raises InvalidPayload for anything that is not a JSON object or that
    cannot be serialized.
    """
    if not isinstance(payload, dict):
        raise InvalidPayload(
            f"payload must be a JSON object (dict), got {type(payload).__name__}"
        )
    try:
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
    except (TypeError, ValueError) as e:
        raise InvalidPayload(f"payload is not JSON-serializable: {e}") from e


class Repository:
    """Owns one sqlite3 connection and all SQL for the queue."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------ #
    # reads
    # ------------------------------------------------------------------ #

    def get(self, job_id: str) -> Job:
        row = self._conn.execute(
            "SELECT " + ", ".join(_ROW_COLUMNS) + " FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            raise JobNotFound(job_id)
        return _row_to_job(row)

    def list(self, status: JobStatus | None = None) -> list[Job]:
        if status is not None:
            rows = self._conn.execute(
                "SELECT " + ", ".join(_ROW_COLUMNS)
                + " FROM jobs WHERE status = ? ORDER BY created_at, id",
                (status.value,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT " + ", ".join(_ROW_COLUMNS)
                + " FROM jobs ORDER BY created_at, id"
            ).fetchall()
        return [_row_to_job(r) for r in rows]

    # ------------------------------------------------------------------ #
    # enqueue (idempotent, D004)
    # ------------------------------------------------------------------ #

    def enqueue(
        self,
        job_id: str,
        job_type: str,
        payload: dict,
        max_attempts: int,
        now: float,
        idempotency_key: str | None,
    ) -> Job:
        payload_json = _canonical_json(payload)
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            if idempotency_key is not None:
                existing = conn.execute(
                    "SELECT " + ", ".join(_ROW_COLUMNS)
                    + " FROM jobs WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing is not None:
                    conn.execute("COMMIT")
                    return _row_to_job(existing)
            conn.execute(
                "INSERT INTO jobs ("
                " id, job_type, payload, status, created_at, updated_at,"
                " attempts, max_attempts, available_at, idempotency_key"
                ") VALUES (?, ?, ?, 'pending', ?, ?, 0, ?, ?, ?)",
                (
                    job_id, job_type, payload_json, now, now,
                    max_attempts, now, idempotency_key,
                ),
            )
            conn.execute("COMMIT")
        except sqlite3.IntegrityError:
            # Concurrent enqueue with the same idempotency key won the race
            # (partial unique index, D004). Roll back and return the winner.
            conn.execute("ROLLBACK")
            if idempotency_key is None:
                raise  # PK collision on a fresh uuid: should be impossible
            row = conn.execute(
                "SELECT " + ", ".join(_ROW_COLUMNS)
                + " FROM jobs WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row is None:  # pragma: no cover - defensive
                raise
            return _row_to_job(row)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        return self.get(job_id)

    # ------------------------------------------------------------------ #
    # claim (D002/D003/D006)
    # ------------------------------------------------------------------ #

    def claim(
        self, worker_id: str, lease_seconds: float, now: float
    ) -> Job | None:
        """Atomically claim one job.

        Preference order (deterministic):
          1. pending jobs with available_at <= now (FIFO by created_at, id)
          2. running jobs whose lease expired (lease_until <= now)

        Returns the claimed Job (with token) or None if nothing is claimable.
        """
        conn = self._conn
        token = secrets.token_hex(16)
        try:
            conn.execute("BEGIN IMMEDIATE")

            row = conn.execute(
                "SELECT " + ", ".join(_ROW_COLUMNS)
                + " FROM jobs"
                + " WHERE status = 'pending' AND available_at <= ?"
                + " ORDER BY created_at, id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    "SELECT " + ", ".join(_ROW_COLUMNS)
                    + " FROM jobs"
                    + " WHERE status = 'running' AND lease_until <= ?"
                    + " ORDER BY lease_until, id LIMIT 1",
                    (now,),
                ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None

            cur = conn.execute(
                "UPDATE jobs SET"
                "  status = 'running',"
                "  worker_id = ?,"
                "  claimed_at = ?,"
                "  lease_until = ?,"
                "  claim_token = ?,"
                "  attempts = attempts + 1,"
                "  last_error = NULL,"
                "  updated_at = ?"
                " WHERE id = ? AND status IN ('pending','running')",
                (worker_id, now, now + lease_seconds, token, now, row["id"]),
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        if cur.rowcount != 1:
            # Lost a race on the row (another connection claimed it between
            # our SELECT and UPDATE inside the same lock window is not
            # possible under IMMEDIATE, but stay safe).
            return None
        # Re-read the committed row so the returned Job reflects the
        # post-claim state (status=running, lease fields, token).
        fresh = self._conn.execute(
            "SELECT " + ", ".join(_ROW_COLUMNS) + " FROM jobs WHERE id = ?",
            (row["id"],),
        ).fetchone()
        return _row_to_job(fresh, token=token)

    # ------------------------------------------------------------------ #
    # complete / fail (D002: token-guarded)
    # ------------------------------------------------------------------ #

    def complete(self, job_id: str, token: str, now: float) -> Job:
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                raise JobNotFound(job_id)
            if row["status"] != "running":
                conn.execute("COMMIT")
                raise InvalidStateTransition(
                    f"cannot complete job in status '{row['status']}'"
                )
            cur = conn.execute(
                "UPDATE jobs SET status = 'completed', worker_id = worker_id,"
                " lease_until = NULL, claim_token = NULL, updated_at = ?"
                " WHERE id = ? AND status = 'running' AND claim_token = ?",
                (now, job_id, token),
            )
            conn.execute("COMMIT")
        except (JobNotFound, InvalidStateTransition):
            raise
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        if cur.rowcount != 1:
            raise LeaseLost(
                "claim token does not own the current lease (stale worker)"
            )
        return self.get(job_id)

    def fail(
        self, job_id: str, token: str, error: str, now: float,
        next_available_at: float, make_dead: bool,
    ) -> Job:
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, attempts, max_attempts FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                raise JobNotFound(job_id)
            if row["status"] != "running":
                conn.execute("COMMIT")
                raise InvalidStateTransition(
                    f"cannot fail job in status '{row['status']}'"
                )
            if make_dead:
                cur = conn.execute(
                    "UPDATE jobs SET status = 'dead', last_error = ?,"
                    " lease_until = NULL, claim_token = NULL, updated_at = ?"
                    " WHERE id = ? AND status = 'running' AND claim_token = ?",
                    (error, now, job_id, token),
                )
            else:
                cur = conn.execute(
                    "UPDATE jobs SET status = 'pending', available_at = ?,"
                    " last_error = ?, lease_until = NULL, claim_token = NULL,"
                    " worker_id = NULL, claimed_at = NULL, updated_at = ?"
                    " WHERE id = ? AND status = 'running' AND claim_token = ?",
                    (next_available_at, error, now, job_id, token),
                )
            conn.execute("COMMIT")
        except (JobNotFound, InvalidStateTransition):
            raise
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        if cur.rowcount != 1:
            raise LeaseLost(
                "claim token does not own the current lease (stale worker)"
            )
        return self.get(job_id)

    # ------------------------------------------------------------------ #
    # recovery (D006)
    # ------------------------------------------------------------------ #

    def recover(self, now: float) -> int:
        """Re-pend all running jobs whose lease expired at or before `now`.

        Returns the number of jobs recovered. Terminal jobs are untouched.
        """
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "UPDATE jobs SET status = 'pending', available_at = ?,"
                " lease_until = NULL, claim_token = NULL, worker_id = NULL,"
                " claimed_at = NULL, updated_at = ?"
                " WHERE status = 'running' AND lease_until <= ?",
                (now, now, now),
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        return cur.rowcount

    # ------------------------------------------------------------------ #

    def close(self) -> None:
        self._conn.close()
