"""jobq CLI (spec §19).

Commands:
  jobq enqueue --db PATH --type TYPE --payload JSON
               [--max-attempts N] [--idempotency-key KEY]
  jobq list    --db PATH [--status STATUS]
  jobq status  --db PATH JOB_ID
  jobq worker  --db PATH --worker-id ID [--handler NAME]
               [--lease-seconds S] [--max-jobs N] [--poll-seconds S]
               [--idle-polls N]
  jobq recover --db PATH

`--db` defaults to ./jobs.db. The worker command uses the built-in demo
handler registry (handlers.py); --handler defaults to 'echo'.
"""

from __future__ import annotations

import argparse
import json
import sys

from .errors import JobQueueError
from .handlers import HANDLERS, handler_for
from .job import JobStatus
from .queue import JobQueue
from .worker import run_worker

DEFAULT_DB = "jobs.db"


def _open_queue(args: argparse.Namespace) -> JobQueue:
    return JobQueue(args.db)


def cmd_enqueue(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as e:
        print(f"error: --payload is not valid JSON: {e}", file=sys.stderr)
        return 2
    if not isinstance(payload, dict):
        print("error: --payload must be a JSON object", file=sys.stderr)
        return 2
    with _open_queue(args) as q:
        job = q.enqueue(
            job_type=args.type,
            payload=payload,
            max_attempts=args.max_attempts,
            idempotency_key=args.idempotency_key,
        )
    print(job.id)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    with _open_queue(args) as q:
        jobs = q.list(status=args.status)
    if not jobs:
        print("(no jobs)")
        return 0
    for j in jobs:
        extra = ""
        if j.status is JobStatus.RUNNING and j.worker_id:
            extra = f" worker={j.worker_id}"
        if j.last_error:
            extra += f" last_error={j.last_error!r}"
        print(
            f"{j.id}  {j.status.value:<10}  type={j.job_type}  "
            f"attempts={j.attempts}/{j.max_attempts}{extra}"
        )
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    with _open_queue(args) as q:
        j = q.get(args.job_id)
    print(f"id:            {j.id}")
    print(f"job_type:      {j.job_type}")
    print(f"status:        {j.status.value}")
    print(f"payload:       {json.dumps(j.payload, sort_keys=True)}")
    print(f"attempts:      {j.attempts}/{j.max_attempts}")
    print(f"created_at:    {j.created_at}")
    print(f"updated_at:    {j.updated_at}")
    print(f"available_at:  {j.available_at}")
    if j.claimed_at is not None:
        print(f"claimed_at:    {j.claimed_at}")
    if j.lease_until is not None:
        print(f"lease_until:   {j.lease_until}")
    if j.worker_id is not None:
        print(f"worker_id:     {j.worker_id}")
    if j.last_error is not None:
        print(f"last_error:    {j.last_error!r}")
    if j.idempotency_key is not None:
        print(f"idempotency:   {j.idempotency_key}")
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    handler = handler_for(args.handler)
    with _open_queue(args) as q:
        processed = run_worker(
            q,
            worker_id=args.worker_id,
            handler=handler,
            lease_seconds=args.lease_seconds,
            max_jobs=args.max_jobs,
            poll_seconds=args.poll_seconds,
            idle_polls=args.idle_polls,
        )
    print(f"[{args.worker_id}] processed {processed} job(s)")
    return 0


def cmd_recover(args: argparse.Namespace) -> int:
    with _open_queue(args) as q:
        n = q.recover()
    print(f"recovered {n} expired lease(s)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="jobq", description="Durable local job queue (SQLite)"
    )
    sub = p.add_subparsers(dest="command", required=True)

    def add_db(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--db", default=DEFAULT_DB, help="SQLite db path")

    sp = sub.add_parser("enqueue", help="enqueue a job")
    add_db(sp)
    sp.add_argument("--type", required=True, help="job type")
    sp.add_argument("--payload", required=True, help="JSON object payload")
    sp.add_argument("--max-attempts", type=int, default=3)
    sp.add_argument("--idempotency-key", default=None)
    sp.set_defaults(func=cmd_enqueue)

    sp = sub.add_parser("list", help="list jobs")
    add_db(sp)
    sp.add_argument("--status", default=None,
                    choices=[s.value for s in JobStatus])
    sp.set_defaults(func=cmd_list)

    sp = sub.add_parser("status", help="show one job")
    add_db(sp)
    sp.add_argument("job_id")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser("worker", help="run a worker loop")
    add_db(sp)
    sp.add_argument("--worker-id", required=True)
    sp.add_argument("--handler", default="echo",
                    choices=sorted(HANDLERS))
    sp.add_argument("--lease-seconds", type=float, default=30.0)
    sp.add_argument("--max-jobs", type=int, default=None)
    sp.add_argument("--poll-seconds", type=float, default=0.5)
    sp.add_argument("--idle-polls", type=int, default=None,
                    help="stop after N consecutive empty polls")
    sp.set_defaults(func=cmd_worker)

    sp = sub.add_parser("recover", help="re-pend expired leases")
    add_db(sp)
    sp.set_defaults(func=cmd_recover)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except JobQueueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except KeyError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
