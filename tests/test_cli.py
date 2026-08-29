"""T18 — CLI smoke test: enqueue / list / status / worker via `jobq`.

Uses the installed console script (pyproject [project.scripts]).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def run_cli(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "durable_job_queue.cli", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_cli_enqueue_list_status(tmp_path: Path):
    db = tmp_path / "cli.db"

    r = run_cli(
        ["enqueue", "--db", str(db), "--type", "echo",
         "--payload", json.dumps({"msg": "hi"})],
        tmp_path,
    )
    assert r.returncode == 0, r.stderr
    job_id = r.stdout.strip().splitlines()[-1]
    assert job_id

    r = run_cli(["list", "--db", str(db)], tmp_path)
    assert r.returncode == 0, r.stderr
    assert job_id in r.stdout
    assert "pending" in r.stdout

    r = run_cli(["status", "--db", str(db), job_id], tmp_path)
    assert r.returncode == 0, r.stderr
    assert "pending" in r.stdout
    assert "echo" in r.stdout


def test_cli_worker_processes_job(tmp_path: Path):
    """jobq worker claims and completes an echo job end-to-end."""
    db = tmp_path / "cli-worker.db"
    out_file = tmp_path / "out.txt"

    r = run_cli(
        ["enqueue", "--db", str(db), "--type", "write-file",
         "--payload", json.dumps({"path": str(out_file), "text": "done!"})],
        tmp_path,
    )
    assert r.returncode == 0, r.stderr
    job_id = r.stdout.strip().splitlines()[-1]

    r = run_cli(
        ["worker", "--db", str(db), "--worker-id", "cli-w",
         "--max-jobs", "1"],
        tmp_path,
    )
    assert r.returncode == 0, r.stderr

    r = run_cli(["status", "--db", str(db), job_id], tmp_path)
    assert r.returncode == 0, r.stderr
    assert "completed" in r.stdout
    assert out_file.read_text() == "done!"


def test_cli_worker_retry_then_dead(tmp_path: Path):
    """A failing job is retried and eventually dead (visible via CLI)."""
    db = tmp_path / "cli-dead.db"

    r = run_cli(
        ["enqueue", "--db", str(db), "--type", "always-fail",
         "--payload", "{}", "--max-attempts", "2"],
        tmp_path,
    )
    assert r.returncode == 0, r.stderr
    job_id = r.stdout.strip().splitlines()[-1]

    # Two worker passes: each claims the job once (backoff is 1s/2s;
    # the CLI worker uses a real clock, so we pass --lease-seconds and
    # rely on the worker loop sleeping past backoff via --poll-seconds).
    for i in range(2):
        r = run_cli(
            ["worker", "--db", str(db), "--worker-id", f"w{i}",
             "--max-jobs", "1", "--poll-seconds", "3"],
            tmp_path,
        )
        assert r.returncode == 0, r.stderr

    r = run_cli(["status", "--db", str(db), job_id], tmp_path)
    assert r.returncode == 0, r.stderr
    assert "dead" in r.stdout
