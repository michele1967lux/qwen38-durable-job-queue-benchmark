"""T17 — public external consumer: import and use from outside the repo.

Runs `python -c` in a subprocess with cwd outside the repository tree,
after the package is importable (editable install in CI/dev).
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path


def test_external_import_and_usage(tmp_path: Path):
    """Import durable_job_queue from outside the repo and enqueue a job."""
    db = tmp_path / "external.db"
    code = f"""
import json
from durable_job_queue import JobQueue, JobStatus

q = JobQueue({str(db)!r})
job = q.enqueue(job_type="resize-image", payload={{"file": "photo.jpg"}})
print(job.id)
assert q.get(job.id).status is JobStatus.PENDING
q.close()
"""
    # cwd outside the repo tree: use the system temp dir.
    outside = Path(tempfile.gettempdir())
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(outside),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        f"external consumer failed:\nstdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
    job_id = result.stdout.strip().splitlines()[-1]
    assert len(job_id) > 0

    # The job is durably in the database.
    from durable_job_queue import JobQueue

    q = JobQueue(str(db))
    assert q.get(job_id).job_type == "resize-image"
    q.close()
