"""Built-in demo handler registry (spec §19/§20).

A handler is a callable: (payload: dict) -> None. Raising an exception
reports a failed attempt (retry/dead per policy).

This is intentionally a small fixed registry, not a plugin framework.
"""

from __future__ import annotations

import time
from typing import Callable

Handler = Callable[[dict], None]


def echo(payload: dict) -> None:
    """Print the payload (demonstrates successful processing)."""
    print(f"[echo] {payload!r}", flush=True)


def sleep(payload: dict) -> None:
    """Sleep for payload['seconds'] (default 1). Demonstrates slow jobs."""
    seconds = float(payload.get("seconds", 1))
    time.sleep(seconds)


def write_file(payload: dict) -> None:
    """Write payload['text'] to payload['path']. Demonstrates side effects."""
    path = payload["path"]
    text = payload.get("text", "")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def always_fail(payload: dict) -> None:
    """Always raises. Demonstrates retry -> dead via the CLI."""
    raise RuntimeError("always-fail handler: intentional failure")


HANDLERS: dict[str, Handler] = {
    "echo": echo,
    "sleep": sleep,
    "write-file": write_file,
    "always-fail": always_fail,
}


def handler_for(job_type: str) -> Handler:
    try:
        return HANDLERS[job_type]
    except KeyError:
        raise KeyError(
            f"no handler registered for job_type {job_type!r}; "
            f"available: {', '.join(sorted(HANDLERS))}"
        ) from None
