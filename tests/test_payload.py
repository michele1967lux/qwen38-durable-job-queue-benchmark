"""T13 — invalid payload: controlled error, database unchanged.

Plus payload serialization round-trip guarantees.
"""

from __future__ import annotations

import pytest

from durable_job_queue import InvalidPayload, JobQueue


def test_unserializable_payload_rejected(db_path, clock):
    """T13: a payload that cannot be JSON-serialized raises InvalidPayload."""
    q = JobQueue(str(db_path), clock=clock)
    with pytest.raises(InvalidPayload):
        q.enqueue(job_type="echo", payload={"bad": object()})
    # Database unchanged: no job was created.
    assert q.list() == []
    q.close()


def test_non_dict_payload_rejected(db_path, clock):
    """Payloads must be JSON objects (dicts) — documented contract."""
    q = JobQueue(str(db_path), clock=clock)
    with pytest.raises(InvalidPayload):
        q.enqueue(job_type="echo", payload=[1, 2, 3])
    with pytest.raises(InvalidPayload):
        q.enqueue(job_type="echo", payload="just a string")
    assert q.list() == []
    q.close()


def test_payload_roundtrip_types(db_path, clock):
    """Numbers, strings, booleans, null, nested structures survive storage."""
    q = JobQueue(str(db_path), clock=clock)
    payload = {
        "int": 42,
        "float": 3.14,
        "str": "hello",
        "bool_true": True,
        "bool_false": False,
        "null": None,
        "list": [1, "two", {"three": 3}],
        "nested": {"a": {"b": [None, True]}},
        "unicode": "ciao — 世界",
    }
    job = q.enqueue(job_type="echo", payload=payload)
    reloaded = q.get(job.id)
    assert reloaded.payload == payload
    q.close()


def test_empty_payload_allowed(db_path, clock):
    q = JobQueue(str(db_path), clock=clock)
    job = q.enqueue(job_type="echo", payload={})
    assert q.get(job.id).payload == {}
    q.close()


def test_invalid_job_type_rejected(db_path, clock):
    """job_type must be a non-empty string."""
    q = JobQueue(str(db_path), clock=clock)
    with pytest.raises(InvalidPayload):
        q.enqueue(job_type="", payload={})
    with pytest.raises(InvalidPayload):
        q.enqueue(job_type=123, payload={})
    assert q.list() == []
    q.close()
