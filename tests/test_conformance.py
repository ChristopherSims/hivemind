import json
import sqlite3
from pathlib import Path

import pytest
import sqlite_vec
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry, Resource

from hivemind.indexes import vector_blob
from scripts.export_schemas import export


@pytest.fixture(scope="module")
def schemas():
    export()
    directory = Path(__file__).resolve().parent.parent / "spec" / "schemas"
    entries = {}
    for file in directory.glob("*.json"):
        schema = json.loads(file.read_text())
        schema["$id"] = file.as_uri()
        Draft202012Validator.check_schema(schema)
        entries[file.stem.removesuffix(".schema")] = schema
    registry = Registry().with_resources((s["$id"], Resource.from_contents(s)) for s in entries.values())
    return entries, registry


def validator(schemas, name):
    entries, registry = schemas
    return Draft202012Validator(entries[name], registry=registry, format_checker=FormatChecker())


def test_published_content_and_selector_variants(schemas):
    content = validator(schemas, "Content")
    content.validate({"media_type": "text/plain", "text": "A finding"})
    content.validate({"media_type": "application/vnd.hivemind.chat+json", "segments": [
        {"kind": "agent_mention", "agent_id": "agent"}, {"kind": "text", "text": " Please reply"}]})
    for invalid in (
        {"media_type": "text/plain", "segments": [{"kind": "text", "text": "bad"}]},
        {"media_type": "application/vnd.hivemind.chat+json", "text": "bad"},
        {"media_type": "text/plain", "text": "ok", "unknown": True},
    ):
        with pytest.raises(ValidationError):
            content.validate(invalid)
    selector = validator(schemas, "Selector")
    selector.validate({"agent_ids": ["agent"]})
    selector.validate({"semantic_query": "navbar"})
    selector.validate({"agent_ids": ["agent"], "semantic_query": "navbar", "match": "intersection"})
    for invalid in ({}, {"agent_ids": ["a", "a"]}, {"semantic_query": " "},
                    {"agent_ids": ["a"], "match": "intersection"}):
        with pytest.raises(ValidationError):
            selector.validate(invalid)


def test_event_discriminator_and_real_timestamp_formats(schemas):
    schema = validator(schemas, "Event")
    event = {"event_id": "event", "agent_id": "agent", "run_id": "run", "project_id": "project",
             "occurred_at": "2026-10-09T12:00:00Z", "type": "hivemind.context.tool.started",
             "source": {"source_id": "source", "kind": "runtime_hook", "sequence": 0},
             "data": {"call_id": "call", "name": "edit"}}
    schema.validate(event)
    for invalid in ({**event, "data": {"call_id": "call", "unrecognized": True}},
                    {**event, "occurred_at": "2026-02-30T12:00:00Z"},
                    {**event, "type": "hivemind.coordination.control.applied"}):
        with pytest.raises(ValidationError):
            schema.validate(invalid)
    path_schema = validator(schemas, "FileData")
    for path in ("../secret", "C:\\secret", "/secret", "\\\\host\\share"):
        with pytest.raises(ValidationError):
            path_schema.validate({"path": path, "change": "modified"})


def test_nested_content_schema_cannot_bypass_variants(schemas):
    schema = validator(schemas, "PostCreate")
    with pytest.raises(ValidationError):
        schema.validate({"client_post_id": "client", "content": {
            "media_type": "text/plain", "segments": [{"kind": "text", "text": "invalid"}]}})


def test_delivery_receipts_and_bounded_read_request_schemas(schemas):
    receipt = validator(schemas, "DeliveryFailure")
    receipt.validate({"run_id": "run", "delivery_id": "receipt", "reason": "handler_failed"})
    for invalid in ({"run_id": "run", "reason": "handler_failed"},
                    {"run_id": "run", "delivery_id": "receipt", "reason": "arbitrary diagnostic"}):
        with pytest.raises(ValidationError):
            receipt.validate(invalid)
    reads = validator(schemas, "ConversationsRead")
    reads.validate({"message_ids": ["message"]})
    for invalid in ({"message_ids": []}, {"message_ids": ["message"] * 101}):
        with pytest.raises(ValidationError):
            reads.validate(invalid)


def test_extraction_schema_never_accepts_verified_flags_or_unbounded_sources(schemas):
    schema = validator(schemas, "MemoryExtract")
    body = {"project_id": "project", "agent_id": "agent",
            "source_refs": [{"kind": "board_post", "id": "finding", "revision": 1}]}
    schema.validate(body)
    for invalid in ({**body, "status": "verified"}, {**body, "source_refs": body["source_refs"] * 17},
                    {**body, "source_refs": [{"kind": "memory", "id": "memory"}]}):
        with pytest.raises(ValidationError):
            schema.validate(invalid)


def test_sqlite_vec_transaction_rollback_and_consistent_backup(world, tmp_path):
    store = world.app.state.store
    vector = vector_blob([1.0] + [0.0] * 767)
    with pytest.raises(RuntimeError), store.transaction():
        store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                         (100, vector, world.project))
        raise RuntimeError("Simulated precommit failure")
    assert store.db.execute("SELECT count(*) FROM vectors WHERE chunk_id=100").fetchone()[0] == 0
    destination = tmp_path / "backup.sqlite"
    with sqlite3.connect(destination) as backup:
        store.db.backup(backup)
        backup.enable_load_extension(True)
        sqlite_vec.load(backup)
        backup.enable_load_extension(False)
        assert backup.execute("SELECT count(*) FROM projects").fetchone()[0] == 1
        assert backup.execute("SELECT count(*) FROM vectors").fetchone()[0] == 0
