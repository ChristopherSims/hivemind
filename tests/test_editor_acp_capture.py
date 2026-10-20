import io
import json
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from hivemind import models as m
from hivemind.editor_capture import EditorRpcCapture, JsonLinesCapture


DATA = {"session_sha256": "a" * 64, "connection_id": "acp_123",
        "delivery": "live", "observation": "plan",
        "entries": [{"content_sha256": "b" * 64, "status": "pending", "priority": "high"}]}


def test_acp_metadata_rejects_raw_text_and_confirmed_attribution():
    assert m.AcpObservationData.model_validate(DATA).entries[0].status == "pending"
    with pytest.raises(ValueError):
        m.AcpObservationData.model_validate({**DATA, "text": "private content"})
    with pytest.raises(ValueError):
        m.Event(event_id="event_1", type="hivemind.context.editor.acp_observed",
                agent_id="agent_1", run_id="run_1", project_id="project_1",
                occurred_at="2026-10-14T00:00:00Z",
                source={"source_id": "source_1", "kind": "sdk", "sequence": 1,
                        "attribution": "confirmed"}, data=DATA)


def test_buffer_observation_cannot_claim_agent_authorship():
    with pytest.raises(ValueError):
        m.AcpObservationData.model_validate({**DATA, "observation": "file_buffer_observed",
            "path": "src/test.py", "sha256": "c" * 64, "byte_size": 4,
            "editor_observation": "changed"})


def fixture_capture(denied=False, retained=False, status="running"):
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE spool(id TEXT, state TEXT)")
    calls = []
    def request(method, path, body=None):
        calls.append((method, path, body))
        if denied:
            raise PermissionError()
        if path.endswith("permits:check"):
            return {"run_id": "run_1", "scope_ids": ["scope_1"], "expires_at": time.time() + 10}
        return {"project_id": "project_1", "agent_id": "agent_1", "status": status}
    def observe(*args, **kwargs):
        db.execute("INSERT INTO spool VALUES('event_1','pending')")
        return "event_1"
    def flush():
        if not retained:
            db.execute("DELETE FROM spool")
    capture = SimpleNamespace(db=db, lock=threading.RLock(),
                              client=SimpleNamespace(request=request), observe=observe, flush=flush)
    rpc = EditorRpcCapture(capture, {"project_id": "project_1", "agent_id": "agent_1",
                                    "run_id": "run_1", "scope_ids": ["scope_1"]})
    return rpc, calls


def test_rpc_ack_requires_live_owned_run_permit_and_server_accepted_event():
    rpc, calls = fixture_capture()
    assert rpc.handle({"request_id": "request_1", "operation": "observe",
                       "payload": {"type": "editor.acp_observed", "data": DATA}})["ok"]
    assert calls[1][1] == "runs/run_1/permits:check"
    assert calls[1][2] == {"scope_ids": ["scope_1"], "paths": []}
    for options in ({"denied": True}, {"retained": True}, {"status": "completed"}):
        rpc, _ = fixture_capture(**options)
        with pytest.raises(PermissionError):
            rpc.handle({"request_id": "request_1", "operation": "authorize", "payload": {}})


def test_rpc_permit_cannot_supply_another_identity_or_path_traversal():
    rpc, calls = fixture_capture()
    assert rpc.handle({"request_id": "request_1", "operation": "permit",
                       "payload": {"path": "src/test.py"}})["ok"]
    assert calls[-1][2]["paths"] == ["src/test.py"]
    for payload in ({"path": "../private"}, {"path": "src/test.py", "run_id": "foreign"}):
        with pytest.raises(ValueError):
            rpc.handle({"request_id": "request_1", "operation": "permit", "payload": payload})


@pytest.mark.parametrize("permit", [
    {"run_id": "foreign", "scope_ids": ["scope_1"], "expires_at": time.time() + 10},
    {"run_id": "run_1", "scope_ids": ["foreign"], "expires_at": time.time() + 10},
    {"run_id": "run_1", "scope_ids": ["scope_1"], "expires_at": 0},
    {"allowed": True},
])
def test_rpc_cannot_use_unbound_or_expired_permission_metadata(permit):
    rpc, _ = fixture_capture()
    original = rpc.capture.client.request
    rpc.capture.client.request = lambda method, path, body=None: (
        permit if path.endswith("permits:check") else original(method, path, body))
    with pytest.raises(PermissionError):
        rpc.authorize()


def test_acp_file_events_are_excluded_and_require_specific_path_permits():
    rpc, calls = fixture_capture()
    data = {**DATA, "observation": "file_read", "actor": "editor_observed",
            "path": "src/test.py", "sha256": "c" * 64, "byte_size": 1}
    rpc.handle({"request_id": "request_1", "operation": "observe",
                "payload": {"type": "editor.acp_observed", "data": data}})
    assert calls[-1][2]["paths"] == ["src/test.py"]
    capture = SimpleNamespace(observe=lambda *args, **kwargs: pytest.fail("Excluded file captured"))
    assert JsonLinesCapture(capture).ingest(json.dumps({
        "type": "editor.acp_observed", "data": {**data, "path": ".env"}}).encode()) is None


def test_rpc_oversized_frame_is_drained_and_errors_do_not_echo_content():
    rpc, _ = fixture_capture()
    output = io.StringIO()
    rpc.consume(io.BytesIO(b"private-content" * 2000 + b"\n" +
        json.dumps({"request_id": "request_1", "operation": "authorize", "payload": {}}).encode() + b"\n"),
        output)
    replies = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [reply["ok"] for reply in replies] == [False, True]
    assert "private-content" not in output.getvalue()


def test_legacy_json_lines_adapter_accepts_only_reported_acp_metadata():
    captured = []
    capture = SimpleNamespace(observe=lambda *args, **kwargs: captured.append((args, kwargs)))
    JsonLinesCapture(capture).ingest(json.dumps({"type": "editor.acp_observed", "data": DATA}).encode())
    assert captured[0][1] == {"attribution": "reported", "kind": "sdk"}
