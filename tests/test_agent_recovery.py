import inspect

import httpx
import pytest
from fastapi.testclient import TestClient

from hivemind.recovery import (
    COMPLETED,
    PENDING,
    UNKNOWN,
    Journal,
    JournalConflict,
    SensitivePayload,
    UnknownOutcome,
    WorkspaceViolation,
    command_key,
    derive_operation_id,
    file_digest,
    payload_checksum,
)
from hivemind.sdk import Client


class FakeClient:
    """Stand-in for hivemind.sdk.Client with the same request() contract."""

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def request(self, method, path, body=None, key=None):
        self.calls.append({"method": method, "path": path, "body": body, "key": key})
        outcome = self.outcomes.pop(0) if self.outcomes else {"ok": True}
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def status_error(status, message="rejected"):
    request = httpx.Request("POST", "http://127.0.0.1:8765/v0.1/runs")
    response = httpx.Response(status, request=request, json={"error": message})
    return httpx.HTTPStatusError(message, request=request, response=response)


def test_completed_operation_replays_across_fresh_connections(tmp_path):
    path = tmp_path / "journal.sqlite"
    body = {"project_id": "p", "agent_id": "a"}
    with Journal(path) as journal:
        client = FakeClient({"id": "run_1", "status": "running"})
        assert journal.request(client, "POST", "runs", body, "op-run") == {"id": "run_1", "status": "running"}
        assert client.calls[0]["key"] == command_key("op-run")
        record = journal.operation("op-run")
        assert record["state"] == COMPLETED and record["attempts"] == 1 and record["attempting"] is False
        assert record["request_hash"] == payload_checksum("POST", "runs", body)
    with Journal(path) as restarted:
        replay = FakeClient({"id": "run_2"})
        assert restarted.request(replay, "POST", "runs", body, "op-run") == {"id": "run_1", "status": "running"}
        assert replay.calls == []
        assert restarted.operation("op-run")["result"] == {"id": "run_1", "status": "running"}


def test_stable_command_keys_are_deterministic_across_restarts(tmp_path):
    path = tmp_path / "journal.sqlite"
    op = derive_operation_id("session-1", "export", 3)
    assert derive_operation_id("session-1", "export", 3) == op != derive_operation_id("session-1", "export", 4)
    with Journal(path) as journal:
        client = FakeClient(httpx.ReadTimeout("slow"))
        with pytest.raises(httpx.ReadTimeout):
            journal.request(client, "POST", "posts", {"text": "x"}, op)
        first_key = client.calls[0]["key"]
    with Journal(path) as restarted:
        assert restarted.operation(op)["state"] == UNKNOWN
        retry = FakeClient({"id": "post_1"})
        with pytest.raises(UnknownOutcome):
            restarted.request(retry, "POST", "posts", {"text": "x"}, op)
        assert retry.calls == []
        assert restarted.request(retry, "POST", "posts", {"text": "x"}, op, safe_to_retry=True) == {"id": "post_1"}
        assert retry.calls[0]["key"] == first_key == command_key(op)
        record = restarted.operation(op)
        assert record["attempts"] == 2 and record["state"] == COMPLETED and record["retryable"] is True


def test_read_only_methods_retry_ambiguous_outcomes_automatically(tmp_path):
    with Journal(tmp_path / "journal.sqlite") as journal:
        client = FakeClient(httpx.ConnectError("down"), [{"id": "agent_1"}])
        with pytest.raises(httpx.ConnectError):
            journal.request(client, "GET", "agents?project_id=p", None, "op-get")
        record = journal.operation("op-get")
        assert record["state"] == UNKNOWN and record["retryable"] is True
        assert journal.request(client, "GET", "agents?project_id=p", None, "op-get") == [{"id": "agent_1"}]
        assert journal.operation("op-get")["state"] == COMPLETED


def test_changed_payload_for_a_recorded_operation_is_refused(tmp_path):
    with Journal(tmp_path / "journal.sqlite") as journal:
        client = FakeClient({"id": "post_1"})
        journal.request(client, "POST", "posts", {"text": "one"}, "op-post")
        original = journal.operation("op-post")
        with pytest.raises(JournalConflict):
            journal.request(client, "POST", "posts", {"text": "two"}, "op-post")
        with pytest.raises(JournalConflict):
            journal.request(client, "POST", "boards/b/posts", {"text": "one"}, "op-post")
        assert len(client.calls) == 1
        assert journal.operation("op-post")["request_hash"] == original["request_hash"]


def test_definite_rejection_stays_pending_and_allows_a_same_payload_retry(tmp_path):
    with Journal(tmp_path / "journal.sqlite") as journal:
        client = FakeClient(status_error(409, "already started"), {"id": "run_1"})
        with pytest.raises(httpx.HTTPStatusError):
            journal.request(client, "POST", "runs", {"agent_id": "a"}, "op-start")
        record = journal.operation("op-start")
        assert record["state"] == PENDING and record["attempting"] is False and record["attempts"] == 1
        assert record["error"] == {"kind": "http_status", "status": 409}
        assert journal.request(client, "POST", "runs", {"agent_id": "a"}, "op-start") == {"id": "run_1"}


def test_interrupted_attempt_is_unknown_after_restart(tmp_path):
    path = tmp_path / "journal.sqlite"
    with Journal(path) as journal:
        journal.begin_operation("op-write", "POST", "posts", {"text": "x"})
        journal.mark_attempt("op-write")
        assert journal.operation("op-write") == journal.operation("op-write")
    with Journal(path) as restarted:
        record = restarted.operation("op-write")
        assert record["state"] == UNKNOWN and record["attempting"] is False
        assert record["error"] == {"kind": "interrupted"}


def test_sensitive_request_body_fails_closed_and_redacts_under_explicit_policy(tmp_path):
    path = tmp_path / "journal.sqlite"
    secret = "Bearer sk-live-abcdefghijklmnop"
    body = {"title": "handoff", "text": secret}
    with Journal(path) as journal:
        with pytest.raises(SensitivePayload):
            journal.request(FakeClient({"id": "m1"}), "POST", "memories", body, "op-mem")
        assert journal.operation("op-mem") is None
        client = FakeClient(httpx.ReadTimeout("slow"))
        with pytest.raises(httpx.ReadTimeout):
            journal.request(client, "POST", "memories", body, "op-mem", allow_sensitive_payload=True)
        record = journal.operation("op-mem")
        assert record["body_redacted"] is True and record["body_persisted"] is False
        assert "sk-live" not in record["body"]
        assert record["request_hash"] == payload_checksum("POST", "memories", body)
        # The exact original payload still matches its checksum, so retries do not conflict.
        with pytest.raises(UnknownOutcome):
            journal.request(FakeClient(), "POST", "memories", body, "op-mem")
    raw = path.read_bytes()
    assert b"sk-live" not in raw


def test_exception_diagnostics_are_never_persisted(tmp_path):
    path = tmp_path / "journal.sqlite"
    with Journal(path) as journal:
        leak = "ReadTimeout: http://127.0.0.1/?api_key=sk-live-abcdefghijklmnop"
        client = FakeClient(httpx.ReadTimeout(leak))
        with pytest.raises(httpx.ReadTimeout):
            journal.request(client, "POST", "posts", {"text": "safe"}, "op-posts")
        record = journal.operation("op-posts")
        assert record["error"] == {"kind": "timeout", "type": "ReadTimeout"}
        assert "sk-live" not in str(record)
    assert b"sk-live" not in path.read_bytes()


def test_reconcile_reports_file_drift_and_never_rewrites_files(tmp_path):
    workspace = tmp_path / "ws"
    (workspace / "src").mkdir(parents=True)
    (workspace / "src" / "app.py").write_text("one")
    kept = workspace / "src" / "kept.py"
    kept.write_text("stable")
    gone = workspace / "src" / "gone.py"
    gone.write_text("bye")
    journal_path = tmp_path / "journal.sqlite"
    with Journal(journal_path, workspace=workspace) as journal:
        journal.checkpoint("before export", files=["src/app.py", "src/kept.py", "src/gone.py"],
                           operation_id="op-export", state={"step": 1})
        journal.begin_operation("op-export", "POST", "posts", {"text": "x"})
        journal.mark_attempt("op-export")
        (workspace / "src" / "app.py").write_text("two")
        (workspace / "src" / "new.py").write_text("created")
        gone.unlink()
    before = {name: (workspace / name).read_text() for name in ("src/app.py", "src/kept.py")}
    with Journal(journal_path, workspace=workspace) as restarted:
        assert restarted.operation("op-export")["state"] == UNKNOWN
        report = restarted.reconcile()
        assert report["files"]["modified"] == ["src/app.py"]
        assert report["files"]["deleted"] == ["src/gone.py"]
        assert report["files"]["unchanged"] == ["src/kept.py"]
        assert report["checked_files"] == 3
        assert report["unknown"] == ["op-export"]
        assert report["unsafe_to_rerun"] == ["op-export"]
        assert report["uncertain_side_effects"] == [{
            "operation_id": "op-export", "method": "POST", "path": "posts", "label": None,
            "files": ["src/app.py", "src/gone.py"], "retryable": False, "reason": "ambiguous_outcome"}]
    assert {name: (workspace / name).read_text() for name in ("src/app.py", "src/kept.py")} == before
    assert not (workspace / "src" / "gone.py").exists()


def test_file_reconciliation_is_workspace_contained(tmp_path):
    workspace = tmp_path / "ws"
    workspace.mkdir()
    (tmp_path / "outside.txt").write_text("outside")
    with Journal(tmp_path / "no-workspace.sqlite") as bare:
        with pytest.raises(WorkspaceViolation):
            bare.checkpoint(files=["anything.txt"])
    with Journal(tmp_path / "journal.sqlite", workspace=workspace) as journal:
        for path in ("../outside.txt", str(tmp_path / "outside.txt"), ".env", "."):
            with pytest.raises(WorkspaceViolation):
                journal.checkpoint(files=[path])
        with pytest.raises(WorkspaceViolation):
            journal.reconcile(files=[str(tmp_path / "outside.txt")])


def test_runtime_and_state_callbacks_are_injected_and_reconciled(tmp_path):
    workspace = tmp_path / "ws"
    workspace.mkdir()
    (workspace / "app.py").write_text("code")
    runtime = {"runtime": "droid", "run_id": "run_1"}
    state = {"step": 1}
    seen = []
    journal_path = tmp_path / "journal.sqlite"
    with Journal(journal_path, workspace=workspace, runtime_provider=lambda: runtime,
                 state_provider=lambda: state,
                 on_event=lambda kind, record: seen.append(kind)) as journal:
        record = journal.checkpoint("start", files=["app.py"])
        assert record["runtime"] == {"runtime": "droid", "run_id": "run_1"}
        assert record["app_state"] == {"step": 1}
        assert record["files"][0]["path"] == "app.py"
        assert record["files"][0]["sha256"] == file_digest(workspace / "app.py")[0]
        report = journal.reconcile()
        assert report["runtime_drift"] == {} and report["state_drift"] == {}
        runtime["run_id"] = "run_2"
        state["step"] = 2
        report = journal.reconcile()
        assert report["runtime_drift"] == {"run_id": {"checkpoint": "run_1", "current": "run_2"}}
        assert report["state_drift"] == {"step": {"checkpoint": 1, "current": 2}}
        assert "checkpoint" in seen and "reconcile" in seen


def test_manual_begin_complete_and_resolve_keep_the_stable_key(tmp_path):
    with Journal(tmp_path / "journal.sqlite") as journal:
        record = journal.begin_operation("op-manual", "POST", "memories", {"title": "t"})
        assert record["state"] == PENDING and record["attempts"] == 0
        journal.mark_attempt("op-manual")
        done = journal.complete_operation("op-manual", {"id": "mem_1"})
        assert done["state"] == COMPLETED and done["result"] == {"id": "mem_1"}
        assert done["command_key"] == command_key("op-manual")
        journal.begin_operation("op-review", "POST", "memories", {"title": "u"})
        journal.mark_attempt("op-review")
        journal.fail_operation("op-review", httpx.ReadTimeout("slow"))
        assert journal.operation("op-review")["state"] == UNKNOWN
        assert journal.resolve_operation("op-review", COMPLETED, {"id": "mem_2"})["state"] == COMPLETED


def test_operations_filter_and_event_callback(tmp_path):
    events = []
    with Journal(tmp_path / "journal.sqlite", on_event=lambda kind, record: events.append(kind)) as journal:
        journal.request(FakeClient({"id": "a"}), "POST", "posts", {"text": "x"}, "op-a")
        journal.begin_operation("op-b", "POST", "posts", {"text": "y"})
        journal.mark_attempt("op-b")
        journal.fail_operation("op-b", httpx.ReadTimeout("slow"))
        assert [item["operation_id"] for item in journal.operations(COMPLETED)] == ["op-a"]
        assert [item["operation_id"] for item in journal.operations(UNKNOWN)] == ["op-b"]
        assert "operation_completed" in events and "operation_unknown" in events


def test_journal_wrapper_matches_the_sdk_client_request_contract():
    parameters = inspect.signature(Client.request).parameters
    assert list(parameters) == ["self", "method", "path", "body", "key"]
    assert parameters["body"].default is None and parameters["key"].default is None


class AmbiguousClient:
    """Real client whose first reply is lost after the server applied the command."""

    def __init__(self, inner):
        self.inner = inner
        self.calls = 0

    def request(self, method, path, body=None, key=None):
        self.calls += 1
        result = self.inner.request(method, path, body, key=key)
        if self.calls == 1:
            raise httpx.ReadTimeout("reply lost")
        return result


def bound_client(world):
    """The real SDK client wired to the in-process service transport."""
    client = Client("http://testserver/v0.1/", world.owner)
    client.http.close()
    client.http = TestClient(world.app, base_url="http://testserver/v0.1/",
                             headers={"Authorization": f"Bearer {world.owner}"})
    return client


def agent_body(world, name):
    return {"project_id": world.project, "display_name": name,
            "supported_actions": [], "scope_permit_enforcement": False}


def test_journal_wraps_the_real_client_and_replays_after_restart(world, tmp_path):
    client = bound_client(world)
    body = agent_body(world, "Journal worker")
    path = tmp_path / "journal.sqlite"
    with Journal(path) as journal:
        created = journal.request(client, "POST", "agents", body, "op-agent")
        assert created["display_name"] == "Journal worker"
    with Journal(path) as restarted:
        assert restarted.request(client, "POST", "agents", body, "op-agent") == created
    assert [agent["id"] for agent in world.call("GET", f"agents?project_id={world.project}")] == [created["id"]]


def test_lost_reply_is_ambiguous_but_a_stable_key_retry_does_not_duplicate(world, tmp_path):
    client = AmbiguousClient(bound_client(world))
    body = agent_body(world, "Ambiguous worker")
    with Journal(tmp_path / "journal.sqlite") as journal:
        with pytest.raises(httpx.ReadTimeout):
            journal.request(client, "POST", "agents", body, "op-ambiguous")
        record = journal.operation("op-ambiguous")
        assert record["state"] == UNKNOWN and record["attempts"] == 1
        with pytest.raises(UnknownOutcome):
            journal.request(client, "POST", "agents", body, "op-ambiguous")
        assert client.calls == 1
        recovered = journal.request(client, "POST", "agents", body, "op-ambiguous", safe_to_retry=True)
        assert recovered["display_name"] == "Ambiguous worker"
        assert client.calls == 2
    assert [agent["id"] for agent in world.call("GET", f"agents?project_id={world.project}")] == [recovered["id"]]
