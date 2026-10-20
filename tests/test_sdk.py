import json

import pytest

from hivemind.models import Content, Selector
from hivemind.sdk import Capture, ControlledAdapter, excluded, redact


class FakeClient:
    def __init__(self):
        self.batches = []

    def request(self, method, path, body=None, key=None):
        self.batches.append(body)
        return [{"accepted": True, "event_id": e["event_id"]} for e in body["events"]]


def test_redacted_spool_survives_restart_and_preserves_ids(tmp_path):
    client = FakeClient()
    path = tmp_path / "spool.sqlite"
    capture = Capture(client, "project", "agent", "run", path, start=False)
    identity = capture.observe("agent.status_reported", {"text": "password=do-not-store-this"})
    assert not client.batches
    raw = capture.db.execute("SELECT payload FROM spool").fetchone()[0]
    assert "do-not-store-this" not in raw and "REDACTED" in raw
    capture.db.close()
    restarted = Capture(client, "project", "agent", "run", path, start=False)
    restarted.flush()
    assert client.batches[0]["events"][0]["event_id"] == identity
    assert restarted.db.execute("SELECT count(*) FROM spool").fetchone()[0] == 0
    restarted.close()


def test_content_selector_and_secret_exclusions():
    assert excluded("src/.env.production")
    assert excluded("node_modules/package/file")
    assert not excluded("src/header.ts")
    assert "actual" not in redact("Authorization: Bearer actual")
    with pytest.raises(ValueError):
        Content(media_type="text/plain", segments=[{"kind": "text", "text": "oops"}])
    with pytest.raises(ValueError):
        Selector(agent_ids=["a", "a"])
    with pytest.raises(ValueError):
        Selector(agent_ids=["a"], match="intersection")
    with pytest.raises(ValueError):
        Content(media_type="text/plain", text="ü" * 65536)


def test_control_adapter_checkpoint_and_duplicate_application(tmp_path):
    control = {"id": "ctl", "action": "steer", "replacement_task_id": "footer",
               "reserved_claim_id": "claim"}
    class ControlClient:
        def __init__(self):
            self.results = []
        def request(self, method, path, body=None, key=None):
            if "/controls?" in path:
                return [control]
            if path.startswith("work-claims"):
                return [{"id": "claim", "scope_ids": ["footer-scope"]}]
            if path.endswith(":result"):
                # Local state has already been durably changed before reporting application.
                assert json.loads((tmp_path / "state.json").read_text())["task_id"] == "footer"
                self.results.append(body)
            return {}
    client = ControlClient()
    adapter = ControlledAdapter(client, "project", "agent", "run", tmp_path / "state.json")
    adapter.boundary()
    adapter.boundary()
    assert not client.results
    adapter.confirm_transition()
    adapter.boundary()
    assert len(client.results) == 1
    assert adapter.state["scope_ids"] == ["footer-scope"]


def test_mcp_tools_load():
    import asyncio
    from hivemind.mcp import server
    tools = asyncio.run(server.list_tools())
    assert len(tools) >= 10
    assert "hivemind_query_context" in {t.name for t in tools}


@pytest.mark.parametrize("action", ["pause", "resume", "cancel", "steer", "assign_task"])
def test_every_supported_adapter_control_has_durable_transition_evidence(tmp_path, action):
    path = tmp_path / "state.json"
    results = []
    control = {"id": "control", "action": action, "target_run_id": "run",
               "replacement_task_id": "replacement", "reserved_claim_id": "claim"}
    class Client:
        def request(self, method, route, body=None, key=None):
            if "/controls?" in route:
                return [control]
            if route.startswith("work-claims"):
                return [{"id": "claim", "scope_ids": ["replacement-scope"]}]
            if route.endswith(":result"):
                saved = json.loads(path.read_text())
                assert saved["pending_results"]["control"] == body
                assert body["checkpoint_id"]
                results.append(body)
            return {}
    adapter = ControlledAdapter(Client(), "project", "agent", "run", path)
    adapter.boundary()
    if action in ("steer", "assign_task"):
        assert not results
        assert adapter.state["task_id"] == "replacement"
        adapter.confirm_transition()
    assert len(results) == 1
    expected = {"pause": "paused", "cancel": "cancelled"}.get(action, "running")
    assert results[0]["observed_status"] == expected
    adapter.boundary()
    assert len(results) == 1


@pytest.mark.parametrize("extra", [{"action": "unknown"}, {"action": "pause", "target_run_id": "foreign"}])
def test_unknown_or_foreign_control_never_becomes_running_success(tmp_path, extra):
    writes = []
    class Client:
        def request(self, method, route, body=None, key=None):
            if method == "GET":
                return [{"id": "control", **extra}]
            writes.append((route, body))
    adapter = ControlledAdapter(Client(), "project", "agent", "run", tmp_path / "state.json")
    with pytest.raises(RuntimeError):
        adapter.boundary()
    assert not writes and not adapter.state["applied"]
