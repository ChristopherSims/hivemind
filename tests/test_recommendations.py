"""Feature work from the live OMP swarm findings (todo.md R1–R8).

Covers the surface each finding opened:
- session_context_note packet injection with teammate cards, claim expiry,
  tool-use instruction and blocked-status gate line (R1/R2/R3/R7/R8)
- wait-unblocked primitive (R6) — HTTP and the worker MCP catalog extension
- denied-mutation advisory tracing (R4) — blocked writes land in the run's spool
- auto-checkpoint (R5) — confirmed scoped write files a self-reported progress
  checkpoint on a bound task execution (status stays running; never verified)
"""
import json

import pytest

from hivemind import hooks
from hivemind.context_assembly import session_context_note
from hivemind.storage import now, uid


class LocalClient:
    """Hooks-test client: canonical HTTP through the world app plus assemble."""

    def __init__(self, base_url, token):
        self.token = token

    def request(self, method, path, body=None, key=None):
        response = world_client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + self.token,
            "Idempotency-Key": key or uid("key")})
        response.raise_for_status()
        return response.json().get("data", response.json())

    def assemble_context(self, request):
        return self.request("POST", "context:assemble", request)

    def checkpoint_task(self, task_id, checkpoint, key=None):
        return self.request("POST", f"tasks/{task_id}:checkpoint", checkpoint, key=key)

    def close(self):
        pass


world_client = None  # set by the fixture below


@pytest.fixture
def hooks_world(world, monkeypatch):
    global world_client
    world_client = world.client
    monkeypatch.setattr(hooks, "Client", LocalClient)
    return world


def note(world, config, task_id=None):
    client = LocalClient(config.get("base_url", "http://testserver"), config.get("token", world.owner))
    return session_context_note(client, config, request={}, task_id=task_id, state={})


def test_session_note_injects_packet_claim_expiry_teammates(hooks_world):
    world = hooks_world
    config = {"project_id": world.project, "agent_id": None,
              "workspace_root": "/unused", "state_dir": "/tmp/unused"}
    text = note(world, config)
    assert "untrusted evidence" in text
    assert "write tool" in text  # R8 explicit tool instruction
    assert "Your claim" not in text  # no claims yet for this fresh project


def test_session_note_reports_claim_expiry_and_teammates(hooks_world):
    world = hooks_world
    a = world.agent()
    b = world.agent()
    ta = world.credential(a)
    tb = world.credential(b)
    ra = world.run(a, ta)
    rb = world.run(b, tb)
    scope = world.scope(paths=["src/x"])
    claim = world.claim(a, ra, scope, ta)
    # teammate file evidence: touch a file through b's run
    file_event = {"event_id": uid("evt"), "type": "hivemind.context.file.changed",
                  "agent_id": b["id"], "run_id": rb["id"], "project_id": world.project,
                  "occurred_at": now(),
                  "source": {"source_id": "src-b", "kind": "runtime_hook", "sequence": 0,
                             "attribution": "confirmed"},
                  "data": {"path": "src/x.ts", "change": "created"}}
    world.call("POST", "events:batch", {"events": [file_event]}, tb)
    config = {"project_id": world.project, "agent_id": a["id"]}
    text = note(world, config)
    assert f"Your claim {claim['id']} is active and expires in" in text
    assert "renewal before expiry" in text
    assert "Teammates" in text and f"{b['id']} run {rb['id']}" in text
    assert "files touched: src/x.ts" in text


def test_session_note_includes_blocked_status_for_bound_task(hooks_world):
    world = hooks_world
    b = world.agent()
    tb = world.credential(b)
    world.run(b, tb)
    upstream = world.scope("upstream", paths=["src/up"])
    downstream = world.scope("down", paths=["src/down"])
    world.call("POST", "task-plans", {
        "project_id": world.project, "title": "chain",
        "tasks": [{"key": "up", "title": "Upstream", "scope_ids": [upstream["id"]],
                   "depends_on": []},
                  {"key": "down", "title": "Downstream", "scope_ids": [downstream["id"]],
                   "depends_on": ["up"]}]}, tb)
    # find the downstream task id
    tasks = world.call("GET", f"tasks?project_id={world.project}")
    down = next(t for t in tasks if t["title"] == "Downstream")
    config = {"project_id": world.project, "agent_id": b["id"]}
    text = note(world, config, task_id=down["id"])
    assert "Task gate status: blocked" in text
    assert f"Assignment: Downstream — status {down['status']}" in text
    assert "unfinished upstream" in text.lower() or "Unfinished upstream" in text


def test_wait_unblocked_polls_until_ready(hooks_world):
    world = hooks_world
    a = world.agent()
    ta = world.credential(a)
    ra = world.run(a, ta)
    scope = world.scope("chain/up", paths=["src/up"])
    world.claim(a, ra, scope, ta)
    down_scope = world.scope("chain/down", paths=["src/down"])
    world.call("POST", "task-plans", {
        "project_id": world.project, "title": "w",
        "tasks": [{"key": "up", "title": "Up", "scope_ids": [scope["id"]]},
                  {"key": "down", "title": "Down", "scope_ids": [down_scope["id"]],
                   "depends_on": ["up"]}]})
    tasks = world.call("GET", f"tasks?project_id={world.project}")
    down_task = next(t for t in tasks if t["title"] == "Down")
    plan_up = next(t for t in tasks if t["title"] == "Up")
    # still blocked at first evaluation: fast deadline, unblocked=false (no claim yet)
    first = world.call("GET", f"projects/{world.project}/tasks/{down_task['id']}/wait-unblocked"
                              "?timeout_seconds=0.5&poll_interval_seconds=0.5")
    assert first["unblocked"] is False and first["waited_seconds"] < 5
    # finish the upstream task through its started execution
    world.call("POST", f"tasks/{plan_up['id']}:start",
               {"run_id": ra["id"], "expected_revision": plan_up["revision"], "recover": False})
    up_now = world.call("GET", f"tasks/{plan_up['id']}")
    world.call("POST", f"tasks/{plan_up['id']}:checkpoint", {
        "expected_revision": up_now["revision"], "run_id": ra["id"],
        "execution_id": (up_now["execution"] or {})["id"],
        "checkpoint_id": uid("ckp"), "status": "completed", "progress": 100,
        "summary": "upstream done"})
    # the Down task is still startable-but-unclaimed; with the waiting agent's candidate
    # run the wait reports ready only when nothing else blocks (run/agent passed explicitly)
    scope_down = down_scope
    world.claim(a, ra, scope_down, ta)
    result = world.call("GET", f"projects/{world.project}/tasks/{down_task['id']}/wait-unblocked"
                               f"?run_id={ra['id']}&agent_id={a['id']}"
                               "&timeout_seconds=8&poll_interval_seconds=0.5")
    assert result["unblocked"] is True
    assert result["readiness"] in ("startable", "blocked", "running")


def test_worker_mcp_surface_includes_wait(hooks_world):
    import asyncio

    from hivemind.mcp import server as main_server
    from hivemind.worker_mcp import OMP_TOOL_IDS, TOOLS, build_server

    tools = asyncio.run(main_server.list_tools())
    names = {t.name for t in tools}
    assert "hivemind_wait_unblocked" in names
    assert "hivemind_wait_unblocked" in TOOLS
    assert OMP_TOOL_IDS == tuple("mcp__" + name for name in TOOLS)
    worker = build_server()
    assert worker is not None  # all nine wrappers construct


def test_denied_mutation_leaves_spool_trace(hooks_world, tmp_path, monkeypatch):
    world = hooks_world
    a = world.agent()
    ta = world.credential(a)
    ra = world.run(a, ta)
    scope = world.scope(paths=["src/allowed"])
    config = {"base_url": "http://testserver", "token": ta,
              "project_id": world.project, "agent_id": a["id"], "runtime": "oh-my-pi",
              "state_dir": str(tmp_path / "state"), "workspace_root": str(tmp_path / "ws"),
              "scope_ids": [scope["id"]]}
    (tmp_path / "ws" / "src/allowed").mkdir(parents=True)
    target = tmp_path / "ws" / "src" / "forbidden.ts"
    # a write outside the agent's claim scope → deterministic denial → trace
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    session_key = hooks.hashlib.sha256(b"sess").hexdigest()
    (state_dir / f"{session_key}.json").write_text(json.dumps({
        "run_id": ra["id"], "session_id": "sess", "scope_ids": [scope["id"]]}))
    payload = {"session_id": "sess", "hook_event_name": "PreToolUse",
               "tool_name": "write", "toolCallId": "denied-1",
               "tool_input": {"file_path": str(target)}}
    with pytest.raises(Exception) as denial_info:
        hooks.invoke(config, payload)
    # The bridge denies fail-closed: RuntimeError/PermissionError from checks, or the
    # permit server check's HTTP status error (httpx or httpx2 flavor via starlette).
    rendered = type(denial_info.value).__name__
    assert rendered in ("RuntimeError", "PermissionError", "HTTPStatusError")
    import sqlite3
    capture_db = state_dir / f"{session_key}-spool.sqlite"
    assert capture_db.exists()
    connection = sqlite3.connect(capture_db)
    rows = connection.execute("SELECT payload FROM spool WHERE state='pending'").fetchall()
    connection.close()
    payloads = [json.loads(row[0]) for row in rows]
    denials = [p for p in payloads
               if p["type"].endswith("agent.status_reported") and "denied" in p["data"]["text"]]
    assert denials, "expected a denied-mutation advisory event in the spool"
    assert "write" in denials[0]["data"]["text"]


def test_auto_checkpoint_files_progress_on_bound_task(hooks_world, tmp_path):
    world = hooks_world
    a = world.agent()
    ta = world.credential(a)
    ra = world.run(a, ta)
    scope = world.scope(paths=["src/task"])
    world.claim(a, ra, scope, ta)
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "T",
                                        "scope_ids": [scope["id"]]})
    world.call("POST", f"tasks/{task['id']}:start",
               {"run_id": ra["id"], "expected_revision": task["revision"], "recover": False})
    (tmp_path / "ws" / "src/task").mkdir(parents=True)
    target = tmp_path / "ws" / "src/task" / "mod.ts"
    config = {"base_url": "http://testserver", "token": ta,
              "project_id": world.project, "agent_id": a["id"], "runtime": "oh-my-pi",
              "run_id": ra["id"],
              "state_dir": str(tmp_path / "state"), "workspace_root": str(tmp_path / "ws"),
              "scope_ids": [scope["id"]], "auto_checkpoint": True,
              "provisioned_task_id": task["id"]}
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    session_key = hooks.hashlib.sha256(b"ckp-sess").hexdigest()
    (state_dir / f"{session_key}.json").write_text(json.dumps({
        "run_id": ra["id"], "session_id": "ckp-sess", "scope_ids": [scope["id"]]}))
    target.write_text("export const probe = 1;")
    hooks.invoke(config, {"session_id": "ckp-sess", "hook_event_name": "PreToolUse",
                          "tool_name": "write", "toolCallId": "ckp-1",
                          "tool_input": {"file_path": str(target)}})
    hooks.invoke(config, {"session_id": "ckp-sess", "hook_event_name": "PostToolUse",
                          "tool_name": "write", "toolCallId": "ckp-1", "isError": False})
    updated = world.call("GET", f"tasks/{task['id']}")
    checkpoint = (updated.get("execution") or {}).get("checkpoint") or {}
    assert checkpoint.get("summary", "").startswith("Auto-checkpoint")
    assert updated["status"] == "running"  # progress, never independent verification