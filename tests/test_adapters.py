import json
from pathlib import Path

import pytest

from hivemind import capture_daemon, hooks
from hivemind.storage import uid


@pytest.mark.parametrize("runtime,pre,post,name", [
    ("droid", "PreToolUse", "PostToolUse", "Create"),
    ("oh-my-pi", "tool_call", "tool_result", "write"),
])
def test_runtime_hook_edit_capture_and_fail_closed(world, tmp_path, monkeypatch, runtime, pre, post, name):
    class LocalClient:
        def __init__(self, base_url, token):
            self.token = token
        def request(self, method, path, body=None, key=None):
            response = world.client.request(method, "/v1/" + path, json=body, headers={
                "Authorization": "Bearer " + self.token, "Idempotency-Key": key or "test-" + str(id(body))})
            response.raise_for_status()
            return response.json()["data"]
        def assemble_context(self, request):
            return self.request("POST", "context:assemble", request)
        def close(self):
            pass
    monkeypatch.setattr(hooks, "Client", LocalClient)
    monkeypatch.setattr(capture_daemon, "Client", LocalClient)
    agent = world.agent(runtime, ["steer", "pause", "resume", "cancel", "assign_task"])
    token = world.credential(agent)
    scope = world.scope(runtime + "/header", paths=["src/header"])
    workspace = tmp_path / "workspace"
    (workspace / "src/header").mkdir(parents=True)
    config = {"base_url": "http://testserver", "token": token, "project_id": world.project,
              "agent_id": agent["id"], "runtime": runtime, "state_dir": str(tmp_path / "state"),
              "scope_ids": [scope["id"]], "workspace_root": str(workspace)}
    startup = {"hook_event_name": "SessionStart", "session_id": runtime, "cwd": str(workspace)}
    hooks.invoke(config, startup)
    target = workspace / "src/header/index.ts"
    tool = {"session_id": runtime, "hook_event_name": pre, "tool_name": name,
            "tool_input": {"file_path": str(target)}, "cwd": str(workspace), "toolCallId": "call-1"}
    hooks.invoke(config, tool)
    target.write_text("export const header = 'navigation';")
    hooks.invoke(config, {**tool, "hook_event_name": post, "isError": False})
    capture_daemon.drain(config)
    run = world.call("GET", f"ledger?project_id={world.project}")["runs"][0]
    assert run["files"]["src/header/index.ts"]["attribution"] == "confirmed"
    assert run["files"]["src/header/index.ts"]["change"] == "created"
    assert run["tools"]["call-1"]["state"] == "completed"
    # Shell/custom mutations cannot sneak around a canonical file scope.
    with pytest.raises(RuntimeError):
        hooks.invoke(config, {**tool, "tool_name": "Execute", "tool_input": {"command": "write anywhere"}})
    with pytest.raises(ValueError):
        hooks.invoke(config, {**tool, "tool_input": {"file_path": str(tmp_path / "outside.ts")}})
    assert not Path(tmp_path / "outside.ts").exists()
    # Repeated failures remain empty observations, not invented edits.
    second = {**tool, "toolCallId": "call-2"}
    hooks.invoke(config, second)
    hooks.invoke(config, {**second, "hook_event_name": post, "isError": True})
    capture_daemon.drain(config)
    events = world.call("GET", f"events?project_id={world.project}")
    assert sum(e["type"].endswith("file.changed") for e in events) == 1
    assert "token" not in json.dumps(run)


@pytest.mark.parametrize("runtime,name", [("droid", "Create"), ("oh-my-pi", "write")])
@pytest.mark.parametrize("read_first", [False, True])
def test_steering_is_not_applied_until_replacement_work_begins(
        world, tmp_path, monkeypatch, runtime, name, read_first):
    class LocalClient:
        def __init__(self, base_url, token):
            self.token = token
        def request(self, method, path, body=None, key=None):
            response = world.client.request(method, "/v1/" + path, json=body, headers={
                "Authorization": "Bearer " + self.token, "Idempotency-Key": key or uid("key")})
            response.raise_for_status()
            return response.json()["data"]
        def assemble_context(self, request):
            return self.request("POST", "context:assemble", request)
        def close(self):
            pass
    monkeypatch.setattr(hooks, "Client", LocalClient)
    monkeypatch.setattr(capture_daemon, "Client", LocalClient)
    a, b, nexus = world.agent("Existing owner"), world.agent(
        runtime, ["steer", "pause", "resume"]), world.agent("Nexus")
    ta, tb, tn = world.credential(a), world.credential(b), world.credential(nexus, coordinator=True)
    ra, rn = world.run(a, ta), world.run(nexus, tn)
    header, footer = world.scope(paths=["src/header"]), world.scope("website/footer", paths=["src/footer"])
    world.claim(a, ra, header, ta)
    root = tmp_path / "workspace"
    (root / "src/header").mkdir(parents=True)
    (root / "src/footer").mkdir()
    original = root / "src/header/index.ts"
    original.write_text("Preserve the existing owner's header.")
    config = {"base_url": "http://testserver", "token": tb, "project_id": world.project, "agent_id": b["id"],
              "runtime": runtime, "state_dir": str(tmp_path / "state"), "workspace_root": str(root), "scope_ids": []}
    hooks.invoke(config, {"hook_event_name": "SessionStart", "session_id": "session"})
    rb = next(r for r in world.call("GET", f"ledger?project_id={world.project}")["runs"] if r["agent_id"] == b["id"])
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Build footer",
                                       "scope_ids": [footer["id"]]})
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": rn["id"]}, tn)
    control = world.call("POST", "controls", {
        "project_id": world.project, "target_run_id": rb["id"], "action": "steer",
        "replacement_task_id": task["id"], "target_run_revision": rb["control_revision"],
        "coordinator_epoch": lease["coordinator_epoch"], "reason": "Retain header ownership"}, tn)
    tool = {"session_id": "session", "hook_event_name": "PreToolUse", "tool_name": name,
            "toolCallId": "header-call", "tool_input": {"file_path": str(original)}}
    if read_first:
        observation = {**tool, "tool_name": "Read" if runtime == "droid" else "read", "toolCallId": "read-call"}
        output = hooks.invoke(config, observation)
        assert "Build footer" in output["additionalContext"]
        hooks.invoke(config, {**observation, "hook_event_name": "PostToolUse", "isError": False})
    else:
        with pytest.raises(RuntimeError):
            hooks.invoke(config, tool)
    assert world.call("GET", f"controls/{control['id']}")["state"] == "applying"
    assert original.read_text() == "Preserve the existing owner's header."
    # Only the reserved replacement scope can be permitted while the checkpoint is applying.
    world.call("POST", f"runs/{rb['id']}/permits:check", {"scope_ids": [header["id"]]}, tb, status=403)
    target = root / "src/footer/index.ts"
    tool = {**tool, "toolCallId": "footer-call", "tool_input": {"file_path": str(target)}}
    hooks.invoke(config, tool)
    assert world.call("GET", f"controls/{control['id']}")["state"] == "applying"
    target.write_text("export const footer = 'new work';")
    hooks.invoke(config, {**tool, "hook_event_name": "PostToolUse", "isError": False})
    assert world.call("GET", f"controls/{control['id']}")["state"] == "applied"
    assert world.call("GET", f"runs/{rb['id']}")["task_id"] == task["id"]
    assert original.read_text() == "Preserve the existing owner's header."
