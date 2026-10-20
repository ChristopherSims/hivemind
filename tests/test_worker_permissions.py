import json
import os
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest

from hivemind.hooks import invoke
from hivemind.worker_permissions import WorkerPermissionRequest, WorkerPermissions, signal_violation
from test_worker_provisioning import create, enabled as enabled_fixture

enabled = enabled_fixture


def test_policy_is_bounded_and_cannot_be_widened():
    profile = WorkerPermissions(allowed_tools=("read", "edit"), read_paths=("src",),
                                write_paths=("src/lib",), allow_coordination_mcp=False)
    narrowed = profile.narrow(WorkerPermissionRequest(
        allowed_tools=["read"], read_paths=["src/lib"], write_paths=[], allow_coordination_mcp=False))
    assert narrowed.allowed_tools == ("read",)
    for request in [
        WorkerPermissionRequest(allowed_tools=["write"], read_paths=["src"], write_paths=[], allow_coordination_mcp=False),
        WorkerPermissionRequest(allowed_tools=["read"], read_paths=["."], write_paths=[], allow_coordination_mcp=False),
        WorkerPermissionRequest(allowed_tools=["read"], read_paths=["src"], write_paths=[], allow_coordination_mcp=True),
    ]:
        with pytest.raises(ValueError):
            profile.narrow(request)
    with pytest.raises(ValueError):
        WorkerPermissions(allowed_tools=("execute",))
    with pytest.raises(ValueError):
        WorkerPermissions(read_paths=("../outside",))


def test_paths_are_typed_workspace_contained_and_prefix_bounded(tmp_path):
    (tmp_path / "src/lib").mkdir(parents=True)
    policy = WorkerPermissions(read_paths=("src",), write_paths=("src/lib",))
    config = {"workspace_root": str(tmp_path)}
    policy.check(config, "Read", {"file_path": "src/lib/a.py"})
    policy.check(config, "Edit", {"file_path": "src/lib/a.py"})
    for name, arguments in [("Read", {"file_path": "../outside"}),
                            ("Read", {"file_path": "src-other/a"}),
                            ("Edit", {"file_path": "src/a.py"}),
                            ("Execute", {"command": "anything"}),
                            ("FetchUrl", {"url": "https://example.com"})]:
        with pytest.raises((ValueError, PermissionError, RuntimeError)):
            policy.check(config, name, arguments)


def test_worker_policy_is_frozen_before_launch_and_unknown_tools_stop_tree(world, enabled):
    _, _, calls, supervisor = enabled
    worker = create(world, permissions={"allowed_tools": ["read"], "read_paths": ["."],
                                      "write_paths": [], "allow_coordination_mcp": False})
    config = calls[0][0]
    assert config["agent_permissions"]["allowed_tools"] == ["read"]
    assert worker["permissions"]["write_paths"] == []
    # Real hook code refuses before API/tool execution and writes the private marker.
    hook_config = {**config, "workspace_root": str(calls[0][1])}
    with pytest.raises(PermissionError):
        invoke(hook_config, {"hook_event_name": "PreToolUse", "tool_name": "Execute",
                             "tool_input": {"command": "secret-never-recorded"}})
    thread = supervisor.handles[worker["id"]]["thread"]
    thread.join(5)
    stopped = world.call("GET", f"workers/{worker['id']}")
    assert stopped["state"] == "stopped" and stopped["reason"] == "tool_policy_violation"
    assert stopped["monitoring"]["incident"]["termination_confirmed"]
    assert "secret-never-recorded" not in json.dumps(stopped)
    assert stopped["monitoring"]["sandbox_verified"] is False
    assert calls[0][4].closed


def test_authenticated_api_denial_terminates_only_its_owned_worker(world, enabled):
    _, _, calls, supervisor = enabled
    worker = create(world)
    thread = supervisor.handles[worker["id"]]["thread"]
    # Model-created identity/provisioning is outside this worker's canonical grant.
    world.call("POST", "agents", {"project_id": world.project, "display_name": "Forbidden"},
               token=calls[0][0]["token"], status=403)
    thread.join(5)
    stopped = world.call("GET", f"workers/{worker['id']}")
    assert stopped["reason"] == "api_policy_violation"
    assert calls[0][4].closed
    assert world.app.state.store.db.execute("SELECT 1 FROM principals WHERE id=?",
                                          (worker["principal_id"],)).fetchone() is None


def test_invalid_credentials_cannot_target_a_worker(world, enabled):
    _, _, calls, supervisor = enabled
    worker = create(world)
    world.call("GET", f"workers/{worker['id']}", token="wrong-token", status=401)
    assert calls[0][3].poll() is None
    supervisor.close()


def test_api_payload_cannot_expand_operator_permissions(world, enabled):
    supervisor = enabled[3]
    supervisor.profiles["local"] = replace(supervisor.profiles["local"],
        permissions=WorkerPermissions(allowed_tools=("read",), read_paths=("src",), write_paths=()))
    create(world, permissions={"allowed_tools": ["edit"], "read_paths": ["src"],
                               "write_paths": [], "allow_coordination_mcp": True}, status=403)
    assert enabled[2] == []


def test_denial_does_not_terminate_other_workers(world, enabled):
    first = create(world, key="first")
    second = create(world, key="second")
    calls, supervisor = enabled[2], enabled[3]
    first_thread = supervisor.handles[first["id"]]["thread"]
    world.call("POST", "agents", {"project_id": world.project, "display_name": "Forbidden"},
               token=calls[0][0]["token"], status=403)
    first_thread.join(5)
    assert calls[0][3].poll() is not None and calls[1][3].poll() is None
    assert world.call("GET", f"workers/{second['id']}")["state"] == "running"
    supervisor.close()


def test_unknown_api_endpoint_is_a_conservative_stop(world, enabled):
    worker = create(world)
    thread = enabled[3].handles[worker["id"]]["thread"]
    world.call("GET", "unsupported-system-access", token=enabled[2][0][0]["token"], status=404)
    thread.join(5)
    assert world.call("GET", f"workers/{worker['id']}")["reason"] == "api_policy_violation"


def test_corrupt_monitor_trips_fail_closed(world, enabled):
    _, _, calls, supervisor = enabled
    worker = create(world)
    thread = supervisor.handles[worker["id"]]["thread"]
    path = Path(calls[0][0]["permission_monitor"]["incident_file"])
    path.write_text('{"forged":true}')
    thread.join(5)
    assert world.call("GET", f"workers/{worker['id']}")["reason"] == "monitor_integrity_failure"


def test_marker_detected_even_when_worker_exits_before_poll(world, enabled):
    _, _, calls, supervisor = enabled
    worker = create(world)
    thread = supervisor.handles[worker["id"]]["thread"]
    signal_violation({**calls[0][0], "workspace_root": str(calls[0][1])})
    calls[0][3].code = 0
    thread.join(5)
    assert world.call("GET", f"workers/{worker['id']}")["reason"] == "tool_policy_violation"


def test_scoped_nexus_can_monitor_only_granted_worker(world, enabled):
    worker = create(world)
    nexus = world.agent("Monitor")
    token = world.credential(nexus, coordinator=True)
    run = world.run(nexus, token)
    world.call("POST", "nexus-grants", {"project_id": world.project, "nexus_agent_id": nexus["id"],
        "target_agent_ids": [worker["agent_id"]], "actions": ["monitor"]})
    world.call("POST", f"projects/{world.project}/nexus:acquire",
               {"agent_id": nexus["id"], "run_id": run["id"]}, token)
    assert world.call("GET", f"workers/{worker['id']}", token=token)["monitoring"]["terminate_on_violation"]
    enabled[3].close()


@pytest.mark.skipif(os.name != "nt", reason="Native provisioning currently supports Windows Job containment")
def test_real_owned_parent_and_child_are_terminated_on_violation(world, enabled, tmp_path):
    from hivemind.process_boundary import ProcessGroup
    supervisor = enabled[3]
    heartbeat = tmp_path / "descendant-heartbeat"
    child_script = tmp_path / "child.py"
    child_script.write_text(
        "import pathlib,sys,time\np=pathlib.Path(sys.argv[1])\n"
        "while True:\n p.write_text(str(time.time_ns()))\n time.sleep(.02)\n")
    parent_script = tmp_path / "parent.py"
    parent_script.write_text(
        "import subprocess,sys,time\n"
        "assert sys.stdin.buffer.readline(16)==b'GO\\n'\n"
        "subprocess.Popen([sys.executable,sys.argv[1],sys.argv[2]])\n"
        "time.sleep(30)\n")
    launched = []
    def launch(config, workspace, runtime):
        process = subprocess.Popen([sys.executable, str(parent_script), str(child_script), str(heartbeat)],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=workspace)
        group = ProcessGroup(process)
        launched.append((process, json.loads(config.read_text())))
        return process, group
    supervisor.policy = replace(supervisor.policy, launcher=launch)
    worker = create(world)
    thread = supervisor.handles[worker["id"]]["thread"]
    deadline = time.monotonic() + 1.5
    while not heartbeat.exists() and time.monotonic() < deadline:
        time.sleep(.02)
    assert heartbeat.exists()
    signal_violation({**launched[0][1], "workspace_root": str(enabled[0]["workspaces"] / worker["id"])})
    thread.join(5)
    assert not thread.is_alive() and launched[0][0].poll() is not None
    settled = heartbeat.read_bytes()
    time.sleep(.2)
    assert heartbeat.read_bytes() == settled  # The descendant stopped, not just its parent.
    stopped = world.call("GET", f"workers/{worker['id']}")
    assert stopped["reason"] == "tool_policy_violation"
    assert stopped["monitoring"]["incident"]["termination_confirmed"]
