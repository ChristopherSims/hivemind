import hashlib
import io
import json
import os

import pytest

from hivemind.worker_provisioning import ProvisioningPolicy, WorkerProfile


class FakeProcess:
    def __init__(self):
        self.stdin = io.BytesIO()
        self.code = None

    def poll(self):
        return self.code

    def wait(self, timeout=None):
        assert self.code is not None
        return self.code


class FakeGroup:
    def __init__(self, process):
        self.process = process
        self.closed = False

    def close(self):
        self.closed = True
        self.process.code = -1


@pytest.fixture
def enabled(world, tmp_path):
    from fastapi import Request

    from hivemind.api import envelope, hash_token
    from hivemind.storage import Problem
    from hivemind.worker_provisioning import install

    roots = {name: tmp_path / name for name in ("repository", "workspaces", "private")}
    for root in roots.values():
        root.mkdir()
    policy_file = roots["repository"] / "hivemind-worker-policy.json"
    data = b'{"version":1,"max_runtime_seconds":2}'
    policy_file.write_bytes(data)
    calls = []

    def launch(config, workspace, runtime):
        process = FakeProcess()
        group = FakeGroup(process)
        calls.append((json.loads(config.read_text()), workspace, runtime, process, group))
        return process, group

    profile = WorkerProfile("local", world.project, "factory", "test-model",
        str(roots["repository"]), str(roots["workspaces"]), str(roots["private"]),
        hashlib.sha256(data).hexdigest(), allow_model_calls=True)
    policy = ProvisioningPolicy(enabled=True, profiles=(profile,), launcher=launch)
    store = world.app.state.store

    def principal(request: Request):
        token = request.headers.get("authorization", "").removeprefix("Bearer ")
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?", (hash_token(token),)).fetchone()
        if row is None:
            raise Problem("authentication_required", 401)
        return dict(row)

    def command(p, operation, key, body, function):
        return envelope(world.app.state.service.mutate(p, operation, key, body.model_dump(), function))

    install(world.app, world.app.state.service, principal, command, envelope, policy=policy)
    return roots, policy_file, calls, world.app.state.worker_provisioner


def create(world, *, token=None, key="create-one", status=200, **extra):
    return world.call("POST", f"projects/{world.project}/workers", {
        "profile": "local", "display_name": "Builder", "instructions": "Build the requested feature.",
        "max_runtime_seconds": 3600, **extra}, token=token, key=key, status=status)


def test_clamp_precedes_payload_and_launch_is_not_replayed(world, enabled):
    _, _, calls, handle = enabled
    worker = create(world)
    assert worker["max_runtime_seconds"] == 2
    assert worker["runtime_capped"] is True
    assert calls[0][0]["max_runtime_seconds"] == 2
    assert calls[0][0]["runtime_options"]["timeout_seconds"] == 2
    assert calls[0][0]["runtime_options"]["provisioned_task_id"] == worker["task_id"]
    assert "token" not in worker
    assert calls[0][0]["token"] not in json.dumps(worker)
    assert create(world)["id"] == worker["id"]
    assert len(calls) == 1
    assert handle.close() is True
    assert calls[0][4].closed
    assert world.app.state.store.db.execute(
        "SELECT 1 FROM principals WHERE id=?", (worker["principal_id"],)).fetchone() is None


def test_deadline_stops_owned_process_and_revokes_credential(world, enabled):
    _, _, calls, handle = enabled
    worker = create(world, max_runtime_seconds=1)
    handle.handles[worker["id"]]["thread"].join(5)
    stopped = world.call("GET", f"workers/{worker['id']}")
    assert stopped["state"] == "stopped"
    assert stopped["reason"] == "runtime_limit"
    assert calls[0][4].closed
    assert stopped["external_effects_proved"] is False


def test_normal_agent_cannot_provision(world, enabled):
    token = world.credential(world.agent())
    create(world, token=token, status=403)
    assert enabled[2] == []


def test_nexus_requires_exact_live_epoch_and_can_launch(world, enabled):
    agent = world.agent("Nexus")
    token = world.credential(agent, coordinator=True)
    run = world.run(agent, token)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire", {
        "agent_id": agent["id"], "run_id": run["id"], "requested_ttl_seconds": 60}, token=token)
    create(world, token=token, coordinator_epoch=lease["coordinator_epoch"] + 1, status=409)
    worker = create(world, token=token, coordinator_epoch=lease["coordinator_epoch"])
    assert world.call("GET", f"workers/{worker['id']}", token=token)["id"] == worker["id"]


def test_nexus_status_withdraws_now_private_task_source(world, enabled):
    agent = world.agent("Nexus")
    token = world.credential(agent, coordinator=True)
    run = world.run(agent, token)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire", {
        "agent_id": agent["id"], "run_id": run["id"], "requested_ttl_seconds": 60}, token=token)
    worker = create(world, token=token, coordinator_epoch=lease["coordinator_epoch"], start=False)
    with world.app.state.store.transaction():
        world.app.state.store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                                         (worker["agent_id"], worker["task_id"]))
    world.call("GET", f"workers/{worker['id']}", token=token, status=404)
    create(world, token=token, coordinator_epoch=lease["coordinator_epoch"],
           start=False, status=404)


def test_policy_edit_cannot_raise_nexus_ceiling(world, enabled):
    _, policy_file, calls, _ = enabled
    policy_file.write_text('{"version":1,"max_runtime_seconds":86400}')
    create(world, status=409)
    assert not calls
    assert not world.app.state.store.list("worker_provision", world.project,
                                         {"role": "admin", "id": "unused"}, 100)


def test_prepared_worker_can_be_canceled_without_process(world, enabled):
    worker = create(world, start=False)
    assert worker["state"] == "prepared"
    stopped = world.call("POST", f"workers/{worker['id']}:stop", {
        "expected_revision": worker["revision"]})
    assert stopped["state"] == "stopped"
    assert enabled[2] == []


@pytest.mark.parametrize("extra", [
    {"max_runtime_seconds": True}, {"max_runtime_seconds": 0},
    {"executable": "arbitrary"}, {"mcp_url": "https://arbitrary"},
])
def test_payload_is_strict_and_cannot_select_host_commands(world, enabled, extra):
    create(world, status=422, **extra)
    assert not enabled[2]


def test_default_policy_does_not_expose_launch_routes(world):
    assert not any(getattr(route, "path", "").endswith("/workers") for route in world.app.routes)


def test_capacity_includes_uncertain_and_prepared_intents(world, enabled):
    for index in range(4):
        create(world, start=False, key=f"prepared-{index}")
    create(world, start=False, key="overflow", status=429)


def test_replay_after_nexus_grant_revocation_is_not_launch_permission(world, enabled):
    agent = world.agent("Nexus")
    token = world.credential(agent, coordinator=True)
    run = world.run(agent, token)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire", {
        "agent_id": agent["id"], "run_id": run["id"], "requested_ttl_seconds": 60}, token=token)
    create(world, token=token, coordinator_epoch=lease["coordinator_epoch"], start=False)
    with world.app.state.store.transaction():
        world.app.state.store.db.execute("UPDATE principals SET coordinator=0 WHERE agent=?", (agent["id"],))
    create(world, token=token, coordinator_epoch=lease["coordinator_epoch"], start=False, status=403)
    assert not enabled[2]


def test_authority_revoked_after_spawn_never_sends_handshake(world, enabled):
    _, _, calls, handle = enabled
    original = handle.policy.launcher

    def launch(config, workspace, runtime):
        result = original(config, workspace, runtime)
        with world.app.state.store.transaction():
            world.app.state.store.db.execute("DELETE FROM principals WHERE role='admin'")
        return result

    object.__setattr__(handle.policy, "launcher", launch)
    create(world, status=409)
    assert calls[0][3].stdin.getvalue() == b""
    assert calls[0][4].closed
    assert world.app.state.store.db.execute(
        "SELECT 1 FROM principals WHERE role='agent'").fetchone() is None


def test_preparing_intent_after_restart_is_held_not_replayed(world, enabled):
    from hivemind.worker_provisioning import WorkerProvisioner

    worker = create(world, start=False)
    store = world.app.state.store
    with store.transaction():
        row = store.raw(worker["id"])
        body = json.loads(row["body"])
        body["state"] = "preparing"
        store.save(row["kind"], row["project"], row["owner"], body, expected=row["revision"])
    restarted = WorkerProvisioner(world.app.state.service, enabled[3].policy)
    row = store.raw(worker["id"])
    assert json.loads(row["body"])["state"] == "unknown"
    assert enabled[2] == []
    assert restarted.close()


def test_generated_mcp_bootstrap_contains_no_inline_secret(world, enabled):
    roots, _, calls, _ = enabled
    worker = create(world)
    descriptor = json.loads((roots["private"] / worker["id"] / "mcp.json").read_text())
    assert descriptor["mcpServers"]["hivemind"]["args"] == ["-m", "hivemind.worker_mcp"]
    assert calls[0][0]["token"] not in json.dumps(descriptor)
    assert worker["mcp"]["configured"] is True
    assert worker["mcp"]["connection_verified"] is False
    assert calls[0][0]["runtime_options"]["auto_mcp_registration"] is True


def test_mcp_registration_is_operator_controlled(world, enabled):
    from dataclasses import replace
    with pytest.raises(ValueError):
        replace(enabled[3].profiles["local"], auto_mcp_registration="true")
    create(world, key="bad-mcp-payload", status=422, auto_mcp_registration=False)
    enabled[3].profiles["local"] = replace(
        enabled[3].profiles["local"], auto_mcp_registration=False)
    worker = create(world)
    assert worker["mcp"]["automatic"] is False
    assert worker["mcp"]["configured"] is False
    assert enabled[2][0][0]["runtime_options"]["auto_mcp_registration"] is False


def test_profile_project_and_assignment_are_fenced(world, enabled):
    worker = create(world, start=False)
    with world.app.state.store.transaction():
        row = world.app.state.store.raw(worker["run_id"])
        body = json.loads(row["body"])
        body["task_id"] = None
        world.app.state.store.save(row["kind"], row["project"], row["owner"], body,
                                   expected=row["revision"])
    world.call("POST", f"workers/{worker['id']}:start",
               {"expected_revision": worker["revision"]}, status=409)
    assert not enabled[2]


@pytest.mark.skipif(os.name != "nt", reason="Native provisioning requires Windows Job containment")
def test_real_bootstrap_refuses_eof_before_loading_config(tmp_path):
    from hivemind.worker_provisioning import WorkerProvisioner

    process, group = WorkerProvisioner._spawn(tmp_path / "does-not-exist.json", tmp_path, "factory")
    try:
        process.stdin.close()
        assert process.wait(timeout=10) == 2
    finally:
        group.close()


def test_linux_native_spawn_admits_cgroup_before_any_startup_handshake(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import hivemind.worker_provisioning as module
    import hivemind.linux_process_boundary as boundary
    process = FakeProcess()
    events = []
    monkeypatch.setattr(module, "os", SimpleNamespace(name="posix", environ={"PATH": "/usr/bin"}))
    monkeypatch.setattr(boundary, "delegated_root", lambda root: events.append("preflight"))
    monkeypatch.setattr(module.subprocess, "Popen", lambda *args, **kwargs: process)
    def admit(child, root):
        assert child.stdin.getvalue() == b""
        events.append("admitted")
        return FakeGroup(child)
    monkeypatch.setattr(boundary, "CgroupProcessGroup", admit)
    child, group = module.WorkerProvisioner._spawn(
        tmp_path / "private.json", tmp_path, "factory", linux_cgroup_root="/sys/fs/cgroup/delegated")
    assert child is process and events == ["preflight", "admitted"]
    assert child.stdin.getvalue() == b""
    group.close()


def test_native_linux_spawn_without_delegation_never_creates_a_process(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import hivemind.worker_provisioning as module
    monkeypatch.setattr(module, "os", SimpleNamespace(name="posix"))
    def forbidden(*args, **kwargs):
        raise AssertionError("Popen must not precede delegation validation")
    monkeypatch.setattr(module.subprocess, "Popen", forbidden)
    with pytest.raises(PermissionError):
        module.WorkerProvisioner._spawn(tmp_path / "private.json", tmp_path, "factory")


def test_isolated_profile_composes_gateway_config_and_preserves_explicit_mcp_gate(world, enabled, monkeypatch):
    from dataclasses import replace
    from types import SimpleNamespace
    import sys
    import hivemind.worker_provisioning as module
    roots, _, calls, provisioner = enabled
    profile = provisioner.profiles["local"]
    with pytest.raises(ValueError, match="MCP"):
        replace(profile, require_os_isolation=True, vendor_model_origins=("https://model.example",))
    profile = replace(profile, require_os_isolation=True, auto_mcp_registration=False,
                      vendor_model_origins=("https://model.example",))
    provisioner.profiles["local"] = profile
    monkeypatch.setattr(module, "sys", SimpleNamespace(platform="linux", executable=sys.executable))
    create(world)
    runtime = calls[0][0]["runtime_options"]
    assert runtime["require_os_isolation"] is True
    assert runtime["vendor_network_gateway"] == {"model_origins": ["https://model.example"]}
    assert runtime["auto_mcp_registration"] is False
    assert provisioner.close()


@pytest.mark.skipif(os.name != "nt", reason="Uses the existing real Windows Job launcher")
def test_untrusted_workspace_cannot_shadow_bootstrap_before_containment(tmp_path):
    from hivemind.worker_provisioning import WorkerProvisioner
    package = tmp_path / "hivemind"
    package.mkdir()
    marker = tmp_path / "untrusted-import"
    (package / "__init__.py").write_text(
        "from pathlib import Path\nPath(" + repr(str(marker)) + ").write_text('executed')\n")
    (package / "worker_bootstrap.py").write_text("raise SystemExit(99)\n")
    process, group = WorkerProvisioner._spawn(tmp_path / "missing-private.json", tmp_path, "factory")
    try:
        process.stdin.close()
        assert process.wait(timeout=10) == 2
        assert not marker.exists()
    finally:
        group.close()
