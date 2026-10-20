import asyncio
import json
import io
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from hivemind import linux_process_boundary as linux
from hivemind import vendor_recovery as vr
from hivemind.vendor_runtimes import VendorRuntime
from hivemind.worker_mcp import TOOLS
from hivemind.worker_mcp_registration import verify_factory
from hivemind.wsl_runtime import WSLPolicy, launch_argv, preflight
from test_additional_integrations import evidence, native
from test_vendor_recovery import FakeVendorClient, load_result
from test_vendor_runtimes import options
from test_worker_mcp_registration import McpSession


def ready_guest():
    return {key: True for key in (
        "wsl2", "unprivileged", "hivemind_installed", "bubblewrap_present",
        "cgroup_ready", "filesystem_verified", "network_namespace_verified"
    )} | {"vendors": {"factory": True, "oh_my_pi": True}}


def wsl_assessment(guest=None):
    return SimpleNamespace(returncode=0, stdout=json.dumps(guest or ready_guest()).encode())


def test_wsl_probe_uses_explicit_distribution_isolated_interpreter_and_policy_binding():
    calls = []
    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return wsl_assessment()
    policy = WSLPolicy("Ubuntu", "/sys/fs/cgroup/hivemind")
    report = preflight(policy, runner=run, executable="wsl.exe", clock=lambda: 10)
    assert report["ready"] and not report["native_windows_sandbox"] and not report["live_vendor_validated"]
    assert calls[0][0][:7] == ["wsl.exe", "--distribution", "Ubuntu", "--exec", "/usr/bin/python3", "-I", "-c"]
    assert calls[0][1]["stdin"] == subprocess.DEVNULL
    argv = launch_argv(policy, "/home/operator/private/worker.json", "factory",
                       assessment=report, executable="wsl.exe", clock=lambda: 11)
    assert "--guest-run" in argv and argv[-1] == policy.cgroup_root
    with pytest.raises(PermissionError):
        launch_argv(replace(policy, distribution="Foreign"), "/home/private/config.json", "factory",
                    assessment=report, executable="wsl.exe", clock=lambda: 11)
    with pytest.raises(PermissionError):
        launch_argv(policy, "/home/private/config.json", "factory",
                    assessment=report, executable="wsl.exe", clock=lambda: 71)


@pytest.mark.parametrize("key", [
    "wsl2", "unprivileged", "hivemind_installed", "bubblewrap_present", "cgroup_ready",
    "filesystem_verified", "network_namespace_verified",
])
def test_missing_wsl_prerequisite_never_downgrades_to_shared_host(key):
    guest = {**ready_guest(), key: False}
    policy = WSLPolicy("Ubuntu", "/sys/fs/cgroup/hivemind")
    result = preflight(policy, runner=lambda *a, **k: wsl_assessment(guest), executable="wsl.exe")
    assert not result["ready"] and key in result["missing"]
    with pytest.raises(PermissionError):
        launch_argv(policy, "/home/private/worker.json", "factory", assessment=result)


@pytest.mark.parametrize("path", ["/mnt/c/private", "../private", "C:\\private", "/home/../private"])
def test_wsl_rejects_windows_mounts_and_ambiguous_paths(path):
    with pytest.raises(ValueError):
        WSLPolicy("Ubuntu", path)


def test_wsl_bad_or_oversized_probe_is_not_readiness_evidence():
    policy = WSLPolicy("Ubuntu", "/sys/fs/cgroup/hivemind")
    for data in (b"not json", b"x" * 65537):
        result = preflight(policy, runner=lambda *a, data=data, **k: SimpleNamespace(returncode=0, stdout=data),
                           executable="wsl.exe")
        assert not result["ready"]
    with pytest.raises(ValueError):
        WSLPolicy("--exec", "/sys/fs/cgroup/hivemind")


def test_cgroup_containment_refuses_this_host_without_delegation(tmp_path):
    with pytest.raises((PermissionError, FileNotFoundError)):
        linux.delegated_root(str(tmp_path))


def fake_cgroup(tmp_path):
    path = tmp_path / "owned"
    path.mkdir()
    for name, value in (("cgroup.procs", "123"), ("cgroup.kill", ""), ("cgroup.events", "populated 0\n")):
        (path / name).write_text(value)
    group = object.__new__(linux.CgroupProcessGroup)
    group.path, group.inode, group.closed = path, path.stat().st_ino, False
    return group


def test_cgroup_kill_requires_drain_and_refuses_replaced_identity(tmp_path, monkeypatch):
    group = fake_cgroup(tmp_path)
    group.kill()
    assert (group.path / "cgroup.kill").read_text() == "1"
    (group.path / "cgroup.events").write_text("populated 1\n")
    times = iter([0, 11])
    monkeypatch.setattr(linux.time, "monotonic", lambda: next(times))
    with pytest.raises(TimeoutError):
        group.kill()
    assert not group.closed
    group.inode += 1
    with pytest.raises(PermissionError):
        group._check()


def test_cgroup_missing_or_malformed_drain_evidence_fails_closed(tmp_path):
    group = fake_cgroup(tmp_path)
    (group.path / "cgroup.events").write_text("populated unknown\n")
    with pytest.raises(PermissionError):
        group._populated()


class ControlledRuntime(VendorRuntime):
    vendor = "droid"
    tool_names = []
    def connected(self):
        return True
    def interrupt(self):
        self.interrupts += 1


@pytest.fixture
def controlled(tmp_path):
    runtime = ControlledRuntime(options(tmp_path))
    runtime.interrupts = 0
    runtime.state = {"run_id": "run", "task_id": "task", "execution_id": "execution",
                     "session_id": "synthetic", "outcome": "idle"}
    return runtime


@pytest.mark.parametrize("action,expected", [("pause", "paused"), ("cancel", "cancelled")])
def test_supported_controls_only_confirm_sealed_checkpoint(controlled, action, expected):
    result = getattr(controlled, action)({"id": "run"})
    assert result["applied"] and result["observed_status"] == expected
    saved = json.loads(controlled.path.with_name(controlled.path.name + ".checkpoint.json").read_text())
    assert saved["id"] == result["checkpoint_id"] and saved["quiescent"] is True
    assert saved["run_id"] == "run" and saved["status"] == expected


def test_overlapping_controls_and_resume_during_active_turn_are_refused(controlled):
    controlled._control_lock.acquire()
    try:
        for action in ("pause", "cancel", "resume"):
            assert getattr(controlled, action)({"id": "run"})["applied"] is None
        assert controlled.interrupts == 0
    finally:
        controlled._control_lock.release()
    controlled.state["outcome"] = "paused"
    controlled._active = True
    assert controlled.resume({"id": "run"})["applied"] is False
    assert controlled.state["outcome"] == "paused"


def test_resume_is_single_transition_and_foreign_runs_cannot_control(controlled):
    assert controlled.pause({"id": "foreign"})["applied"] is None
    assert controlled.interrupts == 0
    controlled.pause({"id": "run"})
    assert controlled.resume({"id": "run"})["applied"] is True
    assert controlled.resume({"id": "run"})["applied"] is False


def test_factory_resume_never_accepts_relative_workspace_or_process_cwd_fallback(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for cwd in (None, ".", "workspace", str(tmp_path / "missing")):
        client = FakeVendorClient(lambda cwd=cwd: load_result(workspace, cwd=cwd))
        with pytest.raises(vr.RecoveryRefused):
            asyncio.run(vr.resume_session_workspace_bound(client, session_id="s", workspace=workspace,
                                                          expected_model="model-a"))
        assert client.closed_sessions == 0 and not client.settings_updates


def test_real_sdk_resume_constructor_is_not_used_for_fenced_continuation(tmp_path, monkeypatch):
    import droid_sdk
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    def forbidden(*args, **kwargs):
        raise AssertionError("High-level SDK resume must not invent a cwd fallback")
    monkeypatch.setattr(droid_sdk.Session, "resume", forbidden)
    client = FakeVendorClient(lambda: load_result(workspace))
    before = os.getcwd()
    session = asyncio.run(vr.resume_session_workspace_bound(
        client, session_id="s", workspace=workspace, expected_model="model-a", restrict_tools=["Read"]))
    assert Path(session.cwd) == workspace.resolve() and os.getcwd() == before
    asyncio.run(session.close())


def test_factory_continuation_through_installed_sdk_wire_parser(tmp_path):
    from droid_sdk.client import DroidClient
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    payload = {"session": {"messages": []},
               "settings": {"modelId": "model-a", "reasoningEffort": "low", "autonomyLevel": "low"},
               "cwd": str(workspace.resolve()), "isAgentLoopInProgress": False}
    requests = []
    class Protocol:
        async def send_request(self, **kwargs):
            assert kwargs["method"] == "droid.load_session"
            requests.append(kwargs)
            return {"result": payload}
    wire = DroidClient(exec_path="not-executed")
    wire._protocol = Protocol()
    class TypedClient(FakeVendorClient):
        async def load_session(self, **kwargs):
            self.loads.append(kwargs)
            if self._restrict is not None:
                payload["settings"]["restrictToolIds"] = self._restrict
            loaded = await wire.load_session(**kwargs)
            self.session_id = wire.session_id
            return loaded
    client = TypedClient(None)
    session = asyncio.run(vr.resume_session_workspace_bound(
        client, session_id="session", workspace=workspace, expected_model="model-a", restrict_tools=["Read"]))
    assert len(requests) == 2 and Path(session.cwd) == workspace.resolve()
    assert all(request["params"]["autoRejectPermissionRequests"] for request in requests)
    asyncio.run(session.close())
    # The real SDK parser preserves a missing cwd as None. Hivemind must hold,
    # not replace it with the host process cwd or the requested directory.
    payload.pop("cwd")
    with pytest.raises(vr.RecoveryRefused) as error:
        asyncio.run(vr.resume_session_workspace_bound(
            client, session_id="session", workspace=workspace, expected_model="model-a"))
    assert vr.WORKSPACE_NOT_PROVEN in error.value.reasons


def test_foreign_mcp_catalog_cannot_masquerade_with_matching_display_names():
    class Foreign(McpSession):
        async def list_tools(self):
            return [SimpleNamespace(source="mcp", display_name=name, id="mcp__foreign__" + name) for name in TOOLS]
    session = Foreign(config={"restrict_tools": []})
    with pytest.raises(PermissionError):
        asyncio.run(verify_factory(session, ["Read"]))


def test_native_verifier_never_counts_injected_runner_as_isolation(tmp_path):
    verifier = native(tmp_path, runner=lambda command: (0, b"", b""))
    verifier.isolated = True
    assert verifier.preflight()["sandbox_ready"] is False
    with pytest.raises(Exception, match="native_sandbox_unverified"):
        verifier.run_bound("syntax", evidence())
    assert list(tmp_path.iterdir()) == []


def test_native_verifier_rejects_staging_root_symlink(tmp_path):
    from hivemind.native_artifact_verifier import NativeArtifactVerifier, NativeCheckProfile
    import sys
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("Symlink privileges unavailable")
    with pytest.raises(ValueError):
        NativeArtifactVerifier(staging_root=link, profiles=[
            NativeCheckProfile("syntax", "project", (sys.executable, "-m", "py_compile", "artifact.py"))])


def test_bootstrap_preserves_worker_failure_exit_code_and_cancels_timer(tmp_path, monkeypatch):
    import hivemind.worker_bootstrap as bootstrap
    import hivemind.cli as cli
    path = tmp_path / "private.json"
    path.write_text(json.dumps({"max_runtime_seconds": 10,
                                "runtime_options": {"workspace": str(tmp_path)}}))
    timers = []
    class Timer:
        def __init__(self, *args):
            self.cancelled = False
            timers.append(self)
        def start(self):
            pass
        def cancel(self):
            self.cancelled = True
    monkeypatch.setattr(bootstrap, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(bootstrap.threading, "Timer", Timer)
    monkeypatch.setattr(bootstrap.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(b"GO\n")))
    monkeypatch.setattr(bootstrap.sys, "argv", ["bootstrap", "--config", str(path), "--runtime", "factory"])
    monkeypatch.setattr(cli, "main", lambda: 7)
    assert bootstrap.main() == 7
    assert timers[0].cancelled
