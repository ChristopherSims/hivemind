"""Parent isolation wiring tests; no OS provisioning or paid vendor calls."""
from types import SimpleNamespace

import pytest

from hivemind import isolation_runtime as wiring
from hivemind.isolation_runtime import IsolationAttestationConfig, IsolationRuntimeGuard
from hivemind.os_isolation import IsolationPolicy
from hivemind.vendor_runtimes import FactoryRuntime, OhMyPiRuntime
from test_vendor_runtimes import SDK, OmpTransport, Session, assignment, options


def policy():
    return IsolationPolicy(mode="required", separate_service_identity=True,
                           separate_agent_identity=True, protect_bare_directories=True)


@pytest.mark.parametrize("value", [True, 0, -1, 301, float("inf"), float("nan"), "60"])
def test_attestation_freshness_is_explicit_bounded_number(value):
    with pytest.raises(ValueError):
        IsolationAttestationConfig("service", max_age_seconds=value)


def test_guard_checks_current_process_and_refreshes_every_boundary(monkeypatch, tmp_path):
    calls = []

    def record(kind):
        return SimpleNamespace(assert_fresh=lambda age: calls.append(("fresh", kind, age)))

    def process(identity, **kwargs):
        calls.append(("process", identity, kwargs["require_unprivileged"]))
        return record("process")

    monkeypatch.setattr(wiring, "attest_process_identity", process)
    monkeypatch.setattr(wiring, "attest_distinct_identities", lambda *a: record("identities"))
    monkeypatch.setattr(wiring, "attest_protected_directory", lambda *a, **kw: record("directory"))
    guard = IsolationRuntimeGuard(policy(), IsolationAttestationConfig("service", "agent", str(tmp_path)))
    guard.bind_resources(tmp_path / "state", tmp_path / "git")
    guard.check()
    assert calls.count(("process", "service", True)) == 2
    assert len(guard.last) == 3
    with pytest.raises(PermissionError, match="escapes"):
        guard.bind_resources(tmp_path.parent, tmp_path / "git")


def test_running_as_wrong_account_refuses_before_directory_claim(monkeypatch, tmp_path):
    def wrong(*args, **kwargs):
        raise wiring.AttestationFailed("Actual process identity differs")

    monkeypatch.setattr(wiring, "attest_process_identity", wrong)
    guard = IsolationRuntimeGuard(policy(), IsolationAttestationConfig("service", "agent", str(tmp_path)))
    with pytest.raises(PermissionError, match="Actual process"):
        guard.check()
    assert guard.last == ()


def test_wrong_storage_refuses_before_opening_canonical_database(tmp_path):
    from hivemind.api import create_app
    protected = tmp_path / "service"
    protected.mkdir()
    data = tmp_path / "outside" / "state"
    guard = IsolationRuntimeGuard(policy(), IsolationAttestationConfig("service", "agent", str(protected)))
    with pytest.raises(PermissionError, match="escapes"):
        create_app(data, capability_options={"agent_automation": {"isolation_guard": guard}})
    assert not data.exists()


def test_native_git_rechecks_before_spawning(monkeypatch, tmp_path):
    from hivemind import git_collaboration

    def refuse():
        raise PermissionError("Attestation changed")

    domain = git_collaboration.GitCollaboration(
        SimpleNamespace(store=SimpleNamespace(directory=tmp_path)))
    domain.isolation_guard = SimpleNamespace(check=refuse)
    monkeypatch.setattr(git_collaboration, "run_bounded",
                        lambda *a, **kw: pytest.fail("Git spawned before attestation"))
    with pytest.raises(PermissionError, match="changed"):
        domain._run(tmp_path, "status")


@pytest.mark.parametrize("flag", ["require_os_isolation", "vendor_filesystem_sandbox"])
def test_vendor_isolation_flags_reject_truthy_strings(tmp_path, flag):
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                              flag: "true"}, sdk=SDK)
    with pytest.raises(ValueError, match="booleans"):
        runtime.execute(*assignment(), None)
    assert not runtime.path.exists()


def test_agent_identity_is_checked_before_vendor_resources(monkeypatch, tmp_path):
    before = len(Session.created)

    def wrong(*args, **kwargs):
        raise wiring.AttestationFailed("Agent process identity differs")

    monkeypatch.setattr(wiring, "attest_process_identity", wrong)
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                              "os_identity": "hivemind-agent"}, sdk=SDK)
    with pytest.raises(PermissionError, match="Agent process"):
        runtime.execute(*assignment(), None)
    assert not runtime.path.exists() and len(Session.created) == before


def test_factory_filesystem_sandbox_connects_official_supplied_transport(monkeypatch, tmp_path):
    from hivemind import vendor_sandbox
    seen = {}

    class Transport:
        async def connect(self):
            seen["connected"] = True

        async def close(self):
            seen["closed"] = True

    transport = Transport()

    def launch(workspace, **kwargs):
        seen.update(kwargs)
        assert kwargs["config_file"].is_file()
        assert kwargs["extra_read_binds"][-1].is_file()  # Hook settings, not a private parent mount.
        assert kwargs["extra_write_binds"][0].is_dir()
        return transport

    monkeypatch.setattr(vendor_sandbox, "assess_vendor_sandbox",
                        lambda *a, **kw: SimpleNamespace(allowed=True))
    monkeypatch.setattr(vendor_sandbox, "factory_sandbox_transport", launch)
    runtime = FactoryRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                              "vendor_filesystem_sandbox": True}, sdk=SDK)
    try:
        runtime.execute(*assignment(), None)
        assert seen["connected"] is True
        assert Session.created[-1].kwargs["runtime"]["transport"] is transport
        assert seen["env"]["HOME"] == str(seen["extra_write_binds"][0])
    finally:
        runtime.close()
    assert seen["closed"] is True


def test_omp_wraps_actual_rpc_argv_before_launch(monkeypatch, tmp_path):
    from hivemind import vendor_sandbox
    seen = {}

    def wrap(argv, workspace, **kwargs):
        seen.update(kwargs)
        assert kwargs["session_dir"].is_dir()
        assert kwargs["extra_read_binds"][-1].name == "hivemind.ts"
        return ["sandbox-fixture", *argv]

    monkeypatch.setattr(vendor_sandbox, "assess_vendor_sandbox",
                        lambda *a, **kw: SimpleNamespace(allowed=True))
    monkeypatch.setattr(vendor_sandbox, "sandbox_vendor_argv", wrap)
    runtime = OhMyPiRuntime({**options(tmp_path), "enabled": True, "allow_model_calls": True,
                            "vendor_filesystem_sandbox": True}, transport_factory=OmpTransport)
    try:
        runtime.execute(*assignment(), None)
        assert runtime.transport.argv[0] == "sandbox-fixture"
        assert seen["config_file"].is_file()
    finally:
        runtime.close()
