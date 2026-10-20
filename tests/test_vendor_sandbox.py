"""Adversarial offline tests plus real probes for vendor sandboxing.

Nothing here opens a paid vendor session or publishes to a network. The real
bubblewrap probe executes only local binaries; the droid transport tests use
the installed SDK's documented ProcessTransport constructor without connecting.
"""
import os
import shutil
import subprocess
import sys
import time

import pytest

from hivemind import os_isolation as oi
from hivemind import vendor_sandbox as vs


def bwrap_which(present=True):
    return lambda _: "/usr/bin/bwrap" if present else None


def passed_probe(age=0.0):
    return vs.ProbeEvidence("bubblewrap", "passed", "verified", 1000.0 - age)


def fresh_probe():
    return vs.ProbeEvidence("bubblewrap", "passed", "verified", time.time())


def failed_probe(detail="sentinel visible"):
    return vs.ProbeEvidence("bubblewrap", "failed", detail, 1000.0)


CLOCK = lambda: 1000.0  # noqa: E731


class TestProbes:
    def test_probe_refuses_non_linux_hosts(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "win32")
        result = vs.probe_bwrap_enforcement(which=bwrap_which(), cache={})
        assert result.status == "failed" and "Linux" in result.detail

    def test_probe_missing_binary_fails(self):
        result = vs.probe_bwrap_enforcement(which=bwrap_which(False), cache={},
                                            clock=CLOCK)
        assert result.status == "failed" and "PATH" in result.detail

    def test_probe_reports_runner_failures(self):
        def runner(cmd, **kwargs):
            class Result:
                returncode = 1
                stderr = b"boom"
            return Result()
        result = vs.probe_bwrap_enforcement(which=bwrap_which(), runner=runner,
                                            cache={}, clock=CLOCK, platform="linux")
        assert result.status == "failed" and "boom" in result.detail

    def test_probe_fails_when_host_path_leaks(self):
        calls = []

        def runner(cmd, **kwargs):
            calls.append(cmd)

            class Result:
                stderr = b""
                returncode = 0 if len(calls) == 1 else 0  # leak test "succeeds" -> leak!
            return Result()
        result = vs.probe_bwrap_enforcement(which=bwrap_which(), runner=runner,
                                            cache={}, clock=CLOCK, platform="linux")
        assert result.status == "failed" and "VISIBLE" in result.detail

    def test_probe_passes_and_caches(self):
        calls = []

        def runner(cmd, **kwargs):
            calls.append(cmd)

            class Result:
                stderr = b""
                returncode = 0 if len(calls) % 2 == 1 else 1  # inner ok, leak test absent
            return Result()
        cache = {}
        first = vs.probe_bwrap_enforcement(which=bwrap_which(), runner=runner,
                                           cache=cache, clock=CLOCK, platform="linux")
        assert first.status == "passed"
        second = vs.probe_bwrap_enforcement(which=bwrap_which(), runner=runner,
                                            cache=cache, clock=CLOCK, platform="linux")
        assert second is first and len(calls) == 2  # cached, not re-run

    def test_probe_freshness_enforced(self):
        assert passed_probe().assert_fresh(clock=CLOCK)
        with pytest.raises(oi.IsolationRefused, match="stale"):
            passed_probe(age=9999).assert_fresh(clock=CLOCK)


class TestAssessment:
    def test_windows_refused_for_both_vendors(self):
        for vendor in ("droid", "oh-my-pi"):
            decision = vs.assess_vendor_sandbox(vendor, platform="windows",
                                                which=bwrap_which(), probe=passed_probe(),
                                                clock=CLOCK)
            assert decision.status == "refused" and "cleanup only" in decision.detail
            assert decision.filesystem_isolated is False

    def test_strict_network_isolation_refused_everywhere(self):
        for platform in ("windows", "linux"):
            for vendor in ("droid", "oh-my-pi"):
                decision = vs.assess_vendor_sandbox(
                    vendor, platform=platform, which=bwrap_which(), probe=passed_probe(),
                    require_network_isolation=True, clock=CLOCK)
                assert decision.status == "refused"
                assert "network" in decision.detail and decision.scope == "none"

    def test_no_backend_refused(self):
        decision = vs.assess_vendor_sandbox("oh-my-pi", platform="linux",
                                            which=bwrap_which(False), clock=CLOCK)
        assert decision.status == "refused"

    def test_presence_without_probe_is_not_enforced(self):
        decision = vs.assess_vendor_sandbox("oh-my-pi", platform="linux",
                                            which=bwrap_which(), clock=CLOCK)
        assert decision.status == "refused" and "not proof" in decision.detail

    def test_stale_probe_refused(self):
        decision = vs.assess_vendor_sandbox("oh-my-pi", platform="linux",
                                            which=bwrap_which(),
                                            probe=passed_probe(age=9999), clock=CLOCK)
        assert decision.status == "refused" and "stale" in decision.detail

    def test_failed_probe_refused(self):
        decision = vs.assess_vendor_sandbox("oh-my-pi", platform="linux",
                                            which=bwrap_which(), probe=failed_probe(),
                                            clock=CLOCK)
        assert decision.status == "refused" and "sentinel visible" in decision.detail

    def test_oh_my_pi_enforced_with_fresh_passed_probe(self):
        decision = vs.assess_vendor_sandbox("oh-my-pi", platform="linux",
                                            which=bwrap_which(), probe=passed_probe(),
                                            clock=CLOCK)
        assert decision.allowed and decision.sandbox == "bubblewrap"
        assert decision.scope == "filesystem-only"
        assert decision.filesystem_isolated is True
        assert decision.network_isolated is False
        assert any("--share-net" in item for item in decision.prerequisites)

    def test_droid_requires_sdk_transport_interface(self):
        class NoTransport:
            transport = None
        decision = vs.assess_vendor_sandbox("droid", platform="linux", which=bwrap_which(),
                                            probe=passed_probe(), sdk=NoTransport(),
                                            clock=CLOCK)
        assert decision.status == "refused" and "ProcessTransport" in decision.detail

    def test_droid_enforced_with_sdk_and_probe(self):
        transport = type("transport", (), {"ProcessTransport": object})
        sdk = type("sdk", (), {"transport": transport})
        decision = vs.assess_vendor_sandbox("droid", platform="linux", which=bwrap_which(),
                                            probe=passed_probe(), sdk=sdk(), clock=CLOCK)
        assert decision.allowed and decision.scope == "filesystem-only"

    def test_real_sdk_transport_interface_detected(self):
        droid_sdk = pytest.importorskip("droid_sdk")
        transport = vs._sdk_with_process_transport(droid_sdk)
        assert transport is not None and hasattr(transport, "ProcessTransport")

    def test_unknown_vendor_rejected(self):
        with pytest.raises(ValueError, match="Unknown vendor"):
            vs.assess_vendor_sandbox("side-door", platform="linux", clock=CLOCK)


@pytest.fixture
def layout(tmp_path):
    workspace = tmp_path / "workspace"
    session = tmp_path / "state" / "omp-sessions"
    workspace.mkdir()
    session.mkdir(parents=True)
    config = tmp_path / "state" / "config.json"
    config.write_text("{}")
    adapter = tmp_path / "adapters" / "oh-my-pi"
    adapter.mkdir(parents=True)
    decision = vs.assess_vendor_sandbox("oh-my-pi", platform="linux", which=bwrap_which(),
                                        probe=fresh_probe())
    return {"workspace": workspace, "session": session, "config": config,
            "adapter": adapter, "decision": decision,
            "argv": ["omp", "--mode", "rpc", "--no-tools"]}


class TestArgvWrapping:
    def test_wraps_with_real_filesystem_policy(self, layout):
        command = vs.sandbox_vendor_argv(
            layout["argv"], layout["workspace"], config_file=layout["config"],
            session_dir=layout["session"], extra_read_binds=(layout["adapter"],),
            decision=layout["decision"], platform="linux")
        assert command[0].endswith("bwrap")
        assert "--unshare-all" in command and "--share-net" in command
        assert "--cap-drop" in command and "ALL" in command
        workspace_index = command.index(str(layout["workspace"].resolve()))
        assert command[workspace_index - 1] == "--bind"  # read-write, agent work area
        assert command[command.index(str(layout["config"].resolve())) - 1] == "--ro-bind"
        assert command[command.index(str(layout["session"].resolve())) - 1] == "--bind"
        assert command[-len(layout["argv"]):] == layout["argv"]
        assert command[-len(layout["argv"]) - 1] == "--"

    def test_refused_decision_raises_before_launch(self, layout):
        decision = vs.assess_vendor_sandbox("oh-my-pi", platform="windows",
                                            which=bwrap_which(), clock=CLOCK)
        with pytest.raises(oi.IsolationRefused, match="refused"):
            vs.sandbox_vendor_argv(["sh", "-c", "exit 42"], layout["workspace"],
                                   config_file=layout["config"],
                                   session_dir=layout["session"], decision=decision)

    def test_strict_network_requirement_refuses_even_with_probe(self, layout):
        with pytest.raises(oi.IsolationRefused, match="network"):
            vs.sandbox_vendor_argv(layout["argv"], layout["workspace"],
                                   config_file=layout["config"],
                                   session_dir=layout["session"], platform="linux",
                                   which=bwrap_which(), require_network_isolation=True)

    def test_missing_config_fails_closed(self, layout):
        layout["config"].unlink()
        with pytest.raises(oi.IsolationRefused, match="config"):
            vs.sandbox_vendor_argv(layout["argv"], layout["workspace"],
                                   config_file=layout["config"],
                                   session_dir=layout["session"], decision=layout["decision"],
                                   platform="linux")

    def test_symlinked_workspace_fails_closed(self, layout, tmp_path):
        if os.name != "posix":
            pytest.skip("symlink creation")
        link = tmp_path / "workspace-link"
        link.symlink_to(layout["workspace"], target_is_directory=True)
        with pytest.raises(oi.IsolationRefused, match="symbolic link"):
            vs.sandbox_vendor_argv(layout["argv"], link, config_file=layout["config"],
                                   session_dir=layout["session"], decision=layout["decision"],
                                   platform="linux")

    def test_config_inside_workspace_rejected(self, layout):
        inside = layout["workspace"] / "config.json"
        inside.write_text("{}")
        with pytest.raises(oi.IsolationRefused, match="outside the agent workspace"):
            vs.sandbox_vendor_argv(layout["argv"], layout["workspace"], config_file=inside,
                                   session_dir=layout["session"], decision=layout["decision"],
                                   platform="linux")

    def test_config_inside_writable_session_rejected(self, layout):
        inside = layout["session"] / "config.json"
        inside.write_text("{}")
        with pytest.raises(oi.IsolationRefused, match="writable private state"):
            vs.sandbox_vendor_argv(layout["argv"], layout["workspace"], config_file=inside,
                                   session_dir=layout["session"], decision=layout["decision"],
                                   platform="linux")

    def test_session_dir_inside_workspace_rejected(self, layout):
        nested = layout["workspace"] / "sessions"
        nested.mkdir()
        with pytest.raises(oi.IsolationRefused, match="outside the agent workspace"):
            vs.sandbox_vendor_argv(layout["argv"], layout["workspace"],
                                   config_file=layout["config"], session_dir=nested,
                                   decision=layout["decision"], platform="linux")

    def test_session_dir_containing_workspace_rejected(self, layout):
        with pytest.raises(oi.IsolationRefused, match="must not contain"):
            vs.sandbox_vendor_argv(layout["argv"], layout["workspace"],
                                   config_file=layout["config"],
                                   session_dir=layout["workspace"].parent,
                                   decision=layout["decision"], platform="linux")

    def test_read_bind_aliasing_workspace_rejected(self, layout):
        inside = layout["workspace"] / "docs"
        inside.mkdir()
        with pytest.raises(oi.IsolationRefused, match="aliases"):
            vs.sandbox_vendor_argv(layout["argv"], layout["workspace"],
                                   config_file=layout["config"],
                                   session_dir=layout["session"],
                                   extra_read_binds=(inside,),
                                   decision=layout["decision"], platform="linux")

    def test_duplicate_binds_are_deduplicated(self, layout):
        command = vs.sandbox_vendor_argv(layout["argv"], layout["workspace"],
                                         config_file=layout["config"],
                                         session_dir=layout["session"],
                                         extra_read_binds=(layout["adapter"], layout["adapter"]),
                                         decision=layout["decision"], platform="linux")
        assert command.count(str(layout["adapter"].resolve())) == 2  # one --ro-bind pair


class TestFactoryTransport:
    def sdk(self):
        class Transport:
            class ProcessTransport:
                def __init__(self, *, exec_path, exec_args=None, cwd=None, env=None,
                             grace_period=5.0):
                    self.exec_path, self.exec_args, self.cwd, self.env = (
                        exec_path, exec_args, cwd, env)
        return type("sdk", (), {"transport": Transport})()

    def test_builds_documented_wrapped_transport(self, tmp_path):
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        config = tmp_path / "config.json"
        config.write_text("{}")
        droid = tmp_path / "bin" / "droid"
        droid.parent.mkdir()
        droid.write_text("#!/bin/sh\n")
        transport = vs.factory_sandbox_transport(
            workspace, config_file=config, exec_path=str(droid), sdk=self.sdk(),
            probe=fresh_probe(), platform="linux", which=bwrap_which())
        assert transport.exec_path.endswith("bwrap")
        joined = transport.exec_args
        assert "--unshare-all" in joined and "--share-net" in joined
        # Exact documented tail: real binary + default stream-jsonrpc flags.
        assert joined[-6:] == [str(droid.resolve()), "exec", "--input-format",
                               "stream-jsonrpc", "--output-format", "stream-jsonrpc"]
        assert str(workspace.resolve()) in joined
        assert transport.cwd == str(workspace.resolve())

    def test_refuses_without_probe_or_on_windows(self, tmp_path):
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        config = tmp_path / "config.json"
        config.write_text("{}")
        with pytest.raises(oi.IsolationRefused):
            vs.factory_sandbox_transport(workspace, config_file=config, exec_path="/x",
                                         sdk=self.sdk(), platform="linux",
                                         which=bwrap_which())  # no probe
        with pytest.raises(oi.IsolationRefused, match="Windows|cleanup"):
            vs.factory_sandbox_transport(workspace, config_file=config, exec_path="/x",
                                         sdk=self.sdk(), probe=fresh_probe(),
                                         platform="windows", which=bwrap_which())

    def test_missing_droid_binary_refused(self, tmp_path):
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        config = tmp_path / "config.json"
        config.write_text("{}")
        with pytest.raises(oi.IsolationRefused, match="executable"):
            vs.factory_sandbox_transport(workspace, config_file=config,
                                         exec_path=str(tmp_path / "absent-droid"),
                                         sdk=self.sdk(), probe=fresh_probe(),
                                         platform="linux", which=bwrap_which())


@pytest.mark.skipif(os.name != "posix" or not shutil.which("bwrap"),
                    reason="real probe requires Linux with bubblewrap installed")
class TestRealBubblewrap:
    def test_real_probe_passes(self):
        evidence = vs.probe_bwrap_enforcement(cache={})
        assert evidence.status == "passed", evidence.detail

    def test_real_sandbox_enforces_filesystem_policy(self, tmp_path):
        from shlex import quote
        workspace = tmp_path / "workspace"
        session = tmp_path / "sessions"
        workspace.mkdir()
        session.mkdir()
        config = tmp_path / "config.json"
        config.write_text("{}")
        argv = ["/bin/sh", "-c",
                "echo ok > agent.out && "
                f"echo state > {quote(str(session / 'session.state'))} && "
                "! (echo pwn > /etc/pwned 2>/dev/null) && "
                "! (echo pwn > \"$HOME/pwned\" 2>/dev/null) && "
                f"cat {quote(str(config))}"]
        command = vs.sandbox_vendor_argv(argv, workspace, config_file=config,
                                         session_dir=session)
        result = subprocess.run(command, capture_output=True, timeout=30)
        assert result.returncode == 0, result.stderr.decode()
        assert (workspace / "agent.out").read_text().strip() == "ok"
        assert (session / "session.state").read_text().strip() == "state"
        assert result.stdout.strip() == b"{}"
