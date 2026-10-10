import os
import shutil
import sys

import pytest

from hivemind import os_isolation as oi


def capabilities(platform="linux", bwrap=True):
    return oi.detect_capabilities(platform=platform, which=lambda _: "/usr/bin/bwrap" if bwrap else None)


class TestPolicyValidation:
    def test_rejects_unknown_mode_and_non_boolean_controls(self):
        with pytest.raises(ValueError, match="one of"):
            oi.IsolationPolicy(mode="paranoid")
        with pytest.raises(ValueError, match="boolean"):
            oi.IsolationPolicy(mode="required", sandbox_processes="yes")

    def test_disabled_mode_cannot_request_controls(self):
        with pytest.raises(ValueError, match="[Dd]isabled"):
            oi.IsolationPolicy(mode="disabled", sandbox_processes=True)

    def test_active_mode_requires_a_control(self):
        with pytest.raises(ValueError, match="at least one"):
            oi.IsolationPolicy(mode="opportunistic")

    def test_agent_identity_requires_service_identity(self):
        with pytest.raises(ValueError, match="service identity"):
            oi.IsolationPolicy(mode="required", separate_agent_identity=True)

    def test_valid_policy_constructs(self):
        policy = oi.IsolationPolicy(mode="required", sandbox_processes=True,
                                    separate_service_identity=True, separate_agent_identity=True,
                                    protect_bare_directories=True)
        assert policy.required and not oi.IsolationPolicy(mode="opportunistic", sandbox_processes=True).required


class TestCapabilityDetection:
    def test_windows_reports_cleanup_only_and_never_a_sandbox(self):
        caps = capabilities("windows", bwrap=True)
        assert caps.platform == "windows" and caps.process_cleanup
        assert caps.sandbox == "none" and not caps.sandbox_filesystem_network
        assert caps.windows_sandbox is False

    def test_linux_bubblewrap_is_a_real_sandbox(self):
        caps = capabilities("linux", bwrap=True)
        assert caps.sandbox == "bubblewrap" and caps.sandbox_filesystem_network

    def test_linux_without_bubblewrap_has_no_sandbox(self):
        caps = capabilities("linux", bwrap=False)
        assert caps.sandbox == "none" and not caps.sandbox_filesystem_network


class TestIdentityValidation:
    def test_accepts_distinct_well_formed_accounts(self):
        assert oi.validate_identities("hivemind-service", "hivemind.agent_1")

    def test_rejects_identical_accounts_case_insensitively(self):
        with pytest.raises(ValueError, match="distinct"):
            oi.validate_identities("Hivemind-Agent", "hivemind-agent")

    @pytest.mark.parametrize("name", ["root", "SYSTEM", "Administrator", "nobody"])
    def test_rejects_reserved_accounts(self, name):
        with pytest.raises(ValueError, match="reserved"):
            oi.validate_identities(name)

    @pytest.mark.parametrize("name", ["", "1lead-digit", "two words", "semi;colon", "a" * 33, None])
    def test_rejects_malformed_names(self, name):
        with pytest.raises(ValueError):
            oi.validate_identities(name)

    def test_agent_identity_is_optional(self):
        assert oi.validate_identities("hivemind-service")


class TestPlanning:
    def test_required_sandbox_refuses_on_windows(self):
        policy = oi.IsolationPolicy(mode="required", sandbox_processes=True)
        with pytest.raises(oi.IsolationRefused, match="unavailable"):
            oi.plan(policy, capabilities("windows", bwrap=True))

    def test_required_sandbox_refuses_on_linux_without_bubblewrap(self):
        policy = oi.IsolationPolicy(mode="required", sandbox_processes=True)
        with pytest.raises(oi.IsolationRefused):
            oi.plan(policy, capabilities("linux", bwrap=False))

    def test_required_sandbox_enforced_with_bubblewrap(self):
        result = oi.plan(oi.IsolationPolicy(mode="required", sandbox_processes=True),
                         capabilities("linux", bwrap=True))
        assert result.allowed and result.decision("sandbox_processes").status == "enforced"

    def test_opportunistic_degrades_instead_of_refusing(self):
        result = oi.plan(oi.IsolationPolicy(mode="opportunistic", sandbox_processes=True),
                         capabilities("windows"))
        decision = result.decision("sandbox_processes")
        assert result.allowed and decision.status == "degraded" and "not a sandbox" in decision.detail

    def test_identity_and_acl_controls_are_external_not_enforced(self):
        policy = oi.IsolationPolicy(mode="required", separate_service_identity=True,
                                    separate_agent_identity=True, protect_bare_directories=True)
        result = oi.plan(policy, capabilities("linux"))
        assert result.allowed
        assert all(result.decision(name).status == "external" for name in
                   ("separate_service_identity", "separate_agent_identity", "protect_bare_directories"))

    def test_disabled_policy_plans_disabled(self):
        result = oi.plan(oi.IsolationPolicy(), capabilities("linux"))
        assert result.allowed and result.decision("isolation").status == "disabled"


class TestComponentApplicability:
    def test_vendor_strict_isolation_is_unsupported(self):
        applicability = oi.component_applicability("vendor")
        assert not applicability["sandbox_supported"] and not applicability["strict_supported"]

    @pytest.mark.parametrize("component", ["subprocess", "check", "extractor"])
    def test_subprocess_components_support_sandbox(self, component):
        assert oi.component_applicability(component)["strict_supported"]

    def test_unknown_component_rejected(self):
        with pytest.raises(ValueError, match="Unknown"):
            oi.component_applicability("sidechannel")


class TestSandboxLauncher:
    def policy(self, mode="required"):
        return oi.IsolationPolicy(mode=mode, sandbox_processes=True)

    def test_vendor_rejected_even_with_bubblewrap(self, tmp_path):
        with pytest.raises(oi.IsolationRefused, match="vendor"):
            oi.sandbox_argv(["/bin/true"], tmp_path, self.policy(),
                            component="vendor", capabilities=capabilities("linux", bwrap=True))

    def test_disabled_sandbox_returns_plain_argv(self, tmp_path):
        argv = ["git", "status"]
        assert oi.sandbox_argv(argv, tmp_path, oi.IsolationPolicy()) == argv

    def test_required_without_backend_refuses_before_launch(self, tmp_path):
        with pytest.raises(oi.IsolationRefused, match="unavailable"):
            oi.sandbox_argv([sys.executable, "-c", "raise Exception('must not run')"], tmp_path,
                            self.policy(), capabilities=capabilities("windows"))

    def test_opportunistic_without_backend_returns_plain_argv(self, tmp_path):
        argv = ["git", "status"]
        assert oi.sandbox_argv(argv, tmp_path, self.policy("opportunistic"),
                               capabilities=capabilities("windows")) == argv

    def test_linux_bubblewrap_wraps_argv(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(oi, "isolated_argv",
                            lambda argv, workspace, require_os_isolation=False:
                            calls.append(require_os_isolation) or ["bwrap", "--", *argv])
        command = oi.sandbox_argv(["/bin/echo", "hi"], tmp_path, self.policy(),
                                  capabilities=capabilities("linux", bwrap=True))
        assert calls == [True] and command == ["bwrap", "--", "/bin/echo", "hi"]


@pytest.mark.skipif(os.name != "posix" or not shutil.which("bwrap"),
                    reason="real sandbox test requires Linux with bubblewrap installed")
def test_real_bubblewrap_sandbox_runs_and_denies_network(tmp_path):
    from hivemind.process_boundary import run_bounded
    env = {"PATH": os.environ.get("PATH", "")}
    code, stdout, _ = run_bounded([sys.executable, "-c", "print('sandboxed')"], tmp_path, env, 10,
                                  require_os_isolation=True)
    assert code == 0 and stdout.strip() == b"sandboxed"
    code, _, _ = run_bounded(
        [sys.executable, "-c", "import socket; socket.create_connection(('10.255.255.1', 9), 2)"],
        tmp_path, env, 10, require_os_isolation=True)
    assert code != 0  # unshared network namespace has no route


class TestDirectoryProtection:
    def test_posix_rejects_group_or_other_writable(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX mode bits")
        target = tmp_path / "bare.git"
        target.mkdir()
        target.chmod(0o775)
        with pytest.raises(PermissionError, match="group or other"):
            oi.validate_directory_protection(target, platform="linux")
        target.chmod(0o700)
        report = oi.validate_directory_protection(target, platform="linux",
                                                  expected_owner_uid=os.geteuid())
        assert report["protected"] is True

    def test_posix_rejects_wrong_owner(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX ownership")
        target = tmp_path / "bare.git"
        target.mkdir()
        target.chmod(0o700)
        with pytest.raises(PermissionError, match="owned"):
            oi.validate_directory_protection(target, platform="linux",
                                             expected_owner_uid=os.geteuid() + 1)

    def test_windows_audit_is_honest_about_limited_assurance(self, tmp_path):
        report = oi.validate_directory_protection(tmp_path, platform="windows")
        assert report["protected"] is None and "operator" in report["detail"]

    def test_network_root_rejected(self, tmp_path):
        with pytest.raises(PermissionError, match="network"):
            oi.validate_directory_protection(r"\\server\share", platform="windows")

    def test_missing_path_rejected(self, tmp_path):
        with pytest.raises(ValueError):
            oi.validate_directory_protection(tmp_path / "absent", platform="linux")
