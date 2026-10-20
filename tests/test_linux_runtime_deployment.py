"""Real Linux deployment checks, never simulated containment evidence."""
import os
import sys
from pathlib import Path

import pytest

from hivemind.linux_process_boundary import delegated_root
from hivemind.storage import Problem
from test_additional_integrations import evidence, native

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Real Linux deployment checks")


def test_real_non_cgroup_directory_refuses(tmp_path):
    with pytest.raises(PermissionError, match="real delegated cgroup"):
        delegated_root(tmp_path)


def test_real_default_cgroup_mount_requires_private_delegation():
    root = Path("/sys/fs/cgroup")
    if not root.exists():
        pytest.skip("No mounted cgroup hierarchy")
    if os.geteuid() != 0 and root.stat().st_uid == os.geteuid() and os.access(root, os.W_OK):
        pytest.skip("This host has already delegated the default cgroup root")
    with pytest.raises(PermissionError):
        delegated_root(root)


def test_real_native_verifier_refuses_unavailable_sandbox_before_staging(tmp_path):
    verifier = native(tmp_path)
    verifier.isolated = True
    assessment = verifier.preflight()
    if assessment["ready"]:
        pytest.skip("Sandbox is available; refusal branch does not apply")
    with pytest.raises(Problem, match="native_sandbox_unverified"):
        verifier.run_bound("syntax", evidence())
    assert not list(tmp_path.iterdir())


def test_real_native_verifier_checks_exact_json_inside_sandbox(tmp_path):
    from hivemind.native_artifact_verifier import NativeArtifactVerifier, NativeCheckProfile
    verifier = NativeArtifactVerifier(
        staging_root=tmp_path, enabled=True, require_os_isolation=True,
        profiles=[NativeCheckProfile("syntax", "project", (sys.executable, "-m", "json.tool", "artifact.json"),
                                    input_path="artifact.json")])
    if not verifier.preflight()["ready"]:
        pytest.skip("Real namespace proof unavailable; no sandboxed verifier claim")
    good = evidence('{"synthetic": true}\n')
    result = verifier.run_bound("syntax", good)
    assert result["ok"] and result["sandbox_verified"] and result["input_unchanged"]
    assert result["evidence"] == good
    invalid = verifier.run_bound("syntax", evidence("{not valid json"))
    assert not invalid["ok"] and invalid["sandbox_verified"] and invalid["input_unchanged"]
