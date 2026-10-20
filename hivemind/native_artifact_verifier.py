"""Operator-approved native profiles over exact staged artifact bytes.

Nothing resolves agent-supplied paths or commands. Each invocation retains its
private snapshot for audit, bounded by an operator-set admission count. Existing
artifact-contract durable intents govern replay and unknown effects.
"""
import hashlib
import json
import os
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path

from . import models as m
from .storage import Problem, canonical
from .verification import TrustedVerifier, _validate_check


@dataclass(frozen=True)
class NativeCheckProfile:
    name: str
    project_id: str
    argv: tuple[str, ...]
    input_path: str = "artifact.txt"
    timeout_seconds: int = 30

    def __post_init__(self):
        object.__setattr__(self, "argv", _validate_check(self.name, self.argv))
        if not Path(self.argv[0]).is_absolute() or not self.project_id:
            raise ValueError("Profiles require absolute operator executables and a project")
        if type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 300:
            raise ValueError("Native check deadline must be a bounded integer")
        if m.safe_path(self.input_path) != self.input_path:
            raise ValueError("Native input path must be a fixed relative path")


class NativeArtifactVerifier:
    def __init__(self, *, staging_root, profiles, enabled=False, require_os_isolation=True,
                 max_snapshots=128, runner=None):
        if type(enabled) is not bool or type(require_os_isolation) is not bool:
            raise ValueError("Native verifier opt-ins must be strict booleans")
        if type(max_snapshots) is not int or not 1 <= max_snapshots <= 1024:
            raise ValueError("Native snapshot bound is invalid")
        from .worker_provisioning import checked_directory
        self.root = checked_directory(staging_root)
        if not self.root.is_dir() or len(profiles) > 64 or not profiles:
            raise ValueError("Native verifier requires a staging directory and bounded profiles")
        self.profiles = {profile.name: profile for profile in profiles}
        if len(self.profiles) != len(profiles):
            raise ValueError("Duplicate native check profile")
        self.enabled, self.isolated, self.limit, self.runner = enabled, require_os_isolation, max_snapshots, runner
        self.lock = threading.Lock()

    def preflight(self):
        """Read-only deployment assessment, never an agent-controlled launcher."""
        from .vendor_sandbox import probe_bwrap_gateway_enforcement
        from .worker_provisioning import checked_directory
        checked_directory(self.root)
        executables = all(Path(profile.argv[0]).is_file() for profile in self.profiles.values())
        proof = probe_bwrap_gateway_enforcement() if self.isolated and self.runner is None else None
        isolated = bool(proof and proof.status == "passed" and proof.network_isolated)
        return {"enabled": self.enabled, "profiles": len(self.profiles), "executables_present": executables,
                "required_os_isolation": self.isolated,
                "sandbox_ready": isolated, "injected_runner": self.runner is not None,
                "ready": self.enabled and executables and (not self.isolated or isolated),
                "sandbox_scope": "read-only-filesystem-and-no-network" if isolated else "unverified"}

    def run_bound(self, name, evidence):
        if not self.enabled:
            raise Problem("native_verification_disabled", 403)
        profile = self.profiles.get(name)
        if profile is None or evidence.get("project_id") != profile.project_id:
            raise Problem("native_check_not_allowlisted", 403)
        if evidence.get("check") != name or not all(evidence.get(k) for k in
                ("contract_id", "contract_revision", "task_id", "execution_id", "run_id", "agent_id", "output")):
            raise Problem("native_evidence_unbound", 422)
        # Deep-freeze caller dictionaries before off-lock native effects.
        frozen = json.loads(canonical(evidence))
        inputs = frozen.get("inputs", [])
        pins = [item for item in frozen.get("artifacts", []) if item["name"] == frozen["output"]]
        if len(inputs) != 1 or len(pins) != 1 or inputs[0]["name"] != frozen["output"]:
            raise Problem("native_input_unbound", 422)
        encoded = inputs[0]["encoded"]
        if (not isinstance(encoded, str) or len(encoded.encode("utf-8")) > 1048576
                or hashlib.sha256(encoded.encode("utf-8")).hexdigest() != pins[0]["sha256"]):
            raise Problem("native_input_checksum_mismatch", 422)
        content = json.loads(encoded)
        text = content.get("text") if isinstance(content, dict) else None
        if not isinstance(text, str):
            raise Problem("native_inline_input_required", 422)
        if self.isolated and not self.preflight()["sandbox_ready"]:
            raise Problem("native_sandbox_unverified", 409)
        data = text.encode("utf-8")
        with self.lock:
            if sum(1 for _, _path in zip(range(self.limit), self.root.iterdir(), strict=False)) >= self.limit:
                raise Problem("native_snapshot_limit", 409)
            stage = Path(tempfile.mkdtemp(prefix="artifact-", dir=self.root))
        if os.name != "nt":
            stage.chmod(0o700)
        destination = stage / profile.input_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        verifier = TrustedVerifier(stage, {name: profile.argv}, runner=self.runner,
                                   timeout=profile.timeout_seconds, require_os_isolation=self.isolated)
        result = verifier.run(name)
        # Revalidate the exact staged input. A mutating native check cannot certify it.
        try:
            safe = all(not part.is_symlink() and not
                       (getattr(part.stat(), "st_file_attributes", 0) & 0x400)
                       for part in (destination, *destination.parents) if part != stage.parent)
            with destination.open("rb") as handle:
                unchanged = safe and handle.read(len(data) + 1) == data
        except OSError:
            unchanged = False
        return {
            "ok": result.ok and unchanged, "evidence": frozen, "name": name,
            "returncode": result.returncode, "timed_out": result.timed_out,
            "input_sha256": hashlib.sha256(data).hexdigest(),
            "input_unchanged": unchanged, "stdout_sha256": result.stdout_sha256,
            "stderr_sha256": result.stderr_sha256,
            "sandbox_verified": self.isolated and self.runner is None and result.returncode is not None,
        }
