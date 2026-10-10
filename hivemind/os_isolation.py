"""Fail-closed OS isolation policy, capability detection, planning, and validation.

This module is strictly read-only with respect to the host. It never creates
accounts, changes ACLs or file permissions, adjusts privileges, or touches
firewall rules; those remain explicit operator actions performed with the
provisioning scripts. What it does provide:

* honest capability detection (what this host can actually enforce),
* policy validation (rejecting unsupported combinations up front),
* fail-closed planning (a required control that cannot be enforced refuses
  the operation instead of silently degrading), and
* read-only validation of operator-provisioned state (distinct identities,
  protected bare-directory permissions).

Truth in advertising:

* Windows: Job Objects provide descendant process cleanup only. That is NOT a
  filesystem or network sandbox, and nothing here claims otherwise.
* Linux: bubblewrap (``bwrap``), when installed, provides a real filesystem/
  network sandbox for spawned subprocesses (git plumbing, allowlisted checks,
  the authorized extractor).
* Vendor SDK/RPC runtimes (Factory droid, oh-my-pi): strict OS isolation is
  unsupported. Their sessions are not verified sandboxes; requesting strict
  isolation for them is rejected, matching ``vendor_runtimes``.
"""
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

from .process_boundary import isolated_argv

MODES = ("disabled", "opportunistic", "required")
# Components whose child processes run through process_boundary.run_bounded.
COMPONENT_SUBPROCESS = "subprocess"   # git plumbing in git_collaboration
COMPONENT_CHECK = "check"             # TrustedVerifier allowlisted checks
COMPONENT_EXTRACTOR = "extractor"     # AuthorizedProcessExtractor
COMPONENT_VENDOR = "vendor"           # Vendor SDK/RPC runtimes
COMPONENTS = (COMPONENT_SUBPROCESS, COMPONENT_CHECK, COMPONENT_EXTRACTOR, COMPONENT_VENDOR)

IDENTITY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\-]{0,31}$")
RESERVED_IDENTITIES = frozenset({
    "root", "system", "administrator", "admin", "localsystem", "local_service",
    "network_service", "localservice", "networkservice", "guest", "default", "nobody"})


class IsolationRefused(PermissionError):
    """A required isolation control cannot be enforced on this host. Fail closed."""


@dataclass(frozen=True)
class IsolationPolicy:
    """Operator intent. ``required`` refuses anything that cannot be enforced."""
    mode: str = "disabled"
    sandbox_processes: bool = False
    separate_service_identity: bool = False
    separate_agent_identity: bool = False
    protect_bare_directories: bool = False

    def __post_init__(self):
        if self.mode not in MODES:
            raise ValueError(f"Isolation mode must be one of {MODES}")
        for name in ("sandbox_processes", "separate_service_identity",
                     "separate_agent_identity", "protect_bare_directories"):
            if getattr(self, name) is not True and getattr(self, name) is not False:
                raise ValueError(f"{name} must be a boolean")
        if self.mode == "disabled" and any((self.sandbox_processes, self.separate_service_identity,
                                            self.separate_agent_identity, self.protect_bare_directories)):
            raise ValueError("Disabled isolation cannot request individual controls")
        if self.mode != "disabled" and not any((self.sandbox_processes, self.separate_service_identity,
                                                self.separate_agent_identity, self.protect_bare_directories)):
            raise ValueError("An active isolation mode must request at least one control")
        if self.separate_agent_identity and not self.separate_service_identity:
            raise ValueError("Agent identity separation requires a separate service identity")

    @property
    def required(self):
        return self.mode == "required"


@dataclass(frozen=True)
class PlatformCapabilities:
    """What this host can actually enforce. Never overstates."""
    platform: str                    # "windows" | "linux" | "other-posix" | "other"
    process_cleanup: bool            # Job Objects / process-group kill: cleanup, not a sandbox
    sandbox: str                     # "bubblewrap" | "none": a real fs/network sandbox
    sandbox_filesystem_network: bool  # True only when a real sandbox is present

    @property
    def windows_sandbox(self):
        return False  # Job Objects are cleanup only; no Windows sandbox is ever claimed.


def detect_capabilities(*, platform=None, which=None):
    """Probe the host (injectable for tests). Detection never mutates anything."""
    which = which or shutil.which
    if platform is None:
        platform = {"win32": "windows", "linux": "linux"}.get(
            sys.platform, "other-posix" if os.name == "posix" else "other")
    sandbox = "none"
    if platform in ("linux", "other-posix") and which("bwrap"):
        sandbox = "bubblewrap"
    return PlatformCapabilities(platform=platform, process_cleanup=True, sandbox=sandbox,
                                sandbox_filesystem_network=sandbox != "none")


def validate_identities(service_identity, agent_identity=None):
    """Validate operator-provisioned account names. Never creates or modifies accounts."""
    if service_identity is None:
        raise ValueError("service_identity is required")
    for label, value in (("service_identity", service_identity), ("agent_identity", agent_identity)):
        if value is None:
            continue
        if not isinstance(value, str) or not IDENTITY_PATTERN.match(value):
            raise ValueError(f"{label} must be 1-32 chars: letters, digits, '_', '.', '-'; leading letter/'_'")
        if value.lower().rstrip("$") in RESERVED_IDENTITIES:
            raise ValueError(f"{label} must not be a reserved or built-in account")
    if agent_identity is not None and agent_identity.lower() == service_identity.lower():
        raise ValueError("Agent and service identities must be distinct accounts")
    return True


@dataclass(frozen=True)
class ControlDecision:
    name: str
    status: str   # "enforced" | "degraded" | "external" | "refused" | "disabled"
    detail: str


@dataclass(frozen=True)
class IsolationPlan:
    policy: IsolationPolicy
    capabilities: PlatformCapabilities
    decisions: tuple = field(default_factory=tuple)

    @property
    def allowed(self):
        return all(decision.status != "refused" for decision in self.decisions)

    def decision(self, name):
        for item in self.decisions:
            if item.name == name:
                return item
        raise KeyError(name)


def plan(policy, capabilities=None):
    """Fail-closed plan: in ``required`` mode an unenforceable control refuses."""
    capabilities = capabilities or detect_capabilities()
    decisions = []

    if policy.sandbox_processes:
        if capabilities.sandbox_filesystem_network:
            decisions.append(ControlDecision(
                "sandbox_processes", "enforced",
                f"{capabilities.sandbox} provides a filesystem/network sandbox for spawned processes"))
        elif policy.required:
            decisions.append(ControlDecision(
                "sandbox_processes", "refused",
                "No OS filesystem/network sandbox is available on this host; refusing to run "
                "unsandboxed under a required policy"))
        else:
            note = ("Windows Job Objects provide descendant cleanup only, not a sandbox"
                    if capabilities.platform == "windows" else
                    "No sandbox backend (bubblewrap) is installed; process cleanup only")
            decisions.append(ControlDecision("sandbox_processes", "degraded", note))

    for name, requested in (("separate_service_identity", policy.separate_service_identity),
                            ("separate_agent_identity", policy.separate_agent_identity),
                            ("protect_bare_directories", policy.protect_bare_directories)):
        if not requested:
            continue
        # These controls are operator-provisioned host state. The code can only
        # validate them (read-only); it cannot enforce them by itself.
        decisions.append(ControlDecision(
            name, "external",
            "Requires explicit operator provisioning (see scripts/provision-isolation.*); "
            "verify with validate_identities()/validate_directory_protection() before use"))

    if not decisions:
        decisions.append(ControlDecision("isolation", "disabled", "No isolation controls requested"))
    result = IsolationPlan(policy, capabilities, tuple(decisions))
    if policy.required and not result.allowed:
        refused = [d for d in decisions if d.status == "refused"]
        raise IsolationRefused("Required OS isolation is unavailable: " + "; ".join(d.detail for d in refused))
    return result


def component_applicability(component):
    """Whether each call-site kind can receive a real OS sandbox at all."""
    if component not in COMPONENTS:
        raise ValueError(f"Unknown isolation component {component!r}")
    if component == COMPONENT_VENDOR:
        return {"component": component, "subprocess_boundary": False, "sandbox_supported": False,
                "strict_supported": False,
                "detail": "Vendor SDK/RPC sessions are not verified OS sandboxes; strict isolation "
                          "is unsupported and must be rejected"}
    return {"component": component, "subprocess_boundary": True, "sandbox_supported": True,
            "strict_supported": True,
            "detail": "Runs through process_boundary.run_bounded; a real sandbox applies where "
                      "the platform provides one"}


def sandbox_argv(argv, workspace, policy, *, component=COMPONENT_SUBPROCESS, capabilities=None):
    """Return the argv to launch under ``policy`` or refuse. Fail closed.

    Vendor runtimes are rejected unconditionally. A required sandbox with no
    backend refuses before anything launches. Opportunistic mode uses the
    sandbox when present and otherwise returns the plain argv (degraded).
    """
    applicability = component_applicability(component)
    if not applicability["sandbox_supported"]:
        raise IsolationRefused(
            f"Strict OS isolation is unsupported for {component} runtimes: {applicability['detail']}")
    if not policy.sandbox_processes:
        return list(argv)
    capabilities = capabilities or detect_capabilities()
    if not capabilities.sandbox_filesystem_network:
        if policy.required:
            raise IsolationRefused("Required OS filesystem/network isolation is unavailable on this host")
        return list(argv)
    return isolated_argv(argv, workspace, require_os_isolation=True)


def validate_directory_protection(path, *, platform=None, expected_owner_uid=None):
    """Read-only audit of a protected bare-directory's permissions.

    POSIX: the directory must not be writable by group or other, and when
    ``expected_owner_uid`` is given the owner must match. Windows: no portable
    ACL API is used here, so the audit confirms only that the path is a real
    local directory (network/UNC roots are rejected, matching
    ``git_collaboration``) and reports that ACL verification is an operator
    responsibility. Never changes permissions.
    """
    if platform is None:
        platform = detect_capabilities().platform
    if str(path).startswith("\\\\"):
        raise PermissionError("Protected directories must not live on a network filesystem")
    root = Path(path)
    if not root.is_dir():
        raise ValueError("Protected path must be an existing directory")
    root = root.resolve(strict=True)
    if str(root).startswith("\\\\"):
        raise PermissionError("Protected directories must not live on a network filesystem")
    if platform == "windows":
        return {"path": str(root), "platform": platform, "protected": None,
                "detail": "ACL inspection is not performed by this module on Windows; the operator "
                          "must verify service/agent ACLs via scripts/provision-isolation.ps1 -CheckOnly"}
    mode = root.stat().st_mode
    if mode & 0o022:
        raise PermissionError("Protected directory is writable by group or other")
    if expected_owner_uid is not None and root.stat().st_uid != expected_owner_uid:
        raise PermissionError("Protected directory is not owned by the expected account")
    return {"path": str(root), "platform": platform, "protected": True,
            "detail": "Directory is not writable by group or other"}
