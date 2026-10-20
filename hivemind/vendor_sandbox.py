"""Verified sandbox assessment and launchers for vendor runtimes.

Truth in advertising, matching ``vendor_runtimes`` and ``os_isolation``:

* **Scope honesty**: filesystem-only mode explicitly shares the host network.
  Strict Linux mode keeps a private network namespace and permits egress only
  through a private, origin-allowlisted Unix-socket gateway. A fresh network
  probe and gateway are mandatory; filesystem-only evidence never upgrades.
* **Proof, not presence**: bubblewrap being on ``PATH`` is not proof of
  enforcement. An ``enforced`` verdict requires a fresh, real, bounded probe
  (:func:`probe_bwrap_enforcement`) that actually enters a sandbox and proves
  an out-of-sandbox path is invisible. Probe results are cached briefly and
  must be fresh.
* **Factory droid**: SDK 0.5.0 exposes a documented process-launch interface,
  ``droid_sdk.transport.ProcessTransport(exec_path=..., exec_args=..., cwd=...,
  env=...)``, which ``connect()`` spawns via ``asyncio.create_subprocess_exec``
  (default args: ``exec --input-format stream-jsonrpc --output-format
  stream-jsonrpc``). Wrapping that documented ``exec_path``/``exec_args`` pair
  in bubblewrap is a genuine sandbox around the real vendor process — no
  invented flags, no invented protocol. :func:`factory_sandbox_transport`
  builds exactly that; the caller passes it to ``Runtime(transport=...)`` and
  connects it per the SDK contract.
* **oh-my-pi**: Hivemind itself launches the vendor process
  (``JsonLinesProcess`` over an argv Hivemind builds), so the same bubblewrap
  wrap applies via :func:`sandbox_vendor_argv`.
* **Windows**: no vendor sandbox exists. Job Objects are descendant cleanup
  only and are never claimed as a sandbox; every vendor sandbox request on
  Windows is refused.

Actual vendor binary paths, auth/session mounts, and egress policy are
explicit operator inputs (``exec_path``, ``extra_read_binds``,
``extra_write_binds``); nothing here guesses them.
"""
import os
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from .os_isolation import IsolationRefused, detect_capabilities

VENDOR_DROID = "droid"
VENDOR_OH_MY_PI = "oh-my-pi"
VENDORS = (VENDOR_DROID, VENDOR_OH_MY_PI)

DROID_DEFAULT_EXEC_ARGS = ("exec", "--input-format", "stream-jsonrpc",
                           "--output-format", "stream-jsonrpc")

NETWORK_PREREQUISITE = (
    "network is shared with the host (--share-net) because vendor model inference "
    "requires egress; this sandbox is FILESYSTEM-ONLY and does not satisfy a "
    "filesystem+network isolation requirement; egress restriction must come from "
    "an operator-managed proxy/firewall")
AUTH_PREREQUISITE = (
    "vendor credentials/config must live outside the agent workspace and are "
    "mounted read-only; the sandbox does not protect against a vendor process "
    "legitimately using its own credentials")
SDK_PREREQUISITE = (
    "factory sandboxing requires droid_sdk >= 0.5.0 with the documented "
    "droid_sdk.transport.ProcessTransport interface")

PROBE_TTL_SECONDS = 300.0


# --- Bounded real enforcement probes -----------------------------------------

@dataclass(frozen=True)
class ProbeEvidence:
    """Result of a real, bounded sandbox-enforcement probe."""
    backend: str          # "bubblewrap"
    status: str           # "passed" | "failed"
    detail: str
    checked_at: float
    network_isolated: bool = False

    def assert_fresh(self, max_age_seconds=PROBE_TTL_SECONDS, *, clock=time.time):
        age = clock() - self.checked_at
        if age < 0 or age > max_age_seconds:
            raise IsolationRefused(
                f"Sandbox probe evidence is stale or from the future "
                f"({age:.1f}s old); re-probe before relying on it")
        return True


_probe_cache = {}


def clear_probe_cache():
    _probe_cache.clear()


def probe_bwrap_enforcement(*, which=shutil.which, runner=subprocess.run,
                            clock=time.time, timeout=10, cache=None,
                            platform=None):
    """Actually enter a bubblewrap sandbox and prove filesystem enforcement.

    Two bounded real executions: (1) a trivial command must succeed inside the
    sandbox, (2) a host path that was NOT bound must be invisible inside.
    Anything else — missing binary, non-zero exits, timeouts, a leaked host
    path — is ``failed`` with a precise detail. Results are cached for
    ``PROBE_TTL_SECONDS``; callers enforce freshness via
    :meth:`ProbeEvidence.assert_fresh`.
    """
    cache = _probe_cache if cache is None else cache
    executable = which("bwrap")
    if not executable:
        return ProbeEvidence("bubblewrap", "failed", "bubblewrap is not on PATH", clock())
    if (platform or sys.platform) != "linux":
        return ProbeEvidence("bubblewrap", "failed",
                             f"probes run on Linux only, not {platform or sys.platform}",
                             clock())
    cached = cache.get(executable)
    if cached is not None and 0 <= clock() - cached.checked_at <= PROBE_TTL_SECONDS:
        return cached

    base = ["--unshare-all", "--share-net", "--new-session", "--die-with-parent",
            "--cap-drop", "ALL", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]
    for directory in ("/usr", "/bin", "/lib", "/lib64"):
        if Path(directory).exists():
            base.extend(["--ro-bind", directory, directory])
    evidence = None
    with tempfile.TemporaryDirectory(prefix="hivemind-probe-") as scratch:
        sentinel = Path(scratch) / "sentinel.must-not-be-visible"
        sentinel.write_text("secret")
        try:
            inside = runner([executable, *base, "--", "/bin/sh", "-c", "exit 0"],
                            capture_output=True, timeout=timeout)
            if inside.returncode != 0:
                evidence = ProbeEvidence("bubblewrap", "failed",
                                         "sandboxed trivial command failed: "
                                         f"{inside.stderr.decode(errors='replace')[:200]}",
                                         clock())
            else:
                leak = runner([executable, *base, "--", "/bin/sh", "-c",
                               f"test -e {sentinel}"], capture_output=True, timeout=timeout)
                if leak.returncode == 0:
                    evidence = ProbeEvidence(
                        "bubblewrap", "failed",
                        "an unbound host path was VISIBLE inside the sandbox; "
                        "filesystem enforcement is broken", clock())
                elif leak.returncode == 1:
                    evidence = ProbeEvidence(
                        "bubblewrap", "passed",
                        "sandboxed command ran and an unbound host path was invisible",
                        clock())
                else:
                    evidence = ProbeEvidence("bubblewrap", "failed",
                                             "filesystem sentinel check did not complete reliably",
                                             clock())
        except (OSError, subprocess.TimeoutExpired) as exc:
            evidence = ProbeEvidence("bubblewrap", "failed",
                                     f"probe could not execute: {exc}", clock())
    cache[executable] = evidence
    return evidence


def probe_bwrap_gateway_enforcement(*, which=shutil.which, runner=subprocess.run,
                                    clock=time.time, timeout=10):
    """Fresh local proof: private filesystem, distinct netns, no host loopback."""
    if sys.platform != "linux":
        return ProbeEvidence("bubblewrap", "failed", "Network probes require Linux", clock())
    executable = which("bwrap")
    python = which("python3")
    if not executable or not python:
        return ProbeEvidence("bubblewrap", "failed", "bubblewrap and Python 3 are required", clock())
    command = [executable, "--unshare-all", "--new-session", "--die-with-parent",
               "--cap-drop", "ALL", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]
    for directory in ("/usr", "/bin", "/lib", "/lib64"):
        if Path(directory).exists():
            command.extend(["--ro-bind", directory, directory])
    with tempfile.TemporaryDirectory(prefix="hivemind-net-probe-") as scratch, \
            socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sentinel:
        hidden = Path(scratch) / "host-sentinel"
        hidden.write_text("synthetic probe")
        sentinel.bind(("127.0.0.1", 0))
        sentinel.listen(1)
        inode = os.stat("/proc/self/ns/net").st_ino
        script = (
            "import os,socket,sys\n"
            "if os.path.exists(sys.argv[1]): sys.exit(11)\n"
            "if os.stat('/proc/self/ns/net').st_ino == int(sys.argv[2]): sys.exit(12)\n"
            "if any(name != 'lo' for _,name in socket.if_nameindex()): sys.exit(13)\n"
            "s=socket.socket(); s.settimeout(1)\n"
            "try: s.connect(('127.0.0.1',int(sys.argv[3])))\n"
            "except OSError: sys.exit(0)\n"
            "sys.exit(14)\n")
        try:
            result = runner([*command, "--", python, "-I", "-c", script, str(hidden),
                             str(inode), str(sentinel.getsockname()[1])],
                            capture_output=True, timeout=timeout)
            passed = result.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            passed = False
    return ProbeEvidence("bubblewrap", "passed" if passed else "failed",
                         "Filesystem and network namespace probe " + ("passed" if passed else "failed"),
                         clock(), network_isolated=passed)


# --- Assessment ---------------------------------------------------------------

@dataclass(frozen=True)
class VendorSandboxDecision:
    """Honest per-vendor, per-host sandbox verdict."""
    vendor: str
    platform: str
    status: str                 # "enforced" | "refused" | "unsupported"
    sandbox: str                # "bubblewrap" | "none"
    scope: str                  # "filesystem-only" | "filesystem-and-gateway" | "none"
    filesystem_isolated: bool
    network_isolated: bool      # direct networking isolated; gateway egress remains granted
    probe: ProbeEvidence = None
    prerequisites: tuple = field(default_factory=tuple)
    detail: str = ""

    @property
    def allowed(self):
        return self.status == "enforced"


def _refused(vendor, platform, detail, prerequisites=(), probe=None):
    return VendorSandboxDecision(
        vendor=vendor, platform=platform, status="refused", sandbox="none",
        scope="none", filesystem_isolated=False, network_isolated=False,
        probe=probe, prerequisites=tuple(prerequisites), detail=detail)


def _sdk_with_process_transport(sdk):
    """Return the SDK module iff the documented ProcessTransport exists."""
    if sdk is None:
        try:
            from droid_sdk import transport
        except ImportError:
            return None
        return transport if hasattr(transport, "ProcessTransport") else None
    # An explicitly supplied SDK stands on its own; never fall back to a
    # different import behind the caller's back.
    transport = getattr(sdk, "transport", None)
    return transport if transport is not None and hasattr(transport, "ProcessTransport") else None


def assess_vendor_sandbox(vendor, *, platform=None, which=None, probe=None,
                          require_network_isolation=False, sdk=None,
                          clock=time.time, gateway=False):
    """Decide, with real capability AND probe evidence, what is enforceable.

    Strict network isolation requires an explicit gateway and fresh namespace
    proof. Without either, refuse instead of using shared networking. The
    filesystem-only scope must be opted into explicitly. Droid also requires
    the documented SDK transport interface.
    """
    if vendor not in VENDORS:
        raise ValueError(f"Unknown vendor {vendor!r}; expected one of {VENDORS}")
    capabilities = detect_capabilities(platform=platform, which=which)

    if require_network_isolation and not gateway:
        return _refused(vendor, capabilities.platform,
                        "vendor sandboxes share the host network (--share-net) because "
                        "model inference requires egress; a filesystem+network isolation "
                        "requirement cannot be met and is refused, not degraded")
    if capabilities.platform == "windows":
        return _refused(vendor, capabilities.platform,
                        "Windows Job Objects provide descendant cleanup only, not a "
                        "sandbox; vendor isolation on Windows is refused")
    if not capabilities.sandbox_filesystem_network:
        return _refused(vendor, capabilities.platform,
                        "No sandbox backend (bubblewrap) is installed; vendor isolation "
                        "is refused rather than degraded",
                        ("install bubblewrap (bwrap) to enable the vendor sandbox",))

    if probe is None and platform is None:
        probe = (probe_bwrap_gateway_enforcement(clock=clock) if require_network_isolation
                 else probe_bwrap_enforcement(clock=clock))
    if probe is None:
        return _refused(vendor, capabilities.platform,
                        "bubblewrap presence is not proof of enforcement and no probe "
                        "evidence was supplied; refusing until a real probe passes")
    try:
        probe.assert_fresh(clock=clock)
    except IsolationRefused as exc:
        return _refused(vendor, capabilities.platform, str(exc), probe=probe)
    if probe.status != "passed":
        return _refused(vendor, capabilities.platform,
                        f"sandbox enforcement probe failed: {probe.detail}", probe=probe)
    if require_network_isolation and not probe.network_isolated:
        return _refused(vendor, capabilities.platform,
                        "Filesystem-only evidence cannot authorize network isolation", probe=probe)

    prerequisites = [AUTH_PREREQUISITE]
    if not require_network_isolation:
        prerequisites.insert(0, NETWORK_PREREQUISITE)
    if vendor == VENDOR_DROID:
        if _sdk_with_process_transport(sdk) is None:
            return _refused(vendor, capabilities.platform,
                            "droid_sdk with the documented transport.ProcessTransport "
                            "interface (SDK >= 0.5.0) is not importable; the factory "
                            "sandbox launcher cannot be constructed",
                            (*prerequisites, SDK_PREREQUISITE), probe=probe)
        prerequisites.append(SDK_PREREQUISITE)
    return VendorSandboxDecision(
        vendor=vendor, platform=capabilities.platform, status="enforced",
        sandbox="bubblewrap", scope="filesystem-and-gateway" if require_network_isolation else "filesystem-only",
        filesystem_isolated=True, network_isolated=require_network_isolation,
        probe=probe, prerequisites=tuple(prerequisites),
        detail=("Fresh probe verified filesystem and direct-network isolation; "
                "egress requires the configured allowlisted gateway"
                if require_network_isolation else
                "bubblewrap enforcement verified by a fresh real probe; sandbox is "
                "filesystem-only with shared network around the actual vendor process"))


# --- Launchers ----------------------------------------------------------------

def _resolved_path(value, what, must_be_dir):
    candidate = Path(value)
    if any(os.path.islink(path) for path in (candidate, *candidate.parents)):
        raise IsolationRefused(f"{what} must not be a symbolic link: {value}")
    if not candidate.exists():
        raise IsolationRefused(f"{what} does not exist: {value}")
    resolved = candidate.resolve()
    if must_be_dir and not resolved.is_dir():
        raise IsolationRefused(f"{what} must be a directory: {value}")
    if not must_be_dir and not resolved.is_file():
        raise IsolationRefused(f"{what} must be a regular file: {value}")
    return resolved


def _validate_bind_set(workspace, *, config_file, write_binds, read_binds):
    """Reject mount sets that alias the workspace or expose private state.

    The workspace is the agent's only writable work area. Session/state write
    binds are PRIVATE state: they must live outside the workspace and outside
    each other. The config file is read-only credential-bearing state: it must
    live outside the workspace and outside every write bind (a write bind over
    its directory would let the sandboxed agent replace or re-point it).
    Read binds inside the workspace or a write bind would shadow/alias.
    """
    workspace_path = _resolved_path(workspace, "Agent workspace", True)
    config_path = _resolved_path(config_file, "Hivemind vendor config", False)
    write_paths = [_resolved_path(item, "Write bind", True) for item in write_binds]
    read_paths = [_resolved_path(item, "Read-only bind", Path(item).is_dir()) for item in read_binds]

    if config_path.is_relative_to(workspace_path):
        raise IsolationRefused("Vendor config must live outside the agent workspace")
    for write_path in write_paths:
        if write_path == workspace_path or write_path.is_relative_to(workspace_path):
            raise IsolationRefused(
                f"Private state {write_path} must live outside the agent workspace")
        if workspace_path.is_relative_to(write_path):
            raise IsolationRefused(
                f"Write bind {write_path} must not contain the agent workspace")
        if config_path.is_relative_to(write_path):
            raise IsolationRefused(
                f"Vendor config must not live inside writable private state {write_path}")
    for read_path in read_paths:
        if (read_path == workspace_path or read_path.is_relative_to(workspace_path)
                or workspace_path.is_relative_to(read_path)):
            raise IsolationRefused(
                f"Read-only bind {read_path} aliases the agent workspace")
        for write_path in write_paths:
            if (read_path == write_path or read_path.is_relative_to(write_path)
                    or write_path.is_relative_to(read_path)):
                raise IsolationRefused(
                    f"Read-only bind {read_path} aliases writable private state {write_path}")
    return workspace_path, config_path, write_paths, read_paths


def _bwrap_command(executable, workspace_path, config_path, write_paths, read_paths,
                   *, assessing, network_isolated=False):
    command = [executable, "--unshare-all", *([] if network_isolated else ["--share-net"]), "--new-session",
               "--die-with-parent", "--cap-drop", "ALL"]
    bound = set()
    # On a real launch bind only the system roots that actually exist; a
    # cross-platform assessment lists the standard set.
    for directory in ("/usr", "/bin", "/lib", "/lib64"):
        if assessing or Path(directory).exists():
            command.extend(["--ro-bind", directory, directory])
            bound.add(directory)
    command.extend(["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"])
    command.extend(["--bind", str(workspace_path), str(workspace_path),
                    "--chdir", str(workspace_path)])
    for write_path in write_paths:
        command.extend(["--bind", str(write_path), str(write_path)])
    command.extend(["--ro-bind", str(config_path), str(config_path)])
    for read_path in read_paths:
        if str(read_path) not in bound:
            command.extend(["--ro-bind", str(read_path), str(read_path)])
            bound.add(str(read_path))
    return command


def _validate_gateway_reads(read_paths, config_path, socket_path):
    """Do not import other host IPC/device channels through trusted read mounts."""
    for root in read_paths:
        if (root != config_path and config_path.is_relative_to(root)) or socket_path.is_relative_to(root):
            raise IsolationRefused("Read mounts must not expose the private worker or gateway directory")
        candidates = [root]
        if root.is_dir():
            candidates = []
            for directory, names, files in os.walk(root, followlinks=False):
                candidates.extend(Path(directory) / name for name in (*names, *files))
                if len(candidates) > 10000:
                    raise IsolationRefused("Trusted read mount exceeds safety scan; use narrower mounts")
        for path in candidates:
            if path.is_symlink() and not path.resolve(strict=True).is_relative_to(root):
                raise IsolationRefused("Trusted read mount contains an escaping symlink")
            mode = path.stat().st_mode
            if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise IsolationRefused("Trusted read mounts cannot contain host sockets or devices")


def _gateway_entry(command, argv, gateway_socket, workspace, write_paths, environment,
                   read_paths=(), config_path=None):
    """Only the gateway socket, not its parent directory, enters the namespace."""
    if sys.platform != "linux":
        raise IsolationRefused("Gateway launch requires a real Linux host")
    socket_path = Path(gateway_socket)
    if (any(path.is_symlink() for path in (socket_path, *socket_path.parents))
            or not stat.S_ISSOCK(socket_path.stat().st_mode)
            or socket_path.stat().st_uid != os.getuid()
            or socket_path.stat().st_mode & 0o077):
        raise IsolationRefused("Gateway must be a private Unix socket owned by the runtime identity")
    python = shutil.which("python3")
    if not python:
        raise IsolationRefused("Gateway namespace bridge requires Python 3")
    python = Path(python).resolve(strict=True)
    helper = Path(__file__).with_name("vendor_gateway_bridge.py").resolve(strict=True)
    for path in (helper, python, socket_path.resolve()):
        if path.is_relative_to(workspace) or any(path.is_relative_to(root) for root in write_paths):
            raise IsolationRefused("Gateway bridge and socket must be outside writable vendor areas")
    _validate_gateway_reads(read_paths, config_path, socket_path.resolve())
    command.extend(["--ro-bind", str(socket_path), "/hivemind-gateway.sock",
                    "--ro-bind", str(helper), "/hivemind-gateway-bridge.py", "--clearenv"])
    allowed = {"PATH": "/usr/bin:/bin", **(environment or {})}
    for name, value in allowed.items():
        if (not isinstance(name, str) or not name.replace("_", "").isalnum()
                or not isinstance(value, str) or "\x00" in value
                or name.startswith(("LD_", "DYLD_", "PYTHON")) or name == "NODE_OPTIONS"):
            raise IsolationRefused("Unsafe sandbox environment setting")
        command.extend(["--setenv", name, value])
    command.extend(["--", str(python), "-I", "/hivemind-gateway-bridge.py",
                    "/hivemind-gateway.sock", "19080", "--", *argv])
    return command


def _assessed_executable(platform, which):
    if platform == "linux" and sys.platform != "linux" and which is None:
        return "bwrap"  # Assessed argv for a Linux target; not launched here.
    executable = (which or shutil.which)("bwrap")
    if not executable:
        raise IsolationRefused("bubblewrap disappeared between assessment and launch; refusing")
    return executable


def sandbox_vendor_argv(argv, workspace, *, config_file, session_dir,
                        extra_read_binds=(), decision=None, platform=None, which=None,
                        require_network_isolation=False, gateway_socket=None, environment=None):
    """Wrap the oh-my-pi launch argv in bubblewrap, or refuse. Fail closed.

    The vendor argv, JSON-lines transport, and RPC protocol are byte-identical;
    only the OS boundary around the spawned process changes. ``session_dir``
    is the vendor's private session state: it is bound read-write and must be
    OUTSIDE the workspace. Anything unprovable raises :class:`IsolationRefused`
    before launch.
    """
    decision = decision or assess_vendor_sandbox(
        VENDOR_OH_MY_PI, platform=platform, which=which,
        require_network_isolation=require_network_isolation, gateway=gateway_socket is not None)
    if decision.vendor != VENDOR_OH_MY_PI:
        raise IsolationRefused(
            f"No verified sandbox exists for vendor {decision.vendor!r}: {decision.detail}")
    if not decision.allowed:
        raise IsolationRefused(f"Vendor sandbox refused: {decision.detail}")
    if require_network_isolation and (not decision.network_isolated or gateway_socket is None
                                      or not decision.probe or not decision.probe.network_isolated):
        raise IsolationRefused("Verified network isolation and a private gateway are required")
    if decision.probe is None:
        raise IsolationRefused("Fresh sandbox proof is required at launch")
    decision.probe.assert_fresh()
    executable = _assessed_executable(platform if platform is not None
                                      else decision.platform, which)
    workspace_path, config_path, write_paths, read_paths = _validate_bind_set(
        workspace, config_file=config_file, write_binds=(session_dir,),
        read_binds=extra_read_binds)
    command = _bwrap_command(executable, workspace_path, config_path, write_paths,
                             read_paths,
                             assessing=platform == "linux" and sys.platform != "linux",
                             network_isolated=require_network_isolation)
    if require_network_isolation:
        return _gateway_entry(command, argv, gateway_socket, workspace_path, write_paths, environment,
                              read_paths, config_path)
    command.extend(["--", *argv])
    return command


def factory_sandbox_transport(workspace, *, config_file, exec_path=None, exec_args=(),
                              env=None, extra_read_binds=(), extra_write_binds=(),
                              sdk=None, probe=None, platform=None, which=None,
                              require_network_isolation=False, gateway_socket=None):
    """Build a bubblewrap-wrapped ``droid_sdk.transport.ProcessTransport``.

    This uses only the documented SDK 0.5.0 launch interface: the returned
    transport has ``exec_path=<bwrap>`` and ``exec_args=[bwrap flags...,
    <droid binary>, 'exec', '--input-format', 'stream-jsonrpc',
    '--output-format', 'stream-jsonrpc', *exec_args]``; ``connect()`` spawns
    exactly that via ``asyncio.create_subprocess_exec``. The caller passes it
    to ``Runtime(transport=...)`` and connects it per the SDK contract (a
    supplied transport must be connected before use by the high-level client).

    ``exec_path`` (the real droid binary), auth mounts (``extra_read_binds``)
    and vendor state mounts (``extra_write_binds``) are explicit operator
    inputs — nothing is guessed. Refuses unless
    :func:`assess_vendor_sandbox` for droid is freshly ``enforced``.
    """
    decision = assess_vendor_sandbox(
        VENDOR_DROID, platform=platform, which=which, probe=probe, sdk=sdk,
        require_network_isolation=require_network_isolation, gateway=gateway_socket is not None)
    if not decision.allowed:
        raise IsolationRefused(f"Factory vendor sandbox refused: {decision.detail}")
    transport_module = _sdk_with_process_transport(sdk)

    if exec_path is None:
        exec_path = shutil.which("droid")
        if not exec_path:
            raise IsolationRefused(
                "No droid binary supplied or on PATH; the vendor executable path is an "
                "explicit operator input")
    binary = _resolved_path(exec_path, "droid executable", False)
    executable = _assessed_executable(platform if platform is not None
                                      else decision.platform, which)
    workspace_path, config_path, write_paths, read_paths = _validate_bind_set(
        workspace, config_file=config_file, write_binds=extra_write_binds,
        read_binds=extra_read_binds)
    command = _bwrap_command(executable, workspace_path, config_path, write_paths,
                             read_paths,
                             assessing=platform == "linux" and sys.platform != "linux",
                             network_isolated=require_network_isolation)
    vendor_argv = [str(binary), *DROID_DEFAULT_EXEC_ARGS, *exec_args]
    if require_network_isolation:
        command = _gateway_entry(command, vendor_argv, gateway_socket, workspace_path, write_paths, env,
                                 read_paths, config_path)
    else:
        command.extend(["--", *vendor_argv])
    return transport_module.ProcessTransport(
        exec_path=command[0], exec_args=command[1:], cwd=str(workspace_path), env=env)
