"""Explicit WSL2 deployment boundary. No installation or paid calls in preflight."""
import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath


# Run only a fixed stdlib bootstrap through the operator's Linux interpreter.
# -I refuses workspace PYTHONPATH/current-directory package shadowing.
GUEST_PROBE = """
import importlib.util,json,os,pathlib,shutil,sys
kernel=pathlib.Path('/proc/sys/kernel/osrelease').read_text().lower()
out={'wsl2':sys.platform=='linux' and 'wsl2' in kernel,
     'unprivileged':os.geteuid()!=0,
     'hivemind_installed':importlib.util.find_spec('hivemind') is not None,
     'bubblewrap_present':shutil.which('bwrap') is not None,
     'cgroup_ready':False,'network_namespace_verified':False,
     'filesystem_verified':False,'vendors':{
       'factory':shutil.which('droid') is not None and importlib.util.find_spec('droid_sdk') is not None,
       'oh_my_pi':shutil.which('omp') is not None and shutil.which('bun') is not None}}
if out['hivemind_installed'] and out['unprivileged']:
    try:
        from hivemind.linux_process_boundary import delegated_root
        delegated_root(sys.argv[1])
        out['cgroup_ready']=True
    except Exception: pass
    if out['bubblewrap_present']:
        try:
            from hivemind.vendor_sandbox import probe_bwrap_gateway_enforcement
            p=probe_bwrap_gateway_enforcement()
            out['filesystem_verified']=p.status=='passed'
            out['network_namespace_verified']=p.status=='passed' and p.network_isolated
        except Exception: pass
print(json.dumps(out))
"""


def linux_path(value):
    if (not isinstance(value, str) or not value.startswith("/") or "\\" in value
            or any(ord(char) < 32 for char in value) or ".." in PurePosixPath(value).parts
            or value == "/mnt" or value.startswith("/mnt/")):
        raise ValueError("Use explicit native Linux paths, not Windows/DrvFS mounts")
    return value


@dataclass(frozen=True)
class WSLPolicy:
    distribution: str
    cgroup_root: str
    python: str = "/usr/bin/python3"

    def __post_init__(self):
        if not isinstance(self.distribution, str) or not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", self.distribution):
            raise ValueError("Select an explicit WSL distribution")
        linux_path(self.python)
        linux_path(self.cgroup_root)


def _fingerprint(policy):
    return hashlib.sha256(json.dumps(asdict(policy), sort_keys=True).encode()).hexdigest()


def preflight(policy, *, runner=subprocess.run, executable=None, clock=time.monotonic):
    executable = executable or shutil.which("wsl.exe")
    if not executable:
        return {"ready": False, "reason": "wsl_unavailable", "native_windows_sandbox": False}
    try:
        result = runner([executable, "--distribution", policy.distribution, "--exec",
                         policy.python, "-I", "-c", GUEST_PROBE, policy.cgroup_root],
                        stdin=subprocess.DEVNULL, capture_output=True, timeout=30, check=False)
        if result.returncode or len(result.stdout) > 65536:
            raise ValueError("WSL probe failed")
        guest = json.loads(result.stdout.decode("utf-8"))
        if not isinstance(guest, dict) or not isinstance(guest.get("vendors"), dict):
            raise ValueError("Malformed WSL evidence")
        required = ("wsl2", "unprivileged", "hivemind_installed", "bubblewrap_present",
                    "cgroup_ready", "filesystem_verified", "network_namespace_verified")
        ready = all(guest.get(key) is True for key in required)
        return {"ready": ready, "guest": guest, "missing": [key for key in required if guest.get(key) is not True],
                "policy_sha256": _fingerprint(policy), "checked_at": clock(),
                "native_windows_sandbox": False, "boundary": "wsl2-linux-bubblewrap-and-explicit-gateway",
                "live_vendor_validated": False}
    except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
        return {"ready": False, "reason": "wsl_probe_unconfirmed", "native_windows_sandbox": False}


def launch_argv(policy, config, runtime, *, assessment, executable=None, clock=time.monotonic):
    """Build only after fresh preflight. No shell, implicit distro or path translation."""
    linux_path(config)
    if runtime not in ("factory", "oh_my_pi") or assessment.get("ready") is not True:
        raise PermissionError("WSL vendor deployment is not ready")
    checked = assessment.get("checked_at")
    if (type(checked) not in (int, float) or not 0 <= clock() - checked <= 60
            or assessment.get("policy_sha256") != _fingerprint(policy)):
        raise PermissionError("WSL assessment is stale or bound to another deployment")
    if assessment.get("guest", {}).get("vendors", {}).get(runtime) is not True:
        raise PermissionError("Selected Linux vendor is not installed")
    executable = executable or shutil.which("wsl.exe")
    if not executable:
        raise PermissionError("WSL is unavailable")
    return [executable, "--distribution", policy.distribution, "--exec", policy.python, "-I", "-m",
            "hivemind.wsl_runtime", "--guest-run", "--config", config, "--runtime", runtime,
            "--cgroup-root", policy.cgroup_root]


def guest_run(config_path, runtime, cgroup_root):
    """Bound a Linux worker under the same native containment and strict gateway gates."""
    from .linux_process_boundary import delegated_root
    from .vendor_recovery import trusted_private_file
    from .worker_provisioning import WorkerProvisioner
    if sys.platform != "linux" or "wsl2" not in Path("/proc/sys/kernel/osrelease").read_text().lower():
        raise PermissionError("WSL2 guest boundary required")
    delegated_root(cgroup_root)
    config_path = Path(linux_path(config_path))
    trusted_private_file(config_path, base_dir=config_path.parent)
    info = config_path.stat()
    import os
    if info.st_uid != os.geteuid() or info.st_mode & 0o077 or os.geteuid() == 0:
        raise PermissionError("Worker configuration must be private and service-owned")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    options = config["runtime_options"]
    workspace = linux_path(options["workspace"])
    linux_path(options["state_file"])
    seconds = config["max_runtime_seconds"]
    if type(seconds) is not int or not 1 <= seconds <= 3600:
        raise ValueError("WSL launch deadline must be 1..3600 seconds")
    if (options.get("require_os_isolation") is not True
            or not options.get("vendor_network_gateway", {}).get("model_origins")
            or options.get("auto_mcp_registration", False)):
        raise PermissionError("Strict gateway isolation is mandatory; sandbox MCP enrollment remains gated")
    process, group = WorkerProvisioner._spawn(config_path, workspace, runtime, linux_cgroup_root=cgroup_root)
    try:
        process.stdin.write(b"GO\n")
        process.stdin.flush()
        process.stdin.close()
        return process.wait(timeout=seconds + 10)
    finally:
        group.close()
        process.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--distribution")
    parser.add_argument("--python", default="/usr/bin/python3")
    parser.add_argument("--cgroup-root", required=True)
    parser.add_argument("--config")
    parser.add_argument("--runtime", choices=("factory", "oh_my_pi"))
    parser.add_argument("--guest-run", action="store_true")
    parser.add_argument("--launch", action="store_true", help="Explicit real worker launch, may incur model charges")
    args = parser.parse_args()
    if args.guest_run:
        if not args.config or not args.runtime:
            parser.error("Guest launch requires config/runtime")
        return guest_run(args.config, args.runtime, args.cgroup_root)
    policy = WSLPolicy(args.distribution, args.cgroup_root, args.python)
    assessment = preflight(policy)
    if not args.launch:
        print(json.dumps(assessment, sort_keys=True))
        return 0 if assessment["ready"] else 1
    if not args.config or not args.runtime:
        parser.error("Launch requires config/runtime")
    argv = launch_argv(policy, args.config, args.runtime, assessment=assessment)
    # Linux guardian enforces the private config's <=3600s deadline and drains
    # its cgroup. Host cancellation alone is NOT evidence of guest termination.
    result = subprocess.run(argv, stdin=subprocess.DEVNULL, timeout=3660, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
