"""Bounded, credential-free Linux readiness evidence on stdout."""
import json
import os
import platform
import shutil
import subprocess
from dataclasses import asdict
from pathlib import Path

from hivemind.linux_process_boundary import delegated_root
from hivemind.vendor_sandbox import probe_bwrap_enforcement, probe_bwrap_gateway_enforcement


def main():
    cgroup = Path("/sys/fs/cgroup")
    try:
        delegated_root(cgroup)
        delegation = {"ready": True}
    except (OSError, ValueError) as error:
        delegation = {"ready": False, "reason": str(error)}
    binary = shutil.which("bwrap")
    version = subprocess.run([binary, "--version"], capture_output=True, text=True,
                             timeout=5).stdout.strip() if binary else None
    report = {
        "kernel": platform.release(), "python": platform.python_version(),
        "uid": os.geteuid(), "uid_map": Path("/proc/self/uid_map").read_text().strip(),
        "bubblewrap": version,
        "filesystem_probe": asdict(probe_bwrap_enforcement(cache={})),
        "network_probe": asdict(probe_bwrap_gateway_enforcement()),
        "default_cgroup": {**delegation, "writable": os.access(cgroup, os.W_OK)},
        "scope": "synthetic_local_probes_only",
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
