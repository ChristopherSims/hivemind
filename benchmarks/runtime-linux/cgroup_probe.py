"""Privileged, container-private test setup. Never run on a host."""
import json
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path


def main():
    root = Path("/sys/fs/cgroup")
    # Refuse before any mutation unless the namespace contains only this PID.
    if (os.geteuid() != 0 or os.getpid() != 1 or not Path("/.dockerenv").is_file()
            or Path("/proc/self/cgroup").read_text().strip() != "0::/"
            or set((root / "cgroup.procs").read_text().split()) != {"1"}
            or any(path.is_dir() for path in root.iterdir())):
        raise RuntimeError("Expected a fresh, private Docker cgroup namespace")
    delegated = root / ("hivemind-test-" + secrets.token_hex(8))
    delegated.mkdir(mode=0o700)
    try:
        for path in (delegated, delegated / "cgroup.procs", delegated / "cgroup.kill",
                     root / "cgroup.procs"):
            os.chown(path, 1000, 1000)
        env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1",
               "HIVEMIND_TEST_CGROUP_ROOT": str(delegated)}
        result = subprocess.run(
            [sys.executable, "-I", "-m", "pytest", "-p", "no:cacheprovider",
             "tests/test_linux_cgroup_integration.py", "-q", "-rs"],
            env=env, user=1000, group=1000, extra_groups=[], timeout=60)
        print(json.dumps({"test_scope": "private Docker cgroup namespace",
                          "service_uid": 1000, "pytest_exit_code": result.returncode}), flush=True)
        return result.returncode
    finally:
        # This verified container-only subtree is owned by this invocation.
        (delegated / "cgroup.kill").write_text("1")
        deadline = time.monotonic() + 10
        while "populated 0" not in (delegated / "cgroup.events").read_text().splitlines():
            if time.monotonic() >= deadline:
                raise TimeoutError("Container-only test cgroup did not drain")
            time.sleep(.01)
        for path in delegated.iterdir():
            if path.is_dir():
                path.rmdir()
        delegated.rmdir()


if __name__ == "__main__":
    raise SystemExit(main())
