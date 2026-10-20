"""Linux cgroup-v2 descendant cleanup, not a filesystem/network sandbox.

Requires an operator-delegated cgroup subtree. Never mounts filesystems, enables
controllers, changes ownership, or falls back to process-group-only containment.
Children must wait for the existing GO handshake until admission succeeds.
"""
import os
import secrets
import stat
import sys
import time
from pathlib import Path


def _link_free(path):
    if not path.is_absolute() or ".." in path.parts:
        raise PermissionError("Cgroup paths must be absolute and normalized")
    for part in (path, *path.parents):
        if stat.S_ISLNK(part.lstat().st_mode):
            raise PermissionError("Cgroup paths cannot traverse links")
    return path


def delegated_root(value):
    if sys.platform != "linux":
        raise PermissionError("Native cgroup containment requires Linux")
    root = _link_free(Path(value))
    mounts = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, right = line.split(" - ", 1)
        fields = left.split()
        mount = Path(fields[4].replace("\\040", " ").replace("\\134", "\\"))
        if root.is_relative_to(mount):
            mounts.append((len(mount.parts), mount, fields[3], right.split()[0]))
    if not mounts or max(mounts)[3] != "cgroup2":
        raise PermissionError("A real delegated cgroup-v2 mount is required")
    _, mount, hierarchy_root, _ = max(mounts)
    info = root.stat()
    if (os.geteuid() == 0 or not root.is_dir() or info.st_uid != os.geteuid() or info.st_mode & 0o022
            or not os.access(root, os.W_OK | os.X_OK)):
        raise PermissionError("Cgroup delegation must be private and owned by this service")
    for name in ("cgroup.procs", "cgroup.events", "cgroup.kill"):
        path = _link_free(root / name)
        if not path.is_file() or not os.access(path, os.R_OK if name == "cgroup.events" else os.W_OK):
            raise PermissionError("Whole-tree cgroup kill/admission is unavailable")
    relative = str(Path(hierarchy_root) / root.relative_to(mount))
    return root, relative


class CgroupProcessGroup:
    def __init__(self, process, root):
        self.process, self.closed = process, False
        parent, hierarchy = delegated_root(root)
        self.path = parent / ("hivemind-" + secrets.token_hex(16))
        self.path.mkdir(mode=0o700)
        self.inode = self.path.stat().st_ino
        try:
            self._check()
            (self.path / "cgroup.procs").write_text(str(process.pid))
            expected = str(Path(hierarchy) / self.path.name)
            membership = Path(f"/proc/{process.pid}/cgroup").read_text().splitlines()
            if f"0::{expected}" not in membership or not self._populated():
                raise PermissionError("Child cgroup admission could not be verified")
        except BaseException:
            # The bootstrap has not received GO or read credentials.
            process.kill()
            process.wait(timeout=10)
            self.close()
            raise

    def _check(self):
        _link_free(self.path)
        if self.path.stat().st_ino != self.inode:
            raise PermissionError("Owned cgroup identity changed")
        for name in ("cgroup.procs", "cgroup.events", "cgroup.kill"):
            _link_free(self.path / name)

    def _populated(self):
        self._check()
        values = dict(line.split() for line in (self.path / "cgroup.events").read_text().splitlines())
        if values.get("populated") not in ("0", "1"):
            raise PermissionError("Cgroup drain evidence is unavailable")
        return values["populated"] == "1"

    def kill(self):
        if self.closed:
            return
        self._check()
        (self.path / "cgroup.kill").write_text("1")
        deadline = time.monotonic() + 10
        while self._populated():
            if time.monotonic() >= deadline:
                raise TimeoutError("Owned cgroup descendants did not drain")
            time.sleep(.01)

    def close(self):
        if self.closed:
            return
        self.kill()
        self._check()
        self.path.rmdir()
        self.closed = True
