"""Opt-in real kernel tests; no fixture files stand in for cgroup evidence."""
import os
import selectors
import subprocess
import sys
from pathlib import Path

import pytest

from hivemind.linux_process_boundary import CgroupProcessGroup

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or not os.environ.get("HIVEMIND_TEST_CGROUP_ROOT"),
    reason="Requires explicitly delegated real Linux test cgroup")


@pytest.mark.parametrize("parent_exits", [False, True], ids=["live-parent", "exited-parent"])
def test_real_cgroup_drains_setsid_descendant(tmp_path, parent_exits):
    child_code = (
        "import os,time\n"
        "print(os.getpid(),os.getsid(0),flush=True)\n"
        "time.sleep(45)\n")
    parent_code = (
        "import subprocess,sys,time\n"
        "if sys.stdin.readline() != 'GO\\n': raise SystemExit(2)\n"
        f"child=subprocess.Popen([sys.executable,'-I','-c',{child_code!r}],"
        "start_new_session=True,stdout=subprocess.PIPE,text=True)\n"
        "print(child.stdout.readline().strip(),flush=True)\n"
        + ("raise SystemExit(0)\n" if parent_exits else "time.sleep(45)\n"))
    process = subprocess.Popen(
        [sys.executable, "-I", "-c", parent_code], cwd=tmp_path,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True)
    group = None
    try:
        group = CgroupProcessGroup(process, os.environ["HIVEMIND_TEST_CGROUP_ROOT"])
        # Admit before GO; the detached descendant inherits the actual cgroup.
        assert group._populated()
        process.stdin.write("GO\n")
        process.stdin.flush()
        with selectors.DefaultSelector() as ready:
            ready.register(process.stdout, selectors.EVENT_READ)
            assert ready.select(timeout=5), "Descendant failed to report readiness"
        pid, session = map(int, process.stdout.readline().split())
        assert pid == session and session != process.pid
        assert str(pid) in (group.path / "cgroup.procs").read_text().split()
        if parent_exits:
            assert process.wait(timeout=5) == 0
        group.kill()
        assert not group._populated()
        process.wait(timeout=5)
        state = Path(f"/proc/{pid}/stat")
        if state.exists():
            assert state.read_text().rsplit(")", 1)[1].split()[0] in ("Z", "X")
        owned = group.path
        group.close()
        group.close()
        assert not owned.exists()
    finally:
        if group is not None:
            group.close()
        else:
            process.kill()
        process.wait(timeout=5)
        for stream in (process.stdin, process.stdout, process.stderr):
            stream.close()
