import os
import sys
import time

import pytest

from hivemind.process_boundary import run_bounded


def test_bounded_output_and_timeout(tmp_path):
    env = {"PATH": os.environ.get("PATH", ""), "SystemRoot": os.environ.get("SystemRoot", "")}
    code, stdout, stderr = run_bounded([sys.executable, "-c", "print('bounded')"], tmp_path, env, 5)
    assert code == 0 and stdout.strip() == b"bounded" and not stderr
    with pytest.raises(ValueError, match="output limit"):
        run_bounded([sys.executable, "-c", "print('x' * 1000000)"], tmp_path, env, 5, max_output=64)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        run_bounded([sys.executable, "-c", "import time; time.sleep(20)"], tmp_path, env, .1)
    assert time.monotonic() - started < 5


def test_strict_isolation_refuses_unsupported_host_before_launch(tmp_path, monkeypatch):
    import hivemind.process_boundary as boundary
    monkeypatch.setattr(boundary.shutil, "which", lambda _: None)
    with pytest.raises(PermissionError, match="unavailable"):
        run_bounded([sys.executable, "-c", "raise Exception('must not execute')"],
                    tmp_path, {}, 1, require_os_isolation=True)


def test_child_process_cannot_outlive_contained_parent(tmp_path):
    sentinel = tmp_path / "escaped"
    child = f"import time; from pathlib import Path; time.sleep(1); Path({str(sentinel)!r}).write_text('escaped')"
    parent = f"import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',{child!r}]); time.sleep(20)"
    with pytest.raises(TimeoutError):
        run_bounded([sys.executable, "-c", parent], tmp_path, dict(os.environ), .15)
    time.sleep(1.1)
    assert not sentinel.exists()
