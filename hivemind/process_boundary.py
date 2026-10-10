"""Bounded trusted child processes. Process cleanup is not a filesystem sandbox."""
import ctypes
import os
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path


class ProcessGroup:
    def __init__(self, process):
        self.process, self.job = process, None
        if os.name == "nt":
            from ctypes import wintypes
            class Basic(ctypes.Structure):
                _fields_ = [("process_time", ctypes.c_int64), ("job_time", ctypes.c_int64),
                    ("flags", wintypes.DWORD), ("minimum", ctypes.c_size_t), ("maximum", ctypes.c_size_t),
                    ("active", wintypes.DWORD), ("affinity", ctypes.c_size_t),
                    ("priority", wintypes.DWORD), ("scheduling", wintypes.DWORD)]
            class Extended(ctypes.Structure):
                _fields_ = [("basic", Basic), ("io", ctypes.c_uint64 * 6),
                    ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
                    ("peak_process", ctypes.c_size_t), ("peak_job", ctypes.c_size_t)]
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.CreateJobObjectW.restype = wintypes.HANDLE
            kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
            kernel.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
            kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
            kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
            kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            job = kernel.CreateJobObjectW(None, None)
            limits = Extended()
            limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            if not job or not kernel.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
                if job:
                    kernel.CloseHandle(job)
                process.kill()
                process.wait()
                raise OSError("Cannot establish child-process containment")
            if not kernel.AssignProcessToJobObject(job, wintypes.HANDLE(int(process._handle))):
                kernel.CloseHandle(job)
                process.kill()
                process.wait()
                raise OSError("Cannot assign child-process containment")
            self.kernel, self.job = kernel, job

    def kill(self):
        if self.job:
            self.kernel.TerminateJobObject(self.job, 1)
        elif os.name != "nt":
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        elif self.process.poll() is None:
            self.process.kill()

    def close(self):
        self.kill()
        if self.job:
            self.kernel.CloseHandle(self.job)
            self.job = None


def isolated_argv(argv, workspace, require_os_isolation=False):
    """Linux bubblewrap provides read-only workspace and no network; otherwise fail closed."""
    if not require_os_isolation:
        return list(argv)
    executable = shutil.which("bwrap") if os.name == "posix" else None
    if not executable:
        raise PermissionError("Required OS filesystem/network isolation is unavailable")
    root = Path(workspace).resolve(strict=True)
    command = [executable, "--unshare-all", "--new-session", "--die-with-parent", "--cap-drop", "ALL"]
    for directory in ("/usr", "/bin", "/lib", "/lib64"):
        if Path(directory).exists():
            command.extend(["--ro-bind", directory, directory])
    command.extend(["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
                    "--ro-bind", str(root), str(root), "--chdir", str(root), "--", *argv])
    return command


def run_bounded(argv, cwd, env, timeout, *, input_bytes=None, max_output=65536, require_os_isolation=False):
    if input_bytes is not None and len(input_bytes) > 2 * 1024 * 1024:
        raise ValueError("Process input exceeds the configured boundary")
    command = isolated_argv(argv, cwd, require_os_isolation)
    process = subprocess.Popen(command, cwd=cwd, env=env, shell=False, stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=os.name != "nt")
    group = ProcessGroup(process)
    output = [bytearray(), bytearray()]
    lock, exceeded = threading.Lock(), threading.Event()
    def drain(stream, index):
        try:
            while block := stream.read(4096):
                with lock:
                    remaining = max(0, max_output - sum(map(len, output)))
                    output[index].extend(block[:remaining])
                    if len(block) > remaining:
                        exceeded.set()
                        group.kill()
        except (OSError, ValueError):
            pass
        finally:
            stream.close()
    def feed():
        try:
            if input_bytes:
                process.stdin.write(input_bytes)
                process.stdin.flush()
        except (OSError, ValueError):
            pass
        finally:
            process.stdin.close()
    threads = [threading.Thread(target=drain, args=(process.stdout, 0), daemon=True),
               threading.Thread(target=drain, args=(process.stderr, 1), daemon=True),
               threading.Thread(target=feed, daemon=True)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + timeout
    timed_out = False
    try:
        process.wait(timeout=max(0.01, deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        timed_out = True
    finally:
        group.close()  # Also terminate descendants when their parent has already exited.
        process.wait(timeout=10)
        for thread in threads:
            thread.join(10)
    if timed_out:
        raise TimeoutError("Contained process exceeded its deadline")
    if exceeded.is_set():
        raise ValueError("Contained process exceeded its output limit")
    if any(thread.is_alive() for thread in threads):
        raise RuntimeError("Contained process streams did not settle")
    return process.returncode, bytes(output[0]), bytes(output[1])
