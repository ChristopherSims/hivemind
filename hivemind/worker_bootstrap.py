"""Owned worker child: containment handshake before loading private credentials."""
import argparse
import json
import os
import signal
import sys
import threading
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--runtime", choices=("factory", "oh_my_pi"), required=True)
    args = parser.parse_args()
    if sys.stdin.buffer.readline(16) != b"GO\n":
        return 2
    if os.name == "posix":
        # Linux child dies if the service disappears; never signal an unrelated PID.
        if sys.platform != "linux":
            return 2
        import ctypes
        parent = os.getppid()
        if parent == 1 or ctypes.CDLL(None).prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
            return 2
        if os.getppid() != parent:
            return 2
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    seconds = config["max_runtime_seconds"]
    if type(seconds) is not int or not 1 <= seconds <= 86400:
        return 2

    def expire():
        if os.name == "posix":
            os.killpg(os.getpgrp(), signal.SIGKILL)
        else:
            os._exit(124)  # Service-owned Windows Job closes the entire child tree.

    watchdog = threading.Timer(seconds, expire)
    watchdog.daemon = True
    watchdog.start()
    from .cli import main as worker_main
    sys.argv = [sys.argv[0], "worker", "--config", args.config, "--runtime", args.runtime,
                 "--journal", str(Path(args.config).with_name("journal.sqlite3")),
                 "--workspace", config["runtime_options"]["workspace"],
                 "--iterations", "1", "--enable"]
    try:
        result = worker_main()
        return result if type(result) is int else 0
    finally:
        watchdog.cancel()


if __name__ == "__main__":
    raise SystemExit(main())
