"""Offline crash probe for hardened Git operations.

Builds a throwaway native Git fixture, kills real bounded Git child processes
mid-operation, runs restart reconciliation, and prints JSON evidence. No
network, no pushes, no shared state; fixtures live under the given directory
(or a fresh temp dir) and are fully agent-created.

Usage:  python scripts/git_crash_probe.py [--directory PATH] [--keep]
"""
import argparse
import json
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hivemind.git_operations import MAIN_REF, GitOperations, pid_alive  # noqa: E402
from hivemind.storage import Problem  # noqa: E402


def build_fixture(ops, root):
    repo = root / "probe.git"
    ops.git(root, "init", "--bare", "--initial-branch=main", str(repo))
    for key, value in (("receive.denyNonFastForwards", "true"), ("receive.denyDeletes", "true"),
                       ("receive.fsckObjects", "true"), ("core.logAllRefUpdates", "true"),
                       ("user.name", "Probe"), ("user.email", "probe@example.invalid")):
        ops.git(repo, "config", key, value)
    tree = ops.git(repo, "mktree", input_bytes=b"")
    commit = ops.git(repo, "commit-tree", tree, input_bytes=b"Probe fixture\n")
    ops.git(repo, "update-ref", MAIN_REF, commit, "0" * 40)
    (repo / "hivemind-ready").write_text("local-only\n", encoding="utf-8")
    (repo / "hooks" / "pre-receive").write_text(
        "#!/bin/sh\nwhile read old new ref; do\n"
        "  echo 'Protected refs require a fenced Nexus API merge' >&2; exit 1\n"
        "done\n", encoding="utf-8")
    return repo, commit


def kill_observer(delay, evidence, before=None):
    def observe(process):
        evidence["pid"] = process.pid
        def killer():
            time.sleep(delay)
            if process.poll() is None:
                if before:
                    before()
                evidence["was_running"] = True
                process.kill()
        threading.Thread(target=killer, daemon=True).start()
    return observe


def probe(root):
    evidence = {"root": str(root), "scenarios": {}}
    ops = GitOperations(root / "git-ops")
    repo, commit = build_fixture(ops, root)

    # Scenario 1: kill a blocking Git child mid-operation.
    kill = {}
    ops.spawn_observer = kill_observer(0.3, kill)
    started = time.monotonic()
    try:
        ops.spawn(repo, ["cat-file", "--batch-check"], hold_stdin=True, timeout=30)
        kill["raised"] = None
    except Problem as error:
        kill["raised"] = error.code
    ops.spawn_observer = None
    kill["duration_seconds"] = round(time.monotonic() - started, 3)
    time.sleep(0.2)
    kill["pid_alive_after"] = pid_alive(kill.get("pid") or -1)
    kill["main_unchanged"] = ops.git(repo, "rev-parse", "--verify", MAIN_REF) == commit
    evidence["scenarios"]["child_kill"] = kill

    # Scenario 2: stale lock left by the killed child's operation scope.
    lock = repo / "refs" / "heads" / "main.lock"
    lock_kill = {}
    ops.spawn_observer = kill_observer(0.2, lock_kill,
                                       before=lambda: lock.write_text("", encoding="utf-8"))
    try:
        ops.spawn(repo, ["cat-file", "--batch-check"], hold_stdin=True, timeout=30)
    except Problem:
        pass
    ops.spawn_observer = None
    locked = ops.reconcile_repository(repo)
    cleaned = ops.reconcile_repository(repo, allow_stale_lock_removal=True)
    evidence["scenarios"]["stale_lock"] = {
        "was_running": lock_kill.get("was_running"),
        "held_without_allowance": lock.exists() is False and False or any(
            item["path"] == str(lock) and not item["removed"] for item in locked["locks"]),
        "removed_with_allowance": cleaned["removed_locks"] == [str(lock)],
        "removed": cleaned["removed_locks"]}

    # Scenario 3: restart reconciliation + lease rollover after kills.
    restarted = GitOperations(root / "git-ops")
    first = restarted.rollover_fence(repo, "probe_lease", 1, expected_oid="0" * 40)
    report = restarted.reconcile_repository(repo)
    evidence["scenarios"]["restart"] = {
        "fsck_ok": report["fsck_ok"], "head": report["head"] == commit,
        "fence_oid": first["oid"],
        "preflight_ready": restarted.upgrade_preflight(repo)["ready"]}
    evidence["ok"] = all((
        kill.get("was_running"), kill["raised"] == "git_command_failed",
        not kill["pid_alive_after"], kill["main_unchanged"],
        evidence["scenarios"]["stale_lock"]["removed_with_allowance"],
        report["fsck_ok"]))
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", help="Fixture directory (default: fresh temp dir)")
    parser.add_argument("--keep", action="store_true", help="Keep fixture directory")
    args = parser.parse_args()
    if args.directory:
        root = Path(args.directory).resolve()
        root.mkdir(parents=True, exist_ok=True)
        cleanup = False
    else:
        root = Path(tempfile.mkdtemp(prefix="hivemind-git-crash-probe-"))
        cleanup = not args.keep
    try:
        evidence = probe(root)
    finally:
        if cleanup:
            shutil.rmtree(root, ignore_errors=True)
    print(json.dumps(evidence, indent=2, default=str))
    return 0 if evidence["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
