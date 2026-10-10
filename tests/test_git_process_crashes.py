"""Actual bounded child-process kills during isolated native Git operations.

Every test terminates a real Git child process that this test suite spawned
(agent-created fixtures only), then asserts repository integrity, journal
evidence, and restart reconciliation. No network, no shared state.
"""
import shutil
import threading
import time

import pytest

from hivemind.git_operations import FENCE_REF, MAIN_REF, GitOperations, pid_alive
from hivemind.recovery import Journal
from hivemind.storage import Problem
from test_git_operations import make_repository

pytestmark = pytest.mark.skipif(not shutil.which("git"), reason="standard Git is required")


def kill_after(delay, *, before=None):
    """spawn_observer seam: kill the real Git child after ``delay`` seconds."""
    state = {"was_running": None, "pid": None}
    def observe(process):
        state["pid"] = process.pid
        def killer():
            time.sleep(delay)
            if process.poll() is None:
                if before:
                    before()
                state["was_running"] = True
                process.kill()
            else:
                state["was_running"] = False
        threading.Thread(target=killer, daemon=True).start()
    observe.state = state
    return observe


def operation_record(ops, operation_id):
    with Journal(ops.journal_path) as journal:
        return journal.operation(operation_id)


def test_killed_blocking_git_child_leaves_repository_intact_and_journaled(tmp_path):
    ops = GitOperations(tmp_path / "git-ops")
    repo, commit = make_repository(ops, tmp_path)
    observer = kill_after(0.3)
    ops.spawn_observer = observer
    started = time.monotonic()
    with pytest.raises(Problem) as error:
        ops.spawn(repo, ["cat-file", "--batch-check"], hold_stdin=True, timeout=30)
    ops.spawn_observer = None  # one-shot seam: later operations are not observed
    assert error.value.code == "git_command_failed"
    evidence = {"was_running": observer.state["was_running"], "pid": observer.state["pid"],
                "duration_seconds": round(time.monotonic() - started, 3)}
    assert evidence["was_running"], "the Git child must be mid-flight when killed"
    deadline = time.monotonic() + 5
    while pid_alive(evidence["pid"]) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not pid_alive(evidence["pid"]), "killed Git child must be gone"
    # Native repository integrity is untouched by the kill.
    assert ops.git(repo, "rev-parse", "--verify", MAIN_REF) == commit
    assert ops.spawn(repo, ["fsck", "--no-progress"], allowed=(0, 1))["returncode"] == 0
    # Journal evidence: the interrupted attempt is *unknown*, never silently retried.
    with Journal(ops.journal_path) as journal:
        unknowns = [record for record in journal.operations("unknown")
                    if "cat-file" in record["path"]]
    assert unknowns and unknowns[0]["attempts"] == 1
    # A fresh owner over the same journal (restart) reconciles cleanly.
    restarted = GitOperations(tmp_path / "git-ops")
    report = restarted.reconcile_repository(repo)
    assert report["fsck_ok"] and report["head"] == commit and not report["locks"]


def test_killed_update_ref_stdin_never_moves_protected_ref(tmp_path):
    ops = GitOperations(tmp_path / "git-ops")
    repo, commit = make_repository(ops, tmp_path)
    observer = kill_after(0.3)
    ops.spawn_observer = observer
    with pytest.raises(Problem):
        # The child blocks waiting for transaction input that never arrives.
        ops.spawn(repo, ["update-ref", "--stdin"], hold_stdin=True, timeout=30)
    ops.spawn_observer = None
    assert observer.state["was_running"]
    assert ops.git(repo, "rev-parse", "--verify", MAIN_REF) == commit
    assert ops.spawn(repo, ["fsck", "--no-progress"], allowed=(0, 1))["returncode"] == 0


def test_killed_child_stale_lock_removed_only_with_journal_proof_and_allowance(tmp_path):
    ops = GitOperations(tmp_path / "git-ops")
    repo, commit = make_repository(ops, tmp_path)
    lock = repo / "refs" / "heads" / "main.lock"
    def plant_lock():
        lock.write_text("", encoding="utf-8")
    observer = kill_after(0.2, before=plant_lock)
    ops.spawn_observer = observer
    with pytest.raises(Problem):
        ops.spawn(repo, ["cat-file", "--batch-check"], hold_stdin=True, timeout=30)
    ops.spawn_observer = None
    assert observer.state["was_running"]
    assert lock.exists(), "the killed child left the planted lock behind"
    # Without explicit allowance the proven-stale lock is reported but kept.
    report = ops.reconcile_repository(repo)
    entry = next(item for item in report["locks"] if item["path"] == str(lock))
    assert entry["classification"] == "stale" and not entry["removed"] and lock.exists()
    # With allowance, only the journal-proven lock owned by the dead PID is removed.
    allowed = ops.reconcile_repository(repo, allow_stale_lock_removal=True)
    assert allowed["removed_locks"] == [str(lock)] and not lock.exists()
    assert ops.git(repo, "rev-parse", "--verify", MAIN_REF) == commit
    assert ops.spawn(repo, ["fsck", "--no-progress"], allowed=(0, 1))["returncode"] == 0
    # Foreign locks still present at reconcile time are never touched.
    foreign = repo / "refs" / "hivemind" / "foreign.lock"
    foreign.parent.mkdir(parents=True, exist_ok=True)
    foreign.write_text("", encoding="utf-8")
    followup = ops.reconcile_repository(repo, allow_stale_lock_removal=True)
    assert foreign.exists()
    assert all(item["path"] != str(foreign) or not item["removed"] for item in followup["locks"])


def test_lease_rollover_and_restart_reconciliation_after_child_kill(tmp_path):
    ops = GitOperations(tmp_path / "git-ops")
    repo, commit = make_repository(ops, tmp_path)
    first = ops.rollover_fence(repo, "lease_a", 1, expected_oid="0" * 40)
    # Kill a blocking Git child, then prove rollover and reconciliation still work.
    observer = kill_after(0.2)
    ops.spawn_observer = observer
    with pytest.raises(Problem):
        ops.spawn(repo, ["cat-file", "--batch-check"], hold_stdin=True, timeout=30)
    ops.spawn_observer = None
    assert observer.state["was_running"]
    with pytest.raises(Problem) as error:
        ops.rollover_fence(repo, "lease_b", 2, expected_oid="0" * 40)
    assert error.value.code == "stale_coordinator_epoch"
    second = ops.rollover_fence(repo, "lease_b", 2, expected_oid=first["oid"])
    assert second["previous_oid"] == first["oid"]
    restarted = GitOperations(tmp_path / "git-ops")
    report = restarted.reconcile_repository(repo)
    assert report["fsck_ok"] and report["refs"][FENCE_REF] == second["oid"]
    assert report["refs"][MAIN_REF] == commit


def test_repeated_kills_never_corrupt_object_database(tmp_path):
    ops = GitOperations(tmp_path / "git-ops")
    repo, commit = make_repository(ops, tmp_path)
    for _round in range(3):
        observer = kill_after(0.15)
        ops.spawn_observer = observer
        with pytest.raises(Problem):
            ops.spawn(repo, ["cat-file", "--batch-check"], hold_stdin=True, timeout=30)
        ops.spawn_observer = None
        assert observer.state["was_running"]
    report = GitOperations(tmp_path / "git-ops").reconcile_repository(
        repo, allow_stale_lock_removal=True)
    assert report["fsck_ok"], report.get("fsck_output")
    assert report["refs"][MAIN_REF] == commit
