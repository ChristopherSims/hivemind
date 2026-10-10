"""Operational hardening tests: real Git, no process kills (see test_git_process_crashes)."""
import json
import shutil
import time

import pytest

from hivemind.git_operations import (FENCE_REF, MAIN_REF, GitOperations,
                                     pid_alive)
from hivemind.recovery import Journal
from hivemind.storage import Problem, canonical

pytestmark = pytest.mark.skipif(not shutil.which("git"), reason="standard Git is required")


@pytest.fixture
def ops(tmp_path):
    return GitOperations(tmp_path / "git-ops")


def make_repository(ops, tmp_path, name="repo.git"):
    repo = tmp_path / name
    ops.git(tmp_path, "init", "--bare", "--initial-branch=main", str(repo))
    for key, value in (("receive.denyNonFastForwards", "true"), ("receive.denyDeletes", "true"),
                       ("receive.fsckObjects", "true"), ("core.logAllRefUpdates", "true"),
                       ("user.name", "Fixture"), ("user.email", "fixture@example.invalid")):
        ops.git(repo, "config", key, value)
    tree = ops.git(repo, "mktree", input_bytes=b"")
    commit = ops.git(repo, "commit-tree", tree, input_bytes=b"Initialize\n")
    ops.git(repo, "update-ref", MAIN_REF, commit, "0" * 40)
    (repo / "hivemind-ready").write_text("local-only\n", encoding="utf-8")
    hook = repo / "hooks" / "pre-receive"
    hook.write_text("#!/bin/sh\nwhile read old new ref; do\n"
                    "  echo 'Protected refs require a fenced Nexus API merge' >&2; exit 1\n"
                    "done\n", encoding="utf-8")
    return repo, commit


def journal_state(ops, operation_id):
    with Journal(ops.journal_path) as journal:
        record = journal.operation(operation_id)
    return record["state"] if record else None


def test_spawn_journals_definite_failure_and_success(ops, tmp_path):
    repo, commit = make_repository(ops, tmp_path)
    ok = ops.spawn(repo, ["rev-parse", "--verify", MAIN_REF])
    assert ok["stdout"] == commit and ok["returncode"] == 0
    assert journal_state(ops, ok["operation_id"]) == "completed"
    with pytest.raises(Problem) as error:
        ops.spawn(repo, ["rev-parse", "--verify", "refs/heads/missing"])
    assert error.value.code == "git_command_failed"
    report = ops.reconcile_repository(repo)
    assert report["fsck_ok"] and report["head"] == commit
    assert report["refs"][MAIN_REF] == commit


def test_backup_restore_validates_and_never_overwrites(ops, tmp_path):
    repo, commit = make_repository(ops, tmp_path)
    ops.fence_oid(repo, "lease_fixture", 1)
    ops.git(repo, "update-ref", FENCE_REF, ops.fence_oid(repo, "lease_fixture", 1))
    bundle = tmp_path / "backup" / "repo.bundle"
    report = ops.backup_repository(repo, bundle)
    assert report["ok"] and report["fsck_ok"] and not report["missing_refs"]
    assert report["source_refs"][MAIN_REF] == commit
    # The non-commit fence blob is carried by the hashed sidecar manifest,
    # not silently dropped nor bundled.
    assert report["manifest_refs"][FENCE_REF]
    assert FENCE_REF not in report["restored_refs"]
    with pytest.raises(Problem) as error:
        ops.backup_repository(repo, bundle)
    assert error.value.code == "git_backup_destination_exists"
    restored = tmp_path / "restored.git"
    result = ops.restore_backup(bundle, restored)
    assert result["fsck_ok"] and result["refs"][MAIN_REF] == commit
    assert result["ready"] and result["receive_guard"] and not result["fence_present"]
    assert FENCE_REF not in result["refs"]  # re-established by lease acquisition
    assert ops.git(restored, "config", "--get", "receive.denyDeletes") == "true"
    assert ops.git(restored, "symbolic-ref", "HEAD") == MAIN_REF
    assert (restored / "hivemind-ready").exists()
    # The published receive guard rejects a direct push to protected main.
    attacker = tmp_path / "attacker"
    ops.git(tmp_path, "clone", str(restored), str(attacker))
    ops.git(attacker, "config", "user.name", "Attacker")
    ops.git(attacker, "config", "user.email", "attacker@example.invalid")
    (attacker / "evil.txt").write_text("direct write\n", encoding="utf-8")
    ops.git(attacker, "add", "evil.txt")
    ops.git(attacker, "commit", "-m", "Direct protected write")
    push = ops.spawn(attacker, ["push", str(restored), "HEAD:refs/heads/main"], allowed=(0, 1))
    assert push["returncode"] == 1
    assert ops.git(restored, "rev-parse", "--verify", MAIN_REF) == commit
    with pytest.raises(Problem) as error:
        ops.restore_backup(bundle, restored)
    assert error.value.code == "git_restore_destination_exists"
    assert ops.git(repo, "rev-parse", "--verify", MAIN_REF) == commit  # source untouched


def test_corrupted_backup_fails_validation(ops, tmp_path):
    repo, _ = make_repository(ops, tmp_path)
    bundle = tmp_path / "corrupt.bundle"
    bundle.write_bytes(b"not a git bundle")
    report = ops.validate_backup(repo, bundle)
    assert not report["ok"] and not report["bundle_verify_ok"]
    with pytest.raises(Problem):
        ops.restore_backup(bundle, tmp_path / "restored.git")
    assert not (tmp_path / "restored.git").exists(), "a failed restore must not remain"


def test_fence_rollover_is_compare_and_swap(ops, tmp_path):
    repo, _ = make_repository(ops, tmp_path)
    assert GitOperations.fence_value("lease_a", 1) == canonical(
        {"lease_id": "lease_a", "epoch": 1}).encode()
    first = ops.rollover_fence(repo, "lease_a", 1, expected_oid="0" * 40)
    blob = ops.git(repo, "cat-file", "-p", first["oid"])
    assert json.loads(blob) == {"lease_id": "lease_a", "epoch": 1}
    with pytest.raises(Problem) as error:
        ops.rollover_fence(repo, "lease_b", 2, expected_oid="0" * 40)
    assert error.value.code == "stale_coordinator_epoch"
    second = ops.rollover_fence(repo, "lease_b", 2, expected_oid=first["oid"])
    assert second["previous_oid"] == first["oid"]
    assert json.loads(ops.git(repo, "cat-file", "-p", FENCE_REF)) == \
        {"lease_id": "lease_b", "epoch": 2}


def test_retention_plans_only_proven_refs_and_requires_confirmation(ops, tmp_path):
    repo, commit = make_repository(ops, tmp_path)
    fence = ops.fence_oid(repo, "lease_a", 1)
    ops.git(repo, "update-ref", FENCE_REF, fence)
    tree = ops.git(repo, "rev-parse", commit + "^{tree}")
    merged = ops.git(repo, "commit-tree", tree, "-p", commit,
                     input_bytes=b"Merged candidate\n")
    ops.git(repo, "update-ref", MAIN_REF, merged, commit)
    ops.git(repo, "update-ref", "refs/hivemind/candidates/mr_merged", merged)
    orphan = ops.git(repo, "hash-object", "-w", "--stdin", input_bytes=b"orphan")
    unmerged_tree = ops.git(repo, "mktree",
                            input_bytes=f"100644 blob {orphan}\twork.txt\n".encode())
    unmerged = ops.git(repo, "commit-tree", unmerged_tree, input_bytes=b"Unpublished work\n")
    ops.git(repo, "update-ref", "refs/hivemind/candidates/mr_unmerged", unmerged)
    plan = ops.plan_retention(repo, prefixes=("refs/hivemind/",))
    planned = {item["ref"] for item in plan["candidates"]}
    excluded = {item["ref"]: item["reason"] for item in plan["excluded"]}
    assert planned == {"refs/hivemind/candidates/mr_merged"}
    assert excluded[FENCE_REF] == "protected"
    assert excluded["refs/hivemind/candidates/mr_unmerged"] == "not_merged"
    # Nothing was deleted by planning.
    assert ops.git(repo, "rev-parse", "--verify", "refs/hivemind/candidates/mr_merged") == merged
    with pytest.raises(Problem) as error:
        ops.execute_retention(repo, plan, confirmation="wrong")
    assert error.value.code == "git_retention_confirmation_required"
    result = ops.execute_retention(repo, plan, confirmation=plan["digest"])
    assert result["removed"] == ["refs/hivemind/candidates/mr_merged"]
    assert ops.spawn(repo, ["rev-parse", "--verify", "refs/hivemind/candidates/mr_merged"],
                     allowed=(0, 1, 128))["returncode"] != 0
    # Unproven refs, the fence and the object database survive.
    assert ops.git(repo, "rev-parse", "--verify", "refs/hivemind/candidates/mr_unmerged") == unmerged
    assert ops.git(repo, "rev-parse", "--verify", FENCE_REF) == fence
    assert ops.git(repo, "cat-file", "-t", unmerged) == "commit"
    # Re-execution re-proves: the already-removed ref is skipped, not an error.
    again = ops.execute_retention(repo, plan, confirmation=plan["digest"])
    assert again["removed"] == [] and again["skipped"]


def test_retention_scope_outside_hivemind_is_refused(ops, tmp_path):
    repo, _ = make_repository(ops, tmp_path)
    with pytest.raises(Problem) as error:
        ops.plan_retention(repo, prefixes=("refs/heads/",))
    assert error.value.code == "git_retention_scope_not_provable"
    with pytest.raises(Problem):
        ops.plan_retention(repo, prefixes=(FENCE_REF,))


def test_upgrade_preflight_is_read_only_and_reports_gaps(ops, tmp_path):
    repo, commit = make_repository(ops, tmp_path)
    before = ops._ref_map(repo)
    report = ops.upgrade_preflight(repo)
    assert report["ready"], report["findings"]
    assert ops._ref_map(repo) == before
    (repo / "hivemind-ready").unlink()
    degraded = ops.upgrade_preflight(repo)
    assert not degraded["ready"]
    assert any(item["check"] == "ready_marker" and not item["ok"]
               for item in degraded["findings"])
    missing = ops.upgrade_preflight(tmp_path / "absent.git")
    assert not missing["ready"]


def test_reconcile_never_removes_unknown_locks(ops, tmp_path):
    repo, _ = make_repository(ops, tmp_path)
    foreign = repo / "refs" / "heads" / "main.lock"
    foreign.write_text("", encoding="utf-8")
    report = ops.reconcile_repository(repo, allow_stale_lock_removal=True)
    assert foreign.exists(), "foreign lock with no journal proof must survive"
    assert report["locks"][0]["classification"] == "unknown"
    assert not report["removed_locks"]


def test_pid_alive_rejects_dead_and_invalid_pids():
    assert not pid_alive(-1) and not pid_alive(0)
    assert pid_alive(__import__("os").getpid())


def test_install_cli_dispatches_json_reports(ops, tmp_path):
    import argparse
    from hivemind.git_operations import install_cli, run_cli
    repo, commit = make_repository(ops, tmp_path)
    parser = argparse.ArgumentParser()
    install_cli(parser.add_subparsers(dest="command", required=True))
    def resolve(args):
        return ops
    report = run_cli(parser.parse_args(["git-ops", "preflight", str(repo)]), resolve)
    assert report["ready"] and report["repo"] == str(repo)
    report = run_cli(parser.parse_args(["git-ops", "reconcile", str(repo)]), resolve)
    assert report["fsck_ok"] and report["refs"][MAIN_REF] == commit
    bundle = tmp_path / "cli.bundle"
    report = run_cli(parser.parse_args(["git-ops", "backup", str(repo), str(bundle)]), resolve)
    assert report["ok"]
    restored = tmp_path / "cli-restored.git"
    report = run_cli(parser.parse_args(["git-ops", "restore", str(bundle), str(restored)]), resolve)
    assert report["ready"] and report["receive_guard"]
    plan = run_cli(parser.parse_args(["git-ops", "retention-plan", str(repo)]), resolve)
    assert plan["digest"] and not plan["candidates"]


def test_retention_guard_injection_and_unknown_effects_block(ops, tmp_path):
    repo, commit = make_repository(ops, tmp_path)
    tree = ops.git(repo, "rev-parse", commit + "^{tree}")
    merged = ops.git(repo, "commit-tree", tree, "-p", commit, input_bytes=b"Merged\n")
    ops.git(repo, "update-ref", MAIN_REF, merged, commit)
    ops.git(repo, "update-ref", "refs/hivemind/candidates/mr_merged", merged)
    # An injected canonical/audit guard is decisive at plan time.
    hostile = ops.plan_retention(repo, guard=lambda ref, oid, evidence: False)
    assert not hostile["candidates"]
    assert {item["ref"]: item["reason"] for item in hostile["excluded"]}[
        "refs/hivemind/candidates/mr_merged"] == "guard_rejected"
    allowed = ops.plan_retention(repo)
    assert [item["ref"] for item in allowed["candidates"]] == ["refs/hivemind/candidates/mr_merged"]
    # The same guard is re-applied at execution time.
    result = ops.execute_retention(repo, allowed, confirmation=allowed["digest"],
                                   guard=lambda ref, oid, evidence: False)
    assert result["removed"] == []
    assert result["skipped"][0]["reason"] == "guard_rejected"
    assert ops.git(repo, "rev-parse", "--verify", "refs/hivemind/candidates/mr_merged") == merged
    # Unknown/unreconciled host effects block destructive flows entirely.
    with pytest.raises(Problem):
        ops.spawn(repo, ["rev-parse", "--verify", "refs/heads/never-existed"])
    with pytest.raises(Problem) as error:
        ops.execute_retention(repo, allowed, confirmation=allowed["digest"])
    assert error.value.code == "git_retention_unknown_effects"
    assert ops.git(repo, "rev-parse", "--verify", "refs/hivemind/candidates/mr_merged") == merged


def test_spawn_deadline_covers_blocked_stdin(ops, tmp_path):
    repo, _ = make_repository(ops, tmp_path)
    started = time.monotonic()
    with pytest.raises(Problem) as error:
        # Child blocks forever waiting for stdin that is never closed; the
        # deadline must bound the wait (no kill seam involved).
        ops.spawn(repo, ["cat-file", "--batch-check"], hold_stdin=True, timeout=2)
    assert error.value.code == "git_command_timeout"
    assert time.monotonic() - started < 20, "deadline must bound a blocked child"
    assert ops.reconcile_repository(repo)["fsck_ok"]


def test_spawn_input_writer_is_concurrent_and_bounded(ops, tmp_path):
    repo, _ = make_repository(ops, tmp_path)
    # A child that never reads stdin: the writer thread must not hang the call.
    started = time.monotonic()
    record = ops.spawn(repo, ["rev-parse", "--verify", MAIN_REF],
                       input_bytes=b"z" * (1024 * 1024), timeout=10)
    assert time.monotonic() - started < 10
    # Large input through the writer thread to a child that does read it.
    payload = b"blob content\n" * 65536
    blob = ops.spawn(repo, ["hash-object", "-w", "--stdin"], input_bytes=payload, timeout=30)
    assert ops.git(repo, "cat-file", "-t", blob["stdout"]) == "blob"
    assert record["returncode"] == 0


def test_spawn_observer_fault_never_leaks_the_child(ops, tmp_path):
    repo, _ = make_repository(ops, tmp_path)
    captured = {}
    def faulty(process):
        captured["pid"] = process.pid
        raise RuntimeError("injected observer fault")
    ops.spawn_observer = faulty
    with pytest.raises(RuntimeError):
        ops.spawn(repo, ["cat-file", "--batch-check"], hold_stdin=True, timeout=60)
    ops.spawn_observer = None
    deadline = time.monotonic() + 5
    while pid_alive(captured["pid"]) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not pid_alive(captured["pid"]), "observer fault must not leak a live child"
    with Journal(ops.journal_path) as journal:
        unknowns = journal.operations("unknown")
    assert any("cat-file" in record["path"] for record in unknowns)
    assert ops.git(repo, "rev-parse", "--verify", MAIN_REF)


def test_reconcile_refuses_lock_recreated_after_observation(ops, tmp_path):
    repo, _ = make_repository(ops, tmp_path)
    lock = repo / "refs" / "heads" / "main.lock"
    ops.spawn_observer = lambda process: lock.write_text("original", encoding="utf-8")
    ops.spawn(repo, ["hash-object", "--stdin"], input_bytes=b"x")
    ops.spawn_observer = None
    # A different file now sits at the observed path: not our artifact anymore.
    lock.write_text("replaced", encoding="utf-8")
    report = ops.reconcile_repository(repo, allow_stale_lock_removal=True)
    entry = next(item for item in report["locks"] if item["path"] == str(lock))
    assert entry["classification"] == "unknown" and not entry["removed"]
    assert lock.exists(), "a recreated lock must never be removed on stale identity"
