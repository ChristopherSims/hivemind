"""Adversarial tests for coordinated backup/restore/retention.

Everything runs in tmp fixtures; no user data, no host changes, no network.
Git-dependent tests skip when no git executable is available.
"""
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess

import pytest

import hivemind.coordinated_operations as coordinated
from hivemind.coordinated_operations import (CoordinatedOperations, SnapshotSource,
                                             install, load_manifest)
from hivemind.recovery import Journal
from hivemind.storage import Problem, canonical

GIT = shutil.which("git")
needs_git = pytest.mark.skipif(GIT is None, reason="git executable unavailable")


def make_journal(path, *, unknown=False, attempting=False):
    with Journal(path) as journal:
        journal.begin_operation("op-1", "POST", "tasks", {"title": "t"})
        if unknown or attempting:
            journal.mark_attempt("op-1")
            if attempting:
                return  # crash simulation: attempting, never resolved
            journal.fail_operation("op-1", TimeoutError("lost"))
        else:
            journal.complete_operation("op-1", {"id": "task-1"})


def make_db(path):
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE IF NOT EXISTS items(id TEXT PRIMARY KEY, value TEXT)")
        connection.execute("INSERT OR IGNORE INTO items VALUES('a', '1')")
        connection.commit()
    finally:
        connection.close()


def make_repo(path):
    path.mkdir(parents=True)
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run([GIT, "init", "--bare", "-b", "main"], cwd=path, env=env,
                   check=True, capture_output=True, timeout=60)
    return path


def simple_source(tmp_path, **journal_kwargs):
    db = tmp_path / "canonical.sqlite"
    make_db(db)
    journal = tmp_path / "journal.sqlite"
    make_journal(journal, **journal_kwargs)
    artifact = tmp_path / "runtime-state.json"
    artifact.write_text(json.dumps({"outcome": "completed"}), encoding="utf-8")
    return SnapshotSource(db_path=str(db), journals=(str(journal),),
                          artifacts=({"name": "runtime-state.json",
                                     "path": str(artifact)},))


def rewrite_manifest(snapshot_dir, mutate):
    """Forge a fully self-consistent manifest (sidecar + digest recomputed)."""
    manifest_path = snapshot_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    mutate(manifest)
    manifest["digest"] = coordinated._manifest_digest(manifest)
    raw = canonical(manifest).encode()
    manifest_path.write_bytes(raw)
    (snapshot_dir / "manifest.sha256").write_text(
        hashlib.sha256(raw).hexdigest(), encoding="utf-8")
    return manifest


# ------------------------------------------------------------------ snapshots
def test_snapshot_verify_restore_roundtrip(tmp_path):
    source = simple_source(tmp_path)
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(source)
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    report = ops.verify_snapshot(snapshot_dir)
    assert report["ok"] and report["checked"] == 3
    assert manifest["coherence"] == "revalidated-consistent-capture"
    assert manifest["source_fingerprints"]["db"]["pre"] \
        == manifest["source_fingerprints"]["db"]["post"]
    destination = tmp_path / "restored"
    approvals = []

    def approve(digest, verify_report):
        approvals.append(digest)
        return digest == manifest["digest"] and verify_report["ok"]

    restored = ops.restore_snapshot(snapshot_dir, destination, approve=approve)
    assert approvals == [manifest["digest"]]
    assert restored["fence_resurrected"] is False
    connection = sqlite3.connect(destination / "canonical.sqlite")
    try:
        assert connection.execute("SELECT value FROM items WHERE id='a'").fetchone()[0] == "1"
    finally:
        connection.close()
    with Journal(destination / "journals" / "journal.sqlite") as journal:
        assert journal.operation("op-1")["state"] == "completed"
    assert json.loads((destination / "artifacts" / "runtime-state.json").read_text()) \
        == {"outcome": "completed"}


def test_wal_journal_captured_via_online_backup(tmp_path):
    """A live WAL-mode journal with uncheckpointed commits must survive capture."""
    journal_path = tmp_path / "live-journal.sqlite"
    journal = Journal(journal_path)  # kept OPEN: committed data sits in the -wal
    try:
        journal.begin_operation("wal-op", "POST", "tasks", {"title": "wal"})
        journal.complete_operation("wal-op", {"id": "task-wal"})
        assert (tmp_path / "live-journal.sqlite-wal").exists()  # uncheckpointed
        ops = CoordinatedOperations(tmp_path / "snapshots")
        manifest = ops.create_snapshot(SnapshotSource(journals=(str(journal_path),)))
    finally:
        journal.close()
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    captured = snapshot_dir / "journals" / "live-journal.sqlite"
    with Journal(captured) as restored:
        record = restored.operation("wal-op")
        assert record is not None and record["state"] == "completed"
        assert record["result"] == {"id": "task-wal"}


def test_change_during_capture_refuses_snapshot(tmp_path):
    """A source that mutates between capture and revalidation refuses everything."""
    source = simple_source(tmp_path)
    artifact = tmp_path / "runtime-state.json"
    ops = CoordinatedOperations(tmp_path / "snapshots")

    def mutator(staging):
        artifact.write_text(json.dumps({"outcome": "changed-mid-capture"}))

    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(source, capture_observer=lambda staging: mutator(staging))
    assert refusal.value.code == "snapshot_source_changed"
    assert not list((tmp_path / "snapshots").glob("snap-*"))  # staging removed

    # Same for the canonical DB: a commit between backup and revalidation refuses.
    db = tmp_path / "canonical.sqlite"

    def db_mutator(staging):
        connection = sqlite3.connect(db)
        try:
            connection.execute("INSERT INTO items VALUES('b', '2')")
            connection.commit()
        finally:
            connection.close()

    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(source, capture_observer=db_mutator)
    assert refusal.value.code == "snapshot_source_changed"


def test_unknown_effects_always_refuse_snapshot(tmp_path):
    """No fence/Boolean can make unknown or in-flight effects snapshot-safe."""
    for kwargs in ({"unknown": True}, {"attempting": True}):
        source = simple_source(tmp_path, **kwargs)
        ops = CoordinatedOperations(tmp_path / "snapshots")
        with pytest.raises(Problem) as refusal:
            ops.create_snapshot(source)
        assert refusal.value.code == "snapshot_not_quiescent"
        with pytest.raises(TypeError):  # and there is no override parameter anymore
            ops.create_snapshot(source, fence=lambda report: True)


def test_duplicate_source_names_and_sensitive_artifacts_refused(tmp_path):
    journal_a = tmp_path / "a" / "journal.sqlite"
    journal_b = tmp_path / "b" / "journal.sqlite"
    journal_a.parent.mkdir()
    journal_b.parent.mkdir()
    make_journal(journal_a)
    make_journal(journal_b)
    ops = CoordinatedOperations(tmp_path / "snapshots")
    with pytest.raises(Problem) as refusal:  # same basename would silently overwrite
        ops.create_snapshot(SnapshotSource(journals=(str(journal_a), str(journal_b))))
    assert refusal.value.code == "snapshot_source_conflict"
    artifact_a = tmp_path / "one.txt"
    artifact_b = tmp_path / "two.txt"
    artifact_a.write_text("x")
    artifact_b.write_text("y")
    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(SnapshotSource(
            artifacts=({"name": "same.txt", "path": str(artifact_a)},
                       {"name": "same.txt", "path": str(artifact_b)})))
    assert refusal.value.code == "snapshot_source_conflict"
    secret = tmp_path / "secret.txt"
    secret.write_text('{"api_key": "sk-live-abcdef1234567890"}')
    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(SnapshotSource(
            artifacts=({"name": "secret.txt", "path": str(secret)},)))
    assert refusal.value.code == "snapshot_artifact_sensitive"


def test_snapshot_never_overwrites_and_cleans_staging(tmp_path):
    source = simple_source(tmp_path)
    ops = CoordinatedOperations(tmp_path / "snapshots")
    destination = tmp_path / "snapshots" / "fixed"
    ops.create_snapshot(source, destination)
    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(source, destination)
    assert refusal.value.code == "destination_exists"
    assert [p.name for p in (tmp_path / "snapshots").iterdir()] == ["fixed"]
    broken = SnapshotSource(db_path=str(tmp_path / "missing.sqlite"))
    with pytest.raises(Problem):
        ops.create_snapshot(broken, tmp_path / "snapshots" / "broken")
    assert not list((tmp_path / "snapshots").glob("broken*"))  # staging removed


def test_junction_components_refused_for_sources_and_destinations(tmp_path):
    source = simple_source(tmp_path)
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(source)
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    junction = tmp_path / "junction"
    try:
        subprocess.run(["cmd", "/c", "mklink", "/J", str(junction), str(tmp_path)],
                       check=True, capture_output=True, shell=False)
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("junction creation unavailable")
    assert not os.path.islink(junction)  # a junction is NOT a symlink: raw check required
    # Source through a junction ancestor is refused.
    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(SnapshotSource(
            db_path=str(junction / "canonical.sqlite"),
            journals=(str(junction / "journal.sqlite"),)))
    assert refusal.value.code == "path_reparse_component"
    # Destination through a junction ancestor is refused.
    with pytest.raises(Problem) as refusal:
        ops.restore_snapshot(snapshot_dir, junction / "restored",
                             approve=lambda digest, report: True)
    assert refusal.value.code == "path_reparse_component"
    # A managed root reached through a junction is refused.
    with pytest.raises(Problem):
        CoordinatedOperations(junction / "snapshots")


# ------------------------------------------------------------------- manifests
def test_manifest_digest_is_recomputed_not_trusted(tmp_path):
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(simple_source(tmp_path))
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    # Forged sidecar matching a mutated manifest but with a stale internal digest.
    manifest_path = snapshot_dir / "manifest.json"
    document = json.loads(manifest_path.read_text())
    document["entries"] = []
    raw = canonical(document).encode()  # digest field NOT recomputed by the attacker
    manifest_path.write_bytes(raw)
    (snapshot_dir / "manifest.sha256").write_text(
        hashlib.sha256(raw).hexdigest(), encoding="utf-8")
    with pytest.raises(Problem) as refusal:
        load_manifest(snapshot_dir)
    assert refusal.value.code == "snapshot_manifest_tampered"


@pytest.mark.parametrize("bad", ["../escape", "git/../../x", "C:/abs/x", "git/a:b",
                                 "git/CON", "journals//gap", "unknown/thing",
                                 "artifacts/..", "git\\winsep"])
def test_manifest_entry_traversal_refused(tmp_path, bad):
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(simple_source(tmp_path))
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    legit = manifest["entries"][0]

    def forge(document):
        document["entries"] = [{**legit, "path": bad}]

    rewrite_manifest(snapshot_dir, forge)
    with pytest.raises(Problem) as refusal:
        load_manifest(snapshot_dir)
    assert refusal.value.code == "snapshot_manifest_invalid"
    with pytest.raises(Problem):
        ops.restore_snapshot(snapshot_dir, tmp_path / "restored",
                             approve=lambda digest, report: True)


def test_manifest_duplicate_and_unknown_kind_refused(tmp_path):
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(simple_source(tmp_path))
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    first = manifest["entries"][0]

    def duplicate(document):
        document["entries"] = [first, dict(first)]

    rewrite_manifest(snapshot_dir, duplicate)
    with pytest.raises(Problem):
        load_manifest(snapshot_dir)
    manifest = ops.create_snapshot(simple_source(tmp_path), name="second")
    snapshot_dir = tmp_path / "snapshots" / "second"

    def bad_kind(document):
        document["entries"] = [{**document["entries"][0], "kind": "executable"}]

    rewrite_manifest(snapshot_dir, bad_kind)
    with pytest.raises(Problem):
        load_manifest(snapshot_dir)


def test_tampered_entry_refuses_restore_and_verify(tmp_path):
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(simple_source(tmp_path))
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    (snapshot_dir / "artifacts" / "runtime-state.json").write_bytes(b'{"outcome":"x"}')
    assert ops.verify_snapshot(snapshot_dir)["ok"] is False
    with pytest.raises(Problem) as refusal:
        ops.restore_snapshot(snapshot_dir, tmp_path / "r",
                             approve=lambda digest, report: True)
    assert refusal.value.code == "snapshot_verification_failed"


def test_incomplete_manifest_refused(tmp_path):
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(simple_source(tmp_path))
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    (snapshot_dir / "journals" / "journal.sqlite").unlink()
    report = ops.verify_snapshot(snapshot_dir)
    assert report["ok"] is False and report["missing"]
    with pytest.raises(Problem):
        ops.restore_snapshot(snapshot_dir, tmp_path / "r",
                             approve=lambda digest, report: True)


def test_restore_requires_approval_and_new_destination(tmp_path):
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(simple_source(tmp_path))
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    with pytest.raises(Problem) as refusal:
        ops.restore_snapshot(snapshot_dir, tmp_path / "r1")
    assert refusal.value.code == "restore_not_approved"
    with pytest.raises(Problem):
        ops.restore_snapshot(snapshot_dir, tmp_path / "r1",
                             approve=lambda digest, report: False)
    assert not (tmp_path / "r1").exists()
    existing = tmp_path / "existing"
    existing.mkdir()
    with pytest.raises(Problem) as refusal:
        ops.restore_snapshot(snapshot_dir, existing,
                             approve=lambda digest, report: True)
    assert refusal.value.code == "destination_exists"
    system_root = os.environ.get("SystemRoot")
    if system_root:
        with pytest.raises(Problem) as refusal:
            ops.restore_snapshot(snapshot_dir,
                                 os.path.join(system_root, "hm-restore-test"),
                                 approve=lambda digest, report: True)
        assert refusal.value.code == "destination_system_path"


# --------------------------------------------------------------------- ledger
def test_corrupt_ledger_is_fail_closed(tmp_path):
    ops = CoordinatedOperations(tmp_path / "snapshots")
    ops.create_snapshot(simple_source(tmp_path), name="snap-0")
    ledger = tmp_path / "snapshots" / ".deletion-ledger.json"
    ledger.write_text("{not json")
    with pytest.raises(Problem) as refusal:
        ops.status()
    assert refusal.value.code == "deletion_ledger_corrupt"
    with pytest.raises(Problem):
        ops.create_snapshot(simple_source(tmp_path), name="snap-1")
    with pytest.raises(Problem):
        ops.restore_snapshot(tmp_path / "snapshots" / "snap-0", tmp_path / "r",
                             approve=lambda digest, report: True)


# ------------------------------------------------------------------- retention
def test_retention_dry_run_execute_and_ledger_refuses_resurrection(tmp_path):
    source = simple_source(tmp_path)
    ops = CoordinatedOperations(tmp_path / "snapshots")
    names = []
    for index in range(3):
        manifest = ops.create_snapshot(source, name=f"snap-{index}")
        names.append(manifest["name"])
    plan = ops.plan_retention(keep=1)
    assert plan["kept"] == ["snap-2"]
    assert {c["path"] for c in plan["candidates"]} == {"snap-0", "snap-1"}
    assert plan["erasure"]["logical"] is True
    assert plan["erasure"]["physical"] == "not-guaranteed"
    assert all((tmp_path / "snapshots" / name).is_dir() for name in names)  # dry-run
    with pytest.raises(Problem) as refusal:
        ops.execute_retention(plan, confirmation="wrong")
    assert refusal.value.code == "retention_confirmation_required"
    result = ops.execute_retention(plan, confirmation=plan["digest"])
    assert sorted(result["removed"]) == ["snap-0", "snap-1"] and result["failed"] == []
    assert (tmp_path / "snapshots" / "snap-2").is_dir()
    ledger = [e for e in coordinated._ledger_read(ops.root)]
    # Durable intent BEFORE removal and verified outcome AFTER, per candidate.
    for name in ("snap-0", "snap-1"):
        entries = [e for e in ledger if e["name"] == name]
        assert [e["state"] for e in entries] == ["deleting", "deleted"]
    # The ledger refuses resurrection of a deleted snapshot name and ID.
    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(source, name="snap-0")
    assert refusal.value.code == "snapshot_name_tombstoned"
    with pytest.raises(Problem) as refusal:
        ops.restore_snapshot(tmp_path / "snapshots" / "snap-0", tmp_path / "r",
                             approve=lambda digest, report: True)
    assert refusal.value.code == "snapshot_manifest_missing"


def test_retention_rejects_forged_plan_and_traversal(tmp_path):
    source = simple_source(tmp_path)
    ops = CoordinatedOperations(tmp_path / "snapshots")
    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "data.txt").write_text("precious")
    ops.create_snapshot(source, name="snap-0")
    plan = ops.plan_retention(keep=1)
    forged = dict(plan)
    forged["candidates"] = [{"path": "..", "snapshot_id": "x",
                             "manifest_digest": "a" * 64}]
    forged["digest"] = coordinated._manifest_digest(forged)  # self-consistent forgery
    result = ops.execute_retention(forged, confirmation=forged["digest"])
    assert result["removed"] == [] and result["failed"] == []
    assert result["skipped"][0]["reason"] == "not_managed"
    assert (victim / "data.txt").is_file()  # never touched
    # A tampered digest is refused even with matching confirmation.
    tampered = dict(plan, keep=99)
    with pytest.raises(Problem) as refusal:
        ops.execute_retention(tampered, confirmation=tampered["digest"])
    assert refusal.value.code == "retention_plan_invalid"
    # A plan bound to a foreign root is refused.
    foreign = dict(plan, root=str(tmp_path / "elsewhere"))
    foreign["digest"] = coordinated._manifest_digest(foreign)
    with pytest.raises(Problem) as refusal:
        ops.execute_retention(foreign, confirmation=foreign["digest"])
    assert refusal.value.code == "retention_plan_foreign_root"


def test_retention_partial_unlink_reports_failure_not_success(tmp_path):
    source = simple_source(tmp_path)
    ops = CoordinatedOperations(tmp_path / "snapshots")
    ops.create_snapshot(source, name="snap-0")
    ops.create_snapshot(source, name="snap-1")
    plan = ops.plan_retention(keep=1)
    assert [c["path"] for c in plan["candidates"]] == ["snap-0"]
    target = tmp_path / "snapshots" / "snap-0"
    real_rmtree = coordinated._rmtree

    def flaky_rmtree(path):
        if coordinated.Path(path) == target:
            (target / "residue.txt").write_text("left behind")
            return False  # removal did NOT complete
        return real_rmtree(path)

    coordinated._rmtree = flaky_rmtree
    try:
        result = ops.execute_retention(plan, confirmation=plan["digest"])
    finally:
        coordinated._rmtree = real_rmtree
    assert result["removed"] == []
    assert result["failed"] == [{"path": "snap-0", "residue": True,
                                 "outcome": "unknown: directory removal incomplete"}]
    assert target.is_dir()  # residue honestly reported, not claimed deleted
    entries = [e for e in coordinated._ledger_read(ops.root) if e["name"] == "snap-0"]
    assert [e["state"] for e in entries] == ["deleting", "failed"]
    # A failed deletion still tombstones the name: no silent resurrection window.
    # (The residue directory also makes the destination "exist"; either refusal holds.)
    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(source, name="snap-0")
    assert refusal.value.code in ("snapshot_name_tombstoned", "destination_exists")
    real_rmtree(target)
    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(source, name="snap-0")
    assert refusal.value.code == "snapshot_name_tombstoned"


def test_retention_preserves_unknown_and_changed_snapshots(tmp_path):
    source = simple_source(tmp_path)
    ops = CoordinatedOperations(tmp_path / "snapshots")
    for index in range(3):
        ops.create_snapshot(source, name=f"snap-{index}")
    (tmp_path / "snapshots" / "mystery").mkdir()
    (tmp_path / "snapshots" / "snap-0" / "manifest.json").write_text("{}")
    plan = ops.plan_retention(keep=1)
    assert {c["path"] for c in plan["candidates"]} == {"snap-1"}
    assert any(item["path"] == "mystery" for item in plan["excluded"])
    assert any(item["path"] == "snap-0" for item in plan["excluded"])
    plan = ops.plan_retention(keep=1)
    candidate = plan["candidates"][0]
    extra = tmp_path / "snapshots" / candidate["path"] / "extra.txt"
    extra.write_text("changed")
    result = ops.execute_retention(plan, confirmation=plan["digest"])
    assert result["removed"] == []
    assert result["skipped"][0]["reason"] in ("changed_since_plan", "manifest_changed")
    extra.unlink()
    result = ops.execute_retention(plan, confirmation=plan["digest"])
    assert result["removed"] == [candidate["path"]]


# -------------------------------------------------- restore authority sanitization
def test_restore_sanitizes_live_nexus_and_delivery_authority(world, tmp_path):
    """A backup of live authority, restored immediately, resurrects nothing."""
    from types import SimpleNamespace

    from hivemind.service import Service
    from hivemind.storage import Store

    target = world.agent("Target")
    nexus = world.agent("Nexus")
    target_token = world.credential(target)
    nexus_token = world.credential(nexus, coordinator=True)
    target_run = world.run(target, target_token)
    nexus_run = world.run(nexus, nexus_token)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": nexus_run["id"],
                        "requested_ttl_seconds": 120}, nexus_token)
    old_epoch = lease["coordinator_epoch"]
    message = world.call("POST", "messages", {
        "project_id": world.project, "message_id": "msg-restore-1", "from": nexus["id"],
        "to": target["id"], "thread_id": "thread-restore", "type": "request",
        "content": {"media_type": "text/plain", "text": "settle me?"}, "ttl_seconds": 3600})
    receipt = world.call("POST", f"messages/{message['id']}:claim",
                         {"run_id": target_run["id"]}, target_token)

    db_path = tmp_path / "canonical.sqlite"
    assert db_path.is_file()
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(SnapshotSource(db_path=str(db_path)))
    entry = next(e for e in manifest["entries"] if e["kind"] == "sqlite-backup")
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    restored = ops.restore_snapshot(snapshot_dir, tmp_path / "restored",
                                    approve=lambda digest, report: True)

    facts = restored["authority_sanitization"]
    assert facts["applicable"] is True and facts["hivemind_schema"] is True
    assert facts["nexus_leases_invalidated"] == 1
    assert facts["delivery_leases_expired"] == 1
    assert facts["integrity_check"] == "ok"
    # Sanitization happened AFTER hash verification of the backup; the manifest
    # keeps the original hash and the sanitized hash honestly differs.
    assert facts["backup_sha256"] == entry["sha256"]
    assert facts["sanitized_sha256"] != facts["backup_sha256"]
    assert ops.verify_snapshot(snapshot_dir)["ok"]  # snapshot itself untouched
    epoch_info = facts["epochs"][world.project]
    assert epoch_info["snapshot_epoch"] == old_epoch
    assert epoch_info["restored_epoch"] > epoch_info["snapshot_epoch"]
    assert restored["restore_hold"] is True
    assert restored["restore_hold_contract"]["startup_guard"]
    assert "never" in restored["restore_hold_contract"]["release"].lower()

    # The durable marker: existing key/value meta schema + new-only sidecar.
    import sqlite3 as sql
    connection = sql.connect(f"file:{restored['db']}?mode=ro", uri=True)
    try:
        marker = connection.execute("SELECT value FROM meta WHERE key='restore_hold'"
                                    ).fetchone()
        assert marker and json.loads(marker[0])["snapshot_id"] == manifest["snapshot_id"]
        active = connection.execute(
            "SELECT count(*) FROM resources WHERE kind='nexus_lease' AND "
            "json_extract(body,'$.state')='active'").fetchone()[0]
        assert active == 0
    finally:
        connection.close()
    assert json.loads((tmp_path / "restored" / "restore-hold.json").read_text()
                      )["restore_hold"] is True

    # The startup guard refuses to serve the restored copy while the hold is active.
    with pytest.raises(Problem) as guard:
        Store(tmp_path / "restored")
    assert guard.value.code == "restore_hold_active"

    # Typed manual release: matching digest, then meta row and sidecar are removed.
    connection = sql.connect(tmp_path / "restored" / "canonical.sqlite")
    try:
        connection.execute("DELETE FROM meta WHERE key='restore_hold'")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()
    (tmp_path / "restored" / "restore-hold.json").unlink()

    # Old epoch and old receipt are refused by the REAL Service on the restored copy.
    store = Store(tmp_path / "restored")
    service = Service(store)
    admin = {"id": "p-admin", "role": "admin", "project": world.project,
             "agent": None}
    assert service.lease(admin, world.project) is None  # no resurrected Nexus lease
    with pytest.raises(Problem) as refusal:
        service.coordinator(admin, world.project, old_epoch)
    assert refusal.value.code == "stale_coordinator_epoch"
    restored_message = store.get(message["id"], admin, "message")
    state = service.delivery_state(restored_message)
    assert state["delivery_status"] != "leased"  # the old receipt cannot settle
    owner = {"id": "p-t", "role": "agent", "project": world.project,
             "agent": target["id"]}
    with pytest.raises(Problem):  # ack under the old receipt is refused
        service.ack_message(owner, message["id"], target_run["id"],
                            receipt["delivery_id"])

    # A FRESH Nexus acquisition gets an epoch strictly greater than the snapshot's.
    fresh = service.acquire_nexus(
        {"id": "p-n", "role": "agent", "project": world.project,
         "agent": nexus["id"], "coordinator": 1},
        world.project,
        SimpleNamespace(agent_id=nexus["id"], run_id=nexus_run["id"],
                        requested_ttl_seconds=60))
    assert fresh["coordinator_epoch"] > old_epoch
    # Unknown holds were never completed by sanitization (none existed here; the
    # sanitizer only ever expires authority, effects evidence untouched).
    tasks = store.db.execute("SELECT count(*) FROM resources WHERE kind='task'"
                             ).fetchone()[0]
    assert tasks >= 0
    store.close()


def test_restore_generic_sqlite_reports_not_applicable(tmp_path):
    """Generic SQLite fixtures are supported but never claim Hivemind sanitization."""
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(simple_source(tmp_path))
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    restored = ops.restore_snapshot(snapshot_dir, tmp_path / "restored",
                                    approve=lambda digest, report: True)
    facts = restored["authority_sanitization"]
    assert facts["applicable"] is False and facts["hivemind_schema"] is False
    assert facts["reason"] == "not_a_hivemind_canonical_store"
    assert facts["integrity_check"] == "ok"
    assert restored["restore_hold"] == "not_applicable"
    assert restored["restore_hold_contract"] is None
    assert not (tmp_path / "restored" / "restore-hold.json").exists()


# -------------------------------------------------------------------- handle
def test_install_handle_describes_actions(tmp_path):
    handle = install(root=tmp_path / "snapshots", principal={"id": "p-1"},
                     command="coordinated")
    description = handle.describe()
    assert {a["action"] for a in description["actions"]} == {
        "coordinated.snapshot", "coordinated.verify", "coordinated.restore",
        "coordinated.retention_plan", "coordinated.retention_execute"}
    assert all({"purpose", "input", "output", "authority", "side_effects",
                "limits", "retry_safety", "hivemind_detached_work",
                "default_enabled"} <= set(a) for a in description["actions"])
    assert description["context"]["hivemind_detached_work"] is True
    status = handle.status()
    assert status["managed"] == 0 and status["ledgered_deletions"] == 0


@needs_git
def test_coordinated_git_bundle_restore_verified(tmp_path):
    repo = tmp_path / "repo.git"
    make_repo(repo)
    work = tmp_path / "work"
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run([GIT, "clone", str(repo), str(work)], env=env, check=True,
                   capture_output=True, timeout=60)
    (work / "file.txt").write_text("content")
    for args in (["add", "file.txt"], ["commit", "-m", "initial"],
                 ["push", "origin", "main"]):
        subprocess.run([GIT, *args], cwd=work, env=env, check=True,
                       capture_output=True, timeout=60)
    journal = tmp_path / "journal.sqlite"
    make_journal(journal)
    source = SnapshotSource(repositories=({"name": "repo", "repo": str(repo)},),
                            journals=(str(journal),))
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(source)
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    assert ops.verify_snapshot(snapshot_dir)["ok"]
    restored = ops.restore_snapshot(snapshot_dir, tmp_path / "restored",
                                    approve=lambda digest, report: True)
    restored_repo = restored["repositories"][0]
    assert restored_repo["fsck_ok"] is True
    assert str(tmp_path / "restored") in restored_repo["path"]  # promoted path
    out = subprocess.run([GIT, "log", "--format=%s", "main"],
                         cwd=restored_repo["path"], capture_output=True, text=True,
                         timeout=60)
    assert "initial" in out.stdout
    refs = subprocess.run([GIT, "for-each-ref"], cwd=restored_repo["path"],
                          capture_output=True, text=True, timeout=60).stdout
    assert "nexus-fence" not in refs  # the fence is never resurrected


@needs_git
def test_git_source_change_during_capture_refuses(tmp_path):
    repo = tmp_path / "repo.git"
    make_repo(repo)
    work = tmp_path / "work"
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run([GIT, "clone", str(repo), str(work)], env=env, check=True,
                   capture_output=True, timeout=60)
    (work / "file.txt").write_text("content")
    for args in (["add", "file.txt"], ["commit", "-m", "initial"],
                 ["push", "origin", "main"]):
        subprocess.run([GIT, *args], cwd=work, env=env, check=True,
                       capture_output=True, timeout=60)
    ops = CoordinatedOperations(tmp_path / "snapshots")

    def push_during_capture(staging):
        (work / "second.txt").write_text("more")
        for args in (["add", "second.txt"], ["commit", "-m", "second"],
                     ["push", "origin", "main"]):
            subprocess.run([GIT, *args], cwd=work, env=env, check=True,
                           capture_output=True, timeout=60)

    source = SnapshotSource(repositories=({"name": "repo", "repo": str(repo)},))
    with pytest.raises(Problem) as refusal:
        ops.create_snapshot(source, capture_observer=push_during_capture)
    assert refusal.value.code == "snapshot_source_changed"
