"""Adversarial tests for hardened restore-hold release.

Everything runs in tmp fixtures against REAL restored canonical stores produced
by the actual snapshot/restore pipeline; no user data, no host changes, no
network, no broad deletion.
"""
import json
import os
import secrets
import sqlite3

import pytest

import hivemind.restore_release as release
from hivemind.coordinated_operations import CoordinatedOperations, SnapshotSource
from hivemind.restore_release import (evidence_digest, load_evidence_file,
                                      release_restore_hold, resume_release)
from hivemind.storage import Problem, Store


@pytest.fixture
def restored(world, tmp_path):
    """A real restored canonical store with an active restore hold.

    The restored DB carries the owner admin principal (from ``initialize``)
    and a non-admin agent principal, both snapshotted, so identity
    verification runs against real current bindings.
    """
    owner = json.loads((tmp_path / "owner.json").read_text())
    agent = world.agent("Worker")
    agent_token = world.credential(agent, role="agent")
    nexus = world.agent("Nexus")
    nexus_token = world.credential(nexus, coordinator=True)
    nexus_run = world.run(nexus, nexus_token)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": nexus_run["id"],
                        "requested_ttl_seconds": 120}, nexus_token)
    ops = CoordinatedOperations(tmp_path / "snapshots")
    manifest = ops.create_snapshot(
        SnapshotSource(db_path=str(tmp_path / "canonical.sqlite")))
    snapshot_dir = tmp_path / "snapshots" / manifest["name"]
    store_dir = tmp_path / "restored"
    report = ops.restore_snapshot(snapshot_dir, store_dir,
                                  approve=lambda digest, verify: True)
    assert report["restore_hold"] is True
    evidence = {
        "snapshot_id": manifest["snapshot_id"],
        "manifest_digest": manifest["digest"],
        "operator": owner["principal_id"],  # must equal the VERIFIED principal
        "authority": "change-ticket CHG-1",
        "approved_store": str(store_dir),
        "acknowledged": True,
    }
    return {"store": store_dir, "manifest": manifest, "evidence": evidence,
            "old_epoch": lease["coordinator_epoch"], "world": world,
            "owner": tmp_path / "owner.json",
            "principal_id": owner["principal_id"],
            "agent_token": agent_token}


def go(restored, **kwargs):
    kwargs.setdefault("identity", restored["owner"])
    return release_restore_hold(restored["store"], restored["evidence"], **kwargs)


def go_resume(restored, **kwargs):
    kwargs.setdefault("identity", restored["owner"])
    return resume_release(restored["store"], restored["evidence"], **kwargs)


def marker(store_dir):
    connection = sqlite3.connect(f"file:{store_dir / 'canonical.sqlite'}?mode=ro",
                                 uri=True)
    try:
        row = connection.execute(
            "SELECT value FROM meta WHERE key='restore_hold'").fetchone()
        return json.loads(row[0]) if row else None
    finally:
        connection.close()


def evidence_for(store_dir, evidence):
    return {**evidence, "approved_store": str(store_dir)}


# ------------------------------------------------------------- happy path
def test_release_removes_markers_and_audits(restored, tmp_path):
    store_dir = restored["store"]
    report = go(restored)
    assert report["restore_hold"] == "released"
    assert report["manifest_digest"] == restored["manifest"]["digest"]
    assert report["principal"] == restored["principal_id"]
    assert report["role"] == "admin"
    assert report["resumed"] is False
    assert "NO fresh Nexus" in report["note"]
    assert marker(store_dir) is None
    assert not (store_dir / "restore-hold.json").exists()
    assert not (store_dir / ".restore-release-intent.json").exists()
    events = [json.loads(line) for line in
              (store_dir / "restore-release-audit.jsonl").read_text().splitlines()]
    assert [e["event"] for e in events] == ["release_intent_staged",
                                            "release_completed"]
    # The audit records the verified principal, never the token or its hash.
    assert events[0]["principal"] == restored["principal_id"]
    owner = json.loads(restored["owner"].read_text())
    assert owner["token"] not in json.dumps(events)
    assert release._hash_token(owner["token"]) not in json.dumps(events)
    # The store now opens and the project epoch stayed strictly monotonic.
    store = Store(store_dir)
    try:
        epoch = store.db.execute("SELECT epoch FROM projects").fetchone()["epoch"]
        assert epoch > restored["old_epoch"]
    finally:
        store.db.close()


def test_release_isolated_check_configured(restored):
    calls = []
    report = go(restored,
                isolation_check=lambda path: calls.append(path) or True)
    assert report["restore_hold"] == "released"
    assert calls == [str(restored["store"])]
    with pytest.raises(Problem) as refusal:
        go(restored)
    assert refusal.value.code == "restore_hold_not_active"  # already released


def test_failed_isolation_check_refuses(restored):
    with pytest.raises(Problem) as refusal:
        go(restored, isolation_check=lambda path: False)
    assert refusal.value.code == "isolation_unverified"
    assert marker(restored["store"]) is not None  # hold untouched


# ------------------------------------------- identity/authority verification
def test_arbitrary_strings_without_identity_refused(restored):
    """Self-declared operator/authority strings alone prove nothing."""
    with pytest.raises(Problem) as refusal:
        release_restore_hold(restored["store"], restored["evidence"])
    assert refusal.value.code == "restore_release_identity_required"
    assert marker(restored["store"]) is not None


def test_forged_admin_name_refused(restored):
    """A valid admin credential cannot release under ANOTHER operator name."""
    forged = {**restored["evidence"], "operator": "principal_someone_else"}
    with pytest.raises(Problem) as refusal:
        release_restore_hold(restored["store"], forged,
                             identity=restored["owner"])
    assert refusal.value.code == "restore_release_identity_mismatch"
    assert marker(restored["store"]) is not None


def test_foreign_token_refused(restored, tmp_path):
    foreign = tmp_path / "foreign.json"
    foreign.write_text(json.dumps({"token": secrets.token_urlsafe(32),
                                   "project_id": restored["world"].project}),
                       encoding="utf-8")
    with pytest.raises(Problem) as refusal:
        release_restore_hold(restored["store"], restored["evidence"],
                             identity=foreign)
    assert refusal.value.code == "restore_release_identity_invalid"
    assert marker(restored["store"]) is not None


def test_revoked_principal_refused(restored):
    """A credential whose principal row is gone from the CURRENT DB fails."""
    connection = sqlite3.connect(restored["store"] / "canonical.sqlite")
    try:
        connection.execute("DELETE FROM principals WHERE id=?",
                           (restored["principal_id"],))
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()
    with pytest.raises(Problem) as refusal:
        go(restored)
    assert refusal.value.code == "restore_release_identity_invalid"
    assert marker(restored["store"]) is not None


def test_non_admin_role_forbidden(restored, tmp_path):
    credential = tmp_path / "agent.json"
    credential.write_text(json.dumps({"token": restored["agent_token"]}),
                          encoding="utf-8")
    evidence = {**restored["evidence"]}  # operator still the admin principal
    with pytest.raises(Problem) as refusal:
        release_restore_hold(restored["store"], evidence, identity=credential)
    assert refusal.value.code == "restore_release_identity_forbidden"
    assert marker(restored["store"]) is not None


def test_verifier_receives_current_bound_snapshot(restored):
    """A trusted parent verifier sees the CURRENT hold identity, not hearsay."""
    captured = {}

    def verify(context):
        captured.update(context)
        return (context["snapshot_id"] == restored["manifest"]["snapshot_id"]
                and context["manifest_digest"] == restored["manifest"]["digest"]
                and context["principal"]["role"] == "admin")

    report = release_restore_hold(restored["store"], restored["evidence"],
                                  identity=restored["owner"],
                                  verify_identity=verify)
    assert report["restore_hold"] == "released"
    from pathlib import Path
    assert captured["evidence_digest"] == evidence_digest(
        release.validate_evidence(restored["evidence"],
                                  store_dir=Path(restored["store"]).resolve()))
    assert captured["principal"]["id"] == restored["principal_id"]


def test_verifier_only_and_refusing_verifier(restored):
    # Verifier-only mode (no credential file) works when the parent vouches.
    report = release_restore_hold(
        restored["store"], restored["evidence"], verify_identity=lambda c: True)
    assert report["restore_hold"] == "released"
    assert report["principal"] is None


def test_refusing_verifier_blocks_release(restored):
    with pytest.raises(Problem) as refusal:
        go(restored, verify_identity=lambda context: False)
    assert refusal.value.code == "restore_release_identity_unverified"
    assert marker(restored["store"]) is not None


def test_throwing_verifier_is_a_refusal(restored):
    def boom(context):
        raise RuntimeError("verifier unavailable")
    with pytest.raises(Problem) as refusal:
        go(restored, verify_identity=boom)
    assert refusal.value.code == "restore_release_identity_unverified"
    assert marker(restored["store"]) is not None


# ------------------------------------------------- evidence/authority checks
def test_authority_digest_mismatch_refuses(restored):
    bad = {**restored["evidence"], "manifest_digest": "0" * 64}
    with pytest.raises(Problem) as refusal:
        release_restore_hold(restored["store"], bad, identity=restored["owner"])
    assert refusal.value.code == "restore_hold_mismatch"
    assert marker(restored["store"]) is not None
    assert (restored["store"] / "restore-hold.json").exists()


def test_foreign_snapshot_refused(restored):
    bad = {**restored["evidence"], "snapshot_id": "someone-elses-snapshot"}
    with pytest.raises(Problem) as refusal:
        release_restore_hold(restored["store"], bad, identity=restored["owner"])
    assert refusal.value.code == "restore_hold_mismatch"


def test_foreign_path_refused(restored, tmp_path):
    other = tmp_path / "other"
    other.mkdir()
    bad = {**restored["evidence"], "approved_store": str(other)}
    with pytest.raises(Problem) as refusal:
        release_restore_hold(restored["store"], bad, identity=restored["owner"])
    assert refusal.value.code == "restore_release_path_mismatch"


def test_approved_store_reparse_ancestor_refused(restored, tmp_path):
    """The RAW approved_store ancestors are checked BEFORE resolve."""
    link = tmp_path / "approval-link"
    try:
        os.symlink(restored["store"], link, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation unavailable")
    bad = {**restored["evidence"], "approved_store": str(link)}
    with pytest.raises(Problem) as refusal:
        release_restore_hold(restored["store"], bad, identity=restored["owner"])
    assert refusal.value.code in ("path_reparse_component",
                                  "restore_release_path_mismatch",
                                  "restore_release_evidence_invalid")


@pytest.mark.parametrize("mutate", [
    lambda e: e.pop("operator"),
    lambda e: e.update(manifest_digest="not-hex"),
    lambda e: e.update(acknowledged="yes"),
    lambda e: e.update(extra_field="surprise"),
    lambda e: e.update(authority="token: abcdef123"),
])
def test_malformed_evidence_refused(restored, mutate):
    bad = dict(restored["evidence"])
    mutate(bad)
    with pytest.raises(Problem) as refusal:
        release_restore_hold(restored["store"], bad, identity=restored["owner"])
    assert refusal.value.code == "restore_release_evidence_invalid"
    assert marker(restored["store"]) is not None


def test_evidence_file_loading(restored, tmp_path):
    path = tmp_path / "approval.json"
    path.write_text(json.dumps(restored["evidence"]), encoding="utf-8")
    loaded = load_evidence_file(path)
    assert evidence_digest(loaded) == evidence_digest(restored["evidence"])
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(Problem) as refusal:
        load_evidence_file(path)
    assert refusal.value.code == "restore_release_evidence_invalid"
    oversized = tmp_path / "big.json"
    oversized.write_bytes(b" " * (release.MAX_EVIDENCE_BYTES + 1))
    with pytest.raises(Problem):
        load_evidence_file(oversized)


# ------------------------------------------------------------- path safety
def test_junction_store_refused(restored, tmp_path):
    link = tmp_path / "linked"
    try:
        os.symlink(restored["store"], link, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation unavailable")
    with pytest.raises(Problem) as refusal:
        release_restore_hold(link, evidence_for(link, restored["evidence"]),
                             identity=restored["owner"])
    assert refusal.value.code in ("restore_release_path_unsafe",
                                  "path_reparse_component")
    assert marker(restored["store"]) is not None


def test_missing_store_refused(tmp_path):
    with pytest.raises(Problem) as refusal:
        release_restore_hold(tmp_path / "nowhere", {"approved_store": "x"})
    assert refusal.value.code == "restore_hold_not_active"


# ------------------------------------------------------ tampered/orphan state
def test_tampered_sidecar_refused(restored):
    sidecar = restored["store"] / "restore-hold.json"
    state = json.loads(sidecar.read_text())
    state["manifest_digest"] = "f" * 64
    sidecar.write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(Problem) as refusal:
        go(restored)
    assert refusal.value.code == "restore_hold_mismatch"
    assert marker(restored["store"]) is not None


def test_corrupt_sidecar_refused_nothing_erased(restored):
    sidecar = restored["store"] / "restore-hold.json"
    sidecar.write_text("{corrupt", encoding="utf-8")
    with pytest.raises(Problem) as refusal:
        go(restored)
    assert refusal.value.code == "restore_hold_marker_corrupt"
    assert sidecar.exists()  # uncertain evidence is never erased
    assert marker(restored["store"]) is not None


def test_orphan_sidecar_without_marker_refused(restored):
    # Simulate a partial release done outside this helper: meta row gone first.
    connection = sqlite3.connect(restored["store"] / "canonical.sqlite")
    try:
        connection.execute("DELETE FROM meta WHERE key='restore_hold'")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()
    with pytest.raises(Problem) as refusal:
        go(restored)
    assert refusal.value.code == "restore_release_orphaned"
    assert (restored["store"] / "restore-hold.json").exists()  # never auto-cleaned
    # The startup guard still refuses to serve the orphaned store.
    with pytest.raises(Problem) as guard:
        Store(restored["store"])
    assert guard.value.code == "restore_hold_active"


def test_missing_sidecar_with_marker_refused(restored):
    (restored["store"] / "restore-hold.json").unlink()
    with pytest.raises(Problem) as refusal:
        go(restored)
    assert refusal.value.code == "restore_hold_not_active"
    assert marker(restored["store"]) is not None


def test_sanitization_residue_refuses(restored):
    connection = sqlite3.connect(restored["store"] / "canonical.sqlite")
    try:
        row = connection.execute(
            "SELECT id, body FROM resources WHERE kind='nexus_lease'").fetchone()
        body = json.loads(row[1])
        body["state"] = "active"
        connection.execute("UPDATE resources SET body=? WHERE id=?",
                           (json.dumps(body), row[0]))
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(Problem) as refusal:
        go(restored)
    assert refusal.value.code == "restore_sanitization_residue"
    assert marker(restored["store"]) is not None


def test_epoch_regression_refuses(restored):
    connection = sqlite3.connect(restored["store"] / "canonical.sqlite")
    try:
        connection.execute("UPDATE projects SET epoch=0")
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(Problem) as refusal:
        go(restored)
    assert refusal.value.code == "restore_epoch_regression"
    assert marker(restored["store"]) is not None


# ------------------------------------------------------- crash coordination
def test_crash_before_mutation_resumes_with_same_evidence(restored, monkeypatch):
    store_dir = restored["store"]

    def crash(store, evidence):
        raise sqlite3.OperationalError("injected crash")
    monkeypatch.setattr(release, "_apply_marker_removal", crash)
    with pytest.raises(Problem) as failure:
        go(restored)
    assert failure.value.code == "restore_release_failed"
    assert (store_dir / ".restore-release-intent.json").exists()
    assert marker(store_dir) is not None  # nothing was released
    # The startup guard refuses while the intent is staged.
    with pytest.raises(Problem) as guard:
        Store(store_dir)
    assert guard.value.code == "restore_release_incomplete"
    monkeypatch.undo()
    # Default: refuse the orphan intent. Wrong evidence: refuse recovery.
    with pytest.raises(Problem) as refusal:
        go(restored)
    assert refusal.value.code == "restore_release_intent_present"
    wrong = {**restored["evidence"], "authority": "change-ticket CHG-OTHER"}
    with pytest.raises(Problem) as mismatch:
        resume_release(store_dir, wrong, identity=restored["owner"])
    assert mismatch.value.code == "restore_release_intent_mismatch"
    assert marker(store_dir) is not None
    # Same evidence: idempotent recovery completes the release.
    report = go_resume(restored)
    assert report["restore_hold"] == "released" and report["resumed"] is True
    assert report["principal"] == restored["principal_id"]
    assert marker(store_dir) is None
    assert not (store_dir / "restore-hold.json").exists()
    assert not (store_dir / ".restore-release-intent.json").exists()


def test_crash_between_marker_and_sidecar_still_guarded(restored, monkeypatch):
    store_dir = restored["store"]

    def crash(store):
        raise OSError("injected crash after marker removal")
    monkeypatch.setattr(release, "_remove_sidecar", crash)
    with pytest.raises(Problem):
        go(restored)
    monkeypatch.undo()
    # Meta row is gone, sidecar remains: the store must STILL refuse to serve.
    assert marker(store_dir) is None
    assert (store_dir / "restore-hold.json").exists()
    with pytest.raises(Problem) as guard:
        Store(store_dir)
    assert guard.value.code in ("restore_hold_active", "restore_release_incomplete")
    report = go_resume(restored)
    assert report["crash_state"] == "marker_removed_sidecar_present"
    assert not (store_dir / "restore-hold.json").exists()
    Store(store_dir).db.close()  # now servable


def test_resume_after_completed_release_is_honest_noop(restored, monkeypatch):
    store_dir = restored["store"]
    real_clear = release._clear_intent

    def crash(store):
        raise OSError("injected crash after completion")
    monkeypatch.setattr(release, "_clear_intent", crash)
    with pytest.raises(OSError):
        go(restored)
    monkeypatch.setattr(release, "_clear_intent", real_clear)
    assert marker(store_dir) is None
    assert not (store_dir / "restore-hold.json").exists()
    report = go_resume(restored)
    assert report["restore_hold"] == "already_released"
    assert report["crash_state"] == "already_released"


def test_restart_and_startup_guard_transitions(restored):
    """Full lifecycle: hold -> guarded -> released -> servable."""
    store_dir = restored["store"]
    with pytest.raises(Problem) as guard:
        Store(store_dir)
    assert guard.value.code == "restore_hold_active"
    go(restored)
    Store(store_dir).db.close()


def test_concurrent_release_refused(restored):
    held = os.open(str(restored["store"] / ".restore-release.lock"),
                   os.O_CREAT | os.O_EXCL | os.O_RDWR)
    try:
        with pytest.raises(Problem) as refusal:
            go(restored)
        assert refusal.value.code == "restore_release_store_busy"
    finally:
        os.close(held)
        (restored["store"] / ".restore-release.lock").unlink()
    assert marker(restored["store"]) is not None
