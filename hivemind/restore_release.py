"""Hardened, crash-safe release of a restored store's durable restore hold.

This module OWNS the release path for the ``restore_hold`` contract written by
``hivemind.coordinated_operations`` (meta row ``restore_hold`` in the restored
``canonical.sqlite`` plus the new-only sidecar ``restore-hold.json`` at the
restore root). It replaces any inline "delete the marker, unlink the sidecar"
logic, which had no authority verification and no crash coordination.

Hardening contract:

* **Offline only, exclusive guard bound to the restored store.** All phases are
  serialized through an exclusive lock file inside the restored store directory
  (bounded wait). The mutation transaction uses a short busy timeout so a store
  that is actually being served (a live writer holding the database) fails the
  release instead of being mutated under it. This is a local heuristic bound to
  THIS store copy — it is honest best-effort, not cross-host liveness proof, and
  the caller is still required to run this offline with no live server.
* **Typed approval evidence, never a Boolean.** Release requires an explicit
  evidence object: exact ``snapshot_id`` AND ``manifest_digest`` (64-hex) AND
  the operator identity AND an authority statement AND the approved restored
  store path. Unknown fields, credential-shaped material, malformed digests,
  or an ``approved_store`` that is not THIS store refuse the release. An
  optional ``acknowledged`` Boolean is RECORDED in the audit trail but is
  never treated as proof of paused/quiesced/no-effects.
* **Verified current identity/authority, never self-declared strings.** The
  evidence ``operator``/``authority`` strings alone prove nothing. Release
  additionally requires EITHER an owner-format credential file (the existing
  ``initialize`` format: ``{"token", "project_id", "principal_id"}``) whose
  token hash is matched against the CURRENT ``principals`` table of the
  restored database — the exact role (must be ``admin``), project binding
  (must exist in ``projects``), and principal binding come from the database,
  not from the evidence — OR a trusted ``verify_identity`` callable supplied
  by the embedding parent, which receives the CURRENT hold's
  ``snapshot_id``/``manifest_digest`` read fresh from the marker plus the
  verified principal (when a credential is also given). The evidence
  ``operator`` must equal the verified principal id: forging another admin's
  name is refused. A credential whose hash matches no principal row (revoked,
  rotated, or foreign) is refused. The token is hashed in memory and is NEVER
  printed, logged, audited, or persisted.
* **Offline exclusion is the operator's duty, not a proven property.** The
  lock file and the short mutation busy-timeout only make a store that is
  actively being written FAIL the release; writer-absence is a local heuristic
  bound to this copy and is NOT accepted as proven quiescence. The caller must
  guarantee offline exclusion (no live server, no other host serving a copy).
* **Path safety before any resolve.** Symlink/reparse components (including
  Windows junctions) are rejected on the RAW path; system roots, drive roots,
  and the user home root are refused as restored stores.
* **Exact marker verification, fail closed.** The sidecar and the meta marker
  must BOTH exist (a missing sidecar with a live marker is tolerated and
  reported; a sidecar without its marker is an orphan and is REFUSED — never
  silently cleaned), parse within size bounds, and match the evidence exactly.
  The database must pass ``quick_check``, carry zero sanitization residue (no
  active Nexus lease, no leased delivery), and keep epoch monotonicity (every
  project epoch strictly greater than any snapshotted coordinator epoch).
* **Crash-safe staged release.** Before any mutation a durable intent file is
  atomically staged (write-temp + rename + fsync) carrying the evidence digest.
  Every outcome is appended to a durable audit log. A crash between "meta row
  deleted" and "sidecar removed" can therefore never enable service: the
  ``Store`` startup guard (``hivemind.storage``) refuses on the sidecar OR the
  intent file even when the meta row is already gone. Recovery is explicit and
  idempotent: ``resume=True`` (or :func:`resume_release`) re-verifies and
  completes ONLY with the SAME evidence digest; a mismatched digest is refused.
* **Never erases uncertain evidence, never over-claims.** Effect/task execution
  evidence is untouched. A failed release leaves the intent and audit trail in
  place and is reported as failed/unknown, never as released. Releasing the
  hold grants NO fresh Nexus/delivery authority: the operator must re-verify
  identity and acquire a fresh Nexus lease (strictly greater epoch) before
  serving. No remote/live authority is acquired and no model is run here.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path

from .coordinated_operations import (RESTORE_HOLD_KEY, _check_raw_components,
                                     _HEX64, _is_reparse_or_symlink,
                                     _system_roots)
from .recovery import looks_sensitive
from .storage import (RESTORE_HOLD_SIDECAR, RESTORE_RELEASE_INTENT, Problem,
                      canonical, now)

__all__ = [
    "EVIDENCE_FIELDS",
    "RELEASE_AUDIT_NAME",
    "evidence_digest",
    "load_evidence_file",
    "release_restore_hold",
    "resume_release",
    "validate_evidence",
]

RELEASE_LOCK_NAME = ".restore-release.lock"
RELEASE_AUDIT_NAME = "restore-release-audit.jsonl"
RELEASE_LOCK_WAIT_SECONDS = 5.0
MUTATION_BUSY_TIMEOUT_MS = 250  # heuristic only: a live writer fails fast
MAX_EVIDENCE_BYTES = 64 * 1024
MAX_MARKER_BYTES = 64 * 1024
MAX_CREDENTIAL_BYTES = 64 * 1024
REQUIRED_ROLE = "admin"  # release authority: exact current role from the DB
EVIDENCE_FIELDS = frozenset(
    {"snapshot_id", "manifest_digest", "operator", "authority",
     "approved_store", "acknowledged"})


# ------------------------------------------------------------------ evidence
def evidence_digest(evidence):
    """Stable digest of typed approval evidence (canonical JSON, sha256).

    The SAME evidence re-presented after a crash (any JSON formatting) yields
    the same digest, which is what binds a resumable recovery to the original
    approval.
    """
    return hashlib.sha256(canonical(evidence).encode("utf-8")).hexdigest()


def validate_evidence(evidence, *, store_dir):
    """Validate typed approval evidence against the exact restored store.

    Returns a normalized dict. Refuses malformed types, unknown fields,
    credential-shaped material, and any ``approved_store`` that is not the
    exact store being released. ``acknowledged`` is kept for the audit record
    only; it is NOT proof of quiescence and is never required to be true.
    """
    if not isinstance(evidence, dict):
        raise Problem("restore_release_evidence_invalid", 400)
    unknown = set(evidence) - EVIDENCE_FIELDS
    if unknown:
        raise Problem("restore_release_evidence_invalid", 400)
    for field in ("snapshot_id", "operator", "authority", "approved_store"):
        if not isinstance(evidence.get(field), str) or not evidence[field].strip():
            raise Problem("restore_release_evidence_invalid", 400)
    digest_value = evidence.get("manifest_digest")
    if not isinstance(digest_value, str) or not _HEX64.match(digest_value):
        raise Problem("restore_release_evidence_invalid", 400)
    acknowledged = evidence.get("acknowledged", False)
    if not isinstance(acknowledged, bool):
        raise Problem("restore_release_evidence_invalid", 400)
    if looks_sensitive({k: v for k, v in evidence.items() if k != "acknowledged"}):
        # Approval evidence must never carry tokens/secrets.
        raise Problem("restore_release_evidence_invalid", 400)
    # Reject symlink/reparse ancestors on the RAW approved path BEFORE resolve,
    # exactly like the store path itself: a junctioned approval path must not
    # be resolved into a match with the real store.
    _check_raw_components(Path(evidence["approved_store"]))
    try:
        approved = Path(evidence["approved_store"]).resolve(strict=True)
    except (OSError, RuntimeError):
        raise Problem("restore_release_evidence_invalid", 400) from None
    if os.path.normcase(str(approved)) != os.path.normcase(str(store_dir)):
        raise Problem("restore_release_path_mismatch", 409)
    return {"snapshot_id": evidence["snapshot_id"],
            "manifest_digest": digest_value,
            "operator": evidence["operator"].strip(),
            "authority": evidence["authority"].strip(),
            "approved_store": str(approved),
            "acknowledged": acknowledged}


def load_evidence_file(path):
    """Load and bound-check an approval evidence JSON file (digest input)."""
    path = Path(path)
    _check_raw_components(path)
    if _is_reparse_or_symlink(path) or not path.is_file():
        raise Problem("restore_release_evidence_invalid", 400)
    if os.lstat(path).st_size > MAX_EVIDENCE_BYTES:
        raise Problem("restore_release_evidence_invalid", 400)
    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise Problem("restore_release_evidence_invalid", 400) from None
    return evidence


# ------------------------------------------------------ identity/authority
def _hash_token(token):
    # Same construction as hivemind.api.hash_token; duplicated here so this
    # offline tool never imports the API/service stack. Used in memory only:
    # the raw token is never printed, logged, audited, or persisted.
    return hashlib.sha256(token.encode()).hexdigest()


def _load_credential_file(path):
    """Load an owner-format credential file (``{"token", "project_id",
    "principal_id"}``). Bounded, reparse-safe; returns the parsed dict."""
    path = Path(path)
    _check_raw_components(path)
    if _is_reparse_or_symlink(path) or not path.is_file():
        raise Problem("restore_release_identity_invalid", 403)
    if os.lstat(path).st_size > MAX_CREDENTIAL_BYTES:
        raise Problem("restore_release_identity_invalid", 403)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise Problem("restore_release_identity_invalid", 403) from None
    if not isinstance(data, dict) or not isinstance(data.get("token"), str) \
            or not data["token"]:
        raise Problem("restore_release_identity_invalid", 403)
    for key in ("project_id", "principal_id"):
        if key in data and not isinstance(data[key], str):
            raise Problem("restore_release_identity_invalid", 403)
    return data


def _verify_identity(store_dir, evidence, evidence_id, credential,
                     verify_identity):
    """Verify the CURRENT operator identity/authority against the restored DB.

    The evidence strings are self-declared and prove nothing. Authority comes
    from the CURRENT ``principals``/``projects`` tables of the restored
    database (matched by token hash, never by name) and/or from a trusted
    ``verify_identity`` callable supplied by the embedding parent. Returns the
    verified principal dict (or None when only a verifier was used).
    """
    if credential is None and verify_identity is None:
        raise Problem("restore_release_identity_required", 403)
    principal = None
    if credential is not None:
        token_hash = _hash_token(credential["token"])  # memory only
        db_path = Path(store_dir) / "canonical.sqlite"
        connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True,
                                     timeout=10)
        try:
            connection.execute("PRAGMA busy_timeout=5000")
            row = connection.execute(
                "SELECT id, role, project, agent, coordinator, "
                "capture_authority FROM principals WHERE token_hash=?",
                (token_hash,)).fetchone()
            if row is None:
                # Revoked, rotated, or foreign credential: no current binding.
                raise Problem("restore_release_identity_invalid", 403)
            principal = {"id": row[0], "role": row[1], "project": row[2],
                         "agent": row[3], "coordinator": bool(row[4]),
                         "capture_authority": bool(row[5])}
            if credential.get("principal_id") \
                    and credential["principal_id"] != principal["id"]:
                raise Problem("restore_release_identity_invalid", 403)
            if credential.get("project_id") \
                    and credential["project_id"] != principal["project"]:
                raise Problem("restore_release_identity_invalid", 403)
            if principal["role"] != REQUIRED_ROLE:
                raise Problem("restore_release_identity_forbidden", 403)
            bound = connection.execute(
                "SELECT 1 FROM projects WHERE id=?",
                (principal["project"],)).fetchone()
            if bound is None:
                # A principal whose project binding is gone has no authority.
                raise Problem("restore_release_identity_invalid", 403)
        finally:
            connection.close()
        # The typed evidence must name the VERIFIED principal, not an
        # arbitrary or forged operator string.
        if evidence["operator"] != principal["id"]:
            raise Problem("restore_release_identity_mismatch", 403)
    if verify_identity is not None:
        context = {"store": str(store_dir),
                   "snapshot_id": evidence["snapshot_id"],
                   "manifest_digest": evidence["manifest_digest"],
                   "operator": evidence["operator"],
                   "authority": evidence["authority"],
                   "evidence_digest": evidence_id,
                   "principal": principal}
        try:
            verified = bool(verify_identity(context))
        except Exception:
            verified = False  # a verifier failure is a refusal, never a pass
        if not verified:
            raise Problem("restore_release_identity_unverified", 403)
    return principal


# -------------------------------------------------------------- path safety
def _check_store_path(store):
    """Resolve the restored store after rejecting raw reparse components."""
    store = Path(store)
    _check_raw_components(store)
    if _is_reparse_or_symlink(store):
        raise Problem("restore_release_path_unsafe", 409)
    try:
        resolved = store.resolve(strict=True)
    except (OSError, RuntimeError):
        raise Problem("restore_hold_not_active", 409) from None
    if not resolved.is_dir() or not (resolved / "canonical.sqlite").is_file():
        raise Problem("restore_hold_not_active", 409)
    home = Path.home().resolve()
    if resolved == home:
        raise Problem("restore_release_path_unsafe", 403)
    for root in _system_roots():
        if resolved == root or root in resolved.parents:
            raise Problem("restore_release_path_unsafe", 403)
    return resolved


class _ReleaseLock:
    """Exclusive guard bound to the restored store directory. Bounded wait."""

    def __init__(self, store_dir):
        self.path = Path(store_dir) / RELEASE_LOCK_NAME
        self.fd = None

    def __enter__(self):
        deadline = time.monotonic() + RELEASE_LOCK_WAIT_SECONDS
        while True:
            try:
                self.fd = os.open(str(self.path),
                                  os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(self.fd, canonical(
                    {"pid": os.getpid(), "at": now()}).encode())
                return self
            except FileExistsError:
                if time.monotonic() > deadline:
                    raise Problem("restore_release_store_busy", 409) from None
                time.sleep(0.05)

    def __exit__(self, *exc_info):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
            try:
                self.path.unlink()
            except OSError:
                pass
        return False


# ----------------------------------------------------------- durable files
def _write_atomic(path, payload):
    """Durable atomic write: temp file in the same directory + rename + fsync."""
    path = Path(path)
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    try:
        with open(temporary, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:  # directory fsync is best-effort on Windows
            directory_fd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
    finally:
        temporary.unlink(missing_ok=True)


def _audit(store_dir, entry):
    """Append one audit record. The audit log is evidence: never truncated."""
    line = (canonical({**entry, "at": now()}) + "\n").encode("utf-8")
    audit_path = Path(store_dir) / RELEASE_AUDIT_NAME
    with open(audit_path, "ab") as stream:
        stream.write(line)
        stream.flush()
        os.fsync(stream.fileno())


def _read_json_file(path, what, *, required):
    path = Path(path)
    try:
        info = os.lstat(path)
    except OSError:
        if required:
            raise Problem("restore_hold_not_active", 409) from None
        return None
    if _is_reparse_or_symlink(path) or info.st_size > MAX_MARKER_BYTES:
        raise Problem("restore_hold_marker_corrupt", 409)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise Problem("restore_hold_marker_corrupt", 409) from None
    if not isinstance(value, dict):
        raise Problem("restore_hold_marker_corrupt", 409)
    return value


def _read_sidecar(store_dir, *, required=True):
    return _read_json_file(Path(store_dir) / RESTORE_HOLD_SIDECAR, "sidecar",
                           required=required)


def _read_intent(store_dir):
    return _read_json_file(Path(store_dir) / RESTORE_RELEASE_INTENT, "intent",
                           required=False)


def _stage_intent(store_dir, evidence, evidence_id):
    intent = {"release_intent": True, "version": 1,
              "evidence_digest": evidence_id,
              "snapshot_id": evidence["snapshot_id"],
              "manifest_digest": evidence["manifest_digest"],
              "operator": evidence["operator"],
              "authority": evidence["authority"],
              "store": str(Path(store_dir)),
              "started_at": now(),
              "phases": ["verify", "stage", "mutate", "sidecar", "audit"]}
    _write_atomic(Path(store_dir) / RESTORE_RELEASE_INTENT,
                  canonical(intent).encode("utf-8"))
    return intent


def _clear_intent(store_dir):
    try:
        (Path(store_dir) / RESTORE_RELEASE_INTENT).unlink()
    except FileNotFoundError:
        pass


def _remove_sidecar(store_dir):
    try:
        (Path(store_dir) / RESTORE_HOLD_SIDECAR).unlink()
    except FileNotFoundError:
        pass  # idempotent: a prior attempt may already have removed it


# ------------------------------------------------------------- verification
def _match_evidence(marker, evidence, source):
    if marker.get("snapshot_id") != evidence["snapshot_id"] \
            or marker.get("manifest_digest") != evidence["manifest_digest"]:
        raise Problem("restore_hold_mismatch", 409)
    return marker


def _verify_markers(store_dir, evidence):
    """Read-only fail-closed verification of the exact hold on this store.

    Returns ``(marker, sidecar)``. Checks the durable marker matches the
    evidence AND the sidecar, the database is intact, sanitization left no
    resurrected authority, and epochs stayed monotonic.
    """
    sidecar = _read_sidecar(store_dir, required=True)
    if sidecar.get("restore_hold") is not True:
        raise Problem("restore_hold_marker_corrupt", 409)
    _match_evidence(sidecar, evidence, "sidecar")
    db_path = Path(store_dir) / "canonical.sqlite"
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    try:
        connection.execute("PRAGMA busy_timeout=5000")
        if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise Problem("restore_hold_marker_corrupt", 409)
        row = connection.execute(
            "SELECT value FROM meta WHERE key=?", (RESTORE_HOLD_KEY,)).fetchone()
        if row is None:
            # A sidecar whose meta marker is gone is an ORPHAN: a prior partial
            # release outside this helper. Refuse; never silently clean it up.
            raise Problem("restore_release_orphaned", 409)
        if len(row[0]) > MAX_MARKER_BYTES:
            raise Problem("restore_hold_marker_corrupt", 409)
        try:
            marker = json.loads(row[0])
        except json.JSONDecodeError:
            raise Problem("restore_hold_marker_corrupt", 409) from None
        if not isinstance(marker, dict):
            raise Problem("restore_hold_marker_corrupt", 409)
        _match_evidence(marker, evidence, "meta")
        # Sanitization residue: nothing the restore invalidated may be back.
        active = connection.execute(
            "SELECT count(*) FROM resources WHERE kind='nexus_lease' AND "
            "json_extract(body,'$.state')='active'").fetchone()[0]
        leased = connection.execute(
            "SELECT count(*) FROM resources WHERE kind='message' AND "
            "json_extract(body,'$.state')='delivered' AND "
            "json_extract(body,'$.claim_until') > 0").fetchone()[0]
        if active or leased:
            raise Problem("restore_sanitization_residue", 409)
        # Epoch monotonicity: every project epoch must be strictly past any
        # snapshotted coordinator epoch; a regression means tampering.
        lease_epochs = {project: int(epoch or 0) for project, epoch in
                        connection.execute(
                            "SELECT project, MAX(CAST(json_extract(body,"
                            "'$.coordinator_epoch') AS INTEGER)) FROM resources "
                            "WHERE kind='nexus_lease' GROUP BY project")}
        for project, epoch in connection.execute(
                "SELECT id, epoch FROM projects").fetchall():
            if int(epoch) <= lease_epochs.get(project, 0):
                raise Problem("restore_epoch_regression", 409)
        return marker, sidecar
    finally:
        connection.close()


def _apply_marker_removal(store_dir, evidence):
    """Delete the meta marker in ONE immediate transaction, re-checking first."""
    db_path = Path(store_dir) / "canonical.sqlite"
    connection = sqlite3.connect(str(db_path), timeout=10)
    try:
        connection.execute(f"PRAGMA busy_timeout={MUTATION_BUSY_TIMEOUT_MS}")
        try:
            connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            # A live writer holds this store: refuse rather than mutate under it.
            raise Problem("restore_release_store_busy", 409) from None
        try:
            row = connection.execute(
                "SELECT value FROM meta WHERE key=?", (RESTORE_HOLD_KEY,)).fetchone()
            if row is None:
                raise Problem("restore_hold_not_active", 409)
            try:
                marker = json.loads(row[0])
            except json.JSONDecodeError:
                raise Problem("restore_hold_marker_corrupt", 409) from None
            if not isinstance(marker, dict):
                raise Problem("restore_hold_marker_corrupt", 409)
            _match_evidence(marker, evidence, "meta")
            connection.execute("DELETE FROM meta WHERE key=?", (RESTORE_HOLD_KEY,))
            connection.execute("COMMIT")
        except BaseException:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()


# ------------------------------------------------------------------- release
def _report(store_dir, evidence, *, resumed, principal=None, extra=None):
    report = {"restore_hold": "released",
              "snapshot_id": evidence["snapshot_id"],
              "manifest_digest": evidence["manifest_digest"],
              "operator": evidence["operator"],
              "principal": principal["id"] if principal else None,
              "role": principal["role"] if principal else None,
              "store": str(store_dir),
              "resumed": resumed,
              "audit": str(Path(store_dir) / RELEASE_AUDIT_NAME),
              "note": "The hold is released on THIS restored copy only. This "
                      "grants NO fresh Nexus/delivery authority: re-verify "
                      "identity and acquire a fresh Nexus lease (strictly "
                      "greater epoch) before serving. The operator "
                      "acknowledgement was recorded, not treated as proof of "
                      "quiescence; no remote authority was acquired and no "
                      "model was run."}
    if extra:
        report.update(extra)
    return report


def _release_locked(store_dir, evidence, evidence_id, credential,
                    verify_identity, isolation_check):
    marker, sidecar = _verify_markers(store_dir, evidence)
    # Verified CURRENT identity/authority from the restored DB and/or a trusted
    # parent verifier — before anything is staged or mutated.
    principal = _verify_identity(store_dir, evidence, evidence_id, credential,
                                 verify_identity)
    if isolation_check is not None:
        # Fresh local isolation verification (e.g. sandbox/namespace health),
        # configured by the caller; must pass for THIS store path right now.
        if not isolation_check(str(store_dir)):
            raise Problem("isolation_unverified", 409)
    _stage_intent(store_dir, evidence, evidence_id)
    _audit(store_dir, {"event": "release_intent_staged",
                       "evidence_digest": evidence_id,
                       "operator": evidence["operator"],
                       "principal": principal["id"] if principal else None,
                       "role": principal["role"] if principal else None,
                       "authority": evidence["authority"],
                       "acknowledged_recorded_not_proof":
                           evidence["acknowledged"]})
    try:
        _apply_marker_removal(store_dir, evidence)
        _remove_sidecar(store_dir)
    except Problem as exc:
        _audit(store_dir, {"event": "release_failed",
                           "evidence_digest": evidence_id, "code": exc.code})
        raise
    except (sqlite3.Error, OSError) as exc:
        _audit(store_dir, {"event": "release_failed",
                           "evidence_digest": evidence_id,
                           "code": "restore_release_failed",
                           "error": type(exc).__name__})
        raise Problem("restore_release_failed", 409) from None
    _audit(store_dir, {"event": "release_completed",
                       "evidence_digest": evidence_id,
                       "manifest_digest": evidence["manifest_digest"]})
    _clear_intent(store_dir)
    return _report(store_dir, evidence, resumed=False, principal=principal)


def _resume_locked(store_dir, evidence, evidence_id, intent, credential,
                   verify_identity, isolation_check):
    """Idempotent crash recovery, bound to the SAME evidence digest."""
    if intent.get("release_intent") is not True \
            or intent.get("evidence_digest") != evidence_id:
        raise Problem("restore_release_intent_mismatch", 409)
    # Recovery is also a release action: verified CURRENT identity/authority
    # is required for every resume branch, even the no-mutation ones.
    principal = _verify_identity(store_dir, evidence, evidence_id, credential,
                                 verify_identity)
    _audit(store_dir, {"event": "release_resume_started",
                       "evidence_digest": evidence_id,
                       "principal": principal["id"] if principal else None})
    db_path = Path(store_dir) / "canonical.sqlite"
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    try:
        marker_present = connection.execute(
            "SELECT 1 FROM meta WHERE key=?", (RESTORE_HOLD_KEY,)).fetchone() \
            is not None
    finally:
        connection.close()
    sidecar = _read_sidecar(store_dir, required=False)
    if sidecar is not None:
        _match_evidence(sidecar, evidence, "sidecar")
    if marker_present:
        # Crashed before/during mutation: full verification, then finish.
        _verify_markers(store_dir, evidence)
        if isolation_check is not None and not isolation_check(str(store_dir)):
            raise Problem("isolation_unverified", 409)
        try:
            _apply_marker_removal(store_dir, evidence)
            _remove_sidecar(store_dir)
        except Problem as exc:
            _audit(store_dir, {"event": "release_failed",
                               "evidence_digest": evidence_id, "code": exc.code})
            raise
        except (sqlite3.Error, OSError) as exc:
            _audit(store_dir, {"event": "release_failed",
                               "evidence_digest": evidence_id,
                               "code": "restore_release_failed",
                               "error": type(exc).__name__})
            raise Problem("restore_release_failed", 409) from None
        _audit(store_dir, {"event": "release_completed",
                           "evidence_digest": evidence_id, "resumed": True})
        _clear_intent(store_dir)
        return _report(store_dir, evidence, resumed=True, principal=principal)
    if sidecar is not None:
        # Crashed after the marker was deleted but before sidecar removal.
        _remove_sidecar(store_dir)
        _audit(store_dir, {"event": "release_completed",
                           "evidence_digest": evidence_id, "resumed": True})
        _clear_intent(store_dir)
        return _report(store_dir, evidence, resumed=True, principal=principal,
                       extra={"crash_state": "marker_removed_sidecar_present"})
    # Both markers already gone: a previous attempt finished. Honest, no-op.
    _audit(store_dir, {"event": "release_already_completed",
                       "evidence_digest": evidence_id})
    _clear_intent(store_dir)
    report = _report(store_dir, evidence, resumed=True, principal=principal,
                     extra={"crash_state": "already_released"})
    report["restore_hold"] = "already_released"
    return report


def release_restore_hold(store, evidence, *, identity=None,
                         verify_identity=None, isolation_check=None,
                         resume=False):
    """Release the durable restore hold on a restored store. OFFLINE ONLY.

    ``store`` is the exact restored store directory (containing
    ``canonical.sqlite`` and the ``restore-hold.json`` sidecar). ``evidence``
    is the typed approval dict (see :func:`validate_evidence`). Exactly one or
    both of the identity proofs is REQUIRED (else
    ``restore_release_identity_required``):

    * ``identity``: path to an owner-format credential file
      (``{"token", "project_id", "principal_id"}``). The token is hashed in
      memory (never printed/persisted/audited) and matched against the CURRENT
      ``principals`` table of the restored database; the principal must have
      the exact ``admin`` role, a live project binding, and must equal the
      evidence ``operator``.
    * ``verify_identity``: a trusted callable supplied by the embedding
      parent, called with a context carrying the hold's CURRENT
      ``snapshot_id``/``manifest_digest``, the evidence digest, the declared
      operator/authority, and the verified principal (if any); it must return
      truthy.

    ``isolation_check`` is an optional callable receiving the store path that
    must return truthy — fresh local isolation verification where the caller
    has it configured. With ``resume=True`` a leftover staged intent from a
    crashed attempt is completed ONLY when its evidence digest matches.

    Offline exclusion is the caller's duty: the lock and busy-timeout make a
    live writer FAIL, but writer-absence is a local heuristic, NOT proven
    quiescence. Never auto-releases, never treats a Boolean as proof of
    quiescence, never acquires remote/live authority, never runs a model.
    """
    store_dir = _check_store_path(store)
    evidence = validate_evidence(evidence, store_dir=store_dir)
    evidence_id = evidence_digest(evidence)
    credential = _load_credential_file(identity) if identity is not None else None
    with _ReleaseLock(store_dir):
        intent = _read_intent(store_dir)
        if intent is not None:
            if not resume:
                # Refuse the orphan state by default; recovery is explicit.
                raise Problem("restore_release_intent_present", 409)
            return _resume_locked(store_dir, evidence, evidence_id, intent,
                                  credential, verify_identity, isolation_check)
        return _release_locked(store_dir, evidence, evidence_id, credential,
                               verify_identity, isolation_check)


def resume_release(store, evidence, *, identity=None, verify_identity=None,
                   isolation_check=None):
    """Explicit idempotent recovery of a crashed release (same evidence only)."""
    return release_restore_hold(store, evidence, identity=identity,
                                verify_identity=verify_identity,
                                isolation_check=isolation_check, resume=True)
