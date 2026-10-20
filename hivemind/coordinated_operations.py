"""Coordinated backup/restore/retention across the canonical DB, Git, and journals.

Corrected hardening contract (supersedes any earlier behavior):

* **Every SQLite source — the canonical DB AND every private journal — is captured
  with the SQLite online backup API** from a read-only connection. A raw file copy
  of a WAL-mode database silently loses committed data still in the ``-wal`` file;
  ``shutil.copyfile`` is never used for a database here.
* **Coherent capture boundary by revalidation, not by a Boolean.** There is no
  cross-process writer fence available offline, so this module implements the
  stable-fingerprint protocol: every source is fingerprinted BEFORE capture
  (SQLite: ``PRAGMA data_version`` + page/freelist counters on the same read-only
  connection that performs the backup; Git: the full ref map + HEAD; files:
  lstat size/mtime + SHA-256), captured, then fingerprinted AGAIN before the
  manifest is finalized and the snapshot promoted. Any changed source refuses the
  entire snapshot. A non-quiescent journal (unknown or in-flight durable effects)
  refuses the snapshot outright — there is NO user Boolean that can make unknown
  effects safe, and nothing here resumes them. Because each plane is captured
  independently, the snapshot is a *revalidated consistent capture*, not a
  claim of a cross-plane atomic point in time; the manifest records the exact
  pre/post fingerprints as durable source evidence.
* **Reparse safety on raw paths.** Every source, the managed root, destinations,
  and every path derived from a manifest is checked component-by-component on the
  RAW (unresolved) path using the same technique as
  ``hivemind.identity_attestation`` (``os.path.islink`` plus the Windows
  ``FILE_ATTRIBUTE_REPARSE_POINT`` attribute), so junctions/reparse points that
  ``Path.is_symlink`` misses are rejected before any resolve/follow.
* **Manifests are untrusted input.** ``load_manifest`` re-checks the sidecar
  SHA-256, recomputes the internal manifest digest (the ``digest`` field is never
  blindly trusted), and validates the schema: unique entries, permitted kinds,
  safe canonical relative names (no ``..``, absolute paths, drive letters, ADS
  ``:`` separators, or Windows reserved device names), bounded count/bytes, and
  every entry confined to the managed top-level layout.
* **Fail-closed deletion ledger.** A corrupt or unparseable ledger is a refusal,
  never an empty ledger. All root-level mutations (snapshot promotion, retention
  execution, ledger appends) are serialized through an exclusive lock file with a
  bounded wait. Deletion intent is ledgered BEFORE any removal and the outcome is
  ledgered AFTER it is verified; a removal that leaves residue is reported as
  ``failed``/unknown WITH the residue, never as success.
* **Retention never follows anything.** Candidates are strict managed names,
  re-verified against the recomputed plan digest, confirmed to be direct
  non-reparse children of the exact managed root, and re-verified against their
  planned manifest digest at apply time. ``..`` or foreign paths in a plan are
  refused, not resolved.
* **No silent collisions, no credential leakage, no broad cleanup.** Duplicate
  journal basenames, repository names, or artifact names refuse the snapshot.
  Artifacts are scanned (bounded) for credential-shaped material and refused.
  Backup-validation scratch dirs are removed only when they were created by THIS
  call (before/after set difference), never by broad pattern deletion.

Erasure honesty is unchanged: retention is filesystem unlink of managed snapshot
directories only; nothing is claimed about physical erasure, other backups, WAL
segments, a ``VACUUM`` of any live database, or remote effects.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import shutil
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from .git_operations import GitOperations
from .identity_attestation import FILE_ATTRIBUTE_REPARSE_POINT
from .recovery import looks_sensitive
from .storage import Problem, canonical, digest, now, uid

__all__ = [
    "COORDINATED_ACTIONS",
    "CoordinatedHandle",
    "CoordinatedOperations",
    "RESTORE_HOLD_CONTRACT",
    "RESTORE_HOLD_KEY",
    "RESTORE_HOLD_SIDECAR",
    "SnapshotSource",
    "install",
    "load_manifest",
]

MANIFEST_NAME = "manifest.json"
MANIFEST_DIGEST_NAME = "manifest.sha256"
LEDGER_NAME = ".deletion-ledger.json"
LOCK_NAME = ".coordinated.lock"
SNAPSHOT_KIND = "hivemind-coordinated-snapshot"
MANIFEST_VERSION = 1
MAX_SNAPSHOT_ENTRIES = 4096
MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
MAX_SNAPSHOT_TOTAL_BYTES = 4 * 1024 * 1024 * 1024
LOCK_WAIT_SECONDS = 5.0
ENTRY_KINDS = frozenset({"sqlite-backup", "git-bundle", "git-bundle-refs",
                         "journal", "artifact"})
TOP_LEVEL = re.compile(r"^(canonical\.sqlite|git|journals|artifacts)(/.*)?$")
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
RESERVED_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL", "CLOCK$"}
    | {f"COM{i}" for i in range(1, 10)} | {f"LPT{i}" for i in range(1, 10)})
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class SnapshotSource:
    """Declared inputs for one coordinated snapshot. Everything must be explicit."""

    db_path: str = ""                      # canonical SQLite database (optional)
    repositories: tuple = ()               # ({"name": ..., "repo": ...}, ...)
    journals: tuple = ()                   # private journal SQLite paths
    artifacts: tuple = ()                  # ({"name": ..., "path": ...}, ...) runtime artifacts
    exclusions: tuple = ()                 # documented exclusions (reported, not copied)


# --------------------------------------------------------------- path safety
def _is_reparse_or_symlink(path):
    """True for symlinks AND Windows reparse points (junctions) on the RAW path."""
    try:
        info = os.lstat(path)
    except OSError:
        return False  # nonexistent components are not reparse points
    if os.path.islink(path):
        return True
    attributes = getattr(info, "st_file_attributes", 0)
    return bool(attributes & FILE_ATTRIBUTE_REPARSE_POINT)


def _check_raw_components(path):
    """Reject any symlink/reparse component on the raw path BEFORE any resolve."""
    path = Path(path)
    current = Path(path.anchor) if path.anchor else Path(".")
    parts = path.parts[1:] if path.anchor else path.parts
    for part in parts:
        current = current / part
        if _is_reparse_or_symlink(current):
            raise Problem("path_reparse_component", 409)


def _safe_name(name):
    """Strict managed name: no separators, traversal, ADS, or device names."""
    if not isinstance(name, str) or not SAFE_NAME.match(name) or ".." in name \
            or name.split(".")[0].upper() in RESERVED_NAMES:
        return False
    return True


def _validate_entry_path(relative):
    """A manifest entry path: safe relative POSIX path inside the managed layout."""
    if not isinstance(relative, str) or not relative or "\\" in relative \
            or ":" in relative or relative.startswith("/") or "//" in relative:
        raise Problem("snapshot_manifest_invalid", 409)
    parts = relative.split("/")
    if any(not _safe_name(part) for part in parts) or ".." in parts:
        raise Problem("snapshot_manifest_invalid", 409)
    if not TOP_LEVEL.match(relative):
        raise Problem("snapshot_manifest_invalid", 409)
    return relative


def _system_roots():
    roots = set()
    for variable in ("SystemRoot", "WINDIR", "ProgramFiles", "ProgramFiles(x86)",
                     "PROGRAMDATA"):
        value = os.environ.get(variable)
        if value:
            try:
                roots.add(Path(value).resolve())
            except OSError:
                pass
    if Path.cwd().anchor:
        roots.add(Path(Path.cwd().anchor))
    return roots


def _check_new_destination(destination):
    """Refuse existing paths, any reparse component, system roots, drive roots."""
    destination = Path(destination)
    if destination.exists() or _is_reparse_or_symlink(destination):
        raise Problem("destination_exists", 409)
    _check_raw_components(destination)
    resolved_parent = destination.parent.resolve()
    home = Path.home().resolve()
    for root in _system_roots():
        if resolved_parent == root or root in resolved_parent.parents:
            raise Problem("destination_system_path", 403)
    if destination.parent == destination:
        raise Problem("destination_system_path", 403)
    if resolved_parent == home:
        raise Problem("destination_user_root", 403)
    return destination


def _require_source_file(path, what):
    path = Path(path)
    _check_raw_components(path)
    if _is_reparse_or_symlink(path) or not path.is_file():
        raise Problem("snapshot_source_missing", 409)
    return path


# ------------------------------------------------------------------ fingerprints
def _sqlite_connect_ro(path):
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def _sqlite_fingerprint(connection, path):
    """Durable source fingerprint from the capturing connection itself."""
    row = connection.execute("PRAGMA data_version").fetchone()
    pages = connection.execute("PRAGMA page_count").fetchone()
    freelist = connection.execute("PRAGMA freelist_count").fetchone()
    return {"path": str(path), "data_version": int(row[0]),
            "page_count": int(pages[0]), "freelist_count": int(freelist[0])}


def _sqlite_files_fingerprint(path):
    """Cross-connection change detector for a SQLite database.

    ``PRAGMA data_version`` is per-connection, so it cannot detect changes that
    happen after the capturing connection closes. The durable detector is the
    file family itself: a committed write always moves the main file (after a
    checkpoint) or the ``-wal``/``-shm`` companions (before it).
    """
    path = Path(path)
    # The ``-shm`` companion is excluded: merely opening a WAL database can
    # rewrite it, so it would false-positive as a source change.
    result = {}
    for suffix in ("", "-wal"):
        member = path.with_name(path.name + suffix)
        try:
            info = os.lstat(member)
        except OSError:
            result[suffix] = None
        else:
            result[suffix] = {"size": info.st_size, "mtime_ns": info.st_mtime_ns}
    return result


def _capture_sqlite(source, target, *, unknown_gate=False):
    """Online-backup one SQLite source; returns (entry_fields, pre, post).

    The pre/post fingerprints come from the SAME read-only connection that runs
    the backup; any concurrent writer commit bumps ``PRAGMA data_version`` and
    refuses the snapshot. ``unknown_gate`` additionally refuses when the
    operations table holds unknown or in-flight durable effects (journals).
    """
    connection = _sqlite_connect_ro(source)
    try:
        pre = {"connection": _sqlite_fingerprint(connection, source),
               "files": _sqlite_files_fingerprint(source)}
        quiescence = None
        if unknown_gate:
            try:
                row = connection.execute(
                    "SELECT SUM(state='unknown') AS u, SUM(attempting=1) AS a "
                    "FROM operations").fetchone()
                quiescence = {"journal": str(source), "unknown": int(row[0] or 0),
                              "attempting": int(row[1] or 0)}
            except sqlite3.Error:
                raise Problem("snapshot_source_unreadable", 409) from None
            if quiescence["unknown"] or quiescence["attempting"]:
                # A fence Boolean can NEVER make unknown effects safe. Hard refuse.
                raise Problem("snapshot_not_quiescent", 409)
            quiescence["quiescent"] = True
        backup = sqlite3.connect(str(target), timeout=10)
        try:
            connection.backup(backup)
        finally:
            backup.close()
        post = {"connection": _sqlite_fingerprint(connection, source),
                "files": _sqlite_files_fingerprint(source)}
        if post != pre:
            raise Problem("snapshot_source_changed", 409)
        return quiescence, pre, post
    finally:
        connection.close()


def _sqlite_revalidate(source, pre):
    """Re-check a captured SQLite source against its pre-capture fingerprint."""
    connection = _sqlite_connect_ro(source)
    try:
        current = {"connection": _sqlite_fingerprint(connection, source),
                   "files": _sqlite_files_fingerprint(source)}
    finally:
        connection.close()
    if current != pre:
        raise Problem("snapshot_source_changed", 409)


def _file_fingerprint(path):
    path = Path(path)
    info = os.lstat(path)
    return {"path": str(path), "size": info.st_size, "mtime_ns": info.st_mtime_ns,
            "sha256": _sha256(path)}


def _git_fingerprint(ops, repo):
    refs = ops._ref_map(repo)
    head = ops.spawn(repo, ["rev-parse", "--verify", "HEAD"], allowed=(0, 1, 128),
                     timeout=20)["stdout"]
    return {"repo": str(repo), "head": head, "refs": refs}


def _sha256(path):
    import hashlib
    digest_obj = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest_obj.update(chunk)
    return digest_obj.hexdigest()


def _looks_sensitive_bytes(path, *, sample_limit=1 << 18):
    """Bounded credential-shape scan of an artifact before it is copied."""
    try:
        with open(path, "rb") as stream:
            sample = stream.read(sample_limit)
    except OSError:
        raise Problem("snapshot_source_unreadable", 409) from None
    text = sample.decode("utf-8", errors="ignore")
    if looks_sensitive(text):
        return True
    try:  # structured artifacts: nested credential keys count too
        value = json.loads(text)
    except ValueError:
        return False
    return looks_sensitive(value)


def _rmtree(path):
    """rmtree clearing read-only attributes first; returns True only when gone."""
    path = Path(path)

    def onerror(function, target, _exc_info):
        try:
            os.chmod(target, 0o700)
            function(target)
        except OSError:
            pass

    # NOTE: ignore_errors=True would suppress onerror entirely; keep it off.
    try:
        shutil.rmtree(path, onerror=onerror)
    except OSError:
        pass
    return not path.exists()


# --------------------------------------------------------------------- ledger
class _RootLock:
    """Exclusive cross-call serialization for one managed root. Bounded wait."""

    def __init__(self, root):
        self.path = Path(root) / LOCK_NAME
        self.fd = None

    def __enter__(self):
        deadline = time.monotonic() + LOCK_WAIT_SECONDS
        while True:
            try:
                self.fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(self.fd, canonical({"pid": os.getpid(), "at": now()}).encode())
                return self
            except FileExistsError:
                if time.monotonic() > deadline:
                    raise Problem("coordinated_root_busy", 409) from None
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


def _ledger_read(root):
    """Fail-CLOSED ledger read: corruption is a refusal, never an empty ledger."""
    path = Path(root) / LEDGER_NAME
    if _is_reparse_or_symlink(path):
        raise Problem("deletion_ledger_corrupt", 409)
    if not path.exists():
        return []
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise Problem("deletion_ledger_corrupt", 409) from None
    if not isinstance(document, dict) or document.get("version") != 1 \
            or not isinstance(document.get("deletions"), list):
        raise Problem("deletion_ledger_corrupt", 409)
    for entry in document["deletions"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str) \
                or not isinstance(entry.get("state"), str):
            raise Problem("deletion_ledger_corrupt", 409)
    return list(document["deletions"])


def _ledger_append(root, entry):
    """Append one entry. Caller must hold the root lock."""
    entries = _ledger_read(root)
    entries.append(dict(entry))
    path = Path(root) / LEDGER_NAME
    scratch = path.with_name(path.name + ".tmp")
    scratch.write_text(canonical({"version": 1, "deletions": entries}), encoding="utf-8")
    os.replace(scratch, path)


def _ledgered(root):
    """Tombstoned (name, snapshot_id) sets. Fail-closed: ANY ledgered intent —
    including an incomplete "deleting" or a "failed" removal — blocks
    resurrection until an operator reconciles the ledger."""
    names, snapshot_ids = set(), set()
    for entry in _ledger_read(root):
        names.add(entry.get("name"))
        snapshot_ids.add(entry.get("snapshot_id"))
    return names, snapshot_ids


# ---------------------------------------------------- restore sanitization
RESTORE_HOLD_KEY = "restore_hold"
RESTORE_HOLD_SIDECAR = "restore-hold.json"
HIVEMIND_TABLES = frozenset({"projects", "resources", "versions"})
RESTORE_HOLD_CONTRACT = {
    "marker": f"meta table row key '{RESTORE_HOLD_KEY}' (existing key/value meta "
              "schema) plus a new-only sidecar restore-hold.json at the restore root",
    "meaning": "The restored canonical copy came from a snapshot: its Nexus lease, "
               "delivery-lease, and control authority was invalidated at restore time.",
    "startup_guard": "The parent's Store/Service startup must refuse to serve a "
                     "canonical store whose meta row 'restore_hold' is present.",
    "release": "Typed MANUAL release only, after fresh identity/authority "
               "verification (new Nexus lease acquisition re-fences Git and gets a "
               "strictly greater epoch): the parent's offline command deletes the "
               "meta row and sidecar. Never auto-released, never released by restore.",
}


def _sanitize_restored_canonical(db_path, *, snapshot_id, manifest_digest,
                                 backup_sha256):
    """Invalidate resurrected authority in a RESTORED canonical copy. Offline only.

    A byte-identical restore of the canonical DB would resurrect the live Nexus
    lease (its expiry may be in the future), active delivery-lease ownership (old
    receipts could settle), and queued/acknowledged controls — even though the Git
    fence is never resurrected. After the backup hash is verified and BEFORE
    promotion, this runs ONE transaction on the restored copy that:

    * expires every ``state='active'`` ``nexus_lease`` (state ->
      ``restored_invalidated``, ``expires_at`` -> 0) and bumps every project's
      monotonic ``epoch`` past the snapshot (never reset, never decremented);
    * expires active delivery-lease ownership on delivered messages
      (``claim_until`` -> 0 with a bounded ``restore_lease_expired`` history
      entry), so old receipts can no longer settle;
    * expires queued/acknowledged controls and marks ``applying`` controls
      ``outcome_unknown`` (mirroring ``Service.retire_controls`` semantics) —
      unknown is NEVER marked completed;
    * writes the durable ``restore_hold`` meta marker (existing key/value meta
      schema) for the parent's startup guard.

    Effect/task execution evidence and UNKNOWN holds are preserved untouched.
    Generic SQLite files without the Hivemind schema are supported but reported
    ``applicable: False`` — no universal sanitization is claimed. The manifest
    keeps the ORIGINAL verified backup hash; the sanitized hash differs and is
    reported as such.
    """
    report = {"applicable": False, "hivemind_schema": False,
              "nexus_leases_invalidated": 0, "delivery_leases_expired": 0,
              "controls_expired": 0, "controls_outcome_unknown": 0,
              "epochs": {}, "restore_hold": None, "integrity_check": None,
              "backup_sha256": backup_sha256, "sanitized_sha256": None}
    connection = sqlite3.connect(str(db_path), timeout=10)
    try:
        connection.execute("PRAGMA busy_timeout=5000")
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not HIVEMIND_TABLES <= tables:
            report["reason"] = "not_a_hivemind_canonical_store"
            report["integrity_check"] = connection.execute(
                "PRAGMA quick_check").fetchone()[0]
            return report
        report["hivemind_schema"] = True
        connection.execute("BEGIN IMMEDIATE")
        try:
            stamp = now()
            for row in connection.execute(
                    "SELECT id, project, body FROM resources "
                    "WHERE kind='nexus_lease'").fetchall():
                body = json.loads(row[2])
                if body.get("state") != "active":
                    continue
                old_epoch = int(body.get("coordinator_epoch") or 0)
                body["state"] = "restored_invalidated"
                body["expires_at"] = 0
                body["restored_invalidated_at"] = stamp
                connection.execute("UPDATE resources SET body=? WHERE id=?",
                                   (canonical(body), row[0]))
                report["nexus_leases_invalidated"] += 1
                previous = report["epochs"].get(row[1], {}).get("snapshot_epoch", 0)
                report["epochs"][row[1]] = {"snapshot_epoch": max(previous, old_epoch)}
            # Monotonic epoch preserved: strictly greater than any snapshot epoch.
            for row in connection.execute("SELECT id, epoch FROM projects").fetchall():
                snapshot_epoch = max(
                    int(row[1]), report["epochs"].get(row[0], {}).get(
                        "snapshot_epoch", 0))
                restored_epoch = snapshot_epoch + 1
                connection.execute("UPDATE projects SET epoch=? WHERE id=?",
                                   (restored_epoch, row[0]))
                report["epochs"][row[0]] = {"snapshot_epoch": int(row[1]),
                                            "restored_epoch": restored_epoch}
            for row in connection.execute(
                    "SELECT id, body FROM resources WHERE kind='message'").fetchall():
                body = json.loads(row[1])
                if body.get("state") != "delivered" or not body.get("claim_until"):
                    continue
                body["claim_until"] = 0  # lease ownership expired: old receipts lose
                history = list(body.get("delivery_history") or [])
                history.append({"action": "restore_lease_expired", "at": stamp,
                                "delivery_id": body.get("delivery_id"),
                                "run_id": body.get("claim_run_id")})
                body["delivery_history"] = history[-50:]
                connection.execute("UPDATE resources SET body=? WHERE id=?",
                                   (canonical(body), row[0]))
                report["delivery_leases_expired"] += 1
            for row in connection.execute(
                    "SELECT id, body FROM resources WHERE kind='control'").fetchall():
                body = json.loads(row[1])
                state = body.get("state")
                if state not in ("queued", "acknowledged", "applying"):
                    continue
                body["state"] = "outcome_unknown" if state == "applying" else "expired"
                body["restored_invalidated_at"] = stamp
                connection.execute("UPDATE resources SET body=? WHERE id=?",
                                   (canonical(body), row[0]))
                key = "controls_outcome_unknown" if state == "applying" \
                    else "controls_expired"
                report[key] += 1
            hold = {"snapshot_id": snapshot_id, "manifest_digest": manifest_digest,
                    "restored_at": stamp, "release": RESTORE_HOLD_CONTRACT["release"],
                    "invalidated": {"nexus_leases": report["nexus_leases_invalidated"],
                                    "delivery_leases": report["delivery_leases_expired"],
                                    "controls_expired": report["controls_expired"],
                                    "controls_outcome_unknown":
                                        report["controls_outcome_unknown"]}}
            connection.execute(
                "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT)")
            connection.execute("INSERT OR REPLACE INTO meta VALUES(?, ?)",
                               (RESTORE_HOLD_KEY, canonical(hold)))
            connection.execute("COMMIT")
            # The canonical store is WAL-mode: without a truncating checkpoint the
            # sanitization would sit in the copy's -wal and be abandoned at
            # promotion (only the main file is restored). Checkpoint, then close.
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        connection.close()
        # Post-checkpoint companions of OUR staged copy (never a live store): remove
        # any stragglers so the promoted restore is exactly the checkpointed file.
        for suffix in ("-wal", "-shm"):
            Path(str(db_path) + suffix).unlink(missing_ok=True)
        connection = sqlite3.connect(str(db_path), timeout=10)  # reopened, checkpointed
        report["applicable"] = True
        report["restore_hold"] = hold
        # Read-only verification pass against the actual schema (no Store init:
        # its migrations would write). Fail closed on any residue.
        verify = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
        try:
            active = verify.execute(
                "SELECT count(*) FROM resources WHERE kind='nexus_lease' AND "
                "json_extract(body,'$.state')='active'").fetchone()[0]
            leased = verify.execute(
                "SELECT count(*) FROM resources WHERE kind='message' AND "
                "json_extract(body,'$.state')='delivered' AND "
                "json_extract(body,'$.claim_until') > 0").fetchone()[0]
            marker = verify.execute(
                "SELECT value FROM meta WHERE key=?", (RESTORE_HOLD_KEY,)).fetchone()
            check = verify.execute("PRAGMA quick_check").fetchone()[0]
        finally:
            verify.close()
        if active or leased or marker is None or check != "ok":
            raise Problem("restore_sanitization_failed", 409)
        report["integrity_check"] = check
        report["sanitized_sha256"] = _sha256(db_path)
        return report
    except Problem:
        raise
    except (sqlite3.Error, ValueError, KeyError):
        raise Problem("restore_sanitization_failed", 409) from None
    finally:
        connection.close()



# -------------------------------------------------------------------- manifest
def _manifest_digest(manifest):
    """Recomputed internal digest; the stored field is never blindly trusted."""
    return digest({key: manifest[key] for key in manifest if key != "digest"})


def _validate_manifest_schema(manifest):
    entries = manifest.get("entries")
    if not isinstance(entries, list) or len(entries) > MAX_SNAPSHOT_ENTRIES:
        raise Problem("snapshot_manifest_invalid", 409)
    seen, total = set(), 0
    for entry in entries:
        if not isinstance(entry, dict):
            raise Problem("snapshot_manifest_invalid", 409)
        relative = _validate_entry_path(entry.get("path"))
        if relative in seen:
            raise Problem("snapshot_manifest_invalid", 409)
        seen.add(relative)
        if entry.get("kind") not in ENTRY_KINDS \
                or not _HEX64.match(str(entry.get("sha256"))) \
                or not isinstance(entry.get("size"), int) or entry["size"] < 0:
            raise Problem("snapshot_manifest_invalid", 409)
        total += entry["size"]
        if total > MAX_SNAPSHOT_TOTAL_BYTES:
            raise Problem("snapshot_manifest_invalid", 409)
    for key in ("snapshot_id", "name", "created_at"):
        if not isinstance(manifest.get(key), str) or not manifest[key]:
            raise Problem("snapshot_manifest_invalid", 409)
    return entries


def load_manifest(snapshot_dir):
    """Read, hash-verify, digest-recompute, and schema-validate a manifest."""
    snapshot_dir = Path(snapshot_dir)
    _check_raw_components(snapshot_dir)
    manifest_path = snapshot_dir / MANIFEST_NAME
    digest_path = snapshot_dir / MANIFEST_DIGEST_NAME
    if _is_reparse_or_symlink(manifest_path) or _is_reparse_or_symlink(digest_path) \
            or not manifest_path.is_file() or not digest_path.is_file():
        raise Problem("snapshot_manifest_missing", 409)
    import hashlib
    raw = manifest_path.read_bytes()
    if len(raw) > 4 * 1024 * 1024:
        raise Problem("snapshot_manifest_invalid", 409)
    recorded = digest_path.read_text(encoding="utf-8").strip()
    if hashlib.sha256(raw).hexdigest() != recorded:
        raise Problem("snapshot_manifest_tampered", 409)
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise Problem("snapshot_manifest_invalid", 409) from None
    if not isinstance(manifest, dict) or manifest.get("kind") != SNAPSHOT_KIND \
            or manifest.get("version") != MANIFEST_VERSION:
        raise Problem("snapshot_manifest_invalid", 409)
    if _manifest_digest(manifest) != manifest.get("digest"):
        raise Problem("snapshot_manifest_tampered", 409)
    _validate_manifest_schema(manifest)
    if manifest.get("tombstones"):
        raise Problem("snapshot_tombstoned", 409)
    return manifest


# ------------------------------------------------------------------- operations
class CoordinatedOperations:
    """Coordinates snapshot/restore/retention over DB + Git + journals + artifacts.

    ``root`` is the managed snapshots root. Its raw path must contain no
    reparse/symlink components. All root-level mutations are serialized through
    an exclusive lock file with a bounded wait.
    """

    def __init__(self, root):
        root = Path(root)
        _check_raw_components(root.parent if not root.exists() else root)
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        if _is_reparse_or_symlink(self.root):
            raise Problem("path_reparse_component", 409)
        self.canonical_root = str(self.root.resolve())

    def _lock(self):
        return _RootLock(self.root)

    # -------------------------------------------------------------- snapshot
    def create_snapshot(self, source, destination=None, *, name=None,
                        capture_observer=None):
        """Create one revalidated, manifest-hashed coordinated snapshot.

        Refuses while any declared journal holds unknown or in-flight durable
        effects (no override exists), refuses duplicate source names, refuses
        credential-shaped artifacts, and refuses the ENTIRE snapshot when any
        source fingerprint changes between capture and promotion revalidation.
        ``capture_observer(manifest_dir)`` is a test seam invoked after capture
        and before revalidation; it must never be wired in production.
        """
        if not isinstance(source, SnapshotSource):
            raise TypeError("source must be a SnapshotSource")
        total_sources = len(source.journals) + len(source.artifacts) \
            + len(source.repositories) + (1 if source.db_path else 0)
        if total_sources == 0:
            raise Problem("snapshot_source_empty", 422)
        if total_sources > MAX_SNAPSHOT_ENTRIES:
            raise Problem("snapshot_source_too_large", 422)
        # Uniqueness: no silent overwrites inside the snapshot layout.
        repo_names = [str(item["name"]) for item in source.repositories]
        artifact_names = [str(item["name"]) for item in source.artifacts]
        journal_names = [Path(item).name for item in source.journals]
        for group in (repo_names, artifact_names, journal_names):
            if len(set(group)) != len(group) or any(not _safe_name(n) for n in group):
                raise Problem("snapshot_source_conflict", 422)
        name = name or f"snap-{secrets.token_hex(6)}"
        if not _safe_name(name):
            raise Problem("snapshot_name_invalid", 422)
        with self._lock():
            return self._create_locked(source, destination, name,
                                       capture_observer=capture_observer)

    def _create_locked(self, source, destination, name, *, capture_observer):
        destination = Path(destination) if destination else self.root / name
        _check_new_destination(destination)
        if destination.parent.resolve() == Path(self.canonical_root):
            names, _ = _ledgered(self.root)
            if destination.name in names:
                raise Problem("snapshot_name_tombstoned", 409)

        db_path = Path(source.db_path) if source.db_path else None
        if db_path is not None:
            _require_source_file(db_path, "db")
        journals = [_require_source_file(Path(item), "journal")
                    for item in source.journals]
        artifacts = [(str(item["name"]),
                      _require_source_file(Path(item["path"]), "artifact"))
                     for item in source.artifacts]
        repos = [(str(item["name"]), Path(item["repo"])) for item in source.repositories]
        for _repo_name, repo in repos:
            _check_raw_components(repo)
            if _is_reparse_or_symlink(repo) or not repo.is_dir():
                raise Problem("snapshot_source_missing", 409)
        for _artifact_name, artifact in artifacts:
            if artifact.stat().st_size > MAX_ARTIFACT_BYTES:
                raise Problem("snapshot_artifact_too_large", 422)
            if _looks_sensitive_bytes(artifact):
                raise Problem("snapshot_artifact_sensitive", 422)

        staging = destination.with_name(destination.name + f".stg-{secrets.token_hex(4)}")
        staging.mkdir(parents=True)
        ops_scratch = Path(tempfile.mkdtemp(prefix="hivemind-coordinated-ops-"))
        entries, git_reports, quiescence, fingerprints = [], [], [], {}
        total_bytes = 0
        try:
            ops = GitOperations(ops_scratch)

            def track(path):
                nonlocal total_bytes
                total_bytes += path.stat().st_size
                if total_bytes > MAX_SNAPSHOT_TOTAL_BYTES:
                    raise Problem("snapshot_too_large", 422)

            if db_path is not None:
                target = staging / "canonical.sqlite"
                _, pre, post = _capture_sqlite(db_path, target)
                fingerprints["db"] = {"pre": pre, "post": post}
                track(target)
                entries.append({"path": target.name, "kind": "sqlite-backup",
                                "source": str(db_path), "sha256": _sha256(target),
                                "size": target.stat().st_size})
            git_root = staging / "git"
            for repo_name, repo in repos:
                git_root.mkdir(exist_ok=True)
                before = set(git_root.glob("hivemind-backup-validate-*"))
                pre = _git_fingerprint(ops, repo)
                bundle = git_root / (repo_name + ".bundle")
                report = ops.backup_repository(repo, bundle)
                post = _git_fingerprint(ops, repo)
                # Remove ONLY validation scratch dirs created by THIS call.
                for residue in set(git_root.glob("hivemind-backup-validate-*")) - before:
                    if residue.is_dir():
                        _rmtree(residue)
                if post != pre:
                    raise Problem("snapshot_source_changed", 409)
                fingerprints[f"git:{repo_name}"] = {"pre": pre, "post": post}
                git_reports.append({"name": repo_name, "ok": report["ok"],
                                    "bundle_sha256": report["bundle_sha256"],
                                    "manifest_sha256": report.get("manifest_sha256"),
                                    "refs": report.get("source_refs")})
                track(bundle)
                entries.append({"path": f"git/{bundle.name}", "kind": "git-bundle",
                                "source": str(repo), "sha256": report["bundle_sha256"],
                                "size": bundle.stat().st_size})
                sidecar = bundle.with_name(bundle.name + ".refs.json")
                track(sidecar)
                entries.append({"path": f"git/{sidecar.name}", "kind": "git-bundle-refs",
                                "source": str(repo), "sha256": _sha256(sidecar),
                                "size": sidecar.stat().st_size})
            for journal in journals:
                target = staging / "journals" / journal.name
                target.parent.mkdir(exist_ok=True)
                # Online backup, never a raw copy: a live WAL journal would lose
                # committed pages with copyfile. Quiescence is gated in the same
                # read-only connection and re-checked by the fingerprint pair.
                gate, pre, post = _capture_sqlite(journal, target, unknown_gate=True)
                quiescence.append(gate)
                fingerprints[f"journal:{journal.name}"] = {"pre": pre, "post": post}
                track(target)
                entries.append({"path": f"journals/{journal.name}", "kind": "journal",
                                "source": str(journal), "sha256": _sha256(target),
                                "size": target.stat().st_size})
            artifact_fps = {}
            for artifact_name, artifact in artifacts:
                artifact_fps[artifact_name] = {"pre": _file_fingerprint(artifact)}
            for artifact_name, artifact in artifacts:
                target = staging / "artifacts" / artifact_name
                target.parent.mkdir(exist_ok=True)
                shutil.copyfile(artifact, target)
                track(target)
                entries.append({"path": f"artifacts/{artifact_name}", "kind": "artifact",
                                "source": str(artifact), "sha256": _sha256(target),
                                "size": target.stat().st_size})
            if capture_observer is not None:
                capture_observer(staging)  # test seam only
            # REVALIDATE before finalize/promotion: any changed source refuses.
            for artifact_name, artifact in artifacts:
                post = _file_fingerprint(artifact)
                artifact_fps[artifact_name]["post"] = post
                if post != artifact_fps[artifact_name]["pre"]:
                    raise Problem("snapshot_source_changed", 409)
            fingerprints["artifacts"] = artifact_fps
            if db_path is not None:
                _sqlite_revalidate(db_path, fingerprints["db"]["pre"])
            for repo_name, repo in repos:
                if _git_fingerprint(ops, repo) != fingerprints[f"git:{repo_name}"]["pre"]:
                    raise Problem("snapshot_source_changed", 409)
            for journal in journals:
                _sqlite_revalidate(journal,
                                   fingerprints[f"journal:{journal.name}"]["pre"])
            manifest = {
                "version": MANIFEST_VERSION, "kind": SNAPSHOT_KIND,
                "snapshot_id": uid("snap"), "name": destination.name,
                "created_at": now(), "entries": entries, "git": git_reports,
                "quiescence": quiescence, "source_fingerprints": fingerprints,
                "coherence": "revalidated-consistent-capture",
                "coherence_note": "Each plane captured independently and revalidated "
                                  "against its pre-capture fingerprint; NOT a "
                                  "cross-plane atomic point in time. Unknown "
                                  "effects are refused, never resumed.",
                "exclusions": list(source.exclusions),
                "tombstones": [],
                "erasure_note": "Snapshot copies are logical copies; deleting them is "
                                "filesystem unlink only and implies nothing about "
                                "physical erasure, other backups, WAL segments, or "
                                "remote effects.",
            }
            manifest["digest"] = _manifest_digest(manifest)
            import hashlib
            raw = canonical(manifest).encode()
            (staging / MANIFEST_NAME).write_bytes(raw)
            (staging / MANIFEST_DIGEST_NAME).write_text(
                hashlib.sha256(raw).hexdigest(), encoding="utf-8")
            if destination.exists() or _is_reparse_or_symlink(destination):
                raise Problem("destination_exists", 409)
            os.rename(staging, destination)  # promote
        except BaseException:
            _rmtree(staging)  # agent-created staging only
            raise
        finally:
            _rmtree(ops_scratch)  # agent-created ops journal
        manifest["path"] = str(destination)
        return manifest

    def verify_snapshot(self, snapshot_dir):
        """Hash-verify every manifest entry. Returns a report; mutates nothing."""
        report = {"snapshot": str(snapshot_dir), "ok": False, "checked": 0,
                  "mismatches": [], "missing": [], "unexpected": []}
        try:
            manifest = load_manifest(snapshot_dir)
        except Problem as error:
            report["error"] = error.code
            return report
        snapshot_dir = Path(snapshot_dir)
        allowed = {MANIFEST_NAME, MANIFEST_DIGEST_NAME}
        expected = set()
        for entry in manifest["entries"]:
            relative = entry["path"]
            expected.add(relative)
            target = snapshot_dir / relative
            if not target.is_file() or _is_reparse_or_symlink(target):
                report["missing"].append(relative)
                continue
            report["checked"] += 1
            if _sha256(target) != entry["sha256"] or \
                    target.stat().st_size != entry["size"]:
                report["mismatches"].append(relative)
        for root_dir, dirs, files in os.walk(snapshot_dir):
            for name in dirs:
                if _is_reparse_or_symlink(Path(root_dir) / name):
                    report["unexpected"].append(
                        str((Path(root_dir) / name).relative_to(snapshot_dir)) + "/")
            for name in files:
                target = Path(root_dir) / name
                relative = str(target.relative_to(snapshot_dir)).replace(os.sep, "/")
                if _is_reparse_or_symlink(target):
                    report["unexpected"].append(relative)
                elif relative not in expected and relative not in allowed:
                    report["unexpected"].append(relative)
        report["manifest_digest"] = manifest["digest"]
        report["snapshot_id"] = manifest["snapshot_id"]
        report["ok"] = not (report["missing"] or report["mismatches"]
                            or report["unexpected"])
        return report

    # --------------------------------------------------------------- restore
    def restore_snapshot(self, snapshot_dir, destination, *, approve=None):
        """Restore one verified snapshot into a NEW EMPTY operator-approved path.

        ``approve(manifest_digest, verify_report)`` must return ``True``. The
        manifest is schema-validated and fully hash-verified before anything is
        written; staging is promoted by rename and reported paths are rebased to
        the promoted destination. Offline command only.
        """
        snapshot_dir = Path(snapshot_dir)
        _check_raw_components(snapshot_dir)
        destination = _check_new_destination(destination)
        with self._lock():
            if destination.parent.resolve() == Path(self.canonical_root):
                names, snapshot_ids = _ledgered(self.root)
                if destination.name in names:
                    raise Problem("snapshot_name_tombstoned", 409)
            else:
                _, snapshot_ids = _ledgered(self.root)
            manifest = load_manifest(snapshot_dir)
            if manifest["snapshot_id"] in snapshot_ids:
                raise Problem("snapshot_deleted", 409)
            verification = self.verify_snapshot(snapshot_dir)
            if not verification["ok"]:
                raise Problem("snapshot_verification_failed", 409)
            if approve is None or approve(manifest["digest"], verification) is not True:
                raise Problem("restore_not_approved", 403)
            staging = destination.with_name(
                destination.name + f".rst-{secrets.token_hex(4)}")
            staging.mkdir(parents=True)
            ops_scratch = Path(tempfile.mkdtemp(prefix="hivemind-coordinated-ops-"))
            restored = {"db": None, "repositories": [], "journals": [], "artifacts": []}
            sanitization = None
            try:
                for entry in manifest["entries"]:
                    source_path = snapshot_dir / entry["path"]
                    if entry["kind"] == "sqlite-backup":
                        target = staging / "canonical.sqlite"
                        shutil.copyfile(source_path, target)
                        if _sha256(target) != entry["sha256"]:
                            raise Problem("snapshot_verification_failed", 409)
                        # The verified copy is byte-identical to the backup here;
                        # sanitization transforms it BEFORE promotion so a restore
                        # can never resurrect old lease/receipt authority.
                        sanitization = _sanitize_restored_canonical(
                            target, snapshot_id=manifest["snapshot_id"],
                            manifest_digest=manifest["digest"],
                            backup_sha256=entry["sha256"])
                        restored["db"] = str(target)
                    elif entry["kind"] == "git-bundle":
                        name = Path(entry["path"]).stem
                        target = staging / "git" / name
                        target.parent.mkdir(exist_ok=True)
                        ops = GitOperations(ops_scratch)
                        outcome = ops.restore_backup(source_path, target)
                        restored["repositories"].append(
                            {"name": name, "path": outcome["restored"],
                             "fsck_ok": outcome["fsck_ok"]})
                    elif entry["kind"] in ("journal", "artifact"):
                        base = "journals" if entry["kind"] == "journal" else "artifacts"
                        target = staging / base / Path(entry["path"]).name
                        target.parent.mkdir(exist_ok=True)
                        shutil.copyfile(source_path, target)
                        if _sha256(target) != entry["sha256"]:
                            raise Problem("snapshot_verification_failed", 409)
                        restored[entry["kind"] + "s"].append(str(target))
            except BaseException:
                _rmtree(staging)  # agent-created staging only
                raise
            finally:
                _rmtree(ops_scratch)
            if sanitization and sanitization.get("applicable"):
                # New-only sidecar, written at restore time (never part of a
                # snapshot): the offline bootstrap contract for the startup guard.
                (staging / RESTORE_HOLD_SIDECAR).write_text(canonical({
                    "restore_hold": True, "snapshot_id": manifest["snapshot_id"],
                    "manifest_digest": manifest["digest"], "created_at": now(),
                    "contract": RESTORE_HOLD_CONTRACT}), encoding="utf-8")
            if destination.exists() or _is_reparse_or_symlink(destination):
                _rmtree(staging)
                raise Problem("destination_exists", 409)
            os.rename(staging, destination)

        def promoted(value):
            return str(Path(destination) / Path(value).relative_to(staging))

        restore_hold = None
        if sanitization is not None:
            restore_hold = True if sanitization.get("applicable") else "not_applicable"
        return {"restored": str(destination), "snapshot_id": manifest["snapshot_id"],
                "manifest_digest": manifest["digest"],
                "db": promoted(restored["db"]) if restored["db"] else None,
                "repositories": [{**item, "path": promoted(item["path"])}
                                 for item in restored["repositories"]],
                "journals": [promoted(item) for item in restored["journals"]],
                "artifacts": [promoted(item) for item in restored["artifacts"]],
                # Git fence truth comes from restore_backup's checks (FENCE_REF
                # parity failure aborts); canonical authority is sanitized above.
                "fence_resurrected": False,
                "authority_sanitization": sanitization,
                "restore_hold": restore_hold,
                "restore_hold_contract": RESTORE_HOLD_CONTRACT if restore_hold is True
                else None,
                "hold_note": "No physical paused/quiesced proof is claimed; the hold "
                             "is a durable marker the startup guard enforces, "
                             "released only by typed manual release after fresh "
                             "identity/authority verification."}

    # ------------------------------------------------------------- retention
    def plan_retention(self, *, keep=3):
        """Dry-run retention plan over managed snapshots under ``root``.

        Only strict-named, non-reparse direct children whose manifest loads,
        digest-recomputes, AND hash-verifies are managed candidates. Anything
        else is preserved and listed under ``excluded``. Deletes nothing.
        """
        keep = max(1, int(keep))
        managed, excluded = [], []
        for child in sorted(self.root.iterdir()):
            if not child.is_dir():
                if child.name not in (LEDGER_NAME, LOCK_NAME):
                    excluded.append({"path": child.name, "reason": "not_a_snapshot"})
                continue
            if not _safe_name(child.name) or _is_reparse_or_symlink(child) \
                    or child.parent != self.root:
                excluded.append({"path": child.name, "reason": "not_managed"})
                continue
            try:
                manifest = load_manifest(child)
                verification = self.verify_snapshot(child)
            except Problem as error:
                excluded.append({"path": child.name, "reason": error.code})
                continue
            if not verification["ok"]:
                excluded.append({"path": child.name, "reason": "verification_failed"})
                continue
            managed.append({"path": child.name, "created_at": manifest["created_at"],
                            "snapshot_id": manifest["snapshot_id"],
                            "manifest_digest": manifest["digest"]})
        managed.sort(key=lambda item: (item["created_at"], item["path"]), reverse=True)
        candidates = managed[keep:]
        plan = {"plan_id": uid("retention"), "kind": "coordinated-retention",
                "root": self.canonical_root, "created_at": now(), "keep": keep,
                "candidates": candidates,
                "kept": [item["path"] for item in managed[:keep]],
                "excluded": excluded,
                "erasure": {"logical": True, "physical": "not-guaranteed",
                            "note": "Deletion is filesystem unlink of managed "
                                    "snapshot directories only; it implies nothing "
                                    "about physical erasure, other backups, WAL "
                                    "segments, a VACUUM of any live database, or "
                                    "remote effects."}}
        plan["digest"] = _manifest_digest(plan)
        return plan

    def execute_retention(self, plan, *, confirmation):
        """Apply a retention plan. Requires the exact recomputed plan digest.

        The plan's digest is RECOMPUTED (never trusted as supplied), the plan must
        name this exact canonical managed root, every candidate must be a strict
        managed name that is a direct non-reparse child of the root, and every
        candidate's manifest digest is re-verified at apply time. Deletion intent
        is ledgered BEFORE removal; the verified outcome (deleted / failed with
        residue) is ledgered AFTER. A removal that leaves residue is reported
        ``failed`` — never as success.
        """
        if not isinstance(plan, dict) or plan.get("kind") != "coordinated-retention" \
                or not isinstance(plan.get("candidates"), list):
            raise Problem("retention_plan_invalid", 422)
        recomputed = _manifest_digest(plan)
        if recomputed != plan.get("digest"):
            raise Problem("retention_plan_invalid", 422)
        if not confirmation or confirmation != recomputed:
            raise Problem("retention_confirmation_required", 409)
        if plan.get("root") != self.canonical_root:
            raise Problem("retention_plan_foreign_root", 403)
        with self._lock():
            removed, skipped, failed = [], [], []
            for candidate in plan["candidates"]:
                if not isinstance(candidate, dict):
                    skipped.append({"path": None, "reason": "plan_invalid"})
                    continue
                name = candidate.get("path")
                # Strict binding: a managed NAME only, never a path to resolve.
                if not _safe_name(name) or not _HEX64.match(
                        str(candidate.get("manifest_digest"))) \
                        or not isinstance(candidate.get("snapshot_id"), str):
                    skipped.append({"path": name, "reason": "not_managed"})
                    continue
                target = self.root / name
                if target.parent != self.root or _is_reparse_or_symlink(target) \
                        or not target.is_dir():
                    skipped.append({"path": name, "reason": "not_managed"})
                    continue
                try:
                    manifest = load_manifest(target)
                    verification = self.verify_snapshot(target)
                except Problem:
                    skipped.append({"path": name, "reason": "manifest_changed"})
                    continue
                if not verification["ok"] \
                        or manifest["digest"] != candidate["manifest_digest"] \
                        or manifest["snapshot_id"] != candidate["snapshot_id"]:
                    skipped.append({"path": name, "reason": "changed_since_plan"})
                    continue
                # Durable intent BEFORE any removal.
                _ledger_append(self.root, {
                    "name": name, "snapshot_id": manifest["snapshot_id"],
                    "manifest_digest": manifest["digest"], "plan_id": plan["plan_id"],
                    "state": "deleting", "at": now()})
                gone = _rmtree(target)  # the proven managed snapshot directory only
                # Verified outcome AFTER the attempt.
                _ledger_append(self.root, {
                    "name": name, "snapshot_id": manifest["snapshot_id"],
                    "manifest_digest": manifest["digest"], "plan_id": plan["plan_id"],
                    "state": "deleted" if gone else "failed",
                    "residue": not gone, "at": now()})
                if gone:
                    removed.append(name)
                else:
                    failed.append({"path": name, "residue": True,
                                   "outcome": "unknown: directory removal incomplete"})
            return {"plan_id": plan["plan_id"], "removed": removed, "skipped": skipped,
                    "failed": failed, "erasure": plan["erasure"]}

    # --------------------------------------------------------------- status
    def status(self):
        """Counts of managed/excluded snapshots plus ledger size. Read-only."""
        plan = self.plan_retention(keep=10 ** 9)
        return {"root": self.canonical_root,
                "managed": len(plan["kept"]),
                "excluded": len(plan["excluded"]),
                "ledgered_deletions": sum(1 for entry in _ledger_read(self.root)
                                          if entry.get("state") == "deleted"),
                "generated_at": now()}


# ------------------------------------------------------------------ public API
COORDINATED_ACTIONS = [
    {"action": "coordinated.snapshot",
     "purpose": "Revalidated consistent capture: online-backup canonical SQLite AND "
                "journals, validated Git bundles, fingerprinted artifact copies, one "
                "versioned manifest",
     "input": "SnapshotSource (db_path, repositories, journals, artifacts, exclusions), "
              "optional destination/name",
     "output": "manifest dict with per-entry SHA-256, quiescence evidence, pre/post "
               "source fingerprints, recomputed internal digest",
     "authority": "offline operator command; hivemind_detached_work=True; default off; "
                  "admin/trusted path only",
     "side_effects": "creates one snapshot directory under the managed root; Git "
                     "backups journal through GitOperations in a scratch dir; takes "
                     "the root lock",
     "limits": "refuses non-quiescent journals (no Boolean override), changed sources "
               "on revalidation, duplicate names, credential-shaped artifacts, "
               "reparse components, existing destinations, ledgered names; bounded "
               "entries/bytes/lock wait; NOT a cross-plane atomic point in time",
     "retry_safety": "idempotent; a failed snapshot removes only its own staging tree",
     "hivemind_detached_work": True, "default_enabled": False},
    {"action": "coordinated.verify",
     "purpose": "Schema-validate and hash-verify a snapshot manifest and every entry",
     "input": "snapshot directory",
     "output": "verification report {ok, checked, missing, mismatches, unexpected}",
     "authority": "read-only",
     "side_effects": "none",
     "limits": "reports rather than repairs",
     "retry_safety": "idempotent",
     "hivemind_detached_work": False, "default_enabled": False},
    {"action": "coordinated.restore",
     "purpose": "Restore a verified snapshot into a NEW EMPTY approved destination",
     "input": "snapshot directory, destination, approve callback",
     "output": "restore report {restored, db, repositories, journals, artifacts}",
     "authority": "explicit operator approval via approve(manifest_digest, report); "
                  "offline command only — never wired into production serving paths; "
                  "hivemind_detached_work=True",
     "side_effects": "creates exactly the destination tree; deletes only its own "
                     "staging tree on failure; never resurrects the Nexus Git fence; "
                     "sanitizes the restored canonical copy in one pre-promotion "
                     "transaction (active Nexus leases invalidated, project epochs "
                     "bumped past the snapshot, active delivery leases expired, "
                     "queued/acknowledged controls expired, applying controls marked "
                     "outcome_unknown — never completed) and writes the durable "
                     "restore_hold meta marker + new-only sidecar",
     "limits": "refuses existing paths, reparse components, system/user roots, "
               "tampered/incomplete/tombstoned manifests, traversal entries, and "
               "ledgered deletions",
     "retry_safety": "idempotent; re-run with the same destination after cleanup",
     "hivemind_detached_work": True, "default_enabled": False},
    {"action": "coordinated.retention_plan",
     "purpose": "Dry-run retention/compaction plan bound to exact manifest digests",
     "input": "keep count",
     "output": "plan dict with candidates, kept, excluded, recomputed digest, honest "
               "erasure note",
     "authority": "read-only",
     "side_effects": "none",
     "limits": "unknown/unverifiable/non-managed entries are preserved, never planned",
     "retry_safety": "idempotent",
     "hivemind_detached_work": False, "default_enabled": False},
    {"action": "coordinated.retention_execute",
     "purpose": "Delete only plan-proven managed snapshots with durable intent/outcome "
                "ledgering",
     "input": "plan dict + exact recomputed plan digest confirmation",
     "output": "{removed, skipped, failed, erasure} — failed removals report residue, "
               "never success",
     "authority": "explicit confirmation equal to the recomputed plan digest; "
                  "hivemind_detached_work=True; default off",
     "side_effects": "filesystem unlink of proven managed snapshot directories; "
                     "append-only fail-closed deletion ledger (deleting → deleted/"
                     "failed); takes the root lock",
     "limits": "recomputes the plan digest, binds the exact canonical root, "
               "re-verifies every candidate at apply time, never resolves or follows "
               "non-managed paths",
     "retry_safety": "idempotent per candidate; re-running skips already-removed "
                     "entries and retries failed ones",
     "hivemind_detached_work": True, "default_enabled": False},
]


@dataclass(frozen=True)
class CoordinatedHandle:
    """Public handle returned by :func:`install`. The parent wires CLI/endpoints."""

    operations: CoordinatedOperations
    context: dict = field(default_factory=dict)

    def snapshot(self, source, destination=None, **kwargs):
        return self.operations.create_snapshot(source, destination, **kwargs)

    def verify(self, snapshot_dir):
        return self.operations.verify_snapshot(snapshot_dir)

    def restore(self, snapshot_dir, destination, **kwargs):
        return self.operations.restore_snapshot(snapshot_dir, destination, **kwargs)

    def plan(self, **kwargs):
        return self.operations.plan_retention(**kwargs)

    def execute(self, plan, **kwargs):
        return self.operations.execute_retention(plan, **kwargs)

    def status(self):
        return self.operations.status()

    def describe(self):
        return {"actions": [dict(item) for item in COORDINATED_ACTIONS],
                "wiring_needs": [],
                "context": dict(self.context)}


def install(app=None, service=None, principal=None, command=None, envelope=None, *,
            root):
    """Build the public coordinated-operations handle rooted at ``root``.

    The parent owns registry/config/CLI wiring; ``app``, ``service``,
    ``principal``, ``command`` and ``envelope`` are recorded as self-describing
    context only and are never mutated. All long-running actions are marked
    ``hivemind_detached_work=True`` and default-off: they are explicit two-phase
    handlers and must never run inside a ``Service.mutate`` transaction or any
    whole-process request path (filesystem and subprocess work is detached).
    Suggested offline CLI (parent-owned):
    ``coordinated snapshot|verify|restore|retention-plan|retention-execute``;
    restore and retention-execute require explicit operator confirmation.
    """
    handle_ops = CoordinatedOperations(root)
    return CoordinatedHandle(operations=handle_ops, context={
        "app": type(app).__name__ if app is not None else None,
        "service": type(service).__name__ if service is not None else None,
        "principal": (principal or {}).get("id") if isinstance(principal, dict) else None,
        "command": command, "envelope": envelope,
        "hivemind_detached_work": True, "default_enabled": False,
    })
