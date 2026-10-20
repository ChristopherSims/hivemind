"""Durable local journal for agent operations, checkpoints, and drift reconciliation.

The journal is a single-owner local SQLite file. It records:

* a stable command key per ``operation_id`` so a retry reuses the server's
  idempotency slot instead of creating a new write;
* a payload checksum over the exact original request, so a changed payload for the
  same operation is refused instead of being silently applied under a reused key;
* exactly one durable outcome per operation: ``pending``, ``completed`` or ``unknown``;
* checkpointed runtime identity, caller-supplied state and workspace file hashes that
  reconcile into a drift report after a restart.

It never stores credentials, never stores arbitrary exception diagnostics, never
re-runs an ``unknown`` non-idempotent effect, and never writes to workspace files.
There is no network access other than the caller-supplied client.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

import httpx

from .sdk import excluded, redact, redact_value
from .storage import canonical, digest, now, uid

__all__ = [
    "COMPLETED",
    "PENDING",
    "UNKNOWN",
    "Journal",
    "JournalConflict",
    "JournalError",
    "SensitivePayload",
    "UnknownOutcome",
    "WorkspaceViolation",
    "RECOVERY_CLASSIFICATIONS",
    "RECOVERY_DECISIONS",
    "command_key",
    "contained_path",
    "derive_operation_id",
    "error_record",
    "file_digest",
    "payload_checksum",
]

PENDING = "pending"
COMPLETED = "completed"
UNKNOWN = "unknown"
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS operations(
  operation_id TEXT PRIMARY KEY,
  command_key TEXT NOT NULL,
  method TEXT NOT NULL,
  path TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  body TEXT,
  body_persisted INTEGER NOT NULL DEFAULT 0,
  body_redacted INTEGER NOT NULL DEFAULT 0,
  retryable INTEGER NOT NULL DEFAULT 0,
  label TEXT,
  state TEXT NOT NULL,
  attempting INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  result TEXT,
  result_redacted INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS operations_state ON operations(state);
CREATE TABLE IF NOT EXISTS checkpoints(
  checkpoint_id TEXT PRIMARY KEY,
  label TEXT,
  operation_id TEXT,
  workspace TEXT,
  runtime TEXT,
  app_state TEXT,
  metadata TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoint_files(
  checkpoint_id TEXT NOT NULL,
  path TEXT NOT NULL,
  sha256 TEXT,
  bytes INTEGER,
  exists_flag INTEGER NOT NULL,
  PRIMARY KEY(checkpoint_id, path)
);
CREATE TABLE IF NOT EXISTS recovery_audit(
  id INTEGER PRIMARY KEY,
  operation_id TEXT NOT NULL,
  classification TEXT NOT NULL,
  decision TEXT,
  evidence_sha256 TEXT,
  detail TEXT,
  recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS recovery_audit_operation ON recovery_audit(operation_id);
"""

#: Evidence classifications a recovery reconciler may record. ``ambiguous`` is the
#: only bucket that carries no proof; ``stale_replaced`` means the canonical record
#: moved on (a newer execution/revision), so the old attempt must never be retried.
RECOVERY_CLASSIFICATIONS = frozenset(
    {"proved_applied", "proved_not_attempted", "ambiguous", "stale_replaced"})
RECOVERY_DECISIONS = frozenset({"completed", "not_applied"})

_CREDENTIAL_TEXT = re.compile(
    r"(?i)(bearer\s+[\w.\-]{6,}|-----BEGIN [^-]*PRIVATE KEY-----|"
    r"(?:api[_-]?key|password|passwd|secret|token|credential|authorization|cookie)\s*[=:]\s*\S)")
_CREDENTIAL_KEY = re.compile(
    r"(?i)^(authorization|cookie|password|passwd|secret|token|api[_-]?key|credential|private[_-]?key)$")


class JournalError(RuntimeError):
    """Base class for refusals that keep the journal fail-closed."""


class JournalConflict(JournalError):
    """The same operation ID was offered with a different request payload."""


class UnknownOutcome(JournalError):
    """An earlier attempt has an ambiguous outcome and must not be re-run blindly."""


class SensitivePayload(JournalError):
    """The request body looks like credential material and was not persisted."""


class WorkspaceViolation(JournalError):
    """A reconciled path escaped (or was excluded from) the configured workspace."""


def command_key(operation_id) -> str:
    """Deterministic Idempotency-Key for an operation ID, stable across restarts."""
    return "journal-" + hashlib.sha256(("command:" + str(operation_id)).encode()).hexdigest()[:40]


def payload_checksum(method, path, body=None) -> str:
    """Checksum of the exact original request. Never computed from a redacted copy."""
    return digest({"method": str(method).upper(), "path": str(path), "body": body})


def derive_operation_id(*parts) -> str:
    """Deterministic operation ID for a repeatable caller step."""
    return "op-" + digest(list(parts))[:32]


def contained_path(root, path):
    """Resolve ``path`` inside ``root``; refuse escapes and excluded locations."""
    if root is None:
        raise WorkspaceViolation("A workspace root is required for file reconciliation")
    root = Path(root).resolve()
    target = Path(path)
    target = (root / target).resolve() if not target.is_absolute() else target.resolve()
    try:
        relative = target.relative_to(root)
    except ValueError as error:
        raise WorkspaceViolation(f"Path escapes the workspace: {path}") from error
    if not relative.parts:
        raise WorkspaceViolation("Path must name a file inside the workspace")
    if excluded(relative.as_posix()):
        raise WorkspaceViolation(f"Path is excluded from capture: {relative.as_posix()}")
    return target, relative.as_posix()


def file_digest(path):
    """Return ``(sha256, byte_count)`` for a regular file, or ``(None, None)``."""
    path = Path(path)
    if not path.is_file():
        return None, None
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            hasher.update(chunk)
            size += len(chunk)
    return hasher.hexdigest(), size


def error_record(error=None, status=None) -> dict:
    """A bounded, credential-free failure record. Exception text is never stored."""
    if status is None and isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
    if status is not None:
        return {"kind": "http_status", "status": int(status)}
    if isinstance(error, httpx.TimeoutException):
        kind = "timeout"
    elif isinstance(error, httpx.TransportError):
        kind = "transport"
    elif isinstance(error, httpx.HTTPError):
        kind = "http_error"
    elif error is None:
        return {"kind": "unspecified"}
    else:
        kind = "exception"
    return {"kind": kind, "type": type(error).__name__}


def _parse(value):
    return json.loads(value) if value is not None else None


def _nested_credential_key(value) -> bool:
    if isinstance(value, dict):
        return any(bool(_CREDENTIAL_KEY.match(str(key).strip())) or _nested_credential_key(item)
                   for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_nested_credential_key(item) for item in value)
    return False


def looks_sensitive(value) -> bool:
    """True when a JSON value appears to carry credential material."""
    if value is None:
        return False
    return bool(_CREDENTIAL_TEXT.search(canonical(value))) or _nested_credential_key(value)


def _json_safe(value, what):
    try:
        canonical(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{what} must be JSON-serializable") from error
    return value


def _safe_metadata(value, what):
    if value is None:
        return None
    _json_safe(value, what)
    return canonical(redact_value(value))


def _diff(before, after):
    """Key-level difference between a checkpoint snapshot and current values."""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return {}
    return {key: {"checkpoint": before.get(key), "current": after.get(key)}
            for key in sorted(set(before) | set(after)) if before.get(key) != after.get(key)}


class Journal:
    """Durable operation journal, checkpoint store, and file drift reconciler.

    ``workspace`` is required only for file checkpoints/reconciliation. The journal is
    single-owner: one process opens it at a time, matching the rest of the reference
    milestone. Reopening it resolves any interrupted attempt to ``unknown``.
    """

    def __init__(self, path, *, workspace=None, runtime_provider=None, state_provider=None,
                 on_event=None, recover_on_open=True):
        self.path = Path(path)
        if self.path.is_dir():
            self.path = self.path / "journal.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.workspace = Path(workspace).resolve() if workspace else None
        self.runtime_provider = runtime_provider
        self.state_provider = state_provider
        self.on_event = on_event
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript(_SCHEMA)
        if recover_on_open:
            self.recover()

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
        return False

    @contextmanager
    def _transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def _emit(self, kind, record):
        if self.on_event is None:
            return
        try:
            self.on_event(kind, record)
        except Exception:
            # Observers are diagnostics; they must never break durability.
            pass

    def _view(self, row):
        record = dict(row)
        record["body"] = _parse(record.get("body"))
        record["result"] = _parse(record.get("result"))
        record["error"] = _parse(record.get("error"))
        for field in ("attempting", "retryable", "body_persisted", "body_redacted", "result_redacted"):
            record[field] = bool(record[field])
        return record

    # ---------------------------------------------------------------- operations

    def operation(self, operation_id):
        """Current durable record for one operation, or ``None``."""
        with self.lock:
            row = self.db.execute("SELECT * FROM operations WHERE operation_id=?",
                                  (str(operation_id),)).fetchone()
        return self._view(row) if row is not None else None

    def operations(self, state=None, limit=200):
        """Recent operation records, optionally filtered by outcome state."""
        with self.lock:
            if state is None:
                rows = self.db.execute("SELECT * FROM operations ORDER BY updated_at DESC, rowid DESC "
                                       "LIMIT ?", (int(limit),)).fetchall()
            else:
                states = [state] if isinstance(state, str) else list(state)
                marks = ",".join("?" for _ in states)
                rows = self.db.execute(
                    f"SELECT * FROM operations WHERE state IN ({marks}) "
                    "ORDER BY updated_at DESC, rowid DESC LIMIT ?",
                    (*[str(item) for item in states], int(limit))).fetchall()
        return [self._view(row) for row in rows]

    def has_unknown_effects(self):
        with self.lock:
            return self.db.execute(
                "SELECT 1 FROM operations WHERE retryable=0 AND (state='unknown' OR "
                "(state='pending' AND (attempting=1 OR result IS NULL OR "
                "COALESCE(json_extract(result,'$.decision'),'')!='not_applied'))) LIMIT 1"
            ).fetchone() is not None

    def resolve_unknown(self, operation_id, decision, reason, evidence):
        """Explicit operator adjudication, not automatic side-effect detection."""
        if decision not in ("completed", "not_applied") or not isinstance(reason, str) or not reason.strip():
            raise ValueError("A decision and non-empty reason are required")
        if not isinstance(evidence, dict) or evidence.get("operation_id") != operation_id or looks_sensitive(evidence):
            raise ValueError("Non-sensitive evidence must identify this exact operation")
        with self.lock, self._transaction():
            record = self.operation(operation_id)
            if record is None or record["state"] != UNKNOWN:
                raise ValueError("Only unknown operations can be reconciled")
            self.db.execute("""CREATE TABLE IF NOT EXISTS effect_reconciliations(
                id INTEGER PRIMARY KEY, operation_id TEXT, decision TEXT, reason TEXT,
                evidence_sha256 TEXT, recorded_at TEXT)""")
            proof = hashlib.sha256(canonical(evidence).encode()).hexdigest()
            self.db.execute("INSERT INTO effect_reconciliations VALUES(NULL,?,?,?,?,?)",
                            (operation_id, decision, redact(reason[:4096]), proof, now()))
            result = canonical({"operator_reconciled": True, "decision": decision, "evidence_sha256": proof})
            self.db.execute("UPDATE operations SET state=?, attempting=0, result=?, error=NULL, updated_at=? "
                            "WHERE operation_id=?",
                            (COMPLETED if decision == "completed" else PENDING, result, now(), operation_id))
        return self.operation(operation_id)

    def record_recovery_audit(self, operation_id, classification, evidence, *,
                              decision=None, detail=None):
        """Append one durable recovery-audit record. Never resolves anything by itself.

        ``classification`` is one of ``RECOVERY_CLASSIFICATIONS``; ``decision`` (when a
        resolution followed the evidence) is one of ``RECOVERY_DECISIONS``. ``evidence``
        must be a non-sensitive mapping identifying this exact operation; only its
        SHA-256 is stored, mirroring :meth:`resolve_unknown`. ``detail`` is an optional
        bounded, redacted mapping (identity fields and status codes, never payloads).
        """
        operation_id = str(operation_id)
        if classification not in RECOVERY_CLASSIFICATIONS:
            raise ValueError("classification must be one of " + ", ".join(sorted(RECOVERY_CLASSIFICATIONS)))
        if decision is not None and decision not in RECOVERY_DECISIONS:
            raise ValueError("decision must be one of " + ", ".join(sorted(RECOVERY_DECISIONS)))
        if not isinstance(evidence, dict) or evidence.get("operation_id") != operation_id \
                or looks_sensitive(evidence):
            raise ValueError("Non-sensitive evidence must identify this exact operation")
        if detail is not None:
            if not isinstance(detail, dict) or looks_sensitive(detail):
                raise ValueError("detail must be a non-sensitive mapping")
            detail_json = redact(canonical(redact_value(detail)))[:4096]
        else:
            detail_json = None
        proof = hashlib.sha256(canonical(evidence).encode()).hexdigest()
        stamp = now()
        with self.lock, self._transaction():
            self.db.execute(
                "INSERT INTO recovery_audit(operation_id, classification, decision, "
                "evidence_sha256, detail, recorded_at) VALUES(?,?,?,?,?,?)",
                (operation_id, classification, decision, proof, detail_json, stamp))
        record = {"operation_id": operation_id, "classification": classification,
                  "decision": decision, "evidence_sha256": proof, "recorded_at": stamp}
        self._emit("recovery_audit", record)
        return record

    def recovery_audits(self, operation_id=None, limit=200):
        """Durable recovery-audit records, newest first."""
        with self.lock:
            if operation_id is None:
                rows = self.db.execute(
                    "SELECT * FROM recovery_audit ORDER BY id DESC LIMIT ?",
                    (int(limit),)).fetchall()
            else:
                rows = self.db.execute(
                    "SELECT * FROM recovery_audit WHERE operation_id=? ORDER BY id DESC LIMIT ?",
                    (str(operation_id), int(limit))).fetchall()
        return [{**dict(row), "detail": _parse(row["detail"])} for row in rows]

    def begin_operation(self, operation_id, method, path, body=None, *, safe_to_retry=None,
                        allow_sensitive_payload=False, label=None):
        """Record intent before sending. Returns the durable record.

        Raises ``JournalConflict`` when the same ID is reused with a different payload
        after any attempt, and ``SensitivePayload`` when a body that would have to be
        persisted looks like credential material and the caller did not opt in explicitly.
        """
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError("operation_id must be a non-empty string")
        method = str(method).upper()
        path = str(path)
        checksum = payload_checksum(method, path, body)
        sensitive = looks_sensitive(body)
        retryable = int(method in SAFE_METHODS if safe_to_retry is None else bool(safe_to_retry))
        stamp = now()
        created = False
        with self.lock:
            with self._transaction():
                row = self.db.execute("SELECT * FROM operations WHERE operation_id=?",
                                      (operation_id,)).fetchone()
                refresh = row is None or not self._view(row)["attempts"]
                if refresh and sensitive and not allow_sensitive_payload:
                    raise SensitivePayload(
                        "Request body appears to contain credential material and was not persisted. "
                        "Pass allow_sensitive_payload=True to journal a redacted copy; the recorded "
                        "checksum still covers the exact original payload.")
                if body is None:
                    stored, persisted = None, 0
                elif sensitive:
                    stored, persisted = canonical(redact_value(body)), 0
                else:
                    stored, persisted = canonical(body), 1
                if row is None:
                    self.db.execute(
                        "INSERT INTO operations(operation_id, command_key, method, path, request_hash, "
                        "body, body_persisted, body_redacted, retryable, label, state, attempting, attempts, "
                        "result, result_redacted, error, created_at, updated_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,0,0,NULL,0,NULL,?,?)",
                        (operation_id, command_key(operation_id), method, path, checksum, stored,
                         persisted, int(sensitive), retryable, label, PENDING, stamp, stamp))
                    created = True
                else:
                    existing = self._view(row)
                    if existing["attempts"] > 0 and (
                            existing["request_hash"] != checksum or existing["method"] != method
                            or existing["path"] != path):
                        raise JournalConflict(
                            f"Operation {operation_id} was already attempted with a different request; "
                            "use a new operation_id for a corrected command.")
                    if refresh:
                        # Never sent, so the key is not yet bound on the server.
                        self.db.execute(
                            "UPDATE operations SET method=?, path=?, request_hash=?, body=?, "
                            "body_persisted=?, body_redacted=?, retryable=?, label=?, updated_at=? "
                            "WHERE operation_id=?",
                            (method, path, checksum, stored, persisted, int(sensitive), retryable, label,
                             stamp, operation_id))
                    elif safe_to_retry is not None:
                        self.db.execute("UPDATE operations SET retryable=?, updated_at=? WHERE operation_id=?",
                                        (retryable, stamp, operation_id))
            record = self.operation(operation_id)
        if created:
            self._emit("operation_begun", {"operation_id": operation_id, "method": method, "path": path})
        return record

    def mark_attempt(self, operation_id):
        """Durably mark that a request for this operation is in flight."""
        with self.lock, self._transaction():
            self.db.execute("UPDATE operations SET attempting=1, attempts=attempts+1, state=?, "
                            "updated_at=? WHERE operation_id=?", (PENDING, now(), str(operation_id)))
        return self.operation(operation_id)

    def complete_operation(self, operation_id, result):
        """Record a confirmed success. Sensitive results are stored redacted."""
        _json_safe(result, "result")
        sensitive = looks_sensitive(result)
        stored = canonical(redact_value(result)) if sensitive else canonical(result)
        with self.lock, self._transaction():
            self.db.execute(
                "UPDATE operations SET state=?, attempting=0, result=?, result_redacted=?, error=NULL, "
                "updated_at=? WHERE operation_id=?",
                (COMPLETED, stored, int(sensitive), now(), str(operation_id)))
        record = self.operation(operation_id)
        self._emit("operation_completed", {"operation_id": operation_id, "result_redacted": sensitive})
        return record

    def fail_operation(self, operation_id, error=None, *, status=None):
        """Record a failed attempt.

        A definite 4xx rejection leaves the operation ``pending`` (nothing was applied,
        so a corrected retry under a new operation ID is possible). Anything else,
        including timeouts, transport errors, 5xx and interrupted attempts, becomes
        ``unknown``.
        """
        record = error_record(error, status)
        definite = record.get("kind") == "http_status" and 400 <= record["status"] < 500
        state = PENDING if definite else UNKNOWN
        with self.lock, self._transaction():
            self.db.execute("UPDATE operations SET state=?, attempting=0, error=?, updated_at=? "
                            "WHERE operation_id=?", (state, redact(canonical(record)), now(),
                                                     str(operation_id)))
        view = self.operation(operation_id)
        self._emit("operation_rejected" if definite else "operation_unknown",
                   {"operation_id": operation_id, "error": record})
        return view

    def resolve_operation(self, operation_id, state, result=None):
        """Manual operator decision for an ambiguous outcome. Never re-runs anything.

        ``completed`` accepts an independently verified result; ``pending`` reopens the
        operation for a deliberate retry. The stable command key is preserved either way.
        """
        if state not in (PENDING, COMPLETED):
            raise ValueError("state must be 'pending' or 'completed'")
        if self.operation(operation_id) is None:
            raise KeyError(operation_id)
        if state == COMPLETED:
            return self.complete_operation(operation_id, result)
        with self.lock, self._transaction():
            self.db.execute("UPDATE operations SET state=?, attempting=0, error=NULL, result=NULL, "
                            "result_redacted=0, updated_at=? WHERE operation_id=?",
                            (PENDING, now(), str(operation_id)))
        record = self.operation(operation_id)
        self._emit("operation_resolved", {"operation_id": operation_id, "state": state})
        return record

    def request(self, client, method, path, body=None, operation_id=None, *, safe_to_retry=None,
                allow_sensitive_payload=False, label=None):
        """Execute ``client.request`` once and record its durable outcome.

        With no ``operation_id`` a fresh ID is generated for this call. Callers that need
        retries across a restart pass a stable ID, or use ``derive_operation_id``.
        A completed operation replays its stored result without a second network call.
        An ``unknown`` operation raises ``UnknownOutcome`` unless the operation is
        retry-safe (read-only method, or an explicit ``safe_to_retry=True``), in which case
        it is re-issued with the same preserved command key.
        """
        if not callable(getattr(client, "request", None)):
            raise TypeError("client must expose request(method, path, body=None, key=None)")
        operation_id = operation_id or uid("op")
        record = self.begin_operation(operation_id, method, path, body,
                                      safe_to_retry=safe_to_retry,
                                      allow_sensitive_payload=allow_sensitive_payload, label=label)
        if record["state"] == COMPLETED:
            return record["result"]
        if record["state"] == UNKNOWN and not record["retryable"]:
            raise UnknownOutcome(
                f"Operation {operation_id} has an ambiguous outcome; its non-idempotent effect "
                "will not be re-run automatically. Verify the remote state, then use "
                "resolve_operation() or pass safe_to_retry=True when the request is repeatable.")
        self.mark_attempt(operation_id)
        try:
            data = client.request(record["method"], record["path"], body, key=record["command_key"])
        except BaseException as error:
            self.fail_operation(operation_id, error)
            raise
        self.complete_operation(operation_id, data)
        return data

    def recover(self):
        """Resolve attempts that were in flight when the process stopped to ``unknown``."""
        with self.lock, self._transaction():
            rows = self.db.execute("SELECT operation_id FROM operations WHERE attempting=1").fetchall()
            if rows:
                self.db.execute("UPDATE operations SET attempting=0, state=?, error=?, updated_at=? "
                                "WHERE attempting=1",
                                (UNKNOWN, redact(canonical({"kind": "interrupted"})), now()))
        recovered = [row["operation_id"] for row in rows]
        if recovered:
            self._emit("recovered", {"operations": recovered})
        return recovered

    # --------------------------------------------------------------- checkpoints

    def _provider(self, callback, fallback):
        if fallback is not None:
            return fallback
        if callback is None:
            return None
        return _json_safe(callback(), "runtime/state callback")

    def _snapshot(self, path):
        target, relative = contained_path(self.workspace, path)
        sha, size = file_digest(target)
        return {"path": relative, "sha256": sha, "bytes": size, "exists": sha is not None}

    def checkpoint(self, label=None, *, files=(), state=None, runtime=None, metadata=None,
                   operation_id=None, checkpoint_id=None):
        """Persist runtime identity, caller state and workspace file hashes.

        Only reads workspace files. ``files`` are resolved inside ``workspace`` and are
        refused when they escape it or hit an excluded location.
        """
        runtime_value = self._provider(self.runtime_provider, runtime)
        state_value = self._provider(self.state_provider, state)
        _json_safe(metadata, "metadata")
        runtime_json = _safe_metadata(runtime_value, "runtime")
        state_json = _safe_metadata(state_value, "state")
        metadata_json = _safe_metadata(metadata, "metadata")
        manifest = [self._snapshot(path) for path in files or ()]
        checkpoint_id = checkpoint_id or uid("checkpoint")
        stamp = now()
        with self.lock, self._transaction():
            self.db.execute(
                "INSERT OR REPLACE INTO checkpoints(checkpoint_id, label, operation_id, workspace, "
                "runtime, app_state, metadata, created_at) VALUES(?,?,?,?,?,?,?,?)",
                (checkpoint_id, label, operation_id,
                 str(self.workspace) if self.workspace else None, runtime_json, state_json,
                 metadata_json, stamp))
            for item in manifest:
                self.db.execute(
                    "INSERT OR REPLACE INTO checkpoint_files(checkpoint_id, path, sha256, bytes, "
                    "exists_flag) VALUES(?,?,?,?,?)",
                    (checkpoint_id, item["path"], item["sha256"], item["bytes"], int(item["exists"])))
        record = {"checkpoint_id": checkpoint_id, "label": label, "operation_id": operation_id,
                  "workspace": str(self.workspace) if self.workspace else None, "created_at": stamp,
                  "runtime": runtime_value, "app_state": state_value, "metadata": metadata,
                  "files": manifest}
        self._emit("checkpoint", {"checkpoint_id": checkpoint_id, "label": label,
                                  "operation_id": operation_id, "files": len(manifest)})
        return record

    def _latest_checkpoint(self):
        row = self.db.execute("SELECT * FROM checkpoints ORDER BY created_at DESC, rowid DESC "
                              "LIMIT 1").fetchone()
        return dict(row) if row is not None else None

    def latest_checkpoint(self):
        """Return the latest redacted checkpoint metadata for integration callers."""
        with self.lock:
            row = self._latest_checkpoint()
            if row is None:
                return None
            return {**row, "runtime": _parse(row["runtime"]), "app_state": _parse(row["app_state"]),
                    "metadata": _parse(row["metadata"])}

    def reconcile(self, checkpoint_id=None, *, files=None):
        """Read-only drift report. Never re-runs an effect and never writes a file.

        Compares the checkpoint's file hashes, runtime identity and application state with
        current values, and reports every ``unknown`` operation as a possible uncertain
        side effect instead of resolving it automatically.
        """
        recovered = self.recover()
        if checkpoint_id is None:
            checkpoint = self._latest_checkpoint()
        else:
            row = self.db.execute("SELECT * FROM checkpoints WHERE checkpoint_id=?",
                                  (str(checkpoint_id),)).fetchone()
            if row is None:
                raise KeyError(checkpoint_id)
            checkpoint = dict(row)
        baseline = {}
        workspace_changed = False
        if checkpoint is not None:
            baseline = {row["path"]: dict(row) for row in self.db.execute(
                "SELECT * FROM checkpoint_files WHERE checkpoint_id=?",
                (checkpoint["checkpoint_id"],)).fetchall()}
            if checkpoint.get("workspace") and self.workspace:
                workspace_changed = Path(checkpoint["workspace"]) != self.workspace
        if files is None:
            requested = list(baseline)
        else:
            requested = []
            for item in files:
                _, relative = contained_path(self.workspace, item)
                requested.append(relative)
        drift = {"modified": [], "created": [], "deleted": [], "unchanged": [], "unverified": []}
        violations = []
        checked = 0
        for relative in requested:
            try:
                target, relative = contained_path(self.workspace, relative)
            except WorkspaceViolation as error:
                violations.append({"path": str(relative), "reason": str(error)})
                continue
            current, _ = file_digest(target)
            checked += 1
            before = baseline.get(relative)
            if before is None:
                drift["unverified"].append(relative)
            elif before["sha256"] == current:
                drift["unchanged"].append(relative)
            elif before["sha256"] is not None and current is None:
                drift["deleted"].append(relative)
            elif before["sha256"] is None and current is not None:
                drift["created"].append(relative)
            else:
                drift["modified"].append(relative)
        for key in ("modified", "created", "deleted"):
            drift[key].sort()
        operations = self.operations(limit=1000)
        unknown = [item for item in operations if item["state"] == UNKNOWN]
        if checkpoint is None:
            checkpoint_drift = []
        else:
            checkpoint_drift = drift["modified"] + drift["created"] + drift["deleted"]
        uncertain = []
        for item in unknown:
            observed = checkpoint_drift if (checkpoint is not None
                                            and checkpoint.get("operation_id") == item["operation_id"]
                                            and checkpoint_drift) else []
            uncertain.append({"operation_id": item["operation_id"], "method": item["method"],
                              "path": item["path"], "label": item["label"], "files": observed,
                              "retryable": item["retryable"], "reason": "ambiguous_outcome"})
        before_runtime = _parse(checkpoint["runtime"]) if checkpoint is not None else None
        before_state = _parse(checkpoint["app_state"]) if checkpoint is not None else None
        report = {
            "workspace": str(self.workspace) if self.workspace else None,
            "workspace_changed": workspace_changed,
            "checkpoint_id": checkpoint["checkpoint_id"] if checkpoint is not None else None,
            "checkpoint_created_at": checkpoint["created_at"] if checkpoint is not None else None,
            "recovered": recovered,
            "checked_files": checked,
            "files": drift,
            "containment_violations": violations,
            "runtime_drift": _diff(before_runtime, self._provider(self.runtime_provider, None)),
            "state_drift": _diff(before_state, self._provider(self.state_provider, None)),
            "pending": [item["operation_id"] for item in operations if item["state"] == PENDING],
            "unknown": [item["operation_id"] for item in unknown],
            "completed": sum(1 for item in operations if item["state"] == COMPLETED),
            "unsafe_to_rerun": [item["operation_id"] for item in unknown if not item["retryable"]],
            "uncertain_side_effects": uncertain,
        }
        self._emit("reconcile", {"checkpoint_id": report["checkpoint_id"],
                                 "unknown": report["unknown"], "files": checked})
        return report
