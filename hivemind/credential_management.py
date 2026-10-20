"""Operator-facing credential rotation and revocation for canonical principals.

This module owns the *administrator* surface for credentials that Hivemind already
issues elsewhere (``hivemind.cli initialize``/``issue-token`` for local principals,
``hivemind.worker_provisioning`` for owned workers). It never issues a credential and
never widens one: rotation replaces the stored token hash of exactly one principal,
and revocation invalidates that principal's stored credential without deleting the
principal, its resources, or the work it owns.

Secrets discipline (non-negotiable):

* Listing and every other read return credential *metadata* only. Stored token
  hashes and raw tokens are never returned, and never echoed into errors.
* Rotation returns the replacement token in the immediate HTTP response only. It is
  never persisted, journalled, audited, logged, telemetered, cached or replayed; the
  module's own idempotency journal stores a safe receipt instead, so a retry answers
  "already applied, token unavailable" rather than re-revealing a secret.
* Revocation writes a non-secret invalidation marker into ``principals.token_hash``
  (no client-supplied token can hash to it) instead of deleting credential material
  or the principal row.

Authorization: only an ``admin`` principal with current project access may list or
mutate, and only within the credential's canonical project/role/agent scope. The
acting principal is re-verified (role/project/agent/token_hash/coordinator/
capture_authority) inside the mutation transaction, so a credential revoked or
downgraded under a request fails closed instead of acting on stale authority.

Local metadata: :class:`CredentialManagement` adds two canonical tables of its own
(``credential_records`` for the per-credential CAS revision and revocation ledger,
``credential_operations`` for rotation/revocation idempotency). Neither table stores
token material, and neither declares a foreign key to ``principals`` so an out-of-band
principal removal (for example an owned-worker stop) keeps working; a record whose
principal row is gone reports the honest state ``missing``.

Honest limits, reported to operators rather than hidden:

* Rotation and revocation delete the principal's browser sessions, including the
  calling operator's own session when they manage their own credential. The current
  bearer token is invalidated by rotation too, so a self-rotation response must be
  saved immediately: the token is shown once.
* An owned worker whose credential is revoked here keeps its canonical
  ``worker_provision`` record and reports ``credential_revoked: false`` in the worker
  administration view, which detects only deleted principals. Its API calls fail
  closed against this invalidation. Reconciling that monitoring flag is the worker
  module's integration point, not this module's.
"""
import hashlib
import json
import secrets
import time
from contextlib import nullcontext
from typing import Literal

from fastapi import Depends, Header, Query, Request
from pydantic import Field

from . import models as m
from .storage import Problem, canonical, digest, now, uid

# Roles that exist in the canonical ``principals`` table.
ROLES = ("admin", "human", "observer", "agent")
# A listing page is bounded; the API is not a bulk credential exporter.
MAX_CREDENTIALS = 200
# Presence window, identical to the canonical ledger view (30s offline, 15s stale).
STALE_AFTER = 15.0
OFFLINE_AFTER = 30.0
# Bounds for the impact projection; only counters/identifiers are returned.
MAX_IMPACT_RUNS = 50
MAX_IMPACT_WORKERS = 50
# Revoked credentials keep their principal row; the stored value can never equal
# hash_token(<anything a client sends>), so authentication fails closed forever.
REVOKED_MARKER = "revoked:"

POLICY = {
    "listing": "Credential metadata only: raw tokens and stored token hashes are never listed. "
               "Hivemind does not record a credential's original creation time, so the ledger "
               "reports only its own rotation and revocation history.",
    "rotation": "A rotation returns one new token in its immediate response only. It is never "
                "stored, journalled, audited, logged or shown again; a retry reports "
                "'already applied, token unavailable'.",
    "revocation": "Revocation is terminal and canonical: the stored credential is invalidated, "
                  "the principal's sessions are deleted, and the principal record is preserved "
                  "for ownership, audit and existing work.",
    "last_usable_admin": "The last usable administrator credential cannot be revoked.",
    "scope": "Only an administrator may list or mutate credentials, within the credential's exact "
             "project, role and agent scope. Rotation and revocation never change project, role, "
             "agent, coordinator or capture authority.",
    "authority_source": "The canonical principals row is authoritative for a credential's project, "
                        "role, agent, coordinator and capture authority; the credential ledger only "
                        "holds this module's revision and revocation state. An out-of-band role or "
                        "project change is therefore honored immediately for listing, impact, "
                        "authorization and the last-administrator guard, and the ledger snapshot "
                        "converges on the next mutation.",
    "confirmation": "Every mutation requires the current revision plus the acknowledged impact "
                    "fingerprint from a fresh impact review.",
    "sessions": "Rotation and revocation delete the credential's existing sessions, including the "
                "calling operator's own session when they manage their own credential.",
    "administrator_token_handling": "A rotating administrator receives the target principal's new "
                                    "bearer token. That is inherent to rotation and is no broader than "
                                    "the administrator's existing authority: no session is created for "
                                    "the target and no other principal's identity is assumed.",
    "no_impersonation": "This surface never creates, adopts or replays another principal's session, "
                        "and never rewrites a principal's role, project, agent, coordinator or capture "
                        "authority.",
}

RECORDS_SCHEMA = """
CREATE TABLE IF NOT EXISTS credential_records(
  principal TEXT PRIMARY KEY,
  project TEXT NOT NULL,
  role TEXT NOT NULL,
  agent TEXT,
  coordinator INTEGER NOT NULL DEFAULT 0,
  capture_authority INTEGER NOT NULL DEFAULT 0,
  revision INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'active',
  rotated_at TEXT,
  revoked_at TEXT,
  rotation_count INTEGER NOT NULL DEFAULT 0)
"""

OPERATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS credential_operations(
  actor TEXT NOT NULL,
  operation TEXT NOT NULL,
  key TEXT NOT NULL,
  credential TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  token_returned INTEGER NOT NULL DEFAULT 1,
  result TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(actor,operation,key))
"""


class CredentialConfirmation(m.Model):
    """Explicit operator confirmation bound to a freshly reviewed impact."""

    expected_revision: int = Field(ge=1)
    acknowledged_impact: str = Field(pattern=r"^[0-9a-f]{64}$")


MODELS = ("CredentialConfirmation",)

# Discovery strings and status the integration layer publishes in
# ``GET /.well-known/hivemind`` once this module is registered. Kept here so the
# integrator wires one canonical source instead of retyping capability names.
DISCOVERY_CAPABILITIES = ("credentials.rotation", "credentials.revocation")
CAPABILITY_STATUS = {
    "credentials.rotation": {
        "enabled": True,
        "administrator_only": True,
        "token_returned": "once_in_immediate_response",
        "token_persisted": False,
        "sessions_invalidated": "all_of_that_credential",
        "privileges_changed": False,
        "idempotent_retry": "already_applied_token_unavailable"},
    "credentials.revocation": {
        "enabled": True,
        "administrator_only": True,
        "terminal": True,
        "principal_preserved": True,
        "resource_deletion": False,
        "sessions_invalidated": "all_of_that_credential",
        "last_usable_admin": "refused"},
}


def descriptors():
    return {name: dict(value) for name, value in OPERATION_DESCRIPTORS.items()}


class CredentialManagement:
    """Canonical credential metadata, CAS revisions, rotation and revocation."""

    def __init__(self, service):
        self.service, self.store = service, service.store
        with self.store.lock:
            self.store.db.execute(RECORDS_SCHEMA)
            self.store.db.execute(OPERATIONS_SCHEMA)

    # ------------------------------------------------------------------ scope
    def operator(self, principal, project):
        """Administrator authority inside one exact project scope."""
        if project is None:
            raise Problem("credential_project_unbound", 409)
        # Non-admins only reach their own project; admins reach the named project.
        self.service.project_access(principal, project)
        if principal["role"] != "admin":
            raise Problem("administrator_required", 403)

    def _actor(self, principal):
        """Fail closed when the acting credential or its authority changed under us."""
        row = self.store.db.execute(
            "SELECT * FROM principals WHERE id=?", (principal["id"],)).fetchone()
        keys = ("role", "project", "agent", "token_hash", "coordinator", "capture_authority")
        if row is None or any(row[key] != principal[key] for key in keys):
            raise Problem("authentication_required", 401)

    # --------------------------------------------------------------- metadata
    def _principal_row(self, credential_id):
        row = self.store.db.execute(
            "SELECT * FROM principals WHERE id=?", (credential_id,)).fetchone()
        return dict(row) if row else None

    def _record_row(self, credential_id):
        row = self.store.db.execute(
            "SELECT * FROM credential_records WHERE principal=?", (credential_id,)).fetchone()
        return dict(row) if row else None

    def _transaction(self):
        """Join the enclosing canonical transaction when one is already open."""
        return nullcontext() if self.store.db.in_transaction else self.store.transaction()

    def _ensure_records(self, rows):
        """Lazily materialize the CAS ledger for canonical principals, once per row."""
        with self._transaction():
            for row in rows:
                if row["project"] is None:
                    continue
                if self._record_row(row["id"]) is None:
                    self.store.db.execute(
                        "INSERT INTO credential_records VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (row["id"], row["project"], row["role"], row["agent"],
                         int(row["coordinator"] or 0), int(row["capture_authority"] or 0),
                         1, "active", None, None, 0))

    def _record(self, credential_id):
        """Return ``(record, principal_row)`` with the canonical project overlaid.

        The ledger is keyed by principal and owns only this module's CAS revision and
        revocation state; ``principals`` stays authoritative for project, role, agent
        and grants, so an out-of-band change is honored immediately instead of being
        frozen into the snapshot.
        """
        principal_row = self._principal_row(credential_id)
        record = self._record_row(credential_id)
        if principal_row is None and record is None:
            raise Problem("not_found", 404)
        if principal_row is None:
            # The principal row disappeared out of band: keep the ledger readable
            # but never report the credential as usable.
            state = "revoked" if record["state"] == "revoked" else "missing"
            return {**record, "state": state}, None
        if record is None:
            self._ensure_records([principal_row])
            record = self._record_row(credential_id)
        return {**record, "project": principal_row["project"]}, principal_row

    @staticmethod
    def _state(record, principal_row):
        if record["state"] == "revoked":
            return "revoked"
        if principal_row is None:
            return "missing"
        return "active"

    @staticmethod
    def _authority(record, principal_row):
        """Canonical authority, never a stale ledger snapshot.

        ``principals`` is the authority for role/project/agent/grants; the ledger only
        holds this module's CAS revision and revocation state. When the principal row
        is gone (out-of-band removal) the last observed snapshot is reported instead,
        so the operator can still see what the credential used to be.
        """
        if principal_row is None:
            return {"role": record["role"], "project": record["project"], "agent": record["agent"],
                    "coordinator": bool(record["coordinator"]),
                    "capture_authority": bool(record["capture_authority"])}
        return {"role": principal_row["role"], "project": principal_row["project"],
                "agent": principal_row["agent"], "coordinator": bool(principal_row["coordinator"]),
                "capture_authority": bool(principal_row["capture_authority"])}

    def _refresh_snapshot(self, record, principal_row):
        """Converge the ledger snapshot with canonical authority on every mutation."""
        if principal_row is None:
            return record
        if (record["role"] != principal_row["role"] or record["agent"] != principal_row["agent"]
                or record["project"] != principal_row["project"]
                or int(record["coordinator"]) != int(principal_row["coordinator"] or 0)
                or int(record["capture_authority"]) != int(principal_row["capture_authority"] or 0)):
            self.store.db.execute(
                "UPDATE credential_records SET role=?, project=?, agent=?, coordinator=?, "
                "capture_authority=? WHERE principal=?",
                (principal_row["role"], principal_row["project"], principal_row["agent"],
                 int(principal_row["coordinator"] or 0), int(principal_row["capture_authority"] or 0),
                 record["principal"]))
            return {**record, "role": principal_row["role"], "project": principal_row["project"],
                    "agent": principal_row["agent"],
                    "coordinator": int(principal_row["coordinator"] or 0),
                    "capture_authority": int(principal_row["capture_authority"] or 0)}
        return record

    def _session_count(self, credential_id):
        return self.store.db.execute(
            "SELECT count(*) FROM sessions WHERE principal=? AND expires>?",
            (credential_id, time.time())).fetchone()[0]

    def _active_runs(self, agent_id):
        if not agent_id:
            return [], False
        rows = self.store.db.execute(
            "SELECT body FROM resources WHERE kind='run' AND deleted=0 AND "
            "json_extract(body,'$.agent_id')=? ORDER BY seq DESC LIMIT ?",
            (agent_id, MAX_IMPACT_RUNS + 1)).fetchall()
        runs, stamp = [], time.time()
        for row in rows:
            body = json.loads(row["body"])
            if body.get("status") != "running":
                continue
            gap = stamp - body.get("last_seen_at", 0)
            runs.append({
                "run_id": body["id"],
                "presence": "offline" if gap >= OFFLINE_AFTER else "stale" if gap >= STALE_AFTER else "online",
                "last_seen_at": body.get("last_seen_at"),
            })
        return runs[:MAX_IMPACT_RUNS], len(runs) > MAX_IMPACT_RUNS

    def _owned_workers(self, credential_id):
        rows = self.store.db.execute(
            "SELECT body FROM resources WHERE kind='worker_provision' AND deleted=0 AND "
            "json_extract(body,'$.principal_id')=? ORDER BY seq DESC LIMIT ?",
            (credential_id, MAX_IMPACT_WORKERS + 1)).fetchall()
        workers = [{"worker_id": json.loads(row["body"])["id"],
                    "state": json.loads(row["body"]).get("state")} for row in rows]
        return workers[:MAX_IMPACT_WORKERS], len(workers) > MAX_IMPACT_WORKERS

    def _agent_name(self, agent_id):
        if not agent_id:
            return None
        row = self.store.db.execute(
            "SELECT body FROM resources WHERE id=? AND kind='agent' AND deleted=0",
            (agent_id,)).fetchone()
        return json.loads(row["body"]).get("display_name") if row else None

    def _usable_admins(self, project=None):
        """Administrator credentials that can still authenticate, from canonical rows.

        Revocation is read from this module's ledger (or from the non-secret
        revocation marker), never from token material.
        """
        sql = ("SELECT id, project, token_hash FROM principals WHERE role='admin'")
        args = []
        if project is not None:
            sql += " AND (project=? OR project IS NULL)"
            args.append(project)
        usable = []
        for row in self.store.db.execute(sql, args):
            record = self._record_row(row["id"])
            if record is not None and record["state"] == "revoked":
                continue
            if row["token_hash"] == REVOKED_MARKER + row["id"]:
                continue
            usable.append(row["id"])
        return usable

    def view(self, principal, record, principal_row=None):
        """Credential metadata. Never token material, hashes included."""
        state = self._state(record, principal_row)
        authority = self._authority(record, principal_row)
        return {
            "credential_id": record["principal"],
            "project_id": authority["project"],
            "role": authority["role"],
            "agent_id": authority["agent"],
            "agent_display_name": self._agent_name(authority["agent"]),
            "coordinator": authority["coordinator"],
            "capture_authority": authority["capture_authority"],
            "revision": record["revision"],
            "state": state,
            "usable": state == "active" and principal_row is not None
            and principal_row["token_hash"] != REVOKED_MARKER + record["principal"],
            "active_sessions": self._session_count(record["principal"]),
            "rotated_at": record["rotated_at"],
            "revoked_at": record["revoked_at"],
            "rotation_count": record["rotation_count"],
            "self": record["principal"] == principal["id"],
        }

    # ------------------------------------------------------------------ reads
    def listing(self, principal, project, role=None, agent_id=None, limit=100):
        self.operator(principal, project)
        if role is not None and role not in ROLES:
            raise Problem("unsupported_role", 422)
        if agent_id is not None:
            # Exact agent scope: the agent must exist in this project.
            self.service.reference(principal, project, agent_id, "agent")
        sql, args = "SELECT * FROM principals WHERE project=?", [project]
        if role is not None:
            sql += " AND role=?"
            args.append(role)
        if agent_id is not None:
            sql += " AND agent=?"
            args.append(agent_id)
        sql += " ORDER BY id LIMIT ?"
        rows = [dict(row) for row in self.store.db.execute(sql, (*args, limit + 1))]
        has_more = len(rows) > limit
        rows = rows[:limit]
        self._ensure_records(rows)
        records = {row["id"]: self._record_row(row["id"]) for row in rows}
        credentials = [self.view(principal, records[row["id"]], row) for row in rows]
        revoked_in_project = self.store.db.execute(
            "SELECT count(*) FROM credential_records r JOIN principals p ON p.id=r.principal "
            "WHERE p.project=? AND r.state='revoked'", (project,)).fetchone()[0]
        return {
            "project_id": project,
            "credentials": credentials,
            "count": len(credentials),
            "limit": limit,
            "has_more": has_more,
            "summary": {
                "usable_admins": len(self._usable_admins()),
                "project_admins": len(self._usable_admins(project)),
                "revoked_credentials": revoked_in_project,
            },
            "policy": dict(POLICY),
            "token_material_returned": False,
        }

    def read(self, principal, credential_id):
        record, principal_row = self._record(credential_id)
        self.operator(principal, record["project"])
        return {"credential": self.view(principal, record, principal_row), "policy": dict(POLICY)}

    def impact(self, principal, credential_id, operation):
        if operation not in ("rotate", "revoke"):
            raise Problem("unsupported_operation", 422)
        record, principal_row = self._record(credential_id)
        self.operator(principal, record["project"])
        return self._impact(principal, record, principal_row, operation)

    # ----------------------------------------------------------------- impact
    def _impact(self, principal, record, principal_row, operation):
        """Impact projection plus the fingerprint a mutation must acknowledge."""
        credential_id, project = record["principal"], record["project"]
        state = self._state(record, principal_row)
        authority = self._authority(record, principal_row)
        sessions = self._session_count(credential_id)
        runs, runs_truncated = self._active_runs(authority["agent"])
        workers, workers_truncated = self._owned_workers(credential_id)
        deployment_admins = self._usable_admins()
        project_admins = self._usable_admins(project)
        last_admin = bool(authority["role"] == "admin" and len(deployment_admins) <= 1)
        self_affected = credential_id == principal["id"]
        warnings = []
        if state == "revoked":
            warnings.append("This credential is already revoked. Rotation cannot revive it and "
                            "revocation is terminal.")
        if state == "missing":
            warnings.append("The canonical principal row is gone, so this credential can no longer "
                            "authenticate. Metadata is retained for audit only.")
        if operation == "rotate":
            if state == "active":
                warnings.append("Rotation replaces the stored token hash for exactly this principal. "
                                "Project, role, agent, coordinator and capture authority are unchanged.")
            if self_affected:
                warnings.append("This is your own credential. The bearer token and browser session used "
                                "for this request stop working immediately; save the new token when it is "
                                "shown, because it is never shown again.")
            if last_admin:
                warnings.append("This is the last usable administrator credential. The replacement token "
                                "must be kept, or this deployment has no administrator.")
        else:
            if last_admin:
                warnings.append("Refused: revoking the last usable administrator credential would leave "
                                "this deployment without an administrator.")
            if self_affected:
                warnings.append("This is your own credential. Revocation signs you out immediately and "
                                "cannot be undone.")
        if sessions:
            warnings.append(f"{sessions} active session(s) for this credential are signed out by this "
                            "mutation and cannot be restored.")
        if runs:
            warnings.append(f"{len(runs)} active run(s) for this agent lose API access immediately. Their "
                            "work, claims and evidence are preserved, but they cannot report progress.")
        if workers:
            warnings.append(f"{len(workers)} owned worker(s) bound to this credential fail closed on "
                            "their next API call; their canonical records are preserved.")
        core = {
            "operation": operation,
            "credential_id": credential_id,
            "project_id": project,
            "role": authority["role"],
            "agent_id": authority["agent"],
            "revision": record["revision"],
            "state": state,
            "active_sessions": sessions,
            "active_run_ids": sorted(run["run_id"] for run in runs),
            "owned_worker_ids": sorted(worker["worker_id"] for worker in workers),
            "self_affected": self_affected,
            "last_usable_admin": last_admin,
            "usable_admins": len(deployment_admins),
            "project_admins": len(project_admins),
        }
        fingerprint = hashlib.sha256(canonical(core).encode()).hexdigest()
        return {
            **core,
            "fingerprint": fingerprint,
            "active_runs": runs,
            "owned_workers": workers,
            "runs_truncated": runs_truncated,
            "workers_truncated": workers_truncated,
            "agent_display_name": self._agent_name(authority["agent"]),
            "warnings": warnings,
            "policy": dict(POLICY),
            "token_material_returned": False,
        }

    def _confirm(self, record, body, impact):
        # Revision first: a retried or stale command must fail its CAS before the
        # (also stale) impact acknowledgement is even considered.
        if record["revision"] != body.expected_revision:
            raise Problem("revision_conflict", 412)
        if not secrets.compare_digest(body.acknowledged_impact, impact["fingerprint"]):
            raise Problem("impact_acknowledgement_stale", 409)

    # -------------------------------------------------------------- mutations
    def mutate(self, principal, operation, credential_id, body, idempotency_key):
        """Shared mutation path: re-authorize, CAS, confirm, journal, then apply.

        Both operations run here instead of the generic ``command`` seam, because a
        retry must be *re-authorized* against current authority and answered with a
        safe receipt rather than a cached response. The generic journal would replay
        the stored body without re-checking the caller, and rotation must never put a
        token into a replayable record at all.
        """
        if operation not in ("rotate", "revoke"):
            raise Problem("unsupported_operation", 422)
        if not idempotency_key or len(idempotency_key) > 128:
            raise Problem("idempotency_key_required", 422)
        payload_hash = digest({"operation": operation, "credential_id": credential_id,
                               "expected_revision": body.expected_revision,
                               "acknowledged_impact": body.acknowledged_impact})
        with self._transaction():
            # Re-verify the acting credential and its project scope under the lock
            # before the journal is even consulted.
            self._actor(principal)
            record, principal_row = self._record(credential_id)
            self.operator(principal, record["project"])
            journal = self.store.db.execute(
                "SELECT * FROM credential_operations WHERE actor=? AND operation=? AND key=?",
                (principal["id"], operation, idempotency_key)).fetchone()
            if journal is not None:
                if journal["payload_hash"] != payload_hash or journal["credential"] != credential_id:
                    raise Problem("idempotency_conflict", 409)
                return self._replay(principal, operation, record, principal_row)
            record = self._refresh_snapshot(record, principal_row)
            impact = self._impact(principal, record, principal_row, operation)
            self._confirm(record, body, impact)
            if operation == "rotate":
                return self._rotate(principal, record, principal_row, idempotency_key, payload_hash)
            return self._revoke(principal, record, principal_row, idempotency_key, payload_hash)

    def _record_operation(self, principal, operation, credential_id, key, payload_hash,
                          token_returned, result):
        # A safe receipt: never the token, never a hash of it.
        self.store.db.execute(
            "INSERT INTO credential_operations VALUES(?,?,?,?,?,?,?,?)",
            (principal["id"], operation, key, credential_id, payload_hash, token_returned,
             canonical(result), now()))

    def _revoke(self, principal, record, principal_row, key, payload_hash):
        credential_id = record["principal"]
        if self._state(record, principal_row) == "revoked":
            raise Problem("credential_already_revoked", 409)
        if principal_row is None:
            raise Problem("not_found", 404)
        if self._authority(record, principal_row)["role"] == "admin" and len(self._usable_admins()) <= 1:
            raise Problem("last_usable_admin_protected", 409)
        sessions = self._session_count(credential_id)
        # Never delete the principal: ownership, audit and existing work stay intact,
        # and no client token can hash to this marker.
        self.store.db.execute("UPDATE principals SET token_hash=? WHERE id=?",
                              (REVOKED_MARKER + credential_id, credential_id))
        self.store.db.execute("DELETE FROM sessions WHERE principal=?", (credential_id,))
        self.store.db.execute(
            "UPDATE credential_records SET state='revoked', revoked_at=?, revision=revision+1 "
            "WHERE principal=?", (now(), credential_id))
        self.store.db.execute("INSERT INTO audit VALUES(?,?,?,?,?)",
                              (uid("aud"), principal["id"], "credential.revoke", credential_id, now()))
        # Signal live cursors so revoked sessions and streams re-verify promptly.
        self.store.tick(record["project"])
        self._record_operation(principal, "revoke", credential_id, key, payload_hash, 0,
                               {"status": "revoked", "token_available": False,
                                "note": "Revocation is terminal; the principal and its work are preserved."})
        current, current_row = self._record(credential_id)
        return {
            "status": "revoked",
            "revoked": True,
            "token_available": False,
            "effective_state": "revoked",
            "credential": self.view(principal, current, current_row),
            "sessions_invalidated": sessions,
            "self_affected": credential_id == principal["id"],
            "principal_preserved": True,
        }

    def _rotate(self, principal, record, principal_row, key, payload_hash):
        credential_id = record["principal"]
        if self._state(record, principal_row) != "active":
            raise Problem("credential_not_active", 409)
        if principal_row is None:
            raise Problem("not_found", 404)
        token = secrets.token_urlsafe(48)
        from .api import hash_token
        sessions = self._session_count(credential_id)
        # One statement swaps the hash: the previous token stops authenticating at the
        # same instant the new one starts, and nothing else about the credential moves.
        self.store.db.execute("UPDATE principals SET token_hash=? WHERE id=?",
                              (hash_token(token), credential_id))
        self.store.db.execute("DELETE FROM sessions WHERE principal=?", (credential_id,))
        self.store.db.execute(
            "UPDATE credential_records SET revision=revision+1, rotated_at=?, "
            "rotation_count=rotation_count+1 WHERE principal=?", (now(), credential_id))
        self.store.db.execute("INSERT INTO audit VALUES(?,?,?,?,?)",
                              (uid("aud"), principal["id"], "credential.rotate", credential_id, now()))
        self.store.tick(record["project"])
        self._record_operation(principal, "rotate", credential_id, key, payload_hash, 1,
                               {"status": "rotated", "token_available": True,
                                "note": "A replacement token was returned exactly once in the response to "
                                        "this command and is not recoverable."})
        current, current_row = self._record(credential_id)
        return {
            "status": "rotated",
            "token": token,
            "token_available": True,
            "token_notice": "Shown once. Hivemind never stores, logs, journals or returns this token "
                            "again; retrying this command reports 'already applied, token unavailable'.",
            "credential": self.view(principal, current, current_row),
            "sessions_invalidated": sessions,
            "self_affected": credential_id == principal["id"],
            "old_token_invalidated": True,
        }

    def _replay(self, principal, operation, record, principal_row):
        """Safe retry receipt: no token, ever, for the same idempotency key."""
        notice = ("This rotation was already applied. The replacement token was returned once and "
                  "cannot be shown again; rotate again with the current revision if you need a new token."
                  if operation == "rotate" else
                  "This revocation was already applied. The credential is terminated and cannot be "
                  "restored; issue a new credential out of band if this principal must act again.")
        return {
            "status": "already_applied",
            "revoked": operation == "revoke",
            "token_available": False,
            "token_notice": notice,
            "effective_state": self._state(record, principal_row),
            "credential": self.view(principal, record, principal_row),
            "sessions_invalidated": 0,
            "self_affected": record["principal"] == principal["id"],
            "old_token_invalidated": True,
        }


OPERATION_DESCRIPTORS = {
    "credential.list": {
        "name": "credential.list",
        "purpose": "Administrator inventory of canonical principal credentials: role, agent, "
                   "revocation state, CAS revision, session count and rotation history.",
        "input": "GET /v1/projects/{project_id}/credentials?role=&agent_id=&limit=",
        "output": "Envelope data {project_id, credentials[], count, limit, has_more, summary, policy}. "
                  "Raw tokens and stored token hashes are never returned.",
        "authority": "Current administrator credential with access to the named project, re-verified "
                     "under the canonical lock; exact role/agent scope filters are project-bound.",
        "side_effects": "None beyond idempotent creation of the module's own credential ledger rows (no "
                        "token material, no project-sequence change).",
        "limits": f"At most {MAX_CREDENTIALS} credentials per page; metadata only.",
        "retry_safety": "Read-only; safe to repeat. Compare each credential's revision before mutating.",
    },
    "credential.impact": {
        "name": "credential.impact",
        "purpose": "Describe exactly what a rotation or revocation would affect right now, and return "
                   "the fingerprint a confirmed mutation must acknowledge.",
        "input": "GET /v1/credentials/{credential_id}/impact?operation=rotate|revoke",
        "output": "Envelope data with counters and identifiers: active_sessions, active_runs, "
                  "owned_workers, self_affected, last_usable_admin, usable_admins, warnings, "
                  "fingerprint. No token material.",
        "authority": "Administrator with access to the credential's project.",
        "side_effects": "None (read-only projection).",
        "limits": f"Active runs and owned workers are truncated at {MAX_IMPACT_RUNS}/{MAX_IMPACT_WORKERS} "
                  "with explicit truncation flags; presence is a 15s/30s window, not an attestation.",
        "retry_safety": "Read-only. The fingerprint expires with the impact it describes; re-review after "
                        "any 412 or 409 rather than reusing an old acknowledgement.",
    },
    "credential.rotate": {
        "name": "credential.rotate",
        "purpose": "Replace one principal's stored token hash atomically, invalidate the previous token "
                   "and its sessions, and return the new random token exactly once.",
        "input": "POST /v1/credentials/{credential_id}:rotate with {expected_revision, "
                 "acknowledged_impact} and a stable Idempotency-Key.",
        "output": "Envelope data {status, token, token_available, token_notice, credential, "
                  "sessions_invalidated, self_affected}. A retry with the same key and body returns "
                  "{status: 'already_applied', token_available: false} and never a token.",
        "authority": "Administrator with access to the credential's project, re-verified together with its "
                     "current token_hash/role/project/agent/coordinator/capture authority inside the "
                     "mutation transaction. Role, project, agent and grants are never changed, and no "
                     "session is created for the target principal.",
        "side_effects": "Updates principals.token_hash, deletes that principal's sessions (including the "
                        "caller's own when self-rotating), writes an audit entry and a safe idempotency "
                        "receipt, and advances the project sequence. The token itself is never written "
                        "anywhere.",
        "limits": "One credential per command; revision plus acknowledged impact are both required. "
                  "The replacement token is displayed once and cannot be recovered. The rotating "
                  "administrator receives a working credential for the target principal, which is "
                  "inherent to rotation and no broader than the administrator's existing authority.",
        "retry_safety": "Command-scoped idempotency: reuse the exact same key and body to receive the safe "
                        "'already_applied' receipt. A new key with a stale revision fails 412 rather than "
                        "silently rotating twice.",
    },
    "credential.revoke": {
        "name": "credential.revoke",
        "purpose": "Terminally invalidate one principal credential without deleting the principal, its "
                   "resources or the work it owns.",
        "input": "POST /v1/credentials/{credential_id}:revoke with {expected_revision, "
                 "acknowledged_impact} and an Idempotency-Key.",
        "output": "Envelope data {status: 'revoked', revoked: true, token_available: false, "
                  "effective_state, credential, sessions_invalidated, self_affected, "
                  "principal_preserved}. A retry with the same key and body returns "
                  "{status: 'already_applied', revoked: true, token_available: false}.",
        "authority": "Administrator with access to the credential's project; the last usable administrator "
                     "credential is refused with last_usable_admin_protected.",
        "side_effects": "Writes the non-secret revocation marker into principals.token_hash, deletes that "
                        "principal's sessions, records revocation in the credential ledger and audit, and "
                        "advances the project sequence. No resource, memory, message, claim or task is "
                        "deleted.",
        "limits": "Terminal: a revoked credential cannot be rotated back to life; the operator issues a "
                  "replacement credential out of band instead.",
        "retry_safety": "Command-scoped idempotency with re-authorization: the same key and body returns the "
                        "safe receipt only while the caller is still an authorized administrator. A new key "
                        "with a stale revision fails 412, and an already-revoked credential reports 409 "
                        "credential_already_revoked.",
    },
}


def install(app, service, principal, command, envelope):
    """Register the administrator credential surface with the integration layer.

    ``command`` is accepted for the standard capability seam but intentionally
    unused: both mutations use :meth:`CredentialManagement.mutate`, which
    re-authorizes a retry instead of replaying a cached response and keeps token
    material out of the canonical command journal.
    """
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    credentials = CredentialManagement(service)
    app.state.credential_management = credentials
    service.credential_management = credentials
    store = service.store
    endpoints = []

    @app.get("/v1/projects/{project_id}/credentials")
    async def credentials_list(request: Request, project_id: str,
                               role: Literal["admin", "human", "observer", "agent"] | None = Query(None),
                               agent_id: str | None = Query(None, max_length=128),
                               limit: int = Query(100, ge=1, le=MAX_CREDENTIALS),
                               p=Depends(principal)):
        """Administrator credential inventory; never returns tokens or token hashes."""
        with store.lock:
            p = app.state.authenticate_request(request)
            return envelope(credentials.listing(p, project_id, role, agent_id, limit))

    @app.get("/v1/credentials/{credential_id}")
    async def credential_read(request: Request, credential_id: str, p=Depends(principal)):
        """One credential's metadata within its canonical project scope."""
        with store.lock:
            p = app.state.authenticate_request(request)
            return envelope(credentials.read(p, credential_id))

    @app.get("/v1/credentials/{credential_id}/impact")
    async def credential_impact(request: Request, credential_id: str,
                                operation: Literal["rotate", "revoke"] = Query("revoke"),
                                p=Depends(principal)):
        """Current impact of a rotation or revocation plus the fingerprint to acknowledge."""
        with store.lock:
            p = app.state.authenticate_request(request)
            return envelope(credentials.impact(p, credential_id, operation))

    @app.post("/v1/credentials/{credential_id}:rotate")
    async def credential_rotate(request: Request, credential_id: str, body: CredentialConfirmation,
                                p=Depends(principal), idempotency_key: str | None = Header(None)):
        """Replace the token hash atomically; the new token is returned once and never journalled."""
        with store.lock:
            p = app.state.authenticate_request(request)
            return envelope(credentials.mutate(p, "rotate", credential_id, body, idempotency_key))

    @app.post("/v1/credentials/{credential_id}:revoke")
    async def credential_revoke(request: Request, credential_id: str, body: CredentialConfirmation,
                                p=Depends(principal), idempotency_key: str | None = Header(None)):
        """Terminally invalidate the credential and its sessions; preserve the principal."""
        with store.lock:
            p = app.state.authenticate_request(request)
            return envelope(credentials.mutate(p, "revoke", credential_id, body, idempotency_key))

    endpoints.append("GET /v1/projects/{project_id}/credentials")
    endpoints.append("GET /v1/credentials/{credential_id}")
    endpoints.append("GET /v1/credentials/{credential_id}/impact")
    endpoints.append("POST /v1/credentials/{credential_id}:rotate")
    endpoints.append("POST /v1/credentials/{credential_id}:revoke")
    return {"handle": credentials, "endpoints": endpoints, "models": list(MODELS)}
