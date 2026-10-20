import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path

import sqlite_vec
from .models import utc_timestamp


def uid(prefix):
    # RFC 9562 UUIDv7, with cryptographically random tail bits.
    ms = int(time.time() * 1000)
    value = (ms << 80) | (7 << 76) | (secrets.randbits(12) << 64) | (2 << 62) | secrets.randbits(62)
    return f"{prefix}_{uuid.UUID(int=value)}"


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class Problem(Exception):
    def __init__(self, code, status=409):
        self.code = code
        self.status = status


# Durable offline restore-hold files at a restored store root. The sidecar is
# written by coordinated_operations at restore time; the staged release intent
# is owned by hivemind.restore_release. Names are duplicated here (rather than
# imported) to avoid an import cycle.
RESTORE_HOLD_SIDECAR = "restore-hold.json"
RESTORE_RELEASE_INTENT = ".restore-release-intent.json"


class Store:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self._commit_listeners = {}
        self._changed_projects = None
        self.commit_listener_failures = 0
        self.memory_freshness_filter = None
        self.memory_provider_result_filter = None
        self._actor = ContextVar("canonical_actor", default=None)
        # Durable restore-hold guard, BEFORE opening (which would create) the
        # database: a leftover staged release intent or the sidecar alone (meta
        # marker possibly already removed by a partial release) both mean the
        # hold state is unresolved and this store must never serve. The exact
        # meta-row check below is preserved unchanged for the normal case.
        if os.path.lexists(self.directory / RESTORE_RELEASE_INTENT):
            raise Problem("restore_release_incomplete", 409)
        if os.path.lexists(self.directory / RESTORE_HOLD_SIDECAR):
            raise Problem("restore_hold_active", 409)
        self.db = sqlite3.connect(self.directory / "canonical.sqlite", check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.enable_load_extension(True)
        sqlite_vec.load(self.db)
        self.db.enable_load_extension(False)
        # Restored snapshots carry a restore_hold marker: their old Nexus/delivery
        # lease authority was invalidated offline and must never serve again until an
        # operator explicitly releases the hold after fresh verification.
        marker_table = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
        if marker_table and self.db.execute(
                "SELECT 1 FROM meta WHERE key='restore_hold'").fetchone():
            self.db.close()
            raise Problem("restore_hold_active", 409)
        try:
            self._initialize_schema()
        except BaseException:
            self.db.close()
            raise
        from .team_access import TeamAccess
        self.team_access = TeamAccess(self)

    def _initialize_schema(self):
        existing = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='migrations' AND type='table'").fetchone()
        if existing and (self.db.execute("SELECT max(version) FROM migrations").fetchone()[0] or 0) > 6:
            raise Problem("schema_version_unsupported")
        self.db.executescript("""
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS migrations(version INTEGER PRIMARY KEY);
        INSERT OR IGNORE INTO migrations VALUES(1);
        CREATE TABLE IF NOT EXISTS projects(id TEXT PRIMARY KEY, title TEXT NOT NULL, seq INTEGER NOT NULL DEFAULT 0,
          epoch INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS principals(id TEXT PRIMARY KEY, token_hash TEXT UNIQUE NOT NULL, role TEXT NOT NULL,
          project TEXT REFERENCES projects(id), agent TEXT, coordinator INTEGER DEFAULT 0,
          capture_authority INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS resources(id TEXT PRIMARY KEY, kind TEXT NOT NULL,
          project TEXT NOT NULL REFERENCES projects(id), owner TEXT NOT NULL, private_agent TEXT,
          revision INTEGER NOT NULL, seq INTEGER NOT NULL, deleted INTEGER NOT NULL DEFAULT 0, body TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS resources_project_kind ON resources(project,kind,deleted);
        CREATE TABLE IF NOT EXISTS versions(id TEXT, revision INTEGER, body TEXT NOT NULL,
          PRIMARY KEY(id, revision));
        CREATE TABLE IF NOT EXISTS events(project TEXT REFERENCES projects(id), id TEXT,
          seq INTEGER, source TEXT, source_seq INTEGER, hash TEXT, body TEXT, result TEXT,
          PRIMARY KEY(project,id), UNIQUE(project,source,source_seq));
        CREATE TABLE IF NOT EXISTS commands(principal TEXT, operation TEXT, key TEXT, hash TEXT, response TEXT,
          PRIMARY KEY(principal,operation,key));
        CREATE TABLE IF NOT EXISTS jobs(id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, seq INTEGER,
          resource TEXT, revision INTEGER, kind TEXT, state TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0,
          error TEXT, UNIQUE(resource,revision,kind));
        CREATE TABLE IF NOT EXISTS relations(project TEXT, source TEXT, target TEXT, kind TEXT,
          PRIMARY KEY(project,source,target,kind));
        CREATE TABLE IF NOT EXISTS sessions(hash TEXT PRIMARY KEY, principal TEXT REFERENCES principals(id),
          csrf_hash TEXT, expires REAL);
        CREATE TABLE IF NOT EXISTS audit(id TEXT PRIMARY KEY, principal TEXT, operation TEXT, resource TEXT, time TEXT);
        CREATE TABLE IF NOT EXISTS run_task_access(run TEXT,task TEXT,PRIMARY KEY(run,task));
        CREATE TABLE IF NOT EXISTS conversation_reads(principal TEXT REFERENCES principals(id),
          project TEXT REFERENCES projects(id), message TEXT REFERENCES resources(id), read_at TEXT NOT NULL,
          PRIMARY KEY(principal,message));
        CREATE TABLE IF NOT EXISTS memory_extractions(fingerprint TEXT PRIMARY KEY,
          memory TEXT REFERENCES resources(id), agent TEXT NOT NULL);
        CREATE VIRTUAL TABLE IF NOT EXISTS lexical USING fts5(id UNINDEXED, project UNINDEXED, text);
        CREATE TABLE IF NOT EXISTS vector_chunks(id INTEGER PRIMARY KEY, resource TEXT, revision INTEGER,
          project TEXT, chunk INTEGER, text TEXT, UNIQUE(resource,revision,chunk));
        CREATE VIRTUAL TABLE IF NOT EXISTS vectors USING vec0(
          chunk_id INTEGER PRIMARY KEY, embedding float[768], project TEXT partition key);
        """)
        columns = {row["name"] for row in self.db.execute("PRAGMA table_info(projects)").fetchall()}
        if "path_case_sensitive" not in columns:
            self.db.execute(
                f"ALTER TABLE projects ADD COLUMN path_case_sensitive INTEGER NOT NULL DEFAULT {int(os.name != 'nt')}")
            self.db.execute("UPDATE projects SET path_case_sensitive=?", (int(os.name != "nt"),))
        self.db.execute("INSERT OR IGNORE INTO migrations VALUES(2)")
        self.db.execute("INSERT OR IGNORE INTO migrations VALUES(3)")
        self.db.execute("INSERT OR IGNORE INTO migrations VALUES(4)")
        # Retained run observations/events can outlive an assignment. A later
        # move to a public task must not disclose observations from a private one.
        self.db.execute("""
            INSERT OR IGNORE INTO run_task_access(run,task)
            SELECT v.id,json_extract(v.body,'$.task_id') FROM versions v
            JOIN resources r ON r.id=v.id WHERE r.kind='run'
            AND json_type(v.body,'$.task_id')='text'
        """)
        self.db.execute("INSERT OR IGNORE INTO migrations VALUES(5)")
        from .projection_rebuild import install
        install(self.db)
        self.db.execute("INSERT OR IGNORE INTO migrations VALUES(6)")
        self.db.execute("COMMIT")

    @contextmanager
    def acting_as(self, principal):
        token = self._actor.set(principal)
        try:
            yield
        finally:
            self._actor.reset(token)

    @contextmanager
    def transaction(self):
        changes, listeners = {}, ()
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            self._changed_projects = set()
            try:
                yield
                changes = {project: self.project(project)["seq"]
                           for project in self._changed_projects}
                self.db.execute("COMMIT")
                listeners = tuple(self._commit_listeners.values())
            except BaseException:
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
                raise
            finally:
                self._changed_projects = None
        # Nonblocking wake-ups only, after releasing this transaction's lock scope.
        # Durable canonical cursors remain authoritative if a listener misses a signal.
        if changes:
            for listener in listeners:
                try:
                    listener(dict(changes))
                except Exception:
                    self.commit_listener_failures += 1

    def subscribe_commits(self, listener):
        """Subscribe to committed project-sequence invalidations, never message content."""
        if not callable(listener):
            raise ValueError("Commit listener must be callable")
        identity = uid("listener")
        with self.lock:
            self._commit_listeners[identity] = listener

        def unsubscribe():
            with self.lock:
                self._commit_listeners.pop(identity, None)
        return unsubscribe

    def close(self):
        self._commit_listeners.clear()
        self.db.close()

    def project(self, project):
        row = self.db.execute("SELECT * FROM projects WHERE id=?", (project,)).fetchone()
        if not row:
            raise Problem("not_found", 404)
        return dict(row)

    def tick(self, project):
        self.db.execute("UPDATE projects SET seq=seq+1 WHERE id=?", (project,))
        if self._changed_projects is not None:
            self._changed_projects.add(project)
        return self.project(project)["seq"]

    def raw(self, resource_id, kind=None):
        row = self.db.execute("SELECT * FROM resources WHERE id=? AND deleted=0", (resource_id,)).fetchone()
        if not row or kind and row["kind"] != kind:
            raise Problem("not_found", 404)
        return dict(row)

    def visible(self, row, principal):
        if principal["role"] != "admin" and row["project"] != principal["project"]:
            return False
        if row["kind"] == "nexus_grant":
            return principal["role"] in ("admin", "human")
        private = row["private_agent"]
        if private and principal["role"] != "admin" and principal["agent"] != private:
            return False
        if not self.team_access.allowed(row, principal):
            return False
        if row["kind"] == "message":
            body = json.loads(row["body"])
            return principal["role"] in ("admin", "observer") or principal["agent"] in (body["from"], body["to"])
        return True

    def get(self, resource_id, principal, kind=None, review_memory=False):
        row = self.raw(resource_id, kind)
        if not self.visible(row, principal):
            raise Problem("not_found", 404)
        body = json.loads(row["body"])
        if row["kind"] == "task":
            body = self.task_view(body, principal)
        if row["kind"] in ("memory", "memory_proposal") and not self.valid_memory(
                body, principal, check_activity=not review_memory):
            raise Problem("not_found", 404)
        return body

    def task_view(self, body, principal):
        def execution_view(execution):
            if not execution or not execution.get("checkpoint"):
                return execution
            checkpoint = execution["checkpoint"]
            for ref in checkpoint.get("evidence_refs", []):
                try:
                    row = self.raw(ref["id"])
                    if not self.visible(row, principal) or row["kind"] != ref["kind"]:
                        raise Problem("not_found", 404)
                    source = json.loads(row["body"])
                    if row["kind"] == "memory" and not self.valid_memory(source, principal):
                        raise Problem("not_found", 404)
                    if ref.get("revision") != source["revision"]:
                        raise Problem("stale_evidence")
                except Problem:
                    return {**execution, "checkpoint": {"id": checkpoint["id"], "evidence_unavailable": True}}
            return execution
        result = {**body, "execution": execution_view(body.get("execution")),
                  "execution_history": [execution_view(item) for item in body.get("execution_history", [])]}
        if body.get("handoff"):
            result["handoff"] = {**body["handoff"], "checkpoint":
                execution_view({"checkpoint": body["handoff"].get("checkpoint")}).get("checkpoint")}
        return result

    def memory_state(self, body):
        if body.get("invalidation_reason"):
            return "invalidated"
        if body.get("active_dispute_ids"):
            return "disputed"
        if body["status"] not in ("proposed", "verified"):
            return body["status"]
        validity = body.get("validity") or {}
        ends = [value for value in (body.get("expires_at"), validity.get("valid_until")) if value]
        if any(utc_timestamp(value) <= time.time() for value in ends):
            return "expired"
        if validity.get("valid_from") and utc_timestamp(validity["valid_from"]) > time.time():
            return "not_yet_valid"
        return body["status"]

    def valid_memory(self, body, principal, visiting=None, cache=None, check_activity=True):
        if body.get("invalidation_reason"):
            return False
        if check_activity and self.memory_state(body) not in ("proposed", "verified"):
            return False
        if check_activity and self.memory_freshness_filter and not self.memory_freshness_filter(body):
            return False
        visiting = set() if visiting is None else visiting
        cache = {} if cache is None else cache
        identity = body["id"]
        if identity in cache:
            return cache[identity]
        if identity in visiting or len(visiting) >= 32:
            return False
        visiting.add(identity)
        for ref in body.get("evidence_refs", []):
            try:
                row = self.raw(ref["id"])
                if not self.visible(row, principal) or ref.get("revision") and ref["revision"] != row["revision"]:
                    visiting.discard(identity)
                    cache[identity] = False
                    return False
                if row["kind"] == "memory" and not self.valid_memory(
                        json.loads(row["body"]), principal, visiting, cache):
                    visiting.discard(identity)
                    cache[identity] = False
                    return False
                if row["kind"] == "task":
                    task = self.task_view(json.loads(row["body"]), principal)
                    if ((task.get("execution") or {}).get("checkpoint") or {}).get("evidence_unavailable"):
                        visiting.discard(identity)
                        cache[identity] = False
                        return False
            except Problem:
                visiting.discard(identity)
                cache[identity] = False
                return False
        visiting.discard(identity)
        cache[identity] = True
        return True

    def list(self, kind, project, principal, limit=200, review_memory=False):
        rows = self.db.execute(
            "SELECT * FROM resources WHERE kind=? AND project=? AND deleted=0 ORDER BY seq DESC LIMIT ?",
            (kind, project, min(limit, 1000)),
        ).fetchall()
        results = []
        for row in rows:
            if self.visible(row, principal):
                body = json.loads(row["body"])
                if kind == "task":
                    body = self.task_view(body, principal)
                if kind != "memory" or self.valid_memory(body, principal, check_activity=not review_memory):
                    results.append(body)
        return results

    def save(self, kind, project, owner, body, private=None, expected=None, seq=None):
        with self.lock:
            if self.db.in_transaction:
                return self._save(kind, project, owner, body, private, expected, seq)
        with self.transaction():
            return self._save(kind, project, owner, body, private, expected, seq)

    def _save(self, kind, project, owner, body, private=None, expected=None, seq=None):
        if self._changed_projects is not None:
            self._changed_projects.add(project)
        resource_id = body["id"]
        old = self.db.execute("SELECT * FROM resources WHERE id=?", (resource_id,)).fetchone()
        if old and old["deleted"]:
            raise Problem("resource_deleted")
        if expected is not None and (not old or old["revision"] != expected):
            raise Problem("revision_conflict", 412)
        actor = self._actor.get()
        if actor is not None and kind in (
                "board", "board_post", "artifact", "memory", "memory_proposal", "memory_dispute",
                "task", "team", "resource_access", "run", "work_claim"):
            candidate = {"id": resource_id, "kind": kind, "project": project, "private_agent": private,
                         "body": canonical(body)}
            if old:
                self.team_access.require_contribute(dict(old), actor)
                candidate["private_agent"] = old["private_agent"]
            self.team_access.require_contribute(candidate, actor)
        if kind == "run":
            tasks = {body.get("task_id")}
            if old:
                tasks.add(json.loads(old["body"]).get("task_id"))
            tasks.discard(None)
            existing = {row["task"] for row in self.db.execute(
                "SELECT task FROM run_task_access WHERE run=?", (resource_id,))}
            if tasks - existing and len(existing | tasks) > 64:
                raise Problem("run_access_source_limit")
            for task in tasks:
                self.db.execute("INSERT OR IGNORE INTO run_task_access VALUES(?,?)", (resource_id, task))
        revision = old["revision"] + 1 if old else 1
        if kind in ("message", "board_post"):
            first = self.db.execute("SELECT body FROM versions WHERE id=? ORDER BY revision LIMIT 1",
                                    (resource_id,)).fetchone() if old else None
            original = json.loads(first["body"]) if first else None
            body = {**body, "created_seq": original.get("created_seq", original["ingestion_seq"]) if original else
                    (seq if seq is not None else self.project(project)["seq"] + 1)}
        seq = seq if seq is not None else self.tick(project)
        body = {**body, "revision": revision, "updated_at": now(), "ingestion_seq": seq}
        encoded = canonical(body)
        self.db.execute(
            """INSERT INTO resources(id,kind,project,owner,private_agent,revision,seq,body) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,seq=excluded.seq,body=excluded.body""",
            (resource_id, kind, project, owner, private, revision, seq, encoded),
        )
        self.db.execute("INSERT INTO versions VALUES(?,?,?)", (resource_id, revision, encoded))
        self.db.execute("DELETE FROM lexical WHERE id=?", (resource_id,))
        text = searchable(body)
        if kind in ("agent", "board_post", "memory", "task", "work_claim", "artifact"):
            self.db.execute("INSERT INTO lexical VALUES(?,?,?)", (resource_id, project, text))
        for job in ("embedding", "graph"):
            self.db.execute("INSERT INTO jobs(project,seq,resource,revision,kind) VALUES(?,?,?,?,?)",
                            (project, seq, resource_id, revision, job))
        return body

    def delete(self, resource_id, principal, expected):
        with self.lock:
            if self.db.in_transaction:
                return self._delete(resource_id, principal, expected)
        with self.transaction():
            return self._delete(resource_id, principal, expected)

    def _delete(self, resource_id, principal, expected):
        row = self.raw(resource_id)
        # Owners may erase inactive/invalidated memory without reading its content.
        if not self.visible(row, principal):
            raise Problem("not_found", 404)
        self.team_access.require_contribute(row, principal)
        if principal["role"] != "admin" and row["owner"] != principal["id"]:
            raise Problem("forbidden", 403)
        if expected != row["revision"]:
            raise Problem("revision_conflict", 412)
        seq = self.tick(row["project"])
        # Purge content-bearing history synchronously; workers remove derived index rows.
        self.db.execute("UPDATE resources SET deleted=1,body='{}',seq=?,revision=revision+1 WHERE id=?",
                        (seq, resource_id))
        self.db.execute("DELETE FROM versions WHERE id=?", (resource_id,))
        self.db.execute("DELETE FROM conversation_reads WHERE message=?", (resource_id,))
        self.db.execute("DELETE FROM lexical WHERE id=?", (resource_id,))
        self.db.execute("UPDATE commands SET response=? WHERE json_extract(response,'$.id')=?",
                        (canonical({"id": resource_id, "deleted": True}), resource_id))
        for job in ("embedding", "graph"):
            self.db.execute("INSERT INTO jobs(project,seq,resource,revision,kind) VALUES(?,?,?,?,?)",
                            (row["project"], seq, resource_id, row["revision"] + 1, job))
        self.db.execute("INSERT INTO audit VALUES(?,?,?,?,?)",
                        (uid("aud"), principal["id"], "delete", resource_id, now()))
        pending, invalidated = [resource_id], {resource_id}
        while pending:
            source = pending.pop()
            dependents = self.db.execute(
                """SELECT r.* FROM resources r JOIN relations e ON e.source=r.id
                WHERE r.kind='memory' AND r.deleted=0 AND e.kind='SUPPORTED_BY' AND e.target=?""",
                (source,)).fetchall()
            for dependent in dependents:
                if dependent["id"] in invalidated:
                    continue
                memory = json.loads(dependent["body"])
                memory.update(status="disputed", invalidation_reason="source_deleted")
                # Canonical invalidation is a consequence of deletion, not an
                # opportunity for the caller to edit the dependent memory.
                with self.acting_as(None):
                    self.save("memory", dependent["project"], dependent["owner"], memory,
                              private=dependent["private_agent"])
                invalidated.add(dependent["id"])
                pending.append(dependent["id"])
        self.db.execute("DELETE FROM relations WHERE source=? OR target=?", (resource_id, resource_id))

    def relate(self, project, source, target, kind):
        with self.lock:
            if self.db.in_transaction:
                return self._relate(project, source, target, kind)
        with self.transaction():
            return self._relate(project, source, target, kind)

    def _relate(self, project, source, target, kind):
        self.db.execute("INSERT OR IGNORE INTO relations VALUES(?,?,?,?)", (project, source, target, kind))
        row = self.raw(source)
        # New relationship facts must reproject a source even if its node job already completed.
        self.db.execute(
            """INSERT INTO jobs(project,seq,resource,revision,kind) VALUES(?,?,?,?,'graph')
            ON CONFLICT(resource,revision,kind) DO UPDATE SET state='pending',seq=excluded.seq,attempts=0""",
            (project, self.project(project)["seq"], source, row["revision"]))

    def freshness(self, project):
        seq = self.project(project)["seq"]
        result = {"canonical_through_ingestion_seq": seq}
        for kind, name in (("embedding", "vector"), ("graph", "graph")):
            first = self.db.execute(
                "SELECT min(seq),count(*) FROM jobs WHERE project=? AND kind=? AND state!='done'", (project, kind)
            ).fetchone()
            result[f"{name}_through_ingestion_seq"] = first[0] - 1 if first[0] is not None else seq
            result[f"{name}_pending_jobs"] = first[1]
        return result


def searchable(body):
    content = body.get("content", {})
    if content.get("segments"):
        text = " ".join(s.get("text", "") for s in content["segments"])
    else:
        text = content.get("text", "")
    return " ".join(str(body.get(k, "")) for k in ("title", "display_name", "description", "intent")) + " " + text
