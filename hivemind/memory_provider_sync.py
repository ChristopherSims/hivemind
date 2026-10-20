"""Bounded data-only provider reads, proposed imports and owned withdrawal."""
import asyncio
import hashlib
import json
import threading
import time
from contextlib import contextmanager
from typing import Literal

from fastapi import Depends, Header
from pydantic import Field, model_validator

from . import models as m
from .storage import Problem, canonical


class ProviderRetrieve(m.Model):
    adapter: Literal["mem0", "langmem", "letta"]
    query: str = Field(default="", max_length=4096)
    limit: int = Field(default=10, ge=1, le=20)


class ProviderImport(m.Model):
    result_id: m.ID
    item_index: int = Field(ge=0, le=19)
    content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    title: str = Field(min_length=1, max_length=512)
    approve_import: bool = Field(json_schema_extra={"const": True})
    evidence_refs: list[m.Ref] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def approval(self):
        if not self.approve_import:
            raise ValueError("Explicit import approval is required")
        return self


class ProviderAdapterRequest(m.Model):
    adapter: Literal["mem0", "langmem", "letta"]


def missing_read(function):
    try:
        return function()
    except Exception as error:
        status = getattr(error, "status_code", None)
        response = getattr(error, "response", None)
        if status == 404 or getattr(response, "status_code", None) == 404:
            return None
        raise


def delete_ack(value, identity):
    if value is None:
        return
    if isinstance(value, dict) and not value.get("error"):
        if value.get("id") == identity or "deleted" in str(value.get("message", "")).casefold():
            return
    if not isinstance(value, dict) and getattr(value, "id", None) == identity:
        return
    raise ValueError("Unsupported provider deletion acknowledgement")


def _value(value, key, default=None):
    return value.get(key, default) if isinstance(value, dict) else getattr(value, key, default)


def normalize_record(value, name, namespace=None):
    if value is None:
        return None
    identity = _value(value, "key" if name == "langmem" else "id")
    if name == "langmem":
        if tuple(_value(value, "namespace", ())) != namespace:
            raise ValueError("Provider returned another namespace")
        body = _value(value, "value", {})
        text, metadata = _value(body, "content"), _value(body, "metadata", {})
    else:
        text = _value(value, "memory" if name == "mem0" else "value")
        metadata = _value(value, "metadata", {})
    if (not isinstance(identity, str) or not 1 <= len(identity) <= 256
            or not isinstance(text, str) or not isinstance(metadata, dict)):
        raise ValueError("Unsupported provider record")
    return {"id": identity, "text": text, "metadata": metadata}


def normalize_page(value, name, limit, namespace=None):
    if isinstance(value, list):
        items = value
    else:
        items = _value(value, "results", _value(value, "items", _value(value, "data")))
    # Never iterate an auto-pager, warm an index or silently fetch another page.
    if not isinstance(items, list) or len(items) > limit:
        raise ValueError("Unsupported bounded provider page")
    result = [normalize_record(item, name, namespace) for item in items]
    if any(item is None for item in result):
        raise ValueError("Unsupported provider page")
    return result


class ProviderSync:
    def __init__(self, bridge):
        self.bridge, self.store, self.service = bridge, bridge.store, bridge.service
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = None
        self._unsubscribe = None
        advanced = any((self.policy.allow_remote_reads, self.policy.allow_imports,
                        self.policy.allow_remote_deletes, self.policy.auto_withdraw))
        if advanced and any(not isinstance(getattr(adapter, "binding_id", None), str)
                or not 1 <= len(adapter.binding_id) <= 128 for adapter in bridge.adapters.values()):
            raise ValueError("Advanced memory integration requires an explicit stable binding_id")
        columns = {row["name"] for row in self.store.db.execute("PRAGMA table_info(memory_mirrors)")}
        for name in ("binding", "snapshot_hash", "content_hash", "owner", "last_sync_state"):
            if name not in columns:
                self.store.db.execute(f"ALTER TABLE memory_mirrors ADD COLUMN {name} TEXT")
        self.store.db.executescript("""
            CREATE TABLE IF NOT EXISTS memory_provider_deletions(
              effect_key TEXT PRIMARY KEY,state TEXT NOT NULL,reason TEXT,updated REAL);
            CREATE TABLE IF NOT EXISTS memory_provider_imports(
              fingerprint TEXT PRIMARY KEY,memory TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS memory_provider_cursors(
              scope TEXT PRIMARY KEY,cursor INTEGER NOT NULL);
        """)
        self.store.memory_provider_result_filter = self.visible_result
        if self.policy.auto_withdraw:
            self._unsubscribe = self.store.subscribe_commits(lambda changes: self._wake.set())
            self._thread = threading.Thread(target=self._run, name="memory-provider-withdrawal", daemon=True)
            self._thread.start()

    @property
    def policy(self):
        return self.bridge.policy

    @staticmethod
    def binding(adapter):
        return hashlib.sha256(canonical([adapter.name, adapter.project_id, adapter.namespace,
                                        adapter.binding_id]).encode()).hexdigest()

    @contextmanager
    def activity(self):
        with self.bridge.condition:
            if self.bridge.closing:
                raise Problem("memory_adapter_closing", 503)
            self.bridge.active += 1
        try:
            yield
        finally:
            with self.bridge.condition:
                self.bridge.active -= 1
                self.bridge.condition.notify_all()

    def operator(self, principal, project):
        row = self.store.db.execute("SELECT * FROM principals WHERE id=?", (principal["id"],)).fetchone()
        if row is None or any(row[key] != principal[key] for key in ("role", "project", "agent", "token_hash")):
            raise Problem("authentication_required", 401)
        self.service.project_access(principal, project)
        if principal["role"] not in ("admin", "human"):
            raise Problem("memory_provider_operator_required", 403)

    def adapter(self, name, project):
        adapter = self.bridge.adapters.get(name)
        if adapter is None or adapter.project_id != project or not adapter.binding_id:
            raise Problem("memory_adapter_unavailable", 404)
        return adapter

    def scoped(self, adapter, record):
        meta = record["metadata"]
        if (meta.get("hivemind_project") != adapter.project_id
                or meta.get("hivemind_namespace") != adapter.namespace
                or meta.get("hivemind_binding") != self.binding(adapter)):
            raise Problem("memory_provider_scope_mismatch", 409)
        text = record["text"]
        if not text or len(text.encode()) > self.policy.max_transfer_bytes:
            raise Problem("memory_provider_record_too_large", 409)

    def source_current(self, row):
        owner = self.store.db.execute("SELECT * FROM principals WHERE id=?", (row["owner"],)).fetchone()
        if (not owner or owner["role"] not in ("admin", "human")
                or owner["role"] != "admin" and owner["project"] != row["project"]
                or not self.policy.enabled or not self.policy.allow_remote_writes):
            return False
        try:
            memory = self.bridge.public_snapshot(row["memory"])
            return hashlib.sha256(canonical(memory).encode()).hexdigest() == row["snapshot_hash"]
        except Problem:
            return False

    def candidate(self, adapter, record):
        self.scoped(adapter, record)
        effect = record["metadata"].get("hivemind_key")
        mirror = self.store.db.execute("SELECT * FROM memory_mirrors WHERE effect_key=?", (effect,)).fetchone()
        if mirror is not None and (mirror["binding"] != self.binding(adapter)
                or mirror["state"] != "done" or not self.source_current(mirror)):
            raise Problem("memory_provider_source_unavailable", 409)
        return {"record_id": record["id"], "text": record["text"],
                "content_sha256": hashlib.sha256(record["text"].encode()).hexdigest(),
                "mirror_effect": effect if mirror else None, "trust": "provider_reported",
                "canonical_authority": False}

    def retrieve(self, principal, project, request, key):
        with self.activity():
            if not key or len(key) > 128:
                raise Problem("idempotency_key_required", 422)
            if not self.policy.enabled or not self.policy.allow_remote_reads:
                raise Problem("memory_provider_reads_disabled", 403)
            with self.store.lock:
                self.operator(principal, project)
                adapter = self.adapter(request.adapter, project)
            try:
                records = adapter.list_records(request.limit)
            except Exception:
                raise Problem("memory_provider_read_failed", 409) from None
            def commit():
                self.operator(principal, project)
                if not self.policy.enabled or not self.policy.allow_remote_reads:
                    raise Problem("memory_provider_reads_disabled", 403)
                if self.adapter(request.adapter, project) is not adapter:
                    raise Problem("memory_adapter_binding_changed", 409)
                items, byte_size = [], 0
                for record in records:
                    try:
                        item = self.candidate(adapter, record)
                    except Problem as error:
                        if error.code == "memory_provider_source_unavailable":
                            continue
                        raise
                    byte_size += len(item["text"].encode())
                    if byte_size > self.policy.max_transfer_bytes:
                        raise Problem("memory_provider_result_too_large", 409)
                    if request.query.casefold() in item["text"].casefold():
                        items.append(item)
                return self.service.create(principal, "memory_provider_result", {
                    "project_id": project, "adapter": adapter.name, "binding": self.binding(adapter),
                    "items": items, "expires_at": time.time() + self.policy.result_ttl_seconds,
                    "canonical_authority": False})
            result = self.service.mutate(principal, "memory-provider-retrieve:" + project, key,
                                         request.model_dump(), commit)
            return self.result(principal, result["id"])

    def result(self, principal, identity):
        with self.store.lock:
            return self._result(principal, identity)

    def _result(self, principal, identity):
        body = json.loads(self.store.raw(identity, "memory_provider_result")["body"])
        self.operator(principal, body["project_id"])
        if not self.policy.enabled or not self.policy.allow_remote_reads:
            raise Problem("memory_provider_reads_disabled", 403)
        if body["expires_at"] <= time.time():
            raise Problem("memory_provider_result_expired", 409)
        adapter = self.adapter(body["adapter"], body["project_id"])
        if body["binding"] != self.binding(adapter):
            raise Problem("memory_adapter_binding_changed", 409)
        for item in body["items"]:
            if item["mirror_effect"]:
                row = self.store.db.execute("SELECT * FROM memory_mirrors WHERE effect_key=?",
                                           (item["mirror_effect"],)).fetchone()
                if not row or row["state"] != "done" or not self.source_current(row):
                    raise Problem("memory_provider_source_unavailable", 409)
        return self.store.get(identity, principal, "memory_provider_result")

    def visible_result(self, row, principal):
        if not self.policy.enabled or not self.policy.allow_remote_reads or self.bridge.closing:
            return False
        try:
            body = json.loads(row["body"])
            self.operator(principal, body["project_id"])
            adapter = self.adapter(body["adapter"], body["project_id"])
            if body["expires_at"] <= time.time() or body["binding"] != self.binding(adapter):
                return False
            for item in body["items"]:
                if item["mirror_effect"]:
                    mirror = self.store.db.execute("SELECT * FROM memory_mirrors WHERE effect_key=?",
                                                  (item["mirror_effect"],)).fetchone()
                    if not mirror or mirror["state"] != "done" or not self.source_current(mirror):
                        return False
            return True
        except (Problem, KeyError, TypeError, ValueError):
            return False

    def import_memory(self, principal, project, request, key):
        with self.activity():
            if not key or len(key) > 128:
                raise Problem("idempotency_key_required", 422)
            if not self.policy.enabled or not self.policy.allow_imports or not self.policy.allow_remote_reads:
                raise Problem("memory_provider_imports_disabled", 403)
            with self.store.lock:
                self.operator(principal, project)
                result = self.result(principal, request.result_id)
                if result["project_id"] != project or request.item_index >= len(result["items"]):
                    raise Problem("not_found", 404)
                item = result["items"][request.item_index]
                if item["content_sha256"] != request.content_sha256:
                    raise Problem("memory_provider_content_changed", 409)
                adapter = self.adapter(result["adapter"], project)
                refs = [ref.model_dump(exclude_none=True) for ref in request.evidence_refs]
                self.service.evidence(principal, project, refs)
                for ref in refs:
                    ref.setdefault("revision", self.store.raw(ref["id"])["revision"])
            try:
                current = adapter.get(item["record_id"])
            except Exception:
                raise Problem("memory_provider_read_failed", 409) from None
            if current is None:
                raise Problem("memory_provider_record_unavailable", 409)
            def commit():
                self.operator(principal, project)
                if not self.policy.enabled or not self.policy.allow_imports or not self.policy.allow_remote_reads:
                    raise Problem("memory_provider_imports_disabled", 403)
                self.result(principal, request.result_id)
                if self.adapter(result["adapter"], project) is not adapter:
                    raise Problem("memory_adapter_binding_changed", 409)
                if self.candidate(adapter, current) != item:
                    raise Problem("memory_provider_content_changed", 409)
                self.service.evidence(principal, project, refs)
                fingerprint = hashlib.sha256(canonical({
                    "project": project, "binding": result["binding"], "item": item,
                    "title": request.title, "refs": refs}).encode()).hexdigest()
                old = self.store.db.execute("SELECT memory FROM memory_provider_imports WHERE fingerprint=?",
                                            (fingerprint,)).fetchone()
                if old:
                    return self.service.memory_get(principal, old["memory"])
                memory = self.service.memory(principal, m.MemoryCreate(
                    project_id=project, title=request.title, scope_owner_id=project,
                    content=m.Content(media_type="text/plain", text=item["text"]),
                    evidence_refs=[m.Ref(**ref) for ref in refs]), provider={
                        "adapter": result["adapter"], "binding": result["binding"],
                        "record_id": item["record_id"], "content_sha256": item["content_sha256"]})
                self.store.db.execute("INSERT INTO memory_provider_imports VALUES(?,?)",
                                      (fingerprint, memory["id"]))
                return memory
            return self.service.mutate(principal, "memory-provider-import:" + project, key,
                                       request.model_dump(), commit)

    @staticmethod
    def ids(row):
        receipt = json.loads(row["receipt"])
        identities = receipt.get("ids") if row["adapter"] == "mem0" else [receipt.get("id")]
        if (not isinstance(identities, list) or not 1 <= len(identities) <= 32
                or len(set(identities)) != len(identities)
                or any(not isinstance(value, str) or not 1 <= len(value) <= 256 for value in identities)):
            raise Problem("memory_provider_receipt_invalid", 409)
        return identities

    def _done(self, effect, reason):
        with self.store.transaction():
            self.store.db.execute(
                "UPDATE memory_provider_deletions SET state='done',reason=?,updated=? WHERE effect_key=?",
                (reason, time.time(), effect))
            self.store.db.execute("UPDATE memory_mirrors SET state='withdrawn' WHERE effect_key=?", (effect,))

    def withdraw(self, row, adapter, principal=None):
        effect = row["effect_key"]
        if row["binding"] != self.binding(adapter) or not row["receipt"]:
            return "binding_or_receipt_hold"
        with self.store.lock:
            prior = self.store.db.execute("SELECT * FROM memory_provider_deletions WHERE effect_key=?",
                                         (effect,)).fetchone()
            if prior and prior["state"] == "done":
                return "withdrawn"
        try:
            identities = self.ids(row)
            records = [adapter.get(identity) for identity in identities]
            if all(record is None for record in records):
                with self.store.transaction():
                    if principal is not None:
                        self.operator(principal, row["project"])
                    if (not self.policy.enabled or not self.policy.allow_remote_deletes
                            or self.bridge.adapters.get(adapter.name) is not adapter
                            or row["binding"] != self.binding(adapter)):
                        return "binding_or_consent_hold"
                    self.store.db.execute(
                        "INSERT OR IGNORE INTO memory_provider_deletions VALUES(?,'reserved',NULL,?)",
                        (effect, time.time()))
                self._done(effect, "absence_observed")
                return "withdrawn"
            if prior:
                return "unknown_hold"  # Reads may prove absence, never authorize a repeat delete.
            for identity, record in zip(identities, records, strict=True):
                if record is None:
                    continue
                self.scoped(adapter, record)
                if (record["id"] != identity or record["metadata"].get("hivemind_key") != effect
                        or record["metadata"].get("content_sha256") != row["content_hash"]
                        or hashlib.sha256(record["text"].encode()).hexdigest() != row["content_hash"]):
                    raise Problem("memory_provider_ownership_mismatch", 409)
        except Exception:
            return "ownership_or_read_hold"  # No delete was admitted.
        with self.store.transaction():
            if principal is not None:
                self.operator(principal, row["project"])
            if self._stop.is_set() or self.bridge.closing:
                return "closing_hold"
            if not self.policy.enabled or not self.policy.allow_remote_deletes:
                return "consent_hold"
            if (self.bridge.adapters.get(adapter.name) is not adapter
                    or row["binding"] != self.binding(adapter)):
                return "binding_or_receipt_hold"
            if self.source_current(row):
                return "current"
            admitted = self.store.db.execute(
                "INSERT OR IGNORE INTO memory_provider_deletions VALUES(?,'reserved',NULL,?)",
                (effect, time.time())).rowcount
            if admitted != 1:
                return "unknown_hold"
        try:
            # One admission covers the entire receipt: never repeat partially
            # successful multi-ID deletes. Already-absent IDs need no SDK write.
            present = [identity for identity, record in zip(identities, records, strict=True) if record is not None]
            receipt = {"ids": present} if row["adapter"] == "mem0" else {"id": present[0]}
            adapter.delete(receipt)
            self._done(effect, "provider_acknowledged")
            return "withdrawn"
        except Exception:
            with self.store.transaction():
                self.store.db.execute(
                    "UPDATE memory_provider_deletions SET state='unknown',reason='provider_outcome_unknown',"
                    "updated=? WHERE effect_key=? AND state='reserved'", (time.time(), effect))
            return "unknown_hold"

    def reconcile(self, principal=None, project=None, name=None):
        with self.activity():
            if not self.policy.enabled or not self.policy.allow_remote_deletes:
                raise Problem("memory_provider_deletes_disabled", 403)
            if principal is None and not self.policy.auto_withdraw:
                raise Problem("memory_provider_automatic_withdrawal_disabled", 403)
            scope = project + ":" + name if project and name else "automatic"
            with self.store.lock:
                if principal is not None:
                    self.operator(principal, project)
                    self.adapter(name, project)
                old = self.store.db.execute("SELECT cursor FROM memory_provider_cursors WHERE scope=?",
                                            (scope,)).fetchone()
                cursor = old["cursor"] if old else 0
                sql, args = "SELECT rowid AS scan_id,* FROM memory_mirrors WHERE rowid>?", [cursor]
                if project:
                    sql += " AND project=? AND adapter=?"
                    args.extend((project, name))
                rows = self.store.db.execute(sql + " ORDER BY rowid LIMIT ?", (*args, self.policy.batch_size)).fetchall()
            outcomes = []
            for row in rows:
                if self._stop.is_set() or self.bridge.closing:
                    break
                if row["state"] == "withdrawn":
                    continue
                with self.store.lock:
                    if principal is not None:
                        self.operator(principal, project)
                    adapter = self.bridge.adapters.get(row["adapter"])
                    if not adapter or adapter.project_id != row["project"]:
                        outcomes.append("adapter_hold")
                        continue
                    stale = not self.source_current(row)
                if stale:
                    outcome = self.withdraw(row, adapter, principal)
                    outcomes.append(outcome)
                    with self.store.transaction():
                        self.store.db.execute("UPDATE memory_mirrors SET last_sync_state=? WHERE effect_key=?",
                                              (outcome, row["effect_key"]))
            with self.store.transaction():
                self.store.db.execute(
                    "INSERT INTO memory_provider_cursors VALUES(?,?) ON CONFLICT(scope) DO UPDATE SET cursor=excluded.cursor",
                    (scope, rows[-1]["scan_id"] if rows else 0))
            return {"examined": len(rows), "outcomes": outcomes, "canonical_authority": False,
                    "physical_erasure_verified": False}

    def status(self, principal, project):
        with self.store.lock:
            self.operator(principal, project)
            rows = self.store.db.execute(
                "SELECT m.effect_key,m.adapter,m.memory,m.state,m.last_sync_state,d.state AS withdrawal_state,d.reason "
                "FROM memory_mirrors m LEFT JOIN memory_provider_deletions d ON d.effect_key=m.effect_key "
                "WHERE m.project=? ORDER BY m.rowid DESC LIMIT ?", (project, self.policy.batch_size)).fetchall()
            return {"mirrors": [dict(row) for row in rows], "physical_erasure_verified": False,
                    "automatic_withdrawal": self.policy.auto_withdraw,
                    "worker_alive": bool(self._thread and self._thread.is_alive())}

    def _run(self):
        due = time.monotonic()
        while not self._stop.is_set():
            self._wake.wait(max(0, due - time.monotonic()))
            self._wake.clear()
            if self._stop.wait(max(0, due - time.monotonic())):
                break
            if self._stop.is_set():
                break
            try:
                self.reconcile()
            except Exception:
                pass  # Bounded periodic catch-up retries reads, never uncertain writes.
            due = time.monotonic() + self.policy.poll_interval_seconds

    def stop(self):
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None
        self._stop.set()
        self._wake.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(self.policy.join_timeout_seconds)


def install_routes(app, bridge, principal, envelope):
    from .agent_feature_gates import set_resource_projection
    set_resource_projection(bridge.service, "memory_provider_result", bridge.sync.result)
    @app.post("/v1/projects/{project_id}/memory-providers:retrieve")
    async def retrieve(project_id: str, body: ProviderRetrieve, p=Depends(principal),
                       idempotency_key: str | None = Header(None)):
        return envelope(await asyncio.to_thread(bridge.sync.retrieve, p, project_id, body, idempotency_key))

    @app.get("/v1/memory-provider-results/{result_id}")
    async def result(result_id: str, p=Depends(principal)):
        return envelope(bridge.sync.result(p, result_id))

    @app.post("/v1/projects/{project_id}/memory-providers:import")
    async def import_memory(project_id: str, body: ProviderImport, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        return envelope(await asyncio.to_thread(bridge.sync.import_memory, p, project_id, body, idempotency_key))

    @app.post("/v1/projects/{project_id}/memory-providers:reconcile")
    async def reconcile(project_id: str, body: ProviderAdapterRequest, p=Depends(principal)):
        return envelope(await asyncio.to_thread(bridge.sync.reconcile, p, project_id, body.adapter))

    @app.get("/v1/projects/{project_id}/memory-providers:status")
    async def status(project_id: str, p=Depends(principal)):
        return envelope(bridge.sync.status(p, project_id))

    for endpoint in (retrieve, import_memory, reconcile):
        endpoint.hivemind_detached_work = True
