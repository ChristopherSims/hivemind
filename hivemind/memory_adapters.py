"""Optional data-only memory mirrors. SQLite remains the authority.

SDK/store objects are injected by operators, not imported from caller names.
No prompts, inference, agent attachment or automatic canonical promotion.
Export is operator-only, default-off, public verified project memory only, with
durable exactly-once-or-unknown intents. SDK calls must have operator timeouts.
"""
import hashlib
import json
import threading
import time
from dataclasses import dataclass
from typing import Literal

from fastapi import Depends, Header
from pydantic import Field

from . import models as m
from .storage import Problem, canonical


@dataclass(frozen=True)
class MemoryAdapterPolicy:
    enabled: bool = False
    allow_remote_writes: bool = False
    max_transfer_bytes: int = 65536
    allow_remote_reads: bool = False
    allow_imports: bool = False
    allow_remote_deletes: bool = False
    auto_withdraw: bool = False
    poll_interval_seconds: float = 5.0
    batch_size: int = 32
    result_ttl_seconds: int = 300
    join_timeout_seconds: float = 5.0

    def __post_init__(self):
        if any(type(value) is not bool for value in (self.enabled, self.allow_remote_writes,
                self.allow_remote_reads, self.allow_imports, self.allow_remote_deletes, self.auto_withdraw)):
            raise ValueError("Memory adapter opt-ins must be strict booleans")
        if type(self.max_transfer_bytes) is not int or not 1 <= self.max_transfer_bytes <= 65536:
            raise ValueError("Memory transfer bound is invalid")
        if self.auto_withdraw and not self.allow_remote_deletes or self.allow_imports and not self.allow_remote_reads:
            raise ValueError("Memory provider consent flags are inconsistent")
        if (type(self.batch_size) is not int or not 1 <= self.batch_size <= 128
                or type(self.result_ttl_seconds) is not int or not 1 <= self.result_ttl_seconds <= 3600
                or type(self.poll_interval_seconds) not in (int, float)
                or not 0.1 <= self.poll_interval_seconds <= 60
                or type(self.join_timeout_seconds) not in (int, float)
                or not 0 <= self.join_timeout_seconds <= 30):
            raise ValueError("Memory provider lifecycle bounds are invalid")


class MemoryMirrorRequest(m.Model):
    adapter: Literal["mem0", "langmem", "letta"]
    expected_revision: int = Field(ge=1)


MODELS = {"MemoryMirrorRequest": MemoryMirrorRequest}


class Mem0Adapter:
    """Memory/MemoryClient data-only add; requires SDK support for infer=False."""
    name = "mem0"

    def __init__(self, client, project_id, namespace, *, binding_id=None):
        self.client, self.project_id, self.namespace = client, project_id, namespace
        self.binding_id = binding_id

    def put(self, key, text, metadata):
        result = self.client.add([{"role": "user", "content": text}],
            user_id=self.namespace, metadata={**metadata, "hivemind_key": key}, infer=False)
        if not isinstance(result, dict) or not isinstance(result.get("results"), list):
            raise ValueError("Unsupported Mem0 acknowledgement")
        ids = [entry["id"] for entry in result["results"]]
        if (not ids or len(ids) > 32
                or any(not isinstance(value, str) or not 1 <= len(value) <= 256 for value in ids)
                or len(set(ids)) != len(ids)):
            raise ValueError("Unsupported Mem0 acknowledgement")
        return {"ids": ids}

    def delete(self, receipt):
        from .memory_provider_sync import delete_ack
        for identity in receipt["ids"]:
            delete_ack(self.client.delete(identity), identity)

    def get(self, identity):
        from .memory_provider_sync import normalize_record, missing_read
        return normalize_record(missing_read(lambda: self.client.get(identity)), "mem0")

    def list_records(self, limit):
        from .memory_provider_sync import normalize_page
        return normalize_page(self.client.get_all(user_id=self.namespace, limit=limit), "mem0", limit)


class LangMemAdapter:
    """LangMem's LangGraph BaseStore namespace port, without model extraction.

    Use this same store and namespace tuple when constructing LangMem tools.
    """
    name = "langmem"

    def __init__(self, store, project_id, namespace, *, binding_id=None):
        self.store, self.project_id, self.namespace = store, project_id, namespace
        self.binding_id = binding_id

    @property
    def store_namespace(self):
        return ("hivemind", self.namespace)

    def put(self, key, text, metadata):
        self.store.put(self.store_namespace, key, {"content": text, "metadata": metadata}, index=False)
        return {"id": key}

    def delete(self, receipt):
        from .memory_provider_sync import delete_ack
        delete_ack(self.store.delete(self.store_namespace, receipt["id"]), receipt["id"])

    def get(self, identity):
        from .memory_provider_sync import normalize_record, missing_read
        return normalize_record(missing_read(lambda: self.store.get(self.store_namespace, identity)), "langmem",
                                namespace=self.store_namespace)

    def list_records(self, limit):
        from .memory_provider_sync import normalize_page
        return normalize_page(self.store.search(self.store_namespace, limit=limit), "langmem", limit,
                              namespace=self.store_namespace)


class LettaAdapter:
    """Standalone Letta blocks, never automatically attached to an agent."""
    name = "letta"

    def __init__(self, client, project_id, namespace, *, binding_id=None):
        self.client, self.project_id, self.namespace = client, project_id, namespace
        self.binding_id = binding_id

    @property
    def label(self):
        return "hivemind_" + hashlib.sha256(canonical([self.project_id, self.namespace]).encode()).hexdigest()[:24]

    def put(self, key, text, metadata):
        block = self.client.blocks.create(
            label=self.label, value=text, limit=max(len(text), 1),
            metadata={**metadata, "hivemind_namespace": self.namespace, "hivemind_key": key})
        identity = block.get("id") if isinstance(block, dict) else getattr(block, "id", None)
        if not isinstance(identity, str) or not 1 <= len(identity) <= 256:
            raise ValueError("Unsupported Letta acknowledgement")
        return {"id": identity}

    def delete(self, receipt):
        from .memory_provider_sync import delete_ack
        delete_ack(self.client.blocks.delete(receipt["id"]), receipt["id"])

    def get(self, identity):
        from .memory_provider_sync import normalize_record, missing_read
        return normalize_record(missing_read(lambda: self.client.blocks.retrieve(identity)), "letta")

    def list_records(self, limit):
        from .memory_provider_sync import normalize_page
        return normalize_page(self.client.blocks.list(label=self.label, limit=limit), "letta", limit)


class MemoryAdapterBridge:
    def __init__(self, service, adapters, policy):
        self.service, self.store, self.policy = service, service.store, policy
        self.adapters = dict(adapters)
        self.condition = threading.Condition()
        self.closing, self.active = False, 0
        if len(adapters) > 32 or any(name != adapter.name or
                not isinstance(adapter.namespace, str) or not 1 <= len(adapter.namespace) <= 128
                or not adapter.project_id for name, adapter in adapters.items()):
            raise ValueError("Invalid fixed memory adapter binding")
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS memory_mirrors("
            "effect_key TEXT PRIMARY KEY,project TEXT,adapter TEXT,memory TEXT,"
            "state TEXT,receipt TEXT)")
        from .memory_provider_sync import ProviderSync
        self.sync = ProviderSync(self)

    def authorize(self, principal, memory_id):
        current = self.store.db.execute("SELECT * FROM principals WHERE id=?", (principal["id"],)).fetchone()
        if current is None or any(current[key] != principal[key] for key in
                                  ("role", "project", "agent", "token_hash")):
            raise Problem("authentication_required", 401)
        if principal["role"] not in ("admin", "human"):
            raise Problem("memory_export_operator_required", 403)
        memory = self.public_snapshot(memory_id)
        self.service.project_access(principal, memory["project_id"])
        return memory

    def public_snapshot(self, memory_id):
        principal = {"role": "admin", "agent": None}
        memory = self.service.memory_get(principal, memory_id)
        if (memory["effective_status"] != "verified" or memory["scope_kind"] != "project"
                or self.store.raw(memory_id)["private_agent"]
                or not self.store.valid_memory(memory, principal)):
            raise Problem("memory_export_source_unavailable", 403)
        public = {"role": "observer", "project": memory["project_id"], "agent": None}
        # Administrative visibility is not consent to disclose private sources.
        pending = [ref["id"] for ref in memory["evidence_refs"]]
        seen = set()
        while pending:
            identity = pending.pop()
            if identity in seen:
                continue
            seen.add(identity)
            if len(seen) > 128:
                raise Problem("memory_export_source_limit", 403)
            row = self.store.raw(identity)
            if row["private_agent"] or not self.store.visible(row, public):
                raise Problem("memory_export_private_source", 403)
            if row["kind"] == "memory":
                pending.extend(ref["id"] for ref in json.loads(row["body"]).get("evidence_refs", []))
        if not self.store.visible(self.store.raw(memory_id), public):
            raise Problem("memory_export_source_unavailable", 403)
        if memory["content"].get("media_type") not in ("text/plain", "text/markdown"):
            raise Problem("memory_export_text_required", 422)
        text = memory["content"].get("text")
        if not isinstance(text, str) or len(text.encode("utf-8")) > self.policy.max_transfer_bytes:
            raise Problem("memory_export_too_large", 422)
        return memory

    def publish(self, principal, memory_id, request, idempotency_key):
        with self.condition:
            if self.closing:
                raise Problem("memory_adapter_closing", 503)
            self.active += 1
        try:
            return self._publish(principal, memory_id, request, idempotency_key)
        finally:
            with self.condition:
                self.active -= 1
                self.condition.notify_all()

    def close(self):
        with self.condition:
            self.closing = True
        self.sync.stop()
        deadline = time.monotonic() + self.policy.join_timeout_seconds
        with self.condition:
            while self.active and time.monotonic() < deadline:
                self.condition.wait(max(0, deadline - time.monotonic()))
            return self.active == 0 and not (self.sync._thread and self.sync._thread.is_alive())

    def _publish(self, principal, memory_id, request, idempotency_key):
        if not self.policy.enabled or not self.policy.allow_remote_writes:
            raise Problem("memory_mirroring_disabled", 403)
        if not idempotency_key or len(idempotency_key) > 128:
            raise Problem("idempotency_key_required", 422)
        with self.store.transaction():
            memory = self.authorize(principal, memory_id)
            if memory["revision"] != request.expected_revision:
                raise Problem("revision_conflict", 412)
            adapter = self.adapters.get(request.adapter)
            if adapter is None or adapter.project_id != memory["project_id"]:
                raise Problem("memory_adapter_unavailable", 404)
            frozen = canonical(memory)
            binding = self.sync.binding(adapter)
            namespace = adapter.namespace
            effect = hashlib.sha256(canonical({
                "adapter": adapter.name, "namespace": adapter.namespace,
                "binding": binding,
                "memory": memory_id, "snapshot": frozen}).encode()).hexdigest()
            old = self.store.db.execute("SELECT * FROM memory_mirrors WHERE effect_key=?", (effect,)).fetchone()
            if old:
                deletion = self.store.db.execute(
                    "SELECT state FROM memory_provider_deletions WHERE effect_key=?", (effect,)).fetchone()
                if (old["state"] != "done" or old["binding"] != binding
                        or deletion is not None or not self.sync.source_current(old)):
                    raise Problem("memory_mirror_outcome_unknown", 409)
                return {"adapter": adapter.name, "state": "mirrored", "canonical_authority": False}
            self.store.db.execute(
                "INSERT INTO memory_mirrors(effect_key,project,adapter,memory,state,receipt,"
                "binding,snapshot_hash,content_hash,owner) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (effect, memory["project_id"], adapter.name, memory_id, "reserved", None,
                 binding, hashlib.sha256(frozen.encode()).hexdigest(),
                 hashlib.sha256(memory["content"]["text"].encode()).hexdigest(), principal["id"]))
        # An admission is committed before the first external write. Never retry
        # missing/invalid acknowledgements, SDK errors or interrupted intent.
        try:
            with self.store.transaction():
                if (canonical(self.authorize(principal, memory_id)) != frozen
                        or self.adapters.get(adapter.name) is not adapter or self.sync.binding(adapter) != binding):
                    raise Problem("memory_export_source_changed", 409)
            text = memory["content"]["text"]
            receipt = adapter.put(effect, text, {
                "content_sha256": hashlib.sha256(text.encode()).hexdigest(), "canonical_authority": False,
                "hivemind_project": memory["project_id"], "hivemind_namespace": namespace,
                "hivemind_binding": binding, "hivemind_key": effect})
            with self.store.transaction():
                # Retain a known acknowledgement even if final source checks fail:
                # privacy cleanup can still target these exact owned remote IDs.
                self.store.db.execute("UPDATE memory_mirrors SET receipt=? WHERE effect_key=? AND state='reserved'",
                                      (canonical(receipt), effect))
            with self.store.transaction():
                if (canonical(self.authorize(principal, memory_id)) != frozen
                        or self.adapters.get(adapter.name) is not adapter or self.sync.binding(adapter) != binding):
                    raise Problem("memory_export_source_changed", 409)
                changed = self.store.db.execute(
                    "UPDATE memory_mirrors SET state='done' WHERE effect_key=? AND state='reserved'", (effect,))
                if changed.rowcount != 1:
                    raise Problem("memory_export_source_changed", 409)
        except Exception:
            with self.store.transaction():
                self.store.db.execute(
                    "UPDATE memory_mirrors SET state='unknown' WHERE effect_key=? AND state='reserved'", (effect,))
            raise Problem("memory_mirror_outcome_unknown", 409) from None
        return {"adapter": adapter.name, "state": "mirrored", "canonical_authority": False}


def install(app, service, principal, command, envelope, *, adapters=None, policy=None):
    from .capabilities import replace_routes
    from .agent_feature_gates import set_resource_projection
    previous = getattr(app.state, "memory_adapter_bridge", None)
    if previous is not None and not previous.close():
        raise ValueError("Cannot replace an undrained memory provider bridge")
    app.state.memory_adapter_bridge = None
    service.store.memory_provider_result_filter = None
    replace_routes(app, __name__)
    replace_routes(app, "hivemind.memory_provider_sync")
    set_resource_projection(service, "memory_provider_result", None)
    policy = policy or MemoryAdapterPolicy()
    if not policy.enabled:
        return
    bridge = MemoryAdapterBridge(service, adapters or {}, policy)
    app.state.memory_adapter_bridge = bridge

    @app.post("/v1/memories/{memory_id}:mirror")
    async def mirror(memory_id: str, body: MemoryMirrorRequest, p=Depends(principal),
                     idempotency_key: str | None = Header(None)):
        import asyncio
        return envelope(await asyncio.to_thread(bridge.publish, p, memory_id, body, idempotency_key))
    mirror.hivemind_detached_work = True
    from .memory_provider_sync import install_routes
    install_routes(app, bridge, principal, envelope)
