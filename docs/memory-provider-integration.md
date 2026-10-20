# Governed memory providers

SQLite remains authoritative. Mem0, LangMem-store and Letta can supply untrusted
retrieval candidates and receive owned mirrors. They cannot verify facts, grant
permissions, clear execution gates or automatically enter agent context.

## Operator configuration

Inject already-configured SDK/store objects, with SDK/network timeouts. Advanced
operations require an explicit stable `binding_id` identifying the provider
account/store deployment. It must not contain credentials. Treat adapter account,
namespace and binding as immutable while work is active; reinstalling the bridge
drains the previous instance before replacing it.

```python
from hivemind.api import create_app
from hivemind.memory_adapters import LangMemAdapter, MemoryAdapterPolicy

app = create_app(capability_options={"memory_adapters": {
    "adapters": {
        "langmem": LangMemAdapter(
            langgraph_store, project_id, "project-memory",
            binding_id="production-account/store-one"),
    },
    "policy": MemoryAdapterPolicy(
        enabled=True,
        allow_remote_writes=True,
        allow_remote_reads=True,
        allow_imports=True,
        allow_remote_deletes=True,
        auto_withdraw=True,
        poll_interval_seconds=5,
        batch_size=32,
        result_ttl_seconds=300),
}})
```

Every consent flag defaults to false. Imports require remote-read consent;
automatic withdrawal requires remote-delete consent. Reads for ownership/absence
reconciliation are part of delete consent, not a public retrieval endpoint.

All HTTP operations are operator-only (`admin` or project-bound `human`), with
fresh credential/project checks. Agents and Nexus cannot approve imports or
request provider cleanup. No caller supplies endpoints, accounts or namespaces.

## Retrieval and import

1. `POST /v1/projects/{project_id}/memory-providers:retrieve` with an
   `Idempotency-Key` and `{"adapter":"langmem","query":"navigation","limit":10}`.
2. Inspect the returned operator-only `memory_provider_result`. Its items contain
   provider text, record ID, SHA-256 and `trust: provider_reported`. Results expire.
   `GET /v1/memory-provider-results/{result_id}` rechecks current source/binding
   permissions. Canonical command receipts use the same projection.
3. Explicitly approve one item using
   `POST /v1/projects/{project_id}/memory-providers:import`:

```json
{
  "result_id": "memory_provider_result_ID",
  "item_index": 0,
  "content_sha256": "SHA256_FROM_RESULT",
  "title": "Claim requiring review",
  "approve_import": true,
  "evidence_refs": [
    {"kind": "board_post", "id": "board_post_ID", "revision": 1}
  ]
}
```

The SHA placeholder must be replaced with the actual 64-character lowercase hex
digest. Retrieval/import require an idempotency key before provider reads.
Imports re-read the exact remote record and recheck canonical evidence revisions
before committing. Identical imports deduplicate across request keys.

Imports create **proposed** project memories with provider provenance and
`trust: provider_reported`. Existing governed review must verify them. Source
references do not magically prove an external claim true. Normal source/team
restrictions still protect the imported memory.

Retrieval reads one bounded namespace catalog page (at most 20 items, at most
`max_transfer_bytes` total text) and applies deterministic substring filtering
locally. It is not unbounded/semantic provider search and does not warm indexes,
invoke inference or automatically page through an SDK iterator.

Provider entries must contain matching `hivemind_project`, `hivemind_namespace`
and `hivemind_binding` metadata. New mirrors supply these automatically.
Operator-approved foreign entries can supply these tags using
`bridge.sync.binding(adapter)`; they remain untrusted, not owned mirror records.
Known mirror candidates additionally require current public canonical sources.
Withdrawn/stale mirror entries are excluded.

Data-only port contracts:

- **Mem0:** `get_all(user_id=namespace, limit=...)`, `get(id)`,
  `add(..., infer=False)` and `delete(id)`.
- **LangMem-store:** `search(("hivemind", namespace), limit=...)` without a
  semantic query, `get(namespace, key)`, `put(..., index=False)` and `delete`.
- **Letta:** standalone `blocks.list(label=namespace_label, limit=...)`,
  `blocks.retrieve(id)`, `blocks.create` and `blocks.delete`. No agent attachment
  or model turn. The label is a fixed project/namespace digest.

Unsupported SDK signatures, record/page shapes or scope metadata fail closed.
There is no ambient discovery, optional-package installation or model fallback.
Different SDK versions may require an operator-supplied compatible shim. Verify
these contracts against the actual provider deployment.

## Automatic withdrawal and deletion propagation

The worker wakes from minimal canonical commit signals and performs bounded,
cursor-based scans. Periodic scans also detect expiry, dropped signals and changes
from another service instance. Wake-ups coalesce and never accelerate scans beyond
the configured minimum interval.

Owned mirrors become withdrawal candidates when their canonical memory/source is
deleted, changed, expired, invalidated, no longer verified/public, or restricted by
team access. Revoked exporting operator authority and external-write consent also
retire mirrors. Obsolete mirror revisions are withdrawn, not silently overwritten.

Before deletion, the bridge checks its frozen account/namespace binding and exact
acknowledged IDs, then reads provider metadata/content to prove the records still
match the owned mirror. Foreign/mutated records, changed bindings, legacy receipts
without the required provenance and missing write acknowledgements remain held.
No `delete_all`, tenant-wide erase or caller-selected remote ID is accepted.

A durable admission precedes each receipt's delete operation. Multi-ID Mem0
receipts never repeat partial effects. An SDK error, timeout or interrupted
admission holds as unknown, even after a restart or another request key.
Subsequent reconciliation may read those IDs and confirm absence, but cannot
repeat their uncertain delete. Read failures are retried only through bounded,
rate-limited reconciliation cycles.

- `POST /v1/projects/{project_id}/memory-providers:reconcile` with
  `{"adapter":"langmem"}` processes a bounded scan page.
- `GET /v1/projects/{project_id}/memory-providers:status` reports mirror,
  withdrawal and hold states without stored text.

Deleting an imported canonical memory withdraws any mirrors subsequently exported
from it. Importing a foreign record does **not** grant ownership or authorize
deleting that original provider record.

Withdrawn snapshots are not automatically re-exported if access is restored.
Create/review a new canonical revision before exporting again.

## Limits and verification

Provider acknowledgement or observed absence is not provider-wide physical
erasure. Responses explicitly retain `physical_erasure_verified: false`.
Provider backups/history, SQLite/WAL/backups, expired retrieval snapshots and
already-received client data are not proven erased.

Ownership reads and deletes are not an atomic provider compare-and-delete. Use a
dedicated namespace and avoid concurrent external edits to owned mirror records.
A configured binding ID is an operator assertion, not cryptographic tenant proof.
SDK/network deadlines remain operator responsibilities. Shutdown tracks active
calls and retains canonical storage if they cannot drain.

Python SDK: `retrieve_provider_memories`, `provider_memory_result`,
`import_provider_memory`, `reconcile_provider_memories`, `provider_memory_status`.
TypeScript exposes the corresponding camel-case methods.

## Recorded validation

The expanded offline batch passed **357 tests**, including 24 provider integration
tests. Coverage includes all three data-only ports, real canonical HTTP
retrieval/import/receipt flows, source deletion/revision/expiry/team revocation,
known acknowledgements after source loss, account rebinding, concurrent
reconcilers, restart holds and bounded shutdown accounting.

Whole-tree Ruff, schema export and strict TypeScript SDK checks passed. Provider
calls used recording/in-memory SDK ports, not live authenticated providers.
The complete offline/browser suites were not rerun, and no paid model calls or
live external deletions were made.
