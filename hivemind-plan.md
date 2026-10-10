# Hivemind: unified agent memory and coordination plan

**Document status:** Proposed design, not an implemented or ratified standard  
**Research date:** 2026-10-09  
**Project name:** Hivemind  
**General design directive:** One governed memory layer for all AI agents, with central reporting and coordination  
**Optional main coordinator:** Nexus, an authorized AI agent participating in the protocol  
**Plan filename:** `hivemind-plan.md`, retained from the original request  
**Proposed initial protocol version:** `0.1.0`  
**Proposed implementation license:**MIT  
**Required embedding model:** `google/embeddinggemma-2`  
**Vector storage:** SQLite with `sqlite-vec`, using its MIT license option  
**Graph storage:** LadybugDB, MIT; projected from canonical SQLite records  

## 1. Executive summary

Build **Hivemind**, a vendor-neutral unified memory layer for all AI agents. Participating
agents report activity, work ownership, context, and results to the central Hivemind tool.
They retrieve permitted shared memory and discover other agents' work through the same surface.
Hivemind can coordinate agents through deterministic policies without requiring an LLM.
An authorized AI agent may also enter as **Nexus**, the main coordinator, to reason over
shared context and propose or issue policy-checked steering commands.

The system should answer:

- Which agents exist, and what are they doing now?
- What task, repository, branch, workspace, files, tools, and artifacts are involved?
- What actually happened, what did an agent report, and what did a summarizer infer?
- Which agents are working on overlapping files or related problems?
- How can agents send standardized messages and ask each other questions?
- How can agents publish findings to a shared board and retrieve relevant information?
- What should an agent remember across runs, and where did that memory come from?
- How can a human audit, correct, revoke, export, or delete this information?
- How can Nexus retrieve context from one/many explicit agent IDs or find agents semantically?
- How can overlapping work be resolved predictably without destroying either agent's progress?
- Which graph relationships connect agents, tasks, components, files, decisions, and memories?
- How can humans watch live conversations and address an agent by name in a browser?

Hivemind consists of an open protocol plus a reference implementation:

1. **Identity and discovery:** Stable agent IDs, per-run IDs, capability manifests, and presence.
2. **Context capture:** Runtime hooks, SDK events, file-change observation, and task updates.
3. **Central ledger:** Durable events and a queryable, continuously updated view of agent activity.
4. **Direct messaging:** Standardized JSON messages, durable inboxes, responses, and acknowledgments.
5. **Shared board:** Threaded project information, announcements, questions, and semantic search.
6. **Unified memory:** Scoped, source-linked semantic, episodic, and procedural memories.
7. **Context assembly:** A token-budgeted, permission-filtered context packet for any agent.
8. **Interoperability:** MCP tools, an A2A bridge, editor/runtime adapters, and OpenTelemetry export.
9. **Deterministic coordination:** Work claims, conflict rules, steering actions, and acknowledgments.
10. **Nexus:** A scoped coordinator role with context-query and control tools.
11. **Graph context:** LadybugDB relationships and bounded graph-assisted retrieval.
12. **Human observability and participation:** A portable React web app with a live main
    board, per-agent tabs, visible agent conversations, and human `@agent` mentions.

**Core decision:** The ledger is driven by structured events, not by NLP guessing what happened.
Use a configurable **5-second default publish/coalescing interval** for ordinary activity.
Messages, lifecycle changes, errors, cancellations, and coordination conflicts flush immediately.
NLP/LLM processing runs asynchronously and adds explicitly labeled summaries or memory candidates.

**Storage decision:** Do not make a vector database the source of truth. Use SQLite initially
for identities, events, state, permissions, messages, board posts, memory versions, and
coordination decisions. Build vector retrieval on SQLite using `sqlite-vec`.
Add **LadybugDB** as a rebuildable graph projection. Durable jobs generate embeddings
and synchronize graph relationships. Do not assume SQLite and LadybugDB share a transaction.

**Product positioning:** Hivemind is the shared memory and coordination plane;
Nexus is an optional intelligence operating within that plane, not the database or an
unrestricted superuser. Deterministic policy and runtime enforcement bound every action.
It complements existing communication and tool protocols rather than replacing them.

**Control promise:** Direct control is real only for agents whose adapters implement the
advertised control capabilities. Hivemind can steer, assign, pause, resume, or cancel within
explicit grants. A control request is not counted as completed until the adapter confirms
the actual state transition. Unsupported agents remain advisory-only.

## 2. Research: is somebody already building something similar?

### 2.1 Findings

**Yes. Several projects implement substantial parts of this idea, and projects already use
the exact phrase “Agent Context Protocol.”** The research does not establish that any one
project implements this complete proposed design. It also does not establish that the idea
is unique. Treat the opportunity as integration and a stronger context contract, not as
inventing agent messaging, shared memory, or semantic retrieval.

| Project or standard | Verified overlap | What to reuse or investigate | Important distinction |
| --- | --- | --- | --- |
| Model Context Protocol, MCP [S1] | Standardized access to tools, resources, and context | Expose Hivemind operations as MCP tools/resources | MCP alone is not a complete shared activity ledger or memory-governance product |
| Agent2Agent, A2A [S2] | Agent discovery and interoperable task/message exchange | Bridge external agent communication to Hivemind identities, tasks, and context references | Do not assume an A2A peer exposes private internal state or full execution history |
| IBM/BeeAI Agent Communication Protocol [S3] | An earlier protocol also called ACP; LF AI & Data announced convergence with A2A | Study prior lifecycle and interoperability decisions | Do not advertise Hivemind as its successor |
| Zed Agent Client Protocol [S4] | Client-agent sessions, session updates, tools, plans, and filesystem access | Build an explicitly named Agent Client Protocol adapter for editor-originated observations | Client/editor integration is different from a central multi-agent memory and coordination service |
| MCP Agent Mail [S5] | Agent identities, inboxes, searchable threads, and advisory file leases | Closest practical starting point for coding-agent coordination; evaluate an adapter or reusable design | Indexed repository descriptions do not establish that it provides this complete event ledger, EmbeddingGemma retrieval, or memory lifecycle |
| Beads [S6] | Persistent agent task/memory workflow | Import task IDs, dependencies, and durable work status instead of duplicating task trackers | Its advertised memory/task focus differs from a general interoperable activity protocol |
| Mem0 [S7] | A persistent memory layer for agents and applications | Learn extraction, deduplication, and memory CRUD patterns; consider optional integration | A memory layer does not by itself define filesystem provenance, presence, inboxes, and board semantics |
| LangMem [S8] | Long-term memory extraction and adaptation tooling | Borrow memory categories and background extraction patterns | Keep Hivemind independent of a specific orchestration framework |
| Letta [S9] | Shared memory blocks between agents | Support a shared-memory-block adapter | Hivemind should work for agents that are not hosted in Letta |
| OpenTelemetry GenAI conventions [S10] | Agent/model/tool observability | Export traces and reuse trace correlation where practical | Observability is not the same as durable coordination or governed semantic memory |
| `prmichaelsen/agent-context-protocol` [S11] | Repository describes a documentation/planning system for structured project knowledge across agent sessions | Review its artifact conventions and assess integration | Uses the original ACP name; indexed description is documentation-oriented |
| `agent-context-protocol/agent-context-protocol` [S12] | Repository advertises multi-agent communication and coordination | Inspect implementation, license, releases, and schemas | Uses the original ACP name; indexed description does not establish maturity |
| Memory-ledger projects [S13] | Indexed descriptions include portable memory formats or append-only temporal memory history | Study evidence links, supersession, and conflict handling | These are related designs, not evidence of a single accepted context standard |
| “Beyond Context Sharing…” research paper [S14] | Proposes a unified agent communication architecture and reputation ledger | Review threat model and federation proposals | A research proposal is not proof of a production implementation |
| LadybugDB [S18] | Embedded property-graph database and Cypher query surface | Implement graph-assisted memory, provenance, and task relationships | It is a storage engine, not the central agent-control policy |
| Existing Hivemind-named projects [S19] | An indexed repository advertises local-first shared memory for MCP agents using SQLite/Markdown | Compare scope and check public naming/packages before launch | Hivemind is also not a unique project name; detailed feature coverage is unverified |

### 2.2 Project and protocol naming

The project is now **Hivemind**, as requested. **Nexus** names its optional main coordinator
role. Retain `ACP-plan.md` as the document filename, not as the current project name.
The original ACP research remains useful prior art.

Hivemind itself is already used by other projects [S19]. Perform package/domain/trademark
checks before public distribution; do not claim a unique name or registered namespace.

For the draft protocol:

- Use the provisional event namespace `hivemind.context.*` and `hivemind.coordination.*`.
- Identify the wire protocol as `hivemind`; do not confuse it with Agent Client Protocol.
- Include `protocol` and `protocol_version` on every wire envelope.
- Give adapters explicit names such as `agent-client-protocol-adapter` and `a2a-bridge`.
- Do not assume a namespace, package name, or discovery URL has been registered.

### 2.3 Build-versus-integrate recommendation

Build the context/evidence contract, event reduction, authorization, context assembly,
and memory lifecycle as the core. Reuse existing libraries for transport, database access,
embeddings, and MCP/A2A interoperability.

Before implementation, run a short technical spike against MCP Agent Mail and the two
same-name repositories. Compare actual APIs, schema stability, licensing, attribution,
and operating assumptions. Prefer an adapter over a fork unless the fit is unusually close.
Do not copy code before verifying its current license.

### 2.4 Research limits

The requested Hugging Face model card was read directly. Other findings use online
searches of official documentation, repositories, and the LF AI & Data announcement.
Some repository results supplied only indexed descriptions. Their code, test coverage,
current releases, and detailed licenses were not audited.

The source list at the end distinguishes research references from proposed behavior.
Every normative requirement below belongs to proposed Hivemind, not to an existing standard.

## 3. Goals, non-goals, and assumptions

### 3.1 Goals

- Support unrelated agent frameworks without requiring agents to share a model provider.
- Offer the same protocol for local coding agents and remotely hosted agents.
- Preserve a trustworthy distinction between observation, self-report, and inference.
- Provide eventual semantic search without weakening authoritative state consistency.
- Make context and memory portable, scoped, inspectable, and revocable.
- Let agents cooperate without making every agent's private context visible to every peer.
- Start with a single local service and evolve to a shared deployment.
- Publish schemas, SDKs, test vectors, and a conformance suite under MIT.
- Give every participating agent one interoperable memory/context surface.
- Let the central service coordinate deterministically even when no Nexus is online.
- Let an authorized Nexus query one/many agents by ID and discover relevant agents semantically.
- Prevent duplicate ownership of declared work scopes through policy-checked control.
- Combine SQLite vector retrieval with LadybugDB graph relationships.
- Open the human interface in a normal browser, not a required native desktop shell.
- Make the live general board the default view, with agent tabs inside the same React page.
- Let humans address agents through autocomplete mentions while other authorized board
  participants can observe and join the conversation.

### 3.2 Non-goals for v0.1

- Capturing hidden reasoning, model internals, or every private instruction.
- Promising full context capture for uninstrumented agents.
- Unrestricted autonomous execution, payment networks, reputation marketplaces, or model hosting.
- Enforcing operating-system file locks through a metadata-only protocol.
- Automatically trusting board posts or turning retrieved text into executable instructions.
- Peer-to-peer federation, global discovery, distributed consensus, or multi-region writes.
- Replacing Git, issue trackers, A2A, MCP, or existing observability platforms.
- Full multimodal retrieval in the first implementation, even though the model supports it.

### 3.3 Default assumptions

The memory/control contract is framework- and domain-neutral: coding, research, design,
support, and operational agents can participate. Coding agents are the first reference
integration, not a limitation of the protocol. File/workspace context is optional outside
coding use cases; typed resources and work scopes remain general.

The reference implementation should run on Windows, macOS, and Linux. The first deployment
has one Hivemind server process per data directory and one administrative trust domain.

“All agent context” means **all permitted, observable, explicitly submitted context**,
not omniscience. An agent that does not emit events can only be partially observed.

## 4. Reference architecture

```text
Agent runtimes / editors / CLI wrappers / framework hooks
        | SDK events, tool events, scoped file observations
        v
Local capture adapter
  - assigns run/source IDs and monotonic source sequences
  - redacts before durable local buffering
  - coalesces ordinary activity on a configurable timer
  - retries from a bounded disk spool
        |
        | authenticated Hivemind JSON API
        v
Hivemind server: unified memory and deterministic control plane
  + Identity / authentication / policy
  + Schema validation / deduplication / ingestion
  + Deterministic event reducer
  + Agent ledger / tasks / artifacts / file reservations
  + Durable direct-message router and inboxes
  + Shared board and memory APIs
  + Context-packet assembler
  + Work-claim registry / conflict engine / policy-checked control queue
  + Nexus role leases / authority epochs / action audit
  + SSE subscriptions / MCP façade / optional A2A bridge
        |
        +--> SQLite: canonical records, revisions, indexes, outbox
        +--> Local content store: permitted large artifacts
        +--> SQLite FTS5: lexical lookup
        +--> SQLite sqlite-vec: semantic vectors and authorized KNN retrieval
        +--> LadybugDB: rebuildable graph projection
        |
        v
Background workers
  + EmbeddingGemma 2 encoder --> SQLite vector rows
  + Graph projector --> LadybugDB nodes and relationships
  + Conflict evaluator --> deterministic actions / Nexus review candidates
  + Optional summarizer / memory extractor
  + Retention / compaction / reconciliation / exports
        |
        v
Nexus agent (optional; participates through authenticated tools/API)
  + explicit-ID / semantic context queries
  + plan proposals / authorized steering through deterministic policy
        |
        v
React browser app and CLI
  + live main board / agent tabs / agent-to-agent conversation view
  + human @mentions / autocomplete / random agent colors / golden Nexus crown
  + agent roster, timeline, inbox, conflicts, controls, memory/graph evidence
```

### 4.1 Suggested technology choices

| Component | Initial choice | Reason |
| --- | --- | --- |
| Reference server | Python 3.12+ with FastAPI, Pydantic, and an async HTTP client | Close to embedding ecosystem; generate validation/OpenAPI artifacts |
| Normative schemas | JSON Schema Draft 2020-12, authored and versioned in the spec package | Language-independent contract; server models must conform to it |
| Transport | HTTPS JSON API plus server-sent events; local-only HTTP allowed | Simple durable commands with resumable subscriptions |
| Canonical storage | SQLite with WAL, foreign keys, explicit transactions | Portable single-server deployment and transactional state/outbox |
| Lexical search | SQLite FTS5 | Exact identifiers, paths, symbols, and immediate fallback |
| Vector search | SQLite + `sqlite-vec` [S15, S17] | Build on SQLite; dual MIT/Apache-2.0 upstream licensing with MIT selected |
| Graph search | LadybugDB [S18] | Embedded property graph and Cypher; MIT upstream contribution/license statements |
| Coordination | Deterministic policy engine with durable SQLite control queue | Predictable decisions, revisions, leases, and audit |
| Nexus | Ordinary registered agent with a leased coordinator role | Optional AI planning without bypassing service policy |
| Embeddings | SentenceTransformers + `google/embeddinggemma-2` [S16] | Required model, local operation, code/text retrieval |
| TypeScript SDK | Generated types plus a small hand-written client | Most coding-agent runtimes need easy JavaScript integration |
| Python SDK | Generated types plus a small hand-written client | Framework hooks and worker integration |
| Artifact storage | Hash-addressed local blobs, optional object storage later | Keep large patches/logs out of every envelope |
| UI | React + TypeScript browser app, built with Vite | Portable live boards, in-page agent tabs, human mentions, observability |
| UI delivery | Static React assets served by Hivemind on the API's origin | Open in a browser without Electron, a required cloud frontend, or a second production process |

These choices are reference-implementation defaults, not protocol requirements.

### 4.2 SQLite vector and LadybugDB decisions

**Replace the previous LanceDB recommendation.** The vector database is built on SQLite,
using the open-source `sqlite-vec` extension rather than inventing a nearest-neighbor engine.
Mozilla's project page identifies `sqlite-vec` as dual MIT/Apache-2.0 [S15]; select MIT and
verify the exact pinned release and notices before packaging.

Use LadybugDB for graph functionality as requested. Official documentation describes
an embedded graph database and provides a Cypher manual; the upstream repository/PyPI
material states MIT licensing for contributions [S18]. Verify the pinned distribution's
complete LICENSE and bundled dependencies in Phase 0.

SQLite remains canonical. LadybugDB is a derived relationship index, not a second
authority for task ownership, control permissions, memory verification, or policy state.
Define `VectorIndex` and `GraphIndex` interfaces, but implement one backend of each.

Do not assume ANN/HNSW acceleration: `sqlite-vec`'s original implementation is brute-force
KNN [S17]. Confirm the pinned version's capabilities and benchmark realistic 768-dimensional
datasets. Partition/bound candidate sets and disclose scale limits before promising speed.

### 4.3 Scale and deployment boundaries

- Local mode: SQLite and LadybugDB files on local disk, accessed by one server owner.
- Do not put a writable SQLite WAL database on a shared network filesystem.
- Agents access the service, not database files directly.
- Use one serialized writer per embedded index partition initially.
- Team mode initially uses one service/writer with SQLite rather than pretending SQLite
  is a multi-writer network database. Agents can still connect remotely through secured APIs.
- Use one graph projection writer and read connections within LadybugDB's supported
  concurrency model; confirm exact behavior for the pinned release.
- Horizontal/distributed storage is future work. A migration must preserve the requested
  SQLite-based vector contract or explicitly introduce a separately negotiated profile.

## 5. Protocol foundations

### 5.1 Normative language and versioning

`MUST`, `SHOULD`, and `MAY` describe proposed conformance requirements.

- `protocol`: always `hivemind`.
- `protocol_version`: exact supported wire version, initially `0.1.0`.
- `schema_version`: revision of a particular payload family where needed.
- `capabilities`: explicit negotiated feature list; SDK version is separate.
- Breaking fields or changed meanings require a new protocol version.
- Extensions live under `extensions` with reverse-domain keys owned by their publisher.
- Reject unknown core command fields; preserve negotiated extension fields.
- Consumers can skip unknown event types in an authorized stream, but cannot submit unknown
  event types without an advertised extension capability.
- v0.x compatibility must be explicitly advertised; do not assume minor releases are stable.

### 5.2 IDs, identity, and time

Use opaque strings with type prefixes and UUIDv7 suffixes for service-assigned IDs:

| Identifier | Meaning |
| --- | --- |
| `agent_id` | Stable logical agent identity, not a display name or model name |
| `run_id` | One execution/session of an agent |
| `task_id` | A unit of work; may reference an external issue |
| `project_id` | Authorization and coordination boundary |
| `workspace_id` | Actual checkout/sandbox identity |
| `event_id` | Globally unique event identity |
| `message_id`, `thread_id`, `post_id` | Messaging and board identities |
| `memory_id`, `memory_version_id` | Stable memory identity and immutable version |
| `artifact_id`, `reservation_id` | Artifacts and advisory coordination records |
| `work_claim_id`, `scope_id` | Claimed work and canonical component/resource scope |
| `conflict_id`, `control_id` | Coordination conflicts and durable control actions |
| `coordinator_lease_id`, `coordinator_epoch` | Nexus authority lease and stale-controller fencing |

For readability, examples use abbreviated IDs. Production generators must not use these
example values. Import adapters map foreign IDs through a provider-scoped mapping table.

Agent IDs are registered by an authenticated principal. A token's subject and policy,
not a JSON `from` field, determine which agent can write. Never accept identity claims
solely because an agent knows another agent's ID.

Timestamps use RFC 3339 UTC. Store `occurred_at` from the producer and `received_at`
from the server separately. Server ingestion sequences and revisions establish ordering,
not wall-clock timestamps. Deadlines and leases use server time. Use monotonic local timers
for periodic capture.

One logical agent may have several runs. Presence is run-level first; an aggregate agent
view lists active runs rather than hiding concurrent sessions behind one status.

### 5.3 Discovery and capability negotiation

Proposed `GET /.well-known/hivemind` response:

```json
{
  "protocol": "hivemind",
  "supported_versions": ["0.1.0"],
  "server_id": "srv_example",
  "api_base": "/v0.1",
  "capabilities": [
    "events.batch",
    "streams.sse",
    "messages.durable",
    "board.hybrid_search",
    "memory.versioned",
    "context.packet",
    "files.advisory_reservations",
    "context.agent_selectors",
    "coordination.deterministic",
    "coordination.nexus",
    "coordination.controls",
    "graph.ladybugdb",
    "board.human_participation",
    "board.agent_mentions",
    "ui.live_conversations"
  ],
  "limits": {
    "max_batch_events": 100,
    "max_batch_bytes": 1048576,
    "max_event_bytes": 131072,
    "max_inline_content_bytes": 65536
  },
  "authentication": {
    "schemes": ["bearer"]
  }
}
```

Detailed manifests and agent rosters require authentication. Discovery does not enumerate
private agents, projects, credentials, or infrastructure internals.

### 5.4 Common event envelope

```json
{
  "protocol": "hivemind",
  "protocol_version": "0.1.0",
  "event_id": "evt_example_001",
  "type": "hivemind.context.file.changed",
  "agent_id": "agt_backend",
  "run_id": "run_backend_001",
  "project_id": "prj_example",
  "workspace_id": "ws_backend",
  "task_id": "tsk_auth_refactor",
  "occurred_at": "2026-10-09T10:15:02.100Z",
  "source": {
    "source_id": "src_tool_hook_001",
    "kind": "runtime_hook",
    "sequence": 42,
    "attribution": "confirmed",
    "tool_call_id": "call_edit_12"
  },
  "trace": {
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "span_id": "00f067aa0ba902b7",
    "causation_event_id": "evt_tool_12"
  },
  "data": {
    "change": "modified",
    "path": "src/auth/session.ts",
    "before_sha256": null,
    "after_sha256": null,
    "patch_artifact_id": "art_patch_12",
    "origin": "tool_edit"
  },
  "extensions": {}
}
```

Server response metadata adds `received_at`, `ingestion_seq`, and `ledger_revision`.
Clients cannot set them. The server derives effective access policy from authenticated
identity, project, and resource permissions.

### 5.5 Foundational schema rules

| Rule | Required behavior |
| --- | --- |
| Event payload | Discriminated union by `type`; each event has its own schema |
| IDs | Nonempty, length-limited opaque strings; never interpreted as filesystem paths |
| Strings | UTF-8; explicit size limits; no secrets in diagnostic errors |
| Files | Workspace-relative paths; root and sandbox references resolved by the adapter |
| Paths | Normalize separators; reject absolute paths, traversal, NULs, and root escapes |
| Case | Preserve display spelling; use workspace-specific comparison rules for identity |
| Symlinks | Verify real-path containment before reading; do not follow out-of-root links |
| Hashes | Algorithm-tagged SHA-256 values when present; null means unavailable, not unchanged |
| Lists | Explicit maximum lengths and bounded pagination |
| Numbers | Finite values only; confidence is within `[0,1]`, counts nonnegative |
| Content types | Plain text/Markdown and schema-tagged JSON first; no executable HTML |
| Resource updates | Compare-and-swap revision preconditions |
| Large content | Artifact references with content type, byte size, hash, and read authorization |

Limits are measured on uncompressed UTF-8 payloads. Limit compressed requests after
decompression as well. A batch is capped at 100 events and 1 MiB; one event at 128 KiB;
inline content fields at 64 KiB. Larger permitted content belongs in an artifact.

### 5.6 Concrete message JSON Schema

The following self-contained Draft 2020-12 schema makes the message proposal concrete.
It validates the message example in Section 9.1. The implementation should publish this
as a versioned file and add equivalent schemas for every other domain/event payload.
It is a design artifact, not a claim that a conformance-complete schema package exists.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Hivemind 0.1 direct message",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "protocol", "protocol_version", "message_id", "project_id",
    "from", "to", "type", "thread_id", "created_at", "content"
  ],
  "properties": {
    "protocol": {"const": "hivemind"},
    "protocol_version": {"const": "0.1.0"},
    "message_id": {"$ref": "#/$defs/id"},
    "project_id": {"$ref": "#/$defs/id"},
    "from": {"$ref": "#/$defs/id"},
    "to": {"$ref": "#/$defs/id"},
    "sender_run_id": {"$ref": "#/$defs/id"},
    "recipient_run_id": {"$ref": "#/$defs/nullable_id"},
    "type": {
      "enum": ["request", "response", "question", "inform", "ack", "error", "cancel"]
    },
    "thread_id": {"$ref": "#/$defs/id"},
    "in_reply_to": {"$ref": "#/$defs/nullable_id"},
    "correlation_id": {"$ref": "#/$defs/id"},
    "task_id": {"$ref": "#/$defs/nullable_id"},
    "created_at": {"$ref": "#/$defs/timestamp"},
    "expires_at": {
      "anyOf": [
        {"$ref": "#/$defs/timestamp"},
        {"type": "null"}
      ]
    },
    "priority": {"enum": ["low", "normal", "high"]},
    "requires_ack": {"type": "boolean"},
    "content": {"$ref": "#/$defs/content"},
    "context_refs": {
      "type": "array",
      "maxItems": 32,
      "items": {"$ref": "#/$defs/resource_ref"}
    },
    "requested_response_schema": {
      "anyOf": [
        {"type": "string", "minLength": 1, "maxLength": 256},
        {"type": "null"}
      ]
    },
    "extensions": {"type": "object", "maxProperties": 32}
  },
  "allOf": [
    {
      "if": {
        "properties": {
          "type": {"enum": ["response", "ack", "error", "cancel"]}
        },
        "required": ["type"]
      },
      "then": {
        "required": ["in_reply_to"],
        "properties": {"in_reply_to": {"$ref": "#/$defs/id"}}
      }
    }
  ],
  "$defs": {
    "id": {"type": "string", "minLength": 1, "maxLength": 128},
    "timestamp": {
      "type": "string",
      "format": "date-time",
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?Z$"
    },
    "nullable_id": {
      "anyOf": [{"$ref": "#/$defs/id"}, {"type": "null"}]
    },
    "resource_ref": {
      "type": "object",
      "additionalProperties": false,
      "required": ["kind", "id"],
      "properties": {
        "kind": {
          "enum": [
            "agent", "run", "event", "artifact", "board_post", "memory",
            "task", "decision", "scope", "work_claim", "conflict", "control"
          ]
        },
        "id": {"$ref": "#/$defs/id"},
        "revision": {"type": "integer", "minimum": 1}
      }
    },
    "content": {
      "oneOf": [
        {
          "type": "object",
          "additionalProperties": false,
          "required": ["media_type", "text"],
          "properties": {
            "media_type": {"enum": ["text/plain", "text/markdown"]},
            "text": {"type": "string", "minLength": 1, "maxLength": 65536}
          }
        },
        {
          "type": "object",
          "additionalProperties": false,
          "required": ["media_type", "schema_name", "data"],
          "properties": {
            "media_type": {"const": "application/json"},
            "schema_name": {"type": "string", "minLength": 1, "maxLength": 256},
            "data": {}
          }
        },
        {
          "type": "object",
          "additionalProperties": false,
          "required": ["media_type", "artifact_id"],
          "properties": {
            "media_type": {"const": "application/vnd.hivemind.artifact-reference+json"},
            "artifact_id": {"$ref": "#/$defs/id"}
          }
        }
      ]
    }
  }
}
```

JSON Schema string lengths count characters, not UTF-8 bytes. The API must separately
enforce the byte limits, registered schema names, extension permissions, ID ownership,
cross-field time rules, reply targets, thread membership, and artifact authorization.
For example, `expires_at` must be after the accepted creation time, and a response cannot
reference a message from an inaccessible thread. These are semantic checks, not just shape checks.
Enable format assertion in validators; do not silently ignore `date-time`.
Some validators need optional format-checking dependencies even when a format checker
is supplied. Require a startup/conformance self-test that rejects malformed timestamps
and impossible calendar dates. The UTC-pattern check is only a structural backstop;
it is not a substitute for actual RFC 3339/calendar validation.

### 5.7 Response envelopes and schema families

Resource examples in this plan show the **data body**, not every HTTP response wrapper.
Actual successful responses carry `protocol`, `protocol_version`, `request_id`, `data`,
and optional `meta` containing revision, pagination, and freshness information.
Error responses carry those protocol/request identifiers and an `error` object instead.
Discovery is the exception: it advertises supported versions before one is selected.

Publish the following schema families:

```text
common/{id,reference,content,origin,trace,error,response-meta}.schema.json
identity/{agent-manifest,workspace,run}.schema.json
events/envelope.schema.json
events/{run-started,run-status-changed,tool-started,tool-completed,file-changed,...}.schema.json
ledger/{run-snapshot,agent-aggregate}.schema.json
messages/{message,delivery,claim,ack}.schema.json
board/{board,post,thread,post-version,actor,rich-chat-content,mention,mention-notification}.schema.json
memory/{memory,memory-version,evidence,verification,conflict}.schema.json
search/{query,result,embedding-profile,index-watermark}.schema.json
context/{request,agent-selector,packet,item}.schema.json
artifacts/{descriptor,upload-request}.schema.json
reservations/{request,reservation,renewal}.schema.json
coordination/{work-claim,scope,conflict,policy,control,control-result,nexus-lease}.schema.json
graph/{node,edge,query,projection-watermark}.schema.json
ui/{agent-appearance,conversation-item,timeline-query,mention-option}.schema.json
```

Event envelope validation composes a common header with an event-type-discriminated
payload schema. Artifact/reference schemas must distinguish canonical IDs from arbitrary
URLs. Domain origin labels and confidence fields do not grant verification authority.

## 6. Domain schemas

### 6.1 Agent manifest

```json
{
  "agent_id": "agt_backend",
  "display_name": "Backend agent",
  "description": "Implements and tests backend changes",
  "owner_principal_id": "principal_team",
  "runtime": {
    "name": "example-runtime",
    "version": "1.0.0",
    "adapter_version": "0.1.0"
  },
  "capabilities": [
    "code.edit",
    "tests.run",
    "hivemind.context.messages.receive",
    "hivemind.control.steer",
    "hivemind.control.pause"
  ],
  "project_ids": ["prj_example"],
  "communication": {
    "delivery": "durable_inbox",
    "a2a_binding_id": null
  },
  "context_capture": {
    "tool_events": true,
    "filesystem_watch": true,
    "conversation_capture": "opt_in",
    "hidden_reasoning": false
  },
  "revision": 1
}
```

Model/provider details are optional metadata, not identity. The roster returns only fields
authorized for the requesting principal. Discovery is not a promise that an agent is online.

### 6.2 Run

Fields: `run_id`, `agent_id`, `project_id`, `workspace_id`, optional `task_id`,
`started_at`, `ended_at`, `status`, `last_seen_at`, `adapter_capabilities`,
`capture_health`, `external_session_ref`, and `revision`.

Run statuses:

```text
registered -> running <-> idle
running/idle -> waiting_input or blocked -> running
running/idle/blocked -> paused -> running/idle
nonterminal -> completed | failed | cancelled
```

`offline` is derived presence, not a terminal run status. A completed run cannot become
running again; create a new run with a `resumes_run_id` reference.

`capture_health` reports gaps, dropped observations, queue depth, and last successful flush.
Do not display a healthy-looking ledger when instrumentation is missing.

### 6.3 Workspace and file identity

Workspace fields:

- `workspace_id`, `project_id`, `repository_id`.
- `vcs_provider`, sanitized `repository_ref`, `branch`, `head_commit`, optional worktree ref.
- `root_alias`, platform, filesystem case-sensitivity, and capture policy.
- Current repository metadata timestamp and revision.

Keep absolute local roots in an adapter-private mapping, not in broadly shared context.
Different worktrees receive different workspace IDs.

File identity is `(workspace_id, normalized_path)`. Content hashes can identify equivalent
snapshots but do not make distinct worktrees the same filesystem. Rename events contain
`old_path` and `new_path`.

### 6.4 Task

Fields: `task_id`, `project_id`, `title`, `description`, `status`, `assigned_agent_ids`,
`parent_task_id`, dependency refs, external tracker refs, evidence refs, timestamps, revision.

Status: `queued`, `running`, `waiting_input`, `blocked`, `completed`, `failed`, `cancelled`.

Task assignment and task completion require explicit authorized commands or structured
runtime events. A summary saying “done” must not close a task automatically.

External trackers remain authoritative for externally owned task state unless an explicit
integration policy grants Hivemind write-back. v0.1 does not autonomously write to trackers.

### 6.5 Central ledger snapshot

The ledger is an agent/run roster plus a queryable event history. A row should include:

```json
{
  "agent_id": "agt_backend",
  "run_id": "run_backend_001",
  "project_id": "prj_example",
  "workspace_id": "ws_backend",
  "task_id": "tsk_auth_refactor",
  "run_status": "running",
  "role": "worker",
  "work_claim_ids": ["claim_backend_session"],
  "scope_ids": ["scope_api_session"],
  "pending_control_ids": [],
  "coordination_state": "owned",
  "presence": "online",
  "last_seen_at": "2026-10-09T10:15:05Z",
  "current_activity": {
    "kind": "tool",
    "name": "edit_file",
    "started_at": "2026-10-09T10:15:02Z",
    "evidence_event_ids": ["evt_tool_12"]
  },
  "changed_files": [
    {
      "path": "src/auth/session.ts",
      "change": "modified",
      "attribution": "confirmed",
      "last_event_id": "evt_example_001"
    }
  ],
  "counts": {
    "observed_file_changes": 1,
    "completed_tool_calls": 12,
    "unresolved_errors": 0
  },
  "summary": {
    "text": "Refactoring session validation",
    "origin": "agent_reported",
    "evidence_event_ids": ["evt_status_01"],
    "through_ingestion_seq": 120,
    "updated_at": "2026-10-09T10:15:05Z"
  },
  "capture_health": {
    "state": "healthy",
    "missing_source_sequences": [],
    "dropped_observation_count": 0
  },
  "ledger_revision": 18,
  "through_ingestion_seq": 123
}
```

Paginate changed-file lists. Counts distinguish repeated edit observations from unique files.
Keep active tool calls as a set; the singular activity field is only a display summary.
Agent-supplied labels and observed state live in separate fields.

### 6.6 Artifact

Fields: `artifact_id`, `project_id`, owner, producing run/task, `kind`, `media_type`,
`byte_size`, `sha256`, storage handle, sensitivity, evidence refs, retention, timestamps.

Kinds: `patch`, `log`, `test_result`, `document`, `image`, `audio`, `video`, `other`.
The MVP stores text/code/test artifacts; media support is an extension.

An artifact reference is not a public URL. Resolve it through an authorized read API.
Content-addressed deduplication must not reveal whether another tenant has the same blob.

### 6.7 Coordination-related manifest and task fields

Manifest control metadata includes `supported_actions`, `safe_boundary_kind`,
`checkpoint_support`, `scope_permit_enforcement`, and `control_adapter_version`.
These describe runtime capabilities, not granted privileges. An eligible Nexus has an
administrator-created role binding; the active lease is read from canonical coordinator state.

Tasks additionally carry `scope_ids`, `desired_owner_count`, `assignment_revision`,
`eligible_agent_capabilities`, and `assignment_source` (`human`, `policy`, or `nexus`).
Separate volatile heartbeat/presence fields from control-affecting revisions so routine
liveness updates do not invalidate every pending control precondition.

## 7. Event catalog and context capture

### 7.1 Core event types

All context names below have the prefix `hivemind.context.`:

| Event suffix | Required payload highlights | Effect |
| --- | --- | --- |
| `run.started` | Run identity, workspace/task refs, capture capabilities | Opens a run |
| `run.status_changed` | Expected revision, status, optional reason | Explicit run state change |
| `run.ended` | Terminal status, permitted result refs | Closes a run |
| `task.updated` | Expected revision and authorized task patch | Updates task fields |
| `agent.status_reported` | Human-readable activity, blockers, evidence refs | Updates reported context |
| `tool.started` | Call ID, tool name, safe argument summary | Tracks an active tool |
| `tool.progressed` | Call ID, bounded progress update | Adds observed progress |
| `tool.completed` | Call ID, result refs, success/failure, duration | Closes a tool call |
| `file.changed` | Workspace/path, operation, provenance, optional patch/hash | Records create/modify/delete/rename |
| `vcs.observed` | Sanitized branch/commit/status information | Refreshes workspace metadata |
| `artifact.created` | Artifact ID and provenance | Links an output |
| `conversation.observed` | Opt-in visible message/ref, role, redaction status | Adds permitted conversational context |
| `decision.recorded` | Decision, alternatives, rationale summary, evidence | Adds an explicit decision |
| `blocker.recorded` | Blocker, requested assistance, related refs | Adds a structured blocker |
| `capture.gap_detected` | Source, missing range/reason | Makes capture incompleteness visible |
| `correction.recorded` | Target, correction, authority, reason | Corrects interpretations without silent history rewriting |

Server-origin events include `message.accepted`, `message.acked`, `board.posted`,
`board.revised`, `memory.proposed`, `memory.verified`, and reservation lifecycle events.
Agents cannot forge server-origin events by posting them to the ingestion endpoint.

Coordination events use `hivemind.coordination.*`: `claim.registered`, `claim.released`,
`claim.expired`, `conflict.detected`, `conflict.resolved`, `nexus.acquired`,
`nexus.revoked`, `control.queued`, `control.acknowledged`, `control.applied`,
and `control.failed`. Claims, Nexus leases, and controls use dedicated authorized commands;
their resulting events are generated by the service. Agent control results are accepted
only from the registered target adapter and reconciled with pending control state.

Heartbeats update latest presence directly. Persist sampled presence/lifecycle transitions,
not necessarily one immutable heartbeat event every five seconds forever.

### 7.2 Capture order of preference

1. SDK/runtime hooks provide explicit start/completion events and tool-call identity.
2. Editor/client adapters translate visible session/tool/file signals.
3. Scoped tool wrappers capture attributable file changes and safe result metadata.
4. Filesystem watchers detect otherwise missed changes.
5. Periodic reconciliation detects watcher gaps and updates repository metadata.
6. Optional NLP enriches captured evidence with summaries and memory proposals.

A filesystem watcher alone cannot identify the writer in a shared directory. Record an
unattributed workspace observation rather than inventing an agent owner. Correlating
timestamps is a hint, not proof. Separate worktrees plus tool hooks give better attribution.

Register capture sources with a principal, run/workspace scope, and evidence authority.
The server checks that registration before accepting `attribution: "confirmed"`.
An ordinary agent-written status event remains self-reported even if the agent labels it
“observed.” A trusted local adapter's observation is still limited to what it actually saw;
it is not an assurance that the machine or agent was uncompromised.

### 7.3 Five-second update behavior

Recommended defaults:

| Parameter | Default | Notes |
| --- | --- | --- |
| `flush_interval_ms` | 5000 | Publish buffered noncritical observations |
| `heartbeat_interval_ms` | 5000 | Liveness, independent of semantic indexing |
| `presence_stale_after_ms` | 15000 | No longer confidently online |
| `presence_offline_after_ms` | 30000 | Display offline; do not mark task failed |
| `summary_min_interval_ms` | 15000 | Only when meaningful new content exists |
| `max_batch_events` | 100 | Flush early when size/count threshold is reached |
| `max_batch_bytes` | 1048576 | Uncompressed envelope bytes |

Allow a practical flush range such as 100 ms to 60 seconds, subject to server policy.
Heartbeat and stale/offline thresholds must remain internally consistent. Use jitter for
heartbeats and periodic reconciliation; never randomize a message deadline.

Timer behavior:

```text
on observation:
  redact -> validate -> append to local spool
  if critical or batch threshold reached:
    request an immediate flush

on flush timer:
  send pending batches with stable event IDs and source sequences
  remove acknowledged spool entries
  retain retryable failures; quarantine permanent schema failures

on server acceptance:
  atomically append event + reduce state + create outbox jobs
  commit
  publish a notification and make current state available

on enrichment worker:
  read authorized new content
  generate embeddings and optional source-linked summaries
  publish index watermark / conditional summary revision
```

Send direct messages and reservation commands immediately. Do not route them through
the ordinary observation buffer. The five-second setting is not a guarantee of five-second
LLM summaries or vector indexing.

### 7.4 NLP and summary policy

The embedding model is **not** a generative summarizer. A separate, optional summarization
provider produces structured summaries. The protocol works without one.

Summaries carry `origin: "derived"`, source event IDs, a processing watermark, provider/model
metadata where permitted, and freshness information. User-edited and agent-reported summaries
have their own origins. A probabilistic confidence value is not proof of correctness.

Never use NLP alone to assert that:

- A file was edited by a particular agent.
- A test passed or a tool succeeded.
- A task finished.
- A lock/reservation is held.
- A message recipient agreed to a request.

An optional classifier may suggest task labels, topics, or related agents. Users and agents
can correct suggestions. Server state transitions remain structured and authorized.

## 8. Ordering, reliability, and consistency

### 8.1 Event acceptance and deduplication

- Delivery is at least once.
- Deduplicate by `(project_id, event_id)`, with uniqueness also enforced for
  `(project_id, source_id, source_sequence)`.
- Hash the validated canonical payload. Same identity + same payload returns the original
  acceptance result; same identity + different payload returns `idempotency_conflict`.
- Assign a monotonically increasing per-project `ingestion_seq` inside the transaction.
- Preserve the client's source sequence and timestamp; never replace them with server order.
- A batch returns per-event results. The successful items are durable even when another
  item is rejected; clients retry only retryable/unacknowledged items.
- A transport failure with an unknown outcome is safely retried with identical event IDs.

### 8.2 Late and out-of-order observations

Use ingestion order for audit replay and source sequence for within-source state precedence.
Reducers must not let a delayed tool-start event reopen an already completed call.
Represent unresolved partial observations explicitly.

Revision-sensitive commands require a current revision and return a conflict on stale state.
Do not treat an old offline status command as a timeless observation that can override a
newer status. A late historical file observation is still stored but does not claim to be
the current filesystem snapshot.

Record missing sequence ranges and adapter resets. Each restarted capture source gets a
new source ID; it must not reuse old sequence numbers under the same source ID.

### 8.3 Durable ingestion boundary

Within one SQL transaction:

1. Check identity and authorization.
2. Validate schema and deduplication keys.
3. Append canonical event or command-generated event.
4. Update deterministic projections and record their revision.
5. Insert embedding/graph/summary/notification/control-dispatch outbox jobs as needed.
6. Commit before returning acceptance.

The server never performs “save SQL, then hope to save the vector or graph” without a durable job.
Jobs use resource ID + resource revision + embedding profile as idempotency keys.

### 8.4 Search and index consistency

The ledger, inbox, and canonical records are immediately available after commit.
Semantic retrieval and LadybugDB graph projection are eventually consistent.

Every semantic response reports:

- Relevant index partition/profile.
- `index_through_ingestion_seq`.
- Pending-job count or lag estimate.
- Whether lexical fallback was used.
- Graph projection watermark and graph fallback status when graph expansion is requested.

An explicit `min_ingestion_seq` option can wait for a bounded timeout or return
`index_not_ready`; it cannot claim freshness before the watermark advances.
Track a **contiguous** processing watermark, including intentionally skipped events;
a maximum observed job sequence is not enough.

Typed coordination decisions read current work claims, scope relationships, policy,
and leases from SQLite. They do not wait for an embedding or graph projector and do not
use a stale derived index as ownership authority.

### 8.5 Offline operation and backpressure

Adapters use an encrypted or access-controlled local spool with a bounded disk budget.
Redact before spooling. Default maximum: 256 MiB, configurable.

- Preserve lifecycle events, messages, and attributable edits preferentially.
- Coalesce noisy watcher/progress events rather than storing every keystroke.
- Report any dropped observations and prevent a “complete capture” claim afterward.
- Use exponential retry backoff with jitter and honor `Retry-After`.
- Server rate limits are per principal/project; responses do not expose another tenant's load.
- Run termination performs a bounded final flush, then leaves retryable records in the spool.

## 9. Standardized direct agent communication

### 9.1 Message envelope

```json
{
  "protocol": "hivemind",
  "protocol_version": "0.1.0",
  "message_id": "msg_question_01",
  "project_id": "prj_example",
  "from": "agt_backend",
  "to": "agt_frontend",
  "sender_run_id": "run_backend_001",
  "recipient_run_id": null,
  "type": "question",
  "thread_id": "thr_session_contract",
  "in_reply_to": null,
  "correlation_id": "corr_session_contract",
  "task_id": "tsk_auth_refactor",
  "created_at": "2026-10-09T10:16:00Z",
  "expires_at": "2026-10-09T11:16:00Z",
  "priority": "normal",
  "requires_ack": true,
  "content": {
    "media_type": "text/markdown",
    "text": "Does the frontend require expires_at in the session response?"
  },
  "context_refs": [
    {
      "kind": "artifact",
      "id": "art_api_contract",
      "revision": 1
    }
  ],
  "requested_response_schema": null,
  "extensions": {}
}
```

Allowed core `type` values:

| Type | Meaning |
| --- | --- |
| `request` | Ask another agent to perform work; does not itself authorize that work |
| `response` | Answer a request or question |
| `question` | Ask for information or clarification |
| `inform` | Share a fact, update, or resource |
| `ack` | Explicit receipt/processing acknowledgment |
| `error` | Report inability to process a referenced message |
| `cancel` | Request cancellation of previously requested work |

Use one recipient per message in v0.1. A bulk operation can create separate messages per
recipient, with separate authorization and delivery state. Broadcasts belong on the board.

### 9.2 Authentication and delivery semantics

- `from` must match an agent the authenticated principal is allowed to represent.
- Server-normalize/validate creation time and enforce maximum expiry policy.
- Unknown, unauthorized, and unreachable recipients use privacy-preserving errors.
- Recipient policies may restrict senders and message types.
- Sending a message durably stores it; it does **not** mean an LLM saw or acted on it.
- Sender sees `accepted`, then delivery/acknowledgment transitions.
- Recipient pulls its durable inbox or subscribes to notifications and then pulls the record.
- Default logical delivery is brokered, agent-addressed communication through the central server.
  “Direct” means targeted to another agent rather than posted publicly.
- Actual server-to-server/peer transport is a later optional profile, not an MVP promise.

Transport states: `accepted`, `available`, `delivered`, `acked`, `expired`, `undeliverable`.
Work-result status belongs in response content, not in transport acknowledgment.

### 9.3 Replies, retries, and cancellation

- Replies set `in_reply_to` and preserve the `thread_id` and `correlation_id`.
- An original question/request can have several responses; no exactly-one-response guarantee.
- Acknowledgment uses an idempotent command; repeated acks do not create duplicate work.
- Inbox claiming gives a short visibility lease to one eligible recipient run.
- A crashed receiver may cause redelivery; receivers deduplicate `message_id`.
- Delivery is at least once, not exactly once.
- Optional `requested_response_schema` must refer to an approved registered JSON Schema.
  Do not execute sender-provided schemas or fetch arbitrary schema URLs.
- `cancel` is a request, not proof of cancellation; the recipient reports the outcome.
- Expiry prevents future delivery but does not undo work already performed.

Incoming messages are untrusted data. They cannot expand permissions, expose secrets,
change system policy, or authorize consequential external actions merely by asking.

### 9.4 A2A mapping

Bridge foreign discovery, tasks, message parts, and artifact references to Hivemind equivalents.
Maintain explicit mappings for remote agent, task, context, and message IDs.

Pin the bridge to a specific A2A specification/SDK version during implementation.
Do not assume the latest A2A schema is identical to Hivemind's `from/to/type/content` envelope.
Preserve original foreign payloads only when permitted, and record the translated source.

The bridge must enforce Hivemind permissions and remote authentication in both directions.
Receiving an A2A task does not grant access to the entire local ledger or shared memory.

## 10. Centralized agent chat board

### 10.1 Board and post schemas

A board belongs to a project and has a membership/read/write policy. Boards can be
project-wide or private to a team/task.

```json
{
  "post_id": "post_session_contract",
  "board_id": "board_engineering",
  "project_id": "prj_example",
  "thread_id": "thr_session_contract",
  "parent_post_id": null,
  "author": {
    "kind": "agent",
    "agent_id": "agt_frontend",
    "run_id": "run_frontend_001"
  },
  "kind": "finding",
  "title": "Session response contract",
  "content": {
    "media_type": "text/markdown",
    "text": "The frontend expects expires_at as an RFC 3339 timestamp."
  },
  "tags": ["auth", "api-contract"],
  "task_ids": ["tsk_auth_refactor"],
  "file_refs": [
    {
      "workspace_id": "ws_frontend",
      "path": "src/api/session.ts"
    }
  ],
  "evidence_refs": [
    {
      "kind": "artifact",
      "id": "art_frontend_test",
      "revision": 1
    }
  ],
  "status": "open",
  "trust": "agent_reported",
  "created_at": "2026-10-09T10:17:00Z",
  "updated_at": "2026-10-09T10:17:00Z",
  "expires_at": null,
  "revision": 1
}
```

Post kinds: `message`, `question`, `finding`, `decision`, `warning`, `request`, `status`, `answer`.
Thread status: `open`, `resolved`, `archived`. `resolved` does not imply factual verification.

Board authors are a discriminated union:

- Agent: `{"kind": "agent", "agent_id": "...", "run_id": "..."}`.
- Human: `{"kind": "human", "principal_id": "..."}`.
- System: `{"kind": "system", "service_id": "..."}` for authorized generated work/control cards.

The server derives authorship from authentication. Humans are not assigned synthetic agent IDs
just to participate, and cannot post as an agent/Nexus through a forged author field.
Direct agent-message envelopes keep their existing agent-only `from`/`to` schema.

In addition to conversational posts, Hivemind publishes permission-filtered structured
**work cards** from accepted work claims: agent/run, task, scope, current owner, state,
blockers, and pending coordination. Agents can semantically search “who is working on
the website header?” and link the card back to canonical ledger/claim revisions.
Work cards update automatically; they never expose excluded private context. Nexus controls
also publish an authorized result card, not an unverified claim of completion.

Revisions preserve authorized history. Deletion hides the post, removes it from retrieval,
and triggers dependent index/memory cleanup according to retention policy.

### 10.2 Retrieval workflow

Agents can search manually or ask Hivemind to assemble relevant context for their current task.
Board search accepts:

- Natural-language query.
- Project/board/task scope and permitted resource kinds.
- Optional file, symbol, tag, author, time-range, and status filters.
- Retrieval mode: `semantic`, `lexical`, or `hybrid`.
- Maximum results, cursor, token budget, and minimum index watermark.

Use hybrid retrieval by default. Exact code symbols and paths often need lexical matches,
while descriptions of similar work benefit from embeddings.

Responses include excerpts, canonical resource IDs/revisions, author, timestamp,
evidence/trust labels, ranking diagnostics, and index freshness. Similarity scores are
ranking signals, not truth scores.

### 10.3 Publishing and noise control

- No automatic public repost of every private agent event.
- Agents explicitly publish findings; optional status summaries are opt-in and rate-limited.
- Deduplicate exact content by scoped normalized hash.
- Semantic duplicates may be suggested for consolidation, not silently merged.
- Thread-level subscriptions use allowlisted filters and permission-checked fan-out.
- Agents can answer questions without generating notification storms.
- Apply per-agent publishing limits and deterministic loop guards.

The board is a **blackboard-style coordination surface**, not an instruction authority.
Retrieved content must remain separate from higher-trust policies and user directives.

### 10.4 Human messages, mentions, and replies

Humans post to the same shared board as agents. A human typing `@agent1` can select a
registered agent from autocomplete and send a public-within-board message directed at it.
**Mentioning one agent does not make the message private or exclude other agents.**
Every other authorized board reader can see it, retrieve it semantically, and reply.

Use typed rich-text segments rather than untrusted display-name parsing or character offsets.
The selected mention stores immutable `agent_id`; the renderer supplies a readable name.
Renaming an agent must not change who a historical mention addressed.

Proposed `POST /v0.1/boards/{board_id}/posts` request:

```json
{
  "client_post_id": "client_human_question_01",
  "kind": "message",
  "thread_id": null,
  "parent_post_id": null,
  "content": {
    "media_type": "application/vnd.hivemind.chat+json",
    "segments": [
      {"kind": "agent_mention", "agent_id": "agt_ui_a"},
      {"kind": "text", "text": ", can you explain the current website-header progress?"}
    ]
  }
}
```

The server assigns the canonical post/thread IDs, author, timestamp, and revision.
It also derives a searchable plain-text representation and stable mention refs. Historical
name snapshots can be server-recorded for display, but routing always uses the immutable ID.
Plain `@text` that is not resolved/selected is ordinary text, not an arbitrary recipient.

Mention processing:

1. Authorize the human/agent to post to the board.
2. Validate selected agent IDs and their current board/project eligibility without exposing
   whether an inaccessible name exists.
3. Commit the post, normalized content, and mention-notification outbox jobs together.
4. Notify each distinct mentioned agent through a durable notification inbox/adapter channel.
5. Notification references the canonical board post; it does not duplicate or privatize it.
6. Other agents discover the post through board subscriptions/search and can post a reply.

An agent reply uses the same `thread_id`, `parent_post_id`, authenticated agent author,
and optional typed mentions. Display the human/agent distinction and original parent.
Agents should reply when relevant; per-thread rate limits and causal loop guards prevent
every agent from replying indefinitely. Repeated retries use `client_post_id`/idempotency
so they do not duplicate posts or notifications.

Chat mentions are not coordinator controls. A human question does not bypass tool approval,
grant cross-project access, or automatically reassign a work claim.
The mention-notification inbox is separate from agent-only direct-message envelopes:
its typed `board_mention` record carries a notification ID, recipient agent ID, canonical
post/thread refs, delivery/ack state, and expiry. It never invents an agent `from` identity
for a human. Acknowledging a notification neither acknowledges a private direct message
nor hides the shared post from other readers.

### 10.5 Rich chat-content JSON Schema

This additional schema validates the chat content in the human-post example. Plain text and
sanitized Markdown remain supported in existing board posts; rich chat is an added content
variant, not a replacement for the direct-message schema.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Hivemind 0.1 rich chat content",
  "type": "object",
  "additionalProperties": false,
  "required": ["media_type", "segments"],
  "properties": {
    "media_type": {"const": "application/vnd.hivemind.chat+json"},
    "segments": {
      "type": "array",
      "minItems": 1,
      "maxItems": 128,
      "items": {
        "oneOf": [
          {
            "type": "object",
            "additionalProperties": false,
            "required": ["kind", "text"],
            "properties": {
              "kind": {"const": "text"},
              "text": {"type": "string", "minLength": 1, "maxLength": 65536}
            }
          },
          {
            "type": "object",
            "additionalProperties": false,
            "required": ["kind", "agent_id"],
            "properties": {
              "kind": {"const": "agent_mention"},
              "agent_id": {"type": "string", "minLength": 1, "maxLength": 128}
            }
          }
        ]
      }
    }
  }
}
```

Enforce aggregate UTF-8 byte limits separately. Cap distinct mention targets, for example at
10 per post, and re-check their authorization server-side. Render segments as React text
and mention components, never by injecting agent-provided HTML.

## 11. Unified memory system

### 11.1 Memory categories

| Category | Example | Lifetime and verification |
| --- | --- | --- |
| Working | Current task summary, active files, open questions | Run/task scoped; short lived |
| Episodic | What happened during a migration or debugging session | Evidence-linked run history |
| Semantic | A stable fact about an API or project | Must include sources and validity scope |
| Procedural | A tested workflow or recovery procedure | Versioned, reviewed; not executable by default |
| Preference | Explicit user/team formatting or workflow preference | Consent and owner-controlled scope |

Working context can be assembled from ledger projections without promoting every event
to persistent memory.

### 11.2 Memory schema

```json
{
  "memory_id": "mem_session_timestamp",
  "memory_version_id": "memver_session_timestamp_01",
  "project_id": "prj_example",
  "category": "semantic",
  "scope": {
    "kind": "project",
    "owner_id": "prj_example"
  },
  "title": "Session timestamp format",
  "content": {
    "media_type": "text/plain",
    "text": "The session API uses RFC 3339 timestamps for expires_at."
  },
  "status": "proposed",
  "origin": "extracted",
  "confidence": 0.82,
  "evidence_refs": [
    {
      "kind": "board_post",
      "id": "post_session_contract",
      "revision": 1
    }
  ],
  "validity": {
    "repository_id": "repo_example",
    "branch": "main",
    "observed_commit": "example-commit",
    "valid_from": "2026-10-09T10:17:00Z",
    "valid_until": null
  },
  "supersedes_version_id": null,
  "conflicts_with_memory_ids": [],
  "expires_at": null,
  "created_by": "worker_memory_extractor",
  "created_at": "2026-10-09T10:18:00Z",
  "revision": 1
}
```

Scopes: `run`, `agent`, `task`, `project`, `organization`, `user`.
The MVP supports run/agent/task/project scopes. Organization and user scopes require
explicit identity, consent, and administration, not automatic scope widening.

### 11.3 Memory lifecycle

```text
capture -> candidate extraction -> scoped retrieval/deduplication
        -> proposed
        -> verified | rejected
verified/proposed -> disputed | superseded | expired | deleted
```

Rules:

1. Store explicit memories or proposed extractor output with source references.
2. Validate generated JSON against the memory schema.
3. Do not accept a model's invented source IDs or unearned verification status.
4. Retrieve related records within authorized scope before deduplication.
5. Exact duplicates can link to the same record; semantic similarity alone cannot merge facts.
6. Contradictions create a dispute/version relationship rather than a destructive overwrite.
7. Verification requires an authorized human, explicit trusted evidence, or a configured rule.
8. Promotion to a broader scope requires authorization and a new policy evaluation.
9. Retrieval defaults to verified/current memories; proposed records require an explicit mode
   and remain labeled.
10. Source deletion, permission loss, changed code, and TTL expiry trigger invalidation checks.

Confidence is optional and is not treated as a calibrated probability without evaluation.
“Frequently retrieved” does not mean “true.”

### 11.4 Context and memory ownership

Separate the memory content's author, evidence owner, scope owner, and verifier.
A memory derived from private evidence cannot become public simply because a summarizer
paraphrased it. Derived content inherits no broader audience than the intersection of
its sources' allowed audiences.

If source permissions are incompatible, split the proposed memory or keep it private.
At read time, re-check the current permissions and source-validity status. Cached summaries,
embeddings, and exports must follow the same restrictions.

### 11.5 Optional Mem0/LangMem/Letta adapters

Keep Hivemind's normative memory schema authoritative. An adapter may import/export compatible
records or use external extraction tooling, but must preserve:

- IDs and versions or an explicit foreign-ID mapping.
- Ownership and scope.
- Source references and verification state.
- Expiry, supersession, and deletion behavior.

Do not adopt an external service as a hidden second source of truth. Keep the MVP independent
of hosted memory products; integrate only when their capabilities and licenses are verified.

## 12. EmbeddingGemma 2 integration

### 12.1 Verified model facts

The requested model card [S16], read on 2026-10-09, states:

- Model ID: `google/embeddinggemma-2`.
- Native shared embedding dimension: **768**.
- Supported Matryoshka dimensions: **128, 256, 512, 768**.
- Context window: **8,192 tokens**.
- Total parameters: **740M**; text-only configuration: **270M**.
- Text, code, image, video, and audio capabilities.
- Retrieval task prefixes and named prompts, including `SearchQuery`, `CodeRetrieval`,
  and `Document`.
- Use `bfloat16` or `float32`; **do not use `float16`** because outputs may be NaN or degraded.
- Truncated embeddings must be L2-normalized again.
- Model card declares Apache 2.0 and separately references a prohibited-use policy;
  review the pinned distribution's applicable license/policy before release.
- The page reported no deployed Hugging Face Inference Provider at research time.
  Plan local inference; do not assume a hosted endpoint exists.

Do not substitute details from the earlier EmbeddingGemma model.

### 12.2 Initial embedding profile

```json
{
  "profile_id": "eg2-text-768-v1",
  "model_id": "google/embeddinggemma-2",
  "model_revision": "<resolve-and-pin-actual-revision>",
  "dimension": 768,
  "modalities": ["text"],
  "precision_policy": "bfloat16_if_supported_else_float32",
  "normalize_embeddings": true,
  "query_prompt_name": "SearchQuery",
  "code_query_prompt_name": "CodeRetrieval",
  "document_format": "title: {title} | text: {content}",
  "chunker_version": "hivemind-text-code-v1",
  "max_chunk_tokens": 1024,
  "overlap_tokens": 128
}
```

The revision placeholder must be resolved to a real immutable model revision before
implementation ships. Pin compatible library versions too.

Start at 768 dimensions for simpler model-card-aligned behavior. Evaluate 256/512 dimensions
against the actual board/code/memory test set before changing the default.

### 12.3 Proposed Python encoding pattern

This sketch follows the model card; it is not validated implementation code.

```python
import torch
from sentence_transformers import SentenceTransformer

dtype = (
    torch.bfloat16
    if torch.cuda.is_available() and torch.cuda.is_bf16_supported()
    else torch.float32
)

model = SentenceTransformer(
    "google/embeddinggemma-2",
    revision=PINNED_MODEL_REVISION,
    config_kwargs={"vision_config": None, "audio_config": None},
    model_kwargs={"torch_dtype": dtype},
)

query_vector = model.encode(
    query,
    prompt_name="SearchQuery",
    normalize_embeddings=True,
    truncate_dim=768,
)

document_vector = model.encode(
    f"title: {title or 'none'} | text: {content}",
    normalize_embeddings=True,
    truncate_dim=768,
)
```

Use `prompt_name="CodeRetrieval"` for natural-language code queries. Do not add a second
document prompt when manually formatting titled documents. According to the model card,
`Document` supplies `title: none`; titled documents should be formatted manually.

Before storing vectors, assert expected shape, finite values, and valid normalization.
Convert storage vectors to a supported numeric format without changing the retrieval profile.

### 12.4 Chunking and indexing

- Tokenize with the pinned model's tokenizer, not a character-count approximation.
- Default to 1,024-token chunks with 128-token overlap, leaving room for prefixes/title.
- Split Markdown on sections and code on meaningful boundaries when possible.
- Preserve file paths, headings, line ranges, language, artifact/post revision, and chunk offsets.
- Split diffs by file/hunk and retain before/after direction so deletions are not mistaken
  for current code.
- Index current board/memory revisions by default; historical versions are an explicit mode.
- Do not embed every heartbeat, tool progress tick, or raw secret-bearing log.
- Index substantive observations, permitted artifacts, board content, summaries, and memories.
- Index current work cards/claims and agent/task descriptors for semantic agent discovery.
- Index authorized human/agent chat posts using normalized text; typed mentions remain
  structured metadata so search and routing do not depend on mutable display names.

Embedding cache keys include authorized partition, normalized content hash, model revision,
prompt/format version, modality, dimension, and chunker version. Deletion invalidates entries.
Do not expose cross-project cache-hit information.

### 12.5 Vector row schema

| Field | Type/purpose |
| --- | --- |
| `vector_row_id` | Stable key for resource revision + chunk + profile |
| `tenant_id`, `project_id`, `visibility_partition_id` | Mandatory isolation metadata |
| `resource_kind`, `resource_id`, `resource_revision` | Canonical back-reference |
| `chunk_id`, `chunk_index`, `line_start`, `line_end` | Chunk provenance |
| `content_hash`, `embedding_profile_id` | Deduplication and compatibility |
| `vector` | Fixed-size float vector, initially 768 dimensions |
| `created_at`, `expires_at` | Retrieval validity hints |
| `tags`, `task_ids`, `agent_ids` | Structured prefilter fields |
| `canonical_ingestion_seq` | Freshness accounting |
| `active` | Current-index eligibility hint, never the sole authorization check |

Store canonical content and final permissions in SQL. Index metadata is an optimization,
not permission authority.

### 12.6 Retrieval ranking and isolation

1. Authenticate the requester and establish an allowed project/board/memory scope.
2. Select only permitted visibility partitions.
3. Apply structured tenant/project/resource filters before retrieval.
4. Perform lexical and vector lookup within those partitions.
5. Optionally expand authorized seed IDs through bounded LadybugDB relationships.
6. Fuse lexical/vector rankings using reciprocal-rank fusion initially; keep graph relationship
   evidence as explicit context, not as a hidden guarantee of relevance.
7. Re-check candidate resource revisions, current ACLs, expiry, and source validity in SQL.
8. Refill within authorized partitions if stale/unauthorized candidates were removed.
9. Deduplicate chunks by canonical resource and assemble permitted excerpts.

Do not perform one global vector search and rely solely on filtering the top results afterward.
That both harms recall and can reveal side-channel information.

For fine-grained permissions that the backend cannot prefilter safely, use separate
visibility partitions or a bounded authorized-ID candidate set. Avoid dynamic arbitrary
query strings; compile allowlisted filters with parameter-safe APIs.

Rank freshness and verification as explicit tie-breakers, not hidden truth multipliers.
An optional reranker comes later and must not receive unauthorized text.

### 12.7 Model upgrades and resource budget

Model revision, dimension, or prefix changes create a new embedding profile and index.
Never mix incompatible vectors in one search operation.

Use a shadow reindex, compare retrieval quality, switch the active profile atomically,
and retain rollback until verification completes.

Raw float32 vector storage at 768 dimensions is 3,072 bytes per chunk, about 2.86 GiB
for one million vectors before metadata/index overhead. This is a sizing illustration,
not a SQLite database or LadybugDB total-storage estimate.

Measure actual CPU/GPU latency, memory, startup, throughput, and Windows packaging.
The parameter count alone does not establish runtime memory requirements.

## 13. Agent-ID and semantic context endpoints

### 13.1 One endpoint for explicit agents, semantic discovery, or both

**`POST /v0.1/context:query`** accepts an `agent_selector` containing **`agent_ids` and/or
`semantic_query` in the same endpoint**. This is the main Nexus context tool, and ordinary
agents can use it within their own grants. Caller identity comes from authentication.
The request's optional `requester_run_id` provides correlation, not authority.

```json
{
  "project_id": "prj_example",
  "requester_run_id": "run_nexus_001",
  "agent_selector": {
    "agent_ids": ["agt_ui_a", "agt_ui_b"],
    "semantic_query": "Agents working on the website header and navigation",
    "match": "union",
    "active_only": true,
    "max_agents": 10
  },
  "query": "Which header work overlaps, and what non-overlapping task can be assigned?",
  "include": [
    "ledger", "work_claims", "tasks", "board", "memory", "artifacts", "graph"
  ],
  "token_budget": 6000,
  "target_tokenizer": "caller-provided-supported-tokenizer",
  "require_verified_memory": true,
  "min_ingestion_seq": null,
  "graph_max_hops": 2,
  "timeout_ms": 5000
}
```

Selector semantics:

| Request | Selection behavior |
| --- | --- |
| `agent_ids: ["agt_ui_a"]` | Context for one authorized agent; no embedding needed |
| Several `agent_ids` | Context for those agents, with per-agent grouping |
| `semantic_query` only | Discover authorized agents by work/activity/context similarity |
| Both, `match: "union"` | Explicit agents plus semantically matched agents |
| Both, `match: "intersection"` | Search only within the explicit agent list |

At least one selector is required. Reject an empty/blank selector instead of selecting all
agents accidentally. Default `match` is `union`; document it explicitly in tool descriptions.
Apply `active_only` consistently to both branches. Never report whether an inaccessible
explicit ID exists. Semantic searches run only inside authorized projects/visibility partitions.

The semantic agent-discovery corpus contains authorized agent profiles, current work-claim
descriptions, task titles, and substantive ledger summaries, embedded with EmbeddingGemma 2.
It is not a scan of every agent's private transcript. Each hit links back to canonical agent,
run, task, source revision, and freshness information.

For union queries, preserve explicit agents first and cap discovered additions.
If the explicit list itself exceeds `max_agents` or the server limit, return a limit error;
do not silently drop explicitly requested agents. Use stable tie-breaking for equal scores.
For intersection queries, scores rank only candidates in the allowed explicit set.

### 13.2 Agent-selector JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Hivemind 0.1 agent selector",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "agent_ids": {
      "type": "array",
      "minItems": 1,
      "maxItems": 50,
      "uniqueItems": true,
      "items": {"type": "string", "minLength": 1, "maxLength": 128}
    },
    "semantic_query": {
      "type": "string",
      "minLength": 1,
      "maxLength": 4096,
      "pattern": "\\S"
    },
    "match": {"enum": ["union", "intersection"]},
    "active_only": {"type": "boolean"},
    "max_agents": {"type": "integer", "minimum": 1, "maximum": 50}
  },
  "anyOf": [
    {"required": ["agent_ids"]},
    {"required": ["semantic_query"]}
  ],
  "allOf": [
    {
      "if": {
        "properties": {"match": {"const": "intersection"}},
        "required": ["match"]
      },
      "then": {"required": ["agent_ids", "semantic_query"]}
    }
  ]
}
```

Return `selected_agents`, `selection_reasons`, source snapshots, omitted/truncated items,
and per-profile vector/graph watermarks. The example is a data-body sketch:

```json
{
  "selected_agents": [
    {
      "agent_id": "agt_ui_a",
      "run_ids": ["run_ui_a"],
      "selection_reasons": ["explicit_id", "semantic_match"],
      "work_scope_ids": ["scope_web_header"],
      "ledger_revision": 12
    },
    {
      "agent_id": "agt_ui_b",
      "run_ids": ["run_ui_b"],
      "selection_reasons": ["explicit_id", "semantic_match"],
      "work_scope_ids": ["scope_web_header"],
      "ledger_revision": 8
    }
  ],
  "freshness": {
    "canonical_through_ingestion_seq": 150,
    "vector_through_ingestion_seq": 147,
    "graph_through_ingestion_seq": 145
  },
  "warnings": ["Derived retrieval is behind current canonical state"],
  "items": [],
  "truncated": false
}
```

### 13.3 Assembly rules and shortcuts

Return a structured packet, not an opaque prompt:

1. Current agent/run/task state and capture-health warnings.
2. Relevant direct messages addressed to this agent, within permission and deadline policy.
3. Active reservations and overlapping-work warnings.
4. Explicit verified decisions and current memories.
5. Relevant board findings/questions.
6. Selected artifact/code excerpts and source links.
7. Clearly separated proposed/derived content when requested.
8. Work ownership, pending controls, and relevant authorized graph neighbors.

Each item carries source, resource revision, observed time, trust/origin, validity,
and token estimate. Report omitted items, staleness, and truncation decisions.

Use the caller's supported tokenizer when available; otherwise return a clearly labeled
estimate. EmbeddingGemma's tokenizer does not necessarily match the target LLM's tokenizer.

A configurable budget allocation might use 20% state/messages, 30% verified memory/decisions,
30% board/artifacts, and 20% code/context. Treat that as a heuristic, not a protocol requirement.

The packet must not claim to contain system instructions, hidden reasoning, or complete
private context. A framework adapter renders it into that framework's context with retrieved
material explicitly separated as untrusted evidence.

`GET /v0.1/agents/{agent_id}/context` is a convenient single-agent shortcut with the same
permissions. `POST /v0.1/context:assemble` renders a packet from the same selector/request
model; it is not a competing API with incompatible semantics.

Nexus does not automatically gain access to other agents' direct messages or private memory.
Delegate those scopes explicitly if needed. ID-only canonical context works when embeddings
or LadybugDB are offline. Semantic/graph modes either report degraded fallback or return a
bounded `index_not_ready`/`graph_not_ready` result; they never fabricate freshness.

## 14. Deterministic coordination, Nexus, and overlapping work

### 14.1 File reservations

Advisory reservations are useful even though they are not required for basic context capture.

Reservation fields: `reservation_id`, project/workspace, normalized path scope,
owner agent/run, mode, task, reason, granted time, expiry, revision, and fencing token.

- MVP scopes are exact files or directory prefixes, not unrestricted regex/glob programs.
- Modes: `exclusive_edit`, `shared_read`, and `intent`.
- Acquire/renew/release are transactional commands with idempotency keys.
- Expired leases become inactive using server time.
- Use a short configurable TTL, for example 120 seconds with renewal before expiry.
- Show conflicts in the ledger and notify owners.
- Cross-worktree edits to the same repository path can trigger a semantic overlap warning
  even when they are not the same filesystem reservation.
- Advisory reservations do not stop an uncooperative editor from writing.
- Fencing tokens become enforceable only when an editor/tool adapter explicitly checks them.

Agents can request coordination through direct messages or a board thread. Hivemind must
never silently overwrite files or discard work to resolve a conflict. Authorized control
can pause/steer an opted-in run with an audit trail and an actual adapter acknowledgment.

### 14.2 Central Hivemind versus Nexus

| Responsibility | Hivemind service | Nexus AI agent |
| --- | --- | --- |
| Persist memory/context | Authoritative storage and permissions | Read/write permitted records |
| Detect exact ownership conflict | Deterministic SQL/rule evaluation | Inspect and explain conflicts |
| Find conceptually similar work | Embeddings/graph candidate retrieval | Interpret ambiguity and propose canonical scopes |
| Pick a winner under configured policy | Deterministic, versioned arbitration | May propose priorities or scoped assignments within grants |
| Execute control | Validate, persist, dispatch, verify outcome | Submit authorized control proposals/commands |
| Change global policy/permissions | Restricted human/admin operation | Not allowed by default |
| Operate when no coordinator is online | Continue configured deterministic rules | Optional participant |

Nexus is a role of an ordinary `agent_id`/`run_id`, not a hard-coded trusted model.
Different model providers can serve as Nexus. An LLM's reasoning is not deterministic;
its proposed decisions become actionable only through deterministic validation against
current canonical state and policy.

### 14.3 Nexus role registration and leadership

An administrator/project owner grants `coordinator:acquire`, scoped context reads, and
specific control privileges to eligible agents. An agent cannot make itself Nexus just
by sending a manifest field or saying “I am the main mind.”

Proposed `POST /v0.1/projects/{project_id}/nexus:acquire`:

```json
{
  "agent_id": "agt_nexus",
  "run_id": "run_nexus_001",
  "requested_ttl_seconds": 60,
  "expected_project_revision": 5
}
```

Response includes `coordinator_lease_id`, `coordinator_epoch`, expiry, permitted operations,
and policy version. Default TTL: 60 seconds, renewal about every 20 seconds.

- At most one active Nexus lease per project in v0.1.
- Acquire/renew/release are transactional and idempotent.
- Every new leadership grant increments a monotonic project authority epoch.
- Control commands from an old/revoked lease are rejected, including delayed queued commands.
- On lease expiry, deterministic service rules continue; AI-origin controls stop.
- A human can revoke/take over through an audited authorized operation.
- Multiple reader/advisor agents are allowed, but they do not acquire independent main-control
  authority. Multi-Nexus domains are future work.

### 14.4 Work scopes and claims

File overlap alone is not enough: two agents can implement the same website header in
different files/worktrees. Introduce canonical **work scopes** and explicit claims.

Scope fields: `scope_id`, `project_id`, `kind`, canonical key, display name,
resource refs, optional parent, ownership policy, verification state, revision.

Kinds include `component`, `repository_path`, `api_contract`, `document_section`,
`research_topic`, and general `resource`. Example canonical key:
`website/component/header`.

```json
{
  "work_claim_id": "claim_ui_a_header",
  "project_id": "prj_example",
  "agent_id": "agt_ui_a",
  "run_id": "run_ui_a",
  "task_id": "tsk_web_header",
  "scope_ids": ["scope_web_header"],
  "mode": "exclusive_owner",
  "intent": "Implement the website header and primary navigation",
  "state": "active",
  "priority": 50,
  "ownership_epoch": 9,
  "expires_at": "2026-10-09T10:25:00Z",
  "revision": 4
}
```

Claim modes: `exclusive_owner`, `collaborator`, `observer`.
Explicit collaboration can allow several agents if the policy declares compatible subscopes.
Header visual design and header accessibility testing are not automatically duplicates.

Agents register claims **before starting new work**, not at the next five-second batch.
Claim commands are checked synchronously and create immediate ledger/board status updates.
The server assigns ownership epoch/revision/expiry; clients cannot forge the granted record.
Claim renewal protects long-running work, and stale claims expire without marking work failed.

The central board exposes current structured “working on” cards derived from claims.
These are distinct from private transcripts and from optional narrative board posts.

### 14.5 Conflict detection and deterministic arbitration

Use three levels:

1. **Exact:** Same exclusive scope, intersecting normalized resource scopes, or incompatible
   declared ownership. Detect and arbitrate using current SQLite records.
2. **Structural:** Parent/child component or dependency overlap backed by canonical scope
   relationships. Read canonical relationships for control, not an eventually consistent graph alone.
3. **Semantic:** EmbeddingGemma/graph retrieval suggests similar intentions, such as “navbar”
   versus “website header.” Open a candidate conflict; do not auto-cancel work on similarity alone.

Nexus may propose that semantic candidates map to the same scope. An authorized configured
rule/reviewer verifies that mapping. Once persisted, exact deterministic rules apply.

Example versioned priority order:

```text
1. explicit human-approved ownership/assignment
2. existing valid exclusive owner, unless an authorized override is present
3. configured task priority (larger value first)
4. earlier server-accepted claim sequence
5. stable agent_id as final tie-breaker
```

The same canonical snapshot + policy version must produce the same decision. Store
`conflict_id`, inputs/evidence, input revisions, policy version, chosen owner, affected claims,
and proposed/applied controls. Revalidate revisions immediately before commit.
Never arbitrate based on an agent's unverified claimed priority or display name.

Before any new mutation, a controlled adapter obtains a scope permit. A rejected new claim
does not start the conflicting work. If legacy/unguarded work is already running, report
that enforcement is advisory until a control-capable adapter reaches a safe boundary.

### 14.6 Durable agent-control envelope

Control is a privileged protocol surface, not just natural-language chat.

```json
{
  "protocol": "hivemind",
  "protocol_version": "0.1.0",
  "control_id": "ctl_redirect_header_02",
  "project_id": "prj_example",
  "issuer": {
    "kind": "nexus",
    "agent_id": "agt_nexus",
    "run_id": "run_nexus_001",
    "coordinator_lease_id": "nexus_lease_07",
    "coordinator_epoch": 7
  },
  "target": {
    "agent_id": "agt_ui_b",
    "run_id": "run_ui_b"
  },
  "action": {
    "type": "steer",
    "away_from_scope_ids": ["scope_web_header"],
    "replacement_task_id": "tsk_web_footer",
    "checkpoint_required": true
  },
  "preconditions": {
    "target_run_revision": 12,
    "work_claim_revision": 4,
    "ownership_epoch": 9,
    "policy_version": 3
  },
  "evidence_refs": [
    {
      "kind": "conflict",
      "id": "conflict_header_01",
      "revision": 1
    }
  ],
  "reason": "The header has an existing exclusive owner; footer work is available.",
  "expires_at": "2026-10-09T10:26:00Z"
}
```

Actions: `assign_task`, `steer`, `pause`, `resume`, `cancel`.
`assign_task` applies to an idle/eligible agent. `steer` changes work at a safe boundary.
`pause` prevents new controlled operations and asks for a checkpoint. `resume` requires
current valid claims/permits. `cancel` requests an explicit terminal transition.

The server resolves the issuer from authentication; the envelope cannot grant coordinator
privileges. Service-origin actions use `issuer.kind: "policy_engine"` with a recorded rule ID.
Action-specific schemas forbid irrelevant fields and arbitrary shell/code execution payloads.

Control state machine:

```text
proposed -> validated -> queued -> delivered -> acknowledged -> applying -> applied
         -> rejected
queued/delivered/acknowledged -> expired | superseded | failed
applying -> applied | failed | outcome_unknown
```

`acknowledged` means received, not completed. Only runtime evidence confirms `applied`.
Expose `outcome_unknown` rather than inventing success after a crash.
Control IDs are deduplicated by both server and adapter; delivery is at least once.

### 14.7 Runtime enforcement and safe steering

Agents advertise supported action types, checkpoint behavior, cancellation boundaries,
and whether tool invocation honors scope permits.

For a header conflict:

1. Hivemind detects that UI-A and UI-B claim `scope_web_header`.
2. Deterministic arbitration retains UI-A as valid owner.
3. Nexus queries both IDs plus “website header” through `context:query`.
4. Nexus proposes steering UI-B to the footer, or the configured policy chooses an
   eligible alternative without a Nexus.
5. The server checks current claim/task/run revisions, replacement-task availability,
   Nexus epoch/grants, and target capabilities.
6. Reserve the replacement task; prevent UI-B from obtaining new header edit permits.
7. UI-B checkpoints partial progress and reports quiescence.
8. Commit the new claim/assignment and release the old claim only at the documented transition.
9. UI-B confirms the footer work began; mark control applied.
10. Publish the ownership/result to the structured board and record graph relationships.

If no safe alternative exists, pause UI-B and request a decision rather than make up work.
If the adapter cannot stop an in-flight tool safely, wait/report the boundary; do not claim
instant preemption. Short-lived permits and epoch checks bound stale-authority windows, but
they do not reverse already completed writes.

Persist adapter checkpoints and applied control IDs. A reconnect reconciles pending controls
against canonical state before further mutations. Controlled mode fails closed for new
mutating tools when permission/lease validation is unavailable; read-only/advisory fallback
can be separately configured.

### 14.8 Limits on coordinator authority

Nexus cannot expand its own permissions, silently change global policy, delete another
agent's work, or authorize external publication/purchases simply by issuing a control.
Human approval gates remain in force for consequential operations.
Control privileges are scoped by project, target agents, operations, resources, and expiry.

Nexus may explain a proposed decision in natural language, but the executable action
must be typed, authorized, revision-checked, and recorded. Conflict resolution and mutation
authorization must not depend on vector or graph index freshness.

## 15. API surface and tool exposure

### 15.1 Core HTTP endpoints

Paths below are proposed under `/v0.1`:

| Method/path | Purpose |
| --- | --- |
| `POST /agents` | Register an authorized logical identity |
| `GET /agents`, `GET /agents/{agent_id}` | Permission-filtered discovery/manifests |
| `PATCH /agents/{agent_id}` | Revision-checked manifest update |
| `POST /runs` | Start an authorized run |
| `POST /runs/{run_id}/heartbeat` | Refresh presence and capture health |
| `GET /runs/{run_id}` | Read run context |
| `POST /events:batch` | Ingest structured observations |
| `GET /events` | Cursor-based authorized history |
| `GET /ledger` | Current agent/run roster |
| `GET /ledger/{agent_id}` | Agent aggregate and active runs |
| `GET /agents/{agent_id}/context` | Single-agent context shortcut |
| `POST /tasks`, `GET /tasks`, `PATCH /tasks/{task_id}` | Scoped task records |
| `POST /messages` | Durably send an agent-addressed message |
| `GET /agents/{agent_id}/inbox` | Pull authorized recipient messages |
| `POST /messages/{message_id}:claim` | Lease delivery to an eligible recipient run |
| `POST /messages/{message_id}:ack` | Acknowledge delivery/processing |
| `GET /agents/{agent_id}/notifications` | Pull durable board-mention notifications |
| `POST /notifications/{id}:ack` | Idempotent notification acknowledgment by its recipient adapter |
| `POST /boards`, `GET /boards` | Create/list boards according to policy |
| `POST /boards/{board_id}/posts` | Publish a post |
| `GET /boards/{board_id}/posts` | List/paginate posts or threads |
| `GET /boards/{board_id}/work-cards` | Current structured agent-work and coordination cards |
| `GET /projects/{id}/mention-options` | Eligible agent-name autocomplete, bounded and permission-filtered |
| `GET /projects/{id}/conversations` | Human-observer agent-to-agent timeline within explicit grants |
| `GET /agents/{agent_id}/conversation` | Inbound/outbound and relevant shared-board items for an agent tab |
| `GET /projects/{id}/agent-appearances` | Persisted base colors and authoritative current Nexus styling |
| `PATCH /posts/{post_id}`, `DELETE /posts/{post_id}` | Revision/delete with cleanup |
| `POST /search` | Authorized lexical/semantic/hybrid retrieval |
| `POST /context:query` | One/many explicit agent IDs and/or semantic agent discovery |
| `POST /memories`, `GET /memories` | Create/read scoped memories |
| `POST /memories/{memory_id}/versions` | Propose a new version |
| `POST /memories/{memory_id}:verify` | Authorized verification transition |
| `DELETE /memories/{memory_id}` | Governed deletion |
| `POST /context:assemble` | Generate a bounded context packet |
| `POST /work-claims`, `GET /work-claims` | Register/list immediate structured work ownership |
| `POST /work-claims/{id}:renew`, `DELETE /work-claims/{id}` | Renew/release work claims |
| `GET /conflicts`, `GET /conflicts/{id}` | Exact/structural/semantic conflict records |
| `POST /conflicts/{id}:resolve` | Policy-checked resolution with expected revisions |
| `POST /projects/{id}/nexus:acquire` | Acquire the authorized main-coordinator lease |
| `POST /projects/{id}/nexus:renew`, `POST /projects/{id}/nexus:release` | Maintain/release Nexus authority |
| `POST /controls`, `GET /controls/{id}` | Validate/queue and inspect typed agent controls |
| `GET /agents/{agent_id}/controls` | Target adapter pulls pending permitted controls |
| `POST /controls/{id}:ack`, `POST /controls/{id}:result` | Report receipt and actual runtime outcome |
| `POST /runs/{id}/permits:check` | Obtain a short-lived scoped tool-mutation permit |
| `POST /graph:query` | Permissioned typed graph queries/expansion, not arbitrary Cypher |
| `POST /artifacts`, `GET /artifacts/{artifact_id}` | Authorized artifact upload/read |
| `POST /reservations`, `POST /reservations/{id}:renew` | Advisory file coordination |
| `DELETE /reservations/{id}` | Authorized release |
| `GET /streams` | Resumable permission-filtered SSE |
| `POST /exports` | Authorized export job, not unrestricted database download |
| `GET /` and UI routes | Serve the portable React app, separate from versioned API routes |

Resource-read endpoints omitted from this overview are added as needed with the same
authorization/revision conventions. Publish actual OpenAPI alongside JSON Schemas.

### 15.2 Idempotency, revisions, and errors

- Events/messages include their own stable IDs.
- Other mutation commands accept `Idempotency-Key`, bound to principal and operation.
- Revision-sensitive mutation requires `If-Match` or an explicit expected revision.
- Use HTTP 409 for semantic conflicts and 412 for failed `If-Match`.
- Use 429 for rate limits, 413 for payload limits, 422 for schema validation.
- Authorization failures do not disclose whether a private resource exists.

Error example:

```json
{
  "error": {
    "code": "revision_conflict",
    "message": "The resource changed. Reload before applying this update.",
    "retryable": false,
    "request_id": "req_example_42",
    "details": {
      "resource_id": "post_session_contract",
      "current_revision": 2
    }
  }
}
```

Only return `current_revision` when the caller may read the resource. Never echo rejected
secrets, raw tool arguments, tokens, or private recipient names into errors.

### 15.3 SSE subscriptions

SSE is a notification/event feed, not the durable inbox itself.

- Support `Last-Event-ID` with an opaque cursor.
- Use per-principal subscription sequences rather than leaking gaps in a global tenant stream.
- Replay retained authorized notifications after reconnect.
- If the cursor is too old, return `resync_required` and provide safe snapshot guidance.
- Send keepalive comments and document idle timeout behavior.
- Re-check permissions as they change; revoked readers stop receiving protected content.
- Bind cursors to a stable filter definition. Changing filters requires a new snapshot/cursor.
- Handle the snapshot-to-stream race with a server-issued snapshot watermark.
- Support board, conversation, agent-presence, appearance, and Nexus-role notification topics.
- Browser clients use same-origin secure session authentication; never put long-lived tokens
  in SSE URLs. Native EventSource cannot freely attach arbitrary authorization headers.

### 15.4 MCP façade

Expose a small initial tool set:

```text
hivemind_register_run
hivemind_report_status
hivemind_get_ledger
hivemind_get_agent_context
hivemind_query_context
hivemind_send_message
hivemind_get_inbox
hivemind_ack_message
hivemind_post_board
hivemind_search
hivemind_remember
hivemind_get_memory
hivemind_assemble_context
hivemind_reserve_files
hivemind_release_reservation
hivemind_claim_work
hivemind_release_work
hivemind_get_conflicts
hivemind_query_graph
nexus_acquire_role
nexus_renew_role
nexus_propose_control
nexus_control_agent
nexus_get_control_result
```

MCP resource URIs can represent authorized ledger rows, board threads, and memory records.
Use compact tool descriptions so Hivemind itself does not consume excessive context.
Tool metadata is not an authorization system; server policy still governs every operation.

An MCP server alone does not automatically observe all other tools in a host. Automatic
capture requires host hooks, an editor adapter, a wrapper, or an explicitly authorized proxy.

`hivemind_query_context` exposes the exact same `agent_selector.agent_ids` and
`agent_selector.semantic_query` fields as the HTTP endpoint. `nexus_control_agent`
submits a typed control; it is not a remote arbitrary-code execution tool.
Agents learn pending controls through their adapter channel, not only by voluntarily
calling a context tool.

## 16. Canonical storage and indexes

### 16.1 Proposed SQL tables

| Table | Key/important fields |
| --- | --- |
| `tenants`, `projects`, `principals` | Administrative and authorization boundaries |
| `agent_manifests` | Agent ID, owner, manifest JSON, revision |
| `agent_memberships` | Principal/agent/project grants |
| `runs` | Run ID, agent/task/workspace, status, latest presence, revision |
| `workspaces` | Workspace/repository identity, sanitized VCS metadata |
| `tasks`, `task_assignments`, `task_dependencies` | Task state and graph references |
| `events` | Event ID, project ingestion sequence, source sequence, payload/hash |
| `ledger_snapshots` | Agent/run projection, revision, reducer version |
| `file_observations` | Workspace/path, event refs, provenance |
| `artifacts` | Content handles, hashes, scope, retention |
| `messages` | Message ID, from/to, type/thread/correlation, expiry |
| `message_deliveries` | Recipient claim, transport state, attempt and ack metadata |
| `boards`, `board_memberships` | Board policy |
| `board_posts`, `board_post_versions` | Current revision and authorized history |
| `board_mentions`, `mention_notifications` | Typed target IDs and idempotent durable mention notifications |
| `agent_appearances` | Random base palette assignment, display metadata, revision |
| `human_ui_preferences` | Optional project/tab/filter preferences, scoped to the human principal |
| `memories`, `memory_versions` | Scoped memory identity/version/status |
| `memory_evidence`, `memory_conflicts` | Source and contradiction relationships |
| `reservations` | Scope, owner, TTL, fencing token, revision |
| `work_scopes`, `scope_relationships`, `work_claims` | Canonical work ownership and overlap facts |
| `coordination_policies`, `coordination_conflicts` | Versioned rules, inputs, outcomes, evidence |
| `coordinator_role_bindings`, `coordinator_leases` | Nexus eligibility, active lease, epoch |
| `controls`, `control_attempts`, `control_results` | Typed action queue, runtime evidence, state |
| `scope_permits`, `agent_checkpoints` | Mutating-tool grants and safe steering checkpoints |
| `relation_facts` | Canonical, permissioned node/edge facts for graph projection |
| `vector_chunks`, `embedding_profiles` | Canonical chunk provenance and encoding compatibility |
| `vec_*` virtual tables | SQLite `sqlite-vec` search structures, managed by the server |
| `graph_watermarks` | LadybugDB partition contiguous projection progress |
| `resource_permissions` | Resource-level grants and policy revision |
| `outbox_jobs` | Job key, resource revision, retries, lease, error class |
| `index_watermarks` | Partition/profile contiguous progress |
| `idempotency_records` | Command key, payload hash, response, expiry |
| `subscription_notifications` | Replayable principal/filter-scoped notifications |
| `audit_entries` | Privileged reads/exports/deletions and policy changes |

Use composite indexes for project+sequence, recipient+state+expiry,
board+thread+time, task+status, and memory scope+status.
Enforce foreign keys and ownership constraints. Bound task dependencies to prevent
accidental cycles when a DAG policy is enabled.

### 16.2 Projection rebuild

Version reducers and snapshot formats. Rebuild deterministic projections from retained
canonical events into a shadow table, verify results, and switch atomically.

Replay must not resend messages, call external services, regenerate different summaries,
or rerun tools. Derived outputs have their own durable records and evidence.
Destructive retention/deletion limits later replay; expose the retention boundary.

### 16.3 Backup and recovery

- Use a consistent SQLite backup API/snapshot, not a casual copy of a live WAL database.
- Back up permitted artifact blobs and canonical records together.
- Vector indexes may be rebuilt from canonical content; record model/profile versions.
- LadybugDB projections may be rebuilt from canonical relationship facts; record graph schema version.
- Test restore plus outbox reconciliation.
- Restore must honor deletion tombstones and not resurrect expired private memories.
- Define backup retention and deletion/crypto-erasure policy before storing sensitive data.

### 16.4 SQLite-based vector storage

Use one canonical SQLite database per service data directory initially, including `vec0`
virtual tables from the pinned `sqlite-vec` extension. Keep provenance/permissions in ordinary
tables and vectors in extension-managed structures.

Each row links to `vector_chunks` with resource revision, chunk offsets, content hash,
embedding profile, project, and visibility partition. Manage dimension/profile-compatible
tables separately. Only trusted administrative mappings generate table names.

Processing:

1. Commit canonical content and its embedding job in the ingestion transaction.
2. Compute the permitted embedding outside the SQLite write transaction.
3. Open a short transaction; re-check current revision, deletion, and indexing eligibility.
4. Insert/replace vector and metadata together; acknowledge the job and update watermark.
5. Ignore obsolete jobs rather than reinsert an old revision.

SQLite/extension transactional behavior must be verified in Phase 0, including rollback,
WAL, backup, upgrade, and extension loading on all supported platforms.
Load only the pinned trusted extension. Disable arbitrary agent-supplied extension paths.

Use `vec0` metadata/partition prefilters only where the pinned version demonstrably supports
the required expression/type. A SQL join that removes unauthorized items *after* a global
top-k operation is not adequate permission isolation.

For unsupported fine-grained filters, use permission-separated vector tables/partitions or
compute distances over a bounded, already authorized candidate set. Hybrid search combines
FTS5 and KNN in application ranking code; it does not depend on LanceDB or another service.
Control decisions never rely on the vector-distance score.

### 16.5 LadybugDB graph model and synchronization

LadybugDB adds graph functionality **alongside**, not instead of, SQLite vector retrieval.
It stores a rebuildable property-graph projection for provenance, relationships, related
work discovery, dependency exploration, and context expansion.

Canonical node kinds:

| Node | Meaning |
| --- | --- |
| `Agent`, `Run` | Logical identity and execution |
| `HumanPrincipal` | Board participant identity, visible only within its granted scope |
| `Task`, `WorkScope`, `WorkClaim` | Work, components/resources, ownership |
| `Repository`, `Workspace`, `File` | Optional coding context |
| `Artifact`, `BoardPost` | Outputs and shared observations |
| `Memory`, `MemoryVersion`, `Decision` | Durable knowledge and evidence |
| `Conflict`, `Control` | Coordination evidence and action outcome |

Suggested edge families:

| Relationship | Example |
| --- | --- |
| `HAS_RUN` | Agent to run |
| `MENTIONS`, `REPLIES_TO` | Shared board post to addressed agent/parent post |
| `WORKS_ON`, `CLAIMS_SCOPE` | Run/claim to task/scope |
| `ASSIGNED_TO`, `COORDINATED_BY` | Task to agent; project work to Nexus |
| `PART_OF`, `DEPENDS_ON` | Component hierarchy and task dependencies |
| `TOUCHED`, `PRODUCED`, `AUTHORED` | Run to file/artifact/post |
| `SUPPORTED_BY`, `DERIVED_FROM` | Memory/decision to evidence |
| `SUPERSEDES`, `CONTRADICTS` | Memory versions and conflicting claims |
| `RELATED_TO` | Explicitly labeled semantic candidate relationship |
| `AFFECTS`, `RESOLVES` | Control to run/claim; decision to conflict |

Every graph fact has canonical IDs, project/visibility partition, revision, provenance,
source event/record refs, origin (`observed`, `reported`, `derived`), verification,
validity bounds, and tombstone status. Canonical `relation_facts` in SQLite can describe
these facts without requiring agents to write Cypher.

Graph projection workflow:

```text
SQLite commit: canonical facts + durable graph job
    -> LadybugDB writer applies idempotent node/edge revision
    -> record partition graph watermark
    -> expose bounded graph retrieval
```

There is **no distributed transaction** across SQLite and LadybugDB. A crash after graph
write but before SQL job acknowledgment causes a safe idempotent retry. Apply revisions
monotonically and record projector/schema versions. A rebuild from SQLite must reproduce
the authorized current graph.

Typed `POST /graph:query` operations include `neighbors`, `dependencies`, `evidence`,
`related_work`, and `impact`. Requests specify permitted seed IDs, edge types, maximum hops,
maximum nodes/edges, and minimum watermark. Defaults: 2 hops, at most 100 nodes/200 edges.
No arbitrary write-Cypher endpoint is exposed to agents.

Illustrative internal Cypher shape, to verify against the pinned LadybugDB schema:

```cypher
MATCH (a:Entity)-[:HAS_RUN]->(r:Entity)-[:WORKS_ON]->(t:Entity)
WHERE a.kind = 'agent'
  AND a.project_id = $project_id
  AND a.id IN $allowed_agent_ids
RETURN a.id, r.id, t.id
```

`Entity` is one possible physical node table with a `kind` discriminator; per-kind tables
are also possible. Choose one after the spike. Bind parameters; never concatenate
agent text into Cypher. This sketch is not a validated LadybugDB migration/query.

Security:

- Partition graph data by tenant/project and, where needed, visibility group.
- Authorize seed and intermediate nodes/edges during traversal, not only final results.
- Canonical SQL checks determine visibility, validity, and current revisions.
- A path through a private intermediate node is private even if its endpoint is public.
- Derived relationships inherit source restrictions.
- No counts/explanations about inaccessible paths leak through the response.

Semantic relationships are candidate context, not verified causal or ownership facts.
Nexus can use graph neighborhoods to understand why work overlaps, but actual arbitration
rechecks canonical scope/claim facts in SQLite.

Delete or revoke nodes, edges, dependent summaries, and graph caches through durable jobs,
while canonical read checks stop exposure immediately. Graph projector failure degrades
graph retrieval, not ledger/memory/control availability.

## 17. Security, privacy, and trust boundaries

### 17.1 Authentication and authorization

Local development:

- Bind to loopback by default.
- Require a generated local credential or protected Unix socket where available.
- Use ACL-restricted named-pipe/local configuration on Windows where applicable.
- Do not expose the service to a LAN without explicit setup.

Shared deployment:

- TLS required; short-lived scoped bearer credentials/OIDC integration.
- Bind write permissions to authenticated principals and registered agents/runs.
- Separate agent capabilities from administrative/human privileges.
- Support per-project and per-resource permissions.
- Treat adapters/sidecars as capture authorities only for their configured workspaces.

Example scopes: `events:write`, `ledger:read`, `messages:send`, `inbox:self`,
`board:read`, `board:write`, `memory:propose`, `memory:verify`, `context:read`,
`reservations:write`, `artifacts:read`, `work-claims:write`, `graph:read`,
`coordinator:acquire`, `controls:propose`, and individually granted
`controls:steer`, `controls:pause`, `controls:resume`, `controls:assign`, `controls:cancel`.
Target adapters have narrowly scoped `controls:receive`/`controls:result` permissions.
Policy administration and authority grants remain separate restricted operations.

Human board participants receive `board:read`/`board:write` as granted. Authorized human
observers additionally receive `conversations:read` for specified projects/agents.
That scope permits inspection, not agent impersonation or inbox claiming/acknowledgment.
Being able to read the main board does not automatically permit reading private direct messages.
Local project setup grants the owning human its intended observation scopes explicitly;
the UI setup flow records which agent conversations are observable. Other board participants
do not inherit the owner's privileges.

### 17.2 Threats and required controls

| Threat | Control |
| --- | --- |
| Agent identity spoofing | Token subject binding; no trusting `from` or `agent_id` claims alone |
| Cross-project data leakage | SQL authorization, vector prefilters/partitions, canonical read checks |
| Prompt injection in board/messages | Treat retrieved text as untrusted; never elevate it to system policy |
| Memory poisoning | Evidence links, proposal status, verification permissions, conflict handling |
| Accidental secret capture | Local redaction/exclusions before spool or upload; safe error handling |
| Unattributed file edits | Provenance labels; no heuristic attribution presented as confirmed |
| Replay/duplicate work | Stable IDs, payload hashes, idempotent inbox consumption |
| Stale status or races | Revisions, source sequences, server-time leases |
| Message/board loops | Rate limits, expiry, reply-depth and causal loop guards |
| Resource exhaustion | Bounded batches, spool, queries, schemas, artifact sizes, deadlines |
| Malicious URLs/artifacts | No arbitrary server-side fetch; sanitized previews; safe downloads |
| Export leakage | Explicit permission, audit, scoped export; no external upload by default |
| Compromised summarizer/provider | Opt-in egress, minimal authorized inputs, source validation |
| Malicious model package | Pin trusted revisions/libraries; avoid arbitrary remote code execution |
| HTML/script in dashboard | Render sanitized Markdown; no raw HTML execution |
| Nexus impersonation or stale leadership | Authorized role binding, single active lease, epochs, revocation |
| Unauthorized control/assignment | Typed actions, target-specific grants, revision/policy checks |
| Semantic false positives halting work | Similarity creates a candidate; canonical scopes drive arbitration |
| Graph-private path leakage | Permissioned seeds/intermediate edges, partitions, canonical rechecks |
| Stale graph authorizing work | Coordination checks current SQLite facts, not a graph projection |
| Duplicate/stale controls after reconnect | Adapter deduplication, checkpoint reconciliation, epoch checks |
| Browser session/CSRF abuse | Same-origin sessions, CSRF protection on writes, restrictive CORS/CSP |
| Forged mentions or Nexus crown | Immutable target IDs and server-derived author/role metadata |
| Autocomplete leaking hidden agents | Authorized, bounded project queries and privacy-preserving errors |
| Private DM repost through UI | Explicit visibility labels; no automatic mirroring/quoting into shared boards |

### 17.3 Capture exclusions and privacy defaults

Default exclusions include `.env*`, private keys, credential stores, browser profiles,
dependency/build folders, and the adapter's own spool. Projects can add explicit policies.
Do not rely solely on filenames; scan permitted text for recognizable secret patterns.

- Capture tool names/timing/results metadata before considering full arguments/output.
- Conversation capture is opt-in and limited to visible permitted messages.
- Never collect hidden chain-of-thought.
- Do not continuously read every file in a repository.
- Embeddings are sensitive derived data, not anonymization.
- A checksum is not permission to publish the corresponding content.
- Redaction reports should reveal counts/types, not the secret itself.

### 17.4 Deletion and “append-only” history

Append-only describes ordinary event operation, not an exemption from privacy obligations.
Support privileged, audited deletion and retention compaction.

When a resource is deleted or its access revoked:

1. Hide/revoke it in canonical reads immediately.
2. Prevent it from appearing in search/context packets immediately via canonical checks.
3. Delete/tombstone vector rows, graph nodes/edges, and caches with a measurable deadline.
4. Invalidate dependent memories/summaries and evaluate permitted replacement versions.
5. Apply spool/export/backup retention or crypto-erasure rules.
6. Retain only legally/permissibly necessary minimal audit metadata.

Do not claim deletion completeness until all required stores and dependencies are accounted for.

## 18. Configuration example

This is a proposed configuration shape, not a supported CLI format yet.
Credential values belong in a secure local mechanism, not this file.

```yaml
protocol: "hivemind"
protocol_version: "0.1.0"

server:
  bind: "127.0.0.1"
  port: 8765
  data_dir: ".hivemind-data"
  deployment_mode: "local"

web:
  enabled: true
  framework: "react"
  serve_static_from_hivemind: true
  base_path: "/"
  open_browser_when_requested: true
  default_view: "main_board"
  realtime_transport: "sse"
  mention_autocomplete_debounce_ms: 150
  max_mention_options: 8
  max_mentions_per_post: 10
  agent_color_assignment: "random_palette_with_replacement"
  persist_agent_colors: true
  reserve_gold_for_nexus: true
  nexus_style: "gold_name_static_glow_crown"
  nexus_crown_icon: "inline_svg"
  respect_reduced_motion: true
  public_mirror_of_private_messages: false

capture:
  flush_interval_ms: 5000
  heartbeat_interval_ms: 5000
  presence_stale_after_ms: 15000
  presence_offline_after_ms: 30000
  max_batch_events: 100
  max_batch_bytes: 1048576
  spool_max_bytes: 268435456
  conversation_capture: false
  collect_hidden_reasoning: false
  exclude:
    - "**/.env*"
    - "**/.ssh/**"
    - "**/node_modules/**"
    - "**/.git/**"
    - "**/.hivemind-data/**"

storage:
  canonical: "sqlite"
  sqlite_wal: true
  vectors: "sqlite_vec"
  lexical: "sqlite_fts5"
  graphs: "ladybugdb"
  graph_dir: ".hivemind-data/graph"
  canonical_relationships: "sqlite"
  sqlite_vec_revision: "<pin-trusted-extension-release>"
  ladybugdb_revision: "<pin-trusted-graph-release>"

graph:
  max_hops: 2
  max_nodes: 100
  max_edges: 200
  allow_agent_write_cypher: false

embedding:
  model_id: "google/embeddinggemma-2"
  model_revision: "<pin-actual-model-revision>"
  profile_id: "eg2-text-768-v1"
  dimension: 768
  modalities: ["text"]
  normalize: true
  precision: "bfloat16_if_supported_else_float32"
  chunk_tokens: 1024
  overlap_tokens: 128
  allow_network_download_on_startup: false

summary:
  enabled: false
  provider: null
  min_interval_ms: 15000
  require_evidence: true
  allow_external_egress: false

memory:
  extraction_enabled: false
  auto_verify: false
  default_scope: "project"
  include_proposed_by_default: false

messages:
  default_ttl_seconds: 3600
  visibility_lease_seconds: 30
  max_reply_depth: 12

reservations:
  default_ttl_seconds: 120
  enforce_in_tools: false

coordination:
  enabled: true
  mode: "deterministic_with_optional_nexus"
  policy_version: 3
  require_work_claim_before_mutation: true
  semantic_conflicts_are_candidates: true
  automatic_rule_actions:
    - "deny_conflicting_claim"
    - "pause_at_safe_boundary"
  preserve_existing_owner: true
  claim_ttl_seconds: 120
  permit_ttl_seconds: 10
  controlled_adapter_offline_behavior: "deny_new_mutations"

nexus:
  enabled: true
  max_active_per_project: 1
  role_requires_admin_grant: true
  lease_ttl_seconds: 60
  renew_interval_seconds: 20
  default_control_actions: ["assign_task", "steer", "pause", "resume"]
  allow_policy_self_modification: false
  allow_permission_self_escalation: false

retention:
  observation_events_days: 30
  completed_run_summaries_days: 90
  message_content_days: 30
  board_posts_days: null
  memory_days: null
  deletion_worker_target_seconds: 60
```

Validate contradictory settings at startup. Indefinite board/memory retention requires an
explicit project policy; the example's `null` values mean policy-defined, not an automatic
promise to retain everything forever. Metadata and artifact retention may differ.
Validate the reserved Nexus style separately from random ordinary color tokens. UI settings
must not authorize conversation reads or Nexus controls.

## 19. End-to-end example

Two UI agents attempt website-header work:

1. UI-A and UI-B register distinct IDs and report to Hivemind through controlled adapters.
2. UI-A obtains the exclusive `website/component/header` claim before editing.
3. Hivemind records its claim immediately and publishes a structured board work card.
4. UI-A's tool hook reports edits; ordinary file/status details flush every five seconds.
5. UI-B asks for the same scope. Hivemind rejects conflicting new ownership deterministically.
6. If UI-B was already performing unguarded legacy work, Hivemind records a conflict and
   requests a supported safe pause; it does not pretend those writes can be undone.
7. An authorized Nexus agent acquires the project coordinator lease.
8. Nexus calls `context:query` with both agent IDs and the semantic phrase “website header.”
9. The context response groups both agents' work, canonical claims, board findings,
   evidence-linked memories, and authorized LadybugDB relationships.
10. Nexus submits a typed `steer` action assigning UI-B to the footer instead.
11. Hivemind validates the action, preserves UI-A's claim, and reserves eligible footer work.
12. UI-B checkpoints at a safe boundary and confirms the new assignment.
13. Hivemind marks the action applied, updates the ledger/board, and projects graph edges.
14. UI-A asks UI-B a standardized direct question about shared design tokens.
15. They publish the agreed contract and source artifact to the shared searchable board.
16. An explicit or reviewed extracted memory records the contract and its evidence.
17. Another agent retrieves that memory through the same context/memory surface.
18. Later source deletion/revocation invalidates dependent vectors, graph evidence,
    summaries, and memories.
19. A human opens the React Main board, selects UI-A from `@` autocomplete, and asks for progress.
20. UI-B can see and answer that shared thread even though UI-A was the addressed agent.
21. The human switches to UI-B's in-page tab and sees `UI-B -> @UI-A` agent messages
    within granted observation permissions; the current Nexus has its golden crown styling.

Repeat the conflict test **without Nexus**: configured central policy must still deny
duplicate ownership and issue its permitted safe-pause action. Nexus adds planning and
contextual steering; it is not required for deterministic collision prevention.

## 20. Implementation phases and acceptance criteria

The schedule below assumes roughly two engineers, with periodic security/product review.
It is a planning estimate, not a delivery commitment. Validate feasibility in Phase 0.

### Phase 0: feasibility and reuse spike, approximately 1 week

Deliver:

- A comparison of MCP Agent Mail, original ACP projects, and Hivemind-named projects.
- License/name/package checks and a record of build-versus-integrate decisions.
- EmbeddingGemma 2 text-only startup, retrieval, finite-output, and packaging tests.
- Windows/macOS/Linux SQLite+sqlite-vec+LadybugDB smoke tests.
- Verify LadybugDB query/concurrency/backup behavior and exact license/package names.
- Verify sqlite-vec transactional rollback, permissioned KNN filtering, and scale limits.
- A small labeled board/code/memory retrieval dataset.
- Threat model and instrumentation feasibility assessment for two target runtimes.

Acceptance:

- Requested model works at a pinned revision with compatible pinned dependencies.
- No float16 or nonfinite-vector path survives testing.
- Permissive dependencies and notices are documented.
- Capturable signals and attribution limits are clearly stated.
- At least one runtime can actually enforce scoped edit permits and checkpoint steering.

### Phase 1: specification and durable core, approximately 2 weeks

Deliver:

- JSON Schemas, examples, OpenAPI, error model, and version negotiation.
- Identity/project/run APIs and authentication.
- Event ingestion, idempotency, revisions, deterministic ledger reducer.
- SQLite migrations, FTS setup, outbox, retention boundary.
- Canonical work scopes/claims, conflict facts, policy versions, and authority epochs.
- SDK emitters and local five-second buffering/spooling.

Acceptance:

- Replayed duplicate batches cannot double-count edits.
- Crashes at the commit/response boundary do not lose acknowledged events.
- Two runs under one agent ID are represented independently.
- Ledger state matches retained event replay.

### Phase 2: capture, inboxes, and live React app, approximately 3 weeks

Deliver:

- Runtime/tool wrapper adapter plus scoped filesystem observer.
- Presence/capture-health reporting and watcher reconciliation.
- Durable messages, claims, acks, response/cancel examples.
- Browser-served React app with Main board as the base, agent-conversation overview,
  dynamic agent tabs, roster, and snapshot/SSE live timeline.
- Stable random nonunique agent colors and authoritative gold/glow/crown Nexus override.
- Advisory reservations and overlap notifications.

Acceptance:

- Two agents exchange a question and response across a server restart.
- Duplicate delivery cannot automatically execute duplicate work in the sample adapter.
- Unattributed watcher events remain unattributed.
- Ordinary capture respects the configured interval; messages bypass buffering.
- Lease conflicts and expiration are deterministic under concurrency.
- Structured work cards match canonical claims, not guessed activity.
- A human can observe authorized `Agent2 -> @Agent1` messages in Agent2's in-page tab.
- Browser reconnect/tab switching does not claim or acknowledge an agent's inbox.

### Phase 3: board, vectors, and graph retrieval, approximately 3 weeks

Deliver:

- Threaded boards with revisions and permissions.
- Human/agent actor schemas, rich mention segments, autocomplete, durable mention notifications,
  shared-thread replies, and public-versus-direct visibility labels.
- Embedding worker and SQLite sqlite-vec tables.
- LadybugDB projection, canonical relation facts, graph watermarks, bounded typed queries.
- Semantic agent-discovery corpus from permitted work cards/agent descriptors.
- Hybrid search, profile/version tracking, index watermarks.
- Canonical ACL rechecks, stale-row filtering, scoped context excerpts.

Acceptance:

- A newly committed post is immediately available lexically.
- Semantic search reports lag until the embedding job finishes.
- Cross-project/fine-grained permission tests reveal no unauthorized content.
- Revision/deletion jobs do not resurrect old board text.
- Retrieval evaluation beats the lexical-only baseline on the semantic subset.
- Graph projection rebuild and crash/retry produce the same authorized relationship view.
- Graph traversal cannot reveal private intermediate nodes/edges.
- Human `@Agent1` posts remain visible to all permitted board readers; Agent2 can reply.
- Agent renames/duplicate names do not change mention recipients.

### Phase 4: memory and context assembly, approximately 2 weeks

Deliver:

- Versioned memory CRUD, scope/verification/conflict policies.
- Optional source-linked extraction, disabled by default.
- Context packet API with token budgets and trust separation.
- Combined agent-ID/semantic selectors, union/intersection behavior, per-agent grouping.
- Source-revocation and deletion dependency handling.

Acceptance:

- Memory conflicts preserve both claims and evidence.
- Unverified extracted text cannot become verified through a model-generated flag.
- A private source cannot be paraphrased into broader-scope memory.
- Context packets remain within their configured token-budget policy.
- Deleted or revoked sources stop appearing in packets immediately.
- Single-ID, multi-ID, semantic-only, union, and intersection queries pass conformance tests.

### Phase 5: deterministic control and Nexus, approximately 3 weeks

Deliver:

- Deterministic claim conflict arbitration and versioned policy execution.
- Nexus role grant/acquire/renew/revoke with single-project leader fencing.
- Typed controls, safe checkpoints, idempotent adapter delivery/results.
- Tool-scope permits, target capabilities, offline/reconnect reconciliation.
- Header-versus-footer demonstration and a no-Nexus conflict test.

Acceptance:

- Identical canonical state and policy produce identical owner decisions.
- Nexus cannot control targets outside granted scopes or expand its own authority.
- Old coordinator epochs and superseded controls cannot start new mutations.
- Unsupported actions report advisory/unsupported instead of false success.
- A steering action is applied only after checkpoint and confirmed runtime transition.
- Vector/graph outages do not stop canonical conflict prevention.

### Phase 6: interoperability and release hardening, approximately 2 weeks

Deliver:

- MCP façade and one Agent Client Protocol adapter.
- Version-pinned A2A bridge prototype.
- OpenTelemetry export, operational metrics, backup/restore docs.
- Conformance runner, language SDK examples, Docker/native packaging as appropriate.
- Security/fault/load tests, model card/license notices, migration/export guide.
- Playwright browser tests for routes/tabs, live messages, mentions, accessibility,
  private-message isolation, random-color collisions, and Nexus role handover.

Acceptance:

- Two different runtimes share memory/context through the same Hivemind service.
- A third-party client can pass the published conformance suite.
- Crash/restart and restore drills preserve acknowledged canonical state.
- Authentication/permission failures do not leak private resource identities.

**Total rough estimate:** 16 weeks, with uncertainty driven by runtime control hooks,
model packaging, graph/vector compatibility, browser interaction scope, permission complexity,
and reuse findings.

## 21. MVP scope and release boundaries

### Must ship in the first usable MVP

- Unique agent/run IDs and project-scoped authentication.
- Structured event ingestion and a deterministic current ledger.
- Configurable capture flush interval, default five seconds.
- Tool/file activity with honest provenance and capture-health labels.
- Durable direct messages using standardized `from`, `to`, `type`, and `content`.
- Threaded board with lexical and EmbeddingGemma 2 semantic retrieval.
- Scoped explicit memory CRUD and source references.
- Context packet API.
- Combined direct-agent-ID/semantic context queries for Nexus and permitted agents.
- SQLite-based vector storage and LadybugDB graph projection/retrieval.
- Structured work claims/cards and deterministic duplicate-ownership prevention.
- Optional authorized Nexus with typed steering controls and adapter enforcement.
- MCP tools, Python/TypeScript clients, CLI, and the portable live React browser app.
- Main board, agent-conversation view, dynamic agent tabs, human mention autocomplete,
  shared replies, ordinary random nonunique colors, and reserved golden Nexus styling.
- Retention, deletion, authorization, and restart safety.

### Can follow after the MVP

- Automatic NLP summaries and memory extraction.
- Advanced procedural memory evaluation.
- More runtime/editor adapters and fuller A2A interop.
- Organization/user memory and enterprise identity integrations.
- Advanced graph analytics/ontology extraction, semantic subscription routing, and reranking.
- Image/audio/video retrieval through the same model.
- Multi-server federation and optional true peer transport.
- A separately negotiated production distributed-storage profile; SQLite is the initial vector layer.

Avoid letting optional automatic memory or federation delay basic trustworthy coordination.

## 22. Testing and evaluation plan

### 22.1 Protocol/schema tests

- Validate every published example against the normative JSON Schemas.
- Positive/negative test vectors for fields, sizes, timestamps, enums, and extensions.
- Version and capability mismatch tests.
- Compare generated Python/TypeScript types against the same schema contract.
- Fuzz parsers, paths, message content, filters, and artifact references.

### 22.2 State and reliability tests

- Duplicate batches, conflicting duplicate payloads, partial acceptance.
- Out-of-order tool start/completion, missing source sequences, late observations.
- Agent/run restart, spool replay, canceled shutdown, disk full.
- Kill the server before/after SQL commit and before/after index updates.
- Outbox retries, worker crashes, poisoned jobs, contiguous watermark correctness.
- Concurrent revision updates and reservations.
- Heartbeat gaps that must not turn into false task failures.
- Replay must not create side effects.
- LadybugDB write succeeds but SQL graph-job acknowledgment crashes; retry remains idempotent.
- Simultaneous exclusive work claims use a stable deterministic winner.
- Delayed Nexus commands after lease expiry/takeover are rejected.
- Duplicate controls, expired permits, partial steering, and crashed checkpoint recovery.
- ID-only context and deterministic policy remain usable during vector/graph outages.

### 22.3 Security tests

- Forge `from`, `agent_id`, `run_id`, owner fields, and server-origin events.
- Cross-project board/message/artifact/memory access.
- Permissions changed after vector indexing or context caching.
- Private-source paraphrase into public memory.
- Prompt injection in a board post, reply, log, and memory.
- Path traversal, Windows drive/UNC paths, case aliases, and symlink escapes.
- Secret capture/redaction and safe error logging.
- HTML/script rendering, dangerous links, arbitrary schema/URL fetches.
- Flooding, oversized payloads, pathological filters, repeated conversation loops.
- Restore after deletion must not resurrect private content.
- Nexus privilege escalation, forged coordinator epochs, and target-run mismatch.
- Graph traversal through private intermediate evidence and relationship-count leakage.
- Semantic false positives must not trigger direct destructive or terminal actions.
- Human-author/agent-author impersonation, forged mention targets, and spoofed Nexus styling.
- Board mention visibility versus restricted direct-message observer access.
- CSRF, session expiry, tab/project permission revocation, and autocomplete roster leakage.

### 22.3.1 React browser and interaction tests

Use Playwright against an isolated test project with synthetic agents/humans:

- Default root opens Main board; deep-link routes survive refresh.
- Open two agent tabs inside the same page; focus one through a recipient mention.
- Receive a live Agent2-to-Agent1 event and verify `Agent2 -> @Agent1`.
- Type `@`, navigate the popup, select the correct immutable ID, and post as a human.
- Validate stale-response cancellation, duplicate-name disambiguation, rename behavior.
- Let a nonmentioned eligible agent reply to the same human thread.
- Verify offline mention delivery, idempotent post retry, and SSE reconnect deduplication.
- Force the random color generator to choose the same ordinary color twice; accept both.
- Verify gold/glow/crown only for the authoritative current Nexus and update on role transfer.
- Test keyboard/screen-reader labels, reduced motion, contrast, mobile widths, unread/scroll behavior.
- Ensure observer feeds do not expose or automatically repost unauthorized direct messages.

These are planned browser checks; no React implementation exists yet in this document-only task.

### 22.4 Retrieval evaluation

Create a versioned, manually labeled dataset containing:

- Conceptually similar board findings with different wording.
- Code-symbol/path exact matches.
- Related tasks across worktrees.
- Outdated decisions and contradictory memories.
- Multilingual content if required by actual users.
- Permission-isolated near-identical documents.
- Hard negatives that mention the same keyword but solve a different problem.
- “Website header”/“navbar” semantic discovery across agents with different descriptions.
- Explicit-ID/semantic union/intersection and exact agent grouping.
- Graph-assisted provenance/dependencies versus merely similar content.

Report Recall@10, MRR/NDCG@10 where appropriate, latency, freshness, and forbidden-result rate.
Forbidden-result rate must be zero in the test suite. Compare lexical-only, vector-only,
and hybrid retrieval at 768/512/256 dimensions. Do not rely only on public model benchmarks.

### 22.5 Initial performance targets

Targets below are provisional local-service goals to benchmark, not claimed capabilities:

- Sustain 50 agents averaging 2 substantive events/second each, excluding sampled heartbeats.
- Ordinary ledger visibility within configured flush interval plus 1 second at p95,
  while connected and within capacity.
- Local message acceptance under 250 ms p95, excluding receiver processing.
- Metadata ledger reads under 200 ms p95 on a representative test dataset.
- Hybrid search under 1 second p95 **excluding cold model startup**, with query encoding included.
- Semantic indexing lag under 15 seconds p95 at the benchmark workload.
- No loss of acknowledged canonical mutations during tested crashes.
- Exact claim-conflict decisions under 250 ms p95, without requiring an embedding call.
- Graph projection lag under 15 seconds p95 at the benchmark workload.
- Bounded spool/index backlog and explicit degraded-state reporting under overload.

Record hardware, artifact sizes, resource count, embedding device, and warm/cold conditions
with every result. If CPU encoding cannot meet the target, document it and offer batching,
preloading, or optional supported GPU operation rather than masking latency.

## 23. Operations and observability

### 23.1 Operational telemetry

Track:

- Event rate, duplicate/conflict rate, ingestion latency, rejection categories.
- Capture queue depth, disk budget, missing sequences, dropped observations.
- Per-run presence and active tool duration.
- Message acceptance/delivery/ack/expiry latency and inbox backlog.
- Board publishing rate and notification-loop guard activations.
- Outbox retry/dead-letter counts and contiguous index lag.
- Embedding latency, batch size, nonfinite outputs, cache hit rate.
- Search latency, empty/fallback rate, authorized candidate refill.
- Memory proposal/verification/dispute/supersession counts.
- Deletion completion, source invalidation, backup/restore success.
- Graph projection lag/retry and traversal bound/fallback rates.
- Work-claim conflicts, deterministic owner decisions, steering outcome/latency.
- Nexus lease/epoch churn, rejected stale controls, adapter permit/checkpoint health.
- Human chat/mention notification latency, live-stream reconnects, and browser stale-view reporting.

Logs use safe identifiers and sanitized metadata, not full prompts/content.
Trace IDs link Hivemind operations to runtime/tool/model observations where authorized.
Do not export private content to an observability vendor by default.

Workers use leased jobs and bounded retries. Permanent failures go to an inspectable
dead-letter queue with safe error metadata. A broken embedding worker should degrade
semantic search, not stop messages or ledger ingestion.

### 23.2 Portable React browser app

Human observability is a **React + TypeScript webpage**, not a required Electron/native app.
Ship a Vite-built static bundle with the Hivemind service and serve it on the same origin
as the API. Users open the app in their normal browser on Windows, macOS, or Linux.
The frontend has no dependency on a particular agent vendor.

Proposed launch UX:

```text
hivemind serve --open
    -> start local service
    -> open http://127.0.0.1:8765/ in the default browser
    -> select authorized project
    -> show its main message board
```

This is a planned command, not an existing installed CLI. Browser opening is an explicit
local launch option. Remote/headless deployments print the configured address instead;
they do not launch a server machine's browser or weaken authentication.

Use standard responsive browser routing and deep links:

```text
/projects/{project_id}/board
/projects/{project_id}/conversations
/projects/{project_id}/agents/{agent_id}
```

Serve SPA fallbacks only for known UI route families; never turn an API error into an
HTML success page. No external CDN/assets are required at runtime. Shared deployments
use TLS and configured authentication; local binding remains loopback by default.
“Portable” means a browser frontend backed by Hivemind, not a serverless/offline-only HTML file.

### 23.3 Main board as the base, with tabs inside the webpage

The default/base tab is **Main board**, the project's live general message board.
Keep it pinned. Adjacent tabs provide an **Agent conversations** overview and dynamically
opened agent tabs. Clicking an agent name, roster row, or mention opens/focuses that agent
inside the React app instead of spawning a separate browser tab/window.

```text
Project: Website
[ Main board ] [ Agent conversations ] [ Agent2 ] [ Agent1 ] [ Nexus ]

Main board:
  Human:       @Agent1 Can you explain header progress?
  Agent1:      The navigation layout is ready; tests are running.
  Agent2:      I can help check keyboard accessibility.
  Nexus:       Agent1 owns the header; Agent2 is assigned to the footer.
```

UI layout:

- Project selector and connection status at the top.
- Agent roster showing presence, task, work scopes, blockers, and unread counts.
- In-page tab strip, with closeable agent tabs and a pinned main board.
- Center timeline with shared-board and direct-message visibility labels.
- Thread/reply pane when requested.
- Composer on the shared board; agent tabs can prefill a selected-agent mention.
- Optional details drawer for ledger, artifacts, memory evidence, and coordination events.

Closing an agent tab does not cancel its run. Switching tabs does not change authentication,
claim an inbox message, or mark it acknowledged by the agent. Human unread state is separate
from agent delivery/acknowledgment state.

### 23.4 Seeing agents talk to each other

The **Agent conversations** view shows authorized agent-to-agent messages across the project.
An agent tab shows that selected agent's incoming/outgoing messages plus shared-board posts
it authored, is mentioned in, or participated in, with filters to reduce noise.

If the human is viewing Agent2 and Agent2 sends to Agent1, render:

```text
Agent2 -> @Agent1
Can you confirm the navigation breakpoint?
```

For an incoming reply:

```text
Agent1 -> @Agent2
The compact navigation begins at the agreed tablet breakpoint.
```

Recipient tags are clickable and open/focus the recipient's in-page tab. The renderer derives
them from immutable `from`/`to` IDs for direct messages; agents cannot spoof the sender or
recipient by inserting a lookalike name in message text.

Use distinct labels for **Shared board** and **Agent direct message**. The observer feed
does not automatically repost private direct messages to the main board or make them searchable
by unrelated agents. Project owners can grant designated humans read-only conversation
visibility; every feed item is still authorized server-side.

The main board remains collaborative: a human's `@Agent1` post is visible to other authorized
board agents, and Agent2 can answer even though it was not the mentioned target.
This is a shared thread, not a private DM masquerading as a public message.

### 23.5 Mention autocomplete and human participation

Typing `@` in the board composer opens a name menu anchored to the caret.

- Filter by readable display name/handle with prefix and substring matches.
- Query only eligible agents within the selected board/project; no global hidden roster.
- Debounce requests, for example by 150 ms, and return at most 8 options initially.
- Abort or discard stale responses when query/project/composer changes.
- Show name, short ID, presence, task snippet, base color, and current Nexus badge.
- If names collide, include short ID/task information; store the selected immutable ID.
- Arrow keys move selection; Enter/Tab select; Escape closes; mouse/touch selection works.
- After selection, insert a mention chip and keep the caret in the composer.
- Unresolved text stays text; the UI warns before assuming any agent was notified.

Opening an agent tab may prefill `@ThatAgent` in a **Shared main board** composer, but the
audience label stays visible: “Visible to everyone permitted on this board.”
The user can remove/change the mention. Reading a private direct message must not prefill
its content into a public post; any disclosure/quoting requires an explicit permitted action.

Names resolve server-side on submission. An offline-but-authorized agent can receive a durable
mention notification; the UI shows offline/queued rather than promising an immediate reply.
The current Nexus appears under its actual agent name with a Nexus role badge, not as a
second fake agent identity.

Replies preserve the shared thread and permit any eligible agent/human to participate.
Posting and mention notification are immediate chat operations, not delayed until the
five-second ordinary capture flush.

### 23.6 Agent colors and unique Nexus styling

Assign each ordinary agent a **random color from an accessible palette with replacement**.
Colors are **not unique**: different agents can have the same color. Persist the base
assignment per agent/project so reconnects and browser sessions do not continually recolor
the roster. Color never acts as an identity or authorization signal.

Reserve golden styling for the **current Nexus**:

- Golden name text with a subtle static glow.
- Crown icon rendered as an inline SVG beside the name.
- Text/screen-reader label “Nexus, main coordinator.”
- The same appearance on the message board, mention menu, roster, and agent tab.

Example ordinary palette: blue, teal, violet, coral, green, and magenta.
Do not include the reserved gold treatment in that random palette.
If two ordinary agents receive teal, retain both; do not retry until colors become unique.
Humans use a clearly labeled neutral/human style rather than impersonating agent colors.

The Nexus style is a server-authoritative **role override**, not a user-set arbitrary CSS field:

```json
{
  "agent_id": "agt_nexus",
  "project_id": "prj_example",
  "base_color_token": "violet",
  "effective_role": "nexus",
  "effective_style": "gold_name_static_glow_crown",
  "coordinator_epoch": 7,
  "revision": 3
}
```

When leadership expires/transfers, live roster/tab/menu styling moves to the new Nexus.
An old coordinator returns to its persisted ordinary base color. Historical messages may
record `role_at_send` and display a clearly labeled “Nexus at send time” badge; that must
not imply the author still has control authority. The unique active golden treatment is
project-scoped, consistent with one active Nexus lease per project.

Respect reduced-motion preferences; a static glow is enough. Avoid flashing/pulsing by
default, meet text contrast requirements in light/dark themes, and never identify Nexus
or an agent solely by color/icon.

### 23.7 Live updates, browser state, and delivery correctness

Use a server-authorized snapshot plus SSE subscriptions for live board/conversation updates.
The React client subscribes to post/reply revisions, permitted direct-message notifications,
presence, work cards, and Nexus appearance changes.

- Request the initial bounded timeline and its stream cursor.
- Subscribe from that cursor to close the snapshot/stream race.
- Normalize items by canonical origin/resource ID and revision.
- Optimistic human posts use `client_post_id`; replace them with canonical records after acceptance.
- Deduplicate reconnect/replay events and ignore older revisions.
- Keep scroll position when the reader is viewing older messages; show a “new messages” indicator.
- Reconnect with backoff and show live/reconnecting/offline/degraded status explicitly.
- Paginate/virtualize long timelines; cap in-memory history.
- Immediately remove protected content after permission revocation and clear scoped caches.
- Do not persist private transcript bodies in browser local storage by default.

Native EventSource uses same-origin session cookies; writes use CSRF protection and secure
session handling. Do not place bearer tokens in query strings or use permissive CORS.
Use a fetch-based SSE client only if the authentication strategy requires it and the same
replay/authorization guarantees are maintained.

The React app is a view over canonical records, not a new message database. A conversation
item stores/returns `origin_kind`, canonical ID/revision, author, target IDs, timestamps,
visibility, content refs, and delivery state. Combining board posts and direct messages
does not duplicate them or alter their audience.

### 23.8 Browser acceptance criteria

- The app opens in a normal browser and defaults to Main board.
- A human can open/close/switch multiple agent tabs within one React webpage.
- Agent2-to-Agent1 messages display `Agent2 -> @Agent1` in Agent2's tab.
- `@` autocomplete supports keyboard/mouse/touch and disambiguates duplicate names.
- A human mention notifies the intended immutable agent ID.
- Another authorized agent can see and reply to that shared thread.
- Two ordinary agents can intentionally receive the same color.
- Only the current project Nexus has active gold/glow/crown styling.
- Role handover changes live styling without granting privileges through the UI.
- Private direct messages remain inaccessible without explicit observer permissions.
- A reconnect does not duplicate posts, mention notifications, or unread accounting.
- The layout works at desktop/mobile widths and with keyboard/screen-reader navigation.

## 24. Proposed repository layout and deliverables

```text
hivemind/
  LICENSE
  NOTICE
  README.md
  spec/
    protocol.md
    security.md
    versioning.md
    schemas/
    examples/
    test-vectors/
  server/
    api/
    auth/
    ingestion/
    reducers/
    messaging/
    boards/
    memory/
    context/
    coordination/
    nexus/
    graph/
    storage/
    workers/
  sdk/
    python/
    typescript/
  adapters/
    runtime-hooks/
    tool-wrapper/
    filesystem/
    agent-client-protocol/
    a2a/
    mcp/
  cli/
  dashboard/
    src/
      app/
      components/
        MainBoard/
        AgentConversationBoard/
        AgentTabs/
        MessageComposer/
        MentionAutocomplete/
        AgentName/
        NexusBadge/
      state/
      api/
      styles/
    tests/
      playwright/
  tests/
    conformance/
    integration/
    security/
    fault-injection/
  benchmarks/
    retrieval/
    ingestion/
  docs/
    quickstart.md
    capture-limitations.md
    operations.md
    licensing.md
    migrations.md
```

The normative spec must be usable independently of this server. Publish conformance profiles:

- **Core:** Identity, runs, events, ledger, authentication, revisions.
- **Messaging:** Durable envelopes, inboxes, acknowledgments.
- **Board:** Posts, threads, revisions, search metadata.
- **Memory:** Scoped versions, evidence, validity, deletion.
- **Capture:** Provenance, source sequences, capture-health reporting.
- **Search:** Embedding profiles, isolation, watermarks, hybrid retrieval.
- **Graph:** Canonical relationship facts, LadybugDB projections, bounded authorized expansion.
- **Coordination:** Work scopes/claims, deterministic policies, controls, adapter evidence.
- **Nexus:** Leased main-coordinator role, ID/semantic context tools, scoped control authority.
- **Human board/UI:** Typed human authors, agent mentions, conversation-observer grants,
  portable React delivery, role-aware appearance, and resumable live views.

An implementation can advertise only the profiles it actually supports.

## 25. Decision register and remaining implementation questions

### 25.1 Decisions recommended by this plan

| Topic | Decision |
| --- | --- |
| Project name/directive | Hivemind: unified memory for all AI agents |
| Primary value | Governed shared memory, context, and deterministic coordination |
| Source of truth | Transactional relational records |
| Event capture | Structured hooks first, watcher fallback |
| Update cadence | Five-second configurable coalescing for ordinary observations |
| NLP | Optional derived enrichment, never the sole activity authority |
| Messaging | Brokered, durable, agent-addressed JSON envelopes |
| Board | Threaded, permissioned, hybrid-searchable |
| Memory | Scoped, versioned, source-linked, verification-aware |
| Embedding model | Required `google/embeddinggemma-2`, pinned revision |
| Vector engine | SQLite + sqlite-vec, MIT option |
| Graph engine | LadybugDB, derived from canonical SQLite relationship facts |
| Main coordinator | Optional authorized AI agent holding the Nexus role |
| Context lookup | Same endpoint accepts one/many agent IDs and/or semantic search |
| Conflict arbitration | Canonical work scopes/claims and versioned deterministic rules |
| Runtime control | Typed actions, scope permits, checkpoints, epochs, confirmed results |
| Human interface | Portable React webpage served by Hivemind; Main board is the base |
| Agent navigation | Multiple in-page agent tabs and an agent-to-agent conversation overview |
| Human messaging | Shared-board `@agent` mentions with eligible-name autocomplete and open replies |
| Agent appearance | Persisted random colors with duplicates allowed; reserved gold/glow/crown for active Nexus |
| Initial dimension | 768; evaluate smaller supported dimensions later |
| Interoperability | MCP façade; Agent Client Protocol adapter; version-pinned A2A bridge |
| Own project license | MIT, with third-party attribution and license compliance |

### 25.2 Resolve during Phase 0, not by silently assuming

- Public packages/namespaces for Hivemind/Nexus, given existing Hivemind-named projects.
- Exact target runtimes and what hooks they expose.
- Whether existing coordination code is worth integrating or only adapting.
- Real model revision and compatible dependency/platform matrix.
- Local-only first release versus a separately secured shared-server profile.
- Actual workspace privacy, retention, and evidence-verification rules.
- CPU/GPU support and measured latency on representative hardware.
- Exact sqlite-vec/LadybugDB release, license notices, transaction and platform support.
- Runtime control boundaries and the initial human-approved automatic coordination policy.
- Initial human observer grants, browser session setup, and supported accessibility/browser matrix.

These questions do not block writing this plan. They do affect implementation/release scope.

## 26. Research sources

Access/research date: **2026-10-09**. Model facts were read from the user-supplied model page.
Other links were discovered through online searches. Repository-level findings should be
rechecked against pinned releases before reuse.

- **[S1] MCP:** https://modelcontextprotocol.io/ and https://github.com/modelcontextprotocol/servers
- **[S2] A2A:** https://github.com/a2aproject/A2A and https://a2a-protocol.org/latest/specification/
- **[S3] LF AI & Data, “ACP Joins Forces with A2A”:**
  https://lfaidata.foundation/communityblog/2025/08/29/acp-joins-forces-with-a2a-under-the-linux-foundations-lf-ai-data/
- **[S4] Agent Client Protocol:** https://agentclientprotocol.com/protocol/v1/overview
  and https://agentclientprotocol.com/protocol/file-system
- **[S5] MCP Agent Mail:** https://github.com/Dicklesworthstone/mcp_agent_mail
  and https://github.com/Dicklesworthstone/mcp_agent_mail_rust
- **[S6] Beads:** https://github.com/steveyegge/beads
- **[S7] Mem0:** https://github.com/mem0ai/mem0
- **[S8] LangMem:** https://github.com/langchain-ai/langmem
  and https://langchain-ai.github.io/langmem/
- **[S9] Letta shared memory:** https://docs.letta.com/v1-sdk/memory/shared-memory
- **[S10] OpenTelemetry GenAI:** https://opentelemetry.io/blog/2026/genai-observability/
  and https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-agent-spans/
  (the latter currently redirects readers conceptually to the newer conventions repository)
- **[S11] Documentation-oriented Agent Context Protocol:**
  https://github.com/prmichaelsen/agent-context-protocol
- **[S12] Same-name multi-agent coordination repository:**
  https://github.com/agent-context-protocol/agent-context-protocol
- **[S13] Related memory-ledger repositories:**
  https://github.com/Riley-Coyote/memory-ledger-protocol-v0.2
  and https://github.com/alexandryasukaitisss-cmd/memory-ledger
- **[S14] Related research proposal:** https://arxiv.org/html/2602.15055v1
- **[S15] SQLite vector licensing and official feature documentation:**
  https://builders.mozilla.org/project/sqlite-vec/
  and https://alexgarcia.xyz/sqlite-vec/features/knn.html
  and https://alexgarcia.xyz/blog/2024/sqlite-vec-metadata-release/index.html
- **[S16] Required EmbeddingGemma 2 model card:**
  https://huggingface.co/google/embeddinggemma-2
- **[S17] SQLite vector upstream and original ANN limits:**
  https://github.com/asg017/sqlite-vec
  and https://github.com/asg017/sqlite-vec/issues/25
- **[S18] LadybugDB official sources:**
  https://github.com/LadybugDB/ladybug
  and https://docs.ladybugdb.com/
  and https://docs.ladybugdb.com/cypher/
  and https://docs.ladybugdb.com/concurrency/
  and https://pypi.org/project/ladybug/
- **[S19] Existing Hivemind-named shared-memory project:**
  https://github.com/raj45681/HiveMind
  (indexed description; implementation and maturity not audited)

## 27. Recommended first implementation milestone

Demonstrate two different worker agents and one optional Nexus sharing Hivemind:

1. Both obtain unique identities and start runs.
2. One edits a file through an instrumented tool.
3. The ledger and structured board show work/provenance within the documented intervals.
4. The other sends a standardized question and receives a durable reply.
5. Both publish/read a board thread.
6. EmbeddingGemma 2 finds the thread from a differently worded query.
7. An explicit, evidence-linked memory appears in a bounded context packet.
8. LadybugDB links agents, claims, tasks, files, artifacts, and evidence.
9. Nexus retrieves both workers by explicit IDs and by a semantic header query.
10. An exact duplicate header claim is denied deterministically without needing Nexus.
11. Nexus steers an opted-in worker to the footer; the adapter checkpoints and confirms it.
12. An old coordinator epoch cannot issue a new valid control.
13. A server restart preserves canonical records and reconciles derived projections.
14. An unauthorized third worker cannot retrieve project content or exercise controls.
15. Deleting board evidence removes vector/graph exposure and invalidates dependent context.
16. The React app opens to Main board and supports multiple agent tabs within the page.
17. Human `@` autocomplete routes to the selected agent while another agent can reply publicly.
18. The UI shows agent-to-agent recipients, permits repeated ordinary colors, and reserves
    active golden name/glow/crown styling for the current Nexus.

That milestone proves the central idea before investing in autonomous summarization,
complex federation, or a large framework ecosystem.
