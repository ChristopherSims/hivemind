# Milestone boundaries

The [autonomous integrations](autonomous-agents.md) are locally implemented and
offline contract-tested, not paid-live vendor validation. Vendor SDK/RPC strict OS
sandboxing and automatic identity/ACL attestation remain unsupported and fail closed.
Operator provisioning scripts exist but were not applied to the host. Message tasks
require an unassigned live run; unknown effects and post-reply crash settlement can
still require explicit operator reconciliation. Git bundles alone do not restore
canonical state or private effect/runtime journals.

Local [Git collaboration](git-collaboration.md) now uses the standard installed
Git CLI with protected `main`, exact source/base/candidate hashes, checks/reviews,
and fenced Nexus publication. Agent clones and receive hooks are not hostile
same-user OS isolation. Keep bare storage service-owned; authentication/HTTPS are
required before any future cross-machine Git transport is enabled. Git is not bundled.

Latest non-UI integration boundaries are documented in
[agent integration operations](agent-integrations.md). Vendor SDK/RPC wiring is
locally contract-tested, not paid-live validated. Windows child-process cleanup is
not a filesystem/network sandbox; required unavailable OS isolation fails closed.
Trusted in-process callbacks remain cooperative. Operator effect adjudication is
not proof of external success. Authorized extraction does not implicitly install,
download, or invoke a generative model. Authorization enhancements remain deferred.

Implemented as a local single-administrative-domain reference, not the full MVP release.

- **Authorization:** project-level participants plus agent-private memories/notifications,
  owner/agent identity binding, human observer separation, and project-wide coordinator grants.
  Fine-grained board/team ACLs, target-specific control grants, OIDC, credential rotation/revocation
  UI, enterprise policies, and remote TLS deployment remain future work. Freshness counters
  are project-wide, not private visibility-partition counters; stronger metadata isolation
  is also a release gate.
- **Storage:** canonical domain records, append-only ordinary events, revisions, command
  idempotency, index jobs, and canonical relationship facts. There is not yet a complete
  event-only shadow-reducer rebuild/conformance profile or broad crash-kill fault matrix.
  Narrow ingestion crash/rollback/replay harnesses have been validated.
- **Messages:** bounded at-least-once delivery, fenced receipts, exponential retry delays,
  dead-letter/manual recovery, structured history, and per-principal explicit unread markers.
  Delivery leases can be renewed with run/receipt fences. There is no exactly-once
  side-effect guarantee, jitter, push-based recipient
  execution, or unlimited inbox pagination. Counts/inboxes consider at most 1,000 recent
  messages. Conversations support full-history literal search and 100-item stable pages;
  dashboard rendering is bounded to 20 pages per source. See [delivery semantics](message-delivery.md)
  and [dashboard paging](dashboard-search.md).
- **Search:** real pinned EmbeddingGemma 2, tokenized 1,024/128 chunks, sqlite-vec scalar cosine
  distances over pre-authorized bounded candidates, FTS5, and reciprocal-rank fusion.
  At most 1,000 recent candidates per kind are considered. This is not ANN/HNSW.
  Index lag and unavailable model fallback are explicit. Large-dataset latency and
  relevance benchmarks have not been established.
- **Graph:** LadybugDB projection with current SQL endpoint/edge authorization, idempotent
  retries, bounded traversal, and canonical fallback when LadybugDB cannot open.
  The graph schema is generic Entity/Link rather than separate tables for every node kind.
- **Context:** explicit/semantic union/intersection, current state, claims, permitted verified
  memory, lexical/vector retrieval, and bounded graph expansion. Token estimates use a
  conservative UTF-8 byte upper bound in legacy `context:query`, whose budget excludes
  envelope metadata. `context:assemble` instead ranks full-history bounded canonical scans,
  uses a local tokenizer or labeled byte fallback, and budgets the complete canonical JSON
  envelope. A local tokenizer need not match the caller's LLM. Index waits return a bounded
  `index_not_ready` immediately rather than waiting for the requested timeout.
- **Memory:** immutable content versions, same-scope replacement proposals, human contradiction
  review/quarantine/resolution, UTC validity/expiry, pinned evidence, and a dashboard review
  workflow. Current authorization/source checks apply to history and cached replies.
  Review/history lists are bounded; selected-source deterministic extractive proposals
  and agent review outcome notifications are implemented. Optional injected-model jobs,
  advisory duplicate/contradiction suggestions, and configured repository-drift retrieval
  filtering are implemented, disabled by default. Human review reminders, authoritative
  semantic contradiction detection, retention erasure, and automatic fact invalidation
  remain future work.
  Verification is available to local human/observer roles, not agents.
  See [memory lifecycle](memory-lifecycle.md).
  See [agent automation scope](agent-memory-automation.md).
- **Control:** existing-owner-first exact/parent-child conflict denial, canonical claims,
  single Nexus lease with fencing, typed target capabilities, replacement reservations,
  pause/resume/cancel/assign/steer result validation, and cooperative file permits.
  There is no automatic safe-pause policy for already-running uninstrumented legacy work,
  nor semantic conflict auto-resolution. Unsupported tools fail closed.
- **Agent tasks:** opt-in injected execution loop, maintenance, dependency/capability
  scheduling, bounded retries, checkpoints and restart journal. Independent verification
  requires allowlisted trusted checks and a separate reviewer; completion/dependencies
  remain self-reported. Arbitrary runtime effects/application state are not transactionally
  preserved. Unknown effects block automatic restart. Runtime controls are cooperative.
  See [integrated agent capabilities](agent-capabilities.md).
- **Capture:** opt-in runtime hooks and SDK spool, not a continuous filesystem watcher,
  full tool proxy, or private-transcript collector. No hidden reasoning is captured.
  Hook daemons run explicitly. Explicit lifecycle reconciliation and bounded hash-only
  file drift scans are available. Confirmed termination needs authoritative observations
  and existing capture/operator grants; stale presence alone cannot end a run. Real vendor
  lifecycle adapters and forced process isolation remain unimplemented.
- **Browser:** same-origin HttpOnly session cookies, CSRF-protected writes, text-safe rich
  segments, snapshots plus SSE invalidations, deep links, keyboard mentions, tabs, role styles,
  and responsive layout. SSE reconnects deliberately request a canonical resnapshot,
  not retained notification replay. Shared-board unread accounting, optimistic posts, caret-position
  mention anchoring, and virtualized rendering remain future work. Timeline search and
  stable keyset pagination are implemented; large-history scan latency is not benchmarked.
- **Interoperability:** Python client/capture/controlled adapter, a TypeScript HTTP client,
  stdio MCP tools, Droid hook bridge, and Oh my Pi extension. A2A, Agent Client Protocol,
  OpenTelemetry export, vendor-hosted session control, and other memory product adapters
  have not been implemented.
- **Operations:** explicit canonical backups, index replay, source hiding and dependent memory
  invalidation. Automated retention compaction, dependency-wide deletion verification,
  encrypted spools, crypto-erasure, backup tombstone reconciliation, packaged wheels with
  embedded frontend assets, and multi-OS testing remain release gates.

Deleted resources stop appearing in authorized reads/search/context immediately. Background
jobs remove current vectors/graph exposure. SQLite free pages/WAL, old backups, adapter spools,
and old dependent memory versions may retain bytes until an appropriate retention/erasure
workflow runs. Do not claim complete physical erasure or restore stale backups into use.

The initial protocol proposal has not been ratified. Generated schemas describe implemented
command shapes; semantic checks, UTF-8 limits, authorization, and source validity are also
enforced server-side. Some command bodies use reduced milestone fields rather than every
optional field in the plan. Discovery advertises only available feature surfaces.
