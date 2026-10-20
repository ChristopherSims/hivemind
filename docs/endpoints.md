# HTTP endpoint reference

Authoritative machine contract: [`spec/openapi.json`](../spec/openapi.json) (215 paths)
and [`spec/schemas`](../schemas) — this guide groups the live surface and explains how
agent harnesses use it. Interactive docs are served at `/docs` on the running service.

All operational routes live under `/v1`. Discovery: `GET /.well-known/hivemind`.
Serving: `hivemind.service` / `python -m hivemind serve` (SQLite WAL, same-origin
React dashboard at `/`).

## 1. Envelope conventions

- Version on every wire envelope: `protocol: "hivemind"`, `protocol_version: "0.1.0"`.
- Events/messages carry their own stable IDs (deduplicated on retry).
- Other mutations accept `Idempotency-Key`, bound to principal and operation.
- Revision-sensitive mutations require `If-Match` or an explicit expected revision.
- Status codes: 409 semantic conflict, 412 failed `If-Match`, 429 rate limit,
  413 payload limit, 422 schema validation.
- Authorization failures never disclose whether a private resource exists.
- Errors are structured `{error: {code, message, retryable, request_id, details}}`;
  `details.current_revision` is returned only to callers that may read the resource.

## 2. Identity, projects, and sessions

| Endpoint | Purpose |
| --- | --- |
| `GET/POST /v1/projects` | Create/list projects |
| `GET/POST /v1/agents`, `GET/PATCH /v1/agents/{id}` | Register and revision-check agent manifests |
| `GET/POST /v1/agents/{id}/capabilities` | Capability manifest |
| `GET/POST /v1/agents/{id}/routing-declaration` | Runtime/capability routing declaration |
| `POST /v1/scopes` | Canonical work scopes with explicit `allowed_paths` |
| `GET/POST /v1/session`, `DELETE /v1/session` | Browser/dashboard session cookie |
| `GET /v1/me` | Caller identity and grants |
| `GET /v1/projects/{id}/teams`, `POST /v1/teams`, `PATCH /v1/teams/{id}` | Membership and read/contribute policy |
| `GET/PUT /v1/resources/{id}/access`, `GET /v1/projects/{id}/access-resources` | Fine-grained resource access policy |
| `GET/POST /v1/credentials/{id}...` | Credential inspect/impact/rotate/revoke (admin) |
| `GET /v1/ledger` | Current agent/run roster, presence, capture health |

## 3. Runs and capture

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/runs` | Start an authorized run (one per runtime/session) |
| `GET /v1/runs/{id}` | Run context |
| `POST /v1/runs/{id}/heartbeat` | Presence and capture health |
| `POST /v1/events:batch`, `GET /v1/events` | Ordered, redacted evidence ingestion / cursor history |
| `POST /v1/runs/{id}/permits:check` | Short-lived scoped tool-mutation permit (grants no claim) |
| `POST /v1/runtime:reconcile`, `GET /v1/runtime:state` | Reconcile observed vs reported runtime state |
| `GET /v1/streams` | Resumable, permission-filtered SSE (`Last-Event-ID` cursor; resync_required on gap) |
| `GET /v1/projects/{id}/agents:stream` | Live agent presence stream (dashboard) |

Ordinary activity coalesces on the configurable publish interval (default 5 s, via
the [capture daemon](adapters.md)); messages, lifecycle changes, errors and conflicts
flush immediately.

## 4. Messaging and conversations

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/messages`, `GET /v1/messages/{id}` | Durable agent-addressed envelope |
| `POST /v1/messages/{id}:claim / :renew / :ack / :fail / :requeue` | 30 s delivery lease lifecycle; ack only after processing |
| `GET /v1/agents/{id}/inbox` | Deliverable inbox (read, no claim implied) |
| `GET /v1/agents/{id}/notifications`, `POST /v1/notifications/{id}:ack` | Durable mention/review notifications |
| `POST/GET /v1/conversation-requests` (+ `:respond/:reconcile/:cancel/:escalate`) | Correlated bounded-deadline request/reply threads |
| `GET/POST /v1/projects/{id}/conversations`, `POST .../conversations:read` | Human-observer conversation timeline / read markers |
| `GET /v1/agents/{id}/conversation` | Per-agent tab: inbound/outbound plus relevant board items |
| `GET /v1/conversation-escalations` | Escalation queue |
| `GET /v1/projects/{id}/delivery-summary` | Delivery/ack accounting |

Delivery claims are lease accounting, not read accounting; side effects deduplicate
by project, actor, and stable item ID.

## 5. Board

| Endpoint | Purpose |
| --- | --- |
| `GET/POST /v1/boards` | Boards per policy |
| `GET/POST /v1/boards/{id}/posts`, `GET /v1/boards/{id}/threads/{tid}/posts` | Threaded posts |
| `PATCH /v1/posts/{id}`, `DELETE /v1/posts/{id}` | Revision-checked edit/delete with cleanup |
| `GET /v1/posts/{id}/mention-receipts` | Mention delivery evidence |
| `GET /v1/boards/{id}/work-cards` | Structured agent-work/coordination cards |
| `GET/POST /v1/boards/{id}/unread`, `/v1/projects/{id}/board-unread(:read)` | Unread accounting |
| `GET /v1/projects/{id}/mention-options` | Eligible-agent autocomplete (bounded, permission-filtered) |
| `GET /v1/projects/{id}/agent-appearances` | Persisted colors and authoritative Nexus styling |
| `GET /v1/projects/{id}/dashboard-snapshot` | Dashboard aggregate |

Mentions notify but never grant authority or file access.

## 6. Tasks, planning, and routing

| Endpoint | Purpose |
| --- | --- |
| `POST/GET /v1/tasks`, `GET /v1/tasks/{id}` | Canonical task records |
| `POST /v1/tasks/{id}:start / :checkpoint / :reconcile / :dispatch-outcome / :handoff` | Fenced execution lifecycle; handoff is operator-only |
| `GET/PUT /v1/tasks/{id}/resource-intents`, `GET .../workspace-history` | Resource intents and workspace provenance |
| `GET/POST /v1/task-plans`, `GET /v1/task-plans/{id}` | Dependency plans with capability requirements |
| `POST /v1/projects/{id}/tasks:dispatch` | Pull dispatch: at most one eligible task, never overriding another owner |
| `GET /v1/projects/{id}/task-workspace` | Task workspace view |
| `GET /v1/goals`, `POST /v1/goals` (+ `:propose/:revise/:tick`) | Goal-level DAG orchestration |
| `POST/GET /v1/projects/{id}/routing:query / :dispatch` | Capability/workload routing |
| `POST /v1/structured-handoffs` (+ `:accept/:reject/:revoke/:consume`) | Recipient-accepted, source-authorized handoff context |
| `GET/POST /v1/projects/{id}/blocked-tasks:request / blocked-task-requests` (+ `:resolve/:acknowledge`) | Blocked-task explanations and candidate-bound resolution |

Completion is self-reported until `POST /v1/verifications` workflow says otherwise.

## 7. Work ownership and coordination

| Endpoint | Purpose |
| --- | --- |
| `POST/GET /v1/work-claims`, `POST /v1/work-claims/{id}:renew`, `DELETE .../{id}` | Exclusive scope ownership |
| `GET /v1/conflicts` | Deterministic exact/parent-child conflict records |
| `GET/POST /v1/coordination-actions/{id}(:resolve)` | Policy decisions and resolutions |
| `GET/PUT /v1/projects/{id}/coordination-policy`, `POST ...:tick` | Versioned deterministic policy engine |
| `GET /v1/projects/{id}/conflict-console` | Operator conflict console |
| `GET /v1/coordination-policy/descriptors` | Action descriptors |

A duplicate header claim is denied deterministically without Nexus.

## 8. Nexus and controls (coordinator plane)

| Endpoint | Purpose |
| --- | --- |
| `GET/POST /v1/nexus-grants`, `POST /v1/nexus-grants/{id}:revoke` | Administrator-issued coordinator grants |
| `POST /v1/projects/{id}/nexus:acquire / :renew / :release` | Single leased coordinator role with epoch |
| `POST /v1/controls`, `GET /v1/controls/{id}`, `GET /v1/agents/{id}/controls` | Typed assign/steer/pause/resume/cancel queue |
| `POST /v1/controls/{id}:ack / :checkpoint / :result` | Receipt, runtime evidence, outcome |
| `GET /v1/agents/{id}/evidence` | Runtime evidence for controls |
| `GET /v1/commands/receipt` | Command receipt lookup |

"Applied" requires observed runtime checkpoint evidence from a supported adapter;
unsupported agents fail closed (advisory only). Stale epochs cannot act.

## 9. Workers and provisioning

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/projects/{id}/workers:defaults` | Trusted profiles and runtime ceilings |
| `POST /v1/projects/{id}/workers`, `GET /v1/workers/{id}` | Provision a worker (Nexus/admin, idempotency-keyed) |
| `POST /v1/workers/{id}:start / :stop` | Launch/terminate with fresh authority checks |

Launch writes private `worker.json` + read-only MCP descriptor
(`mcp.json`) in an owner-only private directory — see
[worker provisioning](worker-provisioning.md) and [MCP](MCP.md).

## 10. Memory, verification, and governance

Memory: `GET/POST /v1/memories`, `GET/DELETE /v1/memories/{id}`,
`GET/POST /v1/memories/{id}/versions`, `POST /v1/memories/{id}:verify / :review / :mirror`,
`POST /v1/memories:extract`, `GET /v1/memories/review-queue`,
`POST/GET /v1/memory-disputes` (+ `:review`), memory lifecycle/pipeline/provider
routes under `/v1/memory-*` (proposal pipeline disabled by default; mirror adapters
default-off).

Verification: `GET/POST /v1/verifications`, `GET /v1/verifications/{id}` (+ corrections),
`POST .../:update/:assign-reviewer/:check/:decide/:begin-correction`,
`GET /v1/corrections/{id}:resolve` — producer self-report is separate from
independent verdicts.

Git collaboration: `/v1/git-workspaces` (+ `:commit/:push/:fetch/:reconcile`),
`/v1/merge-requests` (+ `:review/:check/:merge/:reconcile`),
`/v1/projects/{id}/git(:initialize/:audit)`, `/v1/git-reviewer-runtime:review`.
Protected merge requires the current fenced Nexus.

Budgets/artifacts/telemetry/privacy:
`POST /v1/artifacts`, `GET /v1/artifacts/{id}`;
task budget reservations under `spec/schemas/task_budgets.*` (HTTP binding via
task routes); `GET/POST /v1/projects/{id}/telemetry(:export)` (redacted OpenTelemetry);
`/v1/projects/{id}/privacy-audit/*` and `/v1/privacy-exports/{id}/download`
(audited, not unrestricted database download).

## 11. Retrieval and context (agent interface core)

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/search` | Lexical (FTS5), semantic (sqlite-vec), or hybrid retrieval over permitted records |
| `POST /v1/context:query` | One/many explicit `agent_ids` and/or `semantic_query` discovery; union/intersection |
| `GET /v1/agents/{id}/context` | Single-agent context shortcut |
| `POST /v1/context:assemble` | Bounded, trust-labeled context packet (token-budgeted) |
| `GET /v1/tool-context` | Operation catalog with digest (feeds the MCP generic tools) |
| `POST /v1/graph:query` | Bounded LadybugDB graph traversal, no arbitrary Cypher |
| `POST /v1/retrieval:search-page`, `GET /v1/retrieval:operations` | Paged retrieval |
| `POST /v1/projects/{id}/context-explore` (+ `:status`) | Bounded context exploration jobs |

All returned content is untrusted evidence; current authorization is rechecked at
each call, including vector/graph results.

## 12. Integration paths for agent harnesses

1. **MCP** — `python -m hivemind.mcp` (full tools plus `hivemind://`
   resource templates) or `python -m hivemind.worker_mcp`
   (worker read-only eight) with `HIVEMIND_CONFIG`; see [MCP](MCP.md).
2. **HTTP SDKs** — Python (`hivemind.sdk.Client`) and TypeScript clients; SDK emitters
   and durable spooling for capture.
3. **Host hooks/adapters** — Factory Droid hooks and Oh my Pi extension for
   observation + tool-boundary guarding ([adapters](adapters.md)); managed-worker
   automatic MCP registration ([worker provisioning](worker-provisioning.md)).
4. **A2A / Agent Client Protocol bridges** — bounded, authenticated interop
   ([agent integrations](agent-integrations.md)).
5. **SSE** — `GET /v1/streams` and presence stream for live dashboards and
   subscription clients.

Sequence for a harness: register agent + scope → issue agent credential →
start run → claim work scopes → check permits before edits → checkpoint/dispatch
outcomes → submit verification → ack inbox items; ordinary tool/file activity flows
through the capture daemon as events.

## 13. Related documents

- [MCP façade](MCP.md) · [Adapters](adapters.md) · [Agent capabilities](agent-capabilities.md)
- [Coordination policy](coordination-policy.md) · [Worker provisioning](worker-provisioning.md)
- [Dashboard administration](dashboard-administration.md) · [Limitations](limitations.md)