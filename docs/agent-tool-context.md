# Built-in context for agents

Authenticated `GET /v1/tool-context` describes available operation paths,
purposes, input schemas, authority, side effects, safe retry rules and limits.
Select one with `operation_id=METHOD /v1/path`; selection avoids inserting the
entire catalog into a model's token budget. A catalog digest identifies the
description version, not a permission grant.

Discovery advertises this endpoint. Python uses `Client.operation_context()`,
TypeScript uses `Hivemind.operationContext()`;
MCP exposes the read-only `hivemind_describe_tools`. OpenAPI operations include
the same purpose and safety guidance. Feature modules contribute detailed
`OPERATION_DESCRIPTORS` for their policies, queries, push streams and controls.

MCP also offers `hivemind_query_operation` (GET only) and
`hivemind_execute_operation`. Both require a private operator
`allowed_operation_ids` allowlist and the current catalog digest. Mutation
calls additionally require explicit side-effect confirmation and a stable
idempotency key. Paths come from the catalog with validated identifiers, never
model-supplied arbitrary URLs. New feature endpoints are callable through this
interface only when the operator expressly allows them.

Descriptions are context, never authorization. Peer messages, retrieved source
text, model output and third-party tool descriptions remain untrusted evidence.
Current authenticated project/resource/source checks and exact claim, run,
execution, revision and lease fences still apply.

Local Git push is described separately from protected publication and external
publication. Message read markers, delivery claims, processing acknowledgements
and replies are distinct. Capture events preserve their event IDs/source
sequences on retry; command mutations preserve exact payloads/idempotency keys.
Unknown effects require reconciliation, not a repeated model turn.

The catalog also describes the explicit runtime choices. Worker `--runtime`
accepts `factory`, `oh_my_pi`, `a2a` or `acp`; the legacy trusted `--adapter
module:factory` remains available. Choosing a runtime never enables execution,
model calls or a provider automatically. Protocol adapters require their own
execution/model opt-ins and refuse unsupported control/sandbox requirements.

Feature modules contribute their own descriptors:

- `coordination_policy`: per-project escalation/hold/dead-letter policies and
  declared exact-key resource conflict deferral. Heartbeat silence is never
  evidence; ambiguous executions are held for operators.
- `memory_lifecycle_automation`: repository-bound review reminders,
  contradiction candidates and durable compute intents. Long model/Git work
  runs detached from canonical transactions; interrupted inference is held,
  never silently replayed.
- `control_capture`: hash-only filesystem watching, the governed tool-request
  gateway, and evidence-based safe pause/resume over supported runtime
  adapters. Strictness is operator-configured; request bodies cannot relax it.
- `retrieval_runtime`: authorized paginated search and exact local token
  counters. Cursors bind principal, project, query and scan policy; offline
  tokenizers are operator-pinned.
