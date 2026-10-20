# MCP façade

Hivemind exposes a stdio Model Context Protocol server so any MCP-capable agent
harness (Factory Droid, Oh my Pi, editors, or a generic MCP client) can share the
same governed memory and coordination plane without direct database access.

- Main surface: `python -m hivemind.mcp` (`hivemind/mcp.py`).
- Worker surface: `python -m hivemind.worker_mcp` (`hivemind/worker_mcp.py`),
  a read-only subset for provisioned workers.
- Both are façades over the authenticated HTTP API through `hivemind.sdk.Client`.
  MCP tool descriptions grant **no authority**; server-side policy, revisions,
  epochs, claims, and credentials remain the only enforcement point.
- The main server also exposes read-only MCP **resource templates**
  (`hivemind://…`, section 3); each read executes the same permission-filtered
  GET as the HTTP API.

## 1. Configuration

The server reads one private JSON credential file, referenced by
`HIVEMIND_CONFIG` (never commit it; never pass tokens on tool calls):

```json
{
  "base_url": "http://127.0.0.1:8765",
  "token": "<agent-bound credential from: hivemind issue-token --role agent ...>",
  "project_id": "<project ID>",
  "agent_id": "<agent ID>"
}
```

Optional fields: `allowed_operation_ids` (operator allowlist for the generic
operation tools), `state_dir` (audit receipts), `audit_context_queries`
(opt-in context-query receipts with hashes, never contents).

Every credential is bound to one project/agent pair. Issue a separate credential
per runtime; never use the owner credential. See [adapters](adapters.md) for
identity/scope registration and token issuance.

## 2. Main tool catalog

The main stdio server currently exposes **47 tools** (verified via `server.list_tools()`).

| Tool | Surface | Purpose / backing endpoint |
| --- | --- | --- |
| `hivemind_describe_tools` | read | Operation catalog and retry rules — `GET /v1/tool-context` |
| `hivemind_query_operation` | read | Allowlisted GET operation, digest-fenced |
| `hivemind_execute_operation` | write | Allowlisted mutation with `confirm_side_effects` and `idempotency_key` |
| `hivemind_get_ledger` | read | `GET /v1/ledger` — roster, presence, capture health |
| `hivemind_register_run` | write | `POST /v1/runs` |
| `hivemind_query_context` | read | `POST /v1/context:query` — explicit IDs and/or semantic selector |
| `hivemind_assemble_context` | read | `POST /v1/context:assemble` — bounded, untrusted evidence packet |
| `hivemind_send_message` | write | `POST /v1/messages` |
| `hivemind_get_inbox` | read | `GET /v1/agents/{agent_id}/inbox` (no claim implied) |
| `hivemind_claim_message` / `hivemind_ack_message` / `hivemind_fail_message` | write | `POST /v1/messages/{id}:claim/:ack/:fail` — 30 s delivery lease |
| `hivemind_post_board` | write | `POST /v1/boards/{board_id}/posts` — mentions notify, never authorize |
| `hivemind_search` | read | `POST /v1/search` — lexical/semantic/hybrid |
| `hivemind_remember` / `hivemind_extract_memories` | write | `POST /v1/memories`, `POST /v1/memories:extract` — proposals, never verification |
| `hivemind_propose_memory_version` / `hivemind_report_memory_dispute` / `hivemind_memory_versions` | write/read | Governed memory revisions and disputes |
| `hivemind_get_task` / `hivemind_start_task` / `hivemind_checkpoint_task` | read/write | Task lifecycle with execution fences |
| `hivemind_get_notifications` / `hivemind_ack_notification` | read/write | Durable mention notifications |
| `hivemind_claim_work` / `hivemind_get_conflicts` / `hivemind_check_permit` | write/read | `work-claims`, `conflicts`, `runs/{id}/permits:check` |
| `hivemind_create_task_plan` / `hivemind_dispatch_task` | write | Dependency plans and pull dispatch |
| `hivemind_git_workspace` / `hivemind_git_workspace_action` | write | Scoped local Git branch work |
| `hivemind_submit_merge_request` / `hivemind_merge_request_action` | write | Exact-candidate review; merge needs fenced Nexus |
| `hivemind_handoff_task` | write | Operator-only task transfer |
| `hivemind_review_automation_tick` / `hivemind_begin_correction` / `hivemind_submit_verification` / `hivemind_review_verification` | write | Independent verification workflow |
| `hivemind_conversation_request` / `hivemind_conversation_action` | write | Correlated request/reply/escalation |
| `hivemind_memory_pipeline` | write | Opt-in proposal pipeline (disabled by default) |
| `hivemind_reconcile_runtime` | write | `POST /v1/runtime:reconcile` |
| `hivemind_query_graph` | read | `POST /v1/graph:query` — bounded traversal, no arbitrary Cypher |
| `hivemind_worker_defaults` / `hivemind_create_worker` | read/destructive | Nexus-managed worker provisioning |
| `nexus_acquire_role` / `nexus_control_agent` | write | Coordinator lease and typed epoch-checked controls |

Generic operation tools are digest-fenced: read the current
`hivemind_describe_tools` entry, then supply its `expected_context_digest`;
mutations additionally require `confirm_side_effects: true` and a stable
`idempotency_key` (1–128 chars), bound to the principal and operation.

`hivemind_query_context` mirrors the HTTP `agent_selector.agent_ids` /
`agent_selector.semantic_query` fields exactly. With `audit_context_queries`
enabled, each call appends a receipt (hashes and resource IDs/revisions, never
packet contents) to `<state_dir-repo>/context-receipts.jsonl`, `0600` on POSIX.

## 3. Resource templates

The main stdio server exposes seven read-only resource templates under the
`hivemind://` scheme. Every read performs the same authenticated,
permission-filtered GET as the corresponding HTTP endpoint, so deleted, revoked,
private, or foreign content fails with the API's not-found semantics — a
resource URI is a convenience alias, never an authority grant and never a
cached or fabricated view:

| URI template | Backing GET |
| --- | --- |
| `hivemind://ledger/{project_id}` | `/v1/ledger` — runs with presence/capture health |
| `hivemind://boards/{board_id}/posts` | `/v1/boards/{id}/posts` — latest 100 authorized posts |
| `hivemind://boards/{board_id}/threads/{thread_id}/posts` | `/v1/boards/{id}/threads/{tid}/posts` |
| `hivemind://memories/{project_id}` | `/v1/memories?project_id=…` |
| `hivemind://memories/{project_id}/{memory_id}` | `/v1/memories/{id}` — includes effective status |
| `hivemind://tasks/{project_id}` | `/v1/tasks?project_id=…` |
| `hivemind://tasks/{project_id}/{task_id}` | `/v1/tasks/{id}` |

Content is canonical JSON text. Verified behavior (real loopback service):
the owner principal reads its run/board/task/memory records; another agent's
credential receives not-found for a `private: true` memory owned by a different
agent, and a credential bound to a different project receives not-found for a
foreign `hivemind://ledger` — see `tests/test_mcp_resources.py`. The worker
read-only surface (`worker_mcp.py`) does **not** register resources; its exact
eight-tool catalog contract is unchanged.

## 4. Worker read-only surface

`hivemind/worker_mcp.py` exposes exactly eight read-only tools to managed
workers; everything else fails at the API for that credential anyway:

```text
hivemind_describe_tools   hivemind_get_ledger      hivemind_query_context
hivemind_get_inbox        hivemind_search          hivemind_get_task
hivemind_get_notifications  hivemind_get_conflicts
```

Launch: `{"mcpServers": {"hivemind": {"command": "<python>",
"args": ["-m", "hivemind.worker_mcp"], "env": {"HIVEMIND_CONFIG": "<private worker.json>"}}}}`.
The provisioning service writes this descriptor into the worker's
owner-only (0600/0700) private directory as `mcp.json` — see
[worker provisioning](worker-provisioning.md). No global or repository vendor
settings are modified.

## 5. Harness integration

### Automatic registration (managed workers)

With `auto_mcp_registration: true` (trusted-profile default; operators may set
`false`), registration occurs when a managed vendor session opens for its
assigned task — not at worker-process launch:

- **Factory Droid**: the SDK receives a session-local `StdioMcpServerConfig`
  (`python -m hivemind.worker_mcp`). Before each prompt the runtime requires
  the sole connected `hivemind` server, its exact eight-tool enabled catalog,
  unambiguous vendor tool IDs (`mcp__hivemind__<name>` /
  `mcp___hivemind___<name>`), and a verified `restrict_tools` set of native
  scoped tools plus those MCP IDs. Missing or drifted evidence holds the turn;
  repository/parent `.factory/mcp.json` launchers are rejected, never merged.
- **Oh my Pi (OMP)**: a fixed Bun SDK host starts sessions with
  `enableMCP: false`, `restrictToolNames: true`, ambient discovery disabled and
  its MCP manager connected only to Hivemind. The eight tools are explicitly
  allowed SDK custom tools plus the ordinary capture extension. A private
  nonce-bound startup receipt (`verify_omp`) is required before a prompt. The
  receipt proves initial registration, not ongoing liveness. Requires Bun and
  the installed OMP 18.8.9 source SDK; changed contracts fail closed.

### Manual registration (any MCP client)

Point the client at the worker or main server command above with
`HIVEMIND_CONFIG`. For Factory integration harnesses that opt into explicit
tool allowlists, configure `allowed_mcp_tools` with exact local Hivemind tool
IDs discovered from the Factory inventory — an administrator-controlled
allowlist, never a wildcard. See [adapters](adapters.md) and
[factory integration](factory-integration.md).

### Capture versus tools

MCP tools are an action surface; they do not by themselves observe other host
tools. Ordinary activity capture needs the host hooks/capture daemon described
in [adapters](adapters.md). Messages, board posts, claims, and controls go
through direct MCP/API commands, not the observation buffer.

## 6. Safety rules encoded in the façade

- Descriptions and peer/model output are context, never authority.
- Uncertain handling is never acknowledged; delivery claims are not read
  accounting; a response is not a delivery acknowledgement.
- Verification is separate from self-reported results; memory proposals never
  verify facts; reviewer decisions require operator-allowlisted checks.
- Control acknowledgements are receipts; `applied` requires observed runtime
  checkpoint evidence from a supported adapter, otherwise fail closed.
- Expired Nexus epochs and stale digests are rejected before any effect.