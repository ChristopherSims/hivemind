# A2A and ACP protocol runtimes

`hivemind/protocol_runtimes.py` implements two explicitly enabled, real protocol client
adapters alongside the vendor runtimes. Both follow the trusted adapter contract
(`execute(task, context, checkpoint)` plus `inspect`/`pause`/`resume`/`cancel`/`close`)
and neither performs any remote or model work at construction time.

## Pinned protocol surfaces

### A2A — Agent2Agent Protocol v1.0.0, JSON-RPC 2.0 binding

Specification: https://a2a-protocol.org/v1.0.0/specification/ (normative data model in
`spec/a2a.proto`; sections 3, 4.1, 5.3, 9, and 14 were implemented against).

Supported subset — nothing beyond this is claimed:

- Discovery: `GET /.well-known/agent-card.json`; the card must declare **exactly the
  operator-approved endpoint** as a `supportedInterfaces` JSON-RPC entry (or, when no
  interfaces are declared, the card `url` must equal it) whose `protocolVersion` is
  exactly `1.0`. Empty interface lists, unrelated interfaces, version borrowing from
  other entries, and endpoint switching are all rejected.
- `SendMessage` with `returnImmediately: true`, text parts only
  (`{"role": "ROLE_USER", "parts": [{"text": ...}], "messageId": ...}`).
- Terminal/interrupted state observation via bounded `GetTask` polling, or via
  `SendStreamingMessage` (SSE `task`/`statusUpdate` frames) when the card advertises
  `capabilities.streaming`. A follow-up `GetTask` fetches the authoritative final task
  before settlement.
- `CancelTask`.
- Task states use the canonical `TASK_STATE_*` JSON enum values; every request carries
  the `A2A-Version: 1.0` header.

Explicitly not supported: push notifications, `ListTasks`, `SubscribeToTask`, extended
agent cards, gRPC/REST bindings, file/data parts in either direction, and follow-up
messages on interrupted tasks. `TASK_STATE_INPUT_REQUIRED` / `TASK_STATE_AUTH_REQUIRED`
fail closed: the remote task is cancelled and the outcome settles as cancelled; if
cancellation is not confirmed, an `UnknownOutcome` is raised. `ROLE_AGENT` direct
`Message` results are accepted; other roles are rejected.

### ACP — Agent Client Protocol v1 (`protocolVersion: 1`), stdio JSON-RPC 2.0

Specification: https://agentclientprotocol.com/protocol/v1/overview (initialization,
session setup, prompt turn, and cancellation sections).

Supported subset — nothing beyond this is claimed:

- `initialize` with `protocolVersion: 1` and **empty** `clientCapabilities`. The agent
  must select version 1 and must not demand `authMethods`; otherwise the connection is
  closed and execution fails closed.
- `session/new` with the workspace as `cwd` and no MCP servers. `session/load` is used
  only to re-attach to this runtime's own persisted session id and only when the agent
  advertises the `loadSession` capability.
- `session/prompt` with text content blocks only; `stopReason` handling:
  `end_turn` → completed, `cancelled` → cancelled, `max_tokens` / `max_turn_requests` /
  `refusal` → failed, anything else → malformed (fails closed).
- `session/cancel` notification as the cancel control.
- Agent-to-client traffic: `session/update` notifications are accepted for
  `agent_message_chunk` text only (bounded to 1 MiB per turn). `session/request_permission`
  always fails closed — a `reject_*` option is selected when offered, otherwise the
  request is answered `cancelled`. `fs/*`, `terminal/*`, elicitation, and any other
  unadvertised client method receive a JSON-RPC `-32601` refusal.

Explicitly not supported: `authenticate`/`logout`, `session/set_mode`, `session/delete`,
session list/fork, MCP server injection, and client-mediated terminal/filesystem
access. A trusted ACP executable can still use its own native filesystem or
process tools; permission refusal is not an OS sandbox or arbitrary-syscall
interceptor. Interrupted prompt turns are never auto-replayed.

## Operator configuration

Both runtimes are constructed from an options dict (wired by the parent integration):

Workers can select `--runtime a2a` or `--runtime acp` in place of
`--adapter module:factory`. `--enable` and the separate runtime opt-ins below
remain required. Reviewer integration configuration also accepts these adapter
names, but remote reviewers need the actual candidate evidence in their context,
not merely a local filesystem path.

```python
from hivemind.protocol_runtimes import a2a_runtime, acp_runtime

a2a = a2a_runtime({
    "workspace": "<absolute agent workspace>",
    "state_file": "<absolute PRIVATE state file, outside the workspace>",
    "config": "<absolute PRIVATE config JSON, outside the workspace>",
    "enabled": True,               # opt-in 1: adapter enabled
    "allow_model_calls": True,     # opt-in 2: model/remote calls permitted
    "allow_execution": True,       # opt-in 3: remote execution permitted
    "timeout_seconds": 300,        # 1..3600
    "result_contract": "message",  # or "git_review", or None
})

acp = acp_runtime({
    ...same keys...,
    "argv": ["C:/path/to/agent", "--acp"],   # trusted operator launch argv, literal strings
    "env": {"EXTRA": "value"},               # optional literal string overrides
    "env_passthrough": ["FOO"],              # optional names allowed through from os.environ
})
```

A2A private config JSON: `project_id`, `agent_id`, `agent_url` (the single approved
endpoint; HTTPS required except loopback), optional `token` (sent as a Bearer header and
never persisted or logged). ACP private config JSON: `project_id`, `agent_id` only; the
agent child process is launched with a sanitized minimal environment plus the explicit
operator overrides above.

## Guarantees and limits

- Transport boundary (A2A): a dedicated urllib opener with `ProxyHandler({})` (no
  environment/system proxy inheritance; an empty proxy map registers no handler, so no
  proxying exists at all) and a redirect handler that rejects every 3xx — credentials
  and request bodies never travel to a second hop. Endpoint URLs must be absolute HTTPS
  (plain HTTP only for loopback hosts) with a nonempty host and no userinfo, query, or
  fragment. The overall turn deadline bounds discovery, every RPC, and SSE waits — no
  per-request timeout can overshoot it. SSE parsing enforces a cumulative total wire
  byte cap (never reset per line), proper event framing with multi-line `data:` fields,
  comment-line skipping, and JSON-RPC id/error correlation on enveloped frames.
- Private state binds the exact project/agent/run/task/execution identities plus the
  remote task id (A2A) or session id (ACP), and persists `project_id`/`agent_id` so a
  config identity swap against the same state file fails closed. State files must live
  outside the workspace. Remote output is retained only as a SHA-256 hash in a quiescent
  checkpoint, matching the vendor runtime posture.
- Remote text never becomes local fact: the agent card's name is persisted only as a
  SHA-256 digest, and JSON-RPC error propagation carries the numeric code only — remote
  error text is never surfaced in exceptions or persisted.
- The vendor sandbox opt-ins `require_os_isolation` and `vendor_filesystem_sandbox` are
  validated as booleans and **refused** when true: these adapters perform no local tool
  execution and do not inherit vendor sandbox enforcement, so the opt-in fails closed
  before any state write, process launch, or network call. ACP launches a trusted
  native executable; refusal of client-mediated tools does not constrain that
  executable's own syscalls.
- A persisted `running`/`unknown` outcome blocks the next execution with
  `UnknownOutcome` until canonical reconciliation; a completed outcome for the same task
  position is never replayed. No pseudo task ids are synthesized.
- Structured result contracts (`message`, `git_review`) accept only exact validated
  envelopes via `runtime_results.structured_result`; a malformed final answer fails
  closed with `UnknownOutcome` instead of guessing the outcome.
- `pause` and `resume` are denied as unsupported controls in both protocols (neither
  defines them); they return `{"applied": False, "detail": {"state":
  "unsupported_control"}}` rather than pretending.
- Bounded everywhere: prompts <= 256 KiB, HTTP bodies and cumulative SSE wire <= 4 MiB,
  stdio frames <= 1 MiB, collected agent text <= 1 MiB per turn, A2A artifact text <=
  1 MiB cumulative across all artifacts, single active turn, deadline-bounded polling,
  and `ProcessGroup`-contained child cleanup for ACP.
- These adapters confer no remote tool scope and no cross-machine Git authority; the
  remote agent's own effects are the remote operator's responsibility.
- `protocol_runtime_descriptors()` returns readable purpose/input/output/authority/
  side-effects/limits/retry-safety descriptors per operation and control for tool and
  discovery surfaces.

## Tests

`tests/test_protocol_runtimes.py` runs fully offline: a loopback `ThreadingHTTPServer`
emits canonical A2A v1.0 shapes, and a controlled stdio fixture agent script (created by
the test file, driven by the venv Python) exercises ACP initialize/capability
negotiation, prompt turns, permission denial, unadvertised-method refusal, cancellation,
unsupported pause, restart blocking, and process cleanup. Adversarial coverage includes a
redirect catcher proving no credential or body reaches a second hop, userinfo/query/
fragment/empty-host URL rejection, cards with empty/wrong/unrelated interface
declarations, cumulative SSE wire caps across many frames, cumulative artifact text
caps, refused sandbox opt-ins with no resource setup, config identity swaps against
persisted state, remote error/card text staying out of state and exceptions, and overall
deadline bounding of a hanging `GetTask`. Run it with:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_protocol_runtimes.py -q
```
