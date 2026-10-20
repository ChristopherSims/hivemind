# Capability-based workload routing

`hivemind.capability_routing` answers one question deterministically: which
currently eligible (agent, run) pair could take this queued task? It never
starts work, never impersonates an agent, and never treats a query or a
recorded approval as dispatch authority. Workers still claim tasks through the
canonical `POST /v1/projects/{project_id}/tasks:dispatch` path (or the parent's
exact-pair `Scheduling.dispatch_exact`), which re-checks claims, revisions, and
liveness at claim time.

## Policy

All entry points are default-off and bounded by a trusted operator
`RoutingPolicy` (passed to `install(..., policy=...)`, e.g. via
`capabilities.install` options under `"capability_routing"`):

- `enabled` (default `False`): consistently gates the advisory query, the
  fenced approval, declarations, and every gate hook (`feature_disabled`).
- `max_candidates` (default 32, max 64): deterministic candidate bound;
  truncation is explicit (`candidates_truncated`).
- `max_heartbeat_gap_seconds` (default 30, 1–300): a run whose heartbeat is
  older, or missing, is not availability. Heartbeat silence is never treated
  as proof of safe ownership in either direction.
- `allow_busy_agents` (default `False`): a live task-bearing run disqualifies
  the agent. Independent auxiliary/observer runs neither mask a busy agent nor
  make an idle one look busy.
- `require_declaration` (default `True`): agents need a typed declaration or
  an approved runtime in their canonical agent metadata.
- `allowed_runtimes` (default empty = all approved): names must come from the
  approved `runtime_registry` — never arbitrary imports or URLs.
- `require_worker_run` (default `True`) and `worker_run_ids`: only runs bound
  to the task-worker purpose (canonical run metadata `purpose ==
  "task_worker"`) or named in this explicit allowlist (max 1024) are routable
  work capacity. A newer observer/auxiliary run is never chosen.
- `sandbox_verifier`: optional trusted callable
  `(principal, task, run, declaration) -> bool` enforcing launch-attestation
  checks of the platform and the process/storage boundary for the exact run.
  It must be a pure, side-effect-free local check: gates call it inside the
  caller's canonical transaction and never create records or perform external
  compute. **Absent a verifier, no strict-sandbox requirement is satisfiable.**
  A crashing verifier is a negative answer, never proof.

## Runtime declarations (advisory intent, CAS-frozen in flight)

```http
POST /v1/agents/{agent_id}/routing-declaration
GET  /v1/agents/{agent_id}/routing-declaration
```

The agent itself, or an administrator, declares the runtime name, canonical
tool constraints (names from the governed tool map only), and advisory
sandbox/network intent. Unknown runtime names and unknown tools are rejected;
`strict_sandbox: true` is rejected for runtimes whose registry limits declare
sandbox controls unsupported (`a2a`, `acp`). The flag is **intent only**: it
never authorizes a strict-sandbox task — even on a capable runtime family
(factory/oh_my_pi), eligibility requires the operator's run-bound verifier at
route time, because e.g. Windows has no vendor sandbox at all.

Updates accept an optional `expected_revision` CAS fence (412 on mismatch) and
are **frozen while the agent has in-flight bound work** (409
`declaration_in_flight`; unverifiable bindings freeze rather than guess), so a
currently bound runtime cannot be swapped out from under approved eligibility.

Tasks may carry an optional typed `routing` block (`required_runtime`,
`required_tools`, `strict_sandbox`). Unknown keys, unknown registry/tool names,
or malformed values fail closed: the task is `invalid_routing_requirements`.

## Advisory query

```http
POST /v1/projects/{project_id}/routing:query
{"task_id": "task_...", "candidate_agent_ids": []}
```

The task is always loaded canonically — caller-supplied bodies are never
trusted as tasks (trusted in-process callers may pass canonical task dicts to
the hooks directly). Returns ranked `eligible` entries plus explicit
privacy-safe `rejected` reasons (`capability_mismatch`, `runtime_mismatch`,
`tool_constraint_mismatch`, `strict_sandbox_unsupported`,
`runtime_not_allowed`, `declaration_missing`, `assignment_mismatch`,
`handoff_target_mismatch`, `agent_busy`, `control_hold`, `no_live_run`,
`worker_run_required`, `agent_not_visible`, `agent_not_available`) or a
task-level `blocked` reason. Ranking is deterministic: roster workload, then
active claims, then canonical agent id. Advisory workload counts come **only**
from the privacy-filtered roster's `current_task`; rejections never contain
other-project identities or private task counts.

## Authoritative gates (pure reads; no records, no external compute)

- `choose(principal, task, candidates)` — advisory ranking (above).
- `dispatch_gate(principal, task, run)` — authoritative pre-dispatch check for
  one exact pair. Re-reads bounded raw canonical rows for the chosen run **and
  every peer worker run** of the agent: run bindings, pending unexpired
  controls, and task executions. Any binding that cannot be verified (private,
  missing, truncated) defers with the generic `agent_not_available` — never
  the private identity, never a count.
- `start_gate(principal, task, run)` — the parent's mid-`task_start` recheck.
  `scheduling._start` has already incremented the attempt (task still
  `queued`, `scheduling.state == "dispatched"`), so instead of the generic
  queued-task gates this validates the exact dispatch record (assignment,
  dispatch run id, attempt bounds `attempts <= max_attempts` — the final
  allowed attempt is never denied) and accepts a run already bound to this
  exact task. No attempt is consumed; no queued bypass is introduced.
- `gate_callback(principal, task, run)` — adapter for the parent's gate
  registry contract: returns `{"action": "allow" | "defer", "reason",
  "retry_after"}` from `dispatch_gate`, with a deterministic bounded defer
  hint.

## Fenced durable approval

```http
POST /v1/projects/{project_id}/routing:dispatch
{"task_id": "...", "agent_id": "...", "run_id": "...",
 "coordinator_epoch": 7, "reason": "Operator approved"}
```

Despite the path name, this records a **durable approval only**: a canonical
`routing_decision` resource containing IDs, fence, and reason — no task
content, so a later privacy change cannot leak history through it. Callers
must be admin/human, or a coordinator principal fenced by the current Nexus
lease epoch (`stale_coordinator_epoch` otherwise). The authoritative gate
re-verifies all evidence at approval time (`routing_rejected` on failure). The
approval is idempotent by Idempotency-Key and starts nothing: the worker still
dispatches itself canonically, or a parent hook consumes the pending approval
via `approved_decision(principal, project_id, task_id, agent_id, run_id)` and
drives `Scheduling.dispatch_exact` under the same canonical gates.

## Limits

No generative or semantic inference; required capabilities match as exact
typed sets (max 32 per task). Candidate sets, declared tools (32), required
tools (32), run evidence (32 per agent), decision lookups (200), and runtime
allowlists are strictly typed and bounded. Descriptions in
`OPERATION_DESCRIPTORS` are context, never a permission grant.
