# Governed blocked-task resolution

`hivemind/blocked_tasks.py` answers one question — *why can't this task start right
now?* — and lets agents file a durable, structured request for one specific corrective
action through a governed workflow. It never fixes anything by itself.

- **Explanations are read-only canonical snapshots**, fenced to the exact
  (principal, project, task, run/agent) tuple in the request path. The same registries
  that gate dispatch (`task_dependency_gates`, `task_dispatch_gates`), the scheduling
  eligibility rules, the budget ledger snapshot (pure SQL aggregates only — no vendor
  budget helpers, counters, or heavy I/O), an independent bounded dependency-cycle DFS,
  and **raw bounded canonical claim/control rows** for availability are *called*, never
  mutated: no attempt is consumed, no claim is created, no retry hint is persisted, no
  model or native compute runs. Gate callbacks that raise or return malformed verdicts
  fail closed to a generic hold — never a 500, never permission.
- **Readiness is honest.** `ready` is true ONLY for a queued task with zero current
  blockers. Running, paused, and terminal states are reported as explicit `readiness`
  states (`already_running` / `blocked` / `terminal`), never as ready. A current
  execution's run id is returned only when that run is currently visible to the caller;
  a private or missing prior execution redacts the id and holds the task with a generic
  blocker — unknown executions are never ready.
- **Provenance is explicit.** Every blocker is labeled `canonical_snapshot` (direct
  store reads), `gate_snapshot` (a registered parent gate verdict), or `source_hook`
  (an operator-injected budget/recovery report, guarded and validated down to a generic
  reason class). The module never imports worker, budget, or recovery implementations.
- **Privacy is fail-closed.** Unknown, private, or unverifiable blockers collapse to
  generic codes (`blocker_unavailable`, `scope_conflict_unavailable`,
  `control_state_unavailable`). Raw availability checks disclose the *existence* of a
  hidden conflicting claim or control generically — never the private holder identity,
  never an explicit scope-holder id, never cross-scope counts. Private dependency
  identities and hidden contract text/ids are never disclosed.
- **Requests are durable receipts, not effects.** The allowlisted actions
  (`artifact_needed`, `budget_review`, `dependency_review`, `permission_review`,
  `recovery_review`, `operator_attention`) route work to the responsible human/operator
  surfaces. An agent may *request*; it can never raise a budget, self-verify an
  artifact, release unknown effects, change a permission, or reassign a claim through
  this surface. There are no shell/code/prompt payloads; unknown actions and unknown
  fields are rejected by the strict request models.
- **Triage is explicit.** Acknowledge/resolve/decline require an operator
  (admin/human) or the responsible actor (the canonical task owner); the requester may
  only cancel. Acknowledgment is **not** resolution. `resolved` requires a fresh
  canonical readiness proof bound to the request's exact task — queued with zero
  current blockers, or the canonical terminal `completed` state. No arbitrary evidence
  override, acknowledgment, or operator note is ever accepted as proof. `declined` and
  `request_canceled` are distinct, explicitly-not-fixed outcomes.
- **No lost updates.** Acknowledge/close read the request row once, authorize fresh
  inside the command transaction, and publish with CAS on that captured revision: a
  concurrent triage update conflicts (412) instead of being silently overwritten.
- **Every read is re-authorized and re-redacted**, including idempotent mutation
  replays: the task, the request resource itself, the requester principal, and every
  basis reference are re-checked against current state. Unavailable basis ids and
  source-derived notes are withdrawn from all responses.

## Install

The capability is registered in the root `FEATURE_MODULES` list and installed by the
shared application wiring **default-off**: without explicit configuration it installs
with `explain_enabled=False` and publishes no routes (the module handle and the
`resolution_request` resource projection are still installed so generic command
receipts stay privacy-safe). Explicit opt-in goes through the integration
configuration file (`model_wiring`), which accepts strict JSON booleans only —
anything else (truthy strings/ints, unknown keys) is rejected:

```json
{
  "blocked_tasks": {"explain_enabled": true, "requests_enabled": true}
}
```

`requests_enabled` requires `explain_enabled`. A direct install for tests or custom
embedding uses the conventional capability signature:

```python
from hivemind import blocked_tasks
blocked_tasks.install(app, service, principal, command, envelope,
                      policy=blocked_tasks.BlockedTasksPolicy(requests_enabled=True))
```

`BlockedTasksPolicy` defaults:

| Option | Default | Meaning |
| --- | --- | --- |
| `explain_enabled` | `True` | Read-only blocked-status endpoint is published. |
| `requests_enabled` | `False` | Durable request/triage endpoints require explicit opt-in. |
| `max_requests_per_task` | `64` | Bound on durable requests per task (429 beyond). |
| `budget_source` / `recovery_source` / `extra_sources` | `None`/`()` | Parent-injected callables `(service, principal, task) -> None | {"blocked": bool, "reason_class": str}` reporting generic reason classes (`budget_hold`, `budget_unknown`, `budget_exhausted`, `recovery_required`, `recovery_in_progress`, `effects_unknown`). Guarded: exceptions and malformed output collapse to the generic blocker. |

With the default policy the feature is deterministic, local, and read-only; no
autonomous or paid behavior exists. All handlers are pure bounded SQL under the
canonical lock, so none is marked detached.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/projects/{project_id}/tasks/{task_id}/blocked-status` | Explanation for one exact task, fenced to the path project. Optional `run_id`/`agent_id` query (bounded, authorized) adds run-dependent gates: dispatch gates, assignment/handoff fences, permit fence, raw claim/control availability, capabilities. |
| `POST /v1/projects/{project_id}/blocked-tasks:request` | File a durable `resolution_request` (opt-in policy). Body: `{task_id, expected_task_revision, action, note?, basis?, run_id?, agent_id?}`. The optional `run_id`/`agent_id` pair is a **frozen candidate binding**: explicit caller-supplied IDs (an agent candidate requires its exact run), authorized at filing and re-authorized at every later use — binding grants no authority. Requires `Idempotency-Key`. |
| `GET /v1/projects/{project_id}/blocked-task-requests` | List requests (`task_id` filter optional), re-authorized per view. |
| `GET /v1/blocked-task-requests/{request_id}` | Fetch one request, re-authorized and re-redacted. |
| `POST /v1/blocked-task-requests/{request_id}:acknowledge` | Operator/responsible-actor triage; acknowledgment only. CAS publish. |
| `POST /v1/blocked-task-requests/{request_id}:resolve` | Close as `resolved` (fresh readiness proof required), `declined`, or `request_canceled`. CAS publish. |

## Explanation shape

```json
{
  "task_id": "task_…", "task_revision": 3, "task_status": "queued",
  "readiness": "blocked", "ready": false,
  "readiness_scope": "task_only", "run_checks_evaluated": false,
  "blockers": [
    {"code": "artifact_missing", "action": "artifact_needed",
     "summary": "A required artifact submission is missing.",
     "source": "dependency_gates", "provenance": "gate_snapshot",
     "dependency_id": "task_…"}
  ],
  "suggested_actions": ["artifact_needed"],
  "gates_evaluated": ["scheduling", "dependency_graph", "dependency_gates", "budgets"],
  "authority": "explanation_snapshot_not_execution_permission",
  "as_of": "2026-10-12T17:00:00Z"
}
```

`readiness` is one of `startable` (queued, zero blockers — the only state with
`ready: true`), `blocked`, `already_running`, or `terminal`. `readiness_scope` is
`candidate` only when an exact caller-authorized run was supplied and its
dispatch/permit/claim/control/capability phases actually ran
(`run_checks_evaluated: true`); otherwise the scope is honestly `task_only` and a
task-only "startable" never claims candidate or start permission.
`gates_evaluated` lists only the phases that actually executed for this call
(`scheduling`, `dependency_graph`, `dependency_gates`, `budgets`, `execution`,
`run_liveness`, `scheduling_fences`, `dispatch_gates`, `capabilities`,
`permit_fence`, `claims`, `controls`, and any hook sources) — never the contents
of the gate registry.

Blocker codes are stable and privacy-safe: scheduling (`escalated_hold`,
`attempt_budget_exhausted`, `retry_backoff`, `assignment_mismatch`,
`handoff_target_mismatch`), dependencies (`dependency_incomplete`,
`dependency_unverified`, `dependency_cycle`, `dependency_unavailable`), budgets
(`budget_not_configured`, `budget_settlement_unknown`, `budget_reservation_outstanding`,
`token_budget_insufficient`, `time_budget_exhausted`, `cost_budget_insufficient` — zero
budgets are exhausted budgets, and the mandatory output reserve can never fit a zero
token budget), artifacts (`artifact_missing`, `artifact_contract_revoked`,
`artifact_contract_revised`, `artifact_binding_stale`, `artifact_contract_unsatisfied`,
`artifact_deleted`, `artifact_superseded`, `artifact_checksum_mismatch`,
`artifact_cross_project`), capability/workload (`capability_mismatch`, `runtime_*`,
`tool_constraint_mismatch`, `strict_sandbox_unsupported`, `agent_busy`, `no_live_run`,
`worker_run_required`), permissions/control (`scope_not_owned`, `path_outside_scope`,
`scope_claimed`, `scope_conflict_unavailable`, `control_hold`,
`control_state_unavailable`), and recovery (`run_not_ready`, `execution_active`,
`execution_interrupted`). Anything that cannot be verified for this principal is
`blocker_unavailable` with no identifiers.

## Requests, fingerprints, and lifecycle

Each request records an exact `request_fingerprint` (task, action, requester, basis,
note) and a `source_fingerprint` (task id, task revision, and the current blocker
codes), so later triage can see whether the underlying situation drifted. Creation
validates `expected_task_revision` against the live task (412 on drift), bounds open
requests per task, and validates every basis ref: currently visible, kind-correct,
project-local, revision-current (409 on stale pins), and non-private to the requester —
a private basis across a namespace boundary is rejected (403). The record is stored
through the canonical command seam; idempotent replay re-authorizes and re-redacts
against current state.

State machine: `open` → `acknowledged` → (`resolved` | `declined`), or `open` →
`declined`, or (`open` | `acknowledged`) → `request_canceled` (requester or operator).
Closed requests reject further changes; a same-outcome replay returns the recorded
receipt.

`resolved` is scoped to the *requested* condition, proven fresh at close time:

- **Candidate-scoped actions** (`artifact_needed`, `permission_review`,
  `recovery_review`, `operator_attention`) require the request's frozen candidate
  run, re-authorized now (gone/private/mismatched candidates fail with
  `resolution_candidate_unavailable`), and a candidate-scope explanation with zero
  current blockers — a task-only view or an arbitrary zero-blocker read is never
  proof for these.
- **`dependency_review`** may be proven task-only, once every bounded dependency and
  dependency gate clears and no unknown-effects report stands.
- **All resolution actions** re-snapshot the budget ledger and reject unknown, open, or
  expired holds **even when the task is completed** — settlement state is never
  inferred from task metadata.
- **Source-hook blockers** (unknown effects, recovery reports) are never cleared by
  task status, completion, acknowledgment, or operator intent.
- Terminal `completed` proves only the canonical task condition — never provider
  effects, budget settlement, or recovery state. Candidate-scoped requests cannot
  resolve from terminal metadata; use a genuinely evidenced workflow or explicitly
  decline/cancel the request. Task-level completion still rechecks dependency gates.

Every get/list/replay re-authorizes the *current* principal against the *current* task,
request resource, requester standing, and basis refs. If the requester principal was
revoked or moved out of the project, source-derived details (`note`, `basis`) are
redacted (`source_redacted: true`); if an individual basis ref became unavailable or
stale, it is withdrawn (`basis_redacted: true`) while the durable record itself remains.

## Limitations

- Explanations are point-in-time snapshots; they grant no execution authority and may
  change on the next read.
- Run-dependent blockers (permits, claim/control availability, dispatch gates) require
  the caller to pass its own authorized `run_id`; run/agent fencing is enforced exactly
  as at dispatch.
- Verified-dependency readiness is reported via the composed dependency gates; this
  module performs no verification itself.
- Resolution requests express intent only; acting on them (raising a budget, submitting
  an artifact, adjusting scopes) always goes through the owning feature's own governed
  endpoints.
- The budget picture is the ledger's read-only snapshot; reservation/counting semantics
  remain owned by `task_budgets`.
- A task-only explanation (no `run_id`) intentionally skips the dispatch, permit,
  claim, and control phases; it is a dependency/budget/scheduling picture, not a
  candidate verdict, and cannot prove candidate-scoped resolution requests.
- With the default root registration the capability is installed disabled and
  publishes no routes; enabling is an explicit JSON opt-in
  (`blocked_tasks.explain_enabled` / `requests_enabled`).
