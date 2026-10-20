# Bounded task scheduling

`hivemind/scheduling.py` adds pull-based scheduling on top of the existing durable task,
scope, work-claim, run, and control resources:

- **Plans** group related tasks with plan-local dependency keys, priority, required
  capabilities, and a bounded attempt budget.
- **Dispatch** is the worker contract: one live run asks for at most one eligible task
  and receives the task plus the exclusive scope claim that authorizes it.
- **Attempts** are counted per dispatch, retried with deterministic backoff, and
  escalated when the budget is exhausted.
- **Reconciliation** is explicit: stale or terminal scheduling state is only recovered
  by an administrator/human action, never automatically.

Nothing here executes agent work, starts paid runs, grants authority beyond the existing
scope permits, releases claims, or verifies results. Dispatch acquires the task's exact
scopes through the existing work-claim API and starts the existing task execution, so
core checkpoints, permits, and recovery keep working unchanged.

Three storage additions, no core model changes:

| Resource | Purpose |
| --- | --- |
| `task_plan` | Plan title, description, and ordered `task_ids`. |
| `agent_capability` | Optional explicit capability list per agent (additive to `supported_actions`). |
| `task` body | Adds `priority`, `max_attempts`, `required_capabilities`, `depends_on`, `plan_id`, `plan_key`, `plan_order`, and a `scheduling` metadata block. |

## Create a plan

`POST /v1/task-plans` (requires `Idempotency-Key`):

```json
{
  "project_id": "project_…",
  "title": "Website header",
  "description": "Header and navigation work",
  "tasks": [
    {"key": "design", "title": "Design header", "scope_ids": ["scope_…"], "priority": 20},
    {"key": "nav", "title": "Implement navigation", "scope_ids": ["scope_…"],
     "depends_on": ["design"], "priority": 10, "required_capabilities": ["frontend"],
     "max_attempts": 2}
  ]
}
```

Validation is rejected with `invalid_request` (422) when: keys are missing, blank, or
duplicated; a dependency references a key outside the plan; a task depends on itself;
dependencies contain a cycle; more than 64 steps are sent; `priority` is outside
0–1000; `max_attempts` is outside 1–10; or any scope ID is not a scope in the project.

Planned tasks are ordinary canonical tasks (`status: "queued"`) created in stable
topological order with `WORKS_ON` relations to their scopes and `DEPENDS_ON` relations
between dependent tasks. Reads:

- `GET /v1/task-plans?project_id=…` returns bounded plan summaries.
- `GET /v1/task-plans/{plan_id}` returns `{"plan": …, "tasks": [...]}` where each task
  also reports `dependencies_met` and `dispatchable` (scheduling gates plus dependencies).

## Dispatch (worker contract)

`POST /v1/projects/{project_id}/tasks:dispatch` (requires `Idempotency-Key`):

```json
{"agent_id": "agent_…", "run_id": "run_…"}
```

Returns exactly one of:

```json
{"task": {…}, "claim": {…}}
{"task": null, "claim": null}
```

`task` is the started task (with a fresh `execution`), and `claim` is the exclusive
scope claim that covers it, or `null` for a task with no scopes. Errors:

| Status | Code | Meaning |
| --- | --- | --- |
| 403 | `forbidden` | The caller does not represent `agent_id`. |
| 404 | `not_found` | Unknown run, or a run belonging to another agent/project. |
| 409 | `run_not_ready` | The run is terminal or not running/idle. |
| 409 | decision envelope | A conflicting scope claim denied the dispatch: the response body is the normal envelope with `data = {"task": null, "claim": null, "conflict": {…}}`, and the canonical conflict record is preserved (as with `POST /v1/work-claims`). |
| 409 | `control_pending`, `scope_not_owned`, … | Existing control/permit fencing refused the start; the transaction rolled back and no attempt was consumed. |

Selection is deterministic. A task is eligible when all of the following hold:

1. `status == "queued"` and scheduling `escalated` is false.
2. `scheduling.attempts < max_attempts` and `retry_after` is unset or already past.
3. `scheduling.assigned_agent_id` is unset or equals the requesting agent (sticky retry).
4. Every entry of `required_capabilities` is in the agent's capability set.
5. Every task in `depends_on` is `status == "completed"`.

Candidates are ordered by highest `priority`, then plan order, then ingestion sequence,
then task ID. The run must be running/idle and must not already hold a task assignment.

Dispatch then, inside one canonical transaction:

- reuses an existing live `exclusive_owner` claim of the same run with exactly the same
  scope set, otherwise acquires a new claim through `Service.claim` (intent
  `Dispatch scheduled task …`, TTL 120s, renewable with the existing claim API);
- increments the dispatch attempt counter and records `assigned_agent_id`,
  `last_dispatch_run_id`, `last_dispatch_at`, and an `attempt_history` entry;
- starts the task through `Service.task_start` with the fresh revision, so the existing
  permit checks (live claims, pending controls, scope ownership) still apply.

If any step fails, the transaction rolls back and no attempt is consumed. A denied claim
is returned without an exception so its conflict record stays canonical: the scope
conflict is deterministic, and dispatch never overrides a live exclusive owner.

## Reports are self-reports

`POST /v1/tasks/{task_id}:dispatch-outcome`:

```json
{"expected_revision": 4, "agent_id": "agent_…", "run_id": "run_…",
 "outcome": "failed", "summary": "Toolchain missing",
 "evidence_refs": [{"kind": "board_post", "id": "post_…"}]}
```

- `succeeded` marks the task `completed`; `failed` marks it `queued` again (retry) or
  `failed` (budget exhausted). Both are recorded as self-reports with
  `scheduling.last_outcome.self_report = true`; they are **not** verification and grant
  no authority. The evidence-bearing path remains the core task checkpoint API.
- The report is fenced by `expected_revision`, by the assigned agent, and by
  `scheduling.state == "dispatched"`. Reassigning or reconciling the task invalidates
  older reports (`409 task_not_dispatched`).
- Evidence references are validated and pinned to canonical revisions; unavailable
  evidence fails the report.
- The ended execution is moved into `execution_history` (bounded to 20 entries) with its
  result, and the run's task assignment is cleared.

Retries use deterministic exponential backoff without jitter:
`retry_after = now + min(5 × 2^(attempts−1), 300)` seconds. Jitter is omitted so
arbitration and tests are reproducible; spread work across agents with priority and
assignment instead.

When the budget is exhausted the task becomes terminal (`status: "failed"`) with
`scheduling.escalated = true`, `scheduling.state = "escalated"`, and
`scheduling.escalation` (`attempt_budget_exhausted`, attempts, agent, last summary).
Escalated tasks are never dispatched automatically.

Attempt budgets live in `scheduling.*` on the task and stay independent of work claims:
claims keep their own state, TTL, and renewal, and a failed attempt never releases a
claim.

## Explicit reconciliation

`POST /v1/tasks/{task_id}:reconcile`:

```json
{"expected_revision": 6, "action": "requeue", "reason": "Run never heartbeated again"}
```

| Action | Effect |
| --- | --- |
| `requeue` | Task returns to `queued`, the attempt window resets to 0, escalation clears, the stale execution is archived. |
| `reassign` | As `requeue`, plus `assigned_agent_id` clears and `reassignments` increments so another agent may take it. |
| `escalate` | Scheduling marks the task `escalated` (`review_requested`) so dispatch skips it until a reviewer requeues or reassigns it. |

Rules and safety:

- Only `admin` and `human` principals may reconcile; agents receive `403 forbidden`.
- A live execution is never released implicitly. A task is only recoverable when its
  execution's run is terminal or has not heartbeated for more than 30 seconds; otherwise
  the endpoint returns `409 task_execution_active`. Unverifiable run liveness also counts
  as live.
- Completed and cancelled tasks stay terminal (`409 terminal_task`); scheduling metadata
  is never reset without an explicit revision-checked request.
- Reconciliation archives the previous execution into `execution_history` and clears the
  run's task assignment, but it **does not** touch work claims — releasing or renewing a
  claim stays with the existing claim API.

## Capability matching

`POST` and `GET /v1/agents/{agent_id}/capabilities`:

```json
{"capabilities": ["frontend", "sqlite"]}
```

The effective capability set is the agent's existing `supported_actions` (for example
`assign_task`) plus the explicit list above. `GET` returns `supported_actions`,
`capabilities`, and the sorted `effective` set.

Registration requires the caller to represent that agent (self-registration) or an
administrator acting for it; another agent's credential is refused. Capabilities only
affect scheduling eligibility — they never grant permissions, scopes, or control
authority, and a task still needs live scope claims to start.

## Endpoints

| Method | Path | Idempotent |
| --- | --- | --- |
| POST | `/v1/task-plans` | Yes (key required) |
| GET | `/v1/task-plans?project_id=…` | Read |
| GET | `/v1/task-plans/{plan_id}` | Read |
| POST | `/v1/projects/{project_id}/tasks:dispatch` | Yes (key required) |
| POST | `/v1/tasks/{task_id}:dispatch-outcome` | Yes (key required) |
| POST | `/v1/tasks/{task_id}:reconcile` | Yes (key required) |
| POST | `/v1/agents/{agent_id}/capabilities` | Yes (key required) |
| GET | `/v1/agents/{agent_id}/capabilities` | Read |

`hivemind.scheduling.install(app, service, principal, command, envelope)` registers all
of them; `hivemind.scheduling.MODELS` lists the request models for schema export.

## Limitations

- **No timers or workers.** Dispatch is pull-based: `retry_after` only gates eligibility,
  and nothing runs in the background. A stalled plan needs a worker to poll dispatch, and
  a stale execution needs an explicit reconcile.
- **No automatic handoff.** Exhausted attempts escalate; moving work to another agent is
  an explicit `reassign`.
- **Self-reports are not proof.** Completion and failure come from the assigned agent.
  Independent verification still requires evidence, review, or core checkpoints.
- Capability names are free-form strings; there is no vocabulary registry, namespace
  validation, or wildcard, and `supported_actions` doubles as a capability source even
  though those values describe control actions.
- Task-level completion through the core checkpoint API is respected for dependency
  gating, but such a task keeps its `dispatched` scheduling state until a report or
  reconcile; a core checkpoint `failed` is terminal and needs reconcile to retry.
- Scheduling metadata is stored in the task body, so every scheduling write bumps the
  task revision (and its ingestion sequence), appears in task reads, and can conflict
  with unrelated concurrent task updates (`412 revision_conflict`).
- Backoff is jitter-free, so many agents can retry the same plan in step.
- Idempotent command replays return the original response, which may be older than the
  current task state; read the task before acting on a cached reply.
- No dashboard, UI, paid model runs, or additional privileges are added by this module.
