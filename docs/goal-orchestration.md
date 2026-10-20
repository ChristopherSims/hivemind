# Goal orchestration

Goal orchestration (`hivemind/goal_orchestration.py`) turns a durable, operator-owned
objective into a bounded, explicit task DAG and drives its ready work through the
existing canonical scheduling layer. It adds no parallel runtime: plans are real
`task_plan` resources, tasks are real tasks, and dispatch is the canonical
`Scheduling.dispatch_exact` — the exact intended task of the goal's *current* plan,
with the same authority, gates, claims, and durable executions. A tick can never
select another goal's or an obsolete plan's work.

## Resources

| Kind | Meaning |
| --- | --- |
| `goal` | Objective, current plan pointer, plan revision, logical epoch, bounded immutable revision records, and an optional hold. |
| `goal_plan_intent` | Durable provider reservation/proposal: pins principal, operation, idempotency key, payload hash, and source goal revision/epoch **before** any inference; transitions `reserved` → `done`; single-use. |

Old plans are never mutated; a revision creates a new plan and appends one
immutable record (`goal.revisions`, at most 20 embedded records, `REVISION_LIMIT`).
Plans and tasks are bounded to 64 per goal/revision (`MAX_GOAL_TASKS`), enforced by
both the API models and canonical `TaskPlanCreate` validation.

## Endpoints

Installed by `install(app, service, principal, command, envelope, *, plan_provider=None,
hooks=None)` — the same convention as every other feature module:

| Route | Operation | Notes |
| --- | --- | --- |
| `POST /v1/goals` | `goal.create` | Supplied steps or deterministic extractive decomposition. |
| `GET /v1/goals?project_id=` | list | Bounded to 200; revision sources re-authorized. |
| `GET /v1/goals/{goal_id}` | `goal.read` | Goal view: derived state, plan, tasks, ready ids, held ids. |
| `POST /v1/goals/{goal_id}:revise` | `goal.revise` | 409 + persisted hold when unknown-active executions exist. |
| `POST /v1/goals/{goal_id}:tick` | `goal.tick` | Bounded exact-task ready-work dispatch. |
| `POST /v1/goals/{goal_id}:propose` | `goal.propose` | Durable compute reservation + provider; detached handler. |

All mutations require an `Idempotency-Key` and flow through `Service.mutate`;
replays re-validate goal existence and current ACLs before returning cached results.
`OPERATION_DESCRIPTORS` / `descriptors()` expose plain metadata (name, purpose,
input, output, authority, side effects, limits, retry safety) for parent discovery.

## Plan sources and policy

- **supplied** — the operator provides the full step list, validated exactly like a
  canonical task plan: unique keys, dependencies resolvable inside the plan, acyclic
  (`invalid_plan_steps`, 422 otherwise).
- **extractive** — deterministic decomposition of the explicit statement: list-marked
  lines (`-`, `*`, `•`, `1.`/`1)`) become independent steps; anything else becomes a
  single step. This is extraction of operator-supplied structure, **not** NLP
  planning; no model call is made or implied.
- **provider** — see below.

**Goal scopes.** Declared `scope_ids` are authorized canonically at creation and
*bound* every generated step (extractive steps inherit them) and every supplied step
(each step's scopes must be a subset, else `goal_scope_exceeded` 422). With no
declared goal scopes, per-step scopes are the explicit contract and the goal carries
no overall scope claim — honestly empty metadata, not an implicit grant.

## Provider proposals under durable compute reservation

The `plan_provider` is explicit availability, never inferred intent: without one,
`goal.propose` fails definitely (`plan_provider_unavailable`, 422); a configured
provider is trusted to be synchronous and side-effect free — but that trust never
justifies a replay-driven re-invocation. The flow:

1. **Reserve (under the canonical mutation).** A `goal_plan_intent` is persisted in
   `reserved` state, pinning the exact principal, operation, idempotency key,
   payload hash, and the source goal revision/epoch — *before* any inference.
2. **Claim (durable, once).** `compute_invocations` transitions 0 → 1 atomically.
   Any simultaneous, replayed, or post-crash caller sees the claim and gets a
   definite `plan_intent_held` (409) — the provider is invoked **at most once**
   per reservation, and orphaned reservations are held forever, never
   auto-re-invoked.
3. **Compute (off the canonical lock/transaction).** The route is a detached
   handler; the provider receives only a minimal goal summary.
4. **Commit (under a second canonical mutation).** Revalidates the reserving
   principal, payload hash, and that the goal's revision *and* epoch are unchanged
   since the reservation. A goal that moved during compute fails definitely
   (`plan_intent_source_changed`, 409) — a stale proposal is never rebound to a
   new epoch. Success writes the strictly coerced steps + digest and marks the
   intent `done`.

Replays of a `done` intent re-check current authority (fresh canonical read) and
return the durable result without recompute. Only `done` intents can be applied by
`goal.revise` (`plan_intent_not_ready` otherwise), exactly once
(`plan_intent_consumed`), under epoch validation and digest re-verification.

## Revision policy

`goal.revise` requires administrator/human authority or a Nexus coordinator acting
under a live fenced epoch (`coordinator_epoch`, validated by `Service.coordinator`).
Optimistic concurrency: `expected_revision` (412 on mismatch) and optional
`expected_epoch` (409 `stale_goal_epoch`). Sources are mutually exclusive:
`steps`, `statement`, `intent_id`, or none (carryover of the not-yet-completed
tasks, preserving keys, scopes, priorities, capabilities, and dependencies whose
targets remain incomplete). Completed producers remain explicit canonical
`external_dependency_ids`; their current artifact contracts still gate consumers
in the new plan. Internal and external dependencies together are capped at 64.

**Supersession.** A successful revision atomically retires the old plan's
non-terminal, not-in-flight tasks: each gains `scheduling.escalated` (so canonical
dispatch — including regular worker `tasks:dispatch` — never selects it again) plus
a deterministic `superseded` record bound to the new goal epoch and plan revision.
Completed tasks and their evidence are never touched, claims are never copied, and
the immutable old plan stays readable.

**Holds.** Any current execution on a non-terminal task — active, paused, or of
unknown liveness — holds the revision (`unknown_active_executions`, 409, persisted
on the goal). A stale heartbeat or a terminal run alone is **not** quiescence; only
canonical terminal processing (an outcome report or an explicit reconcile) releases
a task. A task that failed with a terminal recorded outcome can be revised (its
execution is archived); status alone is never treated as proof of quiescence.

Each revision record embeds **canonical observed outcomes**: per-task status,
execution id, self-reported outcome, and `externally_verified`. Silence and
self-reported success are never recorded as external proof — `externally_verified`
is true only with an independent canonical verification (reviewer ≠ executor,
matching checkpoint), mirroring `tasks.verification`.

## Tick policy

`goal.tick` finds the goal's current-plan tasks whose dependencies are met and whose
scheduling gates are open, then dispatches at most `limit` (default 8, max 64)
through canonical `Scheduling.dispatch_exact` — the exact intended task, exact
exclusive-owner scope claims, durable executions, one task per idle run. A gated or
conflicting task is skipped, never replaced by unrelated project work. Dispatch
targets are only runs of agents the caller may currently represent (administrators:
any; agents: their own runs). A coordinator epoch fences orchestration authority but
never widens dispatch identity: there is no arbitrary identity impersonation. A
stale tick epoch fails before any dispatch. Held tasks are reported and left
untouched.

## Derived views re-authorize their sources

`goal.read` and the list endpoint never serve raw cached history: every revision
record's `observed_outcomes` entries and its `plan_id` / `previous_plan_id` /
`intent_id` are re-checked against current visibility (private, deleted, or
ACL-revoked sources are redacted) before any derived summary is returned.

## Background scheduler

`GoalSchedule` + `GoalRunner.scheduled_tick()` mirror
`feature_scheduling.CoordinationRunner`: default off; enabling requires an explicit
`operator_principal_id` and explicit `project_ids` (max 20, unique), a bounded
interval (1–300 s) and tick limit. Every tick re-fetches the operator principal
fresh — a missing operator raises `configured_operator_unavailable` rather than
acting under stale authority. Non-administrator operators additionally need a live
Nexus lease whose exact current epoch fences every tick, or the project is skipped;
administrators act under their standing authority, consistent with the existing
coordination runner policy. Goals with an active hold are skipped. The API worker
lifecycle runs the configured scheduler off the event loop.

Set `integration.json` → `goal_orchestration.background.revise_failed` explicitly
to enable deterministic revision after canonical failed outcomes. It never retries
an unknown execution or invokes a planning provider. `max_plan_revisions` bounds
the total plan count, including the initial plan. Revisions preserve completed
producers as external dependencies and retire obsolete queued tasks atomically.
When routing is enabled, ticks use its eligible ranked runs while retaining all
canonical dispatch and direct-start gates.

## Integration gates (parent-owned dependencies)

Sibling features participate only through the `hooks` mapping injected at install
time; this module never imports or edits them. Hooks must be pure bounded reads (no
encoding/model/blocking work) — several run under canonical locks:

- `budgets(principal, project_id)` — return `{"allowed": False, "reason": str}` to
  deny an entire tick's dispatch (reported as `budget_denied`).
- `routing(principal, task, run)` — return `None` / `{"action": "allow"}` or
  `{"action": "defer"|"deny", "reason": ...}` to skip a candidate (task, run) pair;
  mirrors the parent's `service.task_dispatch_gates` / `task_dependency_gates`
  contract, which canonical dispatch also consults.
- `artifacts(principal, goal, revision_record)` — post-revision observer invoked
  inside the revision mutation; raising `Problem` aborts the revision definitely.

The module is registered by `create_app`, advertised as `goals.orchestration`,
and exposed through Python and TypeScript SDK facades. The explicit Python
installation options accept `plan_provider` and bounded `hooks`; JSON operator
configuration accepts only `background`, not arbitrary callable imports.
Provider work uses tracked detached computation. Cached mutation responses and
generic command receipts reapply current source visibility.

## Limits

- 64 tasks max per plan/revision; 32 goal scope refs; 20 embedded revision records.
- Tick dispatch limit 1–64 (default 8); goal lists bounded to 200.
- Provider invoked at most once per reservation; proposals strictly coerced and
  size-bounded; invalid output is a definite 422 with the reservation held.
- Built-in decomposition and automatic revision make no model calls. An explicitly
  supplied provider may perform inference; its durable one-invocation intent is
  not a provider billing cap or external-effect proof.
