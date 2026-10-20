# Background operation

The service exposes these routes by default with policy automation disabled.
An explicit operator integration configuration can enable periodic ticks:

```json
{"coordination_policy":{"policy":{"enabled":true},"background":{"enabled":true,
 "operator_principal_id":"principal-uuid","project_ids":["project-uuid"],
 "interval_seconds":5,"task_limit":50}}}
```

Use actual canonical IDs. The scheduler refetches the configured principal and
current project authority on every tick. Agent operators also need a current
Nexus lease and coordinator grant. Background ticking does not install an
evidence provider or make unknown executions safe to reassign.
# Coordination policy

`hivemind/coordination_policy.py` adds opt-in, policy-driven governance on top of task
scheduling: bounded escalation, reassignment, and dead-lettering, plus
declared-resource conflict handling that goes beyond literal path claims.

Nothing changes until a project stores a policy with `enabled: true`. The module never
executes agent work, never releases or steals work claims, never publishes anything
externally, and never marks dead-lettered work successful.

## Records

All records are created through the canonical `Service.create`/`Service.mutate` path,
so ids, revisions, optimistic concurrency, idempotency, and audit behave exactly like
every other resource:

| Kind | Purpose |
| --- | --- |
| `coordination_policy` | One typed, bounded policy record per project. |
| `resource_declaration` | One frozen-in-flight effect declaration per task. |
| `coordination_action` | Durable per-action intent/audit record (`intended` → `applied` / `refused` / `manual_hold` / `resolved` / `dismissed`). |
| `dead_letter` | Terminal preservation record containing the full task envelope snapshot. |

## Evidence rules (the hard part)

Heartbeat or presence age is **never** authoritative termination, quiescence, or
no-effect evidence. Every execution is adjudicated into exactly one of:

- **`absent`** — no execution record: never-started work, or an execution already
  archived by canonical seams (report/reconcile/handoff).
- **`settled`** — binding-matched canonical proof: a terminal task checkpoint applied
  under the current task/run/execution (`Service.task_progress` only applies a
  terminal status to the currently bound execution), or an authoritative adjudication
  from the installed **evidence provider** (the parent recovery/ops seam).
- **`active`** — the executing run is verifiably present (fresh heartbeat). Presence
  is only used in this conservative direction: automation does not interfere.
- **`ambiguous`** — everything else: silent, stale, paused, or missing runtimes.
  Ambiguity always becomes a `manual_hold`; nothing is archived, reassigned,
  dead-lettered, or cleared on heartbeat thresholds alone. Absent capture/event data
  is never fabricated into negative proof.

The optional evidence provider is installed at wiring time:

```python
coordination_policy.install(app, service, principal, command, envelope,
                            policy={...}, evidence=my_adjudicator)

def my_adjudicator(principal, task_body):
    # Return None for no opinion (execution stays ambiguous/held), or:
    return {"adjudication": "settled" | "active" | "ambiguous", "basis": {...}}
```

It is the seam through which parent recovery/ops modules contribute authoritative
quiescence or settlement adjudications; this module never derives them itself.

## Policy (`PolicyUpdate`)

| Field | Default | Bounds | Meaning |
| --- | --- | --- | --- |
| `enabled` | `false` | – | Master switch; disabled ticks are explicit no-ops. |
| `max_escalations` | 2 | 0..5 | Policy-driven escalations per task. |
| `escalation_cooldown_seconds` | 60 | 0..86400 | Minimum quiet period between automatic actions on one task. |
| `attempt_deadline_seconds` | 900 | 30..86400 | A dispatched attempt older than this with a non-active runtime is evaluated (and held when ambiguous). |
| `dead_letter_after_escalations` | 2 | 1..5 | Escalations before dead-lettering — applied only to absent/settled executions. |
| `max_reassignments` | 2 | 0..10 | Automatic reassignments per task (never-started or settled executions only). |
| `auto_reassign` | `false` | – | Allow ticks to reassign never-started or canonically settled work. |
| `auto_dead_letter` | `false` | – | Allow ticks to dead-letter escalation-exhausted tasks (absent/settled executions only). |
| `strict_resources` | `false` | – | Defer dispatch for undeclared/unknown effects instead of allowing. |
| `require_coordinator_lease` | `true` | – | Non-admin ticks must fence with the Nexus lease. |
| `conflict_cooldown_seconds` | 30 | 0..3600 | Deterministic deferral delay for resource conflicts. |

## Endpoints

- `PUT /v1/projects/{project_id}/coordination-policy` — set the policy (admin/human).
- `GET /v1/projects/{project_id}/coordination-policy` — policy, counts, recent actions.
- `POST /v1/projects/{project_id}/coordination-policy:tick` — one bounded pass
  (`limit` ≤ 100). Admins/humans tick directly; any other principal must hold the
  Nexus coordinator lease and pass its `coordinator_epoch` (stale/missing epoch → 409,
  missing epoch → 422).
- `PUT /v1/tasks/{task_id}/resource-intents` — declare effects (admin/human or the
  currently assigned agent; `expected_task_revision` required). Rejected with
  `declaration_frozen` (409) while an execution exists: effects cannot be rewritten
  mid-flight. `scope:`/`artifact:`/`dependency:` keys are authorized against the
  canonical store — forged, private, or cross-project references are rejected (404).
  `path:` keys are normalized (`.`/`..` collapsed or rejected); `interface:` keys are
  explicit non-authoritative semantic labels; `label:` keys are hints that never
  participate in conflict decisions.
- `GET /v1/tasks/{task_id}/resource-intents` — read a declaration.
- `POST /v1/coordination-actions/{action_id}:resolve` — operator decision on a
  `manual_hold`: `dismiss`/`escalate` never alter ownership; `reassign`/`dead_letter`
  require an absent or canonically settled execution (409 `execution_not_settled`
  otherwise). `acknowledge_unknown_effects` is audit metadata, not evidence, and
  never bypasses settlement. Ownership transfer for live work remains the existing
  scheduling `:handoff` endpoint.
- `GET /v1/coordination-actions/{action_id}` — read one action record.
- `GET /v1/coordination-policy/descriptors` — machine-readable operation descriptors
  (purpose, permissions, evidence, inputs/outputs, limits, retry semantics) for
  agent-facing discovery and MCP wiring; also exposed as
  `coordination_policy.descriptors()`.

All mutating endpoints require an `Idempotency-Key`; replays return the cached outcome
and never re-apply actions.

## Tick semantics

For each task (bounded by `limit`, skipping completed/cancelled, dead-lettered, and
in-cooldown tasks), a tick adjudicates the execution and then:

- **active** → skipped.
- **ambiguous** → a `manual_hold` coordination action (with the adjudication detail)
  once the attempt deadline has passed, or immediately when an escalated task's
  dead-letter budget is reached — dead-lettering never terminalizes an ambiguous or
  active execution.
- **settled** → `reassign` (when `auto_reassign` and budget remain) or flag-only
  `escalate`. Reassignment archives the settled execution with the adjudication proof
  and clears the run's `task_id` pointer; claims are untouched.
- **absent** → escalated tasks dead-letter per budget with `auto_dead_letter` (the
  full envelope is preserved in a `dead_letter` record; the status becomes the
  terminal non-success `failed`); queued tasks with a stale assignment and no
  execution are legitimately never-started and may be reassigned; a `dispatched`
  scheduling state with no execution record is inconsistent and held.

## Resource conflict handling at dispatch

When a project policy is enabled, scheduling dispatch consults
`service.coordination_gate` after selecting a candidate and before acquiring claims:

- The candidate's declaration is compared against every in-flight (dispatched) peer's
  declaration using exact normalized key equality. `write`/`write` and `write`/`read`
  overlaps conflict; `read`/`read` never does. `interfaces` and `dependencies` count
  as shared writes; `label:` hints never conflict.
- On conflict, dispatch deterministically **defers**: `retry_after` is set to
  `now + conflict_cooldown_seconds`, a `defer` coordination action is recorded with
  the conflicting keys and peer task ids, no attempt budget is consumed, and the
  response carries `deferral` evidence.
- With `strict_resources`, an undeclared candidate or an undeclared in-flight peer
  also defers — unknown effects are refused rather than assumed safe.

Semantic conflicts are not auto-merged. Conflict detection is exact-key over declared
resources; it is not an arbitrary semantic conflict solver. Deferral evidence is
actionable (exact keys and peer task ids), and resolution stays with operators through
the existing reconcile/handoff endpoints or hold resolution.

## Honest limits

- Conflict detection cannot see overlap between differently-named keys, and undeclared
  effects are invisible unless `strict_resources` trades availability for safety.
- Without an installed evidence provider, interrupted executions (terminal run, crash,
  network partition) are always held for operators; only never-started work and
  canonically checkpoint-settled executions are handled automatically.
- A tick evaluates at most `limit` tasks per call; larger backlogs need repeated ticks.
