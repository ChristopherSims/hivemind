# Runtime lifecycle reconciliation

Opt-in reconciler for abandoned sessions and unobserved workspace file changes, plus
cooperative safe-pause/cancel boundaries that cannot report an unverified stop.
`hivemind/runtime_lifecycle.py` is additive: it composes the existing capture spool
(`hivemind.sdk.Capture`) and safe-boundary adapter (`hivemind.sdk.ControlledAdapter`)
and does not change `hivemind/hooks.py`, the SDK, the service, or the API. The parent
wires `install(...)` explicitly; nothing is registered implicitly.

It never signals, kills, or attaches to a user process, never inspects a vendor
runtime, and never reads a transcript or captured reasoning. Runtime facts arrive only
through an injected adapter, and file evidence is content hashes and sizes only.

## Enablement

`RuntimeReconciler(client, project_id, agent_id, run_id, *, state_dir, workspace_root=None,
adapter=None, enabled=False, capture_authority=False, terminal_grant=False,
abandoned_status="failed", stale_after=30.0, absence_grace=300.0, scan_limits=None,
exclude=(), capture_interval=5.0)`

- `run_id` is the **observer** run. It owns the spool source and the control boundary.
  Observations are spooled under that run so this module's own reporting never
  refreshes an abandoned run's `last_seen_at`.
- `enabled=False` is the default and makes every write inert (`reason: "disabled"`).
- `capture_authority` states that the calling principal holds the actor grant the
  service enforces for `confirmed` observations.
- `terminal_grant` is the separate operator opt-in for writing a terminal run status.
- `abandoned_status` may be `failed` (default) or `cancelled`. `completed` is rejected
  at construction: completion is never inferred from an absence.
- `absence_grace` bounds how old an absence observation may be and still authorize a
  transition; `stale_after` only labels derived presence evidence (`offline` at or
  beyond it, `stale` from half of it).

Reconciling a run that belongs to a different agent requires an administrator
principal, because `Service.run` and `accept_event` both re-check agent representation.
An agent principal can reconcile its own agent's runs.

## Adapter protocol

An adapter is any object exposing `inspect`, `pause`, `cancel`, and `resume` (all
optional; a missing method is reported as `unsupported_action`, never as success):

- `inspect(run) -> AdapterObservation | dict | bool | None`
- `pause(run) / cancel(run) / resume(run) -> ControlAttempt | dict | bool | None`

Adapters receive a mapping with at least `id` (the target run id). Return values are
normalized (`normalize_observation`, `normalize_attempt`) without inventing facts: a
missing `present` becomes `present=None` (unknown), a missing `applied` becomes
`applied=None` (unverifiable). Two optional adapter attributes are honored:

- `authoritative_absence` – the adapter owns lifecycle knowledge for the run, so an
  explicit, timestamped `present: false` may classify the run as abandoned.
- `trusted_attribution` – the adapter itself observed a change and may carry a
  `confirmed` attribution when the caller also holds the capture grant.

`FileAdapter(state_file)` is the provided channel over a JSON state file written by a
trusted local runtime wrapper:

```json
{"run_id": "run_...", "present": true, "status": "running", "updated_at": 1770000000.0}
```

- A missing, unreadable, or run-mismatched file is **unknown**, never stopped.
- `present: true` older than `freshness_seconds` (default 30 s) is **unknown**.
- `present: false` (or `status: "stopped"`) naming the same run is authoritative
  absence; it still needs a timestamp inside `absence_grace`.
- Controls are cooperative: the adapter writes `<state>.control.json` and waits,
  bounded, for `<state>.control-ack.json` with the matching `request_id`. No signal is
  sent to any process.

## Classification

`reconcile(runs=None, scan=False, update_baseline=False)` re-reads every run by id, so
a decision always uses the current `control_revision`.

| Classification | Condition | Action |
| --- | --- | --- |
| `terminal` | run status is completed/failed/cancelled | none (`run_already_terminal`) |
| `live` | `present: true` | none |
| `stale` | no observation, `present=None`, non-authoritative absence, absence without timestamp, absence older than `absence_grace`, future timestamp | none |
| `abandoned` | authoritative, timestamped absence for this run | transition if both grants, else evidence only |

Derived presence (`online`/`stale`/`offline`, mirroring the service buckets) is recorded
in the decision as evidence. An ancient `last_seen_at` is never a decision input, so
stale presence is never treated as stopped.

## Terminal reconciliation

Only the canonical path is used: one `hivemind.context.run.ended` event with
`source.kind="runtime_hook"`, `attribution="confirmed"`, and
`expected_revision = run.control_revision`, submitted through `events:batch`. The
service then applies its normal revision, terminal, and capture-authority checks.

Ordering is write-ahead: the intended transition, its evidence, and the outcome are
persisted in `<state_dir>/<run_id>-lifecycle.json` before and after submission. Outcomes
are `applied`, `reported` (already terminal), `retryable` (revision conflict, re-read
next pass), or `outcome_unknown` (transport failure). A rejected event rewinds the
source sequence, so no gap is reported. An absent or failed grant yields `action: none`
with `terminal_grant_required` or `capture_authority_required`.

## Unobserved workspace drift

`snapshot()`, `drift(baseline=None, update_baseline=False)`, and `observe_drift(findings,
target_run=None, attribution=None)` use a bounded scan:

- `ScanLimits(max_files=2000, max_depth=16, max_file_bytes=262144,
  max_total_bytes=33554432, deadline_seconds=5.0, follow_symlinks=False)`; exceeding a
  bound truncates the scan and reports the reason instead of growing.
- Everything is resolved inside `workspace_root`; symlinks are not followed by default
  and any resolved path outside the root is skipped as `outside_workspace`.
- Secrets and dependency trees are excluded through `sdk.excluded` plus credential
  names/suffixes (`.env*`, `.ssh`, `.git`, `node_modules`, `.venv`, `dist`,
  `id_rsa`, `*.pem`, `*.key`, `.netrc`, ...). Excluded paths are never opened.
- Files at or below `max_file_bytes` are hashed with SHA-256; larger ones are recorded
  as `hashed: false` with size only, and equal-size unhashed pairs are treated as
  unobserved rather than reported.

Findings are validated against the canonical `file.changed` schema and spooled as
`kind="filesystem"` with `unattributed` (default) or `reported` attribution. Scan-derived
drift is never `confirmed`: requesting it raises, because the scanner cannot attribute an
actor. Findings whose target workspace differs from the observer run's workspace are
skipped with `workspace_mismatch` rather than recorded against the wrong workspace.
File records are attached to the observer run, which is why the observer run should be
created with the same `workspace_id` as the runs whose workspace it scans.

## Cooperative safe-pause/cancel boundaries

`LifecycleBoundary` subclasses `ControlledAdapter` and overrides `boundary()`:

1. Settle pending runtime results durably (replayed with a stable idempotency key).
2. For each live `pause`/`cancel`/`resume` control: acknowledge it, ask the adapter, and
   report the outcome.
3. `applied` requires the adapter to report `applied: true`, the exact required observed
   status (`paused`/`cancelled`/`running`), **and** a checkpoint id. Anything else is
   `outcome_unknown` (or `failed` for an explicit `applied: false`).
4. If a requested stop could not be established, the local host still stops mutating
   work (`paused`/`cancelled` plus an `unresolved` list) while the service records
   `outcome_unknown`. A requested pause is never silently ignored.
5. The inherited implementation (steer/assignment transitions) runs only after every
   runtime-observed control is settled; otherwise it could report an unverified stop as
   applied. If a result cannot be delivered, the pass returns early and retries next time.
6. `permit()` therefore inherits the stronger boundary and refuses mutating work while a
   stop is requested.

A `resume` still has to satisfy the service rule that the run holds an active work
claim. `enabled=False` disables the stronger boundary with the rest of the module.

## In-process wiring

`ServiceTransport(service, store, principal, command=None)` is a client-shaped
(`.request`) transport over the canonical `Service` for the routes this module uses
(`events:batch`, `runs`, `runs/{id}`, `runs/{id}/heartbeat`, `runs/{id}/permits:check`,
`agents/{id}/controls`, `controls/{id}:ack|:result|:checkpoint`). Unknown paths are
`not_found`; authorization, idempotency, and transition rules remain the service's.

`install(app, service, principal, command=None, envelope=None, *, path="/v0.1/runtime",
transport_factory=None)` registers `POST {path}:reconcile` and `GET {path}:state`.
`scan`, `adapter_state_file`, and cross-agent `targets` are administrator-only because
they read local files or act on another agent's run. State files live under
`<store.directory>/runtime/`.

## Validation and limits

`tests/test_runtime_lifecycle.py` covers, against the real local service (no paid
Factory or Oh my Pi session):

- bounded/contained/excluded scans, including truncation, unhashed oversized files, and
  proof that no content, secret value, or outside path crosses the boundary;
- drift reporting as hash-only, `unattributed`, `filesystem` evidence, workspace
  mismatch skipping, and the refusal to confirm scan-derived drift;
- the abandoned-versus-stale distinction: no observation, stale "running" claim with an
  ancient `last_seen_at`, run-mismatched marker, missing operator grant, missing actor
  grant, expired absence, disabled reconciler, and the successful canonical transition
  (plus `cancelled`, and the rejection of `completed`);
- pause/cancel/resume boundaries: no acknowledgement, success without a checkpoint,
  unsupported action, raising adapter, verified pause, verified cancel, verified resume,
  local fail-closed stop, and durable single settlement;
- control conflicts: a duplicate control is denied while one is pending for the run, and
  a further control is accepted only once the first has been settled and verified;
- a **live local** contract run against a trusted Python child process that publishes a
  state file, answers control requests, and exits on its own before absence is treated as
  authoritative;
- the `ServiceTransport` mapping and the `install` routes, including administrator gating.

Remaining limitations:

- Adapter support actually validated is the provided `FileAdapter` (temp-file channel,
  live-tested with a trusted local child process) and in-process test adapters. No real
  Droid, Factory, or Oh my Pi runtime adapter exists here; each vendor would need its own
  `inspect`/`pause`/`cancel`/`resume` implementation, and an adapter that cannot establish
  a stop or a checkpoint always degrades to `outcome_unknown`.
- The reconciler reacts only when invoked; there is no background scheduler, no
  filesystem watcher, and no retry loop beyond re-running `reconcile`.
- Drift is a scan-to-scan comparison, not an event stream: changes between two scans are
  collapsed into one finding per path, and oversized unhashed files are size-only.
- A control created concurrently between the runtime settle pass and the delegated
  inherited pass can still be reported by the legacy path; the reconciler's own pass is
  authoritative for every control it observes.
- Spooled drift events are ordinary accepted events, so an accepted `file.changed` for the
  observer run does refresh that run's presence. This is why the observer run should be a
  dedicated run rather than the run under investigation.
- `GET {path}:state` returns the last report, which contains workspace-relative paths; it
  is limited to the project/agent identity that produced it, but it is not a separate ACL.
