# Vendor session recovery

`hivemind/vendor_recovery.py` implements evidence-fenced recovery for vendor agent
sessions (Factory Droid and oh-my-pi). It exists because a saved vendor session id
is private bookkeeping, **not** proof that prior work had its intended effects, and
because the installed `droid_sdk` 0.5.0 `Session.resume` has no workspace binding:
when the server load response omits `cwd`/`worktree`, the SDK silently falls back to
the *caller's* process working directory. Resuming that way from a service process
both binds the agent to the wrong directory and makes the workspace equality check
trip after the session was already opened against a live vendor process.

The module separates two concerns so neither can be mistaken for the other:

1. **Local safe workspace launch/readiness** — spawn or bind the vendor client
   rooted at the managed workspace and judge server-reported load evidence. No
   model invocation happens here; the load is a deterministic read-only probe and
   never proves quiescence or prior effects by itself.
2. **Canonical full recovery authorization** — reconcile private runtime state and
   its settled-turn checkpoint against fresh server reads of the task, run, claim,
   permit, and handoff records before any continuation turn is sent.

## Invariants

- The process-global working directory is never changed; the workspace is passed to
  the vendor transport as its spawn directory, so concurrent runtimes stay
  independent.
- A load response that omits workspace, identity, quiescence, or authorization
  evidence **fails closed**. There is no fallback path that treats a missing field
  as a match; `None` model or autonomy in the loaded settings is *unknown*, not
  allowed.
- No quiescence is inferred from silence, terminal run labels, pause declarations,
  acknowledged requests, or a successful session load. An uncertain model turn
  (`is_agent_loop_in_progress` true *or absent*, pending permission/ask-user/queued
  messages) holds and is never replayed.
- Unsupported or drifted SDK contracts fail closed with `sdk_contract_changed`;
  they are never bypassed.
- Refusal never tears the vendor session down: `close_session` is a consequential
  action that could hit another controller's live session, so a refused recovery
  only raises; the caller may close its own client/transport.
- Private state files are regular, link/reparse-free, size-capped JSON documents
  contained in the trusted private state directory. A corrupt or unreadable
  checkpoint or intent file **holds** recovery (fail closed), never looks like a
  clean slate.
- A durable recovery intent is recorded by **exclusive atomic create** before any
  uncertain load — a concurrent helper can never overwrite a pending unknown
  recovery — and is cleared by compare-then-delete against the recorded owner id,
  so one helper cannot drop another owner's intent. The intent binds the full
  session identity plus a hashed (never raw) options fingerprint.
- A session load is retried only by explicit caller decision (the helper performs
  exactly one load per invocation, plus one verification reload after a
  `restrict_tools` write); `restrict_tools` is re-asserted on the server and then
  **verified by a fresh read**, never trusted from the write acknowledgement.
- Refusals carry stable generic reason codes only — no paths, prompt bodies, or
  credentials — so a blocked-task surface can consume them safely.
- A recovered or loaded session is never proof that an old model turn had its
  effects: canonical journal holds (`unknown` outcomes) stand regardless of what
  the vendor reports.

## API

### Local state and canonical evidence (worker preparation)

```python
from hivemind import vendor_recovery as vr

# Private runtime state (outside the workspace). Empty state = fresh start.
# for_continuation=True makes every expected id mandatory.
verdict = vr.validate_private_state(
    runtime.state, vendor=runtime.vendor, workspace=runtime.workspace,
    expect_run_id=context["run_id"], expect_task_id=task["id"],
    expect_execution_id=context["execution_id"],
    source_execution_id=context.get("source_execution_id"),  # execution migration
    state_file=runtime.path, for_continuation=True)

# Fresh server reads reconciled with the intended continuation, including the
# canonical permits:check authorization. `client` is the managed worker's API
# client (duck-typed .task() / .request()).
verdict = vr.canonical_recovery_evidence(
    client, project_id=project_id, agent_id=agent_id, run_id=run_id,
    task_id=task_id, execution_id=execution_id,
    scope_ids=task.get("scope_ids") or ())
vr.require_allowed(verdict)   # raises RecoveryRefused(reasons) otherwise
```

`validate_private_state` seals the outcome vocabulary: only
`paused`/`cancelled`/`failed`/`idle` are eligible for continuation;
`running`/`unknown` hold (`outcome_uncertain`); anything else or a missing
outcome on a known session is `outcome_malformed`; `completed` for the same
task/phase is `already_settled` and may never be replayed. A non-empty state
missing run/task/execution ids is `identity_incomplete`. When the worker carries
`source_execution_id` (parent-provided migration metadata), a state bound to that
source is accepted as `source_migrated` instead of demanding the new execution
match the old local state. The settled-turn checkpoint file is validated as a
bounded private file and bound to the state's run/task/execution ids.

`canonical_recovery_evidence` reads the task, run, claims, and permit fresh and
checks: complete identity, project binding on task and run (`project_mismatch`),
agent binding on run and claims (`agent_mismatch`), task not terminal, execution
id/run binding, checkpoint evidence availability, run live and not busy
(`run_not_live`, `run_busy`), claims matching this exact task (`task_id` unset or
equal — another task's claim on the same run never counts) with integer
`ownership_epoch` (`claim_malformed`), active unexpired exclusive-owner scope
coverage (`claim_missing` / `claim_expired` / `claim_scope_mismatch` /
`grant_revoked`), the server-side `permits:check` authorization
(`permit_refused`; a missing permit surface is `permit_source_unavailable`,
never a silent pass), and accepted-handoff freshness (`handoff_stale`).

### Workspace-bound session load (Factory runtime `_open`)

```python
# runtime_config is the same sdk.Runtime the runtime already builds (including a
# connected sandbox transport). Spawns the vendor process with cwd=workspace.
client = await vr.open_workspace_client(runtime_config, workspace=self.workspace)

session = await vr.resume_session_workspace_bound(
    client, session_id=self.state["session_id"], workspace=self.workspace,
    expected_model=self.options.get("model"),
    restrict_tools=self.tool_names, state_file=self.path,
    intent_binding={"run_id": ..., "task_id": ..., "execution_id": ...},
    options_fingerprint=vr.safe_fingerprint(self.options))
# session is a real droid_sdk.Session, OPEN, bound to the loaded client with the
# governed config (auto-reject permissions, builtin skills disabled, tools held).
```

Load fences (`assess_load_result`) require, from the server's own response:

- at least one explicit `cwd`/`worktree.path`, and *all* reported paths agreeing
  and resolving exactly to the managed workspace (`workspace_not_proven` /
  `workspace_mismatch`);
- any session-id echo matching the requested id, and the connected client's
  session id echoing it too (`session_id_mismatch`);
- `is_agent_loop_in_progress is False` and no pending permission, ask-user, or
  queued messages (`model_turn_uncertain` / `pending_effects_unresolved`);
- a non-delegated, non-orchestrator session (`delegated_session`,
  `session_role_mismatch`);
- settings present with a real model id and a known, non-elevated autonomy
  (`auth_context_missing`, `model_mismatch`, `grant_scope_changed`); an unpinned
  `expected_model` is refused unless the caller explicitly passes
  `allow_unpinned_model=True`.

If a recovery intent is already pending, the load refuses with `intent_pending`
unless the caller explicitly reconciles it via `resume_intent_id` with matching
session/run/task/execution identity. On success the intent is cleared by
compare-then-delete; on any refusal or unknown error it survives for the next
reconciliation pass.

### oh-my-pi validation

```python
verdict = vr.validate_omp_resume(runtime.state, state_response,
                                 sessions_dir=sessions_dir)
```

Requires the resumed session id to match, `isSettled is True`, no active stream,
and the saved session file to live inside the private sessions directory. A match
proves only that the vendor process holds that session quiescent — it asserts
nothing about canonical effects, which remain the job of
`canonical_recovery_evidence`.

## Fail-closed limitations

- `clear_recovery_intent` is compare-then-delete; the read-compare-unlink window
  is narrow but not a kernel-level CAS. Concurrent helpers are still safe because
  creation is exclusive and deletion requires the exact owner id.
- The linked-file guard detects symlinks and Windows reparse points; hard links
  to foreign content are indistinguishable on the same volume, so the private
  state directory must stay on a trusted volume (already required of the runtime
  state file).
- The restrict-tools verification reload relies on the server echoing
  `restrict_tool_ids` in settings; a server that accepts the write but omits the
  echo is treated as a false fence (`grant_scope_changed`), not trusted.
- `SessionNotFoundError` detection is by exception class name so SDK doubles and
  the real SDK both work; a renamed exception surfaces as an unknown error and
  simply leaves the durable intent in place (still fail closed).

## Wiring (integrated)

- `VendorRuntime.configure_recovery_authority(callable)` installs the fresh
  canonical gate. Both connected runtimes invoke it **after** session
  load/restore verification and **immediately before every prompt**
  (`FactoryRuntime.execute` opens first, then checks; `OhMyPiRuntime.execute`
  checks between `_open` and the prompt send). A connected runtime can never
  skip the gate before another turn; refusal means no prompt is sent.
- `VendorRuntime._prepare` validates the prior private state and its
  settled-turn checkpoint via `validate_private_state(..., for_continuation=True,
  allow_task_change=True, source_execution_id=context["source_execution_id"])`
  **before** the new attempt overwrites `outcome`/`checkpoint_id`. Unknown,
  malformed, foreign, or cross-run state refuses; a sealed state bound to a
  different task is legitimate session reuse (`task_changed` evidence), while a
  same-task execution change requires the source reconciliation. The initial
  state read is bounded, link-free, and never resolves the file through a
  symlink.
- `AgentWorker._execute` installs the authority per execution
  (`_make_recovery_authority`): fresh `canonical_recovery_evidence` (task, run,
  claims, permit, handoff — bound to the exact project/agent/run/task/execution)
  plus a fresh control/policy-hold check (`control_pending`). The worker also
  carries `source_execution_id` (the pre-recover execution) and
  `source_task_revision` in the context.
- `FactoryRuntime._open` resume branch: spawns the vendor client rooted at the
  managed workspace via `open_workspace_client` (never a global chdir), records
  the durable intent (full session/run/task/execution identity plus hashed
  options fingerprint), then `resume_session_workspace_bound` with
  `expected_model` pinned from options, `restrict_tools` re-asserted and
  verified by a fresh read, and adoption into a governed real
  `droid_sdk.Session` (auto-reject permissions, builtin skills disabled). On
  refusal only the runtime's own client/transport is closed — never
  `close_session`. The workspace equality check remains as defense in depth.
  Fresh sessions keep the existing `Session(cwd=...)` path.
  - Test seam: an SDK double exposes `open_workspace_client(runtime_config,
    workspace=...)`; the installed `droid_sdk` (or None) always uses the real
    bootstrap. Legacy doubles without the connector keep the historical
    `Session.resume` path (tests only); the real SDK can never take it.
- `OhMyPiRuntime._open` resume branch: the saved session file must be a bounded,
  link-free regular file inside the private sessions directory; a durable intent
  is recorded before `--resume` is spawned; `validate_omp_resume` gates the
  fresh `get_state` (`isSettled` missing holds — never invented proof), and the
  intent is cleared only after every fence passes.

## Deferred / not enabled

- Vendor budget helpers remain unsupported and deferred: no code path here
  enables them, and callers requiring hard budgets must keep refusing vendor
  execution.
- Route refusals to the blocked-task surface using `verdict.reasons` /
  `RecoveryRefused.reasons` (parent retains the API/config/SDK/blocked-module
  integration).
