# Worker recovery: interrupted task creation and wedged auxiliary slots

Two evidence-driven recovery wirings on top of the durable journal. Both are
**default-off and explicit**: nothing here runs eagerly on startup, resumes an
uncertain command, invokes a model/runtime/vendor session, or fabricates an ID.
Everything unproven stays exactly as recorded (`unknown`, held for an operator).
See `docs/recovery-automation.md` for the underlying reconciler semantics.

## Interrupted user/canonical task creation

A `POST /v1/tasks` issued through `JournalClient` is durable before dispatch:
the operation record holds the exact payload, the stable command key
(`command_key(operation_id)`, sent as the server `Idempotency-Key`), and the
request hash. A crash or lost response leaves the operation `unknown`.

**Wiring.** `JournalClient.recover_task_creates(enabled=True)` (or an enabled
`RecoveryPolicy`) calls
`recovery_automation.recover_task_creates(journal, client, policy=...)`, which
for every `unknown` `POST tasks` record (bounded by `policy.max_operations`)
invokes the existing `Reconciler.recover_task_create` with:

* `receipts=command_receipts(client, operation_map={"task-create": "tasks"})` —
  the reconciler addresses the ledger with the logical label `task-create`; the
  canonical service (`api.tasks_create` → `command(p, "tasks", ...)`) records the
  command under **`tasks`** (`TASK_CREATE_COMMAND_OPERATION`), so the adapter maps
  the label for the wire query and the echo check;
* `normalize=task_create_normalize` — the service checksum
  (`commands.hash` / receipt `payload_sha256`) is computed over the Pydantic
  request model **with defaults applied** (`TaskCreate.model_dump(by_alias=True,
  exclude_none=True)`), so the durable payload must be normalized identically
  before any hash comparison. Extra keys are forbidden by the model and fail
  normalization, which is ambiguity, never proof.

**Evidence rules** (unchanged from `recover_task_create`):

* A 200 `committed` receipt whose operation/key echo the query, whose
  `payload_sha256` matches the normalized durable payload, and whose
  `resource_id` exactly equals the current projection's `id` proves application;
  the operation completes with exactly the receipt's **current authorized
  projection**. The receipt proves canonical creation only —
  `external_effects_proved` is recorded, never assumed.
* A 404 (missing, hidden, or deleted cached resource) is **not** authoritative
  absence and never proves `not_attempted`; the operation stays `unknown`.
* Redacted (sensitive) payloads, hash conflicts, lookup failures, and any
  non-authoritative response are held, never guessed.
* A `proved_not_attempted` outcome (authoritative absence, or a never-dispatched
  record) reopens the operation to `pending` under the **same stable command
  key**; any retry afterwards is a separate deliberate act.

The whole sweep is serialized under the journal's shared mutex
(`journal.lock`), and every per-operation mutation is preceded by its strict
durable `recovery_audit` write — a failed audit means no mutation. The report
(`{"recovered", "held", "skipped"}`) carries per-operation context
(`operation_id`, `command_key`, `method`, `path`, `attempts`, outcome/reason).

`JournalClient.resume_commands()` is unchanged: it remains an explicit,
caller-driven replay of retry-safe commands under their preserved keys and is
never invoked automatically.

## Wedged auxiliary recovery

`WorkerMessaging.recover_wedged_auxiliaries()` resolves wedged auxiliary
handler operations (one per slot run) from durable proof only. It requires the
existing `recover_message_effects=True` opt-in.

**Start-integrated sequencing (no parent CLI wiring needed).** When the opt-in
is set, `WorkerMessaging.start()` runs the sweep itself via the supervisor's
`before_dispatch` startup-prep seam: after the canonical auxiliary slot runs
and gateways are bound (so receipts, run identity, and the recovery gate are
current) and **before** any producer/consumer thread is launched, so recovery
never races dispatch of the same unknown records. A failing hook aborts the
start before any thread runs (fail-closed); a duplicate/concurrent `start()`
is refused by the supervisor's existing guard, so recovery runs exactly once.
`start()` returns the supervisor status plus `bindings` (current
`auxiliary_run_ids` and per-slot `slot_state_files`) and `recovery` (the sweep
report, or `null` when the opt-in is off). Without the opt-in the hook is
never installed and startup behavior is unchanged.

**Provenance hardening.** A quiescent checkpoint is never accepted as a bare
`dispatched: false` flag: the turn phase resolves only when the item's
journaled task phase completed durably (exact `task_id`/`execution_id`), the
checkpoint comes from *this* slot's configured state file, and the verified
evidence binds exactly that durable run/task/execution (`pre_dispatch` and
`quiescent` both `True`). The finding's evidence is passed as an immutable
mapping; an incidental or mismatched checkpoint file is held, never proof. No
expired lease is ever silently renewed or stolen to manufacture proof, and
without a current receipt/lease the gate refuses and the record stays held.

Per durable `unknown` `HANDLER` record bound to **this** project/agent and one
of **this supervisor's own** auxiliary runs (anything else is held —
cross-slot isolation is preserved and a foreign slot's evidence never reaches
the gate):

* **Task phase** (`runtime-message-task` / `runtime-notification-task`): proved
  only from the journaled `POST tasks` / `tasks/{id}:start` command records
  under the same deterministic keys. A still-`unknown` create record is first
  resolved from authoritative command receipts (same wiring as above). The
  reconstructed `{"task_id", "execution_id", "revision"}` must match the
  **current** canonical task view exactly (a replaced execution/run is
  stale → held). Resolution goes through `Reconciler.resolve_auxiliary` with a
  `durable_result` proof hashed from those durable records.
* **Turn phase** (`runtime-message-turn` / `runtime-notification-turn`): proved
  only by the vendor runtime's own quiescent checkpoint
  (`vendor_quiescence_evidence`) carrying authoritative **pre-dispatch
  non-invocation** (`dispatched is False`) for the exact slot run/task/execution.
  A mere paused/cancelled checkpoint is interrupted-after-dispatch (a real model
  may have acted first) and stays held; a completed-but-unrecorded turn stays
  held (`result_lost` — a result is never invented).
* **Effect phase** (`runtime-message-effect`): delegated to the existing
  message-effect recovery (validated durable decision + canonical proof +
  reply/settle fencing). Notification effects stay held: notifications carry no
  delivery lease to fence.

Every resolution passes the live gate at the moment of mutation:
`WorkerMessaging._auxiliary_gate(item_id)` wraps the supervisor's public
`authorize_recovery_effect`, which freshly verifies the exact project/agent,
the owning slot run (live, still bound to the task's current execution), the
item's unexpired delivery lease held by *that same slot's run*, and the
control fences (pause/steer/shutdown). Anything but an affirm — including a
raised definite refusal — resolves nothing, and the record stays `unknown`.

A bare `verified_result` or operator claim is never authority: `proved_applied`
requires a `durable_result` proof whose hash matches the result, and
`proved_not_attempted` requires `pre_dispatch_non_invocation` quiescent proof.

## Limitations

* Receipt absence is never proof; an operator resolves true 404 ambiguity.
* Notification task/turn phases resolve from the same proofs, but the live gate
  requires a message delivery lease, so notification items stay held under the
  current supervisor gate (definite, safe direction).
* The task phase resolves only when both durable command records exist (the
  executor's client must be journal-wrapped); without them the record is held.
* Recovery never re-invokes a model to regenerate a lost turn result.
