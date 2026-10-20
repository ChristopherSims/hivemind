# Evidence-driven recovery automation

`hivemind/recovery_automation.py` is a bounded, opt-in reconciler on top of the durable
journal (`hivemind/recovery.py`). It reduces the manual reconciliation burden for the
three cases that previously required an operator to inspect state by hand: unknown
effects after a crash, interrupted vendor turns, and a reply that was sent while the
canonical task was never settled.

It **never** blindly replays a model call or a non-idempotent effect, never invents a
result, and never treats a successful generic GET or the absence of an error as proof.
Everything is off unless a `RecoveryPolicy` explicitly enables it, and the existing
operator paths (`resolve_unknown` / `resolve_operation`) remain available for anything
that stays ambiguous.

## Classifications

Every operation is classified into exactly one of four buckets:

| Classification | Meaning | Automated action |
| --- | --- | --- |
| `proved_applied` | A durable record or an evidence probe proves the exact effect landed. | May resolve to `completed` with the *verified* canonical result (never an invented one). |
| `proved_not_attempted` | The durable record proves nothing was ever dispatched (`attempts == 0`), or an explicit trusted operation-specific probe proves non-application. A definite 4xx after an attempt is **not** sufficient: a native/external effect (e.g. a Git write) can precede a fenced canonical failure. | May reopen to `pending`; any retry is a separate deliberate act under the preserved command key. |
| `ambiguous` | Still unknown. The default when evidence is missing, unreadable, or conflicting. | None. Stays `unknown` for an operator. |
| `stale_replaced` | The canonical record moved to a newer execution/revision. | None. The old attempt is never retried. |

Every classification, resolution and stale/aborted recovery appends a durable record
to the journal's `recovery_audit` table (`Journal.record_recovery_audit` /
`Journal.recovery_audits`), so automated recovery is itself auditable after a restart.
Audit evidence carries operation IDs, canonical IDs, status codes and hashes only —
never payloads or credentials.

## Policy gates

```python
from hivemind.recovery_automation import RecoveryPolicy, Reconciler

policy = RecoveryPolicy(
    enabled=True,                     # read-only classification + auditing
    allow_idempotent_replay=False,    # re-issue journaled retryable commands
    allow_reply_replay=False,         # re-post a proved-missing deterministic reply
    allow_quiescence_resolution=False,# resolve interrupted vendor turns from proof
    max_operations=50)                # hard bound per reconcile() pass
gate = Reconciler(journal, client, project_id=..., agent_id=..., run_id=...,
                  policy=policy)
```

## What the reconciler does

* `classify(operation_id, probe=None)` — durable record first, then an optional
  evidence probe `probe(record, client) -> {"outcome", "evidence", "result"?}`.
  A probe that raises or returns an unrecognized bucket leaves the operation
  `ambiguous`. Classification never mutates the operation record.
* `resolve(operation_id, finding)` — applies only *proved* classifications, and only
  when the finding identifies this exact operation and the record is still `unknown`.
  `proved_applied` requires the finding's independently verified result.
* `reconcile(probes=[(match, probe), ...])` — one bounded pass over `unknown`
  operations; resolves proved outcomes, reports `resolved` / `unresolved` / `stale`.
* `retry_operation(operation_id, fence=None)` — re-issues one journaled command with
  its **exact durable payload and preserved command key** (the payload comes from the
  record, never from the caller). Refused unless the operation was recorded retry-safe,
  its exact body was persisted, and the persisted payload still hashes to the recorded
  request hash. A `fence(record, client) -> "current"|"stale"|"unknown"` proof is
  **mandatory for any previously attempted operation** — an omitted fence never
  authorizes replaying one; only a never-dispatched record may retry fence-free.
  `stale` is audited as `stale_replaced`.

## Post-reply settlement recovery

`recover_message_effect(message, *, notification=False, effect_gate=None)` finishes the
canonical effect phase of one handled message/notification when the turn record already
holds a completed, validated decision:

1. Re-derives the executor's (task, turn, effect) operation IDs from the immutable
   message ID and takes the same per-operation OS exclusion lock as the executor.
2. Refuses unless the durable turn decision is completed and well-formed — an operator
   -reconciled or malformed decision carries no payload to settle and stays unknown.
3. Re-reads the canonical task and requires the **exact execution/run binding to match
   always — including for already-terminal tasks**, so a replaced-then-completed
   execution can never launder a late old reply or a false settlement. The task's
   project must match, and the canonical run is re-read: a foreign or terminal run
   stops recovery as `stale_replaced` (the effect record stays `unknown`).
4. If the decision owes a reply: the deterministic reply ID is queried canonically and
   the **full envelope is validated** (ID, `in_reply_to`, sender, recipient, thread,
   and text) — a bare ID match is not enough. Present and exactly matching → no
   re-post. Canonically absent (404) → re-posts the exact durable text under the
   deterministic message ID and preserved key, only with `allow_reply_replay=True`.
   Conflicting evidence → ambiguous, nothing is posted.
5. Settles the task under the server-side `expected_revision` / `execution_id` fence
   with the deterministic checkpoint ID/key; an already-terminal task under the exact
   binding is a no-op.

`effect_gate` is an optional callback `gate(phase, context) -> True` invoked
immediately before the reply re-post (`phase="reply"`) and before the settle
checkpoint (`phase="settle"`), so a concurrent messaging primary's
pause/cancel/steer/shutdown stays fenced during recovery. Anything but `True` is a
definite refusal: recovery stops, leaving the record `pending` when this run applied
no canonical effect yet, or honestly `unknown` when one already landed.

Auditing is mandatory for mutation: a strict durable audit write precedes every reply
re-post, every settle checkpoint, and the final completion — if the audit write fails,
the mutation does not happen. No runtime, model, or vendor session is ever invoked:
the decision comes from the durable turn record. Recovery is restart- and race-safe —
repeated runs and fresh journal instances are recorded no-ops.

## Vendor interruption evidence

`vendor_quiescence_evidence(state_path, run_id=..., task_id=..., execution_id=...)`
reads only the vendor runtime's own state file and quiescent checkpoint
(`VendorRuntime` writes the checkpoint only after an *observed* settled turn) and keeps
**"the vendor stopped" strictly separate from "the vendor did nothing"**:

* a quiescent `paused`/`cancelled` checkpoint alone → `ambiguous`
  (`interrupted_after_dispatch`): a real model can write a file or fire a tool effect
  and only then be interrupted, so quiescence is never replay permission;
* only an authoritative **pre-dispatch non-invocation** proof (`dispatched: false` in
  the checkpoint, bound to the exact current identity) → `proved_not_attempted`:
  the prompt never reached the model, so zero effect is a construction fact, not an
  inference;
* a `completed`/`failed` checkpoint → `ambiguous` (`result_lost`): only a result hash
  survives, the validated contract is unrecoverable, and a result is never invented;
* missing/non-quiescent/mismatched-identity evidence → `ambiguous`.

`resolve_interrupted_turn(operation_id, evidence)` additionally verifies the exact
binding before resolving: the journal record must still be `unknown` and
non-attempting, its durable body (project/agent/run) must match this reconciler's
identity, and the evidence run/task/execution must match the bound run. Only then, and
only under `allow_quiescence_resolution=True`, is the operation reopened to `pending`;
any retry afterwards is a deliberate decision for the caller. Nothing here resumes a
vendor session or weakens the Factory live SDK 0.5.0 fail-closed workspace-mismatch
guard.

## Worker integration and API

The concurrent worker's `--recover-message-effects` flag now wires this reconciler
through `WorkerMessaging.on_unknown`. It requires current sender authorization,
a completed validated durable turn, exact canonical reply evidence, and fresh
primary/auxiliary task/execution/control/current-delivery-lease checks at each new
effect. It never invokes a runtime or acknowledges a receipt. Refusals remain held;
ordinary later redelivery performs the acknowledgement.

For other trusted integrations:

* After `UnknownOutcome` from `VendorMessagingExecutor.handle_message`, construct
  `Reconciler(handler_journal.journal, client, project_id=..., agent_id=...,
  run_id=...)` and call `classify`/`recover_message_effect` with the redelivered,
  already-authorized message mapping. The IDs, lock paths, reply ID and checkpoint
  identity are byte-identical to the executor's derivations (pinned by
  `test_identities_match_the_executor_derivation`).
* To remove the duplicated derivations, `runtime_messaging` could export
  `reply_message_id()` and the checkpoint-identity helper; `recovery_automation` would
  then import them.
* `Journal.recover()` already flips interrupted `attempting` records to `unknown` on
  open; the reconciler consumes exactly those records.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_recovery_automation.py -q
```

Covers: classification buckets and audit durability across restart; attempted-then-4xx
staying ambiguous without a trusted probe; probe-proved applied/not-attempted
resolution; probe errors and conflicting evidence staying ambiguous; refusal to invent
results; finding/operation binding mismatches refused; exact-payload/preserved-key
retries; fence-free replay of a previously attempted operation refused; tampered
durable payload hash refused; non-retryable and unpersisted-payload refusals;
changed-payload conflict after a crash; a real killed child process leaving a
resolvable `unknown`; reply-sent/task-unsettled recovery with no model call and no
second reply; proved-missing reply re-posted once under its deterministic ID; opt-in
enforcement; stale execution never settled; terminal task with a replaced execution
never settled and never re-replied; terminal run stopping recovery; cross-project task
refusal; effect-gate fencing of the reply and settle phases (definite refusal before
any effect, honest `unknown` after one); unknown turns never recovered; foreign/
cross-project refusal; notification settlement with no reply channel; handled=false
settling failed; executor-identity pinning; bounded reconcile passes; vendor quiescence
buckets including adversarial write-then-pause and foreign-binding proofs; and reply
evidence against the real in-process service.

## Limits

* Classification without a registered probe cannot prove anything about an `unknown`
  operation — it stays `ambiguous` by design.
* The task-creation phase of an interrupted handling is not auto-recreated (the
  created task ID may be unrecoverable locally); that remains an operator path.
* A probe's `proved_not_attempted` from a canonical 404 assumes the server never
  deletes messages; if that invariant changes, replies proved absent must be treated
  as ambiguous instead.
* Single-owner journal and local durability limits of `recovery.py` apply unchanged.

## Interrupted task creation (authoritative command receipts)

`Reconciler.recover_task_create(operation_id, receipts=...)` resolves an interrupted
canonical `POST tasks` strictly from evidence:

* The durable record must be a task-create with its exact payload persisted and still
  hashing to the recorded request hash; a redacted payload can never be hash-bound and
  stays ambiguous.
* `receipts(operation, key, payload_sha256)` is the parent-injected lookup against the
  **authoritative server-side command ledger** (the `commands` table the canonical
  `mutate` path writes). Only an *authoritative* answer decides: a found receipt whose
  payload hash matches the durable payload completes the operation with exactly the
  recorded canonical response (IDs are never fabricated); an authoritative absence
  proves the mutate path never committed and reopens the operation to `pending` for a
  deliberate retry under the preserved command key.
* A generic 4xx, quiescence, or a missing task in a listing is NEVER proof of no
  effect: without authoritative receipts the operation stays `unknown` (a found
  receipt still completes a 4xx-pending record, because the ledger proves the commit).
* Every mutation is preceded by a strict durable `recovery_audit` write.

**Wiring need (parent-owned):** an authenticated, principal-scoped
`GET /commands/{operation}/{key}` endpoint returning the recorded payload hash and
response, or an explicit authoritative-absence 404 body. The exact contract is
`recovery_automation.COMMAND_RECEIPTS_WIRING_NEED` and is also reported by
`RecoveryAutomationHandle.describe()`. Until it exists, attempted task-create
recovery stays `unknown` by design.

## Wedged auxiliary tasks

`Reconciler.resolve_auxiliary(operation_id, finding, gate=...)` resolves a wedged
auxiliary (reviewer/worker sub-task) invocation only when the finding is proved
(`proved_applied` with its independently verified result, or `proved_not_attempted`)
AND exactly bound to the reconciler's project/agent/run plus the durable body's
`task_id`/`execution_id`, AND the optional `gate("resolve", context)` confirms the
current identity/controls/lease are still held. Ambiguous or stale findings remain a
hold; a gate refusal changes nothing; a foreign binding never resolves.

## Public handle

`recovery_automation.install(app, service, principal, command, envelope, *, journal,
client, project_id, agent_id, run_id, policy)` returns a `RecoveryAutomationHandle`
(the parent owns registry/config/CLI/endpoint wiring; the wiring arguments are
recorded as self-describing context only):

* `proposals()` — typed `RecoveryProposal` values for every currently-unknown
  operation (bounded, audited), with a self-describing `action` (`recover_task_create`
  / `resolve` / `hold`), `requires` wiring/policy needs, and evidence summary;
* `status()` — typed `RecoveryStatus` counts by durable state plus the active policy
  flags;
* `tick(probes=..., limit=...)` — one bounded reconcile pass plus a status snapshot;
  never invokes a model, runtime, or vendor session;
* `describe()` — `RECOVERY_ACTIONS` descriptors (purpose / input / output / authority
  / side effects / limits / retry safety) plus unmet wiring needs.

Suggested parent CLI: `recovery proposals|status|tick` (read-only classification) and
`recovery recover-task-create <operation_id>` behind the authenticated
command-receipts endpoint; both should require an explicit `--yes`-style flag for any
state transition.

## Additional limits

* Command receipts prove the canonical mutate path only; a native side effect outside
  the ledger still requires an operator.
* `resolve_auxiliary` never re-invokes the auxiliary task; a reopened record is a
  deliberate retry decision for the caller.

## Corrections (current contract)

* **Command receipts adapter**: `command_receipts(client)` adapts the parent's
  `GET /v1/commands/receipt?operation=...&key=...` endpoint (own-principal only).
  A 200 `{state: "committed", payload_sha256, response_sha256, resource_id, resource,
  current, external_effects_proved: false}` completes the task-create with exactly the
  CURRENT authorized projection, whose `id` must equal `resource_id`. **A 404 is NOT
  authoritative absence** (the cached resource may be hidden or deleted) — it maps to
  non-authoritative and the operation stays `unknown`; non-application is never
  inferred from a 404, a generic 4xx, or quiescence. The receipt proves canonical
  creation only, never external/model effects.
* **Payload normalization**: the server checksums the canonicalized request model
  (Pydantic defaults applied). Pass `normalize=` to `recover_task_create` — the parent
  wires `lambda p: TaskCreate(**p).model_dump()`; the receipt hash is compared against
  `digest(normalize(payload))`. A failing binding is ambiguity, never a guess.
* **Auxiliary resolution authority**: `resolve_auxiliary` now REQUIRES (a) durable
  fenced proof — `evidence["proof"]` of kind `command_receipt` (exact `resource_id` +
  `payload_sha256`), `durable_result` (`result_sha256` must equal the digest of the
  finding's verified result), or `pre_dispatch_non_invocation` (quiescent) — an
  operator proposal or a bare caller claim is never proof; and (b) a mandatory
  current identity/controls/delivery-lease `gate` consulted immediately before the
  mutation.
* **Detached, default-off wiring**: all recovery actions are explicit two-phase long
  handlers (`hivemind_detached_work=True`, `default_enabled=False`,
  `trusted_paths_only=True`) exported as `OPERATION_DESCRIPTORS`; they must never run
  inside a `Service.mutate` transaction or a whole-process request path. The parent
  wires the offline CLI surfaces.
