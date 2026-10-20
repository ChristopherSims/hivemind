# Runtime messaging

`hivemind/runtime_messaging.py` is the opt-in bridge between the worker's poll loop
(`hivemind/agent_worker.py`) and a runtime adapter (the vendor runtimes in
`hivemind/vendor_runtimes.py`). It authorizes inbound messages and notifications, dedups
handling durably by stable ID, distinguishes confirmed from unknown effects, and offers
bounded tracked-conversation workflows. It performs no vendor session, paid model call,
or network egress of its own; adapters decide what handling means under their own
execution opt-ins.

## Pieces

- **`MessagingPolicy`** — deny-by-default authorization. Every dimension needs an
  explicit opt-in:
  - `enabled`: master switch. When off, `message_handler`/`notification_handler` are
    `None`, so a wired worker leaves mail and notices untouched for a real processor.
  - `allowed_senders`: the only agents whose messages may be handled.
  - `allowed_notification_types`: the only notification types (e.g. `board_mention`,
    `memory_review`) that may be reviewed.
  - `allow_replies`: without it, `reply` and tracked answers raise.
  - `max_reply_chars`: hard-bounded at 16384; reply text is truncated to it.
- **`RuntimeMessageGateway`** — per agent/run entry points:
  - `handle_message(receipt)`: resolves the full message (a claim receipt already
    carries it), enforces private recipient visibility (`to` must be this agent, project
    must match) and sender authorization, dedups by the immutable message ID through
    `HandlerJournal`, then invokes `adapter.handle_message(receipt, message)`.
  - `review_notification(notice)`: same shape for `adapter.review_notification(notice)`.
  - `reply(source, text)`: answers the original sender only, reusing the source thread,
    correlation, and `in_reply_to`, under a deterministic message ID
    (`msg-` + derived hash) so a retried reply replays the same server-side command
    instead of duplicating.
- **`ConversationBridge`** — bounded tracked-request workflows built on
  `conversations.ConversationWorkflow` with injected `sleep`/`clock`:
  - `open_request(to, text, ...)`: deadline capped at `max_wait_seconds`.
  - `answer(request_id, text=None)`: only the addressee may answer, `allow_replies` is
    required, and a closed request returns `answered: False` instead of raising, so
    reconcile/answer races stay idempotent. Without text, the adapter's pure
    `compose_reply(request_view)` hook is consulted; a hook returning nothing records no
    answer.
  - `await_answer(request_id, ...)`: bounded reconcile-poll; never exceeds
    `max_wait_seconds` or `max_polls`.
  - `escalate_overdue(request_id)`: escalates only when the recorded deadline passed;
    the one-time guarantee is enforced both here and by the server.

## Confirmed vs unknown effects

The gateway never acks or fails a delivery itself; it only produces three outcomes:

| Outcome | Signal | Delivery effect (owned by `sdk.deliver_inbox` / the worker) |
| --- | --- | --- |
| Confirmed | returns a mapping | safe to ack |
| Definite rejection (unauthorized sender/recipient/type, disabled) | raises `MessagingError` before any adapter effect | safe to fail; the effect provably never ran |
| Unknown | raises `UnknownOutcome` (adapter raised after a possible effect, or the journal recorded a crash) | **no ack, no fail**; the lease expires and redelivery is blocked by the durable handler record until an operator reconciles it with `Journal.resolve_unknown` |

With a `HandlerJournal` present, a completed handling is deduplicated across redeliveries
and process restarts: the adapter is invoked once per stable message/notification ID.
Without a journal the gateway still refuses to confirm uncertain outcomes (any adapter
exception becomes `UnknownOutcome`) but cannot dedup, so production wiring should always
pass the worker's handler journal.

## Adapter protocol

Adapters are injectable and duck-typed (`RuntimeMessagingAdapter` documents the
contract). Any subset of hooks may be implemented; the gateway checks callability per
hook and registers no handler when a hook is absent:

```python
class MyAdapter:
    def handle_message(self, receipt, message):
        ...  # apply the effect; return a mapping or None; raise if uncertain

    def review_notification(self, notice):
        ...  # same contract

    def compose_reply(self, request_view):
        ...  # pure: return reply text or None; no effects
```

Adapter rules: never ack/fail deliveries, never persist raw vendor transcripts or message
bodies beyond the journal's records, and never start a vendor model session unless the
adapter's own `enabled` + `allow_model_calls` opt-ins already permit execution.

## VendorMessagingExecutor: real runtime-backed handling

`VendorMessagingExecutor` processes messages through the existing
`runtime.execute(task, context, checkpoint)` surface — no new vendor protocol.

- **Canonical task authority.** Each message becomes a real task: created via
  `POST /v1/tasks` (empty `scope_ids` — a no-tools runtime needs no file scopes),
  started via `tasks/{id}:start` under the executor's real run, and settled via
  `tasks/{id}:checkpoint` **only after** the confirmed model decision and reply. Task and
  execution IDs are always the server-returned records — the same records managed hooks
  look up — never deterministic pseudo-IDs. Create/start use deterministic idempotency
  keys derived from the immutable message ID, so a retry replays the same records.
  The runtime receives a real fenced checkpoint callback (monotonic progress,
  deterministic checkpoint IDs).
- **Durable intent before the model.** Per message, three journaled operations:
  `runtime-message-task` (canonical create/start), `runtime-message-turn` (begin +
  `mark_attempt` **before** the vendor turn, completed with the validated contract — the
  confirmed decision — before any reply), and `runtime-message-effect` (deterministic
  reply, then task settle). A timeout/crash therefore always leaves a resolvable durable
  unknown record, and redelivery — even from a freshly constructed executor — is
  suppressed before reaching the runtime.
- **Lock discipline.** No canonical-DB or handler-journal lock is held during vendor
  execution. A separate per-operation OS `SessionLock` (released by the OS on crash)
  gives concurrent exclusion; an `attempting` record under a held lock therefore always
  means a crashed holder and is converted to a resolvable `unknown` before raising.
- **Strict structured response.** The prompt (`MESSAGE_RESULT_INSTRUCTION`) explicitly
  requests exactly one JSON object `{"handled": bool, "reply_text": str | null}`. The
  runtime must be built with the private opt-in `result_contract='message'` (wired by the
  parent in `vendor_runtimes.py`) so `execute` returns the validated contract in
  `result['message_result']`. Default hash-only or malformed contracts yield
  `UnknownOutcome` — never a confirmed ack. A confirmed `handled: false` (or a failed
  turn) is journaled as a durable decision and re-raised as `MessageNotHandled`:
  definite, safe to fail, never acked, never re-executed; the canonical task settles
  `failed`.
- **Read-only by default.** A runtime exposing tools outside a read-only set is refused
  unless `allow_tool_mutation=True`. The runtime/factory is created lazily, so
  constructing the executor starts no vendor session.
- **Notifications (opt-in per type).** `allowed_notification_types` explicitly lists the
  notification types (e.g. `board_mention`) the executor may review. While empty, the
  instance's `review_notification` attribute stays `None`, so the gateway's
  `notification_handler` is `None` and notices remain unhandled and unacked. When
  allowed, `review_notification` runs the **same** canonical task/turn/settle flow
  (journal kinds `runtime-notification-{task,turn,effect}`, phase
  `runtime-notification-handling`), bound to the recipient agent and project. The prompt
  (`NOTIFICATION_RESULT_INSTRUCTION`) requires `reply_text: null`: notifications carry no
  reply channel, a contract with one is invalid and stays unknown, and no sender is ever
  impersonated. A confirmed `handled: false` raises `MessageNotHandled` — a definite
  refusal that leaves the notice unacked; the notification is never acked by the executor
  (acking stays with the worker/operator). Review grants no action authority.
- **Self-managed durability.** The executor sets `manages_durability = True`; the gateway
  authorizes and delegates without a second journal lock.

Construction (all explicit, deny-by-default):

```python
executor = VendorMessagingExecutor(
    client, project_id, agent_id, run_id,
    runtime_factory=lambda: FactoryRuntime({**options,
        "state_file": str(private_dir / "vendor-messaging.json"),
        "result_contract": "message",       # parent-owned VendorRuntime opt-in
        "enabled": True, "allow_model_calls": True}, sdk=sdk),
    handler_journal=worker.handler_journal,
    allowed_senders=["agent-peer"], allow_replies=True,
    allowed_notification_types=["board_mention"])  # empty = notifications stay unhandled
```

Use a dedicated messaging state file, separate from any task-execution runtime, so the
executor's phase/state identity stays independent. `runtime=` may be passed instead of a
factory for an already-constructed runtime.

## Worker wiring (owned by the agent_worker checkpoint)

Two small patches wire the gateway in (applied by the checkpoint owner, not this module):

```python
# AgentWorker.__init__ gains keyword-only params: messaging_policy=None, messaging_adapter=None
# (messaging_adapter is typically a VendorMessagingExecutor built as shown above; the CLI
# feeds its --notification-type allowlist into both MessagingPolicy.allowed_notification_types
# and VendorMessagingExecutor.allowed_notification_types)
self.messaging = None
if messaging_policy is not None:
    from .runtime_messaging import RuntimeMessageGateway
    self.messaging = RuntimeMessageGateway(
        client, project_id, agent_id, run_id, adapter=messaging_adapter or runtime,
        policy=messaging_policy, handler_journal=self.handler_journal)

# _poll_messages / _poll_notifications resolve the gateway first:
handler = self.messaging.message_handler if self.messaging else None
if handler is None:
    handler = getattr(self.runtime, "handle_message", None) if self.runtime else None
```

Gateway handlers journal internally, so the worker must not wrap them in a second
`HandlerJournal.handle`; legacy runtime handlers keep the existing wrap. Notifications
that raise `MessagingError` are definite rejections: the poll skips them unacked.

## Vendor runtime contract opt-in (owned by the vendor_runtimes checkpoint)

No `handle_message` no-op hooks are added to the vendor runtimes; message handling goes
through the existing `execute` surface. The one parent-owned change is a private result
contract opt-in on `VendorRuntime`:

```python
# VendorRuntime.__init__: accept and validate the private opt-in
self.result_contract = options.get("result_contract")
if self.result_contract not in (None, "message"):
    raise ValueError("Unsupported result contract")

# VendorRuntime._settled: parse and validate the structured contract when opted in;
# the default path stays hash-only and stores nothing new.
def _settled(self, status, result=""):
    ...
    settled = {...existing mapping...}
    if self.result_contract == "message" and status == "completed":
        contract = json.loads(result)  # the turn's JSON object
        if (not isinstance(contract, dict) or not isinstance(contract.get("handled"), bool)
                or (contract.get("reply_text") is not None
                    and not isinstance(contract["reply_text"], str))):
            raise UnknownOutcome("Vendor returned an invalid message contract")
        settled["message_result"] = {"handled": contract["handled"],
                                     "reply_text": contract.get("reply_text")}
    return settled
```

`VendorMessagingExecutor` treats a missing `message_result` as `UnknownOutcome`, so a
runtime without the opt-in can never produce a confirmed ack.

## Assigned-run concurrency: the auxiliary messaging run

`hivemind/messaging_concurrency.py` (`AssignedRunMessagingSupervisor`) handles messages
and allowed notifications **while the agent's primary run is assigned and its runtime is
in flight**, without stealing or replacing the primary's task/execution and without
granting extra scope.

The canonical service fences one task per run (`tasks/{id}:start` rejects
`run_already_assigned`), and message handling creates real canonical tasks — so handling
mail under the assigned run is impossible without pseudo IDs. Instead the supervisor
creates (or reuses) a **real canonical auxiliary run** for the same agent through
`POST /v1/runs` under a deterministic idempotency key
(`auxiliary-messaging-run:<project>:<agent>:<primary>:<slot>`), so a restart replays to
the same server-side run record. The service permits multiple concurrent runs per agent;
`auxiliary_run_id` may also be passed to reuse one explicitly (validated for agent,
project, and liveness).

Structure per supervisor:

- **Exactly one auxiliary slot.** `max_in_flight` must be 1: one auxiliary run, one
  lazily-built private vendor runtime (`runtime_factory` or a prebuilt `runtime`), one
  consumer thread serializing turns — so no vendor session is ever shared with the
  primary task runtime or executes two turns concurrently. The bounded dispatch queue
  (`queue_capacity`, default 16) holds receipts waiting for that single slot; the
  producer claims only when the queue has room (a claim with no room is failed back
  immediately). No hidden model calls: every turn is a journaled
  `VendorMessagingExecutor` phase under a canonical scope-less task.
- **Outstanding lease tracking.** Every claimed receipt is registered in a bounded,
  duplicate-free outstanding map keyed by stable message ID. The producer renews all
  outstanding leases every tick while the fence is open; a definite 4xx renewal failure
  drops the lease (effects gate off), a transient failure flags the receipt until a
  fresh verification succeeds. Before any turn begins and at every effect boundary the
  supervisor **verifies the exact current lease** (`GET messages/{id}` must still show
  state `delivered`, this slot's run, this delivery, an unexpired `claim_until`, and no
  renewal error). A queued receipt whose lease read fails is discarded without a turn —
  never a model call, never an ack, never a fail (`stale` counter); the canonical
  redelivery path owns what happens next.
- **Shared durable dedup.** Pass the primary worker's `recovery.Journal`: handler dedup
  (`HandlerJournal`) and the per-operation OS `SessionLock` exclusion are keyed by
  stable message ID and journal path, so a redelivery handled by either loop is a
  recorded no-op in the other. Reply IDs stay deterministic (`post_reply`).
- **No ack on unknown.** `UnknownOutcome` leaves the receipt leased (never acked, never
  failed), increments `unknown`, invokes the optional `on_unknown(kind, identifier,
  error)` callback, and is listed by `unknown_operations()`. This is the recovery seam:
  an evidence-driven reconciler (the recovery automation) verifies each record against
  canonical state and then calls `journal.resolve_unknown(...)`; the supervisor never
  auto-resolves an uncertain effect.

### Control fencing

- A queued/acknowledged/applying `pause` or `steer` control targeting the primary or the
  auxiliary run **blocks new claims** and **gates effects**.
- **Any** primary or auxiliary run status outside `running`/`idle` also blocks — this
  includes an *applied* pause with no pending control left (`paused` status). A terminal
  (`completed`/`failed`/`cancelled`) or missing primary/auxiliary run **halts** the
  supervisor permanently (`status()["halt_reason"]` reports the cause), as does a
  pending `cancel` control.
- Effect gating uses the executor's `effect_gate` hook, invoked at **every** effect
  boundary with the same signature `(message, turn_contract)`: before the effect phase
  begins (skipped when the effect record already completed, so restart/recovery replays
  are not fenced retroactively), immediately before the reply posts, and immediately
  before the task settles. The gate raises `EffectsBlocked` (a definite
  `MessagingError`) when the fence is closed, the exact current lease is no longer held,
  a renewal failed, or shutdown was requested.
  - A refusal **before any effect ran** reopens the effect record as `pending`
    (never poisoned `unknown`), the receipt is failed back definitely (never acked),
    and redelivery after the fence clears dedups task+turn and runs only the effect
    phase — the vendor turn is never re-run and the reply posts once.
  - A refusal **after the reply posted** is genuinely uncertain (settlement may or may
    not be required): it becomes an honest `UnknownOutcome` — the record is `unknown`
    for the reconciler, the receipt is never acked, and redelivery stays blocked on the
    durable record (the deterministic reply ID prevents a duplicate reply).
  - The gate checks are fresh client-side reads, not atomic server authorization; the
    residual race between the check and the write is bounded by canonical fencing
    (checkpoints/settlements on a paused or controlled run are rejected server-side)
    and by deterministic reply IDs.
- A gated delivery leaves its canonical handling task running under the auxiliary run.
  Because the run fences one task, a *different* message claimed meanwhile is failed
  back definitely (`run_busy` → `temporary_unavailable`) before any effect, unless the
  busy task is its own journaled task (the gated-resume case). Failed receipts burn no
  delivery attempts and redeliver under normal backoff.
- Server-side, scope-less message tasks pass liveness/control permit fences but hold no
  file grants; controls targeting the auxiliary run additionally hard-fence its
  checkpoints (`control_pending`).

### Recovery authorization

`authorize_recovery_effect(phase, context)` is the public gate the recovery automation
calls (e.g. as `recover_message_effect(..., effect_gate=...)`) before replaying a
reply/settle from an unknown journal record. It freshly verifies that the supervisor is
not stopped, the control fence is open, and the context's `run_id` **is** this
supervisor's own auxiliary run, live, and still owning `context["task_id"]` as its
current task, whose execution matches `context["execution_id"]` under that same run.
`context["item_id"]` must hold the exact current unexpired delivery lease, without
renewal errors. An optional `context["operation_id"]`, when its record exists, must
match the supervisor's identity; the reconciler separately validates the exact
durable operation/decision/payload chain. It
returns `True` when authorized and raises `EffectsBlocked` (fenced/stopped) or
`MessagingError` (foreign run/task/execution identity) otherwise. No private state
access is needed by the reconciler.

### Maintenance and shutdown discipline

- Heartbeats, control/run fence polling, and lease renewal (`messages/{id}:renew` for
  every outstanding receipt) run on the producer; the per-delivery maintenance thread
  only heartbeats and refreshes the fence — short HTTP calls only. No journal, queue,
  or worker boundary lock is held through a vendor turn, and the primary worker's tick
  is never touched.
- `stop(timeout=...)` is bounded: the producer stops claiming, the consumer finishes
  (never interrupts) an in-flight turn — its effects are gated off — queued receipts
  are failed back to `available` (no effects ran on them), and the report is honest:
  `stopped: False` with `alive_threads` when a thread outlives the budget.

### Worker wiring

Constructing no supervisor changes nothing (idle-run behavior and the worker's own
polling are untouched). For clean separation, build the worker with
`messaging_delegate=supervisor`: `_poll_messages`/`_poll_notifications` then claim
nothing, leaving all mail to the auxiliary run. Without the delegate both loops may
claim, but the shared handler journal still guarantees one effect per stable message ID.

```python
supervisor = AssignedRunMessagingSupervisor(
    client, project_id, agent_id,
    primary_run_id=worker.run_id, journal=worker_journal.journal, enabled=True,
    runtime_factory=lambda: VendorRuntime({**opts, "state_file": str(private / "msg.json"),
                                           "result_contract": "message"}),
    allowed_senders=["agent-peer"], allowed_notification_types=["board_mention"],
    allow_replies=True, on_unknown=recovery_hook.note_unknown)
supervisor.start()
...
supervisor.stop()   # bounded; queued receipts fail back, in-flight effects gated
```

## Limitations

- Dedup durability requires a `HandlerJournal`; without one, handling is confirmed or
  unknown but not deduplicated. `VendorMessagingExecutor` refuses construction without
  one.
- The executor handles one message per vendor turn, sequentially; a runtime busy with an
  unsettled turn reports `UnknownOutcome` and redelivery retries later.
- A vendor reply over the character bound is truncated before posting; `handled: false`
  decisions dead-letter through normal delivery retry policy.
- Starting the canonical message task requires the executor's run to be unassigned (or
  already assigned to that task); a `run_already_assigned` rejection is definite and the
  delivery fails for a later retry.
- A crash (or a gate refusal) between reply and task settle leaves the effect operation
  unknown; after an operator resolves it, the dedup path does not re-settle the task
  (reconcile the task via the normal scheduling reconcile surface). A gate refusal
  *before* the reply instead leaves the record cleanly `pending` and retryable.
- The supervisor is scoped to one auxiliary slot; recovering from an *applied* pause of
  the auxiliary run requires an operator resume (the canonical service requires an
  active claim to record `resume`), after which blocked work proceeds automatically.
- Effect-gate checks are fresh but not atomic with the write they precede; canonical
  checkpoint fencing and deterministic reply IDs bound that residual race.
- An operator `not_applied` resolution retries the turn under the same canonical task
  (idempotent keys), but the vendor runtime's own state must also be reconciled first
  (the runtime lifecycle surface owns that).
- The executor's runtime contract is validated against the documented
  `{'handled', 'reply_text'}` shape with fakes of the official adapters; no paid live
  vendor validation was performed.
- Notification definite rejections stay unacked (and reappear on later polls) until an
  operator or a future policy decision handles them; only the worker wiring can choose to
  ack them.
- `await_answer` bounds wait time and poll count but cannot force the peer to answer;
  escalation remains the one-time recorded remedy.
- Reply text is plain text only, matching direct-message rules; rich segments are
  rejected by the server and not attempted here.
- Concurrency: a `pause`/`steer` fence does not interrupt an in-flight vendor turn (no
  safe interruption exists); it blocks its reply/settle and all new work. A long pause
  can dead-letter a gated delivery under the sender's retry policy; redelivery after
  resolution still dedups and never re-runs the turn.
- Concurrency: delivery-failure reasons on the wire are the canonical closed enum
  (`handler_failed`/`interrupted`/`temporary_unavailable`); internal causes
  (`shutdown`, `run_busy`, `control_pending`) map onto it and are recorded locally.
- Concurrency: with `max_in_flight > 1`, message leases are claimed under the first
  slot's run while canonical tasks execute per slot; the `run_already_assigned` 409 is
  the durable fallback when a slot is busy, leaving the task phase `pending` for a
  definite redelivery retry.
- Concurrency: the supervisor is a client-side thread set, not an OS sandbox; the
  primary worker must share its journal for cross-loop dedup, and separate journals
  would double-apply on cross-loop redelivery.
