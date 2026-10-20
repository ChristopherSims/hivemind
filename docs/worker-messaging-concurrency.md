# Concurrent worker messaging

`hivemind worker` accepts `--concurrent-messaging` alongside the existing
`--messaging`, sender/type allowlists, and optional reply permission.

```powershell
hivemind worker --enable --adapter hivemind.vendor_runtimes:factory_runtime `
  --config agent-private.json --workspace agent-workspace --journal worker.sqlite `
  --messaging --message-sender agent-peer --message-replies --concurrent-messaging
```

These flags do not enable paid inference. The runtime's existing execution and
model-call opt-ins are still required. Credentials, journals, and runtime state
must remain outside the agent workspace.

The CLI uses bounded auxiliary messaging slots and real canonical runs for the
same agent. Message tasks have no file scopes and the private messaging
runtime has no tools. The primary task/execution is neither replaced nor borrowed.
Message handling uses a dedicated state file, the worker's existing handler
journal, deterministic replies, and the supervisor's control/effect fences.
`--message-queue-capacity` bounds queued work per slot (1–256, default 16).

## Multiple auxiliary slots

`WorkerMessaging(..., slots=N)` / `AssignedRunMessagingSupervisor(max_in_flight=N)`
(1–8) run **genuinely independent** slots. Every slot binds its own:

- canonical auxiliary run (deterministic creation key per slot index; restart
  replays to the same runs; `auxiliary_run_ids` lists them, `auxiliary_run_id`
  remains slot 0). Explicitly reused runs are validated against identity,
  liveness, and **active controls** — resuming over a queued/applying pause,
  steer, or cancel is refused rather than silently overriding the operator
  fence, and all slot runs must be distinct,
- private vendor runtime and private state file (`WorkerMessaging` derives
  `-slot<N>` state-file paths; slot 0 keeps the exact configured path),
- dispatch queue, consumer thread, and delivery lease lifecycle: a delivery's
  claim, renew, ack, and fail always name the run of the slot that claimed it —
  never slot 0's run.

A prebuilt single runtime is rejected for more than one slot unless passed as an
explicit `{slot_index: runtime}` mapping validated to hold distinct instances.
A `runtime_factory` is invoked once per slot (or accepts a sequence of per-slot
factories); if it ever returns the same instance twice, materialization fails
definitely — no mutable vendor session is ever shared. Dispatch claims for the
least-loaded slot with queue room, so slots actually run concurrently.

`authorize_recovery_effect(phase, context)` accepts any of the supervisor's own
auxiliary runs, but the item's exact unexpired delivery lease must be held by
*that same* run: one slot's lease can never authorize another slot's recovery.
`WorkerMessaging`'s recovery callback derives the owning run from the item's
journaled task execution (not from a shared slot-0 default) and holds the item
when the binding is not provable.

## Push-driven dispatch (opt-in)

`hivemind.message_push` provides an authenticated, agent-targeted SSE signal
stream plus a service-owned `MessagePushHub` with a durable, per-agent bounded
replay log (its own SQLite file, never the canonical store):

- `install(app, service, principal, command, envelope, *, policy=MessagePushPolicy(enabled=True), hub=None, path=None, bridge=True)`
  registers `GET /v1/agents/{agent_id}/message-stream?project_id&after`,
  attaches the hub as `app.state.message_push_hub` and
  `service.message_push_hub`, and — when the store supports
  `subscribe_commits` — automatically binds the commit bridge
  (`app.state.message_push_bridge`). Default-off is safe: without an enabled
  policy the route exists but returns `feature_disabled` and no hub, thread, or
  log file is created.
- Publication is post-commit only and normally automatic: the bridge's store
  callback receives only `{project: seq}` and performs a nonblocking wake (never
  SQLite under the commit lock). Off-lock, the bridge scans authoritative
  committed message/notification rows under short snapshots, extracts identity
  and recipient only (bodies are never retained), and publishes with the durable
  per-project watermark advanced in the same transaction; a
  `(agent, kind, item, source seq)` uniqueness constraint deduplicates
  republication, a polling fallback covers lost wakes, and startup catch-up
  rescans every watermarked project — so a crash between commit and publish is
  repaired, and rolled-back transactions never produce signals. A parent may
  instead call `bind_commit_source(store, hub)` explicitly (returns a
  `close()`/`unsubscribe()` handle) with `bridge=False`, or publish individual
  records via the explicit `hub.notify_post_commit(...)` hook.
- Signals carry identity only (`{id, kind}` plus a **per-agent** cursor, so a
  cursor never reveals other agents' traffic). Authorization is continuous, not
  connect-once: the route is marked `hivemind_detached_work` (the middleware
  skips its global request/store locks for it) and the generator re-verifies —
  under its own short `store.lock` snapshots, before every signal, replay frame,
  and heartbeat — that the principal still exists, matches the project, and
  still represents the exact target agent, that the agent still exists in that
  project, and that each item is still canonically visible to that agent.
  Hidden/deleted/foreign items are dropped silently (never emitted, never
  counted per agent); a revoked credential closes the stream at the next
  heartbeat. Subscriber admission past the per-agent bound fails with 429 before
  response headers.
- Delivery is at-least-once (never exactly-once): reconnects pass
  `after=<cursor>` or `Last-Event-ID` and replay the durable per-agent window;
  a cursor outside the retained window yields an explicit `resync` advisory, a
  full subscriber queue yields a `resync` backpressure advisory, stream
  lifetimes are bounded (`max_stream_seconds`), and `hub.close()` terminates
  streams honestly (`stats()` stays safe after close).
- Workers pass `push_signal=PushWakeSource(...).event` to
  `WorkerMessaging`/`AssignedRunMessagingSupervisor`. The wake source keeps its
  cursor in a private, atomic, identity-bound cursor file; every
  (re)subscription — even an empty stream — wakes a canonical reconcile, and
  `close()` interrupts an in-flight subscription read within its bounded read
  window. The supervisor reconciles at startup, on every wake, and at least
  every `max_reconcile_seconds` (bounded, default 60) regardless of push, while
  heartbeats, lease renewals, and control fencing stay on the timer — a lost
  publication degrades to bounded polling, never to silence.
  `httpx_sse_subscription(client, path, cancel=..., read_timeout=...)` is the
  bounded cancellable HTTP helper for building the source; `parse_sse` enforces
  line/frame byte caps and frame-count caps and never echoes remote bytes into
  errors.

`hivemind.message_push.OPERATION_DESCRIPTORS` / `descriptors()` and
`hivemind.worker_messaging.describe()` return self-describing operation
metadata (purpose/input/output/authority/side effects/limits/retry safety) for
parent discovery and MCP registration.

Suggested CLI additions (parent owns `cli.py`): `--message-slots N` (1–8,
default 1, requires `--concurrent-messaging`) and `--message-push` (requires
`--concurrent-messaging`; builds a `PushWakeSource` against the stream
endpoint with a private cursor file beside the journal and passes its event as
`push_signal`, plus `--message-reconcile-seconds` mapping to
`max_reconcile_seconds`).

`WorkerMessaging` owns the auxiliary runtime lifecycle. An `ExitStack` closes it
before the journal context exits, including on worker failure or interruption.
Shutdown stops dispatch, blocks later effects, waits within a deadline, and requests
bounded cancellation/cleanup from owned vendor runtimes when needed. A thread or
vendor cleanup that cannot be confirmed raises `UnknownOutcome`, not a successful
shutdown claim. It never posts a synthetic verdict or replays an uncertain model.

The primary worker delegates message/notification polling to the supervisor while
retaining task execution, heartbeat, claims, and controls. Existing non-concurrent
messaging behavior is unchanged when the flag is absent.

`--recover-message-effects` additionally opts into evidence-driven recovery on
an unknown message effect. It requires concurrent messaging and the current
sender allowlist. Recovery uses the shared durable validated decision, proves the
exact canonical reply envelope, and checks fresh auxiliary/primary control fences
before reply or settlement. It never invokes a model or acknowledges a receipt.
Missing decisions, stale execution bindings, lost leases, or ambiguous evidence
remain held. A later ordinary redelivery acknowledges the recovered operation
without another turn. Recovery counts appear in the owner's cleanup report.

Parent wiring tests use real local canonical tasks/runs and offline runtime
substitutes. They prove the assigned primary execution remains unchanged while a
correlated auxiliary reply completes, command opt-ins fail before opening a journal,
per-slot bindings stay exact under concurrency and lease theft, reused runs with
active controls are refused, push wakes dispatch, a silent push falls back to the
bounded canonical reconcile, push signals during a closed fence are reconciled on
reopen, stream authorization is continuous (revocation and deletion mid-stream are
enforced), replay truncation and backpressure are explicit, the commit bridge
ignores rolled-back writes and catches up after a crash, and cleanup completes
before the journal closes. They are not paid-live vendor concurrency evidence.
