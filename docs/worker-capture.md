# Worker capture and automatic controls

`hivemind/worker_capture.py` wires two opt-in capabilities around an
`hivemind.agent_worker.AgentWorker` without modifying the watcher, the capture
spool, or the safe-pause controller it composes:

1. **`WorkerCaptureLifecycle`** — a real continuous watcher bound to a
   canonical *observer* run's `Capture` spool for one worker's *main* run.
2. **`AutomaticControls`** — a policy-driven automatic safe pause/resume over
   `SafePauseController`, fed by observed-change / capture-gap / drift /
   conflict signals.

Both are default-off. Constructing them performs no requests, opens no spools,
and starts no threads.

## Capture lifecycle

```python
from hivemind.worker_capture import (
    CleanupError, WorkerCaptureLifecycle, capture_config_from_mapping)

config = capture_config_from_mapping({
    "enabled": True,
    "workspace_root": workspace,          # required when enabled
    "state_dir": state_dir,               # required when enabled; 0o700 on POSIX
    "poll_interval_seconds": 2.0,         # 0.1..60
    "flush_interval": 5.0,                # 0.1..60
    "limits": {"max_files": 2000},        # ScanLimits fields, or a ScanLimits
    "exclude": ["*.pem"],                 # extra secret-style patterns (list of str)
    "max_queue": 256,                     # watcher outbox rows per scan before a gap
    "max_events_per_scan": 500,           # diff cap; truncation is a declared gap
})
lifecycle = WorkerCaptureLifecycle(client, project_id, agent_id, run_id, config,
                                   journal=journal)  # journal: duck-typed append/record
lifecycle.start()                          # or `with lifecycle:` (see CleanupError)
...
report = lifecycle.close()                 # ordered shutdown; failures are reported
```

Unknown config keys, wrong types, bare strings where a list of patterns is
expected (a string would explode into characters), and over-long pattern
entries are all rejected. `workspace_root`/`state_dir` are resolved and
rejected when they land on secret or ignored paths.

### Fences, identity, and dedup

- The lifecycle state file (`<state_dir>/<key>-worker-capture.json`, 0o600)
  records `bound_to = {project_id, agent_id, run_id}` and the observer run id.
  Corrupt or foreign state **refuses to load**; it is never silently reset.
- The observer run is created once with the stable idempotency key
  `worker-observer-<digest(project, agent, run)>` (within the service's
  128-character key limit) and then re-read and fenced (`project_id` and
  `agent_id` must match) on every start. A restart dedups; a duplicate observer
  run is never minted.
- The capture spool (`<state_dir>/<key>-observer-spool.sqlite`) belongs to the
  observer run; watcher events land there with stable event ids.

### Startup and shutdown ordering

Startup is stage-wise: journal start-intent, observer run bind (a fence refusal
owns no resources), spool open (flush thread), watcher construct, watcher
start. A failure at any stage closes every resource started before it — a
watcher constructor failure never leaks the capture flush thread or an open
spool — and leaves the lifecycle startable for a retry.

Shutdown (`close()`) is strictly ordered: watcher stop, watcher drain + signal
accounting, spool flush, spool close, journal entry last. Fail-safe rules:

- A watcher thread that fails to stop keeps its dependent spool **open** —
  closing it under a live writer would corrupt delivery accounting. The report
  is `{"closed": False, "shutdown_pending": True, "errors": [...]}` and every
  live resource is retained for a retry.
- The spool is flushed *before* `Capture.close()` is called, because
  `Capture.close()` always closes its database; only a successfully flushed
  spool is closed and released.
- The journal is only appended to, never closed (the parent owns it). The
  final `capture_stop` entry is written only when every resource was released;
  a partial shutdown writes `capture_stop_pending` instead.
- `with lifecycle:` raises `CleanupError` (carrying `.report`/`.errors`) when
  shutdown is pending, so an `ExitStack` never unwinds past live threads. The
  parent contract: on `CleanupError`, keep the journal open and retry
  `close()`.

### Signal ledger (durable, delivery-based)

Accounting runs at *delivery*: an accounting watcher subclass reports every
drained batch — including batches drained by the watcher's own background poll
thread — synchronously after the spool commit, before any flush can remove the
rows. A backstop scan of the outbox+spool union covers the crash window where
rows reached the spool but the callback never ran. The durable cursor advances
only forward over stable watcher event ids (`<source>-<seq>`); a source change
or sequence discontinuity sets an explicit `reset` marker, never a silent
under-count.

```python
signals = lifecycle.pending_signals()  # peek: {"observed_changes": n, "capture_gaps": n, "reset"?}
snapshot = lifecycle.signal_snapshot() # {"signals", "ids", "reset_count"} — a stable token
lifecycle.ack_signals(snapshot)        # consumes EXACTLY the snapshotted ids/reset counter
```

Pending signals survive process restart. They are acknowledged exactly once,
after `AutomaticControls` has durably persisted its intent (its receipt is
present in the tick outcome); the `AgentWorker` seam performs this ack
automatically. The ack is snapshot-exact: a gap or change that arrives between
the policy evaluation and the ack has a higher sequence, is absent from the
token, and survives. Below-threshold ("clear") evaluations are **not** acked,
so a crash never resets safety evidence that no decision consumed.
`ack_signals()` without a token clears the whole ledger and exists for
operator/legacy use only.

**Observed changes are not drift.** Watcher `file.changed` events — which
include the worker's own tool writes — feed only the `observed_changes`
signal. `drift_findings` is populated solely by an explicit trusted provider
(for example a `RuntimeReconciler` pass correlating runtime observations), via
the worker's `control_signals` seam. Gap events feed `capture_gaps`.

## Trusted signal collector

`TrustedSignalCollector(lifecycle, reconciler)` is the ready-made
`control_signals` provider for the parent wiring helper
(`worker_feature_wiring.py`, parent-owned):

- **Drift**: each `snapshot()` runs one bounded `RuntimeReconciler.drift(
  update_baseline=True)` pass and excludes every finding whose path the
  watcher already delivered as a `file.changed` event (the lifecycle keeps a
  bounded `observed_paths` ledger). Expected tool writes are therefore never
  drift; only unobserved workspace differences count. Without a reconciler the
  collector does not provide `drift_findings`, and `validate_signal_sources`
  refuses a drift-threshold policy against it.
- **Conflicts**: `record_conflict(name)` stores named conflicts (bounded at
  256). `AgentWorker` relays *actual* canonical rejection replies
  (`revision_conflict`, `claim_conflict`, `permit_conflict`, … via
  `CONFLICT_CODES`) from its execute/commit paths into the collector; nothing
  synthetic is recorded.
- **Snapshot/ack**: `snapshot() -> (signals, token)` merges the lifecycle
  ledger and pending conflicts; `ack(token)` consumes exactly the snapshotted
  conflict ids and ledger ids. The plain callable form (`collector()`) returns
  signals without an ack token and is never auto-acked.

`validate_signal_sources(policy, *providers)` raises when an enabled policy
declares triggers (`observed_changes`/`capture_gaps`/`drift_findings`/
`conflicts`) that no installed provider can feed. `AgentWorker` runs this
validation at construction, so a misconfigured CLI fails at startup instead of
silently doing nothing.

## Automatic controls

```python
from hivemind.worker_capture import AutomaticControls, automatic_policy_from_mapping

policy = automatic_policy_from_mapping({
    "enabled": True,
    "observed_changes_threshold": 50,     # watcher-observed events (pending, unacked)
    "capture_gap_threshold": 1,           # explicit gap events
    "drift_findings_threshold": 3,        # trusted-provider drift only
    "conflict_signals": ["claim_conflict", "revision_conflict"],
    "auto_resume": False,                 # the ONLY automatic-resume switch
})
controls = AutomaticControls(
    client, project_id, agent_id, run_id, state_file,
    adapter=runtime_adapter,              # must declare supported_controls (or
                                          # match the vendor/file contract)
    control_client=coordinator_client,    # a SEPARATE coordinator identity
    coordinator_epoch=lambda: current_lease()["coordinator_epoch"],  # refreshed
    policy=policy)
outcome = controls.tick(signals)          # one bounded policy cycle
hold = controls.hold()                    # derived from durable intents only
```

- Every action re-reads the run (current `control_revision`) and calls the
  `coordinator_epoch` callable, so a stale lease is never reused.
- Dispatch goes through `SafePauseController`: the intent is durable and
  stable-keyed before dispatch, service rejections are `failed`, transport
  failures are `held` (unknown — never failed, never replayed), and adapters
  without a declared/contracted `pause` capability are held without any
  canonical control being created.
- `hold()` is derived only from durable controller intents: `pause_applied`
  (with the recorded quiescent checkpoint), `pause_held`, `cancel_applied`,
  `cancel_held`, or `clear`. Heartbeat silence is never an input. A held
  unknown outcome never auto-resolves; it requires an explicit resolution.
- Automatic resume happens only when `auto_resume` is set, the hold is
  `pause_applied`, and the tick carried an **explicit, complete, non-reset**
  signal set that evaluates clear: every configured threshold signal must be
  present with a concrete value, `conflicts` must be present when conflict
  signals are configured, and `reset` must be absent. `tick(None)` (no
  evidence), a reset marker, or a missing value never implies "problem
  cleared". The exact recorded applied-pause checkpoint is consumed exactly
  once; cancel checkpoints are terminal.

## Worker boundary seam

`AgentWorker` accepts three optional, duck-typed seams (all default `None`):

- `capture_lifecycle`: each tick adopts `capture_lifecycle.capture_health()` as
  the run's reported capture health, and (when no explicit `control_signals`
  callable is given) peeks `capture_lifecycle.pending_signals()` as the policy
  input; after a tick outcome with a persisted receipt, it calls
  `ack_signals()` once.
- `automatic_controls`: policy cycles run through `_policy_check()` — one
  bounded, synchronized `automatic_controls.tick(signals)` — at the tick
  boundary before any dispatch **and** at long-turn supported boundaries (the
  durable checkpoint callable and the maintenance thread), so a
  gap/conflict/drift trigger during a long vendor call creates its durable
  pause intent mid-turn, not after it. `_policy_check` is non-reentrant (a
  non-blocking lock): a concurrent check skips and the durable hold governs,
  so slow adapters never cause double ticks or controller deadlock.
  `_pending_control()` then returns a synthetic
  `{"action": "pause"|"cancel", "policy_hold": state}` whenever any hold is
  active. No model work issues while a policy-held or paused/cancelled result
  stands; in-flight turns stop at their next supported boundary via
  `WorkerStopped`.
- `control_signals`: an explicit provider overriding the signal source —
  normally a `TrustedSignalCollector`. Providers with `snapshot()`/`ack()`
  get snapshot-exact acknowledgement; plain callables are read each check and
  never auto-acked. The worker also relays actual canonical conflict replies
  into a provider's `record_conflict`.

## Parent CLI wiring contract (ExitStack)

```python
with contextlib.ExitStack() as stack:
    lifecycle = WorkerCaptureLifecycle(client, project_id, agent_id, run_id,
                                       capture_config_from_mapping(config["capture"]),
                                       journal=journal)
    if lifecycle.config.enabled:
        stack.enter_context(lifecycle)    # start(); close() on exit; CleanupError
                                          # propagates — keep the journal open and retry
    controls = AutomaticControls(client, project_id, agent_id, run_id, state_file,
                                 adapter=adapter, control_client=coordinator_client,
                                 coordinator_epoch=lease.epoch,
                                 policy=automatic_policy_from_mapping(config["controls"]))
    collector = TrustedSignalCollector(lifecycle, reconciler)  # drift + conflicts
    worker = AgentWorker(client, project_id, agent_id, run_id, runtime,
                         enabled=True, capture_lifecycle=lifecycle,
                         automatic_controls=controls, control_signals=collector)
    # Construction raises if the policy declares triggers no installed source
    # feeds (e.g. drift_findings_threshold without a reconciler).
    worker.run()
```

Operation metadata for every public operation (purpose, input, output,
authority, side effects, limits, retry safety) lives in
`worker_capture.WORKER_CAPTURE_DESCRIPTORS`; descriptors describe and never
grant authority. `worker_capture.ack_signals` and `controls.resume` are marked
not retry-safe by contract: ack only after a persisted receipt, and a consumed
checkpoint is never replayed.

## Limitations

- The watcher is a polling observer, not an OS sandbox; see
  `docs/control-capture.md` for the gap vocabulary it inherits.
- Automatic pause is only as authoritative as the adapter: an adapter that
  cannot establish a stop with a quiescent checkpoint yields a hold, not a
  stop. The worker honors holds cooperatively at supported boundaries; a
  runtime that never checkpoints cannot be interrupted mid-call.
- The delivery ledger is bounded (`MAX_PENDING_SIGNALS = 4096`); overflow
  drops the oldest entries and declares `reset`, which blocks auto-resume
  until an explicit clear signal set arrives.
- The coordinator epoch is trusted to be current because the parent refreshes
  it; staleness surfaces as a `failed` receipt via `stale_coordinator_epoch`,
  never a replay.
- `drift_findings` is never derived from watcher events; without a trusted
  `control_signals` provider, a drift-only policy simply never triggers.
