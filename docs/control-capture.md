# Control capture: continuous watcher, tool gateway, safe pause/resume

`hivemind/control_capture.py` is an opt-in feature module composing three
governed-runtime controls. It is additive: it reuses the existing capture spool
(`hivemind.sdk.Capture`), the bounded scanner and cooperative boundary from
`hivemind/runtime_lifecycle.py`, and the canonical service control commands.
Nothing here is an OS sandbox, a syscall interceptor, or a hidden
transcript/keystroke collector.

## Continuous filesystem-change capture

`ContinuousWatcher` polls a trusted, administrator-supplied workspace
configuration (`WatcherPolicy`, `enabled=False` by default, all bounds strictly
typed) and records hash-only identity: path, change
(`created`/`modified`/`deleted`), size, sha256, mtime. File contents are never
read into events.

Durability and bounds:

- Every accepted event and the post-scan cursor commit in **one SQLite
  transaction** (`<workspace>-watch.sqlite`: `outbox` + `cursor` + `meta`), so
  a crash after `scan()` never silently loses accepted observations. Event IDs
  are stable (`<source>-<sequence>`) and persist with the hash-only evidence.
- `drain()` requires a bound `Capture`; it transfers rows into the spool under
  the capture lock with a dedup check on the stable event ID and acknowledges
  each outbox row only after the spool commit. A crash between insert and ack
  dedups on retry; a spool failure aborts with remaining rows retained. Use
  `pending()` to inspect retained rows without acknowledging them.
- A lost cursor after a previous run (durable meta high-water proves it), any
  truncated scan — including the first — and any diff capped by
  `max_events_per_scan` produce explicit `capture.gap_detected` rows. A
  truncated scan never advances the baseline and never infers deletions. Gap
  rows bypass the outbox cap but coalesce when consecutive and identical.
- Recursive traversal reuses `scan_workspace`: secrets (`.env*`, private keys,
  credential files), `.git`, and configured exclusions are never opened.
  Workspace, state, and spool path ancestors are checked with `lstat`
  semantics **before** any `resolve()`: symlinks and Windows
  junction/reparse-point escapes are rejected up front.
- A delete plus a create with identical size+hash in one scan is paired as
  rename evidence in local event detail; canonical events stay within the
  service vocabulary.
- Scans, drains, and cursor writes are serialized under one lock, so
  concurrent callers and the poll thread cannot race. Threads are explicit and
  bounded: `start()` requires `enabled=True` and starts exactly one poll
  thread; `stop()` only clears the thread reference after a successful join,
  and a timed-out join raises while retaining the reference.

`watcher_from_config(config, capture=...)` builds a watcher from a trusted
operator mapping and rejects unknown keys.

## Governed tool-request gateway

`ToolGateway` classifies tool requests using a canonical,
administrator-controlled map (`describe_tool`): namespace (`filesystem`,
`network`, `process`, `discovery`), action (`read`/`write`/`execute`/
`discover`/`unknown`), side effects, retry safety, and required authority
(`read_scope` for reads, `permit` for writes). Host- or model-supplied tool
descriptions are carried as untrusted metadata and are never an input to an
authorization decision.

- Strictness is operator-only: `GatewayPolicy(strict=...)` is validated as a
  real boolean, and neither HTTP request bodies (the route schema forbids
  unknown fields) nor hook payloads can relax it.
- Native hook events are normalized by `from_hook_payload` for the two
  documented adapters: Droid stdin hooks (`hook_event_name`:
  `PreToolUse`/`PostToolUse`/`SessionStart`) and the Oh my Pi extension
  (`type`: `tool_call`/`tool_result`/`session_start`). Any other payload shape
  raises; under a strict policy the caller must deny rather than bypass.
- Unknown tools, non-allowlisted tools/namespaces, out-of-workspace or
  excluded reads, and unsupported write shapes are denied under strict mode.
  Without a trusted workspace root, confinement-checked reads and writes deny
  rather than allowing unbounded access; network reads carry no local path.
- Write decisions carry canonical target paths; `ToolGateway.authorize`
  confirms them through the service's `POST runs/{run}/permits:check` against
  the task/execution/claim scope and is fail-closed on any rejection.
- Native integration: `hivemind/hooks.py` applies the gateway before tool
  execution when the trusted hook config contains `gateway: {"enabled": true,
  ...}` (default off; malformed gateway config fails closed). All pre-existing
  hook checks (allowlists, budgets, permit boundaries) are preserved.

## Safe pause/resume policy

`SafePauseController` turns configured thresholds (`ControlPolicy`: drift
findings, quarantined capture events, named conflict signals — all strictly
typed) into typed, idempotent `ControlIntent`s with revision authority.

- **Capability contract**: dispatch requires the action in
  `runtime_capabilities(adapter)` — an explicit `supported_controls`
  declaration, the documented `VendorRuntime` contract (Factory/OMP runtimes
  settle pause/cancel with quiescent checkpoints and resume from a paused
  outcome), or the `FileAdapter` wrapper contract. Mere callable existence is
  never sufficient; unsupported adapters are `held` with evidence and create
  no canonical control.
- **Fences**: dispatch revalidates the live run (identity, non-terminal
  status, current `control_revision`) before any adapter call; stale
  revisions, terminal runs, and late controls are denied.
- **Idempotency**: `dispatch` accepts a stable caller `intent_key` (default: a
  digest of the exact fence — action, target run, expected revision, reason).
  The intent is persisted as `dispatching` before the canonical command, and
  the service idempotency key derives from the intent key, so a crash or
  timeout retry never creates a second control; deduplicated receipts refresh
  from the canonical control record when available.
- **Outcomes**: service rejections are `failed`; generic transport failures
  are `held` as unknown — never failed and never a silent no-effect. A
  disabled policy denies dispatch and resume outright.
- **Exactly-once resume**: `resume(checkpoint_id)` requires an *applied pause*
  intent whose observed checkpoint matches and is bound to this run. Cancel
  checkpoints are terminal and can never resume; foreign or stale checkpoints
  are denied. The consumption marker is persisted before dispatch, so a crash
  cannot replay it, and corrupt or foreign controller state refuses to load
  rather than losing the history. No automatic replay of uncertain model
  output is performed.
- **Bounded history**: intents and consumed checkpoints are capped
  (`MAX_INTENTS`, `MAX_CONSUMED_CHECKPOINTS`); exceeding a bound raises instead
  of silently dropping history.
- State is guarded by a cross-process `FileLock` and bound to its
  project/agent/run identity on first persist.

## Operation descriptors

`OPERATION_DESCRIPTORS` declares each public operation (`watcher.scan`,
`watcher.drain`, `gateway.evaluate`, `control.dispatch`, `control.resume`) as
a typed `OperationDescriptor` with purpose, input, output, authority, side
effects, limits, and retry safety — the contract the parent CLI /
`createWorker` wiring binds trusted adapters and config against.
`watcher_from_config` and `controller_from_config` are the ready-made,
strictly validated construction helpers for that wiring.

## HTTP surface

`install(app, service, principal, command=None, envelope=None, *, path="/v1/control-capture", gateway_policy=None)`
registers, only when the parent wires it:

- `POST {path}:tool-evaluate` — advisory-but-fail-closed gateway verdict for a
  tool request against a run. `workspace_root` (local path reads for
  confinement) requires an administrator principal; the body agent must match
  the run's actual agent and project; the schema forbids unknown fields, so
  callers cannot smuggle policy relaxations.
- `GET {path}:policy` — administrator-only view of the canonical tool map and
  policy posture.

## Wiring required from the parent

1. Add `"control_capture"` to `capabilities.INSTALLED_FEATURE_MODULES` (the
   registry is owned by the integration layer) so `install` runs at startup;
   pass the operator `gateway_policy` there.
2. CLI / `createWorker` lifecycle: build watchers with `watcher_from_config`
   (binding a `Capture` on the observer run, drained by the existing
   `capture_daemon.py` cadence) and controllers with
   `controller_from_config` (agent client + coordinator `control_client` +
   the trusted vendor runtime adapter). Hook configs opt into the gateway with
   `gateway: {"enabled": true}` plus optional namespace/allowlist narrowing.
3. Bind trusted Factory/OMP runtime adapters; compatibility is negotiated via
   `runtime_capabilities` (`supported_controls` declaration preferred), so
   runtimes that cannot produce quiescent checkpoints degrade to `held`
   instead of false `applied`.

## Supported runtimes and honest gaps

- Supported today: any runtime fronted by the Droid stdin hook or the Oh my Pi
  extension events above (tool interception), `VendorRuntime` subclasses
  (Factory, Oh my Pi) and the `FileAdapter` wrapper (pause/cancel/resume with
  quiescent checkpoints), and any workspace reachable by local polling
  (filesystem capture).
- Gaps: runtimes without a native hook/extension surface get no interception
  (strict mode denies their tools rather than bypassing); runtimes whose
  adapters cannot produce a quiescent checkpoint get `held`/`outcome_unknown`,
  never `applied`; the watcher is polling, so sub-interval changes collapse to
  their net effect and are reported as such — this is declared, not hidden.
