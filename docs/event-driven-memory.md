# Event-Driven Memory Maintenance

`hivemind/memory_event_bridge.py` replaces purely periodic memory maintenance with
event-driven triggers on top of the existing (unchanged) lifecycle automation. Canonical
commits and trusted repository notifications wake a **targeted** review instead of
waiting for the next scheduled tick.

Repository publishes go through
`MemoryLifecycleAutomation.detect_changes`, whose own snapshot → lock-free compute →
validate-publish pipeline, durable fingerprints, and exact-commit invalidation rules
apply unchanged. Periodic `scheduled_tick` stays as the backstop; both paths dedup
through the same `memory_lifecycle_fingerprints` table, so running both never
double-publishes. The periodic tick may be left enabled or turned down when the event
path is installed — coordination is by construction, not configuration.

The module is registered by `create_app`. Enable
`integration.json` → `"memory_event_bridge": {"enabled": true}` alongside an enabled
`memory_lifecycle.policy` with an explicit `operator_principal_id`. No repository
bindings are needed for canonical task/source review. Automatic supporting-source
invalidation is a separate opt-in (`auto_invalidate_supporting_refs`), not the default.

Operators can read the bounded authorized queue through
`GET /v1/memory-events:reviews?project_id=...`, including review signals for memories
whose stale evidence makes ordinary memory GET return 404. This exposes review
metadata, not stale memory content. Deleted or no-longer-visible memories are omitted.
Python SDK methods: `trigger_memory_events`, `memory_event_status`,
`memory_event_reviews`, `release_memory_event_hold`. TypeScript uses
`triggerMemoryEvents`, `memoryEventStatus`, `memoryEventReviews`,
`releaseMemoryEventHold`. Releasing a hold skips it; it never permits uncertain replay.

## Triggers

1. **Canonical commit signals** — `Store.subscribe_commits` delivers minimal
   `{project_id: new_seq}` invalidations after the store lock is released. The callback
   only records a bounded in-memory dirty hint and wakes the worker; it never receives
   bodies, never takes the store lock, and never publishes synchronously (a nested
   publish inside the outer commit would deadlock).
   Target-kind resources (`task` progress/completion, `verification` decisions,
   `artifact`, `memory` revisions, `board_post`) with `seq > watermark` wake a targeted
   `detect_changes` for the bindings of exactly that project **plus** the deterministic
   supporting-source review below.
2. **Trusted watcher seam** — local/native git ref changes produce no canonical commit.
   An operator-injected `watcher(bindings) -> iterable[...]` callable is polled each
   heartbeat. A hint is `{binding_key}` plus an optional `{event_id}` (≤128 chars) from
   the operator's trusted source. Hints are never proof — the bridge re-derives ref
   state itself and `detect_changes` re-reads git before anything is published. For
   repositories the ref scan cannot observe (`.git`-file worktrees, bare repos), a bare
   hint dedups forever and never infers an effect; only **distinct trusted event ids**
   wake real detection (same id dedups). Unknown binding keys and malformed events are
   rejected and counted (`watcher_rejected`).
3. **Hardened fallback ref scan** — a slow-cadence backstop (`fallback_scan_seconds`)
   that reads only `.git/HEAD`, the HEAD target ref, `refs/heads/<branch>`, and
   `.git/packed-refs` (each bounded by `max_ref_bytes`). Every component is validated
   **before** the read: symlinks, junctions, reparse points, non-regular files,
   absolute refs, `..`/`\`/`:`/`//` components fail closed with the stable
   `untrusted_ref` marker — no path or error detail is ever exposed, and a linked
   component is never read through. A `.git`-file (worktree) or bare repository yields
   `snapshot=None` forever, deferring to the watcher seam. No hooks, no code execution,
   no fetch. The scan is a backstop for missed notifications, **not** the sole or
   primary mechanism.

## Supporting-source maintenance (real work on canonical commits)

A checkpoint or revision bump on a `task` / `artifact` / `memory` / `board_post` that a
memory pins in `evidence_refs` (exact `{kind, id, revision}`) triggers real,
deterministic maintenance — even when the git HEAD never moved:

- The batch finds citing memories with **bounded keyset-paged targeted SQL**
  (`support_page_limit`, `support_pages_per_drain`; the durable cursor lives in the
  intent's detail), never an unbounded whole-project scan. The watermark advances only
  after every targeted decision in the batch is durable.
- Default policy: a canonical `memory_bridge_review` request is published with
  `reason=supporting_source_revision_changed`, the memory id, the pinned revision, the
  observed outcome (`changed`/`deleted`), and a `ref_digest` of the source ref — never
  the raw private source id. Review requests are canonical but not retrieval-indexed.
  Private sources cannot signal into shared or different-agent memory namespaces;
  these combinations hold generically instead. Source and memory namespaces are
  revalidated in the decision transaction.
- Opt-in (`auto_invalidate_supporting_refs=True`): the memory gains a new immutable
  revision with `invalidation_reason=supporting_source_revision_changed`; history is
  preserved (no deletion, no content rewrite).
- **Strict combined privacy**: a private memory citing a private source from a
  *different* namespace produces only a generic durable hold (`held_generic`) — no
  derived cross-namespace signal, no ids in status.
- Every decision is a durable idempotent row in `memory_event_bridge_decisions`
  (fingerprint = memory id+revision, ref, pinned revision, observed outcome), so
  retries and re-drains never double-publish. Memories already
  invalidated/disputed/expired record `skipped_state` and never accumulate signals.
- Deterministic only: no paid encoder, contradiction, or model compute runs on this
  path. Unpinned refs are not exact-bound and are ignored.

## Durability: watermarks, intents, holds, fencing

- One durable **watermark** per project (`memory_event_bridge_watermarks`), upserted
  with `MAX(seq, excluded.seq)` so a concurrent accept can never regress it. Accept +
  watermark advance happen in **one** store transaction with a fresh operator check.
- One durable **intent** per trigger fingerprint (`memory_event_bridge_intents`).
  Claiming is atomic across separate Store instances and processes: `INSERT OR IGNORE`
  plus an owner-token CAS inside a single explicit transaction.
- **Owner fencing**: each `running` intent carries an instance owner token and PID. A
  live owner — in-process (live bridge instance registry) or cross-process (best-effort
  PID liveness; anything unprovable is treated as alive) — is never claimed, never
  converted, never recomputed (`inflight`). Only a *provably dead* owner is a crash
  witness, CAS'd to a durable `unknown` hold. Held triggers are never replayed — no
  repeated paid or side-effecting compute — until an operator explicitly releases them
  via `POST /v1/memory-events:release` (marks them `skipped`). Release never
  re-executes the held compute; subsequent work comes only from deterministic
  git/source re-reads (fresh commits/scans) or a new operator trigger, and the
  automation's own fingerprints dedup anything the crashed run managed to publish.
- Transient `Problem` failures re-hint the project with a bounded retry count
  (`retry_limit`); the fallback cycle re-hints every known project regardless, so a
  dropped listener wake-up is recovered from the durable watermark without a second
  commit.
- The bounded pending queue drops hints (never blocks the committer) on overflow and
  sets a gap flag; the next drain then runs a watermark-driven catch-up over every
  known project — overflow loses signals but never loses work.
- `start()` re-hints every known project, so work missed while the bridge was stopped
  is caught up after a restart.

## Concurrency

- Single daemon worker, heartbeat-driven (`heartbeat_seconds`), so a blocked
  git/encoder call never wedges the subscription path and shutdown stays responsive.
- Per drain, work is bounded by `batch_limit`, `max_batches_per_drain`,
  `support_page_limit`, `support_pages_per_drain`; leftover backlog persists its cursor
  and is re-hinted for the next iteration.
- Coordination with the existing periodic `scheduled_tick` is by construction: both
  call `detect_changes`, which dedups via `memory_lifecycle_fingerprints`; the bridge's
  intent/decision tables and operation names are disjoint from the automation's.

## Operator and privacy discipline

- Disabled by default (`BridgePolicy.enabled = False`); starts only when the bridge
  policy, the lifecycle policy, and a configured operator principal are all present.
- The operator principal is **re-fetched and re-validated on every pass and per
  decision**; a revoked operator stops work with a surfaced
  `lifecycle_operator_unavailable` error and no hold.
- Status exposes counts, watermarks, decision tallies, and fingerprint digests only —
  no memory bodies, no ids of private material, no workspace paths.
- The trigger route accepts only a project id and optional binding key; all evidence is
  re-fetched from canonical state. Callers supply no proof.

## Install

```python
from hivemind import memory_lifecycle_automation as lifecycle
from hivemind import memory_event_bridge as bridge

automation = lifecycle.install(app, service, principal, command, envelope,
                               policy=lifecycle.LifecyclePolicy(
                                   enabled=True, operator_principal_id=operator_id),
                               bindings=[lifecycle.RepositoryBinding(
                                   project_id=project_id, workspace=path)])
bridge = bridge.install(app, service, automation, principal, command, envelope,
                        policy=bridge.BridgePolicy(enabled=True),
                        watcher=my_trusted_watcher)   # optional
...
result = bridge.close()   # explicit, bounded, idempotent; call BEFORE Store.close()
store.close()
```

`BridgePolicy` (all bounds validated at construction): `enabled`,
`queue_capacity` (8..65536), `batch_limit` (1..200), `max_batches_per_drain` (1..64),
`heartbeat_seconds` (0.05..60), `fallback_scan_seconds` (1..86400),
`max_ref_bytes` (1 KiB..1 MiB), `join_timeout_seconds` (0..60), `retry_limit` (0..16),
`support_page_limit` (1..200), `support_pages_per_drain` (1..64),
`auto_invalidate_supporting_refs` (bool).

There are no other factory/config switches: the bridge derives everything else from the
installed lifecycle automation (bindings, operator principal) and the store. Enabling
the event path does not require disabling the periodic tick; both dedup through the
same fingerprint tables.

## API

All routes are registered by `install` (reinstall replaces only this module's routes).
`trigger`, `release`, and `status` are marked `hivemind_detached_work` and run through
`app.state.run_detached` (shutdown-tracked worker threads; `asyncio.to_thread`
fallback) — never inline on the event loop, and never under the middleware's global
request/store lock.

| Route | Auth | Effect |
| --- | --- | --- |
| `POST /v1/memory-events:trigger` | operator | `{project_id, binding_key?}` — targeted scan now, or queue a catch-up hint. Returns `accepted` / `advance` / `inflight` / `hold` / `queued`. |
| `POST /v1/memory-events:release` | operator | `{project_id, fingerprint?}` — mark unknown holds `skipped`; idempotent; never replays the held compute. |
| `GET /v1/memory-events:status` | operator | Watermark, pending/gap counts, decision tallies, holds, watcher stats, last error. |
| `GET /v1/memory-events:descriptors` | authenticated | Data-only `OPERATION_DESCRIPTORS` (`event_catchup`, `supporting_source_review`, `repository_scan`, `hold_release`). |

## Lifecycle contract

- `close()` unsubscribes the commit listener, stops and joins the worker with
  `join_timeout_seconds`, and is idempotent. If a compute is blocked mid-call,
  `close()` returns `{"closed": True, "drained": False}` within the bound instead of
  hanging; a repeated `close()` re-checks the actual worker thread and reports
  `drained: True` once it really exited. The daemon thread finishes or dies with the
  process.
- `drained: False` means a compute may still touch the store: the host must keep the
  store open (or fully quiesce the bridge) until a repeat `close()` reports drained.
  Always close the bridge before `Store.close()`.

## Tests

`tests/test_memory_event_bridge.py` (real git fixtures, no network/hooks/paid models):

- `test_canonical_task_checkpoint_wakes_targeted_review` — real task start/checkpoint
  commits wake targeted review; exactly one deduped `source_change`.
- `test_checkpoint_with_constant_git_head_triggers_real_source_review` — a checkpoint
  that bumps a pinned task's revision with the git HEAD unchanged produces exactly one
  `memory_bridge_review` (`supporting_source_revision_changed`, digest-only ref), no
  memory rewrite, deduped across re-drains.
- `test_supporting_source_invalidation_requires_opt_in_and_preserves_history` — with
  `auto_invalidate_supporting_refs=True` the memory gains an invalidation revision with
  full history preserved and no review request.
- `test_private_memory_private_foreign_source_holds_generically` — private memory +
  private source from another namespace → generic durable hold; no ids in status, no
  review.
- `test_ref_component_validation_fails_closed` — hostile HEAD targets (traversal, ADS,
  absolute, `//`, encoded) and a symlinked ref into an arbitrary file all yield the
  stable `untrusted_ref` marker; nothing is read through, nothing leaks.
- `test_gitfile_worktree_watcher_needs_distinct_trusted_event_ids` — `.git`-file
  worktrees are unobservable to the scan; bare hints dedup without inference, distinct
  trusted event ids wake real detection, repeated ids dedup, oversized ids rejected.
- `test_foreign_live_owner_is_never_claimed_and_dead_owner_holds` — a live
  foreign-process owner is never claimed or converted; a provably dead owner becomes a
  durable unknown hold, never recomputed.
- `test_transient_error_rehints_and_recovers` — transient failures re-hint bounded and
  recover without manual intervention.
- `test_dropped_hint_recovers_via_watermark_fallback` — a fully dropped listener
  wake-up is recovered by the fallback cycle from the durable watermark alone.
- `test_high_volume_commit_burst_coalesces` — 40-commit burst coalesces into one
  deduped event; watermark monotone.
- `test_event_loop_responsive_and_close_rechecks_thread` — status/trigger answer while
  compute is blocked; `close()` honestly reports `drained: False` and re-checks on
  repeat until `drained: True`.
- `test_rolled_back_commit_is_silent`,
  `test_stopped_and_restarted_bridge_catches_up`,
  `test_private_sources_never_leak_and_revoked_operator_stops_work`,
  `test_failed_compute_holds_unknown_and_never_replays`,
  `test_blocked_compute_keeps_close_responsive`,
  `test_queue_overflow_falls_back_to_watermark_catchup`,
  `test_local_repository_watcher_seam_and_bounded_fallback`,
  `test_descriptors_status_and_authorization`, `test_disabled_bridge_is_inert`,
  `test_policy_validation`.

## Limitations

- The bridge triggers **repository-change review and deterministic supporting-source
  decisions only**. It never runs contradiction scans — those may invoke paid
  encoder/detector compute and stay on the periodic/explicit paths. No arbitrary
  generated-memory rewrites exist anywhere in this design.
- The fallback scan cannot observe bare repositories or `.git`-file worktrees through
  ref files; those rely on trusted watcher event ids or canonical triggers.
- PID liveness is best-effort; anything unprovable (including cross-platform or
  permission-denied probes) is treated as a live owner — the intent holds rather than
  risking a double compute.
- Watermark catch-up covers target kinds only; other kinds never wake review.
- Watcher delivery is best-effort in-process polling, not an OS filesystem watch.
- A `GET` on a memory whose pinned evidence revision drifted already returns 404
  (canonical `valid_memory` rule); the bridge's review request is the durable signal
  that this happened.
