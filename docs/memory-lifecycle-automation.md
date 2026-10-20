# Repository-driven memory lifecycle automation

An **opt-in** extension (`hivemind/memory_lifecycle_automation.py`) that watches
**operator-configured, trusted repository bindings** and turns observable changes into
**review proposals and durable reminders** for the existing memory lifecycle. It also
produces contradiction **review candidates** for semantic memories and runs
reminder/expiry policies around the store's explicit UTC/version fences.

It is disabled by default. It never loads, downloads, discovers, or pays for a model,
never executes repository hooks or fsmonitor helpers, never fetches remotes, and never
deletes data. The only write it can perform on a memory is a **deterministic logical
invalidation** — a new immutable revision carrying `invalidation_reason` — of material
pinned to the *exact* superseded commit, and only when a binding explicitly sets
`auto_invalidate=True` (a strict boolean). Every prior revision stays in the canonical
version history; semantic review authority stays with the existing
human/admin/observer pipeline.

## Lock and idempotency discipline

Every compute operation (`tick`, `detect`, `scan-contradictions`, `reminders`) is
**two-phase**:

1. **Snapshot** — one short `store.lock` section copies a bounded, authorized,
   exact-revision snapshot (principal role/project/agent, binding state revision,
   memory ids/revisions/pins/text fields).
2. **Compute** — encoder, detector, and git work run with **no** canonical store lock
   and **no** open transaction held. Compute never gains authority from the snapshot.
3. **Validate-publish** — one short transaction re-fetches the acting principal
   (role/project/agent must match the snapshot), the binding state revision, and every
   memory's current revision/visibility/activity. Anything that changed concurrently —
   a revoked principal, edited or hidden evidence, a newer binding state — discards the
   computed result instead of publishing stale authority. Durable fingerprints dedup
   inside the publish transaction, so racing passes cannot duplicate events.

The four compute routes are marked `endpoint.hivemind_detached_work = True`. The API
middleware skips its global request/store lock for exactly these handlers; they
authenticate through the app's own `principal` dependency (which holds `store.lock`
itself) and run the two-phase work via `asyncio.to_thread`, keeping the event loop
and ordinary requests responsive while long compute proceeds.

### Durable intent/checksum/result contract

`detached_call` reserves `(principal, operation, key, payload_hash)` in the
`memory_lifecycle_intents` table in a short transaction **before any**
encoder/detector/git invocation. Intent states:

| State | Meaning | Same-key behavior |
| --- | --- | --- |
| `reserved` | Compute in progress, **or orphaned by a crash** (indistinguishable). | 409 `compute_intent_in_progress`; **no** compute runs. |
| `unknown` | A failure at/after the first tracked external invocation (callback exception, timeout, publish revalidation). | 409 `compute_outcome_unknown`; never automatically replayed or re-opened. Only an explicit **new** operator key authorizes new work. |
| `done` | Result persisted (`canonical` JSON). | 200 replay of the stored result with **zero** re-invocation, after re-checking *current* authority: the principal row is re-fetched (role/project/agent must be unchanged) and the endpoint's own guard (enabled, operator role, project access) runs again. Revoked authority gets no cached output. |

A failure raised with **zero tracked external invocations** (validation, disabled
policy, access errors) is authoritatively pre-invocation: the reservation is deleted
and the same key may retry. External invocations are tracked through
`_mark_invocation()` on every git process, encoder call, and detector call, so the
pre/post-invocation distinction is authoritative, not heuristic. A reused key with a
different payload is `idempotency_conflict` (409); a missing key is
`idempotency_key_required` (422).

### Durable detector screening (background and direct)

When a detector is injected, every qualifying pair is reserved in a durable
**screen** fingerprint keyed by `(left id+revision, right id+revision, detector
identity, encoder identity)` *before* the detector is invoked. Outcomes are recorded
as `clear` (no contradiction), the published event id, or `unknown` (the invocation
happened but the outcome is uncertain — e.g. crash/exception). Screened pairs are
never re-invoked, across ticks, restarts, and new request keys; an `unknown` pair is
held, not silently re-opened. New work requires new deterministic identity: a revised
memory, a changed detector/encoder identity, or an explicit new operator request key
(which authorizes a new *intent*; unchanged pairs still dedup). The default pure
lexical/hash path is deterministic and side-effect-free, so it is documented as
repeatable and needs no reservation; **no injected callback is assumed pure**.

## Install

```python
from hivemind.memory_lifecycle_automation import (
    LifecyclePolicy, RepositoryBinding, install)

automation = install(
    app, service, principal, command, envelope,
    policy=LifecyclePolicy(enabled=True, operator_principal_id=admin_principal_id),
    bindings=[RepositoryBinding(project_id=project_id, workspace="/srv/repos/spec",
                                branch="main", notify_agent_id=agent_id,
                                auto_invalidate=False)],
    encoder=None,     # default: local hashing encoder (lexical, not semantic)
    detector=None,    # explicitly opt-in contradiction detector callable
    time_source=None) # injectable clock for deterministic scheduling tests
```

* `principal`, `command`, `envelope` are the app's own authentication dependency,
  idempotent mutation wrapper, and response wrapper. This module never replaces them.
* `install` returns the `MemoryLifecycleAutomation` and sets `app.state.memory_lifecycle`.
  `replace_routes` makes reinstallation clean.
* `automation.bind(binding)` registers additional trusted bindings; bindings are the
  **only** repositories ever observed.
* There is no background thread. The host scheduler calls `scheduled_tick()` (gated by
  `interval_seconds`, operator re-validated every pass), or `tick()` runs one bounded
  pass explicitly. Both follow the same two-phase lock discipline as the endpoints.
* Encoder and detector callbacks are trusted operator configuration supplied at
  install time; nothing here downloads or discovers them.

`LifecyclePolicy` (frozen dataclass, validated on construction; all numeric fields
must be finite, `enabled` must be a strict boolean):

| Field | Default | Meaning |
| --- | --- | --- |
| `enabled` | `False` | Opt-in switch for every route |
| `operator_principal_id` | `None` | Principal for `scheduled_tick()`; re-fetched and re-validated (admin/human, project access) on every pass |
| `notify_agent_id` | `None` | Recipient agent for project-scope reminders (private memories always notify their owner) |
| `reminder_lead_seconds` | `86400` | Remind this long before an expiry fence |
| `reminder_cooldown_seconds` | `43200` | Cooldown bucket for repeated reminders |
| `review_after_seconds` | `None` | Optional age-based `stale_review` for memories without fences |
| `notification_ttl_seconds` | `86400` | Notification expiry |
| `max_events_per_tick` | `32` | Source-change events per pass |
| `max_reminders_per_tick` | `16` | Reminders per pass |
| `max_diff_paths` / `max_path_bytes` | `64` / `256` | Changed-path evidence bounds |
| `max_git_output_bytes` | `262144` | Hard stdout+stderr bound per git command (4 KiB–1 MiB) |
| `contradiction_scan_limit` | `32` | Semantic memories compared per scan (2–64) |
| `contradiction_similarity` | `0.6` | Cosine prefilter for pair comparison |
| `interval_seconds` | `60` | Minimum gap between `scheduled_tick()` passes |
| `git_timeout_seconds` | `5.0` | Wall-clock deadline per git command |
| `secret_path_markers` | `.env`, `secret`, `credential`, … | Changed paths containing a marker are persisted only as digests |

## Repository change detection

Git access is read-only, bounded, and non-executing. Commands run through
`process_boundary.run_bounded` (bounded stdout/stderr with kill-on-overflow,
wall-clock deadlines, and process-group cleanup) with:

* `-c core.fsmonitor=false` (a repository-configured fsmonitor helper is never run),
* `-c core.hooksPath=` (empty: no repository-selected hook path),
* `-c diff.external=` (no external diff drivers),
* `GIT_CONFIG_NOSYSTEM=1` and `GIT_CONFIG_GLOBAL=<nul>` (no system/global config),
* `GIT_OPTIONAL_LOCKS=0`, `--no-optional-locks`, `--literal-pathspecs`,
* `GIT_ALLOW_PROTOCOL=file`, `GIT_TERMINAL_PROMPT=0`, and literal operator-trusted
  argv only — never model- or request-supplied arguments.

Only `rev-parse`, `status --porcelain`, `diff --name-only`, `merge-base
--is-ancestor`, and `cat-file -e` are used. Timeouts map to `git_observe_timeout`
(503), output overflow to `git_observe_output_boundary` (503). Absolute workspace
paths are never persisted — events carry the hashed `repository_id` and bounded,
secret-redacted, repo-relative paths only.

Each binding keeps a canonical `memory_lifecycle_binding` state resource. The first
observation records a **baseline** (no events). Later passes compare HEAD commit,
branch, and dirty-file count:

* **New commit** → a `source_change` event with `from_commit`/`to_commit`, bounded
  `changed_paths`, and every repository-pinned memory whose
  `validity.observed_commit` no longer matches HEAD, marked `review_proposal`.
* **Rewrite** (old HEAD is not an ancestor of the new HEAD) → `rewritten: true`;
  path diffing is skipped.
* **Dirty working tree** → `commit_changed: false` with the bounded dirty-file count.
  Dirty sources never invalidate anything.
* **Other branch checked out** than the configured `branch` → skipped explicitly.

Events are deduplicated by a durable fingerprint of the observed state transition and
split per privacy scope, so private memory ids never leak into project-wide
visibility.

### Deterministic logical invalidation (explicit opt-in)

When a binding sets `auto_invalidate=True`, a memory whose
`validity.observed_commit` equals the **exact superseded HEAD commit** receives one
new immutable revision with `invalidation_reason: "repository_commit_superseded"`
plus an `invalidation` evidence block. The pin equality, the current revision, and
the operator's authority are **re-checked inside the publish transaction**; a
concurrent edit, dispute, or revoked access discards the invalidation. The store's
existing rules then report the memory `invalidated`; history is preserved in the
`versions` table and nothing is physically deleted. Memories pinned to any *other*
commit are never invalidated automatically.

## Contradiction review candidates

`scan_contradictions` compares **semantic** memories that are currently
`proposed`/`verified` within the same project, scope, and privacy class:

1. The injected encoder produces vectors **without holding any store lock**; pairs
   below `contradiction_similarity` are skipped. The default `LocalHashingEncoder` is
   a **lexical hasher, not a semantic model** — `encoder.kind` is `lexical` for the
   default and `injected` (with `semantic: true`) only for an explicitly injected
   encoder. Encoder failure maps to `similarity_encoder_unavailable` /
   `similarity_encoder_invalid_output`.
2. The deterministic **lexical** heuristic (`contradiction_signals`: negation,
   polarity, value conflicts) flags candidates with `method: "lexical"` and explicit
   limitations — it never claims authoritative semantic understanding.
3. An **operator-injected, explicitly opt-in** detector callable
   `detector(left_text, right_text) -> {"verdict": "contradiction"|"none"|"unknown",
   "confidence": 0..1, "evidence": str, "limitations": [str]}` may add verdicts, also
   computed lock-free. Output is strictly validated; a raising detector fails the call
   explicitly; `unknown` verdicts produce nothing and can never quarantine anything.

Candidates are recorded as `contradiction_review` events carrying confidence,
similarity, signals, evidence, encoder/detector identity, and limitations. They are
**review proposals only** — never disputes, quarantine, invalidation, or rewrites.
Pairs whose revision, visibility, or activity changed during compute are discarded at
publish. Two memories private to *different* agents are never correlated.

## Reminders and expiry policies

`run_reminders` schedules durable, deduplicated review reminders (planning is pure
computation over snapshot copies; publishing re-plans from freshly re-fetched memory
rows and discards anything whose fingerprint changed):

| Reminder | Trigger | Dedup |
| --- | --- | --- |
| `expiry_reminder` | `expires_at`/`validity.valid_until` within the lead window | per memory revision per cooldown bucket |
| `expired_review` | fence already passed (the store reports `expired`) | once per memory revision |
| `stale_review` | age ≥ `review_after_seconds`, no fence, still active | per memory revision per cooldown bucket |

Notifications are created through the canonical `notification` resource, **private to
the memory's owning agent**, or to the configured `notify_agent_id` for project-scope
memories; with no configured recipient the reminder is recorded without any
notification. Reminder fingerprints persist in `memory_lifecycle_fingerprints`, so
dedup survives restarts; a new memory revision resets dedup. Logical expiry itself is
enforced by the store's explicit UTC/version fences — this module never deletes or
physically expires anything.

## API

Compute routes are detached two-phase handlers (see above); read routes use the normal
serialized path. `tick`/`detect` require an admin/human operator.

| Route | Effect |
| --- | --- |
| `POST /v1/memory-lifecycle:tick` | One bounded pass: repository detection, contradiction scan, reminders |
| `POST /v1/memory-lifecycle:detect` | Repository detection only |
| `POST /v1/memory-lifecycle:scan-contradictions` | Contradiction candidates for one project |
| `POST /v1/memory-lifecycle:reminders` | Reminder pass for one project |
| `GET /v1/memory-lifecycle:events?project_id=&type=` | Lifecycle events (visibility-scoped) |
| `GET /v1/memory-lifecycle:reminders?project_id=` | Reminder records |
| `GET /v1/memory-lifecycle:status?project_id=` | Policy, bindings, counts, encoder/detector identity |
| `GET /v1/memory-lifecycle:descriptors` | Operation descriptors |

`OPERATION_DESCRIPTORS` is a module-level, data-only constant (also returned by
`automation.descriptors()` and the descriptors route) that the parent tool-context
layer consumes. Each entry carries `purpose`, `input`, `output`, `authority`,
`side_effects`, `limits`, and `retry_safety` for `source_change`,
`contradiction_review`, the reminder kinds, and `logical_invalidation`.

## Limits

* `review_only` everywhere except the explicitly opted-in `logical_invalidation`.
* Lock assertions are per-thread: compute threads must not hold `store.lock`
  (`_is_owned()`); the sqlite connection's global `in_transaction` flag may
  legitimately reflect an unrelated write on another thread and is not asserted.
* Contradiction scans are capped at 64 memories per pass (pairwise budget bounded).
* Git commands are allow-listed, bounded in output, deadline-bound, and lock-free; a
  missing or non-repository workspace yields an explicit `unavailable` result.
* Unknown detector evidence cannot quarantine authoritative facts; adversarial or
  malformed detector output is rejected without touching memories.
* Secret-marked paths are persisted only as digests; private bindings' events stay in
  their privacy scope; the acting operator's credentials never appear in events.
