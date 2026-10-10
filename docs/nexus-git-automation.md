# Autonomous Nexus Merge Loop (`hivemind/nexus_git_automation.py`)

Opt-in, default-disabled automation that lets the elected Nexus coordinator merge
queued merge requests without a human in the loop — strictly through the existing
canonical Git collaboration APIs. It adds no new merge path, no new transport, and
no new privilege: every mutation goes through `GitCollaboration.check`,
`GitCollaboration.review`, `GitCollaboration.merge`, and `Service.acquire_nexus`,
so revision fences, exact-hash pinning, protected-ref fencing, the trusted-check
policy hash, and the durable audit ledgers all apply unchanged.

## What one tick does

`NexusGitAutomation.tick()` (or the throttled `scheduled_tick()` for loop
integration) performs a single bounded pass. Neither entry point holds an
enclosing SQLite transaction or the store lock: **every canonical mutation
(lease acquire/renew, check, review, merge) is its own short idempotent
`Service.mutate` command** with a deterministic, revision-pinned key (a retried
tick replays rather than duplicates effects), and the reviewer runtime is always
invoked with **no active transaction**, free to call back into canonical service
APIs without deadlocking or blocking heartbeats.

1. **Identity gate.** The acting principal must be the configured Nexus agent
   principal (`role == "agent"`), with a live configured run (`running`/`idle`).
   No other principal may operate the loop; the loop never impersonates anyone.
2. **Conservative lease handling.** If another coordinator holds a live lease,
   the tick yields (`{"state": "yield"}`) — it never preempts. If the automation
   already holds the lease and it is inside `renew_margin_seconds` of expiry, it
   renews in place (same lease id, same coordinator epoch). Only when no active
   lease exists does it acquire one via `Service.acquire_nexus`, which bumps the
   project epoch and installs the Git fencing ref.
3. **Bounded discovery.** At most `max_listing` merge requests are listed; only
   `status == "open"` records with a `candidate_oid` are eligible, and at most
   `max_merges_per_tick` are processed per tick, oldest first.
4. **Configured checks.** For each name in `policy.checks`, the recorded receipt
   is trusted only if its `policy_sha256` matches the domain's *current* policy
   fingerprint (`GitCollaboration._check_policy`) and its source/target/candidate
   pins match the live record; otherwise the receipt is obsolete and the check is
   re-run under the configured admin/human *operator* principal — agents can never
   record trusted checks (`trusted_verifier_required` still applies). Check
   idempotency keys include the policy fingerprint, so refreshing a stale receipt
   never collides with the old command. Unknown check names stay blocked with
   `git_check_not_configured` and are never refreshed blindly.
5. **Independent review.** A configured reviewer principal (agent, same project,
   never the producer) is selected deterministically. If none is configured and
   `require_independent_review` is set, the item waits
   (`waiting_independent_reviewer`). Otherwise the pluggable
   `MergeReviewRuntime` is consulted on an immutable hash-pinned snapshot that
   names the `reviewer_principal_id`/`reviewer_agent_id` the decision will be
   recorded under; a runtime bound to a different reviewer (via a
   `reviewer_principal_id` attribute) or returning an outcome that
   self-identifies as a different reviewer is treated as `abstain` — an approval
   is never recorded under a reviewer the runtime did not answer for. Without a
   runtime the loop abstains by default — it never invents approvals.
   The runtime bridge may itself canonically record the review while it runs:
   after the runtime returns, the merge request is re-read and an existing
   decision by the exact selected reviewer with matching hash pins is recognized
   instead of duplicated against an old revision; a recorded rejection still
   blocks, and a record whose pins don't match fails closed
   (`git_review_hash_mismatch`).
   While the runtime runs, a bounded maintenance thread renews only the
   automation's own lease and heartbeats only its own run (short commands, never
   a transaction spanning model execution); it stops on lease replacement, a
   paused/terminal own run, or a pending control targeting our run, and is always
   stopped and joined before the call returns. After a long review the owning
   run is re-read (`nexus_run_not_live` on terminal status — liveness is never
   fabricated for another agent's run), a pending control yields with
   `control_pending` (the tick stops; the control applies before any merge), and
   the lease is revalidated: renewed in place near expiry, or the item yields
   with `lease_lost` when a replacement coordinator took over. This automation
   owns exactly the configured Nexus run; it never touches any other agent or run.
6. **Exact-hash fenced publication.** `GitCollaboration.merge` is called with the
   current lease id, coordinator epoch, run id, and the exact
   source/target/candidate hashes. Stale branches, stale epochs, changed check
   policy, missing approvals, or a dead run all fail closed inside the canonical
   merge; the per-item result is reported as `blocked` with the canonical error
   code. An unexpected (non-`Problem`) exception stops the tick for remaining
   items rather than retrying an unknown effect.

## Configuration

```python
from hivemind.nexus_git_automation import NexusGitAutomation, NexusGitPolicy

policy = NexusGitPolicy(
    enabled=True,
    project_id="project_...",
    nexus_principal_id="principal_...",     # agent principal with coordinator grant
    nexus_run_id="run_...",                 # that agent's live run
    operator_principal_id="principal_...",  # admin/human principal recording checks
    reviewer_principal_ids=("principal_...",),
    checks=("smoke",),                      # must exist in GitCollaboration.checks
)
automation = NexusGitAutomation(service, git_collaboration, policy, runtime=my_runtime)
```

Bounds: `interval_seconds` 1–3600, `requested_ttl_seconds` 10–120 (matches the
canonical `NexusRequest`), `renew_margin_seconds` < TTL, `max_merges_per_tick`
1–16, `max_listing` 1–1000. `enabled` must be a real boolean (truthy integers are
rejected). Enabling without the project/nexus/operator identifiers raises
`ValueError`.

## Review runtime protocol (parent integration point)

The runtime implementation is owned elsewhere. Inject any object satisfying:

```python
class MergeReviewRuntime(Protocol):
    def review_merge(self, request: Mapping[str, Any]) -> MergeReviewOutcome: ...
```

- `request` is a read-only snapshot with exactly: `id`, `project_id`, `task_id`,
  `agent_id`, `title`, `source_oid`, `target_oid`, `candidate_oid`.
- Return `MergeReviewOutcome(decision, reason, reviewer_principal_id=None)` where
  `decision` is `"approve" | "reject" | "abstain"`, `reason` is 1–4096 chars, and
  the optional `reviewer_principal_id` — when set — must equal the
  `reviewer_principal_id` from the snapshot. A runtime may also bind itself by
  exposing a `reviewer_principal_id` attribute; any mismatch yields `abstain`.
- Any other return value is treated as `abstain`.
- The runtime is invoked with no active SQLite transaction (`store.db.in_transaction
  is False`) and may call back into canonical `Service` reads/writes.

## Wiring still required (parent scope)

This module deliberately owns no shared files. To activate it, the parent must:

1. **Construction**: instantiate `NexusGitAutomation(service, app.state.git_collaboration, policy, runtime)`
   and place it on `app.state` (suggested: `app.state.nexus_git_automation`),
   alongside the `review_automation` install path in `api.py` /
   `model_wiring.py` (`capability_options`), plus a `"nexus_git_automation"`
   entry in `capabilities.py` route list if an HTTP tick endpoint is desired.
2. **Scheduling**: call `scheduled_tick()` from the `api.py` lifespan worker loop
   (next to `review_automation.scheduled_tick`) or from a `WorkerSupervisor`
   reconciler-style callback. `scheduled_tick` self-throttles by
   `interval_seconds` and performs no enclosing transaction (safe to call from
   the 1-second worker loop); `tick()` may also be invoked directly (e.g. from
   a CLI or an HTTP
   `:tick` endpoint modeled on `review_automation:tick` with a
   `NexusGitTick` body).
3. **Runtime**: supply the `MergeReviewRuntime` implementation shown above.
4. **CLI/config**: `model_wiring.py` key allowlist would need
   `"nexus_git_automation"` added for config-driven enablement (shared file —
   parent scope).

## Limits and non-goals

- No network exposure: all Git operations remain local through
  `GitCollaboration` (`GIT_ALLOW_PROTOCOL=file`, protected pre-receive hook).
- No paid model calls; the runtime protocol is synchronous and local.
- No Git identity, host, or config mutation; no administrator impersonation —
  checks record under the configured operator principal, approvals under the
  configured reviewer principal, merges under the Nexus agent principal.
- No retries of unknown effects, no preempting a live coordinator, no stale
  approvals: every review and check is re-pinned to the exact current hashes at
  merge time by the canonical domain.
- Conflicted merge requests (`status == "conflicts"`) are never touched; they
  return to the producing agent through the existing notification path.
