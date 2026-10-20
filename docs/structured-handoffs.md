# Structured handoffs (recipient-accepted)

`hivemind.structured_handoffs` adds a durable `structured_handoff` resource: a
canonical handoff **proposal** from the agent currently executing a task to another
agent, with an explicit, fenced recipient **accept/reject** decision. It complements
— and never replaces — the existing administrator/human ownership action
`POST /v1/tasks/{task_id}:handoff` (see [task-scheduling.md](task-scheduling.md)),
whose semantics are unchanged.

## What an offer is (and is not)

An offer is bound to the exact sender snapshot:

- exact `project_id`, `task_id`, `expected_task_revision`, `source_run_id`,
  `source_execution_id` (the task's current execution must match all of them);
- the exact `recipient_agent_id` and optionally a pinned `recipient_run_id`
  (live, unassigned, owned by the recipient);
- `context_refs` and typed `deliverables` as exact kind/id/revision pins;
- a **sanitized, server-derived checkpoint** (summary, progress, and evidence refs
  re-pinned with exact revisions from the canonical task execution — never
  client-supplied; checkpoint evidence without an exact revision refuses the offer
  with `checkpoint_evidence_unsupported`, it is never silently dropped);
- a bounded `unresolved_questions` list (max 8 entries, 1024 chars each);
- `transfer`: `"context"` (default) or `"ownership"`.

An offer **never** transfers ownership, releases claims, changes task status, or
declares a runtime quiescent. Only facts and references are transferred — never raw
hidden reasoning, and credential-shaped content (per `recovery.looks_sensitive`)
in the checkpoint, summary, notes, questions, or decision reasons refuses the
operation (`checkpoint_not_transferable` / `sensitive_content_refused`, 422).

Source references authorize **repeatedly, per principal**: at offer (against the
offering principal), at every read (against the reading principal), at accept
(against the recipient principal), and at every context consumption (against the
consuming run's principal) — including idempotent command replays, which re-read
and reauthorize current canonical state rather than serving a cached payload.

## Visibility: cleared context vs blind identity view

The record is a standard project resource, but views are restricted:

- Only the **exact sender agent**, the **exact recipient agent**, or an
  **administrator** see the context payload (summary, checkpoint, context refs,
  deliverables, unresolved questions, decision detail) — and only while every
  pinned source is still visible to that exact viewer.
- If any pinned source becomes stale or invisible to the viewer, the view
  suppresses all context-derived payload (`context_available=false`,
  `context_note="context_suppressed_source_stale_or_invisible"`).
- Every other project member (including human/observer roles) receives a **blind
  view**: identity and state fields only, with `context_available=false` and no
  per-resource freshness detail. Freshness reason codes are generic
  (`context_ref_unavailable`, `task_revision_changed`, …) and never name or
  enumerate private resources.

## Endpoints

| Endpoint | Body | Notes |
| --- | --- | --- |
| `POST /v1/structured-handoffs` | `HandoffOffer` | Key scope `structured-handoff-offer:{task_id}` |
| `POST /v1/structured-handoffs:consume` | `RunTaskContext` | Key scope `structured-handoff-consume:{task_id}` |
| `GET /v1/structured-handoffs?project_id=[&task_id][&recipient_agent_id][&state]` | — | Bounded list (200), per-viewer authorized views |
| `GET /v1/structured-handoffs/{id}` | — | Single view |
| `POST /v1/structured-handoffs/{id}:accept` | `HandoffAccept` | Key scope `structured-handoff-accept:{id}` |
| `POST /v1/structured-handoffs/{id}:reject` | `HandoffReject` | Key scope `structured-handoff-reject:{id}` |
| `POST /v1/structured-handoffs/{id}:revoke` | `HandoffRevoke` | Key scope `structured-handoff-revoke:{id}` |

All models are strict (`extra="forbid"`, finite bounds). All mutating routes go
through the standard command/idempotency path and then **re-read or re-run the
domain logic**, so every response — including receipt replays — is reauthorized
against current visibility. `OPERATION_DESCRIPTORS` / `descriptors()` describe the
operations in the tool-context manifest. The module is registered by `create_app`.
Installing attaches `app.state.structured_handoffs` /
`service.structured_handoffs`; when not installed the hook is simply absent
(`getattr(service, "structured_handoffs", None)` — default empty).

## Acceptance fences

Accept requires, all checked fresh at decision time:

1. The exact offer revision (`revision_conflict`, 412) and `state == "pending"`
   (`handoff_not_pending`, 409 — a second accept under a different key fails; the
   same Idempotency-Key replays the canonical receipt and then re-reads fresh).
2. The principal currently represents the recipient agent (`forbidden`, 403).
3. The exact current recipient run: live (`running`/`idle`), same project, owned
   by the recipient agent, equal to any offer-pinned recipient run
   (`recipient_run_mismatch`, 409).
4. The source snapshot is unchanged (same task revision, same source execution,
   checkpoint still available: `handoff_source_stale` /
   `checkpoint_evidence_unavailable`, 409) — unless a settled/intent path applies
   (below).
5. The recipient's current capabilities still cover `required_capabilities`
   (`capability_mismatch`).
6. Every pinned context ref, deliverable, and checkpoint evidence ref is still
   visible to the **recipient** at the pinned revision (404 / `stale_evidence`).

Acceptance records the durable decision on the offer — it does **not** change the
task, runs, claims, or executions. Ownership invariants hold unconditionally.

## Ownership transfer policy (default off)

`StructuredHandoffPolicy(allow_ownership_transfer=False)` is the default; an
ownership offer (or accept) without the explicit operator opt-in fails with
`transfer_policy_disabled` (403). Even with the opt-in, acceptance of an ownership
offer requires one of:

- **Verified settled-execution evidence** (`support="settled_execution_evidence"`):
  the exact source execution ENDED with a canonical terminal checkpoint — the task
  is `completed`/`failed` with that execution's recorded checkpoint still
  available, or the archived execution carries a canonical terminal outcome
  (`succeeded`/`failed`) plus its checkpoint. **A stale or absent heartbeat, a
  terminal run row, a paused run, an executing source, or an unknown/deleted
  source is NEVER external quiescence proof and holds the accept**
  (`source_effects_active`, 409).
- **Explicit administrator intent** (`support="intent_only_unverified"`): a pending
  `tasks/{id}:handoff` record bound to the same source execution and recipient.
  This is intent, not proof of settled source effects: it authorizes accepting the
  context across the moved task snapshot, but the ownership request is labeled
  `intent_only_unverified` / `unsupported_without_settled_evidence` and must not be
  presented as a verified-safe handoff.

At most one ownership offer per task can be accepted (`handoff_already_accepted`,
409), so concurrent accepts have exactly one winner. Regardless of support level,
`decision.ownership.effected` is always `false`: ownership moves exclusively
through the existing `tasks/{id}:handoff` + dispatch machinery.

## Revocation

The sender agent (or an administrator) can revoke a **pending** offer or an
**accepted-but-unconsumed** offer (`handoff_not_revocable`, 409 otherwise).
Revocation durably scrubs ALL context payload — summary, checkpoint, unresolved
questions, and the context-ref/deliverable identities themselves
(`context_scrubbed=true`) — so no hidden cached context survives revocation for
readers or replays. The reason is screened for credential-shaped content. Rejected,
revoked, and consumed states are final.

## Worker context consumption

`POST /v1/structured-handoffs:consume` (typed `RunTaskContext`: `run_id`,
`task_id`, `execution_id`, `expected_task_revision`) is the standard command route
the parent SDK/worker uses; `consume_accepted_context(service, principal, request)`
is the equivalent in-process helper for the parent worker wiring. Contract:

- The principal must currently represent the run's agent; the run must be live,
  currently hold the task, match the accepted decision's exact
  `recipient_run_id` (and any offer-pinned run), and the task's current
  revision/execution must match the request exactly (`run_task_mismatch` /
  `stale_task_execution` / `revision_conflict`). A same-agent **other** run gets
  nothing.
- Every pinned ref and checkpoint evidence ref is re-authorized against the
  consuming principal on **every** call; revoked/private/deleted sources fail
  closed even on a canonical receipt replay.
- Delivery binds durably to the exact (run, execution). Retries are safe: the
  parent worker uses a stable execution-bound Idempotency-Key (canonical command
  receipt replay) or a new key (fenced idempotent re-delivery); both return the
  same context, reauthorized fresh. A lost response therefore never loses the
  context, and delivery to a different run or execution fails with
  `handoff_consumed_elsewhere`. Returns `context=null` when no accepted handoff
  exists.
- Consumption NEVER changes task status, run assignment, claims, executions, or
  ownership. Once delivered, the offer can no longer be revoked.

## Honest limits

- Python and TypeScript SDKs expose offer/decision/consume facades. Managed workers
  opt in with `--consume-handoffs`; consumed context reaches the shared bounded
  vendor/protocol prompt envelope as untrusted evidence with no restore guarantee.
- Freshness on reads is informational; only the accept/consume-time fences
  authorize.
- Offer creation is bounded (`max_open_offers_per_task`, default 8, hard max 64);
  offers for the same execution are otherwise independent proposals.
- Nothing here proves a runtime quiescent. "Settled" is exactly the canonical
  terminal-checkpoint evidence defined above; heartbeat staleness, terminal run
  rows, and pause states are never treated as settled.
- The checkpoint sanitizer is a credential-pattern heuristic, not a proof that
  arbitrary sensitive prose is excluded; the visibility blinding above is the
  primary confidentiality control.
