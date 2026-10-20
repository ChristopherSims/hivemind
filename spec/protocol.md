# Implemented reference contract, 0.1.0

The independent design remains `hivemind-plan.md`. This document describes the current,
reduced local milestone and must not be mistaken for a ratified protocol standard.

Success envelopes contain `protocol`, `protocol_version`, `request_id`, and `data`.
Errors contain those identifiers and a safe `error` object. Discovery advertises the
implemented capabilities before authentication; it does not expose an agent roster.

Most mutations require `Idempotency-Key`. Events/messages/client posts supply stable IDs.
Identical retries return the original acceptance; conflicting payloads return 409.
Replays are rechecked against current deletion/visibility policy. Revision precondition
failures return 412. Structured validation returns 422 without echoing input content.

Event batches are capped at 100/1 MiB. Individual validated events are capped at 128 KiB.
Batch event shape/semantic errors are returned per item; valid items commit independently.
Canonical sequencing is per project and transactional. Source IDs are bound to the first
accepted run; sequence collisions with changed payloads are rejected. Terminal runs cannot
be reopened by status events. A late tool start cannot reopen an observed completed call.

Auth roles in this local milestone:

- `admin`: local bootstrap owner, registers identities and has explicit observer access.
- `agent`: represents exactly its configured agent in one project. Optional local capture
  authority and Nexus eligibility are granted only through the local administration CLI.
- `human`: project shared-board participation and explicit memory review, no private DM inspection.
- `observer`: human participation/review plus read-only inspection of project agent messages.
  Observation never permits inbox claims/acks or agent impersonation.

Project board material is shared with all project participants. Direct messages remain
visible only to their agent participants and explicitly authorized admin/observer humans.
Mentions route by immutable IDs and are separate durable notifications.

`messages.retry` adds bounded retry policies, fenced `delivery_id` receipts,
explicit processing failures, effective delivery state, and revision-fenced dead-letter
requeue. `conversations.unread` adds explicit per-principal read markers and authorized
delivery summaries. Read markers are never delivery acknowledgments. Delivery is
at least once; the receiver must deduplicate side effects by message ID.
See [delivery command semantics](../docs/message-delivery.md).

`dashboard.timeline_search` and `dashboard.keyset_pagination` extend board and
conversation reads with bounded `limit`, literal Unicode `query`, and stable
creation-sequence `before` cursors. Visibility/participant filters precede page
selection; revisions never move an item to another creation page.
See [timeline semantics](../docs/dashboard-search.md).

`memory.extraction` exposes explicit, actor-bound extractive proposals from selected
findings/artifacts. Candidates are unverified and deduplicated against pinned evidence.
`memory.review_notifications` adds private outcome receipts to the existing notification
feed; reading/acknowledging a receipt never verifies facts or grants runtime authority.
See [agent memory automation](../docs/agent-memory-automation.md).

Agent extensions advertise `tasks.scheduling`, `tasks.verification`,
`agents.capabilities`, `conversations.workflows`, `messages.lease_renewal`,
`context.compact_assembly`, and `memory.pipeline_opt_in`. Runtime reconciliation is
served at `runtime:reconcile` with existing identity/operator gates.
See [installed capability contracts and boundaries](../docs/agent-capabilities.md).
Capability names describe functionality, not new authorization grants.
`context:assemble` uses the full canonical JSON envelope budget and rejects a budget
too small for safe metadata; `context:query` retains its legacy contract.
Self-reported task completion never implies independent verification.

`tasks.durable_execution` adds revision-fenced start/recovery and checkpoint commands.
Execution IDs bind updates to the current run; existing work claims and controls still
govern all mutation authority. Progress/completion are agent reports, not verified
outcomes. See [task execution semantics](../docs/agent-task-execution.md).

The `memory.lifecycle` capability adds version proposals, human review, contradiction
reports/resolution, UTC validity/expiry, and authorized version history. These are
revision-fenced commands, not agent self-verification or semantic auto-resolution.
New evidence references pin canonical revisions. Inactive memories are excluded at
read time from retrieval even when a derived index has not caught up. Explicit
management reads may show authorized inactive content; revoked source content is hidden.
See [lifecycle command semantics](../docs/memory-lifecycle.md) and generated request schemas.

Search pre-authorizes a bounded candidate set before computing vector distances with
sqlite-vec. Candidates are current SQL records with current source validity. Index
freshness reports the earliest incomplete job, never the maximum completed job alone.

Nexus has one current project lease and a monotonic leadership epoch. Controls require
current authority, target capabilities, a control revision, and a typed action. Runtime
results require a persisted checkpoint and the expected observed state. Claims/permits
are decided in SQLite, never by vector similarity or graph freshness.

JSON Schemas in `spec/schemas` describe request models and strict event payload families.
Regenerate with `python scripts/export_schemas.py`. Cross-resource authorization,
byte counts, leases, conditional list limits, and evidence validity remain semantic
checks in addition to schema validation.
