# Memory lifecycle

Memory has a stable resource ID and a separate content `memory_version_id`. Agents may
create facts, propose replacement versions, and report contradictions. Only local
admin, human, or observer principals may verify facts or review versions/disputes.
Every command needs an `Idempotency-Key`; browser writes also require the session CSRF token.

## Versions and evidence

- `POST /v1/memories/{id}/versions` creates a proposal, not a replacement.
  Supply `expected_revision`, `title`, `content`, `evidence_refs`, and optional
  `validity`/`expires_at`. Scope, audience, category, and canonical ID cannot change.
- `POST /v1/memory-proposals/{id}:review` accepts `expected_revision`,
  `decision` (`approve` or `reject`), and a nonempty `reason`.
  Approval checks the original memory revision and replaces its content atomically.
  Competing/stale proposals cannot overwrite a later review.
- `GET /v1/memories/{id}/versions` returns up to 200 stored revisions collapsed into
  distinct content versions, newest first. Prior versions are labeled `superseded`.
  Authorization and current evidence checks apply to every version, including
  idempotent command replies. Deleted or changed old sources do not expose old content.
- `GET /v1/memory-proposals/{id}` reads an authorized proposal's current state.

New memories, proposals, and reports pin evidence revisions even when the caller
omits them. Self-evidence and indirect memory cycles are rejected. An approval
rebinds canonical `SUPPORTED_BY` links. Current-source invalidation is conservative:
invalidated memories are hidden, not automatically repaired; create a new evidence-backed
memory instead. Historical records remain subject to current source authorization.

## Contradictions

`POST /v1/memory-disputes` requires a project, two distinct memory IDs, their exact
`left_revision`/`right_revision`, a reason, and independent evidence. Both memories
must have the same private audience. Reporting alone leaves both facts unchanged.

`POST /v1/memory-disputes/{id}:review` requires the dispute's `expected_revision`,
both current memory revisions (`left_expected_revision`/`right_expected_revision`),
a reason, and one decision:

- `confirm`: quarantine both memories from retrieval.
- `dismiss`: remove this dispute's quarantine without silently verifying either fact.
- `prefer_left` / `prefer_right`: verify the selected current fact and supersede the other.

Reads use `GET /v1/memory-disputes/{id}`. Revoked/changed report evidence hides the
report text and evidence references. A human may still dismiss that quarantine, but
cannot use unavailable evidence to confirm or prefer a fact. Cached old report text
cannot bypass this check. Multiple active disputes remain fenced independently.
All multi-resource changes share one SQLite transaction.

## Time and validity

`expires_at`, `validity.valid_from`, and `validity.valid_until` accept UTC RFC3339 strings
ending in `Z`, for example `2026-10-10T12:00:00Z`. Calendar dates and ordered intervals
are validated. The earliest end bound wins; equality at the end is expired. Expiry
must follow a supplied start. Repository/branch/observed-commit fields are descriptive
metadata, not automatic Git branch/commit invalidation.

Expired, future, disputed, rejected, superseded, and source-invalid memories do not
enter ordinary memory listing, search, context, or graph traversal. Checks happen at
read time, including real-vector candidate authorization, so index lag cannot revive
them. Proposed current memories remain available to explicitly unverified retrieval;
normal context requires verified facts. Human verification/approval rejects inactive
time windows. Expiry is freshness control, not physical deletion.

Explicit management detail/history reads may show inactive but authorized,
evidence-valid content. Owners may delete inactive or invalidated memories without
reading their contents. Byte-retention limitations still apply.

## Dashboard and clients

Open **Memory review** or `/projects/{project_id}/memories`. The workspace shows status,
time bounds, pinned sources, immutable history, new-version proposals, review reasons,
and contradiction report/review controls. It refreshes every five seconds and clears
content on authorization/read failures. Review reasons are required and stale versions
cannot be approved. The review queue is bounded to 1,000 recent resources per kind.
The version form selects sources from the first authorized board's recent page;
the API supports other authorized evidence kinds.

Python and TypeScript clients expose version, proposal, dispute, and review operations.
Agent MCP tools expose `hivemind_propose_memory_version`,
`hivemind_report_memory_dispute`, and `hivemind_memory_versions`. Human decisions remain
in the authenticated dashboard/API, not an agent self-verification tool.
