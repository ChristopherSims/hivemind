# Tracked agent conversation workflows

Direct messages already carry durable envelopes, delivery leases, and
`claim`/`ack`/`fail`. This layer adds a separate **workflow** record for conversations that
need a tracked answer: response deadlines, reply helpers, cancellation records, fenced lease
renewal, and one-time escalation of unanswered requests.

Workflow state and delivery state are independent. Nothing here acknowledges, retries, or
impersonates a recipient, and nothing here schedules work on its own. There is no scheduler,
no paid agent session, and no network egress: deadlines, overdue flags, and escalations are
evaluated on explicit client calls, so clients poll. Message content stays evidence, never
instructions, and server-authored notices never embed client text.

## Integration

`hivemind/conversations.py` registers its routes through
`install(app, service, principal, command, envelope)`, reusing the existing `principal`
dependency, `command` idempotency wrapper, and `envelope` helper from `hivemind/api.py`.
The shared `hivemind/capabilities.py` feature registry already lists `conversations`, so the
remaining integration step is calling `capabilities.install(...)` from `create_app` (owned by
the parent workstream). Until then the routes below are not served and do not appear in
`spec/openapi.json`. The module is self-contained: it adds models, routes, and a client
helper without changing existing tables or delivery semantics.

## Tracked requests

`POST /v0.1/conversation-requests` creates the durable request message and its workflow
record in one idempotent command:

```json
{
  "project_id": "prj_example",
  "message_id": "msg_example",
  "from_agent": "agt_backend",
  "to": "agt_frontend",
  "thread_id": "thr_session_contract",
  "correlation_id": "corr_session_contract",
  "type": "request",
  "ttl_seconds": 3600,
  "response_deadline_seconds": 300,
  "content": {"media_type": "text/markdown", "text": "Does the session response need expires_at?"}
}
```

The message itself goes through the existing send path, so the usual rules still apply:
`from` must be an agent the principal may represent, `to` must be a live agent in the same
project, and only `request` or `question` may be tracked. Mention segments, unknown fields,
out-of-range TTLs, and out-of-range deadlines are rejected with `invalid_request` (422).
Idempotency keeps one workflow per `message_id`: the workflow ID is derived from the message
ID, so a repeat create returns the recorded workflow instead of a duplicate, and the same
message ID with different content is an `idempotency_conflict` (409).

`correlation_id` defaults to the thread ID, matching the message envelope. The recorded
`response_deadline` is the requested deadline clamped to the message's own `expires_at`,
so a workflow can never outlive its envelope. Omitting `response_deadline_seconds` records
no deadline and disables escalation for that request.

Reads:

- `GET /v0.1/conversation-requests/{id}` returns the workflow, the underlying message with
  its delivery state, and the escalation record if one exists.
- `GET /v0.1/conversation-requests?project_id=...&state=open&limit=200` lists workflows the
  caller may see, optionally filtered by `open`, `answered`, `cancelled`, or `escalated`.

`state` is workflow state (`open`, `answered`, `cancelled`, `escalated`) and is returned
both as `state` and `workflow_state`; delivery state remains on the nested message as
`delivery_status`. Derived read-time fields are `unanswered`, `deadline_reached`,
`response_overdue` (unanswered and past deadline), and `message_unavailable`.

## Replies and reconciliation

`POST /v0.1/conversation-requests/{id}:respond` is the reply helper. Only the addressee may
use it; the sender, an unrelated agent, an observer, or a human gets `forbidden` (403), and a
non-participant gets `not_found` (404). The reply is created through the existing send path
with `in_reply_to` set to the request, the workflow's thread and correlation, and the
participants reversed, so thread/correlation/participant mismatches still fail with
`thread_mismatch` (422). A closed workflow returns `request_closed` (409).

Recording an answer never acknowledges the request envelope. The request can stay
`available` (or leased, retried, or dead-lettered) while the workflow is `answered`; an
explicit `ack` remains the recipient's delivery receipt, and workflow `answered` is not a
claim that the work was correct.

`POST /v0.1/conversation-requests/{id}:reconcile` scans durable messages the caller can
already read and persists the first qualifying answer or cancel. A candidate must share the
thread, the correlation, the workflow's participants, and reply to the tracked message;
`response`, `ack`, and `error` from the addressee count as answers, `cancel` from the sender
counts as a cancellation, and anything else (`inform`, unrelated chatter, another
correlation) is ignored. Answers take precedence over cancellations. Reconciling never
widens message visibility, and the response reports `reconciled: true` only when workflow
state changed. An optional `expected_revision` fences the scan with `revision_conflict` (412).

## Cancellation

`POST /v0.1/conversation-requests/{id}:cancel` with `expected_revision`, `reason`, and
`notify` records the sender withdrawing its own tracked request. Only a principal
representing the request's `from_agent` may cancel (administrators act through the same
representation check), so a cancellation is never issued in the target's name. The
recipient, an observer, or an unrelated agent is refused.

The cancellation is a workflow record plus, when `notify` is true, one durable `cancel`
message in the same thread and correlation. It records a request, not proof that work
stopped, and it does not acknowledge, fail, or requeue delivery. Repeating a cancel for an
already-cancelled workflow returns the recorded cancellation without sending a second
notice; an answered workflow returns `request_already_answered` (409). A `cancel` message
created directly through `POST /v0.1/messages` is detected by `:reconcile` and recorded the
same way.

## One-time escalation

`POST /v0.1/conversation-requests/{id}:escalate` records an escalation for an unanswered
request whose response deadline has passed. It requires the represented sender, an
`expected_revision`, and an `open` workflow; before the deadline it returns
`response_deadline_pending` (409).

One workflow can be escalated at most once. A repeat call returns the recorded escalation
instead of creating a second record or a second notice, so escalation is safe to retry.
The canonical `conversation_escalation` record stores the request, participants, target,
reason, deadline, `escalated_by`, the notice message ID, `one_time: true`, and resolution
state. The escalation notice is a server-authored `inform` message in the same thread and
correlation, addressed to `escalate_to` (default: the original addressee). Escalating to a
third agent references an existing agent in the project (`not_found` 404 otherwise), and
escalating to the sender itself is refused with `invalid_escalation_target` (422); a third
agent can read the escalation record but gains no access to the workflow or the thread.
`GET /v0.1/conversation-escalations?project_id=...&request_id=...` lists records the caller
may see.

Escalation is a status record: it does not cancel work, does not reassign ownership, and does
not prove that any human or agent noticed it. A later answer or cancellation resolves the
recorded escalation (`state: "resolved"`, `resolution` of `answered` or `cancelled`, and
`answer_message_id` when an answer was recorded).

## Fenced delivery lease renewal

`POST /v0.1/messages/{id}:renew` extends one live delivery lease without creating a new
attempt:

```json
{"run_id": "run_example", "delivery_id": "delivery_example", "ttl_seconds": 60}
```

The caller must represent the run, the run must be the message's addressee and non-terminal,
and the immutable `delivery_id` must still match; otherwise the call fails with `not_found`
(404), `terminal_run` (409), or `delivery_receipt_stale` (409). Renewal refuses a lease that
is not currently `leased`, including acknowledged, expired, failed, replaced, or
dead-lettered deliveries, with `delivery_lease_expired` (409).

Renewal only moves `claim_until` forward: it never changes `state`, `attempt_count`,
`delivery_id`, or `expires_at`, never acknowledges, and never revives an expired or replaced
lease. Each grant adds at most `LEASE_MAX_SECONDS` (120) and is clamped to the message's
original expiry, so a short-TTL envelope cannot be extended past the TTL it was sent with. A
grant that would not extend the current lease is a no-op that returns the unchanged receipt.
Accepted renewals append one bounded `lease_renewed` entry to the delivery history. Because
renewal is an idempotent command, replaying a command key returns the recorded receipt even
if the lease has since expired; read `GET /v0.1/messages/{id}` for current state.

## Access scope

No authorization was broadened. Read access matches message visibility: administrators and
observers, or a principal representing one of the two participants. Everything else is
`not_found` (404), including human roles that lack conversation visibility. Listing uses the
existing conversation grant, so a human role without it gets
`conversation_observer_grant_required` (403). Actions reuse `represent` and `reference`,
which is why an observer can read a workflow but cannot answer, cancel, or escalate it.

## Python helper

`hivemind.conversations.ConversationWorkflow` wraps any client exposing
`request(method, path, body=None, key=None)` — including `hivemind.sdk.Client` — and injects
`sleep`/`clock` so polling is deterministic in tests:

```python
helper = ConversationWorkflow(client, project_id, agent_id, run_id=None)
opened = helper.open_request(recipient_id, "Confirm the response schema", deadline_seconds=300)
answer = helper.wait_for_reply(opened["id"], timeout_seconds=300, interval_seconds=5)
if answer["wait_result"] == "timeout":
    helper.escalate_if_overdue(opened["id"])
```

- `open_request(to, text, thread_id=None, deadline_seconds=120, kind="request", media_type=..., ttl_seconds=3600, key=None)`
- `read_request(id)`, `reconcile(id, expected_revision=None, key=None)`
- `reply(id, text, kind="response", media_type=..., key=None)` — as the addressee
- `cancel(id, reason, expected_revision=None, notify=True, key=None)`
- `escalate(id, reason=..., expected_revision=None, escalate_to=None, key=None)`
- `escalate_if_overdue(id, reason=..., key=None)` — escalates at most once
- `renew_lease(message_id, delivery_id, ttl_seconds=30, key=None)` — requires `run_id`
- `wait_for_reply(id, timeout_seconds=60, interval_seconds=5)` — bounded poll loop returning
  `wait_result` of `answered`, `cancelled`, or `timeout` plus the poll count

The helper never acknowledges delivery and never renews a lease it does not hold. Callers
still own their own scheduling, side-effect deduplication, and retry keys.

## Limitations

- No scheduler, background worker, or push notification: overdue detection, escalation, and
  deadline reminders happen only when a client calls an endpoint. Clients must poll.
- No exactly-once semantics. Delivery stays at least once; deduplicate by `message_id` and
  record command keys locally if a client must retry across its own crash.
- Idempotent replays return the recorded acceptance, which may be older than current state.
  Read the workflow before choosing the next transition.
- `answered` means an answer was recorded, not that the answer is correct, complete, or
  verified; `cancelled` records a request, not a stopped process.
- Escalation records the notice but cannot force a response, reassign a task, or prove that
  anyone read it. Escalating to a third agent does not grant that agent thread access.
- Client-provided `reason` text is bounded (1024 characters) and stored canonically in
  workflow and escalation records; ordinary retention and deletion limits apply to it.
- Workflows do not re-anchor a deleted or replaced message: a deleted envelope leaves the
  workflow readable with `message_unavailable: true` instead of fabricating content.
- Only the workflow endpoints in this module are covered. No dashboard, MCP tool, or
  TypeScript client is added here. Discovery capability strings are not registered, and
  `spec/openapi.json` gains these routes only when the integration step regenerates it; the
  shared export pipeline already publishes the request models as
  `spec/schemas/conversations.*.schema.json`.
- Concurrency is the existing single-store serialization: revision fences, `delivery_id`
  fences, and one escalation per workflow. There is no cross-project or multi-writer
  coordination beyond that.

## Tests

`tests/test_conversation_workflows.py` covers workflow/delivery separation, deadline
clamping, reply and reconcile binding to thread/correlation/participants, addressee-only
responses, cancellation without impersonation or acknowledgment, one-time escalation with
late resolution, fenced renewal bounds (stale, expired, replaced, acked, terminal), access
scope, canonical persistence across a fresh connection, and the bounded Python helper. The
suite uses the local app and a mock transport only: no paid sessions and no network.
