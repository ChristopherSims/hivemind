# Message delivery and unread accounting

Delivery is **at least once**, not exactly once. Acceptance records an envelope;
claiming records a processing lease; acknowledging records the recipient's completion
receipt. None proves a model understood or correctly acted on a message.
Deduplicate external side effects by the stable `message_id`.

## Delivery loop

1. `GET /v0.1/agents/{agent_id}/inbox?project_id={project_id}` returns currently
   eligible messages. Reading is side-effect-free.
2. `POST /v0.1/messages/{id}:claim` with `run_id` claims a lease of at most 30 seconds,
   bounded by the original message TTL. Preserve the returned `delivery_id`.
3. Process the envelope using its stable ID for deduplication.
4. `POST /v0.1/messages/{id}:ack` with `run_id` and `delivery_id` acknowledges completion.
   Or `POST /v0.1/messages/{id}:fail` with those fields and a structured `reason`
   (`handler_failed`, `interrupted`, or `temporary_unavailable`) schedules a retry.

Commands require an `Idempotency-Key`, except sending uses the stable message ID.
Repeating a live claim from the same run returns the same receipt without extending its
deadline or incrementing attempts. Competing runs cannot take a live lease. A retry
after lease abandonment gets a new receipt; stale IDs cannot acknowledge later work.
Cached claim replies are also fenced after expiry/failure/reassignment.
Terminal runs cannot claim or finish a live delivery.

For compatibility, an original first delivery can still acknowledge with `run_id`
alone. After any redelivery or manual retry, the receipt ID is mandatory. New clients
and MCP tools always provide it.

## Retry limits and recovery

New sends may include `retry_policy`:

```json
{"max_attempts":5,"base_delay_seconds":2,"max_delay_seconds":60}
```

Absent policies use those defaults. Attempts are bounded to 1–20. Base delays are
1–60 seconds, maximum delays 1–600 seconds, and maximum must not be below base.
Delay doubles after each failed attempt, capped at the maximum. Explicit failure
starts the delay immediately; an abandoned lease starts it at lease expiry.
No jitter is applied in this bounded local reference.

Effective `delivery_status` is `available`, `leased`, `retry_wait`, `dead_letter`,
`expired`, or `acked`. Status is derived from persisted attempts/deadlines at read time;
there is no scheduler required to recover after restart. Polling resumes eligible work.
The older canonical `state` field remains compatible (`available`, `delivered`, `acked`);
use `delivery_status` for current behavior.

An exhausted unexpired envelope becomes a dead letter. Its sender, recipient, or
local admin can explicitly `POST /v0.1/messages/{id}:requeue` with `expected_revision`.
This resets the current retry budget but preserves the envelope ID, lifetime total
attempt count, original expiry, and structured history. It does not claim, acknowledge,
or impersonate recipient processing. Observers cannot requeue.
Expired messages cannot be revived; send a new envelope instead.

`GET /v0.1/messages/{id}` returns authorized delivery detail. The last 50 structured
history entries record claims, observed abandoned leases, failures, acknowledgments,
and manual retries, never handler exception text or model reasoning. TTL ends delivery,
not historical authorized viewing or physical retention.

## Unread is not delivery

`GET /v0.1/projects/{project_id}/delivery-summary` returns effective status counts,
recipient pending/dead-letter counts, and the calling principal's unread count over
up to 1,000 recent authorized messages. For an agent, unread counts incoming messages
only; human observers count their authorized conversation feed.

`POST /v0.1/projects/{project_id}/conversations:read` takes 1–100 `message_ids`.
The entire batch is authorized before committing. Markers are explicit, persistent,
and per principal. Snapshot reads never create them, and read markers never acknowledge
agent delivery or suppress an agent inbox entry. Retry attempts retain the same logical
read marker. Human roles without conversation visibility do not get DM unread counts.

## Dashboard and clients

The **Agent conversations** tab shows unread badges, pending/dead-letter counts,
per-message delivery status, retry timing, and structured history. Use **Mark displayed
conversations read** explicitly; this marks only displayed messages, not unseen pages.
Admins can **Retry dead-letter message**. Observers can inspect and mark their own feed
read but cannot retry or acknowledge recipient processing. SSE invalidates on writes;
the existing 15-second refresh and **Refresh delivery** also update time-based states.
The [search/pagination extension](dashboard-search.md) keeps loaded history current and
marks all displayed unread messages in batches; undisplayed pages remain unread.

Python and TypeScript clients expose claim/ack/fail/requeue, summary, and read marking.
Python `deliver_inbox(agent_id, project_id, run_id, handler)` performs one eligible
poll, claims each item, acknowledges successful handlers, and schedules failed handlers
without capturing exception diagnostics. Call it again to perform later retries.
Handlers exceeding the lease or performing side effects before crashing still require
application-level deduplication. It does not renew leases or guarantee exactly-once work.

MCP provides `hivemind_claim_message`, `hivemind_ack_message`, and
`hivemind_fail_message`, alongside the existing non-mutating inbox reader.
No paid agent execution is required for these features or their automated tests.
