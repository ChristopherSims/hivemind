# Agent activity subscriptions (SSE)

`hivemind/activity_stream.py` adds an authorized, per-project Server-Sent Events
stream of the agent activity roster — the same projection as
`GET /v1/agents` (see `docs/agent-activity-roster.md`) — pushed as typed frames
so dashboards and coordinators can react to completion, blocking, and
availability changes without polling.

The feature is **default-disabled**. Installing with an enabled
`ActivityStreamPolicy` registers the route and starts the bridge; without a
policy the route exists but returns `feature_disabled` (503) and no hub, bridge,
or thread is created. A reinstall replaces only this module's own routes.

```http
GET /v1/projects/{project_id}/agents:stream?after=<cursor>
Authorization: Bearer <token>
Accept: text/event-stream
```

## Frames

Every data frame carries a scoped cursor as its SSE `id` and a JSON object with a
`type` matching the event name. Frame, line, and stream sizes are strictly
capped; snapshots are chunked (`snapshot_chunk` agents per frame, with
`index`/`count`).

- `snapshot`: `{type, index, count, agents[], truncated}` — the full authorized
  roster baseline, emitted on every (re)connect and after any `resync`
  advisory, so a subscriber never sits silent wondering whether it missed
  history. Baselines are hard-capped at 1000 agents; excess is flagged
  `truncated: true` (reread paginated `/v1/agents`), never silently dropped.
- `activity`: `{type, agent_id, change, removed, agent}` — one agent's fresh,
  visibility-filtered roster entry. `change` is one of `status`, `task`,
  `presence`, `roster`, `withdrawal` (a generic invalidation with
  `agent_id`-less attribution only when a full recompute is required). `agent`
  is `null` with `removed: true` only for an agent that was previously visible
  in this stream and no longer is.
- `resync`: `{reason}` — `cursor_outside_window` (retention) or `backpressure`
  (overflow). Always followed by a fresh `snapshot`; the client must drop any
  state derived from skipped events.
- `shutdown`: `{reason}` — the hub is closing; reconnect after the service
  restarts.
- `: heartbeat` comments — liveness only; each one is preceded by a fresh
  authorization check.

Content-identical repeats are deduplicated per stream (compare the roster
entry), so consumers should still treat delivery as **at-least-once** and dedupe
by `(agent_id, revision, presence)`.

## Cursors and replay privacy

Cursors are scoped so that observing one never reveals traffic a principal may
not see:

- admins: `g<n>` — a project-wide log cursor (admins are authorized for all
  scopes);
- agent principals: `s<n>.a<m>` — shared-scope plus own-agent-scope positions;
- other project members (human/observer): `s<n>` — shared scope only.

Reconnect with `?after=<cursor>` or `Last-Event-ID`. A cursor from a different
viewer class or a malformed cursor is refused with 422 **before** response
headers, as is subscriber admission (429 `activity_subscriber_limit` past
`max_subscribers_per_project`). Replayed events are filtered by scope
visibility; a private run/task event lives in its agent's scope and is never
delivered to — or counted in — another principal's cursor. The durable log
(`activity-stream.sqlite` beside the canonical store, its own file) retains
`replay_limit` events per (project, scope); older cursors get an explicit
`resync` rather than a silent gap.

## What drives events

- **Commits** (`Store.subscribe_commits`): committed run/task/agent writes and
  deletions, plus board_post/memory writes (checkpoint evidence sources whose
  revocation can withdraw a derived summary). The bridge extracts agent identity
  and change class only — bodies are parsed and discarded, never retained — and
  advances a durable per-project watermark in the same transaction as the event
  inserts, so a crash between commit and publish is repaired by the next scan
  and a rolled-back transaction publishes nothing. Message traffic is never
  scanned and never counted anywhere. A **bounded periodic catch-up**
  (round-robin over watermarked + currently-subscribed projects,
  `catchup_batch` per pass) retries a failed scan even when no further commit
  ever arrives — wake-ups are an optimization, the durable watermark is the
  authority. A first subscription to a project unseen since binding catches up
  from zero.
- **Scope transitions**: the hub durably records each resource's visibility
  scope (`resource_scopes`). When a previously *public* resource becomes
  private, the bridge prepends a generic shared `withdrawal` invalidation (no
  ids) so public caches recompute and drop the stale summary promptly; the
  content-bearing event stays in the agent scope and never touches shared
  cursors. A private-A→private-B move withdraws only A's scope (A drops the
  cached summary; B's traffic never surfaces in shared cursors); private→public
  needs nothing (the shared event itself reaches the old audience). Resources
  first seen as private never affect shared cursors at all.
- **Baseline scope adoption**: every emitted `snapshot` baseline CAS-adopts the
  visibility scopes of the sources it shows (runs, current tasks, and their
  checkpoint evidence refs — collected under the *same* canonical snapshot as
  the roster, via SQL metadata projection only, never body reads, never foreign
  projects, hard-capped at 16384 ids with fail-closed semantics). Adoption is
  insert-only (`INSERT OR IGNORE`), so it can never overwrite scanner history;
  if the scanner already moved a resource before adoption landed, adoption
  emits the withdrawal the scanner skipped. A public task that existed before
  install and is privatized before the bridge first scans it is therefore still
  withdrawn promptly from live baselines — no presence wait, no reconnect.
- **Catch-up integrity**: watermarks advance monotonically (`MAX(seq, new)`),
  so two concurrently running bridges can never regress each other's receipt.
- **Presence timer** (bounded `presence_interval`): the canonical heartbeat
  route rewrites run bodies *without* `Store.tick`, so no commit signal exists
  for liveness. The timer re-derives per-run presence (online/stale/offline,
  matching roster thresholds) from `last_seen_at` and publishes only
  *transitions* — including the purely time-driven online→stale→offline
  expirations. It is keyset-paginated per interested project (`presence_batch`
  runs per project per tick, round-robin page cursors) and SQL-projects only
  the needed fields (`agent_id`/`status`/`last_seen_at`): full run bodies and
  foreign projects are never read. It is presence maintenance/catch-up only;
  it never polls task/run bodies.

Every frame (snapshot, replay, live, heartbeat) is preceded by a fresh short
canonical snapshot verifying that the principal still exists, still matches its
connect-time fingerprint (role, agent, project, token_hash), and still has
project access; each projection is recomputed through the authorized
`activity_roster` view. Revocation, credential rotation, or a role/agent
downgrade closes the stream silently at the next frame or heartbeat — and a
stale higher-class cursor (e.g. an admin `g<n>` after downgrade) is refused
with 422, never counted against scopes the principal can no longer see.
Hidden/deleted/private runs and tasks are dropped by the projection itself, so
prior summaries or ids are never re-emitted. The route is marked
`hivemind_detached_work`: all waiting happens on the subscription queue, never
while holding the parent's global request/store locks, so heartbeats and other
requests stay responsive while long streams are open.

## Install and shutdown contract for the parent

```python
from hivemind.activity_stream import ActivityStreamPolicy, install

install(app, service, principal, command, envelope,
        policy=ActivityStreamPolicy(enabled=True, ...))
```

- Handles for lifespan shutdown: `app.state.activity_stream_hub` and
  `app.state.activity_stream_bridge` (both `None` when disabled); call
  `close()` on each. `hub.close()` terminates every open stream with a
  `shutdown` advisory. `bridge.close(timeout)` is honest about liveness: it
  returns `True` only when the daemon thread is actually dead — a timed-out
  close returns `False`, leaves the handle retryable, and means the lifespan
  must NOT tear down the hub/store the bridge still uses.
- Registered automatically by `create_app`; enable with
  `integration.json` → `"activity_stream": {"enabled": true}`.
  Python installation options key `activity_stream`
  accepts `policy`, `path` (log location), and `bridge=False` to bind
  `ActivityBridge(store, hub)` manually. `OPERATION_DESCRIPTORS` /
  `descriptors()` self-describe `activity_stream.stream`,
  `activity_stream.commit_bridge`, and `activity_stream.client_subscription`.

## Python SDK subscription helper

The Python SDK wraps the cancellable helper:

```python
subscription = client.subscribe_agent_activity(
    project_id, cursor_path=".hivemind-data/activity-cursor.json")
for frame in subscription.frames():  # {"id", "event", "data"}
    ...
subscription.close()  # bounded; interrupts an active read within read_timeout
```

- `frames()` blocks and yields typed frames; it reconnects from the durable
  cursor after transient transport failures and returns on a clean server-side
  close (shutdown or the bounded stream lifetime — reconnect to continue).
- The cursor file is atomic (tmp + `os.replace`), strictly shaped, and bound to
  the exact project id; a foreign or malformed file is refused, never trusted.
  A server `resync` advisory resets the local cursor so the following snapshot
  re-baselines state.
- `parse_activity_sse` / `httpx_activity_subscription` are the lower-level
  primitives (bounded line/frame bytes, capped frames per stream, fixed-text
  `ActivityStreamError` that never echoes remote bytes). The push module's
  strict ID/cursor primitives are reused read-only; `message_push.py` is
  unchanged.

The TypeScript SDK exposes `subscribeAgentActivity(projectId, {after, signal,
readTimeoutMs})` as a bounded async generator. Persist yielded frame ids to resume;
it does not automatically reconnect or write a cursor file. Both clients validate
16 KiB line/frame limits. Server snapshots use byte-aware chunks, not just counts.
The server reauthenticates the original bearer or session on every read window;
credential rotation, session revocation/expiry and principal changes close streams.

## Limits

Delivery is at-least-once with no exactly-once guarantee. Presence resolution
is bounded by `presence_interval` (plus the 15s/30s staleness thresholds), not
instantaneous. A presence transition for a run the bridge has never seen is
adopted as baseline (only transitions are published), so a bridge restart
republishes at most current state. The roster projection is authoritative but
reported activity, not process attestation. Large rosters are chunked into
bounded snapshot frames and hard-capped at 1000 agents (honestly flagged
`truncated`); an oversized single entry is deterministically trimmed (run list,
then summary) to fit the frame budget. A baseline whose visible source set
exceeds 16,384 ids fails closed with a `baseline_sources_exceeded` resync
advisory (poll `/v1/agents` instead) — an untracked baseline is never
silently accepted. Checkpoint evidence adoption tracks at most 64 refs per
task; withdrawals for truncated refs still arrive via the task's own change
events.
