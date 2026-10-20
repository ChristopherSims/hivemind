# Durable agent task execution

The task API now records pull-based assignment, execution fencing, the latest durable
checkpoint, monotonic progress, and explicit recovery. It does not execute scripts,
grant write scopes, or claim objective proof of completion. No dashboard or paid
agent sessions are required.

## Start and checkpoints

Create a task with the existing `POST /v1/tasks`. Acquire its exact scope IDs using
the existing work-claim API. Then:

- `POST /v1/tasks/{id}:start` takes `expected_revision`, `run_id`, and optional
  `recover` (default false).
- `GET /v1/tasks/{id}` reads current durable state.
- `POST /v1/tasks/{id}:checkpoint` takes `expected_revision`, `run_id`,
  `execution_id`, `checkpoint_id`, `status`, `progress`, `summary`, and optional
  board-post/artifact `evidence_refs`.

All commands require an `Idempotency-Key`. Start binds the task to the run's agent and
returns a fresh `execution.id`. Scoped tasks require live exclusive scope ownership;
empty-scope tasks grant no file authority. Pending controls and runtime status still
gate permits. A run cannot start a second task while its assignment remains active.

Checkpoint status is `running`, `paused`, `completed`, or `failed`. Progress is 0–100,
never decreases within an execution, and completion requires 100. Completion/failure
are terminal for that task and clear the run's task assignment. Claims are not
automatically released; the existing claim lifecycle remains explicit.
Checkpoint summaries are self-reports, not verified results or executable instructions.

`paused` records a cooperative task checkpoint, not a runtime pause command. The agent
must stop its own task work; actual runtime pause/resume/steering still uses the existing
Nexus/control protocol. A checkpoint cannot continue an assignment that steering moved
to another task.

Repeated command keys do not create new writes. Reusing the latest checkpoint ID with
different contents is rejected; use a fresh checkpoint ID for a new update.
Current revision and execution ID fence competing/stale updates.
An idempotent reply records the original acceptance, not necessarily the latest task
state; use the detail read before choosing the next transition.
The latest checkpoint is returned directly; ordinary immutable resource revisions
remain in canonical storage. Up to 20 previous execution snapshots are retained in
`execution_history`.

Evidence is pinned to canonical revisions. Deleted/changed/inaccessible evidence
redacts checkpoint summaries and references in detail, lists, and cached command
replies. Such task evidence cannot keep a dependent memory valid. Historic bytes may
remain in canonical revisions/backups under the existing retention limitations.

## Recovery

Resume after an application restart by reading the current task. To take over with
a new execution token, call start with `recover: true` and its current revision.
Recovery preserves prior progress and the latest available checkpoint.

Only the same agent can recover an existing execution. A different run must wait
until the old run is terminal or has not heartbeated for more than 30 seconds.
The new run must independently own the task scopes; stale presence does not revoke
the old file claims, so wait for expiry/release and acquire them explicitly.
The same run may explicitly recover cooperatively, fencing its old task token.

Old execution IDs and cached start receipts from an older execution cannot
commit checkpoints after recovery. This is cooperative recovery, not an OS sandbox
or proof that an abandoned process stopped. Existing file permits/claim epochs remain
the authority for actual mutations.

## Agent clients

Python provides `client.task(id)`, `client.start_task(id, run_id, revision,
recover=False, key=None)`, and `client.checkpoint_task(id, body, key=None)`.
TypeScript provides `task`, `startTask`, and `checkpointTask` with a typed checkpoint
request. MCP provides `hivemind_get_task`, `hivemind_start_task`, and
`hivemind_checkpoint_task`.

Store command keys locally before requests if the application needs retries across
its own process crash. This API persists server state, not an application's arbitrary
call stack, files, secret state, or hidden reasoning. There is no automatic task
dispatcher, lease renewal, retry scheduler, or model execution worker.
