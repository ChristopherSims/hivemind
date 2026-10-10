# Persistent execution worker

`hivemind.agent_worker.AgentWorker` is an opt-in, long-lived Python loop that lets one
agent **run** for a while: poll for assignments, drain messages and review notifications,
keep its run present, renew its file claims, and drive the durable task lifecycle
(`start` -> `checkpoint` -> result) through the existing Python SDK.

It is deliberately narrow:

- It launches **no** paid, vendor, or model session and installs no hooks. Disabled
  construction is inert. Enabled execution starts a bounded maintenance thread for
  heartbeats, claim renewal, and pending-control checks while the adapter runs.
- It never executes task text. Task titles, descriptions, message bodies, board text,
  memories, and graph results remain untrusted evidence: they are passed **as data** to a
  host-supplied runtime and never interpreted as source. The module contains no
  `exec`/`eval`/`compile`/`subprocess`.
- It is a **cooperative** client loop, not an OS sandbox, scheduler, or exactly-once
  guarantee. All authority stays with the existing service: revision fences, execution
  tokens, scope permits, and controls.

## Runtime protocol

Execution is delegated to an injected runtime adapter:

```python
class Runtime:
    def execute(self, task, context, checkpoint) -> dict:
        """Return a structured result for the current task."""

    # Optional:
    def handle_message(self, receipt) -> None: ...
    def review_notification(self, notice) -> None: ...
```

`execute` receives:

- `task` - the current task detail just returned by `start` (`id`, `title`, `description`,
  `scope_ids`, `revision`, `execution`). Untrusted evidence.
- `context` - a plain dict with `project_id`, `agent_id`, `run_id`, `task_id`,
  `execution_id`, `title`, `description`, `scope_ids`, `claim_id`, `resumed`, `progress`,
  `poll_interval`, and `should_stop` (a zero-argument callable).
- `checkpoint` - a callable the adapter calls to durably record progress:
  `checkpoint(summary, progress=None, status="running", evidence_refs=None)`. It re-reads
  task state, verifies the execution token, and posts `tasks/{id}:checkpoint`. While a
  cooperative stop is requested, a `"running"` checkpoint is recorded as `"paused"`.

## Integrated journal and CLI

`hivemind.agent_session.JournalClient` applies the recovery journal to writes while
keeping detail/context reads live. `WorkerJournal` records transition/file checkpoints
and marks runtime effects uncertain if the process exits before recording an outcome.
An uncertain runtime effect blocks automatic restart, rather than guessing whether
external side effects should be repeated. `SessionLock` enforces one CLI owner per
journal and is released by the OS after a process crash.

```powershell
.\.venv\Scripts\hivemind.exe worker --config private-agent.json `
  --adapter your_trusted_module:create_runtime --journal agent-state.sqlite `
  --workspace G:\Droid\YourProject --iterations 1 --enable
```

The private config needs `base_url`, `token`, `project_id`, `agent_id`, and `run_id`.
Optional `runtime_options` go only to the explicitly selected trusted factory, and
`checkpoint_files` selects contained files to hash. The factory receives its options
and returns the runtime protocol above. Without `--enable`, no config/plugin is loaded.
Do not select a paid or outward-writing adapter unless separately authorized. The
orchestrator's tests use only injected local runtimes.

The worker re-reads an existing run assignment before asking for fresh work. Live
pending controls gate message handlers as well as task execution; maintenance detects
new controls during a long-running adapter call. Stopping remains cooperative.

The CLI defaults to assembled context, pending verification submissions, the stronger
checkpoint-backed lifecycle boundary, and message lease maintenance. Library users
select these via constructor options. Scheduling failures use the scheduler's bounded
retry budget. Task-runtime exceptions with a journal stop as unknown outcomes.
Message/notification side effects still require stable-ID application deduplication.

`execute` returns a dict. Recognized keys: `status` (`running`, `paused`, `completed`,
`failed`; anything else falls back to `completed` when `ok` is truthy, otherwise `failed`),
`summary`, `progress` (0-100), and `evidence_refs` (board-post/artifact refs). A bare
string is treated as a completed summary. `completed` is normalized to progress 100 and a
`failed`/`completed` result is terminal for that task. An exception raised by the adapter is
recorded as a `failed` checkpoint whose summary names only the exception **type**
(`Runtime adapter raised RuntimeError`); its message may contain secrets and is not sent.

The adapter owns the actual work and any file mutations. File authority still comes from
live claims and `runs/{id}/permits:check`; the worker does not widen scope.

## Loop behavior

`AgentWorker(client, project_id, agent_id, run_id, runtime, enabled=True).tick()` runs one
bounded cycle and returns a structured report. `run(iterations=N)` runs N cycles;
`run()` loops until `stop()`. `enabled` defaults to `False`, so construction performs no
requests.

Each cycle:

1. **Heartbeat** `POST runs/{run_id}/heartbeat`.
2. **Renew claims** near expiry (`claim_renew_margin`, default 60s) via
   `POST work-claims/{id}:renew`.
3. **Messages** - only when the runtime exposes `handle_message`: one `deliver_inbox`
   poll claims, handles, and acknowledges. Without a handler the inbox is left untouched
   so nothing is silently consumed.
4. **Review notifications** - only when the runtime exposes `review_notification`: each
   notice is handled and then `notifications/{id}:ack` is called. Without a handler,
   notices stay unacknowledged.
5. **Controls** - `GET agents/{id}/controls`. A live `pause`/`cancel`/`steer` control for
   this run halts new work; the worker does **not** acknowledge or apply controls (that is
   the controlled adapter's job). Full typed control application is unchanged.
6. **Assignment** - when idle, `POST projects/{project}/tasks:dispatch` with
   `{"agent_id", "run_id"}`. The reply is `{"task": {...}, "claim": {...}}` or
   `{"task": null}`. A scheduler surface that is not installed (HTTP 404/405/501) is
   treated as idle. Other HTTP errors surface to the caller.

### Task transitions

Before every transition the worker re-reads current state and obeys it:

- `GET tasks/{id}`; a terminal task is skipped, and a task whose `execution.run_id` is
  another run is skipped.
- `GET runs/{run_id}`; a run already assigned to a different task is skipped.
- A scoped task requires an `exclusive_owner` claim for this run that covers the task's
  scopes; an empty-scope task needs none. Otherwise the task is skipped (`scope_not_owned`).
- Recovery: if the task already carries this run's execution token, `start` is called with
  `recover=True` and prior progress is preserved. A matching journal resume hint also
  forces `recover=True`.
- Results are committed as one fenced checkpoint. Progress never regresses and `completed`
  is always 100.

If a `403` (`control_pending`, `scope_not_owned`, `mutation_not_permitted`) or a stale
execution token is observed mid-flight, the worker aborts that task without inventing a
result and records `halt_reason`.

## Cooperative bounded stop

`stop(reason=None)` sets a thread-safe flag. It does not interrupt an in-flight `execute`;
the adapter is expected to poll `context["should_stop"]` (or a requested checkpoint) and
return promptly. After the adapter returns, any pending `"running"` result is recorded as
`"paused"`, and `run()` exits at the next check. `run(iterations=N)` is bounded even if no
stop is requested. Stopping does not release file claims; the explicit claim lifecycle is
unchanged, so a claim simply expires or is renewed/released explicitly.

## Optional journal (duck typing)

A separate journal/recovery module may be injected; the worker imports nothing from it and
assumes no internals. It only probes attributes:

- `append(entry)` **or** `record(entry)` - called with a dict per transition
  (`kind` in `start`/`checkpoint`/`result`, plus `project_id`, `agent_id`, `run_id`,
  `task_id`, `execution_id`, `checkpoint_id`, `status`, `progress`, `at`).
- `recover(run_id)` - called once at construction; a returned dict is used as a resume
  hint (`task_id` matching the dispatched task forces recovery).

Missing methods are no-ops, so `journal=None` is fully standalone: recovery is still
driven from the task's own durable state. A journal that raises propagates (fail closed on
durability); pass `None` to opt out.

## Usage

```python
from hivemind.sdk import Client
from hivemind.agent_worker import AgentWorker

client = Client(base_url, token)

class MyRuntime:
    def execute(self, task, context, checkpoint):
        checkpoint("reading inputs", progress=10)
        if context["should_stop"]():
            return {"status": "paused", "summary": "stopped", "progress": 10}
        # ... perform the agent's scoped work; record evidence-backed checkpoints ...
        return {"status": "completed", "summary": "Export implemented", "progress": 100,
                "evidence_refs": [{"kind": "board_post", "id": post_id}]}

worker = AgentWorker(client, project_id, agent_id, run_id, MyRuntime(),
                     journal=None, enabled=True, poll_interval=5.0)
worker.run()          # until worker.stop()
```

## Limitations

- Cooperative only: the worker cannot preempt a runtime that ignores `should_stop`, and it
  does not roll back completed writes. Pause/cancel stop subsequent work, not history.
- The dispatch endpoint and its reservation/claim policy are owned by the scheduler
  surface, not this module. `task: null` means idle; uniqueness/idempotency of a dispatch
  reservation is the scheduler's contract.
- Checkpoint summaries are self-reports, not verified results, and file permits/claim
  epochs remain the authority for actual mutations.
- Messages and notifications are not consumed without a runtime handler, to avoid
  dropping evidence a real processor still needs.
- There is no automatic task dispatcher, lease renewal for messages, retry scheduler, or
  model execution here; the worker persists only server state, not an arbitrary call
  stack, secret state, or hidden reasoning.
