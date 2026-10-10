# Real Factory coordination test

This second opt-in scenario extends a previously passing real-agent issue tracker.
It does not rebuild the app or replace live agents with fixtures.

## Run

```powershell
cd G:\Droid\Hivemind
.\.venv\Scripts\python.exe -m scripts.factory_coordination --live --auto high `
  --baseline G:\Droid\Hivemind\.hivemind-test\factory-live_01a12353-649d-78ee-a720-0e31aeaa4110
```

The baseline must have a passing `report.json`, and the API, UI, and test hashes
must still match that report. Only those five files are copied into a new ignored
`factory-coordination_*` workspace. Original files, credentials, settings, and
databases are not changed. Use your own passing `factory_integration` run as the
baseline for future runs.

Optional model and timeout flags match the [first integration test](factory-integration.md).
There are at most four real Factory executions, five minutes per execution by
default, and 35 tool attempts per session (including the resumed worker's earlier
attempts). These are not monetary/token spending caps. High autonomy is explicit;
there is no unsafe permission bypass. Generated code executes locally with a
minimal environment, **not in an OS sandbox**.

## Scenario

1. **Owner:** Exclusively claims `api/ownership.txt`, writes its ownership note,
   publishes an export-format board finding, and proposes a project memory backed
   by that actual finding. A fresh random format marker is supplied only to this
   owner, outside the shared workspace.
2. **Local review:** The harness checks the memory format/evidence and verifies
   that version. Agents cannot self-verify it. The marker must not have appeared
   in any workspace file.
3. **Worker:** Starts with `scratch/`, attempts one conflicting exclusive API claim,
   receives the real denial, reports the conflict to Nexus, and saves its progress.
   The harness independently checks that the same credential cannot obtain a
   permit for the protected API path.
4. **Nexus:** A separate real Factory agent with an explicitly granted coordinator
   credential reads the conflict, acquires a leased role, and submits a typed,
   revision/epoch-checked steer to a nonoverlapping `reports/` task.
5. **Same worker resumed:** The original Factory session resumes without the new
   task contents or marker copied into its prompt. Its hook processes the control;
   read-only context discovery exposes the pending task. The worker queries the
   verified memory and implements an export program in the replacement scope.
6. **Independent verification:** Control history must be queued → acknowledged →
   applying → applied. The checkpoint must match the runtime result, and actual
   replacement-file changes must have been captured. The API and old progress
   hashes must remain unchanged. The worker must cite the precise verified memory
   version and reproduce the hidden marker in its output. CSV quoting is tested,
   followed by the baseline API, persistence, agent-written tests, and Chromium checks.

No harness call issues, acknowledges, checkpoints, or applies the steering control.
Those actions belong to the live Nexus agent and the actual worker adapter.
Apart from initial identity/scope/task setup, the harness only reviews memory;
it does not steer the worker.

## Retrieval receipts and context

`audit_context_queries` is opt-in adapter configuration. For successful native
`hivemind_query_context` calls, it records caller identity, resource IDs/revisions,
timestamp, and request/packet hashes in a private JSONL file. It does not record
memory contents or query text. The scenario verifies that the worker's adapter
actually returned the precise verified memory revision before accepting reuse.

The MCP context tool accepts `include` to request just task/control or memory data.
Pending replacement tasks remain evidence, not committed run assignments: they
are exposed while a live control is queued, acknowledged, or applying. The run's
task only changes after observed replacement work. Read-only discovery may consume
a steering checkpoint without falsely applying it; an old-assignment mutation
still fails closed.

## Artifacts

- `report.json`: sessions, execution limits, conflict, control/checkpoint, memory
  receipt, export, and application results.
- `coordination-evidence.json`: canonical claims, conflicts, runs, memories,
  messages, posts, events, and immutable control versions.
- `context-receipts.json`: worker retrieval receipts, without context contents.
- `conversations/*.log`: pair and per-thread transcripts, plus `main-board.log`.
  Paths print during execution; empty pair files explicitly show no direct exchange.
- `*.stdout.log` / `*.stderr.log`: actual Factory and validation process output.
- `export-probe.stdout.log`: the actual export program's JSON/CSV result.
- `issue-tracker.png`: real Chromium evidence from the copied, still-working app.

Artifacts remain on failure. Treat all canonical/session exports as private and
review before sharing. This is not a semantic embedding-relevance, live Oh my Pi,
or full host-conformance test.

## Worker interruption and service restart

Add `--recovery` to the command above. This variant interrupts the real worker
process tree only after its scratch edit has completed and reached canonical
storage. The intentionally interrupted execution is recorded separately from
unexpected failures. Its real session ID comes from the runtime hook, not a
synthetic session or a replacement identity.

The runner stops publishing, shuts down Hivemind, closes its stores, and starts a
fresh app/server instance on a new loopback port using the same persisted data.
Private adapter connection settings are updated without changing credentials.
Message IDs/revisions, ownership epochs, run identity, and captured file state
must survive. Publishing resumes before the real Nexus and resumed-worker stages.

The same worker must read its inbox again and report the original pending message
ID to the board. The canonical message must exist exactly once, and the original
scratch edit must have exactly one captured observation and an unchanged hash.
The normal steering, verified-memory reuse, export, and application checks also run.

This covers process interruption after a completed durable edit and a clean
service restart. It does not cover server kill-during-transaction, mid-write
interruption, or delivery acknowledgment/exactly-once action guarantees. Reading
an inbox is passive; this scenario intentionally leaves messages unacknowledged.
