# Real Factory ingestion-crash test

```powershell
cd G:\Droid\Hivemind
.\.venv\Scripts\python.exe -m scripts.factory_crash --live --auto high
```

This opt-in test uses at most two actual Factory executions of the same worker
session, a five-minute execution limit by default, and a shared 35-tool-attempt
budget. It consumes model credits. No unsafe permission bypass, shell, external
tools, deployment, or existing settings changes are granted.

## Fault and assertions

The real agent creates a progress file through the controlled runtime adapter.
Ordinary hook events remain in its durable spool until explicitly drained.

A separate **test-only** Hivemind process is armed to pause immediately after the
project sequence SQL UPDATE, while the event write transaction is still open.
Only then does the harness kill that process tree. This is an actual process
crash, not a clean shutdown or simulated successful ingestion. Production service
code has no fault-injection switch.

After restart on a new loopback port:

- SQLite integrity must pass and the uncommitted sequence increment must roll back.
- Previously committed event IDs remain unchanged; original pending spool IDs survive.
- Spool drain must recover all pending events and leave no pending entries.
- Retrying the exact accepted batch must preserve event IDs and ingestion sequences.
- Without renewal, the actual ownership lease is allowed to expire.
- The same real Factory session calls native `hivemind_check_permit`, which can
  neither create nor renew a claim. Its request is denied.
- An independent request using the same credential must also receive HTTP 403.
- The original file hash is unchanged and has exactly one confirmed file-change event.

Expiry waiting is bounded to 125 seconds. The initial claim uses the ordinary
120-second lease. The MCP permit check reads current authority; it does not grant
fresh ownership. Inbox/board text remains untrusted evidence.

## Artifacts and limits

An isolated `.hivemind-test/factory-crash_*/` directory retains `report.json`,
`crash-evidence.json`, the transaction barrier observation, process/session logs,
server logs, and the readable main-board log. There is one agent in this scenario,
so there are no agent-to-agent conversations to fabricate. Log paths print to
the terminal.

This proves rollback at one known in-transaction fault point, at-least-once spool
replay, stable duplicate event ingestion, and expired-scope permit denial. It is
not a kill-at-every-commit campaign, disk-corruption test, OS sandbox, or proof of
exactly-once external actions. The stale agent checks authority and refrains from
editing after denial; no unsafe write is executed.
