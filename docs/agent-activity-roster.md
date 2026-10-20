# Agent activity roster

All API routes now use `/v1`. The former `/v0.1` routes are removed, not aliased.
Update client base URLs and cached tool-operation catalogs. The package and event
protocol versions are unchanged; this migration concerns HTTP API paths.

```http
GET /v1/agents?project_id=<project_id>
Authorization: Bearer <agent_token>
```

The response remains the usual envelope with an array of agent records. Each agent
now also includes:

- `status`: the selected run's reported status, or `idle` if no run exists.
- `current_task`: authorized task identity, title, status, revision, execution,
  progress and checkpoint summary, or `null`.
- `summary`: latest selected task checkpoint summary, otherwise an available
  agent-reported run summary, or `null`.
- `context`: selected `run_id`, heartbeat-derived `presence`, `last_seen_at`,
  summary origin, and up to 32 recent authorized run contexts. `runs_truncated`
  explicitly indicates that more runs exist.

A task-bearing nonterminal run takes priority over newer auxiliary/observer runs.
Otherwise the latest nonterminal run is selected, then the latest terminal run.
This preserves activity from independent messaging runs in `context.runs` instead
of pretending an agent can have only one live run.

Hidden/deleted tasks and stale execution bindings never become current tasks.
Checkpoint summaries retain canonical source-visibility checks. A hidden task's
derived run summary is not returned. Project authorization applies to the roster.

Status and summaries are canonical reported activity, not proof of an actual live
process or successful work. Presence is `online` below 15 seconds since heartbeat,
`stale` below 30 seconds, and `offline` thereafter.
