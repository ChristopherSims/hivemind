# OMP headless agent swarm — live test

Real model turns from three Oh my Pi agents coordinated through a running Hivemind
service: identity, scoped claims, capture hooks, direct tool use in a shared
workspace. The example project is a small TODO REST service built in deliberately
interlocking parts (`store.js` → `validate.js` → `app.js` → `README.md`), so each
agent must read files the previous agents wrote.

This is the executable version of plan §27's milestone scenario:
capture → claims → file edits → retrieval → README references prior work.

Reference-runs evidence: `benchmarks/omp-swarm-live-README.txt`.

## Prerequisites

- The local service running against an initialized data directory:

  ```powershell
  .\.venv\Scripts\hivemind.exe --data-dir .hivemind-swarm-data init --title "OMP swarm"
  .\.venv\Scripts\hivemind.exe --data-dir .hivemind-swarm-data serve --port 8765
  ```

- Bun + `omp` installed and **configured with a working model** (check
  `~/.omp/agent/config.yml` `modelRoles.default` or pass `--model`).
- A model provider — each agent turn is a real model call. The reference run
  used `ollama-cloud/glm-5.3-flash:low` (four small turns, ≈5 minutes total).
- The repository checkout (`adapters/oh-my-pi/hivemind.ts` must exist).

## Run

```powershell
# provision only (no model turns; verify identities/scopes/tasks first)
.\.venv\Scripts\python.exe scripts\omp_swarm_live.py --dry-run

# full swarm: three OMP sessions (sequential dependency order) with the capture extension
.\.venv\Scripts\python.exe scripts\omp_swarm_live.py
```

Useful flags:

| Flag | Purpose |
| --- | --- |
| `--data-dir DIR` | Running service's data directory (default `.hivemind-swarm-data`) |
| `--workspace DIR` | Scratch area for the generated repo + agent-private configs (default `.hivemind-swarm`) |
| `--model NAME` | Override every agent's model |
| `--timeout S` | Per-turn wall clock (default 300) |
| `--skip-messages` | Reserved for the follow-up messaging stage |

The script is idempotent: reruns reuse the same project/agents and existing
scopes (`scope_key_exists` falls back to a canonical lookup), then re-create
task records for the turn (task creation is per-stage, one set per run).

## What it does, in order

1. **Provisioning** — registers three agents (`Builder`, `Backend`, `Documenter`),
   issues per-agent credentials into owner-only private files
   (`.hivemind-swarm/agent-private/**/credential.json`, never committed), and
   declares four canonical scopes pinned to exact paths:
   `code/store → src/store.js`, `code/validate → src/validate.js`,
   `code/app → src/app.js`, `docs → README.md`.
2. **Scope assignment** — each agent's private config lists the scopes its
   hooks session will claim: Builder `store+validate`, Backend `app+validate`,
   Documenter `docs` plus read coverage of the code it must document.
3. **Tasks** — four tasks record the work, one per scope.
4. **Capture daemons** — one `hivemind.capture_daemon` per agent drains that
   agent's write-tool spool every 5 s, reports capture health, and renews claims.
5. **Model turns** — `omp -p` sessions with the Hivemind extension
   (`-e adapters/oh-my-pi/hivemind.ts`), **in dependency order**
   (`store.js` → `validate.js` → `app.js` → `README.md`). Each `session_start`
   claims its scopes; each write is permit-checked then spooled:
   `tool.started → file.changed → tool.completed`.
6. **Review** — queries the ledger (runs, presence, capture health) and the
   conflict list; writes `swarm-report.json` into the workspace root.

## Expected evidence on success

After the run, the service shows — within seconds of the model turns:

- `GET /v1/ledger?project_id=…`: one run per session with
  `capture_health.state=healthy`.
- `GET /v1/events`: `tool.started` / `file.changed` / `tool.completed` per file,
  attributed per agent with `confirmed` tool-provenance.
- `GET /v1/work-claims?project_id=…`: claims `active` (renewed by the daemon),
  one owner per scope.
- `GET /v1/conflicts?project_id=…`: at most advisory candidates — none denied,
  because the scopes pin disjoint paths and Backend's two assignments overlap
  only inside the same agent.
- `GET /v1/agents/{id}/context`: each packet contains the run state, that
  agent's files, its claim, and graph links (`TOUCHED`, `CLAIMS_SCOPE`).
- Semantic search (with `--embeddings`) for "atomic todo persistence" retrieves
  `src/store.js` from the captured events/board/memory, not only the board.
- The generated repo under `--workspace`: `src/store.js`, `src/validate.js`,
  `src/app.js`, `README.md` with curl examples matching the actual routes.

## First-run findings (context-sufficiency gaps agents hit for real)

The first run (parallel turns, partial scope assignment) exposed exactly what
the platform can improve for agents. Each item is actionable in the
implementation:

1. **Dispatched task text never reaches a bare `omp -p` turn.** Tasks existed
   server-side, but the model only saw what the dispatcher pasted into the
   prompt. The assembled context packet (`POST /v1/context:assemble`) should be
   injected as part of the turn (the CLI worker does this; a plain headless
   session does not). Feature: *packet-injection for headless runtimes*.
2. **Dependency gating was manual prose.** The `app.js` prompt had to say
   "read src/store.js and src/validate.js first; they exist already" — and
   still failed when they did not exist. Task-plan dependencies +
   `GET /v1/tasks/{id}/blocked-status` already encode the readiness facts;
   a dispatch layer should hold the turn until upstream tasks report completion
   and then deliver "your upstream modules are ready" automatically.
   Feature: *dependency-aware turn gating*.
3. **Agents could not see each other.** Nothing told the Documenter that the
   Backend had not yet written `validate.js` (it went looking and reported the
   files "absent"); nothing told Backend where Builder's progress stood. A
   compact roster card in the packet — name, ID, current task, presence,
   file-touch evidence (§ agent roster) — would make waiting/coordination
   self-evident. Feature: *teammate cards in assembled context*.
4. **Blocked writes left no trace.** Backend's rejected write attempt produced
   no event, no conflict, no spool entry — the agent just learned the permit
   failed. A `coordination.blocked_write` observation (agent-reported, no
   authority granted) would make the friction visible to the coordinator and to
   the next reviewer instead of lost in a stderr string.
   Feature: *denied-mutation events* (advisory).
5. **Task status went stale.** Agent runs ended without task progress updates
   (`POST /v1/tasks/{id}:checkpoint` / terminal `dispatch-outcome`): the ledger
   still showed tasks as `queued` after the agents finished. The hooks session
   knows the task binding; it should checkpoint on `tool_result` completion.
   Feature: *adapter-side automatic checkpointing*.
6. **README quality depended on code the agent could half-see.** The
   Documenter's first turn correctly refused to write (its stdout said so) —
   good fail-closed behavior — but it had no *wait for dependency* primitive;
   it could only guess or exit. Feature: a `wait_until`/`block_on` task state
   exposed through the worker MCP surface.

After fixing scope coverage (Backend owns `code/validate` too) and running the
turns in dependency order, the second reference run completed end to end:
see `benchmarks/omp-swarm-live-README.txt` for per-turn durations, per-file
capture provenance, and the final ledger state.

## Follow-up run findings (post-`bdeb73c`, live verification)

The first reference runs exposed two real platform defects, both fixed and now
covered by tests:

- **A claimless session stuck forever.** The hooks bridge wrote the session
  mapping before creating the initial claim; when that POST failed (e.g. a
  `ttl_seconds` above the 600 s schema maximum), every later invocation reused
  the claimless mapping and every write was denied. Fixed in
  `hivemind/hooks.py`: the mapping is written only after the claim exists, and
  a mapping found without a claim retries creation on the next invocation.
  Regression: `tests/test_event_gap_closure.py`, plus core tests.
- **The same agent's finished-session claim denied its own fresh session.**
  Claims from dead sessions kept exclusive ownership until expiry, so an
  agent's next legitimate session hit a deterministic denial against itself.
  Same-agent re-claims now supersede (`superseded` + `superseded_by_run_id`)
  instead of denying; cross-agent denials are unchanged.
  Regression: `tests/test_core.py::test_same_agent_new_run_supersedes_own_expired_session_claim`.

One model-behavior finding (not a platform bug): the flash-tier model sometimes
answers implement-prompts in chat without calling the write tool. Prompts with
the proven shape ("Implement … Export {…}. Nothing else.") produced writes; a
platform-side mitigation is the headless packet injection from finding 1
(explicit tool-use instruction inside the assembled packet).

Full findings and measured evidence: `benchmarks/omp-swarm-live-README.txt`.
