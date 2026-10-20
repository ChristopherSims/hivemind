# Hivemind — findings and recommendations

**Source:** live OMP headless agent swarm (3 agents, real model turns), 2026-10-20.
**Evidence:** `benchmarks/omp-swarm-live-README.txt`, `docs/agent-swarm-live-test.md`,
commits `bdeb73c` (feature gaps) and `72ed681` (swarm run + claim-lifecycle fixes).
Status: open items only; resolved work is recorded in git history and the docs.

## Platform defects found by the live run (both fixed)

- [x] Claimless-session deadlock — hooks wrote the session mapping before the
  initial claim POST existed; if that POST failed (e.g. `ttl_seconds: 900`
  above the schema maximum of 600), every later invocation reused a claimless
  mapping and every write was denied forever.
  Fix: `hivemind/hooks.py` writes the mapping only after the claim exists and
  retries claim creation on the next invocation when the mapping lacks one.
  Tests: supersession/claimless-recovery regressions (`tests/test_event_gap_closure.py`,
  `tests/test_core.py::test_same_agent_new_run_supersedes_own_expired_session_claim`).
- [x] Same-agent self-denial across sessions — a fresh session of the same agent
  was denied by its own dead session's unexpired claim.
  Fix: same-agent re-claims supersede (`state: "superseded"` +
  `superseded_by_run_id`); cross-agent deterministic denial unchanged.

## Agent-facing feature recommendations (open)

- [ ] **1. Packet injection for headless runtimes** — dispatched task text never
  reached a bare `omp -p` turn; the model only saw the dispatcher's prompt.
  Wire `POST /v1/context:assemble` into the turn for headless/vendor sessions
  (the CLI worker already does this).
- [ ] **2. Dependency-aware turn gating** — parallel dispatch without dependency
  gates burned model turns on missing upstream modules; the prompt carried the
  dependency facts as prose ("read src/store.js first; it exists"). Hold a turn
  until upstream tasks report completion (`GET /v1/tasks/{id}/blocked-status`)
  and tell the agent "your upstream modules are ready" automatically.
- [ ] **3. Teammate cards in assembled context** — agents had no visibility of
  who held which scope, who was mid-file, or who was online; the Documenter
  explored an empty workspace without knowing Backend hadn't started. Add a
  compact roster card (name, agent ID, current task, presence, files touched)
  to each context packet.
- [ ] **4. Denied-mutation events (advisory)** — a blocked write produced no
  canonical trace (no event, no conflict, no spool entry); only the agent's
  own chat output recorded it. Add a `coordination.blocked_write`
  agent-reported observation so coordinators/reviewers see the friction.
- [ ] **5. Adapter-side automatic checkpointing** — sessions ended without
  `POST /v1/tasks/{id}:checkpoint` / terminal `dispatch-outcome`; the ledger
  showed tasks `queued` after the work finished. The hooks session knows the
  task binding; checkpoint on observed completion at a tool boundary.
- [ ] **6. Wait/block-on-upstream primitive** — agents that depend on a peer's
  output can only poll or exit (the Documenter correctly refused to document
  absent code but had no way to wait; a flash-tier turn later documented
  endpoints from prompt prose while `app.js` was absent and had to be corrected
  by a later agent turn). Expose a bounded `wait_until` task state through the
  worker MCP surface.
- [ ] **7. Claim TTL vs long model turns** — schema caps `ttl_seconds` at 600 s
  while real turns ranged 15–370 s (one stalled past 480 s); claims expired
  mid-turn whenever the capture daemon was not running. Options: allow a
  configured longer initial TTL, or surface "claim expires in Ns" inside the
  assembled packet so the run renew path is not the only lifeline.
- [ ] **8. Model-behavior mitigation (tool-call refusal)** — the flash-tier
  model sometimes answered implement-prompts in chat without calling the write
  tool (4 writes vs ~690 reads/globs). The proven prompt shape ("Implement…
  Export {…}. Nothing else.") worked; a platform-side mitigation is the
  packet injection from finding 1 with an explicit tool-use instruction inside
  the assembled packet.

## Verification notes

- Live evidence after the fixes: supersession verified against the running
  service (claim `28bff8` → `superseded` by the agent's new run); the completed
  example codebase (`store.js`, `validate.js`, `app.js`, `README.md`) with
  README endpoints matching `app.js`; capture health `healthy` in all sessions;
  fail-closed held in every ambiguous case (no fabricated writes).
- Remaining caveats intentionally not re-run here: no vendor OS sandbox, no
  macOS/Linux coverage, no third-party protocol conformance. See
  `docs/limitations.md` and the README "Alpha limitations" section.