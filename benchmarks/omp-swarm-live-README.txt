OMP headless agent swarm live test, 2026-10-20
==============================================

Setup: 1 Hivemind service (Windows, loopback 8765, no embeddings for the reference
runs), 3 registered OMP agents (Builder / Backend / Documenter, ollama-cloud
glm-5.3-flash:low), 4 exact-path scopes, 4 tasks, 1 capture daemon per agent,
OMP 18.12.3 headless `-p` sessions with the capture extension.

Recipe: docs/agent-swarm-live-test.md. Script: scripts/omp_swarm_live.py.

Four full runs + targeted completion turns (every run restarts sessions; all
real model turns; no scripted file writes).

Final generated workspace (all content written by the agents through the
write tool under scoped claims):
  src/store.js  — node:fs/promises load/save with tmp+rename atomic write (ESM)
  src/validate.js — validateTodo -> {ok, errors}; non-empty title; optional bool done
  src/app.js  — Express; GET/POST /todos, PATCH/DELETE /todos/:id; 400/404 handling
  README.md   — run instructions + endpoint/curl documentation matching app.js

Canonical evidence after all runs (GET /v1/ledger, /v1/events, /v1/conflicts):
  32 sessions (runs), capture health: healthy
  tool calls captured: read 482, glob 116, grep 94, write 4 (all completed)
  event types: tool.started 48, tool.completed 48, file.changed 4,
               agent.status_reported 3 per session
  conflicts: 18 deterministic denials (exact/structural overlap between parallel
             runs of different agents AND same-agent pre-supersession), 0 candidates
             (flash model did not produce similar-but-unequal intents this run)
  claims: final 2 active (others superseded/expired); same-agent new-run
          supersession verified live (claim 28bff8 -> superseded by run …aba7)

Findings (driving docs/agent-swarm-live-test.md "context-sufficiency" work):
  F1 dispatched task text does not reach a bare headless turn (needs packet injection)
  F2 dependency gating was manual prompt prose; parallel turns burned model time
  F3 agents had no teammate visibility (roster cards absent from packet)
  F4 denied writes leave no canonical trace (silent refusal; agent paraphrases it)
  F5 task status stayed "queued" after successful work (adapter checkpointing absent)
  F6 no wait/until-upstream primitive (Documenter first-run refused correctly but
     could only exit; flash model in later turns documented endpoints from prompt
     prose while app.js was absent — README was later corrected by the agent)
  F7 claim TTL (max 600s) shorter than long model turns; renewal depends on the
     daemon being alive; a claimless mapping previously stuck forever (fixed in
     hivemind/hooks.py: claim before mapping write + claim retry on next invocation)
  F8 flash-tier model sometimes answers implement-prompts in chat without calling
     the write tool (behavior finding; prompts that say "Nothing else." worked)

Environment gates: no live vendor sandbox; the OMP restriction check remains
the cooperative hooks bridge (documented); model turns are paid calls.
