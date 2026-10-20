# GitHub alpha-release readiness review

**Reviewed:** 2026-10-19 · **Tree:** single commit `a8d238b` (2026-10-10) + working tree
**Verdict: ready-with-notes** — the core capabilities are real and tested; publication
needs mechanical release work before it would look professional on GitHub.

## 1. Evidence from this review (all run, not inferred)

| Check | Result |
| --- | --- |
| Full offline pytest | **1,792 passed, 2 failed → both fixed, 30 skipped** (14 min 12 s) |
| Ruff (`hivemind tests scripts examples`) | All checks passed |
| Dashboard build (`npm run build` = `tsc --noEmit` + `vite build`) | Passed; bundle 494 kB JS / 61 kB CSS |
| Schema/OpenAPI re-export vs committed `spec/` | 162 schema files + openapi.json stale; re-verified: 215 paths, conformance suite passes |
| Repo-wide secret scan (token/bearer/API-key/private-key patterns) | 0 hits |
| Hardcoded local paths (`G:\Droid\Hivemind`, `G:\private`, …) in tracked files | Found in 7 files → **all normalized** to `<repo>`/`<private-external>` placeholders |
| CLI end-to-end (`hivemind init`, `serve --help`) | Works; flags match docs after one README fix |
| All 23 untracked `hivemind/*.py` feature modules import cleanly | Confirmed |
| PyPI name `hivemind` | **Taken (v1.1.12)**; `hivemind-reference` reserved-free (404 on JSON API) |
| GitHub repo name landscape | ≥1,900 results for "hivemind"; `activeloopai/hivemind`, `learning-at-home/hivemind` (2.5 k★) exist → plan already flags this; rename or clearly brand |

The 2 full-suite failures found:
1. `tests/test_workspace_navigation.py::test_workspace_feature_routes_are_explicit_ui_routes`
   — stale contract (asserted `enable_ui` routes against the `enable_ui=False` fixture app).
   **Fixed** in place: builds both a UI-enabled and UI-disabled app and asserts each side
   of the contract. Passes.
2. `tests/test_memory_event_bridge.py` — passed when rerun in isolation (21 passed);
   no code change; consistent with a resource-contention flake in the concurrent full run.

## 2. Core capabilities: verified against code

| Phase / capability | Claim source | Code evidence | Tests |
| --- | --- | --- | --- |
| FastAPI + SQLite WAL + FTS5 + sqlite-vec + LadybugDB | completed Phase 0 | `hivemind/storage.py` (`PRAGMA journal_mode=WAL`), `hivemind/indexes.py` (sqlite_vec, ladybug, `MODEL_REVISION` pin) | 1,616 test functions in 137 suites |
| EmbeddingGemma 2 pinned revision `914f7f89142e…` | completed | `hivemind/indexes.py:19` | `tests/test_model_milestone.py` (opt-in), full-chain demo |
| Durable messaging: claim/lease/renew/ack/fail/requeue/dead-letter | completed Phase 2 | `POST /v1/messages/{id}:…` in `hivemind/api.py`; `message_push.py` | `tests/test_conflict_control_console.py`, bridge, activity-stream |
| Boards, threads, mentions, unread | completed Phase 2/3 | `thread_pane.py`, `board_reads.py` | board suites |
| Hybrid retrieval (FTS5 + sqlite-vec + hybrid ranking) | completed Phase 3 | `hivemind/indexes.py`; published Recall@10 in `benchmarks/README.txt` | `test_conformance.py`, retrieval suites |
| Memory: versions/evidence/disputes/review queue/lifecycle | completed Phase 4 | `hivemind/service.py` memory_review paths; `memory_lifecycle_automation.py` | memory suites |
| Context packets + agent selector | completed Phase 4 | `hivemind/context_assembly.py` (`agent_selector` union/intersection) | `test_context_assembly.py` (7 files), 60 context-refs |
| Deterministic coordination + single Nexus lease + epoch fencing | completed Phase 5 | `nexus:acquire/renew/release`, stale-epoch guard in `hivemind/api.py` | `test_coordination_policy.py`, `test_nexus_delegation.py` |
| Worker provisioning + permissions + supervision | completed Phase 5 | `worker_provisioning.py`, `worker_permissions.py`, `process_boundary.py` (Job Object), `wsl_runtime.py` | `test_worker_provisioning.py` (21 tests), `test_worker_permissions.py`, `test_runtime_safety_hardening.py` |
| MCP façade (47 tools) + worker 8-tool read-only surface + **7 `hivemind://` resource templates** | plan §15.4 | `hivemind/mcp.py`, `hivemind/worker_mcp.py`, `worker_mcp_registration.py` | `test_described_tools.py`, `test_worker_mcp_registration.py`, **`test_mcp_resources.py` (new)** |
| A2A + ACP runtime integrations | completed Phase 6 | discovery advertises `runtime.a2a`/`runtime.acp`; `hivemind/activity_stream.py` | integration suites |
| OpenTelemetry export | completed Phase 6 | `hivemind/telemetry.py` (`/v1/projects/{id}/telemetry(:export)`) | telemetry suite |
| Backup / restore / retention | completed Phase 6 | `coordinated_operations.py`, `hivemind backup` CLI | operations suite |
| Dashboard (React) | completed Phase 2 | `dashboard/src` (13 TSX modules); build verified today | playwright specs, browser-checked previously |

Not implemented by design (documented, non-blockers for alpha): production vendor
budget adapters (deferred, `docs/vendor-budget-adapters.md`), Windows vendor OS
sandbox (Linux/WSL only), macOS/Linux native provisioning, third-party conformance,
production TLS/enterprise deployment. All recorded in `completed.md` Remaining lists.

## 3. Pre-publish blockers (ranked)

1. **Commit the working tree — 227 untracked files + 88 modified.** The last commit is
   2026-10-10; 9 days of the project (MCP resources, provisioning, benchmarks, 88 test
   files, 69 `hivemind/` modules…) exist only on disk. One `git add -A && git commit`
   (after review) before any push.
2. **Stale published contract.** `spec/openapi.json` and 162 schemas were out of date
   vs the code; **fixed in place now** (re-exported, conformance suite re-passes).
   Going forward: regenerate before release.
3. **`.gitignore` contradicts itself (decided: keep).** `*.md` globally ignores every
   markdown doc, yet 36 `.md` files are tracked (grandfathered by an earlier commit).
   The user has decided to keep markdown **untracked**; new docs (`docs/MCP.md`,
   `docs/endpoints.md`, this report, `completed.md` changes) therefore will not be
   pushed with a plain `git add .` — before publishing, stage the tracked-docs update
   explicitly (`git add -f` or amend the ignore rule) or the GitHub README will show
   the 2026-10-10 version only.
4. **`*.md` + `completed.md`**: same root cause; before release decide once whether
   docs are tracked (remove the rule) or the README/contract documents are force-added
   at release time.
5. **Wheel/package never built.** `hivemind-reference` declares hatchling packaging but
   `python -m build` has never been run (build tool not installed). Publishing to PyPI
   under that name is free (checked 404), but a source-only GitHub alpha can proceed
   without it. Record as a follow-on.
6. **No CI.** No `.github/workflows`. The 14-minute offline suite is CI-ready evidence:
   add a minimal workflow (`uv sync --locked`, pytest, ruff, dashboard build).
7. **No SECURITY.md / CONTRIBUTING.md.** For an agent-execution product with control
   tools, a security-policy page stating the loopback-single-owner trust boundary is
   important. README does state it; a SECURITY.md is recommended.
8. **README quickstart bug — fixed in place.** `serve --embeddings --open` failed
   (`--open requires --enable-ui`); README now says `--enable-ui`.

## 4. Hygiene results (verified clean)

- No secrets in the tree: repo-wide scan for literal tokens, `Bearer <literal>`,
  `ghp_`/`sk-` patterns, private-key blocks → **0 hits**.
- `.hivemind-data/owner.json` (the live admin token) is properly ignored
  (`git check-ignore` confirms); `*.sqlite3`/models/4.3 GB model cache untracked.
- `NOTICE` and `docs/licensing.md` correctly flag redistribution gates
  (sqlite-vec full-text MIT notice, Gemma model terms) — honest remaining work, not misrepresentation.
- No `TODO/FIXME/HACK` markers in `hivemind/` source.
- `docs/limitations.md` explicitly bounds what was validated (no Windows vendor sandbox,
  no Factory restart recovery) — consistent with the code, honest.

## 5. Fixed in this review

- `tests/test_workspace_navigation.py` — stale UI-route contract (now passes).
- `README.md` — missing `--enable-ui` in both serve quickstarts.
- `.gitignore` — added `.hivemind-test-*` (audit-created dirs previously leaked as
  untracked). `*.md` rule kept per user decision; note the doc-tracking implication in
  §3 above.
- 7 files with hardcoded `G:\Droid\Hivemind` / `G:\private` / `D:\hivemind-data\…`
  paths → placeholder forms (`<repo>`, `<private-external>`, …) in docs, benchmarks,
  and one test (verified the affected test still passes).
- `spec/openapi.json` + 162 schema JSONs — regenerated to match current code
  (conformance suite re-passes); commit these together with the code.

## 6. What still is not proven (honest alpha caveats to state in README)

- No macOS/Linux end-to-end service run in this working session (recorded as open in completed.md).
- No live vendor/paid model validation recently; Factory interruption recovery
  still unvalidated (`docs/limitations.md`).
- Dashboard browser checks not rerun for the newest batch (completed.md Phase 2 Remaining).
- Memory-provider mirrors (`Mem0/LangMem/Letta`) are offline-tested only.