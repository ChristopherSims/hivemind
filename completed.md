# Hivemind completion checklist

**Updated:** 2026-10-13
**Source plan:** [hivemind-plan.md](hivemind-plan.md), especially sections 20–25 and 27.

This tracks the implemented local reference system, not a completed production
release or full protocol conformance. Some planned endpoints and table names have
implemented equivalents rather than the exact proposed shapes.

- `[x]` Completed implementation or recorded validation.
- `[ ]` Remaining work. **Implement** means missing functionality; **Verify** means
  further evidence is needed, not necessarily missing code.
- A phase is partial whenever its remaining list is nonempty.

## Phase 0: Feasibility and reuse spike

### Completed

- [x] Record prior-art research and the vendor-neutral, build-versus-integrate design.
- [x] Implement the Python/FastAPI, SQLite WAL, FTS5, sqlite-vec and LadybugDB architecture.
- [x] Pin EmbeddingGemma 2 to revision
  `914f7f89142e33e77833254d9c9b90c3cef7303b`.
- [x] Run real text embedding/retrieval tests with finite normalized vectors and CPU float32.
- [x] Exercise vector transaction/authorization behavior and graph projection/retry behavior.
- [x] Provide MIT project licensing, dependency locks and attribution notices.
- [x] Implement Factory Droid and Oh my Pi integrations; record capture/control limits.
- [x] Validate approved Windows standard-account ownership and directory ACL separation
  in retained test fixtures.

### Remaining
- [x] Implement VS Code hash-only buffer capture and bounded generic JSON-lines
  runtime capture over the persistent authorized SDK spool.
- [ ] **Verify/extend:** interactive editor deployment, additional host adapters and
  complete instrumentation coverage for each advertised host.
  license audit, including model and bundled dependency terms.
- [ ] **Verify:** hands-on reuse comparisons against all proposed third-party projects.
- [ ] **Verify:** SQLite, sqlite-vec, LadybugDB and runtime behavior on macOS and Linux.
- [ ] **Verify:** real Linux filesystem-only vendor sandbox launchers and enforcement.
- [ ] **Verify:** representative retrieval quality, scale, graph concurrency and hardware
  performance measurements.
- [ ] **Verify:** GPU operation and the broader supported dependency/platform matrix.

## Phase 1: Specification and durable core

### Completed

- [x] Publish generated JSON Schemas, OpenAPI, examples, protocol envelopes and safe errors.
- [x] Use `/v1` for operational HTTP APIs; retain wire/package version `0.1.0`.
- [x] Implement project/agent/run identities, authentication and independent concurrent runs.
- [x] Implement structured event ingestion, source sequencing, deduplication and revisions.
- [x] Persist canonical state, relationship facts, index jobs and command receipts in SQLite.
- [x] Implement work scopes, claims, conflicts, policy versions and coordinator epochs.
- [x] Provide Python/TypeScript clients, SDK emitters and bounded durable capture spooling.
- [x] Test duplicate events, conflicting retries, transaction rollback and narrow
  commit/response crash boundaries.

### Remaining

- [x] Implement/verify versioned event-only SQL shadow replay, exact equivalence and
  atomic activation for projects/resources/versions/relations/run-task access/FTS.
  Legacy stores expose a bootstrap boundary; independent authority/side ledgers
  are preserved, not reconstructed. See `benchmarks/durability-README.txt`.
- [ ] **Verify:** full proposed schema/extension/version conformance, beyond implemented
  command contracts.
- [ ] **Verify:** broad crash-kill, disk-full, migration and upgrade fault campaigns.
  Canonical SQL campaigns now pass (214 Windows tests, 33 Linux durability tests,
  including SIGKILL and genuine private-tmpfs ENOSPC). Power-loss, massive load,
  plugin migrations and whole-service/side-ledger campaigns remain open.

## Phase 2: Capture, inboxes and live React app

### Completed

- [x] Implement runtime hooks, SDK capture, scoped file observation/drift reconciliation,
  redaction, capture-health reporting and configurable coalescing.
- [x] Keep uncertain filesystem attribution distinct from confirmed tool observations.
- [x] Implement durable direct messages, delivery claims, lease renewal, acknowledgments,
  correlated replies, retries, dead letters and explicit recovery.
- [x] Add push signals, auxiliary messaging runs and independent messaging slots without
  replacing the primary task execution.
- [x] Implement advisory reservations and canonical overlap/conflict notifications.
- [x] Serve the React dashboard with Main board, agent tabs, conversations, roster,
  timelines, canonical refresh and SSE invalidations.
- [x] Add visual team membership, full resource-access policies, scoped Nexus grants
  and administrator-only owned-worker incident/stop controls. Preserve revisions,
  explicit confirmations, sticky authority limits and disabled-integration states.
  Refresh the dark visual system and mobile forms, stabilize board selection and
  support authorized memory evidence-board selection. **17 browser checks** passed,
  with **5 administration checks** rerun after final request-key retention; **58
  backend checks**, production build, Ruff and schema export passed. Browser checks
  use a real loopback service with synthetic data, not live vendor/model workers.
- [x] Implement persisted nonunique agent colors and authoritative Nexus gold/crown styling.
- [x] Provide authorized current task/status/summary and bounded per-run activity context.
- [x] Add durable scoped activity subscriptions, privacy withdrawals, fresh authorization,
  reconnect cursors and Python/TypeScript subscription clients.
- [x] Test restart/redelivery behavior without treating retries as permission to repeat
  uncertain work; browser observation does not claim or acknowledge inboxes.

### Remaining

- [ ] **Implement/verify:** additional editor/runtime capture adapters and complete
  instrumentation coverage for each advertised host.
- [ ] **Verify:** capture latency and sustained-load targets on representative deployments.
- [ ] **Verify:** the latest feature batch in browsers; the prior managed Chromium suite
  passed, but it was not rerun for this batch.
- [ ] **Verify:** broad cross-browser, accessibility, reduced-motion and large-history UX.
- [ ] **Implement/verify:** remaining advanced UI behavior such as shared-board unread
  accounting, optimistic posting, caret-anchored mentions and virtualized rendering.

## Phase 3: Boards, vectors and graph retrieval

### Completed

- [x] Implement threaded boards, revisions, human/agent authors and visibility checks.
- [x] Implement immutable-ID mention segments, eligible-agent autocomplete, durable
  notifications and shared-thread replies.
- [x] Implement FTS5 lexical retrieval, EmbeddingGemma encoding/chunking, authorized
  sqlite-vec candidate search and hybrid ranking.
- [x] Track embedding profiles, jobs, index freshness and stale/deleted source filtering.
- [x] Implement semantic agent discovery from permitted agent/work context.
- [x] Implement LadybugDB projection, canonical relationship facts, bounded graph
  traversal, authorization rechecks and canonical fallback.
- [x] Test cross-project/private-source isolation, stale jobs, graph retries and
  private-intermediate-node filtering.

### Remaining

- [ ] **Verify:** a versioned labeled board/code/memory retrieval dataset and published
  Recall@10, MRR/NDCG, forbidden-result rate and latency results.
- [ ] **Verify:** lexical/vector/hybrid comparisons, including the proposed 768/512/256
  dimension evaluation and semantic-subset improvement over lexical-only search.
- [ ] **Verify:** large-dataset retrieval, indexing lag and graph rebuild performance.
- [x] Implement operator-managed teams and fine-grained read/contribute policies for
  boards, posts, artifacts, memories and tasks, with source/reply inheritance,
  fresh membership checks, replay fences and retained run/task access provenance.

## Phase 4: Memory and context assembly

### Completed

- [x] Implement scoped memory CRUD, immutable versions, source evidence and explicit review.
- [x] Preserve contradictory claims with quarantine/review/resolution rather than
  automatically declaring inferred facts authoritative.
- [x] Enforce validity/expiry, source-revision checks and deletion/revocation filtering.
- [x] Implement opt-in source-linked extraction and configured model enrichment;
  model output cannot independently grant verification.
- [x] Implement bounded context packets, trust labeling, per-agent grouping and
  explicit-ID/semantic union/intersection selectors.
- [x] Add full-envelope context budgeting and explicitly configured hash-pinned tokenizers;
  retain labeled fallback estimates where exact counting is unavailable.
- [x] Implement lifecycle reminders, repository-change review and event-driven supporting
  source review, including task changes with unchanged Git HEAD.
- [x] Expose an authorized durable memory review queue; automatic supporting-source
  invalidation is a separate operator opt-in.
- [x] Preserve history and hold unsupported privacy combinations or unknown computation
  instead of replaying them.

### Remaining

- [ ] **Verify:** tokenizer correctness for each production target model; a local
  tokenizer or byte fallback is not automatically that model's exact accounting.
- [ ] **Verify:** authoritative semantic quality and procedural-memory evaluation;
  contradiction suggestions still need governed review.
- [x] Implement default-off canonical-first Mem0/LangMem-store/Letta memory mirror adapters.
- [x] Implement governed namespace-bound provider retrieval, operator-approved
  proposed imports, automatic owned withdrawal/deletion propagation and durable
  unknown holds with read-only absence reconciliation.
- [ ] **Verify/extend:** live provider compatibility, broader/semantic provider
  retrieval and provider-wide physical erasure.
- [ ] **Verify:** dependency-wide physical erasure across retained versions, spools,
  backups and SQLite/WAL storage.

## Phase 5: Deterministic control and Nexus

### Completed

- [x] Implement exact/parent-child conflict prevention and versioned deterministic policy.
- [x] Implement a single leased Nexus per project, epoch fencing, renewal and revocation.
- [x] Implement typed assign/steer/pause/resume/cancel controls, checkpoints, results,
  tool-scope permits and reconnect reconciliation.
- [x] Distinguish receipt acknowledgments, reported state and actual runtime evidence.
- [x] Keep unsupported controls advisory or fail closed; heartbeat silence is not
  termination or quiescence proof.
- [x] Test deterministic conflict handling without Nexus, stale epochs and guarded
  steering/assignment paths.
- [x] Add goal-level DAG orchestration, exact-task dispatch, immutable plan revisions,
  obsolete-task supersession and bounded failed-goal revision.
- [x] Preserve completed producers as external dependencies across plan revisions.
- [x] Add recipient-accepted, revocable, source-authorized structured handoff context.
- [x] Add capability/runtime/workload routing with composed dispatch and direct-start gates.
- [x] Add typed artifact contracts, exact producer/revision bindings and dependency gates.
- [x] Add cumulative token/time/cost reservations, frozen policies, unknown holds and
  mandatory managed-worker governance when required by the server.
- [x] Add local Git collaboration, independent exact-candidate review and fenced
  Nexus-only protected publication.
- [x] Integrate workspace-bound vendor recovery with private-state/checkpoint
  validation, durable load intents and fresh managed-worker authority checks.
- [x] Add default-off blocked-task explanations and opt-in, candidate-bound
  resolution requests with source-aware notes, basis and command receipts.
- [x] Add administrator/current-Nexus worker provisioning, private startup configuration,
  hash-pinned repository runtime ceilings and owned Windows process-tree supervision.
- [x] Add profile-bounded per-worker JSON tool/path permissions, automatic termination
  on observed tool/API denials, private launch-bound incident signals, credential
  revocation and scoped Nexus monitoring. This is not OS-level isolation.
- [x] Add opt-in independent local Git snapshots for API-created workers, exact
  commit/private-branch provenance, bounded setup, profile fingerprint binding,
  source-user-work preservation and safe tracked-tree validation.
- [x] Extend worker snapshots to approved linked/bare sources, exact mapped local
  submodules and SHA-256/size-verified local LFS objects. Private source views ignore
  ambient helpers/URLs, preserve SHA-1/SHA-256 and shallow metadata, and enforce
  aggregate content/copy/deadline limits. **153 selected checks** passed across the
  expanded worker batch and corrected task-execution suite; Ruff/schema export passed.
  Empty workspaces remain the default; remote dependency fetching is not enabled.
- [x] Add automatic worker-local MCP registration for Factory/OMP with a fixed
  read-only worker surface, exact tool restrictions, connection/catalog fences,
  saved-session descriptors and nonce-bound OMP startup evidence.

### Remaining

- [ ] **Implement/verify:** production budget adapters for stock Factory/OMP runtimes
  with exact serialization and genuinely enforced output maxima. Current stock
  adapters fail closed under this budget contract.
  - **Deferred by user, 2026-10-12:** installed Factory SDK/OMP RPC do not expose
    the required verified contract. Capability investigation is recorded in
    [docs/vendor-budget-adapters.md](docs/vendor-budget-adapters.md); helper prototypes
    are not enabled as production adapters.
- [ ] **Implement/verify:** required filesystem-plus-network vendor isolation and a
  Windows vendor sandbox; approved ACL fixtures are not production sandbox proof.
- [x] Implement finer target/action-specific Nexus control grants, sticky restrictions,
  operator-only management and fresh issue/apply/replay authorization.
- [ ] **Implement/verify:** native provisioning on non-Windows platforms with equivalent
  whole-tree containment; current native launches fail closed outside Windows.
- [ ] **Verify/extend:** live Factory MCP enrollment and full model/vendor
  interoperability; automatic enrollment with filesystem-sandbox binds is currently
  rejected. OMP startup evidence proves initial registration, not ongoing liveness.
- [ ] **Verify:** production-grade runtime transitions and safe boundaries for every
  supported action/adapter; controls and time cancellation remain cooperative.
- [ ] **Verify:** Factory interrupted-session continuation with correct workspace binding;
  the earlier live attempt failed closed.
- [x] Implement native artifact verifier profiles with exact staged bytes, execution-bound
  receipts, bounded trusted argv, durable intent and input-tamper checks.
- [ ] **Verify:** native verifier deployment/sandbox contracts and broader artifact formats.

## Phase 6: Interoperability and release hardening

### Completed

- [x] Implement stdio MCP exposure, operation descriptions, Python/TypeScript SDKs and CLI.
- [x] Implement A2A and Agent Client Protocol runtime integrations with bounded,
  authenticated, permission-checked handling.
- [x] Implement OpenTelemetry export and operational diagnostics.
- [x] Implement consistent backups, guarded restore, retention/deletion workflows,
  protected Git reconciliation and explicit restore authority holds/release.
- [x] Provide native installation scripts, locked dependencies, configuration and
  recovery documentation.
- [x] Add protocol, security, fault, runtime, graph/vector and browser regression tests.
- [x] Record bounded live Factory/OMP file-tool, review and messaging validation.
- [x] Reproduce the core capture → memory → retrieval → assignment → execution →
  verification → recovery chain in one current offline A2A + ACP scenario, including
  cached real Gemma retrieval, bounded runtime context, real file/subprocess work,
  canonical application restart and receipt-only lost-response recovery.
  Evidence and scope: `benchmarks/full-chain-README.txt`. This does not close the
  entire section 27, live-vendor, UI, Nexus or process-crash gates below.

### Remaining

- [ ] **Verify:** independent third-party client conformance and broader external
  A2A/Agent Client Protocol interoperability.
- [ ] **Implement/verify:** production shared deployment with TLS, enterprise identity,
  granular permissions and secure operational procedures.
- [ ] **Verify:** distributable wheel/frontend packaging, clean-host installation,
  upgrades and the full macOS/Linux/Windows release matrix.
- [ ] **Verify:** end-to-end backup/restore and erasure across every configured side ledger,
  private journal, artifact and runtime state store.
- [ ] **Verify:** production security review, parser fuzzing, comprehensive fault/load
  campaigns and all section 22.5 performance targets.
- [ ] **Verify:** the entire section 27 demonstration as one reproducible, current,
  multi-runtime end-to-end scenario, not just separate passing component tests.

## Optional post-MVP work (not required to call the local reference usable)

Plan sections 3.2 and 21 distinguish these from first-release requirements.

- [ ] Additional framework/editor integrations and richer automatic enrichment.
- [ ] Advanced graph analytics, ontology extraction, semantic subscription routing
  and reranking.
- [ ] Organization/user memory and enterprise integrations.
- [ ] Image/audio/video retrieval.
- [ ] Multi-server federation, true peer transport and distributed storage.

## Validation supporting this checklist

### Runtime safety and recovery update, 2026-10-17

- Operator-selected Windows boundary: WSL2 Linux filesystem/network sandbox
  with an explicit gateway; offline/synthetic validation only.
- Added explicit native Linux cgroup-v2 admission/drain support, isolated
  bootstrap imports and failure-exit propagation. Missing delegation fails closed;
  it is not replaced by process-group-only supervision.
- Added policy-bound, expiring WSL2 readiness and strict-gateway launch gates.
  The real local Ubuntu assessment correctly refuses: required Linux Hivemind,
  bubblewrap, delegated cgroup and vendor prerequisites are absent.
- Tightened absolute workspace evidence and exercised Factory continuation with
  the installed SDK wire parser, without any live session or model prompt.
- Added serialized vendor controls and an offline durable-evidence matrix for
  every generic adapter action; unknown/foreign controls refuse before acknowledgement.
- Tightened Factory MCP catalog namespace checks and native-verifier sandbox
  assessment, including refusal to count injected callbacks as OS isolation.
- Actual Linux sandbox/containment deployment, service-crash cleanup, live MCP
  enrollment, and live interrupted continuation remain open. No host security
  changes, vendor authentication, paid turns or installations were performed.
- Evidence and guide: `benchmarks/runtime-safety-README.txt`,
  `benchmarks/runtime-safety-wsl-2026-10-17.json`,
  `docs/runtime-safety-validation.md`.
- Final scoped regression: **386 passed, 7 skipped** for Linux/bubblewrap,
  POSIX ownership/modes and unavailable symlink privileges. Ruff, whitespace,
  and in-memory worker JSON Schema/OpenAPI checks passed. Full result:
  `benchmarks/runtime-safety-results-2026-10-17.json`.
- Follow-up default-restriction Docker/WSL testing: **388 passed, 9 skipped** in
  WSL, including real namespace/gateway denial and exact-input sandboxed native
  JSON verification. Docker refusal checks: **3 passed, 1 skipped**. Its positive
  sandbox checks are blocked by namespace denial; the broader Docker attempt
  timed out and is not a completed pass.
- Test-only image dependencies and temporary unprivileged WSL mounts were used,
  without system installs, security exceptions or privileged containers.
  Fixed real-test fixture paths and a watcher scheduling assumption.
  Cgroup whole-tree cleanup, protected service deployment and live vendor gates
  remain open. Evidence: `benchmarks/runtime-safety-linux-2026-10-17.json`;
  reproduction: `benchmarks/runtime-linux/README.txt`.
- Separately approved Docker sandbox exceptions: **146 passed, 2 branch skips**.
  Separately approved privileged/private-cgroup setup: **2 real kernel tests
  passed**, with service UID 1000, admission before GO, live/exited-parent
  `setsid` descendants, kill/drain and subgroup cleanup. No host mounts or
  credentials. The broader Docker matrix did not complete and exited nonzero;
  its root cause remains open. Production service-crash and live vendor gates
  remain open. Evidence:
  `benchmarks/runtime-safety-docker-enabled-2026-10-17.json`.

### Retrieval and memory evidence update, 2026-10-17

#### Search-performance and real-code quality follow-up

- Isolated 10,000-record search: semantic p95 **810.92 ms** and hybrid
  **930.58 ms**, versus matched baseline **7,513.60 / 7,762.13 ms**.
  Baseline five samples/mode; final twenty. Same emitted top-10 IDs and scan flags,
  full candidate scans, explicitly truncated 4,096 ranking window.
  Lexical p95 increased from 507.68 to 661.31 ms, not an improvement claim.
- Identified SQLite page-cache thrashing. The read-only connection now targets
  32 MiB; graph default remains 64 MiB. Exact cosine scores, authorized current
  revisions and final canonical authorization checks remain intact. SQL performs
  one vector scan, groups minimum chunk distances, and returns at most depth+1.
  A SQL-only rewrite did not improve latency and remains published as such.
- Added a frozen real-code corpus: 64 excerpts from eight allowlisted modules,
  18 developer questions, source/excerpt hashes and identical forbidden clones.
  Pinned cached-model Recall@10: lexical **0.8056**, semantic **0.9722**, hybrid
  **0.9167**; zero forbidden results. Metrics unchanged after optimization.
  Hybrid underperforms semantic-only. Labels are assistant-authored, not
  independent production judgments; no ranking tuning against these labels.
- **100 Windows tests passed** in three batches; **24 Linux Docker tests passed**
  with default security restrictions and private tmpfs. The ordinary-storage
  Docker attempt exited without usable results, not a pass. Ruff passed.
- Evidence: `benchmarks/retrieval-search-summary-2026-10-17.json` and
  `benchmarks/README.txt`. The 10k timing fixture uses synthetic vectors and bulk
  seeding, not real-model indexing. Independent board/memory quality, diverse
  real-encoder 10k queries, concurrent deployment load and cold-cache latency
  remain open. The historical evidence below is preserved.

- Added `benchmarks/retrieval-v1.json`, a versioned manually labeled synthetic
  corpus, and an offline production-path benchmark. Published all nine
  lexical/semantic/hybrid and 768/512/256 comparisons in
  `benchmarks/retrieval-results-2026-10-17.json`. Zero forbidden results; this is
  a small regression corpus, not representative production quality proof.
- Measured 2,000-record indexing, concurrent graph callers and exact projection
  rebuild equivalence. Semantic/hybrid p95 was about 1.63 seconds, exceeding
  the provisional one-second search goal even with synthetic encoding.
- Preserved a failed 10,000-record attempt and reproduced exhaustion of the
  default 64 MiB LadybugDB buffer pool. Added explicit bounded graph-memory
  configuration for benchmark/operator construction; the service default is unchanged.
- The 256 MiB retry indexed 10,000 resources and rebuilt 9,999 graph edges with
  exact equivalence, including 80 successful reads during rebuild. Semantic/hybrid
  search p95 was about 7.8 seconds; ranking-depth truncation was disclosed.
  The integrity pass does not satisfy the plan's latency target.
- Added pinned raw-text tokenizer golden-count validation. **The operator will
  provide a local LLM later.** Its production tokenizer/model binding remains gated.
- Added offline Mem0/LangMem-store/Letta read-after-delete checks. Live providers
  remain gated on approved disposable namespaces, credentials and pinned versions.
- Verified disposable memory-history/index deletion and fixture SQLite/WAL
  compaction. A read-only audit detects retained backup/spool copies and never
  reports whole-system physical erasure. Production data is not purged.
- Details, commands and external gates: `docs/retrieval-memory-validation.md`.
  Broader benchmarks, live compatibility and dependency-wide physical erasure
  remain open; the original phase checkboxes above are not blanket-completed.

- Full default offline run for recovery, blocked-task integration and provisioning:
  **1,310 passed, 22 skipped**. After a final terminal-resolution proof correction,
  the focused combined suite passed **145 tests**, including final provisioning
  source-authorization checks; the full suite was not repeated.
- Whole-tree Ruff, final schema export, SDK/dashboard typechecks, dashboard build
  and `git diff --check` passed.
- Latest repository-checkout change: **94 focused/adjacent tests passed** with real
  local Git and injected vendor launchers. The full offline/browser suites were not
  rerun for this checkout-only change.
- Latest automatic-MCP change: **205 focused/adjacent tests passed**, including real
  local MCP handshake, installed OMP extension/manager registration and adapter
  typechecking and discovery-disabled SDK-host startup. No paid model turns,
  live Factory enrollment or full authenticated OMP SDK-host/model turns were run.
- Latest scoped-Nexus/integration batch: **319 expanded regression tests passed**
  plus a real native artifact-check API/replay test. Ruff, TypeScript facade
  checks, VS Code module syntax and schema export passed. The prior full run had
  **1,380 passed, 22 skipped, 3 failures**; the error-order regression was fixed
  and both timing tests passed in the final batch. No complete rerun after fixes,
  live memory-provider transfer, interactive VS Code deployment or paid calls.
- Latest worker-permissions batch: **186 tests passed**, including real owned
  Windows parent/child termination, policy narrowing, tool/API refusals,
  credential revocation and scoped Nexus incident monitoring. Ruff, SDK
  typechecking, schema export and diff checks passed. No live vendor/paid calls
  or full offline/browser rerun; filesystem/network OS isolation is not claimed.
- Latest team/board access batch: **333 tests passed**, including 18 new access
  tests for membership/policy CAS, read/contribute separation, source/reply
  inheritance, privacy revocation, retained run history, Nexus controls,
  artifact submissions and restart behavior. Ruff, schema export, strict SDK
  typechecking and the dashboard production build passed. No paid calls or
  complete offline/browser rerun.
- Latest governed memory-provider batch: **357 tests passed**, including 24
  provider-port/API/privacy/concurrency/lifecycle tests. Ruff, schema export and
  strict TypeScript SDK checks passed. Imports remain proposed/provider-reported;
  uncertain deletes are held, never blindly repeated. No live authenticated
  provider calls, paid inference, external deletions or full offline/browser rerun.
- Earlier managed Chromium validation: **12 passed**; browser tests were not rerun
  for the latest seven-feature batch.
- Earlier real-model and bounded live-vendor evidence is recorded in
  [docs/validation.md](docs/validation.md). Latest work made no paid vendor calls.
- Skips, historical live results and unsupported adapters are not marked as newly
  verified production capabilities.

Implementation guides: [README.md](README.md), [docs](docs/).
New guides: [worker provisioning](docs/worker-provisioning.md),
[vendor recovery](docs/vendor-session-recovery.md), and
[blocked-task resolution](docs/blocked-task-resolution.md).
Integration references: [docs/MCP.md](docs/MCP.md) (MCP façade, harness
registration for Droid/OMP/managed workers, and the seven `hivemind://`
resource templates from plan section 15.4) and
[docs/endpoints.md](docs/endpoints.md) (grouped live `/v1` endpoint surface
with harness integration paths), linked from the README and OpenAPI contract.
- `[x]` Implement MCP resource URIs for authorized ledger rows, board threads,
  task records and memory records over the same authenticated GET endpoints;
  private/foreign reads fail closed. Regression: `tests/test_mcp_resources.py`
  (template catalog, authorized reads, agent-private and foreign-project
  refusals against a real loopback service).
Older milestone documents contain some superseded “not implemented” statements;
the newer feature guides and current implementation take precedence. This checklist
does not modify the original proposed plan or claim its every acceptance criterion is met.
