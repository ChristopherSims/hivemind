# Validation record

Date: 2026-10-09. Platform: Windows 10, Python 3.12.13, Node.js 25.8.2.
Real model inference used CPU **float32**; CUDA was unavailable.

## Subsequent non-UI agent integration

### Identity, recovery and assigned-run messaging (2026-10-10)

Final parent-integrated validation:

- **567 offline backend/model tests passed**, **21 platform-dependent skips**,
  330.80 seconds. The pinned embedding model ran from the local offline cache.
- **12 Chromium tests passed**, 13.6 seconds, with an explicitly owned synthetic
  fixture and Windows Job cleanup. The stock Playwright webserver teardown hung
  after all assertions passed on two earlier runs; these were not counted as
  clean runs. `.hivemind-test/browser-managed-check.py` and its managed config
  bypass that teardown; `browser-managed-report.json` records exit code zero and
  the fixture owner's exit.
- Ruff, schema export, SDK/dashboard TypeScript, production build, offline
  dependency-tree validation and `git diff --check` passed.
- Final fixture observation: **zero owned fixture processes and zero listeners
  on the browser fixture port**.

Three workers delivered identity/sandbox attestation, evidence-driven recovery,
and concurrent messaging; parent review tightened actual process/ancestor/probe
evidence, ambiguous-turn rules, exact reply envelope proofs, delivery leases and
per-effect control gates. An intermediate full run caught three messaging
regressions during that worker's hardening. The final full run above passed after
those fixes and the parent's current-lease recovery check.

The user-approved **`hivemind-svc` and `hivemind-agent` standard accounts** and
**`<isolated-root>`** were actually provisioned. Independent native probes
observed distinct process owners, allowed own-directory writes, denied cross/
trusted-tools writes, and denied agent access to the service credential sentinel.
The approved resources and protected encrypted credentials remain intentionally.
See [Windows evidence and scope](windows-isolation-test.md). Earlier failed probes
remain preserved. This is not a production service migration, vendor sandbox or
network-isolation proof.

Automatic service startup/storage/native-Git identity checks and agent startup/
vendor-turn process checks are opt-in and fail closed. Filesystem-only Linux
vendor launchers wrap actual OMP argv and Factory's official connected supplied
transport, gated by a fresh enforcement probe. **Real Linux execution is skipped
on this Windows host**; Windows vendor sandbox requests and strict vendor
filesystem+network isolation remain refused.

Assigned-run messaging has one private no-tools auxiliary runtime/run and a
bounded queue, shared journal/dedup, exact renewed delivery leases, and fresh
control checks before reply and settlement. Primary task/execution binding stays
unchanged in real local-service tests. `--recover-message-effects` recovers only
proved deterministic reply/settlement effects from validated durable decisions;
it never repeats an uncertain model turn or acknowledges a delivery. Lost leases,
paused/replaced runs and insufficient evidence remain held. Client-side gates are
fresh checks, not atomic authorization with the subsequent write.

No additional paid vendor turns, external publication, production installation,
firewall changes or account/resource removal occurred in this phase.

### Real Factory and Oh my Pi validation (2026-10-10)

User authorized bounded live testing up to $10 aggregate. Installed Factory CLI
0.233.0, SDK 0.5.0, OMP 18.8.9; explicit low-cost models were `gpt-6-luna` and
`ollama-cloud/deepseek-v4.1-flash`. Disposable loopback services, canonical agents/
tasks/executions, private credentials outside workspaces, and local-only Git were used.
These results are **real vendor execution**, not the offline substitutes below.

| Live check | Factory | OMP |
| --- | --- | --- |
| Authenticated session/context or RPC/model discovery | Passed | Passed |
| Scoped input read and exact output-file creation | Passed | Passed |
| Independent exact-candidate Git review, unchanged snapshot | Approved | Approved |
| Duplicate review without another model call | Passed | Passed |
| Canonical message task, one correlated reply, receipt acknowledgement | Passed | Passed |
| Message journal reopened; redelivery suppresses another turn/reply | Passed | Passed |
| In-flight interruption and observed quiescent paused checkpoint | Passed | Passed |
| Session reopened and canonical checkpoint consumed | **Blocked before prompt** | Passed |

Factory resume failed closed on a workspace mismatch. SDK 0.5.0 `Session.resume`
has no public cwd argument; its `load_cwd` falls back to the caller's cwd when the
load response omits cwd/worktree. On this host that differs from the saved isolated
workspace. The observed mismatch is consistent with that SDK fallback; the live
load response itself was not retained. The interrupted fixture was retired; the continuation remains unknown,
was not blindly replayed, and no continuation model prompt was sent. Do not weaken
workspace validation or claim Factory restart recovery from these results.

Live testing exposed and fixed OMP discarding final public assistant text: explicit
structured contracts now use documented `get_last_assistant_text`, never full
transcripts. Empty OMP tool allowlists now use `--no-tools`; message turns reported
zero tool calls. Factory supports an explicit installed executable and records SDK
usage. OMP usage collection uses documented `get_session_stats`, opt-in only.

Metering: Factory reported **2,262 credits** across completed turns (542 file,
897 review, 823 message); its interrupted turn reported zero usage. OMP's reported
model-cost calculations total **$0.002628576**, including a no-prompt observation
of the saved file-tool session. Interrupted usage reported zero, which is not proof
the provider will not bill partial work. Factory credit-to-dollar conversion and
invoice-level aggregate spend were not available; no cash hard-cap guarantee is
claimed. No further paid turns were run.

Redacted reports: `.hivemind-test/vendor-live-smoke-01/report.json`,
`vendor-live-suite-01/report.json`, `vendor-live-recovery-01/report.json`,
`vendor-live-recovery-omp-01/report.json`, with OMP file-turn metering in
`vendor-live-smoke-01/omp/usage-observation.json`. Private runtime artifacts and
credentials stay ignored, outside agent workspaces. Services/sessions were closed.
Final process observation found zero owned fixtures. The blocked Factory recovery
report retains one cleanup warning: closing an already-closed SDK loop raised
`RuntimeError`. Factory close is now idempotent and covered by an offline regression;
the original warning is preserved rather than rewritten.
No external Git publication, host identity/ACL provisioning, or UI changes occurred.
The opt-in harness is `scripts/vendor_live.py`; ordinary pytest never launches it.
After the adapter and cleanup fixes, **71 focused offline regressions passed** in 51.72 seconds,
including a resume-workspace rejection-before-prompt regression. Ruff passed.
The full backend/model and browser suites were not rerun for these adapter-only changes.

### Seven parallel autonomous integration tracks (2026-10-10)

Final parent-integrated validation:

- **415 offline backend/model tests passed**, **3 platform-dependent skips**, 298.39 seconds.
- **12 Chromium regressions passed**, 15.1 seconds.
- Ruff, Python schema export, SDK/dashboard TypeScript, production build, and
  locked 108-package dependency check passed.

Tracks cover autonomous Git provisioning/publication/verified conflict resolution,
protected-tip fetch/fast-forward without discarding dirty work, bounded Nexus
lease/check/review orchestration, real canonical reviewer and message/notification
tasks, vendor checkpoint/handoff prompt consumption, optional fail-closed OS policy,
and operational backup/restore/retention/preflight.

Parent integration fixed mismatched runtime result contracts, pseudo execution
identities, model calls held inside write transactions, missing pre-turn effect
intents, reviewer endpoint impersonation, duplicate canonical reviews, changed-policy
receipts, and protected-baseline file imports. A complete local producer-to-reviewer
to-Nexus test records one independent review and one fenced merge.

Actual Git child-process kill tests preserve protected refs and pass native integrity
checks; killed/ambiguous effects remain unknown. Restore validation reinstalls protected
receive hooks and refuses existing destinations. CLI retention also requires the
canonical project and a merged matching candidate, not ancestry alone.

Factory and OMP model contracts use local SDK/RPC substitutes, not paid-live calls.
No external Git push, transport exposure, account creation, ACL provisioning, or
privileged host modification occurred. POSIX permissions and real Linux bubblewrap
tests are the three skipped checks on Windows. Windows process cleanup is not a sandbox;
strict unsupported vendor or unattested identity/ACL controls refuse execution.

One browser invocation initially selected no tests because of PowerShell argument
quoting; the corrected invocation completed all 12 tests successfully.

### Local Git collaboration extension (2026-10-10)

| Check | Result |
| --- | --- |
| Full offline backend/model suite, including real local Git | **263 passed**, 101.50 seconds |
| Existing Chromium regressions | **12 passed**, 16.3 seconds |
| Git collaboration tests | **7 passed**, installed Git 2.53.0.windows.1 |
| Deployment prerequisite tests | **2 passed** |
| Ruff, SDK/dashboard TypeScript, production build, schema export | Passed |
| Real Windows installer prerequisite check; Unix installer syntax | Passed |

Real standard Git tests create temporary bare repositories and isolated agent clones,
commit/push local task changes, reject protected pushes/self-review, run candidate
checks, publish only with the current Nexus, and return native merge conflicts.
They cover stale Nexus epochs, source advancement, changed check policy, internal
Git fencing-ref mismatch, and protected publication surviving canonical rollback
through audited reflog reconciliation. Windows long-path support fixed a failure
in deep clone/ref paths.

The missing-Git Windows package-manager flow is mocked; no actual package install
or Git identity modification was performed. Linux/macOS package installation was
not executed. No external Git remote was pushed, no Git transport listener was
exposed, and no paid vendor/model calls were launched. Git transport is local-only.
The same-user bare-directory write-access limitation remains explicit in
[Git operations and boundaries](git-collaboration.md).

The next eight integration tracks were implemented sequentially: official SDK/RPC
adapters, supervision, automatic independent review/corrections, optional verified
dependencies, cross-agent handoff, durable effect handling/reconciliation, bounded
process/tool controls, and authorized extractor/runtime-tokenizer wiring.

| Check | Result |
| --- | --- |
| Final full offline model-inclusive pytest suite | **254 passed**, 72.75 seconds |
| Existing Chromium regressions | **12 passed**, 16.3 seconds |
| Ruff; installed OpenAPI/module-qualified schema export | Passed |
| Dashboard/standalone SDK TypeScript checks and Vite build | Passed |
| `uv lock --check` | Passed, 108 locked packages |

New tests cover stale-execution handoff, waiting for fresh claims, saved checkpoint
transfer, current-checkpoint verification gates, independent correction resubmission,
handler deduplication across restart, operator adjudication, unsafe effects hidden
behind more than 200 recent operations, and completed trusted checks surviving
canonical transaction rollback without repeating their side effects. Real local
child processes test bounded output, timeouts, and descendant cleanup. Tokenizer
tests verify model/file hash binding and unavailable-target fallback.

The first combined run found a session-resume test still expecting duplicate
completed work; it now checks duplicate refusal and new-task session reuse.
A subsequent run found a Windows acknowledgement-file cleanup race, now fixed
with a sharing-violation regression. Scheduler discovery fixtures were updated
for the ninth endpoint. Final validation has no failing tests. Windows Node command
shims initially failed and one browser teardown timed out; an explicit per-process
Windows search path resolved command lookup and the browser suite exited successfully.

No paid Factory/Oh my Pi sessions or external model API calls were launched.
SDK/RPC collaborators are local contract substitutes. The extraction wiring test
uses a trusted local protocol command, not generative model inference. Pinned
EmbeddingGemma inference is included in the offline full suite. Windows Job Object
cleanup is **not** filesystem/network isolation. Vendor strict OS sandbox requests
are refused; Linux bubblewrap isolation was not executed on this Windows host.
See [operations and explicit limits](agent-integrations.md).

## Eight integrated agent capability workstreams

Completed 2026-10-10 with eight isolated development subagents and parent integration.
Work added agent functionality, not dashboard features. No live paid vendor runtime
sessions or external model API calls were launched. Authorization enhancements remain
deferred.

| Check | Result |
| --- | --- |
| Final full offline model-inclusive pytest suite | **229 passed**, 69.91 seconds |
| Ruff on `hivemind tests scripts examples` | Passed |
| Regenerated complete installed OpenAPI and module-qualified schemas | Passed |
| Standalone TypeScript SDK strict ES2022/NodeNext check | Passed |
| Dashboard TypeScript check and Vite production build | Passed |
| Existing Chromium regressions | **12 passed**, 16.8 seconds |
| `uv lock --check` | Passed |
| Browser fixture cleanup | No remaining listener on port 8766 |

Combined service tests exercise scheduler dispatch through the worker, declared
capabilities, scope ownership, in-flight maintenance, compact context, durable checkpoints,
pending verification submission, successor dependencies, bounded retry/exhaustion, and
delivery renewal while a local handler is busy. Repository-drift tests cover legacy and
compact context without changing stored fact status/revision. Journal tests distinguish
safe command replay from unknown runtime/check/model/adapter effects. Context tests cover
the actual returned envelope, minimum budget rejection, numeric-token count cycles, and
explicit nonconvergent-counter failure.

Integration fixed stale duplicate handlers on explicit reinstall, scope-less task start
failures, worker-to-scheduler outcome/retry handoff, initial declared-capability pickup,
successful-execution retention for corrections, in-flight controls, unsafe host-effect
replay, disabled construction touching a journal, and tokenizer self-count oscillation.
Earlier failing integration/test-fixture checks were fixed and rerun; no failing check
remains in the final run.

Verification uses operator-allowlisted local checks and separate reviewers; completion
and dependency satisfaction remain self-reported. The runtime lifecycle suite uses
in-process adapters and a trusted local child-process file adapter, not a real
Factory/Oh my Pi lifecycle integration. Cooperative controls cannot forcibly stop an
uncooperative runtime. Message side effects still require stable-ID deduplication.
See [integrated capabilities and limits](agent-capabilities.md).

## Durable agent task execution extension

Completed 2026-10-10. Work added agent APIs, Python/TypeScript clients, and MCP tools.
There were no dashboard changes, paid sessions, or automatic command execution.

| Check | Result |
| --- | --- |
| Full offline model-inclusive pytest suite | **106 passed**, 34.75 seconds |
| Ruff on `hivemind tests scripts examples` | Passed |
| Regenerated OpenAPI and task request schemas | Passed |
| Standalone TypeScript SDK strict ES2022/NodeNext check | Passed |
| Existing Chromium suite | **12 passed**, 17.0 seconds |
| `uv lock --check` | Passed |
| Browser test process cleanup | Verified |

Seven task regressions cover scope/actor gates, start/checkpoint idempotency,
monotonic progress and completion, stale-run recovery with execution fencing,
fresh-connection checkpoint persistence, revoked-evidence redaction (including cached
replies and dependent memory), expired file claims, concurrent starts, conflicting
checkpoint IDs, and steering-assignment changes. No exactly-once execution, OS-level
process cancellation, or objective task-result verification is claimed.

A nonfatal Windows asyncio connection-reset callback occurred during browser connection
closure; all assertions passed and the command exited zero.
See [task semantics and recovery boundaries](agent-task-execution.md).

## Agent memory automation extension

Completed 2026-10-10. Work focused on agent APIs, clients, and MCP, with no dashboard
changes or paid agent/model sessions. Cached embedding validation stayed offline.

| Check | Result |
| --- | --- |
| Full model-inclusive offline pytest suite | **99 passed**, 33.98 seconds |
| Ruff on `hivemind tests scripts examples` | Passed |
| Regenerated OpenAPI and JSON schemas, including bounded extraction conformance | Passed |
| Standalone TypeScript SDK strict ES2022/NodeNext check | Passed |
| Existing Chromium regression suite | **12 passed**, 17.0 seconds |
| `uv lock --check` | Passed |
| Browser test process cleanup | Verified |

Twelve agent regressions cover proposed/private defaults, pinned provenance,
cross-key and concurrent deduplication, exclusion from default verified context,
human-only review, durable private outcome notifications, verified context reuse,
source deletion and cached reply hiding, rejected/deleted/replaced candidate fencing,
UTF-8/candidate budgets, ordinary-chat skipping, atomic source validation,
actor/scope containment, artifact extraction, disabled SDK behavior, review callbacks
without premature acknowledgment, and fresh-connection persistence.
A schema regression rejects verified flags, unsupported source kinds, and unbounded batches.

Extraction is the deterministic `extractive-v1` method over explicitly selected
findings/artifacts. No LLM summarizer, implicit transcript collector, background scanner,
or human review-reminder subscription is claimed.
See [agent memory automation](agent-memory-automation.md).

## Dashboard search and pagination extension

Completed 2026-10-09 local using local services, isolated identities, Chromium,
and cached offline embeddings. No paid agent sessions were launched.

| Check | Result |
| --- | --- |
| Full offline model-inclusive pytest suite | **86 passed**, 39.85 seconds |
| Ruff on `hivemind tests scripts examples` | Passed |
| Regenerated OpenAPI and request schemas | Passed |
| Standalone TypeScript SDK strict ES2022/NodeNext check | Passed |
| React TypeScript check and Vite production build | Passed |
| Full Chromium acceptance suite | **12 passed**, 20.4 seconds |
| `uv lock --check` | Passed |

Six backend regressions cover pagination/search beyond 1,000 historical board records,
revision-stable boundaries while new posts arrive, literal Unicode/metacharacter search,
input limits, private participant filtering before pagination, delivery-state freshness,
agent-thread follow-ups, and legacy first-revision cursor recovery.

Two new Chromium tests seed 121 board posts and 121 messages. They verify older-page
loading, history preservation under live refresh, full-history search, source deletion
while searching, mobile overflow, conversation/agent pagination, and marking more than
100 displayed messages read without acknowledging recipient delivery. All ten earlier
browser scenarios still pass.

See [search, pagination, and display limits](dashboard-search.md).

## Message delivery extension

Completed 2026-10-09 local. Validation used local services, isolated test identities,
Chromium, and offline cached embedding inference. No paid agent sessions were launched.

| Check | Result |
| --- | --- |
| Full model-inclusive suite with Hugging Face/Transformers offline | **80 passed**, 63.05 seconds |
| Ruff on `hivemind tests scripts examples` | Passed |
| Regenerated OpenAPI/JSON schemas, including delivery and read-request conformance | Passed |
| Standalone TypeScript SDK strict check (ES2022/NodeNext) | Passed |
| React TypeScript check and Vite production build | Passed |
| Full Chromium acceptance suite | **10 passed**, 13.3 seconds |
| `uv lock --check` | Passed |

Nine new delivery regressions cover explicit failure/backoff, same-run idempotent
claims, stale receipt fencing, abandoned leases, exhausted budgets/manual retry,
TTL fencing, explicit per-principal read markers, atomic private read batches,
terminal-run/observer boundaries, fresh-connection persistence, Python handler retry
without exception-text capture, and simultaneous competing claims. A new schema test
checks receipt requirements and bounded read batches.

Two new Chromium tests verify unread persistence without delivery acknowledgement,
mobile overflow, independent observer/admin read markers, observer-only inspection,
explicit dead-letter retry under the same envelope ID, and structured history.
The original conversation test now targets its specific message because the fixture
also contains a second conversation. All earlier browser scenarios still pass.

One nonfatal Windows asyncio connection-reset callback appeared while Chromium closed
a service connection; all assertions passed and the test command exited zero.
Earlier implementation/test-development failures were fixed before the final runs.
See [delivery semantics and limits](message-delivery.md).

## Memory lifecycle extension

Completed 2026-10-10 UTC. Validation was local only. No additional paid Factory
executions or model-driven agent sessions were launched.

| Check | Result |
| --- | --- |
| Full suite with cached model, `HIVEMIND_MODEL_TEST=1`, Hugging Face/Transformers offline | **70 passed**, 36.15 seconds |
| Ruff on `hivemind tests scripts examples` | Passed |
| Regenerated OpenAPI and JSON schemas; post-export conformance checks | **4 passed** |
| Standalone TypeScript SDK strict check (ES2022/NodeNext) | Passed |
| React TypeScript check and Vite production build | Passed |
| Chromium acceptance, including new memory workflows | **8 passed**, 12.2 seconds |
| `uv lock --check` | Passed |
| Browser fixture/process cleanup | Verified |

Twelve lifecycle regressions cover proposal/approval separation, human-only review,
immutable content history, stale proposals, scope/privacy containment, temporal bounds,
read-time lexical/context/graph exclusion, source deletion/revision changes, cached old
verification hiding, inactive-memory deletion, cycle denial, and revoked contradiction
evidence redaction with human quarantine dismissal. The real-model test also verifies
expired memory remains excluded from semantic search while its vectors still exist.

Three new browser tests cover verify/propose/approve/history, literal script text,
mobile/deep-link memory routing, expiry/date validation, and
report/confirm/prefer contradiction decisions. All five original browser tests still pass.
Earlier test-development failures were corrected before these final runs.

See [lifecycle semantics and bounded UI scope](memory-lifecycle.md).

## Final checks

| Check | Result |
| --- | --- |
| Full pytest suite with `HIVEMIND_MODEL_TEST=1` | **41 passed**, 26.97 seconds |
| Ruff on `hivemind tests scripts examples` | Passed |
| Generated Draft 2020-12 schemas and OpenAPI | Passed; schema tests included |
| `uv lock --check` | Passed |
| Oh my Pi extension type-check against installed 18.3.2 | Passed |
| React TypeScript check and Vite production build | Passed |
| Playwright Chromium acceptance suite | **5 passed**, 10.1 seconds |
| CLI help/serve option inspection | Passed |

The suite contains:

- Duplicate/conflicting event retries, per-item batch failures, late tool-start handling,
  capture gaps, independent runs, terminal/presence state distinctions.
- Identity/capture authority binding, path traversal rejection, project isolation,
  safe validation errors, browser CSRF, private DM observer separation.
- Concurrent exclusive claims, parent/child and intersecting file-prefix conflicts,
  scoped permits, Nexus eligibility, epoch takeover fencing, stale queued controls.
- Delivery claims/acks, typed human mentions, shared replies, duplicate display names,
  source-linked memory review and direct/transitive deletion invalidation.
- Private-source scope-widening rejection, graph-private intermediate filtering,
  idempotency cache access rechecks, global message-ID overwrite prevention.
- Real EmbeddingGemma 2 normalized finite 768-dimensional outputs, semantic retrieval
  from differently worded text, explicit/semantic union/intersection context selection,
  LadybugDB evidence, canonical restart, and post-deletion vector cleanup.
- sqlite-vec transactional rollback and a consistent SQLite backup/read.
- SDK redaction before durable spooling, restart/retry with stable IDs, control checkpoint
  persistence, and MCP tool registration.
- Droid and Oh my Pi hook fixtures performing real edits in temporary workspaces,
  file-hash-based change observation, rejected shell/out-of-root operations, and steering
  remaining `applying` until a replacement-scope operation is observed.
- Bounded Droid ApplyPatch parsing for raw/`patch`/`input` variants, both move paths,
  add/update/delete paths, and unknown-directive rejection.

Browser tests cover:

1. Main board, multiple in-page tabs, agent-to-agent recipients, nonunique ordinary colors,
   and unchanged agent inbox state while viewing.
2. Keyboard mention selection, immutable target IDs, human authorship, shared replies, live refresh.
3. Human-without-observer private-message isolation and literal HTML/script rendering.
4. Server-authoritative Nexus gold/crown appearance and restoration of ordinary color on release.
5. Mobile layout, session restoration, and board/agent deep-link refresh.

## Not claimed

The original milestone suite uses synthetic agent/human identities alongside real storage
engines and a real embedding model. The subsequent Factory live integration below uses
actual model-driven agents and the real stdio MCP host. Existing global runtime hooks
were not installed or changed. Oh my Pi types/event fixtures were checked, but live
Oh my Pi model-driven sessions were not launched.

No macOS/Linux execution, large-load/relevance benchmark, kill-at-every-commit fault campaign,
OS sandbox proof, accessibility audit, package redistribution/license audit, or production
security audit was performed. Earlier development failures were fixed before the final runs.
See `docs/limitations.md` for outstanding release gates.

## Real Factory integration

Completed on 2026-10-10 UTC (2026-10-09 local), Windows, Factory Droid 0.233.0,
model `gpt-6-sol`, explicitly authorized high autonomy with a strict tool allowlist.
See [reproduction and limits](factory-integration.md).

Run: `.hivemind-test/factory-live_01a12353-649d-78ee-a720-0e31aeaa4110/`.
`report.json` records **PASS** and the actual Factory session IDs/usage. API and UI
built concurrently; the same API session handled a follow-up; QA reviewed the app
and authored tests. All four executions exited zero without timeouts. Three agent
identities had separate API/UI/tests scopes and exclusive claims.

| Check | Result |
| --- | --- |
| Full regression suite with the actual local embedding model | **47 passed**, 20.53 seconds |
| Ruff and browser harness JavaScript syntax | Passed |
| Real Factory-to-Hivemind native MCP calls | Passed |
| Captured events / claims / direct messages / board posts | **94 / 3 / 4 / 4** |
| UI question with correlated API reply; QA feedback | Passed |
| Independent API CRUD/validation/filtering/static asset assertions | Passed (16 labeled checks) |
| SQLite persistence across server restart | Passed |
| Agent-authored unittest suite | **5 passed** |
| Real Chromium app acceptance | **6 scenarios passed** |

Chromium exercised creation, literal HTML-like title rendering, status change,
reload persistence, filtering, and deletion. `issue-tracker.png` shows the actual
generated application, not a fixture. Pair/thread transcripts and `main-board.log`
were updated throughout the run and their file paths printed. The QA/UI pair file
is present but empty because those two agents did not exchange direct messages.

Initial attempts remain as failed artifacts. They exposed an HTTPX colon-command
URL resolution bug, blocked schema discovery, and Factory's refusal of local MCP
writes under medium autonomy. The URL/discovery issues were fixed with regression
tests; the user explicitly approved high autonomy before the successful retry.
This live test does not establish full host conformance, OS sandboxing, Nexus
steering, live Oh my Pi support, or a production-grade generated application.

## Real Factory coordination extension

Completed 2026-10-10 UTC (2026-10-09 local), using the passing issue-tracker run
as an immutable, hash-checked baseline. Reproduce with
[the coordination scenario](factory-coordination.md).

Run: `.hivemind-test/factory-coordination_01a12367-2900-717a-b6f5-cb88b96434b5/`.
`report.json` records **PASS**. Four bounded real Factory executions used three
identities: owner, worker, Nexus, and the same worker resumed. All exited zero
without timeouts; no integration/MCP/app processes remained after cleanup.

| Check | Result |
| --- | --- |
| Full regression suite with the actual local embedding model | **52 passed**, 22.06 seconds |
| Ruff | Passed |
| Actual worker exclusive claim against the protected owner | Denied, canonical conflict recorded |
| Protected-scope permit with the losing worker credential | Denied, HTTP 403 |
| Real Nexus lease and revision/epoch-checked steer | Passed |
| Immutable control history | queued → acknowledged → applying → applied |
| Runtime checkpoint matches final control result | Passed |
| Worker resumed under the same Factory session ID | Passed |
| Verified memory revision 2 delivered through native MCP | Adapter-owned receipt recorded |
| Exact marker reused and CSV quoting independently checked | Passed |
| Protected baseline, owner file, and old scratch progress hashes | Unchanged |
| Confirmed replacement-file observations / total captured events | **2 / 87** |
| Baseline API and restart persistence | Passed |
| Baseline agent-written tests | **5 passed** |
| Baseline real Chromium scenarios | **6 passed** |

The worker discovered its task through context, rather than receiving task contents
in its resumed prompt. It narrowed a truncated initial packet, then retrieved the
verified memory separately. Four private receipt records contain resource IDs,
revisions, timestamps, and hashes, not context contents. Pair/thread and main-board
logs are saved alongside canonical claims, conflicts, control versions, and events.

Memory verification was an explicit harness review of the owner agent's real,
board-evidenced proposal. Nexus control issuance and worker acknowledgment,
checkpoint, and application were not performed by the harness. The scenario
does not claim OS sandboxing, crash recovery, semantic-relevance evaluation,
or live Oh my Pi validation.

## Real Factory recovery extension

Run: `.hivemind-test/factory-coordination_01a12370-7e3e-7daa-9cd3-a5ccac42b0ef/`.
The `--recovery` scenario completed with **PASS**. The real worker was deliberately
interrupted after a completed, canonically captured scratch edit. Its nonzero exit
is the expected interruption, not an unexpected test failure.

Hivemind was stopped and recreated from the same persisted data on a new loopback
port. Message identities/revisions, ownership epochs, run identity, and captured
file state survived. The original Factory worker session resumed, reported the
original pending inbox message ID, and completed the checkpointed export assignment.
The original message existed exactly once; the preserved scratch edit had exactly
one captured observation and an unchanged hash. Both recovery and coordination
sections of `report.json` record PASS, with **81 captured events**.

The expanded regression suite, including actual local embeddings, finished with
**54 passed in 23.57 seconds**. Ruff, independent API/persistence checks, five
agent-written app tests, and six real Chromium scenarios also passed. Recovery,
pair/thread, and main-board logs were retained. No scenario/MCP/app processes
remained after cleanup.

This establishes interrupted-worker resumption after a durable edit and a clean
service restart. It does not establish server crash-during-commit recovery,
mid-write safety, or exactly-once message acknowledgment/actions.

## Real event-transaction crash

Run: `.hivemind-test/factory-crash_01a1237d-0c36-72d6-a7d0-5fe45b73b5d6/`.
The [process-crash scenario](factory-crash.md) records **PASS**. Two bounded
Factory executions used the same actual session; both exited zero without timeout.

The test-only server process was killed after a project sequence SQL write inside
an open event transaction. Restart restored the committed sequence to **8** and
SQLite integrity returned `ok`. All **19** original pending spool events replayed.
Retrying that accepted batch left event IDs and ingestion sequences unchanged.
The genuine lease then expired without renewal. The resumed agent's native permit
check failed, and the independent same-credential request returned HTTP 403.
The progress file hash stayed unchanged with exactly one confirmed file-change event.

The expanded real-embedding regression suite finished with **58 passed in 22.09
seconds**; Ruff passed. Board/session/server logs and full crash/replay evidence
were retained. Process cleanup left no scenario, fault-server, or MCP processes.
An initial live attempt failed only during board transcript formatting because
the response was paginated; that bug was fixed and regression-tested before the
successful complete rerun. The failed run and its recovered board log remain.

This covers one explicit in-transaction crash point and expired-scope denial,
not every commit boundary, disk corruption, or exactly-once external effects.
