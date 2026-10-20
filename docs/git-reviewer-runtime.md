# Independent reviewer runtime (`hivemind/git_reviewer_runtime.py`)

Opt-in, bounded execution of an **independent reviewer agent** against a read-only
snapshot of a merge request's exact candidate commit. The reviewer's strictly validated
structured verdict is submitted through the ordinary canonical
`merge-requests/{id}:review` path — independence, exact source/target/candidate hash
binding, revision fencing, and audit all still apply.

## Guarantees

- **Two explicit opt-ins**: `ReviewerRuntimePolicy(enabled=True)` *and*
  `allow_model_calls=True`. The vendor adapter's own opt-ins are never overridden.
- **Real canonical identity, never synthetic authority**: each review creates an
  ordinary run (`Service.start_run`), a **scope-less** review task (`Service.create` —
  no file grant), and a real execution (`Service.task_start`) under the reviewer agent's
  own principal. The `run_id`/`task_id`/`execution_id` given to the vendor adapter are
  canonical, so the existing managed vendor hooks (`hivemind.hooks`) validate them,
  enforce the scope-less read-only permit boundary, and heartbeats operate on the live
  run.
- **Independent identity**: the Nexus agent passes `reviewer_principal_id` /
  `reviewer_agent_id`; both are validated against the principal row, an optional policy
  allowlist, and the producer (`independent_reviewer_required` on self-review).
- **Snapshot-bound runtime**: the runtime comes from an injected
  `runtime_factory(workspace=snapshot_path, context=dict)`. Execution is refused unless
  `runtime.workspace` resolves to the exact snapshot
  (`review_runtime_workspace_mismatch`), and the adapter's `tool_names` are restricted
  to the vendor's read-only subset (`droid`: Read/Grep/Glob/LS; `oh-my-pi`:
  read/grep/glob; unknown vendors refused) before any model session.
- **Exact candidate snapshot**: fresh `--no-local` clone, detached at `candidate_oid`,
  verified clean (HEAD + `git status --porcelain`) before and after execution; any
  modification cancels the review (`review_snapshot_modified`) and settles the review
  task failed.
- **Bounded input**: the read-only diff from the canonical repo is rejected at
  `max_diff_bytes` (default 128 KiB) *before* any canonical review task or model
  session is created.
- **Transaction segmentation**: canonical mutations happen in short separate
  `store.transaction()` segments (open run/task/execution; later submit review + settle
  task). Model execution runs **outside** any SQLite transaction/store lock — the
  module refuses to execute while `store.db.in_transaction` is true — so vendor hooks
  can call the service and heartbeats work during the turn.
- **Durable, fenced receipts**: execution and submission are separate
  `HandlerJournal` phases in a `recovery.Journal`
  (default `<git-collaboration>/reviewer-effects.sqlite3`). A crash or timeout leaves an
  `unknown` receipt and raises `git_review_effect_unknown`; **a verdict from an
  abandoned (timed-out) thread is never submitted**. Definite failures (invalid verdict,
  tampered snapshot, workspace mismatch) settle the canonical review task `failed`;
  unknown outcomes leave it `running` for explicit operator reconciliation.
- **No duplicate reviews**: a retry after success is recognized from the canonical
  record (same reviewer principal + exact binding) and returns
  `MergeReviewOutcome(already_reviewed=True)` without creating runs or re-running any
  model session.

## Structured verdict contract (parent-owned VendorRuntime addition)

The reviewer module consumes **only** `result["structured"]`. Parent adds to
`VendorRuntime` an explicitly enabled, bounded structured-output return, preserving the
current hash-only default:

- New runtime option, e.g. `"structured_output": {"enabled": true, "max_bytes": 16384}`
  (default absent/disabled → today's hash-only `{"status", "summary"}`, unchanged).
- When enabled and the turn completes, `execute` returns:

  ```json
  {"status": "completed", "summary": "<bounded text summary>",
   "structured": {"decision": "approve|reject", "reason": "1..4096 chars",
                  "source_oid": "<40 hex>", "target_oid": "<40 hex>",
                  "candidate_oid": "<40 hex>", "findings": ["... <=32 items"]}}
  ```

- `structured` must be the parsed single JSON object from the model's final message,
  serialized size ≤ 16384 bytes; absent on failure/pause or when disabled.

The reviewer module validates `structured` with `ReviewerVerdict` (pydantic,
`extra="forbid"`, OID patterns, bounded findings) and requires the exact
source/target/candidate triple to match the merge request
(`review_verdict_binding_mismatch` otherwise). A legacy hash-only result fails closed
with `review_structured_output_unavailable`.

### Prompt contract (`review_verdict/v1`)

The canonical review task description carries
`canonical({"contract": "review_verdict/v1", "merge_request_id", "base_oid",
"binding": {source_oid, target_oid, candidate_oid},
"respond_with_exactly_one_json_object": <JSON schema>})` and instructs the reviewer to
answer with exactly one JSON object echoing all three OIDs. The execution `packet`
carries the same binding plus `base_oid`, the bounded `diff`, `snapshot_read_only:
true`, and `contract`.

## Injectable bridge (Nexus loop agent)

```python
outcome = reviewer_runtime.review_merge(
    merge_request_id: str, *,
    reviewer_principal_id: str, reviewer_agent_id: str,
    expected_revision: int | None = None,
) -> MergeReviewOutcome
```

`MergeReviewOutcome`: `merge_request_id`, `reviewer_principal_id`,
`reviewer_agent_id`, `decision` (`approve|reject`), `already_reviewed`,
`review_run_id` / `review_task_id` / `review_execution_id` (canonical IDs, `None` on
duplicates), the three OIDs, `snapshot`, `snapshot_verified_unchanged`, and `receipts`
(execution/submission fingerprints and dedup flags).

Worker notification entry point (duck-typed for `AgentWorker._poll_notifications`):
`review_notification(notice)` — consumes notices carrying `merge_request_id` (plus
optional `reviewer_principal_id`; falls back to the policy allowlist), returns
`{"handled": False, ...}` for anything else.

## Wiring steps for the parent

1. **Install after git collaboration**:

   ```python
   from hivemind.git_reviewer_runtime import (GitReviewerRuntime,
                                              ReviewerRuntimePolicy, install)
   executor = install(app, service, principal, envelope=envelope,
                      runtime_factory=runtime_factory,
                      policy=ReviewerRuntimePolicy(
                          enabled=True, allow_model_calls=True,
                          reviewer_principal_id="<optional allowlist principal id>"))
   ```

   Mounts `POST /v1/git-reviewer-runtime:review` with body
   `{"merge_request_id", "reviewer_principal_id", "reviewer_agent_id",
   "expected_revision"?}`. The endpoint deliberately does **not** use
   `command`/`service.mutate`: those wrap the handler in one store transaction, which
   would hold the canonical lock during model execution and break vendor
   hooks/heartbeats. The executor performs its own segmented transactions and
   journal-fenced idempotency instead. (The handler is a blocking sync call inside the
   async endpoint, consistent with the other domain handlers.)

2. **Runtime factory** (host-supplied): `runtime_factory(workspace, context)` must
   construct the existing vendor adapter (`FactoryRuntime`/`OhMyPiRuntime`) with
   `options["workspace"] = str(workspace)` (the snapshot path it receives), a fresh
   `state_file` outside the workspace, and a reviewer-identity config
   (`project_id`/`agent_id`/`token` for the reviewer) so the managed hooks
   authenticate as the reviewer against the canonical run/task/execution in `context`.
   Do not invent vendor wire protocols.

3. **Vendor structured output** (parent, shared file `vendor_runtimes.py`): add the
   opt-in `structured_output` return described above; disabled default preserves
   hash-only output.

4. **Nexus loop**: call `review_merge(...)` with the reviewer identities, or emit a
   `git_review_requested` notification carrying `merge_request_id` (+
   `reviewer_principal_id`) so `review_notification` can consume it.

5. **CLI** (optional): `git-reviewer review <merge-request-id>
   --reviewer-principal <id> --reviewer-agent <id>` loading the same policy options;
   both opt-ins stay explicit flags, disabled by default.

## Limits (honest)

- **Not an OS sandbox.** Read-only enforcement is canonical (scope-less task → no write
  permit; managed hooks enforce permits per tool call), tool-policy restriction
  (read-only tool allowlist), and verified-unchanged snapshot checks. A process that
  maliciously writes outside permitted paths at the OS level is not contained by this
  module; the existing `require_os_isolation` vendor option remains the correct refusal
  path where a verified OS sandbox is required.
- Timeout abandons the execution thread (daemon); reconciliation is via the vendor
  adapter's own `pause`/`cancel`. No destructive cleanup is performed; review snapshots
  accumulate under `<git-collaboration>/reviewer-snapshots/` for operator GC.
- The endpoint blocks the event loop during execution (consistent with existing sync
  domain handlers); run behind a worker if that matters.

## Validation

`tests/test_git_reviewer_runtime.py` — 13 offline tests using the **native**
Factory/OhMyPi adapters with their local fake SDK/transport contracts (no paid or live
model calls); the structured verdict is supplied by a test-local subclass standing in
for the parent-owned structured-output opt-in:

```
.venv\Scripts\python.exe -m pytest tests/test_git_reviewer_runtime.py -q   # 13 passed
```

Coverage: disabled/opt-in gates, identity mismatch and allowlist rejection, producer
self-review refusal, full native-Factory flow proving canonical run/task/execution IDs,
managed hook config binding (managed IDs, reviewer identity, snapshot workspace root),
read-only tool policy, no held SQLite transaction during execution, durable receipts,
native OMP contract and read-only tools, hash-only fail-closed path with task settled
failed, timeout fencing to unknown with no submission and an unreconciled running task,
mis-bound verdict refusal, snapshot tamper cancellation, workspace-mismatch refusal
before any model call, duplicate recognition without reruns, diff-bound refusal before
creating work, notification bridge, revision fencing, strict verdict schema.
