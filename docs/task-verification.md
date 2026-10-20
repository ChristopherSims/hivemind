# Objective task verification

A second, separate resource records what a producing agent *reports* about a task
execution and what an *independent* reviewer concludes after local trusted checks.
The module is `hivemind/verification.py`; it is installed by the host application and
does not modify task state, memory, claims, controls, or any other domain.

This is a local, single-administrative-domain milestone. There is no sandbox, no
privilege escalation, no downloaded code, no remote write, and no paid agent session.
Every limit below is a real limit of this implementation.

## Reported completion is not verification

`POST /v1/tasks/{id}:checkpoint` with `status: "completed"` is a self-report. The task
record keeps that meaning. Verification adds a distinct resource whose own status is
`pending`, `needs_corrections`, `verified`, or `rejected`, and never rewrites the task,
never releases claims, and never creates or promotes a memory.

Producer-supplied text is stored as an attestation with `trust: "self_reported"` and
`objective: false`:

- `reported.completion_claimed`, `reported.summary`, `reported.reported_by_agent_id`.
- `test_evidence[]` entries carry `name`, `outcome` (`pass`/`fail`/`unknown`), optional
  `detail`, plus `trust: "self_reported"` and `objective: false`.

Self-reported evidence can only ever obstruct a decision:

- A reported `fail` blocks `verified` (`reported_test_failure`) until it is resolved.
- A reported `pass` is never sufficient. `verified` additionally requires at least one
  trusted local check with exit code 0 and no trusted check that failed
  (`trusted_check_required`, `trusted_check_failed`).
- Omitted or hidden failures are not detected, because self-reports are not trusted and
  are not compared against the workspace.

A verification view exposes both halves explicitly: `reported_completion` (boolean, from
the producer) and `independently_verified` (boolean, true only when the stored status is
`verified`, the reviewer agent is recorded, and the pinned evidence is still available).

## Submission

`POST /v1/verifications`

```json
{
  "task_id": "tsk_export",
  "run_id": "run_worker_1",
  "execution_id": "execution_...",
  "completion_claimed": true,
  "summary": "Export implemented; unit tests reported passing",
  "artifact_refs": [{"kind": "artifact", "id": "art_patch", "revision": 1}],
  "test_evidence": [{"name": "unit", "outcome": "pass", "detail": "agent-reported"}]
}
```

The submission is bound to authoritative state, not to caller claims:

- The task is read through the caller's authorization; `run_id` must belong to the
  producing agent, the same project, and must not be terminal (`terminal_run`).
- `execution_id` must match the task's current execution and that execution's run
  (`task_execution_required`, `stale_task_execution`). A recovered or replaced execution
  cannot be extended by the old verification.
- `artifact_refs` accept only `artifact` and `board_post`, are re-validated with the
  shared evidence rules, and are pinned to the current revision. Unpinned references are
  filled with the current revision. A task cannot reference itself
  (`self_evidence_forbidden`).
- Private evidence is refused: evidence private to another agent is invisible (`not_found`),
  and evidence private to the producer is rejected (`evidence_scope_violation`, 403).
- One *open* verification (status `pending` or `needs_corrections`) may exist per
  (task, execution); a second attempt returns `verification_open`. Rejected or verified
  verifications are immutable history and do not block a fresh submission.

`POST /v1/verifications/{id}:update` amends a `pending` submission by the producing
agent (an administrator may also act).
It restates `artifact_refs` and `test_evidence` completely, is bound to the same
execution, and is revision fenced. Decided verifications cannot be amended
(`verification_not_editable`).

Reads: `GET /v1/verifications/{id}` and
`GET /v1/verifications?project_id=&task_id=&status=`. The list is bounded to 1,000
recent verifications per project and is a bounded scan, not a scheduler.

## Reviewer assignment

`POST /v1/verifications/{id}:assign-reviewer` with `expected_revision` and
`reviewer_agent_id`.

- The reviewer must be a registered agent in the same project, and must differ from the
  producing agent (`reviewer_must_differ_from_producer`).
- Assigners: an administrator or human principal, or the producing agent requesting a
  reviewer. Unrelated agents and observers are refused (`reviewer_assignment_forbidden`).
  A reviewer agent cannot assign itself.
- Assignment is possible while the verification is `pending` or `needs_corrections`, so a
  second reviewer can take over after corrections. Decided verifications cannot be
  reassigned (`verification_decided`).

`POST /v1/verifications/{id}:decide` with `expected_revision`, `decision`, `reason`, and
`correction_items` (required for `needs_corrections`).

- A decision requires an assigned reviewer (`reviewer_assignment_required`) and the
  caller must be able to represent that reviewer agent (`forbidden`). An administrative
  principal may record the decision, but a principal that represents the producing agent
  cannot (`self_review_forbidden`), and the reviewer agent must still differ from the
  producer.
- `verified` requires: no open corrections, all pinned evidence currently available, no
  producer-reported failing test, at least one trusted check with exit code 0, and no
  failing trusted check.
- `rejected` is terminal and may be recorded even while corrections are open.
- `needs_corrections` requires at least one correction item and is bounded by a cycle
  limit (default 5, `correction_limit_reached`). A second correction cannot be stacked
  while one is open (`corrections_open`).
- The decision is recorded in `review` (`decision`, `reviewer_agent_id`,
  `decided_by_principal_id`, `decided_at`, `reason`, `trust: "independent_review"`).

## Trusted local verifier

`TrustedVerifier(workspace_root, checks, *, runner=None, timeout=60, max_output_bytes=65536,
max_file_bytes=1048576, extra_environment=None)`.

`checks` maps a check name to an operator-supplied argv array. Agents never supply a
command, a flag, an interpreter, or a shell string; a request can only name an
allowlisted entry.

Enforced properties:

- `shell=False` always; no `sh -c`, no inline source flags (`-c`, `-e`, `--eval`,
  `-command`, `/c`, `/k`, `-encodedCommand`, interactive flags), no shell programs as
  `argv[0]`, no `..` in the program path, no URL-like arguments (nothing is downloaded).
- Working directory is the resolved workspace root; every declared artifact path is
  normalized workspace-relative and resolved, and a path that escapes the root (including
  via symlink) is refused (`artifact_path_escape`, `invalid_artifact_path`).
- The environment is rebuilt from a fixed allowlist of variables plus explicit
  `extra_environment` entries; arbitrary inherited variables (secrets, tokens) are dropped.
- `stdin` is `DEVNULL`, output is captured, and the run is bounded by `timeout` (at most
  600 s). A timeout is recorded, not raised.
- Each run records `argv`, `cwd`, `returncode`, `timed_out`, `duration_ms`, full-stream
  `stdout_sha256`/`stderr_sha256` with byte counts, truncated text (`max_output_bytes`),
  and declared artifacts with `sha256`/`byte_size` (or `missing`/`skipped`).
- `ok` means exit code 0, no timeout, and no internal error. A broken check is recorded
  with `error` instead of failing the request.
- An injected `runner` may replace the subprocess: it receives a frozen `TrustedCommand`
  (`name`, `argv`, `cwd`, `timeout`) and returns `(returncode, stdout, stderr)` or an
  object with those attributes; raising `TimeoutError` records a timeout. This is how the
  tests exercise pass/fail without executing anything.

`POST /v1/verifications/{id}:check` with `expected_revision`, `check_name`, and optional
`artifact_paths`. Only an administrator or human principal may record a check
(`trusted_verifier_required`, 403); no verifier configured returns
`trusted_verifier_unavailable` (503). Checks are allowed while `pending` or
`needs_corrections`, are capped at 16 per verification, and store
`trust: "trusted_local_check"` and `objective: true`.

What a trusted check does and does not prove: it proves that the allowlisted command ran
with that argv, in that working directory, and exited with that code at that time, and it
records the hashes of the declared artifacts. It does **not** prove that the producing
agent ran the command, that the workspace was unmodified at the time, or that the work is
correct. Point the verifier at a pinned or operator-controlled checkout, not a live
directory the reviewed agent can freely rewrite, and prefer checks whose assertions are
in the operator's own test code.

## Corrections

`needs_corrections` creates a `correction` resource:

- `items` (required changes), `state` (`open`/`resolved`), `reason`, `reviewer_agent_id`,
  `verification_id`, `task_id`.
- Reads: `GET /v1/corrections/{id}` and
  `GET /v1/verifications/{id}/corrections` (open then resolved ids, deleted corrections
  skipped).

`POST /v1/corrections/{id}:resolve` with `expected_revision`, `run_id`, `execution_id`,
`summary`, and optional `artifact_refs`.

- Only a principal that can represent the producing agent may resolve (the reviewer and
  unrelated agents get `forbidden`; an administrator may also act), and only for the
  verification's original execution (`stale_task_execution`), so a recovered execution
  must submit a new verification instead.
- Resolution evidence is pinned and validated like submission evidence, appended to the
  verification's `evidence_refs` (deduplicated by kind/id/revision), and marked
  `trust: "self_reported"`.
- The correction becomes `resolved`, the verification returns to `pending` for re-review
  (the current reviewer assignment is kept unless it is reassigned), and both
  `correction_history` entries are appended (bounded to 20). A resolved correction cannot
  be resolved twice (`correction_not_open`).
- A cycle is counted when a correction is created. Nothing resolves automatically.

## Evidence invalidation and read privacy

Verification and correction reads re-validate every pinned reference against canonical
state on each read, including cached idempotent replies:

- Deleted, kind-mismatched, re-revisioned (tampered), or no-longer-visible evidence sets
  `evidence_unavailable: true`.
- When evidence is unavailable the producer summary and test evidence are redacted (the
  boolean `completion_claimed` and reporter identity remain), and `effective_status`
  becomes `pending`. The stored `status` is preserved for audit, but the verification can
  no longer be treated as proof (`independently_verified` is false).
- Verification reads are project-scoped. Another project cannot read a verification or
  list this project's verifications (`not_found`). Observers may read but cannot submit,
  assign, or decide.

## Commands, idempotency, and fencing

All mutating routes require an `Idempotency-Key` and go through the shared command path.
Repeating a key with the same payload returns the original response; reusing it with
different contents returns `idempotency_conflict`. Cached replies are re-validated against
current authorization and evidence state, so a retry cannot bypass a deletion or a privacy
change. Updates carry `expected_revision` and return `revision_conflict` (412) on stale
state.

## Limitations

- No task, claim, control, or memory is changed by any verification route. There is no
  automatic promotion to memory, no automatic task completion, and no automatic
  correction resolution.
- `verified` is an independent-review verdict backed by local trusted checks. It is not
  proof that the agent's work is correct, that the workspace was untouched, or that a
  hidden test failure does not exist.
- The trusted verifier is not a sandbox. It does not isolate the process, restrict network
  access, or prevent the workspace contents from influencing a check. Privilege,
  filesystem, and network isolation remain the operator's responsibility.
- Artifact hashing happens after the command completes (TOCTOU): a workspace that changes
  a declared file between the run and the hash read is not detected.
- Check records are historical observations. Later changes to the workspace do not
  invalidate them, and no re-run is scheduled automatically.
- Verification bodies are not lexically or semantically indexed (the shared indexer only
  covers agent, board post, memory, task, work claim, and artifact), so verification text
  is not searchable through the board search surface.
- Reads and lists are bounded (1,000 recent verifications; a bounded scan per project) and
  there is no pagination or retention workflow for verification history.
- There is no reviewer eligibility policy beyond project membership and "not the producer",
  no reputation, no reviewer recusal, and no multi-reviewer quorum.
- There is no background scheduler, lease, or notification: a parent process must poll
  `GET /v1/verifications?status=pending` (or open corrections) to drive review.
- Fine-grained authorization grants, remote/multi-tenant deployment, SDK, MCP, CLI, and
  dashboard surfaces are not part of this milestone.

## Host integration

```python
from hivemind import verification

domain = verification.install(app, verifier=verification.TrustedVerifier(root, {
    "unit": ["python", "-m", "pytest", "-q"],
}))
```

`install(app, service=None, principal=None, command=None, envelope=None, *, verifier=None)`
accepts the same collaborators the API factory uses (service, principal dependency,
command/idempotency wrapper, response envelope) and falls back to self-contained defaults
built from `app.state.service` when they are omitted. It registers the routes above,
stores the domain on `app.state.verification`, stores the verifier on
`app.state.trusted_verifier` (replaceable at runtime), and returns the domain so a parent
can adjust `max_cycles`.

Follow-ups the host owns:

- Add a discovery capability such as `tasks.verification` to `GET /.well-known/hivemind`.
- Add SDK/MCP/dashboard surfaces if they are wanted; this milestone adds HTTP routes only.
- Wire a scheduler if pending verifications and open corrections should be driven
  automatically; the API exposes the pending queue and open correction ids for that.
