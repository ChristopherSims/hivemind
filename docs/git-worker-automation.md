# Autonomous worker Git workflow

`hivemind/git_worker_automation.py` implements an **opt-in, bounded, resumable** Git
workflow for a single worker run. It is a cooperative client-side companion to
`AgentWorker`: it launches no model sessions, opens no listeners, pushes only to the
project-local bare repository through the canonical API, and never touches host Git
configuration or identity.

## Guarantees

| Guarantee | Mechanism |
| --- | --- |
| Default off | `enabled=False` performs zero requests and zero filesystem writes (`git_automation_disabled`). |
| Bounded provisioning | One durable workspace per agent; `max_workspaces` (1..8) is a hard fence (`git_workspace_budget_exhausted`). |
| Task/execution bound | Every commit/push carries `task_id`/`run_id`/`execution_id`; a task whose execution is not owned by this run is refused client-side (`stale_task_execution`) and again server-side. |
| Exact hashes | Source/target OIDs come only from canonical API responses and are echoed verbatim; a submit whose response echoes different hashes raises `GitAutomationUnknown`. |
| Scope claims preserved | Client-side path fence (`git_path_not_permitted`) mirrors the server fence; the server re-checks permits regardless. |
| Dirty/user work preserved | Native Git is used only for `fetch`, `rev-parse`, `diff`, `add` (inside a host resolver), and inspection — never `reset`, `clean`, `checkout --`, branch switches, or ref updates. |
| Author identity preserved | No `git config` writes; commits use the ordinarily configured identity plus the server-appended `Co-authored-by` trailer. |
| Conflicts durable and resumable | Conflicted submissions are recorded in an atomic state file with a bounded attempt budget; resolution is reported only after native verification observes an in-progress merge with zero unmerged paths. Success is never invented. |
| Unknown effects fenced | Timeouts, transport-boundary violations, and ambiguous echoes raise `GitAutomationUnknown` instead of fabricating a result. |
| No paid calls / network exposure | All HTTP goes to the local service; native Git runs with `GIT_TERMINAL_PROMPT=0` and `GIT_ALLOW_PROTOCOL=file`. |

## Interface

```python
from hivemind.git_worker_automation import (
    GitWorkerAutomation, GitAutomationError, GitAutomationUnknown, validate_paths,
)

auto = GitWorkerAutomation(
    client, project_id, agent_id, run_id,
    enabled=True,                    # required opt-in
    state_file=state_dir / "git-automation-state.json",  # durable, resumable
    max_workspaces=1,
    max_resolution_attempts=3,       # hard budget per conflict session
    allow_native_git=True,           # separate opt-in for local fetch/verify
    git_timeout=20,
    journal=None,                    # optional duck-typed append/record sink
)

workspace = auto.provision()                       # idempotent per (project, agent)
result = auto.publish(workspace, task, execution_id,
                      "Implement export result", ["export/result.txt"], "Export implementation")
# result["status"] == "open"      -> merge_request with exact candidate_oid
# result["status"] == "conflicts" -> result["conflict"] is a durable session

auto.fetch_protected(workspace)                    # git fetch origin main only
auto.conflicts()                                   # unresolved durable sessions
resolved = auto.resolve_conflict(merge_request_id, resolver)
# resolver(workspace_path, conflict_paths, session) -> iterable of resolved paths;
# typically: git merge --no-edit origin/main, edit files, git add -- <paths>
```

`commit`, `push`, and `submit` are also exposed individually with deterministic
idempotency keys (`deterministic_key(action, ...)`): identical retries replay the recorded
canonical command, changed intent is a new command.

## Worker-facing facade

`WorkerGitFacade(automation)` is the surface the parent wires around the runtime turn:

```python
facade = WorkerGitFacade(auto)

# BEFORE the runtime adapter executes (parent CLI provisions the clone first and
# constructs the runtime with workspace=<clone path>; no user workspace is relocated):
workspace = facade.prepare(task, runtime)
# - provisions the bounded clone (idempotent),
# - refuses unless Path(runtime.workspace).resolve() == clone path
#   (git_runtime_workspace_mismatch),
# - refuses any pre-existing dirty/staged/untracked entry (git_workspace_dirty);
#   autonomous editing never starts over user work.

result = facade.finish(task, execution_id, runtime, context, checkpoint)
# AFTER a completed runtime turn, BEFORE the terminal checkpoint:
# - derives literal changed paths from `git status --porcelain=v1 -z --untracked-files=all`,
# - returns {"status": "clean", "changed_paths": [], "published": False} for a no-change
#   turn (nothing is claimed),
# - otherwise validates paths, records a RUNNING checkpoint, then commits/pushes/submits
#   through the canonical API while the canonical task is still running,
# - stale execution ownership raises git/stale_task_execution (GitAutomationError).
```

## Vendor conflict resolver

`VendorGitConflictResolver(auto, runtime, task, context, checkpoint)` is a ready-made
`resolver` for `GitWorkerAutomation.resolve_conflict`:

1. `git merge --no-commit --no-ff origin/main` (never force/reset/clean).
2. One bounded, scoped vendor `runtime.execute(task, context, checkpoint)` on the **real
   current task/execution** with a new deterministic `phase_id` =
   `git-conflict-<merge_request_id>` — a replay hits the vendor runtime's durable
   state fence instead of double-executing. The runtime's own `enabled`/`allowed` opt-ins
   remain the paid-call boundary; construction refuses runtimes lacking them
   (`git_resolver_runtime_disabled`).
3. Stages **only** the allowed recorded conflict paths (`git add -- <paths>`), then
   independently verifies zero unmerged entries; otherwise returns `[]` and the automation
   durably records `unresolved`. Baseline files staged by the merge itself are left for
   `_baseline_imports` to exact-verify against `origin/main`.

## Required parent change: baseline validation in `GitCollaboration.commit`

Importing current `main` during conflict resolution stages protected-baseline files the
agent never edited. The canonical commit currently rejects the merge commit with
`git_unrelated_staged_changes` whenever any staged path is outside the request's path list,
and `_permit` rejects any staged path outside the task's scopes — even when the staged
content is byte-identical to the protected tip. The automation works around the in-scope
case by exact-verifying each extra staged path (`git rev-parse :<path>` ==
`git rev-parse origin/main:<path>`) and including only those in the commit request. The
remaining gap, owned by the parent (shared module, not edited here):

> In `GitCollaboration.commit`, when a merge is in progress (`MERGE_HEAD` present), treat
> staged entries whose index blob equals `origin/main`'s blob as baseline imports: exclude
> them from the `git_unrelated_staged_changes` check and from the `_permit` path list, and
> audit them as `baseline_imports`. Content equality is exact and verifiable; scope
> enforcement for agent-authored paths is unchanged. Until this lands, a merge that imports
> an out-of-scope baseline file is honestly refused (`git_unrelated_staged_changes` /
> permit 403) and the conflict session stays durable.

Covered by `test_vendor_resolver_with_conflict_and_protected_baseline`: the native merge
brings both the conflicted file and a different protected-baseline file; the resolution
merge commit carries the baseline byte-identical to main.

## Parent integration (patch instructions)

No shared core files were modified. Wire the automation in the integration layer:

1. **Worker (`hivemind/agent_worker.py`)** — inject as an optional collaborator, mirroring
   the `journal` duck-typing pattern; default stays off:

   ```python
   # AgentWorker.__init__(..., git_facade=None):
   self.git_facade = git_facade  # WorkerGitFacade; never constructed implicitly
   ```

   In `_execute`, **before** `self.runtime.execute(...)`:

   ```python
   if self.git_facade is not None:
       self.git_facade.prepare(started, self.runtime)  # refuses over user work
   ```

   and after a `final["status"] == "completed"` result, **before** the terminal
   `self._commit(...)`:

   ```python
   if self.git_facade is not None and final["status"] == "completed":
       publication = self.git_facade.finish(started, execution_id, self.runtime,
                                            context, checkpoint)
       if publication["status"] == "conflicts":
           pass  # durable session recorded; resume via resolve_conflict on a later tick
   ```

   Catch `GitAutomationUnknown` like `WorkerStopped`: stop and reconcile, never retry
   blindly. On `notification.type == "git_merge_conflict"`, the worker's existing
   `_poll_notifications` handler can schedule
   `git_automation.resolve_conflict(notice["merge_request_id"], VendorGitConflictResolver(...))`.

2. **SDK (`hivemind/sdk.py`)** — no changes required; the automation uses
   `create_git_workspace`, `git_workspace_action`, `submit_merge_request`,
   `git_repository`, `task`, and plain `request("GET", ...)` for workspace/merge-request
   status. If a typed convenience is desired, add:
   `git_workspace(self, workspace_id)` → `GET git-workspaces/{id}` and
   `merge_request(self, merge_request_id)` → `GET merge-requests/{id}` (the automation
   currently issues these via `Client.request`).

3. **CLI (`hivemind/cli.py`)** — add an explicit opt-in flag (default off), e.g.
   `hivemind worker run --git-automation --git-state-file <path> [--allow-native-git]`,
   constructing `GitWorkerAutomation(client, project_id, agent_id, run_id, enabled=True,
   state_file=..., allow_native_git=...)`, calling `provision()`, and **then** creating the
   runtime adapter with `workspace=<provisioned clone path>` before passing both to
   `AgentWorker` (no relocation of any original user workspace). Do not enable either flag
   from configuration file defaults.

4. **Hooks (`hivemind/hooks.py`)** — no changes required. The bare repository's
   `pre-receive` hook (installed by `GitCollaboration.initialize`) already restricts
   pushes to `refs/heads/agents/agent_*`; the automation only pushes through the canonical
   API, which satisfies that hook. Merge publication remains fenced behind the Nexus
   lease path (`merge-requests/{id}:merge`), which this module deliberately never calls.

## Missing domain operations (for other workstreams)

* **Server-side conflict-resolution endpoint.** `resolve_conflict` currently verifies the
  in-progress merge natively in the clone and then uses the ordinary commit/push/submit
  endpoints. A canonical `git-workspaces/{id}:resolve` (accepting `merge_request_id`,
  expected head, and resolved paths, performing the merge commit server-side) would remove
  the need for `allow_native_git` during resolution.
* **Workspace fetch endpoint.** `fetch_protected` shells out to `git fetch origin main`
  because no canonical "sync workspace with protected tip" read exists. A
  `POST git-workspaces/{id}:fetch` endpoint would close this gap.
* **Conflict session listing.** `conflicts()` is local to the automation state file;
  durable cross-process visibility for operators would need a canonical query over
  `merge_request` with `status=conflicts` filtered by agent (available today via
  `GET /merge-requests?project_id=`).

## Remaining limits

* Resolution requires `allow_native_git=True` (see missing endpoints above) and a resolver
  that performs a real `git merge` — other strategies (rebase/cherry-pick) are honestly
  reported as `unresolved`.
* `push` without `allow_native_git` can fail server-side (`git_command_failed`) when the
  clone has not fetched an advanced protected tip; the automation pre-fetches when native
  Git is allowed, otherwise the host must sync the workspace first.
* The attempt budget is per state file; deleting the state file resets it (sessions are
  re-recorded from the still-`conflicts` merge request on the next `publish`/`submit`).
* This is a cooperative client-side workflow, not an OS sandbox and not an exactly-once
  scheduler; fencing of publication remains server-side (revision CAS, exact hashes, Nexus
  lease fence).

## Tests

`tests/test_git_worker_automation.py` (14 tests, all passing): default-off fence, bounded
idempotent provisioning, exact-hash publish with preserved author identity, client-side
scope fence, stale-execution refusal, durable/bounded/resumable conflict resolution with a
true merge commit, attempt-budget exhaustion, native-Git opt-in fence, unknown-outcome
fencing (timeout → `GitAutomationUnknown`), porcelain status parsing, facade
prepare/bind/dirty-refusal, facade finish (literal paths, honest clean, running
checkpoint, stale-execution refusal), vendor resolver with conflict + protected-baseline
merge (deterministic phase on the real task/execution, byte-identical baseline import),
and resolver opt-in refusals. Tests use the in-process app with isolated temporary
repositories; no paid calls, no network, no host Git config changes, no cleanup of user
files.
