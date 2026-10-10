# Local Git collaboration and Nexus merges

Hivemind uses the installed **standard Git CLI**, minimum version 2.38. It does not
implement Git objects, merge algorithms, or a custom transport. Each project has
a service-owned bare repository and each agent has its own clone and
`agents/<agent-ID>` branch. `main` is protected. Candidate commits are retained
under internal service refs, not published to `main` until approved.

## Deployment installation

Run `scripts/install.ps1` on Windows or `sh scripts/install.sh` on Linux/macOS.
Missing Git is installed using the standard package manager:

- Windows: `winget`, package `Git.Git`.
- Linux: available `apt-get`, `dnf`, or `apk`, with administrator permission.
- macOS: Homebrew.

Unsupported hosts fail with an actionable prerequisite message rather than download
or bundle an arbitrary binary. Old Git versions must be upgraded. `uv` is required
for the subsequent locked service dependency install. The installers neither
download model weights nor start/expose a service.

Windows `-CheckOnly` and Unix `--check-only` check Git without installing anything;
Windows `-GitOnly` installs/verifies only Git. No Git distribution is bundled.
An offline deployment should provision standard Git separately. Bundling remains
unjustified unless offline installation requirements require redistribution and
its license/update obligations.

Configure the service account's normal Git identity before creating repositories.
Hivemind does **not** set or override author/committer identity. Missing identity
fails closed. API commits include the Factory bot co-author trailer and preserve
normal Git commit hooks. Git for Windows long-path support is enabled for these
new repositories and Git invocations.

Supply check commands through private operator configuration, then run
`hivemind serve --agent-integrations <configuration.json>`:

```json
{"git_collaboration":{"checks":{"tests":["G:/trusted/python.exe","G:/private/verify-project.py"]}}}
```

The verifier script runs with the exact candidate checkout as its current
directory. At least one configured check is required for publication. Inline
shell/code flags are refused by the trusted verifier.

## API workflow

1. Operator: `POST /v0.1/projects/{project}/git:initialize`.
2. Agent: `POST /v0.1/git-workspaces` with `project_id` and `agent_id`.
   The returned `workspace_path` is the agent's clone. Use it as the runtime
   workspace; keep runtime credentials, journals, and state outside this clone.
3. With a live assigned task and fresh scope claims, edit files and call
   `POST /v0.1/git-workspaces/{workspace}:commit` with `expected_revision`,
   `task_id`, `run_id`, `execution_id`, `message`, and literal `paths`.
   Unrelated staged work is rejected, not discarded.
4. `POST /v0.1/git-workspaces/{workspace}:push` takes the same task/run/execution
   binding and exact `source_oid`. It publishes only the assigned agent branch.
   New commit history, not just the final diff, is checked against file scopes.
5. `POST /v0.1/merge-requests` takes `project_id`, `workspace_id`, `task_id`,
   `source_oid`, `target_oid`, and `title`. Read the current protected tip through
   `GET /v0.1/projects/{project}/git`. A request targets the exact current `main`
   commit and the exact pushed agent tip.
6. Operator: `:check` with `expected_revision` and a configured `check_name`.
   Git fetches and checks out the exact candidate into a separate detached clone.
   Checks use trusted allowlisted commands with bounded processes/output. Commands
   never come from agents or merge-request descriptions. HEAD/tracked-working-tree changes
   invalidate the check. Check-policy fingerprints also bind the result.
7. Independent reviewer: `:review` with `expected_revision`, `source_oid`,
   `target_oid`, `candidate_oid`, `decision` (`approve` or `reject`), and `reason`.
   Producers cannot self-approve; a rejection blocks publication.
8. Current Nexus agent: `:merge` with `expected_revision`, the three exact hashes,
   `coordinator_epoch`, `lease_id`, and Nexus `run_id`.

Protected publication requires a live current Nexus agent credential with its
coordinator grant, matching lease/run/epoch, enough remaining lease time, no
pending run control, all configured passing checks, and an independent approval.
An administrator cannot impersonate Nexus to merge.

An atomic native `git update-ref --stdin` transaction verifies the Nexus fencing
ref and compare-and-swaps `main` from the approved base to the approved candidate.
New Nexus leases change the fencing ref. Source/base advancement, changed check
policy, stale epochs, short leases, and mismatched hashes all refuse publication.
Every new source/base pair needs a new request and fresh checks/approvals.

## Conflicts and recovery

Git's own `merge-tree` detects conflicts. A conflicted request returns filenames
and creates a private `git_merge_conflict` notification for the submitting agent.
It does not modify the agent clone or protected branch.

Agents fetch the latest `main`, integrate it using normal Git in their own clone,
resolve conflicts, commit, push, and submit a new exact-hash request. Force pushes
and deletions are rejected; preserve history when integrating the protected tip.
Runtime profiles must explicitly authorize any native Git/tool runner they use;
Git/MCP tools are not implicitly added to file-only vendor tool allowlists.

Git files and canonical SQLite transactions cannot share an atomic commit.
Independent durable audit/check-effect journals survive canonical rollback:

- After an uncertain commit or a manual native Git commit, inspect the clone and
  use workspace `:reconcile` with `expected_revision`, task/run/execution binding,
  and the observed exact `source_oid`. Never blindly create another commit.
- After uncertain protected publication, operator request `:reconcile` needs
  `expected_revision`. It requires a durable matching merge intent, exact candidate
  ancestry in `main`, and the native reflog entry with the recorded Nexus epoch.
  It reconciles metadata only; it does not publish another merge or bypass reviews.
- Unknown checks remain blocked for explicit effect adjudication. An operator
  assertion without a real check receipt cannot manufacture a passing result.

Submission, review, check, merge intent/completion, and reconciliation are audited.
`GET /v0.1/projects/{project}/git:audit?limit=100` returns bounded project-specific
audit records. Source/base/candidate hashes, principal, lease, and epoch are retained;
raw check output and source file contents are not included in this API.
The audit database, bare repositories, refs/reflogs, and clones require backup and
operator-managed retention together with canonical data.

## Local security boundary

Only local filesystem transport is enabled by the broker (`GIT_ALLOW_PROTOCOL=file`);
UNC repository roots are refused. No Git daemon, SSH endpoint, smart HTTP endpoint,
or network listener is started. Service hosting stays loopback-only. Cross-machine
Git transport remains disabled until authenticated, authorized **HTTPS** is implemented
and validated. Mounting the data directory from another machine is not supported
as an alternative transport.

A standard receive hook rejects protected/internal refs and allows only agent-branch
pushes; receive configuration rejects non-fast-forward updates and deletions.
Authenticated API operations bind each clone to its agent and project.
This is a local trusted administrative domain, **not a hostile same-user filesystem
sandbox**: a process with direct bare-repository write access can bypass hooks with
plumbing/configuration commands. Keep that directory service-owned and inaccessible
to untrusted agent OS accounts before claiming stronger isolation. Existing Windows
process cleanup is not an ACL/network sandbox. Git object storage also retains
historical content; do not commit secrets and do not bypass scoped broker checks.

Python/TypeScript SDK and MCP expose workspace operations and merge requests.
Installed OpenAPI/module-qualified schemas specify the strict request contracts.
