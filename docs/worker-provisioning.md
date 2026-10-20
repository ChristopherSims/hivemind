# Automatic permission-violation termination

API-created workers now receive an immutable, profile-bounded JSON permissions
policy before vendor startup. A creation payload can narrow it:

```json
{
  "profile": "local",
  "display_name": "Builder",
  "instructions": "Implement the assigned task.",
  "max_runtime_seconds": 300,
  "permissions": {
    "allowed_tools": ["read", "grep", "glob", "ls", "edit"],
    "read_paths": ["src", "tests"],
    "write_paths": ["src"],
    "allow_coordination_mcp": true
  }
}
```

These are normalized workspace-relative directory/file prefixes, not globs.
Empty tool/path lists deny that capability. Caller permissions cannot exceed
the operator profile; canonical task/scope permits still apply to writes.
The same `permissions` object can be configured on an operator worker profile
in the JSON integration configuration. Profile fingerprints include the policy.
This release accepts JSON, not automatic YAML/config discovery.

Supported native names are `read`, `grep`, `glob`, `ls`, `create`, `write`,
`edit`, `applypatch`; availability also depends on the vendor. Shell execution,
network tools, unknown tools, and arbitrary integration tools are not permitted.
Coordination MCP means only the fixed authenticated read-only Hivemind tools,
not unrestricted MCP servers. Vendor model-provider access is separate from
model-selected tools and remains operator-configured.

Pre-tool hooks validate names and typed paths. A refusal writes an exclusive
private worker/launch-bound incident marker; later tools remain refused.
Authenticated API **403/404** denials also trigger a conservative safety stop.
This can stop a worker on an unavailable resource, not only malicious intent.
Unauthenticated callers cannot select a process to terminate.

The trusted service supervisor polls at up to 100 ms, closes the worker's
owned process group/Windows Job (including descendants), revokes its agent
credential and records a bounded incident. It does not wait for an LLM/Nexus
judgment. Marker corruption fails closed. Failed process drain yields `unknown`,
never a falsely confirmed stop. Stopped workers are not automatically restarted.

Administrators and a live Nexus can inspect `GET /v1/workers/{worker_id}`.
Restricted Nexus identities need a `monitor` grant for that worker's agent;
monitoring grants do not confer spawn, start, stop or other control permission.
`monitoring.incident.termination_confirmed` is process-drain evidence only.
Killed processes do not prove rollback or absence of prior external effects.

**Enforcement limit:** this is tool/API-boundary monitoring, **not OS isolation**.
It cannot detect every direct syscall, hidden vendor action, account-level
access, bypassed hook or network connection. A same-account agent with unmodeled
host access may reach private files; JSON alone does not prevent that.
Current Windows Jobs are process cleanup, not a filesystem/network sandbox.
The feature terminates only API-created processes owned by this service; it
cannot safely kill arbitrary externally launched agents by PID. Stronger
system isolation remains a separate, currently unsupported Windows requirement.

Validation includes a real local parent/child termination test with a Windows
Job and no vendor/model launch, plus denied hooks/API access, policy narrowing,
corrupt markers, credential revocation and scoped Nexus monitoring tests.
The final batch passed **186 tests**, whole-tree Ruff, SDK typechecking,
schema export and diff checks. Full offline/browser suites and live vendor
violation tests were not rerun; no paid model calls were made.

# Governed worker provisioning API

`POST /v1/projects/{project_id}/workers` creates an agent identity, a task whose
description contains the instructions, a task-worker run, and a durable launch
record. With `start: true`, it launches the fixed, trusted Hivemind worker entry
point. This is different from `POST /v1/agents`, which only registers an identity.

Provisioning is **default-off**. Administrators and the current, live,
coordinator-granted Nexus may use it. Nexus must send its exact current
`coordinator_epoch`; ordinary agents cannot provision workers.

## Request

Send a stable `Idempotency-Key` header:

```json
{
  "profile": "project-factory",
  "display_name": "API implementation worker",
  "instructions": "Implement the assigned API change and report evidence.",
  "max_runtime_seconds": 1800,
  "scope_ids": ["<authorized-project-scope-id>"],
  "coordinator_epoch": 1,
  "start": true
}
```

Administrators omit `coordinator_epoch`. Use `start: false` to prepare the
canonical records without launching or minting credentials. Callers cannot
provide executables, environment variables, credential values, model overrides,
filesystem paths, arbitrary adapters, or external MCP URLs.

## Deterministic repository ceiling

The repository contains `hivemind-worker-policy.json`:

```json
{
  "version": 1,
  "max_runtime_seconds": 900
}
```

The server computes:

```text
effective runtime = min(requested runtime, repository ceiling, operator profile ceiling)
```

This happens **before constructing the private startup payload**. The response
reports `requested_max_runtime_seconds`, `max_runtime_seconds`, and
`runtime_capped`. A request for 1800 seconds under the example policy sends
at most 900 seconds to the child. It does not rely on a model following instructions.

The operator pins the policy file's SHA-256 in private server configuration.
A missing, malformed, linked, oversized, or changed policy refuses provisioning.
Nexus cannot increase its ceiling by editing the repository file. An intentional
operator policy update requires updating the pin and restarting the service.

## Private operator configuration

Pass this through `serve --agent-integrations <private-json>`:

```json
{
  "worker_provisioning": {
    "enabled": true,
    "api_base_url": "http://127.0.0.1:8080",
    "max_active_workers": 4,
    "profiles": [{
      "name": "project-factory",
      "project_id": "<project-id>",
      "runtime": "factory",
      "model": "<approved-model-id>",
      "repository_root": "<projects-path>",
      "workspace_root": "<workspace-root>",
      "private_root": "<private-root>",
      "policy_sha256": "<sha256-of-the-exact-policy-file-bytes>",
      "max_runtime_seconds": 900,
      "allow_model_calls": true
    }]
  }
}
```

All roots must already exist, be link-free, and be operator-controlled. The
private root must be outside repository/workspace roots and inaccessible to
untrusted users. On Windows, the operator must establish private directory ACLs;
Python file modes alone are not Windows isolation. No accounts, ACLs, global
Factory settings, or host permissions are changed by this endpoint.

Each launch gets a **new empty workspace by default**. To seed it from the
profile's local repository, add these private operator profile fields:

```json
{
  "workspace_mode": "git_clone",
  "allow_native_git": true,
  "repository_ref": "HEAD",
  "checkout_timeout_seconds": 30,
  "checkout_max_files": 10000,
  "checkout_max_bytes": 67108864
}
```

`repository_root` must be the exact root of a normal local Git repository, a linked
worktree, or a bare repository. The policy file must still be present and pinned
at `repository_root/hivemind-worker-policy.json`, including for bare sources. A ref may be
`HEAD`, a full commit ID, or an operator-selected `refs/heads/...` ref; callers
cannot provide a repository URL, ref override, or Git command.

The launcher resolves the source to a commit and records that commit **before
fetching**. It builds an independent shallow repository in the new worker
directory and checks out branch `hivemind/worker/<worker-id>` at that exact commit.
The response and private startup payload record the commit, branch, tracked byte
count, materialized byte count and file count in `workspace` / `workspace_source`.
Both tracked and materialized sizes must fit the configured byte ceiling.
Moving the source branch
after resolution does not change the requested snapshot.

Only committed tracked content is copied. Source staged edits, unstaged edits,
untracked files, index, refs and configuration are not modified or swept into
the worker's tree. The target has no configured remote, shared object alternates,
source hooks, credential helpers or inherited smudge filters. Source replacement
refs are ignored so they cannot alter the selected commit's tree. No commit, push,
identity change or source worktree registration occurs.

Source objects and reference metadata are copied into private bare views before
Git resolves or fetches them. Git never uses source upload-pack helpers, hooks,
config includes or filters. Object-format detection reads only the bounded source
config with includes disabled. SHA-1 and SHA-256, packed refs and shallow boundaries
are preserved. Source alternates, grafts, promisor packs and metadata links are rejected.

### Linked metadata, submodules and local LFS

These additional fields belong only to the trusted operator profile:

```json
{
  "repository_metadata_roots": ["<repository>.git"],
  "submodule_sources": [
    ["vendor/library", "<vendor-dependencies>"],
    ["vendor/library/nested", "<vendor-dependencies-nested>"]
  ],
  "checkout_max_submodules": 32,
  "checkout_max_submodule_depth": 8,
  "checkout_max_object_bytes": 268435456,
  "materialize_lfs": true,
  "lfs_object_roots": ["<repository>.git/lfs/objects"]
}
```

All roots must already exist, be absolute, local and free of symlinks/reparse
points. A linked `.git` directive and its `commondir` must stay within approved
metadata roots. Real `.git` directories of explicitly trusted source repositories
are implicit metadata roots, allowing initialized submodules inside their parent's
metadata. Otherwise linked metadata requires explicit approval. Worker targets
remain independent repositories, not linked worktrees sharing source metadata.

Submodules are disabled unless a mapping is provided. Every gitlink, including
nested links, must have an exact, full workspace-relative path mapping. Each
dependency is checked out at its gitlink commit, not its current branch tip, into
its own private repository and worker branch with no remote or shared objects.
Missing mappings/commits, depth/count/aggregate limits or incompatible sources hold
launch. `.gitmodules` URLs are never followed and update commands are never run.
Git's integrity checks still reject unsafe executable update entries.

LFS stays opt-in; without it pointers remain pointers. With it, strict standard
v1 pointers (at most 1 KiB, LF-only, SHA-256 and exact decimal size) must resolve
from the supplied local object roots, using `<first-2>/<next-2>/<full-oid>`. Each
object is copied privately, size-rechecked and SHA-256 verified before replacing
its working-tree pointer. The Git commit/index retain the pointer, so hydrated
files can appear modified without an installed clean filter. No git-lfs executable,
`.lfsconfig` URL, network transport or implicit cache is used. Unsupported pointer
extensions, missing/corrupt objects and expanded content limits hold launch.

Symlinks, unsupported Windows paths, case-colliding paths and excessive content
are rejected before vendor startup. File, tracked-byte and actual materialized-byte
limits apply across the complete dependency tree. Copied object/ref metadata has
an aggregate `checkout_max_object_bytes` limit (default 256 MiB, maximum 1 GiB).
Setup has a single deadline of `min(checkout_timeout_seconds, effective runtime)`,
a 2 MiB command-output and aggregate tree-listing bound, and a 10,000-entry
source-metadata scan bound per source. Model runtime retains its separate ceiling.
Windows Git long-path handling is enabled in the generated repositories.

Repository setup runs outside the canonical mutex, under the durable launch
intent. Authority, assignment and policy pin are rechecked before credentials
and launch confirmation. A failed or interrupted setup remains `unknown`; partial
owned directories are retained, never overwritten, deleted or silently retried.
The complete operator profile is fingerprint-bound at reservation. Changing it
requires a new reservation; older prepared records without that binding fail closed.

**Validation:** **146 checks** passed in the expanded repository, provisioning,
permission, MCP-registration, supervisor, recovery and Git-worker batch. All **7
task-execution checks** also passed after replacing an obsolete nonexistent-task
fixture with a real replacement task, **153 selected checks** in total.
Whole-tree Ruff and schema export passed. Real local Git is exercised with
injected vendor launchers; no paid model calls or remote Git transport are used.
Hooks/filters, unsupported trees, dirty-source preservation, exact commit binding,
profile changes, replacement refs, materialized-byte expansion, expired setup
deadlines and revoked authority have regression tests. The broader-source checks
also cover linked-metadata approval/refusal, bare SHA-1/SHA-256 sources, shallow
boundaries, initialized/nested submodules, local LFS including nested assets,
cache-size races, corruption and dependency bounds. No complete offline-suite
rerun or live authenticated vendor startup was performed for this extension.

Supported profiles select `factory` or `oh_my_pi`; runtime selection is not an
API-controlled import or command. Enabling a profile's model calls authorizes
future calls requested through that profile. Tests do not enable paid inference.

## Lifecycle and authority

- `GET /v1/projects/{project_id}/workers:defaults`: profiles, ceilings, sample payload.
- `GET /v1/workers/{worker_id}`: current launch status and canonical identities.
- `POST /v1/workers/{worker_id}:start`: `expected_revision`, plus Nexus epoch.
- `POST /v1/workers/{worker_id}:stop`: same revision/epoch fields.

The startup JSON and credential stay in the private worker directory. Neither
the response, durable command receipt, nor process argv contains a bearer token.
The worker credential is project/agent-bound, permits capture for its own
governed runtime, and has **no coordinator grant**.

A durable `preparing` intent precedes filesystem/process work. The child waits
for an explicit containment handshake before loading its config or starting a
vendor. Server authorization, assignment and repository pin are rechecked before
launch confirmation. A replay never renews launch permission.

Native launches currently require **Windows Job Object containment**, which
owns the child process tree. A monotonic server deadline and child watchdog
bound runtime; stop/deadline/shutdown close the owned process boundary and revoke
the worker credential. Native non-Windows launches fail closed until an equivalent
whole-tree supervisor is integrated; a POSIX process group alone cannot contain
vendors that create new sessions.

Service restart does not attach to a saved PID or replay an uncertain launch:
`preparing`/`running` records become `unknown`, credentials are revoked, and the
record remains an operator hold. Unknown records consume capacity. Process exit
or termination is **not** proof that a task or external effect completed.

## MCP and SDK

The Nexus can call `hivemind_worker_defaults` and `hivemind_create_worker` through
the existing authenticated stdio MCP server. Its private tool policy must
explicitly allow those tools. The server applies exactly the same epoch/profile/
runtime checks as HTTP; MCP descriptions grant no authority.

Each launched worker also gets a private standard `mcp.json` descriptor for the
local Hivemind stdio server, using `HIVEMIND_CONFIG` rather than exposing a token.
Trusted profiles now default to `"auto_mcp_registration": true`. The caller
cannot override this field; an operator may explicitly set it to `false`.
Registration happens when a managed vendor session opens for its assigned task,
not merely when the worker process is launched. No global or repository vendor
settings are modified.

- **Factory:** the SDK receives a session-local `StdioMcpServerConfig` pointing to
  `python -m hivemind.worker_mcp`. Saved-session loads receive the same descriptor.
  Before each prompt, the runtime requires the sole Hivemind server to be connected,
  its exact eight-tool catalog to be enabled, unambiguous vendor tool IDs, and a
  verified exact restriction to native scoped tools plus those MCP IDs. Missing or
  drifted evidence holds the turn. Repository/parent `.factory/mcp.json` launchers
  are rejected rather than merged or overwritten.
- **OMP:** a fixed private Bun SDK host starts the session with `enableMCP: false`,
  `restrictToolNames: true`, no ambient extension discovery and cache warming
  disabled. Its separate MCP manager connects only the fixed Hivemind server.
  The eight tools are supplied as explicitly allowed SDK custom tools, with
  the ordinary Hivemind capture extension loaded for hooks. A private nonce-bound
  startup receipt is required before a prompt. The host disconnects its manager
  on session shutdown.
  The receipt proves initial registration, not perpetual server liveness.
  This path requires Bun and the installed **OMP 18.8.9 source SDK** beside the
  default Bun global installation. Missing or changed SDK contracts fail closed;
  the runtime never falls back to ambient CLI discovery.

The worker surface exposes descriptions, ledger, context queries, inbox, search,
task reads, notifications and conflicts. It deliberately excludes mutation,
claim, execution, worker-spawn and Nexus-control tools. Normal SDK coordination
continues to own task execution and checkpoints. Every MCP API read still uses
the worker's project/agent-bound credential and canonical source authorization.
Hook allowlists include only the verified IDs; discovery and arbitrary custom
commands remain disabled.

The API `mcp.automatic` reports the profile selection. `mcp.configured` reports
startup wiring, **not a live vendor connection**. API launch receipts retain
`connection_verified: false`; actual verification is recorded in the private
runtime state after vendor initialization. A failed initialization never sends
the model prompt. Strict `no_tools` and filesystem-sandbox configurations currently
reject automatic enrollment instead of silently broadening permissions or binds.

**Validation:** offline vendor/capture/provisioning/recovery suites pass:
**205 tests** after SDK-host isolation hardening. These include a real local MCP
handshake, loading the installed OMP extension against its real MCP manager,
both OMP adapter typechecks, checkout/API/capture/recovery regressions, and a
SDK-host startup test with a real MCP manager and recording session factory.
Factory registration and refusal are exercised with contract doubles, and
saved-session adoption uses the installed SDK types. Live Factory enrollment
and full authenticated OMP SDK-host/model turns have not been exercised for this change.

Python and TypeScript clients expose create/defaults/status/start/stop methods.
Time ceilings are not monetary/token budgets. Strict production Factory/OMP
budget adapters remain explicitly deferred and unsupported turns remain held.
