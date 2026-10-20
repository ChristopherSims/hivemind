# Runtime safety and recovery, 2026-10-17

Final offline regression: **386 passed, 7 skipped** (82.79 seconds), with scoped
Ruff/whitespace checks and in-memory worker JSON Schema/OpenAPI generation
passing. Skips cover real Linux/bubblewrap/kernel isolation, POSIX mode/ownership,
and unavailable symlink privileges. Schema snapshots were not overwritten.
Nonignored evidence: `benchmarks/runtime-safety-results-2026-10-17.json`.

## Docker/WSL follow-up

The operator subsequently authorized Docker/WSL testing under **default Docker
restrictions only**. WSL completed **388 passed, 9 skipped** in 63.99 seconds,
including real namespace/gateway enforcement and exact-input native JSON checks.
Docker's actual missing-sandbox refusal checks completed **3 passed, 1 skipped**.
Docker's positive sandbox checks remain blocked by namespace denial, and its
broader regression attempt timed out; neither is a successful sandbox validation.

The credential-free image supplies Python, bubblewrap and test dependencies to
temporary WSL `/tmp` snapshots. An unprivileged private user/mount namespace binds
that Python and its loader cache at their expected paths only inside the test
namespace. WSL system files and security settings remain unchanged. This validates
synthetic kernel enforcement, not an installed or protected vendor service.

The real host UID-1000 filesystem/network probes also passed. The default cgroup
root is not writable or privately delegated, so whole-tree cgroup cleanup and
service-crash lifetime handling remain release gates. No privileged container,
security exceptions, root delegation, live vendor calls or model downloads were
used. Detailed evidence and commands:
`benchmarks/runtime-safety-linux-2026-10-17.json`,
`benchmarks/runtime-linux/README.txt`.

## Approved Docker sandbox/cgroup follow-up

The operator separately approved container-only seccomp/AppArmor exceptions and
a separate privileged container with a private cgroup namespace. The real Docker
sandbox suite passed **146 tests, 2 branch skips**. The cgroup suite passed
**2 tests, no skips**, running the helper as UID 1000 after a root setup restricted
to the verified container-only cgroup subtree. Both live-parent and exited-parent
`setsid` descendants were killed, observed drained to `populated 0`, and their
owned cgroups removed. Setup/cleanup never mounts or delegates host cgroups.

These are trusted synthetic tests, not authorization to run agents inside a
privileged container. They do not prove protected production deployment,
malicious same-UID containment, service-crash cleanup or live vendor behavior.
A separate broader Docker matrix exited nonzero with incomplete output and
remains an unresolved regression gate, not a pass.
Evidence: `benchmarks/runtime-safety-docker-enabled-2026-10-17.json`;
reproduction: `benchmarks/runtime-linux/README.txt`.

## Original/default-only approved scope

The operator selected a **WSL2 Linux sandbox with an explicit gateway** for
Windows-hosted vendor execution, not a native Windows AppContainer implementation.
Only offline and synthetic tests are authorized. No paid turns, live Factory MCP
enrollment, vendor authentication, system-wide installation, account/ACL changes, cgroup
delegation, firewall changes or host provisioning were performed.

The existing Windows service and workers are not automatically migrated to WSL.
The WSL distro is a separate Linux deployment with its own trusted installation,
credentials, native Linux workspaces and explicitly configured API connectivity.
Do not advertise a native Windows vendor sandbox.

## WSL2 readiness and launch boundary

```powershell
.venv/Scripts/python.exe -m hivemind.wsl_runtime `
  --distribution Ubuntu --cgroup-root /sys/fs/cgroup/hivemind
```

This defaults to a bounded read-only assessment, except for the existing ephemeral
namespace enforcement probes when their prerequisites are installed. The fixed
Linux bootstrap uses an explicit distro/interpreter and Python isolated mode.
There is no shell interpolation, implicit default distro, PYTHONPATH injection
or automatic Windows-to-Linux path conversion.

The assessment requires:

- A real WSL2 Linux kernel and an unprivileged guest account.
- Hivemind installed in the selected Linux interpreter.
- Bubblewrap and a fresh successful filesystem/network namespace proof.
- A service-owned delegated cgroup-v2 subtree with admission, events and kill.
- The selected vendor's Linux executable/SDK before launch.

Real assessment evidence: `benchmarks/runtime-safety-wsl-2026-10-17.json`.
Ubuntu runs as UID 1000 on WSL2, but the required Linux Hivemind installation,
bubblewrap, writable cgroup delegation, namespace proof and vendor installations
are absent. It correctly returns `ready: false` and exit code 1.

Launch is an explicit additional operation (`--launch --config ... --runtime ...`)
which may call a model and is **not authorized or exercised here**. Assessments
are policy-fingerprint-bound and expire after 60 monotonic seconds. Config,
state and workspace paths must be native absolute Linux paths, not `/mnt`/DrvFS.
The guest rechecks private configuration ownership/mode and cgroup prerequisites.
It requires strict OS isolation plus declared model origins; there is no
shared-network fallback. The runtime then uses the existing allowlisted private
Unix-socket gateway. DNS/IP pinning and permission checks remain enforced.

The guest guardian supervises at most a 3,600-second worker run and drains its
cgroup in cleanup. Killing the Windows `wsl.exe` client alone is **not** evidence
that a Linux worker stopped. The guardian's bounded lifetime, actual Linux
cleanup and fresh canonical recovery evidence are separate requirements.
Host interruption or a missing receipt remains unknown, never automatic replay.
Supervise the Linux service/guardian with operator-managed service lifetime and
kill semantics before a production deployment.

Automatic MCP enrollment with sandbox mounts remains gated. A strict profile
must explicitly set `auto_mcp_registration: false`; it cannot silently claim
that existing Windows/unsandboxed enrollment evidence applies inside WSL.

## Native Linux provisioning

The default native launcher now accepts only Windows Job containment or an
explicit Linux cgroup-v2 delegation. macOS and missing delegation still refuse.
Ordinary POSIX process groups are not a whole-tree boundary because descendants
can call `setsid`.

Operator configuration adds:

```json
{
  "worker_provisioning": {
    "enabled": true,
    "linux_cgroup_root": "/sys/fs/cgroup/hivemind",
    "profiles": []
  }
}
```

The example is incomplete deliberately: actual profiles, existing roots, a pinned
repository policy and model approvals are also required. No script creates or
delegates this cgroup automatically.

`linux_process_boundary.delegated_root` verifies the real cgroup-v2 mount,
link-free paths, owner/private write permissions and required kernel controls.
The child initially waits for `GO` without reading credentials. The parent creates
a uniquely named private subgroup, admits the PID, verifies `/proc/PID/cgroup`
membership and populated evidence, then returns the handle. Only after canonical
launch checks does provisioning send `GO`.
The bootstrap interpreter also uses isolated mode, so a checkout containing a
lookalike `hivemind` package cannot execute code before cgroup/Job admission.
The trusted Hivemind package must be installed in that interpreter.

Cleanup uses `cgroup.kill` and waits for `populated 0`, including descendants that
created new sessions. Missing, changed or malformed evidence cannot become a
confirmed stop. No stale PID attachment or implicit launch replay is introduced.
The bootstrap now propagates the worker CLI exit status instead of always returning
success, and cancels its timer on normal completion.

Optional strict vendor profiles add:

```json
{
  "require_os_isolation": true,
  "vendor_model_origins": ["https://operator-approved-model.example"],
  "auto_mcp_registration": false
}
```

These are operator policy, never agent-controlled argv. Origins must be bounded
HTTPS origin grants; the existing gateway resolves/pins approved destinations
at runtime and refuses private model destinations. Cgroup cleanup alone is not
filesystem/network isolation, billing enforcement or malicious same-user
protection. Use the strict vendor sandbox and a protected service deployment.
The approved private Docker test now proves real admission and detached-descendant
cleanup. Service-crash handling and the protected production cgroup deployment
still need end-to-end validation in their actual service environment.

## Factory continuation and controls

The real installed SDK's high-level `Session.resume` can infer cwd from its caller.
Hivemind continues through the workspace-rooted low-level client and assesses the
server's actual load response before adopting it. Every reported cwd/worktree must
now be an existing **absolute** directory matching the managed workspace.
Relative paths, omitted cwd, nonexistent paths and conflicting worktrees refuse.
The process-global cwd is never changed.

Offline tests pass through the installed SDK's real `DroidClient.load_session`
wire parser, then the governed session adoption and fresh restriction read-back.
They prove local contract handling, not a successful live interrupted continuation.
If a real vendor load response omits cwd, the only safe result remains a hold.
Do not invent workspace evidence or relax the fence to make recovery appear to pass.

Pause/cancel/resume are serialized with a nonblocking control lock. Concurrent
controls return unknown/in-progress rather than racing checkpoint publication.
Resume refuses an active turn even if a stale state says paused. A confirmed
pause/cancel still requires the observed settled turn and a run-bound durable
quiescent checkpoint. Unsupported actions remain unsupported/advisory.

Assignment and steering remain governed by the existing controlled adapter,
canonical revision/epoch/claim/permit gates and runtime action capabilities.
Tests do not establish safe interruption of uninstrumented or unsupported vendors.
The generic safe-boundary adapter also refuses unknown/foreign controls before
acknowledging them, rather than treating an unrecognized action as a successful
transition to running.

## MCP interoperability

Factory enrollment must still show the sole connected `hivemind` server, exact
enabled tool catalog and verified restriction echo. Tool IDs must now bind to
that actual server namespace, not merely match a display name or suffix.
A foreign server's lookalike tool cannot be granted through matching names.
Known supported ID encodings are explicit; unknown encodings fail closed.

Live Factory enrollment, production sandbox mounts and ongoing vendor MCP liveness
remain unvalidated. Offline protocol/contract tests are not live enrollment proof.

## Native artifact-verifier deployment

Staging roots now reject raw symlink/reparse ancestors before resolving paths.
`NativeArtifactVerifier.preflight()` performs a read-only deployment assessment:
allowlisted executables must exist; required isolation must have fresh real Linux
filesystem/network namespace evidence. An injected runner can never satisfy the
required sandbox condition, even if it returns success.

Required but unverified isolation refuses **before creating the artifact stage**.
Exact input hashes, fixed operator argv, execution binding, bounded output/deadlines
and post-check input revalidation remain enforced. Sandbox evidence is reported only
for the real isolated subprocess path, not process cleanup or injected callbacks.

The follow-up temporary WSL deployment proves native JSON checks in a real
sandbox; the original system-wide WSL readiness gate remains incomplete.
Broader executable/dependency mounts,
artifact formats, deployment, process-crash and hostile-input campaigns remain
release gates.

## Remaining validation

1. Operator-provision the selected Linux installation, bubblewrap and protected
   cgroup delegation under an approved service-lifetime boundary.
2. Real namespace/gateway denial tests passed in the temporary WSL deployment;
   whole-tree `setsid` cleanup also passed in the separately approved private
   Docker cgroup tests. Repeat against the protected production service and
   its crash/lifetime boundary before any vendor session.
3. Real native JSON checks over exact staged inputs passed. Validate additional
   production verifier profiles and their deployment mounts separately.
4. With separate authorization, perform live Factory MCP enrollment and
   interruption/reopen with correct server-reported workspace evidence.
5. Run each advertised runtime/action through current controls and recovery
   fences. Keep unsupported/missing evidence explicit.

The local LLM will be supplied later. Nothing here downloads or configures a model
or bypasses the pending production tokenizer/model binding.
