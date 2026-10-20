# OS isolation policies

`hivemind/os_isolation.py` provides **fail-closed** policy definition, capability
detection, planning, and read-only validation for optional OS-level isolation.
It never mutates the host: no account creation, no ACL/permission changes, no
privilege or firewall changes. Those remain explicit operator actions performed
with `scripts/provision-isolation.ps1` / `scripts/provision-isolation.sh`.

## What is actually enforceable (and what is not)

| Control | Windows | Linux with `bwrap` | Linux without `bwrap` |
|---|---|---|---|
| Descendant process cleanup (Job Objects / process groups) | Enforced | Enforced | Enforced |
| Filesystem/network sandbox for spawned subprocesses | **Not available** — Job Objects are cleanup only, never claimed as a sandbox | Enforced (read-only workspace bind, `--unshare-all`, no network) | Not available |
| Separate service identity | External (operator-provisioned; code validates names read-only) | External | External |
| Separate agent identity | External | External | External |
| Protected bare-directory permissions | Read-only attestation with real OS evidence (`GetNamedSecurityInfoW` DACL, inheritance-protection flag, ACE write masks, `GetEffectiveRightsFromAclW`; rejects reparse points, inherited DACLs, broad-principal or agent write access) | Read-only attestation (`lstat` ownership/mode, symlink rejection, `os.access(effective_ids=True)` probe; rejects group/other-writable) | Read-only attestation |
| Identity separation | **Attested** — `LookupAccountNameW` SIDs; aliased names resolving to the same SID are rejected; built-in/system SIDs rejected | **Attested** — `pwd` UIDs; UID-0 and aliased UIDs rejected | **Attested** |
| Vendor sandbox (filesystem-only): oh-my-pi | **Refused** — Job Objects are cleanup only, never a sandbox | **Enforced** — bubblewrap wraps the actual launch argv, gated on a fresh real enforcement probe | Refused (no backend / no probe) |
| Vendor sandbox (filesystem-only): Factory droid | **Refused** | **Enforced** — bubblewrap-wrapped `droid_sdk.transport.ProcessTransport` (documented SDK 0.5.0 launch interface), gated on a fresh probe | Refused (no backend / no probe / SDK without ProcessTransport) |
| Vendor direct-network isolation | **Refused** | **Enforced with explicit gateway and fresh network proof** — private namespace, no `--share-net`; only pinned model/Hivemind origins through a private Unix socket | **Refused** |
| Current-process identity matches expected account | **Attested** — real process token (`OpenProcessToken`/`GetTokenInformation`: user SID, groups, elevation) | **Attested** — real euid/egid/supplementary groups | **Attested** |

`hivemind/identity_attestation.py` turns operator provisioning into *verified*
state using only read-only OS evidence, failing closed on any unsupported
platform, API error, or effective-access gap. Every `Attestation` is bound to
the exact identity (SID/UID, not name) and resolved resource path, carries a
`checked_at` timestamp, and must be re-validated with
`assert_fresh(max_age_seconds)` before reuse.

`hivemind/vendor_sandbox.py` provides filesystem-only and strict
filesystem-plus-gateway vendor sandboxes. oh-my-pi is launched by Hivemind itself as a child process
(`JsonLinesProcess` over an argv Hivemind builds); Factory droid is launched
through the documented SDK 0.5.0 `droid_sdk.transport.ProcessTransport`
(`exec_path`/`exec_args`/`cwd`/`env`, spawned by `connect()` via
`asyncio.create_subprocess_exec` with default args `exec --input-format
stream-jsonrpc --output-format stream-jsonrpc`). Wrapping either launch in
bubblewrap is a real OS boundary around the real vendor process without
inventing any vendor flags or protocol.

Two hard rules:

* **Proof, not presence**: `enforced` requires a fresh
  `probe_bwrap_enforcement()` result — a real bounded execution that enters a
  sandbox and proves an unbound host path is invisible. Bubblewrap on PATH
  alone is refused ("presence is not proof"). Probe evidence is cached for
  `PROBE_TTL_SECONDS` (300s) and staleness refuses.
* **Scope honesty**: filesystem-only mode shares networking and never satisfies
  strict isolation. Strict mode requires `probe_bwrap_gateway_enforcement()`:
  an invisible host sentinel, a distinct network-namespace inode, loopback-only
  interfaces and denied access to a host loopback listener. No gateway or no fresh
  positive network evidence means refusal, not shared-network fallback.

Mount safety (`_validate_bind_set`): the workspace is the only agent-writable
work area; session/state write binds must live OUTSIDE the workspace and must
not contain it; the credential-bearing config file is read-only and must live
outside the workspace and outside every write bind; read-only binds that alias
the workspace or writable state are rejected; symlinks on any bound path are
rejected. Vendor binary paths, auth mounts, and egress policy remain explicit
operator inputs. On Windows every vendor sandbox request stays a refusal,
consistent with `VendorRuntime._prepare` rejecting `require_os_isolation`.

## Policy model

```python
from hivemind.os_isolation import IsolationPolicy, plan, sandbox_argv

policy = IsolationPolicy(
    mode="required",                 # "disabled" | "opportunistic" | "required"
    sandbox_processes=True,
    separate_service_identity=True,
    separate_agent_identity=True,    # requires separate_service_identity
    protect_bare_directories=True,
)
```

* `disabled` — no controls; requesting any control raises `ValueError`.
* `opportunistic` — sandbox used when available; otherwise **degraded**
  (documented, allowed). Required identity/ACL controls are always `external`
  (operator-provisioned) and never claimed as code-enforced.
* `required` — **fail closed**: any control that cannot be enforced refuses via
  `IsolationRefused` (a `PermissionError`). A required sandbox on Windows, or on
  Linux without bubblewrap, refuses before any child process launches.

## API

- `detect_capabilities(platform=None, which=None)` — honest host probe;
  injectable for tests. Windows never reports a sandbox.
- `IsolationPolicy` — validated frozen dataclass; invalid combinations raise.
- `plan(policy, capabilities=None)` — returns an `IsolationPlan` of
  `ControlDecision`s (`enforced` / `degraded` / `external` / `refused` /
  `disabled`); raises `IsolationRefused` for unenforceable required controls.
- `validate_identities(service_identity, agent_identity=None)` — read-only name
  validation: 1–32 safe characters, distinct accounts, no reserved/built-in
  names. Never creates accounts.
- `component_applicability(component)` — per call-site sandbox applicability
  for `subprocess` (git plumbing), `check` (TrustedVerifier), `extractor`
  (AuthorizedProcessExtractor), `vendor` (rejected).
- `sandbox_argv(argv, workspace, policy, component=..., capabilities=None)` —
  launcher decision: returns the wrapped (bubblewrap) or plain argv, or raises
  `IsolationRefused`. Wraps `process_boundary.isolated_argv`.
- `validate_directory_protection(path, platform=None, expected_owner_uid=None)`
  — read-only audit of a bare-repository directory. POSIX: rejects group/other
  write bits and optional owner mismatch. Windows: rejects network roots and
  reports `protected: None` (prefer `identity_attestation` for real ACL
  evidence).

## Identity/permission attestation (`hivemind/identity_attestation.py`)

- `attest_distinct_identities(service_identity, agent_identity=None, ...)` —
  resolves both accounts through the OS (Windows `LookupAccountNameW` → string
  SID; POSIX `pwd` → UID/GID), rejects well-known/built-in accounts, and
  rejects **aliased** accounts that resolve to the same SID/UID. Returns an
  `Attestation` bound to the resolved identifiers.
- `attest_protected_directory(path, owner_identity=..., forbidden_writers=...)`
  — fails closed on symlinks/reparse points, network roots, missing paths,
  wrong owner, group/other-writable modes (POSIX), inheritance-enabled DACLs,
  broad-principal (`Everyone`/`Users`/...) write ACEs, and any *effective*
  write access held by a forbidden writer (`GetEffectiveRightsFromAclW` on
  Windows; group-membership analysis plus `os.access(effective_ids=True)` on
  POSIX). OS API errors raise `AttestationFailed`; nothing is assumed.
- `attest_process_identity(expected_identity, require_unprivileged=False, ...)`
  — proves the *current process* actually runs as the expected account, not
  merely that the account exists. Windows: real process token
  (`OpenProcessToken`/`GetTokenInformation`: user SID, group SIDs, elevation).
  POSIX: real euid/egid/supplementary groups. Token/UID mismatch fails closed;
  with `require_unprivileged=True`, elevation or privileged-group membership
  (Administrators/Operators SIDs; root/wheel/sudo/admin groups) also fails.
- `attest_provisioning(service, agent, bare_root, ...)` — one attestation
  binding identities and the protected directory together; the required-mode
  startup preflight.
- `Attestation.assert_fresh(max_age_seconds=300)` — staleness/clock-skew
  rejection; re-attest before relying on cached results.
- Ancestor boundary: `attest_protected_directory(..., check_ancestors=True)`
  (default) checks the *raw* path and every ancestor for symlink/reparse
  escapes BEFORE `resolve()` (a junction can no longer hide behind
  resolution), and fails closed on movable/agent-writable ancestors (POSIX:
  group/other-writable without sticky; Windows: agent effective
  `FILE_DELETE_CHILD`/`DELETE`/`WRITE_DAC`/`WRITE_OWNER`/`GENERIC_ALL` on any
  ancestor via `GetEffectiveRightsFromAclW`).

## Vendor sandbox (`hivemind/vendor_sandbox.py`)

- `probe_bwrap_enforcement(...)` — real bounded probe: executes a trivial
  sandboxed command and asserts an unbound host sentinel is invisible.
  Returns `ProbeEvidence(status, detail, checked_at)`; cached 300s;
  `assert_fresh()` enforced by assessment.
- `assess_vendor_sandbox(vendor, platform=None, which=None, probe=None,
  require_network_isolation=False, sdk=None)` — honest verdict
  (`enforced` / `refused` / `unsupported`) with `scope`, `network_isolated`,
  `probe`, and explicit `prerequisites`. On a real Linux host the probe runs
  automatically; injected-platform assessments require injected probe
  evidence. Strict mode additionally requires `gateway=True` and network-positive
  probe evidence. Launchers recheck freshness, not just assessment.
- `sandbox_vendor_argv(argv, workspace, config_file=..., session_dir=...,
  extra_read_binds=..., require_network_isolation=False)` — bubblewrap command
  for oh-my-pi or `IsolationRefused`. Session dir is private state and must
  live outside the workspace; config outside workspace and all write binds.
- `factory_sandbox_transport(workspace, config_file=..., exec_path=...,
  exec_args=(), env=None, extra_read_binds=(), extra_write_binds=(), ...)`
  — returns a `droid_sdk.transport.ProcessTransport` whose documented
  `exec_path`/`exec_args` launch the real droid binary inside bubblewrap
  (`exec --input-format stream-jsonrpc --output-format stream-jsonrpc` tail
  preserved verbatim). Integration contract: the caller passes it to
  `Runtime(transport=...)` and connects it first — SDK 0.5.0 requires a
  supplied transport to be connected before high-level use.

## Provisioning (explicit operator action only)

The scripts default to a **read-only audit** and preserve anything that already
exists (existing accounts/directories are flagged, never modified):

```powershell
pwsh scripts/provision-isolation.ps1 -ServiceAccount hivemind-svc `
    -AgentAccount hivemind-agent -BareRoot <data-drive>\git-collaboration
# Apply (elevated, interactive confirmation required):
pwsh scripts/provision-isolation.ps1 -Apply -Confirm:$false ... # rejected
pwsh scripts/provision-isolation.ps1 -Apply -ServiceAccount ... -AgentAccount ... -BareRoot ...
```

```sh
sudo scripts/provision-isolation.sh --check \
    --service-account hivemind-svc --agent-account hivemind-agent \
    --bare-root /var/lib/hivemind/git-collaboration
sudo scripts/provision-isolation.sh --apply --confirm-isolation \
    --service-account hivemind-svc --agent-account hivemind-agent \
    --bare-root /var/lib/hivemind/git-collaboration
```

Neither script touches firewall rules; neither is invoked by Hivemind code,
tests, or automation.

## Shared runtime wiring

`model_wiring.integration_options` accepts `os_isolation.attestation`:

```json
{
  "os_isolation": {
    "mode": "required",
    "separate_service_identity": true,
    "separate_agent_identity": true,
    "protect_bare_directories": true,
    "attestation": {
      "service_identity": "hivemind-svc",
      "agent_identity": "hivemind-agent",
      "protected_root": "<isolated-root>\\service",
      "max_age_seconds": 60
    }
  }
}
```

This is a configuration example, not a production deployment instruction.
`IsolationRuntimeGuard` checks the actual unprivileged service process, distinct
accounts, directory ownership/DACL/modes and ancestors. Canonical data and Git
storage must remain inside the protected root. Checks happen before opening the
canonical database, on capability installation/status, and before each native
Git command. Status exposes attestation subjects/timestamps. The original
desktop operator cannot pass a service-account identity check.

Worker `runtime_options.os_identity` checks the actual unprivileged agent process
before worker startup and each vendor turn. Native account launch, role-specific
credentials, workspace access and production service installation remain operator
deployment work; these checks do not switch identities or grant ACL access.

`runtime_options.vendor_filesystem_sandbox: true` is a separate, explicit
filesystem-only opt-in. OMP wraps its actual RPC launch; Factory connects the
official supplied `ProcessTransport` before constructing `Runtime`. Private
session directories are created outside the workspace. Hook settings/extension
files are bound read-only, not their private parent directory. Additional binary,
Python/extension dependency, certificate/DNS and provider-auth access must be
explicitly supplied through `sandbox_read_binds` and `sandbox_env`. Factory uses
a private `HOME`; its owned supplied transport is closed even if opening fails.
Read-only binds cannot shadow the workspace or private writable session roots.

### Strict controlled egress

Configure vendor worker `runtime_options` explicitly:

```json
{
  "require_os_isolation": true,
  "vendor_network_gateway": {
    "model_origins": ["https://api.example-model.com"]
  }
}
```

The Hivemind origin comes from the private worker config's `base_url`, not a
model-supplied URL. Supply 1–16 distinct HTTPS model origins, without paths,
userinfo, query strings or fragments. Model DNS results must be public IPs;
metadata/link-local, multicast and wildcard addresses are rejected even for
Hivemind. DNS addresses are resolved and pinned once; configuration/DNS changes
require a runtime restart.

`vendor_gateway.py` owns a private mode-0600 Unix socket in a mode-0700 directory,
checks peer UID, and permits only exact granted host/port pairs. HTTPS uses
CONNECT with end-to-end TLS; only the configured HTTP Hivemind origin permits
ordinary proxy requests. This is an origin/address boundary, not TLS interception,
path-level authorization or prevention of uploads to an explicitly allowed model.
CONNECT checks its requested authority and pinned TCP address; it does not inspect
encrypted HTTP Host headers. Use controlled endpoints that reject domain fronting,
not shared forwarding proxies.
Hivemind's credential and scope authorization still applies.

Inside the private namespace, `vendor_gateway_bridge.py` exposes a loopback-only
HTTP proxy, sets upper/lowercase proxy variables, clears bypass settings, and
launches the vendor. No host network interface, host resolver or host TCP listener
is available directly. Proxy-unaware vendor traffic fails closed. Environment is
cleared and rebuilt from explicit settings; loader/Python startup overrides and
`NODE_OPTIONS` are rejected. The bridge runs isolated Python from a read-only
mounted helper outside writable areas.

Only workspace and explicit private vendor-state directories are writable.
Hook state and MCP readiness files remain in that private state, not the protected
worker-config directory. Extra read mounts cannot expose worker/gateway parents,
host sockets/devices, escaping symlinks or unbounded trees (10,000 entries).
Use narrow runtime/dependency, provider-auth and CA-file mounts. Trusted host
administration remains an assumption; separate identities and protected storage
still require explicit operator provisioning.

The broker bounds concurrency (16), headers (32 KiB), HTTP bodies (8 MiB), stream
bytes (64 MiB) and connection lifetime (300 seconds). It rejects duplicate headers,
ambiguous framing, transfer encoding, upgrades and ordinary-HTTP pipelining into a
second upstream request. No request bodies or credentials are logged. Runtime close
terminates active connections and removes the private socket directory.

Strict mode without this gateway, Windows, missing bubblewrap, stale/failed probes
and unsupported Factory transport interfaces all refuse before vendor launch.
The generic `os_isolation.sandbox_argv(component="vendor")` remains refused;
vendor adapters must use the dedicated launchers.

Actual Ubuntu WSL validation (unprivileged UID 1000, temporary bubblewrap 0.9.0)
passed filesystem/network namespace probes and a sandboxed child test: workspace
writes worked, an unbound host file was hidden, local Hivemind-shaped HTTP traffic
worked through the gateway, direct host-loopback access failed and an ungranted
metadata URL returned 403. No system packages, firewall or kernel security settings
were changed. Live Factory/OMP provider sessions were not run; provider proxy
compatibility and deployment mounts must be validated separately.
See [the approved Windows account/ACL test](windows-isolation-test.md).
