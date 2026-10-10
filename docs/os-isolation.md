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
| Protected bare-directory permissions | External + limited read-only audit (no portable ACL API; rejects network roots) | Read-only audit (rejects group/other-writable, optional owner check) | Read-only audit |
| Strict isolation of vendor SDK/RPC runtimes (Factory droid, oh-my-pi) | **Unsupported — always rejected** | **Unsupported — always rejected** | **Unsupported — always rejected** |

Vendor runtimes do not run through `process_boundary` and their sessions are not
verified sandboxes, so `component_applicability("vendor")` reports
`strict_supported: False` and `sandbox_argv(..., component="vendor")` raises
`IsolationRefused`, consistent with `VendorRuntime._prepare` rejecting
`require_os_isolation`.

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
  reports `protected: None` (ACL verification is an operator step).

## Provisioning (explicit operator action only)

The scripts default to a **read-only audit** and preserve anything that already
exists (existing accounts/directories are flagged, never modified):

```powershell
pwsh scripts/provision-isolation.ps1 -ServiceAccount hivemind-svc `
    -AgentAccount hivemind-agent -BareRoot D:\hivemind-data\git-collaboration
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

## Required parent integrations (not wired here)

This module is policy/planning/validation only. The shared wiring is owned by
the parent agent:

1. **Configuration**: accept an `os_isolation` section in
   `model_wiring.integration_options` (or service config) producing an
   `IsolationPolicy`, and surface `plan()` decisions at startup (refuse to boot
   on `IsolationRefused` when `mode="required"`).
2. **Process boundary call sites**: thread the policy (or the resulting
   `require_os_isolation` decision from `sandbox_argv`) into
   `GitCollaboration._run`, `TrustedVerifier.require_os_isolation`, and
   `AuthorizedProcessExtractor.require_os_isolation`.
3. **Vendor runtimes**: keep rejecting `require_os_isolation` for
   `VendorRuntime` options; optionally surface `component_applicability("vendor")`
   in configuration errors.
4. **Git collaboration**: after repository initialization, optionally run
   `validate_directory_protection(repo)` and record the audit result.
5. **CLI**: an operator-facing `isolation plan`/`isolation check` command can
   expose `plan()` and the read-only audits.
