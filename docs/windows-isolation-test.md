# Approved Windows identity and ACL test

On 2026-10-10 the operator approved creating only:

- Local standard accounts `hivemind-svc` and `hivemind-agent`.
- A new `<isolated-root>` test directory.

The operator separately approved removing the first failed attempt's disabled
service account and empty test directory before recreating them. No preexisting
accounts, project data, firewall rules, or logon policy were changed.

## Verified host effects

Both accounts are enabled standard Users, not Administrators. Explicit protected
ACLs separate these directories:

| Directory | Test identity access |
| --- | --- |
| `service` | Service owns/full control; agent has no access |
| `agent` | Agent owns/full control; service has no access |
| `tools` | Both identities read/execute only |
| `operator` | Administrators and SYSTEM only |

Administrators and SYSTEM retain management access. Root traversal does not grant
access to protected descendants. Service children include `git`, `state`, and
`credentials`; these are test directories, not a migrated production deployment.

Generated passwords are retained only as DPAPI-encrypted `PSCredential` files in
the operator directory. They are bound to the provisioning operator's Windows
identity, protected by ACLs, and were never printed, sent to a model, or copied to
repository evidence. Do not expose or copy these files. Provisioning scripts do
not overwrite existing resources.

## Real verification

The final native .NET Framework probe was compiled from hash-pinned local source
into the administrator-protected tools directory. Separate bounded processes ran
under the two accounts. The elevated parent independently queried each live PID's
owner SID before accepting its result:

- Service and agent process identities are distinct and match the named accounts.
- Each process wrote its own test directory.
- Both were denied cross-directory writes and writes to trusted tools.
- The agent was denied reading the service credential sentinel.

The service's secret-read-denial flag in the historical fixture result is a
non-applicable pass marker, **not** a claim that the service cannot read its own
credentials. Only the agent's secret-read check was exercised.

Redacted successful evidence:
`.hivemind-test/os-isolation-live-20261010-b/verification-native-v2-report.json`.
Earlier failure reports are retained in the same directory. The first launch hit
a PowerShell principal-binding error; reporting was corrected to use `Out-File
-NoClobber`. WindowsApps PowerShell cannot serve as a fresh-account launcher.
The subsequent PowerShell probe failed process-owner observation;
the native probe used a bounded observation window and completed successfully.
Failed attempts disabled newly created test accounts rather than leaving
unverified active identities.

Scripts: `scripts/provision-isolation-test.ps1`,
`scripts/verify-isolation-test.ps1`, and `scripts/isolation-probe.cs`.
Normal pytest only parses guard scripts and compiles the native probe; it never
creates users, changes ACLs, enables accounts, or runs the protected-root probe.

## Limits

This verifies **account and directory permissions**, not a vendor sandbox.
Windows Job Objects provide process cleanup, not filesystem/network containment.
These standard users may access other resources their Windows groups permit.
No paid vendor session was run under these accounts. No Hivemind server was
installed as a Windows service or moved from the existing project directory.
Strict vendor sandbox requirements must still refuse unsupported Windows execution.

The accounts and protected test root are intentionally retained as authorized.
Any later deletion, credential rotation, service installation, or additional
host-policy change needs its own explicit operator scope.
