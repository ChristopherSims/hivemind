Runtime safety and recovery, 2026-10-17

Approved approach: WSL2 Linux sandbox and an explicit allowlisted gateway.
Approved validation: offline and synthetic only. No paid turns, live vendor
enrollment, system-wide installations, account/ACL/firewall changes or cgroup delegation.
Follow-up testing builds dependencies inside Docker and extracts temporary WSL
test environments; it does not install anything in the WSL system.
This is NOT a native Windows AppContainer sandbox or an automatic WSL migration.

Actual host preflight: runtime-safety-wsl-2026-10-17.json.
Ubuntu runs unprivileged on WSL2, but required Linux Hivemind packages, bubblewrap,
writable cgroup delegation and Linux vendors are absent. Ready=false is expected.
Do not treat offline contract tests as proof that this deployment is sandboxed.

Implemented and offline-tested:
- Explicit Linux cgroup-v2 admission before the startup GO handshake; no
  process-group-only fallback. cgroup.kill plus populated=0 are required for
  confirmed descendant cleanup.
- Python isolated-mode bootstrap prevents workspace packages from executing
  before Job/cgroup admission. Worker failure exit codes are preserved.
- WSL2 explicit distro/native paths, bounded preflight, expiring policy-bound
  readiness and strict-gateway guest launch gates. Launch was NOT exercised.
- Factory continuation through the real installed SDK wire parser and governed
  session adoption; missing/relative/conflicting workspace evidence refuses.
- Serialized pause/cancel/resume, active-turn resume refusal, and durable
  generic-adapter coverage for pause/resume/cancel/steer/task assignment.
  Unknown or foreign controls do not become running-success acknowledgements.
- Exact Factory MCP server/tool-ID binding, not display-name/suffix guessing.
- Native-verifier deployment assessment and required real namespace proof;
  injected runners never count as sandbox evidence. Staging symlinks refuse.

Still gated:
Protected production cgroup deployment and service-level lifetime,
service-crash lifetime handling, live
Factory MCP enrollment and live interrupted-session continuation. macOS native
whole-tree provisioning and native Windows vendor sandboxing are not provided.

Detailed guide: docs/runtime-safety-validation.md.
The existing *.md ignore rule makes that new guide local-only; this text
summary and the JSON preflight evidence are not covered by that rule.

Final offline validation: 386 passed, 7 skipped, in 82.79 seconds.
The skips require real Linux/bubblewrap/kernel isolation, POSIX ownership/modes
or unavailable symlink privileges. Ruff, scoped whitespace checks and in-memory
worker JSON Schema/OpenAPI generation passed. Existing schema snapshots were
not overwritten. Result record: runtime-safety-results-2026-10-17.json.

Docker/WSL follow-up:
- WSL: 388 passed, 9 skipped, 63.99 seconds, including real filesystem/network
  namespace, allowlisted gateway and isolated native JSON verification checks.
- Docker default restrictions block nested bubblewrap. Actual refusal checks:
  3 passed, 1 skipped. Original positive checks failed (139 passed, 3 failed,
  2 skipped); the broader Docker run timed out and is NOT a completed pass.
- No security exceptions, privileged containers, host installs or delegation.
- Temporary WSL tools and image remain for reproduction; no test containers run.
- Cgroup admission/whole-tree cleanup and live vendor release gates remain open.
Evidence: runtime-safety-linux-2026-10-17.json.
Reproduction and exact temporary-mount scope: runtime-linux/README.txt.

Subsequent explicitly approved Docker sandbox/cgroup follow-up:
- Nonprivileged UID-1000 sandbox suite with container-only seccomp/AppArmor
  exceptions: 146 passed, 2 branch skips (4.13 seconds).
- Separate privileged/private-cgroup setup delegates only its own unique test
  subtree, then runs real admission/setsid cleanup as UID 1000: 2 passed,
  0 skipped (0.13 seconds). Live and exited parent cases drain to populated=0.
- No host directories, cgroups, credentials or Docker socket are mounted.
  Container-only delegation is killed, drained and removed before exit.
- A broader sandbox-enabled Docker matrix exited nonzero without completing;
  its root cause is not established, and it remains a regression gate.
- Production service-crash handling, malicious same-UID protection and live
  vendor behavior are not established by these synthetic tests.
Evidence: runtime-safety-docker-enabled-2026-10-17.json.
