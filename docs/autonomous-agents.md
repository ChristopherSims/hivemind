# Autonomous agent integration

These integrations remain explicitly opt-in and local-only. Runtime adapters
retain separate execution and model-call opt-ins. No paid-live validation or
host account/ACL provisioning is implied by offline contract tests.

## Worker Git workflow

An operator initializes the project's bare repository first. Run a worker with
`--enable --git-automation --allow-native-git --git-state-file <private-state.json>`.
`--allow-native-git` separately authorizes bounded host-side fetch/integration
commands; it does not grant the model a shell tool.

The worker provisions its private clone before constructing the runtime, then
uses that clone as the runtime and journal workspace. Private credentials, runtime
state, and journals must stay outside the clone. Dirty/staged user work must not
be silently swept into an automatic commit. Git publication occurs before terminal
task reporting, while execution and claim fences remain live.

See [worker Git automation](git-worker-automation.md).

## Nexus service loop

Private `serve --agent-integrations <file.json>` configuration accepts
`nexus_git_automation`, with the fields of `NexusGitPolicy`. The policy defaults
to disabled and binds an exact project, Nexus principal/run, trusted operator
principal, independent reviewer principals, check names, and per-tick limits.

The background service schedules bounded ticks. Reviewer calls occur outside
canonical write transactions so callbacks and heartbeat can proceed. Missing or
invalid reviewer decisions abstain, never manufacture approval. Only the configured
current fenced Nexus publishes the exact approved hashes.

`GET /v0.1/projects/{project}/git-automation` reports enablement, reviewer availability,
the last bounded error type, and isolation control status.

See [Nexus Git automation](nexus-git-automation.md) and
[reviewer runtime](git-reviewer-runtime.md).

Configure `git_reviewer_runtime` in the same private integration JSON with a
`policy` (including both boolean `enabled` and `allow_model_calls`), and a
`runtime` containing `adapter` (`factory` or `oh_my_pi`) and private `options`.
The options retain the adapter's own execution/model-call opt-ins and reviewer
credential-file binding. Use a private `state_file` location outside snapshots;
each canonical review execution gets separate state under its parent directory.
The service automatically bridges an enabled reviewer executor into the Nexus loop.

For worker messaging, `--messaging` requires explicit `--message-sender <agent-id>`
allowlist entries. `--message-replies` separately enables correlated replies.
The dedicated message runtime uses no tools and its own private state file.
`--notification-type <type>` separately allows canonical notification turns.
Notices never grant tool/action authority and have no reply channel. Declined or
unknown notices remain unacknowledged.

## Vendor output and resume context

Factory and OMP prompts receive bounded canonical checkpoint/progress/handoff
evidence, with provenance, credential redaction, stale-execution rejection, and
an explicit warning that this evidence grants no authority and promises no restored
vendor state.

Normal vendor results remain hash-only. Private runtime option
`result_contract: "git_review"` explicitly permits a validated exact-hash JSON
verdict. `result_contract: "message"` permits a bounded validated handled/reply
object. Raw transcripts are not retained. `read_only_tools: true` restricts
reviewer tools; `no_tools: true` disables tools for text-only messaging phases.

See [resume context](runtime-context-consumption.md) and
[runtime messaging](runtime-messaging.md).

## Isolation and operations

Private integration configuration accepts `os_isolation`. Required unavailable
sandbox controls refuse startup. Required account/ACL controls also refuse until
automatic attestation exists: operator provisioning alone is not proof of a verified
runtime identity boundary. Opportunistic mode reports these controls as external
or degraded. Enforceable sandbox policy applies to configured checks/extractors.
Trusted broker Git mutations require service-owned write access, not a read-only
agent sandbox. Vendor SDK/RPC strict sandboxing remains unsupported.

Provisioning scripts default to audit-only and were not applied during validation.
See [OS isolation](os-isolation.md).

`hivemind git-ops` exposes `preflight`, `reconcile`, `backup`, `restore`,
`retention-plan`, and `retention-execute`. Repair/removal and retention require
explicit scope/confirmation; unknown effects are not automatically replayed.
Restores must target a new directory. A Git bundle is not a complete Hivemind data
backup: canonical SQLite, private effect journals, runtime identities, and service
configuration must also be backed up consistently. See [Git operations](git-operations.md).
