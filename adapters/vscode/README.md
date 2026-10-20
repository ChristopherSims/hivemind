# Hivemind VS Code ACP integration

Connect VS Code to an operator-approved **Agent Client Protocol v1** stdio agent.
Capture authorized session, tool, plan and editor-file metadata into an existing
Hivemind run. This is an ACP client, not a transparent observer of other extensions.

## Setup

1. Install Hivemind into an operator Python outside the workspace.
2. Create an active Hivemind run and exclusive-owner claims for its scopes.
3. Keep the token, descriptors, SQLite spool and session state outside the workspace.
   Use private directory permissions/ACLs. Do not put these files in repository settings.
4. Create a private capture descriptor:

```json
{
  "base_url": "http://127.0.0.1:8765",
  "token_file": "/private/hivemind/agent-token",
  "project_id": "project_example",
  "agent_id": "agent_example",
  "run_id": "run_example",
  "scope_ids": ["scope_example"],
  "workspace_root": "/work/project",
  "spool": "/private/hivemind/editor.sqlite",
  "python_executable": "/opt/hivemind/bin/python"
}
```

Use HTTPS for non-loopback deployment. Supply absolute paths appropriate to your OS.
The Python interpreter must import the installed `hivemind` package from the operator
home directory. No repository Python configuration or executable discovery is used.

5. Create a separate private ACP descriptor:

```json
{
  "capture_descriptor": "/private/hivemind/capture.json",
  "executable": "/opt/agent/bin/acp-agent",
  "args": [],
  "session_state": "/private/hivemind/acp-session.json",
  "allow_execution": true,
  "environment_passthrough": [],
  "read_files": false
}
```

The command must be an actual ACP agent's documented stdio entry point. This extension
does not invent vendor flags, install an agent, discover credentials, or enable paid
models by itself. Starting that executable may perform provider-specific setup;
each prompt is explicitly entered through VS Code and may use paid inference.
Only explicitly named additional environment variables are passed through.

## Install and use

For development, launch VS Code with this directory as `--extensionDevelopmentPath`
and open one trusted local workspace. For a VSIX, package this directory with
`@vscode/vsce`, then install the generated VSIX through VS Code's Extensions menu.
No marketplace publication is required.

- **Hivemind: Connect ACP Agent** selects the private ACP descriptor.
- **Hivemind: Prompt ACP Agent** sends one bounded prompt. Replies appear only in an
  ephemeral, script-disabled editor panel, not in captured logs or session state.
- **Hivemind: Cancel ACP Turn** sends `session/cancel`; it does not fabricate an applied result.
- **Hivemind: Reconnect ACP Session** initializes a new owned agent process and uses
  `session/load` only when the agent advertises it. It never resends a prompt.
- **Hivemind: Disconnect ACP Agent** stops the owned connection and capture bridge.

The existing **Start Private Capture** command remains available for metadata-only
buffer capture without an ACP agent. Avoid running both against the same run/spool.

## Authorization and attribution

Before agent launch, every prompt, every captured event and every permitted file
read, the private Python bridge reads the actual run and checks canonical
`permits:check` ownership, pending controls and liveness. File paths receive specific
permit checks before and after reading. Capture acknowledgments require server
acceptance; pending or quarantined SQLite rows are not treated as delivered.
Failure blocks further processing, rather than granting stale authorization.

`editor.acp_observed` is **reported evidence**, never filesystem/effect proof or
canonical run/task completion:

- Session IDs and tool-call IDs are SHA-256 correlations, bound to a connection ID.
- Tool statuses and plan entries come from the agent. Plan content is hash-only.
- Replayed `session/load` updates are tagged `delivery: replay`.
- ACP file reads carry editor-observed content hashes and workspace-relative paths.
- Ordinary editor changes are `actor: unattributed`. A human edit is not claimed
  as an agent edit merely because a session is connected.
- Prompt/message text, tool arguments/results and file contents are not captured.

An interrupted/running persisted turn becomes `unknown`. Reconnect can load its
history, but cannot submit another prompt. Reconcile the canonical work and use a
new correctly bound run/session descriptor; never delete uncertainty to retry a
possibly executed prompt.

## Supported boundaries

ACP v1 initialization, new/load session, prompt, cancel, tool/plan updates and optional
`fs/read_text_file` are supported. Write-file, terminal and permission requests
are denied. Reads reject traversal, symlinks, excluded sensitive/dependency directories
and buffers over 1 MiB. JSON-RPC frames, pending calls, notification queues, prompts,
plans and UI output are bounded.

This is **not an OS sandbox** for the launched agent. An operator-approved executable
may access the host or network directly outside ACP; use the separately configured
OS isolation/deployment boundary where required. Other editors/extensions' sessions
are not adopted. There is no automatic model retry or local reconciliation override.

## Validation

`npm test` exercises a real stdio subprocess with a synthetic ACP agent, including
reconnect/replay, uncertainty, attribution rejection and denied capabilities.
`test/extension-host.js` is an opt-in real VS Code host harness with synthetic local
API/agent fixtures. It substitutes picker/input UI only; document/edit/save APIs are
real. Set `HIVEMIND_TEST_PYTHON` and `HIVEMIND_TEST_NODE`, use a fresh editor profile,
and pass this file as `--extensionTestsPath`. It writes `result.json` beside the
test workspace. No marker means no successful host-validation claim.
Live third-party ACP agents and paid provider sessions require separate operator
validation.

The current local validation passed 12 Node stdio/security tests. The final Python
capture/protocol/contract batch passed 152 tests; one existing artifact timing test
failed its 0.6-second elapsed-time assertion and passed on focused rerun.
Ruff, JavaScript syntax and whitespace checks passed. Attempts to launch the installed VS Code host
did not produce the required result marker, so actual GUI-host execution is not
claimed as verified.
