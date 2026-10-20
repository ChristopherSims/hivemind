# Scoped Nexus delegation and optional integrations

## Nexus control grants

An administrator or project human operator can create a grant:

```json
{
  "project_id": "project-id",
  "nexus_agent_id": "nexus-agent-id",
  "target_agent_ids": ["worker-agent-id"],
  "actions": ["pause", "resume"],
  "ttl_seconds": 3600
}
```

Send it to `POST /v1/nexus-grants` with an idempotency key.
List grants through `GET /v1/projects/{project_id}/nexus-grants`.
Revoke through `POST /v1/nexus-grants/{grant_id}:revoke` with
`{"expected_revision": 1}`. Python and TypeScript clients expose these operations.
Grants are visible only to operators.

The first grant permanently switches that project/Nexus-agent pair to restricted
authority. Active grants are additive, but each permission must match **both**
the target agent and action within the same grant. There are no wildcards.
Expiry, revocation, hidden/deleted records, restart and cached command receipts
do not restore legacy unrestricted access. Grants never create credentials,
enable the base coordinator flag, acquire a lease or refresh its epoch.

Authorization is checked at command issuance, replay, acknowledgement,
checkpoint and result application. Pending commands lose authority when grants
are withdrawn; each command pins its original grant/revision and bounds its
expiry to that grant. A later grant cannot revive an older command. First-time
restriction also retires unbound legacy commands. Retirement preserves
`outcome_unknown` and its reservation for an applying command.
An uncertain physical effect is not undone or declared absent. Revoked Nexus
agents can still release their own live lease.

These grants cover the five run-control actions: `steer`, `assign_task`, `pause`,
`resume`, `cancel`, plus `monitor` for viewing a provisioned worker's status and
permission incidents. Monitoring does not grant process control.
Scoped Nexus agents cannot use unscoped Git publication,
goal dispatch, routing or provisioning privileges as a bypass. Those operations
fail closed for restricted Nexus identities. Administrators retain operator
authority; legacy Nexus identities without a restriction ledger are unchanged.
Fine-grained delegation of those administrative operations is not advertised.

## VS Code and generic JSON-lines capture

The extension also provides a real ACP v1 stdio client with explicit agent launch,
prompt/cancel, session load/reconnect, authorized metadata capture and reported
session/tool/plan/file attribution. See [VS Code ACP setup](../adapters/vscode/README.md).
Interrupted turns are never replayed. ACP host file writes/terminals and agent
permission requests remain denied; optional client file reads require fresh
canonical scope permits. Agent-side direct access still needs OS isolation.


`adapters/vscode` contains a directly loadable JavaScript extension. Use an
Extension Development Host or package it with the usual VS Code tooling.
Run **Hivemind: Start Private Capture** and select an operator descriptor outside
the one trusted workspace:

```json
{
  "base_url": "http://127.0.0.1:8080",
  "token_file": "C:/private/agent-token",
  "project_id": "project-id",
  "agent_id": "agent-id",
  "run_id": "run-id",
  "workspace_root": "C:/work/project",
  "spool": "C:/private/editor-spool.sqlite",
  "python_executable": "C:/trusted/python.exe"
}
```

The trusted Python must have Hivemind installed and reside outside the workspace.
The extension does not discover repository launchers/configuration. It launches
only the fixed module, keeps credentials out of arguments, and observes open,
change, save and close events as path/version/size/SHA-256 metadata. Buffer text
is never sent. Secret/dependency directories and buffers above 1 MiB are excluded
or reported as capture gaps. Backpressure emits gaps, not fabricated continuity.
Stop Capture closes stdin so the persistent SDK spool can flush.

Other runtimes can launch `python -m hivemind.editor_capture ABSOLUTE_DESCRIPTOR`
and send bounded UTF-8 JSON-lines:

```json
{"type":"tool.started","data":{"call_id":"call-1","name":"read"}}
{"type":"tool.completed","data":{"call_id":"call-1","success":true}}
{"type":"agent.status_reported","data":{"text":"Waiting for input"}}
```

Records cannot choose identities, confirmed attribution, commands, terminal
run status or control results. The binding is re-authorized before capture;
canonical event ingestion rechecks each event. Editor observations use a
separate `editor.buffer_observed` type: saving a buffer is **not** proof that disk
changed, a command applied, or a task completed.

## Native artifact verifier profiles

Enable artifact contracts and configure fixed profiles through the existing
operator integration file:

```json
{
  "artifact_contracts": {"enabled": true},
  "native_artifact_verifiers": {
    "enabled": true,
    "staging_root": "C:/private/artifact-checks",
    "require_os_isolation": true,
    "max_snapshots": 128,
    "profiles": [{
      "name": "lint",
      "project_id": "project-id",
      "argv": ["C:/trusted/ruff.exe", "check", "--no-cache", "artifact.py"],
      "input_path": "artifact.py",
      "timeout_seconds": 30
    }]
  }
}
```

Name the check in the file output's contract. The checker verifies canonical
content checksums, stages only the submitted inline text at the fixed input
path, and binds its receipt to project, contract/revision, task, execution, run,
agent, output and artifact pins. Caller paths/argv/environment are not accepted.
Modified input cannot pass. The contract's durable intents prohibit uncertain
check replay, and its final authorization/CAS recheck still applies.

Timeout and output bounds use the existing whole-tree process boundary.
Snapshots are retained for audit; admission is capped. Operators manage
retention, dependencies and trusted check executables. This is not semantic
correctness proof or a production filesystem sandbox. Required isolation fails
closed where the existing OS isolation runner is unavailable, including stock
Windows. Disabling that requirement is an explicit operator risk decision.

## Optional Mem0, LangMem and Letta adapters

Inject already configured, timeout-bounded SDK/store instances into trusted
Python application configuration. No optional package is imported or installed
automatically:

```python
from hivemind.api import create_app
from hivemind.memory_adapters import (
    LangMemAdapter, LettaAdapter, Mem0Adapter, MemoryAdapterPolicy,
)

app = create_app(capability_options={"memory_adapters": {
    "policy": MemoryAdapterPolicy(enabled=True, allow_remote_writes=True),
    "adapters": {
        "mem0": Mem0Adapter(mem0_client, project_id, "operator-fixed-namespace"),
        "langmem": LangMemAdapter(langgraph_store, project_id, "operator-fixed-namespace"),
        "letta": LettaAdapter(letta_client, project_id, "operator-fixed-namespace"),
    },
}})
```

`POST /v1/memories/{memory_id}:mirror` accepts
`{"adapter":"mem0","expected_revision":2}` plus an idempotency key.
The Python/TypeScript facades expose this operation. It is operator-only and
default-off, including a separate external-write opt-in.

Only current, verified, public **project** memory with current public evidence
can be exported. Authorization/source pins are rechecked before and after the
provider call and on replay. A durable reserved intent precedes the external
write. Exceptions, unsupported acknowledgements, source changes and interrupted
intents hold as unknown; changing the request key never repeats that effect.
Shutdown drains provider work or retains canonical storage open.

- **Mem0:** data-only `add(..., user_id=namespace, infer=False)`. SDKs without
  this contract fail closed. No fallback to extraction or paid inference.
- **LangMem:** the LangGraph BaseStore namespace port used by LangMem tools.
  `put(..., index=False)` avoids embedding/model calls. Configure LangMem tools
  against the same `("hivemind", namespace)` tuple. This is a storage adapter,
  not LangMem model extraction or automatic learning.
- **Letta:** standalone blocks with namespace metadata. No automatic attachment
  to an agent and no model turns.

SQLite remains authoritative. Provider content cannot verify memory, clear a
gate, grant permission or enter canonical context automatically. Namespace,
endpoint, credentials, SDK timeouts and network disclosure consent belong to
the operator, not API/model payloads. SDK versions/contracts must be verified
in the target deployment. Governed data-only retrieval, proposed imports and
automatic owned withdrawal/deletion propagation are now available through
separate opt-ins. See [Memory provider integration](memory-provider-integration.md).
Uncertain effects remain held; provider-wide physical erasure is not claimed.

## Validation limits

The final expanded regression batch passed **319 tests**. An additional real
native-check API round trip passed, including idempotent replay without another
native run. Whole-tree Ruff, TypeScript facade checks, VS Code module syntax and
schema export passed.

The earlier full offline run had **1,380 passed, 22 skipped and 3 failures**.
The stale-epoch/authentication error-order regression was fixed. Both background
timing tests passed on isolated rerun and in the final expanded batch; no claim
is made that the complete suite was rerun after these fixes.

Tests exercise real local native syntax checks, the loaded VS Code module with
host doubles, canonical editor-event ingestion and provider SDK/store contract
doubles. No external provider data was sent and no paid inference was run.
Interactive VS Code deployment, live SDK compatibility and production sandbox
verification remain deployment checks.
