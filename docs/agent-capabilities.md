# Integrated agent capabilities

All eight workstreams are installed or composed in the local reference. Existing
identity, visibility, revision, claim, and control checks remain in force. Authorization
enhancements remain deferred. No dashboard features or paid agent/model sessions were added.

| Capability | Integration | Details |
| --- | --- | --- |
| Persistent execution | Opt-in `AgentWorker` and guarded `hivemind worker` CLI | [Worker](agent-worker.md) |
| Scheduling | Dependency plans, priorities, declared capabilities, pull dispatch, bounded retries and escalation | [Scheduling](task-scheduling.md) |
| Restart recovery | Durable command journal, single-owner lock, explicit safe replay, unknown-effect stop | [Recovery](agent-recovery.md) |
| Conversations | Correlated requests/replies, deadlines, cancellation, escalation and delivery-lease renewal | [Conversations](agent-conversations.md) |
| Objective verification | Self-reported submissions, independent reviewers/corrections and operator-allowlisted local checks | [Verification](task-verification.md) |
| Context assembly | Ranked canonical candidates, compact untrusted evidence, local tokenizer and envelope-inclusive budget | [Context](agent-context.md) |
| Memory pipeline | Disabled-by-default injected extraction, advisory duplicate/contradiction suggestions and repository freshness | [Memory pipeline](memory-pipeline.md) |
| Runtime lifecycle | Explicit reconciliation, contained hash-only file drift and checkpoint-backed cooperative controls | [Lifecycle](runtime-lifecycle.md) |

## Service wiring

`create_app` installs all feature routes. Python and TypeScript HTTP clients and MCP
tools expose the main operations. Model schemas are exported with module-qualified
filenames under `spec/schemas`; OpenAPI describes the complete installed API.

Model extraction and trusted verification are configured only by the operator's Python
host, not by incoming task text:

```python
from hivemind.api import create_app
from hivemind.memory_pipeline import PipelineConfig
from hivemind.verification import TrustedVerifier

app = create_app(
    "private-service-data",
    capability_options={
        "verification": {"verifier": TrustedVerifier(
            workspace_root, {"unit": trusted_unit_test_argv})},
        "memory_pipeline": {
            "config": PipelineConfig(enabled=True, workspace=workspace_root),
            "extractor": explicitly_authorized_local_extractor,
        },
    },
)
```

The placeholders are trusted, operator-supplied objects. Nothing selects a vendor,
downloads a model, installs hooks, or executes task/evidence text. Memory processing is
explicit unless the host also opts into a background worker.

When an enabled pipeline has a configured repository snapshot source, canonical memory
retrieval, including legacy and compact context, excludes drifted repository pins.
Snapshots are shared only within one HTTP request and refreshed on the next request.
Unavailable snapshots fail closed for pinned memories. This does not change stored
memory status/revision; detail/history remain available for review. Without a configured
snapshot source, repository filtering is not claimed.

## Worker composition

The CLI checks the durable journal before loading the selected trusted runtime factory.
Its defaults assemble context and submit completed results as **pending**, self-reported
verifications. The worker reuses a newly dispatched execution, checkpoints progress,
reports scheduling outcomes, and clears the run assignment through the server.
Explicit failed results follow scheduler backoff and attempt budgets; they are not
silently treated as completed. Successful executions remain available to the verification
and correction workflows.

CLI controls use `LifecycleBoundary`: pause/cancel is reported applied only with the
adapter's observed status and checkpoint. Missing evidence produces an unknown outcome
and stops local mutating work. Live task/message handlers have heartbeat/claim maintenance;
message handlers can also renew delivery leases. These controls remain cooperative.

Library users opt into `assemble_context`, `submit_verification`,
`renew_message_leases`, and a supplied `control_boundary` on `AgentWorker`.
The worker is inert unless `enabled=True`. See the CLI example in [worker docs](agent-worker.md).

## Boundaries

- Task-runtime effects, injected checks, model processing, and runtime reconciliation
  with an unknown journal outcome are never automatically replayed. Explicit reconciliation
  is required. Ordinary durable server commands can replay under their original keys.
- Message/notification handlers remain at least once and need application-level
  deduplication by stable IDs. The journal is not a transaction around their side effects.
- Task completion and dependency satisfaction are self-reported. Verification is a
  separate verdict; a passing reported test alone never verifies a task. Reviewer
  assignment and correction handling remain explicit, not an automatic reviewer loop.
- There is no OS sandbox, forced process cancellation, exactly-once execution, automatic
  cross-agent handoff, or real Factory/Oh my Pi lifecycle driver. Local injected adapters
  and a trusted child-process file adapter were tested.
- Queue/history retention and wide-dataset performance remain bounded reference behavior.
  Local tokenization is exact for the selected counter, not necessarily the target LLM.
  Counts cover the canonical JSON envelope; too-small budgets return 422.
- A trusted extractor that ignores shutdown cannot be forcibly stopped. Shutdown sets
  `app.state.shutdown_pending` and leaves its storage open rather than closing it underneath
  that thread. Isolate or bound host plugins before production use.
