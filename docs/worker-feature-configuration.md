# Worker feature wiring

All features are opt-in. No option enables paid inference or grants tool access.
Keep worker configuration, coordinator credentials and state outside agent workspaces.

## Capture and automatic controls

The `worker --config` JSON accepts:

```json
{
  "capture": {
    "enabled": true,
    "state_dir": "<private-external>/worker-capture",
    "poll_interval_seconds": 2,
    "flush_interval": 5
  },
  "automatic_controls": {
    "policy": {
      "enabled": true,
      "capture_gap_threshold": 1,
      "observed_changes_threshold": 20,
      "auto_resume": false
    },
    "state_file": "<private-external>/worker-controls.json",
    "coordinator": {
      "token": "<private coordinator credential>",
      "agent_id": "<canonical coordinator agent ID>",
      "run_id": "<canonical live coordinator run ID>"
    }
  }
}
```

These are additions to the usual worker identity/runtime configuration.
Capture defaults to the actual worker workspace, including provisioned Git clones;
a different configured workspace is refused. State must live outside that workspace.
The lifecycle creates a distinct canonical observer run with a stable command key,
starts the watcher and spool, and records lifecycle audits in the worker journal.

Automatic controls renew the configured coordinator's lease before dispatch,
acquiring only when canonical renewal proves no matching lease. Another coordinator's
active lease is never stolen. Unsupported controls and uncertain runtime outcomes
hold the worker. Resume requires explicit policy and an exact applied pause checkpoint.
Ordinary captured changes are not unobserved drift. With capture enabled, the CLI
installs a trusted collector for actual canonical conflict replies; a drift threshold
also enables bounded local reconciliation against a durable baseline, excluding
watcher-observed paths. This is detection of unobserved changes, not proof that a
change was authorized or safe. Drift/conflict policies without capture are refused.
Policies run before dispatch and at supported long-turn checkpoint/maintenance
boundaries. New signals arriving during acknowledgement stay pending.

Shutdown stops the watcher before the spool and journal. A timed-out watcher keeps
dependent handles and the session lock open in an embedded CLI process and reports
failure. Process exit is not a verified runtime pause. Investigate retained state
before restarting. Polling captures net changes, not every intermediate write.

## Recovery

- `--recover-task-creates`: before startup command resumption, reconcile unknown task
  creation against exact principal-scoped canonical receipts, with service-normalized
  payload hashes. Missing/deleted receipts remain unknown.
- `--recover-message-effects`: also runs auxiliary recovery after restoring slot
  bindings but before starting consumers. Current delivery leases, controls, run/task/
  execution identity and durable provenance are required. No model turn is replayed.

## Exact budgets and restore release

Service integration configuration supports the strict `runtime_tokenizers` object
documented in `scalable-retrieval.md`. Exact counters use pinned local bytes and one
shared registry; unavailable targets fail before retrieval.

Offline release now requires:

```text
hivemind coordinated release-hold --root <snapshots-root> --restored-store <store>
  --approval <approval.json> --identity <owner-credential.json> --confirm <approval-digest>
```

The digest is `restore_release.evidence_digest(approval)`, not the manifest digest alone.
An interrupted release requires explicit `--resume-release` with the same approval.
Credentials are checked against the current restored canonical admin principal and
never written to audit. See `restore-release.md` for the approval schema and offline
exclusion requirements. Release itself grants no fresh Nexus authority.
