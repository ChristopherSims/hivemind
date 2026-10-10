# Durable agent restart journal

`hivemind/recovery.py` is a local SQLite journal that makes a worker agent's own
Hivemind commands restart-safe. It records the intent, the exact payload checksum and
the durable outcome of each operation, checkpoints runtime/state/workspace files, and
reports drift after a process restart.

It is a client-side durability helper. It does not execute agent work, grant authority,
renew claims, or verify that a task is complete. It performs no network access except
the caller-supplied `hivemind.sdk.Client`, and it never writes to workspace files.

## Operation states

Every operation has exactly one durable outcome:

| State | Meaning | Safe next step |
| --- | --- | --- |
| `pending` | Recorded but never sent, or the last attempt was definitively rejected (4xx). Nothing was applied. | Retry with the same payload. |
| `completed` | A success reply was recorded. | Replaying returns the stored result without a second network call. |
| `unknown` | A timeout, transport error, 5xx, or an attempt interrupted by a process stop. The server may or may not have applied it. | Never re-run automatically. Verify remotely, then `resolve_operation` or an explicit `safe_to_retry=True` retry. |

`attempting` is a separate transient marker set before a request is sent. Any operation
still marked `attempting` when the journal is reopened is resolved to `unknown` by
`recover()` (also run automatically on open).

## Command keys and payload checksums

* `command_key(operation_id)` is a deterministic Idempotency-Key, so a retry reuses the
  server's idempotency slot instead of creating a second write. It is stable across
  restarts and does not change with the attempt count.
* `payload_checksum(method, path, body)` is a SHA-256 over the exact original request.
  It is always computed from the unredacted payload, so the key/payload binding survives
  redaction.
* Reusing an operation ID with a different method, path or payload after any attempt
  raises `JournalConflict`. Use a new operation ID for a corrected command. This is
  stricter than the server, which only binds a key to its payload after a successful
  command is cached.

## Credentials and diagnostics

* A request body that looks like credential material (bearer tokens, `password=`,
  `token:`, private keys, or a credential-named JSON key) is **not persisted**. The call
  raises `SensitivePayload` unless the caller passes `allow_sensitive_payload=True`.
  With that explicit policy the body is stored redacted, `body_redacted` is set, and the
  checksum still covers the exact original payload, so retry detection stays exact.
* Results are stored canonically; a result that looks sensitive is stored redacted with
  `result_redacted` set, so a later replay returns the redacted view.
* Failure records contain only a kind, an HTTP status, and an exception class name.
  Exception messages, tracebacks, URLs and headers are never persisted.
* Runtime identity, caller state and metadata are passed through `sdk.redact_value`
  before storage.

## Checkpoints and reconciliation

`checkpoint(label, files=[...], state=..., runtime=..., metadata=..., operation_id=...)`
persists the runtime identity, the caller state, and the SHA-256 and size of each listed
file. Paths are resolved inside the configured `workspace`; a path that escapes the
workspace, is absolute outside it, or hits an `sdk.excluded` location raises
`WorkspaceViolation`. Checkpointing only reads files.

`reconcile(checkpoint_id=None, files=None)` is read-only. It returns:

* `files`: `modified`, `created`, `deleted`, `unchanged`, `unverified` (relative paths).
* `runtime_drift` / `state_drift`: per-key `{checkpoint, current}` differences.
* `recovered`: operations moved from an interrupted attempt to `unknown`.
* `pending`, `unknown`, `completed`: current outcome inventory.
* `unsafe_to_rerun`: `unknown` operations that are not read-only, which must not be
  re-issued without a human or explicit policy decision.
* `uncertain_side_effects`: every `unknown` operation, with the drifted files when the
  operation is linked to the reconciled checkpoint.
* `containment_violations`: stored paths that no longer resolve inside the workspace
  (they are reported, never followed).

Reconciliation never re-runs an effect, never resolves an `unknown` outcome on its own,
and never rewrites a workspace file. A stored relative path that now escapes the
workspace through a symlink is reported instead of hashed.

## Callbacks

`Journal(path, workspace=..., runtime_provider=..., state_provider=..., on_event=...)`.
The providers are invoked when a checkpoint is written and again during `reconcile`, so
runtime/state drift is observed rather than assumed. `on_event(kind, record)` receives
`operation_begun`, `operation_completed`, `operation_rejected`, `operation_unknown`,
`operation_resolved`, `recovered`, `checkpoint` and `reconcile`. Observer failures are
swallowed so they can never break durability.

## Usage

```python
from hivemind.recovery import Journal, derive_operation_id
from hivemind.sdk import Client

journal = Journal(".hivemind-data/journal.sqlite", workspace=".",
                  runtime_provider=lambda: {"runtime": "droid", "session_id": session},
                  state_provider=lambda: {"step": worker.step})
client = Client(base_url, token)

journal.checkpoint("before export", files=["src/export.py"], operation_id=operation_id)
result = journal.request(client, "POST", "tasks/{id}:checkpoint", body, operation_id)
```

Callers that must survive their own crash derive a stable ID, for example
`derive_operation_id(session_id, "checkpoint", revision)`. `begin_operation`,
`mark_attempt`, `complete_operation` and `fail_operation` expose the same bookkeeping for
callers that own their transport; `request` is a thin wrapper over them.

After a restart:

```python
report = journal.reconcile()
if report["unknown"]:
    # Verify the remote state, then either:
    journal.resolve_operation(op_id, "completed", verified_result)
    # ...or, only when the request is genuinely repeatable:
    journal.request(client, method, path, body, op_id, safe_to_retry=True)
```

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_agent_recovery.py -q
```

The suite covers replay after a fresh connection, deterministic command keys across
restarts, changed-payload conflicts, ambiguous outcomes that refuse an automatic retry,
definite rejections that stay `pending`, interrupted attempts, credential fail-closed and
explicit-redaction paths, exception-diagnostic scrubbing, workspace containment, file
drift, uncertain side effects, runtime/state callbacks, and two real-service cases through
the actual `Client.request` (a stable-key retry after a lost reply creates exactly one
agent). No network, paid model, or user file is used.

## Limits

* Single-owner and single-process, matching the rest of the reference milestone. Two
  processes sharing one journal file can mislabel each other's in-flight attempts.
* The journal assumes a 4xx reply means the server did not apply the command, which
  matches Hivemind's transactional command handling (rejected commands are not cached and
  their transaction rolls back). Against a service where that is untrue, record the
  outcome with `fail_operation(operation_id)` instead, which keeps it `unknown`.
* Local durability only: this is not an exactly-once guarantee for external effects, an
  OS sandbox, or proof that an abandoned process stopped.
* `unknown` is resolved by the caller, not by the journal. Nothing here inspects remote
  state on its own.
* Reconciliation compares hashes only; it does not inspect file contents, merge
  concurrent edits, or restore anything.
* Credential detection is pattern-based. A caller with an unusual secret shape can still
  opt in to persisting a redacted copy, so treat the journal file as private data.
