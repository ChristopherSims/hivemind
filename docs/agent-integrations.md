# Agent integration operations

The eight non-UI integration tracks are wired in order. Defaults do not launch a
vendor, extractor, or model. Local SDK/RPC substitutes validate contracts, not live
Factory/Oh my Pi compatibility or paid model performance.

## 1. Vendor adapters

Install the optional `runtimes` dependency extra for Factory's official asyncio SDK
(locked to `droid-sdk` 0.5.0). Worker adapter entry points are
`hivemind.vendor_runtimes:factory_runtime` and
`hivemind.vendor_runtimes:oh_my_pi_runtime`.

Private worker JSON contains the usual `base_url`, `token`, `project_id`,
`agent_id`, and `run_id`, plus `runtime_options`:

```json
{
  "enabled": true,
  "allow_model_calls": true,
  "workspace": "<external-workspace>",
  "config": "<private-external>/agent-credential.json",
  "state_file": "<private-external>/agent-runtime.json",
  "timeout_seconds": 300
}
```

Both booleans authorize actual vendor model execution; do not enable them for
offline testing. The worker also requires `--enable`. Credentials/runtime state
must be outside the agent workspace. OMP optionally accepts `executable` and
`model`; either adapter accepts an operator-declared `tokenizer_id`.

Tools are a fixed file-only allowlist. Every managed mutation checks its actual
paths against current scope permits and its current task execution. Managed hooks
reuse the existing run/claim, rather than minting another identity. Session state
stores identity and result hashes, not raw vendor transcripts. Confirmed controls
need an observed settled turn and a saved quiescent checkpoint.

## 2. Supervision

```text
hivemind worker --config <private-external>/worker.json --adapter hivemind.vendor_runtimes:factory_runtime --journal <private-external>/worker.sqlite3 --workspace <external-workspace> --enable --supervise --max-restarts 3 --reconcile-interval 30
```

One OS session lock owns the journal. Restarts have bounded backoff. Unknown
nonretryable effects and applied controls prevent automatic restart. Reconciliation
uses a separate observer run; stale presence is not proof that a process stopped.

## 3. Independent review and correction

The service accepts explicit operator wiring through `create_app(...,
capability_options=...)`, or `serve --agent-integrations <private JSON>`.
`review_automation` config selects existing independent reviewer principal IDs,
allowlisted check names, and optional background scheduling. `verification`
config supplies `TrustedVerifier` constructor fields (`workspace_root`, `checks`,
deadline/output limits, and optionally `require_os_isolation`).

`POST /v1/review-automation:tick` requires operator authority. Producers cannot
review themselves. Decisions require fresh passing checks; requested corrections
reactivate the original producer execution through `:begin-correction`. Worker
CLI processing resubmits corrections and invalidates previous checks. Correction
cycles and execution attempts are bounded. Trusted command outcomes have a
separate durable `check-effects.sqlite3` ledger, so canonical transaction rollback
does not silently rerun a completed host effect.

## 4. Verified dependencies

Set `require_verified_dependencies: true` on a planned task. Its dependencies must
have an independently verified result for their **current execution and checkpoint**.
Old reviews or changed checkpoints cannot unlock dispatch. Default remains false.

## 5. Handoff

`POST /v1/tasks/{id}:handoff` takes `expected_revision`, `target_agent_id`,
optional `target_run_id`, and `reason`. Only operators can transfer work. A live
unpaused execution is denied. The old execution is archived and fenced; the
receiver gets progress/checkpoint data on dispatch. Evidence visibility and
revision checks also apply to transferred checkpoints. Existing claims are never
stolen or forcibly released: receiver dispatch waits for a fresh valid claim.

The runtime receives handoff data and current authorized context, not the old
agent's private transcript, hidden reasoning, or credentials.

## 6. Uncertain effects

Message/notification handlers deduplicate by project, actor, and stable item ID.
A callback crash records an unknown outcome; it is not acknowledged or blindly
repeated. Runtime failure receipts distinguish observed failure from transport
uncertainty. Restart checks query the entire ledger, not just recent entries.

```text
hivemind resolve-effect --journal <private-external>/worker.sqlite3 --operation <operation-ID> --decision completed --reason "Inspected the applied change" --evidence <private-external>/evidence.json
```

Evidence JSON must identify the exact `operation_id` and contain no credentials.
The ledger stores the evidence hash and a bounded redacted reason. `not_applied`
allows an explicitly adjudicated retry. Optional `--runtime-state` requires exact
run/task/execution identity matching before updating an uncertain vendor state.
This is operator adjudication, **not automatic proof** of external effects.
Canonical task position must also be reconciled; a completed vendor turn cannot
silently execute the same task/correction phase twice. A missing trusted-check
result receipt is not replaced by an operator assertion of test success.

## 7. Containment

Trusted checks and process extractors have bounded stdin/stdout/stderr/deadlines
and sanitized environments. Windows Job Objects and POSIX process groups clean up
owned child processes, including descendants after timeout or parent exit.

These controls are **not a Windows filesystem/network sandbox** and do not protect
against a malicious same-user process. POSIX groups alone are not a hostile-process
sandbox either. Strict check/extractor isolation uses Linux bubblewrap when
available (read-only workspace, no network). Unsupported strict requests fail
before launch. Vendor SDK/RPC adapters accept `require_os_isolation: true` only on
Linux with a fresh filesystem/network probe and explicit `vendor_network_gateway`
model origins. Direct networking is denied; model and Hivemind traffic must use
the origin-allowlisted gateway. Unsupported or proxy-unaware traffic fails closed. The distinct
`vendor_filesystem_sandbox` opt-in wraps actual Linux vendor launches behind a fresh
enforcement probe, sharing network for provider egress; Windows refuses it.
Service/agent process identity and protected-storage attestation are wired through
explicit account configuration. See [OS isolation](os-isolation.md) for deployment
scope and [concurrent messaging](worker-messaging-concurrency.md) for auxiliary
runs and evidence-only effect recovery. Trusted injected
in-process callbacks remain cooperative and can ignore cancellation.

## 8. Extraction and tokenizers

Operator integration JSON supports:

- `memory_pipeline.config`: `PipelineConfig` fields, including `enabled`.
- `memory_pipeline.extractor`: `AuthorizedProcessExtractor` fields, including
  `argv`, dedicated `workspace`, `model_id`, `enabled`, `allow_model_calls`,
  `timeout_seconds`, and `require_os_isolation` (defaults true).
- `memory_pipeline.background`: explicit background opt-in.
- `runtime_tokenizers`: entries with `model_id`, local `tokenizer_file`, and its
  required `expected_sha256`.

Extractor stdin is JSON `{model_id, prompt, max_output_bytes}`; stdout must match
the proposal JSON contract in `memory-pipeline.md`. Evidence is untrusted data.
Environment model-loading flags are offline; no credentials are inherited for a
provider. Model output cannot select proposal authority, privacy, or verification.
This wiring supports an operator's local model command, but does not download or
choose a generative model.

Tokenizer loading uses local data only, with padding/truncation disabled. Workers
select the runtime's declared tokenizer ID. Missing targets use a labeled UTF-8
upper-bound fallback, never a different cached embedding tokenizer. Set worker
`require_exact_context_tokens: true` to refuse a fallback. Counts cover the assembled
response envelope, not undocumented vendor system/tool prompt overhead.

Python/TypeScript clients and MCP expose handoff, review ticks, and correction
reactivation. OpenAPI/schema discovery covers their strict request models.
