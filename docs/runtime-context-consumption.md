# Runtime context consumption

How canonical checkpoint/progress/handoff records reach vendor (Factory and Oh My Pi)
prompts, and the guarantees that surround them.

## Contract

`hivemind.agent_worker.AgentWorker._context` assembles the worker-side context dict
(`run_id`, `execution_id`, `progress`, `resumed`, `handoff`, `packet`, ...). The
scheduling handoff (`hivemind.scheduling.Scheduling.handoff` / dispatch accept) stores
an `accepted` handoff on the task carrying the prior execution's progress, checkpoint,
`from_execution_id`, and `accepted_execution_id`.

`hivemind.vendor_runtimes.resume_context(task, context, secrets, budget)` converts that
canonical state into a `managed_resume_context` block that `VendorRuntime._prepare`
embeds in every vendor prompt alongside the task and context packet.

## Guarantees

- **Provenance and labeling.** Every block is labeled
  `provenance: canonical_hivemind_task_record` and `trust: untrusted_evidence`; the
  checkpoint and handoff sub-sections carry their own provenance tags
  (`canonical_execution_checkpoint`, `canonical_scheduling_handoff`). The prompt
  preamble instructs the vendor that the block is bookkeeping, not authority.
- **No invented restore guarantee.** `restore_guarantee` is always `"none"`. Vendor
  output is retained only as a SHA-256 hash; the block is a resume *hint* and never
  claims the vendor session restored prior state.
- **Stale execution rejection.** A handoff that is not `state == "accepted"`, whose
  `accepted_execution_id` does not match the current `execution_id`, or whose
  `from_execution_id` equals the current execution raises `UnknownOutcome` before any
  vendor session starts and before runtime state flips to `running`. A task whose
  embedded execution id does not match the context execution id is rejected the same
  way. Checkpoints marked `evidence_unavailable` are never consumed.
- **Credential redaction.** Adapter config secrets (any string config value whose key
  matches token/secret/key/password) are removed from summaries, handoff reasons, and
  the task description before serialization, and common credential shapes (bearer
  tokens, `key=value` secrets, private-key blocks, well-known token prefixes) are
  redacted by pattern.
- **Bounded envelope.** The serialized resume block is capped at
  `RESUME_CONTEXT_BUDGET_BYTES` (16 KiB, overridable via the
  `resume_context_budget_bytes` runtime option, clamped to 1 KiB..64 KiB). Trimming is
  deterministic: handoff prior-checkpoint summary first, then execution checkpoint
  summary, then the handoff prior checkpoint, handoff reason, then whole sections.
  Progress is clamped to 0..100, checkpoint summaries to 4096 characters, evidence
  refs to 20 entries. The overall vendor prompt transport boundary (256 KiB) is still
  enforced after the block is embedded.

## What is preserved

- Existing opt-in, identity, workspace-isolation, and quiescence checks in
  `VendorRuntime._prepare` run unchanged and still precede any vendor I/O.
- Tool restrictions (`tool_names`, disabled tools, managed hooks config) are
  untouched; resume context adds no tool authority.
- Resume/pause/cancel checkpoint semantics (`_settled`, `_control`) are unchanged.
