# Vendor Budget Adapters

**Status: deferred by the user on 2026-10-12.** Production budget adapters are not
enabled or complete. `hivemind/vendor_budget_adapters.py` records the capability
investigation and candidate helper seams; the stock runtimes do not delegate to them.
The existing `WorkerBudgetGovernance` (see `docs/task-budgets.md`)
requires a `PreparedBudgetTurn` with an exact serialized prompt, the budget's exact
`model_id` and named local `counter`, and a turn that **genuinely enforces** the
declared output maximum.

## Capability status: both vendors fail closed today

Capability is **probed from the installed vendor surface**, never declared by
configuration, CLI flags, or metadata.

- **Factory** (`droid exec` 0.233.0 / `droid_sdk` 0.5.0): **unsupported**.
  `Session.stream` accepts only `prompt/images/files/output/timeout/
  include_partial_messages`; `SessionConfig`/`SessionSettings` carry model and tool
  configuration only; the `droid exec` CLI and the JSON-RPC client expose no
  output-token limit. The serialized prompt is additionally not the exact full model
  input — the vendor prepends its system prompt, tool schemas and hook frames — so a
  local count of it would undercount input tokens.
- **Oh My Pi** (`omp` 18.8.9, `--mode rpc`): **unsupported** through the wired RPC.
  The documented `prompt` params are `{message, images, streamingBehavior}`
  (rpc-wire schema); no per-turn output cap command exists. The internal
  `runEphemeralTurn` `maxTokens` (used by `/btw`) is not RPC-exposed, and catalog
  `model.maxTokens` is policy metadata: `fitOutputTokensToContextWindow` only lowers
  it but floors at 1024 tokens near a full context window,
  `ensureMaxTokensForThinking` raises it for budget-thinking models, and
  `omitMaxOutputTokens` compat policies can drop it silently. Hard enforcement is
  never inferred from such metadata.

`prepare_factory_budgeted_turn` / `prepare_omp_budgeted_turn` therefore raise
`BudgetAdapterError(reason="budget_capability_missing", capability=...)` **before**
any vendor contact, state write, or durable runtime intent — nothing is invoked,
counted, or persisted. `factory_capability()` / `omp_capability()` /
`capability_status(options)` / `describe_adapters()` expose the structured reasons
and probe evidence for operators. A future vendor surface that adds a cap-like
parameter is still refused (reported as unverified evidence) until its enforcement
semantics are verified and this module attests it.

## Integration contract

```
turn = runtime.prepare_budgeted_turn(task, context, output_max_tokens=int)
# -> worker_budget_governance.PreparedBudgetTurn(serialized_prompt, model_id,
#    counter, output_max_tokens, invoke, enforces_output_max=True)
```

When a probed surface is ever attested, `_build_turn` provides the exact governed
preparation:

1. **Configuration-source revalidation** (`_validate_turn_request`) precedes any
   vendor contact: the output reserve is an explicit int in `[1, 10**9]`, the exact
   vendor model id must be pinned in runtime options (`budget_model_unspecified`
   otherwise — a vendor default cannot be matched to the budget `model_id`), and a
   named exact local counter must be bound.
2. `runtime._prepare` performs the identity, isolation, and private-config checks
   and records the durable `running` intent. If the subsequent reservation is
   refused, no model call occurred and the runtime fails closed on the next
   preparation until reconciled — a durable intent, never a silent retry.
3. `invoke(checkpoint)` runs exactly one turn through the runtime's own documented
   machinery (`Session.stream` for Factory; `prompt` → `prompt_result` →
   `session_settled` → `get_state` quiescence for Oh My Pi). The checkpoint runs
   before any vendor call, so an exhausted aggregate time budget spends nothing.
4. Post-turn, reported generated tokens (Oh My Pi: `output + reasoning`, the
   provider-billed generation) are verified against the declared bound. A violation
   (`OutputCapViolation`) means the cap was not enforced: the durable outcome becomes
   `unknown` and the reservation stays held — it is never settled as if the bound
   held. Lost or malformed responses are likewise unknown outcomes; a lost prompt is
   never replayed because a retry is indistinguishable from a duplicate model call.

## Usage normalization (settlement evidence)

- `factory_usage_report(usage)` maps droid_sdk `Usage` to a `UsageReport`-shaped
  dict. `factory_credits` is a governed currency unit, **never converted** to
  dollars: it settles without a hold only against a `factory_credits` budget.
  Cache creation/read and thinking tokens are billed by the vendor but priced
  separately; they are recorded in `vendor_overhead_note`, which also documents that
  vendor framing (system prompt, tool schemas) is not observable.
- `omp_usage_report(stats, *, cost_currency=None)` maps the documented
  `get_session_stats` SessionStats shape. `cost` is a vendor-computed estimate in an
  unspecified unit and is attached only when the operator explicitly attests its
  governed currency; otherwise cost stays unknown and the ledger holds. Stats are
  session-cumulative vendor reports.
- Missing token counts stay missing (never zero, never estimated): settlement keeps
  the reserved worst case and records an explicit unknown hold. Malformed shapes
  raise `usage_shape_invalid` — adapters never guess.

## Unsupported paths and honest limits

- Any bounded-turn claim from local truncation, interruption, byte bounds, or
  metadata declarations is refused; only probed, semantically verified vendor
  enforcement would ever be attested.
- Aggregate task-time cancellation is cooperative (checkpoints at turn boundaries
  plus the worker maintenance thread); there is no invoice hard-cap guarantee.
- Vendor-hidden overhead (system prompts, tool frames, cache behavior) is not
  observable and never estimated.
