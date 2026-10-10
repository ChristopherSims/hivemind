# Task-specific context assembly

`hivemind/context_assembly.py` is an **additive** context surface. The pre-existing
`service.context` implementation remains at `POST /v0.1/context:query` for compatibility.
`POST /v0.1/context:assemble` now serves the ranked assembler. Its canonical candidates
also honor the host's configured repository-freshness filter.

## Interfaces

- `assemble(service, principal, request, counter=None, scan_limit=None, wrap=None) -> dict`
  builds a ranked, compact packet. `request` is the existing `models.ContextQuery`, so no
  new request schema is introduced.
- `install(app, service, principal, command, envelope, counter=None, scan_limit=None)`
  registers `POST /v0.1/context:assemble` on a FastAPI app. `principal` is the request
  dependency (as in `hivemind.api`), `command` is accepted for signature parity (assembly
  is read-only), and `envelope` wraps the response **and** is used to measure the
  envelope-inclusive budget.
- `default_counter(target_tokenizer=None)`, `local_tokenizer_available(...)`,
  `reset_tokenizer_cache()`, and `Counter` expose the token-counting policy.

### Integration note

`hivemind.api.create_app` installs this route without the old alias. The endpoint freezes
one response header/request ID before counting, so it measures the envelope actually
returned. Dedicated tests and combined service/worker tests cover the installed route.

## Token accounting

The token budget covers the **canonical JSON response envelope**: protocol fields,
`request_id`, selection metadata, items, and graph edges. The returned `token_count` is
measured on the returned canonical envelope and is guaranteed `<= token_budget`.
If safe metadata cannot fit, the endpoint returns 422 `token_budget_too_small`.
Numeric count fields can themselves affect tokenization. A bounded, inert
`token_count_padding` field breaks count cycles when needed; it is included in the
budget. An injected counter that cannot settle fails with 503 `token_counter_unstable`.

Counter resolution order:

1. A caller/test-injected counter (`callable` or an object exposing `.count(text)`).
2. A real tokenizer from the **local** Hugging Face cache, loaded with
   `tokenizers.Tokenizer.from_file` (no network access). The pinned model directory is
   preferred, then the caller's `target_tokenizer` name, then any cached tokenizer. The
   counter reports `counter_id` (e.g. `bert-base-uncased`).
3. A conservative UTF-8 byte upper bound, explicitly labeled
   `conservative_utf8_bytes_upper_bound` with `fallback: true`.

The `tokenization` block reports `counter_id`, `method`, `exact_for_target_tokenizer`,
`fallback`, `target_tokenizer`, and a human-readable `note`. A local tokenizer is exact for
*that* tokenizer, which may differ from the caller's target LLM; the note says so instead of
implying otherwise. No model weights are ever loaded by this module.

## Candidate discovery and authorization

Candidates are read straight from the canonical SQL `resources` table, so the recent-1000
`Store.list` cap no longer truncates the candidate set. Every row is then filtered through
the same `Store` helpers the rest of the service uses:

- `Store.visible` — project membership, private-resource ownership, message from/to.
- `Store.valid_memory` — invalidation, dispute, expiry, not-yet-valid, and recursive
  evidence-source validity/revision checks.
- `Store.task_view` — execution/evidence availability.

Private intermediates are never traversed, and graph expansion rechecks both endpoints and
the canonical relation. A relevant record that is older than the 1,000 most recent records
is therefore still discoverable, while private, expired, disputed, invalidated, stale-
evidence, and cross-project records stay hidden.

Per-request bounds are explicit and configurable (`DEFAULT_SCAN_LIMIT = 20000`,
`MAX_SCAN_LIMIT = 200000`) rather than a silent recent-1000 drop. The packet exposes
`scan.scan_limit`, `scan.scanned` (rows considered per kind), `scan.scan_truncated`
(whether the bound was hit), and `scan.runtime_ms`. `truncated` and `omitted` (count,
bounded `resource_ids`, per-category reasons) expose every inclusion decision.

## Ranking and compaction

Ranking is deterministic and prioritizes the requester's task, active claims, current
verified memory, and query-relevant source content. Category weights are:

```
task 1.00 > work_claim 0.90 > memory 0.80 > artifact 0.66 > board_post 0.64
      > control 0.55 > message 0.54 > run 0.40 > graph 0.30
```

A query-match bonus (up to `0.08`) orders items within a category, and the requester's own
task receives an additional binding bonus so a task-specific packet leads with it. Ties
break on category order, then `seq`, then `resource_id`, so identical inputs yield an
identical item order.

Soft per-category budget shares (`CATEGORY_SHARE`) prevent one large category from
starving the rest; leftover budget is filled in a second pass. Oversized text fields are
compacted to a per-item token cap (`min(1024, budget // 4)`) with `content_truncated`
flagged, and the packet is trimmed until it fits the budget.

## Evidence and bindings

Every item carries `trust`/`origin`, `untrusted_evidence: true`, and explicit
`bindings` (`project_id`, `agent_id`, `run_id`, `task_id`). The packet carries a top-level
`evidence_notice` and `bindings` (`project_id`, `requester_principal_id`,
`requester_agent_id`, `requester_run_id`, `requester_task_id`). Items are retrieved data,
never instructions, system prompts, or hidden reasoning. When a `requester_run_id` is
supplied its agent always participates in selection so the run/task binding is visible.

## Tests

`tests/test_context_assembly.py` covers:

- honest tokenizer labeling (real local tokenizer or labeled fallback);
- exact injected-counter budget, envelope-inclusive, with `token_count` re-verified against
  the returned packet;
- discovery of a relevant record older than the 1,000 most recent (proving the SQL scan
  beats `Store.list`, which drops it);
- exclusion of private, expired, disputed, not-yet-valid, stale-evidence, and invalidated
  memory for a non-admin agent principal;
- deterministic ranking (`task`, `work_claim`, `memory`, `board_post`);
- compaction of oversized content within budget;
- endpoint registration, request validation, authentication, and envelope shape.
