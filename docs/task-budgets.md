# Task Budgets

When server budget governance is enabled, canonical executions carry
`budget_required`. Managed workers automatically install the governance controller,
even without `--task-budgets`; they refuse invocation without its private durable
journal and exact-serialization/output-bound adapter contract. The CLI flag also
allows opting in early. This is cooperative framework enforcement, not an OS
sandbox or a guarantee that arbitrary external clients cannot call a provider.

Per-task token, time, and cost governance with durable, cumulative pre-turn
reservations. Implemented by `hivemind/task_budgets.py` (`VERSION = "task-budgets-v3"`,
`MODELS` exported for the schema exporter); the shared layers (API/service/storage/
models/scheduling/worker/CLI/SDK) are owned by the integration layer.

## Budgets

`TaskBudget` is strict: every field explicit, non-negative, finite, coercion-free
(bools and int-as-float rejected). Each budget binds a task to one `model_id`, one named
local token `counter`, a governed `currency` (`USD`, `EUR`, `GBP`, `factory_credits` —
distinct units, **never converted**; factory credits are not dollars), `token_budget`,
`seconds_budget`, `cost_budget`, a mandatory `output_reserve` (declared hard output
maximum; zero is an unbounded-runtime claim and rejected), and unique escalation
thresholds. `set_budget` refuses replacement while unsettled holds exist.
`enforce_cost=true` by default: a zero cost budget really forbids positive spending.
Token/time-only tasks must explicitly set `enforce_cost=false`; their cost is not governed.

## Accounting: no silent refunds

Each reservation row stores its accounted values at bind/settle time, so reads are exact
SQL aggregates (`snapshot`) — no Python iteration over history, no writes on read paths.

- Open/expired reservations account their **reserved worst case** (exact prompt tokens +
  declared output reserve; quoted reserved cost when known).
- Settlement with **trusted, complete** usage (provider seam or frozen price schedule)
  accounts actuals.
- **Untrusted or incomplete** usage (missing token counts, API-reported claims — even a
  spoofed `0 tokens / $0`) keeps the reserved worst case accounted and records an
  explicit unknown hold. Nothing is refunded without trusted complete evidence.
- Unknown or crashed invocations block new reservations and dispatch until explicit
  authorized resolution (a trusted operator settle, which may resolve expired holds).
- Open live reservations block the next turn by default; concurrent open reservations
  require the explicit `allow_concurrent_open` policy opt-in. Expired and unknown holds
  always block.

## Reservations

- Keys are SHA256 over the canonical JSON tuple `(task_id, run_id, execution_id,
  turn_key)`; each reservation stores the exact prompt fingerprint — replay with a
  different prompt is `reservation_replay_conflict`, and a replayed receipt is never
  permission to execute (`precheck` raises `reservation_already_bound`, zero runtime
  calls).
- Two-phase binding: `prepare()` computes the exact local count and quote **off all
  locks** (pure, replay-safe, never triggers inference); `publish()` revalidates the
  budget fingerprint **inside** a `BEGIN IMMEDIATE` transaction (mid-flight budget
  replacement fails closed with `budget_changed`) and cumulatively admits — atomic
  across concurrent ledger instances/processes.
- Each reservation freezes the budget and price schedule it was bound under; settlement
  and checkpoints use the frozen copies.
- Lease expiry uses persistent wall-clock evidence scoped to the current boot nonce:
  holds created before a restart are never auto-released. Expiry maintenance
  (`sweep_expired`) runs only on write paths, never in read/gate paths.
- Caller-reported elapsed time is a monotonic max: a lower or replayed value never
  reduces persisted time.

## Runtime and worker contracts

- `RuntimeBudgetGate.precheck(task, context, serialized_prompt, capabilities)` —
  requires `GateCapabilities(trusted_serializer=True, enforces_output_max=True)`, else
  fail closed; on refusal nothing is invoked or counted.
- `checkpoint(reservation, elapsed_turn_seconds=...)` — persisted monotonic-max gate
  against the frozen time budget.
- `after_result(reservation, usage, ...)` — trusted seam settlement.
- `budgeted_turn(gate, task, context, serialized_prompt, capabilities, invoke)` —
  reserve → invoke → settle; crashed invoke leaves a durable blocking hold.
- The worker CLI's `--task-budgets` installs `WorkerBudgetGovernance`. It requires
  the private runtime journal and an operator-trusted adapter implementing
  `prepare_budgeted_turn(task, context, output_max_tokens=...)`, returning a
  `PreparedBudgetTurn` whose callback actually enforces that output limit. Stock
  vendor adapters do not currently implement this contract and are refused before
  invocation. No silent ungoverned fallback occurs.
- Worker maintenance and checkpoint boundaries persist elapsed time. Missing or
  untrusted usage keeps the reservation held and stops further worker dispatch.

## Scheduling gates

`budget_dispatch_gate(ledger)` and `budget_task_start_gate(ledger)` return
`(principal, task, run) -> {"action": "allow"|"defer", "reason": str} | None` callbacks
for `service.task_dispatch_gates` / `service.task_start_gates`: pure bounded canonical
reads (SQL aggregates only, verifiably zero writes). They defer on unconfigured budgets,
exhaustion, outstanding open holds, or unknown/expired holds.
`budget_dependency_gate(ledger)` always returns `None`.

## Cost authority

Authoritative provider usage from the trusted seam, or the frozen trusted operator
`PriceSchedule`. Client-submitted usage is coerced to `reported` evidence unless the
caller is an authorized admin/human operator. Missing usage is held, never zero. Under
`strict_cost` (default), an enforced cost budget without usable price authority refuses
the turn (`exact_cost_unsupported`), including zero budgets. Disabling cost governance
does not attest that a runtime is free.

## Escalation

Each `(task, threshold, kind)` fires exactly once, durably, bounded (64 per task), before
exhaustion; notifications are private-safe (ids/fractions only) and follow the durable
write. Fraction ≥ 1.0 yields `control_hold`. A newly admitted reservation may use the
exact remaining capacity; it does not become an unusable hold merely for filling it.

## HTTP surface (`install`)

Disabled policy publishes **no routes** (state is still installed for introspection).
When enabled, canonical routes under `/v1/projects/{project_id}` with strict typed
bodies: `POST /budgets` (admin/human; task re-authorized as visible and in-project
inside the command receipt), `GET /budgets/{task_id}`, `POST /budgets:reserve`,
`POST /budgets:settle` (caller represents the run's agent; operators may resolve
exact archived executions), `POST /budgets:checkpoint`, `GET /budgets/{task_id}/escalations`. Mutations go
through the shared `command`/`Service.mutate` receipts; receipt replays never re-bind
the side ledger. Reservation replies recompute `created` per request: a receipt
replay reports `created=false`, never renewed permission to invoke.
Cross-project or private-invisible tasks are `not_found`.

## Storage safety

Directory and existing DB/WAL/SHM paths are reparsed and link-checked before open;
WAL + bounded busy timeout.

## Honest limits

- Counter exactness is operator-seam dependent; the ledger validates shape, not
  tokenizer internals.
- Trusted provider usage is only as trustworthy as the authorized seam delivering it.
- Vendor-hidden overhead is not observable and never estimated.
- Expiry is lazy (swept on write activity); a pre-restart hold blocks until swept by a
  same-boot operation or explicitly resolved — it is never silently released.
- Mid-turn cancellation is cooperative; the cumulative hard pre-reservation bounds the
  worst case.
