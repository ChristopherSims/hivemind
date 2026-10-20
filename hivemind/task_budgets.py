"""Per-task token/time/cost budget governance with durable pre-turn reservations.

Owned contracts (the parent wires these in shared layers):

- ``budget_dispatch_gate(ledger)`` / ``budget_task_start_gate(ledger)`` -> callbacks
  for ``service.task_dispatch_gates`` / ``service.task_start_gates``:
  ``(principal, task, run) -> {"action": "allow"|"defer", "reason": str} | None``.
  Pure bounded canonical reads: SQL aggregates only, no writes, no expiry sweeps,
  no encoding, no external I/O.
- ``budget_dependency_gate(ledger)`` -> ``service.task_dependency_gates`` callback;
  budgets never gate dependencies, always ``None``.
- ``RuntimeBudgetGate.precheck(task, context, serialized_prompt, capabilities)`` ->
  reservation; called BEFORE any model invocation, raises ``Problem`` to refuse.
  ``capabilities`` must attest a trusted serializer and real output-max enforcement,
  otherwise the turn fails closed (``budget_capability_missing``).
- ``RuntimeBudgetGate.checkpoint(reservation, elapsed_turn_seconds=...)`` records
  cumulative caller-reported turn time as a monotonic max against the frozen budget.
- ``RuntimeBudgetGate.after_result(reservation, usage, ...)`` settles via the trusted
  runtime/provider seam only.
- ``budgeted_turn(gate, task, context, serialized_prompt, capabilities, invoke)``:
  reserve -> invoke -> settle; a replayed or already-bound reservation never invokes.

Accounting rules: a reservation's worst-case reserved tokens/cost stay accounted until
a TRUSTED, COMPLETE usage report proves otherwise; untrusted or incomplete usage never
refunds reserved amounts and leaves an explicit unknown hold. Unknown or crashed
invocations block new reservations and dispatch until explicit authorized resolution.
Cost authority is provider usage from the trusted seam or the trusted operator price
schedule only; missing usage is held, never zero; governed currency units (including
factory credits) are never converted.
"""
import json
import math
import os
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, Header, Request
from pydantic import Field, field_validator

from . import models as m
from .storage import Problem, digest

VERSION = "task-budgets-v3"
CURRENCIES = ("USD", "EUR", "GBP", "factory_credits")  # distinct units, never converted
USAGE_SOURCES = ("provider", "price_schedule", "reported")
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})
MAX_PROMPT_CHARS = 1048576
MAX_ESCALATIONS_PER_TASK = 64


class TokenCounterUnavailable(Exception):
    """The bound local token counter cannot produce an exact count. Fail closed."""


class TaskBudget(m.Model):
    """A strict per-task budget. All fields explicit, non-negative, finite, and
    coercion-free. ``output_reserve`` is the mandatory declared hard output maximum;
    zero would be an unbounded runtime claim and is rejected."""
    task_id: m.ID
    project_id: m.ID
    model_id: str = Field(min_length=1, max_length=256)
    counter: str = Field(min_length=1, max_length=64)  # named local counter binding
    token_budget: int = Field(strict=True, ge=0, le=10**9)
    seconds_budget: float = Field(gt=0, le=86400, allow_inf_nan=False)
    cost_budget: float = Field(ge=0, allow_inf_nan=False)
    enforce_cost: bool = True
    currency: str = Field(min_length=1, max_length=32)
    output_reserve: int = Field(strict=True, ge=1, le=10**9)
    escalation_thresholds: tuple[float, ...] = (0.5, 0.8, 1.0)

    @field_validator("seconds_budget", mode="before")
    @classmethod
    def _explicit_float(cls, value):
        if type(value) is not float:
            raise ValueError("seconds_budget must be an explicit float, never coerced")
        return value

    @field_validator("cost_budget", mode="before")
    @classmethod
    def _no_coercion(cls, value):
        if isinstance(value, bool) or type(value) not in (int, float):
            raise ValueError("Budget amounts reject bool/coerced values")
        return value

    @field_validator("escalation_thresholds", mode="before")
    @classmethod
    def _tuple(cls, value):  # durable JSON round-trips tuples as lists
        return tuple(value) if isinstance(value, list) else value

    def model_post_init(self, context, /):
        if self.currency not in CURRENCIES:
            raise ValueError("Currency must be an explicit governed unit")
        seen = []
        for threshold in self.escalation_thresholds:
            if (type(threshold) not in (int, float) or isinstance(threshold, bool)
                    or not math.isfinite(threshold) or not 0 < threshold <= 1
                    or threshold in seen):
                raise ValueError("Escalation thresholds must be unique finite fractions in (0, 1]")
            seen.append(threshold)


class BudgetPolicy:
    """Operator configuration. Disabled by default; enabled requires a private directory.

    ``allow_concurrent_open``: explicit opt-in letting open (live, known-cost)
    reservations of the same task coexist; by default any unsettled reservation blocks
    the next turn. Unknown holds always block regardless of this flag.
    """
    def __init__(self, enabled=False, directory=None, lease_seconds=600.0,
                 allow_reported_usage=False, strict_cost=True,
                 allow_concurrent_open=False, notifier=None):
        for flag in (enabled, allow_reported_usage, strict_cost, allow_concurrent_open):
            if type(flag) is not bool:
                raise ValueError("Budget policy flags must be booleans")
        if (type(lease_seconds) not in (int, float) or isinstance(lease_seconds, bool)
                or not math.isfinite(lease_seconds) or not 10 <= lease_seconds <= 86400):
            raise ValueError("Reservation lease must be a finite duration in [10, 86400] seconds")
        if enabled and not directory:
            raise ValueError("Enabled budgets require a private local storage directory")
        if notifier is not None and not callable(notifier):
            raise ValueError("Budget notifier must be callable")
        self.enabled, self.directory = enabled, directory
        self.lease_seconds = float(lease_seconds)
        self.allow_reported_usage = allow_reported_usage
        self.strict_cost = strict_cost
        self.allow_concurrent_open = allow_concurrent_open
        self.notifier = notifier


class GateCapabilities:
    """Worker adapter attestation: a trusted serializer for the exact full prompt and
    a runtime that actually enforces the declared output maximum. Without both, the
    gate fails closed -- no unbounded or unserialized turn is admitted."""
    def __init__(self, trusted_serializer=False, enforces_output_max=False):
        if type(trusted_serializer) is not bool or type(enforces_output_max) is not bool:
            raise ValueError("Gate capabilities must be booleans")
        self.trusted_serializer = trusted_serializer
        self.enforces_output_max = enforces_output_max


class UsageReport(m.Model):
    """Actual usage for settlement. Trusted completeness decides accounting:

    - trusted + complete tokens: accounted at actual; cost from provider report or the
      frozen operator price schedule; missing cost is an explicit unknown hold.
    - untrusted (``reported``) or incomplete: reserved worst case stays accounted and
      the reservation is an unknown hold until explicit authorized resolution.
    """
    source: str = Field(min_length=1, max_length=32)
    input_tokens: int | None = Field(default=None, strict=True, ge=0, le=10**9)
    output_tokens: int | None = Field(default=None, strict=True, ge=0, le=10**9)
    cost: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    currency: str | None = Field(default=None, min_length=1, max_length=32)
    vendor_overhead_note: str | None = Field(default=None, max_length=256)

    def model_post_init(self, context, /):
        if self.source not in USAGE_SOURCES:
            raise ValueError("Usage source must be provider, price_schedule or reported")
        if self.cost is not None and self.currency not in CURRENCIES:
            raise ValueError("Cost requires an explicit governed currency")
        if (self.cost is None) != (self.currency is None):
            raise ValueError("Cost and currency must be both present or both absent")


class PriceSchedule(m.Model):
    """Trusted operator price schedule; the only non-provider cost authority."""
    currency: str = Field(min_length=1, max_length=32)
    prices: dict[str, dict[str, float]] = Field(max_length=256)

    def model_post_init(self, context, /):
        if self.currency not in CURRENCIES:
            raise ValueError("Price schedule requires a governed currency")
        for model, entry in self.prices.items():
            if not model or len(model) > 256 or set(entry) != {"input_per_1k", "output_per_1k"}:
                raise ValueError("Each price needs a model id and input/output rates")
            for rate in entry.values():
                if (type(rate) not in (int, float) or isinstance(rate, bool)
                        or not math.isfinite(rate) or not 0 <= rate <= 10**6):
                    raise ValueError("Price rates must be finite non-negative and bounded")

    def quote(self, model_id, input_tokens, output_tokens):
        entry = self.prices.get(model_id)
        if entry is None:
            return None  # unknown price: unknown hold, never a fabricated zero
        return (input_tokens * entry["input_per_1k"]
                + output_tokens * entry["output_per_1k"]) / 1000.0


class ExactLocalCounter:
    """Exact local token counter seam. ``count`` must be exact for the serialized
    full prompt; anything else must raise TokenCounterUnavailable (fail closed)."""
    def __init__(self, count):
        if not callable(count):
            raise ValueError("Token counter must be callable")
        self._count = count

    def count(self, serialized_prompt):
        value = self._count(serialized_prompt)
        if type(value) is not int or not 0 <= value <= 10**9:
            raise TokenCounterUnavailable("counter returned a non-exact result")
        return value


def _now_wall():
    return datetime.now(timezone.utc).isoformat()


def reservation_key(task_id, run_id, execution_id, turn_key):
    """Unambiguous key: SHA256 over the canonical JSON tuple."""
    for part in (task_id, run_id, execution_id, turn_key):
        if not isinstance(part, str) or not part or len(part) > 256:
            raise Problem("invalid_reservation_key", 400)
    return digest(["task-budget-reservation", task_id, run_id, execution_id, turn_key])


def _safe_db_path(directory):
    """Reparse and link-check the directory and any existing DB/WAL files before open."""
    path = Path(directory)
    if not path.is_absolute():
        raise ValueError("Budget directory must be absolute")
    for part in (path, *path.parents):
        if part.is_symlink() or getattr(part, "is_junction", lambda: False)():
            raise ValueError("Budget ledger cannot follow linked directories")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = path / "task-budgets.sqlite"
    for candidate in (db, db.with_suffix(db.suffix + "-wal"), db.with_suffix(db.suffix + "-shm")):
        if os.path.lexists(candidate) and (os.path.islink(candidate)
                                           or getattr(candidate, "is_junction", lambda: False)()):
            raise ValueError("Budget ledger files cannot be links")
    return db


class BudgetLedger:
    """Durable reservation/settlement store.

    Accounting is stored per row at reservation/settlement time (``accounted_tokens``,
    ``accounted_cost``, ``accounted_unknown``) so reads are exact SQL aggregates over
    bounded history with no Python row iteration. Untrusted or incomplete settlements
    keep the reserved worst case accounted: nothing is silently refunded. Expiry uses
    persistent wall-clock evidence scoped to the current boot nonce, so holds created
    before a restart are never auto-released. Each reservation freezes the budget and
    price fingerprint it was bound under; binding revalidates the budget inside the
    write transaction so a mid-flight budget replacement fails closed."""
    def __init__(self, policy=None, *, counters=None, prices=None):
        self.policy = policy or BudgetPolicy()
        self.counters = dict(counters or {})
        self.prices = prices
        self.lock = threading.RLock()
        self.boot_nonce = secrets.token_hex(16)
        self.db = None
        if self.policy.enabled:
            db = _safe_db_path(self.policy.directory)
            self.db = sqlite3.connect(db, check_same_thread=False, isolation_level=None)
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA busy_timeout=5000")
            self.db.executescript(
                "CREATE TABLE IF NOT EXISTS budgets(task_id TEXT PRIMARY KEY, body TEXT NOT NULL);"
                "CREATE TABLE IF NOT EXISTS reservations("
                "key TEXT PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT NOT NULL,"
                "execution_id TEXT NOT NULL, turn_key TEXT NOT NULL, state TEXT NOT NULL,"
                "reserved_tokens INTEGER NOT NULL, reserved_cost REAL, currency TEXT,"
                "cost_known INTEGER NOT NULL, wall_start TEXT NOT NULL,"
                "expires_at_wall REAL NOT NULL, boot_nonce TEXT NOT NULL,"
                "elapsed_seconds REAL NOT NULL DEFAULT 0,"
                "accounted_tokens INTEGER NOT NULL, accounted_cost REAL,"
                "accounted_unknown INTEGER NOT NULL,"
                "actual TEXT, prompt_fingerprint TEXT NOT NULL, frozen_budget TEXT NOT NULL,"
                "frozen_prices TEXT);"
                "CREATE INDEX IF NOT EXISTS reservations_task ON reservations(task_id);"
                "CREATE TABLE IF NOT EXISTS escalations("
                "id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, threshold REAL NOT NULL,"
                "kind TEXT NOT NULL, at TEXT NOT NULL, UNIQUE(task_id, threshold, kind));")

    def _require(self):
        if not self.policy.enabled or self.db is None:
            raise Problem("task_budgets_disabled", 403)

    def close(self):
        with self.lock:
            if self.db is not None:
                self.db.close()
                self.db = None

    @contextmanager
    def _write(self):
        """Explicit immediate transaction: check + insert are atomic even across
        concurrent ledger instances; any failure rolls back."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")

    # -- budget set/status -------------------------------------------------
    def set_budget(self, budget):
        """Set or replace a task budget. Replacement is refused while unsettled holds
        exist: budgets are frozen against in-flight work."""
        self._require()
        body = budget.model_dump(mode="json")
        with self.lock, self._write():
            live = self.db.execute(
                "SELECT COUNT(*) FROM reservations WHERE task_id=? AND state!='settled'",
                (budget.task_id,)).fetchone()[0]
            exists = self.db.execute("SELECT 1 FROM budgets WHERE task_id=?",
                                     (budget.task_id,)).fetchone()
            if exists and live:
                raise Problem("budget_has_live_holds", 409)
            self.db.execute("INSERT INTO budgets(task_id, body) VALUES(?,?) "
                            "ON CONFLICT(task_id) DO UPDATE SET body=excluded.body",
                            (budget.task_id, json.dumps(body, sort_keys=True)))
        return body

    def get_budget(self, task_id):
        self._require()
        with self.lock:
            row = self.db.execute("SELECT body FROM budgets WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise Problem("budget_not_configured", 404)
        return json.loads(row[0])

    def snapshot(self, task_id):
        """Pure bounded read: exact SQL aggregates over stored per-row accounting.
        No writes, no expiry sweeps, no row iteration -- safe under the canonical
        store transaction used by dispatch/start gates."""
        self._require()
        with self.lock:
            budget_row = self.db.execute("SELECT body FROM budgets WHERE task_id=?",
                                         (task_id,)).fetchone()
            row = self.db.execute(
                "SELECT COALESCE(SUM(accounted_tokens),0), COALESCE(SUM(elapsed_seconds),0.0),"
                " COALESCE(SUM(accounted_unknown),0),"
                " SUM(CASE WHEN state='open' THEN 1 ELSE 0 END),"
                " SUM(CASE WHEN state='expired' THEN 1 ELSE 0 END)"
                " FROM reservations WHERE task_id=?", (task_id,)).fetchone()
            spent = {currency: total for currency, total in self.db.execute(
                "SELECT currency, SUM(accounted_cost) FROM reservations"
                " WHERE task_id=? AND accounted_cost IS NOT NULL GROUP BY currency"
                " LIMIT 16", (task_id,))}
            escalations = [dict(zip(("threshold", "kind", "at"), item, strict=True))
                           for item in self.db.execute(
                               "SELECT threshold, kind, at FROM escalations WHERE task_id=?"
                               " ORDER BY id LIMIT 256", (task_id,))]
        return {"task_id": task_id,
                "budget": json.loads(budget_row[0]) if budget_row else None,
                "spent_tokens": row[0], "spent_seconds": row[1], "spent_cost": spent,
                "unknown_holds": row[2], "open_holds": row[3] or 0,
                "expired_holds": row[4] or 0, "escalations": escalations,
                "vendor_overhead": "not_observable"}

    def status(self, task_id):
        snap = self.snapshot(task_id)
        if snap["budget"] is None:
            raise Problem("budget_not_configured", 404)
        snap["open_or_unknown_holds"] = snap["unknown_holds"] + snap["open_holds"]
        return snap

    # -- reservation snapshot/publish (counting happens off all locks) -----
    def prepare(self, task_id, run_id, execution_id, turn_key, serialized_prompt):
        """Snapshot phase: exact local count and quote computed WITHOUT holding any
        lock or transaction. Pure and safe to recompute on replay; binds nothing."""
        self._require()
        if not isinstance(serialized_prompt, str) \
                or not 0 < len(serialized_prompt) <= MAX_PROMPT_CHARS:
            raise Problem("invalid_prompt", 400)
        budget = TaskBudget(**self.get_budget(task_id))
        counter = self.counters.get(budget.counter)
        if counter is None:
            raise Problem("token_counter_unavailable", 503)
        try:
            prompt_tokens = counter.count(serialized_prompt)
        except TokenCounterUnavailable:
            raise Problem("token_counter_unavailable", 503) from None
        reserved_tokens = prompt_tokens + budget.output_reserve
        quote = self.prices.quote(budget.model_id, prompt_tokens, budget.output_reserve) \
            if self.prices else None
        cost_known = (quote is not None and self.prices.currency == budget.currency) \
            or not budget.enforce_cost
        reserved_cost = quote if (quote is not None
                                  and self.prices.currency == budget.currency) \
            else (0.0 if not budget.enforce_cost else None)
        if not cost_known and self.policy.strict_cost and budget.enforce_cost:
            raise Problem("exact_cost_unsupported", 403)  # fail closed, never assume zero
        frozen_budget = json.dumps(budget.model_dump(mode="json"), sort_keys=True)
        return {"key": reservation_key(task_id, run_id, execution_id, turn_key),
                "task_id": task_id, "run_id": run_id, "execution_id": execution_id,
                "turn_key": turn_key, "reserved_tokens": reserved_tokens,
                "reserved_cost": reserved_cost, "currency": budget.currency,
                "cost_known": cost_known,
                "prompt_fingerprint": digest(["task-budget-prompt", serialized_prompt]),
                "frozen_budget": frozen_budget,
                "frozen_prices": self.prices.model_dump_json() if self.prices else None}

    def publish(self, snapshot):
        """Publish phase: revalidate the budget fingerprint INSIDE the write
        transaction (a mid-flight budget replacement fails closed), then cumulatively
        admit. Returns ``(record, created)``; replay requires the exact same prompt
        fingerprint and is a receipt, never permission to run."""
        self._require()
        key = snapshot["key"]
        with self.lock:
            self._sweep_expired_locked()  # write path: expiry maintenance lives here
            with self._write():
                current = self.db.execute("SELECT body FROM budgets WHERE task_id=?",
                                          (snapshot["task_id"],)).fetchone()
                if current is None:
                    raise Problem("budget_not_configured", 404)
                if current[0] != snapshot["frozen_budget"]:
                    raise Problem("budget_changed", 409)  # stale price/limits: refuse
                budget = TaskBudget(**json.loads(current[0]))
                existing = self.db.execute("SELECT * FROM reservations WHERE key=?",
                                           (key,)).fetchone()
                if existing is not None:
                    if existing[18] != snapshot["prompt_fingerprint"]:
                        raise Problem("reservation_replay_conflict", 409)
                    return self._row(existing), False
                totals = self.db.execute(
                    "SELECT COALESCE(SUM(accounted_tokens),0),"
                    " COALESCE(SUM(elapsed_seconds),0.0), COALESCE(SUM(accounted_unknown),0),"
                    " SUM(CASE WHEN state='open' THEN 1 ELSE 0 END),"
                    " SUM(CASE WHEN state='expired' THEN 1 ELSE 0 END),"
                    " COALESCE(SUM(CASE WHEN currency=? THEN accounted_cost ELSE 0 END),0.0)"
                    " FROM reservations WHERE task_id=?",
                    (budget.currency, snapshot["task_id"])).fetchone()
                if totals[2] or totals[4]:
                    raise Problem("budget_settlement_unknown", 409)  # holds block first
                if totals[3] and not self.policy.allow_concurrent_open:
                    raise Problem("budget_reservation_outstanding", 409)
                if totals[0] + snapshot["reserved_tokens"] > budget.token_budget:
                    raise Problem("token_budget_insufficient", 403)
                if totals[1] >= budget.seconds_budget:
                    raise Problem("time_budget_exhausted", 403)
                if budget.enforce_cost:
                    if not snapshot["cost_known"] and self.policy.strict_cost:
                        raise Problem("exact_cost_unsupported", 403)
                    if snapshot["cost_known"] and \
                            totals[5] + (snapshot["reserved_cost"] or 0.0) > budget.cost_budget:
                        raise Problem("cost_budget_insufficient", 403)
                self.db.execute(
                    "INSERT INTO reservations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?,?,?,?)",
                    (key, snapshot["task_id"], snapshot["run_id"], snapshot["execution_id"],
                     snapshot["turn_key"], "open", snapshot["reserved_tokens"],
                     snapshot["reserved_cost"], snapshot["currency"],
                     int(snapshot["cost_known"]), _now_wall(),
                     time.time() + self.policy.lease_seconds, self.boot_nonce,
                     snapshot["reserved_tokens"],
                     snapshot["reserved_cost"] if snapshot["cost_known"] else None,
                     0 if snapshot["cost_known"] else 1,
                     None, snapshot["prompt_fingerprint"], snapshot["frozen_budget"],
                     snapshot["frozen_prices"]))
                row = self.db.execute("SELECT * FROM reservations WHERE key=?", (key,)).fetchone()
        return self._row(row), True

    def reserve(self, task_id, run_id, execution_id, turn_key, serialized_prompt, *,
                monotonic=None):
        """Combined snapshot+publish for local callers."""
        return self.publish(self.prepare(task_id, run_id, execution_id, turn_key,
                                         serialized_prompt))

    def get_reservation(self, key):
        self._require()
        with self.lock:
            row = self.db.execute("SELECT * FROM reservations WHERE key=?", (key,)).fetchone()
        if row is None:
            raise Problem("reservation_not_found", 404)
        return self._row(row)

    @staticmethod
    def _row(row):
        return {"key": row[0], "task_id": row[1], "run_id": row[2], "execution_id": row[3],
                "turn_key": row[4], "state": row[5], "reserved_tokens": row[6],
                "reserved_cost": row[7], "currency": row[8], "cost_known": bool(row[9]),
                "wall_start": row[10], "elapsed_seconds": row[13],
                "accounted_tokens": row[14], "accounted_cost": row[15],
                "accounted_unknown": bool(row[16]),
                "actual": json.loads(row[17]) if row[17] else None,
                "prompt_fingerprint": row[18],
                "budget_fingerprint": digest(json.loads(row[19])),
                "declared_output_bound": TaskBudget(**json.loads(row[19])).output_reserve}

    def _frozen(self, row):
        return TaskBudget(**json.loads(row[19])), \
            PriceSchedule(**json.loads(row[20])) if row[20] else None

    def _sweep_expired_locked(self):
        # Expiry evidence is wall-clock + boot nonce: holds from before a restart are
        # never auto-released; only current-boot reservations past their lease expire.
        self.db.execute(
            "UPDATE reservations SET state='expired' WHERE state='open'"
            " AND boot_nonce=? AND ? > expires_at_wall", (self.boot_nonce, time.time()))

    def sweep_expired(self):
        """Explicit maintenance entry point (never on read/gate paths)."""
        self._require()
        with self.lock, self._write():
            self._sweep_expired_locked()

    def settle(self, key, usage, *, execution_id, elapsed_seconds, trusted=False):
        """Settle against the FROZEN budget and prices. Only the current execution may
        settle. Trusted+complete usage accounts actuals; untrusted or incomplete usage
        keeps the reserved worst case accounted and records an explicit unknown hold.
        ``trusted=True`` on an expired reservation is the authorized operator
        resolution path for unknown holds. Caller-reported elapsed never lowers the
        persisted time (monotonic max)."""
        self._require()
        usage = UsageReport(**(usage if isinstance(usage, dict) else usage.model_dump()))
        if (type(elapsed_seconds) not in (int, float) or isinstance(elapsed_seconds, bool)
                or not math.isfinite(elapsed_seconds) or not 0 <= elapsed_seconds <= 86400):
            raise Problem("invalid_elapsed", 400)
        with self.lock, self._write():
            row = self.db.execute("SELECT * FROM reservations WHERE key=?", (key,)).fetchone()
            if row is None:
                raise Problem("reservation_not_found", 404)
            if row[3] != execution_id:
                raise Problem("stale_execution", 409)
            if row[5] == "settled":
                return self._row(row)  # idempotent: a settled turn never re-settles
            if row[5] == "expired" and not trusted:
                raise Problem("reservation_expired", 409)
            budget, prices = self._frozen(row)
            source = usage.source if trusted else "reported"
            complete = trusted and usage.input_tokens is not None \
                and usage.output_tokens is not None
            if complete and usage.output_tokens > budget.output_reserve:
                raise Problem("output_bound_exceeded", 409)  # runtime broke its hard cap
            actual = {"source": source, "input_tokens": usage.input_tokens,
                      "output_tokens": usage.output_tokens, "cost": None,
                      "currency": budget.currency,
                      "vendor_overhead": usage.vendor_overhead_note or "not_observable"}
            accounted_tokens, accounted_cost, unknown = row[14], row[15], 1
            if not trusted:
                actual["trusted"] = False  # API evidence never refunds reserved worst case
            elif not complete:
                actual["hold"] = "unknown"  # trusted but tokens missing: hold reserved
            elif source == "provider":
                accounted_tokens = usage.input_tokens + usage.output_tokens
                if usage.cost is None:
                    actual["hold"] = "unknown"  # missing provider usage: hold, not zero
                elif usage.currency != budget.currency:
                    raise Problem("currency_mismatch", 409)
                else:
                    actual["cost"], accounted_cost, unknown = usage.cost, usage.cost, 0
            elif source == "price_schedule":
                accounted_tokens = usage.input_tokens + usage.output_tokens
                quote = prices.quote(budget.model_id, usage.input_tokens,
                                     usage.output_tokens) if prices else None
                if quote is None or prices.currency != budget.currency:
                    actual["hold"] = "unknown"
                else:
                    actual["cost"], accounted_cost, unknown = quote, quote, 0
            if not trusted:
                actual["hold"] = "unknown"
            self.db.execute(
                "UPDATE reservations SET state='settled', actual=?, accounted_tokens=?,"
                " accounted_cost=?, accounted_unknown=?,"
                " elapsed_seconds=MAX(elapsed_seconds, ?) WHERE key=?",
                (json.dumps(actual, sort_keys=True), accounted_tokens, accounted_cost,
                 unknown, float(elapsed_seconds), key))
            row = self.db.execute("SELECT * FROM reservations WHERE key=?", (key,)).fetchone()
        return self._row(row)

    def checkpoint(self, key, *, elapsed_turn_seconds, execution_id):
        """Record cumulative caller-reported turn time as a monotonic max (a lower or
        replayed value never reduces persisted time) and gate against the FROZEN budget."""
        self._require()
        if (type(elapsed_turn_seconds) not in (int, float)
                or isinstance(elapsed_turn_seconds, bool)
                or not math.isfinite(elapsed_turn_seconds)
                or not 0 <= elapsed_turn_seconds <= 86400):
            raise Problem("invalid_elapsed", 400)
        with self.lock, self._write():
            row = self.db.execute("SELECT * FROM reservations WHERE key=?", (key,)).fetchone()
            if row is None:
                raise Problem("reservation_not_found", 404)
            if row[3] != execution_id:
                raise Problem("stale_execution", 409)
            if row[5] != "open":
                raise Problem("reservation_not_open", 409)
            budget, _ = self._frozen(row)
            others = self.db.execute(
                "SELECT COALESCE(SUM(elapsed_seconds),0.0) FROM reservations"
                " WHERE task_id=? AND key!=?", (row[1], key)).fetchone()[0]
            total = others + max(row[13], elapsed_turn_seconds)
            self.db.execute(
                "UPDATE reservations SET elapsed_seconds=MAX(elapsed_seconds, ?) WHERE key=?",
                (float(elapsed_turn_seconds), key))
        if total > budget.seconds_budget:
            self.escalate(row[1])
            raise Problem("time_budget_exhausted", 403)
        return {"task_id": row[1], "task_elapsed_seconds": total,
                "seconds_budget": budget.seconds_budget, "admitted": True}

    # -- escalation ---------------------------------------------------------
    def escalate(self, task_id):
        """Cross thresholds BEFORE exhaustion; record durably (bounded), notify
        privately after the durable write, hold at exhaustion."""
        self._require()
        status = self.status(task_id)
        budget = TaskBudget(**status["budget"])
        fractions = {
            "tokens": status["spent_tokens"] / budget.token_budget if budget.token_budget else 1.0,
            "seconds": status["spent_seconds"] / budget.seconds_budget,
            "cost": (status["spent_cost"].get(budget.currency, 0.0) / budget.cost_budget)
            if budget.enforce_cost and budget.cost_budget else (
                1.0 if budget.enforce_cost and status["spent_cost"].get(budget.currency, 0.0) > 0 else 0.0),
        }
        fired = []
        with self.lock, self._write():
            for threshold in budget.escalation_thresholds:
                for kind, fraction in fractions.items():
                    if fraction < threshold:
                        continue
                    count = self.db.execute(
                        "SELECT COUNT(*) FROM escalations WHERE task_id=?", (task_id,)).fetchone()[0]
                    if count >= MAX_ESCALATIONS_PER_TASK:
                        break
                    try:
                        self.db.execute("INSERT INTO escalations(task_id, threshold, kind, at)"
                                        " VALUES(?,?,?,?)", (task_id, threshold, kind, _now_wall()))
                    except sqlite3.IntegrityError:
                        continue  # durable: each (task, threshold, kind) fires once
                    fired.append({"threshold": threshold, "kind": kind})
        for event in fired:  # private-safe: ids and fractions only, never content
            if self.policy.notifier:
                try:
                    self.policy.notifier({"task_id": task_id, "threshold": event["threshold"],
                                          "kind": event["kind"], "version": VERSION})
                except Exception:
                    pass  # notification failure must never drop the durable record
        return {"task_id": task_id, "fired": fired,
                "control_hold": any(f >= 1.0 for f in fractions.values())}

    def list_escalations(self, task_id):
        self._require()
        with self.lock:
            return [dict(zip(("threshold", "kind", "at"), row, strict=True))
                    for row in self.db.execute(
                        "SELECT threshold, kind, at FROM escalations WHERE task_id=?"
                        " ORDER BY id LIMIT 256", (task_id,))]


class RuntimeBudgetGate:
    """The runtime wrapper the parent calls before execute, at checkpoints, after results."""
    def __init__(self, ledger, *, clock=time.monotonic):
        self.ledger, self.clock = ledger, clock

    def precheck(self, task, context, serialized_prompt, capabilities):
        """Reserve BEFORE model invocation. Fail closed unless the worker adapter
        attests a trusted serializer and real output-max enforcement; fail closed on
        an already-bound reservation -- a replayed receipt is not permission to run."""
        if not self.ledger.policy.enabled:
            raise Problem("task_budgets_disabled", 403)
        if (not isinstance(capabilities, GateCapabilities)
                or not capabilities.trusted_serializer or not capabilities.enforces_output_max):
            raise Problem("budget_capability_missing", 403)
        reservation, created = self.ledger.reserve(
            task["id"], context["run_id"], context["execution_id"], context["turn_key"],
            serialized_prompt)
        if not created:
            raise Problem("reservation_already_bound", 409)
        # The freshly reserved turn may use the exact remaining capacity. The
        # atomic publish check already refused oversubscription; threshold
        # notifications must not turn a valid reservation into a dead hold.
        self.ledger.escalate(task["id"])
        return reservation

    def checkpoint(self, reservation, *, elapsed_turn_seconds):
        return self.ledger.checkpoint(reservation["key"],
                                      elapsed_turn_seconds=elapsed_turn_seconds,
                                      execution_id=reservation["execution_id"])

    def after_result(self, reservation, usage, *, elapsed_turn_seconds):
        """Settle with usage from the trusted runtime/provider seam only."""
        return self.ledger.settle(reservation["key"], usage,
                                  execution_id=reservation["execution_id"],
                                  elapsed_seconds=elapsed_turn_seconds, trusted=True)


def budgeted_turn(gate, task, context, serialized_prompt, capabilities, invoke):
    """Worker turn wrapper contract:

    1. precheck reserves durably before the model is invoked (a failed precheck --
       including a replayed/already-bound reservation -- means zero runtime calls);
    2. invoke(serialized_prompt) runs the turn with the declared output bound;
    3. after_result settles with the trusted usage report; on raise the reservation
       stays open as a hold (never auto-refunded, never replayed).
    """
    reservation = gate.precheck(task, context, serialized_prompt, capabilities)
    started = gate.clock()
    try:
        result = invoke(serialized_prompt)
    except BaseException:
        raise  # open hold persists for operator settlement
    usage = getattr(result, "usage", None) or (result.get("usage") if isinstance(result, dict) else None)
    settlement = gate.after_result(reservation, usage or {"source": "reported"},
                                   elapsed_turn_seconds=gate.clock() - started)
    return {"result": result, "reservation": reservation, "settlement": settlement}


def _gate_verdict(ledger, task):
    try:
        snap = ledger.snapshot(task["id"])
    except Problem as problem:
        return {"action": "defer", "reason": problem.code}
    if snap["budget"] is None:
        return {"action": "defer", "reason": "budget_not_configured"}
    if snap["unknown_holds"] or snap["expired_holds"]:
        return {"action": "defer", "reason": "budget_settlement_unknown"}
    if snap["open_holds"] and not ledger.policy.allow_concurrent_open:
        return {"action": "defer", "reason": "budget_reservation_outstanding"}
    budget = TaskBudget(**snap["budget"])
    if budget.token_budget and snap["spent_tokens"] >= budget.token_budget:
        return {"action": "defer", "reason": "token_budget_insufficient"}
    if snap["spent_seconds"] >= budget.seconds_budget:
        return {"action": "defer", "reason": "time_budget_exhausted"}
    cost = snap["spent_cost"].get(budget.currency, 0.0)
    if budget.enforce_cost and (cost > budget.cost_budget or (budget.cost_budget > 0 and cost >= budget.cost_budget)):
        return {"action": "defer", "reason": "cost_budget_insufficient"}
    return {"action": "allow", "reason": "within_budget"}


def budget_dispatch_gate(ledger):
    """Return the ``service.task_dispatch_gates`` callback. Pure bounded canonical
    reads only: SQL aggregates via ``snapshot``; no writes, sweeps, encoding, or I/O."""
    def gate(principal, task, run):
        if not ledger.policy.enabled:
            return None
        return _gate_verdict(ledger, task)
    return gate


def budget_task_start_gate(ledger):
    """Return the ``service.task_start_gates`` callback; same contract as dispatch."""
    return budget_dispatch_gate(ledger)


def budget_dependency_gate(ledger):
    """``service.task_dependency_gates`` adapter: budgets never gate dependencies."""
    def gate(principal, task, dependency):
        return None
    return gate


class ReserveBody(m.Model):
    task_id: m.ID
    run_id: m.ID
    execution_id: str = Field(min_length=1, max_length=256)
    turn_key: str = Field(min_length=1, max_length=256)
    serialized_prompt: str = Field(min_length=1, max_length=MAX_PROMPT_CHARS)


class SettleBody(m.Model):
    key: str = Field(min_length=64, max_length=64, pattern="^[0-9a-f]{64}$")
    execution_id: str = Field(min_length=1, max_length=256)
    elapsed_seconds: float = Field(ge=0, le=86400, allow_inf_nan=False)
    usage: UsageReport


class BudgetCheckpoint(m.Model):
    key: str = Field(min_length=64, max_length=64, pattern="^[0-9a-f]{64}$")
    execution_id: str = Field(min_length=1, max_length=256)
    elapsed_turn_seconds: float = Field(ge=0, le=86400, allow_inf_nan=False)


MODELS = ("TaskBudget", "UsageReport", "PriceSchedule", "ReserveBody", "SettleBody",
          "BudgetCheckpoint")

OPERATION_DESCRIPTORS = [
    {"operation": "budgets.set", "path": "/v1/projects/{project_id}/budgets",
     "method": "POST", "authority": "project_admin", "idempotent": True},
    {"operation": "budgets.status", "path": "/v1/projects/{project_id}/budgets/{task_id}",
     "method": "GET", "authority": "project_member", "idempotent": True},
    {"operation": "budgets.reserve", "path": "/v1/projects/{project_id}/budgets:reserve",
     "method": "POST", "authority": "agent_current_execution", "idempotent": True},
    {"operation": "budgets.settle", "path": "/v1/projects/{project_id}/budgets:settle",
     "method": "POST", "authority": "agent_current_execution", "idempotent": True},
    {"operation": "budgets.checkpoint", "path": "/v1/projects/{project_id}/budgets:checkpoint",
     "method": "POST", "authority": "agent_current_execution", "idempotent": True},
    {"operation": "budgets.escalations",
     "path": "/v1/projects/{project_id}/budgets/{task_id}/escalations",
     "method": "GET", "authority": "project_member", "idempotent": True},
]


def describe_operations():
    return {"version": VERSION, "descriptors": [dict(op) for op in OPERATION_DESCRIPTORS]}


def install(app, service, principal, command, envelope, *, policy=None, counters=None,
            prices=None):
    """Canonical /v1 budget surface. A disabled policy publishes NO routes (the module
    state is still installed so the integration layer can introspect it). Counting is
    snapshotted off all locks before the canonical command transaction; the command
    callback only revalidates and publishes the durable intent."""
    from .capabilities import replace_routes
    from .agent_feature_gates import set_feature_gates
    replace_routes(app, __name__)
    previous = getattr(app.state, "task_budgets", None)
    if previous is not None:
        previous.close()
    ledger = BudgetLedger(policy or BudgetPolicy(), counters=counters, prices=prices)
    app.state.task_budgets = ledger
    service.task_budgets = ledger
    if not ledger.policy.enabled:
        if "task_budgets" in getattr(service, "_agent_feature_gates", {}):
            set_feature_gates(service, "task_budgets")
        return {"endpoints": [], "models": list(MODELS)}
    set_feature_gates(service, "task_budgets", dispatch=budget_dispatch_gate(ledger),
                      start=budget_task_start_gate(ledger))
    endpoints = []

    def visible_task(p, project_id, task_id):
        with service.store.lock:
            current = service.store.db.execute(
                "SELECT * FROM principals WHERE id=?", (p["id"],)).fetchone()
            if current is None or any(current[key] != p[key]
                                      for key in ("role", "project", "agent", "token_hash")):
                raise Problem("authentication_required", 401)
            service.project_access(p, project_id)
            task = service.store.get(task_id, p, "task")
        if task.get("project_id") != project_id:
            raise Problem("not_found", 404)
        return task

    def current_execution(p, project_id, task_id, run_id, execution_id, *,
                          invoking=False, allow_archived=False):
        task = visible_task(p, project_id, task_id)
        operator_resolution = allow_archived and p["role"] in ("admin", "human")
        run = (service.reference(p, project_id, run_id, "run") if operator_resolution
               else service.run(p, run_id))
        if run["project_id"] != project_id or (
                run["status"] in TERMINAL_RUN_STATUSES and not operator_resolution):
            raise Problem("terminal_run", 409)
        execution = task.get("execution")
        if operator_resolution and (not execution or execution["id"] != execution_id):
            execution = next((item for item in task.get("execution_history") or []
                              if item.get("id") == execution_id), None)
        if (not execution or execution["run_id"] != run["id"] or execution["id"] != execution_id
                or execution.get("agent_id") != run["agent_id"]):
            raise Problem("stale_task_execution", 409)
        if invoking:
            if (run["status"] not in ("running", "idle") or task["status"] != "running"
                    or run.get("task_id") != task_id):
                raise Problem("stale_task_execution", 409)
            service.task_permit(p, run_id, task)
        return task

    @app.post("/v1/projects/{project_id}/budgets")
    async def set_budget(project_id: str, body: TaskBudget, p=Depends(principal),
                         idempotency_key: str | None = Header(None)):
        with service.store.lock:
            service.project_access(p, project_id)
            if p["role"] not in ("admin", "human"):
                raise Problem("administrator_required", 403)
        if body.project_id != project_id:
            raise Problem("project_mismatch", 400)

        def bind():
            visible_task(p, project_id, body.task_id)  # reauthorize inside the receipt
            return ledger.set_budget(body)
        return command(p, "task-budgets:set:" + body.task_id, idempotency_key, body, bind)
    endpoints.append("/v1/projects/{project_id}/budgets")

    @app.get("/v1/projects/{project_id}/budgets/{task_id}")
    async def budget_status(project_id: str, task_id: str, p=Depends(principal)):
        visible_task(p, project_id, task_id)
        return envelope(ledger.status(task_id))
    endpoints.append("/v1/projects/{project_id}/budgets/{task_id}")

    @app.post("/v1/projects/{project_id}/budgets:reserve")
    async def reserve(project_id: str, body: ReserveBody, request: Request, p=Depends(principal),
                      idempotency_key: str | None = Header(None)):
        with service.store.lock:
            current_execution(p, project_id, body.task_id, body.run_id, body.execution_id,
                              invoking=True)
        # Exact counting happens off the canonical lock/transaction; replay only
        # repeats a pure local count, never a model inference.
        run_detached = getattr(app.state, "run_detached", None)
        args = (body.task_id, body.run_id, body.execution_id, body.turn_key, body.serialized_prompt)
        if run_detached is None:
            import asyncio
            snapshot = await asyncio.to_thread(ledger.prepare, *args)
        else:
            snapshot = await run_detached(ledger.prepare, *args)
        import inspect
        fresh = principal(request)
        if inspect.isawaitable(fresh):
            fresh = await fresh
        if fresh["id"] != p["id"]:
            raise Problem("authentication_required", 401)
        p = fresh

        def bind():
            current_execution(p, project_id, body.task_id, body.run_id, body.execution_id,
                              invoking=True)
            record, _ = ledger.publish(snapshot)
            return record
        # Recompute admission per request instead of replaying a cached permission.
        # A lost response leaves a hold; a later request can obtain its receipt
        # but cannot invoke another turn.
        with service.store.lock:
            current_execution(p, project_id, body.task_id, body.run_id, body.execution_id,
                              invoking=True)
            try:
                ledger.get_reservation(reservation_key(
                    body.task_id, body.run_id, body.execution_id, body.turn_key))
                created = False
            except Problem as error:
                if error.code != "reservation_not_found":
                    raise
                created = True
            result = command(p, "task-budgets:reserve:" + body.task_id, idempotency_key, body, bind)
        await escalate_detached(body.task_id)
        fresh = principal(request)
        if inspect.isawaitable(fresh):
            fresh = await fresh
        if fresh["id"] != p["id"]:
            raise Problem("authentication_required", 401)
        with service.store.lock:
            current_execution(fresh, project_id, body.task_id, body.run_id, body.execution_id,
                              invoking=True)
        return envelope({**result["data"], "created": created})
    reserve.hivemind_detached_work = True
    endpoints.append("/v1/projects/{project_id}/budgets:reserve")

    @app.post("/v1/projects/{project_id}/budgets:settle")
    async def settle(project_id: str, body: SettleBody, p=Depends(principal),
                     idempotency_key: str | None = Header(None)):
        existing = ledger.get_reservation(body.key)  # authorize the bound identities
        current_execution(p, project_id, existing["task_id"], existing["run_id"],
                          body.execution_id, allow_archived=True)

        def finish():
            current_execution(p, project_id, existing["task_id"], existing["run_id"],
                              body.execution_id, allow_archived=True)
            # API-submitted usage is evidence, never provider authority; only an
            # authorized admin/human operator may attest provider-sourced usage.
            trusted = p["role"] in ("admin", "human")
            return ledger.settle(body.key, body.usage, execution_id=body.execution_id,
                                 elapsed_seconds=body.elapsed_seconds, trusted=trusted)
        with service.store.lock:
            result = command(p, "task-budgets:settle:" + body.key, idempotency_key, body, finish)
        escalation = await escalate_detached(existing["task_id"])
        return envelope({**result["data"], "control_hold": escalation["control_hold"]})
    settle.hivemind_detached_work = True
    endpoints.append("/v1/projects/{project_id}/budgets:settle")

    @app.post("/v1/projects/{project_id}/budgets:checkpoint")
    async def budget_checkpoint(project_id: str, body: BudgetCheckpoint, p=Depends(principal),
                                idempotency_key: str | None = Header(None)):
        existing = ledger.get_reservation(body.key)
        current_execution(p, project_id, existing["task_id"], existing["run_id"], body.execution_id)

        def record():
            current_execution(p, project_id, existing["task_id"], existing["run_id"], body.execution_id)
            return ledger.checkpoint(body.key, execution_id=body.execution_id,
                                     elapsed_turn_seconds=body.elapsed_turn_seconds)
        try:
            with service.store.lock:
                result = command(p, "task-budgets:checkpoint:" + body.key, idempotency_key, body, record)
        finally:
            await escalate_detached(existing["task_id"])
        return result
    budget_checkpoint.hivemind_detached_work = True
    endpoints.append("/v1/projects/{project_id}/budgets:checkpoint")

    async def escalate_detached(task_id):
        run_detached = getattr(app.state, "run_detached", None)
        if run_detached is None:
            import asyncio
            return await asyncio.to_thread(ledger.escalate, task_id)
        return await run_detached(ledger.escalate, task_id)

    @app.get("/v1/projects/{project_id}/budgets/{task_id}/escalations")
    async def escalations(project_id: str, task_id: str, p=Depends(principal)):
        visible_task(p, project_id, task_id)
        return envelope(ledger.list_escalations(task_id))
    endpoints.append("/v1/projects/{project_id}/budgets/{task_id}/escalations")
    return {"endpoints": endpoints, "models": list(MODELS), "ledger": ledger}
