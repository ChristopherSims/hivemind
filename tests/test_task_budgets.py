"""Local and HTTP tests for per-task budget governance. No paid/host/remote calls."""
import json
import threading
import time

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from hivemind import models as m
from hivemind.api import envelope, hash_token
from hivemind.storage import Problem, canonical, uid
from hivemind.task_budgets import (MODELS, BudgetLedger, BudgetPolicy, ExactLocalCounter,
                                   GateCapabilities, PriceSchedule, RuntimeBudgetGate,
                                   TaskBudget, TokenCounterUnavailable,
                                   budget_dependency_gate, budget_dispatch_gate,
                                   budget_task_start_gate, budgeted_turn,
                                   describe_operations, install, reservation_key)

CAPABLE = GateCapabilities(trusted_serializer=True, enforces_output_max=True)


def policy(tmp_path, **fields):
    fields.setdefault("strict_cost", False)  # token/time tests opt out; cost tests opt in
    return BudgetPolicy(enabled=True, directory=str(tmp_path), **fields)


def budget(task_id=None, **fields):
    fields.setdefault("enforce_cost", fields.get("cost_budget", 0.0) > 0)
    base = {"task_id": task_id or uid("task"), "project_id": uid("project"),
            "model_id": "kimi-k3", "counter": "exact", "token_budget": 1000,
            "seconds_budget": 60.0, "cost_budget": 0.0, "currency": "USD",
            "output_reserve": 100}
    base.update(fields)
    return TaskBudget(**base)


def ledger(tmp_path, counters=None, prices=None, **fields):
    return BudgetLedger(policy(tmp_path, **fields),
                        counters=counters if counters is not None
                        else {"exact": ExactLocalCounter(len)},
                        prices=prices)


def context(turn="t1", execution=None, run=None):
    return {"run_id": run or uid("run"), "execution_id": execution or uid("execution"),
            "turn_key": turn}


def provider_usage(input_tokens, output_tokens, cost=None):
    return {"source": "provider", "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": cost, "currency": "USD" if cost is not None else None}


def test_budgets_are_strict_finite_and_explicitly_bound():
    with pytest.raises(ValueError):
        budget(token_budget=-1)
    with pytest.raises(ValueError):
        budget(token_budget=True)  # strict: bools are not ints
    with pytest.raises(ValueError):
        budget(enforce_cost="false")
    with pytest.raises(ValueError):
        budget(seconds_budget=float("inf"))
    with pytest.raises(ValueError):
        budget(seconds_budget=5)  # strict float, no int coercion
    with pytest.raises(ValueError):
        budget(cost_budget=float("nan"))
    with pytest.raises(ValueError):
        budget(currency="credits")  # only governed units; credits are not dollars
    with pytest.raises(ValueError):
        budget(model_id="")
    with pytest.raises(ValueError):
        budget(escalation_thresholds=(0.5, 0.5))
    with pytest.raises(ValueError):
        budget(escalation_thresholds=(True,))  # no bool coercion
    with pytest.raises(ValueError):
        budget(output_reserve=0)  # zero is an unbounded runtime claim: rejected
    with pytest.raises(ValueError):
        TaskBudget(task_id=uid("task"), project_id=uid("project"), model_id="m",
                   counter="c", token_budget=1, seconds_budget=1.0, cost_budget=0,
                   currency="USD", output_reserve=1, bogus=True)


def test_zero_cost_budget_is_a_real_zero_unless_explicitly_disabled(tmp_path):
    prices = PriceSchedule(currency="USD", prices={
        "kimi-k3": {"input_per_1k": 1.0, "output_per_1k": 1.0}})
    store = ledger(tmp_path, prices=prices, strict_cost=True)
    governed = budget(cost_budget=0.0, enforce_cost=True)
    store.set_budget(governed)
    binding = context()
    with pytest.raises(Problem) as error:
        store.reserve(governed.task_id, binding["run_id"], binding["execution_id"],
                      binding["turn_key"], "hello")
    assert error.value.code == "cost_budget_insufficient"
    assert store.status(governed.task_id)["spent_tokens"] == 0
    store.close()


def test_factory_credits_are_a_distinct_unit_and_never_converted(tmp_path):
    store = ledger(tmp_path)
    task = budget(currency="factory_credits", cost_budget=10.0)
    store.set_budget(task)
    store.reserve(task.task_id, *context().values(), "hello")
    status = store.status(task.task_id)
    assert status["budget"]["currency"] == "factory_credits"
    assert status["spent_cost"] == {}  # no fabricated dollar conversion
    store.close()


def test_precheck_prevents_runtime_call_when_budget_insufficient(tmp_path):
    store = ledger(tmp_path)
    task = budget(token_budget=10, output_reserve=5)
    store.set_budget(task)
    gate = RuntimeBudgetGate(store)
    calls = []
    with pytest.raises(Problem) as error:
        budgeted_turn(gate, {"id": task.task_id}, context(), "x" * 100, CAPABLE,
                      lambda prompt: calls.append(prompt))
    assert error.value.code == "token_budget_insufficient"
    assert calls == []  # a refused precheck means nothing was invoked or counted
    assert store.status(task.task_id)["spent_tokens"] == 0  # nothing fake-counted
    store.close()


def test_reservation_is_cumulative_across_turns(tmp_path):
    """Each new turn is checked against settled actuals PLUS outstanding holds,
    so infinite small turns cannot exceed the task budget."""
    store = ledger(tmp_path)
    task = budget(token_budget=250, output_reserve=100)
    store.set_budget(task)
    ctx1, ctx2, ctx3 = context("t1"), context("t2"), context("t3")
    first, _ = store.reserve(task.task_id, *ctx1.values(), "a" * 30)   # 130 held
    store.settle(first["key"], provider_usage(30, 50, cost=0.0),
                 execution_id=ctx1["execution_id"], elapsed_seconds=1.0, trusted=True)
    second, _ = store.reserve(task.task_id, *ctx2.values(), "b" * 20)   # 80 + 120 = 200
    store.settle(second["key"], provider_usage(20, 80, cost=0.0),
                 execution_id=ctx2["execution_id"], elapsed_seconds=1.0, trusted=True)
    with pytest.raises(Problem) as error:
        store.reserve(task.task_id, *ctx3.values(), "c")    # 100 settled + 101 > 250
    assert error.value.code == "token_budget_insufficient"
    store.close()


def test_unsettled_and_unknown_holds_block_new_turns(tmp_path):
    store = ledger(tmp_path)
    task = budget(cost_budget=1.0)  # no prices: reserved cost unknown
    store.set_budget(task)
    store.reserve(task.task_id, *context().values(), "held")
    with pytest.raises(Problem) as unknown:
        store.reserve(task.task_id, *context(turn="t2").values(), "next")
    assert unknown.value.code == "budget_settlement_unknown"

    known = budget()  # zero cost budget: cost dimension known, still serial by default
    store.set_budget(known)
    store.reserve(known.task_id, *context().values(), "held")
    with pytest.raises(Problem) as outstanding:
        store.reserve(known.task_id, *context(turn="t2").values(), "next")
    assert outstanding.value.code == "budget_reservation_outstanding"

    concurrent = ledger(tmp_path / "cc", allow_concurrent_open=True)  # explicit opt-in
    concurrent.set_budget(budget(task_id=known.task_id))
    concurrent.reserve(known.task_id, *context().values(), "one")
    record, created = concurrent.reserve(known.task_id, *context(turn="t2").values(), "two")
    assert created and record["state"] == "open"
    concurrent.close()
    store.close()


def test_reservation_key_is_unambiguous_and_replay_validates_prompt(tmp_path):
    assert reservation_key("a|b", "c", "d", "e") != reservation_key("a", "b|c", "d", "e")
    store = ledger(tmp_path)
    task = budget()
    store.set_budget(task)
    ctx = context()
    record, created = store.reserve(task.task_id, *ctx.values(), "prompt")
    assert created is True
    again, created = store.reserve(task.task_id, *ctx.values(), "prompt")
    assert created is False and again["key"] == record["key"]
    with pytest.raises(Problem) as error:
        store.reserve(task.task_id, *ctx.values(), "different prompt")
    assert error.value.code == "reservation_replay_conflict"
    store.close()


def test_replayed_receipt_is_not_permission_to_execute(tmp_path):
    store = ledger(tmp_path)
    task = budget()
    store.set_budget(task)
    gate = RuntimeBudgetGate(store)
    ctx = context()
    gate.precheck({"id": task.task_id}, ctx, "prompt", CAPABLE)
    calls = []
    with pytest.raises(Problem) as error:
        budgeted_turn(gate, {"id": task.task_id}, ctx, "prompt", CAPABLE,
                      lambda prompt: calls.append(prompt))
    assert error.value.code == "reservation_already_bound"
    assert calls == []  # open reservation replay: zero runtime calls
    store.close()


def test_settled_retry_never_invokes_again(tmp_path):
    store = ledger(tmp_path)
    task = budget()
    store.set_budget(task)
    gate = RuntimeBudgetGate(store)
    ctx = context()
    outcome = budgeted_turn(gate, {"id": task.task_id}, ctx, "hello", CAPABLE,
                            lambda prompt: {"usage": provider_usage(5, 7, cost=0.0)})
    assert outcome["settlement"]["state"] == "settled"
    calls = []
    with pytest.raises(Problem) as error:
        budgeted_turn(gate, {"id": task.task_id}, ctx, "hello", CAPABLE,
                      lambda prompt: calls.append(prompt))
    assert error.value.code == "reservation_already_bound" and calls == []
    store.close()


def test_crash_restart_double_wrapper_makes_zero_calls(tmp_path):
    task = budget(cost_budget=1.0)  # cost governed: the crashed turn is an unknown hold
    store = ledger(tmp_path)
    store.set_budget(task)
    ctx = context()

    def crash(prompt):
        raise RuntimeError("model exploded")

    with pytest.raises(RuntimeError):
        budgeted_turn(RuntimeBudgetGate(store), {"id": task.task_id}, ctx, "hello",
                      CAPABLE, crash)
    store.close()  # crash between reservation and settlement
    reopened = ledger(tmp_path)
    assert reopened.status(task.task_id)["unknown_holds"] == 1  # durable open hold
    calls = []
    with pytest.raises(Problem) as error:
        budgeted_turn(RuntimeBudgetGate(reopened), {"id": task.task_id}, ctx, "hello",
                      CAPABLE, lambda prompt: calls.append(prompt))
    assert error.value.code == "reservation_already_bound" and calls == []
    with pytest.raises(Problem) as blocked:
        reopened.reserve(task.task_id, *context(turn="t2").values(), "next")
    assert blocked.value.code == "budget_settlement_unknown"  # unknown holds block
    reopened.close()


def test_missing_provider_tokens_hold_reserved_worst_case(tmp_path):
    """Trusted provider usage with missing token counts must NOT refund the exact
    reserved worst case, even on a token-only (zero cost) budget."""
    store = ledger(tmp_path)
    task = budget()  # token-only budget, cost dimension known-zero
    store.set_budget(task)
    ctx = context()
    reservation, _ = store.reserve(task.task_id, *ctx.values(), "abcdef")  # 6 + 100
    settled = store.settle(reservation["key"], provider_usage(None, None, cost=0.0),
                           execution_id=ctx["execution_id"], elapsed_seconds=1.0,
                           trusted=True)
    assert settled["actual"]["hold"] == "unknown"
    status = store.status(task.task_id)
    assert status["spent_tokens"] == 106  # reserved worst case preserved
    assert status["unknown_holds"] == 1
    with pytest.raises(Problem) as blocked:
        store.reserve(task.task_id, *context(turn="t2").values(), "next")
    assert blocked.value.code == "budget_settlement_unknown"
    store.close()


def test_spoofed_zero_usage_cannot_refund_reserved_tokens(tmp_path):
    """Untrusted reported usage claiming zero tokens/cost keeps the reserved worst
    case accounted and leaves an unknown hold."""
    store = ledger(tmp_path)
    task = budget()
    store.set_budget(task)
    ctx = context()
    reservation, _ = store.reserve(task.task_id, *ctx.values(), "abcdef")  # 106
    settled = store.settle(reservation["key"],
                           {"source": "provider", "input_tokens": 0, "output_tokens": 0,
                            "cost": 0.0, "currency": "USD"},
                           execution_id=ctx["execution_id"], elapsed_seconds=1.0,
                           trusted=False)  # API-reported: coerced to untrusted
    assert settled["actual"]["source"] == "reported"
    assert settled["actual"]["trusted"] is False
    status = store.status(task.task_id)
    assert status["spent_tokens"] == 106  # reserved worst case: zero claim cannot refund
    assert status["spent_cost"] == {"USD": 0.0}  # only the known zero reservation
    assert status["unknown_holds"] == 1  # held until authorized resolution
    store.close()


def test_unknown_usage_settles_as_hold_never_zero(tmp_path):
    store = ledger(tmp_path)
    task = budget(cost_budget=1.0)
    store.set_budget(task)
    ctx = context()
    reservation, _ = store.reserve(task.task_id, *ctx.values(), "prompt")
    settled = store.settle(reservation["key"], provider_usage(5, 3, cost=None),
                           execution_id=ctx["execution_id"], elapsed_seconds=1.0,
                           trusted=True)
    assert settled["actual"]["hold"] == "unknown" and settled["actual"]["cost"] is None
    assert store.status(task.task_id)["spent_cost"] == {}  # unknown is never zero
    store.close()


def test_expired_holds_persist_block_and_need_authorized_resolution(tmp_path):
    store = ledger(tmp_path, lease_seconds=10.0)
    task = budget(cost_budget=1.0)
    store.set_budget(task)
    ctx = context()
    reservation, _ = store.reserve(task.task_id, *ctx.values(), "prompt")
    store.db.execute("UPDATE reservations SET expires_at_wall=?", (time.time() - 1,))
    with pytest.raises(Problem) as blocked:
        store.reserve(task.task_id, *context(turn="t2").values(), "next")
    assert blocked.value.code == "budget_settlement_unknown"
    assert store.get_reservation(reservation["key"])["state"] == "expired"
    with pytest.raises(Problem) as error:
        store.settle(reservation["key"], {"source": "reported"},
                     execution_id=ctx["execution_id"], elapsed_seconds=1.0)
    assert error.value.code == "reservation_expired"
    # Explicit authorized (trusted operator) resolution settles the expired hold:
    resolved = store.settle(reservation["key"], provider_usage(6, 2, cost=0.001),
                            execution_id=ctx["execution_id"], elapsed_seconds=1.0,
                            trusted=True)
    assert resolved["state"] == "settled" and resolved["actual"]["cost"] == 0.001
    assert store.status(task.task_id)["unknown_holds"] == 0
    store.close()


def test_restart_never_auto_releases_prior_boot_holds(tmp_path):
    task = budget()
    store = ledger(tmp_path, lease_seconds=10.0)
    store.set_budget(task)
    ctx = context()
    reservation, _ = store.reserve(task.task_id, *ctx.values(), "prompt")
    store.db.execute("UPDATE reservations SET expires_at_wall=?", (time.time() - 1,))
    store.close()
    reopened = ledger(tmp_path, lease_seconds=10.0)  # new boot nonce
    reopened.sweep_expired()  # prior-boot holds are wall evidence, never auto-released
    assert reopened.get_reservation(reservation["key"])["state"] == "open"
    reopened.close()


def test_only_the_current_execution_can_settle_and_settle_is_idempotent(tmp_path):
    store = ledger(tmp_path)
    task = budget()
    store.set_budget(task)
    ctx = context()
    reservation, _ = store.reserve(task.task_id, *ctx.values(), "prompt")
    with pytest.raises(Problem) as error:
        store.settle(reservation["key"], {"source": "reported"},
                     execution_id=uid("execution"), elapsed_seconds=1.0)
    assert error.value.code == "stale_execution"
    usage = provider_usage(2, 2, cost=0.01)
    first = store.settle(reservation["key"], usage, execution_id=ctx["execution_id"],
                         elapsed_seconds=1.0, trusted=True)
    second = store.settle(reservation["key"], usage, execution_id=ctx["execution_id"],
                          elapsed_seconds=1.0, trusted=True)
    assert first["actual"] == second["actual"]
    store.close()


def test_price_schedule_finite_bounded_and_frozen_at_reservation(tmp_path):
    entry = {"input_per_1k": 1.0, "output_per_1k": 2.0}
    assert PriceSchedule(currency="USD", prices={"m": entry}).quote("m", 1000, 1000) == 3.0
    assert PriceSchedule(currency="USD", prices={"m": entry}).quote("other", 1, 1) is None
    for bad in (float("inf"), float("nan"), -1.0, 10**7, True):
        with pytest.raises(ValueError):
            PriceSchedule(currency="USD", prices={"m": {"input_per_1k": bad,
                                                        "output_per_1k": 0.1}})
    prices = PriceSchedule(currency="USD", prices={"kimi-k3": dict(entry)})
    store = ledger(tmp_path, prices=prices, strict_cost=True)
    task = budget(cost_budget=0.5)
    store.set_budget(task)
    ctx = context()
    reservation, _ = store.reserve(task.task_id, *ctx.values(), "ab")
    assert reservation["cost_known"] and reservation["reserved_cost"] == pytest.approx(0.202)
    store.prices = None  # operator schedule vanishes AFTER binding: frozen copy applies
    settled = store.settle(reservation["key"], {"source": "price_schedule",
                                                "input_tokens": 2, "output_tokens": 90},
                           execution_id=ctx["execution_id"], elapsed_seconds=1.0,
                           trusted=True)
    assert settled["actual"]["cost"] == pytest.approx(0.182)
    store.close()


def test_strict_cost_fails_closed_without_authority(tmp_path):
    strict = ledger(tmp_path, strict_cost=True)  # no price schedule: exact cost unsupported
    task = budget(cost_budget=1.0)
    strict.set_budget(task)
    with pytest.raises(Problem) as error:
        strict.reserve(task.task_id, *context().values(), "prompt")
    assert error.value.code == "exact_cost_unsupported"
    strict.close()


def test_unavailable_or_dishonest_counter_fails_closed(tmp_path):
    store = ledger(tmp_path, counters={"exact": ExactLocalCounter(
        lambda prompt: (_ for _ in ()).throw(TokenCounterUnavailable()))})
    task = budget()
    store.set_budget(task)
    with pytest.raises(Problem) as error:
        store.reserve(task.task_id, *context().values(), "prompt")
    assert error.value.code == "token_counter_unavailable"
    store.close()

    with pytest.raises(TokenCounterUnavailable):
        ExactLocalCounter(lambda p: -1).count("prompt")  # dishonest counter rejected

    missing = ledger(tmp_path / "missing", counters={})
    missing.set_budget(task)
    with pytest.raises(Problem):
        missing.reserve(task.task_id, *context().values(), "prompt")
    missing.close()


def test_stale_budget_race_fails_closed_during_counting(tmp_path):
    """A budget replaced between the off-lock count and the write transaction must
    not admit the turn under stale limits: publish revalidates the fingerprint."""
    store = ledger(tmp_path)
    other = ledger(tmp_path)
    task = budget()
    store.set_budget(task)
    swapped = {"done": False}

    def racy_counter(prompt):
        if not swapped["done"]:
            swapped["done"] = True
            other.set_budget(budget(task_id=task.task_id, token_budget=1))
        return len(prompt)

    store.counters["exact"] = ExactLocalCounter(racy_counter)
    with pytest.raises(Problem) as error:
        store.reserve(task.task_id, *context().values(), "prompt")
    assert error.value.code == "budget_changed"
    assert store.status(task.task_id)["spent_tokens"] == 0  # nothing bound
    store.close()
    other.close()


def test_capabilities_are_required_or_the_turn_fails_closed(tmp_path):
    store = ledger(tmp_path)
    task = budget()
    store.set_budget(task)
    gate = RuntimeBudgetGate(store)
    for caps in (None, GateCapabilities(), GateCapabilities(trusted_serializer=True),
                 GateCapabilities(enforces_output_max=True)):
        with pytest.raises(Problem) as error:
            gate.precheck({"id": task.task_id}, context(), "prompt", caps)
        assert error.value.code == "budget_capability_missing"
    with pytest.raises(ValueError):
        GateCapabilities(trusted_serializer="yes")
    store.close()


def test_declared_output_bound_is_enforced_at_settlement(tmp_path):
    store = ledger(tmp_path)
    task = budget(output_reserve=10)
    store.set_budget(task)
    ctx = context()
    reservation, _ = store.reserve(task.task_id, *ctx.values(), "prompt")
    assert reservation["declared_output_bound"] == 10
    with pytest.raises(Problem) as error:
        store.settle(reservation["key"], provider_usage(1, 11, cost=0.0),
                     execution_id=ctx["execution_id"], elapsed_seconds=1.0, trusted=True)
    assert error.value.code == "output_bound_exceeded"  # runtime broke its hard cap
    store.close()


def test_budget_is_frozen_against_live_holds(tmp_path):
    store = ledger(tmp_path)
    task = budget()
    store.set_budget(task)
    store.reserve(task.task_id, *context().values(), "prompt")
    with pytest.raises(Problem) as error:
        store.set_budget(budget(task_id=task.task_id, token_budget=5))
    assert error.value.code == "budget_has_live_holds"
    store.close()


def test_checkpoint_is_monotonic_max_persisted_and_frozen(tmp_path):
    store = ledger(tmp_path)
    task = budget(seconds_budget=10.0)
    store.set_budget(task)
    gate = RuntimeBudgetGate(store)
    ctx = context()
    reservation = gate.precheck({"id": task.task_id}, ctx, "prompt", CAPABLE)
    assert gate.checkpoint(reservation, elapsed_turn_seconds=4.0)["admitted"] is True
    # A lower caller-reported value never reduces persisted time:
    assert gate.checkpoint(reservation, elapsed_turn_seconds=1.0)["task_elapsed_seconds"] == 4.0
    assert gate.checkpoint(reservation, elapsed_turn_seconds=8.0)["task_elapsed_seconds"] == 8.0
    with pytest.raises(Problem) as error:
        gate.checkpoint(reservation, elapsed_turn_seconds=12.0)
    assert error.value.code == "time_budget_exhausted"
    store.close()
    reopened = ledger(tmp_path)  # elapsed survives restart: no time is lost
    assert reopened.get_reservation(reservation["key"])["elapsed_seconds"] == 12.0
    fresh = context(turn="t2")
    with pytest.raises(Problem) as exhausted:
        reopened.reserve(task.task_id, *fresh.values(), "next")
    assert exhausted.value.code in ("time_budget_exhausted", "budget_reservation_outstanding")
    reopened.close()


def test_escalations_fire_once_notify_privately_and_hold_at_exhaustion(tmp_path):
    events = []
    store = ledger(tmp_path, notifier=events.append)
    task = budget(token_budget=100, output_reserve=1, escalation_thresholds=(0.5, 1.0))
    store.set_budget(task)
    ctx = context()
    reservation, _ = store.reserve(task.task_id, *ctx.values(), "x" * 59)  # 60 tokens
    store.settle(reservation["key"], provider_usage(60, 0, cost=0.0),
                 execution_id=ctx["execution_id"], elapsed_seconds=1.0, trusted=True)
    first = store.escalate(task.task_id)
    assert any(e["threshold"] == 0.5 and e["kind"] == "tokens" for e in first["fired"])
    assert first["control_hold"] is False
    assert store.escalate(task.task_id)["fired"] == []  # each threshold fires once
    assert events and all("x" * 5 not in json.dumps(e) for e in events)  # private-safe

    ctx2 = context(turn="t2")
    second, _ = store.reserve(task.task_id, *ctx2.values(), "y" * 39)  # 100 cumulative
    store.settle(second["key"], provider_usage(40, 0, cost=0.0),
                 execution_id=ctx2["execution_id"], elapsed_seconds=1.0, trusted=True)
    assert store.escalate(task.task_id)["control_hold"] is True
    assert len(store.list_escalations(task.task_id)) <= 64  # bounded
    store.close()


def test_dispatch_and_start_gates_are_pure_bounded_reads(tmp_path):
    store = ledger(tmp_path, allow_concurrent_open=True)
    task = budget()
    gate = budget_dispatch_gate(store)
    assert gate({}, {"id": task.task_id}, {})["reason"] == "budget_not_configured"
    store.set_budget(task)
    assert gate({}, {"id": task.task_id}, {}) == {"action": "allow",
                                                  "reason": "within_budget"}
    assert budget_task_start_gate(store)({}, {"id": task.task_id}, {})["action"] == "allow"
    assert budget_dependency_gate(store)({}, {"id": task.task_id}, {}) is None

    changes = store.db.total_changes  # gates/status must perform zero writes
    for _ in range(3):
        gate({}, {"id": task.task_id}, {})
        store.status(task.task_id)
    assert store.db.total_changes == changes

    governed = budget(cost_budget=1.0)  # unknown hold defers dispatch
    store.set_budget(governed)
    store.reserve(governed.task_id, *context().values(), "held")
    assert gate({}, {"id": governed.task_id}, {})["reason"] == "budget_settlement_unknown"
    disabled = BudgetLedger(BudgetPolicy())
    assert budget_dispatch_gate(disabled)({}, {"id": uid("task")}, {}) is None
    store.close()


def test_status_is_exact_sql_aggregate_over_long_history(tmp_path):
    store = ledger(tmp_path, allow_concurrent_open=True)
    task = budget(token_budget=10**9, output_reserve=1, seconds_budget=86400.0)
    store.set_budget(task)
    for turn in range(2000):
        ctx = context(turn=f"t{turn}")
        record, _ = store.reserve(task.task_id, *ctx.values(), "x")
        store.settle(record["key"], provider_usage(1, 1, cost=0.0),
                     execution_id=ctx["execution_id"], elapsed_seconds=0.5, trusted=True)
    status = store.status(task.task_id)
    assert status["spent_tokens"] == 4000 and status["spent_seconds"] == 1000.0
    assert status["unknown_holds"] == 0 and status["open_holds"] == 0
    store.close()


def test_concurrent_ledger_instances_reserve_atomically(tmp_path):
    """Two ledger instances (as in separate processes) race for one budget; the
    cumulative check + insert is atomic under BEGIN IMMEDIATE, never oversubscribed."""
    task = budget(token_budget=303, output_reserve=100)  # exactly 3 turns of 101 fit
    first = ledger(tmp_path, allow_concurrent_open=True)
    first.set_budget(task)
    second = ledger(tmp_path, allow_concurrent_open=True)
    results, errors = [], []
    barrier = threading.Barrier(8)

    def attempt(store, index):
        barrier.wait(timeout=10)
        try:
            results.append(store.reserve(task.task_id, *context(turn=f"t{index}").values(), "p"))
        except Problem as error:
            errors.append(error.code)

    threads = [threading.Thread(target=attempt, args=(store, i))
               for i, store in enumerate([first, second] * 4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert len(results) == 3 and errors == ["token_budget_insufficient"] * 5
    assert first.status(task.task_id)["spent_tokens"] == 303  # exactly, never over
    first.close()
    second.close()


def test_policy_validation_and_disabled_fail_closed(tmp_path):
    with pytest.raises(ValueError):
        BudgetPolicy(enabled=True)  # enabled requires a private directory
    with pytest.raises(ValueError):
        BudgetPolicy(enabled="yes")
    with pytest.raises(ValueError):
        BudgetPolicy(lease_seconds=float("inf"))
    disabled = BudgetLedger(BudgetPolicy())
    for call in (lambda: disabled.set_budget(budget()),
                 lambda: disabled.get_reservation("ab" * 32),
                 lambda: disabled.snapshot(uid("task"))):
        with pytest.raises(Problem) as error:
            call()
        assert error.value.code == "task_budgets_disabled"


def test_ledger_path_rejects_links(tmp_path):
    target = tmp_path / "real"
    target.mkdir()
    link = tmp_path / "linked"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation requires privilege")
    with pytest.raises(ValueError):
        ledger(link)


def test_operation_descriptors_and_models_are_canonical():
    described = describe_operations()
    assert described["version"] == "task-budgets-v3"
    paths = {(op["method"], op["path"]) for op in described["descriptors"]}
    assert ("POST", "/v1/projects/{project_id}/budgets") in paths
    assert ("GET", "/v1/projects/{project_id}/budgets/{task_id}") in paths
    assert ("POST", "/v1/projects/{project_id}/budgets:reserve") in paths
    assert ("POST", "/v1/projects/{project_id}/budgets:settle") in paths
    assert ("GET", "/v1/projects/{project_id}/budgets/{task_id}/escalations") in paths
    assert set(MODELS) == {"TaskBudget", "UsageReport", "PriceSchedule",
                               "ReserveBody", "SettleBody", "BudgetCheckpoint"}


# -- HTTP surface: visibility, authority, idempotency, strict schemas --------------

def put(store, kind, project, owner, body, private=None):
    from hivemind.memory_pipeline import now
    revision = body.get("revision", 1)
    seq = store.tick(project)
    encoded = canonical({**body, "revision": revision, "updated_at": now(),
                         "ingestion_seq": seq})
    with store.transaction():
        store.db.execute(
            "INSERT INTO resources(id,kind,project,owner,private_agent,revision,seq,body)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (body["id"], kind, project, owner, private, revision, seq, encoded))
        store.db.execute("INSERT INTO versions VALUES(?,?,?)", (body["id"], revision, encoded))
    return json.loads(encoded)


@pytest.fixture
def http(world, tmp_path):
    service = world.app.state.service
    store = world.app.state.store
    app = FastAPI()

    @app.exception_handler(Problem)
    async def problem(request: Request, error: Problem):
        return JSONResponse({"error": {"code": error.code}}, status_code=error.status)

    def principal_dep(request: Request):
        auth = request.headers.get("authorization", "")
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hash_token(auth[7:]) if auth.startswith("Bearer ") else "",)
                               ).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    def command(p, operation, key, payload, function):
        body = payload.model_dump(by_alias=True, mode="json") if isinstance(payload, m.Model) \
            else payload
        return envelope(service.mutate(p, operation, key, body, function))

    installed = install(app, service, principal_dep, command, envelope,
                        policy=BudgetPolicy(enabled=True,
                                            directory=str(tmp_path / "budgets"),
                                            strict_cost=False),
                        counters={"exact": ExactLocalCounter(len)})
    return world, TestClient(app, raise_server_exceptions=False), installed["ledger"]


def H(token):  # every distinct mutation needs a fresh idempotency key
    return {"Authorization": "Bearer " + token, "Idempotency-Key": uid("key")}


def task_with_execution(world, project, run_id, agent_id, private=None, execution=None):
    store = world.app.state.store
    owner = store.db.execute("SELECT id FROM principals WHERE project=? LIMIT 1",
                             (project,)).fetchone()[0]
    return put(store, "task", project, owner, {
        "id": uid("task"), "project_id": project, "title": "governed",
        "status": "running", "scope_ids": [],
        "execution": {"id": execution or uid("execution"), "run_id": run_id,
                      "agent_id": agent_id}}, private=private)


def wired(world, private=None):
    store = world.app.state.store
    agent = world.agent()
    token = world.credential(agent=agent)
    run = world.call("POST", "runs", {"project_id": world.project, "agent_id": agent["id"]},
                     token=token)
    task = task_with_execution(world, world.project, run["id"], agent["id"],
                               private=private)
    with store.transaction():
        row = store.raw(run["id"], "run")
        run["task_id"] = task["id"]
        run = store.save("run", world.project, row["owner"], run, expected=run["revision"])
    return agent, token, run, task


def test_http_cross_project_and_private_visibility(http):
    world, client, store = http
    agent, token, run, task = wired(world)
    body = budget(task_id=task["id"], project_id=world.project).model_dump(mode="json")
    assert client.post(f"/v1/projects/{world.project}/budgets", json=body,
                       headers=H(token)).status_code == 403  # agents cannot set budgets
    assert client.post(f"/v1/projects/{world.project}/budgets", json=body,
                       headers=H(world.owner)).status_code == 200
    assert client.get(f"/v1/projects/{world.project}/budgets/{task['id']}",
                      headers=H(world.owner)).status_code == 200

    other_project = uid("project")  # principal project mismatch: not found, not leaked
    assert client.get(f"/v1/projects/{other_project}/budgets/{task['id']}",
                      headers=H(world.owner)).status_code == 404
    foreign = budget(task_id=task["id"], project_id=other_project).model_dump(mode="json")
    assert client.post(f"/v1/projects/{other_project}/budgets", json=foreign,
                       headers=H(world.owner)).status_code == 404  # cannot set cross-project

    private_task = task_with_execution(world, world.project, run["id"], agent["id"],
                                       private=agent["id"])
    other_agent = world.agent("Other")
    other_token = world.credential(agent=other_agent)
    assert client.get(f"/v1/projects/{world.project}/budgets/{private_task['id']}",
                      headers=H(other_token)).status_code == 404
    denied = budget(task_id=private_task["id"], project_id=world.project).model_dump(mode="json")
    assert client.post(f"/v1/projects/{world.project}/budgets", json=denied,
                       headers=H(world.owner)).status_code == 200  # admin may configure
    assert client.get(f"/v1/projects/{world.project}/budgets/{private_task['id']}",
                      headers=H(other_token)).status_code == 404


def test_http_reserve_requires_current_run_and_execution(http):
    world, client, store = http
    agent, token, run, task = wired(world)
    client.post(f"/v1/projects/{world.project}/budgets",
                json=budget(task_id=task["id"], project_id=world.project).model_dump(mode="json"),
                headers=H(world.owner))
    body = {"task_id": task["id"], "run_id": run["id"],
            "execution_id": task["execution"]["id"], "turn_key": "t1",
            "serialized_prompt": "hello"}
    assert client.post(f"/v1/projects/{world.project}/budgets:reserve", json=body,
                       headers=H(token)).status_code == 200
    wrong_run = dict(body, run_id=uid("run"), turn_key="t2")
    assert client.post(f"/v1/projects/{world.project}/budgets:reserve", json=wrong_run,
                       headers=H(token)).status_code == 404  # unknown run
    wrong_execution = dict(body, execution_id=uid("execution"), turn_key="t2")
    response = client.post(f"/v1/projects/{world.project}/budgets:reserve",
                           json=wrong_execution, headers=H(token))
    assert response.json()["error"]["code"] == "stale_task_execution"

    stranger = world.agent("Stranger")  # another agent cannot reserve this run
    stranger_token = world.credential(agent=stranger)
    other = dict(body, turn_key="t3")
    assert client.post(f"/v1/projects/{world.project}/budgets:reserve", json=other,
                       headers=H(stranger_token)).status_code == 403


def test_http_spoofed_provider_usage_and_strict_schemas(http):
    world, client, store = http
    agent, token, run, task = wired(world)
    governed = budget(task_id=task["id"], project_id=world.project, cost_budget=1.0)
    client.post(f"/v1/projects/{world.project}/budgets",
                json=governed.model_dump(mode="json"), headers=H(world.owner))
    reservation = client.post(f"/v1/projects/{world.project}/budgets:reserve",
                              json={"task_id": task["id"], "run_id": run["id"],
                                    "execution_id": task["execution"]["id"],
                                    "turn_key": "t1", "serialized_prompt": "hi"},
                              headers=H(token)).json()["data"]
    spoofed = {"key": reservation["key"], "execution_id": task["execution"]["id"],
               "elapsed_seconds": 1.0,
               "usage": {"source": "provider", "input_tokens": 0, "output_tokens": 0,
                         "cost": 0.0, "currency": "USD"}}
    settled = client.post(f"/v1/projects/{world.project}/budgets:settle", json=spoofed,
                          headers=H(token)).json()["data"]
    assert settled["actual"]["source"] == "reported"  # agent cannot attest provider
    assert settled["actual"]["cost"] is None
    assert settled["accounted_tokens"] == reservation["reserved_tokens"]  # no refund

    ctx2 = client.post(f"/v1/projects/{world.project}/budgets:reserve",
                       json={"task_id": task["id"], "run_id": run["id"],
                             "execution_id": task["execution"]["id"],
                             "turn_key": "t2", "serialized_prompt": "hi"},
                       headers=H(token))
    assert ctx2.json()["error"]["code"] == "budget_settlement_unknown"

    bogus = client.post(f"/v1/projects/{world.project}/budgets:reserve",
                        json={"task_id": task["id"], "run_id": run["id"],
                              "execution_id": task["execution"]["id"], "turn_key": "t3",
                              "serialized_prompt": "hi", "extra": "nope"},
                        headers=H(token))
    assert bogus.status_code == 422  # strict schema: extra fields rejected
    bad_key = client.post(f"/v1/projects/{world.project}/budgets:settle",
                          json={"key": "z" * 64, "execution_id": "e",
                                "elapsed_seconds": 1.0, "usage": {"source": "reported"}},
                          headers=H(token))
    assert bad_key.status_code == 422  # key must be a canonical sha256 hex digest


def test_http_idempotent_reserve_replay_does_not_double_bind(http):
    world, client, store = http
    agent, token, run, task = wired(world)
    client.post(f"/v1/projects/{world.project}/budgets",
                json=budget(task_id=task["id"], project_id=world.project).model_dump(mode="json"),
                headers=H(world.owner))
    headers = {"Authorization": "Bearer " + token, "Idempotency-Key": "reserve-1"}
    body = {"task_id": task["id"], "run_id": run["id"],
            "execution_id": task["execution"]["id"], "turn_key": "t1",
            "serialized_prompt": "hello"}
    first = client.post(f"/v1/projects/{world.project}/budgets:reserve", json=body,
                        headers=headers).json()["data"]
    replay = client.post(f"/v1/projects/{world.project}/budgets:reserve", json=body,
                         headers=headers).json()["data"]
    assert replay["key"] == first["key"]
    assert first["created"] is True
    assert replay["created"] is False
    count = store.db.execute("SELECT COUNT(*) FROM reservations").fetchone()[0]
    assert count == 1  # command receipt replay: no duplicate side-ledger intent


def test_http_blocking_counter_does_not_hold_canonical_locks(http):
    """A slow exact counter runs off the store lock and canonical transaction: other
    requests stay responsive while it computes (no inference is ever triggered)."""
    world, client, store = http
    agent, token, run, task = wired(world)
    client.post(f"/v1/projects/{world.project}/budgets",
                json=budget(task_id=task["id"], project_id=world.project).model_dump(mode="json"),
                headers=H(world.owner))
    entered, release = threading.Event(), threading.Event()

    def slow_counter(prompt):
        entered.set()
        assert release.wait(timeout=10)
        return len(prompt)

    store.counters["exact"] = ExactLocalCounter(slow_counter)
    outcome = {}

    def reserve():
        outcome["response"] = client.post(
            f"/v1/projects/{world.project}/budgets:reserve",
            json={"task_id": task["id"], "run_id": run["id"],
                  "execution_id": task["execution"]["id"], "turn_key": "t1",
                  "serialized_prompt": "hello"}, headers=H(token))

    thread = threading.Thread(target=reserve)
    thread.start()
    assert entered.wait(timeout=10)  # counter is computing
    started = time.monotonic()  # concurrent read must not wait on the counter
    response = client.get(f"/v1/projects/{world.project}/budgets/{task['id']}",
                          headers=H(world.owner))
    assert response.status_code == 200 and time.monotonic() - started < 5
    release.set()
    thread.join(timeout=15)
    assert outcome["response"].status_code == 200


def test_disabled_policy_publishes_no_routes(world, tmp_path):
    service = world.app.state.service
    app = FastAPI()
    result = install(app, service, lambda request: None, None, envelope,
                     policy=BudgetPolicy())
    assert result["endpoints"] == [] and result["models"] == list(MODELS)
    assert service.task_budgets is not None and app.state.task_budgets is not None
    client = TestClient(app, raise_server_exceptions=False)
    assert client.get(f"/v1/projects/{world.project}/budgets/anything").status_code == 404
