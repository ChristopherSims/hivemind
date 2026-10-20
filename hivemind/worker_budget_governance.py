"""Opt-in worker governance for adapters with an explicit bounded-turn contract.

Stock vendor adapters are not assumed to expose exact serialized prompts or hard
output limits. Unsupported adapters refuse before invocation, never fall back to
an ungoverned execute call.
"""
from dataclasses import dataclass
import threading
import time
from typing import Callable

from .storage import digest


@dataclass(frozen=True)
class PreparedBudgetTurn:
    serialized_prompt: str
    model_id: str
    counter: str
    output_max_tokens: int
    invoke: Callable
    enforces_output_max: bool = False


class WorkerBudgetGovernance:
    def __init__(self, client, project_id, *, clock=time.monotonic):
        self.client, self.project_id, self.clock = client, project_id, clock
        self.lock = threading.RLock()
        self.current = None
        self.held = False
        self.halt_reason = None

    def checkpoint(self):
        """Persist elapsed time at maintenance/checkpoint boundaries."""
        from .agent_worker import WorkerStopped
        with self.lock:
            if self.current is None:
                return
            reservation, started, remaining = self.current
            elapsed = max(0.0, self.clock() - started)
            # Record exhaustion remotely too, leaving a durable hold on failure.
            self.client.request("POST", f"projects/{self.project_id}/budgets:checkpoint", {
                "key": reservation["key"], "execution_id": reservation["execution_id"],
                "elapsed_turn_seconds": elapsed},
                key="budget-check-" + digest([reservation["key"], elapsed]))
            if elapsed >= remaining:
                raise WorkerStopped("time_budget_exhausted")

    def execute(self, runtime, task, context, checkpoint, journal):
        from .agent_worker import WorkerStopped
        durable = getattr(journal, "journal", None)
        if (durable is None or not callable(getattr(durable, "begin_operation", None))
                or not callable(getattr(durable, "has_unknown_effects", None))):
            raise WorkerStopped("budget_requires_durable_runtime_journal")
        prepare = getattr(runtime, "prepare_budgeted_turn", None)
        if not callable(prepare):
            raise WorkerStopped("budget_runtime_unsupported")
        status = self.client.request(
            "GET", f"projects/{self.project_id}/budgets/{task['id']}")
        budget = status["budget"]
        turn = prepare(task, context, output_max_tokens=budget["output_reserve"])
        if (not isinstance(turn, PreparedBudgetTurn) or turn.enforces_output_max is not True
                or not isinstance(turn.serialized_prompt, str) or not turn.serialized_prompt
                or turn.model_id != budget["model_id"] or turn.counter != budget["counter"]
                or type(turn.output_max_tokens) is not int
                or turn.output_max_tokens != budget["output_reserve"]
                or not callable(turn.invoke)):
            raise WorkerStopped("budget_capability_missing")
        turn_key = digest([task["id"], context["execution_id"], context.get("phase_id")])
        reservation = self.client.request(
            "POST", f"projects/{self.project_id}/budgets:reserve", {
                "task_id": task["id"], "run_id": context["run_id"],
                "execution_id": context["execution_id"], "turn_key": turn_key,
                "serialized_prompt": turn.serialized_prompt}, key="budget-turn-" + turn_key)
        if reservation.get("created") is not True:
            raise WorkerStopped("reservation_already_bound")
        if (reservation.get("budget_fingerprint") != digest(budget)
                or reservation.get("declared_output_bound") != turn.output_max_tokens):
            raise WorkerStopped("budget_changed")
        started = self.clock()
        remaining = budget["seconds_budget"] - status["spent_seconds"]
        with self.lock:
            self.current = (reservation, started, remaining)
        try:
            def governed_checkpoint(*args, **kwargs):
                self.checkpoint()
                return checkpoint(*args, **kwargs)

            result = turn.invoke(governed_checkpoint)
            self.checkpoint()
            usage = result.get("usage") if isinstance(result, dict) else getattr(result, "usage", None)
            settlement = self.client.request(
                "POST", f"projects/{self.project_id}/budgets:settle", {
                    "key": reservation["key"], "execution_id": context["execution_id"],
                    "elapsed_seconds": max(0.0, self.clock() - started),
                    "usage": usage or {"source": "reported"}},
                key="budget-settle-" + turn_key)
            self.held = (settlement.get("accounted_unknown") is True
                         or settlement.get("control_hold") is True)
            self.halt_reason = ("budget_settlement_unknown" if settlement.get("accounted_unknown")
                                else "budget_exhausted" if self.held else None)
            return result
        finally:
            # Crashes retain the remote reservation and the runtime journal intent.
            with self.lock:
                self.current = None
