"""Bounded worker restart policy and periodic local reconciliation."""
import threading
import time

from .agent_session import SessionLock
from .recovery import UnknownOutcome
from .vendor_runtimes import atomic_state


class WorkerSupervisor:
    def __init__(self, factory, journal, state_file, *, reconciler=None, enabled=False,
                 max_restarts=3, poll_interval=5, reconcile_interval=30, clock=time.monotonic):
        if not 0 <= max_restarts <= 20 or not .05 <= poll_interval <= 600:
            raise ValueError("Invalid supervisor bounds")
        if not 1 <= reconcile_interval <= 3600:
            raise ValueError("Invalid reconciliation interval")
        self.factory, self.journal, self.state_file = factory, journal, state_file
        self.reconciler, self.enabled = reconciler, enabled
        self.max_restarts, self.poll_interval = max_restarts, poll_interval
        self.reconcile_interval, self.clock = reconcile_interval, clock
        self.stop_event = threading.Event()
        self.worker = None
        self.state = {"state": "disabled", "restarts": 0, "ticks": 0, "reconciliations": 0}

    def stop(self):
        self.stop_event.set()
        if self.worker:
            self.worker.stop("supervisor_shutdown")

    def _persist(self, **fields):
        self.state.update(fields)
        atomic_state(self.state_file, self.state)

    def _safe_start(self):
        self.journal.recover()
        if self.journal.has_unknown_effects():
            raise UnknownOutcome("Unsafe effect needs explicit reconciliation")
        self.worker = self.factory()
        if not self.worker.enabled:
            raise ValueError("Supervisor cannot start a disabled worker")

    def _close(self):
        if self.worker:
            close = getattr(self.worker.runtime, "close", None)
            if callable(close):
                close()
            self.worker = None

    def run(self, iterations=None):
        if not self.enabled:
            return dict(self.state)
        if iterations is not None and iterations < 1:
            raise ValueError("Iterations must be positive")
        with SessionLock(self.state_file):
            next_reconcile = self.clock()
            try:
                while not self.stop_event.is_set():
                    if iterations is not None and self.state["ticks"] >= iterations:
                        break
                    if self.worker is None:
                        self._safe_start()
                        self._persist(state="running")
                    if self.reconciler and self.clock() >= next_reconcile:
                        try:
                            self.reconciler.reconcile()
                            self.state["reconciliations"] += 1
                            self.state.pop("reconciliation_error", None)
                        except Exception as error:
                            self.state["reconciliation_error"] = type(error).__name__
                        next_reconcile = self.clock() + self.reconcile_interval
                    try:
                        report = self.worker.tick()
                        self.state["ticks"] += 1
                        if report.get("stopped") or self.worker.stopped:
                            self._persist(state="stopped", reason=self.worker.halt_reason or "worker_stopped")
                            break  # Controls and unknown effects never cause an automatic restart.
                    except Exception as error:
                        self._close()
                        self.journal.recover()
                        unsafe = self.journal.has_unknown_effects()
                        if unsafe:
                            raise UnknownOutcome("Worker failed with uncertain effects") from None
                        if self.state["restarts"] >= self.max_restarts:
                            self._persist(state="exhausted", error=type(error).__name__)
                            break
                        self.state["restarts"] += 1
                        self._persist(state="restart_wait", error=type(error).__name__)
                        if self.stop_event.wait(min(2 ** self.state["restarts"], 60)):
                            break
                        continue
                    self._persist()
                    if iterations is None or self.state["ticks"] < iterations:
                        self.stop_event.wait(self.poll_interval)
            except UnknownOutcome:
                self._persist(state="blocked_unknown")
            finally:
                self._close()
                if self.state["state"] == "running":
                    self._persist(state="stopped", reason="bounded_run_or_shutdown")
        return dict(self.state)
