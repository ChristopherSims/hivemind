from types import SimpleNamespace

from hivemind.recovery import Journal
from hivemind.worker_supervisor import WorkerSupervisor


class Worker:
    enabled, stopped, halt_reason = True, False, None
    runtime = SimpleNamespace(close=lambda: None)
    def tick(self):
        return {"stopped": False}
    def stop(self, reason):
        self.stopped = True


def test_disabled_supervisor_is_inert(tmp_path):
    with Journal(tmp_path / "journal.sqlite") as journal:
        manager = WorkerSupervisor(lambda: (_ for _ in ()).throw(AssertionError()), journal,
                                   tmp_path / "manager.json")
        assert manager.run()["state"] == "disabled"
        assert not (tmp_path / "manager.json").exists()


def test_bounded_supervision_schedules_reconciliation(tmp_path):
    reconciler = SimpleNamespace(reconcile=lambda: None)
    with Journal(tmp_path / "journal.sqlite") as journal:
        manager = WorkerSupervisor(Worker, journal, tmp_path / "manager.json", enabled=True,
                                   poll_interval=.05, reconciler=reconciler)
        result = manager.run(iterations=2)
        assert result["ticks"] == 2 and result["reconciliations"] == 1
        assert result["state"] == "stopped"


def test_unknown_runtime_effect_blocks_factory_before_restart(tmp_path):
    with Journal(tmp_path / "journal.sqlite") as journal:
        journal.begin_operation("effect", "RUNTIME", "execute", {}, safe_to_retry=False)
        journal.mark_attempt("effect")
        manager = WorkerSupervisor(lambda: (_ for _ in ()).throw(AssertionError()), journal,
                                   tmp_path / "manager.json", enabled=True)
        assert manager.run(iterations=1)["state"] == "blocked_unknown"


def test_confirmed_worker_stop_is_not_restarted(tmp_path):
    calls = []
    class Stopped(Worker):
        stopped, halt_reason = True, "control:pause"
    def factory():
        calls.append(1)
        return Stopped()
    with Journal(tmp_path / "journal.sqlite") as journal:
        manager = WorkerSupervisor(factory, journal, tmp_path / "manager.json", enabled=True)
        result = manager.run(iterations=10)
        assert result["reason"] == "control:pause" and calls == [1]
