import pytest
from types import SimpleNamespace

from hivemind.agent_session import HandlerJournal
from hivemind.recovery import Journal, UnknownOutcome


def test_handler_deduplicates_across_restart_and_unknown_requires_operator(tmp_path):
    path = tmp_path / "agent.sqlite3"
    effects = []
    with Journal(path) as journal:
        handler = HandlerJournal(journal, "project", "agent")
        handler.handle("message", "message_one", lambda: effects.append("one"))
        def uncertain():
            effects.append("two")
            raise RuntimeError("crash after side effect")
        with pytest.raises(UnknownOutcome):
            handler.handle("message", "message_two", uncertain)
    with Journal(path) as journal:
        handler = HandlerJournal(journal, "project", "agent")
        handler.handle("message", "message_one", lambda: effects.append("duplicate"))
        with pytest.raises(UnknownOutcome):
            handler.handle("message", "message_two", lambda: effects.append("duplicate"))
        operation = journal.operations(state="unknown")[0]
        journal.resolve_unknown(operation["operation_id"], "completed", "Inspected the applied change",
                                {"operation_id": operation["operation_id"], "observed": "applied"})
        handler.handle("message", "message_two", lambda: effects.append("duplicate"))
        assert effects == ["one", "two"]


def test_explicit_not_applied_allows_retry_and_old_unknown_is_not_hidden(tmp_path):
    with Journal(tmp_path / "agent.sqlite3") as journal:
        journal.begin_operation("old", "EFFECT", "local", safe_to_retry=False)
        journal.mark_attempt("old")
        journal.fail_operation("old", RuntimeError())
        for index in range(205):
            journal.begin_operation(str(index), "GET", "local")
            journal.complete_operation(str(index), {})
        assert journal.has_unknown_effects()
        with pytest.raises(ValueError):
            journal.resolve_unknown("old", "not_applied", "Inspection", {"operation_id": "different"})
        journal.resolve_unknown("old", "not_applied", "Confirmed no effects", {"operation_id": "old"})
        assert journal.operation("old")["state"] == "pending"
        assert not journal.has_unknown_effects()


def test_completed_check_is_not_repeated_after_canonical_rollback(world):
    from hivemind.verification import TrustedCheck
    from test_task_execution import setup, start
    agent, token, run, scope, task = setup(world)
    task = start(world, task, run, token)
    verification = world.call("POST", "verifications", {
        "task_id": task["id"], "run_id": run["id"], "execution_id": task["execution"]["id"],
        "summary": "Local evidence", "completion_claimed": False}, token)
    store = world.app.state.store
    operator = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    effects = []
    verifier = SimpleNamespace(run=lambda name, paths: (
        effects.append(name) or SimpleNamespace(as_record=lambda: {"name": name, "ok": True, "artifacts": []})))
    request = TrustedCheck(expected_revision=verification["revision"], check_name="unit")
    with pytest.raises(RuntimeError):
        with store.transaction():
            world.app.state.verification.record_check(operator, verification["id"], request, verifier)
            raise RuntimeError("Simulated canonical rollback")
    with store.transaction():
        checked = world.app.state.verification.record_check(operator, verification["id"], request, verifier)
    assert checked["checks"][0]["ok"] and effects == ["unit"]
