import httpx
import pytest

from hivemind.agent_session import JournalClient, WorkerJournal
from hivemind.agent_session import SessionLock
from hivemind.recovery import Journal, UnknownOutcome
from hivemind.sdk import Client
from test_task_execution import setup


def client_for(world, token):
    def forward(request):
        response = world.client.request(request.method, request.url.raw_path.decode(),
                                        content=request.content, headers=dict(request.headers))
        return httpx.Response(response.status_code, headers=dict(response.headers), content=response.content)
    return Client("http://testserver", token, transport=httpx.MockTransport(forward))


def test_lost_task_start_reply_is_replayed_under_the_same_server_key(world, tmp_path):
    _, token, run, _, task = setup(world)
    client = client_for(world, token)
    original = client.request
    lost = [False]
    def interrupted(method, path, body=None, key=None):
        result = original(method, path, body, key=key)
        if path.endswith(":start") and not lost[0]:
            lost[0] = True
            raise httpx.ReadError("Synthetic lost reply")
        return result
    client.request = interrupted
    path = tmp_path / "journal.sqlite"
    try:
        with Journal(path) as journal:
            wrapped = JournalClient(client, journal)
            with pytest.raises(httpx.ReadError):
                wrapped.start_task(task["id"], run["id"], task["revision"], key="stable-start")
        with Journal(path) as journal:
            wrapped = JournalClient(client, journal)
            results = wrapped.resume_commands()
            assert len(results) == 1
            assert results[0]["revision"] == 2
            assert wrapped.task(task["id"])["revision"] == 2
            assert len(journal.operations()) == 1  # Detail reads remain live, never journal cached.
    finally:
        client.close()


def test_uncertain_runtime_effect_blocks_automatic_restart(tmp_path):
    path = tmp_path / "journal.sqlite"
    entry = {"kind": "start", "project_id": "project", "agent_id": "agent", "run_id": "run",
             "task_id": "task", "execution_id": "execution"}
    with Journal(path) as journal:
        WorkerJournal(journal, "project", "agent").append(entry)
    with Journal(path) as journal:
        with pytest.raises(UnknownOutcome):
            WorkerJournal(journal, "project", "agent").recover("new-run")


@pytest.mark.parametrize("route", [
    "verifications/verification_local:check", "memory-pipeline:process", "runtime:reconcile"])
def test_injected_host_effects_are_not_automatically_replayed(tmp_path, route):
    class Interrupted:
        def request(self, *args, **kwargs):
            raise httpx.ReadError("Synthetic lost response")
    path = tmp_path / "journal.sqlite"
    with Journal(path) as journal:
        client = JournalClient(Interrupted(), journal)
        with pytest.raises(httpx.ReadError):
            client.request("POST", route, {"project_id": "project"}, key="stable-host-command")
    with Journal(path) as journal:
        assert not journal.operations()[0]["retryable"]
        assert JournalClient(Interrupted(), journal).resume_commands() == []
        with pytest.raises(UnknownOutcome):
            WorkerJournal(journal, "project", "agent").recover("run")


def test_session_owner_lock_is_released_without_deleting_files(tmp_path):
    path = tmp_path / "journal.sqlite"
    with SessionLock(path):
        with pytest.raises(RuntimeError):
            with SessionLock(path):
                pass
    with SessionLock(path):
        assert (tmp_path / "journal.sqlite.lock").exists()


def test_worker_cli_is_inert_without_explicit_enable(monkeypatch, capsys):
    from hivemind.cli import main
    monkeypatch.setattr("sys.argv", ["hivemind", "worker", "--config", "missing.json",
        "--adapter", "not_installed:factory", "--journal", "unused.sqlite", "--workspace", "."])
    assert main() == 1
    assert "disabled" in capsys.readouterr().err
