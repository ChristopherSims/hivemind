"""Real app, worker, private journal and synthetic bounded-turn integration."""
import json
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from hivemind.agent_session import WorkerJournal
from hivemind.agent_worker import AgentWorker
from hivemind.api import create_app
from hivemind.cli import initialize
from hivemind.recovery import Journal
from hivemind.storage import uid
from hivemind.vendor_runtimes import resume_context
from hivemind.task_budgets import BudgetPolicy, ExactLocalCounter
from hivemind.worker_budget_governance import PreparedBudgetTurn, WorkerBudgetGovernance


class LocalClient:
    def __init__(self, http, token):
        self.http, self.token = http, token

    def request(self, method, path, body=None, key=None):
        response = self.http.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + self.token, "Idempotency-Key": key or uid("key")})
        if response.status_code >= 400:
            failure = httpx.Response(response.status_code, content=response.content,
                                     request=httpx.Request(method, "http://testserver/v1/" + path))
            try:
                failure.raise_for_status()
            except httpx.HTTPStatusError as error:
                error.add_note(response.json().get("error", {}).get("code", "validation_failed"))
                raise
        return response.json()["data"]

    def task(self, task_id):
        return self.request("GET", f"tasks/{task_id}")

    def start_task(self, task_id, run_id, revision, recover=False, key=None):
        return self.request("POST", f"tasks/{task_id}:start", {
            "run_id": run_id, "expected_revision": revision, "recover": recover}, key=key)

    def checkpoint_task(self, task_id, request, key=None):
        return self.request("POST", f"tasks/{task_id}:checkpoint", request, key=key)

    def notifications(self, agent_id, project_id):
        return self.request("GET", f"agents/{agent_id}/notifications?project_id={project_id}")


class BoundedRuntime:
    def __init__(self):
        self.calls = 0

    def execute(self, *args):
        raise AssertionError("Governance must never use the unbounded fallback")

    def prepare_budgeted_turn(self, task, context, *, output_max_tokens):
        def invoke(checkpoint):
            self.calls += 1
            return {"status": "completed", "progress": 100, "summary": "Bounded result",
                    "usage": {"source": "provider", "input_tokens": 5, "output_tokens": 1,
                              "cost": 0.0, "currency": "USD"}}
        return PreparedBudgetTurn("hello", "local-test", "exact", output_max_tokens,
                                  invoke, enforces_output_max=True)


@pytest.fixture
def governed(tmp_path):
    config = json.loads(initialize(tmp_path / "canonical").read_text())
    app = create_app(tmp_path / "canonical", workers=False, capability_options={
        "task_budgets": {"policy": BudgetPolicy(enabled=True, directory=tmp_path / "budgets"),
                        "counters": {"exact": ExactLocalCounter(len)}}})
    with TestClient(app) as http, Journal(tmp_path / "worker-journal.sqlite") as journal:
        client = LocalClient(http, config["token"])
        project = config["project_id"]
        agent = client.request("POST", "agents", {"project_id": project, "display_name": "Budget worker"})
        run = client.request("POST", "runs", {"project_id": project, "agent_id": agent["id"],
                                            "purpose": "task_worker"})
        task = client.request("POST", "tasks", {"project_id": project, "title": "Governed task"})
        client.request("POST", f"projects/{project}/budgets", {
            "project_id": project, "task_id": task["id"], "model_id": "local-test",
            "counter": "exact", "token_budget": 21, "seconds_budget": 30.0,
            "cost_budget": 0.0, "enforce_cost": False, "currency": "USD", "output_reserve": 16})
        transitions = WorkerJournal(journal, project, agent["id"])
        yield client, project, agent["id"], run["id"], task["id"], transitions


def test_real_worker_reserves_then_invokes_then_settles(governed):
    client, project, agent, run, task, journal = governed
    runtime = BoundedRuntime()
    controller = WorkerBudgetGovernance(client, project)
    worker = AgentWorker(client, project, agent, run, runtime, journal=journal,
                         enabled=True, budget_governance=controller, consume_handoffs=True)
    report = worker.tick()
    assert report["status"] == "completed", report
    assert runtime.calls == 1
    status = client.request("GET", f"projects/{project}/budgets/{task}")
    assert status["spent_tokens"] == 6
    assert status["unknown_holds"] == 0
    assert not journal.journal.has_unknown_effects()


def test_server_required_budget_cannot_be_disabled_by_omitting_cli_flag(governed):
    client, project, agent, run, _, journal = governed
    runtime = BoundedRuntime()
    worker = AgentWorker(client, project, agent, run, runtime, journal=journal, enabled=True)
    report = worker.tick()
    assert report["status"] == "completed", report
    assert isinstance(worker.budget_governance, WorkerBudgetGovernance)
    assert runtime.calls == 1


def test_accepted_handoff_reaches_real_governed_recipient_prompt(governed):
    client, project, agent, run, task_id, journal = governed
    client.request("POST", f"projects/{project}/budgets", {
        "project_id": project, "task_id": task_id, "model_id": "local-test",
        "counter": "exact", "token_budget": 10000, "seconds_budget": 30.0,
        "cost_budget": 0.0, "enforce_cost": False, "currency": "USD", "output_reserve": 16})
    recipient = client.request("POST", "agents", {"project_id": project, "display_name": "Recipient"})
    target_run = client.request("POST", "runs", {"project_id": project, "agent_id": recipient["id"],
                                              "purpose": "task_worker"})
    offered = {}

    class FailedProducer(BoundedRuntime):
        def prepare_budgeted_turn(self, task, context, *, output_max_tokens):
            def invoke(checkpoint):
                source = client.task(task_id)
                offered.update(client.request("POST", "structured-handoffs", {
                    "project_id": project, "task_id": task_id,
                    "expected_task_revision": source["revision"], "source_run_id": run,
                    "source_execution_id": source["execution"]["id"],
                    "recipient_agent_id": recipient["id"], "recipient_run_id": target_run["id"],
                    "summary": "Inspect the output contract before retrying.",
                    "unresolved_questions": ["Which output is required?"]}))
                return {"status": "failed", "progress": 25, "summary": "Needs another attempt",
                        "usage": {"source": "provider", "input_tokens": 5, "output_tokens": 1,
                                  "cost": 0.0, "currency": "USD"}}
            return PreparedBudgetTurn("hello", "local-test", "exact", output_max_tokens,
                                      invoke, enforces_output_max=True)

    AgentWorker(client, project, agent, run, FailedProducer(), journal=journal,
                enabled=True).tick()
    client.request("POST", f"structured-handoffs/{offered['id']}:accept", {
        "expected_revision": offered["revision"], "recipient_run_id": target_run["id"]})
    fresh = client.task(task_id)
    started = client.start_task(task_id, target_run["id"], fresh["revision"])

    class Recipient(BoundedRuntime):
        def prepare_budgeted_turn(self, task, context, *, output_max_tokens):
            evidence = resume_context(task, context)["structured_handoff"]
            assert evidence["summary"] == "Inspect the output contract before retrying."
            assert evidence["unresolved_questions"] == ["Which output is required?"]
            assert evidence["trust"] == "untrusted_evidence"
            assert evidence["restore_guarantee"] == "none"
            prompt = json.dumps({"task": task["id"], "evidence": evidence}, sort_keys=True)

            def invoke(checkpoint):
                self.calls += 1
                assert json.loads(prompt)["evidence"]["summary"] == evidence["summary"]
                return {"status": "completed", "progress": 100, "summary": "Bounded result",
                        "usage": {"source": "provider", "input_tokens": len(prompt),
                                  "output_tokens": 1, "cost": 0.0, "currency": "USD"}}
            return PreparedBudgetTurn(prompt, "local-test", "exact", output_max_tokens,
                                      invoke, enforces_output_max=True)

    with Journal(Path(client.http.app.state.store.directory) / "recipient-journal.sqlite") as durable:
        worker = AgentWorker(client, project, recipient["id"], target_run["id"], Recipient(),
                             enabled=True, consume_handoffs=True,
                             journal=WorkerJournal(durable, project, recipient["id"]))
        assert worker._execute(started, already_started=True)["status"] == "completed"


def test_unsupported_runtime_is_never_invoked(governed):
    client, project, agent, run, _, journal = governed

    class UnsupportedRuntime:
        calls = 0

        def execute(self, *args):
            self.calls += 1

    runtime = UnsupportedRuntime()
    worker = AgentWorker(client, project, agent, run, runtime, journal=journal,
                         enabled=True, budget_governance=WorkerBudgetGovernance(client, project))
    report = worker.tick()
    assert report["status"] == "aborted"
    assert report["error"] == "budget_runtime_unsupported"
    assert runtime.calls == 0


def test_governed_worker_requires_private_durable_journal(governed):
    client, project, agent, run, _, _ = governed
    runtime = BoundedRuntime()
    worker = AgentWorker(client, project, agent, run, runtime, enabled=True,
                         budget_governance=WorkerBudgetGovernance(client, project))
    report = worker.tick()
    assert report["error"] == "budget_requires_durable_runtime_journal"
    assert runtime.calls == 0
