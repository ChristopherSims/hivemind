import time
import httpx
import json

from hivemind.agent_worker import AgentWorker
from test_agent_worker import AGENT, PROJECT, RUN, FakeClient, make_claim, make_task
from hivemind.sdk import Client
from hivemind.memory_pipeline import PipelineConfig
from hivemind.context_assembly import default_counter
from hivemind.storage import canonical, uid
from test_memory_lifecycle import memory


def connection(world, token):
    def forward(request):
        response = world.client.request(request.method, request.url.raw_path.decode(),
                                        content=request.content, headers=request.headers)
        return httpx.Response(response.status_code, content=response.content, headers=response.headers)
    return Client("http://testserver", token, transport=httpx.MockTransport(forward))


def test_execution_maintains_heartbeat_while_runtime_is_busy():
    client = FakeClient(tasks=[make_task()], claims=[make_claim()])

    class Runtime:
        def execute(self, task, context, checkpoint):
            time.sleep(0.25)
            return {"status": "completed", "summary": "Bounded local runtime finished"}

    worker = AgentWorker(client, PROJECT, AGENT, RUN, Runtime(), enabled=True, poll_interval=0.01)
    assert worker.tick()["status"] == "completed"
    assert client.heartbeats >= 3


def test_pending_control_prevents_message_handler_side_effects():
    client = FakeClient(controls=[{"target_run_id": RUN, "action": "pause"}],
                        inbox=[{"id": "message"}], notices=[{"id": "notice"}])

    class Runtime:
        def handle_message(self, receipt):
            raise AssertionError("Pending control must gate message work")
        def review_notification(self, notice):
            raise AssertionError("Pending control must gate notification work")

    worker = AgentWorker(client, PROJECT, AGENT, RUN, Runtime(), enabled=True)
    assert worker.tick()["control"] == "pause"
    assert not client.delivered and not client.acked


def test_scheduler_worker_context_and_independent_verification(world):
    agent = world.call("POST", "agents", {"project_id": world.project, "display_name": "Local",
                                         "capabilities": ["build"]})
    token, scope = world.credential(agent), world.scope("local/component")
    run = world.run(agent, token)
    plan = world.call("POST", "task-plans", {"project_id": world.project, "title": "Local plan",
        "tasks": [{"key": "build", "title": "Build component", "scope_ids": [scope["id"]],
                   "required_capabilities": ["build"]},
                  {"key": "followup", "title": "Follow up", "depends_on": ["build"]}]})
    executed = []
    class Runtime:
        def execute(self, task, context, checkpoint):
            assert context["packet"]["token_count"] <= context["packet"]["token_budget"]
            executed.append(task["id"])
            checkpoint("Local checkpoint", 50)
            return {"status": "completed", "summary": "Local self-report"}
    with_client = connection(world, token)
    try:
        worker = AgentWorker(with_client, world.project, agent["id"], run["id"], Runtime(),
                             enabled=True, assemble_context=True, submit_verification=True)
        assert worker.tick()["status"] == "completed"
        completed = with_client.task(plan["tasks"][0]["id"])
        assert completed["scheduling"]["state"] == "completed"
        assert completed["scheduling"]["attempts"] == 1
        assert len(completed["execution_history"]) == 1
        verification = world.call("GET", f"verifications?project_id={world.project}", token=token)["verifications"][0]
        assert verification["effective_status"] == "pending"
        assert not verification["independently_verified"]
        assert worker.tick()["status"] == "completed"
        assert executed == [task["id"] for task in plan["tasks"]]
    finally:
        with_client.close()


def test_worker_failure_uses_scheduler_retry_budget(world):
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    plan = world.call("POST", "task-plans", {"project_id": world.project, "title": "Retry",
        "tasks": [{"key": "one", "title": "Bounded failure", "max_attempts": 2}]})
    class Runtime:
        def execute(self, task, context, checkpoint):
            return {"status": "failed", "summary": "Local failure"}
    client = connection(world, token)
    try:
        worker = AgentWorker(client, world.project, agent["id"], run["id"], Runtime(), enabled=True)
        assert worker.tick()["status"] == "queued"
        assert worker.tick()["task_id"] is None
        store = world.app.state.store
        with store.transaction():
            task = json.loads(store.raw(plan["tasks"][0]["id"])["body"])
            task["scheduling"]["retry_after"] = time.time() - 1
            row = store.raw(task["id"])
            store.save("task", row["project"], row["owner"], task, expected=task["revision"])
        assert worker.tick()["status"] == "failed"
        task = client.task(task["id"])
        assert task["scheduling"]["attempts"] == 2 and task["scheduling"]["escalated"]
        assert worker.tick()["task_id"] is None
    finally:
        client.close()


def test_context_endpoint_counts_its_actual_envelope_and_rejects_tiny_budget(world):
    agent = world.agent()
    body = {"project_id": world.project, "agent_selector": {"agent_ids": [agent["id"]]},
            "token_budget": 6000}
    response = world.client.post("/v1/context:assemble", json=body,
                                 headers={"Authorization": "Bearer " + world.owner})
    assert response.status_code == 200
    packet = response.json()
    assert default_counter()(canonical(packet)) == packet["data"]["token_count"]
    world.call("POST", "context:assemble", {**body, "token_budget": 100}, status=422)


def test_repository_drift_filters_legacy_and_compact_context_without_rewriting(world):
    pipeline = world.app.state.memory_pipeline
    pipeline.config = PipelineConfig(enabled=True)
    snapshot = {"repository_id": "repo_local", "branch": "main", "commit": "before"}
    pipeline.snapshot_provider = lambda: dict(snapshot)
    current = memory(world, title="Repository canary", text="Repository canary",
                     validity={"repository_id": "repo_local", "branch": "main",
                               "observed_commit": "before"})
    current = world.call("POST", f"memories/{current['id']}:verify", {"expected_revision": 1})
    agent = world.agent()
    body = {"project_id": world.project, "agent_selector": {"agent_ids": [agent["id"]]},
            "include": ["memory"]}
    for route in ("context:query", "context:assemble"):
        assert current["id"] in {item["resource_id"] for item in world.call("POST", route, body)["items"]}
    snapshot["commit"] = "after"
    for route in ("context:query", "context:assemble"):
        assert current["id"] not in {item["resource_id"] for item in world.call("POST", route, body)["items"]}
    unchanged = world.call("GET", f"memories/{current['id']}")
    assert unchanged["status"] == "verified" and unchanged["revision"] == current["revision"]


def test_worker_renews_delivery_while_local_message_handler_is_busy(world):
    sender, agent = world.agent("Sender"), world.agent("Receiver")
    token = world.credential(agent)
    run = world.run(agent, token)
    message = world.call("POST", "messages", {"project_id": world.project,
        "message_id": uid("message"), "thread_id": uid("thread"),
        "from": sender["id"], "to": agent["id"], "type": "request",
        "content": {"media_type": "text/plain", "text": "Local request"}})
    handled = []
    class Runtime:
        def handle_message(self, receipt):
            handled.append(receipt["id"])
            time.sleep(0.25)
    client = connection(world, token)
    try:
        worker = AgentWorker(client, world.project, agent["id"], run["id"], Runtime(),
                             enabled=True, poll_interval=0.01, renew_message_leases=True)
        assert worker.tick()["messages"] == 1
        assert handled == [message["id"]]
        updated = world.call("GET", f"messages/{message['id']}", token=token)
        assert updated["delivery_status"] == "acked"
        assert updated["attempt_count"] == 1 and updated["revision"] >= message["revision"] + 3
    finally:
        client.close()
