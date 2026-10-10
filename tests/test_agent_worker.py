"""Narrow synthetic and in-process tests for the persistent execution worker.

No real network, scheduler service, paid model, or vendor session is used. Most cases
run against a synthetic client/runtime double; two cases run the real service through
the in-process ``world`` fixture to exercise genuine task fencing.
"""

import json
import time
from pathlib import Path

import httpx

from hivemind import agent_worker
from hivemind.agent_worker import AgentWorker
from hivemind.storage import uid

PROJECT = "project-1"
AGENT = "agent-1"
RUN = "run-1"


def http_error(status, code=None):
    request = httpx.Request("GET", "http://worker.test/")
    payload = {"error": {"code": code}} if code else {}
    return httpx.HTTPStatusError(str(code), request=request,
                                 response=httpx.Response(status, json=payload, request=request))


def make_task(task_id="task-1", scope_ids=None, status="queued", **overrides):
    task = {"id": task_id, "project_id": PROJECT, "title": "Implement export",
            "description": "Ship the export module", "scope_ids": ["scope-1"] if scope_ids is None else scope_ids,
            "status": status, "revision": 1, "execution": None}
    task.update(overrides)
    return task


def make_claim(claim_id="claim-1", expires_at=None, scope_ids=None, **overrides):
    claim = {"id": claim_id, "revision": 1, "run_id": RUN, "state": "active", "mode": "exclusive_owner",
             "scope_ids": ["scope-1"] if scope_ids is None else scope_ids,
             "expires_at": time.time() + 600 if expires_at is None else expires_at}
    claim.update(overrides)
    return claim


class FakeClient:
    """Synthetic stand-in for ``hivemind.sdk.Client`` with faithful-enough fencing."""

    def __init__(self, tasks=(), claims=(), controls=(), inbox=(), notices=(), dispatch_enabled=True,
                 checkpoint_error=None):
        self.tasks = {task["id"]: task for task in tasks}
        self.queue = list(self.tasks.values())
        self.claims = list(claims)
        self.controls = list(controls)
        self.inbox = list(inbox)
        self.notices = list(notices)
        self.dispatch_enabled = dispatch_enabled
        self.checkpoint_error = checkpoint_error
        self.run = {"id": RUN, "agent_id": AGENT, "project_id": PROJECT, "task_id": None,
                    "status": "running", "last_seen_at": 0.0}
        self.calls, self.starts, self.checkpoints = [], [], []
        self.heartbeats = self.dispatches = 0
        self.renewed, self.acked, self.delivered = [], [], []

    def request(self, method, path, body=None, key=None):
        self.calls.append((method, path, body, key))
        if path.endswith("tasks:dispatch"):
            self.dispatches += 1
            if not self.dispatch_enabled:
                raise http_error(404, "not_found")
            if not self.queue:
                return {"task": None, "claim": None}
            return {"task": self.queue.pop(0), "claim": self.claims[0] if self.claims else None}
        if path.startswith("work-claims/"):
            claim_id = path[len("work-claims/"):-len(":renew")]
            for claim in self.claims:
                if claim["id"] == claim_id:
                    claim["revision"] += 1
                    claim["expires_at"] = time.time() + 120
                    self.renewed.append(claim_id)
            return {"id": claim_id}
        if path.startswith("work-claims?"):
            return list(self.claims)
        if path.startswith("agents/") and "/controls?" in path:
            return list(self.controls)
        if path.startswith("runs/") and path.endswith("/heartbeat"):
            self.heartbeats += 1
            return {"last_seen_at": time.time()}
        if path == f"runs/{RUN}":
            return dict(self.run)
        raise AssertionError(f"unexpected request {method} {path}")

    def task(self, task_id):
        return self.tasks[task_id]

    def start_task(self, task_id, run_id, expected_revision, recover=False, key=None):
        task = self.tasks[task_id]
        assert task["revision"] == expected_revision, "start must use the current revision"
        if task["status"] in ("completed", "failed"):
            raise http_error(409, "terminal_task")
        prior = task.get("execution")
        task["execution"] = {"id": uid("execution"), "run_id": run_id,
                             "progress": prior.get("progress", 0) if prior else 0,
                             "checkpoint": prior.get("checkpoint") if prior else None}
        task["status"] = "running"
        task["revision"] += 1
        self.run["task_id"] = task_id
        self.starts.append({"task_id": task_id, "recover": recover, "key": key})
        return task

    def checkpoint_task(self, task_id, checkpoint, key=None):
        if self.checkpoint_error is not None:
            raise self.checkpoint_error
        task = self.tasks[task_id]
        execution = task.get("execution") or {}
        if (execution.get("id") != checkpoint["execution_id"]
                or execution.get("run_id") != checkpoint["run_id"]):
            raise http_error(409, "stale_task_execution")
        if task["revision"] != checkpoint["expected_revision"]:
            raise http_error(412, "revision_conflict")
        if checkpoint["progress"] < execution.get("progress", 0):
            raise http_error(422, "invalid_task_progress")
        if checkpoint["status"] == "completed" and checkpoint["progress"] != 100:
            raise http_error(422, "invalid_task_progress")
        self.checkpoints.append(dict(checkpoint))
        execution["progress"] = checkpoint["progress"]
        execution["checkpoint"] = {"id": checkpoint["checkpoint_id"], "summary": checkpoint["summary"]}
        task["status"] = checkpoint["status"]
        task["revision"] += 1
        if checkpoint["status"] in ("completed", "failed"):
            self.run["task_id"] = None
        return task

    def deliver_inbox(self, agent_id, project_id, run_id, handler):
        results = []
        for message in list(self.inbox):
            receipt = {"message_id": message["id"], "delivery_id": uid("delivery")}
            handler(receipt)
            results.append(receipt)
        self.inbox.clear()
        self.delivered.extend(results)
        return results

    def notifications(self, agent_id, project_id):
        return list(self.notices)

    def ack_notification(self, notification_id, key=None):
        self.acked.append(notification_id)
        return {"id": notification_id, "state": "acked"}


class SyntheticRuntime:
    """Host runtime double: records calls and emits optional checkpoint steps."""

    def __init__(self, result=None, steps=()):
        self.result = {"status": "completed", "summary": "Export implemented", "progress": 100} \
            if result is None else result
        self.steps = list(steps)
        self.calls = []
        self.handled_messages = []
        self.handled_notices = []

    def execute(self, task, context, checkpoint):
        self.calls.append({"task": task, "context": context})
        for step in self.steps:
            checkpoint(**step)
        return self.result

    def handle_message(self, receipt):
        self.handled_messages.append(receipt)

    def review_notification(self, notice):
        self.handled_notices.append(notice)


def worker(client, runtime=None, **overrides):
    return AgentWorker(client, PROJECT, AGENT, RUN, runtime, enabled=True, **overrides)


def test_disabled_worker_is_inert_until_opted_in():
    client = FakeClient(tasks=[make_task()])
    runtime = SyntheticRuntime()
    journal = RecordingJournal()
    idle = AgentWorker(client, PROJECT, AGENT, RUN, runtime, enabled=False, journal=journal)
    assert idle.tick()["enabled"] is False
    assert client.calls == [] and client.starts == [] and runtime.calls == []
    assert journal.asked == [] and journal.entries == []
    assert idle.run(iterations=3)["ticks"] == 3


def test_worker_dispatches_starts_checkpoints_and_completes():
    client = FakeClient(tasks=[make_task()], claims=[make_claim()])
    runtime = SyntheticRuntime(steps=[{"summary": "half done", "progress": 40}])
    report = worker(client, runtime).tick()
    assert report["task_id"] == "task-1" and report["status"] == "completed"
    assert client.starts[0]["recover"] is False
    assert [c["progress"] for c in client.checkpoints] == [40, 100]
    assert [c["status"] for c in client.checkpoints] == ["running", "completed"]
    assert client.tasks["task-1"]["status"] == "completed" and client.run["task_id"] is None
    assert runtime.calls[0]["task"]["title"] == "Implement export"
    assert runtime.calls[0]["context"]["execution_id"] == client.tasks["task-1"]["execution"]["id"]
    assert runtime.calls[0]["context"]["should_stop"]() is False


def test_idle_dispatch_without_assignment_and_absent_scheduler_are_safe():
    idle = FakeClient(tasks=[])
    runtime = SyntheticRuntime()
    report = worker(idle, runtime).tick()
    assert report["task_id"] is None and report["heartbeat"] is True
    assert idle.starts == [] and runtime.calls == []
    absent = FakeClient(tasks=[make_task()], dispatch_enabled=False)
    assert worker(absent, SyntheticRuntime()).tick()["task_id"] is None
    assert absent.starts == []


def test_heartbeat_and_near_expiry_claim_renewal():
    claim = make_claim(expires_at=time.time() + 10)
    client = FakeClient(tasks=[], claims=[claim])
    worker(client, SyntheticRuntime(), claim_renew_margin=60).tick()
    assert client.heartbeats == 1 and client.renewed == ["claim-1"]
    assert claim["revision"] == 2
    far = FakeClient(tasks=[], claims=[make_claim(expires_at=time.time() + 600)])
    worker(far, SyntheticRuntime(), claim_renew_margin=60).tick()
    assert far.renewed == []


def test_messages_are_delivered_and_review_notifications_acknowledged():
    client = FakeClient(tasks=[], inbox=[{"id": "msg-1"}], notices=[{"id": "note-1"}])
    runtime = SyntheticRuntime()
    report = worker(client, runtime).tick()
    assert report["messages"] == 1 and report["notifications"] == 1
    assert len(runtime.handled_messages) == 1
    assert runtime.handled_messages[0]["message_id"] == "msg-1"
    assert runtime.handled_messages[0]["delivery_id"] == client.delivered[0]["delivery_id"]
    assert runtime.handled_notices == [{"id": "note-1"}]
    assert client.acked == ["note-1"]


def test_messages_and_notifications_stay_unhandled_without_a_runtime_handler():
    client = FakeClient(tasks=[], inbox=[{"id": "msg-1"}], notices=[{"id": "note-1"}])
    report = worker(client, runtime=None).tick()
    assert report["messages"] == 0 and report["notifications"] == 0
    assert client.acked == [] and client.delivered == [] and client.inbox == [{"id": "msg-1"}]


def test_pending_control_halts_dispatch_without_acking():
    control = {"id": "ctl-1", "action": "pause", "target_run_id": RUN}
    client = FakeClient(tasks=[make_task()], controls=[control])
    runtime = SyntheticRuntime()
    handle = worker(client, runtime)
    report = handle.tick()
    assert report["control"] == "pause"
    assert client.dispatches == 0 and client.starts == [] and runtime.calls == []
    assert handle.halt_reason == "control:pause" and client.acked == []


def test_control_pending_checkpoint_aborts_without_losing_progress():
    client = FakeClient(tasks=[make_task()], claims=[make_claim()],
                        checkpoint_error=http_error(403, "control_pending"))
    report = worker(client, SyntheticRuntime()).tick()
    assert report["status"] == "aborted" and report["error"] == "control_pending"
    assert client.tasks["task-1"]["status"] == "running" and client.checkpoints == []


def test_lost_execution_fence_aborts_the_commit():
    client = FakeClient(tasks=[make_task()], claims=[make_claim()])

    class Hijack:
        def execute(self, task, context, checkpoint):
            client.tasks[task["id"]]["execution"] = {"id": "other-execution", "run_id": "other-run",
                                                     "progress": 0}
            return {"status": "completed", "summary": "ignored", "progress": 100}

    report = worker(client, Hijack()).tick()
    assert report["status"] == "aborted" and client.checkpoints == []


def test_runtime_failure_and_exception_are_reported_not_swallowed():
    failed = FakeClient(tasks=[make_task()], claims=[make_claim()])
    worker(failed, SyntheticRuntime(result={"status": "failed", "summary": "Compile error"})).tick()
    assert failed.checkpoints[-1]["status"] == "failed" and failed.run["task_id"] is None

    class Boom:
        def execute(self, task, context, checkpoint):
            raise RuntimeError("token=do-not-leak")

    crashed = FakeClient(tasks=[make_task()], claims=[make_claim()])
    worker(crashed, Boom()).tick()
    assert crashed.checkpoints[-1]["status"] == "failed"
    assert "do-not-leak" not in json.dumps(crashed.checkpoints)
    assert crashed.checkpoints[-1]["summary"] == "Runtime adapter raised RuntimeError"


def test_cooperative_stop_pauses_and_run_is_bounded():
    client = FakeClient(tasks=[make_task()], claims=[make_claim()])

    class Stopping:
        def __init__(self):
            self.handle = None

        def execute(self, task, context, checkpoint):
            checkpoint("part one", progress=30)
            self.handle.stop("host requested stop")
            checkpoint("part two", progress=60)
            return {"status": "running", "summary": "still running", "progress": 60}

    runtime = Stopping()
    handle = worker(client, runtime)
    runtime.handle = handle
    summary = handle.run(iterations=3)
    assert summary["ticks"] == 1 and handle.stopped is True
    assert [c["progress"] for c in client.checkpoints] == [30, 60, 60]
    assert [c["status"] for c in client.checkpoints] == ["running", "paused", "paused"]
    assert client.tasks["task-1"]["status"] == "paused"
    assert handle.status()["halt_reason"] == "host requested stop"


def test_scoped_task_requires_an_owned_claim_but_empty_scope_does_not():
    unclaimed = FakeClient(tasks=[make_task()])
    runtime = SyntheticRuntime()
    report = worker(unclaimed, runtime).tick()
    assert report["skipped"] == "scope_not_owned" and unclaimed.starts == [] and runtime.calls == []
    empty = FakeClient(tasks=[make_task(scope_ids=[])])
    assert worker(empty, SyntheticRuntime()).tick()["status"] == "completed"


def test_terminal_task_is_not_restarted():
    client = FakeClient(tasks=[make_task(status="completed")], claims=[make_claim()])
    runtime = SyntheticRuntime()
    report = worker(client, runtime).tick()
    assert report["skipped"] == "terminal" and client.starts == [] and runtime.calls == []


class RecordingJournal:
    def __init__(self, hint=None):
        self.entries, self.asked, self.hint = [], [], hint

    def append(self, entry):
        self.entries.append(entry)

    def recover(self, run_id):
        self.asked.append(run_id)
        return self.hint


def test_journal_duck_typing_records_transitions_and_forces_recovery():
    journal = RecordingJournal(hint={"task_id": "task-1", "progress": 40})
    client = FakeClient(tasks=[make_task()], claims=[make_claim()])
    handle = worker(client, SyntheticRuntime(), journal=journal)
    assert journal.asked == [RUN] and handle.resume_hint["progress"] == 40
    handle.tick()
    assert [entry["kind"] for entry in journal.entries] == ["start", "checkpoint", "result"]
    assert all(entry["run_id"] == RUN and entry["project_id"] == PROJECT and "at" in entry
               for entry in journal.entries)
    assert client.starts[0]["recover"] is True


def test_worker_is_standalone_without_a_journal():
    client = FakeClient(tasks=[make_task()], claims=[make_claim()])
    handle = worker(client, SyntheticRuntime(), journal=None)
    assert handle.resume_hint is None
    assert handle.tick()["status"] == "completed"


def test_module_contains_no_dynamic_execution_primitive():
    source = Path(agent_worker.__file__).read_text(encoding="utf-8")
    for token in ("exec(", "eval(", "compile(", "subprocess", "os.system", "pickle.loads"):
        assert token not in source


class LocalClient:
    """Adapts the in-process TestClient to the Client surface the worker uses."""

    def __init__(self, world, token):
        self.world, self.token, self.queue, self.starts = world, token, [], []

    def assign(self, task, claim=None):
        detail = self.world.call("GET", f"tasks/{task['id']}", token=self.token)
        self.queue.append((detail, claim))

    def request(self, method, path, body=None, key=None):
        if path.endswith("tasks:dispatch"):
            return {"task": self.queue[0][0], "claim": self.queue[0][1]} if self.queue \
                else {"task": None, "claim": None}
        return self.world.call(method, path, body, token=self.token, key=key)

    def task(self, task_id):
        return self.world.call("GET", f"tasks/{task_id}", token=self.token)

    def start_task(self, task_id, run_id, expected_revision, recover=False, key=None):
        self.starts.append({"task_id": task_id, "recover": recover})
        return self.world.call("POST", f"tasks/{task_id}:start", {
            "run_id": run_id, "expected_revision": expected_revision, "recover": recover},
            token=self.token, key=key)

    def checkpoint_task(self, task_id, checkpoint, key=None):
        return self.world.call("POST", f"tasks/{task_id}:checkpoint", checkpoint,
                               token=self.token, key=key)

    def notifications(self, agent_id, project_id):
        return self.world.call("GET", f"agents/{agent_id}/notifications?project_id={project_id}",
                               token=self.token)

    def ack_notification(self, notification_id, key=None):
        return self.world.call("POST", f"notifications/{notification_id}:ack", {},
                               token=self.token, key=key)

    def deliver_inbox(self, agent_id, project_id, run_id, handler):
        results = []
        for message in self.world.call("GET", f"agents/{agent_id}/inbox?project_id={project_id}",
                                       token=self.token):
            receipt = self.world.call("POST", f"messages/{message['id']}:claim", {"run_id": run_id},
                                      token=self.token)
            handler(receipt)
            results.append(self.world.call("POST", f"messages/{message['id']}:ack",
                                           {"run_id": run_id, "delivery_id": receipt["delivery_id"]},
                                           token=self.token))
        return results


def real_task_setup(world, name):
    agent = world.agent(name)
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = world.scope(f"worker/{name}", paths=[name])
    world.claim(agent, run, scope, token)
    claim = next(claim for claim in world.call("GET", f"work-claims?project_id={world.project}")
                 if claim["run_id"] == run["id"])
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Implement export",
                                        "scope_ids": [scope["id"]]})
    return agent, token, run, claim, task


def test_worker_completes_a_real_service_task(world):
    agent, token, run, claim, task = real_task_setup(world, "export")
    client = LocalClient(world, token)
    client.assign(task, claim)
    runtime = SyntheticRuntime(steps=[{"summary": "half done", "progress": 40}])
    handle = AgentWorker(client, world.project, agent["id"], run["id"], runtime, enabled=True)
    report = handle.tick()
    assert report["task_id"] == task["id"] and report["status"] == "completed"
    detail = world.call("GET", f"tasks/{task['id']}", token=token)
    assert detail["status"] == "completed" and detail["execution"]["progress"] == 100
    assert world.call("GET", f"runs/{run['id']}", token=token)["task_id"] is None
    assert runtime.calls[0]["context"]["should_stop"]() is False


def test_worker_recovers_the_same_run_after_a_cooperative_stop(world):
    agent, token, run, claim, task = real_task_setup(world, "recover")

    class Stopping:
        def __init__(self):
            self.handle = None

        def execute(self, task, context, checkpoint):
            checkpoint("part one", progress=40)
            self.handle.stop("simulated crash")
            return {"status": "paused", "summary": "stopped", "progress": 40}

    client = LocalClient(world, token)
    client.assign(task, claim)
    runtime = Stopping()
    first = AgentWorker(client, world.project, agent["id"], run["id"], runtime, enabled=True)
    runtime.handle = first
    assert first.tick()["status"] == "paused"
    assert world.call("GET", f"tasks/{task['id']}", token=token)["execution"]["progress"] == 40

    restarted = LocalClient(world, token)
    restarted.assign(world.call("GET", f"tasks/{task['id']}", token=token), claim)
    second = AgentWorker(restarted, world.project, agent["id"], run["id"], SyntheticRuntime(), enabled=True)
    report = second.tick()
    assert report["status"] == "completed" and restarted.starts[0]["recover"] is True
    detail = world.call("GET", f"tasks/{task['id']}", token=token)
    assert detail["status"] == "completed" and detail["execution"]["progress"] == 100
