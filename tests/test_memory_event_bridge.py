"""Event-driven memory maintenance bridge: canonical commits, watchers, fallback scans.

Owns its own harness: it installs the existing lifecycle automation and the new bridge
with the exact injected contract (`install(app, service, principal, command, envelope)`),
so no shared module changes. Git fixtures are real temporary repositories; no network,
no hooks, no fetches, no paid models.
"""

import contextlib
import itertools
import json
import os
import secrets
import shutil
import subprocess
import threading
import time
from pathlib import Path

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from hivemind import models as m
from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.memory_event_bridge import (OPERATION_DESCRIPTORS, BridgePolicy,
                                          MemoryEventBridge)
from hivemind.memory_event_bridge import install as bridge_install
from hivemind.memory_lifecycle_automation import (STATE_KIND, LifecyclePolicy,
                                                  RepositoryBinding)
from hivemind.memory_lifecycle_automation import install as lifecycle_install
from hivemind.memory_pipeline import repository_identity
from hivemind.storage import Problem, now, uid

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")

GIT_ENV = {"GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.com",
           "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.com",
           "GIT_CONFIG_NOSYSTEM": "1"}


def git(repository, *arguments):
    completed = subprocess.run(["git", "-C", str(repository), *arguments],
                               capture_output=True, text=True,
                               env={**os.environ, **GIT_ENV})
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def make_repository(directory):
    directory.mkdir(parents=True, exist_ok=True)
    git(directory, "init", "-q", "-b", "main")
    (directory / "spec.md").write_text("# Export format\nUTC timestamps everywhere.\n")
    git(directory, "add", ".")
    git(directory, "commit", "-q", "-m", "initial")
    return git(directory, "rev-parse", "HEAD")


def commit_change(repository, name="spec.md", text="changed", message="update"):
    (repository / name).write_text(text)
    git(repository, "add", ".")
    git(repository, "commit", "-q", "-m", message)
    return git(repository, "rev-parse", "HEAD")


def wait_for(predicate, timeout=15.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def make_dependencies(store, service):
    async def principal(request: Request):
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise Problem("authentication_required", 401)
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (hash_token(auth[7:]),)).fetchone()
        if not row:
            raise Problem("authentication_required", 401)
        return dict(row)

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0", "data": data}

    def command(p, operation, key, request, function):
        payload = (request.model_dump(by_alias=True, exclude_none=True)
                   if isinstance(request, m.Model) else request)
        return envelope(service.mutate(p, operation, key, payload, function))

    return principal, command, envelope


class BridgeWorld:
    def __init__(self, directory, *, lifecycle_policy=None, bindings=(),
                 bridge_policy=None, watcher=None):
        config = json.loads(initialize(directory).read_text())
        self.directory = Path(directory)
        self.project, self.owner = config["project_id"], config["token"]
        self.operator_id = config["principal_id"]
        self.app = create_app(directory, workers=False)
        self.store, self.service = self.app.state.store, self.app.state.service
        principal, command, envelope = make_dependencies(self.store, self.service)
        lifecycle = lifecycle_policy or LifecyclePolicy(
            enabled=True, operator_principal_id=self.operator_id)
        self.automation = lifecycle_install(self.app, self.service, principal, command,
                                            envelope, policy=lifecycle, bindings=bindings)
        self.bridge = bridge_install(self.app, self.service, self.automation, principal,
                                     command, envelope,
                                     policy=bridge_policy or BridgePolicy(
                                         enabled=True, heartbeat_seconds=0.05,
                                         fallback_scan_seconds=1.0,
                                         join_timeout_seconds=1.0),
                                     watcher=watcher)
        self.stack = contextlib.ExitStack()
        self.client = self.stack.enter_context(TestClient(self.app))

    def close(self):
        self.bridge.close()
        self.stack.close()

    def reinstall_bridge(self, **options):
        self.bridge.close()
        principal, command, envelope = make_dependencies(self.store, self.service)
        self.bridge = bridge_install(self.app, self.service, self.automation, principal,
                                     command, envelope, **options)
        return self.bridge

    def call(self, method, path, body=None, token=None, key=None, status=200):
        response = self.client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + (token or self.owner),
            "Idempotency-Key": key or uid("key")})
        assert response.status_code == status, response.text
        return response.json().get("data", response.json())

    def agent(self, name="Bridge worker"):
        return self.call("POST", "agents", {"project_id": self.project, "display_name": name,
                                           "supported_actions": [],
                                           "scope_permit_enforcement": False})

    def credential(self, agent=None, role="agent"):
        token = secrets.token_urlsafe(32)
        with self.store.transaction():
            self.store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                                  (uid("principal"), hash_token(token), role, self.project,
                                   agent["id"] if agent else None, 0, 0))
        return token

    def bind(self, workspace, **overrides):
        return self.automation.bind(
            RepositoryBinding(project_id=self.project, workspace=str(workspace), **overrides))

    def board(self):
        return self.call("GET", f"boards?project_id={self.project}")[0]

    def post(self, text="The export uses UTC timestamps", token=None):
        return self.call("POST", f"boards/{self.board()['id']}/posts", {
            "client_post_id": uid("client"), "kind": "finding",
            "content": {"media_type": "text/plain", "text": text}}, token)

    def memory(self, text, title, token=None, **fields):
        source = self.post(text, token)
        body = {"project_id": self.project, "title": title,
                "content": {"media_type": "text/plain", "text": text},
                "evidence_refs": [{"kind": "board_post", "id": source["id"],
                                   "revision": source["revision"]}],
                **fields}
        if "scope_kind" not in body:
            body.update(scope_kind="project", scope_owner_id=self.project)
        return self.call("POST", "memories", body, token)

    def task(self, title="Bridge task"):
        return self.call("POST", "tasks", {"project_id": self.project, "title": title})

    def run(self, agent):
        return self.call("POST", "runs", {"project_id": self.project,
                                          "agent_id": agent["id"]})

    def canonical_commit(self, kind="task"):
        """A real canonical commit through the store transaction/listener path."""
        with self.store.transaction():
            return self.store.save(kind, self.project, self.operator_id,
                                   {"id": uid(kind), "project_id": self.project,
                                    "created_at": "2026-01-01T00:00:00Z", "title": kind})

    def events(self, token=None, **query):
        suffix = "".join(f"&{key}={value}" for key, value in query.items())
        return self.call("GET", f"memory-lifecycle:events?project_id={self.project}{suffix}",
                         token=token)

    def source_changes(self):
        return self.events(type="source_change")["events"]

    def status(self, token=None):
        return self.call("GET", f"memory-events:status?project_id={self.project}",
                         token=token)

    def trigger(self, token=None, **body):
        return self.call("POST", "memory-events:trigger",
                         {"project_id": self.project, **body}, token=token)

    def counts(self, kind):
        return self.store.db.execute(
            "SELECT count(*) AS n FROM resources WHERE kind=? AND deleted=0",
            (kind,)).fetchone()["n"]

    def reviews(self):
        rows = self.store.db.execute(
            "SELECT * FROM resources WHERE kind='memory_bridge_review' AND deleted=0"
            ).fetchall()
        return [json.loads(row["body"]) for row in rows]

    def decisions(self, action=None):
        if action:
            return self.store.db.execute(
                "SELECT * FROM memory_event_bridge_decisions WHERE action=?",
                (action,)).fetchall()
        return self.store.db.execute("SELECT * FROM memory_event_bridge_decisions").fetchall()

    def intent_rows(self, state=None):
        sql = "SELECT * FROM memory_event_bridge_intents"
        if state:
            return self.store.db.execute(sql + " WHERE state=?", (state,)).fetchall()
        return self.store.db.execute(sql).fetchall()


@pytest.fixture
def builder(tmp_path):
    counter, worlds = itertools.count(), []

    def build(**options):
        directory = tmp_path / f"data-{next(counter)}"
        directory.mkdir()
        world = BridgeWorld(directory, **options)
        worlds.append(world)
        return world

    yield build
    for world in worlds:
        world.close()


def canonical_only_policy(**overrides):
    """Bridge policy with the fallback scan effectively off: canonical signals only."""
    return BridgePolicy(enabled=True, heartbeat_seconds=0.05, fallback_scan_seconds=3600,
                        join_timeout_seconds=1.0, **overrides)


def pinned_world(builder, tmp_path, bridge_policy=None, **binding_overrides):
    """World with a real repository binding and a memory pinned to the first commit."""
    repository = tmp_path / "repo"
    first_commit = make_repository(repository)
    world = builder(bridge_policy=bridge_policy or canonical_only_policy())
    key = world.bind(repository, **binding_overrides)
    token = world.credential(world.agent())
    memory = world.memory("The export uses UTC timestamps", "Format", token=token,
                          validity={"repository_id": repository_identity(repository),
                                    "branch": "main", "observed_commit": first_commit})
    world.trigger(binding_key=key)  # durable baseline
    assert wait_for(lambda: world.counts(STATE_KIND) == 1)
    assert world.source_changes() == []
    return world, repository, key, memory, first_commit


def checkpoint_task(world, task, agent, token, summary="progress"):
    """Drive a real task start (once) + checkpoint through the API; returns the task."""
    task = world.call("GET", f"tasks/{task['id']}")
    execution = task.get("execution")
    if not execution:
        run = world.run(agent)
        task = world.call("POST", f"tasks/{task['id']}:start",
                          {"expected_revision": task["revision"], "run_id": run["id"]},
                          token=token)
        execution = task["execution"]
    return world.call("POST", f"tasks/{task['id']}:checkpoint", {
        "expected_revision": task["revision"], "run_id": execution["run_id"],
        "execution_id": execution["id"], "checkpoint_id": uid("checkpoint"),
        "status": "running", "progress": min(90, (execution.get("progress") or 0) + 10),
        "summary": summary}, token=token)



# --------------------------------------------------------------------------- triggers
def test_canonical_task_checkpoint_wakes_targeted_review(builder, tmp_path):
    world, repository, key, memory, first_commit = pinned_world(builder, tmp_path)
    settle(world)
    second_commit = commit_change(repository)
    agent, run = world.agent(), None
    token = world.credential(agent)
    task = world.task()
    run = world.run(agent)
    started = world.call("POST", f"tasks/{task['id']}:start",
                         {"expected_revision": task["revision"], "run_id": run["id"]},
                         token=token)
    world.call("POST", f"tasks/{started['id']}:checkpoint", {
        "expected_revision": started["revision"], "run_id": run["id"],
        "execution_id": started["execution"]["id"], "checkpoint_id": uid("checkpoint"),
        "status": "running", "progress": 50, "summary": "halfway"}, token=token)
    assert wait_for(lambda: any(
        event["from_commit"] == first_commit and event["to_commit"] == second_commit
        for event in world.source_changes()))
    event = next(event for event in world.source_changes()
                 if event["to_commit"] == second_commit)
    assert event["type"] == "source_change"
    assert event["from_commit"] == first_commit and event["to_commit"] == second_commit
    assert [item["memory_id"] for item in event["affected"]] == [memory["id"]]
    assert str(repository) not in json.dumps(event)
    # Repeated drains dedup through the automation's own fingerprints: still one event.
    world.trigger(binding_key=key)
    world.trigger()
    time.sleep(0.3)
    assert len(world.source_changes()) == 1
    status = world.status()
    assert status["worker"] is True and status["watermark"] > 0
    assert status["holds"] == [] and status["last_error"] is None


def settle(world, quiet=0.3):
    """Wait until the bridge has no pending hints and stays quiet."""
    assert wait_for(lambda: world.status()["pending"] == 0)
    time.sleep(quiet)
    assert world.status()["pending"] == 0


def test_rolled_back_commit_is_silent(builder, tmp_path):
    world, repository, key, memory, first_commit = pinned_world(builder, tmp_path)
    settle(world)
    commit_change(repository)
    intents_before = len(world.intent_rows())
    with pytest.raises(RuntimeError):
        with self_transaction(world):
            raise RuntimeError("boom")
    time.sleep(0.5)
    # A rolled-back transaction fires no listener: no hints, no intents, no events,
    # even though the repository genuinely moved (fallback scan is disabled here).
    assert world.source_changes() == []
    assert len(world.intent_rows()) == intents_before
    assert world.store.commit_listener_failures == 0
    assert world.status()["watermark"] >= 0


@contextlib.contextmanager
def self_transaction(world):
    with world.store.transaction():
        world.store.save("task", world.project, world.operator_id,
                         {"id": uid("task"), "project_id": world.project,
                          "created_at": "2026-01-01T00:00:00Z", "title": "rolled back"})
        yield


def test_stopped_and_restarted_bridge_catches_up(builder, tmp_path):
    world, repository, key, memory, first_commit = pinned_world(builder, tmp_path)
    assert world.bridge.close() == {"closed": True, "drained": True}
    assert world.bridge.close() == {"closed": True, "drained": True}  # idempotent
    second_commit = commit_change(repository)
    world.canonical_commit("task")  # listener unsubscribed: no signal at all
    time.sleep(0.3)
    assert world.source_changes() == []
    world.reinstall_bridge(policy=BridgePolicy(enabled=True, heartbeat_seconds=0.05,
                                               fallback_scan_seconds=3600,
                                               join_timeout_seconds=1.0))
    assert wait_for(lambda: len(world.source_changes()) == 1)
    event = world.source_changes()[0]
    assert event["from_commit"] == first_commit and event["to_commit"] == second_commit
    time.sleep(0.3)
    assert len(world.source_changes()) == 1  # catch-up dedups, no double publish


def test_private_sources_never_leak_and_revoked_operator_stops_work(builder, tmp_path):
    repository = tmp_path / "repo"
    first_commit = make_repository(repository)
    world = builder()
    key = world.bind(repository)
    agent = world.agent()
    token = world.credential(agent)
    memory = world.memory("The export uses UTC timestamps", "Format", token=token,
                          scope_kind="agent", scope_owner_id=agent["id"],
                          validity={"repository_id": repository_identity(repository),
                                    "branch": "main", "observed_commit": first_commit})
    world.trigger(binding_key=key)
    assert wait_for(lambda: world.counts(STATE_KIND) == 1)
    settle(world)
    admin = world.credential(role="admin")
    commit_change(repository)
    world.canonical_commit("task")
    assert wait_for(lambda: len(world.source_changes()) == 1)
    # Status is counts/digests only: no memory ids, no workspace paths.
    status_text = json.dumps(world.status())
    assert memory["id"] not in status_text and str(repository) not in status_text
    # Revoke the configured operator: work stops without holds, error is surfaced.
    with world.store.transaction():
        world.store.db.execute("UPDATE principals SET role='agent' WHERE id=?",
                               (world.operator_id,))
    commit_change(repository, text="third")
    world.canonical_commit("artifact")
    time.sleep(0.5)
    assert len(world.events(token=admin)["events"]) == 1  # nothing new after revoke
    assert world.status(token=admin)["last_error"] == "lifecycle_operator_unavailable"
    world.call("POST", "memory-events:trigger", {"project_id": world.project},
               token=world.owner, status=403)


def test_failed_compute_holds_unknown_and_never_replays(builder, tmp_path):
    world, repository, key, memory, first_commit = pinned_world(builder, tmp_path)
    original = world.automation.detect_changes
    state = {"fail": False, "calls": 0}

    def flaky(principal, **kwargs):
        state["calls"] += 1
        if state["fail"]:
            raise Problem("detector_unavailable", 503)
        return original(principal, **kwargs)

    world.automation.detect_changes = flaky
    settle(world)
    commit_change(repository)
    state["fail"] = True
    calls_before = state["calls"]
    world.canonical_commit("task")
    assert wait_for(lambda: world.status()["holds"] != [])
    failing_calls = state["calls"] - calls_before
    assert failing_calls >= 1
    assert world.source_changes() == []
    # Restart: the held trigger is not replayed even across a fresh bridge.
    world.reinstall_bridge(policy=BridgePolicy(enabled=True, heartbeat_seconds=0.05,
                                               fallback_scan_seconds=3600,
                                               join_timeout_seconds=1.0))
    time.sleep(0.4)
    assert state["calls"] - calls_before == failing_calls  # held trigger never replays
    assert world.status()["holds"] != []
    # Operator release: held trigger is skipped, then a fresh scan detects the change.
    state["fail"] = False
    released = world.call("POST", "memory-events:release",
                          {"project_id": world.project})
    assert released["released"] == 1
    world.trigger(binding_key=key)
    assert wait_for(lambda: len(world.source_changes()) == 1)
    assert len(world.source_changes()) == 1


def test_blocked_compute_keeps_close_responsive(builder, tmp_path):
    world = builder(bridge_policy=BridgePolicy(enabled=True, heartbeat_seconds=0.05,
                                               fallback_scan_seconds=3600,
                                               join_timeout_seconds=0.2))
    repository = tmp_path / "repo"
    make_repository(repository)
    world.bind(repository)
    original = world.automation.detect_changes
    entered, release = threading.Event(), threading.Event()

    def blocked(principal, **kwargs):
        entered.set()
        release.wait(10)
        return original(principal, **kwargs)

    world.automation.detect_changes = blocked
    world.canonical_commit("task")
    assert entered.wait(5)
    started = time.monotonic()
    result = world.bridge.close()
    elapsed = time.monotonic() - started
    assert result == {"closed": True, "drained": False}
    assert elapsed < 5
    release.set()
    world.bridge._thread.join(10)


def test_queue_overflow_falls_back_to_watermark_catchup(builder, tmp_path):
    world, repository, key, memory, first_commit = pinned_world(builder, tmp_path)
    settle(world)
    # A standalone, not-yet-started bridge: callbacks record hints deterministically.
    bridge = MemoryEventBridge(world.service, world.automation,
                               policy=BridgePolicy(enabled=True, queue_capacity=8,
                                                   fallback_scan_seconds=3600))
    bridge._on_commit({f"project_fake_{index}": index + 1 for index in range(9)})
    assert bridge._gap is True and bridge._gap_count == 1
    second_commit = commit_change(repository)
    world.canonical_commit("task")  # installed bridge may also process; fingerprints dedup
    bridge._drain()  # gap -> catch-up from durable watermarks for every known project
    bridge.close()
    assert wait_for(lambda: any(event["to_commit"] == second_commit
                                for event in world.source_changes()))


def test_local_repository_watcher_seam_and_bounded_fallback(builder, tmp_path):
    reports = []

    def watcher(bindings):
        result, reports[:] = list(reports), []
        return result

    repository = tmp_path / "repo"
    first_commit = make_repository(repository)
    world = builder(watcher=watcher)
    key = world.bind(repository)
    token = world.credential(world.agent())
    memory = world.memory("The export uses UTC timestamps", "Format", token=token,
                          validity={"repository_id": repository_identity(repository),
                                    "branch": "main", "observed_commit": first_commit})
    world.trigger(binding_key=key)
    assert wait_for(lambda: world.counts(STATE_KIND) == 1)
    settle(world)
    assert world.source_changes() == []
    # External ref change with no canonical commit: trusted watcher hint wakes detection.
    second_commit = commit_change(repository)
    reports.append({"binding_key": key})
    assert wait_for(lambda: any(event["to_commit"] == second_commit
                                for event in world.source_changes()))
    event = next(event for event in world.source_changes()
                 if event["to_commit"] == second_commit)
    assert [item["memory_id"] for item in event["affected"]] == [memory["id"]]
    # Junk watcher input is rejected and counted, never trusted.
    reports.extend([{"binding_key": "binding_missing"}, "junk", {"nope": 1}])
    assert wait_for(lambda: world.bridge._watcher_rejected >= 3)
    # Fallback hash scan is the backstop for missed notifications, not the sole path.
    third_commit = commit_change(repository, text="third")
    world.bridge.fallback_scan()
    assert len(world.source_changes()) == 2
    assert {event["to_commit"] for event in world.source_changes()} == \
           {second_commit, third_commit}
    world.bridge.fallback_scan()  # unchanged refs dedup: no third event
    assert len(world.source_changes()) == 2


# ------------------------------------------------------------------------ surfaces
def test_descriptors_status_and_authorization(builder, tmp_path):
    world = builder()
    descriptors = world.call("GET", "memory-events:descriptors")
    assert descriptors["version"]
    assert [item["name"] for item in descriptors["descriptors"]] == \
           [item["name"] for item in OPERATION_DESCRIPTORS]
    assert {"event_catchup", "repository_scan", "hold_release"} <= \
           {item["name"] for item in descriptors["descriptors"]}
    agent_token = world.credential(world.agent())
    world.call("POST", "memory-events:trigger", {"project_id": world.project},
               token=agent_token, status=403)
    world.call("GET", f"memory-events:status?project_id={world.project}",
               token=agent_token, status=403)
    world.call("GET", "memory-events:descriptors", token=agent_token)  # data-only, authenticated
    status = world.status()
    assert status["enabled"] is True and status["status_effect"] == "review_only"
    assert status["watcher"] is None and status["holds"] == []


def test_disabled_bridge_is_inert(builder, tmp_path):
    world = builder(bridge_policy=BridgePolicy(enabled=False))
    assert world.bridge._thread is None
    world.call("POST", "memory-events:trigger", {"project_id": world.project}, status=409)
    world.call("GET", f"memory-events:status?project_id={world.project}", status=409)
    world.canonical_commit("task")
    time.sleep(0.3)
    assert world.intent_rows() == []


def test_policy_validation():
    with pytest.raises(ValueError):
        BridgePolicy(enabled=True, queue_capacity=1)
    with pytest.raises(ValueError):
        BridgePolicy(enabled=True, heartbeat_seconds=float("inf"))
    with pytest.raises(ValueError):
        BridgePolicy(enabled=True, batch_limit=0)
    with pytest.raises(ValueError):
        BridgePolicy(enabled=True, auto_invalidate_supporting_refs="yes")
    with pytest.raises(ValueError):
        BridgePolicy(enabled=True, support_page_limit=0)


# ------------------------------------------------------- supporting-source maintenance
def test_checkpoint_with_constant_git_head_triggers_real_source_review(builder, tmp_path):
    world, repository, key, _pin_memory, first_commit = pinned_world(builder, tmp_path)
    agent = world.agent()
    token = world.credential(agent)
    task = world.task("Supported task")
    task = checkpoint_task(world, task, agent, token)  # running, revision bumped once
    memory = world.memory("The export uses UTC timestamps", "Format", token=token,
                          evidence_refs=[{"kind": "task", "id": task["id"],
                                          "revision": task["revision"]}])
    settle(world)
    assert world.reviews() == []
    checkpoint_task(world, task, agent, token, summary="moved on")  # revision -> exact drift
    assert wait_for(lambda: len(world.reviews()) == 1)
    review = world.reviews()[0]
    assert review["type"] == "supporting_source_review"
    assert review["reason"] == "supporting_source_revision_changed"
    assert review["memory_id"] == memory["id"] and review["observed"] == "changed"
    assert review["status_effect"] == "review_only" and review["trust"] == "deterministic"
    # The git HEAD never moved: no repository event, no memory rewrite.
    assert world.source_changes() == []
    # GET hides the memory now (pinned evidence revision drifted) — inspect the row.
    current = json.loads(world.store.raw(memory["id"])["body"])
    assert "invalidation_reason" not in current
    # Durable decision dedups: further drains/triggers never double the review.
    world.trigger()
    settle(world)
    assert len(world.reviews()) == 1
    assert world.status()["decisions"].get("review_requested") == 1


def test_supporting_source_invalidation_requires_opt_in_and_preserves_history(
        builder, tmp_path):
    world = builder(bridge_policy=canonical_only_policy(auto_invalidate_supporting_refs=True))
    agent = world.agent()
    token = world.credential(agent)
    task = world.task("Supported task")
    task = checkpoint_task(world, task, agent, token)
    memory = world.memory("The export uses UTC timestamps", "Format", token=token,
                          evidence_refs=[{"kind": "task", "id": task["id"],
                                          "revision": task["revision"]}])
    settle(world)
    checkpoint_task(world, task, agent, token, summary="drifted")

    def memory_body():
        return json.loads(world.store.raw(memory["id"])["body"])  # GET hides invalidated

    assert wait_for(lambda: "invalidation_reason" in memory_body())
    current = memory_body()
    assert current["invalidation_reason"] == "supporting_source_revision_changed"
    assert current["invalidation"]["observed"] == "changed"
    assert "task_" not in json.dumps(current["invalidation"])  # digest only, no ids
    assert world.reviews() == []  # invalidation opted in: no review request
    versions = world.store.db.execute(
        "SELECT count(*) AS n FROM versions WHERE id=?", (memory["id"],)).fetchone()["n"]
    assert versions == 2  # create + invalidation; history preserved, nothing deleted
    assert world.decisions("invalidated") != []


def test_private_memory_private_foreign_source_holds_generically(builder, tmp_path):
    world = builder(bridge_policy=canonical_only_policy())
    agent, other = world.agent("Private worker"), world.agent("Other worker")
    token = world.credential(agent)
    post = world.post(token=token)
    memory = world.memory("The export uses UTC timestamps", "Format", token=token,
                          scope_kind="agent", scope_owner_id=agent["id"],
                          evidence_refs=[{"kind": "board_post", "id": post["id"],
                                          "revision": post["revision"]}])
    settle(world)
    # Post-hoc namespace change: the source becomes private to a *different* agent,
    # then its revision moves (the canonical commit fires the bridge listener).
    with world.store.transaction():
        world.store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                               (other["id"], post["id"]))
    row = world.store.raw(post["id"])
    world.store.save("board_post", world.project, row["owner"],
                     json.loads(row["body"]), expected=row["revision"])
    world.canonical_commit("task")  # bare save fires no listener; this wakes the bridge
    assert wait_for(lambda: len(world.decisions("held_generic")) == 1)
    settle(world)
    assert world.reviews() == []  # no derived cross-namespace signal
    assert world.decisions("review_requested") == []
    status_text = json.dumps(world.status())
    assert memory["id"] not in status_text and post["id"] not in status_text
    assert world.status()["decisions"].get("held_generic") == 1


# ----------------------------------------------------------------- hardened ref scan
def test_ref_component_validation_fails_closed(builder, tmp_path):
    world, repository, key, memory, first_commit = pinned_world(builder, tmp_path)
    settle(world)
    binding = world.automation.bindings[key]
    head = repository / ".git" / "HEAD"
    original = head.read_bytes()
    for hostile in (b"ref: ../outside\n", b"ref: refs/heads/main:stream\n",
                    b"ref: /absolute/ref\n", b"ref: refs//heads\n",
                    b"ref: refs/heads/..%2f..%2f\n"):
        head.write_bytes(hostile)
        assert world.bridge._ref_snapshot(binding) == "untrusted_ref", hostile
    head.write_bytes(original)
    assert world.bridge._ref_snapshot(binding) != "untrusted_ref"
    # A symlinked ref into an arbitrary service file is never read through.
    secret = tmp_path / "service-account-key.pem"
    secret.write_text("PRIVATE KEY MATERIAL")
    target = repository / ".git" / "refs" / "heads" / "main"
    original_ref = target.read_bytes()
    linked = False
    try:
        target.unlink()
        os.symlink(secret, target)
        linked = True
    except OSError:
        target.write_bytes(original_ref)  # platform without symlink privilege
    if linked:
        assert world.bridge._ref_snapshot(binding) == "untrusted_ref"
        world.bridge.fallback_scan()  # fail closed: no crash, no event, no leak
        assert world.source_changes() == []
        assert str(secret) not in json.dumps(world.status())
        target.unlink()
        target.write_bytes(original_ref)


def test_gitfile_worktree_watcher_needs_distinct_trusted_event_ids(builder, tmp_path):
    reports = []

    def watcher(bindings):
        result, reports[:] = list(reports), []
        return result

    repository = tmp_path / "repo"
    first_commit = make_repository(repository)
    # Turn .git into a gitfile: real git still works, but the ref scan cannot observe.
    os.rename(repository / ".git", repository / ".git-real")
    (repository / ".git").write_text("gitdir: .git-real\n")
    world = builder(watcher=watcher,
                    bridge_policy=canonical_only_policy())
    key = world.bind(repository)
    calls = {"count": 0}
    original = world.automation.detect_changes

    def counting(principal, **kwargs):
        calls["count"] += 1
        return original(principal, **kwargs)

    world.automation.detect_changes = counting
    assert world.trigger(binding_key=key)["outcome"] == "accepted"  # baseline
    assert wait_for(lambda: world.counts(STATE_KIND) == 1)
    baseline_calls = calls["count"]
    # A bare hint without a trusted event id dedups forever: the scan sees nothing.
    commit_change(repository)
    reports.append({"binding_key": key})
    assert wait_for(lambda: world.bridge._watcher_rejected == 0 and
                    world.status()["pending"] == 0)
    time.sleep(0.3)
    assert calls["count"] == baseline_calls  # never inferred without a real re-read
    assert world.source_changes() == []
    # Distinct trusted event ids wake real detection (detect_changes re-reads git).
    reports.append({"binding_key": key, "event_id": "native-1"})
    assert wait_for(lambda: len(world.source_changes()) == 1)
    assert world.source_changes()[0]["from_commit"] == first_commit
    replayed = calls["count"]
    reports.append({"binding_key": key, "event_id": "native-1"})  # same id dedups
    time.sleep(0.3)
    assert calls["count"] == replayed
    assert len(world.source_changes()) == 1
    reports.append({"binding_key": key, "event_id": "x" * 200})  # oversized: rejected
    assert wait_for(lambda: world.bridge._watcher_rejected >= 1)


# ------------------------------------------------------------- ownership and retries
def test_foreign_live_owner_is_never_claimed_and_dead_owner_holds(builder, tmp_path):
    world = builder(bridge_policy=canonical_only_policy())
    import sys
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    foreign = "f" * 64

    def insert_running(fingerprint, owner, pid):
        with world.store.transaction():
            world.store.db.execute(
                "INSERT INTO memory_event_bridge_intents"
                "(project, fingerprint, state, detail, owner, pid, created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (world.project, fingerprint, "running", None, owner, pid, now(), now()))

    try:
        insert_running(foreign, "bridge_foreign", process.pid)
        decision, _ = world.bridge._intent_begin(world.project, foreign, None)
        assert decision == "inflight"  # live external owner: hold, never claim/convert
        row = [r for r in world.intent_rows() if r["fingerprint"] == foreign][0]
        assert row["state"] == "running" and row["owner"] == "bridge_foreign"
    finally:
        process.kill()
    dead = "d" * 64  # same process, no live instance: a crashed prior bridge
    insert_running(dead, "bridge_crashed", os.getpid())
    decision, _ = world.bridge._intent_begin(world.project, dead, None)
    assert decision == "hold"
    row = [r for r in world.intent_rows() if r["fingerprint"] == dead][0]
    assert row["state"] == "unknown"  # durable hold, never recomputed


def test_transient_error_rehints_and_recovers(builder, tmp_path):
    world, repository, key, memory, first_commit = pinned_world(builder, tmp_path)
    settle(world)
    original = world.bridge._catch_up
    state = {"failures": 0}

    def flaky(project):
        if state["failures"] < 2:
            state["failures"] += 1
            raise Problem("store_busy", 503)
        return original(project)

    world.bridge._catch_up = flaky
    second_commit = commit_change(repository)
    world.canonical_commit("task")
    assert wait_for(lambda: any(event["to_commit"] == second_commit
                                for event in world.source_changes()))
    assert world.status()["holds"] == []


def test_dropped_hint_recovers_via_watermark_fallback(builder, tmp_path):
    world, repository, key, memory, first_commit = pinned_world(builder, tmp_path)
    settle(world)
    with world.bridge._state_lock:  # simulate a fully dropped listener wake-up
        world.bridge._pending.clear()
        world.bridge._gap = False
    second_commit = commit_change(repository)  # no canonical commit ever follows
    world.bridge.fallback_scan()  # re-hints every known project, re-reads git refs
    assert wait_for(lambda: any(event["to_commit"] == second_commit
                                for event in world.source_changes()))


def test_high_volume_commit_burst_coalesces(builder, tmp_path):
    world, repository, key, memory, first_commit = pinned_world(builder, tmp_path)
    settle(world)
    second_commit = commit_change(repository)
    for _ in range(40):
        world.canonical_commit("task")
    assert wait_for(lambda: len(world.source_changes()) == 1)
    settle(world, quiet=0.5)
    assert len(world.source_changes()) == 1
    assert world.source_changes()[0]["to_commit"] == second_commit
    assert world.status()["watermark"] > 0
    assert world.status()["holds"] == []


def test_event_loop_responsive_and_close_rechecks_thread(builder, tmp_path):
    world = builder(bridge_policy=canonical_only_policy())
    repository = tmp_path / "repo"
    make_repository(repository)
    world.bind(repository)
    original = world.automation.detect_changes
    entered, release = threading.Event(), threading.Event()

    def blocked(principal, **kwargs):
        entered.set()
        release.wait(10)
        return original(principal, **kwargs)

    world.automation.detect_changes = blocked
    world.canonical_commit("task")
    assert entered.wait(5)
    started = time.monotonic()
    status = world.status()  # detached route answers while compute is blocked
    assert status["worker"] is True
    assert world.trigger()["outcome"] == "queued"
    assert time.monotonic() - started < 5
    first = world.bridge.close()
    assert first == {"closed": True, "drained": False}
    assert world.bridge.close()["drained"] is False  # honest while still blocked
    release.set()
    world.bridge._thread.join(10)
    assert world.bridge.close() == {"closed": True, "drained": True}  # re-checked
