"""Repository-driven memory lifecycle: change detection, contradiction candidates, reminders.

These tests own a harness that installs the lifecycle automation with the exact injected
contract the service uses (`install(app, service, principal, command, envelope)`), so no
shared module changes. Git fixtures are real temporary repositories; encoders/detectors
are local stubs. No network, no paid model, no repository hooks, no fetches.
"""

import contextlib
import itertools
import json
import secrets
import shutil
import subprocess
import threading
import time
from pathlib import Path

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from hivemind import memory_lifecycle_automation as lifecycle_module
from hivemind import models as m
from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.memory_lifecycle_automation import (EVENT_KIND, OPERATION_DESCRIPTORS,
                                                  REMINDER_KIND, STATE_KIND, LifecyclePolicy,
                                                  RepositoryBinding, install)
from hivemind.memory_pipeline import LocalHashingEncoder
from hivemind.storage import Problem, uid

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")

GIT_ENV = {"GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.com",
           "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.com",
           "GIT_CONFIG_NOSYSTEM": "1"}


def git(repository, *arguments):
    completed = subprocess.run(["git", "-C", str(repository), *arguments], capture_output=True,
                               text=True, env={**dict(__import__("os").environ), **GIT_ENV})
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


class LifecycleWorld:
    def __init__(self, directory, **options):
        config = json.loads(initialize(directory).read_text())
        self.directory, self.project, self.owner = Path(directory), config["project_id"], config["token"]
        self.app = create_app(directory, workers=False)
        self.store, self.service = self.app.state.store, self.app.state.service
        principal, command, envelope = make_dependencies(self.store, self.service)
        self.automation = install(self.app, self.service, principal, command, envelope, **options)
        self.stack = contextlib.ExitStack()
        self.client = self.stack.enter_context(TestClient(self.app))

    def close(self):
        self.stack.close()

    def call(self, method, path, body=None, token=None, key=None, status=200):
        response = self.client.request(method, "/v1/" + path, json=body, headers={
            "Authorization": "Bearer " + (token or self.owner), "Idempotency-Key": key or uid("key")})
        assert response.status_code == status, response.text
        return response.json().get("data", response.json())

    def agent(self, name="Lifecycle worker"):
        return self.call("POST", "agents", {"project_id": self.project, "display_name": name,
                                           "supported_actions": [], "scope_permit_enforcement": False})

    def credential(self, agent=None, role="agent"):
        token = secrets.token_urlsafe(32)
        with self.store.transaction():
            self.store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                                  (uid("principal"), hash_token(token), role, self.project,
                                   agent["id"] if agent else None, 0, 0))
        return token

    def principal_id(self, token):
        return self.store.db.execute("SELECT id FROM principals WHERE token_hash=?",
                                     (hash_token(token),)).fetchone()["id"]

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
                "evidence_refs": [{"kind": "board_post", "id": source["id"], "revision": source["revision"]}],
                **fields}
        if "scope_kind" not in body:
            body.update(scope_kind="project", scope_owner_id=self.project)
        return self.call("POST", "memories", body, token)

    def events(self, token=None, **query):
        suffix = "".join(f"&{key}={value}" for key, value in query.items())
        return self.call("GET", f"memory-lifecycle:events?project_id={self.project}{suffix}", token=token)

    def reminders(self, token=None):
        return self.call("GET", f"memory-lifecycle:reminders?project_id={self.project}", token=token)

    def notifications(self, agent, token=None):
        return self.call("GET", f"agents/{agent['id']}/notifications?project_id={self.project}", token=token)

    def counts(self, kind):
        return self.store.db.execute("SELECT count(*) AS n FROM resources WHERE kind=? AND deleted=0",
                                     (kind,)).fetchone()["n"]


@pytest.fixture
def builder(tmp_path):
    counter, worlds = itertools.count(), []

    def build(**options):
        directory = tmp_path / f"data-{next(counter)}"
        directory.mkdir()
        world = LifecycleWorld(directory, **options)
        worlds.append(world)
        return world

    yield build
    for world in worlds:
        world.close()


def repo_binding(world, workspace, **overrides):
    return RepositoryBinding(project_id=world.project, workspace=str(workspace), **overrides)


def bound_world(builder, workspace, policy_object, **binding_overrides):
    """Build a world, then bind the repository to its project id (install-time surface)."""
    world = builder(policy=policy_object)
    world.automation.bind(repo_binding(world, workspace, **binding_overrides))
    return world


def policy(**overrides):
    return LifecyclePolicy(enabled=True, **overrides)


# --------------------------------------------------------------------------- lifecycle

def test_disabled_by_default(builder):
    world = builder()
    world.call("POST", "memory-lifecycle:tick", {}, status=409)
    world.call("POST", "memory-lifecycle:scan-contradictions",
               {"project_id": world.project}, status=409)
    world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project}, status=409)
    world.call("GET", f"memory-lifecycle:events?project_id={world.project}", status=409)
    status = world.call("GET", f"memory-lifecycle:status?project_id={world.project}")
    assert status["enabled"] is False


def test_policy_and_binding_validation():
    with pytest.raises(ValueError):
        LifecyclePolicy(enabled=True, max_diff_paths=0)
    with pytest.raises(ValueError):
        LifecyclePolicy(enabled=True, contradiction_scan_limit=1)
    with pytest.raises(ValueError):
        LifecyclePolicy(enabled=True, reminder_cooldown_seconds=1)
    with pytest.raises(ValueError):
        LifecyclePolicy(enabled=True, max_git_output_bytes=1024)
    with pytest.raises(ValueError):  # strict boolean, not truthy int
        LifecyclePolicy(enabled=1)
    with pytest.raises(ValueError):  # bounded finite values only
        LifecyclePolicy(enabled=True, git_timeout_seconds=float("inf"))
    with pytest.raises(ValueError):
        LifecyclePolicy(enabled=True, contradiction_similarity=float("nan"))
    with pytest.raises(ValueError):
        LifecyclePolicy(enabled=True, secret_path_markers=["secret"])  # tuple required
    with pytest.raises(ValueError):
        RepositoryBinding(project_id="", workspace="/tmp/x")
    with pytest.raises(ValueError):
        RepositoryBinding(project_id="p", workspace="")
    with pytest.raises(ValueError):  # strict boolean binding
        RepositoryBinding(project_id="p", workspace="/tmp/x", auto_invalidate=1)
    assert RepositoryBinding(project_id="p", workspace="/tmp/x",
                             auto_invalidate=True).auto_invalidate is True


def test_baseline_then_new_commit_review_proposal(builder, tmp_path):
    repository = tmp_path / "repo"
    first_commit = make_repository(repository)
    from hivemind.memory_pipeline import repository_identity
    repo_id = repository_identity(repository)
    world = bound_world(builder, repository, policy())
    token = world.credential(world.agent())
    memory = world.memory("The export uses UTC timestamps", "Format", token=token,
                          validity={"repository_id": repo_id, "branch": "main",
                                    "observed_commit": first_commit})

    first = world.call("POST", "memory-lifecycle:detect", {})
    assert first["results"][0]["state"] == "baseline"
    assert world.events()["events"] == [] and world.counts(STATE_KIND) == 1
    assert world.call("POST", "memory-lifecycle:detect", {})["results"][0]["state"] == "unchanged"

    second_commit = commit_change(repository)
    result = world.call("POST", "memory-lifecycle:detect", {})
    outcome = result["results"][0]
    assert outcome["state"] == "changed" and outcome["rewritten"] is False
    assert outcome["changed_paths"] == 1
    events = world.events()["events"]
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "source_change" and event["commit_changed"] is True
    assert event["from_commit"] == first_commit and event["to_commit"] == second_commit
    assert event["changed_paths"] == ["spec.md"] and event["status_effect"] == "review_only"
    assert event["affected"] == [{"memory_id": memory["id"], "memory_revision": memory["revision"],
                                  "observed_commit": first_commit, "currentness": "drifted",
                                  "currentness_reasons": ["commit_drift"], "action": "review_proposal"}]
    assert str(repository) not in json.dumps(event)

    # A repeated tick dedups: no new event, memory untouched (proposal only).
    assert world.call("POST", "memory-lifecycle:detect", {})["results"][0]["state"] == "unchanged"
    assert len(world.events()["events"]) == 1
    current = world.call("GET", f"memories/{memory['id']}")
    assert current["revision"] == memory["revision"] and current["status"] == "proposed"


def test_tick_requires_operator_role(builder, tmp_path):
    repository = tmp_path / "repo"
    make_repository(repository)
    world = bound_world(builder, repository, policy())
    agent = world.agent()
    token = world.credential(agent)
    world.call("POST", "memory-lifecycle:tick", {}, token=token, status=403)
    world.call("POST", "memory-lifecycle:detect", {}, token=token, status=403)
    result = world.call("POST", "memory-lifecycle:tick", {})
    assert result["repositories"][0]["state"] == "baseline"


def test_logical_invalidation_requires_explicit_opt_in(builder, tmp_path):
    repository = tmp_path / "repo"
    first_commit = make_repository(repository)
    from hivemind.memory_pipeline import repository_identity
    repo_id = repository_identity(repository)
    world = bound_world(builder, repository, policy())
    token = world.credential(world.agent())
    memory = world.memory("The export uses UTC timestamps", "Format", token=token,
                          validity={"repository_id": repo_id, "branch": "main",
                                    "observed_commit": first_commit})
    world.call("POST", "memory-lifecycle:detect", {})
    commit_change(repository)
    world.call("POST", "memory-lifecycle:detect", {})
    current = world.call("GET", f"memories/{memory['id']}")
    assert current["revision"] == memory["revision"] and not current.get("invalidation_reason")
    assert world.events()["events"][0]["affected"][0]["action"] == "review_proposal"


def test_auto_invalidate_exact_commit_binding_preserves_history(builder, tmp_path):
    repository = tmp_path / "repo"
    first_commit = make_repository(repository)
    from hivemind.memory_pipeline import repository_identity
    repo_id = repository_identity(repository)
    world = bound_world(builder, repository, policy(), auto_invalidate=True)
    token = world.credential(world.agent())
    exact = world.memory("The export uses UTC timestamps", "Format", token=token,
                         validity={"repository_id": repo_id, "branch": "main",
                                   "observed_commit": first_commit})
    other = world.memory("The dashboard renders boards", "Dashboard", token=token,
                         validity={"repository_id": repo_id, "branch": "main",
                                   "observed_commit": "0" * 40})
    world.call("POST", "memory-lifecycle:detect", {})
    second_commit = commit_change(repository)
    world.call("POST", "memory-lifecycle:detect", {})

    # Invalidated memories are logically gone for readers (the store's own rule);
    # the immutable history and the new revision remain in the canonical store.
    world.call("GET", f"memories/{exact['id']}", status=404)
    row = world.store.raw(exact["id"])
    invalidated = json.loads(row["body"])
    assert invalidated["revision"] == exact["revision"] + 1
    assert invalidated["invalidation_reason"] == "repository_commit_superseded"
    assert invalidated["invalidation"]["from_commit"] == first_commit
    assert invalidated["invalidation"]["to_commit"] == second_commit
    assert world.store.memory_state(invalidated) == "invalidated"
    versions = [json.loads(entry["body"]) for entry in world.store.db.execute(
        "SELECT body FROM versions WHERE id=? ORDER BY revision", (exact["id"],)).fetchall()]
    assert len(versions) == 2 and "invalidation_reason" not in versions[0]

    # Not exactly bound to the superseded commit: proposal only.
    other_current = world.call("GET", f"memories/{other['id']}")
    assert other_current["revision"] == other["revision"]
    actions = {entry["memory_id"]: entry["action"] for entry in world.events()["events"][0]["affected"]}
    assert actions[exact["id"]] == "logically_invalidated"
    assert actions[other["id"]] == "review_proposal"
    assert world.events()["events"][0]["status_effect"] == "logical_invalidation"

    # Repeating the tick never double-invalidates.
    world.call("POST", "memory-lifecycle:detect", {})
    assert json.loads(world.store.raw(exact["id"])["body"])["revision"] == exact["revision"] + 1


def test_rewrite_and_dirty_source_detection(builder, tmp_path):
    repository = tmp_path / "repo"
    first_commit = make_repository(repository)
    world = bound_world(builder, repository, policy())
    world.call("POST", "memory-lifecycle:detect", {})

    (repository / "spec.md").write_text("dirty, uncommitted\n")
    result = world.call("POST", "memory-lifecycle:detect", {})["results"][0]
    assert result["state"] == "changed" and result["observed"]["dirty_files"] == 1
    event = world.events()["events"][0]
    assert event["commit_changed"] is False and event["dirty_files"] == 1
    git(repository, "checkout", "--", "spec.md")

    # History rewrite: force HEAD back to a brand-new unrelated commit.
    git(repository, "checkout", "-q", "--orphan", "rewritten")
    git(repository, "commit", "-q", "--allow-empty", "-m", "unrelated")
    git(repository, "branch", "-f", "main", "rewritten")
    git(repository, "checkout", "-q", "main")
    git(repository, "branch", "-D", "rewritten")
    result = world.call("POST", "memory-lifecycle:detect", {})["results"][0]
    assert result["state"] == "changed" and result["rewritten"] is True
    event = world.events()["events"][0]
    assert event["rewritten"] is True and event["changed_paths"] == []
    # The old object may still exist locally; the rewrite is detected via ancestry.
    assert commit_exists_check(repository, first_commit)
    assert subprocess.run(["git", "-C", str(repository), "merge-base", "--is-ancestor",
                           first_commit, "HEAD"], capture_output=True).returncode == 1


def commit_exists_check(repository, commit):
    return subprocess.run(["git", "-C", str(repository), "cat-file", "-e", f"{commit}^{{commit}}"],
                          capture_output=True).returncode == 0


def test_branch_binding_ignores_other_checked_out_branches(builder, tmp_path):
    repository = tmp_path / "repo"
    make_repository(repository)
    world = bound_world(builder, repository, policy(), branch="main")
    git(repository, "checkout", "-q", "-b", "feature")
    commit_change(repository, message="feature work")
    result = world.call("POST", "memory-lifecycle:detect", {})["results"][0]
    assert result["state"] == "other_branch_checked_out"
    assert world.events()["events"] == [] and world.counts(STATE_KIND) == 0
    git(repository, "checkout", "-q", "main")
    assert world.call("POST", "memory-lifecycle:detect", {})["results"][0]["state"] == "baseline"


def test_unavailable_workspace_is_explicit(builder, tmp_path):
    repository = tmp_path / "repo"
    make_repository(repository)
    world = bound_world(builder, tmp_path / "missing", policy())
    result = world.call("POST", "memory-lifecycle:detect", {})["results"][0]
    assert result["state"] == "unavailable" and result["reason"] == "workspace_unavailable"


# --------------------------------------------------------------------- contradictions

def semantic_pair(world, token, left, right):
    return (world.memory(left, "Left", token=token, category="semantic"),
            world.memory(right, "Right", token=token, category="semantic"))


def test_lexical_contradiction_candidate_is_review_only(builder):
    world = builder(policy=policy())
    token = world.credential(world.agent())
    left, right = semantic_pair(world, token,
                                "The export pipeline uses UTC timestamps for all records",
                                "The export pipeline never uses UTC timestamps for all records")
    result = world.call("POST", "memory-lifecycle:scan-contradictions",
                        {"project_id": world.project})
    assert result["compared"] == 2 and result["status_effect"] == "review_only"
    assert len(result["candidates"]) == 1
    candidate = result["candidates"][0]
    assert candidate["method"] == "lexical" and "negation_conflict" in candidate["signals"]
    assert 0.0 <= candidate["confidence"] <= 1.0
    assert any("Lexical" in item for item in candidate["limitations"])
    assert candidate["state"] == "proposed"

    # Review only: memories unchanged, no dispute/quarantine created.
    assert world.call("GET", f"memories/{left['id']}")["status"] == "proposed"
    assert world.call("GET", f"memories/{right['id']}")["status"] == "proposed"
    assert world.counts("memory_dispute") == 0
    # Repeated scan dedups on the same revisions.
    assert world.call("POST", "memory-lifecycle:scan-contradictions",
                      {"project_id": world.project})["candidates"] == []


def test_lexical_scan_needs_overlap_and_signals(builder):
    world = builder(policy=policy())
    token = world.credential(world.agent())
    semantic_pair(world, token, "Cats are animals", "Dogs are not minerals")
    result = world.call("POST", "memory-lifecycle:scan-contradictions",
                        {"project_id": world.project})
    assert result["candidates"] == []
    assert world.counts(EVENT_KIND) == 0


def injected_detector(verdict="contradiction", confidence=0.9, evidence="Heuristic stub",
                      limitations=None):
    def detector(left_text, right_text):
        return {"verdict": verdict, "confidence": confidence, "evidence": evidence,
                "limitations": limitations or ["stub detector"]}
    detector.name = "stub-detector-v1"
    return detector


def test_injected_detector_is_opt_in_and_validated(builder):
    world = builder(policy=policy(), detector=injected_detector())
    token = world.credential(world.agent())
    # Same polarity words, no lexical contradiction signals; the detector decides.
    semantic_pair(world, token,
                  "The sync service stores checkpoint metadata in the primary region",
                  "The sync service stores checkpoint metadata in the secondary region")
    result = world.call("POST", "memory-lifecycle:scan-contradictions",
                        {"project_id": world.project})
    assert len(result["candidates"]) == 1
    candidate = result["candidates"][0]
    assert candidate["method"] == "injected:stub-detector-v1"
    assert candidate["detector"] == "stub-detector-v1" and candidate["confidence"] == 0.9
    assert candidate["evidence"] == "Heuristic stub" and "stub detector" in candidate["limitations"]
    assert candidate["status_effect"] == "review_only"
    world.close()

    world = builder(policy=policy(), detector=lambda a, b: {"verdict": "everything", "bad": True})
    token = world.credential(world.agent())
    semantic_pair(world, token,
                  "The sync service stores checkpoint metadata in the primary region",
                  "The sync service stores checkpoint metadata in the secondary region")
    result = world.call("POST", "memory-lifecycle:scan-contradictions",
                        {"project_id": world.project})
    assert result["candidates"] == [] and result["detector_errors"] == ["invalid_shape"]
    world.close()

    def exploding(a, b):
        raise RuntimeError("boom")
    world = builder(policy=policy(), detector=exploding)
    token = world.credential(world.agent())
    semantic_pair(world, token,
                  "The sync service stores checkpoint metadata in the primary region",
                  "The sync service stores checkpoint metadata in the secondary region")
    world.call("POST", "memory-lifecycle:scan-contradictions",
               {"project_id": world.project}, status=503)


def test_unknown_evidence_never_quarantines(builder):
    world = builder(policy=policy(), detector=injected_detector(verdict="unknown"))
    token = world.credential(world.agent())
    left, right = semantic_pair(world, token,
                                "The sync service stores checkpoint metadata in the primary region",
                                "The sync service stores checkpoint metadata in the secondary region")
    result = world.call("POST", "memory-lifecycle:scan-contradictions",
                        {"project_id": world.project})
    assert result["candidates"] == [] and world.counts(EVENT_KIND) == 0
    assert world.call("GET", f"memories/{left['id']}")["status"] == "proposed"


def test_private_scopes_are_never_correlated(builder):
    world = builder(policy=policy())
    agent_a, agent_b = world.agent("A"), world.agent("B")
    token_a, token_b = world.credential(agent_a), world.credential(agent_b)
    world.memory("The export pipeline uses UTC timestamps for all records", "A", token=token_a,
                 category="semantic", scope_kind="agent", scope_owner_id=agent_a["id"])
    world.memory("The export pipeline never uses UTC timestamps for all records", "B", token=token_b,
                 category="semantic", scope_kind="agent", scope_owner_id=agent_b["id"])
    # Admin scans: cross-private pairs are skipped entirely.
    result = world.call("POST", "memory-lifecycle:scan-contradictions",
                        {"project_id": world.project})
    assert result["candidates"] == [] and world.counts(EVENT_KIND) == 0

    # Same-agent private pair produces a private event invisible to the other agent.
    world.memory("The export pipeline never uses UTC timestamps for all records", "A2", token=token_a,
                 category="semantic", scope_kind="agent", scope_owner_id=agent_a["id"])
    result = world.call("POST", "memory-lifecycle:scan-contradictions",
                      {"project_id": world.project})
    assert len(result["candidates"]) == 1
    visible_a = world.events(token=token_a)["events"]
    visible_b = world.events(token=token_b)["events"]
    assert len(visible_a) == 1 and visible_b == []


# --------------------------------------------------------------------------- reminders

def future_utc(seconds):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + seconds))


def past_utc(seconds):
    return future_utc(-seconds)


def test_expiry_reminder_dedup_cooldown_and_notification(builder):
    clock = [time.time()]
    world = builder(policy=policy(reminder_lead_seconds=3600, reminder_cooldown_seconds=600),
                    time_source=lambda: clock[0])
    agent = world.agent()
    token = world.credential(agent)
    memory = world.memory("Temporary fact", "Temp", token=token, scope_kind="agent",
                          scope_owner_id=agent["id"], expires_at=future_utc(1800))

    first = world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert len(first["reminders"]) == 1
    reminder = first["reminders"][0]
    assert reminder["reminder"] == "expiry_reminder" and reminder["memory_id"] == memory["id"]
    assert reminder["notified_agent_id"] == agent["id"]
    notices = world.notifications(agent, token)
    assert len(notices) == 1 and notices[0]["type"] == "memory_lifecycle"
    assert notices[0]["lifecycle_type"] == "expiry_reminder"

    # Same cooldown bucket: fully deduplicated, no extra notification.
    again = world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert again["reminders"] == [] and world.counts(REMINDER_KIND) == 1
    assert len(world.notifications(agent, token)) == 1

    # After the cooldown window a new reminder is allowed.
    clock[0] += 601
    third = world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert len(third["reminders"]) == 1 and world.counts(REMINDER_KIND) == 2


def test_expired_review_is_once_and_nothing_is_deleted(builder):
    world = builder(policy=policy())
    token = world.credential(world.agent())
    memory = world.memory("Old fact", "Old", token=token, expires_at=past_utc(10))
    first = world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert [item["reminder"] for item in first["reminders"]] == ["expired_review"]
    assert first["reminders"][0]["memory_state"] == "expired"
    again = world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert again["reminders"] == [] and world.counts(REMINDER_KIND) == 1
    # The memory still exists; only its logical state changed (via the store's fence).
    assert world.call("GET", f"memories/{memory['id']}")["effective_status"] == "expired"


def test_stale_review_policy_and_project_scope_recipient(builder):
    world = builder(policy=policy(review_after_seconds=3600))
    agent = world.agent("Recipient")
    token = world.credential(agent)
    old = world.memory("Long-lived fact", "Long", token=token)
    backdated = past_utc(7200)
    with world.store.transaction():
        row = world.store.raw(old["id"])
        world.store.save("memory", world.project, row["owner"], {**old, "created_at": backdated},
                         expected=row["revision"])
    # No configured recipient for a project-scope memory: reminder recorded, no notification.
    result = world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert len(result["reminders"]) == 1 and result["reminders"][0]["reminder"] == "stale_review"
    assert result["reminders"][0]["notified_agent_id"] is None
    assert world.counts("notification") == 0


def test_stale_review_notifies_configured_project_recipient(builder):
    world = builder(policy=policy(review_after_seconds=3600))
    agent = world.agent("Recipient")
    token = world.credential(agent)
    import dataclasses
    world.automation.policy = dataclasses.replace(world.automation.policy,
                                                  notify_agent_id=agent["id"])
    memory = world.memory("Long-lived fact", "Long", token=token)
    with world.store.transaction():
        row = world.store.raw(memory["id"])
        world.store.save("memory", world.project, row["owner"],
                         {**memory, "created_at": past_utc(7200)}, expected=row["revision"])
    result = world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert result["reminders"][0]["notified_agent_id"] == agent["id"]
    notices = world.notifications(agent, token)
    assert len(notices) == 1 and notices[0]["lifecycle_type"] == "stale_review"


def test_hidden_or_revoked_evidence_is_skipped_safely(builder):
    world = builder(policy=policy(reminder_lead_seconds=3600))
    token = world.credential(world.agent())
    memory = world.memory("Temporary fact", "Temp", token=token, expires_at=future_utc(1800))
    # Revoke the evidence source: the memory becomes invisible to validity checks.
    post_id = memory["evidence_refs"][0]["id"]
    with world.store.transaction():
        world.store.db.execute("UPDATE resources SET deleted=1 WHERE id=?", (post_id,))
    result = world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert result["reminders"] == [] and world.counts(REMINDER_KIND) == 0


def test_revision_change_resets_reminder_dedup(builder):
    clock = [time.time()]
    world = builder(policy=policy(reminder_lead_seconds=7200, reminder_cooldown_seconds=600),
                    time_source=lambda: clock[0])
    agent = world.agent()
    token = world.credential(agent)
    memory = world.memory("Temporary fact", "Temp", token=token, scope_kind="agent",
                          scope_owner_id=agent["id"], expires_at=future_utc(1800))
    world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert world.counts(REMINDER_KIND) == 1
    # A new memory version (human proposal approved path) resets dedup via revision binding.
    proposal = world.call("POST", f"memories/{memory['id']}/versions", {
        "expected_revision": memory["revision"], "title": "Temp",
        "content": {"media_type": "text/plain", "text": "Temporary fact v2"},
        "evidence_refs": memory["evidence_refs"], "expires_at": future_utc(1800)}, token)
    world.call("POST", f"memory-proposals/{proposal['id']}:review", {
        "expected_revision": proposal["revision"], "decision": "approve",
        "reason": "Updated content accepted"})
    result = world.call("POST", "memory-lifecycle:reminders", {"project_id": world.project})
    assert len(result["reminders"]) == 1
    assert result["reminders"][0]["memory_revision"] == memory["revision"] + 1


# ------------------------------------------------------------------------ surfaces

def test_status_events_descriptors_and_tick(builder, tmp_path):
    repository = tmp_path / "repo"
    make_repository(repository)
    world = bound_world(builder, repository, policy(reminder_lead_seconds=3600), label="spec-repo")
    token = world.credential(world.agent())
    world.memory("The export pipeline uses UTC timestamps for all records", "Left", token=token,
                 category="semantic")
    world.memory("The export pipeline never uses UTC timestamps for all records", "Right",
                 token=token, category="semantic")
    tick = world.call("POST", "memory-lifecycle:tick", {})
    assert tick["repositories"][0]["state"] == "baseline"
    assert len(tick["contradictions"][0]["candidates"]) == 1
    assert tick["status_effect"] == "review_only"

    status = world.call("GET", f"memory-lifecycle:status?project_id={world.project}")
    assert status["enabled"] is True and status["bindings"][0]["label"] == "spec-repo"
    assert status["events"]["contradiction_review"] == 1
    descriptors = world.call("GET", "memory-lifecycle:descriptors")
    names = {entry["name"] for entry in descriptors["descriptors"]}
    assert {"source_change", "contradiction_review",
            "expiry_reminder / expired_review / stale_review",
            "logical_invalidation"} == names
    for entry in descriptors["descriptors"]:
        assert all(entry[key] for key in ("purpose", "input", "output", "authority",
                                          "side_effects", "limits", "retry_safety"))


def test_scheduled_tick_interval_and_operator(builder, tmp_path):
    repository = tmp_path / "repo"
    make_repository(repository)
    world = bound_world(builder, repository, policy())
    assert world.automation.scheduled_tick() is None  # no operator configured
    world.close()

    world = bound_world(builder, repository, policy(operator_principal_id="principal_missing"))
    assert world.automation.scheduled_tick() is None
    world.close()

    world = bound_world(builder, repository, policy(interval_seconds=3600))
    operator_id = world.principal_id(world.owner)
    import dataclasses
    world.automation.policy = dataclasses.replace(world.automation.policy,
                                                  operator_principal_id=operator_id)
    first = world.automation.scheduled_tick()
    assert first is not None and first["repositories"][0]["state"] == "baseline"
    assert world.automation.scheduled_tick() is None  # interval gate


def test_read_only_git_leaves_no_marks(builder, tmp_path):
    repository = tmp_path / "repo"
    make_repository(repository)
    world = bound_world(builder, repository, policy())
    commit_change(repository)
    before = sorted((str(path.relative_to(repository)), path.stat().st_mtime_ns)
                    for path in repository.rglob("*") if path.is_file())
    world.call("POST", "memory-lifecycle:detect", {})
    world.call("POST", "memory-lifecycle:detect", {})
    after = sorted((str(path.relative_to(repository)), path.stat().st_mtime_ns)
                   for path in repository.rglob("*") if path.is_file())
    assert before == after
    assert not (repository / ".git" / "index.lock").exists()

def test_compute_never_holds_locks(builder):
    """Encoder and detector run without this thread holding the store lock.

    `store.db.in_transaction` is connection-wide and may legitimately be True because
    of an unrelated write on another thread; the discipline is per-thread: the compute
    thread holds neither the lock nor its own transaction, even while another thread
    writes.
    """
    world = builder(policy=policy())
    token = world.credential(world.agent())
    semantic_pair(world, token,
                  "The export pipeline uses UTC timestamps for all records",
                  "The export pipeline never uses UTC timestamps for all records")
    calls = []
    writing, stop_writing = [], threading.Event()

    def unrelated_writer():
        while not stop_writing.is_set():
            world.post("Unrelated concurrent finding", token)
            writing.append(1)

    def lock_free_encoder(texts):
        # Another thread may hold a transaction; this thread must hold neither.
        assert not world.store.lock._is_owned()
        calls.append(len(texts))
        return LocalHashingEncoder().vectors(texts)

    def lock_free_detector(left_text, right_text):
        assert not world.store.lock._is_owned()
        return {"verdict": "none", "confidence": 0.1, "evidence": "", "limitations": []}

    lock_free_encoder.name = "test-semantic-encoder"
    world.automation.encoder = lock_free_encoder
    world.automation._encoder_injected = True
    world.automation.detector = lock_free_detector
    writer = threading.Thread(target=unrelated_writer, daemon=True)
    writer.start()
    result = world.call("POST", "memory-lifecycle:scan-contradictions",
                        {"project_id": world.project})
    stop_writing.set()
    writer.join(10)
    assert writing  # an unrelated write overlapped the encoder window
    assert calls == [2] and len(result["candidates"]) == 1  # lexical signals still fire
    assert result["candidates"][0]["method"] == "lexical"
    assert result["encoder"] == {"kind": "injected", "name": "test-semantic-encoder",
                                 "semantic": True, "note": None}
    world.automation.encoder = LocalHashingEncoder()
    world.automation._encoder_injected = False
    status = world.call("GET", f"memory-lifecycle:status?project_id={world.project}")
    assert status["encoder"]["kind"] == "lexical" and status["encoder"]["semantic"] is False
    assert "not a semantic model" in status["encoder"]["note"]


def test_concurrent_revoke_during_compute_discards_candidate(builder):
    """A blocked encoder must not grant authority from a stale snapshot."""
    world = builder(policy=policy())
    token = world.credential(world.agent())
    left, right = semantic_pair(world, token,
                                "The export pipeline uses UTC timestamps for all records",
                                "The export pipeline never uses UTC timestamps for all records")
    entered, release = threading.Event(), threading.Event()

    def blocked_encoder(texts):
        entered.set()
        assert release.wait(10)
        return LocalHashingEncoder().vectors(texts)

    world.automation.encoder = blocked_encoder
    world.automation._encoder_injected = True
    outcome = {}
    # Use a real principal row so publish-time revalidation succeeds while compute runs.
    operator = dict(world.store.db.execute(
        "SELECT * FROM principals WHERE token_hash=?", (hash_token(world.owner),)).fetchone())
    worker = threading.Thread(target=lambda: outcome.update(
        world.automation.scan_contradictions(operator, world.project)))
    worker.start()
    assert entered.wait(10)
    # While the encoder is blocked (no locks held), revoke the evidence of one memory.
    post_id = left["evidence_refs"][0]["id"]
    with world.store.transaction():
        world.store.db.execute("UPDATE resources SET deleted=1 WHERE id=?", (post_id,))
    release.set()
    worker.join(10)
    assert not worker.is_alive()
    assert outcome["candidates"] == [] and world.counts(EVENT_KIND) == 0


def test_detached_routes_serve_other_requests_while_compute_blocked(builder):
    """Marked endpoints skip the global request lock; ordinary requests proceed."""
    world = builder(policy=policy())
    token = world.credential(world.agent())
    semantic_pair(world, token,
                  "The export pipeline uses UTC timestamps for all records",
                  "The export pipeline never uses UTC timestamps for all records")
    entered, release = threading.Event(), threading.Event()

    def blocked_encoder(texts):
        entered.set()
        assert release.wait(10)
        return LocalHashingEncoder().vectors(texts)

    world.automation.encoder = blocked_encoder
    world.automation._encoder_injected = True
    responses = {}
    worker = threading.Thread(target=lambda: responses.update(
        {"scan": world.client.post("/v1/memory-lifecycle:scan-contradictions",
                                   json={"project_id": world.project},
                                   headers={"Authorization": "Bearer " + world.owner,
                                            "Idempotency-Key": uid("key")})}))
    worker.start()
    assert entered.wait(10)
    started = time.monotonic()
    boards = world.client.get(f"/v1/boards?project_id={world.project}",
                              headers={"Authorization": "Bearer " + world.owner})
    assert boards.status_code == 200 and time.monotonic() - started < 10
    release.set()
    worker.join(10)
    assert responses["scan"].status_code == 200
    routes = {(route.path, method): route.endpoint for route in world.app.router.routes
              if getattr(route, "endpoint", None) is not None
              for method in (getattr(route, "methods", None) or ())}
    for path in ("/v1/memory-lifecycle:tick", "/v1/memory-lifecycle:detect",
                 "/v1/memory-lifecycle:scan-contradictions", "/v1/memory-lifecycle:reminders"):
        assert getattr(routes[(path, "POST")], "hivemind_detached_work", False) is True
    for path in ("/v1/memory-lifecycle:events", "/v1/memory-lifecycle:status"):
        assert getattr(routes[(path, "GET")], "hivemind_detached_work", False) is False


def test_detached_idempotency_replays_without_recompute(builder):
    world = builder(policy=policy())
    token = world.credential(world.agent())
    semantic_pair(world, token,
                  "The export pipeline uses UTC timestamps for all records",
                  "The export pipeline never uses UTC timestamps for all records")
    calls = []

    def counting_encoder(texts):
        calls.append(1)
        return LocalHashingEncoder().vectors(texts)

    world.automation.encoder = counting_encoder
    world.automation._encoder_injected = True
    body = {"project_id": world.project}
    first = world.call("POST", "memory-lifecycle:scan-contradictions", body, key="scan-key")
    second = world.call("POST", "memory-lifecycle:scan-contradictions", body, key="scan-key")
    assert len(calls) == 1  # replay served from the stored response, no recompute
    assert second["candidates"][0]["id"] == first["candidates"][0]["id"]
    assert world.counts(EVENT_KIND) == 1
    world.call("POST", "memory-lifecycle:scan-contradictions", {**body, "limit": 8},
               key="scan-key", status=409)  # same key, different intent
    missing = world.client.post("/v1/memory-lifecycle:scan-contradictions", json=body,
                                headers={"Authorization": "Bearer " + world.owner})
    assert missing.status_code == 422  # idempotency key required
    third = world.call("POST", "memory-lifecycle:scan-contradictions", body, key="other-key")
    assert len(calls) == 2 and third["candidates"] == []  # recompute allowed, dedup holds
    rows = world.store.db.execute(
        "SELECT state, count(*) AS n FROM memory_lifecycle_intents"
        " WHERE operation='memory-lifecycle-scan-contradictions' GROUP BY state").fetchall()
    assert {row["state"]: row["n"] for row in rows} == {"done": 2}


def test_malicious_fsmonitor_never_executes(builder, tmp_path):
    repository = tmp_path / "repo"
    make_repository(repository)
    commit_change(repository)  # helper git runs before the malicious config exists
    spy = repository / "spy.sh"
    spy.write_text("#!/bin/sh\ntouch fsmonitor_executed\n")
    (repository / "post-status").write_text("#!/bin/sh\ntouch hook_executed\n")
    git(repository, "config", "core.fsmonitor", "sh spy.sh")
    git(repository, "config", "core.hooksPath", ".")
    world = bound_world(builder, repository, policy())
    result = world.call("POST", "memory-lifecycle:detect", {})["results"][0]
    assert result["state"] == "baseline"
    assert not (repository / "fsmonitor_executed").exists()
    assert not (repository / "hook_executed").exists()


def test_git_output_boundary_and_timeout_mapping(builder, tmp_path, monkeypatch):
    repository = tmp_path / "repo"
    make_repository(repository)
    world = bound_world(builder, repository, policy(max_git_output_bytes=4096))
    world.call("POST", "memory-lifecycle:detect", {})
    for index in range(200):
        (repository / f"file_with_a_rather_long_name_{index:04d}.txt").write_text("x")
    git(repository, "add", ".")
    git(repository, "commit", "-q", "-m", "many files")
    result = world.call("POST", "memory-lifecycle:detect", {})["results"][0]
    assert result["state"] == "unavailable" and result["reason"] == "git_observe_output_boundary"

    def timing_out(*args, **kwargs):
        raise TimeoutError("deadline")
    monkeypatch.setattr(lifecycle_module, "run_bounded", timing_out)
    result = world.call("POST", "memory-lifecycle:detect", {})["results"][0]
    assert result["state"] == "unavailable" and result["reason"] == "git_observe_timeout"


def test_secret_path_evidence_is_redacted(builder, tmp_path):
    repository = tmp_path / "repo"
    make_repository(repository)
    world = bound_world(builder, repository, policy())
    world.call("POST", "memory-lifecycle:detect", {})
    (repository / ".env").write_text("TOKEN=secret-value")
    (repository / "config").mkdir()
    (repository / "config" / "secret_token.txt").write_text("opaque")
    (repository / "visible.md").write_text("fine")
    git(repository, "add", ".")
    git(repository, "commit", "-q", "-m", "add config")
    world.call("POST", "memory-lifecycle:detect", {})
    event = world.events()["events"][0]
    assert event["redacted_paths"] == 2
    paths = list(event["changed_paths"])
    assert "visible.md" in paths
    assert sum(1 for path in paths if path.startswith("redacted:")) == 2
    assert all(not path.startswith("redacted:") or len(path) == len("redacted:") + 24
               for path in paths)
    encoded = json.dumps(event)
    assert ".env" not in encoded and "secret_token" not in encoded
    assert "secret-value" not in encoded and "opaque" not in encoded


def test_descriptors_constant_matches_endpoint(builder):
    world = builder(policy=policy())
    descriptors = world.call("GET", "memory-lifecycle:descriptors")
    assert descriptors["descriptors"] == [dict(item) for item in OPERATION_DESCRIPTORS]
    names = {item["name"] for item in OPERATION_DESCRIPTORS}
    assert names == {"source_change", "contradiction_review",
                     "expiry_reminder / expired_review / stale_review", "logical_invalidation"}


def test_simultaneous_same_key_invokes_encoder_once(builder):
    """Two concurrent identical requests: exactly one computes; the other is held."""
    world = builder(policy=policy())
    token = world.credential(world.agent())
    semantic_pair(world, token,
                  "The export pipeline uses UTC timestamps for all records",
                  "The export pipeline never uses UTC timestamps for all records")
    entered, release = threading.Event(), threading.Event()
    calls = []

    def blocked_encoder(texts):
        calls.append(1)
        entered.set()
        assert release.wait(10)
        return LocalHashingEncoder().vectors(texts)

    world.automation.encoder = blocked_encoder
    world.automation._encoder_injected = True
    responses = {}

    def post(name):
        responses[name] = world.client.post(
            "/v1/memory-lifecycle:scan-contradictions", json={"project_id": world.project},
            headers={"Authorization": "Bearer " + world.owner, "Idempotency-Key": "race-key"})

    winner = threading.Thread(target=post, args=("winner",))
    winner.start()
    assert entered.wait(10)
    post("loser")  # in this thread, while the winner is blocked in compute
    assert responses["loser"].status_code == 409
    assert responses["loser"].json()["error"]["code"] == "compute_intent_in_progress"
    release.set()
    winner.join(10)
    assert responses["winner"].status_code == 200
    assert len(calls) == 1 and world.counts(EVENT_KIND) == 1
    # After completion the same key replays the stored result with no compute.
    replay = world.call("POST", "memory-lifecycle:scan-contradictions",
                        {"project_id": world.project}, key="race-key")
    assert len(calls) == 1 and replay["candidates"][0]["id"] == \
        responses["winner"].json()["data"]["candidates"][0]["id"]


def test_crash_after_invocation_holds_outcome_unknown(builder):
    """A failure at/after an external invocation is durably held, never auto-replayed."""
    world = builder(policy=policy())
    operator = dict(world.store.db.execute(
        "SELECT * FROM principals WHERE token_hash=?", (hash_token(world.owner),)).fetchone())
    invoked = []

    def crashing_compute():
        world.automation._encode(["touch the encoder"])  # tracked external invocation
        invoked.append(1)
        raise RuntimeError("process died mid-flight")

    with pytest.raises(RuntimeError):
        world.automation.detached_call(operator, "memory-lifecycle-scan-contradictions",
                                       "crash-key", {"project_id": world.project},
                                       crashing_compute)
    row = world.store.db.execute(
        "SELECT state FROM memory_lifecycle_intents WHERE key='crash-key'").fetchone()
    assert row["state"] == "unknown"

    # A fresh automation instance (post-restart) over the same store still holds the key.
    from hivemind.memory_lifecycle_automation import MemoryLifecycleAutomation
    restarted = MemoryLifecycleAutomation(world.service, policy=world.automation.policy)
    with pytest.raises(Problem) as held:
        restarted.detached_call(operator, "memory-lifecycle-scan-contradictions", "crash-key",
                                {"project_id": world.project}, lambda: invoked.append(2))
    assert held.value.code == "compute_outcome_unknown" and invoked == [1]

    # An orphaned (never-completed, never-marked) reservation is likewise held.
    world.store.db.execute(
        "INSERT INTO memory_lifecycle_intents VALUES(?,?,?,?,?,?,?)",
        (operator["id"], "memory-lifecycle-scan-contradictions", "orphan-key",
         "x" * 64, "reserved", None, "2026-01-01T00:00:00Z"))
    with pytest.raises(Problem) as orphan:
        restarted.detached_call(operator, "memory-lifecycle-scan-contradictions", "orphan-key",
                                {"project_id": world.project}, lambda: invoked.append(3))
    assert orphan.value.status == 409 and invoked == [1]

    # An explicit new operator request key authorizes new work.
    fresh = restarted.detached_call(operator, "memory-lifecycle-scan-contradictions", "new-key",
                                    {"project_id": world.project},
                                    lambda: {"results": [], "status_effect": "review_only"})
    assert fresh["status_effect"] == "review_only"


def test_pre_invocation_failure_releases_key(builder):
    """A failure authoritatively before any invocation lets the same key retry."""
    world = builder(policy=policy())
    body = {"project_id": world.project}
    # Disabled policy fails before any encoder/detector/git invocation.
    import dataclasses
    world.automation.policy = dataclasses.replace(world.automation.policy, enabled=False)
    world.call("POST", "memory-lifecycle:scan-contradictions", body, key="retry-key", status=409)
    assert world.store.db.execute(
        "SELECT count(*) AS n FROM memory_lifecycle_intents WHERE key='retry-key'"
    ).fetchone()["n"] == 0  # reservation released
    world.automation.policy = dataclasses.replace(world.automation.policy, enabled=True)
    token = world.credential(world.agent())
    semantic_pair(world, token,
                  "The export pipeline uses UTC timestamps for all records",
                  "The export pipeline never uses UTC timestamps for all records")
    result = world.call("POST", "memory-lifecycle:scan-contradictions", body, key="retry-key")
    assert len(result["candidates"]) == 1
    row = world.store.db.execute(
        "SELECT state FROM memory_lifecycle_intents WHERE key='retry-key'").fetchone()
    assert row["state"] == "done"


def test_completed_replay_requires_current_authority(builder, tmp_path):
    """Revoked operator role/project: no replay, no cached output."""
    repository = tmp_path / "repo"
    make_repository(repository)
    world = bound_world(builder, repository, policy())
    first = world.call("POST", "memory-lifecycle:tick", {}, key="tick-key")
    assert first["repositories"][0]["state"] == "baseline"
    assert world.call("POST", "memory-lifecycle:tick", {}, key="tick-key") == first
    operator_id = world.principal_id(world.owner)
    with world.store.transaction():
        world.store.db.execute("UPDATE principals SET role='agent' WHERE id=?", (operator_id,))
    world.call("POST", "memory-lifecycle:tick", {}, key="tick-key", status=403)
    with world.store.transaction():
        world.store.db.execute("UPDATE principals SET role='admin' WHERE id=?", (operator_id,))
    assert world.call("POST", "memory-lifecycle:tick", {}, key="tick-key") == first


def test_detector_screening_is_durable_across_ticks(builder):
    """Unchanged sources never re-invoke the detector; crash-held pairs stay held."""
    world = builder(policy=policy(), detector=injected_detector(verdict="none"))
    token = world.credential(world.agent())
    semantic_pair(world, token,
                  "The sync service stores checkpoint metadata in the primary region",
                  "The sync service stores checkpoint metadata in the secondary region")
    calls = []

    def counting_detector(left_text, right_text):
        calls.append(1)
        return {"verdict": "none", "confidence": 0.4, "evidence": "", "limitations": []}

    world.automation.detector = counting_detector
    body = {"project_id": world.project}
    first = world.call("POST", "memory-lifecycle:scan-contradictions", body, key="k1")
    assert first["candidates"] == [] and len(calls) == 1
    # New operator key authorizes a new pass, but the screened pair is not re-invoked.
    second = world.call("POST", "memory-lifecycle:scan-contradictions", body, key="k2")
    assert second["candidates"] == [] and second["screened"] == 1 and len(calls) == 1

    # A detector failure holds the pair: a retry with a new key does not re-invoke.
    def exploding_detector(left_text, right_text):
        calls.append(1)
        raise RuntimeError("detector crashed")

    world.automation.detector = exploding_detector
    other = world.agent("Other")
    token_b = world.credential(other)
    semantic_pair(world, token_b,
                  "Billing retention keeps invoice archives indefinitely",
                  "Billing retention keeps invoice archives forever")
    world.call("POST", "memory-lifecycle:scan-contradictions", body, key="k3", status=503)
    assert len(calls) == 2
    world.automation.detector = counting_detector
    world.call("POST", "memory-lifecycle:scan-contradictions", body, key="k4")
    assert len(calls) == 2  # the crashed pair stays held, not silently re-opened
    rows = world.store.db.execute(
        "SELECT resource, count(*) AS n FROM memory_lifecycle_fingerprints WHERE kind='screen'"
        " GROUP BY resource").fetchall()
    assert {row["resource"]: row["n"] for row in rows} == {"clear": 1, "unknown": 1}
