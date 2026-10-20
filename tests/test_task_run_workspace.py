import pytest
from fastapi import Request

from hivemind.blocked_tasks import BlockedTasksPolicy
from hivemind.storage import uid
from hivemind.task_run_workspace import history_entries, install


def test_history_projection_cannot_forward_arbitrary_nested_fields_or_spoof_kind():
    entries = history_entries({"execution_history": [{
        "id": "execution-synthetic", "kind": "attempt", "progress": 25,
        "summary": {"private_extra": "synthetic-value"},
        "checkpoint": {"extra": "synthetic-value"}, "unknown_field": "synthetic-value",
    }]})
    assert entries == [{"kind": "execution", "id": "execution-synthetic", "progress": 25}]


@pytest.fixture
def workspace(world):
    """Install the workspace read routes the way the integration layer will."""
    app = world.app

    async def principal(request: Request):
        return app.state.authenticate_request(request)

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0",
                "request_id": uid("req"), "data": data}

    def command(*args, **kwargs):  # read-only surface: no command route may exist
        raise AssertionError("task workspace must not expose a command route")

    install(app, app.state.service, principal, command, envelope)
    return world


def make_task(world, title="Task", **extra):
    return world.call("POST", "tasks", {"project_id": world.project, "title": title, **extra})


def test_workspace_page_lists_visible_tasks_with_counts(workspace):
    first = make_task(workspace, "Wire the board header")
    second = make_task(workspace, "Review the diff")
    page = workspace.call("GET", f"projects/{workspace.project}/task-workspace")
    ids = [task["id"] for task in page["tasks"]]
    assert ids == [second["id"], first["id"]]  # newest first, stable
    assert page["task_count"] == 2 and page["tasks_has_more"] is False
    assert page["next_offset"] is None
    assert page["counts"]["total"] == 2
    assert page["counts"]["by_status"] == {"queued": 2}
    assert page["counts"]["truncated"] is False
    assert page["project_id"] == workspace.project
    assert len(page["snapshot_cursor"]) == 64
    task = page["tasks"][0]
    assert task["revision"] == second["revision"]
    assert task["scheduling"]["state"] == "queued"
    assert task["execution"] is None and task["latest_execution"] is None
    assert task["execution_history_count"] == 0
    # The default world policy disables the blocked explanation surface.
    assert "blocked" not in task
    assert page["blocked_requests"] is None


def test_workspace_pages_walk_without_gaps_or_duplicates(workspace):
    tasks = [make_task(workspace, f"Task {index}") for index in range(5)]
    path = f"projects/{workspace.project}/task-workspace"
    page = workspace.call("GET", f"{path}?limit=2")
    collected = page["tasks"]
    assert page["tasks_has_more"] is True and page["next_offset"] == 2
    offset = page["next_offset"]
    while offset is not None:
        page = workspace.call("GET", f"{path}?limit=2&offset={offset}")
        collected = collected + page["tasks"]
        offset = page["next_offset"]
    assert [task["id"] for task in collected] == [task["id"] for task in reversed(tasks)]
    assert len({task["id"] for task in collected}) == len(tasks)
    # Counts always describe the whole visible set, not just the page.
    assert page["counts"]["total"] == 5


def test_workspace_hides_restricted_tasks_and_their_dependency_ids(workspace):
    hidden = make_task(workspace, "Restricted predecessor")
    dependent = make_task(workspace, "Visible dependent")
    plan = workspace.call("POST", "task-plans", {
        "project_id": workspace.project, "title": "Release plan",
        "tasks": [{"key": "a", "title": "Plan task",
                   "external_dependency_ids": [dependent["id"]]}]})
    planned_id = plan["plan"]["task_ids"][0]
    outsider = workspace.agent("Outsider")
    outsider_token = workspace.credential(outsider)
    path = f"projects/{workspace.project}/task-workspace"
    before = workspace.call("GET", path, token=outsider_token)
    assert dependent["id"] in [task["id"] for task in before["tasks"]]
    workspace.call("PUT", f"resources/{dependent['id']}/access",
                   {"public_read": False, "reader_agent_ids": []})
    after = workspace.call("GET", path, token=outsider_token)
    visible_ids = [task["id"] for task in after["tasks"]]
    assert dependent["id"] not in visible_ids
    assert after["counts"]["total"] == len(visible_ids)
    # The hidden task ID must not leak through dependencies or plan task lists.
    planned = next(task for task in after["tasks"] if task["id"] == planned_id)
    assert planned["depends_on"] == []
    assert all(dependent["id"] not in listed["task_ids"] for listed in after["plans"])
    # The owner still sees the dependency edge.
    owner_view = workspace.call("GET", path)
    owned = next(task for task in owner_view["tasks"] if task["id"] == planned_id)
    assert owned["depends_on"] == [dependent["id"]]
    # The never-rerestricted predecessor stays visible to everyone.
    assert hidden["id"] in visible_ids


def test_workspace_plan_task_ids_include_only_visible_tasks(workspace):
    plan = workspace.call("POST", "task-plans", {
        "project_id": workspace.project, "title": "Two step plan",
        "tasks": [{"key": "first", "title": "First"},
                  {"key": "second", "title": "Second", "depends_on": ["first"]}]})
    first_id, second_id = plan["plan"]["task_ids"]
    page = workspace.call("GET", f"projects/{workspace.project}/task-workspace")
    listed = next(item for item in page["plans"] if item["id"] == plan["plan"]["id"])
    assert set(listed["task_ids"]) == {first_id, second_id}
    assert listed["task_count"] == 2
    second = next(task for task in page["tasks"] if task["id"] == second_id)
    assert second["depends_on"] == [first_id]
    assert second["plan_id"] == plan["plan"]["id"] and second["plan_key"] == "second"


def test_workspace_snapshot_cursor_and_input_limits(workspace):
    make_task(workspace)
    path = f"projects/{workspace.project}/task-workspace"
    cursor = workspace.call("GET", path)["snapshot_cursor"]
    assert workspace.call("GET", f"{path}?snapshot_cursor={cursor}")["snapshot_cursor"] == cursor
    workspace.call("GET", f"{path}?snapshot_cursor={'0' * 64}", status=409)
    assert workspace.client.get(f"/v1/{path}").status_code == 401
    for suffix in ("limit=0", "limit=201", "offset=-1", "snapshot_cursor=nothex"):
        workspace.call("GET", f"{path}?{suffix}", status=422)


def test_workspace_history_is_bounded_and_authorized(workspace):
    task = make_task(workspace, "History task")
    path = f"tasks/{task['id']}/workspace-history"
    page = workspace.call("GET", path)
    assert page["entries"] == [] and page["has_more"] is False
    assert page["next_offset"] is None and page["total"] == 0
    assert page["revision"] == task["revision"]
    assert page["basis"] == "agent_self_reports_and_operator_actions"
    assert workspace.client.get(f"/v1/{path}").status_code == 401
    workspace.call("GET", f"tasks/{uid('task')}/workspace-history", status=404)
    workspace.call("GET", f"{path}?limit=0", status=422)
    workspace.call("GET", f"{path}?limit=101", status=422)
    # A restricted task's history is indistinguishable from a missing one.
    workspace.call("PUT", f"resources/{task['id']}/access",
                   {"public_read": False, "reader_agent_ids": []})
    outsider = workspace.credential(workspace.agent("Outsider"))
    workspace.call("GET", path, token=outsider, status=404)


def test_workspace_history_pages_merged_entries(workspace):
    task = make_task(workspace, "Paged history")
    store = workspace.app.state.store
    body = dict(workspace.call("GET", f"tasks/{task['id']}"))
    body["execution_history"] = [
        {"id": uid("execution"), "run_id": uid("run"), "agent_id": "agent-1",
         "started_at": 1000 + index, "progress": index, "outcome": "failed"}
        for index in range(4)]
    row = store.raw(task["id"], "task")
    store.save("task", row["project"], row["owner"], body, expected=row["revision"])
    path = f"tasks/{task['id']}/workspace-history"
    page = workspace.call("GET", f"{path}?limit=3")
    assert page["count"] == 3 and page["has_more"] is True and page["next_offset"] == 3
    assert page["total"] == 4
    assert [entry["progress"] for entry in page["entries"]] == [3, 2, 1]
    rest = workspace.call("GET", f"{path}?limit=3&offset={page['next_offset']}")
    assert rest["has_more"] is False and rest["next_offset"] is None
    assert [entry["progress"] for entry in rest["entries"]] == [0]
    assert all(entry["kind"] == "execution"
               for entry in page["entries"] + rest["entries"])


def test_workspace_history_snapshot_cursor_fencing(workspace):
    task = make_task(workspace, "Fenced history")
    path = f"tasks/{task['id']}/workspace-history"
    page = workspace.call("GET", path)
    assert len(page["snapshot_cursor"]) == 64
    replay = workspace.call("GET", f"{path}?snapshot_cursor={page['snapshot_cursor']}")
    assert replay["snapshot_cursor"] == page["snapshot_cursor"]
    workspace.call("GET", f"{path}?snapshot_cursor={'0' * 64}", status=409)


def test_workspace_omits_run_links_hidden_by_run_task_access(workspace):
    """A run that once touched a restricted task is unreadable to outsiders;
    the workspace must drop the run link while keeping self-report metadata."""
    store = workspace.app.state.store
    admin = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    agent = workspace.agent("Runner")
    visible_task = make_task(workspace, "Visible task")
    hidden_task = make_task(workspace, "Restricted source task")
    run_id = uid("run")
    run_body = {"id": run_id, "project_id": workspace.project, "agent_id": agent["id"],
                "task_id": hidden_task["id"], "status": "running", "last_seen_at": 1,
                "capture_health": {"state": "unknown"}, "tools": {}, "files": {},
                "summary": None, "source_highwater": {}, "control_revision": 1}
    store.save("run", workspace.project, admin["id"], run_body)
    moved = dict(run_body, task_id=visible_task["id"])
    row = store.raw(run_id, "run")
    store.save("run", workspace.project, row["owner"], moved, expected=row["revision"])
    # The visible task's execution and history reference the run and agent.
    task_body = dict(workspace.call("GET", f"tasks/{visible_task['id']}"))
    task_body["status"] = "running"
    task_body["execution"] = {"id": uid("execution"), "run_id": run_id,
                              "agent_id": agent["id"], "started_at": 1767225600,
                              "progress": 40}
    task_body["execution_history"] = [
        {"id": uid("execution"), "run_id": run_id, "agent_id": agent["id"],
         "started_at": 1767220000, "progress": 10, "outcome": "failed"}]
    row = store.raw(visible_task["id"], "task")
    store.save("task", row["project"], row["owner"], task_body, expected=row["revision"])
    workspace.call("PUT", f"resources/{hidden_task['id']}/access",
                   {"public_read": False, "reader_agent_ids": []})
    outsider = workspace.credential(workspace.agent("Outsider"))
    page = workspace.call("GET", f"projects/{workspace.project}/task-workspace",
                          token=outsider)
    view = next(task for task in page["tasks"] if task["id"] == visible_task["id"])
    assert view["execution"]["progress"] == 40  # public self-report metadata stays
    assert view["execution"]["run_id"] is None
    assert view["execution"]["agent_id"] == agent["id"]  # agents remain visible
    assert run_id not in str(view)
    history = workspace.call("GET", f"tasks/{visible_task['id']}/workspace-history",
                             token=outsider)
    assert history["entries"][0]["progress"] == 10
    assert history["entries"][0].get("agent_id") == agent["id"]
    assert "run_id" not in history["entries"][0]
    assert run_id not in str(history)
    # The owner still sees the run link.
    owned = workspace.call("GET", f"tasks/{visible_task['id']}/workspace-history")
    assert owned["entries"][0].get("run_id") == run_id


def test_workspace_includes_blocked_explanation_when_policy_enabled(workspace):
    """An explicit operator opt-in turns on per-task readiness explanations."""
    from hivemind import blocked_tasks
    app = workspace.app
    # Reinstall blocked tasks with explanations enabled (replaces only its routes).
    async def principal(request: Request):
        return app.state.authenticate_request(request)

    def envelope(data):
        return {"data": data}

    def command(p, route, key, body, fn):
        return envelope(fn())

    blocked_tasks.install(app, app.state.service, principal, command, envelope,
                          policy=BlockedTasksPolicy(explain_enabled=True,
                                                    requests_enabled=True))
    task = make_task(workspace, "Explained task")
    page = workspace.call("GET", f"projects/{workspace.project}/task-workspace")
    view = next(item for item in page["tasks"] if item["id"] == task["id"])
    assert view["blocked"]["readiness"] == "startable"
    assert view["blocked"]["blocker_count"] == 0
    assert view["blocked"]["authority"] == "explanation_snapshot_not_execution_permission"
    assert page["blocked_requests"] == []
