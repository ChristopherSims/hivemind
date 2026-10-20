import json

import pytest

from hivemind.storage import Problem, Store, uid
from test_core import event


def members(world):
    a, b, c = [world.agent(name) for name in ("Writer", "Reader", "Outsider")]
    return [(agent, world.credential(agent)) for agent in (a, b, c)]


def test_operator_access_catalog_is_bounded_project_scoped_and_metadata_only(world):
    agent, token = members(world)[0]
    first = board(world, {})
    second = board(world, {"reader_agent_ids": [agent["id"]]})
    item = post(world, second["id"], text="catalog must not include this body")
    catalog = world.call("GET", f"projects/{world.project}/access-resources?kind=board&limit=1")
    assert len(catalog["resources"]) == 1 and catalog["has_more"]
    assert set(catalog["resources"][0]) == {"id", "project_id", "kind", "title"}
    assert catalog["resources"][0]["project_id"] == world.project
    posts = world.call("GET", f"projects/{world.project}/access-resources?kind=board_post")
    assert item["id"] in json.dumps(posts)
    assert "catalog must not include this body" not in json.dumps(posts)
    assert first["id"] not in json.dumps(posts)
    world.call("GET", f"projects/{world.project}/access-resources", token=token, status=403)
    world.call("GET", f"projects/{world.project}/access-resources?kind=run", status=422)
    world.call("GET", f"projects/{world.project}/access-resources?limit=201", status=422)


def team(world, agents):
    return world.call("POST", "teams", {"project_id": world.project, "display_name": "Engineering",
                                       "member_agent_ids": [a["id"] for a in agents]})


def board(world, access):
    return world.call("POST", "boards", {"project_id": world.project, "title": "Restricted", "access": access})


def post(world, identity, token=None, text="secret orchard plans", key=None, **extra):
    return world.call("POST", f"boards/{identity}/posts", {
        "client_post_id": key or uid("client"), "kind": "finding",
        "content": {"media_type": "text/plain", "text": text}, **extra}, token)


def test_team_board_read_contribute_and_operator_metadata(world):
    (a, at), (b, bt), (c, ct) = members(world)
    writers = team(world, [a])
    readers = team(world, [b])
    restricted = board(world, {"contributor_team_ids": [writers["id"]], "reader_team_ids": [readers["id"]]})
    item = post(world, restricted["id"], at)
    for token in (at, bt):
        boards = world.call("GET", f"boards?project_id={world.project}", token=token)
        assert restricted["id"] in [x["id"] for x in boards]
        assert item["id"] in json.dumps(world.call("GET", f"boards/{restricted['id']}/posts", token=token))
    assert restricted["id"] not in json.dumps(world.call("GET", f"boards?project_id={world.project}", token=ct))
    world.call("GET", f"boards/{restricted['id']}/posts", token=ct, status=404)
    world.call("POST", f"boards/{restricted['id']}/posts", {
        "client_post_id": uid("client"), "kind": "finding",
        "content": {"media_type": "text/plain", "text": "readonly"}}, bt, status=403)
    world.call("POST", "teams", {"project_id": world.project, "display_name": "Escalate"}, ct, status=403)
    world.call("PUT", f"resources/{restricted['id']}/access", {"public_contribute": True}, ct, status=403)
    world.call("GET", f"projects/{world.project}/teams", token=at, status=403)
    store = world.app.state.store
    actor = {"role": "agent", "project": world.project, "agent": a["id"]}
    assert not store.list("team", world.project, actor)
    assert not store.list("resource_access", world.project, actor)


def test_membership_revocation_filters_search_context_and_replay(world):
    (a, at), _, _ = members(world)
    group = team(world, [a])
    restricted = board(world, {"contributor_team_ids": [group["id"]]})
    key = uid("client")
    item = post(world, restricted["id"], at, key=key)
    query = {"project_id": world.project, "query": "orchard"}
    assert item["id"] in json.dumps(world.call("POST", "search", query, at))
    world.call("PATCH", f"teams/{group['id']}", {
        "project_id": world.project, "display_name": group["display_name"],
        "member_agent_ids": [], "expected_revision": 1})
    assert item["id"] not in json.dumps(world.call("POST", "search", query, at))
    assert "orchard" not in json.dumps(world.call("POST", "context:query", {
        "project_id": world.project, "agent_selector": {"agent_ids": [a["id"]]},
        "include": ["board", "graph"], "token_budget": 4000}, at))
    world.call("POST", f"boards/{restricted['id']}/posts", {
        "client_post_id": key, "kind": "finding",
        "content": {"media_type": "text/plain", "text": "secret orchard plans"}}, at, status=404)
    world.call("GET", f"commands/receipt?operation=post:{restricted['id']}&key={key}", token=at, status=404)


def test_read_only_downgrade_blocks_owned_edit_delete_and_replay(world):
    (a, at), _, _ = members(world)
    restricted = board(world, {"contributor_agent_ids": [a["id"]]})
    key = uid("client")
    item = post(world, restricted["id"], at, key=key)
    policy = world.call("GET", f"resources/{restricted['id']}/access")
    world.call("PUT", f"resources/{restricted['id']}/access", {
        "expected_revision": policy["revision"], "reader_agent_ids": [a["id"]]})
    world.call("PATCH", f"posts/{item['id']}", {
        "expected_revision": 1, "content": {"media_type": "text/plain", "text": "overwrite"}}, at, status=403)
    world.call("DELETE", f"posts/{item['id']}", {"expected_revision": 1}, at, status=403)
    world.call("POST", f"boards/{restricted['id']}/posts", {
        "client_post_id": key, "kind": "finding",
        "content": {"media_type": "text/plain", "text": "secret orchard plans"}}, at, status=403)


def test_mentions_require_recipient_board_access_and_withdraw_notifications(world):
    (a, at), (b, bt), (c, _) = members(world)
    group = team(world, [a, b])
    restricted = board(world, {"contributor_team_ids": [group["id"]]})
    options = world.call("GET", f"projects/{world.project}/mention-options?board_id={restricted['id']}", token=at)
    assert {x["id"] for x in options} == {a["id"], b["id"]}
    payload = {"client_post_id": uid("client"), "kind": "finding", "content": {
        "media_type": "application/vnd.hivemind.chat+json",
        "segments": [{"kind": "agent_mention", "agent_id": c["id"]}]}}
    world.call("POST", f"boards/{restricted['id']}/posts", payload, at, status=422)
    payload["content"]["segments"][0]["agent_id"] = b["id"]
    world.call("POST", f"boards/{restricted['id']}/posts", payload, at)
    store = world.app.state.store
    actor = {"role": "agent", "project": world.project, "agent": b["id"]}
    assert len(store.list("notification", world.project, actor)) == 1
    world.call("PATCH", f"teams/{group['id']}", {
        "project_id": world.project, "display_name": "Engineering",
        "member_agent_ids": [a["id"]], "expected_revision": 1})
    assert not store.list("notification", world.project, actor)
    world.call("GET", f"boards/{restricted['id']}/posts", token=bt, status=404)


def test_individual_post_restrictions_intersect_with_board_and_evidence(world):
    (a, at), (b, bt), _ = members(world)
    restricted = board(world, {"contributor_agent_ids": [a["id"], b["id"]]})
    item = post(world, restricted["id"], at)
    world.call("PUT", f"resources/{item['id']}/access", {"reader_agent_ids": [a["id"]]})
    assert item["id"] not in json.dumps(world.call("GET", f"boards/{restricted['id']}/posts", token=bt))
    public = world.board()
    derived = post(world, public["id"], at, evidence_refs=[
        {"id": item["id"], "kind": "board_post", "revision": 1}])
    assert derived["id"] not in json.dumps(world.call("GET", f"boards/{public['id']}/posts", token=bt))
    world.call("PUT", f"resources/{derived['id']}/access", {"public_read": True})
    assert derived["id"] not in json.dumps(world.call("GET", f"boards/{public['id']}/posts", token=bt))


def test_policy_and_membership_cas_atomic_creation_and_input_limits(world):
    (a, at), _, _ = members(world)
    group = team(world, [a])
    world.call("PATCH", f"teams/{group['id']}", {
        "project_id": world.project, "display_name": "Engineering", "member_agent_ids": [],
        "expected_revision": 5}, status=412)
    world.call("POST", "teams", {"project_id": world.project, "display_name": "Duplicates",
                                "member_agent_ids": [a["id"], a["id"]]}, status=422)
    before = world.call("GET", f"boards?project_id={world.project}")
    world.call("POST", "boards", {"project_id": world.project, "title": "Fail",
                                 "access": {"reader_team_ids": ["missing"]}}, status=404)
    assert world.call("GET", f"boards?project_id={world.project}") == before
    restricted = board(world, {})
    world.call("GET", f"boards/{restricted['id']}/posts", token=at, status=404)
    world.call("PUT", f"resources/{restricted['id']}/access", {"public_read": True}, status=412)
    assert world.call("GET", f"resources/{restricted['id']}/access")["revision"] == 1


def test_team_access_survives_store_restart(world):
    (a, _), (b, _), _ = members(world)
    restricted = board(world, {"reader_agent_ids": [a["id"]]})
    other = Store(world.app.state.store.directory)
    try:
        for agent, allowed in ((a, True), (b, False)):
            actor = {"role": "agent", "project": world.project, "agent": agent["id"]}
            assert other.visible(other.raw(restricted["id"]), actor) is allowed
    finally:
        other.close()


def test_private_resources_and_cross_project_grants_cannot_be_widened(world):
    (a, _), (b, _), _ = members(world)
    store = world.app.state.store
    with store.transaction():
        row = store.save("artifact", world.project, "operator", {
            "id": uid("artifact"), "project_id": world.project, "title": "Private"}, private=a["id"])
    world.call("PUT", f"resources/{row['id']}/access", {"public_read": True, "public_contribute": True})
    actor = {"role": "agent", "project": world.project, "agent": b["id"]}
    with pytest.raises(Problem):
        store.get(row["id"], actor)
    with store.transaction():
        project = uid("project")
        store.db.execute("INSERT INTO projects(id,title) VALUES(?,?)", (project, "Other"))
        foreign = store.save("team", project, "operator", {
            "id": uid("team"), "project_id": project, "member_agent_ids": [a["id"]], "active": True})
    world.call("PUT", f"resources/{world.board()['id']}/access",
               {"reader_team_ids": [foreign["id"]]}, status=404)


def test_replies_inherit_parent_policy_and_cannot_notify_excluded_agent(world):
    (a, at), (b, bt), _ = members(world)
    public = world.board()
    root = post(world, public["id"], at)
    world.call("PUT", f"resources/{root['id']}/access", {"contributor_agent_ids": [a["id"]]})
    child = post(world, public["id"], at, parent_post_id=root["id"], thread_id=root["thread_id"])
    assert child["id"] not in json.dumps(world.call("GET", f"boards/{public['id']}/posts", token=bt))
    world.call("POST", f"boards/{public['id']}/posts", {
        "client_post_id": uid("client"), "kind": "finding",
        "parent_post_id": root["id"], "thread_id": root["thread_id"],
        "content": {"media_type": "application/vnd.hivemind.chat+json", "segments": [
            {"kind": "agent_mention", "agent_id": b["id"]}]}}, at, status=422)


def test_task_policies_protect_linked_runs_events_and_write_boundaries(world):
    (a, at), (b, bt), _ = members(world)
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "secret task"})
    world.call("PUT", f"resources/{task['id']}/access", {"contributor_agent_ids": [a["id"]]})
    run = world.call("POST", "runs", {"project_id": world.project, "agent_id": a["id"], "task_id": task["id"]}, at)
    value = event(world, a, run)
    assert world.call("POST", "events:batch", {"events": [value]}, at)[0]["accepted"]
    assert value["event_id"] in json.dumps(world.call("GET", f"events?project_id={world.project}", token=at))
    assert value["event_id"] not in json.dumps(world.call("GET", f"events?project_id={world.project}", token=bt))
    assert run["id"] not in json.dumps(world.call("GET", f"ledger?project_id={world.project}", token=bt))
    world.call("GET", f"tasks/{task['id']}", token=bt, status=404)
    world.call("PUT", f"resources/{task['id']}/access", {
        "expected_revision": 1, "reader_agent_ids": [a["id"], b["id"]]})
    denied = world.call("POST", "events:batch", {"events": [event(world, a, run, 1)]}, at)
    assert denied[0]["error"] == "resource_contribution_forbidden"
    denied = world.call("POST", "events:batch", {"events": [value]}, at)
    assert denied[0]["error"] == "resource_contribution_forbidden"
    other_run = world.run(b, bt)
    world.call("POST", f"tasks/{task['id']}:start", {
        "expected_revision": 1, "run_id": other_run["id"]}, bt, status=403)
    current = world.call("GET", f"tasks/{task['id']}", token=bt)
    assert current["status"] == task["status"] and current["revision"] == task["revision"]


def test_artifact_and_memory_policies_filter_sources_versions_and_search(world):
    (a, at), (b, bt), _ = members(world)
    source = world.post("orchard source")
    memory = world.call("POST", "memories", {"project_id": world.project, "title": "orchard memory",
        "scope_kind": "project", "scope_owner_id": world.project,
        "content": {"media_type": "text/plain", "text": "orchard knowledge"},
        "evidence_refs": [{"kind": "board_post", "id": source["id"], "revision": 1}]})
    world.call("POST", f"memories/{memory['id']}:verify", {"expected_revision": 1})
    world.call("PUT", f"resources/{memory['id']}/access", {"reader_agent_ids": [a["id"]]})
    world.call("GET", f"memories/{memory['id']}", token=bt, status=404)
    world.call("GET", f"memories/{memory['id']}/versions", token=bt, status=404)
    assert memory["id"] not in json.dumps(world.call("POST", "search", {
        "project_id": world.project, "query": "orchard"}, bt))
    world.call("GET", f"memories/{memory['id']}", token=at)
    world.call("POST", f"memories/{memory['id']}/versions", {
        "expected_revision": 2, "title": "overwrite", "content": {"media_type": "text/plain", "text": "new"},
        "evidence_refs": [{"kind": "board_post", "id": source["id"], "revision": 1}]}, at, status=403)
    store = world.app.state.store
    with store.transaction():
        artifact = store.save("artifact", world.project, "operator", {
            "id": uid("artifact"), "project_id": world.project, "title": "orchard artifact",
            "evidence_refs": [{"kind": "memory", "id": memory["id"]}]})
    world.call("GET", f"artifacts/{artifact['id']}", token=bt, status=404)
    world.call("PUT", f"resources/{artifact['id']}/access", {"public_read": True})
    world.call("GET", f"artifacts/{artifact['id']}", token=bt, status=404)
    world.call("GET", f"artifacts/{artifact['id']}", token=at)


def test_inactive_or_deleted_teams_fail_closed(world):
    (a, at), _, _ = members(world)
    group = team(world, [a])
    restricted = board(world, {"reader_team_ids": [group["id"]]})
    world.call("PATCH", f"teams/{group['id']}", {
        "project_id": world.project, "display_name": "Engineering", "member_agent_ids": [a["id"]],
        "expected_revision": 1, "active": False})
    world.call("GET", f"boards/{restricted['id']}/posts", token=at, status=404)
    world.call("PATCH", f"teams/{group['id']}", {
        "project_id": world.project, "display_name": "Engineering", "member_agent_ids": [a["id"]],
        "expected_revision": 2, "active": True})
    world.call("GET", f"boards/{restricted['id']}/posts", token=at)
    store = world.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET deleted=1 WHERE id=?", (group["id"],))
    world.call("GET", f"boards/{restricted['id']}/posts", token=at, status=404)


def test_public_read_and_explicit_writer_grants(world):
    (a, at), _, (c, ct) = members(world)
    restricted = board(world, {"public_read": True, "contributor_agent_ids": [a["id"]]})
    post(world, restricted["id"], at)
    world.call("GET", f"boards/{restricted['id']}/posts", token=ct)
    observer = world.credential(role="observer")
    world.call("GET", f"boards/{restricted['id']}/posts", token=observer)
    world.call("POST", f"boards/{restricted['id']}/posts", {
        "client_post_id": uid("client"), "kind": "finding",
        "content": {"media_type": "text/plain", "text": "attempt"}}, ct, status=403)
    world.call("PUT", f"resources/{restricted['id']}/access", {
        "expected_revision": 1, "public_contribute": True})
    post(world, restricted["id"], ct)


def test_memory_mirrors_reject_team_restricted_memory_or_sources(world):
    from test_additional_integrations import bridge, memory
    from hivemind.memory_adapters import MemoryMirrorRequest
    item, source = memory(world)
    (a, _), _, _ = members(world)
    mirror, principal = bridge(world, lambda *args: pytest.fail("restricted memory exported"))
    world.call("PUT", f"resources/{item['id']}/access", {"reader_agent_ids": [a["id"]]})
    request = MemoryMirrorRequest(adapter="langmem", expected_revision=item["revision"])
    with pytest.raises(Problem, match="memory_export_source_unavailable"):
        mirror.publish(principal, item["id"], request, "restricted")
    world.call("PUT", f"resources/{item['id']}/access", {"expected_revision": 1, "public_read": True})
    world.call("PUT", f"resources/{source['id']}/access", {"reader_agent_ids": [a["id"]]})
    with pytest.raises(Problem, match="memory_export_private_source"):
        mirror.publish(principal, item["id"], request, "restricted-source")


def test_read_only_producer_cannot_submit_artifact_evidence(world):
    from test_artifact_contracts import completed_task, install_on, json_contract, make_artifact, submit
    install_on(world)
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    task = completed_task(world, agent, token, run)
    artifact = make_artifact(world, token, run, '{"version":1,"name":"Deliverable"}')
    contract = json_contract(world, task)
    world.call("PUT", f"resources/{task['id']}/access", {"reader_agent_ids": [agent["id"]]})
    submit(world, token, contract, task, artifact, status=403)
    assert not world.app.state.store.db.execute(
        "SELECT id FROM resources WHERE kind='artifact_submission'").fetchall()


def test_sdk_team_and_access_facades_use_encoded_paths():
    import httpx
    from hivemind.sdk import Client
    calls = []

    def transport(request):
        calls.append((request.method, request.url.raw_path.decode()))
        return httpx.Response(200, json={"data": {}})

    client = Client("http://test", "test", transport=httpx.MockTransport(transport))
    try:
        client.create_team({"project_id": "p", "display_name": "Team"}, key="create")
        client.teams("p/other")
        client.update_team("team/other", {"expected_revision": 1}, key="update")
        client.create_board({"project_id": "p", "title": "Board"}, key="board")
        client.resource_access("board/other")
        client.set_resource_access("board/other", {"public_read": True}, key="access")
    finally:
        client.close()
    assert [method for method, _ in calls] == ["POST", "GET", "PATCH", "POST", "GET", "PUT"]
    assert calls[-1][1].endswith("/board%2Fother/access")


def test_moving_run_to_public_task_does_not_disclose_private_history(world):
    (a, at), (_, bt), _ = members(world)
    private = world.call("POST", "tasks", {"project_id": world.project, "title": "Private task"})
    public = world.call("POST", "tasks", {"project_id": world.project, "title": "Public task"})
    world.call("PUT", f"resources/{private['id']}/access", {"contributor_agent_ids": [a["id"]]})
    run = world.call("POST", "runs", {
        "project_id": world.project, "agent_id": a["id"], "task_id": private["id"]}, at)
    value = event(world, a, run)
    world.call("POST", "events:batch", {"events": [value]}, at)
    from test_artifact_contracts import make_artifact
    artifact = make_artifact(world, at, run, "Private result")
    store = world.app.state.store
    with store.transaction():
        current = store.get(run["id"], {"role": "admin"})
        current["task_id"] = public["id"]
        store.save("run", world.project, store.raw(run["id"])["owner"], current)
    world.call("GET", f"runs/{run['id']}", token=bt, status=404)
    world.call("GET", f"artifacts/{artifact['id']}", token=bt, status=404)
    assert value["event_id"] not in json.dumps(world.call("GET", f"events?project_id={world.project}", token=bt))
    other = Store(store.directory)
    try:
        actor = {"role": "agent", "agent": a["id"], "project": world.project}
        assert other.visible(other.raw(run["id"]), actor)
        assert other.db.execute("SELECT count(*) FROM run_task_access WHERE run=?", (run["id"],)).fetchone()[0] == 2
    finally:
        other.close()


def test_nexus_team_write_revocation_fences_control_issue_apply_and_replay(world):
    worker = world.agent("Worker", ["pause"])
    nexus = world.agent("Nexus")
    wt, nt = world.credential(worker), world.credential(nexus, coordinator=True)
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Team work"})
    world.call("PUT", f"resources/{task['id']}/access", {
        "contributor_agent_ids": [worker["id"]], "reader_agent_ids": [nexus["id"]]})
    run = world.call("POST", "runs", {
        "project_id": world.project, "agent_id": worker["id"], "task_id": task["id"]}, wt)
    nr = world.run(nexus, nt)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": nr["id"]}, nt)
    request = {"project_id": world.project, "target_run_id": run["id"], "action": "pause",
               "target_run_revision": run["control_revision"], "coordinator_epoch": lease["coordinator_epoch"],
               "reason": "Pause"}
    world.call("POST", "controls", request, nt, status=403)
    world.call("PUT", f"resources/{task['id']}/access", {
        "expected_revision": 1, "contributor_agent_ids": [worker["id"], nexus["id"]]})
    key = uid("key")
    control = world.call("POST", "controls", request, nt, key=key)
    world.call("PUT", f"resources/{task['id']}/access", {
        "expected_revision": 2, "contributor_agent_ids": [worker["id"]], "reader_agent_ids": [nexus["id"]]})
    world.call("POST", "controls", request, nt, key=key, status=403)
    world.call("POST", f"controls/{control['id']}:ack", {}, wt, status=403)
    store = world.app.state.store
    principal = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    with store.transaction():
        world.app.state.service.retire_controls(principal, world.project)
    assert json.loads(store.raw(control["id"])["body"])["state"] == "superseded"
