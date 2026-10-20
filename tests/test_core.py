import json
import threading
import time

from hivemind import models as m
from hivemind.storage import now, uid


def event(world, agent, run, sequence=0, suffix="file.changed", data=None, event_id=None, source="source"):
    return {"event_id": event_id or uid("evt"), "type": "hivemind.context." + suffix,
            "agent_id": agent["id"], "run_id": run["id"], "project_id": world.project, "occurred_at": now(),
            "source": {"source_id": source, "kind": "runtime_hook", "sequence": sequence, "attribution": "confirmed"},
            "data": data or {"path": "src/header.ts", "change": "modified"}}


def test_event_dedup_ordering_partial_and_multiple_runs(world):
    agent = world.agent()
    token = world.credential(agent)
    first, second = world.run(agent, token), world.run(agent, token)
    evt = event(world, agent, first)
    accepted = world.call("POST", "events:batch", {"events": [evt]}, token)
    assert accepted[0]["accepted"]
    assert world.call("POST", "events:batch", {"events": [evt]}, token) == accepted
    changed = {**evt, "data": {"path": "src/footer.ts", "change": "modified"}}
    assert world.call("POST", "events:batch", {"events": [changed]}, token)[0]["error"] == "idempotency_conflict"
    end = event(world, agent, first, 2, "tool.completed", {"call_id": "call", "success": True})
    start = event(world, agent, first, 1, "tool.started", {"call_id": "call", "name": "edit"})
    assert all(x["accepted"] for x in world.call("POST", "events:batch", {"events": [end, start]}, token))
    run = world.call("GET", f"runs/{first['id']}", token=token)
    assert run["tools"]["call"]["state"] == "completed"
    assert run["capture_health"]["state"] == "gaps"
    roster = world.call("GET", f"ledger?project_id={world.project}", token=token)["runs"]
    assert {r["id"] for r in roster} == {first["id"], second["id"]}
    good = event(world, agent, second, source="second")
    bad = {**event(world, agent, first, 3), "agent_id": "missing"}
    result = world.call("POST", "events:batch", {"events": [good, bad]}, token)
    assert result[0]["accepted"] and not result[1]["accepted"]


def test_identity_paths_origin_and_protocol(world):
    a, b = world.agent(), world.agent()
    token = world.credential(a, capture=False)
    run = world.run(a, token)
    evt = event(world, a, run)
    assert world.call("POST", "events:batch", {"events": [evt]}, token)[0]["error"] == "unregistered_capture_authority"
    world.call("POST", "runs", {"project_id": world.project, "agent_id": b["id"]}, token, status=403)
    for path in ("../secret", "C:\\secret", "\\\\host\\share", "/secret", "src/../../secret"):
        evt["data"]["path"] = path
        assert world.call("POST", "events:batch", {"events": [evt]}, token)[0]["error"] == "invalid_request"
    evt["data"]["path"] = "src/ok"
    evt["protocol_version"] = "9.0"
    assert world.call("POST", "events:batch", {"events": [evt]}, token)[0]["error"] == "invalid_request"
    response = world.client.post("/v1/session", headers={
        "Authorization": "Bearer " + world.owner, "Origin": "https://attacker.example"}, json={})
    assert response.status_code == 403
    world.call("POST", "events:batch", {"events": [event(world, a, run)]}, token, status=200)


def test_exact_structural_claims_and_scoped_permits(world):
    a, b = world.agent(), world.agent()
    ra, rb = world.run(a), world.run(b)
    parent = world.scope(paths=["src/header"])
    child = world.scope("website/header/menu", parent["id"])
    world.claim(a, ra, parent)
    denial = world.claim(b, rb, child, status=409)
    assert denial["denied"]
    assert denial["conflict"]["policy_version"] == 1
    alias = world.scope("different-name/same-files", paths=["src/header/subdirectory"])
    world.claim(b, rb, alias, status=409)
    world.call("POST", f"runs/{ra['id']}/permits:check",
               {"scope_ids": [parent["id"]], "paths": ["src/header/menu.ts"]})
    world.call("POST", f"runs/{ra['id']}/permits:check",
               {"scope_ids": [parent["id"]], "paths": ["src/footer.ts"]}, status=403)
    world.call("POST", f"runs/{rb['id']}/permits:check", {"scope_ids": [parent["id"]]}, status=403)


def test_atomic_simultaneous_claims(world):
    from hivemind.storage import Problem
    a, b = world.agent(), world.agent()
    ra, rb = world.run(a), world.run(b)
    scope = world.scope()
    service = world.app.state.service
    principal = dict(world.app.state.store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    results = []
    def claim(agent, run):
        request = m.ClaimCreate(project_id=world.project, agent_id=agent["id"], run_id=run["id"],
                                scope_ids=[scope["id"]], intent="Same work")
        try:
            results.append(service.mutate(principal, "claims", uid("key"), request.model_dump(),
                                           lambda: service.claim(principal, request)))
        except Problem as exc:
            results.append(exc)
    threads = [threading.Thread(target=claim, args=pair) for pair in ((a, ra), (b, rb))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(results) == 2
    assert sum(isinstance(r, dict) and r.get("denied", False) for r in results) == 1


def test_direct_messages_mentions_and_observer_isolation(world):
    a, b, c = world.agent("Same name"), world.agent("Same name"), world.agent("Unrelated")
    ta, tb, tc = world.credential(a), world.credential(b), world.credential(c)
    ra, rb = world.run(a, ta), world.run(b, tb)
    message = {"message_id": uid("msg"), "project_id": world.project, "from": a["id"], "to": b["id"],
               "type": "question", "thread_id": uid("thread"),
               "content": {"media_type": "text/plain", "text": "What is the API contract?"}}
    accepted = world.call("POST", "messages", message, ta)
    assert world.call("POST", "messages", message, ta) == accepted
    world.call("POST", "messages", {**message, "message_id": uid("msg"), "from": b["id"]}, ta, status=403)
    world.call("GET", f"agents/{b['id']}/inbox?project_id={world.project}", token=tc, status=403)
    observer = world.credential(role="observer")
    assert world.call("GET", f"projects/{world.project}/conversations", token=observer)["items"]
    human = world.credential(role="human")
    world.call("GET", f"projects/{world.project}/conversations", token=human, status=403)
    world.call("POST", f"messages/{accepted['id']}:claim", {"run_id": ra["id"]}, ta, status=404)
    delivery = world.call("POST", f"messages/{accepted['id']}:claim", {"run_id": rb["id"]}, tb)
    assert delivery["state"] == "delivered"
    assert world.call("POST", f"messages/{accepted['id']}:ack", {"run_id": rb["id"]}, tb)["state"] == "acked"
    post = world.post(token=human, segments=[{"kind": "agent_mention", "agent_id": a["id"]},
                                            {"kind": "text", "text": " Can you explain progress?"}])
    assert post["author"]["kind"] == "human"
    notes = world.call("GET", f"agents/{a['id']}/notifications?project_id={world.project}", token=ta)
    assert notes[0]["post_id"] == post["id"]
    shared = world.call("GET", f"boards/{world.board()['id']}/posts", token=tb)["items"]
    assert shared[0]["id"] == post["id"]
    world.call("POST", f"boards/{world.board()['id']}/posts", {
        "client_post_id": uid("client"), "thread_id": post["thread_id"], "parent_post_id": post["id"],
        "content": {"media_type": "text/plain", "text": "I can help too."}}, tb)


def test_memory_verification_deletion_and_lexical_fallback(world):
    a = world.agent()
    token = world.credential(a)
    world.run(a, token)
    post = world.post()
    memory = world.call("POST", "memories", {"project_id": world.project, "title": "Header contract",
        "content": {"media_type": "text/plain", "text": "Header includes navigation."},
        "scope_owner_id": world.project, "evidence_refs": [{"kind": "board_post", "id": post["id"],
                                                          "revision": post["revision"]}]}, token)
    world.call("POST", f"memories/{memory['id']}:verify", {"expected_revision": 1}, token, status=403)
    world.call("POST", f"memories/{memory['id']}:verify", {"expected_revision": 1})
    result = world.call("POST", "context:query", {"project_id": world.project,
        "agent_selector": {"agent_ids": [a["id"]]}, "token_budget": 6000}, token)
    assert any(i["kind"] == "memory" for i in result["items"])
    assert result["token_estimate"] <= 6000
    search = world.call("POST", "search", {"project_id": world.project, "query": "navigation"}, token)
    assert search["lexical_fallback"] and search["results"]
    world.call("DELETE", f"posts/{post['id']}", {"expected_revision": post["revision"]})
    result = world.call("POST", "context:query", {"project_id": world.project,
        "agent_selector": {"agent_ids": [a["id"]]}}, token)
    assert not any(i["kind"] == "memory" for i in result["items"])
    assert not world.call("GET", f"memories?project_id={world.project}", token=token)
    world.call("POST", "context:query", {"project_id": world.project,
        "agent_selector": {"semantic_query": " "}}, token, status=422)


def test_cross_project_and_safe_errors(world):
    foreign = uid("project")
    store = world.app.state.store
    with store.transaction():
        store.db.execute("INSERT INTO projects(id,title) VALUES(?,?)", (foreign, "Private"))
    token = world.credential(role="human", project=foreign)
    post = world.post(text="private content")
    for path in (f"boards?project_id={world.project}", f"agents?project_id={world.project}",
                 f"ledger?project_id={world.project}"):
        world.call("GET", path, token=token, status=404)
    world.call("POST", "search", {"project_id": world.project, "query": "private"}, token, status=404)
    response = world.client.post("/v1/memories", headers={"Authorization": "Bearer " + token},
                                 json={"secret": "DO_NOT_ECHO_ME"})
    assert response.status_code == 422 and "DO_NOT_ECHO_ME" not in response.text
    assert store.db.execute("SELECT count(*) FROM jobs WHERE resource=?", (post["id"],)).fetchone()[0] == 2


def test_nexus_steering_checkpoint_epoch_and_colors(world):
    a, b, nexus = world.agent("Worker A", ["steer", "pause", "resume"]), world.agent(
        "Worker B", ["steer", "pause", "resume"]), world.agent("Coordinator")
    ta, tb, tn = world.credential(a), world.credential(b), world.credential(nexus, coordinator=True)
    ra, rb, rn = world.run(a, ta), world.run(b, tb), world.run(nexus, tn)
    header, footer = world.scope(paths=["src/header"]), world.scope("website/footer", paths=["src/footer"])
    world.claim(a, ra, header, ta)
    world.claim(b, rb, header, tb, status=409)
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Build footer",
                                       "scope_ids": [footer["id"]]})
    world.call("POST", f"projects/{world.project}/nexus:acquire",
               {"agent_id": b["id"], "run_id": rb["id"]}, tb, status=403)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": rn["id"]}, tn)
    request = {"project_id": world.project, "target_run_id": rb["id"], "action": "steer",
               "replacement_task_id": task["id"], "target_run_revision": rb["control_revision"],
               "coordinator_epoch": lease["coordinator_epoch"], "reason": "Preserve header owner"}
    control = world.call("POST", "controls", request, tn)
    world.call("POST", f"controls/{control['id']}:result",
               {"outcome": "applied", "observed_status": "running"}, tb, status=409)
    world.call("POST", f"controls/{control['id']}:ack", {}, tb)
    world.call("POST", f"controls/{control['id']}:result",
               {"outcome": "applied", "observed_status": "running"}, tb, status=422)
    checkpoint = uid("checkpoint")
    world.call("POST", f"controls/{control['id']}:checkpoint", {"checkpoint_id": checkpoint}, tb)
    result = world.call("POST", f"controls/{control['id']}:result",
                        {"outcome": "applied", "observed_status": "running", "checkpoint_id": checkpoint}, tb)
    assert result["state"] == "applied"
    world.call("POST", f"runs/{rb['id']}/permits:check",
               {"scope_ids": [footer["id"]], "paths": ["src/footer/footer.ts"]}, tb)
    world.call("POST", f"projects/{world.project}/nexus:release", {}, tn)
    world.call("POST", "controls", request, tn, status=409)
    assert not any(a["effective_role"] == "nexus" for a in world.call(
        "GET", f"projects/{world.project}/agent-appearances"))


def test_graph_replay_and_deleted_evidence(world):
    a = world.agent()
    run = world.run(a)
    post = world.post(token=world.credential(a), segments=[
        {"kind": "agent_mention", "agent_id": a["id"]}, {"kind": "text", "text": "Shared evidence"}])
    world.app.state.indexes.drain()
    graph = world.call("POST", "graph:query", {"project_id": world.project, "seed_ids": [a["id"]]})
    assert any(e["kind"] == "HAS_RUN" for e in graph["edges"])
    assert any(n["id"] == run["id"] for n in graph["nodes"])
    assert any(e["kind"] == "MENTIONS" for e in graph["edges"])
    with world.app.state.store.transaction():
        world.app.state.store.db.execute("UPDATE jobs SET state='pending' WHERE kind='graph'")
    world.app.state.indexes.drain()
    assert world.call("POST", "graph:query", {"project_id": world.project, "seed_ids": [a["id"]]}) == graph
    world.call("DELETE", f"posts/{post['id']}", {"expected_revision": post["revision"]})
    graph = world.call("POST", "graph:query", {"project_id": world.project, "seed_ids": [a["id"]]})
    assert not any(n["id"] == post["id"] for n in graph["nodes"])


def test_session_csrf_and_no_html_api_fallback(world):
    result = world.call("POST", "session", {})
    response = world.client.post(f"/v1/boards/{world.board()['id']}/posts", json={})
    assert response.status_code == 403
    assert "csrf_token" in result
    assert world.client.get("/v1/no-such-route").status_code == 404


def test_stale_presence_is_not_task_failure(world):
    a = world.agent()
    run = world.run(a)
    store = world.app.state.store
    with store.transaction():
        body = store.get(run["id"], dict(store.db.execute("SELECT * FROM principals LIMIT 1").fetchone()))
        body["last_seen_at"] = time.time() - 100
        store.db.execute("UPDATE resources SET body=? WHERE id=?", (json.dumps(body), run["id"]))
    ledger = world.call("GET", f"ledger?project_id={world.project}")["runs"][0]
    assert ledger["presence"] == "offline" and ledger["status"] == "running"
