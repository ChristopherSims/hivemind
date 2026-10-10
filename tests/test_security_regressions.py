from hivemind.api import hash_token
from hivemind.storage import uid


def memory(world, post, token, private=False):
    return world.call("POST", "memories", {
        "project_id": world.project, "title": "Evidence-derived fact",
        "scope_owner_id": world.project, "private": private,
        "content": {"media_type": "text/plain", "text": "Navigation contract"},
        "evidence_refs": [{"kind": "memory" if "memory_version_id" in post else "board_post", "id": post["id"]}],
    }, token)


def test_private_source_cannot_be_paraphrased_publicly_or_traversed(world):
    a, b = world.agent(), world.agent()
    ta, tb = world.credential(a), world.credential(b)
    post = world.post()
    private = memory(world, post, ta, True)
    world.call("POST", "memories", {
        "project_id": world.project, "title": "Public paraphrase", "scope_owner_id": world.project,
        "content": {"media_type": "text/plain", "text": "I paraphrased private source"},
        "evidence_refs": [{"kind": "memory", "id": private["id"]}],
    }, ta, status=403)
    world.app.state.indexes.drain()
    graph = world.call("POST", "graph:query", {"project_id": world.project, "seed_ids": [post["id"]]}, tb)
    assert private["id"] not in {n["id"] for n in graph["nodes"]}
    assert private["id"] not in {h["resource_id"] for h in world.call("POST", "search", {
        "project_id": world.project, "query": "Navigation"}, tb)["results"]}


def test_transitive_source_deletion_invalidates_all_derived_memory(world):
    a = world.agent()
    token = world.credential(a)
    post = world.post()
    first = memory(world, post, token)
    second = memory(world, first, token)
    world.call("DELETE", f"posts/{post['id']}", {"expected_revision": 1})
    assert not world.call("GET", f"memories?project_id={world.project}", token=token)
    for resource in (first, second):
        row = world.app.state.store.raw(resource["id"])
        assert '"source_deleted"' in row["body"]
        assert world.app.state.store.db.execute(
            "SELECT count(*) FROM jobs WHERE resource=? AND revision=?",
            (resource["id"], row["revision"])).fetchone()[0] == 2


def test_message_id_cannot_be_overwritten_by_another_principal(world):
    a, b = world.agent(), world.agent()
    ta, tb = world.credential(a), world.credential(b)
    payload = {"message_id": uid("msg"), "project_id": world.project, "from": a["id"], "to": b["id"],
               "type": "inform", "thread_id": uid("thread"),
               "content": {"media_type": "text/plain", "text": "Original statement"}}
    world.call("POST", "messages", payload, ta)
    world.call("POST", "messages", {**payload, "from": b["id"], "to": a["id"],
        "content": {"media_type": "text/plain", "text": "Forged replacement"}}, tb, status=409)


def test_deleted_post_cannot_be_recovered_from_idempotency_cache(world):
    board = world.board()
    key = uid("client")
    payload = {"client_post_id": key, "content": {"media_type": "text/plain", "text": "Remove this evidence"}}
    post = world.call("POST", f"boards/{board['id']}/posts", payload)
    world.call("DELETE", f"posts/{post['id']}", {"expected_revision": 1})
    result = world.call("POST", f"boards/{board['id']}/posts", payload, status=404)
    assert "Remove this evidence" not in str(result)


def test_canonical_context_and_claims_work_with_graph_outage(world):
    a, b = world.agent(), world.agent()
    ra, rb = world.run(a), world.run(b)
    scope = world.scope()
    world.claim(a, ra, scope)
    indexes = world.app.state.indexes
    graph = indexes.graph
    indexes.graph = None
    try:
        context = world.call("POST", "context:query", {"project_id": world.project,
            "agent_selector": {"agent_ids": [a["id"]]}})
        assert context["selected_agents"][0]["agent_id"] == a["id"]
        assert "canonical relationship fallback" in str(context["warnings"])
        world.claim(b, rb, scope, status=409)
    finally:
        indexes.graph = graph


def test_heartbeat_does_not_break_pending_control_revision(world):
    a = world.agent("Controlled", ["pause"])
    n = world.agent("Nexus")
    ta, tn = world.credential(a), world.credential(n, coordinator=True)
    ra, rn = world.run(a, ta), world.run(n, tn)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": n["id"], "run_id": rn["id"]}, tn)
    control = world.call("POST", "controls", {
        "project_id": world.project, "target_run_id": ra["id"], "action": "pause",
        "target_run_revision": ra["control_revision"], "coordinator_epoch": lease["coordinator_epoch"],
        "reason": "Safe boundary"}, tn)
    world.call("POST", f"runs/{ra['id']}/heartbeat", {}, ta)
    world.call("POST", f"controls/{control['id']}:ack", {}, ta)


def test_old_epoch_and_pending_control_are_fenced_after_takeover(world):
    target, first, second = world.agent("Target", ["pause"]), world.agent("First Nexus"), world.agent("Second Nexus")
    tt, tf, ts = (world.credential(target), world.credential(first, coordinator=True),
                  world.credential(second, coordinator=True))
    rt, rf, rs = world.run(target, tt), world.run(first, tf), world.run(second, ts)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": first["id"], "run_id": rf["id"]}, tf)
    payload = {"project_id": world.project, "target_run_id": rt["id"], "action": "pause",
               "target_run_revision": rt["control_revision"], "coordinator_epoch": lease["coordinator_epoch"],
               "reason": "Coordinator pause"}
    queued = world.call("POST", "controls", payload, tf)
    world.call("POST", f"projects/{world.project}/nexus:release", {}, tf)
    replacement = world.call("POST", f"projects/{world.project}/nexus:acquire",
                            {"agent_id": second["id"], "run_id": rs["id"]}, ts)
    assert replacement["coordinator_epoch"] > lease["coordinator_epoch"]
    world.call("POST", f"controls/{queued['id']}:ack", {}, tt, status=409)
    world.call("POST", "controls", payload, tf, status=409)
    world.call("POST", "controls", {**payload, "coordinator_epoch": replacement["coordinator_epoch"]}, tf, status=403)


def test_project_binding_changes_deny_cached_mutation_replays(world):
    a = world.agent()
    token = world.credential(a)
    key = uid("key")
    payload = {"project_id": world.project, "agent_id": a["id"]}
    world.call("POST", "runs", payload, token, key)
    store = world.app.state.store
    with store.transaction():
        other = uid("project")
        store.db.execute("INSERT INTO projects(id,title) VALUES(?,?)", (other, "Other project"))
        store.db.execute("UPDATE principals SET project=? WHERE token_hash=?", (other, hash_token(token)))
    world.call("POST", "runs", payload, token, key, status=404)


def test_wrong_evidence_kind_cannot_be_forged(world):
    post = world.post()
    world.call("POST", "memories", {
        "project_id": world.project, "title": "False evidence", "scope_owner_id": world.project,
        "content": {"media_type": "text/plain", "text": "Supposed artifact"},
        "evidence_refs": [{"kind": "artifact", "id": post["id"]}],
    }, status=422)
