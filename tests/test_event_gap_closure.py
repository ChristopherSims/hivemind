"""Plan 7.1 gap closure: tool.progressed, vcs.observed, conversation/blocker/correction events.

Each new event type is validated as agent-reported evidence, reducer-applied to the
run's canonical state (bounded lists), and never treated as a permission grant or
confirmed filesystem effect.
"""
from hivemind.storage import now, uid


def event(world, agent, run, sequence=0, suffix="tool.progressed", data=None, source="src"):
    return {"event_id": uid("evt"), "type": "hivemind.context." + suffix,
            "agent_id": agent["id"], "run_id": run["id"], "project_id": world.project,
            "occurred_at": now(),
            "source": {"source_id": source, "kind": "runtime_hook",
                       "sequence": sequence, "attribution": "confirmed"},
            "data": data}


def post_event(world, token, *events):
    return world.call("POST", "events:batch", {"events": list(events)}, token)


def run_view(world, agent, run, token):
    body = world.call("GET", f"runs/{run['id']}", token=token)
    return body


def test_tool_progress_updates_active_call_requires_started_call(world):
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    # progress for an unknown call fails closed
    response = post_event(world, token, event(world, agent, run, 0, "tool.progressed",
                                              {"call_id": "call", "detail": "half done"}))
    assert response[0]["accepted"] is False
    started = post_event(world, token,
                         event(world, agent, run, 1, "tool.started",
                               {"call_id": "call", "name": "edit"}),
                         event(world, agent, run, 2, "tool.progressed",
                               {"call_id": "call", "detail": "half done"}),
                         event(world, agent, run, 3, "tool.progressed",
                               {"call_id": "call", "detail": "applying patch"}))
    assert all(item["accepted"] for item in started)
    tools = run_view(world, agent, run, token)["tools"]
    assert tools["call"]["state"] == "active"
    assert tools["call"]["detail"] == "applying patch"
    # bounded progress history references the two progress events
    assert len(tools["call"]["progress"]) == 2
    # progress after completion is rejected like a stale call
    post_event(world, token, event(world, agent, run, 4, "tool.completed",
                                   {"call_id": "call", "success": True}))
    late = post_event(world, token, event(world, agent, run, 5, "tool.progressed",
                                          {"call_id": "call", "detail": "too late"}))
    assert late[0]["accepted"] is False


def test_vcs_observed_refreshes_sanitized_workspace_metadata(world):
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    post_event(world, token, event(world, agent, run, 0, "vcs.observed", {
        "workspace_path": "worktree", "branch": "agent/probe",
        "head_sha256": "a" * 64, "dirty_count": 2}))
    assert run_view(world, agent, run, token)["vcs"]["branch"] == "agent/probe"
    # a non-observation (same head/branch/dirty) is idempotent state, extra events fine
    post_event(world, token, event(world, agent, run, 1, "vcs.observed", {
        "workspace_path": "worktree", "branch": "agent/probe",
        "head_sha256": "a" * 64, "dirty_count": 2}))
    changed = run_view(world, agent, run, token)["vcs"]
    assert len(changed["observed_event_ids"]) == 1
    # changed head refreshes state
    post_event(world, token, event(world, agent, run, 2, "vcs.observed", {
        "workspace_path": "worktree", "branch": "agent/probe",
        "head_sha256": "b" * 64, "dirty_count": 0}))
    updated = run_view(world, agent, run, token)["vcs"]
    assert updated["head_sha256"] == "b" * 64
    # garbage is rejected by the schema (bad sha, absolute path)
    bad = post_event(world, token, event(world, agent, run, 3, "vcs.observed", {
        "workspace_path": "worktree", "branch": "main", "head_sha256": "nothex"}))
    assert bad[0]["accepted"] is False
    traversal = post_event(world, token, event(world, agent, run, 4, "vcs.observed", {
        "workspace_path": "../outside", "branch": "main"}))
    assert traversal[0]["accepted"] is False


def test_blocker_recorder_needs_valid_evidence_and_updates_summary(world):
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    # blocker with invalid evidence refs is rejected (unknown resource)
    with_badevidence = post_event(world, token, event(world, agent, run, 0, "blocker.recorded", {
        "text": "waiting for API access", "requested_assistance": "operator",
        "evidence_refs": [{"kind": "task", "id": "task_missing"}]}))
    assert with_badevidence[0]["accepted"] is False
    recorded = post_event(world, token, event(world, agent, run, 1, "blocker.recorded", {
        "text": "waiting for API access", "requested_assistance": "operator"}))
    assert recorded[0]["accepted"]
    view = run_view(world, agent, run, token)
    blockers = view["summary"]["blockers"]
    assert blockers[0]["text"] == "waiting for API access"
    assert blockers[0]["requested_assistance"] == "operator"


def test_correction_recorder_requires_target_refs(world):
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = world.scope()
    missing = post_event(world, token, event(world, agent, run, 0, "correction.recorded", {
        "text": "the earlier statement was wrong", "target_refs": []}))
    assert missing[0]["accepted"] is False
    corrected = post_event(world, token, event(world, agent, run, 1, "correction.recorded", {
        "text": "the earlier statement was wrong",
        "target_refs": [{"kind": "scope", "id": scope["id"]}]}))
    assert corrected[0]["accepted"]
    corrections = run_view(world, agent, run, token)["corrections"]
    assert corrections[0]["target_refs"][0]["id"] == scope["id"]


def test_conversation_observed_is_reported_only_and_bounded(world):
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    for index in range(10):
        post_event(world, token, event(world, agent, run, index, "conversation.observed", {
            "subject": f"conversation {index}", "redaction": "partial"}))
    contexts = run_view(world, agent, run, token)["conversation_contexts"]
    assert len(contexts) == 8  # bounded, newest retained
    assert contexts[-1]["subject"] == "conversation 9"


def test_server_id_is_stable_and_limits_declare_inline_bytes(world):
    from hivemind.api import discovery_server_id
    response = world.client.get("/.well-known/hivemind").json()
    assert response["server_id"] == discovery_server_id(world.app)
    assert response["server_id"].startswith("srv_")
    assert response["limits"]["max_inline_content_bytes"] == 65536
    assert response["limits"]["max_event_bytes"] == 131072


def test_semantic_conflict_candidate_never_denies_but_records(world, monkeypatch):
    agent = world.agent()
    other = world.agent()
    token = world.credential(agent)
    other_token = world.credential(other)
    first_run = world.run(agent, token)
    scope = world.scope(key="website/header")
    claim = world.claim(agent, first_run, scope, token)
    assert claim.get("denied") is not True

    # Simulate a semantic hit: retrieval suggests the second claim's intent resembles
    # the first claim's. Similarity alone must not deny the second claim (plan 14.5).
    hit_body = {**claim, "state": "active", "expires_at": claim["expires_at"] + 60}

    class FakeIndexes:
        def search(self, principal, request, scan_limit=None):
            return {"results": [{"resource_id": claim["id"], "kind": "work_claim",
                                 "revision": 1, "score": 0.9, "resource": hit_body}],
                    "freshness": {}, "embedding_profile": "fake",
                    "lexical_fallback": False, "warnings": [], "scan": {}}

    monkeypatch.setattr(world.app.state.service, "indexes", FakeIndexes())
    second_run = world.run(other, other_token)
    scope2 = world.scope(key="docs/homepage")
    result = world.call("POST", "work-claims", {
        "project_id": world.project, "agent_id": other["id"], "run_id": second_run["id"],
        "scope_ids": [scope2["id"]], "intent": "Implement the website navigation bar",
        "mode": "exclusive_owner"}, other_token, status=200)
    # accepted, not denied — similarity alone never blocks work
    assert result.get("denied") is not True
    candidates = [c for c in world.call("GET", f"conflicts?project_id={world.project}",
                                        token=other_token)
                  if c["state"] == "candidate"]
    assert candidates, "expected a semantic candidate conflict"
    assert candidates[0]["enforcement"] == "advisory_similarity"
    assert candidates[0]["winner_claim_id"] == claim["id"]
    # the first claim remains active/untouched — no auto-cancel
    refreshed = world.app.state.store.get(claim["id"], {"role": "admin", "project": world.project},
                                          "work_claim")
    assert refreshed["state"] == "active"


def test_semantic_candidates_absent_without_meaningful_intent(world, monkeypatch):
    agent = world.agent()
    token = world.credential(agent)
    run = world.run(agent, token)
    scope = world.scope()
    from hivemind.models import ClaimCreate
    # short intents skip retrieval entirely
    request = ClaimCreate(project_id=world.project, agent_id=agent["id"], run_id=run["id"],
                          scope_ids=[scope["id"]], intent="fix")
    hits = []
    monkeypatch.setattr(world.app.state.service.indexes, "search",
                        lambda *a, **k: hits.append(1) or {"results": []})
    claim = world.app.state.service.claim(
        {"role": "admin", "id": "admin", "project": world.project, "agent": None},
        request)
    assert claim.get("denied") is not True
    assert hits == []  # short intent never queried