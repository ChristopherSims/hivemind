def memory(world, title="Format", text="Original format", token=None, **fields):
    post = world.post(text, token)
    return world.call("POST", "memories", {
        "project_id": world.project, "title": title, "content": {"media_type": "text/plain", "text": text},
        "scope_kind": "project", "scope_owner_id": world.project,
        "evidence_refs": [{"kind": "board_post", "id": post["id"], "revision": post["revision"]}], **fields}, token)


def version(world, current, text="Replacement format", token=None):
    post = world.post(text, token)
    return world.call("POST", f"memories/{current['id']}/versions", {
        "expected_revision": current["revision"], "title": current["title"],
        "content": {"media_type": "text/plain", "text": text},
        "evidence_refs": [{"kind": "board_post", "id": post["id"], "revision": post["revision"]}]}, token)


def test_version_proposals_preserve_current_until_human_approval(world):
    agent = world.agent()
    token = world.credential(agent)
    original = memory(world, token=token)
    original = world.call("POST", f"memories/{original['id']}:verify", {"expected_revision": original["revision"]})
    proposal = version(world, original, token=token)
    assert world.call("GET", f"memories/{original['id']}")["content"]["text"] == "Original format"
    review = {"expected_revision": proposal["revision"], "decision": "approve", "reason": "Reviewed evidence"}
    world.call("POST", f"memory-proposals/{proposal['id']}:review", review, token, status=403)
    world.call("POST", f"memory-proposals/{proposal['id']}:review", review, key="approve-version")
    replay = world.call("POST", f"memory-proposals/{proposal['id']}:review", review, key="approve-version")
    assert replay["status"] == "approved"
    updated = world.call("GET", f"memories/{original['id']}")
    assert updated["id"] == original["id"] and updated["status"] == "verified"
    assert updated["memory_version_id"] == proposal["memory_version_id"]
    assert updated["supersedes_version_id"] == original["memory_version_id"]
    assert updated["content"]["text"] == "Replacement format"
    history = world.call("GET", f"memories/{original['id']}/versions")
    assert len(history) == 2
    assert history[1]["effective_status"] == "superseded"
    assert history[1]["content"]["text"] == "Original format"


def test_stale_competing_proposal_and_scope_widening_are_rejected(world):
    current = memory(world)
    first, second = version(world, current, "First"), version(world, current, "Second")
    world.call("POST", f"memory-proposals/{first['id']}:review",
               {"expected_revision": 1, "decision": "approve", "reason": "Valid evidence"})
    world.call("POST", f"memory-proposals/{second['id']}:review",
               {"expected_revision": 1, "decision": "approve", "reason": "Stale"}, status=412)
    post = world.post("New evidence")
    world.call("POST", f"memories/{current['id']}/versions", {
        "expected_revision": 2, "title": "Attempt widening", "content": {"media_type": "text/plain", "text": "x"},
        "scope_kind": "project", "evidence_refs": [{"kind": "board_post", "id": post["id"]}]}, status=422)


def test_expired_and_future_memory_are_hidden_from_retrieval_but_reviewable(world):
    expired = memory(world, expires_at="2000-01-01T00:00:00Z")
    future = memory(world, validity={"valid_from": "2099-01-01T00:00:00Z"})
    assert world.call("GET", f"memories?project_id={world.project}") == []
    queue = world.call("GET", f"memories/review-queue?project_id={world.project}")
    assert {item["effective_status"] for item in queue["memories"]} == {"expired", "not_yet_valid"}
    for item in (expired, future):
        world.call("POST", f"memories/{item['id']}:verify", {"expected_revision": 1}, status=409)
    post = world.post("Date validation")
    world.call("POST", "memories", {
        "project_id": world.project, "title": "Bad date", "content": {"media_type": "text/plain", "text": "x"},
        "scope_owner_id": world.project, "evidence_refs": [{"kind": "board_post", "id": post["id"]}],
        "expires_at": "2026-02-30T00:00:00Z"}, status=422)


def test_verified_expiry_hides_search_context_and_graph_immediately(world, monkeypatch):
    import time
    from datetime import datetime, timezone
    expiry = datetime.fromtimestamp(time.time() + 3600, timezone.utc).isoformat().replace("+00:00", "Z")
    current = memory(world, text="Expiry retrieval canary", expires_at=expiry)
    current = world.call("POST", f"memories/{current['id']}:verify", {"expected_revision": 1})
    agent = world.agent()
    world.run(agent)
    monkeypatch.setattr(time, "time", lambda: datetime.fromisoformat(expiry).timestamp() + 1)
    search = world.call("POST", "search", {"project_id": world.project, "query": "canary", "mode": "lexical"})
    assert all(hit["resource"]["id"] != current["id"] for hit in search["results"])
    context = world.call("POST", "context:query", {"project_id": world.project,
        "agent_selector": {"agent_ids": [agent["id"]], "active_only": False}, "include": ["memory"]})
    assert not context["items"]
    world.call("POST", "graph:query", {"project_id": world.project, "seed_ids": [current["id"]]}, status=404)


def test_human_confirms_and_resolves_evidence_backed_contradiction(world):
    agent = world.agent()
    token = world.credential(agent)
    left, right = memory(world, "One", "Value one", token), memory(world, "Two", "Value two", token)
    for item in (left, right):
        world.call("POST", f"memories/{item['id']}:verify", {"expected_revision": 1})
    proof = world.post("These claims contradict each other", token)
    dispute = world.call("POST", "memory-disputes", {
        "project_id": world.project, "left_memory_id": left["id"], "right_memory_id": right["id"],
        "left_revision": 2, "right_revision": 2, "reason": "Contradiction",
        "evidence_refs": [{"kind": "board_post", "id": proof["id"], "revision": 1}]}, token)
    assert len(world.call("GET", f"memories?project_id={world.project}")) == 2
    review = {"expected_revision": 1, "left_expected_revision": 2, "right_expected_revision": 2,
              "decision": "confirm", "reason": "Evidence checked"}
    world.call("POST", f"memory-disputes/{dispute['id']}:review", review, token, status=403)
    world.call("POST", f"memory-disputes/{dispute['id']}:review", review)
    assert not world.call("GET", f"memories?project_id={world.project}")
    queue = world.call("GET", f"memories/review-queue?project_id={world.project}")
    assert {item["effective_status"] for item in queue["memories"]} == {"disputed"}
    world.call("POST", f"memory-disputes/{dispute['id']}:review", {
        "expected_revision": 2, "left_expected_revision": 3, "right_expected_revision": 3,
        "decision": "prefer_left", "reason": "One is supported"})
    active = world.call("GET", f"memories?project_id={world.project}")
    assert len(active) == 1 and active[0]["id"] == left["id"]
    assert world.call("GET", f"memories/{right['id']}")["effective_status"] == "superseded"


def test_source_deletion_hides_versions_proposals_and_review_queue(world):
    current = memory(world)
    proposal = version(world, current)
    source = current["evidence_refs"][0]["id"]
    world.call("DELETE", f"posts/{source}", {"expected_revision": 1})
    world.call("GET", f"memories/{current['id']}/versions", status=404)
    queue = world.call("GET", f"memories/review-queue?project_id={world.project}")
    assert not queue["memories"] and not queue["proposals"]
    world.call("POST", f"memory-proposals/{proposal['id']}:review",
               {"expected_revision": 1, "decision": "approve", "reason": "Missing evidence"}, status=404)


def test_changed_proposal_evidence_blocks_approval_and_idempotent_readback(world):
    current = memory(world)
    proof = world.post("Version evidence")
    payload = {"expected_revision": 1, "title": "Revised", "content": {"media_type": "text/plain", "text": "New"},
               "evidence_refs": [{"kind": "board_post", "id": proof["id"], "revision": 1}]}
    proposal = world.call("POST", f"memories/{current['id']}/versions", payload, key="proposal-retry")
    world.call("PATCH", f"posts/{proof['id']}",
               {"expected_revision": 1, "content": {"media_type": "text/plain", "text": "Changed evidence"}})
    world.call("GET", f"memory-proposals/{proposal['id']}", status=404)
    world.call("POST", f"memories/{current['id']}/versions", payload, key="proposal-retry", status=404)
    world.call("POST", f"memory-proposals/{proposal['id']}:review",
               {"expected_revision": 1, "decision": "approve", "reason": "Changed evidence"}, status=404)


def test_private_versions_stay_private_and_agents_cannot_open_human_queue(world):
    agent, outsider = world.agent("Private worker"), world.agent("Other worker")
    token, other = world.credential(agent), world.credential(outsider)
    post = world.post("Private lifecycle source", token)
    current = world.call("POST", "memories", {
        "project_id": world.project, "title": "Private lifecycle", "scope_kind": "agent",
        "scope_owner_id": agent["id"], "private": True,
        "content": {"media_type": "text/plain", "text": "Private lifecycle content"},
        "evidence_refs": [{"kind": "board_post", "id": post["id"], "revision": 1}]}, token)
    proposal = version(world, current, token=token)
    world.call("GET", f"memories/{current['id']}/versions", token=other, status=404)
    world.call("GET", f"memory-proposals/{proposal['id']}", token=other, status=404)
    world.call("GET", f"memories/review-queue?project_id={world.project}", token=token, status=403)
    human = world.credential(role="human")
    queue = world.call("GET", f"memories/review-queue?project_id={world.project}", token=human)
    assert not queue["memories"] and not queue["proposals"]
    assert world.call("GET", f"memories/{current['id']}/versions", token=token)[0]["id"] == current["id"]


def test_inactive_memory_can_be_deleted_without_reading_invalid_sources(world):
    expired = memory(world, expires_at="2000-01-01T00:00:00Z")
    world.call("DELETE", f"memories/{expired['id']}", {"expected_revision": 1})
    world.call("GET", f"memories/{expired['id']}", status=404)
    invalidated = memory(world)
    world.call("DELETE", f"posts/{invalidated['evidence_refs'][0]['id']}", {"expected_revision": 1})
    world.call("DELETE", f"memories/{invalidated['id']}", {"expected_revision": 2})
    assert not world.call("GET", f"memories/review-queue?project_id={world.project}")["memories"]


def test_version_cannot_cite_itself_or_its_dependent_memory(world):
    current = memory(world)
    child = world.call("POST", "memories", {
        "project_id": world.project, "title": "Dependent", "scope_owner_id": world.project,
        "content": {"media_type": "text/plain", "text": "Dependent content"},
        "evidence_refs": [{"kind": "memory", "id": current["id"], "revision": current["revision"]}]})
    for source in (current, child):
        world.call("POST", f"memories/{current['id']}/versions", {
            "expected_revision": 1, "title": "Cycle", "content": {"media_type": "text/plain", "text": "Cycle"},
            "evidence_refs": [{"kind": "memory", "id": source["id"], "revision": source["revision"]}]}, status=422)


def test_missing_dispute_evidence_is_redacted_and_human_can_release_quarantine(world):
    left, right = memory(world, "Left"), memory(world, "Right")
    proof = world.post("Revocable contradiction proof")
    dispute = world.call("POST", "memory-disputes", {
        "project_id": world.project, "left_memory_id": left["id"], "right_memory_id": right["id"],
        "left_revision": 1, "right_revision": 1, "reason": "Revocable secret derived from proof",
        "evidence_refs": [{"kind": "board_post", "id": proof["id"]}]}, key="dispute-original")
    review = {"expected_revision": 1, "left_expected_revision": 1, "right_expected_revision": 1,
              "decision": "confirm", "reason": "Reviewed"}
    world.call("POST", f"memory-disputes/{dispute['id']}:review", review)
    world.call("DELETE", f"posts/{proof['id']}", {"expected_revision": 1})
    unavailable = world.call("GET", f"memory-disputes/{dispute['id']}")
    assert unavailable["evidence_unavailable"] and unavailable["evidence_refs"] == []
    assert "Revocable secret" not in unavailable["reason"]
    review.update(expected_revision=2, left_expected_revision=2, right_expected_revision=2, decision="prefer_left")
    world.call("POST", f"memory-disputes/{dispute['id']}:review", review, status=409)
    review["decision"] = "dismiss"
    resolved = world.call("POST", f"memory-disputes/{dispute['id']}:review", review)
    assert resolved["status"] == "resolved" and resolved["evidence_unavailable"]
    assert len(world.call("GET", f"memories?project_id={world.project}")) == 2


def test_retired_source_is_hidden_in_history_and_cached_old_verification(world):
    original = memory(world)
    verify = {"expected_revision": 1}
    current = world.call("POST", f"memories/{original['id']}:verify", verify, key="old-verify")
    proposal = version(world, current)
    world.call("POST", f"memory-proposals/{proposal['id']}:review",
               {"expected_revision": 1, "decision": "approve", "reason": "Independent replacement source"})
    world.call("DELETE", f"posts/{original['evidence_refs'][0]['id']}", {"expected_revision": 1})
    history = world.call("GET", f"memories/{original['id']}/versions")
    assert len(history) == 1 and history[0]["content"]["text"] == "Replacement format"
    world.call("POST", f"memories/{original['id']}:verify", verify, key="old-verify", status=404)
