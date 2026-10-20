"""Bounded, authorized agent evidence aggregation for the dashboard details drawer."""

import json

import pytest
from fastapi import Request

from hivemind.agent_evidence import install as install_evidence
from hivemind.api import envelope
from hivemind.storage import now, uid


def _install(app):
    """Stand-in for the integration layer's capabilities registration."""

    async def principal(request: Request):
        return app.state.authenticate_request(request)

    def command(*args, **kwargs):  # agent_evidence registers no commands
        raise AssertionError("agent_evidence is read-only")

    install_evidence(app, app.state.service, principal, command, envelope)


@pytest.fixture
def evidence(world):
    _install(world.app)
    return world


def event(world, agent, run, sequence, suffix="tool.started", data=None, source="evidence-source"):
    return {"event_id": uid("evt"), "type": "hivemind.context." + suffix,
            "agent_id": agent["id"], "run_id": run["id"], "project_id": world.project,
            "occurred_at": now(),
            "source": {"source_id": source, "kind": "runtime_hook", "sequence": sequence,
                       "attribution": "confirmed"},
            "data": data or {"call_id": uid("call"), "name": "edit"}}


def artifact(world, run, title="Navigation plan", text="Breakpoint is 720px"):
    return world.call("POST", "artifacts", {
        "project_id": world.project, "title": title, "run_id": run["id"],
        "content": {"media_type": "text/plain", "text": text}})


def memory(world, agent, ref, title="Header decision"):
    return world.call("POST", "memories", {
        "project_id": world.project, "title": title, "scope_kind": "agent",
        "scope_owner_id": agent["id"],
        "content": {"media_type": "text/plain", "text": "Compact navigation starts at 720px"},
        "evidence_refs": [{"kind": ref["kind"], "id": ref["id"], "revision": ref["revision"]}]})


def test_evidence_reports_authorized_ledger_artifacts_memories_and_events(evidence):
    agent = evidence.agent()
    run = evidence.run(agent)
    made = artifact(evidence, run)
    recorded = memory(evidence, agent, {"kind": "artifact", **made})
    token = evidence.credential(agent)
    accepted = evidence.call("POST", "events:batch",
                             {"events": [event(evidence, agent, run, 0)]}, token)
    assert accepted[0]["accepted"]

    result = evidence.call("GET", f"agents/{agent['id']}/evidence?project_id={evidence.project}")
    assert result["agent"]["id"] == agent["id"]
    assert [item["id"] for item in result["ledger"]["items"]] == [run["id"]]
    assert result["ledger"]["items"][0]["status"] == run["status"]
    assert result["ledger"]["items"][0]["presence"] == "online"
    assert [item["id"] for item in result["artifacts"]["items"]] == [made["id"]]
    assert result["artifacts"]["items"][0]["title"] == "Navigation plan"
    assert result["artifacts"]["items"][0]["run_id"] == run["id"]
    assert [item["id"] for item in result["memories"]["items"]] == [recorded["id"]]
    assert result["memories"]["items"][0]["effective_status"] in ("proposed", "verified")
    assert result["memories"]["items"][0]["scope_kind"] == "agent"
    assert [item["event_id"] for item in result["events"]["items"]] == \
        [accepted[0]["event_id"]]
    assert result["events"]["items"][0]["type"] == "hivemind.context.tool.started"
    for section in ("ledger", "artifacts", "memories", "events"):
        assert result[section]["next_before"] is None


def test_evidence_excludes_private_memories_and_hidden_runs(evidence):
    author, observer = evidence.agent("Author"), evidence.agent("Observer")
    run = evidence.run(author)
    recorded = memory(evidence, author, {"kind": "run", **run})
    token = evidence.credential(observer)
    result = evidence.call("GET", f"agents/{author['id']}/evidence", token=token)
    assert [item["id"] for item in result["memories"]["items"]] == []
    # The ledger is project-visible, matching GET /v1/ledger for the same principal.
    assert [item["id"] for item in result["ledger"]["items"]] == [run["id"]]
    store = evidence.app.state.store
    with store.transaction():
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?", (author["id"], run["id"]))
    hidden = evidence.call("GET", f"agents/{author['id']}/evidence", token=token)
    assert hidden["ledger"]["items"] == []
    visible = evidence.call("GET", f"agents/{author['id']}/evidence")
    assert [item["id"] for item in visible["ledger"]["items"]] == [run["id"]]
    assert [item["id"] for item in visible["memories"]["items"]] == [recorded["id"]]


def test_evidence_paginates_coordination_events_and_skips_unauthorized(evidence):
    agent = evidence.agent()
    run = evidence.run(agent)
    token = evidence.credential(agent)
    sent = [event(evidence, agent, run, sequence, data={"call_id": f"call-{sequence}", "name": "edit"})
            for sequence in range(4)]
    accepted = evidence.call("POST", "events:batch", {"events": sent}, token)
    assert all(item["accepted"] for item in accepted)

    first = evidence.call("GET", f"agents/{agent['id']}/evidence?limit=2")
    assert len(first["events"]["items"]) == 2
    assert first["events"]["next_before"] is not None
    assert first["events"]["items"][0]["event_id"] == accepted[3]["event_id"]
    second = evidence.call(
        "GET", f"agents/{agent['id']}/evidence?limit=2&events_before={first['events']['next_before']}"
               "&ledger_before=0&artifacts_before=0&memories_before=0")
    assert [item["event_id"] for item in second["events"]["items"]] == \
        [accepted[1]["event_id"], accepted[0]["event_id"]]
    assert second["events"]["next_before"] is None
    assert second["ledger"]["items"] == [] and second["memories"]["items"] == []


def test_evidence_pages_past_a_full_window_of_unauthorized_events(evidence):
    agent = evidence.agent()
    run = evidence.run(agent)
    token = evidence.credential(agent)
    made = artifact(evidence, run)
    accepted = evidence.call("POST", "events:batch", {"events": [
        event(evidence, agent, run, 0, data={"call_id": "call-old", "name": "edit"})]}, token)
    revoked = event(evidence, agent, run, 1, suffix="decision.recorded",
                    data={"text": "Superseded decision",
                          "evidence_refs": [{"kind": "artifact", "id": made["id"],
                                             "revision": made["revision"]}]})
    newer = event(evidence, agent, run, 2, data={"call_id": "call-new", "name": "edit"})
    latest = event(evidence, agent, run, 3, data={"call_id": "call-latest", "name": "edit"})
    assert all(item["accepted"] for item in
               evidence.call("POST", "events:batch", {"events": [revoked, newer, latest]}, token))
    store = evidence.app.state.store
    with store.transaction():
        row = store.raw(made["id"], "artifact")
        body = json.loads(row["body"])
        store.save("artifact", evidence.project, row["owner"],
                   {**body, "content": {"media_type": "text/plain", "text": "changed"}})
    # DESC order is latest, newer, revoked, accepted; a limit of 1 makes the first window
    # the two newest authorized events, and the caller must still reach the oldest one.
    first = evidence.call("GET", f"agents/{agent['id']}/evidence?limit=1")
    assert [item["event_id"] for item in first["events"]["items"]] == [latest["event_id"]]
    second = evidence.call(
        "GET", f"agents/{agent['id']}/evidence?limit=1&events_before={first['events']['next_before']}"
               "&ledger_before=0&artifacts_before=0&memories_before=0")
    assert [item["event_id"] for item in second["events"]["items"]] == [newer["event_id"]]
    third = evidence.call(
        "GET", f"agents/{agent['id']}/evidence?limit=1&events_before={second['events']['next_before']}"
               "&ledger_before=0&artifacts_before=0&memories_before=0")
    assert [item["event_id"] for item in third["events"]["items"]] == [accepted[0]["event_id"]]
    assert third["events"]["next_before"] is None


def test_evidence_denies_mismatched_project_and_unknown_agent(evidence):
    agent = evidence.agent()
    evidence.call("GET", f"agents/{agent['id']}/evidence?project_id=project_other", status=404)
    evidence.call("GET", "agents/agent_missing/evidence", status=404)
    evidence.call("GET", f"agents/{agent['id']}/evidence?limit=51", status=422)


def test_evidence_event_disappears_when_its_evidence_is_revoked(evidence):
    agent = evidence.agent()
    run = evidence.run(agent)
    made = artifact(evidence, run)
    token = evidence.credential(agent)
    decision = event(evidence, agent, run, 0, suffix="decision.recorded",
                     data={"text": "Ship the compact header",
                           "evidence_refs": [{"kind": "artifact", "id": made["id"],
                                              "revision": made["revision"]}]})
    assert evidence.call("POST", "events:batch", {"events": [decision]}, token)[0]["accepted"]
    before = evidence.call("GET", f"agents/{agent['id']}/evidence")
    assert len(before["events"]["items"]) == 1
    store = evidence.app.state.store
    with store.transaction():
        row = store.raw(made["id"], "artifact")
        body = json.loads(row["body"])
        store.save("artifact", evidence.project, row["owner"],
                   {**body, "content": {"media_type": "text/plain", "text": "changed"}})
    after = evidence.call("GET", f"agents/{agent['id']}/evidence")
    assert after["events"]["items"] == []


def test_evidence_flags_denied_mutation_events(evidence):
    """R4: an agent-reported 'mutation denied' status event surfaces with
    denied_mutation/denied_tool markers on the agent evidence read."""
    world = evidence
    a = world.agent()
    ta = world.credential(a)
    ra = world.run(a, ta)
    denial = event(world, a, ra, 0, "agent.status_reported",
                   {"text": "mutation denied: write — RuntimeError"})
    ordinary = event(world, a, ra, 1, "tool.started", {"call_id": uid("c"), "name": "read"})
    world.call("POST", "events:batch", {"events": [denial, ordinary]}, ta)
    body = world.call("GET", f"agents/{a['id']}/evidence?project_id={world.project}")
    events_section = body["events"]
    flagged = [item for item in events_section["items"] if item.get("denied_mutation")]
    assert flagged and flagged[0]["denied_tool"] == "write"
    # ordinary tool events are never flagged
    assert all(not item.get("denied_mutation")
               for item in events_section["items"] if item.get("type", "").endswith("tool.started"))
