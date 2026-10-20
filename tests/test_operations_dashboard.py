"""Operations summary: authorized, bounded, read-only health evidence."""
import json

import pytest
from fastapi import Request

from hivemind.api import hash_token
from hivemind.operations_dashboard import (MESSAGE_SCAN_LIMIT, RUN_SAMPLE_LIMIT, install,
                                           operations_summary)
from hivemind.storage import uid


@pytest.fixture
def operations(world):
    """Install the summary route the way the integration layer will."""

    async def principal(request: Request):
        return world.app.state.authenticate_request(request)

    def envelope(data):
        return {"protocol": "hivemind", "protocol_version": "0.1.0",
                "request_id": uid("req"), "data": data}

    def command(*args, **kwargs):  # read-only: no mutation route may be installed
        raise AssertionError("operations dashboard must not expose a command route")

    install(world.app, world.app.state.service, principal, command, envelope)
    return world


def summary(world, token=None, status=200, project=None):
    return world.call("GET", f"projects/{project or world.project}/operations-summary",
                      token=token, status=status)


def test_summary_sections_and_watermarks(operations):
    operations.post("A finding that needs indexing")
    result = summary(operations)
    assert result["scope"] == "project" and result["project_id"] == operations.project
    assert result["role"] == "admin"
    ingestion = result["ingestion"]
    freshness = operations.app.state.store.freshness(operations.project)
    assert ingestion["canonical_through_ingestion_seq"] == freshness["canonical_through_ingestion_seq"]
    # Workers are off in tests, so the semantic watermark lags the canonical one.
    assert ingestion["vector"]["pending_jobs"] > 0
    assert (ingestion["vector"]["lag_ingestion_seq"] ==
            freshness["canonical_through_ingestion_seq"] - freshness["vector_through_ingestion_seq"])
    assert ingestion["graph"]["pending_jobs"] > 0
    assert ingestion["index_read_path"] in ("dedicated", "canonical_fallback")
    assert ingestion["graph_projection"] in ("available", "unavailable")
    assert result["jobs"]["project"]["total"]["pending"] > 0
    assert result["generated_at_unix_seconds"] > 0
    assert result["warnings"] == [] or isinstance(result["warnings"], list)


def test_watermarks_advance_after_drain_and_no_lag_is_not_invented(operations):
    operations.post("Drainable finding")
    before = summary(operations)
    assert before["ingestion"]["vector"]["lag_ingestion_seq"] > 0
    operations.app.state.indexes.drain()
    after = summary(operations)
    # Graph jobs drain to completion; the graph watermark catches up exactly.
    assert after["ingestion"]["graph"]["pending_jobs"] == 0
    assert after["ingestion"]["graph"]["lag_ingestion_seq"] == 0
    assert (after["ingestion"]["graph"]["through_ingestion_seq"] ==
            after["ingestion"]["canonical_through_ingestion_seq"])
    assert after["jobs"]["project"]["by_kind"]["graph"]["pending"] == 0
    # No encoder is configured in tests, so the vector backlog honestly remains.
    assert after["ingestion"]["semantic_encoder"] == "absent"
    assert after["ingestion"]["vector"]["pending_jobs"] > 0
    assert any(w["code"] == "semantic_encoder_absent" for w in after["warnings"])


def test_project_admin_sees_only_project_counters_and_telemetry(operations):
    operations.post("Global counter fixture")
    operations.app.state.telemetry.dropped = 123
    result = summary(operations)
    jobs = result["jobs"]
    assert jobs["scope"] == "project"
    assert jobs["global"] is None
    telemetry = result["telemetry"]
    assert telemetry["available"] is True
    assert telemetry["content_capture"] is False
    assert telemetry["queued"] >= 0 and telemetry["dropped"] is None
    assert telemetry["dropped_scope"] == "global_all_projects"
    assert not any(warning["code"] == "telemetry_spans_dropped" for warning in result["warnings"])


def test_unbound_administrator_can_read_explicitly_labeled_global_counters(operations):
    operations.app.state.telemetry.dropped = 321
    token = operations.credential(None, role="admin")
    store = operations.app.state.store
    with store.transaction():
        store.db.execute("UPDATE principals SET project=NULL WHERE token_hash=?", (hash_token(token),))
    result = summary(operations, token=token)
    assert result["jobs"]["global"]["scope"] == "global_all_projects"
    assert result["jobs"]["global"]["by_state"]["pending"] >= result["jobs"]["project"]["total"]["pending"]
    assert result["telemetry"]["dropped"] == 321
    assert result["telemetry"]["dropped_scope"] == "global_all_projects"


def test_agent_scope_hides_global_counters_and_other_projects(operations):
    agent = operations.agent("Scoped")
    token = operations.credential(agent)
    result = summary(operations, token=token)
    assert result["role"] == "agent"
    assert result["jobs"]["global"] is None
    assert result["jobs"]["available"] is False
    assert result["jobs"]["reason"] == "administrator_required"
    assert result["telemetry"] == {"available": False, "reason": "administrator_required"}
    # A principal scoped to one project cannot read another's operations surface.
    store = operations.app.state.store
    with store.transaction():
        store.db.execute("INSERT INTO projects VALUES(?,?,0,0,?)", (uid("project"), "Hidden", 1))
    hidden = store.db.execute("SELECT id FROM projects WHERE title='Hidden'").fetchone()[0]
    summary(operations, token=token, status=404, project=hidden)
    assert operations.client.get(f"/v1/projects/{operations.project}/operations-summary").status_code == 401


def _dead_letter_message(world):
    sender, recipient = world.agent("Sender"), world.agent("Recipient")
    token = world.credential(recipient)
    run = world.run(recipient, token)
    message = world.call("POST", "messages", {
        "message_id": uid("msg"), "project_id": world.project, "from": sender["id"],
        "to": recipient["id"], "type": "inform", "thread_id": uid("thread"),
        "ttl_seconds": 3600, "retry_policy": {"max_attempts": 1},
        "content": {"media_type": "text/plain", "text": "Deliver this once"}})
    receipt = world.call("POST", f"messages/{message['id']}:claim", {"run_id": run["id"]}, token)
    world.call("POST", f"messages/{message['id']}:fail", {
        "run_id": run["id"], "delivery_id": receipt["delivery_id"], "reason": "handler_failed"}, token)
    return message


def test_delivery_dead_letter_counts_and_warning(operations):
    _dead_letter_message(operations)
    result = summary(operations)
    delivery = result["delivery"]
    assert delivery["available"] is True and delivery["scope"] == "authorized_principal"
    assert delivery["states"]["dead_letter"] == 1
    assert delivery["limit"] == MESSAGE_SCAN_LIMIT
    warning = next(w for w in result["warnings"] if w["code"] == "dead_letter_messages")
    assert warning["count"] == 1 and warning["scope"] == "project"


def test_delivery_window_is_principal_scoped_for_agents(operations):
    _dead_letter_message(operations)
    bystander = operations.agent("Bystander")
    token = operations.credential(bystander)
    delivery = summary(operations, token=token)["delivery"]
    # The agent cannot see other agents' conversations, so it counts none of them.
    assert delivery["states"]["dead_letter"] == 0
    assert all(count == 0 for count in delivery["states"].values())


def test_runs_presence_and_capture_buckets_are_measured_not_inferred(operations):
    agent = operations.agent("Runner")
    token = operations.credential(agent)
    run = operations.run(agent, token)
    result = summary(operations)
    runs = result["runs"]
    assert runs["presence"]["online"] == 1
    assert runs["presence_basis"] == "last_seen_at_heartbeats"
    assert "not evidence of verified capture" in runs["presence_note"]
    # A fresh run reports capture state 'unknown'; it is never rendered as healthy.
    assert runs["capture"]["unknown"] == 1 and runs["capture"]["healthy"] == 0
    assert runs["presence_thresholds_seconds"] == {"online_within": 15, "stale_within": 30}
    sample = next(item for item in runs["sample"] if item["id"] == run["id"])
    assert sample["presence"] == "online" and sample["capture_state"] == "unknown"
    assert sample["agent_id"] == agent["id"]
    assert runs["sample_limit"] == RUN_SAMPLE_LIMIT
    # The sample carries only coarse operational fields.
    assert set(sample) == {"id", "agent_id", "status", "presence", "capture_state", "last_seen_at"}


def test_heartbeat_capture_gaps_surface_as_measured_warning(operations):
    agent = operations.agent("Gapped")
    token = operations.credential(agent)
    run = operations.run(agent, token)
    operations.call("POST", f"runs/{run['id']}/heartbeat",
                    {"capture_health": {"state": "gaps", "missing_source_sequences": [
                        {"source_id": "editor", "from": 2, "to": 4}]}}, token)
    result = summary(operations)
    assert result["runs"]["capture"]["gaps"] == 1
    warning = next(w for w in result["warnings"] if w["code"] == "capture_gaps_reported")
    assert warning["count"] == 1
    # Online presence and capture gaps are independent measurements.
    assert result["runs"]["presence"]["online"] == 1


def test_dead_index_jobs_warn_without_revealing_job_bodies(operations):
    operations.post("A finding whose graph job will die")
    store = operations.app.state.store
    with store.transaction():
        store.db.execute("UPDATE jobs SET state='dead', attempts=5, error='index_processing_failed' "
                         "WHERE kind='graph'")
    result = summary(operations)
    jobs = result["jobs"]
    assert jobs["project"]["total"]["dead"] > 0
    assert jobs["project"]["by_kind"]["graph"]["dead"] > 0
    warning = next(w for w in result["warnings"] if w["code"] == "index_jobs_dead")
    assert warning["count"] == jobs["project"]["total"]["dead"]
    payload = json.dumps(result)
    assert "index_processing_failed" not in payload


def test_no_secrets_or_private_identifiers_leak(operations):
    agent = operations.agent("Leaky check")
    token = operations.credential(agent)
    run = operations.run(agent, token)
    operations.call("POST", f"runs/{run['id']}/heartbeat",
                    {"capture_health": {"state": "healthy"}}, token)
    payload = json.dumps(summary(operations))
    for forbidden in ("token_hash", "csrf", "session", "task_id", "tools", "files", "summary"):
        assert forbidden not in payload


def test_failed_section_degrades_to_unknown_not_healthy(operations, monkeypatch):
    store = operations.app.state.store
    monkeypatch.setattr(store, "freshness", lambda project: 1 / 0)
    result = summary(operations)
    assert result["ingestion"] == {"available": False, "reason": "section_measurement_failed"}
    # Other sections still measure; nothing infers health from the failed one.
    assert result["runs"]["available"] is True


def test_operations_summary_helper_runs_without_http(operations):
    store = operations.app.state.store
    principal = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    result = operations_summary(operations.app, operations.app.state.service, principal,
                                operations.project)
    assert result["project_id"] == operations.project
    assert result["jobs"]["global"] is None


def test_reinstall_replaces_only_own_route(operations):
    # A second install replaces this module's handler without touching others.
    board = operations.board()

    async def principal(request: Request):
        return operations.app.state.authenticate_request(request)

    install(operations.app, operations.app.state.service,
            principal, lambda *a: None, lambda data: {"data": data})
    result = operations.client.get(
        f"/v1/projects/{operations.project}/operations-summary",
        headers={"Authorization": "Bearer " + operations.owner})
    assert result.status_code == 200
    assert operations.call("GET", f"boards/{board['id']}/posts")["items"] is not None
