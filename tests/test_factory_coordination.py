import json
import hashlib

import pytest

from scripts.factory_coordination import validate_receipt


def pending_steer(world):
    worker = world.agent("Worker", ["steer"])
    wt = world.credential(worker)
    run = world.run(worker, wt)
    scope = world.scope("replacement", paths=["reports"])
    task = world.call("POST", "tasks", {"project_id": world.project, "title": "Write export",
                                       "description": "Use verified format memory", "scope_ids": [scope["id"]]})
    nexus = world.agent("Nexus")
    nt = world.credential(nexus, coordinator=True)
    nr = world.run(nexus, nt)
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                       {"agent_id": nexus["id"], "run_id": nr["id"]}, nt)
    control = world.call("POST", "controls", {
        "project_id": world.project, "target_run_id": run["id"], "action": "steer",
        "replacement_task_id": task["id"], "target_run_revision": run["control_revision"],
        "coordinator_epoch": lease["coordinator_epoch"], "reason": "Use nonoverlapping scope"}, nt)
    return worker, wt, run, task, control


def test_context_exposes_pending_assignment_without_committing_it(world):
    worker, token, run, task, control = pending_steer(world)
    body = {"project_id": world.project, "agent_selector": {"agent_ids": [worker["id"]]},
            "include": ["tasks", "controls", "ledger"]}
    packet = world.call("POST", "context:query", body, token)
    assert any(item["resource_id"] == task["id"] for item in packet["items"])
    world.call("POST", f"controls/{control['id']}:ack", {}, token)
    world.call("POST", f"controls/{control['id']}:checkpoint", {"checkpoint_id": "checkpoint"}, token)
    packet = world.call("POST", "context:query", body, token)
    assert any(item["resource_id"] == control["id"] and item["data"]["state"] == "applying"
               for item in packet["items"])
    assert any(item["resource_id"] == task["id"] for item in packet["items"])
    ledger_run = next(item["data"] for item in packet["items"] if item["resource_id"] == run["id"])
    assert ledger_run.get("task_id") != task["id"]
    # An expired command is not presented as a pending authoritative assignment.
    store = world.app.state.store
    with store.transaction():
        row = store.raw(control["id"])
        expired = json.loads(row["body"])
        expired["expires_at"] = 0
        store.save("control", world.project, row["owner"], expired)
    packet = world.call("POST", "context:query", body, token)
    assert all(item["resource_id"] not in (control["id"], task["id"]) for item in packet["items"])


def test_context_receipt_is_opt_in_and_contains_no_memory_contents(tmp_path, monkeypatch):
    pytest.importorskip("mcp")
    from hivemind import mcp
    config = {"state_dir": str(tmp_path / "private/worker/state"), "agent_id": "worker",
              "audit_context_queries": True}
    monkeypatch.setattr(mcp, "configuration", lambda: config)
    calls = []
    packet = {"items": [{"kind": "memory", "resource_id": "memory-1", "revision": 2,
                         "data": {"content": {"text": "Sensitive context must not enter the receipt"}}}]}
    def call(method, path, body):
        calls.append((method, path, body))
        return packet
    monkeypatch.setattr(mcp, "call", call)
    assert mcp.hivemind_query_context("project", {"agent_ids": ["owner"]}, include=["memory"]) == packet
    assert calls[0][2]["include"] == ["memory"]
    destination = tmp_path / "private/worker/context-receipts.jsonl"
    raw = destination.read_text()
    assert "Sensitive" not in raw
    receipt = json.loads(raw)
    assert len(receipt["packet_sha256"]) == 64
    validate_receipt([receipt], "worker", "project", {"id": "memory-1", "revision": 2})
    with pytest.raises(AssertionError):
        validate_receipt([receipt], "worker", "project", {"id": "memory-1", "revision": 3})
    config["audit_context_queries"] = False
    mcp.hivemind_query_context("project", {"agent_ids": ["owner"]})
    assert destination.read_text() == raw


def test_baseline_copy_requires_a_passing_report_and_matching_hashes(tmp_path, monkeypatch):
    from scripts import factory_coordination as scenario
    monkeypatch.setattr(scenario, "ROOT", tmp_path)
    source = tmp_path / ".hivemind-test/factory-live_unit"
    hashes = {}
    for relative in scenario.BASELINE_FILES:
        path = source / "app" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("real-run-artifact-placeholder", encoding="utf-8")
        hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    report = {"status": "PASS", "files": hashes}
    (source / "report.json").write_text(json.dumps(report), encoding="utf-8")
    assert scenario.copy_verified_baseline(source, tmp_path / "copy") == hashes
    report["status"] = "FAIL"
    (source / "report.json").write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError, match="passing"):
        scenario.copy_verified_baseline(source, tmp_path / "failed-copy")
    report["status"] = "PASS"
    (source / "report.json").write_text(json.dumps(report), encoding="utf-8")
    (source / "app/api/server.py").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="changed"):
        scenario.copy_verified_baseline(source, tmp_path / "changed-copy")
