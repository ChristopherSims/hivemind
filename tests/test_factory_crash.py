import json
import sqlite3
import threading
import time

import pytest

from scripts.factory_crash import pending_events
from scripts.fault_ingestion_server import install_transaction_barrier


def test_barrier_pauses_after_sql_write_but_before_commit(world, tmp_path):
    store = world.app.state.store
    arm, reached = tmp_path / "arm", tmp_path / "reached.json"
    original = store.project(world.project)["seq"]
    arm.write_text("armed")
    install_transaction_barrier(store, arm, reached)
    def write():
        with store.transaction():
            store.tick(world.project)
    thread = threading.Thread(target=write)
    thread.start()
    try:
        deadline = time.monotonic() + 3
        while not reached.exists():
            assert time.monotonic() < deadline
            time.sleep(.02)
        with sqlite3.connect(store.directory / "canonical.sqlite") as reader:
            assert reader.execute("SELECT seq FROM projects WHERE id=?", (world.project,)).fetchone()[0] == original
        assert json.loads(reached.read_text())["uncommitted_sequence"] == original + 1
    finally:
        arm.unlink()
        thread.join(timeout=3)
    assert not thread.is_alive()
    assert store.project(world.project)["seq"] == original + 1


def test_pending_spool_snapshot_preserves_original_event_ids(tmp_path):
    with sqlite3.connect(tmp_path / "session-spool.sqlite") as db:
        db.execute("CREATE TABLE spool(id TEXT, payload TEXT, state TEXT)")
        db.execute("INSERT INTO spool VALUES(?,?,?)", ("evt1", json.dumps({"event_id": "evt1"}), "pending"))
        db.execute("INSERT INTO spool VALUES(?,?,?)", ("evt2", json.dumps({"event_id": "evt2"}), "quarantined"))
    assert pending_events(tmp_path) == [{"event_id": "evt1"}]


def test_native_permit_check_cannot_create_or_renew_claims(monkeypatch):
    pytest.importorskip("mcp")
    from hivemind import mcp
    seen = []
    monkeypatch.setattr(mcp, "call", lambda method, path, body: seen.append((method, path, body)))
    mcp.hivemind_check_permit("run", ["scope"], ["work/progress.txt"])
    assert seen == [("POST", "runs/run/permits:check",
                     {"scope_ids": ["scope"], "paths": ["work/progress.txt"]})]


def test_board_transcript_uses_paginated_items(world, tmp_path):
    from scripts.factory_integration import export_conversation_logs
    board = world.board()
    world.post("Crash-check finding")
    response = world.call("GET", f"boards/{board['id']}/posts")
    export_conversation_logs(tmp_path, [], response["items"], {})
    assert "Crash-check finding" in (tmp_path / "main-board.log").read_text()
