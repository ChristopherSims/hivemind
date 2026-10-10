import json
import os
import struct
import time
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from hivemind.api import create_app
from hivemind.cli import initialize
from hivemind.indexes import Gemma, vector_blob
from hivemind.storage import uid
from conftest import World


@pytest.mark.skipif(os.environ.get("HIVEMIND_MODEL_TEST") != "1", reason="Opt-in local model integration")
def test_real_semantic_graph_restart_and_source_deletion(tmp_path, monkeypatch):
    config = json.loads(initialize(tmp_path).read_text())
    encoder = Gemma()
    vec = encoder.query("navigation")
    assert len(vec) == 768 * 4
    assert abs(sum(x*x for x in struct.unpack("<768f", vec)) - 1) < 0.01
    app = create_app(tmp_path, encoder=encoder, workers=False)
    with TestClient(app) as client:
        world = World(app, client, config)
        a = world.agent("Droid header worker")
        b = world.agent("Oh my Pi research worker")
        world.run(a)
        world.run(b)
        post = world.post("The masthead houses the primary menu and site links.")
        world.post("The database uses WAL journaling and durable transactions.")
        header = world.scope()
        run = world.call("GET", f"ledger?project_id={world.project}")["runs"]
        world.claim(a, next(r for r in run if r["agent_id"] == a["id"]), header)
        app.state.indexes.drain()
        assert not world.call("GET", f"ledger?project_id={world.project}")["freshness"]["vector_pending_jobs"]
        results = world.call("POST", "search", {"project_id": world.project, "query": "website navigation bar",
                                              "mode": "semantic", "resource_kinds": ["board_post"]})["results"]
        assert results[0]["resource_id"] == post["id"]
        selector = world.call("POST", "context:query", {"project_id": world.project,
            "agent_selector": {"agent_ids": [a["id"], b["id"]], "semantic_query": "website header",
                               "match": "union"}, "query": "navigation bar"})
        assert {x["agent_id"] for x in selector["selected_agents"]} == {a["id"], b["id"]}
        intersection = world.call("POST", "context:query", {"project_id": world.project,
            "agent_selector": {"agent_ids": [a["id"]], "semantic_query": "navigation", "match": "intersection"}})
        assert [x["agent_id"] for x in intersection["selected_agents"]] == [a["id"]]
        message = world.call("POST", "messages", {"message_id": uid("msg"), "project_id": world.project,
            "from": a["id"], "to": b["id"], "type": "question", "thread_id": uid("thread"),
            "content": {"media_type": "text/plain", "text": "Confirm the navigation contract?"}})
        memory = world.call("POST", "memories", {"project_id": world.project, "title": "Masthead navigation",
            "scope_owner_id": world.project, "content": {"media_type": "text/plain", "text": "Masthead contains menu"},
            "expires_at": datetime.fromtimestamp(time.time() + 3600, timezone.utc).isoformat().replace("+00:00", "Z"),
            "evidence_refs": [{"kind": "board_post", "id": post["id"], "revision": 1}]})
        world.call("POST", f"memories/{memory['id']}:verify", {"expected_revision": 1})
        app.state.indexes.drain()
    # A fresh canonical and graph connection, not an in-memory reuse.
    restored = create_app(tmp_path, encoder=encoder, workers=False)
    with TestClient(restored) as client:
        world = World(restored, client, config)
        assert world.call("GET", f"agents/{b['id']}/inbox?project_id={world.project}")[0]["id"] == message["id"]
        context = world.call("POST", "context:query", {"project_id": world.project,
            "agent_selector": {"agent_ids": [a["id"]]}})
        assert any(i["resource_id"] == memory["id"] for i in context["items"])
        graph = world.call("POST", "graph:query", {"project_id": world.project, "seed_ids": [memory["id"]]})
        assert any(e["kind"] == "SUPPORTED_BY" for e in graph["edges"])
        assert restored.state.store.db.execute(
            "SELECT count(*) FROM vector_chunks WHERE resource=?", (memory["id"],)).fetchone()[0] > 0
        with monkeypatch.context() as clock:
            expired = time.time() + 3601
            clock.setattr(time, "time", lambda: expired)
            result = world.call("POST", "search", {"project_id": world.project, "query": "navigation",
                                                 "mode": "semantic", "resource_kinds": ["memory"]})
            assert not result["results"]
            world.call("POST", "graph:query", {"project_id": world.project, "seed_ids": [memory["id"]]}, status=404)
        world.call("DELETE", f"posts/{post['id']}", {"expected_revision": 1})
        result = world.call("POST", "search", {"project_id": world.project, "query": "navigation bar",
                                             "mode": "semantic"})
        assert not {post["id"], memory["id"]} & {h["resource_id"] for h in result["results"]}
        restored.state.indexes.drain()
        count = restored.state.store.db.execute("SELECT count(*) FROM vector_chunks WHERE resource=?",
                                                (post["id"],)).fetchone()[0]
        assert count == 0


def test_vector_shape_and_nonfinite_rejection():
    with pytest.raises(ValueError):
        vector_blob([float("nan")] * 768)
    with pytest.raises(ValueError):
        vector_blob([0.1] * 256)
    with pytest.raises(ValueError):
        vector_blob([0.0] * 768)
