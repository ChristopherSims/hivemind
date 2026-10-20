"""A timed-out bridge must retain the storage used by its outstanding work."""
from types import SimpleNamespace

from fastapi.testclient import TestClient

from hivemind.api import create_app


def test_pending_bridge_retains_hub_indexes_and_canonical_store(tmp_path):
    app = create_app(tmp_path, workers=False)
    calls = []
    app.state.memory_event_bridge = SimpleNamespace(close=lambda: calls.append("memory") or False)
    app.state.activity_stream_bridge = SimpleNamespace(close=lambda: calls.append("activity") or True)
    app.state.activity_stream_hub = SimpleNamespace(close=lambda: calls.append("hub"))
    try:
        with TestClient(app):
            pass
        assert calls == ["memory", "activity"]
        assert app.state.shutdown_pending
        assert app.state.shutdown_pending_features == ("memory_event_bridge",)
        assert app.state.store.db.execute("SELECT 1").fetchone()[0] == 1
    finally:
        app.state.indexes.close()
        app.state.store.close()
