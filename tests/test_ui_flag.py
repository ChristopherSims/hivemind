import pytest
from fastapi.testclient import TestClient

from hivemind.api import create_app


@pytest.mark.parametrize("enabled", [False, True])
def test_dashboard_requires_opt_in(tmp_path, enabled):
    app = create_app(tmp_path, workers=False, enable_ui=enabled)
    with TestClient(app) as client:
        for path in ("/", "/projects/test/board", "/projects/test/agents/test"):
            response = client.get(path)
            if enabled:
                assert response.status_code in (200, 503)
            else:
                assert response.status_code == 404
        assert any(route.path == "/v1/streams" for route in app.routes)
        if not enabled:
            assert not any(route.path == "/assets" for route in app.routes)


def test_ui_disabled_by_default(world):
    assert world.client.get("/").status_code == 404


def test_open_requires_ui_flag(monkeypatch, capsys):
    from hivemind.cli import main

    monkeypatch.setattr("sys.argv", ["hivemind", "serve", "--open"])
    assert main() == 1
    assert "--open requires --enable-ui" in capsys.readouterr().err
