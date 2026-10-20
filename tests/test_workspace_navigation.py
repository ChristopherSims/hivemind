def test_workspace_feature_routes_are_explicit_ui_routes(tmp_path):
    from hivemind.api import create_app
    from hivemind.cli import initialize

    initialize(tmp_path)
    app = create_app(tmp_path, workers=False, enable_ui=True)
    routes = {route.path for route in app.routes if "GET" in getattr(route, "methods", ())}
    for view in ("tasks", "coordination", "context", "operations", "privacy"):
        assert f"/projects/{{project_id}}/{view}" in routes
    assert "/projects/{project_id}/agents/{agent_id}" in routes
    # UI-disabled apps do not mint those SPA routes (404, JSON errors) instead.
    disabled = create_app(tmp_path / "disabled", workers=False, enable_ui=False)
    disabled_routes = {route.path for route in disabled.routes
                       if "GET" in getattr(route, "methods", ())}
    assert not any(r == "/projects/{project_id}/tasks" for r in disabled_routes)


def test_unknown_routes_remain_json_errors_not_spa_successes(world):
    for path in ("/v1/no-such-endpoint", f"/projects/{world.project}/no-such-view"):
        response = world.client.get(path)
        assert response.status_code == 404
        assert response.headers["content-type"].startswith("application/json")


def test_default_workspace_modules_are_discoverable_and_require_authentication(world):
    capabilities = world.client.get("/.well-known/hivemind").json()["capabilities"]
    for capability in ("dashboard.tasks", "dashboard.coordination", "dashboard.context_explorer",
                       "dashboard.operations", "dashboard.privacy_audit"):
        assert capability in capabilities
    for endpoint in ("task-workspace", "conflict-console", "context-explore:status",
                     "operations-summary", "privacy-audit/permissions"):
        response = world.client.get(f"/v1/projects/{world.project}/{endpoint}")
        assert response.status_code == 401
