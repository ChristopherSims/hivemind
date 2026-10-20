"""Privacy & audit surface: operator-scoped reads, honest boundaries, safe exports."""
import pytest
from fastapi import Request

from hivemind.privacy_audit import PrivacyAudit, install
from hivemind.storage import Problem, uid


@pytest.fixture
def surface(world):
    """Install the module the way the integration layer will."""
    app = world.app

    async def principal(request: Request):
        return app.state.authenticate_request(request)

    from hivemind.api import envelope

    def command(p, operation, key, request, function):
        payload = request.model_dump(by_alias=True, exclude_none=True) if hasattr(request, "model_dump") else request
        return envelope(app.state.service.mutate(p, operation, key, payload, function))

    install(app, app.state.service, principal, command, envelope)
    return world


def other_project(world, project="proj_other"):
    """A real second project row so cross-project principals satisfy the FK."""
    store = world.app.state.store
    with store.transaction():
        store.db.execute("INSERT OR IGNORE INTO projects(id, title) VALUES(?,?)",
                         (project, "Other project"))
    return project


def test_permissions_review_lists_canonical_policies(surface):
    agent = surface.agent("Reviewed worker")
    board = surface.board()
    # Default posture: no policy row means the documented default.
    result = surface.call("GET", f"projects/{surface.project}/privacy-audit/permissions")
    assert result["project_id"] == surface.project
    entry = next(item for item in result["resources"] if item["resource_id"] == board["id"])
    assert entry["policy"]["public_read"] is True
    assert entry["policy"]["revision"] is None
    assert entry["deletable_via"] is None  # boards have no canonical delete endpoint

    # A canonical policy written through team_access is reflected as-is.
    surface.call("PUT", f"resources/{board['id']}/access", {
        "public_read": False, "public_contribute": False,
        "reader_agent_ids": [agent["id"]], "contributor_agent_ids": [],
        "reader_team_ids": [], "contributor_team_ids": []})
    result = surface.call("GET", f"projects/{surface.project}/privacy-audit/permissions")
    entry = next(item for item in result["resources"] if item["resource_id"] == board["id"])
    assert entry["policy"]["public_read"] is False
    assert entry["policy"]["reader_agent_ids"] == [agent["id"]]
    assert entry["policy"]["revision"] == 1


def test_permission_review_is_operator_only(surface):
    worker = surface.agent("Worker")
    agent_token = surface.credential(worker)
    observer = surface.credential(None, role="observer")
    for path in ("permissions", "audit", "retention", "exports"):
        surface.call("GET", f"projects/{surface.project}/privacy-audit/{path}",
                     token=agent_token, status=403)
        surface.call("GET", f"projects/{surface.project}/privacy-audit/{path}",
                     token=observer, status=403)
    stranger = surface.credential(None, role="human", project=other_project(surface))
    surface.call("GET", f"projects/{surface.project}/privacy-audit/permissions",
                 token=stranger, status=404)


def test_audit_history_is_whitelisted_and_project_bound(surface):
    surface.agent("Audited worker")
    board = surface.board()
    surface.call("PUT", f"resources/{board['id']}/access", {
        "public_read": True, "public_contribute": True,
        "reader_agent_ids": [], "contributor_agent_ids": [],
        "reader_team_ids": [], "contributor_team_ids": []})
    result = surface.call("GET", f"projects/{surface.project}/privacy-audit/audit")
    assert result["events"], "the canonical policy update must be audited"
    event = next(item for item in result["events"] if item["resource_id"] == board["id"])
    assert event["operation"] == "resource_access.update"
    assert event["resource_kind"] == "board"
    assert event["resource_deleted"] is False
    assert event["principal_role"] == "admin"
    # Fixed whitelist: no principal id, no payload, nothing else.
    assert set(event) == {"id", "operation", "resource_id", "resource_kind",
                          "resource_deleted", "principal_role", "time"}
    assert result["scope"] == "canonical_audit_metadata_only"


def test_audit_history_hides_other_projects_and_marks_deletions(surface):
    post = surface.post("Temporary finding")
    surface.call("DELETE", f"posts/{post['id']}", {"expected_revision": post["revision"]})
    result = surface.call("GET", f"projects/{surface.project}/privacy-audit/audit")
    event = next(item for item in result["events"]
                 if item["operation"] == "delete" and item["resource_id"] == post["id"])
    assert event["resource_deleted"] is True
    # A foreign principal id is never disclosed: only the role survives.
    assert all("principal" not in key for item in result["events"] for key in item
               if key != "principal_role")


def test_retention_capabilities_report_disabled_boundaries_honestly(surface):
    result = surface.call("GET", f"projects/{surface.project}/privacy-audit/retention")
    caps = result["capabilities"]
    assert caps["logical_deletion"]["supported"] is True
    assert caps["index_invalidation"]["supported"] is True
    assert caps["memory_expiry"]["supported"] is True
    for name in ("physical_erasure", "cache_purge", "external_provider_deletion",
                 "background_retention_sweep"):
        assert caps[name]["supported"] is False
        assert caps[name]["reason"], name


def test_deletion_preview_reports_affected_scope_without_deleting(surface):
    post = surface.post("Evidence for a memory")
    memory = surface.call("POST", "memories", {
        "project_id": surface.project, "scope_owner_id": surface.project, "title": "Derived fact",
        "content": {"media_type": "text/plain", "text": "The header has links"},
        "evidence_refs": [{"kind": "board_post", "id": post["id"], "revision": post["revision"]}],
    })
    surface.call("POST", f"memories/{memory['id']}:review",
                 {"expected_revision": memory["revision"], "decision": "verify",
                  "reason": "reviewed"})
    current = surface.call("GET", f"memories/{memory['id']}")

    preview = surface.call("GET", f"projects/{surface.project}/privacy-audit/deletion-preview"
                                   f"?resource_id={post['id']}")
    assert preview["deletable"] is True
    assert preview["endpoint"] == f"DELETE /v1/posts/{post['id']}"
    assert preview["revision"] == post["revision"]
    assert preview["dependent_count"] == 1
    assert preview["dependent_memories"][0]["id"] == memory["id"]
    assert "invalidated" in preview["dependent_memories"][0]["effect"]
    assert any("physical erasure" in item for item in preview["not_performed"])
    # The preview is read-only: nothing was deleted or invalidated.
    posts = surface.call("GET", f"boards/{surface.board()['id']}/posts")["items"]
    assert any(item["id"] == post["id"] for item in posts)
    assert surface.call("GET", f"memories/{memory['id']}")["revision"] == current["revision"]


def test_deletion_preview_marks_unsupported_kinds_and_enforces_delete_rules(surface):
    board = surface.board()
    preview = surface.call("GET", f"projects/{surface.project}/privacy-audit/deletion-preview"
                                  f"?resource_id={board['id']}")
    assert preview["deletable"] is False
    assert "board" in preview["unsupported_reason"]

    # A stranger cannot preview what they could not delete.
    stranger = surface.credential(None, role="human", project=other_project(surface))
    surface.call("GET", f"projects/{surface.project}/privacy-audit/deletion-preview"
                        f"?resource_id={board['id']}", token=stranger, status=404)
    surface.call("GET", f"projects/{surface.project}/privacy-audit/deletion-preview"
                        f"?resource_id={uid('missing')}", status=404)


def test_export_requires_typed_reviewed_confirmation(surface):
    surface.call("POST", f"projects/{surface.project}/privacy-audit/exports",
                 {"confirmation": "export please"}, status=422)
    created = surface.call("POST", f"projects/{surface.project}/privacy-audit/exports",
                           {"confirmation": f"EXPORT {surface.project}"})
    assert created["scope"] == "operator_privacy_audit"
    assert created["payload_storage"] == "none_regenerated_at_download"


def test_export_download_reauthorizes_and_contains_only_whitelisted_records(surface):
    agent = surface.agent("Exported worker")
    board = surface.board()
    surface.call("PUT", f"resources/{board['id']}/access", {
        "public_read": True, "public_contribute": True, "reader_agent_ids": [agent["id"]],
        "contributor_agent_ids": [], "reader_team_ids": [], "contributor_team_ids": []})
    created = surface.call("POST", f"projects/{surface.project}/privacy-audit/exports",
                           {"confirmation": f"EXPORT {surface.project}"})

    payload = surface.call("GET", f"privacy-exports/{created['id']}/download")
    assert payload["export_id"] == created["id"]
    assert payload["regenerated"] is True and payload["principal_bound"] is True
    assert payload["permissions"]["resources"]
    assert payload["audit"]["events"]
    import json as _json
    encoded = _json.dumps(payload)
    for forbidden in ("token_hash", ".hivemind-data", "BEGIN", "password", "csrf"):
        assert forbidden not in encoded
    # The download itself is audited.
    history = surface.call("GET", f"projects/{surface.project}/privacy-audit/audit")
    assert any(item["operation"] == "privacy_export.download" and item["resource_id"] == created["id"]
               for item in history["events"])

    # A non-operator is rejected at download time even with the export id.
    worker_token = surface.credential(agent)
    surface.call("GET", f"privacy-exports/{created['id']}/download", token=worker_token, status=403)
    stranger = surface.credential(None, role="human", project=other_project(surface))
    surface.call("GET", f"privacy-exports/{created['id']}/download", token=stranger, status=404)
    # The list is operator-scoped too.
    listed = surface.call("GET", f"projects/{surface.project}/privacy-audit/exports")
    assert [item["id"] for item in listed["exports"]] == [created["id"]]
    surface.call("GET", f"projects/{surface.project}/privacy-audit/exports",
                 token=worker_token, status=403)


def test_export_creation_is_idempotent_per_key(surface):
    key = uid("export")
    first = surface.call("POST", f"projects/{surface.project}/privacy-audit/exports",
                         {"confirmation": f"EXPORT {surface.project}"}, key=key)
    second = surface.call("POST", f"projects/{surface.project}/privacy-audit/exports",
                          {"confirmation": f"EXPORT {surface.project}"}, key=key)
    assert first["id"] == second["id"]
    listed = surface.call("GET", f"projects/{surface.project}/privacy-audit/exports")
    assert len(listed["exports"]) == 1


def test_helper_enforces_operator_scope_directly(surface):
    service = surface.app.state.service
    store = surface.app.state.store
    admin = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    helper = PrivacyAudit(service)
    assert helper.retention_capabilities(admin, surface.project)["capabilities"]
    outsider = {**admin, "id": uid("principal"), "role": "human",
                "project": other_project(surface)}
    with pytest.raises(Problem) as failure:
        helper.permissions_review(outsider, surface.project)
    assert failure.value.status == 404
