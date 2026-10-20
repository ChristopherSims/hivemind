"""Administrator credential rotation/revocation: secrets, scope, CAS and guards."""
import json
import re

import pytest
from fastapi import Request

from hivemind import credential_management as cm
from hivemind.api import envelope, hash_token
from hivemind.storage import uid

ROTATE = "credentials/{id}:rotate"
REVOKE = "credentials/{id}:revoke"


def _install(app):
    """Stand-in for the integration layer's capabilities registration."""

    async def principal(request: Request):
        return app.state.authenticate_request(request)

    def command(p, operation, key, request, function):
        payload = request.model_dump(by_alias=True, exclude_none=True) if hasattr(request, "model_dump") \
            else request
        return envelope(app.state.service.mutate(p, operation, key, payload, function))

    return cm.install(app, app.state.service, principal, command, envelope)


@pytest.fixture
def credentials(world):
    world.installed = _install(world.app)
    world.store = world.app.state.store
    return world


def owner_id(world):
    return world.store.db.execute("SELECT id FROM principals WHERE role='admin'").fetchone()[0]


def add_admin(world, project=None):
    """A second administrator credential as ``(principal_id, token)``."""
    token = world.credential(role="admin", project=project)
    identity = world.store.db.execute(
        "SELECT id FROM principals WHERE token_hash=?", (hash_token(token),)).fetchone()[0]
    return identity, token


def add_agent_credential(world, agent=None):
    agent = agent or world.agent("Rotatable")
    token = world.credential(agent)
    identity = world.store.db.execute(
        "SELECT id FROM principals WHERE token_hash=?", (hash_token(token),)).fetchone()[0]
    return agent, identity, token


def impact(world, credential_id, operation="revoke", token=None, status=200):
    return world.call("GET", f"credentials/{credential_id}/impact?operation={operation}",
                      token=token, status=status)


def rotate(world, credential_id, revision, fingerprint, token=None, key=None, status=200):
    return world.call("POST", ROTATE.format(id=credential_id),
                      {"expected_revision": revision, "acknowledged_impact": fingerprint},
                      token, key=key, status=status)


def revoke(world, credential_id, revision, fingerprint, token=None, key=None, status=200):
    return world.call("POST", REVOKE.format(id=credential_id),
                      {"expected_revision": revision, "acknowledged_impact": fingerprint},
                      token, key=key, status=status)


def scan(store, needle):
    """Every stored text value in the canonical database, for secret-absence checks."""
    found = []
    tables = [row[0] for row in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    for name in tables:
        try:
            rows = store.db.execute(f'SELECT * FROM "{name}"').fetchall()
        except Exception:  # extension-managed tables (vec0) may refuse a plain scan
            continue
        for index, row in enumerate(rows):
            for value in tuple(row):
                if isinstance(value, str) and needle in value:
                    found.append((name, index))
                elif isinstance(value, bytes) and needle.encode() in value:
                    found.append((name, index))
    return found


def test_install_registers_credential_routes_and_descriptors(credentials):
    installed = credentials.installed
    assert set(installed["endpoints"]) == {
        "GET /v1/projects/{project_id}/credentials",
        "GET /v1/credentials/{credential_id}",
        "GET /v1/credentials/{credential_id}/impact",
        "POST /v1/credentials/{credential_id}:rotate",
        "POST /v1/credentials/{credential_id}:revoke",
    }
    assert installed["models"] == list(cm.MODELS) == ["CredentialConfirmation"]
    assert set(cm.descriptors()) == set(cm.OPERATION_DESCRIPTORS)
    required = {"name", "purpose", "input", "output", "authority", "side_effects", "limits",
                "retry_safety"}
    for name, descriptor in cm.OPERATION_DESCRIPTORS.items():
        assert required <= set(descriptor)
        assert descriptor["name"] == name
    assert set(cm.POLICY) >= {"listing", "rotation", "revocation", "last_usable_admin", "scope",
                              "confirmation", "sessions"}
    assert cm.DISCOVERY_CAPABILITIES == ("credentials.rotation", "credentials.revocation")
    assert set(cm.CAPABILITY_STATUS) == set(cm.DISCOVERY_CAPABILITIES)
    for name in cm.DISCOVERY_CAPABILITIES:
        assert cm.CAPABILITY_STATUS[name]["enabled"] is True
        assert cm.CAPABILITY_STATUS[name]["administrator_only"] is True
        assert cm.CAPABILITY_STATUS[name]["sessions_invalidated"] == "all_of_that_credential"
    assert cm.CAPABILITY_STATUS["credentials.rotation"]["token_persisted"] is False
    assert cm.CAPABILITY_STATUS["credentials.rotation"]["privileges_changed"] is False
    assert cm.CAPABILITY_STATUS["credentials.revocation"]["principal_preserved"] is True
    assert cm.CAPABILITY_STATUS["credentials.revocation"]["resource_deletion"] is False
    assert "no_impersonation" in cm.POLICY and "administrator_token_handling" in cm.POLICY


def test_listing_is_administrator_only_and_exposes_no_token_material(credentials):
    agent, credential_id, agent_token = add_agent_credential(credentials)
    human_token = credentials.credential(role="human")
    assert credentials.client.get(
        f"/v1/projects/{credentials.project}/credentials").status_code == 401
    credentials.call("GET", f"projects/{credentials.project}/credentials", token=agent_token, status=403)
    credentials.call("GET", f"projects/{credentials.project}/credentials", token=human_token, status=403)

    result = credentials.call("GET", f"projects/{credentials.project}/credentials")
    ids = [item["credential_id"] for item in result["credentials"]]
    assert owner_id(credentials) in ids and credential_id in ids
    assert result["count"] == len(ids) and result["has_more"] is False and result["limit"] == 100
    assert result["token_material_returned"] is False
    assert result["summary"]["usable_admins"] == 1 and result["summary"]["project_admins"] == 1
    assert result["summary"]["revoked_credentials"] == 0
    rendered = json.dumps(result["credentials"])
    assert "token" not in rendered.lower()
    assert re.search(r"[0-9a-f]{64}", rendered) is None
    listed = next(item for item in result["credentials"] if item["credential_id"] == owner_id(credentials))
    assert listed["role"] == "admin" and listed["usable"] is True and listed["self"] is True
    assert listed["state"] == "active" and listed["revision"] == 1
    assert listed["active_sessions"] == 0 and listed["rotation_count"] == 0
    assert listed["rotated_at"] is None and listed["revoked_at"] is None
    agent_item = next(item for item in result["credentials"] if item["credential_id"] == credential_id)
    assert agent_item["agent_id"] == agent["id"] and agent_item["agent_display_name"] == agent["display_name"]
    assert agent_item["self"] is False and agent_item["role"] == "agent"

    only_agents = credentials.call("GET", f"projects/{credentials.project}/credentials?role=agent")
    assert {item["role"] for item in only_agents["credentials"]} == {"agent"}
    scoped = credentials.call("GET",
                              f"projects/{credentials.project}/credentials?agent_id={agent['id']}")
    assert [item["credential_id"] for item in scoped["credentials"]] == [credential_id]
    limited = credentials.call("GET", f"projects/{credentials.project}/credentials?limit=1")
    assert len(limited["credentials"]) == 1 and limited["has_more"] is True
    credentials.call("GET", f"projects/{credentials.project}/credentials?role=root", status=422)
    credentials.call("GET", f"projects/{credentials.project}/credentials?limit=0", status=422)
    credentials.call("GET", f"projects/{credentials.project}/credentials?limit=201", status=422)
    credentials.call("GET", f"projects/{credentials.project}/credentials?agent_id={uid('agent')}",
                     status=404)
    credentials.call("GET", f"projects/{uid('project')}/credentials", status=404)
    assert credentials.call("GET", f"credentials/{credential_id}")["credential"]["credential_id"] == \
        credential_id


def test_rotation_returns_one_token_and_never_persists_it(credentials):
    credential_id, _ = add_admin(credentials)
    original = credentials.store.db.execute("SELECT * FROM principals WHERE id=?",
                                            (credential_id,)).fetchone()
    review = impact(credentials, credential_id, "rotate")
    assert review["state"] == "active" and review["self_affected"] is False
    assert review["last_usable_admin"] is False and review["usable_admins"] == 2
    assert review["token_material_returned"] is False
    assert any("token hash" in warning for warning in review["warnings"])
    assert re.search(r"[0-9a-f]{64}", json.dumps({k: v for k, v in review.items()
                                                  if k != "fingerprint"})) is None

    key = uid("key")
    result = rotate(credentials, credential_id, review["revision"], review["fingerprint"], key=key)
    token = result["token"]
    assert result["status"] == "rotated" and result["token_available"] is True
    assert result["old_token_invalidated"] is True and result["sessions_invalidated"] == 0
    assert isinstance(token, str) and len(token) > 32
    assert token not in json.dumps(result["credential"])
    assert result["credential"]["revision"] == review["revision"] + 1
    assert result["credential"]["rotation_count"] == 1 and result["credential"]["rotated_at"]
    assert result["credential"]["role"] == "admin" and result["credential"]["usable"] is True

    # The new token authenticates as exactly that principal; the old one is dead.
    assert credentials.call("GET", "me", token=token)["principal_id"] == credential_id
    current = credentials.store.db.execute("SELECT * FROM principals WHERE id=?",
                                           (credential_id,)).fetchone()
    for column in ("role", "project", "agent", "coordinator", "capture_authority"):
        assert current[column] == original[column]
    assert current["token_hash"] != original["token_hash"]

    # The replacement secret exists only in the immediate response.
    assert scan(credentials.store, token) == []
    journal = credentials.store.db.execute("SELECT * FROM credential_operations").fetchall()
    assert len(journal) == 1 and journal[0]["token_returned"] == 1
    assert token not in journal[0]["result"] and "token_available" in journal[0]["result"]
    assert credentials.store.db.execute(
        "SELECT count(*) FROM commands WHERE operation LIKE 'credential%'").fetchone()[0] == 0
    audit = credentials.store.db.execute(
        "SELECT * FROM audit WHERE operation='credential.rotate'").fetchall()
    assert len(audit) == 1 and audit[0]["resource"] == credential_id

    # An idempotent retry reports a safe receipt: never the token, never a second rotation.
    retry = rotate(credentials, credential_id, review["revision"], review["fingerprint"], key=key)
    assert retry["status"] == "already_applied" and retry["token_available"] is False
    assert "token" not in retry and "cannot be shown again" in retry["token_notice"]
    assert retry["credential"]["revision"] == review["revision"] + 1
    assert credentials.call("GET", "me", token=token)["principal_id"] == credential_id
    # A fresh key with the stale revision fails the CAS instead of rotating twice.
    rotate(credentials, credential_id, review["revision"], review["fingerprint"], key=uid("key"),
           status=412)
    assert credentials.store.db.execute(
        "SELECT revision FROM credential_records WHERE principal=?",
        (credential_id,)).fetchone()[0] == review["revision"] + 1
    assert credentials.call("GET", "me", token=token)["principal_id"] == credential_id
    # A reused key with a different payload is a conflict, never a silent re-rotation.
    rotate(credentials, credential_id, review["revision"] + 1, review["fingerprint"], key=key,
           status=409)


def test_rotation_requires_fresh_impact_confirmation_and_a_key(credentials):
    credential_id, _ = add_admin(credentials)
    review = impact(credentials, credential_id, "rotate")
    rotate(credentials, credential_id, review["revision"], "0" * 64, status=409)
    rotate(credentials, credential_id, review["revision"] + 5, review["fingerprint"], status=412)
    body = {"expected_revision": review["revision"], "acknowledged_impact": review["fingerprint"]}
    missing_key = credentials.client.post(
        f"/v1/credentials/{credential_id}:rotate", json=body,
        headers={"Authorization": "Bearer " + credentials.owner})
    assert missing_key.status_code == 422, missing_key.text
    assert missing_key.json()["error"]["code"] == "idempotency_key_required"
    credentials.call("POST", ROTATE.format(id=credential_id),
                     {"expected_revision": 0, "acknowledged_impact": "nope"}, status=422)
    # Impact review is administrator-only and refuses unknown operations/resources.
    human_token = credentials.credential(role="human")
    impact(credentials, credential_id, "rotate", token=human_token, status=403)
    impact(credentials, credential_id, "delete", status=422)
    credentials.call("GET", f"credentials/{uid('principal')}/impact", status=404)
    credentials.call("GET", f"credentials/{uid('principal')}", status=404)
    # Nothing was rotated by any of the refused attempts.
    assert credentials.store.db.execute(
        "SELECT count(*) FROM credential_operations").fetchone()[0] == 0
    assert credentials.call("GET", f"credentials/{credential_id}")["credential"]["rotation_count"] == 0


def test_rotation_deletes_sessions_including_the_operators_own(credentials):
    credential_id, token = add_admin(credentials)
    login = credentials.client.post("/v1/session", headers={"Authorization": "Bearer " + token})
    assert login.status_code == 200, login.text
    csrf = login.json()["data"]["csrf_token"]
    assert credentials.client.get("/v1/me").json()["data"]["principal_id"] == credential_id

    review = credentials.client.get(
        f"/v1/credentials/{credential_id}/impact?operation=rotate").json()["data"]
    assert review["active_sessions"] == 1 and review["self_affected"] is True
    assert any("your own credential" in warning for warning in review["warnings"])
    assert any("signed out" in warning for warning in review["warnings"])
    # The same review read by a different administrator is not self-affecting.
    other = credentials.call("GET", f"credentials/{credential_id}/impact?operation=rotate")
    assert other["self_affected"] is False and other["fingerprint"] != review["fingerprint"]
    response = credentials.client.post(
        f"/v1/credentials/{credential_id}:rotate",
        json={"expected_revision": review["revision"], "acknowledged_impact": review["fingerprint"]},
        headers={"X-CSRF-Token": csrf, "Idempotency-Key": uid("key")})
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["token_available"] is True and data["self_affected"] is True
    assert data["sessions_invalidated"] == 1
    # Honest self-service: the operator's own session and old token stop working.
    assert credentials.client.get("/v1/me").status_code == 401
    credentials.call("GET", "me", token=token, status=401)
    assert credentials.call("GET", "me", token=data["token"])["principal_id"] == credential_id


def test_revocation_is_terminal_and_preserves_the_principal_and_its_work(credentials):
    agent, credential_id, agent_token = add_agent_credential(credentials, credentials.agent("Revocable"))
    run = credentials.run(agent)
    resources = credentials.store.db.execute("SELECT count(*) FROM resources").fetchone()[0]
    before = credentials.store.db.execute(
        "SELECT role, project, agent, coordinator, capture_authority FROM principals WHERE id=?",
        (credential_id,)).fetchone()

    review = impact(credentials, credential_id, "revoke")
    assert review["active_run_ids"] == [run["id"]] and review["active_runs"][0]["presence"] == "online"
    assert any("lose API access" in warning for warning in review["warnings"])
    result = revoke(credentials, credential_id, review["revision"], review["fingerprint"])
    assert result["status"] == "revoked" and result["revoked"] is True
    assert result["token_available"] is False and result["principal_preserved"] is True
    assert result["effective_state"] == "revoked" and result["credential"]["usable"] is False
    assert result["credential"]["revoked_at"] and result["sessions_invalidated"] == 0
    credentials.call("GET", "me", token=agent_token, status=401)

    after = credentials.store.db.execute("SELECT * FROM principals WHERE id=?",
                                         (credential_id,)).fetchone()
    for column in ("role", "project", "agent", "coordinator", "capture_authority"):
        assert after[column] == before[column]
    assert after["token_hash"] == cm.REVOKED_MARKER + credential_id
    assert credentials.store.db.execute("SELECT count(*) FROM resources").fetchone()[0] == resources
    assert credentials.call("GET", f"runs/{run['id']}")["id"] == run["id"]
    assert credentials.call("GET", f"agents/{agent['id']}")["id"] == agent["id"]
    assert credentials.store.db.execute(
        "SELECT count(*) FROM audit WHERE operation='credential.revoke' AND resource=?",
        (credential_id,)).fetchone()[0] == 1

    # Terminal: never revived, never re-revoked, never rotated back to life.
    stated = credentials.call("GET", f"credentials/{credential_id}")["credential"]
    assert stated["state"] == "revoked" and stated["usable"] is False
    revoke(credentials, credential_id, stated["revision"] + 5, review["fingerprint"], status=412)
    fresh = impact(credentials, credential_id, "revoke")
    assert any("already revoked" in warning for warning in fresh["warnings"])
    revoke(credentials, credential_id, fresh["revision"], fresh["fingerprint"], key=uid("key"),
           status=409)
    rotating = impact(credentials, credential_id, "rotate")
    rotate(credentials, credential_id, rotating["revision"], rotating["fingerprint"], status=409)
    credentials.call("GET", "me", token=agent_token, status=401)


def test_revocation_invalidates_sessions_and_idempotent_replay_stays_safe(credentials):
    human_token = credentials.credential(role="human")
    credential_id = credentials.store.db.execute(
        "SELECT id FROM principals WHERE role='human'").fetchone()[0]
    login = credentials.client.post("/v1/session", headers={"Authorization": "Bearer " + human_token})
    assert login.status_code == 200
    assert credentials.client.get("/v1/me").json()["data"]["principal_id"] == credential_id

    review = impact(credentials, credential_id, "revoke")
    assert review["active_sessions"] == 1
    key = uid("key")
    result = revoke(credentials, credential_id, review["revision"], review["fingerprint"], key=key)
    assert result["sessions_invalidated"] == 1
    assert credentials.client.get("/v1/me").status_code == 401
    credentials.call("GET", "me", token=human_token, status=401)
    # The exact retry replays a safe, re-authorized receipt: no second revocation,
    # no token, and the canonical revision is unchanged.
    replay = revoke(credentials, credential_id, review["revision"], review["fingerprint"], key=key)
    assert replay["status"] == "already_applied" and replay["revoked"] is True
    assert replay["token_available"] is False and "cannot be restored" in replay["token_notice"]
    assert replay["sessions_invalidated"] == 0
    assert credentials.store.db.execute(
        "SELECT revision FROM credential_records WHERE principal=?",
        (credential_id,)).fetchone()[0] == review["revision"] + 1
    assert credentials.store.db.execute(
        "SELECT count(*) FROM audit WHERE operation='credential.revoke'").fetchone()[0] == 1
    # A reused key with a different payload is a conflict, never a silent second action.
    revoke(credentials, credential_id, review["revision"] + 1, review["fingerprint"], key=key,
           status=409)


def test_last_usable_admin_is_protected_from_revocation(credentials):
    owner = owner_id(credentials)
    review = impact(credentials, owner, "revoke")
    assert review["last_usable_admin"] is True and review["usable_admins"] == 1
    assert review["self_affected"] is True
    assert any("Refused" in warning for warning in review["warnings"])
    revoke(credentials, owner, review["revision"], review["fingerprint"], status=409)
    assert credentials.call("GET", "me")["principal_id"] == owner

    second, second_token = add_admin(credentials)
    second_review = impact(credentials, second, "revoke")
    assert second_review["last_usable_admin"] is False
    revoke(credentials, second, second_review["revision"], second_review["fingerprint"])
    credentials.call("GET", "me", token=second_token, status=401)
    owner_review = impact(credentials, owner, "revoke")
    assert owner_review["last_usable_admin"] is True
    revoke(credentials, owner, owner_review["revision"], owner_review["fingerprint"], status=409)

    # Rotating the last administrator is allowed: the credential remains usable.
    rotating = impact(credentials, owner, "rotate")
    assert rotating["last_usable_admin"] is True
    assert any("last usable administrator" in warning for warning in rotating["warnings"])
    rotated = rotate(credentials, owner, rotating["revision"], rotating["fingerprint"])
    assert rotated["self_affected"] is True and rotated["old_token_invalidated"] is True
    credentials.call("GET", "me", status=401)
    assert credentials.call("GET", "me", token=rotated["token"])["principal_id"] == owner
    assert credentials.call("GET", f"projects/{credentials.project}/credentials",
                            token=rotated["token"])["summary"]["usable_admins"] == 1


def test_scope_stays_exact_and_never_increases_privileges(credentials):
    agent = credentials.agent("Scoped")
    _, credential_id, _ = add_agent_credential(credentials, agent)
    before = credentials.store.db.execute(
        "SELECT role, project, agent, coordinator, capture_authority FROM principals WHERE id=?",
        (credential_id,)).fetchone()
    review = impact(credentials, credential_id, "rotate")
    result = rotate(credentials, credential_id, review["revision"], review["fingerprint"])
    after = credentials.store.db.execute(
        "SELECT role, project, agent, coordinator, capture_authority FROM principals WHERE id=?",
        (credential_id,)).fetchone()
    assert tuple(after) == tuple(before)
    assert result["credential"]["role"] == "agent"
    assert result["credential"]["agent_id"] == agent["id"]
    assert result["credential"]["coordinator"] is False
    # The rotated agent credential keeps agent authority only.
    credentials.call("POST", "agents", {"project_id": credentials.project, "display_name": "Nope"},
                     token=result["token"], status=403)
    credentials.call("GET", f"projects/{credentials.project}/credentials", token=result["token"],
                     status=403)

    other_project = uid("project")
    with credentials.store.transaction():
        credentials.store.db.execute("INSERT INTO projects(id,title) VALUES(?,?)",
                                     (other_project, "Other project"))
    foreign, foreign_token = add_admin(credentials, other_project)
    other = credentials.call("GET", f"projects/{other_project}/credentials", token=foreign_token)
    assert [item["credential_id"] for item in other["credentials"]] == [foreign]
    assert other["credentials"][0]["project_id"] == other_project
    assert other["summary"]["project_admins"] == 1
    assert other["summary"]["usable_admins"] == 2  # administrators serve every project
    # A project principal can never read or mutate another project's credentials.
    credentials.call("GET", f"projects/{other_project}/credentials", token=result["token"], status=404)
    credentials.call("GET", f"credentials/{foreign}", token=result["token"], status=404)
    foreign_review = credentials.call("GET", f"credentials/{foreign}/impact?operation=revoke")
    assert foreign_review["credential_id"] == foreign
    credentials.call("POST", REVOKE.format(id=foreign),
                     {"expected_revision": foreign_review["revision"],
                      "acknowledged_impact": foreign_review["fingerprint"]},
                     token=result["token"], status=404)
    assert credentials.call("GET", "me", token=foreign_token)["principal_id"] == foreign
    # Administrator scope is still bounded by the credential's own project record.
    isolated = {"credential_id": foreign, "project_id": other_project}
    assert credentials.call("GET", f"credentials/{foreign}")["credential"]["project_id"] == \
        isolated["project_id"]


def test_impact_reports_owned_workers_and_rotation_does_not_revive_them(credentials):
    agent, credential_id, agent_token = add_agent_credential(credentials, credentials.agent("Worker"))
    run = credentials.run(agent)
    # A canonical worker_provision record owned by this credential (World fixture, no
    # real process, private config or credential file is created).
    worker_id = uid("worker")
    with credentials.store.transaction():
        credentials.store.db.execute(
            "INSERT INTO resources(id,kind,project,owner,revision,seq,body) VALUES(?,?,?,?,?,?,?)",
            (worker_id, "worker_provision", credentials.project, owner_id(credentials), 1, 900,
             json.dumps({"id": worker_id, "project_id": credentials.project, "agent_id": agent["id"],
                         "run_id": run["id"], "task_id": None, "principal_id": credential_id,
                         "state": "running"})))
    review = impact(credentials, credential_id, "rotate")
    assert [worker["worker_id"] for worker in review["owned_workers"]] == [worker_id]
    assert any("owned worker" in warning for warning in review["warnings"])
    rotated = rotate(credentials, credential_id, review["revision"], review["fingerprint"])
    assert rotated["token_available"] is True
    # The worker record and its owner are untouched; only the credential changed.
    row = credentials.store.raw(worker_id, "worker_provision")
    assert row["owner"] == owner_id(credentials)
    assert json.loads(row["body"])["principal_id"] == credential_id
    assert credentials.call("GET", "me", token=agent_token, status=401)
    assert credentials.call("GET", "me", token=rotated["token"])["principal_id"] == credential_id
    assert json.dumps(rotated).count(rotated["token"]) == 1


def test_listing_and_guards_follow_canonical_authority_not_a_stale_snapshot(credentials):
    """An out-of-band role/agent change is reported and enforced canonically."""
    agent, credential_id, token = add_agent_credential(credentials, credentials.agent("Promoted"))
    credentials.call("GET", f"projects/{credentials.project}/credentials")
    with credentials.store.transaction():
        credentials.store.db.execute("UPDATE principals SET role='observer' WHERE id=?",
                                     (credential_id,))
    listed = next(item for item in
                  credentials.call("GET", f"projects/{credentials.project}/credentials?limit=200")[
                      "credentials"] if item["credential_id"] == credential_id)
    assert listed["role"] == "observer"
    # The demoted credential keeps working with its new authority and cannot manage credentials.
    assert credentials.call("GET", "me", token=token)["role"] == "observer"
    credentials.call("GET", f"projects/{credentials.project}/credentials", token=token, status=403)
    # A promotion recorded out of band is enforced before the next mutation converges the ledger.
    with credentials.store.transaction():
        credentials.store.db.execute("UPDATE principals SET role='admin', agent=NULL WHERE id=?",
                                     (credential_id,))
    promoted = credentials.call("GET", f"credentials/{credential_id}")["credential"]
    assert promoted["role"] == "admin" and promoted["agent_id"] is None
    assert credentials.call("GET", f"projects/{credentials.project}/credentials",
                            token=token)["summary"]["usable_admins"] == 2
    # Revocation now treats it as an administrator: not the last one, so it is allowed,
    # and the ledger snapshot converges to the canonical role.
    review = impact(credentials, credential_id, "revoke")
    assert review["last_usable_admin"] is False
    revoke(credentials, credential_id, review["revision"], review["fingerprint"])
    record = credentials.store.db.execute(
        "SELECT * FROM credential_records WHERE principal=?", (credential_id,)).fetchone()
    assert record["role"] == "admin" and record["agent"] is None and record["state"] == "revoked"
    assert credentials.call("GET", "me", token=token, status=401)
    assert credentials.call("GET", f"projects/{credentials.project}/credentials")["summary"][
        "usable_admins"] == 1
    # The last administrator is protected even after an out-of-band demotion removed the
    # other one, because the guard reads canonical principals.
    other, other_token = add_admin(credentials)
    with credentials.store.transaction():
        credentials.store.db.execute("UPDATE principals SET role='observer' WHERE id=?",
                                     (other,))
    owner = owner_id(credentials)
    owner_review = impact(credentials, owner, "revoke")
    assert owner_review["last_usable_admin"] is True
    revoke(credentials, owner, owner_review["revision"], owner_review["fingerprint"], status=409)
    assert credentials.call("GET", "me")["principal_id"] == owner


def test_revoked_and_downgraded_actors_lose_current_authority(credentials):
    first, first_token = add_admin(credentials)
    second, second_token = add_admin(credentials)
    assert credentials.call("GET", f"projects/{credentials.project}/credentials",
                            token=first_token)["summary"]["usable_admins"] == 3
    review = impact(credentials, first, "revoke")
    key = uid("key")
    revoke(credentials, first, review["revision"], review["fingerprint"], key=key)
    # Authority is evaluated per request: a revoked administrator cannot manage credentials.
    credentials.call("GET", f"projects/{credentials.project}/credentials", token=first_token,
                     status=401)
    credentials.call("GET", f"credentials/{second}/impact", token=first_token, status=401)
    # Even an exact idempotent retry of its own committed command is refused once the
    # acting credential is revoked: the safe receipt is re-authorized, not replayed.
    credentials.call("POST", REVOKE.format(id=first),
                     {"expected_revision": review["revision"],
                      "acknowledged_impact": review["fingerprint"]},
                     token=first_token, key=key, status=401)
    # An out-of-band invalidation of the acting credential also fails closed.
    with credentials.store.transaction():
        credentials.store.db.execute("UPDATE principals SET token_hash=? WHERE id=?",
                                     (cm.REVOKED_MARKER + second, second))
    credentials.call("GET", f"projects/{credentials.project}/credentials", token=second_token,
                     status=401)
    # A downgraded role loses administrator operations on the next request.
    third, third_token = add_admin(credentials)
    with credentials.store.transaction():
        credentials.store.db.execute("UPDATE principals SET role='observer' WHERE id=?", (third,))
    credentials.call("GET", f"projects/{credentials.project}/credentials", token=third_token,
                     status=403)
    credentials.call("POST", ROTATE.format(id=third),
                     {"expected_revision": 1, "acknowledged_impact": "0" * 64},
                     token=third_token, status=403)
    # A credential whose principal row was removed out of band reads honestly as missing
    # and can no longer be mutated. The ledger must already know it (it was listed).
    fourth, _ = add_admin(credentials)
    credentials.call("GET", f"projects/{credentials.project}/credentials")
    credentials.store.db.execute("DELETE FROM principals WHERE id=?", (fourth,))
    missing = credentials.call("GET", f"credentials/{fourth}")["credential"]
    assert missing["state"] == "missing" and missing["usable"] is False
    missing_impact = impact(credentials, fourth, "revoke")
    assert any("no longer authenticate" in warning for warning in missing_impact["warnings"])
    revoke(credentials, fourth, missing_impact["revision"], missing_impact["fingerprint"], status=404)
    rotate_impact = impact(credentials, fourth, "rotate")
    # The impact fingerprint is operation-bound: a revocation acknowledgement can never
    # authorize a rotation of the same revision.
    assert rotate_impact["fingerprint"] != missing_impact["fingerprint"]
    rotate(credentials, fourth, missing_impact["revision"], missing_impact["fingerprint"], status=409)
    # A missing principal is never "active", so rotation refuses it by state.
    rotate(credentials, fourth, rotate_impact["revision"], rotate_impact["fingerprint"], status=409)
    # An unknown credential is indistinguishable from a private one.
    credentials.call("GET", f"credentials/{uid('principal')}", status=404)
