import json

from test_worker_provisioning import create, enabled as provisioning_enabled

enabled = provisioning_enabled


def test_worker_catalog_is_operator_bounded_and_reports_current_credential_state(world, enabled):
    worker = create(world, start=False)
    path = f"projects/{world.project}/workers"
    result = world.call("GET", path)
    assert [item["id"] for item in result["workers"]] == [worker["id"]]
    assert not result["has_more"]
    agent = world.call("GET", f"agents/{worker['agent_id']}")
    token = world.credential(agent)
    world.call("GET", path, token=token, status=403)
    store = world.app.state.store
    principal_id = store.db.execute("SELECT id FROM principals WHERE agent=? AND role='agent'",
                                   (worker["agent_id"],)).fetchone()[0]
    with store.transaction():
        row = store.raw(worker["id"], "worker_provision")
        body = json.loads(row["body"])
        body["principal_id"] = principal_id
        store.save(row["kind"], row["project"], row["owner"], body, expected=row["revision"])
    assert world.call("GET", path)["workers"][0]["monitoring"]["credential_revoked"] is False
    with store.transaction():
        store.db.execute("DELETE FROM principals WHERE id=?", (principal_id,))
    assert world.call("GET", path)["workers"][0]["monitoring"]["credential_revoked"] is True
    assert "token" not in json.dumps(world.call("GET", path)).lower()
