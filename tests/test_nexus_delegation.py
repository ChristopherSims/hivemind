import time


def setup(world):
    nexus = world.agent("Nexus")
    target = world.agent("Allowed", ["pause", "cancel"])
    other = world.agent("Other", ["pause"])
    token = world.credential(nexus, coordinator=True)
    run = world.run(nexus, token)
    target_run = world.run(target)
    other_run = world.run(other)
    grant = world.call("POST", "nexus-grants", {
        "project_id": world.project, "nexus_agent_id": nexus["id"],
        "target_agent_ids": [target["id"]], "actions": ["pause"]})
    lease = world.call("POST", f"projects/{world.project}/nexus:acquire",
                      {"agent_id": nexus["id"], "run_id": run["id"]}, token)
    return nexus, target, token, target_run, other_run, grant, lease


def issue(world, token, run, lease, action="pause", status=200, key=None):
    return world.call("POST", "controls", {
        "project_id": world.project, "target_run_id": run["id"], "action": action,
        "target_run_revision": run["control_revision"], "coordinator_epoch": lease["coordinator_epoch"],
        "reason": "Scoped command"}, token, key=key, status=status)


def test_target_action_and_replay_are_scoped(world):
    _, _, token, run, other, grant, lease = setup(world)
    issue(world, token, other, lease, status=403)
    issue(world, token, run, lease, "cancel", status=403)
    control = issue(world, token, run, lease, key="same-command")
    world.call("POST", f"nexus-grants/{grant['id']}:revoke",
               {"expected_revision": grant["revision"]})
    issue(world, token, run, lease, status=403, key="same-command")
    assert world.app.state.store.raw(control["id"]) is not None


def test_revocation_blocks_target_ack_and_checkpoint(world):
    _, target, token, run, _, grant, lease = setup(world)
    control = issue(world, token, run, lease)
    target_token = world.credential(target)
    world.call("POST", f"nexus-grants/{grant['id']}:revoke",
               {"expected_revision": grant["revision"]})
    world.call("POST", f"controls/{control['id']}:ack", {}, target_token, status=409)
    assert world.call("GET", f"agents/{target['id']}/controls?project_id={world.project}",
                      token=target_token) == []


def test_deleted_and_expired_grants_never_restore_legacy_authority(world):
    nexus, _, token, run, _, grant, lease = setup(world)
    store = world.app.state.store
    with store.transaction():
        body = store.get(grant["id"], {"role": "admin", "id": "test"}, "nexus_grant")
        body["expires_at"] = time.time() - 1
        row = store.raw(grant["id"])
        store.save(row["kind"], row["project"], row["owner"], body)
    issue(world, token, run, lease, status=403)
    with store.transaction():
        store.db.execute("DELETE FROM resources WHERE id=?", (grant["id"],))
    issue(world, token, run, lease, status=403)
    assert store.db.execute("SELECT 1 FROM nexus_restrictions WHERE agent=?", (nexus["id"],)).fetchone()


def test_only_operator_can_create_or_revoke_and_revision_is_required(world):
    nexus, target, token, _, _, grant, _ = setup(world)
    world.call("POST", "nexus-grants", {
        "project_id": world.project, "nexus_agent_id": nexus["id"],
        "target_agent_ids": [target["id"]], "actions": ["pause"]}, token, status=403)
    world.call("POST", f"nexus-grants/{grant['id']}:revoke",
               {"expected_revision": 999}, status=412)


def test_new_grant_cannot_resurrect_old_control_and_pins_bound_expiry(world):
    nexus, target, token, run, _, grant, lease = setup(world)
    control = issue(world, token, run, lease)
    assert control["delegation"] == {"grant_id": grant["id"], "revision": grant["revision"]}
    assert control["expires_at"] <= grant["expires_at"]
    world.call("POST", f"nexus-grants/{grant['id']}:revoke",
               {"expected_revision": grant["revision"]})
    world.call("POST", "nexus-grants", {
        "project_id": world.project, "nexus_agent_id": nexus["id"],
        "target_agent_ids": [target["id"]], "actions": ["pause"]})
    world.call("POST", f"controls/{control['id']}:ack", {}, world.credential(target), status=409)
    assert world.app.state.store.get(control["id"],
        {"role": "admin", "id": "operator"}, "control")["state"] == "superseded"


def test_restriction_survives_service_restart_and_operators_only_view_grants(world):
    from hivemind.service import Service
    nexus, _, token, _, _, _, _ = setup(world)
    service = Service(world.app.state.store)
    assert service.nexus_delegation.restricted(world.project, nexus["id"])
    world.call("GET", f"projects/{world.project}/nexus-grants", token=token, status=403)
    human = world.credential(role="human")
    assert len(world.call("GET", f"projects/{world.project}/nexus-grants", token=human)) == 1
