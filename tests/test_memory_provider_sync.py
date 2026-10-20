import json
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from hivemind.api import create_app
from hivemind.memory_adapters import (
    LangMemAdapter, LettaAdapter, Mem0Adapter, MemoryAdapterBridge, MemoryAdapterPolicy, MemoryMirrorRequest,
)
from hivemind.memory_provider_sync import ProviderImport, ProviderRetrieve
from hivemind.storage import Problem
from test_additional_integrations import memory


class DataPort:
    def __init__(self):
        self.records = {}
        self.calls = []
        self.fail_delete = None
        self.fail_get = None

    def put(self, namespace, key, value, *, index):
        assert index is False
        self.calls.append(("put", key))
        self.records[key] = {"key": key, "namespace": namespace, "value": value}

    def get(self, namespace, key):
        self.calls.append(("get", key))
        if self.fail_get:
            self.fail_get()
        return self.records.get(key)

    def search(self, namespace, *, limit):
        self.calls.append(("list", namespace))
        return list(self.records.values())[:limit]

    def delete(self, namespace, key):
        self.calls.append(("delete", key))
        if self.fail_delete:
            self.fail_delete(key)
        else:
            self.records.pop(key, None)


@pytest.fixture
def setup(world):
    bridges = []
    def build(**flags):
        port = DataPort()
        adapter = LangMemAdapter(port, world.project, "ns", binding_id="account-one/store-one")
        policy = MemoryAdapterPolicy(enabled=True, allow_remote_writes=True,
            allow_remote_reads=True, allow_imports=True, allow_remote_deletes=True, **flags)
        bridge = MemoryAdapterBridge(world.app.state.service, {"langmem": adapter}, policy)
        bridges.append(bridge)
        principal = dict(world.app.state.store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
        return bridge, principal, port, adapter
    yield build
    for bridge in bridges:
        assert bridge.close()


def publish(world, setup, **flags):
    item, source = memory(world)
    bridge, principal, port, adapter = setup(**flags)
    bridge.publish(principal, item["id"], MemoryMirrorRequest(adapter="langmem", expected_revision=item["revision"]), "put")
    return item, source, bridge, principal, port, adapter


def retrieve(bridge, principal, project, key="retrieve"):
    return bridge.sync.retrieve(principal, project, ProviderRetrieve(adapter="langmem"), key)


def request(result, source):
    return ProviderImport(result_id=result["id"], item_index=0,
        content_sha256=result["items"][0]["content_sha256"], title="Imported claim", approve_import=True,
        evidence_refs=[{"kind": "board_post", "id": source["id"], "revision": source["revision"]}])


def drain(bridge, principal, project, count=3):
    return [bridge.sync.reconcile(principal, project, "langmem") for _ in range(count)]


def test_retrieval_and_import_are_data_only_proposed_and_deduplicated(world, setup):
    _, source, bridge, principal, port, _ = publish(world, setup)
    result = retrieve(bridge, principal, world.project)
    assert result["items"][0]["trust"] == "provider_reported"
    imported = bridge.sync.import_memory(principal, world.project, request(result, source), "import")
    assert imported["status"] == "proposed" and imported["origin"] == "provider_import"
    assert imported["trust"] == "provider_reported"
    assert bridge.sync.import_memory(principal, world.project, request(result, source), "other-key")["id"] == imported["id"]
    assert len([call for call in port.calls if call[0] == "put"]) == 1
    assert not world.app.state.store.list("memory_provider_result", world.project, {
        "role": "agent", "project": world.project, "agent": world.agent()["id"]})


def test_foreign_namespace_bound_record_requires_approval_and_evidence(world, setup):
    _, source = memory(world)
    bridge, principal, port, adapter = setup()
    metadata = {"hivemind_project": world.project, "hivemind_namespace": "ns",
                "hivemind_binding": bridge.sync.binding(adapter)}
    port.put(adapter.store_namespace, "foreign", {"content": "External untrusted claim", "metadata": metadata}, index=False)
    result = retrieve(bridge, principal, world.project)
    assert result["items"][0]["mirror_effect"] is None
    imported = bridge.sync.import_memory(principal, world.project, request(result, source), "foreign-import")
    assert imported["status"] == "proposed"
    assert imported["evidence_refs"][0]["revision"] == source["revision"]
    with pytest.raises(ValueError):
        ProviderImport(**{**request(result, source).model_dump(), "approve_import": False})


def test_scope_metadata_mismatch_never_publishes_result(world, setup):
    _, _, bridge, principal, port, _ = publish(world, setup)
    first = next(iter(port.records.values()))
    first["value"]["metadata"]["hivemind_namespace"] = "other"
    with pytest.raises(Problem, match="memory_provider_scope_mismatch"):
        retrieve(bridge, principal, world.project)
    assert not world.app.state.store.db.execute(
        "SELECT id FROM resources WHERE kind='memory_provider_result'").fetchall()


def test_import_rechecks_remote_content_and_canonical_sources(world, setup):
    _, source, bridge, principal, port, _ = publish(world, setup)
    result = retrieve(bridge, principal, world.project)
    next(iter(port.records.values()))["value"]["content"] = "Changed remotely"
    with pytest.raises(Problem, match="memory_provider_content_changed"):
        bridge.sync.import_memory(principal, world.project, request(result, source), "changed")
    world.call("DELETE", f"posts/{source['id']}", {"expected_revision": source["revision"]})
    with pytest.raises(Problem, match="memory_provider_source_unavailable"):
        bridge.sync.result(principal, result["id"])
    assert not world.app.state.store.db.execute(
        "SELECT id FROM resources WHERE json_extract(body,'$.origin')='provider_import'").fetchall()


@pytest.mark.parametrize("change", ["memory_delete", "source_delete", "source_revision", "team_acl", "expiry"])
def test_canonical_lifecycle_changes_propagate_owned_withdrawal(world, setup, change):
    item, source, bridge, principal, port, _ = publish(world, setup)
    if change == "memory_delete":
        world.call("DELETE", f"memories/{item['id']}", {"expected_revision": item["revision"]})
    elif change == "source_delete":
        world.call("DELETE", f"posts/{source['id']}", {"expected_revision": source["revision"]})
    elif change == "source_revision":
        world.call("PATCH", f"posts/{source['id']}", {
            "expected_revision": source["revision"], "content": {"media_type": "text/plain", "text": "Changed"}})
    elif change == "team_acl":
        world.call("PUT", f"resources/{source['id']}/access", {})
    else:
        with world.app.state.store.transaction():
            row = world.app.state.store.raw(item["id"])
            body = json.loads(row["body"])
            body["expires_at"] = "2020-01-01T00:00:00Z"
            world.app.state.store.save("memory", world.project, row["owner"], body)
    outcomes = drain(bridge, principal, world.project)
    assert not port.records
    assert len([call for call in port.calls if call[0] == "delete"]) == 1
    assert any("withdrawn" in result["outcomes"] for result in outcomes)
    assert bridge.sync.status(principal, world.project)["physical_erasure_verified"] is False


def test_uncertain_delete_never_repeats_but_absence_can_be_reconciled(world, setup):
    item, _, bridge, principal, port, _ = publish(world, setup)
    def uncertain(key):
        port.records.pop(key)
        raise TimeoutError()
    port.fail_delete = uncertain
    world.call("DELETE", f"memories/{item['id']}", {"expected_revision": item["revision"]})
    first = bridge.sync.reconcile(principal, world.project, "langmem")
    assert first["outcomes"] == ["unknown_hold"]
    drain(bridge, principal, world.project)
    assert len([call for call in port.calls if call[0] == "delete"]) == 1
    assert bridge.sync.status(principal, world.project)["mirrors"][0]["withdrawal_state"] == "done"


def test_uncertain_still_present_delete_stays_held_across_new_bridge(world, setup):
    item, _, bridge, principal, port, adapter = publish(world, setup)
    port.fail_delete = lambda key: (_ for _ in ()).throw(TimeoutError())
    world.call("DELETE", f"memories/{item['id']}", {"expected_revision": item["revision"]})
    drain(bridge, principal, world.project)
    another = MemoryAdapterBridge(world.app.state.service, {"langmem": adapter}, bridge.policy)
    try:
        drain(another, principal, world.project)
        assert len([call for call in port.calls if call[0] == "delete"]) == 1
        assert another.sync.status(principal, world.project)["mirrors"][0]["withdrawal_state"] == "unknown"
    finally:
        another.close()


def test_rebinding_or_remote_tamper_holds_without_deleting(world, setup):
    item, _, bridge, principal, port, adapter = publish(world, setup)
    world.call("DELETE", f"memories/{item['id']}", {"expected_revision": item["revision"]})
    adapter.binding_id = "different-account"
    assert "binding_or_receipt_hold" in bridge.sync.reconcile(principal, world.project, "langmem")["outcomes"]
    adapter.binding_id = "account-one/store-one"
    next(iter(port.records.values()))["value"]["content"] = "Someone else's edit"
    drain(bridge, principal, world.project)
    assert not [call for call in port.calls if call[0] == "delete"]
    assert port.records


def test_expired_result_and_revoked_operator_are_fenced(world, setup):
    _, _, bridge, principal, _, _ = publish(world, setup)
    result = retrieve(bridge, principal, world.project)
    with world.app.state.store.transaction():
        row = world.app.state.store.raw(result["id"])
        body = json.loads(row["body"])
        body["expires_at"] = 0
        world.app.state.store.save("memory_provider_result", world.project, row["owner"], body)
    with pytest.raises(Problem, match="memory_provider_result_expired"):
        bridge.sync.result(principal, result["id"])
    with world.app.state.store.transaction():
        world.app.state.store.db.execute("DELETE FROM principals WHERE id=?", (principal["id"],))
    with pytest.raises(Problem, match="authentication_required"):
        retrieve(bridge, principal, world.project, key="revoked")


def test_automatic_commit_and_expiry_polling_withdraws_without_manual_route(world, setup):
    item, _, bridge, _, port, _ = publish(world, setup, auto_withdraw=True, poll_interval_seconds=0.1)
    world.call("DELETE", f"memories/{item['id']}", {"expected_revision": item["revision"]})
    deadline = time.monotonic() + 3
    while port.records and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not port.records
    assert bridge.close()


def test_default_off_flags_strict_binding_and_bounded_results(world, setup):
    _, _, bridge, principal, port, _ = publish(world, setup)
    bridge.policy = replace(bridge.policy, allow_imports=False, allow_remote_reads=False)
    with pytest.raises(Problem, match="memory_provider_reads_disabled"):
        retrieve(bridge, principal, world.project)
    for value in (1, "true"):
        with pytest.raises(ValueError):
            MemoryAdapterPolicy(allow_remote_deletes=value)
    with pytest.raises(ValueError):
        MemoryAdapterBridge(world.app.state.service, {
            "langmem": LangMemAdapter(port, world.project, "ns")},
            MemoryAdapterPolicy(enabled=True, allow_remote_reads=True))


def test_http_routes_import_and_receipt_recheck_after_source_revocation(world):
    item, source = memory(world)
    port = DataPort()
    adapter = LangMemAdapter(port, world.project, "ns", binding_id="tenant/store")
    policy = MemoryAdapterPolicy(enabled=True, allow_remote_writes=True, allow_remote_reads=True,
                                allow_imports=True, allow_remote_deletes=True)
    app = create_app(world.app.state.store.directory, workers=False, capability_options={
        "memory_adapters": {"adapters": {"langmem": adapter}, "policy": policy}})
    headers = {"Authorization": "Bearer " + world.owner, "Idempotency-Key": "retrieve"}
    with TestClient(app) as client:
        response = client.post(f"/v1/memories/{item['id']}:mirror",
            json={"adapter": "langmem", "expected_revision": item["revision"]}, headers=headers)
        assert response.status_code == 200
        response = client.post(f"/v1/projects/{world.project}/memory-providers:retrieve",
                               json={"adapter": "langmem"}, headers=headers)
        assert response.status_code == 200, response.text
        result = response.json()["data"]
        response = client.post(f"/v1/projects/{world.project}/memory-providers:import",
                               json=request(result, source).model_dump(), headers=headers)
        assert response.status_code == 200 and response.json()["data"]["status"] == "proposed"
        world.call("DELETE", f"posts/{source['id']}", {"expected_revision": source["revision"]})
        response = client.get("/v1/commands/receipt", params={
            "operation": "memory-provider-retrieve:" + world.project, "key": "retrieve"}, headers=headers)
        assert response.status_code == 404


@pytest.mark.parametrize("name", ["mem0", "letta"])
def test_native_provider_read_delete_ports_are_data_only_and_namespace_scoped(world, name):
    records, calls = {}, []
    def create(**kw):
        identity = "remote"
        records[identity] = {"id": identity, "value": kw["value"], "metadata": kw["metadata"]}
        calls.append(("create", kw))
        return {"id": identity}
    def delete(identity):
        calls.append(("delete", identity))
        records.pop(identity, None)
    if name == "letta":
        client = SimpleNamespace(blocks=SimpleNamespace(create=create, retrieve=lambda key: records.get(key),
            delete=delete, list=lambda **kw: (calls.append(("list", kw)) or list(records.values()))))
        adapter = LettaAdapter(client, world.project, "ns", binding_id="tenant")
    else:
        def add(messages, **kw):
            calls.append(("add", kw))
            records["remote"] = {"id": "remote", "memory": messages[0]["content"], "metadata": kw["metadata"]}
            return {"results": [{"id": "remote"}]}
        client = SimpleNamespace(add=add, get=lambda key: records.get(key), delete=delete,
            get_all=lambda **kw: (calls.append(("list", kw)) or {"results": list(records.values())}))
        adapter = Mem0Adapter(client, world.project, "ns", binding_id="tenant")
    bridge = MemoryAdapterBridge(world.app.state.service, {name: adapter},
        MemoryAdapterPolicy(enabled=True, allow_remote_writes=True, allow_remote_reads=True, allow_remote_deletes=True))
    try:
        item, _ = memory(world)
        principal = dict(world.app.state.store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
        bridge.publish(principal, item["id"], MemoryMirrorRequest(adapter=name, expected_revision=item["revision"]), "key")
        result = bridge.sync.retrieve(principal, world.project, ProviderRetrieve(adapter=name), "get")
        assert result["items"][0]["record_id"] == "remote"
        options = next(value for method, value in calls if method == "list")
        assert options.get("user_id", options.get("label")) == ("ns" if name == "mem0" else adapter.label)
        if name == "mem0":
            assert next(value for method, value in calls if method == "add")["infer"] is False
        world.call("DELETE", f"memories/{item['id']}", {"expected_revision": item["revision"]})
        bridge.sync.reconcile(principal, world.project, name)
        assert not records
    finally:
        bridge.close()


def test_concurrent_reconcilers_admit_only_one_delete(world, setup):
    item, _, bridge, principal, port, adapter = publish(world, setup)
    another = MemoryAdapterBridge(world.app.state.service, {"langmem": adapter}, bridge.policy)
    barrier = threading.Barrier(2)
    port.fail_get = lambda: barrier.wait(timeout=3)
    world.call("DELETE", f"memories/{item['id']}", {"expected_revision": item["revision"]})
    errors = []
    def run(sync):
        try:
            sync.reconcile(principal, world.project, "langmem")
        except Exception as error:
            errors.append(type(error).__name__)
    threads = [threading.Thread(target=run, args=(value.sync,)) for value in (bridge, another)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        assert not errors and not any(thread.is_alive() for thread in threads)
        assert len([call for call in port.calls if call[0] == "delete"]) == 1
    finally:
        port.fail_get = None
        another.close()


def test_shutdown_keeps_active_provider_read_accounted_until_drained(world, setup):
    _, _, bridge, principal, port, _ = publish(world, setup, join_timeout_seconds=0.01)
    entered, release = threading.Event(), threading.Event()
    original = port.search
    def blocked(namespace, *, limit):
        entered.set()
        release.wait(3)
        return original(namespace, limit=limit)
    port.search = blocked
    errors = []
    def run():
        try:
            retrieve(bridge, principal, world.project)
        except Problem as error:
            errors.append(error.code)
    thread = threading.Thread(target=run)
    thread.start()
    assert entered.wait(1)
    assert bridge.close() is False
    release.set()
    thread.join(3)
    assert not thread.is_alive()
    assert bridge.close() is True
    with pytest.raises(Problem, match="memory_adapter_closing"):
        retrieve(bridge, principal, world.project, key="after-close")


def test_missing_request_key_is_denied_before_provider_read(world, setup):
    _, _, bridge, principal, port, _ = publish(world, setup)
    before = list(port.calls)
    with pytest.raises(Problem, match="idempotency_key_required"):
        retrieve(bridge, principal, world.project, key=None)
    assert port.calls == before


def test_unacknowledged_put_preserves_known_receipt_for_withdrawal(world, setup):
    item, source = memory(world)
    bridge, principal, port, adapter = setup()
    original = port.put
    def invalidate(namespace, key, value, *, index):
        original(namespace, key, value, index=index)
        world.call("DELETE", f"posts/{source['id']}", {"expected_revision": source["revision"]})
    port.put = invalidate
    with pytest.raises(Problem, match="memory_mirror_outcome_unknown"):
        bridge.publish(principal, item["id"], MemoryMirrorRequest(
            adapter="langmem", expected_revision=item["revision"]), "unknown-source")
    row = world.app.state.store.db.execute("SELECT * FROM memory_mirrors").fetchone()
    assert row["state"] == "unknown" and row["receipt"]
    drain(bridge, principal, world.project)
    assert not port.records
    assert adapter.binding_id == "account-one/store-one"


def test_export_deduplication_never_crosses_account_binding(world, setup):
    item, _, bridge, principal, port, adapter = publish(world, setup)
    adapter.binding_id = "account-two/store-one"
    bridge.publish(principal, item["id"], MemoryMirrorRequest(
        adapter="langmem", expected_revision=item["revision"]), "new-account")
    assert len([call for call in port.calls if call[0] == "put"]) == 2
    rows = world.app.state.store.db.execute("SELECT binding FROM memory_mirrors").fetchall()
    assert len({row["binding"] for row in rows}) == 2


def test_python_provider_facades_encode_paths_and_match_operations():
    import httpx
    from hivemind.sdk import Client
    calls = []
    def handle(request):
        calls.append((request.method, request.url.raw_path.decode()))
        return httpx.Response(200, json={"data": {}})
    client = Client("http://test", "test", transport=httpx.MockTransport(handle))
    try:
        client.retrieve_provider_memories("project/one", {"adapter": "mem0"}, key="retrieve")
        client.provider_memory_result("result/one")
        client.import_provider_memory("project/one", {}, key="import")
        client.reconcile_provider_memories("project/one", "mem0")
        client.provider_memory_status("project/one")
    finally:
        client.close()
    assert [call[0] for call in calls] == ["POST", "GET", "POST", "POST", "GET"]
    assert all("%2F" in call[1] for call in calls)
