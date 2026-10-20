import asyncio
import threading

from hivemind.storage import uid


def test_explicit_two_phase_handler_keeps_heartbeat_endpoint_responsive(world):
    entered, release = threading.Event(), threading.Event()
    store = world.app.state.store

    @world.app.post("/v1/test-detached-model")
    async def detached():
        def compute():
            assert not store.db.in_transaction
            assert not store.lock._is_owned()
            entered.set()
            assert release.wait(5)
        await asyncio.to_thread(compute)
        with store.transaction():
            store.tick(world.project)
        return {"complete": True}

    detached.hivemind_detached_work = True
    responses = []

    def invoke():
        responses.append(world.client.post("/v1/test-detached-model", json={},
            headers={"Authorization": "Bearer " + world.owner, "Idempotency-Key": uid("key")}))

    thread = threading.Thread(target=invoke)
    thread.start()
    try:
        assert entered.wait(3)
        response = world.client.get("/v1/ledger?project_id=" + world.project,
                                   headers={"Authorization": "Bearer " + world.owner})
        assert response.status_code == 200 and thread.is_alive()
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive() and responses[0].status_code == 200
