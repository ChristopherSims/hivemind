import time

from hivemind.storage import uid


def setup(world, policy=None, ttl=3600):
    sender, recipient = world.agent("Sender"), world.agent("Recipient")
    token = world.credential(recipient)
    run = world.run(recipient, token)
    payload = {"message_id": uid("msg"), "project_id": world.project, "from": sender["id"], "to": recipient["id"],
               "type": "inform", "thread_id": uid("thread"), "ttl_seconds": ttl,
               "content": {"media_type": "text/plain", "text": "Deliver this exactly as evidence"}}
    if policy:
        payload["retry_policy"] = policy
    message = world.call("POST", "messages", payload)
    return sender, recipient, token, run, message, payload


def claim(world, message, run, token, **kwargs):
    return world.call("POST", f"messages/{message['id']}:claim", {"run_id": run["id"]}, token, **kwargs)


def test_failed_delivery_retries_with_backoff_fencing_and_no_duplicate_envelope(world, monkeypatch):
    _, recipient, token, run, message, payload = setup(world)
    clock = [time.time()]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    first = claim(world, message, run, token, key="first-claim")
    again = claim(world, message, run, token)
    assert again["delivery_id"] == first["delivery_id"] and again["attempt_count"] == 1
    world.call("POST", f"messages/{message['id']}:fail", {
        "run_id": run["id"], "delivery_id": first["delivery_id"], "reason": "handler_failed"}, token)
    assert not world.call("GET", f"agents/{recipient['id']}/inbox?project_id={world.project}", token=token)
    claim(world, message, run, token, key="first-claim", status=409)
    clock[0] += 3
    second = claim(world, message, run, token)
    assert second["attempt_count"] == 2 and second["delivery_id"] != first["delivery_id"]
    for receipt in ({"run_id": run["id"], "delivery_id": first["delivery_id"]}, {"run_id": run["id"]}):
        world.call("POST", f"messages/{message['id']}:ack", receipt, token, status=409)
    ack = {"run_id": run["id"], "delivery_id": second["delivery_id"]}
    world.call("POST", f"messages/{message['id']}:ack", ack, token, key="ack-second")
    world.call("POST", f"messages/{message['id']}:ack", ack, token, key="ack-second")
    world.call("POST", "messages", payload)
    summary = world.call("GET", f"projects/{world.project}/delivery-summary")
    assert summary["count"] == 1 and summary["states"]["acked"] == 1


def test_abandoned_lease_recovery_budget_and_expiry_are_read_time_checks(world, monkeypatch):
    _, recipient, token, run, message, _ = setup(world, {"max_attempts": 2})
    clock = [time.time()]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    first = claim(world, message, run, token)
    competing = world.run(recipient, token)
    claim(world, message, competing, token, status=409)
    clock[0] += 31
    claim(world, message, competing, token, status=409)
    clock[0] += 2
    recovered = claim(world, message, competing, token)
    assert recovered["attempt_count"] == 2
    assert "lease_expired" in [entry["action"] for entry in recovered["delivery_history"]]
    world.call("POST", f"messages/{message['id']}:ack",
               {"run_id": run["id"], "delivery_id": first["delivery_id"]}, token, status=404)
    clock[0] += 31
    detail = world.call("GET", f"messages/{message['id']}", token=token)
    assert detail["delivery_status"] == "dead_letter"
    assert not world.call("GET", f"agents/{recipient['id']}/inbox?project_id={world.project}", token=token)
    claim(world, message, competing, token, status=409)
    world.call("POST", f"messages/{message['id']}:requeue", {"expected_revision": detail["revision"]})
    assert claim(world, message, competing, token)["total_attempts"] == 3
    clock[0] += 3601
    expired = world.call("GET", f"messages/{message['id']}")
    assert expired["delivery_status"] == "expired"
    world.call("POST", f"messages/{message['id']}:requeue", {"expected_revision": expired["revision"]}, status=409)


def test_delivery_and_reader_state_survive_a_fresh_canonical_connection(world):
    from hivemind.service import Service
    from hivemind.storage import Store
    _, _, token, run, message, _ = setup(world)
    receipt = claim(world, message, run, token)
    world.call("POST", f"projects/{world.project}/conversations:read", {"message_ids": [message["id"]]})
    restored = Store(world.app.state.store.directory)
    try:
        service = Service(restored)
        owner = dict(restored.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
        assert service.delivery_summary(owner, world.project)["unread"] == 0
        assert service.delivery_state(restored.get(message["id"], owner))["delivery_id"] == receipt["delivery_id"]
        assert service.delivery_summary(owner, world.project)["states"]["leased"] == 1
    finally:
        restored.close()


def test_python_delivery_poller_retries_handler_failure_without_logging_exception(world, monkeypatch):
    import httpx
    from hivemind.sdk import Client
    _, recipient, token, run, message, _ = setup(world)
    def forward(request):
        response = world.client.request(request.method, request.url.raw_path.decode(),
                                        content=request.content, headers=dict(request.headers))
        return httpx.Response(response.status_code, headers=dict(response.headers), content=response.content)
    client = Client("http://testserver", token)
    client.http.close()
    client.http = httpx.Client(base_url="http://testserver/v0.1/",
                              headers={"Authorization": f"Bearer {token}"}, transport=httpx.MockTransport(forward))
    def failing(_):
        raise ValueError("private handler diagnostic must not be captured")
    try:
        result = client.deliver_inbox(recipient["id"], world.project, run["id"], failing)
        assert result[0]["delivery_status"] == "retry_wait"
        clock = time.time() + 3
        monkeypatch.setattr(time, "time", lambda: clock)
        processed = []
        result = client.deliver_inbox(recipient["id"], world.project, run["id"], lambda item: processed.append(item["id"]))
        assert processed == [message["id"]] and result[0]["delivery_status"] == "acked"
        assert "private handler diagnostic" not in str(result)
    finally:
        client.close()


def test_read_accounting_is_explicit_per_principal_and_never_acknowledges(world):
    _, recipient, token, _, message, _ = setup(world)
    observer = world.credential(role="observer")
    for _ in range(2):
        snapshot = world.call("GET", f"projects/{world.project}/conversations", token=observer)
        assert not snapshot["items"][0]["is_read"]
    assert world.call("GET", f"projects/{world.project}/delivery-summary", token=observer)["unread"] == 1
    world.call("POST", f"projects/{world.project}/conversations:read",
               {"message_ids": [message["id"], message["id"]]}, observer, key="seen")
    world.call("POST", f"projects/{world.project}/conversations:read", {"message_ids": [message["id"], message["id"]]},
               observer, key="seen")
    assert world.call("GET", f"projects/{world.project}/delivery-summary", token=observer)["unread"] == 0
    assert world.call("GET", f"projects/{world.project}/delivery-summary", token=token)["unread"] == 1
    inbox = world.call("GET", f"agents/{recipient['id']}/inbox?project_id={world.project}", token=token)
    assert inbox[0]["state"] == "available"
    human = world.credential(role="human")
    world.call("GET", f"projects/{world.project}/delivery-summary", token=human, status=403)
    world.call("POST", f"projects/{world.project}/conversations:read", {"message_ids": [message["id"]]}, human, status=403)


def test_read_batch_is_atomic_and_does_not_expose_other_agent_messages(world):
    sender, _, token, _, message, _ = setup(world)
    other = world.agent("Other recipient")
    hidden = world.call("POST", "messages", {
        "message_id": uid("msg"), "project_id": world.project, "from": sender["id"], "to": other["id"],
        "type": "inform", "thread_id": uid("thread"), "content": {"media_type": "text/plain", "text": "Hidden"}})
    world.call("POST", f"projects/{world.project}/conversations:read",
               {"message_ids": [message["id"], hidden["id"]]}, token, status=404)
    assert world.call("GET", f"projects/{world.project}/delivery-summary", token=token)["unread"] == 1
    world.call("GET", f"messages/{hidden['id']}", token=token, status=404)


def test_short_ttl_fences_ack_and_expired_messages_cannot_be_requeued(world, monkeypatch):
    _, _, token, run, message, _ = setup(world, ttl=1)
    clock = time.time()
    receipt = claim(world, message, run, token)
    assert receipt["claim_until"] <= message["expires_at"]
    monkeypatch.setattr(time, "time", lambda: clock + 2)
    world.call("POST", f"messages/{message['id']}:ack",
               {"run_id": run["id"], "delivery_id": receipt["delivery_id"]}, token, status=409)
    claim(world, message, run, token, status=409)


def test_terminal_run_cannot_claim_and_observer_cannot_requeue(world):
    _, _, token, run, message, _ = setup(world)
    store = world.app.state.store
    with store.transaction():
        principal = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
        body = store.get(run["id"], principal)
        body["status"] = "completed"
        store.save("run", world.project, principal["id"], body)
    claim(world, message, run, token, status=409)
    observer = world.credential(role="observer")
    world.call("POST", f"messages/{message['id']}:requeue", {"expected_revision": 1}, observer, status=403)


def test_concurrent_claims_create_one_receipt(world):
    from concurrent.futures import ThreadPoolExecutor
    _, recipient, token, first, message, _ = setup(world)
    second = world.run(recipient, token)
    def take(run):
        return world.client.post(f"/v0.1/messages/{message['id']}:claim",
            json={"run_id": run["id"]}, headers={"Authorization": f"Bearer {token}", "Idempotency-Key": uid("key")})
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(take, [first, second]))
    assert sorted(response.status_code for response in responses) == [200, 409]
    detail = world.call("GET", f"messages/{message['id']}", token=token)
    assert detail["attempt_count"] == 1
    assert len(detail["delivery_history"]) == 1
