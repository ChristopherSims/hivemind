from hivemind.sdk import Client


def test_search_page_preserves_request_and_opaque_cursor(monkeypatch):
    client = Client("http://127.0.0.1:8766", "fixture-token")
    calls = []
    monkeypatch.setattr(client, "request",
                        lambda method, path, body: calls.append((method, path, body)) or {"results": []})
    request = {"project_id": "fixture-project", "query": "review"}
    try:
        assert client.search_page(request, "opaque-cursor") == {"results": []}
        assert calls == [("POST", "retrieval:search-page", {**request, "cursor": "opaque-cursor"})]
        assert request == {"project_id": "fixture-project", "query": "review"}
    finally:
        client.close()
