import json
import secrets

import pytest
from fastapi.testclient import TestClient

from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.storage import uid


class World:
    def __init__(self, app, client, config):
        self.app, self.client = app, client
        self.project = config["project_id"]
        self.owner = config["token"]

    def call(self, method, path, body=None, token=None, key=None, status=200):
        response = self.client.request(method, "/v0.1/" + path, json=body, headers={
            "Authorization": "Bearer " + (token or self.owner), "Idempotency-Key": key or uid("key")})
        assert response.status_code == status, response.text
        return response.json().get("data", response.json())

    def agent(self, name="Worker", actions=None):
        return self.call("POST", "agents", {
            "project_id": self.project, "display_name": name,
            "supported_actions": actions or [], "scope_permit_enforcement": bool(actions)})

    def credential(self, agent=None, role="agent", coordinator=False, project=None, capture=True):
        token = secrets.token_urlsafe(32)
        store = self.app.state.store
        with store.transaction():
            store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                             (uid("principal"), hash_token(token), role, project or self.project,
                              agent["id"] if agent else None, int(coordinator), int(capture)))
        return token

    def run(self, agent, token=None):
        return self.call("POST", "runs", {"project_id": self.project, "agent_id": agent["id"]}, token)

    def scope(self, key="website/header", parent=None, paths=None):
        return self.call("POST", "scopes", {"project_id": self.project, "canonical_key": key,
                                           "display_name": key, "parent_scope_id": parent,
                                           "allowed_paths": paths or []})

    def claim(self, agent, run, scope, token=None, status=200):
        return self.call("POST", "work-claims", {
            "project_id": self.project, "agent_id": agent["id"], "run_id": run["id"],
            "scope_ids": [scope["id"]], "intent": "Implement website navigation"}, token, status=status)

    def board(self):
        return self.call("GET", f"boards?project_id={self.project}")[0]

    def post(self, text="The header uses navigation links", token=None, segments=None):
        return self.call("POST", f"boards/{self.board()['id']}/posts", {
            "client_post_id": uid("client"), "kind": "finding",
            "content": {"media_type": "application/vnd.hivemind.chat+json", "segments": segments}
            if segments else {"media_type": "text/plain", "text": text}}, token)


@pytest.fixture
def world(tmp_path):
    owner = initialize(tmp_path)
    config = json.loads(owner.read_text())
    app = create_app(tmp_path, workers=False)
    with TestClient(app) as client:
        yield World(app, client, config)
