"""Synthetic, loopback-only browser test service. No user agent sessions or credentials."""
import json
import secrets
import sys
from pathlib import Path

import uvicorn

from hivemind.api import create_app, hash_token
from hivemind.cli import initialize
from hivemind.models import AgentCreate, MessageCreate, RunCreate
from hivemind.storage import uid


def main():
    root = Path(__file__).resolve().parent.parent
    directory = root / ".hivemind-test" / uid("browser")
    owner = json.loads(initialize(directory, "Hivemind test workspace").read_text())
    app = create_app(directory, workers=True)
    store, service = app.state.store, app.state.service
    principal = dict(store.db.execute("SELECT * FROM principals LIMIT 1").fetchone())
    with store.transaction():
        agents = [service.agent(principal, AgentCreate(
            project_id=owner["project_id"], display_name=name, description=description))
            for name, description in (("Droid worker", "Building the navigation"),
                                       ("Oh my Pi worker", "Reviewing accessibility"),
                                       ("Coordinator", "Nexus candidate"))]
        for agent in agents:
            agent["base_color_token"] = "teal"
            store.save("agent", owner["project_id"], principal["id"], agent)
            service.start_run(principal, RunCreate(project_id=owner["project_id"], agent_id=agent["id"]))
        service.message(principal, MessageCreate.model_validate({
            "message_id": uid("msg"), "project_id": owner["project_id"],
            "from": agents[1]["id"], "to": agents[0]["id"], "thread_id": uid("thread"), "type": "question",
            "content": {"media_type": "text/plain", "text": "Can you confirm the navigation breakpoint?"},
        }))
        human_token = secrets.token_urlsafe(32)
        store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                         (uid("principal"), hash_token(human_token), "human", owner["project_id"], None, 0, 0))
        observer_token = secrets.token_urlsafe(32)
        store.db.execute("INSERT INTO principals VALUES(?,?,?,?,?,?,?)",
                         (uid("principal"), hash_token(observer_token), "observer", owner["project_id"], None, 0, 0))
    fixture = {**owner, "agents": agents, "human_token": human_token,
               "observer_token": observer_token,
               "board_id": store.list("board", owner["project_id"], principal)[0]["id"]}
    (root / ".hivemind-test" / "browser.json").write_text(json.dumps(fixture))
    print("Synthetic browser fixture ready", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=8766, log_level="warning")
    return 0


if __name__ == "__main__":
    sys.exit(main())
