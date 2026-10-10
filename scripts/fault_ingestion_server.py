"""Test-only process server. Pauses after a transaction's first SQL write."""
import argparse
import json
import time
from pathlib import Path

import uvicorn

from hivemind.api import create_app


def install_transaction_barrier(store, arm, reached):
    original = store.tick
    def tick(project):
        sequence = original(project)
        # Only an explicitly armed isolated test server can pause here.
        if arm.exists() and store.db.in_transaction:
            reached.write_text(json.dumps({"project_id": project, "uncommitted_sequence": sequence}),
                               encoding="utf-8")
            while arm.exists():
                time.sleep(.05)
        return sequence
    store.tick = tick


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--arm", required=True, type=Path)
    parser.add_argument("--reached", required=True, type=Path)
    args = parser.parse_args()
    app = create_app(args.data_dir, workers=False)
    install_transaction_barrier(app.state.store, args.arm, args.reached)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
