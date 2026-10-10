"""Explicit local five-second drain/heartbeat process for short-lived runtime hooks."""
import argparse
import json
import time
from pathlib import Path

import httpx
from filelock import FileLock

from .sdk import Capture, Client


def drain(config):
    state_dir = Path(config["state_dir"])
    client = Client(config["base_url"], config["token"])
    try:
        for mapping in state_dir.glob("*.json"):
            if mapping.name.endswith("-controls.json"):
                continue
            state = json.loads(mapping.read_text())
            if "run_id" not in state:
                continue
            key = mapping.stem
            with FileLock(str(state_dir / f"{key}.lock"), timeout=2):
                capture = Capture(client, config["project_id"], config["agent_id"], state["run_id"],
                                  state_dir / f"{key}-spool.sqlite", start=False)
                try:
                    capture.flush()
                    count = capture.db.execute("SELECT count(*) FROM spool WHERE state='quarantined'").fetchone()[0]
                    client.request("POST", f"runs/{state['run_id']}/heartbeat",
                                   {"capture_health": {"state": "gaps" if count else "healthy",
                                                       "quarantined_event_count": count}})
                    claims = client.request("GET", f"work-claims?project_id={config['project_id']}")
                    for claim in claims:
                        if claim["run_id"] == state["run_id"] and claim["state"] == "active" and (
                                claim["expires_at"] - time.time() < 60):
                            client.request("POST", f"work-claims/{claim['id']}:renew",
                                           {"expected_revision": claim["revision"]})
                finally:
                    capture.db.close()
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    while True:
        try:
            drain(config)
        except (httpx.HTTPError, OSError, ValueError):
            pass
        time.sleep(config.get("flush_interval_seconds", 5))


if __name__ == "__main__":
    main()
