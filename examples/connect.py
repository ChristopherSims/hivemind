"""Register an agent/scope, without printing or granting agent credentials."""
import argparse
import json
from pathlib import Path

from hivemind.sdk import Client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--owner", required=True, help="Private local owner credential JSON")
    parser.add_argument("--name", required=True)
    parser.add_argument("--runtime", choices=["droid", "oh-my-pi"], required=True)
    parser.add_argument("--scope-key", required=True)
    parser.add_argument("--allowed-path", action="append", required=True)
    args = parser.parse_args()
    owner = json.loads(Path(args.owner).read_text())
    client = Client(owner["base_url"], owner["token"])
    try:
        agent = client.request("POST", "agents", {
            "project_id": owner["project_id"], "display_name": args.name, "runtime": args.runtime,
            "supported_actions": ["pause", "resume", "cancel", "steer", "assign_task"],
            "scope_permit_enforcement": True,
        })
        scope = client.request("POST", "scopes", {
            "project_id": owner["project_id"], "canonical_key": args.scope_key,
            "display_name": args.scope_key, "allowed_paths": args.allowed_path,
        })
        print(json.dumps({"agent_id": agent["id"], "project_id": owner["project_id"], "scope_id": scope["id"]},
                         indent=2))
        print("Issue a separate agent token with hivemind issue-token; then configure the reviewed runtime adapter.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
