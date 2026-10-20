"""Bounded JSON-lines capture. Host records cannot choose identity or attribution.

Buffer observations are reported editor state, never filesystem or effect proof.
The CLI only reads an explicitly selected private operator descriptor, never
repository discovery. It does not launch a vendor, end a run or apply controls.
"""
import json
import sys
import time
from pathlib import Path

from . import models as m
from .sdk import Capture, Client, excluded
from .storage import now

MAX_LINE_BYTES = 16384
TYPES = {
    "editor.buffer_observed": m.EditorBufferData,
    "tool.started": m.ToolStart, "tool.completed": m.ToolEnd,
    "agent.status_reported": m.StatusData, "capture.gap_detected": m.GapData,
    "editor.acp_observed": m.AcpObservationData,
}


class JsonLinesCapture:
    def __init__(self, capture):
        self.capture = capture

    def ingest(self, line):
        if not isinstance(line, bytes) or len(line) > MAX_LINE_BYTES:
            raise ValueError("Capture record exceeds the byte bound")
        record = json.loads(line.decode("utf-8"))
        if not isinstance(record, dict) or set(record) != {"type", "data"}:
            raise ValueError("Capture records require only type and data")
        model = TYPES.get(record["type"])
        if model is None:
            raise ValueError("Unsupported capture record")
        data = model.model_validate(record["data"]).model_dump()
        if record["type"] in ("editor.buffer_observed", "editor.acp_observed") and (
                data.get("path") and excluded(data["path"])):
            return None
        return self.capture.observe(record["type"], data, attribution="reported", kind="sdk")

    def consume(self, stream):
        while True:
            line = stream.readline(MAX_LINE_BYTES + 1)
            if not line:
                break
            try:
                self.ingest(line)
            except (ValueError, TypeError, UnicodeError):
                self.capture.observe("capture.gap_detected", {
                    "reason": "Invalid or oversized host record", "dropped_observation_count": 1})
                # Oversized lines are discarded through their newline with bounded reads.
                while line and not line.endswith(b"\n"):
                    line = stream.readline(MAX_LINE_BYTES + 1)


class EditorRpcCapture:
    """Fresh permit fences and acknowledged capture; no identity from host frames."""

    def __init__(self, capture, config):
        self.capture, self.config = capture, config

    def authorize(self, paths=None):
        config = self.config
        run = self.capture.client.request("GET", f"runs/{config['run_id']}")
        if ((run["project_id"], run["agent_id"]) != (config["project_id"], config["agent_id"])
                or run["status"] not in ("running", "idle")):
            raise PermissionError("Editor run is not live and authorized")
        request = m.PermitRequest(scope_ids=config.get("scope_ids", []), paths=paths or [])
        permit = self.capture.client.request(
            "POST", f"runs/{config['run_id']}/permits:check", request.model_dump())
        if (not isinstance(permit, dict) or permit.get("run_id") != config["run_id"]
                or set(permit.get("scope_ids", [])) != set(request.scope_ids)
                or not isinstance(permit.get("expires_at"), (int, float))
                or not time.time() < permit["expires_at"] <= time.time() + 15):
            raise PermissionError("Editor permit is not current and bound")

    def handle(self, frame):
        if not isinstance(frame, dict) or set(frame) != {"request_id", "operation", "payload"}:
            raise ValueError("Invalid editor request")
        if not isinstance(frame["request_id"], str) or len(frame["request_id"]) > 128:
            raise ValueError("Invalid editor request identity")
        operation, payload = frame["operation"], frame["payload"]
        if operation == "authorize" and payload == {}:
            self.authorize()
            event = self.capture.observe("agent.status_reported", {
                "text": "VS Code ACP authorization checked"}, attribution="reported", kind="sdk")
        elif operation == "permit" and isinstance(payload, dict) and set(payload) == {"path"}:
            self.authorize([m.safe_path(payload["path"])])
            return {"request_id": frame["request_id"], "ok": True}
        elif operation == "observe":
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
                raise ValueError("Invalid observation")
            path = payload["data"].get("path")
            self.authorize([m.safe_path(path)] if path is not None else [])
            event = JsonLinesCapture(self.capture).ingest(json.dumps(payload).encode())
            if event is None:
                raise PermissionError("Excluded observation")
        else:
            raise ValueError("Unsupported editor operation")
        self.capture.flush()
        # Only acknowledge server-accepted delivery; pending/quarantined rows are not proof.
        with self.capture.lock:
            row = self.capture.db.execute("SELECT state FROM spool WHERE id=?", (event,)).fetchone()
        if row is not None:
            raise PermissionError("Observation was not accepted")
        return {"request_id": frame["request_id"], "ok": True}

    def consume(self, stream, output):
        while True:
            line = stream.readline(MAX_LINE_BYTES + 1)
            if not line:
                return
            request_id = None
            try:
                if len(line) > MAX_LINE_BYTES or not line.endswith(b"\n"):
                    raise ValueError("Invalid editor frame")
                frame = json.loads(line)
                if isinstance(frame, dict) and isinstance(frame.get("request_id"), str):
                    request_id = frame["request_id"][:128]
                reply = self.handle(frame)
            except Exception:
                reply = {"request_id": request_id, "ok": False}
                while line and not line.endswith(b"\n"):
                    line = stream.readline(MAX_LINE_BYTES + 1)
            output.write(json.dumps(reply) + "\n")
            output.flush()


def main():
    rpc = len(sys.argv) == 3 and sys.argv[2] == "--editor-rpc"
    if len(sys.argv) != 2 and not rpc:
        raise ValueError("An explicit private capture descriptor is required")
    path = Path(sys.argv[1])
    if (not path.is_absolute() or any(value.is_symlink() for value in (path, *path.parents))
            or path.stat().st_size > 16384):
        raise ValueError("Invalid capture descriptor")
    config = json.loads(path.read_text(encoding="utf-8"))
    required = {"base_url", "token_file", "project_id", "agent_id", "run_id", "spool", "workspace_root"}
    if not isinstance(config, dict) or set(config) - (required | {"python_executable", "scope_ids"}) or required - set(config):
        raise ValueError("Invalid capture descriptor keys")
    token_path = Path(config["token_file"])
    spool_path = Path(config["spool"])
    workspace = Path(config["workspace_root"]).resolve(strict=True)
    if (not token_path.is_absolute() or not spool_path.is_absolute()
            or any(value.is_symlink() for leaf in (token_path, spool_path)
                   for value in (leaf, *leaf.parents))
            or token_path.stat().st_size > 4096):
        raise ValueError("Private token and spool require absolute local paths")
    if any(value.resolve().is_relative_to(workspace) for value in (path, token_path, spool_path)):
        raise ValueError("Private capture descriptor, credential and spool must be outside the workspace")
    if len({value.resolve() for value in (path, token_path, spool_path)}) != 3:
        raise ValueError("Capture descriptor, token and spool must be distinct")
    token = token_path.read_text(encoding="utf-8").strip()
    client = Client(config["base_url"], token)
    # GET re-authorizes the actual run before opening the bound spool.
    capture = None
    try:
        run = client.request("GET", f"runs/{config['run_id']}")
        if (run["project_id"], run["agent_id"]) != (config["project_id"], config["agent_id"]):
            raise ValueError("Capture descriptor identity mismatch")
        capture = Capture(client, config["project_id"], config["agent_id"], config["run_id"], spool_path)
        if rpc:
            EditorRpcCapture(capture, config).consume(sys.stdin.buffer, sys.stdout)
        else:
            capture.observe("agent.status_reported", {"text": "Host capture connected at " + now()})
            JsonLinesCapture(capture).consume(sys.stdin.buffer)
    finally:
        try:
            if capture is not None:
                capture.close()
        finally:
            client.close()


if __name__ == "__main__":
    main()
