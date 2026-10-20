"""Export the implemented contract; event payloads are a discriminated union."""
import inspect
import json
from pathlib import Path

from hivemind import models
from hivemind.api import create_app
from hivemind.capabilities import model_modules


def export():
    root = Path(__file__).resolve().parent.parent
    destination = root / "spec" / "schemas"
    destination.mkdir(parents=True, exist_ok=True)
    for module in [models, *model_modules()]:
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__module__ != module.__name__ or not issubclass(cls, models.Model) or cls is models.Model:
                continue
            schema = constrain(cls.model_json_schema(by_alias=True))
            schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
            schema_name = name if module is models else f"{module.__name__.split('.')[-1]}.{name}"
            (destination / f"{schema_name}.schema.json").write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
    envelope = json.loads((destination / "Event.schema.json").read_text())
    variants = {
        "agent.status_reported": "StatusData", "tool.started": "ToolStart", "tool.completed": "ToolEnd",
        "file.changed": "FileData", "run.status_changed": "RunStatus", "run.ended": "RunStatus",
        "capture.gap_detected": "GapData", "decision.recorded": "DecisionData",
        "editor.buffer_observed": "EditorBufferData",
        "editor.acp_observed": "AcpObservationData",
    }
    envelope["allOf"] = [{
        "if": {"properties": {"type": {"const": "hivemind.context." + suffix}}, "required": ["type"]},
        "then": {"properties": {"data": {"$ref": name + ".schema.json"}}},
    } for suffix, name in variants.items()]
    envelope["allOf"].append({
        "if": {"properties": {"type": {"const": "hivemind.context.editor.acp_observed"}}, "required": ["type"]},
        "then": {"properties": {"source": {"properties": {"attribution": {"const": "reported"}}}}},
    })
    envelope["allOf"].append({
        "if": {"properties": {"type": {"const": "hivemind.context.editor.buffer_observed"}}, "required": ["type"]},
        "then": {"properties": {"source": {"properties": {"attribution": {"const": "reported"}}}}},
    })
    (destination / "Event.schema.json").write_text(json.dumps(envelope, indent=2) + "\n", encoding="utf-8")
    import tempfile
    with tempfile.TemporaryDirectory() as directory:
        from hivemind.blocked_tasks import BlockedTasksPolicy
        from hivemind.worker_provisioning import ProvisioningPolicy
        from hivemind.memory_adapters import MemoryAdapterPolicy
        # Document optional APIs without enabling any profile, process or model.
        app = create_app(directory, workers=False, capability_options={
            "blocked_tasks": {"policy": BlockedTasksPolicy(requests_enabled=True)},
            "worker_provisioning": {"policy": ProvisioningPolicy(enabled=True)},
            "memory_adapters": {"policy": MemoryAdapterPolicy(enabled=True)}})
        (root / "spec" / "openapi.json").write_text(json.dumps(app.openapi(), indent=2) + "\n", encoding="utf-8")
        app.state.indexes.close()
        app.state.store.close()


def constrain(schema):
    for _name, definition in schema.get("$defs", {}).items():
        constrain(definition)
    name = schema.get("title")
    if name == "Event":
        schema["properties"]["occurred_at"]["format"] = "date-time"
    elif name == "AcpObservationData":
        schema["allOf"] = [
            {"if": {"properties": {"observation": {"enum": [
                "tool_pending", "tool_in_progress", "tool_completed", "tool_failed"]}}},
             "then": {"required": ["item_sha256"], "properties": {
                 "item_sha256": {"type": "string", "pattern": "^[a-f0-9]{64}$"}}}},
            {"if": {"properties": {"observation": {"enum": ["file_read", "file_buffer_observed"]}}},
             "then": {"required": ["path", "sha256", "byte_size"], "properties": {
                 "path": {"type": "string"}, "sha256": {"type": "string"},
                 "byte_size": {"type": "integer"}}}},
            {"if": {"properties": {"observation": {"const": "file_buffer_observed"}}},
             "then": {"required": ["editor_observation", "actor"], "properties": {
                 "actor": {"const": "unattributed"},
                 "editor_observation": {"enum": ["opened", "changed", "saved", "closed"]}}}},
        ]
    elif name == "Content":
        schema["allOf"] = [{
            "if": {"properties": {"media_type": {"const": "application/vnd.hivemind.chat+json"}}},
            "then": {"required": ["segments"], "properties": {
                "text": {"type": "null"}, "segments": {"type": "array"}}},
            "else": {"required": ["text"], "properties": {
                "segments": {"type": "null"}, "text": {"type": "string", "minLength": 1}}},
        }]
    elif name == "Segment":
        schema["allOf"] = [{
            "if": {"properties": {"kind": {"const": "text"}}},
            "then": {"required": ["text"], "properties": {
                "text": {"type": "string"}, "agent_id": {"type": "null"}}},
            "else": {"required": ["agent_id"], "properties": {
                "agent_id": {"type": "string"}, "text": {"type": "null"}}},
        }]
    elif name == "Selector":
        schema["anyOf"] = [
            {"required": ["agent_ids"], "properties": {"agent_ids": {"type": "array", "minItems": 1}}},
            {"required": ["semantic_query"], "properties": {"semantic_query": {"type": "string", "pattern": r"\S"}}},
        ]
        schema["properties"]["agent_ids"]["anyOf"][0]["uniqueItems"] = True
        schema["properties"]["semantic_query"]["anyOf"][0]["pattern"] = r"\S"
        schema["allOf"] = [{
            "if": {"properties": {"match": {"const": "intersection"}}, "required": ["match"]},
            "then": {"required": ["agent_ids", "semantic_query"]},
        }]
    elif name in ("FileData", "EditorBufferData"):
        schema["properties"]["path"]["pattern"] = r"^(?![/\\])(?!.*(?:^|[/\\])\.\.(?:[/\\]|$))(?!.*[:\u0000]).+$"
    return schema


if __name__ == "__main__":
    export()
