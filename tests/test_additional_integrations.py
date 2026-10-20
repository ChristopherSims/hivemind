import hashlib
import io
import json
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from hivemind import models as m
from hivemind.editor_capture import JsonLinesCapture
from hivemind.memory_adapters import (
    LangMemAdapter, LettaAdapter, Mem0Adapter, MemoryAdapterBridge,
    MemoryAdapterPolicy, MemoryMirrorRequest,
)
from hivemind.native_artifact_verifier import NativeArtifactVerifier, NativeCheckProfile
from hivemind.storage import Problem, canonical


class RecordingCapture:
    def __init__(self):
        self.records = []

    def observe(self, suffix, data, **options):
        self.records.append((suffix, data, options))
        return "event"


def test_capture_is_bound_reported_hash_only_and_excludes_secrets():
    capture = RecordingCapture()
    adapter = JsonLinesCapture(capture)
    data = {"path": "src/main.py", "observation": "saved", "sha256": "a" * 64,
            "byte_size": 100, "version": 2}
    adapter.ingest(json.dumps({"type": "editor.buffer_observed", "data": data}).encode())
    assert capture.records[0][2] == {"attribution": "reported", "kind": "sdk"}
    assert capture.records[0][1] == data
    assert adapter.ingest(json.dumps({"type": "editor.buffer_observed",
        "data": {**data, "path": ".env"}}).encode()) is None
    for record in [
        {"type": "editor.buffer_observed", "data": {**data, "text": "secret"}},
        {"type": "editor.buffer_observed", "data": {**data, "path": "../private"}},
        {"type": "tool.started", "data": {"name": "read", "call_id": "x"}, "agent_id": "other"},
        {"type": "run.ended", "data": {"status": "completed", "expected_revision": 1}},
    ]:
        with pytest.raises(ValueError):
            adapter.ingest(json.dumps(record).encode())


def test_bad_json_lines_are_bounded_gaps_and_valid_records_continue():
    capture = RecordingCapture()
    stream = io.BytesIO(b"x" * 20000 + b"\n" + b'{"type":"tool.started","data":{"call_id":"1","name":"read"}}\n')
    JsonLinesCapture(capture).consume(stream)
    assert [record[0] for record in capture.records] == ["capture.gap_detected", "tool.started"]


def evidence(text="x = 1\n"):
    encoded = canonical({"media_type": "text/plain", "text": text})
    return {"project_id": "project", "contract_id": "contract", "contract_revision": 1,
            "task_id": "task", "execution_id": "execution", "run_id": "run", "agent_id": "agent",
            "output": "code", "check": "syntax",
            "artifacts": [{"name": "code", "artifact_id": "artifact", "revision": 1,
                           "sha256": hashlib.sha256(encoded.encode()).hexdigest()}],
            "inputs": [{"name": "code", "encoded": encoded}]}


def native(tmp_path, **kwargs):
    return NativeArtifactVerifier(staging_root=tmp_path, profiles=[
        NativeCheckProfile("syntax", "project", (sys.executable, "-m", "py_compile", "artifact.py"),
                           input_path="artifact.py")], enabled=True, require_os_isolation=False, **kwargs)


def test_real_native_syntax_profile_binds_exact_bytes_and_execution(tmp_path):
    verifier = native(tmp_path)
    good = evidence()
    result = verifier.run_bound("syntax", good)
    assert result["ok"] is True and result["evidence"] == good
    assert result["sandbox_verified"] is False
    assert verifier.run_bound("syntax", evidence("def !broken\n"))["ok"] is False
    assert len(list(tmp_path.iterdir())) == 2


def test_native_refuses_wrong_project_hash_missing_execution_and_bounds(tmp_path):
    verifier = native(tmp_path, max_snapshots=1, runner=lambda command: (0, b"", b""))
    for bad in [{**evidence(), "project_id": "other"},
                {**evidence(), "execution_id": None},
                {**evidence(), "inputs": [{"name": "code", "encoded": "{}"}]}]:
        with pytest.raises(Problem):
            verifier.run_bound("syntax", bad)
    assert not list(tmp_path.iterdir())
    assert verifier.run_bound("syntax", evidence())["ok"]
    with pytest.raises(Problem, match="native_snapshot_limit"):
        verifier.run_bound("syntax", evidence())


def test_mutating_native_check_cannot_certify_input(tmp_path):
    def mutate(command):
        (Path(command.cwd) / "artifact.py").write_text("changed")
        return 0, b"", b""
    result = native(tmp_path, runner=mutate).run_bound("syntax", evidence())
    assert not result["ok"] and not result["input_unchanged"]


def test_optional_provider_ports_are_data_only_and_namespace_bound():
    calls = []
    mem0 = Mem0Adapter(SimpleNamespace(add=lambda *a, **kw:
        (calls.append((a, kw)) or {"results": [{"id": "remote"}]})), "project", "private-namespace")
    assert mem0.put("key", "text", {}) == {"ids": ["remote"]}
    assert calls[0][1]["infer"] is False and calls[0][1]["user_id"] == "private-namespace"
    lang = LangMemAdapter(SimpleNamespace(put=lambda *a, **kw: calls.append((*a, kw))), "project", "ns")
    lang.put("key", "text", {})
    assert calls[-1][0] == ("hivemind", "ns")
    assert calls[-1][-1]["index"] is False
    letta = LettaAdapter(SimpleNamespace(blocks=SimpleNamespace(create=lambda **kw:
        (calls.append(kw) or SimpleNamespace(id="block")))), "project", "ns")
    assert letta.put("key", "text", {}) == {"id": "block"}
    assert calls[-1]["metadata"]["hivemind_namespace"] == "ns"
    assert "agent_id" not in calls[-1]


def memory(world):
    post = world.post()
    item = world.call("POST", "memories", {
        "project_id": world.project, "scope_kind": "project", "scope_owner_id": world.project,
        "title": "Public knowledge", "content": {"media_type": "text/plain", "text": "Reviewed fact"},
        "evidence_refs": [{"kind": "board_post", "id": post["id"], "revision": post["revision"]}]})
    return world.call("POST", f"memories/{item['id']}:review", {
        "expected_revision": item["revision"], "decision": "verify", "reason": "Reviewed"}), post


def bridge(world, put):
    store = world.app.state.store
    principal = dict(store.db.execute("SELECT * FROM principals WHERE role='admin'").fetchone())
    adapter = LangMemAdapter(SimpleNamespace(put=lambda *args, **kwargs: put(*args)), world.project, "ns")
    return MemoryAdapterBridge(world.app.state.service, {"langmem": adapter},
        MemoryAdapterPolicy(enabled=True, allow_remote_writes=True)), principal


def test_mirror_is_canonical_current_advisory_and_replay_does_not_write(world):
    item, _ = memory(world)
    calls = []
    mirror, principal = bridge(world, lambda *args: calls.append(args))
    request = MemoryMirrorRequest(adapter="langmem", expected_revision=item["revision"])
    result = mirror.publish(principal, item["id"], request, "key")
    assert result["canonical_authority"] is False
    assert mirror.publish(principal, item["id"], request, "different-key") == result
    assert len(calls) == 1
    assert world.call("GET", f"memories/{item['id']}")["revision"] == item["revision"]


def test_mirror_unknown_provider_effect_is_never_repeated(world):
    item, _ = memory(world)
    calls = []
    def fail(*args):
        calls.append(args)
        raise TimeoutError("Uncertain provider response")
    mirror, principal = bridge(world, fail)
    request = MemoryMirrorRequest(adapter="langmem", expected_revision=item["revision"])
    for key in ("first", "second"):
        with pytest.raises(Problem, match="memory_mirror_outcome_unknown"):
            mirror.publish(principal, item["id"], request, key)
    assert len(calls) == 1


def test_mirror_reauthorizes_source_after_provider_and_on_replay(world):
    item, post = memory(world)
    def withdraw(*args):
        with world.app.state.store.transaction():
            world.app.state.store.db.execute("UPDATE resources SET deleted=1 WHERE id=?", (post["id"],))
    mirror, principal = bridge(world, withdraw)
    with pytest.raises(Problem, match="memory_mirror_outcome_unknown"):
        mirror.publish(principal, item["id"], MemoryMirrorRequest(
            adapter="langmem", expected_revision=item["revision"]), "key")


def test_mirror_default_off_role_project_and_private_source_fences(world):
    item, post = memory(world)
    calls = []
    mirror, principal = bridge(world, lambda *args: calls.append(args))
    request = MemoryMirrorRequest(adapter="langmem", expected_revision=item["revision"])
    mirror.policy = MemoryAdapterPolicy()
    with pytest.raises(Problem, match="memory_mirroring_disabled"):
        mirror.publish(principal, item["id"], request, "key")
    mirror.policy = MemoryAdapterPolicy(enabled=True, allow_remote_writes=True)
    with world.app.state.store.transaction():
        world.app.state.store.db.execute("UPDATE resources SET private_agent='private' WHERE id=?", (post["id"],))
    with pytest.raises(Problem, match="memory_export_private_source"):
        mirror.publish(principal, item["id"], request, "key")
    assert calls == []


def test_mirror_shutdown_prevents_new_effects(world):
    item, _ = memory(world)
    mirror, principal = bridge(world, lambda *args: None)
    assert mirror.close()
    with pytest.raises(Problem, match="memory_adapter_closing"):
        mirror.publish(principal, item["id"], MemoryMirrorRequest(
            adapter="langmem", expected_revision=item["revision"]), "key")


def test_strict_integration_opt_ins():
    with pytest.raises(ValueError):
        MemoryAdapterPolicy(enabled=1)
    with pytest.raises(ValueError):
        NativeCheckProfile("shell", "project", ("pwsh", "-Command", "unknown"))
    with pytest.raises(ValueError):
        m.EditorBufferData(path="x", observation="saved", sha256="a"*64, byte_size=True, version=1)


def test_editor_event_roundtrip_does_not_claim_filesystem_effect(world):
    agent = world.agent()
    run = world.run(agent)
    token = world.credential(agent)
    event = m.Event(event_id="editor-event", type="hivemind.context.editor.buffer_observed",
        project_id=world.project, agent_id=agent["id"], run_id=run["id"],
        occurred_at="2026-10-12T00:00:00Z",
        source=m.Source(source_id="editor", kind="sdk", sequence=0, attribution="reported"),
        data={"path": "file.py", "observation": "saved", "sha256": "a" * 64,
              "byte_size": 10, "version": 1})
    result = world.call("POST", "events:batch", {"events": [event.model_dump()]}, token)
    assert result[0]["accepted"]
    assert world.call("GET", f"runs/{run['id']}")["files"] == {}


def test_native_operator_config_keeps_verifier_when_contracts_are_enabled(tmp_path):
    from hivemind.model_wiring import integration_options
    config = tmp_path / "operator.json"
    config.write_text(json.dumps({
        "artifact_contracts": {"enabled": True},
        "native_artifact_verifiers": {"enabled": True, "staging_root": str(tmp_path),
            "profiles": [{"name": "syntax", "project_id": "project",
                          "argv": [sys.executable, "-m", "py_compile", "artifact.py"],
                          "input_path": "artifact.py"}]}}))
    options = integration_options(config)
    assert options["artifact_contracts"]["policy"].enabled
    assert isinstance(options["artifact_contracts"]["verifier"], NativeArtifactVerifier)


def test_actual_vscode_module_emits_hashes_without_buffer_text(tmp_path):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node required for VS Code module harness")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    file = workspace / "main.py"
    file.write_text("private buffer contents")
    descriptor = tmp_path / "private.json"
    descriptor.write_text(json.dumps({"workspace_root": str(workspace), "python_executable": sys.executable}))
    root = Path(__file__).resolve().parents[1]
    harness = tmp_path / "probe.cjs"
    harness.write_text("""
const Module=require('node:module'), EventEmitter=require('node:events');
const handlers={}, events={}, writes=[];
const fake={workspace:{isTrusted:true,workspaceFolders:[{uri:{fsPath:process.argv[3]}}],
  onDidChangeWorkspaceFolders:f=>({}),
  onDidOpenTextDocument:f=>(events.open=f,{}),
  onDidChangeTextDocument:f=>(events.change=f,{}),
  onDidSaveTextDocument:f=>(events.save=f,{}),
  onDidCloseTextDocument:f=>(events.close=f,{})},
  commands:{registerCommand:(n,f)=>(handlers[n]=f,{})},
  window:{showOpenDialog:async()=>[{scheme:'file',fsPath:process.argv[4]}],
          showErrorMessage:()=>{throw Error('Startup failed')}}};
const child=new EventEmitter(); child.stdin=new EventEmitter();
child.stdin.write=s=>(writes.push(JSON.parse(s)),true); child.stdin.end=()=>child.emit('exit');
const original=Module.prototype.require;
Module.prototype.require=function(n){
  if(n==='vscode')return fake;
  if(n==='node:child_process')return {spawn:(exe,args,options)=>{
    if(options.shell!==false || args[1]!=='hivemind.editor_capture')throw Error('Unsafe spawn');
    return child;}};
  return original.call(this,n);};
(async()=>{
 const extension=require(process.argv[2]); extension.activate({subscriptions:[]});
 await handlers['hivemind.startCapture']();
 events.save({uri:{scheme:'file',fsPath:process.argv[5]},version:1,getText:()=> 'private buffer contents'});
 extension.deactivate(); console.log(JSON.stringify(writes));
})().catch(()=>process.exit(1));
""", encoding="utf-8")
    result = subprocess.run([node, str(harness), str(root / "adapters/vscode/extension.js"),
                             str(workspace), str(descriptor), str(file)],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    records = json.loads(result.stdout)
    assert records[0]["type"] == "editor.buffer_observed"
    assert records[0]["data"]["sha256"] == hashlib.sha256(b"private buffer contents").hexdigest()
    assert "private buffer contents" not in result.stdout
