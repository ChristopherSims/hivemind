import hashlib
import json
from types import SimpleNamespace

import pytest

from hivemind.memory_adapters import LangMemAdapter, LettaAdapter, Mem0Adapter
from hivemind.memory_validation import (
    audit_retained_files, erasure_fixture, provider_absence, validate_tokenizer,
)


@pytest.fixture
def golden_tokenizer(tmp_path):
    from tokenizers import Tokenizer
    from tokenizers.models import WordLevel
    from tokenizers.pre_tokenizers import Whitespace
    tokenizer = Tokenizer(WordLevel({"[UNK]": 0, "hello": 1, "world": 2, "!": 3}, unk_token="[UNK]"))
    tokenizer.pre_tokenizer = Whitespace()
    tokenizer.enable_truncation(1)
    tokenizer.enable_padding(length=10)
    path = tmp_path / "tokenizer.json"
    tokenizer.save(str(path))
    manifest = {
        "schema_version": 1, "model_id": "fixture/wordlevel", "model_revision": "fixture-v1",
        "tokenizer_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "reference_source": "Hand-counted fixture vocabulary; not production model evidence",
        "samples": [{"id": "empty", "text": "", "expected_tokens": 0},
                    {"id": "punctuation", "text": "hello world!", "expected_tokens": 3},
                    {"id": "unknown", "text": "unlisted", "expected_tokens": 1},
                    {"id": "long", "text": "hello " * 2048, "expected_tokens": 2048},
                    {"id": "unicode", "text": "你好", "expected_tokens": 1},
                    {"id": "json", "text": '{"hello":"world"}', "expected_tokens": 5}],
    }
    return path, manifest


def test_tokenizer_disables_truncation_padding_and_checks_independent_counts(golden_tokenizer):
    path, manifest = golden_tokenizer
    report = validate_tokenizer(manifest, path)
    assert report["passed"]
    assert report["vendor_model_binding_verified"] is False
    assert "hello" not in json.dumps(report)


def test_tokenizer_count_mismatch_and_hash_mismatch_fail_closed(golden_tokenizer):
    path, manifest = golden_tokenizer
    manifest["samples"][0]["expected_tokens"] = 1
    assert not validate_tokenizer(manifest, path)["passed"]
    manifest["tokenizer_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        validate_tokenizer(manifest, path)


def test_tokenizer_requires_reference_evidence_and_unique_samples(golden_tokenizer):
    path, manifest = golden_tokenizer
    manifest["reference_source"] = ""
    with pytest.raises(ValueError):
        validate_tokenizer(manifest, path)
    manifest["reference_source"] = "fixture"
    manifest["samples"].append(manifest["samples"][0])
    with pytest.raises(ValueError):
        validate_tokenizer(manifest, path)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16-le", "utf-16-be"])
def test_audit_detects_canary_across_chunk_boundaries_without_writes(tmp_path, encoding):
    marker = "disposable-canary-abc"
    path = tmp_path / "retained"
    path.write_bytes(b"x" * 5 + marker.encode(encoding) + b"tail")
    original = path.read_bytes()
    report = audit_retained_files([{"label": "backup", "category": "backup", "path": path}],
                                  marker, chunk_size=7)
    assert report["inventory_scan_complete"]
    assert report["files"][0]["marker_found"] is True
    assert not report["marker_absent_from_inventory"]
    assert path.read_bytes() == original
    assert marker not in json.dumps(report) and str(path) not in json.dumps(report)


def test_audit_missing_oversized_and_relative_inputs_do_not_claim_absence(tmp_path):
    path = tmp_path / "big"
    path.write_bytes(b"x" * 30)
    report = audit_retained_files([
        {"label": "missing", "category": "wal", "path": tmp_path / "missing"},
        {"label": "large", "category": "backup", "path": path},
        {"label": "relative", "category": "spool", "path": "relative"},
    ], "canary-marker", max_bytes=10)
    assert not report["inventory_scan_complete"] and not report["marker_absent_from_inventory"]
    assert [row["status"] for row in report["files"]] == [
        "unreadable_or_unsafe", "size_limit", "unreadable_or_unsafe"]


def test_audit_plaintext_absence_is_not_physical_erasure(tmp_path):
    path = tmp_path / "clean"
    path.write_bytes(b"clean")
    report = audit_retained_files([{"label": "canonical", "category": "sqlite", "path": path}],
                                  "canary-marker")
    assert report["marker_absent_from_inventory"]
    assert not report["physical_erasure_verified"]


def test_audit_rejects_symlinks_when_supported(tmp_path):
    path = tmp_path / "target"
    path.write_bytes(b"canary-marker")
    link = tmp_path / "link"
    try:
        link.symlink_to(path)
    except OSError:
        pytest.skip("Symlink privileges unavailable")
    report = audit_retained_files([{"label": "link", "category": "spool", "path": link}], "canary-marker")
    assert not report["inventory_scan_complete"]


def test_disposable_deletion_compaction_and_retained_copy_detection():
    result = erasure_fixture()
    assert result["fixture_passed"]
    assert result["production_data_modified"] is False
    assert not result["audit"]["physical_erasure_verified"]
    assert not result["audit"]["marker_absent_from_inventory"]


def test_source_deletion_retains_dependent_history_and_must_not_claim_erasure(world):
    marker = "dependent-memory-retention-canary"
    source = world.post(text="Supporting source without sensitive content")
    memory = world.call("POST", "memories", {
        "project_id": world.project,
        "title": "Dependent memory", "category": "semantic", "scope_kind": "project",
        "scope_owner_id": world.project,
        "content": {"media_type": "text/plain", "text": marker},
        "evidence_refs": [{"kind": "board_post", "id": source["id"], "revision": source["revision"]}],
    })
    world.call("DELETE", f"posts/{source['id']}", {"expected_revision": source["revision"]})
    store = world.app.state.store
    current = json.loads(store.raw(memory["id"])["body"])
    assert current["status"] == "disputed" and current["invalidation_reason"] == "source_deleted"
    versions = store.db.execute("SELECT body FROM versions WHERE id=?", (memory["id"],)).fetchall()
    assert len(versions) >= 2 and all(marker in row["body"] for row in versions)
    # Invalidating evidence hides dependent content; it is not permission to erase it.
    assert store.db.execute("SELECT deleted FROM resources WHERE id=?", (memory["id"],)).fetchone()[0] == 0


@pytest.mark.parametrize("name", ["mem0", "langmem", "letta"])
def test_provider_read_after_delete_verifies_absence_not_acknowledgement(name):
    records = {}
    def get(identity):
        return records.get(identity)
    def delete(identity):
        records.pop(identity, None)
    if name == "mem0":
        def put(messages, **options):
            assert options["infer"] is False
            records["one"] = {"id": "one", "memory": messages[0]["content"], "metadata": options["metadata"]}
            return {"results": [{"id": "one"}]}
        client = SimpleNamespace(add=put, get=get, delete=delete)
        adapter = Mem0Adapter(client, "project", "disposable", binding_id="offline")
    elif name == "langmem":
        def put(namespace, key, value, *, index):
            assert index is False
            records[key] = {"key": key, "namespace": namespace, "value": value}
        client = SimpleNamespace(put=put, get=lambda namespace, key: get(key),
                                 delete=lambda namespace, key: delete(key))
        adapter = LangMemAdapter(client, "project", "disposable", binding_id="offline")
    else:
        def put(**options):
            records["one"] = {"id": "one", "value": options["value"], "metadata": options["metadata"]}
            return {"id": "one"}
        client = SimpleNamespace(blocks=SimpleNamespace(create=put, retrieve=get, delete=delete))
        adapter = LettaAdapter(client, "project", "disposable", binding_id="offline")
    receipt = adapter.put("one", "Synthetic compatibility record", {"purpose": "offline-test"})
    assert not provider_absence(adapter, receipt)["absence_verified"]
    adapter.delete(receipt)
    result = provider_absence(adapter, receipt)
    assert result["absence_verified"] and not result["physical_erasure_verified"]
    assert result["provider_writes"] == 0


def test_provider_unknown_and_wrong_identity_do_not_claim_absence():
    adapter = SimpleNamespace(name="letta", get=lambda identity: {"id": "another"})
    assert provider_absence(adapter, {"id": "one"})["states"] == ["identity_mismatch"]
    def fail(identity):
        raise TimeoutError()
    adapter.get = fail
    assert not provider_absence(adapter, {"id": "one"})["absence_verified"]
    with pytest.raises(ValueError):
        provider_absence(adapter, {"id": None})
    with pytest.raises(ValueError):
        provider_absence(SimpleNamespace(name="mem0"), {"ids": [["bad"]]})
