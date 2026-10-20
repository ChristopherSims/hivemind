import hashlib
import json
from pathlib import Path

import pytest

from hivemind.retrieval_benchmark import load_dataset
from hivemind.retrieval_dataset import build_dataset

ROOT = Path(__file__).resolve().parents[1]


def test_real_corpus_pins_source_excerpts_and_has_judged_developer_questions():
    dataset, _ = load_dataset(ROOT / "benchmarks/retrieval-repository-v1.json")
    assert len(dataset["documents"]) == 64 and len(dataset["queries"]) == 18
    assert "Not independent labels" in dataset["corpus_scope"]
    for document in dataset["documents"]:
        source = document["source"]
        assert source["path"].startswith("hivemind/") and source["path"].endswith(".py")
        assert source["end_line"] - source["start_line"] < 96
        excerpt = document["text"].split("\n", 1)[1]
        assert hashlib.sha256(excerpt.encode()).hexdigest() == source["excerpt_sha256"]


def test_builder_is_deterministic_and_uses_only_explicit_allowlist():
    manifest = json.loads((ROOT / "benchmarks/retrieval-repository-v1.manifest.json").read_text())
    first = build_dataset(ROOT, manifest)
    assert first == build_dataset(ROOT, manifest)
    assert len(first["documents"]) == 64
    assert {q["id"] for q in first["queries"]} == {q["id"] for q in manifest["queries"]}


@pytest.mark.parametrize("path", ["../private.py", ".hivemind-data/private.py", "hivemind/../private.py"])
def test_builder_rejects_non_source_or_escaping_paths(path):
    manifest = {"sources": [path], "max_documents": 1, "max_lines": 10}
    with pytest.raises(ValueError, match="allowlisted"):
        build_dataset(ROOT, manifest)
