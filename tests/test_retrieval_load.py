import copy
import json
from pathlib import Path

import pytest

from hivemind.retrieval_benchmark import SyntheticEncoder
from hivemind.retrieval_load import balanced_queries, measure, prepare
from hivemind.retrieval_workload import (
    blind_pack, build_workload, checksum, fusion_trace, load_workload, save_workload, score_review, validate_review,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def dataset():
    return build_workload(ROOT, 100)


def test_mixed_corpus_has_real_code_and_synthetic_board_memory_without_production_data(dataset, tmp_path):
    assert len(dataset["documents"]) == 100 and len(dataset["queries"]) == 26
    assert {doc["kind"] for doc in dataset["documents"]} == {"artifact", "board_post", "memory"}
    assert {q["domain"] for q in balanced_queries(dataset)[:3]} == {"code", "board", "memory"}
    assert dataset["independent_judgments"] == "pending"
    assert all(path.startswith(("hivemind/", "docs/")) for path in dataset["source_pins"])
    snapshot = tmp_path / "snapshot.json.gz"
    save_workload(snapshot, dataset)
    assert load_workload(snapshot) == dataset
    with pytest.raises(FileExistsError):
        save_workload(snapshot, dataset)
    with pytest.raises(ValueError):
        build_workload(ROOT, 99)


def test_real_production_path_with_synthetic_test_encoder_and_load_checks(dataset, tmp_path):
    work = tmp_path / "work"
    state = prepare(dataset, work, SyntheticEncoder())
    assert state["resources"] == 100 and state["encoder"] == "synthetic_test_only"
    assert len(state["forbidden"]) == 24
    result, pack, key = measure(dataset, work, SyntheticEncoder(), seconds=.02, workers=(1, 2), cold_samples=1)
    assert result["state"] == "completed"
    assert result["independent_review"] == "pending" and result["os_cold_cache"] == "not_established"
    assert not result["ranking_policy_changed"]
    assert result["warm"]["requests"] == 78
    assert all(row["scan"]["authorized_candidates"]["memory"] > 0 for row in result["warm"]["samples"])
    assert all(load["duration_reached"] and not load["errors"] for load in result["loads"])
    assert key["dataset_sha256"] == checksum(dataset)
    assert all(candidate["grade"] is None for query in pack["questions"] for candidate in query["candidates"])
    with pytest.raises(FileExistsError):
        prepare(dataset, work, SyntheticEncoder())
    modified = copy.deepcopy(dataset)
    modified["queries"][0]["text"] += " changed"
    with pytest.raises(ValueError, match="mismatched"):
        measure(modified, work, SyntheticEncoder(), seconds=.01)


def test_rank_blind_pack_hides_ranks_modes_ids_and_provisional_labels(dataset):
    rankings = {q["id"]: {mode: list(q["relevance"]) for mode in ("lexical", "semantic", "hybrid")}
                for q in dataset["queries"]}
    pack, key = blind_pack(dataset, rankings)
    for query in pack["questions"]:
        for candidate in query["candidates"]:
            assert set(candidate) == {"candidate_id", "title", "text", "kind", "grade"}
            assert not candidate["title"].startswith("Synthetic fixture ")
    reviewed = copy.deepcopy(pack)
    with pytest.raises(ValueError, match="declaration"):
        validate_review(pack, reviewed, key)
    reviewed["reviewer"] = {"id": "test-reviewer", "independent": True}
    with pytest.raises(ValueError, match="judgment"):
        validate_review(pack, reviewed, key)
    for query in reviewed["questions"]:
        for candidate in query["candidates"]:
            candidate["grade"] = 2
    validated = validate_review(pack, reviewed, key)
    assert validated["independence"] == "reviewer_declared_not_identity_verified"
    reviewed["questions"][0]["candidates"][0]["text"] += " changed"
    with pytest.raises(ValueError, match="changed"):
        validate_review(pack, reviewed, key)


def test_review_mapping_is_pinned(dataset):
    rankings = {q["id"]: {mode: [] for mode in ("lexical", "semantic", "hybrid")} for q in dataset["queries"]}
    pack, key = blind_pack(dataset, rankings)
    reviewed = copy.deepcopy(pack)
    reviewed["reviewer"] = {"id": "test", "independent": True}
    for query in reviewed["questions"]:
        for candidate in query["candidates"]:
            candidate["grade"] = 0
    first = next(iter(key["mapping"]))
    key["mapping"][first]["document_id"] = "forged"
    with pytest.raises(ValueError, match="changed"):
        validate_review(pack, reviewed, key)


def test_fusion_trace_explains_consensus_boost_and_known_positive_demotion():
    lexical = ["both", "other"]
    semantic = ["answer", "both"] + [f"s{i}" for i in range(10)]
    trace = fusion_trace(lexical, semantic, ["both"] + [f"s{i}" for i in range(9)], {"answer": 2})
    assert trace["known_positive_demotions"] == ["answer"]
    rows = {row["id"]: row for row in trace["candidates"]}
    assert rows["both"]["lexical_vote"] == pytest.approx(1 / 61)
    assert rows["both"]["semantic_vote"] == pytest.approx(1 / 62)
    assert rows["answer"]["lexical_vote"] == 0


def test_missing_cache_failure_is_recorded_without_exception_content(dataset, tmp_path, monkeypatch):
    from hivemind import retrieval_load
    source, output = tmp_path / "data.gz", tmp_path / "output.json"
    save_workload(source, dataset)
    def unavailable():
        raise RuntimeError("private cache location must not appear")
    monkeypatch.setattr(retrieval_load, "Gemma", unavailable)
    monkeypatch.setattr("sys.argv", ["load", "prepare", "--dataset", str(source), "--work", str(tmp_path / "w"),
                                    "--output", str(output)])
    assert retrieval_load.main() == 1
    report = json.loads(output.read_text())
    assert report["failure"] == {"type": "RuntimeError"}
    assert "private cache" not in output.read_text()


def test_metadata_permission_checks_own_the_canonical_connection_lock(dataset, tmp_path, monkeypatch):
    from hivemind.indexes import Indexes
    from hivemind.models import SearchQuery
    from hivemind.storage import Store
    work = tmp_path / "work"
    state = prepare(dataset, work, SyntheticEncoder())
    store, indexes = Store(work / "data"), None
    original = store.visible
    def guarded(row, principal):
        assert store.lock._is_owned(), "Canonical permission traversal requires the store lock"
        return original(row, principal)
    monkeypatch.setattr(store, "visible", guarded)
    try:
        indexes = Indexes(store, SyntheticEncoder())
        result = indexes.search(state["principal"], SearchQuery(project_id="benchmark", query="memory",
                                mode="hybrid", resource_kinds=["artifact", "board_post", "memory"]))
        assert sum(result["scan"]["authorized_candidates"].values()) == 100
    finally:
        if indexes:
            indexes.close()
        store.close()


def test_synthetic_fixture_cannot_be_relabelled_as_real_encoder(dataset, tmp_path):
    work = tmp_path / "work"
    state = prepare(dataset, work, SyntheticEncoder())
    state["encoder"] = "pinned_real_model"
    (work / "state.json").write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(ValueError, match="encoder identity"):
        measure(dataset, work, SyntheticEncoder(), seconds=.01)


def test_completed_review_scores_only_pinned_pool_and_handles_no_positive_answers(dataset):
    rankings = {q["id"]: {mode: list(q["relevance"]) for mode in ("lexical", "semantic", "hybrid")}
                for q in dataset["queries"]}
    pack, key = blind_pack(dataset, rankings)
    reviewed = copy.deepcopy(pack)
    reviewed["reviewer"] = {"id": "test-reviewer", "independent": True}
    for query in reviewed["questions"]:
        for candidate in query["candidates"]:
            candidate["grade"] = 2
    report = score_review(pack, reviewed, key, rankings)
    assert all(row["pooled_recall_at_10"] == 1 for row in report["rows"])
    for candidate in reviewed["questions"][0]["candidates"]:
        candidate["grade"] = 0
    assert score_review(pack, reviewed, key, rankings)["rows"][0]["ndcg_at_10"] is None
    changed = copy.deepcopy(rankings)
    changed[next(iter(changed))]["hybrid"] = []
    with pytest.raises(ValueError, match="rankings changed"):
        score_review(pack, reviewed, key, changed)
