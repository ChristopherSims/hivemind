import math
import json
import struct
from pathlib import Path

import pytest

from hivemind.retrieval_benchmark import (
    BenchmarkIncomplete, SyntheticEncoder, latency, load_dataset, quality, reduced_blob,
    run_quality, run_scale, run_search_scale,
)


DATASET = Path(__file__).resolve().parents[1] / "benchmarks" / "retrieval-v1.json"


def test_quality_uses_all_relevant_documents_and_graded_discount():
    result = quality(["wrong", "b", "a"], {"a": 2, "b": 1})
    ideal = 3 + 1 / math.log2(3)
    assert result["recall_at_10"] == 1
    assert result["mrr_at_10"] == .5
    assert result["ndcg_at_10"] == pytest.approx((1 / math.log2(3) + 3 / 2) / ideal)
    assert quality([], {"a": 1})["ndcg_at_10"] == 0
    assert quality(["hidden"], {"a": 1}, {"hidden"})["forbidden_result_rate"] == 1


def test_quality_does_not_inflate_recall_beyond_top_ten():
    assert quality([str(i) for i in range(11)], {"10": 1})["recall_at_10"] == 0
    with pytest.raises(ValueError):
        quality(["a", "a"], {"a": 1})
    with pytest.raises(ValueError):
        quality([], {})


def test_latency_nearest_rank_p95():
    assert latency(list(range(1, 21))) == {"samples": 20, "p50_ms": 10.5, "p95_ms": 19, "max_ms": 20}
    with pytest.raises(ValueError):
        latency([])


@pytest.mark.parametrize("dimension", [256, 512, 768])
def test_reduced_vector_preserves_unit_norm_and_zero_padding(dimension):
    blob = reduced_blob(SyntheticEncoder().query("sample"), dimension)
    values = struct.unpack("<768f", blob)
    assert sum(value * value for value in values) == pytest.approx(1)
    assert all(value == 0 for value in values[dimension:])
    with pytest.raises(ValueError):
        reduced_blob(blob, 42)


def test_versioned_dataset_and_production_lexical_path():
    dataset, checksum = load_dataset(DATASET)
    assert len(checksum) == 64
    assert len(dataset["queries"]) == 8
    report = run_quality(dataset, repeats=1)
    comparison, = report["comparisons"]
    assert comparison["mode"] == "lexical" and comparison["dimension"] is None
    assert comparison["forbidden_result_rate"] == 0
    assert len(comparison["queries"]) == 8
    symbol = next(row for row in comparison["queries"] if row["query_id"] == "symbol")
    assert symbol["recall_at_10"] == 1
    # Some non-exact natural-language queries genuinely fail lexical matching.
    assert comparison["recall_at_10"] < 1


def test_all_modes_and_dimensions_with_explicitly_synthetic_vectors():
    dataset, _ = load_dataset(DATASET)
    dataset = {**dataset, "queries": dataset["queries"][-1:]}
    report = run_quality(dataset, SyntheticEncoder(), repeats=1)
    assert len(report["comparisons"]) == 9
    assert {row["dimension"] for row in report["comparisons"]} == {256, 512, 768}
    assert all(row["forbidden_result_rate"] == 0 for row in report["comparisons"])


def test_scale_reads_and_real_graph_rebuild():
    report = run_scale(12, readers=3, reads=9)
    assert report["graph_rebuild_equivalent"]
    assert report["graph_read_latency"]["samples"] == 9
    assert report["resources"] == 12
    assert report["indexed_resources_per_second"] > 0
    assert report["graph_reads_during_rebuild"]["samples"] == 27
    with pytest.raises(ValueError):
        run_scale(0)


def test_cli_preserves_failed_benchmark_evidence_and_refuses_overwrite(tmp_path, monkeypatch):
    from hivemind import retrieval_benchmark as benchmark
    output = tmp_path / "report.json"
    monkeypatch.setattr("sys.argv", ["benchmark", "--dataset", str(DATASET),
                                    "--output", str(output), "--scale", "10000"])
    monkeypatch.setattr(benchmark, "run_quality", lambda *args, **kwargs: {"comparisons": []})
    def fail(*args, **kwargs):
        raise BenchmarkIncomplete([{"kind": "graph", "state": "pending", "jobs": 2}])
    monkeypatch.setattr(benchmark, "run_scale", fail)
    assert benchmark.main() == 1
    report = json.loads(output.read_text())
    assert report["status"] == "failed"
    assert report["failure"]["requested_scale"] == 10000
    assert report["failure"]["incomplete_jobs"][0]["jobs"] == 2
    original = output.read_bytes()
    with pytest.raises(SystemExit):
        benchmark.main()
    assert output.read_bytes() == original


@pytest.mark.parametrize("budget", [True, 0, 1.5, 8 * 1024 * 1024 * 1024])
def test_graph_buffer_budget_rejects_invalid_values(budget):
    from hivemind.indexes import Indexes
    with pytest.raises(ValueError, match="Graph buffer pool"):
        Indexes(None, graph_buffer_pool_size=budget)


def test_search_only_scale_reports_complete_candidate_scans_and_stage_samples():
    report = run_search_scale(15, repeats=2)
    assert report["resources"] == 15 and report["semantic_query_plan"]
    for result in report["comparisons"].values():
        assert result["latency"]["samples"] == 2
        assert result["stages"]["_authorized_meta"]["samples"] == 2
        assert result["scan"]["authorized_candidates"]["board_post"] == 15
        assert not any(result["scan"]["scan_truncated"].values())
        assert not result["scan"]["time_budget_exceeded"]
    with pytest.raises(ValueError):
        run_search_scale(10, repeats=0)


def test_model_startup_failure_is_recorded_without_sensitive_messages(tmp_path, monkeypatch):
    from hivemind import retrieval_benchmark as benchmark
    output = tmp_path / "failure.json"
    def unavailable():
        raise RuntimeError("sensitive encoder diagnostic")
    monkeypatch.setattr(benchmark, "Gemma", unavailable)
    monkeypatch.setattr("sys.argv", ["benchmark", "--dataset", str(DATASET),
                                    "--output", str(output), "--real-model"])
    assert benchmark.main() == 1
    assert json.loads(output.read_text())["failure"]["type"] == "RuntimeError"
    assert "sensitive" not in output.read_text()
