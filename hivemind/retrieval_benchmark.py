"""Offline, disposable benchmarks against the actual authorized retrieval path."""
import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import statistics
import struct
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .indexes import MODEL_ID, MODEL_REVISION, Gemma, Indexes, vector_blob
from .models import GraphQuery, SearchQuery
from .storage import Store, canonical


class BenchmarkIncomplete(RuntimeError):
    def __init__(self, states):
        self.states = states
        super().__init__("Index jobs incomplete")


def latency(samples):
    ordered = sorted(samples)
    if not ordered:
        raise ValueError("Latency samples are required")
    return {"samples": len(ordered), "p50_ms": statistics.median(ordered),
            "p95_ms": ordered[math.ceil(len(ordered) * .95) - 1], "max_ms": max(ordered)}


def quality(ranked, relevance, forbidden=(), k=10):
    if len(ranked) != len(set(ranked)):
        raise ValueError("Duplicate ranked IDs")
    if not relevance or any(type(v) is not int or not 1 <= v <= 3 for v in relevance.values()):
        raise ValueError("Positive integer relevance judgments are required")
    top = ranked[:k]
    hits = [i for i, identity in enumerate(top, 1) if identity in relevance]
    dcg = sum((2 ** relevance.get(identity, 0) - 1) / math.log2(i + 1)
              for i, identity in enumerate(top, 1))
    ideal = sum((2 ** grade - 1) / math.log2(i + 1)
                for i, grade in enumerate(sorted(relevance.values(), reverse=True)[:k], 1))
    denied = sum(identity in forbidden for identity in top)
    return {"recall_at_10": len(hits) / len(relevance),
            "mrr_at_10": 1 / hits[0] if hits else 0, "ndcg_at_10": dcg / ideal,
            "forbidden_results": denied, "returned_results": len(top),
            "forbidden_result_rate": denied / len(top) if top else 0}


def load_dataset(path):
    raw = Path(path).read_bytes()
    data = json.loads(raw)
    ids = [item["id"] for item in data["documents"]]
    queries = [item["id"] for item in data["queries"]]
    if (not ids or not queries or len(ids) != len(set(ids)) or len(queries) != len(set(queries))
            or any(not item["text"].strip() for item in data["documents"] + data["queries"])):
        raise ValueError("Dataset IDs and texts must be nonempty and unique")
    for query in data["queries"]:
        if not set(query["relevance"]) <= set(ids):
            raise ValueError("Relevance refers to unknown documents")
        quality([], query["relevance"])
    return data, hashlib.sha256(raw).hexdigest()


def project_fixture(store):
    store.db.execute("INSERT INTO projects(id,title) VALUES('benchmark','Synthetic benchmark')")
    store.db.execute("INSERT INTO projects(id,title) VALUES('other','Isolation negative')")
    store.db.execute("INSERT INTO principals(id,token_hash,role,project,agent) "
                     "VALUES('reader','not-a-credential','agent','benchmark','reader-agent')")
    for project in ("benchmark", "other"):
        store.save("board", project, "reader",
                   {"id": project + "-board", "project_id": project, "title": "Benchmark"})
    return dict(store.db.execute("SELECT * FROM principals WHERE id='reader'").fetchone())


def add_document(store, identity, text, *, project="benchmark", private=None):
    return store.save("board_post", project, "reader", {
        "id": identity, "project_id": project, "board_id": project + "-board",
        "content": {"media_type": "text/plain", "text": text},
    }, private=private)


def reduced_blob(blob, dimension):
    if dimension not in (256, 512, 768):
        raise ValueError("Unsupported benchmark dimension")
    values = struct.unpack("<768f", blob)[:dimension]
    norm = math.sqrt(sum(v * v for v in values))
    if not norm or not math.isfinite(norm):
        raise ValueError("Degenerate truncated vector")
    return vector_blob([v / norm for v in values] + [0.] * (768 - dimension))


class DimensionEncoder:
    """Zero padding preserves reduced cosine in the fixed 768-column fixture schema."""
    def __init__(self, base, dimension, documents):
        self.base, self.dimension, self.cache = base, dimension, documents

    def documents(self, title, text):
        return [(chunk, reduced_blob(blob, self.dimension)) for chunk, blob in self.cache[(title, text)]]

    def query(self, text):
        return reduced_blob(self.base.query(text), self.dimension)


def environment():
    packages = {}
    for name in ("sqlite-vec", "ladybug", "sentence-transformers", "torch", "tokenizers"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    return {"platform": platform.platform(), "python": platform.python_version(),
            "machine": platform.machine(), "processor": platform.processor(),
            "logical_cpus": os.cpu_count(), "packages": packages}


def check_jobs(store, kind=None):
    sql = "SELECT count(*) FROM jobs WHERE state!='done'"
    args = ()
    if kind:
        sql += " AND kind=?"
        args = (kind,)
    count = store.db.execute(sql, args).fetchone()[0]
    if count:
        states = [dict(row) for row in store.db.execute(
            "SELECT kind,state,count(*) AS jobs FROM jobs WHERE state!='done' GROUP BY kind,state")]
        raise BenchmarkIncomplete(states)


def run_quality(dataset, encoder=None, *, dimensions=(768, 512, 256), repeats=3):
    if repeats < 1:
        raise ValueError("At least one measured repetition is required")
    # Preparing document embeddings once avoids charging three identical model passes.
    # Query encoding is never cached and is included in measured search latency.
    from .storage import searchable
    cache, encoding_ms = {}, []
    if encoder:
        for item in dataset["documents"]:
            body = {"content": {"media_type": "text/plain", "text": item["text"]}}
            text = searchable(body)
            started = time.perf_counter()
            cache[("", text)] = encoder.documents("", text)
            encoding_ms.append((time.perf_counter() - started) * 1000)
    reports = []
    for dimension in dimensions if encoder else (None,):
        with tempfile.TemporaryDirectory(prefix="hivemind-retrieval-") as directory:
            store = Store(directory)
            indexes = None
            try:
                principal = project_fixture(store)
                forbidden = set()
                for item in dataset["documents"]:
                    add_document(store, item["id"], item["text"])
                    # Identical high-similarity negatives exercise both privacy boundaries.
                    for prefix, project, private in (
                            ("private-", "benchmark", "another-agent"), ("cross-", "other", None)):
                        identity = prefix + item["id"]
                        forbidden.add(identity)
                        add_document(store, identity, item["text"], project=project, private=private)
                selected = DimensionEncoder(encoder, dimension, cache) if encoder else None
                indexes = Indexes(store, selected)
                indexes.drain(limit=len(dataset["documents"]) * 4)
                if encoder:
                    check_jobs(store)
                for mode in ("lexical", "semantic", "hybrid") if encoder else ("lexical",):
                    rows, timings = [], []
                    for query in dataset["queries"]:
                        request = SearchQuery(project_id="benchmark", query=query["text"],
                                              resource_kinds=["board_post"], mode=mode, limit=10)
                        indexes.search(principal, request)  # explicit unmeasured warm-up
                        for _ in range(repeats):
                            started = time.perf_counter()
                            result = indexes.search(principal, request)
                            timings.append((time.perf_counter() - started) * 1000)
                            scan = result["scan"]
                            if (result["lexical_fallback"] or any(scan["scan_truncated"].values())
                                    or scan["rank_truncated"] or scan["time_budget_exceeded"]):
                                raise RuntimeError("Benchmark retrieval fell back or was truncated")
                            ids = [item["resource_id"] for item in result["results"]]
                            measured = quality(ids, query["relevance"], forbidden)
                            if measured["forbidden_results"]:
                                raise RuntimeError("Forbidden benchmark result")
                            rows.append({"query_id": query["id"], "subset": query["subset"],
                                         "ranked_ids": ids, **measured})
                    means = {key: statistics.mean(row[key] for row in rows)
                             for key in ("recall_at_10", "mrr_at_10", "ndcg_at_10", "forbidden_result_rate")}
                    subsets = {subset: {
                        key: statistics.mean(row[key] for row in rows if row["subset"] == subset)
                        for key in means} for subset in {row["subset"] for row in rows}}
                    reports.append({"mode": mode, "dimension": dimension, **means,
                                    "latency": latency(timings), "subsets": subsets, "queries": rows})
            finally:
                if indexes:
                    indexes.close()
                store.close()
    return {"comparisons": reports,
            "document_encoding": latency(encoding_ms) if encoding_ms else None,
            "method": "Production Indexes.search, FTS5/sqlite-vec/RRF and authorization. "
                      "Reduced dimensions are prefix-truncated, renormalized and zero-padded; "
                      "storage/latency do not simulate a smaller vec0 column. "
                      "Document encodings reused across dimensions; search includes query encoding. "
                      "One warm-up per query/mode/dimension; no HTTP overhead."}


class SyntheticEncoder:
    """Deterministic capacity workload, never evidence of semantic relevance."""
    def documents(self, title, text):
        return [(text, self.query(text))]

    def query(self, text):
        raw = hashlib.shake_256(text.encode()).digest(768 * 2)
        values = [value - 32767.5 for value in struct.unpack("<768H", raw)]
        norm = math.sqrt(sum(value * value for value in values))
        return vector_blob([value / norm for value in values])


def graph_snapshot(indexes):
    if not indexes.graph:
        raise RuntimeError("LadybugDB unavailable; fallback is not a graph benchmark")
    def rows(query):
        result = indexes.graph.execute(query)
        values = []
        while result.has_next():
            values.append(result.get_next())
        return sorted(values)
    with indexes.lock:
        return {"nodes": rows("MATCH (n:Entity) RETURN n.id,n.project"),
                "edges": rows("MATCH (a:Entity)-[r:Link]->(b:Entity) RETURN a.id,b.id,r.kind")}


def run_search_scale(size=10000, repeats=5):
    """Search-only capacity fixture; vector writes are bulk seeded, not indexing evidence."""
    if type(size) is not int or not 2 <= size <= 100000 or type(repeats) is not int or not 1 <= repeats <= 100:
        raise ValueError("Search benchmark bounds exceeded")
    with tempfile.TemporaryDirectory(prefix="hivemind-search-scale-") as directory:
        store, indexes = Store(directory), None
        try:
            principal = project_fixture(store)
            encoder = SyntheticEncoder()
            with store.transaction():
                for number in range(size):
                    identity = f"scale-{number:07d}"
                    text = f"Capacity sample number {number} database indexing"
                    add_document(store, identity, text)
                    chunk = store.db.execute(
                        "INSERT INTO vector_chunks(resource,revision,project,chunk,text) VALUES(?,1,?,0,?)",
                        (identity, "benchmark", text)).lastrowid
                    store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                                     (chunk, encoder.query(text), "benchmark"))
                store.db.execute("UPDATE jobs SET state='done' WHERE kind='embedding'")
            indexes = Indexes(store, encoder)
            results, plans = {}, []
            original_query = indexes._ro_query
            def capture_plan(sql, args, deadline):
                if "vec_distance_cosine" in sql and not plans:
                    rows, _ = original_query("EXPLAIN QUERY PLAN " + sql, args, deadline)
                    plans.extend([list(row) for row in rows])
                return original_query(sql, args, deadline)
            indexes._ro_query = capture_plan
            for mode in ("lexical", "semantic", "hybrid"):
                request = SearchQuery(project_id="benchmark", query="database indexing", mode=mode,
                                      resource_kinds=["board_post"], limit=10)
                indexes.search(principal, request, scan_limit=size + 1)
                samples, stages = [], {}
                originals = {}
                for name in ("_authorized_meta", "_lexical_ranking", "_semantic_ranking"):
                    method = getattr(indexes, name)
                    originals[name] = method
                    def measured(*args, _name=name, _method=method, _stages=stages, **kwargs):
                        started = time.perf_counter()
                        try:
                            return _method(*args, **kwargs)
                        finally:
                            _stages.setdefault(_name, []).append((time.perf_counter() - started) * 1000)
                    setattr(indexes, name, measured)
                try:
                    for _ in range(repeats):
                        started = time.perf_counter()
                        result = indexes.search(principal, request, scan_limit=size + 1)
                        samples.append((time.perf_counter() - started) * 1000)
                        if (any(result["scan"]["scan_truncated"].values())
                                or result["scan"]["time_budget_exceeded"]):
                            raise RuntimeError("Search scale scan incomplete")
                finally:
                    for name, method in originals.items():
                        setattr(indexes, name, method)
                results[mode] = {"latency": latency(samples),
                                 "samples_ms": samples,
                                 "stages": {name: latency(values) for name, values in stages.items()},
                                 "ranked_ids": [row["resource_id"] for row in result["results"]],
                                 "scan": result["scan"]}
            return {"resources": size, "repeats": repeats, "comparisons": results,
                    "sqlite_read_cache_setting": indexes.ro_db.execute("PRAGMA cache_size").fetchone()[0],
                    "semantic_query_plan": plans,
                    "method": "Production search, identical deterministic one-chunk vectors and "
                              "canonical documents to run_scale. Bulk fixture seeding, no graph work "
                              "or indexing throughput claim. One warm-up per mode; query encoding "
                              "included. Synthetic vectors are not semantic-quality evidence."}
        finally:
            if indexes:
                indexes.close()
            store.close()


def run_scale(size=2000, readers=4, reads=20, graph_memory_mib=256):
    if not 2 <= size <= 100000 or not 1 <= readers <= 32 or not 1 <= reads <= 1000:
        raise ValueError("Scale benchmark bounds exceeded")
    if type(graph_memory_mib) is not int or not 16 <= graph_memory_mib <= 4096:
        raise ValueError("Graph benchmark memory must be 16..4096 MiB")
    with tempfile.TemporaryDirectory(prefix="hivemind-scale-") as directory:
        store, indexes = Store(directory), None
        try:
            principal = project_fixture(store)
            started = time.perf_counter()
            with store.transaction():
                for number in range(size):
                    add_document(store, f"scale-{number:07d}", f"Capacity sample number {number} database indexing")
                for number in range(1, size):
                    store.db.execute("INSERT INTO relations VALUES(?,?,?,?)", (
                        "benchmark", f"scale-{number - 1:07d}", f"scale-{number:07d}", "RELATED_TO"))
            ingest_ms = (time.perf_counter() - started) * 1000
            indexes = Indexes(store, SyntheticEncoder(), graph_buffer_pool_size=graph_memory_mib * 1024 * 1024)
            started = time.perf_counter()
            indexes.drain(limit=size + 10)
            indexing_ms = (time.perf_counter() - started) * 1000
            check_jobs(store)
            before = graph_snapshot(indexes)
            graph_request = GraphQuery(project_id="benchmark", seed_ids=["scale-0000000"])
            def read(_):
                tick = time.perf_counter()
                # Match the API canonical-read guard and lock order. The SQLite
                # connection, like the graph connection, is not a parallel pool.
                with store.lock:
                    result = indexes.query_graph(principal, graph_request)
                if result["graph_fallback"] or len(result["nodes"]) != min(size, 3):
                    raise RuntimeError("Graph read mismatch")
                return (time.perf_counter() - tick) * 1000
            started = time.perf_counter()
            with ThreadPoolExecutor(max_workers=readers) as pool:
                timings = list(pool.map(read, range(reads)))
            concurrent_ms = (time.perf_counter() - started) * 1000
            query_timings, query_scans = {}, {}
            for mode in ("lexical", "semantic", "hybrid"):
                samples = []
                for _ in range(5):
                    tick = time.perf_counter()
                    result = indexes.search(principal, SearchQuery(
                        project_id="benchmark", query="database indexing", mode=mode,
                        resource_kinds=["board_post"], limit=10), scan_limit=size + 1)
                    samples.append((time.perf_counter() - tick) * 1000)
                    if any(result["scan"]["scan_truncated"].values()) or result["scan"]["time_budget_exceeded"]:
                        raise RuntimeError("Scale search truncated")
                query_timings[mode] = latency(samples)
                query_scans[mode] = result["scan"]
            indexes.close()
            indexes = None
            # Preserve the old graph; rebuild a fresh projection from canonical facts.
            Path(directory, "graph").rename(Path(directory, "graph-before-rebuild"))
            store.db.execute("UPDATE jobs SET state='pending',attempts=0,error=NULL WHERE kind='graph'")
            indexes = Indexes(store, SyntheticEncoder(), graph_buffer_pool_size=graph_memory_mib * 1024 * 1024)
            barrier = threading.Barrier(readers + 1)
            def rebuild():
                barrier.wait(timeout=30)
                tick = time.perf_counter()
                for _ in range(size + 10):
                    if not indexes.process_one("graph"):
                        break
                return (time.perf_counter() - tick) * 1000
            def rebuilding_reads(_):
                barrier.wait(timeout=30)
                samples = []
                for _ in range(reads):
                    tick = time.perf_counter()
                    with store.lock:
                        result = indexes.query_graph(principal, graph_request)
                    if result["graph_fallback"] or not 1 <= len(result["nodes"]) <= min(size, 3):
                        raise RuntimeError("Graph read during projection rebuild failed")
                    samples.append((time.perf_counter() - tick) * 1000)
                return samples
            started = time.perf_counter()
            with ThreadPoolExecutor(max_workers=readers + 1) as pool:
                writer = pool.submit(rebuild)
                reader_jobs = [pool.submit(rebuilding_reads, number) for number in range(readers)]
                rebuilding_timings = [sample for job in reader_jobs for sample in job.result()]
                writer_ms = writer.result()
            rebuild_ms = (time.perf_counter() - started) * 1000
            check_jobs(store, "graph")
            equivalent = graph_snapshot(indexes) == before
            if not equivalent:
                raise RuntimeError("Graph rebuild differs from original")
            return {"resources": size, "edges": size - 1, "graph_buffer_pool_mib": graph_memory_mib,
                    "ingest_ms": ingest_ms,
                    "indexing_ms": indexing_ms, "indexed_resources_per_second": size / (indexing_ms / 1000),
                    "graph_rebuild_ms": rebuild_ms, "graph_rebuild_writer_ms": writer_ms,
                    "graph_rebuild_equivalent": equivalent,
                    "graph_concurrent_readers": readers, "graph_read_latency": latency(timings),
                    "graph_read_wall_ms": concurrent_ms, "search_latency": query_timings,
                    "search_scan": query_scans, "graph_reads_during_rebuild": latency(rebuilding_timings),
                    "encoder": "synthetic SHAKE-256 normalized vectors; not model throughput/quality",
                    "concurrency": "Concurrent callers; canonical/graph connections are lock-serialized as in API. "
                                   "Includes concurrent readers during projection rebuild; "
                                   "canonical mutation/disk-full/kill stress are separate campaigns."}
        finally:
            if indexes:
                indexes.close()
            store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="New report file; never overwritten")
    parser.add_argument("--real-model", action="store_true", help="Use already-cached pinned model, no downloads")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--dimensions", type=int, nargs="+", choices=(768, 512, 256), default=[768, 512, 256])
    parser.add_argument("--scale", type=int, default=0, help="Synthetic capacity workload size; 0 skips")
    parser.add_argument("--search-scale", type=int, default=0,
                        help="Search-only synthetic capacity size; bulk vectors, no graph rebuild")
    parser.add_argument("--graph-memory-mib", type=int, default=256,
                        help="Scale graph memory budget, 16..4096 MiB; service default stays 64 MiB")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Report already exists")
    dataset, checksum = load_dataset(args.dataset)
    started = time.perf_counter()
    encoder = None
    report = {"schema_version": 1, "status": "running", "environment": environment(),
              "dataset_version": dataset["version"],
              "dataset_sha256": checksum, "dataset_documents": len(dataset["documents"]),
              "dataset_queries": len(dataset["queries"]), "model": MODEL_ID if args.real_model else None,
              "model_revision": MODEL_REVISION if args.real_model else None,
              "embedding_device": None, "embedding_dtype": None, "model_load_ms": None,
              "retrieval_implementation_sha256": hashlib.sha256(
                  Path(__file__).with_name("indexes.py").read_bytes()).hexdigest()}
    try:
        encoder = Gemma() if args.real_model else None
        if encoder:
            report.update(embedding_device=str(encoder.model.device),
                          embedding_dtype=str(next(encoder.model.parameters()).dtype),
                          model_load_ms=(time.perf_counter() - started) * 1000)
        report["quality"] = run_quality(dataset, encoder, dimensions=args.dimensions, repeats=args.repeats)
        report["scale"] = run_scale(args.scale, graph_memory_mib=args.graph_memory_mib) if args.scale else None
        report["search_scale"] = run_search_scale(args.search_scale, args.repeats) if args.search_scale else None
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = {"type": type(error).__name__,
                             "requested_scale": args.scale,
                             "incomplete_jobs": error.states if isinstance(error, BenchmarkIncomplete) else None}
        # Deliberately omit exception messages: injected encoders may carry content.
        report["elapsed_ms"] = (time.perf_counter() - started) * 1000
    with args.output.open("x", encoding="utf-8") as file:
        json.dump(report, file, indent=2, ensure_ascii=False, allow_nan=False)
        file.write("\n")
    print(canonical({"report": str(args.output), "status": report["status"]}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
