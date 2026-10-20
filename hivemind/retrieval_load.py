"""Offline real-encoder mixed-corpus search, cache-state and closed-loop load probes."""
import argparse
import hashlib
import itertools
import json
import os
import statistics
import struct
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .indexes import MODEL_ID, MODEL_REVISION, Gemma, Indexes, vector_blob
from .models import SearchQuery
from .retrieval_benchmark import environment, latency, project_fixture, quality
from .retrieval_workload import (
    blind_pack, build_workload, checksum, fusion_trace, load_workload, save_workload,
)
from .storage import Store, canonical, searchable

MODES = ("lexical", "semantic", "hybrid")
KINDS = ("artifact", "board_post", "memory")


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as file:
        json.dump(value, file, indent=2, ensure_ascii=False, allow_nan=False)
        file.write("\n")


def balanced_queries(dataset):
    groups = [[q for q in dataset["queries"] if q["domain"] == domain] for domain in ("code", "board", "memory")]
    return [query for row in itertools.zip_longest(*groups) for query in row if query]


def _body(doc, evidence, project="benchmark"):
    body = {"id": doc["id"], "project_id": project,
            "content": {"media_type": "text/plain", "text": doc["text"]}}
    if doc["kind"] != "board_post":
        body["title"] = doc["title"]
    else:
        body.update(board_id=project + "-board", kind="finding")
    if doc["kind"] == "memory":
        body.update(status="verified", category="semantic", scope_kind="project", scope_owner_id=project,
                    evidence_refs=[{"id": evidence, "kind": "board_post", "revision": 1}])
    return body


def _encoded_batches(encoder, documents, batch_size):
    """Same token windows/title template as Gemma.documents; offline batched preparation.

    This bypasses production job scheduling, not the embedding model. Query timing
    always uses the unchanged Gemma.query path. Never an indexing-throughput claim.
    """
    if not isinstance(encoder, Gemma):
        for doc, body in documents:
            for chunk, blob in encoder.documents(body.get("title", ""), searchable(body)):
                yield doc["id"], chunk, blob
        return
    pending = []
    for doc, body in documents:
        text = searchable(body)
        tokens = encoder.model.tokenizer.encode(text, add_special_tokens=False)
        for start in range(0, max(1, len(tokens)), 896):
            chunk = encoder.model.tokenizer.decode(tokens[start:start + 1024])
            pending.append((doc["id"], chunk, f"title: {body.get('title') or 'none'} | text: {chunk}"))
            if len(pending) == batch_size:
                with encoder.lock:
                    vectors = encoder.model.encode([item[2] for item in pending], prompt="",
                                                   normalize_embeddings=True, truncate_dim=768,
                                                   batch_size=batch_size, show_progress_bar=False)
                for item, vector in zip(pending, vectors, strict=True):
                    yield item[0], item[1], vector_blob(vector)
                pending = []
    if pending:
        with encoder.lock:
            vectors = encoder.model.encode([item[2] for item in pending], prompt="", normalize_embeddings=True,
                                           truncate_dim=768, batch_size=batch_size, show_progress_bar=False)
        for item, vector in zip(pending, vectors, strict=True):
            yield item[0], item[1], vector_blob(vector)


def prepare(dataset, work, encoder, *, batch_size=8):
    if type(batch_size) is not int or not 1 <= batch_size <= 32:
        raise ValueError("Batch size must be 1..32")
    work = Path(work)
    work.mkdir(parents=True, exist_ok=False)
    state = {"state": "preparing", "dataset_sha256": checksum(dataset)}
    write_json(work / "state.json", state)
    store = Store(work / "data")
    started = time.perf_counter()
    try:
        principal = project_fixture(store)
        evidence = next(doc["id"] for doc in dataset["documents"] if doc["kind"] == "board_post")
        bodies = [(doc, _body(doc, evidence)) for doc in dataset["documents"]]
        forbidden, clones = [], {}
        with store.transaction():
            for doc, body in sorted(bodies, key=lambda item: item[0]["kind"] == "memory"):
                store.save(doc["kind"], "benchmark", "reader", body)
            for kind in KINDS:
                for doc in [doc for doc in dataset["documents"] if doc["kind"] == kind][:3]:
                    for prefix, project, private in (("private-", "benchmark", "different-agent"),
                                                     ("cross-", "other", None)):
                        identity = prefix + doc["id"]
                        body = {**_body(doc, evidence, project), "id": identity}
                        store.save(kind, project, "reader", body, private=private)
                        forbidden.append(identity)
                        clones.setdefault(doc["id"], []).append((identity, project))
                    if kind == "memory":
                        for prefix, updates in (("expired-", {"expires_at": "2000-01-01T00:00:00Z"}),
                                                ("disputed-", {"status": "disputed"})):
                            identity = prefix + doc["id"]
                            store.save(kind, "benchmark", "reader",
                                       {**_body(doc, evidence), "id": identity, **updates})
                            forbidden.append(identity)
                            clones.setdefault(doc["id"], []).append((identity, "benchmark"))
        encoded, chunk_numbers, public_chunks = 0, Counter(), 0
        reference, checked, max_component_error = {}, 0, 0.
        if isinstance(encoder, Gemma):
            for kind in KINDS:
                doc, body = next(item for item in bodies if item[0]["kind"] == kind)
                reference.update({(doc["id"], number): blob for number, (_, blob) in enumerate(
                    encoder.documents(body.get("title", ""), searchable(body)))})
        with store.transaction():
            for identity, chunk, blob in _encoded_batches(encoder, bodies, batch_size):
                number = chunk_numbers[identity]
                chunk_numbers[identity] += 1
                if (identity, number) in reference:
                    expected = struct.unpack("<768f", reference.pop((identity, number)))
                    actual = struct.unpack("<768f", blob)
                    similarity = sum(a * b for a, b in zip(actual, expected, strict=True))
                    if similarity < .9999:
                        raise RuntimeError("Batch encoding diverges from production document protocol")
                    max_component_error = max(max_component_error, max(
                        abs(a - b) for a, b in zip(actual, expected, strict=True)))
                    checked += 1
                for resource, project in [(identity, "benchmark"), *clones.get(identity, [])]:
                    rowid = store.db.execute(
                        "INSERT INTO vector_chunks(resource,revision,project,chunk,text) VALUES(?,1,?,?,?)",
                        (resource, project, number, chunk)).lastrowid
                    store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                                     (rowid, blob, project))
                public_chunks += 1
                if number == 0:
                    encoded += 1
                    if encoded % 500 == 0:
                        print(canonical({"phase": "encoding", "public_documents_started": encoded}), flush=True)
            if encoded != len(dataset["documents"]):
                raise RuntimeError("Document embedding coverage incomplete")
            if reference:
                raise RuntimeError("Batch reference coverage incomplete")
            store.db.execute("UPDATE jobs SET state='done' WHERE kind='embedding'")
        store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        state.update(state="ready", principal=principal, forbidden=forbidden, resources=encoded,
                     public_vector_chunks=public_chunks, preparation_ms=(time.perf_counter() - started) * 1000,
                     kind_counts=dict(Counter(doc["kind"] for doc in dataset["documents"])),
                     origin_counts=dict(Counter(doc["origin"] for doc in dataset["documents"])),
                     encoder="pinned_real_model" if isinstance(encoder, Gemma) else "synthetic_test_only",
                     model=MODEL_ID if isinstance(encoder, Gemma) else None,
                     model_revision=MODEL_REVISION if isinstance(encoder, Gemma) else None,
                     batch_size=batch_size,
                     batch_reference_chunks_checked=checked, batch_max_component_error=max_component_error,
                     method="Batched production token-window/title protocol, bulk vector seeding; graph jobs not run. "
                            "All public records encoded, forbidden identical-text clones reuse their public vector.")
        temporary = work / "state-ready.json"
        write_json(temporary, state)
        os.replace(temporary, work / "state.json")
        return state
    finally:
        store.close()


class TimedEncoder:
    def __init__(self, base):
        self.base, self.local = base, threading.local()

    def query(self, text):
        started = time.perf_counter()
        try:
            return self.base.query(text)
        finally:
            self.local.ms = (time.perf_counter() - started) * 1000


def _request(indexes, principal, query, mode, size, forbidden, timer):
    timer.local.ms = 0.
    started = time.perf_counter()
    result = indexes.search(principal, SearchQuery(project_id="benchmark", query=query["text"],
                            resource_kinds=list(KINDS), mode=mode, limit=10), scan_limit=size + 1)
    elapsed = (time.perf_counter() - started) * 1000
    identities = [row["resource_id"] for row in result["results"]]
    scan = result["scan"]
    if set(identities) & forbidden or len(identities) != len(set(identities)):
        raise RuntimeError("Invalid or forbidden retrieval result")
    partial = (result["lexical_fallback"] or any(scan["scan_truncated"].values())
               or scan["time_budget_exceeded"] or sum(scan["authorized_candidates"].values()) != size)
    return {"query_id": query["id"], "domain": query["domain"], "mode": mode, "ms": elapsed,
            "encoder_ms_including_lock_wait": timer.local.ms, "ids": identities,
            "partial": partial, "scan": scan}


def _aggregate(rows, wall_seconds):
    return {"requests": len(rows), "wall_seconds": wall_seconds,
            "throughput_rps": len(rows) / wall_seconds if wall_seconds else 0,
            "partial_requests": sum(row["partial"] for row in rows),
            "modes": {mode: {"latency": latency([row["ms"] for row in rows if row["mode"] == mode]),
                            "encoder_including_lock_wait": latency([
                                row["encoder_ms_including_lock_wait"] for row in rows if row["mode"] == mode])}
                      for mode in MODES if any(row["mode"] == mode for row in rows)},
            "samples": rows}


def concurrent_load(indexes, principal, queries, size, forbidden, timer, *, workers, seconds):
    if type(workers) is not int or not 1 <= workers <= 8 or not .01 <= seconds <= 600:
        raise ValueError("Load bounds exceeded")
    barrier = threading.Barrier(workers + 1)
    started = time.monotonic()
    stop = [None]
    def worker(number):
        barrier.wait(timeout=30)
        rows, errors = [], []
        for turn in range(2000):
            if time.monotonic() >= stop[0]:
                break
            try:
                rows.append(_request(indexes, principal, queries[(turn * workers + number) % len(queries)],
                                     MODES[(turn + number) % 3], size, forbidden, timer))
            except Exception as error:
                errors.append(type(error).__name__)
                break
        return rows, errors
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, number) for number in range(workers)]
        started = time.monotonic()
        stop[0] = started + seconds
        barrier.wait(timeout=30)
        outcomes = [future.result() for future in futures]
    elapsed = time.monotonic() - started
    rows = [row for outcome in outcomes for row in outcome[0]]
    errors = [error for outcome in outcomes for error in outcome[1]]
    return {**_aggregate(rows, elapsed), "workers": workers, "requested_seconds": seconds,
            "started_requests": len(rows) + len(errors), "failed_requests": len(errors),
            "errors": errors, "method": "Closed-loop clients, no independent arrival rate; latency includes shared "
            "encoder and SQLite lock waits, wall time includes in-flight drain",
            "duration_reached": elapsed >= seconds}


def measure(dataset, work, encoder, *, seconds=60., workers=(1, 4), cold_samples=3):
    if type(cold_samples) is not int or not 1 <= cold_samples <= 12:
        raise ValueError("Cold sample bounds exceeded")
    work = Path(work)
    state = json.loads((work / "state.json").read_text())
    if state["state"] != "ready" or state["dataset_sha256"] != checksum(dataset):
        raise ValueError("Fixture is incomplete or mismatched")
    if ((state["encoder"] == "pinned_real_model") != isinstance(encoder, Gemma)
            or state["encoder"] == "pinned_real_model" and (
                state["model"] != MODEL_ID or state["model_revision"] != MODEL_REVISION)):
        raise ValueError("Prepared/query encoder identity mismatch")
    queries, timer = balanced_queries(dataset), TimedEncoder(encoder)
    principal, forbidden = state["principal"], set(state["forbidden"])
    size = len(dataset["documents"])
    cold = []
    cold_started = time.perf_counter()
    for mode in MODES:
        for query in queries[:cold_samples]:
            started = time.perf_counter()
            store, indexes = Store(work / "data"), None
            try:
                indexes = Indexes(store, timer)
                setup_ms = (time.perf_counter() - started) * 1000
                row = _request(indexes, principal, query, mode, size, forbidden, timer)
                row["connection_setup_ms"] = setup_ms
                cold.append(row)
            finally:
                if indexes:
                    indexes.close()
                store.close()
    cold_wall = time.perf_counter() - cold_started
    store, indexes = Store(work / "data"), None
    try:
        indexes = Indexes(store, timer)
        rankings, traces, warm = {}, {}, []
        captured = {}
        original_lexical, original_semantic = indexes._lexical_ranking, indexes._semantic_ranking
        def capture_lexical(*args, **kwargs):
            result = original_lexical(*args, **kwargs)
            captured["lexical"] = list(result[0])
            return result
        def capture_semantic(*args, **kwargs):
            result = original_semantic(*args, **kwargs)
            captured["semantic"] = list(result[0])
            return result
        for mode in MODES:
            _request(indexes, principal, queries[0], mode, size, forbidden, timer)
        indexes._lexical_ranking, indexes._semantic_ranking = capture_lexical, capture_semantic
        warm_started = time.perf_counter()
        for query in queries:
            rankings[query["id"]] = {}
            for mode in MODES:
                row = _request(indexes, principal, query, mode, size, forbidden, timer)
                warm.append(row)
                rankings[query["id"]][mode] = row["ids"]
            traces[query["id"]] = fusion_trace(captured["lexical"], captured["semantic"],
                                              rankings[query["id"]]["hybrid"], query["relevance"])
        indexes._lexical_ranking, indexes._semantic_ranking = original_lexical, original_semantic
        warm_wall = time.perf_counter() - warm_started
        loads = [concurrent_load(indexes, principal, queries, size, forbidden, timer,
                                workers=count, seconds=seconds) for count in workers]
        pack, key = blind_pack(dataset, rankings)
        provisional = {}
        for mode in MODES:
            groups = {"overall": queries, **{domain: [query for query in queries if query["domain"] == domain]
                                             for domain in ("code", "board", "memory")}}
            provisional[mode] = {domain: {metric: statistics.mean(
                quality(rankings[query["id"]][mode], query["relevance"])[metric] for query in group)
                for metric in ("recall_at_10", "mrr_at_10", "ndcg_at_10")}
                for domain, group in groups.items() if group}
        all_rows = cold + warm + [row for load in loads for row in load["samples"]]
        return {"state": "completed_with_issues" if any(row["partial"] for row in all_rows)
                or any(load["errors"] or not load["duration_reached"] for load in loads) else "completed",
                "fixture": {key: value for key, value in state.items() if key not in ("principal", "forbidden")},
                "fresh_connection": {**_aggregate(cold, cold_wall),
                                     "cache_scope": "Fresh SQLite connections only; canonical startup reads and "
                                     "OS file cache remain. Not OS-cold or new-process/model-start latency."},
                "warm": _aggregate(warm, warm_wall), "loads": loads,
                "provisional_known_anchor_metrics": provisional,
                "judgment_scope": "Assistant-authored anchor positives only. Capacity distractors unjudged; "
                                  "not independent relevance or exhaustive recall.",
                "fusion_diagnostics": traces, "ranking_policy_changed": False,
                "independent_review": "pending", "os_cold_cache": "not_established",
                "subsecond_warm_p95": {mode: latency([row["ms"] for row in warm if row["mode"] == mode])["p95_ms"] < 1000
                                       for mode in MODES}}, pack, key
    finally:
        if indexes:
            indexes.close()
        store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "prepare", "measure"))
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--size", type=int, default=10000)
    parser.add_argument("--work", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 4])
    args = parser.parse_args()
    if args.action == "build":
        if args.root is None:
            parser.error("--root required")
        dataset = build_workload(args.root, args.size)
        save_workload(args.dataset, dataset)
        print(canonical({"dataset_sha256": checksum(dataset), "documents": len(dataset["documents"]),
                         "queries": len(dataset["queries"]), "origins": dict(Counter(
                             doc["origin"] for doc in dataset["documents"]))}))
        return 0
    if args.work is None or args.output is None:
        parser.error("--work and --output required")
    if args.output.exists():
        parser.error("Output already exists")
    dataset = load_workload(args.dataset)
    report = {"environment": environment(), "dataset_sha256": checksum(dataset), "model": MODEL_ID,
              "model_revision": MODEL_REVISION, "status": "running", "independent_review": "pending",
              "source_sha256": {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                                for name in ("indexes.py", "storage.py", "retrieval_load.py", "retrieval_workload.py")}}
    started = time.perf_counter()
    try:
        encoder = Gemma()
        report.update(model_load_ms=(time.perf_counter() - started) * 1000,
                      device=str(encoder.model.device), dtype=str(next(encoder.model.parameters()).dtype))
        if args.action == "prepare":
            report["preparation"] = prepare(dataset, args.work, encoder, batch_size=args.batch_size)
        else:
            result, pack, key = measure(dataset, args.work, encoder, seconds=args.seconds, workers=args.workers)
            report["measurement"] = result
            write_json(args.output.with_suffix(".blind-review.json"), pack)
            write_json(args.output.with_suffix(".review-key.json"), key)
        report["status"] = "passed" if args.action == "prepare" or result["state"] == "completed" else "failed"
    except Exception as error:
        report.update(status="failed", failure={"type": type(error).__name__})
    report["elapsed_ms"] = (time.perf_counter() - started) * 1000
    write_json(args.output, report)
    print(canonical({"report": str(args.output), "status": report["status"]}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
