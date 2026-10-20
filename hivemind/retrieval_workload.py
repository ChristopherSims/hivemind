"""Permitted-source mixed retrieval fixtures and rank-blind human review packs."""
import ast
import gzip
import hashlib
import json
from pathlib import Path

from .storage import canonical

DOCS = ("docs/operations.md", "docs/scalable-retrieval.md", "docs/memory-pipeline.md",
        "docs/task-verification.md", "docs/worker-provisioning.md", "docs/team-board-access.md")


def checksum(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _read_source(root, relative):
    path = root / relative
    if any(p.is_symlink() or getattr(p.lstat(), "st_file_attributes", 0) & 0x400
           for p in (path, *path.parents) if p.is_relative_to(root)):
        raise ValueError("Source links are not permitted")
    with path.open("rb") as file:
        raw = file.read(1048577)
    if len(raw) > 1048576:
        raise ValueError("Source exceeds bound")
    return raw.decode("utf-8"), hashlib.sha256(raw).hexdigest()


def _functions(nodes, prefix=""):
    for node in nodes:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield prefix + node.name, node
            yield from _functions(node.body, prefix + node.name + ".")
        elif isinstance(node, ast.ClassDef):
            yield from _functions(node.body, prefix + node.name + ".")


def build_workload(root, size=10000):
    if type(size) is not int or not 100 <= size <= 20000:
        raise ValueError("Workload size must be 100..20000")
    root = Path(root).resolve(strict=True)
    frozen = json.loads((root / "benchmarks/retrieval-repository-v1.json").read_text(encoding="utf-8"))
    synthetic = json.loads((root / "benchmarks/retrieval-v1.json").read_text(encoding="utf-8"))
    documents = [{**doc, "kind": "artifact", "title": doc["text"].splitlines()[0][:512],
                  "origin": "frozen_repository_excerpt"} for doc in frozen["documents"]]
    frozen_keys = {(doc["source"]["path"], doc["source"]["symbol"]) for doc in documents}
    for number, doc in enumerate(synthetic["documents"]):
        documents.append({**doc, "kind": "board_post" if number // 3 % 2 == 0 else "memory",
                          "title": "Synthetic fixture " + doc["id"], "origin": "synthetic_regression"})
    domains = {doc["id"]: ("memory" if doc["kind"] == "memory" else "board")
               for doc in documents if doc["origin"] == "synthetic_regression"}
    queries = [{**query, "domain": "code"} for query in frozen["queries"]]
    queries += [{**query, "domain": domains[next(iter(query["relevance"]))]} for query in synthetic["queries"]]
    pins, extra = {}, []
    paths = sorted((root / "hivemind").glob("*.py"))
    for path in paths:
        relative = path.relative_to(root).as_posix()
        source, file_hash = _read_source(root, relative)
        pins[relative] = file_hash
        lines = source.splitlines()
        for symbol, node in _functions(ast.parse(source).body):
            if (relative, symbol) in frozen_keys or node.end_lineno - node.lineno < 3:
                continue
            text = "\n".join(lines[node.lineno - 1:min(node.end_lineno, node.lineno + 23)])
            key = f"{relative}::{symbol}"
            extra.append({"id": "code-" + hashlib.sha256(key.encode()).hexdigest()[:24],
                          "kind": "artifact", "title": key[:512], "text": key + "\n" + text,
                          "origin": "repository_code",
                          "source": {"path": relative, "symbol": symbol, "start_line": node.lineno,
                                     "file_sha256": file_hash, "excerpt_sha256": hashlib.sha256(text.encode()).hexdigest()}})
    for relative in DOCS:
        source, file_hash = _read_source(root, relative)
        pins[relative] = file_hash
        lines = source.splitlines()
        for start in range(0, len(lines), 24):
            text = "\n".join(lines[start:start + 24])
            if not text.strip():
                continue
            extra.append({"id": "doc-" + checksum([relative, start])[:24], "kind": "artifact",
                          "title": f"{relative}:{start + 1}", "text": text, "origin": "repository_docs",
                          "source": {"path": relative, "start_line": start + 1, "file_sha256": file_hash,
                                     "excerpt_sha256": hashlib.sha256(text.encode()).hexdigest()}})
    extra.sort(key=lambda doc: checksum(doc["id"]))
    documents += extra[:max(0, min(len(extra), size - len(documents)))]
    topics = ("database upgrades", "worker cancellation", "memory expiration", "coordinator fencing",
              "backup restoration", "network isolation", "token accounting", "artifact verification",
              "private conversations", "task scheduling", "Git publication", "search pagination")
    actions = ("stage a reversible change", "wait for explicit operator review", "verify the current revision",
               "reconcile existing evidence", "refuse an ambiguous retry", "record a durable receipt")
    while len(documents) < size:
        number = len(documents)
        topic = topics[number % len(topics)]
        action = actions[number // len(topics) % len(actions)]
        documents.append({"id": f"incident-{number:05d}",
                          "kind": "memory" if number % 2 else "board_post",
                          "title": f"Synthetic case {number}: {topic}", "origin": "synthetic_capacity",
                          "text": f"Synthetic incident {number}, component svc-{number % 97}, workspace ws-{number % 43}. "
                                  f"The review concerns {topic}. The proposed response is to {action}. "
                                  f"Evidence revision {number % 31 + 1} belongs to task ticket-{number}. "
                                  "This is generated capacity prose, not a historical project decision or verified advice."})
    ids = [doc["id"] for doc in documents]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate workload identities")
    return {"version": "mixed-retrieval-v1", "documents": documents, "queries": queries,
            "source_pins": pins, "independent_judgments": "pending",
            "label_policy": "Existing assistant-authored anchor labels, provisional only; capacity distractors are unjudged.",
            "scope": "Repository code/docs plus synthetic board/memory; no production stores. Frozen excerpts may predate current source."}


def save_workload(path, dataset):
    with Path(path).open("xb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as file:
            file.write(canonical(dataset).encode())


def load_workload(path):
    with gzip.open(path, "rb") as file:
        raw = file.read(64 * 1024 * 1024 + 1)
    if len(raw) > 64 * 1024 * 1024:
        raise ValueError("Workload exceeds bound")
    data = json.loads(raw)
    identities = [doc["id"] for doc in data["documents"]]
    queries = [query["id"] for query in data["queries"]]
    if (not identities or len(identities) > 20000 or len(identities) != len(set(identities))
            or not queries or len(queries) != len(set(queries))
            or any(doc["kind"] not in ("artifact", "board_post", "memory") or not doc["text"].strip()
                   for doc in data["documents"])
            or any(query["domain"] not in ("code", "board", "memory") or not query["text"].strip()
                   or not set(query["relevance"]) <= set(identities) for query in data["queries"])):
        raise ValueError("Invalid workload identities")
    return data


def fusion_trace(lexical, semantic, hybrid, relevance):
    lex = {identity: rank + 1 for rank, identity in enumerate(lexical)}
    sem = {identity: rank + 1 for rank, identity in enumerate(semantic)}
    candidates = list(dict.fromkeys([*hybrid[:10], *relevance]))
    return {"known_positive_demotions": sorted(set(semantic[:10]) & set(relevance) - set(hybrid[:10])),
            "candidates": [{"id": identity, "lexical_rank": lex.get(identity), "semantic_rank": sem.get(identity),
                            "lexical_vote": 1 / (60 + lex[identity]) if identity in lex else 0.,
                            "semantic_vote": 1 / (60 + sem[identity]) if identity in sem else 0.}
                           for identity in candidates],
            "policy": "Unchanged production equal-weight RRF, k=60; OR-term FTS votes have no confidence threshold"}


def blind_pack(dataset, rankings):
    docs = {doc["id"]: doc for doc in dataset["documents"]}
    questions, mapping = [], {}
    for query in dataset["queries"]:
        pool = set(query["relevance"])
        for mode in ("lexical", "semantic", "hybrid"):
            pool.update(rankings[query["id"]][mode][:10])
        candidates = []
        for identity in sorted(pool, key=lambda identity: checksum([query["id"], identity])):
            candidate = checksum(["blind", query["id"], identity])[:24]
            mapping[candidate] = {"query_id": query["id"], "document_id": identity}
            doc = docs[identity]
            title = ("Synthetic " + doc["kind"] + " excerpt"
                     if doc["origin"] == "synthetic_regression" else doc["title"])
            candidates.append({"candidate_id": candidate, "title": title, "text": doc["text"],
                               "kind": doc["kind"], "grade": None})
        questions.append({"query_id": query["id"], "text": query["text"], "domain": query["domain"],
                          "candidates": candidates})
    pack = {"version": 1, "dataset_sha256": checksum(dataset), "rankings_sha256": checksum(rankings), "reviewer": None,
            "instructions": "Grade 0 irrelevant, 1 useful background, 2 answers partly, 3 directly answers. "
                            "Judge without results, original labels or review-key. Edit only grades and reviewer; "
                            "do not reorder or change entries. "
                            "Provide reviewer={id: pseudonym, independent: true} only if independently reviewed.",
            "questions": questions}
    key = {"dataset_sha256": checksum(dataset), "mapping": mapping}
    pack["review_key_sha256"] = checksum(key)
    return pack, key


def validate_review(original, reviewed, key):
    """Validate a declared independent review; identity/independence are not verified."""
    reviewer = reviewed.get("reviewer")
    if (not isinstance(reviewer, dict) or set(reviewer) != {"id", "independent"}
            or reviewer.get("independent") is not True or not isinstance(reviewer.get("id"), str)
            or not 1 <= len(reviewer["id"].strip()) <= 128):
        raise ValueError("Independent reviewer declaration required")
    def fixed(pack):
        return {**pack, "reviewer": None, "questions": [
            {**query, "candidates": [{**candidate, "grade": None} for candidate in query["candidates"]]}
            for query in pack["questions"]]}
    if (checksum(fixed(original)) != checksum(fixed(reviewed))
            or key["dataset_sha256"] != original["dataset_sha256"]
            or checksum(key) != original["review_key_sha256"]):
        raise ValueError("Review content or dataset changed")
    judgments = {}
    for query in reviewed["questions"]:
        judgments[query["query_id"]] = {}
        for candidate in query["candidates"]:
            grade = candidate["grade"]
            if type(grade) is not int or not 0 <= grade <= 3:
                raise ValueError("All pooled candidates require a 0..3 judgment")
            entry = key["mapping"][candidate["candidate_id"]]
            if entry["query_id"] != query["query_id"]:
                raise ValueError("Review mapping mismatch")
            judgments[query["query_id"]][entry["document_id"]] = grade
    return {"judgments": judgments, "independence": "reviewer_declared_not_identity_verified",
            "metric_scope": "Pooled judgments only, not exhaustive corpus recall"}


def score_review(original, reviewed, key, rankings):
    """Score only the exact rank-blind pool once a complete independent review arrives."""
    from .retrieval_benchmark import quality
    validated = validate_review(original, reviewed, key)
    if checksum(rankings) != original["rankings_sha256"]:
        raise ValueError("Review rankings changed")
    rows = []
    for query in original["questions"]:
        grades = validated["judgments"][query["query_id"]]
        positives = {identity: grade for identity, grade in grades.items() if grade}
        for mode in ("lexical", "semantic", "hybrid"):
            ids = rankings[query["query_id"]][mode][:10]
            if len(ids) != len(set(ids)) or not set(ids) <= grades.keys():
                raise ValueError("Unjudged or duplicate retrieved candidate")
            metrics = quality(ids, positives) if positives else None
            rows.append({"query_id": query["query_id"], "domain": query["domain"], "mode": mode,
                         "pooled_recall_at_10": metrics["recall_at_10"] if metrics else None,
                         "mrr_at_10": metrics["mrr_at_10"] if metrics else None,
                         "ndcg_at_10": metrics["ndcg_at_10"] if metrics else None,
                         "pool_has_positive": bool(positives)})
    return {"independence": validated["independence"], "metric_scope": validated["metric_scope"], "rows": rows}
