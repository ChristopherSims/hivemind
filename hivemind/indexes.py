import json
import math
import re
import struct
import threading
import time

import ladybug

from .storage import Problem, searchable

MODEL_ID = "google/embeddinggemma-2"
MODEL_REVISION = "914f7f89142e33e77833254d9c9b90c3cef7303b"
PROFILE = "eg2-text-768-v1"
INDEXED = {"agent", "board_post", "memory", "task", "work_claim", "artifact"}


def vector_blob(vector):
    vector = [float(v) for v in vector]
    if len(vector) != 768 or not all(math.isfinite(v) for v in vector):
        raise ValueError("Invalid embedding shape or nonfinite values")
    norm = math.sqrt(sum(v * v for v in vector))
    if not 0.99 <= norm <= 1.01:
        raise ValueError("Embedding must be L2 normalized")
    return struct.pack("<768f", *vector)


class Gemma:
    def __init__(self):
        import torch
        from sentence_transformers import SentenceTransformer

        dtype = torch.bfloat16 if torch.cuda.is_available() and torch.cuda.is_bf16_supported() else torch.float32
        self.model = SentenceTransformer(
            MODEL_ID, revision=MODEL_REVISION, local_files_only=True, trust_remote_code=False,
            config_kwargs={"vision_config": None, "audio_config": None},
            model_kwargs={"torch_dtype": dtype},
        )
        self.lock = threading.Lock()

    def documents(self, title, text):
        tokenizer = self.model.tokenizer
        tokens = tokenizer.encode(text, add_special_tokens=False)
        chunks = [tokenizer.decode(tokens[start:start + 1024]) for start in range(0, max(1, len(tokens)), 896)]
        with self.lock:
            vectors = self.model.encode(
                [f"title: {title or 'none'} | text: {chunk}" for chunk in chunks],
                normalize_embeddings=True, truncate_dim=768, prompt="",
            )
        return list(zip(chunks, (vector_blob(v) for v in vectors), strict=True))

    def query(self, text):
        with self.lock:
            result = self.model.encode(text, prompt_name="SearchQuery", normalize_embeddings=True, truncate_dim=768)
        return vector_blob(result)


class Indexes:
    def __init__(self, store, encoder=None):
        self.store = store
        self.encoder = encoder
        self.lock = threading.RLock()
        self.graph_db = None
        self.graph = None
        try:
            self.graph_db = ladybug.Database(str(store.directory / "graph"), buffer_pool_size=64 * 1024 * 1024)
            self.graph = ladybug.Connection(self.graph_db)
            self.graph.execute("CREATE NODE TABLE IF NOT EXISTS Entity(id STRING, project STRING, PRIMARY KEY(id))")
            self.graph.execute("CREATE REL TABLE IF NOT EXISTS Link(FROM Entity TO Entity, kind STRING)")
        except Exception:
            if self.graph:
                self.graph.close()
            if self.graph_db:
                self.graph_db.close()
            self.graph = self.graph_db = None

    def close(self):
        if self.graph:
            self.graph.close()
        if self.graph_db:
            self.graph_db.close()

    def process_one(self, kind):
        with self.store.lock, self.lock:
            with self.store.lock:
                if kind == "embedding" and not self.encoder:
                    # Cleanup/obsolete jobs must not sit behind a model-dependent backlog.
                    job = self.store.db.execute(
                        """SELECT j.* FROM jobs j LEFT JOIN resources r ON r.id=j.resource
                        WHERE j.kind='embedding' AND j.state='pending' AND (
                          r.id IS NULL OR r.deleted=1 OR r.revision!=j.revision OR
                          r.kind NOT IN ('agent','board_post','memory','task','work_claim','artifact') OR
                          (r.kind='memory' AND json_extract(r.body,'$.status')!='verified') OR
                          (r.kind='work_claim' AND (
                            json_extract(r.body,'$.state') NOT IN ('active','reserved') OR
                            json_extract(r.body,'$.expires_at')<=?)))
                        ORDER BY j.seq,j.id LIMIT 1""", (time.time(),)).fetchone()
                else:
                    job = self.store.db.execute(
                        "SELECT * FROM jobs WHERE kind=? AND state='pending' ORDER BY seq,id LIMIT 1", (kind,)
                    ).fetchone()
                if not job:
                    return False
                job = dict(job)
                row = self.store.db.execute("SELECT * FROM resources WHERE id=?", (job["resource"],)).fetchone()
                body = json.loads(row["body"]) if row and not row["deleted"] else None
                stale = not row or row["revision"] != job["revision"]
            try:
                if stale:
                    vectors = []
                elif kind == "embedding":
                    eligible = (body and row["kind"] in INDEXED and
                                (row["kind"] != "memory" or body["status"] == "verified") and
                                (row["kind"] != "work_claim" or body["state"] in ("active", "reserved")
                                 and body["expires_at"] > time.time()))
                    if eligible and not self.encoder:
                        return False
                    vectors = (self.encoder.documents(body.get("title", body.get("display_name", "")), searchable(body))
                               if eligible else [])
                else:
                    vectors = []
                with self.store.transaction():
                    current = self.store.db.execute("SELECT * FROM resources WHERE id=?", (job["resource"],)).fetchone()
                    obsolete = not current or current["revision"] != job["revision"]
                    if not obsolete:
                        if kind == "embedding":
                            old_chunks = self.store.db.execute(
                                "SELECT id FROM vector_chunks WHERE resource=?", (job["resource"],)).fetchall()
                            for chunk in old_chunks:
                                self.store.db.execute("DELETE FROM vectors WHERE chunk_id=?", (chunk[0],))
                            self.store.db.execute("DELETE FROM vector_chunks WHERE resource=?", (job["resource"],))
                            for i, (text, blob) in enumerate(vectors):
                                inserted = self.store.db.execute(
                                    """INSERT INTO vector_chunks(resource,revision,project,chunk,text)
                                    VALUES(?,?,?,?,?)""", (job["resource"], job["revision"], job["project"], i, text))
                                self.store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                                                      (inserted.lastrowid, blob, job["project"]))
                        else:
                            self.project_graph(job, current)
                    self.store.db.execute("UPDATE jobs SET state='done',error=NULL WHERE id=?", (job["id"],))
            except Exception:
                with self.store.transaction():
                    self.store.db.execute(
                        """UPDATE jobs SET attempts=attempts+1,error='index_processing_failed',
                        state=CASE WHEN attempts>=4 THEN 'dead' ELSE 'pending' END WHERE id=?""", (job["id"],))
                return False
            return True

    def drain(self, limit=10000):
        for kind in ("graph", "embedding"):
            for _ in range(limit):
                if not self.process_one(kind):
                    break

    def project_graph(self, job, row):
        if not self.graph:
            raise RuntimeError("Graph projection unavailable")
        entity = {"id": job["resource"], "project": job["project"]}
        body = json.loads(row["body"])
        if row["deleted"] or body.get("invalidation_reason"):
            self.graph.execute("MATCH (n:Entity {id: $id}) DETACH DELETE n", {"id": job["resource"]})
            return
        self.graph.execute("MERGE (n:Entity {id: $id}) SET n.project=$project", entity)
        # Replace outgoing derived edges; retries after graph/SQLite boundary are safe.
        self.graph.execute("MATCH (n:Entity {id: $id})-[r:Link]->() DELETE r", {"id": job["resource"]})
        edges = self.store.db.execute("SELECT * FROM relations WHERE project=? AND source=?",
                                      (job["project"], job["resource"])).fetchall()
        for edge in edges:
            target = self.store.db.execute("SELECT deleted FROM resources WHERE id=?", (edge["target"],)).fetchone()
            if not target or target["deleted"]:
                continue
            self.graph.execute("MERGE (n:Entity {id: $id}) SET n.project=$project",
                               {"id": edge["target"], "project": job["project"]})
            self.graph.execute(
                """MATCH (a:Entity {id: $source}), (b:Entity {id: $target})
                MERGE (a)-[r:Link {kind: $kind}]->(b)""",
                {"source": edge["source"], "target": edge["target"], "kind": edge["kind"]})

    def search(self, principal, request):
        project = request.project_id
        freshness = self.store.freshness(project)
        if request.min_ingestion_seq is not None and freshness["vector_through_ingestion_seq"] < request.min_ingestion_seq:
            raise Problem("index_not_ready", 503)
        warnings, lexical, semantic = [], [], []
        allowed = {}
        # Pre-authorize candidates before KNN. No global top-k filtering.
        for kind in request.resource_kinds or list(INDEXED):
            for body in self.store.list(kind, project, principal, 1000):
                if kind == "memory" and body["status"] != "verified":
                    continue
                if kind == "work_claim" and (body["state"] not in ("active", "reserved")
                                             or body["expires_at"] <= time.time()):
                    continue
                allowed[body["id"]] = (kind, body)
        if len(allowed) >= 1000:
            warnings.append("Search candidate set is bounded; use narrower resource filters")
        words = re.findall(r"\w+", request.query, re.UNICODE)[:32]
        if words:
            match = " OR ".join('"' + w + '"' for w in words)
            hits = self.store.db.execute(
                "SELECT id FROM lexical WHERE lexical MATCH ? AND project=? ORDER BY bm25(lexical)",
                (match, project)).fetchall()
            lexical = [x[0] for x in hits if x[0] in allowed]
        if request.mode != "lexical":
            if self.encoder:
                query = self.encoder.query(request.query)
                # sqlite-vec scalar distances over the bounded authorized-ID set.
                ids = list(allowed)
                ranked = []
                for start in range(0, len(ids), 300):
                    batch = ids[start:start + 300]
                    placeholders = ",".join("?" for _ in batch)
                    rows = self.store.db.execute(
                        f"""SELECT c.resource,c.revision,vec_distance_cosine(v.embedding,?) distance
                        FROM vector_chunks c JOIN vectors v ON v.chunk_id=c.id
                        WHERE c.project=? AND c.resource IN ({placeholders}) ORDER BY distance""",
                        [query, project, *batch]).fetchall()
                    ranked.extend((float(r["distance"]), r["resource"]) for r in rows
                                  if r["revision"] == allowed[r["resource"]][1]["revision"])
                semantic = list(dict.fromkeys(resource for _, resource in sorted(ranked)))
            else:
                if request.mode == "semantic":
                    raise Problem("index_not_ready", 503)
                warnings.append("Embedding model unavailable; lexical fallback")
        if freshness["vector_pending_jobs"]:
            warnings.append("Semantic index is behind canonical state")
        rankings = [lexical] if request.mode == "lexical" else (
            [semantic] if request.mode == "semantic" else [lexical, semantic])
        scores = {}
        for ranking in rankings:
            for rank, resource in enumerate(ranking):
                scores[resource] = scores.get(resource, 0) + 1 / (60 + rank + 1)
        results = []
        for resource in sorted(scores, key=lambda x: (-scores[x], x))[:request.limit]:
            kind, body = allowed[resource]
            results.append({"resource_id": resource, "kind": kind, "revision": body["revision"],
                            "score": scores[resource], "trust": body.get("origin", body.get("trust", "structured")),
                            "resource": body})
        return {"results": results, "freshness": freshness, "embedding_profile": PROFILE,
                "lexical_fallback": request.mode == "hybrid" and not self.encoder, "warnings": warnings}

    def query_graph(self, principal, request):
        nodes, edges, frontier = {}, {}, set(request.seed_ids)
        graph_fallback = self.graph is None
        for seed in frontier:
            body = self.store.get(seed, principal)
            if body["project_id"] != request.project_id:
                raise Problem("not_found", 404)
            nodes[seed] = body
        if len(nodes) > request.max_nodes:
            raise Problem("graph_limit", 422)
        with self.lock:
            for _ in range(request.max_hops):
                next_frontier = set()
                for seed in sorted(frontier):
                    neighbors = []
                    if self.graph:
                        try:
                            result = self.graph.execute(
                                """MATCH (a:Entity)-[r:Link]->(b:Entity)
                                WHERE a.project=$project AND b.project=$project AND (a.id=$seed OR b.id=$seed)
                                RETURN a.id,b.id,r.kind LIMIT 200""",
                                {"project": request.project_id, "seed": seed})
                            while result.has_next():
                                neighbors.append(result.get_next())
                        except Exception:
                            graph_fallback = True
                    if graph_fallback:
                        neighbors = self.store.db.execute(
                            "SELECT source,target,kind FROM relations WHERE project=? AND (source=? OR target=?) LIMIT 200",
                            (request.project_id, seed, seed)).fetchall()
                    for source, target, kind in neighbors:
                        # Recheck both endpoints and canonical relation. Private intermediates never traversed.
                        try:
                            a = self.store.get(source, principal)
                            b = self.store.get(target, principal)
                        except Problem:
                            continue
                        if a["project_id"] != request.project_id or b["project_id"] != request.project_id:
                            continue
                        exists = self.store.db.execute(
                            "SELECT 1 FROM relations WHERE project=? AND source=? AND target=? AND kind=?",
                            (request.project_id, source, target, kind)).fetchone()
                        if not exists:
                            continue
                        if len(edges) >= request.max_edges or len(set(nodes) | {source, target}) > request.max_nodes:
                            continue
                        edges[(source, target, kind)] = {"source": source, "target": target, "kind": kind}
                        for resource, body in ((source, a), (target, b)):
                            if resource not in nodes:
                                next_frontier.add(resource)
                                nodes[resource] = body
                frontier = next_frontier
        return {"nodes": list(nodes.values()), "edges": list(edges.values()),
                "freshness": self.store.freshness(request.project_id), "graph_fallback": graph_fallback,
                "warnings": ["LadybugDB unavailable; canonical relationship fallback"] if graph_fallback else []}
