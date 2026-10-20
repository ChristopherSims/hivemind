import base64
import hashlib
import hmac
import json
import math
import re
import secrets
import sqlite3
import struct
import threading
import time

import ladybug
import sqlite_vec

from .storage import Problem, canonical, searchable

MODEL_ID = "google/embeddinggemma-2"
MODEL_REVISION = "914f7f89142e33e77833254d9c9b90c3cef7303b"
PROFILE = "eg2-text-768-v1"
INDEXED = {"agent", "board_post", "memory", "task", "work_claim", "artifact"}

# Candidate scans page through canonical SQL (keyset on ``seq``) instead of
# ``Store.list``'s hard first-1000 cap. Bounds keep memory/time bounded; exhaustion is
# always flagged so truncation is never silent.
SEARCH_SCAN_DEFAULT = 20000
SEARCH_SCAN_MAX = 200000
SCAN_PAGE = 512
# Ranking depth bounds how many ranked entries are held (bounded top-k window). Only
# emitted results are ever materialized; deeper history is flagged, never silently cut.
RANK_DEPTH_DEFAULT = 4096
RANK_DEPTH_MAX = 65536
SCAN_TIME_BUDGET_MS = 20000
SQLITE_READ_CACHE_KIB = 32 * 1024
CURSOR_VERSION = 2


def search_scan_bound(value):
    if value is None:
        return SEARCH_SCAN_DEFAULT
    return max(1, min(int(value), SEARCH_SCAN_MAX))


def rank_depth_bound(value):
    if value is None:
        return RANK_DEPTH_DEFAULT
    return max(1, min(int(value), RANK_DEPTH_MAX))


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
    def __init__(self, store, encoder=None, *, graph_buffer_pool_size=64 * 1024 * 1024):
        if (type(graph_buffer_pool_size) is not int
                or not 16 * 1024 * 1024 <= graph_buffer_pool_size <= 4 * 1024 * 1024 * 1024):
            raise ValueError("Graph buffer pool must be 16 MiB..4 GiB")
        self.store = store
        self.encoder = encoder
        self.lock = threading.RLock()
        # Process-local cursor signing key; cursors are only meaningful to this instance
        # and fail closed (cursor_invalid) after a restart.
        self._cursor_secret = secrets.token_bytes(32)
        # Composite keyset index for bounded streaming scans (additive; canonical schema
        # in storage.py is untouched).
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS resources_scan "
            "ON resources(project,kind,deleted,seq,id)")
        # Dedicated read-only connection for heavy retrieval queries (FTS5/vec scans).
        # WAL allows concurrent readers, so ranking never contends with canonical
        # writers, and its progress handler bounds *query* time without ever touching
        # the global canonical connection's handler.
        self._ro_lock = threading.Lock()
        self.ro_db = None
        try:
            import sqlite3
            path = store.directory / "canonical.sqlite"
            ro = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
            ro.row_factory = sqlite3.Row
            ro.execute("PRAGMA busy_timeout=5000")
            # vec0 embedding chunks exceed SQLite's default ~2 MiB cache. That
            # default repeatedly evicts/rereads chunk pages during exact scans.
            # This is a per-connection page-cache target, not a result/auth cache.
            ro.execute(f"PRAGMA cache_size=-{SQLITE_READ_CACHE_KIB}")
            ro.enable_load_extension(True)
            sqlite_vec.load(ro)
            ro.enable_load_extension(False)
            self.ro_db = ro
        except Exception:
            self.ro_db = None
        self.graph_db = None
        self.graph = None
        try:
            self.graph_db = ladybug.Database(str(store.directory / "graph"),
                                            buffer_pool_size=graph_buffer_pool_size)
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
        if self.ro_db:
            self.ro_db.close()
        if self.graph:
            self.graph.close()
        if self.graph_db:
            self.graph_db.close()

    def _fail_job(self, job_id):
        with self.store.lock, self.lock:
            with self.store.transaction():
                self.store.db.execute(
                    """UPDATE jobs SET attempts=attempts+1,error='index_processing_failed',
                    state=CASE WHEN attempts>=4 THEN 'dead' ELSE 'pending' END WHERE id=?""",
                    (job_id,))

    def process_one(self, kind):
        """Snapshot -> compute -> validate-and-commit.

        The encoder (a trusted in-process CPU callback that cannot be preempted) runs
        with *no* canonical-store or index locks held, so heartbeats and retrieval are
        never blocked behind it. The commit phase re-validates the job against current
        canonical state inside the transaction, so rows superseded during encoding are
        discarded rather than written.
        """
        # Phase 1: snapshot the job and its canonical row under short locks.
        with self.store.lock, self.lock:
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
        # Phase 2: compute outside every lock.
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
        except Exception:
            self._fail_job(job["id"])
            return False
        # Phase 3: validate the job is still current and commit atomically.
        with self.store.lock, self.lock:
            try:
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
                pass
        # The error mark runs outside the commit attempt; job rows are idempotent.
        # If the commit above failed, record the failure for retry/dead-letter.
        with self.store.lock, self.lock:
            done = self.store.db.execute(
                "SELECT state FROM jobs WHERE id=?", (job["id"],)).fetchone()
            if done and done["state"] == "pending":
                try:
                    self._fail_job(job["id"])
                except Exception:
                    pass
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

    def _visibility_sql(self, kind, principal):
        """Exact SQL rendering of ``Store.visible`` for scanned kinds.

        Pushing visibility into the scan query means row-examination counts and
        truncation flags reflect only rows the principal may see: hidden/private rows
        are never counted, so scan metadata cannot reveal their existence or volume.
        ``Store.visible`` is still applied in Python afterwards as defense in depth.
        """
        if principal["role"] == "admin":
            return "", []
        if kind == "message":
            if principal["role"] == "observer":
                return "", []
            return (" AND (json_extract(body,'$.from')=? OR json_extract(body,'$.to')=?)",
                    [principal.get("agent"), principal.get("agent")])
        return " AND (private_agent IS NULL OR private_agent=?)", [principal.get("agent")]

    def _current_principal(self, principal):
        """Re-fetch the principal row under a short canonical lock. Role/project/agent
        bindings are always read fresh -- a revoked or rebound principal takes effect
        immediately, including mid-pagination."""
        with self.store.lock:
            row = self.store.db.execute("SELECT * FROM principals WHERE id=?",
                                        (principal["id"],)).fetchone()
        if not row:
            raise Problem("forbidden", 403)
        return dict(row)

    def _authorize_request(self, principal, request):
        """Phase A (short canonical lock): current principal, current project access,
        current freshness gate. Nothing expensive runs here."""
        current = self._current_principal(principal)
        if current["role"] != "admin" and current.get("project") != request.project_id:
            raise Problem("not_found", 404)
        with self.store.lock:
            self.store.project(request.project_id)
            freshness = self.store.freshness(request.project_id)
        if request.min_ingestion_seq is not None and freshness["vector_through_ingestion_seq"] < request.min_ingestion_seq:
            raise Problem("index_not_ready", 503)
        return current, freshness

    def _ro_query(self, sql, args, deadline):
        """Run a read-only retrieval query on the dedicated index connection with a
        hard progress-handler deadline (never the canonical connection's handler).
        Returns ``(rows, timed_out)``; an expired deadline aborts the SQLite statement
        and reports partial work instead of blocking."""
        if self.ro_db is None:
            with self.store.lock:
                return self.store.db.execute(sql, args).fetchall(), False

        def handler():
            return 0 if time.monotonic() <= deadline else 1

        with self._ro_lock:
            self.ro_db.set_progress_handler(handler, 1000)
            try:
                return self.ro_db.execute(sql, args).fetchall(), False
            except sqlite3.OperationalError as exc:
                if "interrupted" in str(exc):
                    return [], True
                raise
            finally:
                self.ro_db.set_progress_handler(None, 0)

    def _authorized_meta(self, kinds, project, principal, scan_limit, deadline):
        """Stream authorized candidate *metadata* from canonical SQL, beyond any 1000 cap.

        Runs on the dedicated read-only connection (no canonical or index locks held),
        paging keyset-style over the ``resources_scan(project,kind,deleted,seq,id)``
        index in ``SCAN_PAGE`` chunks with visibility pushed into SQL. Bodies are parsed
        transiently and discarded: the retained map holds only ``id -> (kind,
        revision)``. JSON-local validity state (memory status/state machine, work-claim
        state/expiry) is applied here; evidence-recursive ``valid_memory`` revalidation
        happens at materialization under a short canonical lock. The scan stops at
        ``scan_limit`` rows per kind or at ``deadline`` (monotonic; the SQLite progress
        handler enforces it inside long statements too); both are flagged, never
        silent. Returns ``(meta, authorized_counts, truncated, timed_out)``.
        """
        meta, authorized_counts, truncated = {}, {}, {}
        timed_out = False
        for kind in kinds:
            examined, before, authorized = 0, None, 0
            hit_deadline = False
            vis_sql, vis_args = self._visibility_sql(kind, principal)
            while examined < scan_limit:
                if time.monotonic() >= deadline:
                    timed_out = hit_deadline = True
                    break
                size = min(SCAN_PAGE, scan_limit - examined)
                sql = ("SELECT id,kind,project,private_agent,revision,seq,body "
                       "FROM resources WHERE kind=? AND project=? AND deleted=0" + vis_sql)
                args = [kind, project, *vis_args]
                if before is not None:
                    sql += " AND seq<?"
                    args.append(before)
                sql += " ORDER BY seq DESC LIMIT ?"
                args.append(size)
                rows, aborted = self._ro_query(sql, args, deadline)
                timed_out = timed_out or aborted
                hit_deadline = hit_deadline or aborted
                if not rows:
                    break
                before = rows[-1]["seq"]
                examined += len(rows)
                for row in rows:
                    # TeamAccess traverses parents/policies on the canonical
                    # connection. check_same_thread=False is not permission to
                    # use that connection concurrently with other searches.
                    # Lock only this short check, never encoding/vector ranking.
                    with self.store.lock:
                        visible = self.store.visible(dict(row), principal)
                    if not visible:
                        continue  # defense in depth; the SQL predicate is exact
                    body = json.loads(row["body"])
                    if kind == "memory":
                        if body.get("status") != "verified":
                            continue
                        if self.store.memory_state(body) != "verified":
                            continue  # expired / disputed / invalidated / not-yet-valid
                    if kind == "work_claim" and (body["state"] not in ("active", "reserved")
                                                 or body["expires_at"] <= time.time()):
                        continue
                    if row["id"] not in meta:
                        authorized += 1
                        meta[row["id"]] = (kind, row["revision"])
                if aborted:
                    break
            authorized_counts[kind] = authorized
            truncated[kind] = examined >= scan_limit or hit_deadline
        return meta, authorized_counts, truncated, timed_out

    def _recheck(self, principal, resource_id, kind, revision):
        """Re-read canonical state for one candidate under a short canonical lock, with
        the *current* principal bindings re-fetched first. Derivative index rows are
        only a hint: the result is dropped unless the current canonical row still
        exists, is visible to the current principal, matches the ranked revision, and
        passes full current validity (including evidence-recursive ``valid_memory``).
        Revocation mid-compute therefore yields no private results."""
        with self.store.lock:
            current_row = self.store.db.execute("SELECT * FROM principals WHERE id=?",
                                                (principal["id"],)).fetchone()
            if not current_row:
                return None
            current = dict(current_row)
            try:
                row = self.store.raw(resource_id, kind)
            except Problem:
                return None
            if row["revision"] != revision or not self.store.visible(row, current):
                return None
            body = json.loads(row["body"])
            if kind == "memory":
                try:
                    if not self.store.valid_memory(body, current):
                        return None
                except Problem:
                    return None
                if body["status"] != "verified":
                    return None
            if kind == "work_claim" and (body["state"] not in ("active", "reserved")
                                         or body["expires_at"] <= time.time()):
                return None
            if kind == "task":
                body = self.store.task_view(body, current)
        return body

    def _cursor_fingerprint(self, principal, request, scan_limit, rank_depth):
        """Binds a cursor to the authenticated principal, project, query, and scan policy."""
        return hashlib.sha256(canonical({
            "principal_id": principal.get("id"), "role": principal.get("role"),
            "agent": principal.get("agent"),
            "project_id": request.project_id, "query": request.query, "mode": request.mode,
            "resource_kinds": sorted(request.resource_kinds or []), "limit": request.limit,
            "scan_limit": scan_limit, "rank_depth": rank_depth,
        }).encode()).hexdigest()

    def _encode_cursor(self, fingerprint, score, resource_id, page):
        payload = canonical({"v": CURSOR_VERSION, "q": fingerprint,
                             "s": score, "id": resource_id, "n": page}).encode()
        signature = hmac.new(self._cursor_secret, payload, hashlib.sha256).hexdigest()
        return base64.urlsafe_b64encode(payload).decode() + "." + signature

    def _decode_cursor(self, fingerprint, cursor):
        try:
            raw, signature = cursor.rsplit(".", 1)
            payload = base64.urlsafe_b64decode(raw.encode())
        except Exception:
            raise Problem("cursor_invalid", 422) from None
        if not hmac.compare_digest(
                signature, hmac.new(self._cursor_secret, payload, hashlib.sha256).hexdigest()):
            raise Problem("cursor_invalid", 422)
        try:
            data = json.loads(payload)
        except ValueError:
            raise Problem("cursor_invalid", 422) from None
        if data.get("v") != CURSOR_VERSION or not isinstance(data.get("s"), (int, float)) \
                or not isinstance(data.get("id"), str) or not isinstance(data.get("n"), int):
            raise Problem("cursor_invalid", 422)
        if data.get("q") != fingerprint:
            raise Problem("cursor_query_mismatch", 422)
        return data

    def _lexical_ranking(self, project, principal, kinds, query, depth, deadline):
        """SQL-side streaming intersection on the read-only index connection: FTS5 match
        joined against canonical visibility, ordered by bm25, hard-limited to ``depth``
        hits with a progress-handler deadline. Only candidate IDs cross the SQL
        boundary; unauthorized rows are never returned or counted."""
        words = re.findall(r"\w+", query, re.UNICODE)[:32]
        if not words:
            return [], False, False
        match = " OR ".join('"' + w + '"' for w in words)
        # Messages are not in INDEXED, so the per-kind private-agent predicate is the
        # exact visibility rule here (admin sees all).
        vis_sql, vis_args = self._visibility_sql("resource", principal)
        placeholders = ",".join("?" for _ in kinds)
        sql = ("SELECT lexical.id FROM lexical JOIN resources r ON r.id=lexical.id "
               "WHERE lexical MATCH ? AND lexical.project=? AND r.deleted=0 "
               f"AND r.kind IN ({placeholders})" + vis_sql +
               " ORDER BY bm25(lexical) LIMIT ?")
        rows, timed_out = self._ro_query(
            sql, [match, project, *kinds, *vis_args, depth + 1], deadline)
        capped = len(rows) > depth or timed_out
        return [row[0] for row in rows[:depth]], capped, timed_out

    def _semantic_ranking(self, project, meta, query, depth, deadline):
        """Exact cosine minimum per authorized resource/revision, in one vec0 scan.

        A bounded JSON relation avoids SQLite's variable limit without rescanning
        vector storage for each 300-ID batch. SQL filters eligible chunks before
        scoring, groups multi-chunk resources, and returns only depth+1 IDs. This
        is not vec0 KNN (its default L2 metric would change cosine semantics).
        The read-only connection's progress handler still enforces the deadline.
        """
        if not meta:
            return [], False, False
        if time.monotonic() >= deadline:
            return [], True, True
        revisions = canonical([[identity, revision] for identity, (_, revision) in meta.items()])
        rows, timed_out = self._ro_query(
            """SELECT c.resource,MIN(vec_distance_cosine(v.embedding,?)) AS distance
            FROM vectors v CROSS JOIN vector_chunks c ON c.id=v.chunk_id
            WHERE v.chunk_id IN (
                SELECT id FROM vector_chunks WHERE project=? AND (resource,revision) IN (
                    SELECT json_extract(value,'$[0]'),json_extract(value,'$[1]') FROM json_each(?)
                )
            )
            GROUP BY c.resource ORDER BY distance,c.resource LIMIT ?""",
            [query, project, revisions, depth + 1], deadline)
        return [row["resource"] for row in rows[:depth]], len(rows) > depth or timed_out, timed_out

    def _ranked(self, principal, request, scan_limit, rank_depth=None,
                time_budget_ms=None):
        """Compute the deterministic authorized ranking shared by search/search_page.

        Order is ``(-rrf_score, resource_id)``; ties are therefore stable across calls
        and pages. Memory is bounded: candidates are ``id -> (kind, revision)`` metadata
        only, ranking windows are capped at ``rank_depth``, and full bodies are
        materialized (with canonical recheck) only for emitted results. Returns
        ``(ordered, meta_map, meta)`` where ``ordered`` is a list of
        ``(score, resource_id)``.
        """
        project = request.project_id
        # Phase A (short canonical lock): current principal, project access, freshness.
        current, freshness = self._authorize_request(principal, request)
        depth = rank_depth_bound(rank_depth)
        budget_ms = SCAN_TIME_BUDGET_MS if time_budget_ms is None else time_budget_ms
        deadline = time.monotonic() + budget_ms / 1000.0
        warnings, lexical, semantic = [], [], []
        kinds = request.resource_kinds or sorted(INDEXED)
        # Phase B (short per-row canonical authorization locks): metadata on the
        # read-only index connection. No global top-k filtering, no first-1000 cap.
        meta_map, authorized_counts, truncated, timed_out = self._authorized_meta(
            kinds, project, current, scan_limit, deadline)
        if any(truncated.values()):
            warnings.append(
                "Search candidate scan reached its bound; narrow resource_kinds or raise "
                "scan_limit to cover older history")
        if timed_out:
            warnings.append("Search scan time budget exhausted; candidates are partial")
        lexical_capped = semantic_capped = False
        if request.mode != "semantic" or not self.encoder:
            lexical, lexical_capped, lex_timed_out = self._lexical_ranking(
                project, current, kinds, request.query, depth, deadline)
            timed_out = timed_out or lex_timed_out
            lexical = [resource for resource in lexical if resource in meta_map]
        if request.mode != "lexical":
            if self.encoder:
                # Trusted in-process CPU callback; runs with no locks held. It cannot
                # be preempted, so the time budget is a soft deadline checked around
                # it -- a slow encoder fails the budget afterwards, it is not killed.
                embedding = self.encoder.query(request.query)
                if time.monotonic() > deadline:
                    timed_out = True
                    warnings.append(
                        "Query encoder exceeded the time budget; semantic ranking partial")
                semantic, semantic_capped, sem_timed_out = self._semantic_ranking(
                    project, meta_map, embedding, depth, deadline)
                timed_out = timed_out or sem_timed_out
            else:
                if request.mode == "semantic":
                    raise Problem("index_not_ready", 503)
                warnings.append("Embedding model unavailable; lexical fallback")
        rank_truncated = lexical_capped or semantic_capped
        if rank_truncated:
            warnings.append(
                "Ranking depth bound reached; results beyond the bounded top-k window are "
                "flagged, not emitted")
        if freshness["vector_pending_jobs"]:
            warnings.append("Semantic index is behind canonical state")
        rankings = [lexical] if request.mode == "lexical" else (
            [semantic] if request.mode == "semantic" else [lexical, semantic])
        scores = {}
        for ranking in rankings:
            for rank, resource in enumerate(ranking):
                scores[resource] = scores.get(resource, 0) + 1 / (60 + rank + 1)
        ordered = sorted(((scores[r], r) for r in scores), key=lambda x: (-x[0], x[1]))
        meta = {"freshness": freshness, "warnings": warnings,
                "scan": {"scan_limit": scan_limit, "rank_depth": depth,
                         "authorized_candidates": authorized_counts,
                         "scan_truncated": truncated, "rank_truncated": rank_truncated,
                         "time_budget_exceeded": timed_out},
                "lexical_fallback": request.mode == "hybrid" and not self.encoder}
        return ordered, meta_map, meta

    def search(self, principal, request, scan_limit=None):
        ordered, meta_map, meta = self._ranked(principal, request,
                                               search_scan_bound(scan_limit))
        results = []
        for score, resource in ordered[:request.limit]:
            kind, revision = meta_map[resource]
            # Canonical recheck at materialization: index lag can never resurrect a
            # revoked, deleted, or superseded revision into a result.
            body = self._recheck(principal, resource, kind, revision)
            if body is None:
                continue
            results.append({"resource_id": resource, "kind": kind, "revision": body["revision"],
                            "score": score, "trust": body.get("origin", body.get("trust", "structured")),
                            "resource": body})
        return {"results": results, "freshness": meta["freshness"], "embedding_profile": PROFILE,
                "lexical_fallback": meta["lexical_fallback"], "scan": meta["scan"],
                "warnings": meta["warnings"], "hivemind_detached_work": True}

    def search_page(self, principal, request, cursor=None, scan_limit=None,
                    rank_depth=None, time_budget_ms=None):
        """Authorized top-k with stable keyset pagination over the ranked history.

        ``cursor`` is the previous page's ``next_cursor``. Cursors are HMAC-signed with
        a process-local key and bound to a fingerprint of the *authenticated principal*,
        project, query, and scan policy; tampering, cross-principal replay, or
        cross-query reuse is rejected before any retrieval work. Cursors are ephemeral:
        a service restart rotates the key and old cursors fail closed with
        ``cursor_invalid``. Pages are not a snapshot -- every emitted result is
        rechecked against canonical state (visibility, revision, memory validity) at
        materialization time, so results always reflect current authorization even when
        the derivative indexes lag; entries may appear or disappear between pages if
        canonical state changes. The cursor carries only the last emitted (score,
        authorized resource id) pair -- no metadata about unauthorized candidates.
        """
        bound = search_scan_bound(scan_limit)
        depth = rank_depth_bound(rank_depth)
        fingerprint = self._cursor_fingerprint(principal, request, bound, depth)
        mark = self._decode_cursor(fingerprint, cursor) if cursor else None
        ordered, meta_map, meta = self._ranked(principal, request, bound,
                                               rank_depth=depth,
                                               time_budget_ms=time_budget_ms)
        results, last, consumed = [], mark, False
        for score, resource in ordered:
            if last is not None:
                if consumed:
                    pass
                elif (-score, resource) > (-last["s"], last["id"]):
                    consumed = True
                else:
                    continue
            kind, revision = meta_map[resource]
            body = self._recheck(principal, resource, kind, revision)
            if body is None:
                continue
            results.append({"resource_id": resource, "kind": kind, "revision": body["revision"],
                            "score": score, "trust": body.get("origin", body.get("trust", "structured")),
                            "resource": body})
            if len(results) >= request.limit:
                break
        page_number = (mark["n"] if mark else 0) + 1
        has_more = False
        if results:
            tail_score, tail_id = results[-1]["score"], results[-1]["resource_id"]
            has_more = any((-score, resource) > (-tail_score, tail_id)
                           for score, resource in ordered)
        next_cursor = None
        if has_more and results:
            next_cursor = self._encode_cursor(fingerprint, results[-1]["score"],
                                              results[-1]["resource_id"], page_number)
        return {"results": results, "next_cursor": next_cursor, "page": page_number,
                "has_more": has_more,
                "exhausted": {"scan_truncated": any(meta["scan"]["scan_truncated"].values()),
                              "rank_truncated": meta["scan"]["rank_truncated"],
                              "time_budget_exceeded": meta["scan"]["time_budget_exceeded"],
                              "result_budget": not has_more},
                "freshness": meta["freshness"], "embedding_profile": PROFILE,
                "lexical_fallback": meta["lexical_fallback"], "scan": meta["scan"],
                "warnings": meta["warnings"], "hivemind_detached_work": True}

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
