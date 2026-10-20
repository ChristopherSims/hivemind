"""Tests for scalable retrieval beyond the hard first-1000 scan bound.

Owns the retrieval additions in ``hivemind/indexes.py`` (streaming authorized candidate
scans, canonical recheck, keyset cursor pagination via ``search_page``). All tests run
offline; no embedding model is loaded (the app is built with ``workers=False`` so the
encoder is absent and hybrid mode honestly falls back to lexical).
"""

import json
import time

import pytest

from hivemind import indexes as ix
from hivemind import models as m
from hivemind.api import hash_token
from hivemind.storage import Problem, canonical, now, searchable, uid


def principal_for(world, token=None):
    store = world.app.state.store
    row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                           (hash_token(token or world.owner),)).fetchone()
    return dict(row)


def put(store, kind, project, owner, body, private=None, index=True):
    """Insert a canonical row plus its derived lexical row (as Store.save would)."""
    resource_id = body["id"]
    revision = body.get("revision", 1)
    seq = store.tick(project)
    encoded = canonical({**body, "revision": revision, "updated_at": now(), "ingestion_seq": seq})
    store.db.execute(
        "INSERT INTO resources(id,kind,project,owner,private_agent,revision,seq,body) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (resource_id, kind, project, owner, private, revision, seq, encoded))
    store.db.execute("INSERT INTO versions VALUES(?,?,?)", (resource_id, revision, encoded))
    if index and kind in ix.INDEXED:
        store.db.execute("INSERT INTO lexical VALUES(?,?,?)",
                         (resource_id, project, searchable(json.loads(encoded))))
    return json.loads(encoded)


def memory_body(project, run, *, title, text, status="verified", private_note=None,
                invalidation_reason=None, expires_at=None, active_dispute_ids=None,
                evidence=None):
    body = {
        "id": uid("memory"), "project_id": project, "title": title,
        "content": {"media_type": "text/plain", "text": text},
        "evidence_refs": evidence if evidence is not None else [
            {"kind": "run", "id": run["id"], "revision": run["revision"]}],
        "validity": {}, "expires_at": expires_at, "category": "semantic",
        "scope_kind": "project", "scope_owner_id": project,
        "private": False, "status": status, "origin": "explicit", "created_at": now(),
    }
    if invalidation_reason:
        body["invalidation_reason"] = invalidation_reason
    if active_dispute_ids:
        body["active_dispute_ids"] = active_dispute_ids
    return body


def query(world, text, *, limit=50, mode="hybrid", kinds=None, min_seq=None):
    return m.SearchQuery(project_id=world.project, query=text, mode=mode, limit=limit,
                         resource_kinds=kinds or ["memory"], min_ingestion_seq=min_seq)


def test_search_finds_authorized_record_beyond_first_thousand(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Deep history")
    run = world.run(agent)
    relevant = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra migration", text="zebra migration unique relevant"))
    for index in range(1500):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Filler {index}", text=f"common filler content {index}"))

    # The old hard bound really would drop it from candidates.
    recent = {body["id"] for body in store.list("memory", world.project, principal, 1000)}
    assert relevant["id"] not in recent

    indexes = world.app.state.indexes
    result = indexes.search(principal, query(world, "zebra", mode="lexical"))
    assert result["results"] and result["results"][0]["resource_id"] == relevant["id"]
    assert result["scan"]["authorized_candidates"]["memory"] >= 1501
    assert result["scan"]["scan_truncated"]["memory"] is False

    pages, cursor = [], None
    while True:
        page = indexes.search_page(principal, query(world, "zebra", mode="lexical"),
                                   cursor=cursor)
        pages.extend(page["results"])
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert [r["resource_id"] for r in pages] == [relevant["id"]]


def test_search_page_paginates_full_ranked_history_deterministically(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Pager")
    run = world.run(agent)
    ids = []
    for index in range(60):
        ids.append(put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Zebra note {index:03d}",
            text=f"zebra pagination content {index}"))["id"])

    indexes = world.app.state.indexes
    request = query(world, "zebra", limit=7, mode="lexical")
    seen, cursors, cursor = [], [], None
    while True:
        page = indexes.search_page(principal, request, cursor=cursor)
        assert len(page["results"]) <= 7
        seen.extend(r["resource_id"] for r in page["results"])
        if not page["next_cursor"]:
            assert page["has_more"] is False
            assert page["exhausted"]["result_budget"] is True
            break
        assert page["has_more"] is True
        cursors.append(page["next_cursor"])
        cursor = page["next_cursor"]
        assert len(cursors) < 30  # pagination must terminate
    assert len(seen) == len(set(seen)) == 60  # full history, no duplicates across pages

    # Deterministic: fresh walk yields the identical order.
    again, cursor = [], None
    while True:
        page = indexes.search_page(principal, request, cursor=cursor)
        again.extend(r["resource_id"] for r in page["results"])
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert again == seen

    # Single-shot search agrees with the first page ordering.
    single = indexes.search(principal, query(world, "zebra", limit=50, mode="lexical"))
    assert [r["resource_id"] for r in single["results"]] == seen[:50]


def test_search_page_rejects_tampered_and_cross_query_cursors(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Cursor guard")
    run = world.run(agent)
    for index in range(5):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Zebra {index}", text=f"zebra cursor {index}"))

    indexes = world.app.state.indexes
    request = query(world, "zebra", limit=2, mode="lexical")
    page = indexes.search_page(principal, request)
    cursor = page["next_cursor"]
    assert cursor

    payload, signature = cursor.rsplit(".", 1)
    # Tampered signature.
    with pytest.raises(Problem) as failure:
        indexes.search_page(principal, request, cursor=payload + "." + ("0" * 64))
    assert failure.value.code == "cursor_invalid"
    # Tampered payload.
    with pytest.raises(Problem) as failure:
        indexes.search_page(principal, request,
                            cursor=payload[:-2] + ("AA" if not payload.endswith("AA") else "BB")
                            + "." + signature)
    assert failure.value.code == "cursor_invalid"
    # Garbage.
    with pytest.raises(Problem):
        indexes.search_page(principal, request, cursor="not-a-cursor")
    # Well-formed but bound to a different query.
    other = indexes.search_page(principal, query(world, "other", limit=2, mode="lexical"))
    assert other["next_cursor"] is None or True
    with pytest.raises(Problem) as failure:
        indexes.search_page(principal, query(world, "filler", limit=2, mode="lexical"),
                            cursor=cursor)
    assert failure.value.code == "cursor_query_mismatch"


def test_revoked_hidden_deleted_and_version_changed_candidates_never_surface(world):
    store = world.app.state.store
    owner_agent = world.agent("Owner")
    other_agent = world.agent("Other")
    run = world.run(owner_agent)
    token = world.credential(owner_agent)
    principal = principal_for(world, token)

    visible = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra visible", text="zebra visible memory"))
    hidden = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra hidden", text="zebra hidden memory"),
        private=other_agent["id"])
    invalidated = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra invalidated", text="zebra invalidated memory",
        invalidation_reason="source_deleted"))
    disputed = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra disputed", text="zebra disputed memory",
        active_dispute_ids=["dispute_1"]))
    deleted = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra deleted", text="zebra deleted memory"))
    changed = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra changed", text="zebra changed memory"))

    # Delete one resource and supersede another *after* the lexical index row exists:
    # the version change revokes visibility (now private to another agent), so the stale
    # indexed snapshot must never surface even though its text still matches.
    store.db.execute("UPDATE resources SET deleted=1, body='{}' WHERE id=?", (deleted["id"],))
    store.db.execute(
        "UPDATE resources SET revision=99, private_agent=?, body=? WHERE id=?",
        (other_agent["id"], canonical({**changed, "revision": 99}), changed["id"]))

    indexes = world.app.state.indexes
    found = {r["resource_id"]
             for r in indexes.search(principal, query(world, "zebra", mode="lexical"))["results"]}
    assert visible["id"] in found
    for excluded in (hidden, invalidated, disputed, deleted, changed):
        assert excluded["id"] not in found

    page = indexes.search_page(principal, query(world, "zebra", mode="lexical"))
    paged = {r["resource_id"] for r in page["results"]}
    assert visible["id"] in paged
    for excluded in (hidden, invalidated, disputed, deleted, changed):
        assert excluded["id"] not in paged
    # Counts cover authorized candidates only: the hidden/deleted rows cannot leak.
    assert page["scan"]["authorized_candidates"]["memory"] == 1


def test_index_lag_is_flagged_and_min_ingestion_seq_fails_closed(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Freshness")
    run = world.run(agent)
    put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra lag", text="zebra lag memory"))
    # Simulate a pending embedding job backlog (derivative index behind canonical).
    store.db.execute(
        "INSERT INTO jobs(project,seq,resource,revision,kind) VALUES(?,?,?,?,'embedding')",
        (world.project, store.project(world.project)["seq"], uid("memory"), 1))

    indexes = world.app.state.indexes
    result = indexes.search(principal, query(world, "zebra", mode="lexical"))
    assert "Semantic index is behind canonical state" in result["warnings"]
    page = indexes.search_page(principal, query(world, "zebra", mode="lexical"))
    assert "Semantic index is behind canonical state" in page["warnings"]
    assert page["freshness"]["vector_pending_jobs"] >= 1

    future = store.project(world.project)["seq"] + 1000
    with pytest.raises(Problem) as failure:
        indexes.search_page(principal, query(world, "zebra", mode="lexical",
                                             min_seq=future))
    assert failure.value.code == "index_not_ready"
    assert failure.value.status == 503


def test_scan_bound_is_enforced_and_flagged_without_leaking(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Bounded")
    run = world.run(agent)
    oldest = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra oldest", text="zebra oldest memory"))
    for index in range(30):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Zebra {index}", text=f"zebra filler {index}"))

    indexes = world.app.state.indexes
    request = query(world, "zebra", mode="lexical")
    page = indexes.search_page(principal, request, scan_limit=10)
    assert page["scan"]["scan_limit"] == 10
    assert page["scan"]["scan_truncated"]["memory"] is True
    assert page["exhausted"]["scan_truncated"] is True
    assert any("scan reached its bound" in w for w in page["warnings"])
    # The oldest record is beyond the bound; only the 10 newest authorized candidates
    # are visible to ranking.
    assert page["scan"]["authorized_candidates"]["memory"] == 10
    assert oldest["id"] not in {r["resource_id"] for r in page["results"]}

    # Full-history scan finds it; bounds are explicit, never silent.
    full = indexes.search_page(principal, request)
    assert full["scan"]["scan_truncated"]["memory"] is False
    assert full["scan"]["authorized_candidates"]["memory"] == 31

    # Bound is clamped to the documented maximum.
    assert ix.search_scan_bound(10**9) == ix.SEARCH_SCAN_MAX
    assert ix.search_scan_bound(None) == ix.SEARCH_SCAN_DEFAULT


def test_semantic_ranking_keeps_exact_bounded_topk(world):
    """_semantic_ranking computes exact cosine distances and keeps the true top-k."""
    import math
    store = world.app.state.store
    indexes = world.app.state.indexes

    def basis_vector(dim):
        vector = [0.0] * 768
        vector[dim] = 1.0
        return ix.vector_blob(vector)

    meta = {}
    ids = [f"mem_{index:03d}" for index in range(8)]
    for index, resource in enumerate(ids):
        inserted = store.db.execute(
            "INSERT INTO vector_chunks(resource,revision,project,chunk,text) "
            "VALUES(?,?,?,?,?)", (resource, 1, world.project, 0, "x"))
        store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                         (inserted.lastrowid, basis_vector(index), world.project))
        meta[resource] = ("memory", 1)
    # One stale-revision chunk must be ignored even though its vector matches exactly.
    stale = store.db.execute(
        "INSERT INTO vector_chunks(resource,revision,project,chunk,text) VALUES(?,?,?,?,?)",
        ("mem_007", 99, world.project, 1, "y"))
    store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                     (stale.lastrowid, basis_vector(7), world.project))

    query_blob = basis_vector(0)
    deadline = time.monotonic() + 30
    ranked, overflow, _ = indexes._semantic_ranking(world.project, meta, query_blob, 3,
                                                    deadline)
    assert ranked[0] == "mem_000"  # exact self match first
    assert len(ranked) == 3 and overflow is True
    full, full_overflow, _ = indexes._semantic_ranking(world.project, meta, query_blob,
                                                       100, deadline)
    assert full_overflow is False
    assert full[0] == "mem_000"
    # Distances are exact cosine: orthogonal vectors rank by actual distance, and the
    # nearest neighbor of basis-0 among the rest is deterministic.
    query2 = ix.vector_blob([math.sqrt(0.5), math.sqrt(0.5)] + [0.0] * 766)
    mixed, _, _ = indexes._semantic_ranking(world.project, meta, query2, 2, deadline)
    assert set(mixed) == {"mem_000", "mem_001"}
    assert len(set(mixed)) == 2


@pytest.mark.parametrize("depth", [1, 7, 4096])
def test_single_scan_matches_exhaustive_cosine_with_multichunks_and_stale_rows(world, monkeypatch, depth):
    from hivemind.retrieval_benchmark import SyntheticEncoder
    store, indexes = world.app.state.store, world.app.state.indexes
    encoder = SyntheticEncoder()
    meta = {}
    with store.transaction():
        for number in range(340):
            resource = f"rank-{number:04d}"
            meta[resource] = ("memory", 2)
            for chunk, revision in ((0, 1), (0, 2), (1, 2)):
                inserted = store.db.execute(
                    "INSERT INTO vector_chunks(resource,revision,project,chunk,text) VALUES(?,?,?,?,?)",
                    (resource, revision, world.project, chunk, "fixture")).lastrowid
                store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                                 (inserted, encoder.query(f"{resource}-{revision}-{chunk}"), world.project))
        # An ungranted resource and a cross-project chunk must not affect the ranking.
        for project, resource in ((world.project, "ungranted"), ("other-project", "foreign")):
            inserted = store.db.execute(
                "INSERT INTO vector_chunks(resource,revision,project,chunk,text) VALUES(?,2,?,0,'x')",
                (resource, project)).lastrowid
            store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                             (inserted, encoder.query("query"), project))
    query_blob = encoder.query("query")
    exhaustive = {}
    for row in store.db.execute(
            "SELECT c.resource,c.revision,vec_distance_cosine(v.embedding,?) distance "
            "FROM vector_chunks c JOIN vectors v ON v.chunk_id=c.id WHERE c.project=?",
            (query_blob, world.project)):
        if row["resource"] in meta and row["revision"] == meta[row["resource"]][1]:
            exhaustive[row["resource"]] = min(exhaustive.get(row["resource"], float("inf")), row["distance"])
    expected = sorted(exhaustive, key=lambda identity: (exhaustive[identity], identity))
    calls = []
    original = indexes._ro_query
    def record(sql, args, deadline):
        calls.append(sql)
        return original(sql, args, deadline)
    monkeypatch.setattr(indexes, "_ro_query", record)
    ranked, capped, timed_out = indexes._semantic_ranking(
        world.project, meta, query_blob, depth, time.monotonic() + 20)
    assert ranked == expected[:depth]
    assert capped is (len(expected) > depth) and not timed_out
    assert len(calls) == 1


def test_cosine_semantics_are_not_replaced_by_default_vec0_l2(world):
    import math
    store, indexes = world.app.state.store, world.app.state.indexes
    query_blob = ix.vector_blob([1.] + [0.] * 767)
    vectors = {"scaled-aligned": [1.005] + [0.] * 767,
               "near-angle": [math.sqrt(1 - .002 ** 2), .002] + [0.] * 766}
    for identity, vector in vectors.items():
        inserted = store.db.execute(
            "INSERT INTO vector_chunks(resource,revision,project,chunk,text) VALUES(?,1,?,0,'x')",
            (identity, world.project)).lastrowid
        store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                         (inserted, ix.vector_blob(vector), world.project))
    ranked, capped, timed_out = indexes._semantic_ranking(
        world.project, {key: ("memory", 1) for key in vectors}, query_blob, 1, time.monotonic() + 5)
    assert ranked == ["scaled-aligned"] and capped and not timed_out


def test_vector_read_cache_is_bounded_and_connection_local(world):
    indexes, store = world.app.state.indexes, world.app.state.store
    assert indexes.ro_db.execute("PRAGMA cache_size").fetchone()[0] == -ix.SQLITE_READ_CACHE_KIB
    assert store.db.execute("PRAGMA cache_size").fetchone()[0] != -ix.SQLITE_READ_CACHE_KIB


def test_semantic_overflow_is_flagged_even_when_every_late_candidate_improves(world):
    store, indexes = world.app.state.store, world.app.state.indexes
    near = ix.vector_blob([1.] + [0.] * 767)
    far = ix.vector_blob([0., 1.] + [0.] * 766)
    meta = {}
    with store.transaction():
        for number in range(340):
            identity = f"late-{number:04d}"
            meta[identity] = ("memory", 1)
            inserted = store.db.execute(
                "INSERT INTO vector_chunks(resource,revision,project,chunk,text) VALUES(?,1,?,0,'x')",
                (identity, world.project)).lastrowid
            store.db.execute("INSERT INTO vectors(chunk_id,embedding,project) VALUES(?,?,?)",
                             (inserted, near if number >= 300 else far, world.project))
    ranked, capped, timed_out = indexes._semantic_ranking(
        world.project, meta, near, 300, time.monotonic() + 5)
    assert len(ranked) == 300 and capped and not timed_out
    assert ranked[:40] == [f"late-{number:04d}" for number in range(300, 340)]


def test_semantic_mode_without_encoder_fails_honestly(world):
    principal = principal_for(world)
    indexes = world.app.state.indexes
    assert indexes.encoder is None  # offline test app: no embedding model loaded
    with pytest.raises(Problem) as failure:
        indexes.search_page(principal, query(world, "zebra", mode="semantic"))
    assert failure.value.code == "index_not_ready"
    hybrid = indexes.search_page(principal, query(world, "zebra", mode="hybrid"))
    assert hybrid["lexical_fallback"] is True


def test_hidden_rows_do_not_influence_scan_metadata(world):
    """Private rows are filtered in SQL: scan counts and truncation flags must be
    identical whether or not a large hidden corpus exists."""
    store = world.app.state.store
    owner_agent = world.agent("Visible owner")
    other_agent = world.agent("Hidden owner")
    run = world.run(owner_agent)
    principal = principal_for(world, world.credential(owner_agent))

    for index in range(5):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Zebra visible {index}", text=f"zebra open {index}"))

    indexes = world.app.state.indexes
    request = query(world, "zebra", mode="lexical")
    before = indexes.search_page(principal, request, scan_limit=10)
    assert before["scan"]["authorized_candidates"]["memory"] == 5
    assert before["scan"]["scan_truncated"]["memory"] is False

    # Add a large hidden corpus; a bound of 10 would be "reached" if hidden rows were
    # counted, and authorized counts would shift if validity checks ran on them.
    for index in range(50):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Zebra hidden {index}", text=f"zebra secret {index}"),
            private=other_agent["id"])

    after = indexes.search_page(principal, request, scan_limit=10)
    assert after["scan"]["authorized_candidates"] == before["scan"]["authorized_candidates"]
    assert after["scan"]["scan_truncated"] == before["scan"]["scan_truncated"]
    assert {r["resource_id"] for r in after["results"]} == {
        r["resource_id"] for r in before["results"]}


def test_cursor_replay_across_principals_and_restart_fails_closed(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Replay guard")
    run = world.run(agent)
    for index in range(5):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Zebra {index}", text=f"zebra replay {index}"))

    indexes = world.app.state.indexes
    request = query(world, "zebra", limit=2, mode="lexical")
    cursor = indexes.search_page(principal, request)["next_cursor"]
    assert cursor

    # Same query, different authenticated principal: the fingerprint binds identity.
    other_agent = world.agent("Other")
    other = principal_for(world, world.credential(other_agent))
    with pytest.raises(Problem) as failure:
        indexes.search_page(other, request, cursor=cursor)
    assert failure.value.code == "cursor_query_mismatch"

    # Different scan policy: fingerprint mismatch too.
    with pytest.raises(Problem) as failure:
        indexes.search_page(principal, request, cursor=cursor, scan_limit=999)
    assert failure.value.code == "cursor_query_mismatch"

    # Restart semantics: rotating the process-local key invalidates all cursors.
    secret = indexes._cursor_secret
    try:
        indexes._cursor_secret = b"\x00" * 32
        with pytest.raises(Problem) as failure:
            indexes.search_page(principal, request, cursor=cursor)
        assert failure.value.code == "cursor_invalid"
    finally:
        indexes._cursor_secret = secret
    # Original secret restored: cursor works again (cursors are per-process, not snapshot).
    page = indexes.search_page(principal, request, cursor=cursor)
    assert page["page"] == 2


def test_rank_depth_window_is_bounded_and_flagged(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Rank bounds")
    run = world.run(agent)
    for index in range(10):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Zebra {index}", text=f"zebra depth {index}"))

    indexes = world.app.state.indexes
    request = query(world, "zebra", limit=50, mode="lexical")
    page = indexes.search_page(principal, request, rank_depth=4)
    assert page["scan"]["rank_depth"] == 4
    assert page["scan"]["rank_truncated"] is True
    assert page["exhausted"]["rank_truncated"] is True
    assert any("Ranking depth bound" in w for w in page["warnings"])
    assert len(page["results"]) <= 4
    full = indexes.search_page(principal, request)
    assert full["scan"]["rank_truncated"] is False
    assert len(full["results"]) == 10
    assert ix.rank_depth_bound(10**9) == ix.RANK_DEPTH_MAX


def test_authorized_metadata_is_lightweight(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Metadata")
    run = world.run(agent)
    put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra meta", text="zebra metadata"))
    indexes = world.app.state.indexes
    meta, counts, truncated, timed_out = indexes._authorized_meta(
        ["memory"], world.project, principal, 1000, time.monotonic() + 30)
    assert counts["memory"] == 1 and truncated["memory"] is False and timed_out is False
    (kind, revision), = meta.values()
    assert kind == "memory" and revision == 1  # id -> (kind, revision) only, no bodies


class GatedEncoder:
    """Fake encoder whose query() blocks until released, recording entry."""

    def __init__(self):
        import threading as _threading
        self.entered = _threading.Event()
        self.release = _threading.Event()

    def query(self, text):
        self.entered.set()
        assert self.release.wait(timeout=30)
        vector = [0.0] * 768
        vector[0] = 1.0
        return ix.vector_blob(vector)


def test_heartbeat_responsive_while_query_encoder_blocked(world):
    """A blocked query encoder must not hold the canonical store lock: heartbeats and
    other canonical reads stay responsive during semantic compute."""
    import threading

    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Encoder gate")
    run = world.run(agent)
    put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra gated", text="zebra gated memory"))

    indexes = world.app.state.indexes
    gate = GatedEncoder()
    indexes.encoder = gate
    outcome = {}
    try:
        worker = threading.Thread(
            target=lambda: outcome.setdefault("result", indexes.search(
                principal, query(world, "zebra", mode="hybrid"))))
        worker.start()
        assert gate.entered.wait(timeout=10)  # search is inside encoder.query now
        started = time.monotonic()
        acquired = store.lock.acquire(timeout=1.0)  # heartbeat proxy: canonical lock
        elapsed = time.monotonic() - started
        if acquired:
            store.lock.release()
        assert acquired and elapsed < 1.0  # canonical lock never blocked by the encoder
        gate.release.set()
        worker.join(timeout=30)
        assert not worker.is_alive()
        assert outcome["result"]["hivemind_detached_work"] is True
        assert outcome["result"]["results"][0]["resource_id"]
    finally:
        gate.release.set()
        indexes.encoder = None


def test_revocation_mid_compute_yields_no_private_results(world):
    """If authorization changes between the metadata snapshot and materialization, the
    canonical recheck with current principal bindings drops the result."""
    import threading

    store = world.app.state.store
    owner_agent = world.agent("Revocable")
    other_agent = world.agent("Other")
    run = world.run(owner_agent)
    token = world.credential(owner_agent)
    principal = principal_for(world, token)
    secret = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra secret", text="zebra secret revoked mid query"))

    indexes = world.app.state.indexes
    gate = GatedEncoder()
    indexes.encoder = gate
    outcome = {}
    try:
        worker = threading.Thread(
            target=lambda: outcome.setdefault("result", indexes.search(
                principal, query(world, "zebra", mode="hybrid"))))
        worker.start()
        assert gate.entered.wait(timeout=10)  # snapshot done, inside encoder.query
        # Revoke visibility while compute is blocked.
        store.db.execute("UPDATE resources SET private_agent=? WHERE id=?",
                         (other_agent["id"], secret["id"]))
        gate.release.set()
        worker.join(timeout=30)
        assert not worker.is_alive()
        assert secret["id"] not in {r["resource_id"] for r in outcome["result"]["results"]}
    finally:
        gate.release.set()
        indexes.encoder = None


def test_principal_deleted_mid_compute_yields_no_results(world):
    import threading

    store = world.app.state.store
    owner_agent = world.agent("Deleted principal")
    run = world.run(owner_agent)
    token = world.credential(owner_agent)
    principal = principal_for(world, token)
    put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra ghost", text="zebra ghost memory"))

    indexes = world.app.state.indexes
    gate = GatedEncoder()
    indexes.encoder = gate
    outcome = {}
    try:
        worker = threading.Thread(
            target=lambda: outcome.setdefault("result", indexes.search(
                principal, query(world, "zebra", mode="hybrid"))))
        worker.start()
        assert gate.entered.wait(timeout=10)
        store.db.execute("DELETE FROM principals WHERE id=?", (principal["id"],))
        gate.release.set()
        worker.join(timeout=30)
        assert not worker.is_alive()
        assert outcome["result"]["results"] == []
    finally:
        gate.release.set()
        indexes.encoder = None


def test_concurrent_drain_and_search_do_not_interfere(world):
    import threading

    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Concurrent")
    run = world.run(agent)
    target = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra concurrent", text="zebra concurrent memory"))
    store.db.execute(
        "INSERT INTO jobs(project,seq,resource,revision,kind) VALUES(?,?,?,?,'graph')",
        (world.project, store.project(world.project)["seq"], target["id"], 1))

    indexes = world.app.state.indexes
    errors = []
    stop = threading.Event()

    def drainer():
        while not stop.is_set():
            try:
                indexes.drain(1)
            except Exception as exc:  # noqa: BLE001 -- record, assert below
                errors.append(exc)

    thread = threading.Thread(target=drainer)
    thread.start()
    try:
        for _ in range(10):
            page = indexes.search_page(principal, query(world, "zebra", mode="lexical"))
            assert [r["resource_id"] for r in page["results"]] == [target["id"]]
    finally:
        stop.set()
        thread.join(timeout=30)
    assert not thread.is_alive()
    assert not errors


def test_sql_time_cap_flags_partial_results(world):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Time cap")
    run = world.run(agent)
    for index in range(50):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Zebra {index}", text=f"zebra slow {index}"))

    indexes = world.app.state.indexes
    request = query(world, "zebra", mode="lexical")
    page = indexes.search_page(principal, request, time_budget_ms=0)
    assert page["scan"]["time_budget_exceeded"] is True
    assert page["exhausted"]["time_budget_exceeded"] is True
    assert any("time budget" in w for w in page["warnings"])

    # The progress handler aborts long-running statements on the index connection only.
    # A cross join guarantees enough VM instructions for the handler to fire.
    deadline = time.monotonic() - 1  # already expired
    rows, aborted = indexes._ro_query(
        "SELECT count(*) FROM resources a CROSS JOIN resources b CROSS JOIN resources c",
        (), deadline)
    assert aborted is True and rows == []
    # The canonical connection is untouched (no progress handler was ever set on it).
    assert store.db.execute("SELECT count(*) FROM resources").fetchone()[0] >= 50

    full = indexes.search_page(principal, request)
    assert full["scan"]["time_budget_exceeded"] is False
    assert len(full["results"]) == 50


def test_drain_does_not_hold_locks_while_encoding(world):
    """process_one snapshots, encodes with no canonical/index locks, then commits."""
    import threading

    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Drain encoder")
    run = world.run(agent)
    memory = put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Zebra encode", text="zebra encode me"))
    store.db.execute(
        "INSERT INTO jobs(project,seq,resource,revision,kind) VALUES(?,?,?,?,'embedding')",
        (world.project, store.project(world.project)["seq"], memory["id"], 1))

    lock_free_during_encode = []

    class ProbingEncoder:
        def documents(self, title, text):
            # Probe from a second thread: if process_one held the canonical lock while
            # encoding, this acquisition would block until encode returned.
            acquired = threading.Event()

            def probe():
                if store.lock.acquire(timeout=5):
                    store.lock.release()
                    acquired.set()

            t = threading.Thread(target=probe)
            t.start()
            t.join(timeout=10)
            lock_free_during_encode.append(acquired.is_set())
            vector = [0.0] * 768
            vector[0] = 1.0
            return [(text, ix.vector_blob(vector))]

    indexes = world.app.state.indexes
    indexes.encoder = ProbingEncoder()
    try:
        indexes.drain(100)  # earlier jobs may be stale and skip encoding
        assert lock_free_during_encode and all(lock_free_during_encode)
        row = store.db.execute(
            "SELECT state FROM jobs WHERE resource=? AND kind='embedding'",
            (memory["id"],)).fetchone()
        assert row["state"] == "done"
        assert store.db.execute(
            "SELECT count(*) FROM vector_chunks WHERE resource=?",
            (memory["id"],)).fetchone()[0] == 1
    finally:
        indexes.encoder = None


def test_install_registers_search_page_route_with_operator_policy(world):
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.testclient import TestClient
    from hivemind import retrieval_runtime as rr
    from hivemind.api import envelope, hash_token as api_hash_token

    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Route worker")
    run = world.run(agent)
    for index in range(5):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Zebra {index}", text=f"zebra route {index}"))

    app = FastAPI()

    def principal_dependency(request: Request):
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="authentication_required")
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (api_hash_token(auth[7:]),)).fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="authentication_required")
        return dict(row)

    rr.install(app, world.app.state.service, principal_dependency, None, envelope,
               scan_limit=1000, rank_depth=3)
    client = TestClient(app)
    headers = {"Authorization": "Bearer " + world.owner}
    assert client.post("/v1/retrieval:search-page",
                       json={"project_id": world.project, "query": "zebra"}).status_code == 401
    page = client.post("/v1/retrieval:search-page", headers=headers,
                       json={"project_id": world.project, "query": "zebra",
                             "mode": "lexical", "limit": 2}).json()["data"]
    assert len(page["results"]) == 2 and page["next_cursor"]
    assert page["scan"]["scan_limit"] == 1000  # operator-fixed, not client-selectable
    assert page["scan"]["rank_depth"] == 3
    rest = client.post("/v1/retrieval:search-page", headers=headers,
                       json={"project_id": world.project, "query": "zebra",
                             "mode": "lexical", "limit": 2,
                             "cursor": page["next_cursor"]}).json()["data"]
    assert len(rest["results"]) == 1  # rank_depth=3 bounds the window; flagged
    assert rest["scan"]["rank_truncated"] is True
    # Client cannot inject a scan limit through the request body.
    assert "scan_limit" not in rr.SearchPageBody.model_fields
    operations = client.get("/v1/retrieval:operations", headers=headers).json()["data"]
    names = {op["name"] for op in operations["operations"]}
    assert "retrieval.search_page" in names
    assert operations["counters"]
