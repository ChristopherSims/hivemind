Broader retrieval validation, 2026-10-17
=======================================

Status: real-encoder preparation TIMED OUT at the 1800-second command guard.
The last progress record was public_documents_started=6500, not a completed
document count. The disposable state still says preparing and is NOT ready.
No 10k latency/load measurements or blind pack were produced. Do not interpret
the protocol, partial corpus or functional tests as completed performance or
independent relevance evidence. See retrieval-mixed-preparation-timeout-2026-10-17.json.
The final Linux functional regression passed 34 tests in 15.26 seconds; that
does not replace the blocked real-encoder measurements.

Approved scope
--------------
Repository code/docs and clearly synthetic board/memory only. No production
stores, user conversations, live providers, model downloads or host cache flushes.
The existing pinned local embedding cache is used, not a generative runtime LLM.

Frozen corpus: retrieval-mixed-v1.json.gz.
Canonical dataset SHA-256:
6489b9113adae5efbcb4e9650a04e0b310049ad2d7f3537477d6ce957d8984ad
10000 public records: 2073 real code/docs excerpts and 7927 synthetic records.
Three searchable kinds: artifact (code/docs), board_post and verified fixture memory.
26 queries: 18 code, four board, four memory. Existing assistant-authored labels
are only known-answer anchors; additional capacity records remain UNJUDGED.
Memory verification and evidence links are fixture assertions, not factuality proof.
Frozen excerpts can predate current implementation; source/excerpt hashes preserve
what was actually embedded rather than silently rebuilding changed source.

Predeclared method: retrieval-broad-protocol-2026-10-17.json.
Every public document receives actual pinned model embeddings using the production
token windows/title template. Offline batching is checked against original
Gemma.documents for each kind. Identical-text private/foreign/expired/disputed
negatives reuse the corresponding real vector. Bulk preparation is NOT evidence
of production indexing throughput, graph work or event ingestion latency.

Query measurements always use production Indexes.search and uncached Gemma.query.
One fresh SQLite connection per cold sample, with setup recorded separately.
This does NOT flush the OS file cache or constitute a new-model/process cold start.
Warm measurements run all 26 queries in every mode, followed by mixed closed-loop
loads for 60 seconds each at one and four clients. Wall time includes drain;
encoder timing includes its serialization lock wait. No HTTP transport or
independent open-loop arrival-rate claim. Record partial scans/deadlines/errors
instead of silently dropping bad requests. The production rank cap remains 4096
and is disclosed separately from complete authorized-candidate scanning.

Concurrency defect found during synthetic functional probing:
metadata authorization called TeamAccess on the shared canonical SQLite connection
without its lock. Parallel requests caused InterfaceError/invalid row values.
Indexes now takes a short per-row canonical permission lock, never across model
encoding or vector scoring. A deterministic lock-ownership regression covers it.

Hybrid ranking investigation
----------------------------
Production policy remains unchanged: equal-weight rank-only RRF (k=60), with
OR-term FTS matching. Component traces expose lexical/semantic ranks and each vote,
including known-anchor demotions. Weak broad lexical matches can receive full
votes and boost consensus ahead of semantic-only answers. Whether this is harmful
for newly pooled candidates requires independent review. No weights, routing,
thresholds or production defaults are tuned against these provisional labels.

Human review workflow
---------------------
Measurement exports .blind-review.json and .review-key.json beside its report.
Give the independent reviewer ONLY the blind pack, not the key/results/original
labels. Candidates are the shuffled union of mode top-10s and known anchors, using
opaque per-query IDs. Method, rank, old grades and coded negative-title hints are
not exposed. Relevance grades are initially null; independent review is PENDING.

Edit only candidate grades (0 irrelevant, 1 background, 2 partial answer, 3 direct
answer) and reviewer={"id":"pseudonym","independent":true}. Preserve all other data
and ordering. The reviewer declaration is NOT verified identity/independence.
retrieval_workload.validate_review refuses incomplete grades, changed candidates,
wrong dataset/mapping digests and malformed declarations.
retrieval_workload.score_review also pins exact rankings and reports pooled
Recall@10/MRR/NDCG. Queries with no judged positive have null metrics, not an
invented denominator. These are pooled judgments, not exhaustive 10k corpus recall.
Keep a separate tuning set before changing fusion; do not call this pool held-out
evidence after tuning on it.

Reproduction
------------
From the repository root, using new output/work paths:
python -m hivemind.retrieval_load build --root ABSOLUTE_REPOSITORY --dataset NEW_CORPUS.json.gz --size 10000
Set HF_HOME to the existing .hivemind-data/models cache, HF_HUB_OFFLINE=1 and
TRANSFORMERS_OFFLINE=1. The Gemma loader also uses local_files_only=True.
python -m hivemind.retrieval_load prepare --dataset CORPUS.json.gz --work NEW_DISPOSABLE_WORK --output NEW_PREPARATION.json --batch-size 8
python -m hivemind.retrieval_load measure --dataset CORPUS.json.gz --work PREPARED_WORK --output NEW_MEASUREMENT.json --seconds 60 --workers 1 4
Run performance measurement without other test/model jobs. Preparation and
measurement are separate so functional checks can finish before timed phases.

External gates
--------------
Independent human judgments, genuinely cold OS-cache measurements, production
board/memory relevance, longer open-loop/API deployment load, writer contention,
and fusion calibration on independent training/held-out sets remain open.
