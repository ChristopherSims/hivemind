Retrieval and memory evidence, 2026-10-17

Search performance and real-code quality follow-up
------------------------------------------------
Summary and artifact hashes: retrieval-search-summary-2026-10-17.json.
Unchanged 10k baseline: retrieval-search-baseline-2026-10-17.json.
Final isolated run: retrieval-search-isolated-2026-10-17.json.

                       baseline p95       final p95
lexical                     507.68 ms       661.31 ms
semantic                   7513.60 ms       810.92 ms (9.27x faster)
hybrid                     7762.13 ms       930.58 ms (8.34x faster)

Same fixture/query; baseline 5 samples/mode, final 20, one warm-up per mode.
Final emitted top-10 IDs and scan flags match. All 10k authorized candidates
scanned; rank window 4096 remains explicitly truncated. Synthetic vectors and
bulk fixture seeding isolate search; this is NOT real-encoder 10k latency,
indexing/graph throughput, cold-start or sustained-load evidence.
Lexical p95 increased and is not claimed as an improvement.

Root fix: SQLite read-connection page-cache target 32 MiB instead of ~2 MiB.
Single-scan SQL alone did not materially help; retain that failed experiment in
retrieval-search-single-scan-2026-10-17.json. Cache-size measurements and the
earlier timed-out experiment are in retrieval-cache-profile-2026-10-17.json.
Exact cosine and current authorized resource/revision filtering remain intact.
SQL computes minimum chunk distance per resource and returns at most depth+1.
No ANN, result cache or authorization cache; graph pool default remains 64 MiB.
The SQLite cache setting is a target, not a hard process-memory cap.

Real-code corpus: retrieval-repository-v1.json and its frozen manifest.
64 actual Python excerpts from eight allowlisted project files, 18 developer
questions, source/excerpt hashes; private/cross-project identical-text negatives.
Final pinned offline-model results: retrieval-repository-final-2026-10-17.json.
Recall@10: lexical 0.8056, semantic 0.9722, hybrid 0.9167.
NDCG@10: lexical 0.6763, semantic 0.8937, hybrid 0.7530.
Forbidden-result rate: 0 in all modes. Quality metrics unchanged after optimization.
Hybrid is worse than semantic-only here; ranking weights were not tuned to labels.
Labels are assistant-authored, not independent; source was frozen before model
evaluation. This narrow developer-search corpus is not production board/memory
traffic or a general relevance guarantee.

Validation: 100 Windows tests (60+39+1), Ruff, scoped whitespace/JSON checks.
24 Linux Docker search tests passed with default security restrictions and private
RAM-backed /tmp (10.57s). No host mounts, credentials or external network.
The first ordinary-storage Docker attempt exited 1 without usable output.
Its root cause is unproven; the tmpfs pass is NOT disk durability evidence.
retrieval-search-final-2026-10-17.json overlapped model-quality work and is retained
but NOT used for the isolated latency claim. No owned test containers remain.

Reproduce from the repository root, choosing new output paths:
python -m hivemind.retrieval_benchmark --dataset benchmarks/retrieval-v1.json --output benchmarks/new-search.json --search-scale 10000 --repeats 20
Set HF_HOME to the existing .hivemind-data/models cache and HF_HUB_OFFLINE=1:
python -m hivemind.retrieval_benchmark --dataset benchmarks/retrieval-repository-v1.json --output benchmarks/new-quality.json --real-model --dimensions 768 --repeats 2
python -m hivemind.retrieval_dataset --root . --manifest benchmarks/retrieval-repository-v1.manifest.json --output benchmarks/new-source-snapshot.json
The builder refuses snapshot overwrite. Changed source yields a new source hash;
the published snapshot remains frozen rather than silently regenerated.

Historical retrieval/memory evidence below
------------------------------------------
Dataset: retrieval-v1.json, manually labeled synthetic documents, eight queries.
Quality results: retrieval-results-2026-10-17.json.
All lexical/semantic/hybrid and 768/512/256 comparisons use production retrieval.
Reduced vectors are prefix-truncated, normalized and zero-padded to the existing
768-dimensional schema. No smaller-column storage or latency claim is made.

Semantic/hybrid Recall@10: 1.0. Lexical Recall@10: 0.8125.
Zero forbidden results in the small evaluation. Not production quality proof.
The 2000-record synthetic capacity run exceeded the one-second search target.

Scale failure: retrieval-scale-failure-2026-10-17.json records the first
10000-record attempt at the unchanged 64 MiB service graph-buffer default.
A synthetic diagnostic reproduced LadybugDB buffer-pool exhaustion.
The retry uses an explicitly disclosed 256 MiB graph buffer, not the service default.
The retry passed: retrieval-scale-10000-2026-10-17.json.
10000 resources / 9999 edges, 256 MiB graph buffer:
indexing 318.09 s (31.44 records/s), graph rebuild 234.69 s,
exact node/edge equivalence, 80 successful graph reads during rebuild.
Search p95: lexical 500.31 ms, semantic 7831.72 ms, hybrid 7749.70 ms.
Full candidate scans completed; ranking was capped at 4096 and explicitly flagged.
This passes workload integrity, NOT the one-second search performance target.

Erasure evidence: erasure-results-2026-10-17.json.
The disposable fixture removes deleted memory versions and derived chunks and
compacts SQLite/WAL. Retained backups and spools still contain the test canary
and are correctly detected. This is NOT whole-system physical erasure.
Production stores, journals, spools and backups were not purged.

Production tokenizer validation remains gated: the operator will provide a
local LLM later. Fill tokenizer-validation.template.json with the immutable
model revision, local tokenizer hash and independent golden counts.
Never substitute the embedding tokenizer for the runtime-model tokenizer.

Live memory-provider checks remain gated. Offline tests exercise the actual
Mem0/LangMem-store/Letta adapters against injected ports, including read-after-delete
verification. API absence does not establish deletion of provider backups or logs.

Final focused regression run: 95 passed. Ruff and scoped git diff --check passed.
No full-repository, browser, live-provider or production-model validation was run.

Commands (from repository root; substitute absolute paths and new output files):
python -m hivemind.retrieval_benchmark --help
python -m hivemind.memory_validation --help

Detailed methodology and operational gates:
docs/retrieval-memory-validation.md
That new Markdown file is local-only under the existing *.md ignore rule.
This README and the JSON evidence are not covered by that ignore rule.
