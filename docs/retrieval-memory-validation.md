# Retrieval and memory evidence

## Scope and status, 2026-10-17

The benchmark and validation tools run offline against disposable stores. They do
not download models, contact memory providers, or erase retained production data.
The dashboard remains parked. Reports are local JSON files, not external uploads.

**The operator will provide a local LLM later.** Until its exact model revision,
tokenizer and independent golden counts are supplied, production-model tokenizer
validation remains blocked. EmbeddingGemma is the retrieval encoder, not a substitute
for that LLM's tokenizer or a newly configured memory-extraction model.

Live Mem0, LangMem-store and Letta checks remain explicitly gated. Offline adapter
tests do not establish live SDK compatibility or provider-wide physical erasure.

## Search-performance and real-code follow-up

Current summary: `benchmarks/retrieval-search-summary-2026-10-17.json`.
The isolated 10k search-only run measured semantic p95 **810.92 ms** and hybrid
**930.58 ms**, compared with matched baseline **7513.60 / 7762.13 ms**.
Baseline has five samples per mode; final has twenty after one warm-up.
Final emitted top-10 IDs and scan flags match. Lexical p95 increased
from 507.68 to 661.31 ms and is not claimed as an improvement.

The key bottleneck was SQLite's approximately 2 MiB read page cache.
An independent cache-size experiment reduced exact semantic scoring from about
6.4–7.0 seconds at 2 MiB to 0.25 seconds at 32 MiB. The read-only connection now
targets 32 MiB, without changing the canonical writer connection or 64 MiB graph
default. This is a suggested SQLite page-cache size, not a hard RSS limit.
There is no query-result or authorization cache. One SQL scan filters the
authorized resource/revision relation, computes exact cosine minimum per resource,
and returns at most depth+1. It does not switch to vec0's default L2 KNN metric.
Final canonical authorization and revision rechecks still run.

The SQL-only experiment did not meaningfully improve latency and is retained.
The first batched SQL experiment timed out. The separately retained
`retrieval-search-final-2026-10-17.json` overlapped CPU model evaluation;
only `retrieval-search-isolated-2026-10-17.json` supports the timing claim.
The timing fixture uses bulk-seeded synthetic vectors through production search.
It measures neither indexing throughput nor real-model 10k search latency.
Full 10k authorized candidate scans completed; ranking remains capped at 4096
and explicitly flagged. Diverse queries, cold caches and concurrent sustained
deployment load remain necessary.

`retrieval-repository-v1.json` freezes 64 real Python excerpts from eight
allowlisted project modules and 18 developer questions. Source/excerpt SHA-256
hashes pin provenance. The AST builder refuses traversal/source links and existing
output overwrite. Rebuilding changed source must create a new snapshot.
The snapshot predates the final cache/SQL implementation; use the frozen bytes
for matched relevance evaluation rather than silently rebuilding it.

At pinned cached 768-dimensional EmbeddingGemma:

| Mode | Recall@10 | MRR@10 | NDCG@10 | Forbidden rate |
| --- | ---: | ---: | ---: | ---: |
| Lexical | 0.8056 | 0.6644 | 0.6763 | 0 |
| Semantic | 0.9722 | 0.8951 | 0.8937 | 0 |
| Hybrid | 0.9167 | 0.7122 | 0.7530 | 0 |

Metrics are unchanged after optimization. Hybrid is worse than semantic-only
on this corpus; weights were not tuned to its labels. Questions and relevance
grades were assistant-authored and frozen before the first model evaluation.
They are not independent judgments or production board/memory traffic.
Identical-text private and foreign-project negatives bring total resources to
192. General quality claims require independent permitted-project judgments,
harder negatives and mixed board/code/memory queries.

Validation: 100 Windows tests across three batches, Ruff and scoped whitespace.
The Linux Docker search suite passed 24 tests in 10.57 seconds with default
security restrictions, no external network/host mounts and private tmpfs.
The first ordinary-storage Docker attempt exited 1 without usable output.
Its cause is unproven; the tmpfs pass is correctness, not disk durability evidence.
No owned test containers remain. Commands and raw report names are in
`benchmarks/README.txt`.

## Original synthetic retrieval dataset and reproduction

`benchmarks/retrieval-v1.json` contains 24 manually authored synthetic documents and
eight queries: six semantic, one exact code-symbol query, and one Spanish query.
Each query has two graded relevant documents. Other documents include hard negatives.
For every document, the harness inserts identical-text private-agent and
cross-project negatives, giving 72 searchable records and 48 forbidden records.

This is a small regression dataset, not an independently labeled production
evaluation. Add representative permitted project data, independent labeling,
harder negative pools and uncertainty estimates before making general quality claims.
Do not tune ranking against this dataset and then call it held-out evidence.

From the repository root in PowerShell, using an already-cached model:

```powershell
$env:HF_HOME = '<repo>/.hivemind-data/models'
$env:HF_HUB_OFFLINE = '1'
$env:TRANSFORMERS_OFFLINE = '1'
.venv/Scripts/python.exe -m hivemind.retrieval_benchmark `
  --dataset <repo>/benchmarks/retrieval-v1.json `
  --output <repo>/benchmarks/retrieval-results-new.json `
  --real-model --repeats 3 --scale 2000
```

Output must be a new file. Every workload owns a temporary store; it cannot select
an existing production database. Without `--real-model`, only lexical quality is
reported. Synthetic scale vectors are never reported as model relevance evidence.

Search uses the actual authorized `Indexes.search` implementation: FTS5, sqlite-vec,
reciprocal-rank fusion and fresh canonical materialization. Semantic/hybrid fallback,
candidate truncation or a quality-query timeout fails the run. A forbidden result
also fails it. There is one unmeasured warm-up per query/mode/dimension; three measured
repetitions produce 24 latency samples per comparison, but only eight unique queries.
Latency includes uncached query encoding, excludes model startup and HTTP transport.
Document encodings are computed once and reused across dimensional comparisons.

Lower-dimensional vectors use prefix truncation, L2 normalization and zero-padding
to the existing 768-dimensional fixture schema. This measures ranking quality, not
the storage savings or performance of native 256/512-dimensional SQLite columns.
No production embedding profile or storage schema changes.

## Measured retrieval results

Raw report: `benchmarks/retrieval-results-2026-10-17.json`.

Windows 10, Python 3.12.13, pinned EmbeddingGemma 2 revision
`914f7f89142e33e77833254d9c9b90c3cef7303b`, sqlite-vec 0.1.9, LadybugDB
0.21.2, SentenceTransformers 6.1.0, PyTorch 2.14.1. Environment identifiers and
dataset SHA-256 are retained in the report. Model initialization took 55.39 seconds.
The test host was not a dedicated/quiesced benchmark machine; these are local
observations, not certified deployment targets.

| Mode | Dimensions | Recall@10 | MRR@10 | NDCG@10 | p95 search, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Lexical | n/a | 0.8125 | 0.7917 | 0.6892 | 3.43 |
| Semantic | 768 | 1.0000 | 1.0000 | 1.0000 | 184.69 |
| Hybrid | 768 | 1.0000 | 0.9375 | 0.9219 | 188.36 |
| Semantic | 512 | 1.0000 | 1.0000 | 0.9900 | 206.41 |
| Hybrid | 512 | 1.0000 | 0.9375 | 0.9219 | 191.49 |
| Semantic | 256 | 1.0000 | 1.0000 | 0.9799 | 189.39 |
| Hybrid | 256 | 1.0000 | 0.9375 | 0.9196 | 212.33 |

All comparisons returned zero forbidden results. Lexical rankings are unchanged
across dimensional runs; the table lists one lexical run rather than implying
dimensions affect lexical retrieval.

On the six-query semantic subset, lexical Recall@10 was 0.8333 and NDCG@10 was
0.6501. Semantic 768 achieved 1.0 for both. Hybrid 768 achieved Recall@10 1.0 and
NDCG@10 0.8959. Hybrid did **not** beat semantic-only ranking here. Keep the
production profile unchanged until a broader held-out evaluation justifies a change.

## Capacity, graph concurrency and rebuilds

The initial 2,000-record synthetic workload had 1,999 chain edges:

- Canonical ingestion: 0.98 seconds.
- Vector/graph indexing with a cheap deterministic encoder: 39.14 seconds,
  51.10 indexed records/second. This is not real-model indexing throughput.
- Fresh graph projection rebuilt from canonical resources/relations in 33.50 seconds.
  Sorted node and edge sets exactly matched the original graph.
- Four concurrent graph callers, 20 requests: p95 82.05 ms.
- Search p95: lexical 108.94 ms, semantic 1,630.31 ms, hybrid 1,629.19 ms.

The semantic/hybrid measurements already exceed the plan's provisional one-second
search target, even with cheap synthetic query encoding. Do not extrapolate them
into a production scale claim.

The first 10,000-record run at the production default 64 MiB graph budget failed
with 6,313 jobs incomplete. A separate 5,000-record graph-only diagnostic reproduced
LadybugDB buffer-pool exhaustion after 3,610 jobs. The exact failure threshold is
not a stable capacity guarantee. Evidence:
`benchmarks/retrieval-scale-failure-2026-10-17.json`.

`Indexes` now accepts an explicit bounded `graph_buffer_pool_size` keyword for
operator/benchmark construction. The existing service default remains 64 MiB.
Scale benchmarks default to a disclosed 256 MiB budget; select it explicitly with
`--graph-memory-mib 256`. Results at that budget must not be attributed to the
unchanged default service configuration.

The extended harness accepts 2..100,000 records and additionally runs graph readers
during a fresh projection rebuild. It requires LadybugDB, refuses silent fallback,
checks completed jobs and compares rebuilt node/edge sets. Concurrent callers share
the canonical/graph locks as the API does; this is not a parallel connection pool.
Read-during-rebuild results may be incomplete while the projection catches up.
The benchmark does not test kill/disk-full conditions or concurrent canonical mutation.
Search scan/ranking bounds are disclosed in extended scale reports.
Failed runs also produce a nonzero exit status and a structured failure report;
they must not be treated as completed performance measurements.

The explicit 256 MiB retry **passed**:
`benchmarks/retrieval-scale-10000-2026-10-17.json`.
It indexed 10,000 resources and 9,999 edges in 318.09 seconds (31.44 records/second).
Graph rebuild took 234.69 seconds and reproduced the exact sorted node/edge sets.
Four callers completed 20 graph reads at p95 267.64 ms; 80 reads during projection
rebuild completed at p95 127.41 ms. Search p95 was lexical 500.31 ms, semantic
7,831.72 ms and hybrid 7,749.70 ms. Candidate scans covered all 10,000 records
without timeouts, while ranking depth was capped at 4,096 and explicitly flagged.
This is a workload-integrity pass, not a performance-target pass. Synthetic vectors,
lock-serialized graph access and a non-dedicated test host limit interpretation.

## Production tokenizer validation

Fill `benchmarks/tokenizer-validation.template.json` only after the local LLM is
provided. Supply the immutable model revision, SHA-256 of its local `tokenizer.json`,
and independent expected counts. Include empty text, Unicode, code, JSON,
whitespace and long text exceeding any configured truncation length. Never derive
golden counts from the implementation being tested.

```powershell
.venv/Scripts/python.exe -m hivemind.memory_validation `
  --output <repo>/benchmarks/tokenizer-results-new.json tokenizer `
  --manifest <private-external>/local-llm-counts.json `
  --tokenizer-file <private-external>/tokenizer.json
```

The production `runtime_counter` verifies and parses the same pinned bytes,
disables padding/truncation and counts without added special tokens. A count
mismatch gives a nonzero exit status. Reports omit sample text and local paths.
An operator-supplied model name/revision is not independent proof of vendor binding,
so the report leaves `vendor_model_binding_verified` false.

Passing raw-text counts does not validate chat templates, tool schemas, hidden
provider framing, output maxima or stock vendor budget enforcement. Those need
separate provider/runtime contracts. The checked-in template intentionally fails
until real pins and expected counts replace its placeholders.

## Memory-provider compatibility and deletion

Offline regression coverage:

```powershell
.venv/Scripts/python.exe -m pytest `
  tests/test_memory_provider_sync.py tests/test_memory_validation.py -q
```

Tests exercise the actual Mem0, LangMem-store and Letta adapters with injected data
ports, including data-only writes, receipts and read-after-delete checks. Existing
sync tests cover namespace/account binding, uncertain effects, concurrent
reconcilers, source revocation and permissions.

`memory_validation.provider_absence(adapter, receipt)` performs bounded read-only
checks against exact receipt IDs. A timeout, malformed read or identity mismatch
cannot prove absence. A deletion acknowledgement alone is not absence evidence.
Neither API absence nor a successful acknowledgement proves deletion from provider
backups, logs, replicas or underlying media.

Before any live checks:

1. Obtain explicit approval for the named provider/account and a disposable namespace.
2. Pin the SDK/server versions; supply credentials privately with bounded timeouts.
3. Confirm data-only operations do not trigger inference or paid model calls.
4. Write only uniquely marked synthetic records, retain exact receipts, read them,
   delete only those owned records, then check read-after-delete with bounded polling.
5. Hold unknown outcomes. Reconcile with reads; never blindly repeat a write/delete.
6. Record provider retention/backup limitations separately from API absence.

No live provider credentials, SDK installations, requests or deletes were used here.
Supplying a local LLM later does not automatically authorize provider access.

## Regression validation

Final focused run: **95 passed**, covering both new tools, provider synchronization,
scalable retrieval, context assembly, model wiring and core graph behavior. Ruff
and scoped tracked-file whitespace checks passed. The full repository suite,
browser suite, live providers and not-yet-supplied production LLM were not tested.
The repository's existing `*.md` ignore rule makes this new guide local-only;
`benchmarks/README.txt` and the JSON evidence provide nonignored summaries.

## Physical-erasure evidence and retained-file audit

Recommended first scope: disposable fixture verification and read-only retained-file
audit. No automatic production purge, journal rewrite or backup deletion is enabled.

```powershell
.venv/Scripts/python.exe -m hivemind.memory_validation `
  --output <repo>/benchmarks/erasure-results-new.json erasure-fixture
```

The fixture creates two content versions of a synthetic memory, indexes it, makes a
backup and spool, then uses canonical deletion and index cleanup. On **only this
temporary database**, it enables SQLite secure deletion, optimizes FTS, checkpoints
WAL, compacts with VACUUM and checkpoints again. Blocked checkpoints fail.

Recorded result: `benchmarks/erasure-results-2026-10-17.json`. No memory versions or
derived chunks remain; compacted SQLite/WAL contain no plaintext canary. The retained
backup and spool still contain it and are correctly detected. Consequently,
`physical_erasure_verified` is false. This is a successful detection test, not a
claim that all copies have been erased.

For an explicitly inventoried, quiesced set of files:

```json
[
  {"label":"canonical","category":"sqlite","path":"<private-external>/store/canonical.sqlite"},
  {"label":"wal","category":"wal","path":"<private-external>/store/canonical.sqlite-wal"},
  {"label":"spool","category":"spool","path":"<private-external>/capture/spool.jsonl"},
  {"label":"backup","category":"backup","path":"<private-external>/backups/snapshot.sqlite"}
]
```

```powershell
.venv/Scripts/python.exe -m hivemind.memory_validation `
  --output <repo>/benchmarks/retained-audit-new.json audit `
  --inventory <private-external>/audit-inventory.json --marker-file <private-external>/non-secret-canary.txt
```

Marker bytes include any whitespace/newline in the marker file. Use synthetic,
non-secret canaries, never prompts or credentials. The scanner reads explicit
absolute files, detects UTF-8/UTF-16 matches across chunk boundaries, checks file
stability, bounds file sizes and refuses symlinks/reparse ancestors. Missing,
oversized, changed or unreadable inputs make the scan incomplete. Output contains
inventory labels, sizes, status and hashes, not content, paths or marker text.
Use nonsensitive labels. A found marker or incomplete scan returns nonzero.

Inventory completeness is the operator's responsibility. Include old versions,
dependent memories, command/event journals, artifact stores, adapter spools,
runtime journals, backup generations and filesystem snapshots. This scanner does
not decode compressed/encrypted copies, prove SSD block sanitization, find unknown
copies or establish remote erasure. Absence of a plaintext marker is never reported
as whole-system physical erasure.

Before enabling production erasure, define retention/legal holds, dependency closure,
operator approval, service quiescence, file ownership, backup tombstones and restore
reconciliation. Preserve unknown-effect/recovery evidence until its disposition is
explicitly approved. Test failed/interrupted erasure and stale-backup restore before
enabling destructive operations.
