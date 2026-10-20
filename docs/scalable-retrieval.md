# Scalable retrieval and exact token budgeting

This document describes the large-scale retrieval path and the runtime-specific exact
token budgeting added on top of the existing query surfaces. Everything is local and
offline: no network calls, no model or tokenizer downloads, no cache mutation, and no
API-selected code modules, tokenizer files, or hashes.

## 0. Concurrency and lock discipline (two-phase snapshot/compute/recheck)

Retrieval and indexing never hold the canonical store lock or the index lock during
expensive work, so heartbeats and other canonical reads stay responsive:

- **Phase A (short `store.lock`):** `_authorize_request` re-fetches the *current*
  principal row (role/project/agent), checks current project access, and evaluates the
  freshness gate. Nothing expensive runs under the lock.
- **Phase B (no locks):** the candidate metadata snapshot and FTS5/vector ranking run
  on a **dedicated read-only SQLite connection** (`Indexes.ro_db`, WAL reader). All
  heavy statements execute under a per-query **progress-handler deadline** set on that
  connection only — the canonical connection's handler is never touched. An expired
  deadline aborts the statement (`interrupted`) and is reported as
  `scan.time_budget_exceeded` with partial results, instead of blocking.
- **Trusted CPU callbacks** (the query encoder, token counters) are invoked with *no*
  locks held. They cannot be preempted in-process, so the time budget is a *soft*
  deadline checked around them — a slow encoder flags the result afterwards; it is not
  killed.
- **Phase D (short `store.lock` per emitted result):** materialization re-fetches the
  current principal row again and revalidates visibility, revision, and full
  evidence-recursive `valid_memory`. Revocation or rebinding mid-compute yields no
  private results; a deleted principal yields none at all.
- **Index drain** (`process_one`) is snapshot → encode → validate-and-commit: the job
  and row are read under short locks, `encoder.documents` runs lock-free, and the
  commit revalidates the job revision inside the transaction (obsolete work is
  discarded). Concurrent drain and search are safe: ranking uses the read-only
  connection, commit phases take `store.lock`/`self.lock` briefly.
- All responses from `search`/`search_page` carry `hivemind_detached_work: True`, and
  the HTTP wrappers in `retrieval_runtime.install` / `context_assembly.install` execute
  the work via `asyncio.to_thread`, so the async event loop and any parent request
  mutex are never blocked by retrieval, encoding, or counting. In
  `context:assemble` the marker is part of the measured envelope, preserving the
  envelope-inclusive exact `token_count` guarantee.

## 1. Scalable retrieval (`hivemind/indexes.py`)

### Candidate discovery beyond the first 1000

`Indexes.search` previously pre-authorized candidates via `Store.list(..., 1000)`,
silently truncating history. Candidates now stream straight from canonical SQL:

- `_authorized_meta(kinds, project, principal, scan_limit, deadline)` pages keyset-style
  over the composite `resources_scan(project,kind,deleted,seq,id)` index (created
  additively by `Indexes.__init__`; the canonical schema in `storage.py` is untouched),
  in 512-row chunks ordered by `seq DESC`, on the read-only index connection.
- **Bounded memory:** bodies are parsed transiently for authorization/validity and
  discarded. The retained structure is `id -> (kind, revision)` metadata only, so a
  200k-record corpus of 131 KB bodies never materializes in Python memory.
- **Visibility in SQL:** the exact `Store.visible` predicate (project match,
  `private_agent`, message from/to rules) is pushed into the scan query, with
  `Store.visible` still applied in Python as defense in depth. Row-examination counts
  and `scan_truncated` flags therefore cover only rows the principal may see: hidden or
  private rows are never counted and their existence/volume cannot be inferred from
  scan metadata.
- **Finite work bounds:** the scan stops at `scan_limit` rows per kind (default 20000,
  max 200000) or at a wall-clock budget (`SCAN_TIME_BUDGET_MS`, default 20 s, soft
  around trusted CPU callbacks), enforced *inside* long FTS/vector statements by the
  index connection's progress handler as well as between pages and at materialization.
  Exhaustion is flagged (`scan.scan_truncated`, `scan.time_budget_exceeded` +
  warnings), never silent.
- Memory candidates pass JSON-local state checks (verified status, `memory_state` for
  expiry/dispute/invalidation) at snapshot time; evidence-recursive `valid_memory`
  runs at materialization under a short canonical lock. Work claims require
  active/reserved + unexpired at both phases.

### Bounded ranking; materialize only output

- **Lexical:** one SQL query intersects the FTS5 `lexical` index with canonical
  visibility/kind/deleted predicates, ordered by bm25, hard-limited to `rank_depth`
  (+1 probe row) hits. Only candidate IDs cross the SQL boundary.
- **Semantic:** exact sqlite-vec cosine distances over the authorized-ID set in
  300-ID batches (SQL variable maps stay bounded regardless of corpus size), keeping
  only a bounded top-`rank_depth` heap. **No ANN approximation is used or claimed.**
  Stale-revision chunks are dropped during scoring.
- **Fusion:** deterministic RRF over the two bounded rankings, ordered by
  `(-score, resource_id)`; ties stable across calls and pages.
- `rank_depth` defaults to 4096 (max 65536); when a ranking exceeds the window,
  `scan.rank_truncated` is set and a warning is emitted — deeper history is flagged,
  never silently dropped. Full result bodies are materialized (with canonical recheck)
  only for emitted results.

### Canonical recheck per result

`_recheck` re-reads each emitted result from canonical storage: the row must still
exist, be visible to the requesting principal, match the ranked revision, and still be
valid memory / an active claim. Index lag cannot resurrect revoked, deleted, hidden, or
superseded snapshots. Freshness lag is reported (`freshness.vector_pending_jobs`,
"Semantic index is behind canonical state"); `min_ingestion_seq` fails closed with
`index_not_ready` (503).

### Cursor semantics (`search_page`)

- `next_cursor` = `base64url(canonical-json) + "." + hmac_sha256(process_local_key)`.
  Payload: `{v, q, s, id, n}` — version, **fingerprint binding the authenticated
  principal (id/role/agent), project, query, and scan policy** (scan_limit,
  rank_depth), last emitted score, last emitted *authorized* resource id, page number.
  Nothing about unauthorized candidates is carried.
- Bad signature/format → `cursor_invalid` (422). Fingerprint mismatch — different
  principal, query, or scan policy — → `cursor_query_mismatch` (422). Both fail before
  any retrieval work.
- **Restart semantics:** the signing key is process-local; a service restart rotates it
  and old cursors fail closed with `cursor_invalid`.
- **Snapshot semantics:** pages are *not* an isolated snapshot. Each page rechecks
  canonical state at materialization time, so results always reflect current
  authorization; entries may appear/disappear between pages if canonical state changes.
  `has_more` is a hint (a later page may be empty after rechecks).

### Backward compatibility

`Indexes.search(principal, request)` keeps its signature and result keys
(`results`, `freshness`, `embedding_profile`, `lexical_fallback`, `warnings`), adding a
`scan` block. `process_one`, `drain`, and `query_graph` are untouched; the memory
worker's index job processing is unaffected.

## 2. Context assembly (`hivemind/context_assembly.py`)

The same guarantees now apply to the assembly candidate path, with existing budget
behavior preserved:

- `_scan` is a keyset-paged generator with visibility pushed into SQL; bodies are
  consumed transiently. `scan.scanned[kind]` counts authorized candidates and
  `scan.scan_truncated` reflects visible rows only — hidden rows cannot leak through
  assembly scan metadata either.
- Ranking retains only lightweight stubs (id/revision/seq + scoring), pruned to a
  4096-candidate window (`scan.candidates_truncated` + warning when pruned — far above
  what any token budget ≤ 32000 can place).
- `_materialize_candidate` re-fetches each ranked candidate's canonical body with
  visibility/revision/validity recheck while placing items; anything that changed since
  the scan is omitted.

## 3. Exact token budgeting (`hivemind/retrieval_runtime.py`)

### Counter registry (operator-side bindings only)

| counter | binding | exactness |
| --- | --- | --- |
| `local_hf_tokenizer` | target tokenizer id (`org/model`) resolved from the local HF cache; optional operator SHA-256 pin of `tokenizer.json` — the hashed file is exactly the file loaded | exact for that tokenizer |
| `tiktoken` (prepared) | operator-registered prepared `tiktoken.Encoding` objects (`register_encoding`), optionally with model aliases | exact for that encoding |
| Anthropic | — | unavailable offline: counting requires an external API call, which is never performed |
| `utf8_bytes_upper_bound` | none | never exact; fallback that cannot satisfy `require_exact` |

The registry **never imports tiktoken, never calls `get_encoding`/`encoding_for_model`
or any constructor that can lazily download encoding data, and never reads, writes, or
deletes cache files**. Vendor encodings enter only as operator-prepared objects,
validated by a self-test encode at registration (`encoding_invalid`, 422 on
corrupt/incompatible objects). Client/API input can never select tokenizer files,
modules, or hashes.

The hash-pinned local tokenizer load is TOCTOU-safe: the file's bytes are read once,
SHA-256'd, and the tokenizer is parsed *from those same verified bytes*
(`Tokenizer.from_buffer`) — never re-read from the path afterwards, and no fallback to
any other candidate file when the pin fails or the bytes are corrupt.

`resolve(target, model_hash=None, require_exact=False, counter=None)`:

- with `require_exact=True`, raises `Problem("exact_counter_unavailable", 422)`
  **before any counting or retrieval work** when no counter bound to the target exists;
- injected bare callables are trusted as exact by their injector (existing
  `context_assembly` semantics); a `Counter` self-declared non-exact is refused under
  `require_exact`;
- without `require_exact`, resolution degrades to the honestly labeled UTF-8 upper
  bound (`fallback=True`).

### Serialized packet vs vendor overhead

`budget_serialized(value, counter=..., target_tokenizer=..., budget=..., wrap=...)`
measures the fully serialized packet (optionally through the caller's envelope `wrap`)
and returns `{token_count, counter_id, method, exact_for_target, budget, fits,
covers_serialized_canonical_packet, vendor_overhead}`. The count is exact for the
serialized canonical packet **only**; vendor-side hidden system prompts, tool schemas,
and API overhead are never locally observable and are always reported as excluded.
Reserve headroom for that overhead separately. The embedding tokenizer is never
substituted for a runtime tokenizer.

### HTTP surface (`retrieval_runtime.install`)

```python
retrieval_runtime.install(app, service, principal_dep, command, envelope,
                          counters={target: counter},   # operator-bound counters
                          scan_limit=20000,             # operator-fixed scan policy
                          rank_depth=4096,
                          registry=None)                # default: process registry
```

Registers:

- `POST /v1/retrieval:search-page` — body is `SearchQuery` + `cursor`
  (`SearchPageBody`). Scan policy (`scan_limit`/`rank_depth`) is operator-fixed;
  clients cannot widen it or inject tokenizer files/hashes. `project_access` is
  enforced before any work; the search itself runs via `asyncio.to_thread` and the
  response carries `hivemind_detached_work: True`.
- `GET /v1/retrieval:operations` — `{operations, counters}` self-describing listing
  for discovery/MCP manifests (from the `OPERATION_DESCRIPTORS` constant and
  `CounterRegistry.describe_counters()`).

### Assembly seams

- `context_assembly.assemble(..., require_exact=False, registry=None)` — with
  `require_exact=True`, the counter is resolved via the operator-configured `registry`
  (or the process default) and assembly raises `exact_counter_unavailable` **before any
  candidate scanning**.
- `context_assembly.install(..., require_exact=False, registry=None)` passes both
  through to `POST /v1/context:assemble`; `counter=`/`counters={target: c}` remain
  the per-target injection points. The strict policy is operator-fixed at install —
  never client-selectable — and the route runs assembly via `asyncio.to_thread` with
  the `hivemind_detached_work` marker inside the measured envelope.
- Packet `tokenization.vendor_overhead` (`included=false` + note) accompanies the
  existing `method` / `exact_for_target_tokenizer` fields.

### Operator configuration (`model_wiring.integration_options`)

Operators wire exact budgeting and scan policy through the `--agent-integrations`
JSON file (`hivemind/model_wiring.py`), section `runtime_tokenizers`. Two shapes are
accepted; both are fully offline and both hash-pin *and load the exact same bytes*
(TOCTOU-safe `Tokenizer.from_buffer` on the hashed bytes — never a re-read from the
path, never a download):

**Strict object form (recommended):**

```json
{
  "runtime_tokenizers": {
    "tokenizers": [
      {"model_id": "org/model",
       "tokenizer_file": "C:/operator/prepared/tokenizer.json",
       "expected_sha256": "<64 hex chars of that exact file>"}
    ],
    "require_exact": true,
    "scan_limit": 20000,
    "rank_depth": 4096
  }
}
```

- Unknown keys at every level are denied; `require_exact` must be a real boolean;
  `scan_limit` (1..200000) and `rank_depth` (1..65536) must be finite integers (never
  booleans/floats; non-finite JSON like `Infinity` is rejected at parse time).
- All pinned counters are registered into **one shared**
  `retrieval_runtime.CounterRegistry` passed to both `context_assembly.install` and
  `retrieval_runtime.install`, so the same exact counters and registry serve assembly
  and retrieval discovery (`GET /v1/retrieval:operations`).
- `require_exact: true` is operator-fixed: requests whose `target_tokenizer` has no
  bound counter fail with `exact_counter_unavailable` (422) **before any scanning or
  model work**; an unavailable remote/vendor counter (e.g. Anthropic, no offline
  tokenizer) fails the same way rather than degrading silently.
- `scan_limit`/`rank_depth` are operator-fixed retrieval/assembly bounds; clients
  cannot widen them (the request schemas forbid those fields entirely).
- The pinned tokenizer files must be explicitly local and operator-trusted: no
  API-supplied paths or hashes are accepted anywhere, and nothing is downloaded.
  The embedding tokenizer is never used as a stand-in exact counter for a runtime
  target without an explicit binding.

**Legacy list form (unchanged compatibility):**

```json
{
  "runtime_tokenizers": [
    {"model_id": "org/model", "tokenizer_file": "...", "expected_sha256": "..."}
  ]
}
```

produces exactly the historical behavior — `{"context_assembly": {"counters": ...}}`
only, no strict policy, no retrieval wiring, and unbound targets degrade to the
honestly labeled UTF-8 byte upper-bound fallback.

## 4. Limits

- Candidate history per kind is bounded by `scan_limit` (default 20000, max 200000
  visible rows), ranking windows by `rank_depth` (default 4096, max 65536), wall clock
  by the scan time budget; all exhaustion is flagged via `scan.*` / `exhausted.*`.
- Cursors are process-local and ephemeral; pages are not a snapshot (see above).
- Semantic ranking requires the pinned local EmbeddingGemma encoder; without it,
  `semantic` mode fails with `index_not_ready` and `hybrid` reports `lexical_fallback`.
- Exact token counts exclude vendor hidden overhead by construction.

## 5. Tests

- `tests/test_scalable_retrieval.py` — >1000-record datasets with an old relevant
  authorized result; deterministic multi-page walks; cursor tamper / cross-query /
  **cross-principal replay** / restart-rotation rejection; revoked, hidden, deleted,
  and version-changed exclusion under index lag; freshness flags and fail-closed
  `min_ingestion_seq`; scan-bound enforcement; **hidden-corpus scan-metadata
  invariance**; rank-depth window bounds; lightweight metadata assertion; exact
  bounded semantic top-k; **heartbeat responsiveness while the query encoder is
  blocked** (canonical lock stays free); **revocation and principal deletion
  mid-compute** producing no private results; concurrent drain/search safety; SQLite
  progress-handler time caps (index connection only); drain snapshot-encode-commit
  lock probing; the `retrieval:search-page` / `retrieval:operations` routes with
  operator-fixed policy.
- `tests/test_runtime_token_budget.py` — fixture tokenizer exactness and hash pinning
  with TOCTOU safety (parsed bytes == hashed bytes; `from_file` forbidden; swapped or
  corrupt pinned files refused with no path fallback), UTF-8/emoji edges vs byte
  counts, fail-before-execution for unbindable targets, prepared-encoding registration
  (exact model binding), corrupt/incompatible encoding refused at registration,
  socket/urlopen traps proving resolution performs no network, a guard that the
  registry never imports tiktoken, injected-counter trust rules, packet-vs-overhead
  accounting, self-description schema, `assemble(require_exact=True)` failing before
  any candidate scan, and the `ca.install(registry=..., require_exact=True)` HTTP seam
  (strict operator-fixed policy, detached-work marker inside the measured envelope).
- `tests/test_exact_budget_configuration.py` — end-to-end
  `create_app(capability_options=integration_options(...))` wiring: strict object
  form binding a hash-pinned fixture tokenizer (200 + envelope-inclusive exact
  `token_count` measured with the operator counter), unbound/absent targets refused
  422 before any candidate scan, offline traps (no socket egress/urlopen during
  configuration or requests), hash-mismatch refused at configuration time, unknown or
  loose configuration denied (unknown section/item keys, non-boolean `require_exact`,
  boolean/out-of-range/float scan bounds, non-finite JSON), legacy list-form
  backcompat (fallback preserved, no strict policy), and operator-fixed
  `scan_limit`/`rank_depth` that clients cannot widen (request fields rejected 422;
  exhaustion honestly flagged; assembly carries the same bound).
- `tests/test_context_assembly.py` — plus hidden-corpus scan-metadata invariance for
  the assembly path.
