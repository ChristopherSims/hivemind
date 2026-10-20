# Background memory pipeline

An **opt-in** extension that turns explicitly selected, revision-pinned evidence into
**unverified memory proposals** using an **injected local model callable**, then records
advisory duplicate/contradiction suggestions and repository freshness marks.

It is disabled by default. It never loads, downloads, discovers, or pays for a model:
the host process must inject a configured extractor, and the service never accepts a
model name, path, prompt, or tool list from a request. There is no default background
thread, hooks, or scanners. The integrated host applies repository-freshness filtering
only when this pipeline is enabled with a configured snapshot source.

The pipeline is **proposals only**. Nothing here verifies, rejects, quarantines,
disputes, invalidates, supersedes, or deletes a memory, and nothing here widens scope,
privacy, or evidence revision. Those decisions stay with the existing memory lifecycle
and human review.

## Install

```python
from hivemind.memory_pipeline import PipelineConfig, install

pipeline = install(app, service, principal, command, envelope,
                   config=PipelineConfig(enabled=True),
                   extractor=my_local_extractor,
                   encoder=None,            # defaults to the local hashing encoder
                   snapshot_provider=None,  # or config.workspace for read-only git
                   background=False)
```

* `principal`, `command`, `envelope` are the app's own authentication dependency,
  idempotent mutation wrapper, and response wrapper. This module never replaces them.
* `install` returns the `MemoryPipeline` and also sets `app.state.memory_pipeline`.
  Prefer installing once; explicit reinstallation replaces this module's handlers
  and requests shutdown of its previous background worker.
* The integration layer (`hivemind/capabilities.py`) calls feature modules as
  `module.install(app, service, principal, command, envelope)`. Adding
  `"memory_pipeline"` is installed by default with routes **disabled**;
  the operator then enables them by passing a config and extractor.
* `background=True` starts a daemon worker **only** when the pipeline is enabled and an
  extractor is configured. Otherwise it is a no-op. `start_worker()`/`stop_worker()`
  control it explicitly; `tick()` runs one bounded pass without a thread.

`PipelineConfig` (frozen dataclass, validated on construction):

| Field | Default | Meaning |
| --- | --- | --- |
| `enabled` | `False` | Opt-in switch for every route |
| `method` | `model-proposal-v1` | Method label stored on jobs and proposals |
| `model_id` | `injected-local-extractor-v1` | Configured model identity used in fingerprints |
| `max_candidates` | `4` | Ceiling for proposals per job (1–8) |
| `max_source_bytes` | `4096` | Ceiling per source text and per proposal text |
| `max_output_bytes` | `16384` | Ceiling for raw model output (256–65536) |
| `prompt_bytes` | `16384` | Ceiling for the rendered prompt |
| `max_compare` | `32` | Existing memories compared per new proposal |
| `max_attempts` | `3` | Attempts before a failed job becomes `dead_letter` |
| `lease_seconds` | `120` | Job lease before a stalled job can be reclaimed |
| `duplicate_similarity` | `0.92` | Cosine threshold for a duplicate suggestion |
| `contradiction_similarity` | `0.6` | Cosine threshold for a contradiction suggestion |
| `workspace` | `None` | Repository path used for read-only snapshots |
| `allow_injected_snapshot` | `False` | Accept caller-supplied repository snapshots |
| `worker_batch` | `2` | Jobs per background `tick()` |

### Extractor contract

`extractor(prompt: str) -> str`, returning **raw, untrusted text**. If the signature
accepts `max_output_bytes`, it is passed as a keyword argument. An optional
`extractor.model_id` attribute is recorded as provenance (the fingerprint uses
`PipelineConfig.model_id`, which is stable across restarts).

### Encoder contract

`encoder.vectors(texts) -> list[list[float]]` (or a plain callable). Output is validated
(shape, finite numbers, non-zero norm, consistent dimension) and L2-normalized by the
pipeline. The default `LocalHashingEncoder` is a deterministic dependency-free
bag-of-words hash encoder: it never imports `torch`, `transformers`, or
`sentence-transformers`, so parallel test runs stay light. An injected encoder that
fails is recorded as `suggestion_error`; it never blocks proposal creation.

## Queue jobs

`POST /v1/memory-pipeline:enqueue` (idempotency key required):

```json
{
  "project_id": "project_id",
  "agent_id": "agent_id",
  "requester_run_id": "run_id",
  "source_refs": [{"kind": "board_post", "id": "post_id", "revision": 3}],
  "scope_kind": "agent",
  "scope_owner_id": null,
  "pin": {"repository_id": "repo_...", "branch": "main", "observed_commit": "..."},
  "max_candidates": 4,
  "max_source_bytes": 4096
}
```

Rules (identical to deterministic extraction, so both paths agree):

* Actor, project, run, and scope bindings are validated before any write; a terminal
  requester run is rejected (`terminal_run`).
* Only board posts labeled `finding`, `decision`, or `answer` and explicitly selected
  textual artifacts are supported. Ordinary board chat is skipped (`not_a_finding`),
  as are empty or oversized sources (`empty_or_oversized`) and beyond-limit sources
  (`candidate_limit`).
* `revision` is pinned when it matches the current revision; a mismatch is
  `stale_evidence` (409). Sources the caller cannot see (for example another actor's
  private artifact) raise `not_found`.
* A private source stays private: pinning it outside its own agent scope is
  `evidence_scope_violation` (403), and a private source makes the job private.
* Request limits can only *lower* the configured ceilings.

Each job is a canonical resource of kind `memory_pipeline_job` with revisions,
project scoping, and visibility. Agent- and run-scoped jobs are private to that agent;
task/project-scoped jobs with only public sources are project-visible. Job bodies store
source ids, revisions, byte counts, and text digests — **never source text and never raw
model output**. Queue rows are deliberately excluded from the embedding/graph index.

Re-enqueuing the same request returns the same job (`created: false`). The fingerprint
binds pipeline version, method, project, actor, scope, repository pin, pinned source
revisions, effective limits, and configured model id — so a changed revision needs a new
job and a new decision.

## Processing

`POST /v1/memory-pipeline:process` (`{"project_id": ..., "job_ids": [], "limit": 1,
"include_suggestions": true}`) processes visible jobs synchronously.
`POST /v1/memory-pipeline:tick` (`{"limit": 2}`) runs one bounded pass over every
project and requires an administrator.

Both fail explicitly without a configured extractor:

* `memory_pipeline_disabled` (409) — `enabled` is false.
* `memory_pipeline_model_not_configured` (409) — no injected extractor. The API can
  never select or enable a model.

The two switches are separate on purpose. With `enabled: false` every route except
`GET /v1/memory-pipeline:status` returns `memory_pipeline_disabled` (409) and the
extension is inert. With `enabled: true` and **no** extractor, inspection and advisory
surfaces (`:jobs`, `:suggestions`, `:freshness`, `:retrieve`) work while `:enqueue`
queues and `:process`/`:tick` fail with `memory_pipeline_model_not_configured` — so a
deployment can use freshness marks and retrieval filtering without any model at all.

A job runs **under the authority of the principal that enqueued it**, never the caller's.
An administrator can trigger another actor's visible job, but the resulting proposals are
owned and scoped exactly as if the enqueuer had run it, so triggering cannot amplify
privilege. Jobs are leased (`lease_expires_at`) and a stalled `running` job is reclaimed;
job writes use optimistic revisions, so concurrent processors cannot double-create.

Proposals are created through `Service.memory`, so every existing scope, privacy,
evidence, version, and deletion rule still applies:

* `status: proposed`, `origin: extracted`, `private`/scope from the job only.
* Evidence references are the pinned `(kind, id, revision)` triples. The model can only
  *select* from the pinned set; it cannot add evidence, change a revision, choose scope
  or privacy, set a status, or set validity.
* `validity` comes from the job's `pin` (repository id/branch/observed commit), recorded
  as the enqueuer reported it, never as verified truth.
* `extraction` records method, pipeline version, live model id, agent, run, job id, job
  and source fingerprints, output digest, confidence, and the proposal fingerprint.
* Only a human/admin/observer decision can verify or reject a proposal; review outcomes
  still notify the proposing agent through the existing notification path.

Accepted model output is exactly one JSON object:

```json
{"proposals": [{"title": "string", "text": "string", "category": "semantic",
                "source_ref_ids": ["post_id"], "confidence": 0.5}]}
```

Rejected output leaves the job `failed` (retryable) or `dead_letter` with a bounded
reason code, and **no** proposals. Raw output is discarded; only its digest and byte
count are stored. Reason codes: `not_text`, `output_too_large`, `control_characters`,
`not_json`, `empty_output`, `not_an_object`, `unexpected_keys`, `proposals_not_a_list`,
`too_many_proposals`, `proposal_not_an_object`, `proposal_unexpected_keys`,
`invalid_title`, `invalid_text`, `text_too_large`, `invalid_category`,
`invalid_source_ref_ids`, `unpinned_source_ref`, `invalid_confidence`.

Terminal versus retryable failures:

| Condition | State |
| --- | --- |
| Malformed or oversized model output, `extractor_error` | `failed`, then `dead_letter` at `max_attempts` |
| Pinned revision changed (`stale_evidence`) | `dead_letter` (a new enqueue is required) |
| Source deleted or no longer readable (`source_unavailable`) | `dead_letter` |
| Enqueuer principal missing | `dead_letter` (`principal_unavailable`) |
| Evidence scope violation at process time | `dead_letter` (`evidence_scope_violation`) |

Model output is never executed, interpolated into commands, treated as instructions, or
allowed to request tools. Evidence text itself stays untrusted data.

## Queue inspection

`GET /v1/memory-pipeline:jobs?project_id=...&state=&limit=50` returns visibility-filtered
job resources, per-state counts, and the model identity.
`GET /v1/memory-pipeline:status?project_id=...` returns `enabled`, model and encoder
identity, effective limits and thresholds, repository configuration, queue and suggestion
counts, worker state, and `status_effect: none`. Neither route exposes source text, raw
model output, prompts, or credentials.

## Suggestions

`GET /v1/memory-pipeline:suggestions?project_id=...&kind=&include_dismissed=false&include_stale=false&limit=50`

For each new or reused proposal the pipeline compares it with existing memories in the
**same project, scope kind, scope owner, and privacy** only (at most `max_compare`,
most recent first), using the injected encoder:

* `duplicate` — cosine ≥ `duplicate_similarity`.
* `contradiction` — cosine ≥ `contradiction_similarity` **and** at least one
  deterministic, explainable signal: `negation_conflict`, `polarity_conflict`, or
  `value_conflict`.

Suggestions are canonical `memory_suggestion` resources with `state: proposed` and
`status_effect: none`; they are private whenever either side is private. Listing
validates both memories at the current revision: changed revisions hide the observation
unless `include_stale=true` (`hidden_stale`), and unreadable evidence hides it entirely
(`hidden_unavailable`). When a repository snapshot is available, each side reports
`currentness`/`currentness_reasons`, so a moved branch or commit is visible without
changing any fact.

`POST /v1/memory-pipeline/suggestions/{id}:dismiss` (`{"expected_revision": n,
"reason": "..."}`) records an advisory dismissal only. It revalidates visibility and
evidence, is idempotent under one key, returns `412` on a stale revision, and `409`
(`suggestion_already_decided`) for a second decision. Dismissing a suggestion never
touches the memories it points at, and the API exposes no "accept"/"supersede" action.

## Freshness and retrieval filtering

`POST /v1/memory-pipeline:freshness` records currentness marks for visible memories
(optionally a bounded `memory_ids` list) against one repository snapshot. Each mark is a
canonical `memory_freshness_mark` resource with `state` (`current`, `drifted`,
`untracked`), `reasons` (`repository_mismatch`, `branch_drift`, `commit_drift`,
`snapshot_*_unknown`, `no_repository_pin`), the pinned and observed values, and
`status_effect: none`. Marks are idempotent per memory revision and snapshot.

Snapshots come from exactly one configured source:

* `snapshot_provider()` returning `{"repository_id", "branch", "commit"}` (tests,
  custom hosts).
* `config.workspace` — `git -C <workspace> rev-parse --show-toplevel`,
  `--abbrev-ref HEAD`, and `--verify HEAD` only, with `GIT_OPTIONAL_LOCKS=0`, no shell,
  and a timeout. No lock file, index, ref, config, or user file is written. Failures are
  explicit: `workspace_unavailable` (503), `workspace_not_a_repository` (503).
* Caller-supplied `snapshot` in the request body — accepted **only** when
  `allow_injected_snapshot` is true, otherwise `snapshot_injection_disabled` (403).

Requests never carry a filesystem path, and no absolute path is returned:
`repository_identity(path)` hashes the resolved top-level directory into a stable
`repo_...` id that callers may pin in `MemoryValidity.repository_id`.

`POST /v1/memory-pipeline:retrieve` is the explicit, opt-in retrieval filter
(`{"memory_ids": [], "require_current": true, "include_untracked": true, "snapshot":
null}`). It returns `included`/`excluded` entries with `currentness` and
`excluded_because` (`repository_drift`, `no_repository_pin`), is read-only
(`mutated: false`), writes no marks, and changes no memory. Without a configured
snapshot it fails explicitly (`repository_snapshot_unavailable`, 503) instead of
silently reporting everything as current.

## Agent interfaces

```python
from hivemind.memory_pipeline import (MemoryPipeline, PipelineConfig, EnqueueRequest,
                                      ProcessRequest, FreshnessRequest,
                                      RetrievalFilterRequest, SuggestionDismiss, git_snapshot,
                                      repository_identity)

pipeline.enqueue(principal, EnqueueRequest.model_validate(payload))
pipeline.process(principal, ProcessRequest.model_validate({"project_id": project_id}))
pipeline.tick(limit=2)                      # background pass, no HTTP
pipeline.suggestions(principal, project_id, kind="contradiction", include_stale=True)
pipeline.dismiss_suggestion(principal, suggestion_id, SuggestionDismiss.model_validate({...}))
pipeline.freshness(principal, FreshnessRequest.model_validate(payload))
pipeline.retrieval_filter(principal, RetrievalFilterRequest.model_validate(payload))
pipeline.status(principal, project_id)
```

Route paths live under `/v1/memory-pipeline...` on purpose: they never collide with
`/v1/memories/{memory_id}` or `/v1/memory-proposals/{id}`, so registration order can
never shadow an existing route. The request models subclass `hivemind.models.Model`, so
they are strict, size-bounded, and exported by `capabilities.model_modules()` for the
schema export.

## What this does not do

* No model download, discovery, API key, paid call, or request-selected model.
* No automatic verification, quarantine, supersession, dispute, or deletion.
* No automatic source discovery, transcript summarization, or scope widening.
* No prompt/tool execution: model output is parsed as bounded JSON, never evaluated.
* Canonical retrieval including `context:query` and `context:assemble` excludes drifted
  repository pins when the host enables a configured snapshot source. Stored fact
  status/revision never changes automatically, and detail/history stay reviewable.
* No claim that duplicate/contradiction thresholds are quality benchmarks; they are
  tunable, explainable signals on lexical or injected embeddings.

## Limitations

* Jobs, suggestions, and marks are canonical rows; nothing prunes them. Listings are
  bounded (500–1000 recent rows per kind), so very large histories need an operator
  retention policy.
* The `LocalHashingEncoder` is lexical. Injected encoders are only validated for shape,
  finiteness, and normalization, not for semantic quality.
* Suggestion comparison is bounded to `max_compare` most recent same-scope memories, and
  contradiction signals are deliberately conservative substring/token heuristics.
* Job listings are visibility-filtered, so agent-private jobs are not inspectable by
  humans or observers; suggestions are the human-facing surface.
* Freshness reflects whatever the configured snapshot reports. A `snapshot_provider` is
  trusted host code; caller-injected snapshots require explicit operator opt-in.
* `freshness` marks every visible memory in a project when no `memory_ids` are given, so
  large projects should pass explicit ids.
* The published `spec/openapi.json` and `spec/schemas` only include these routes and
  models after the integrator wires `install(...)` into `create_app` and re-runs
  `scripts/export_schemas.py`.

## Integration requirements

1. Routes are already installed by `create_app`. Supply trusted options through
   `capability_options={"memory_pipeline": {"config": ..., "extractor": ...}}`.
2. Supply `config=PipelineConfig(enabled=True, ...)` plus a local extractor; without both,
   every route fails explicitly and no model is ever contacted.
3. Optionally set `workspace` (read-only git snapshots) and a `snapshot_provider`.
4. The host wires repository currentness into canonical active-memory reads. The module
   also keeps the explicit freshness and retrieval-filter surfaces available.
5. Regenerate the published contract (`scripts/export_schemas.py`) after wiring.
