# Artifact Contracts

Task-bound, typed deliverable contracts over canonical artifacts
(`hivemind/artifact_contracts.py`). A contract declares the named, typed outputs a
producer task must deliver; a dependent task may not be claimed or started until
every contract bound to its dependency is satisfied by **exact evidence** — never
by a producer's self-reported claim.

Default off. Install with `ContractPolicy(enabled=True)` (strict bools only);
with the feature off, no routes are registered, no handle is attached, and the
scheduling gate is a pass-through.

## Model

Three canonical resource kinds:

- `artifact_contract` — `{project_id, task_id, title, outputs[], status,
  created_by}`, revision-controlled with exact CAS (`expected_revision`).
  `status` is `active` or `revoked`.
- `artifact_contract_binding` — durable applicability ledger
  `{task_id, contract_id, state: "bound"}` written atomically with the contract.
  The gate enumerates applicability from this ledger **without** visibility
  filtering: a hidden applicable contract holds the gate
  (`contract_not_visible`, no id leaked), and a physically deleted contract holds
  as `contract_deleted_unreleased`. Deletion can never silently liberate a
  planned dependency; only explicit authoritative revocation under
  `ContractPolicy(revocation_releases=True)` releases it (default: revocation
  holds forever).
- `artifact_submission` — immutable evidence + computed verdict:
  `{contract_id, contract_revision, task_id, run_id, execution_id,
  artifacts: [{name, artifact_id, revision, sha256}], verdict, diagnostics,
  receipts[], submitted_by, self_report: false}`. Receipts are immutable
  (submissions are never updated), scoped to the submitting principal and
  project, and bound to the contract revision and execution id.

### Output types

Each contract has 1..16 uniquely named outputs (`^[A-Za-z0-9_.-]{1,64}$`):

| type   | required metadata                                   | validation |
|--------|-----------------------------------------------------|------------|
| `json` | `schema` (bounded safe subset, see below)           | inline content text parsed as JSON (non-JSON constants rejected), validated deterministically |
| `text` | `media_type` = `text/plain`/`text/markdown`; optional `min_length`/`max_length` | media metadata must match the artifact's content exactly |
| `file` | `media_type`, optional `max_bytes` (≤ 1 MiB), optional `checks` (≤ 4 per output) | media + byte bound; checks run through the trusted verifier, bound to the exact evidence |

### Safe JSON-schema subset

Data-only keywords: `type, properties, required, additionalProperties, items,
enum, const, minimum, maximum, minLength, maxLength, minItems, maxItems,
description`. Types: `object, array, string, number, integer, boolean, null`.

Refused at contract write time (422): `$ref`, `definitions`/`$defs`, combinators
(`anyOf`/`oneOf`/`allOf`/`not`/`if`…), `format`, `pattern`/`patternProperties`
(regex DoS), any other keyword; non-string/unsupported `type` values; schemas
over 16 KiB, deeper than 16 levels, more than 64 properties per object, enums
over 128 values, `required` over 64 names.

Validation is local and deterministic: no external resolution, no network, no
regex, a hard node budget (4096) on submitted data, diagnostics bounded at 64.

## Authorization

- **Manage** (create/revise/revoke): operators only (`admin`/`human`); an agent
  cannot write the rules for its own deliverables (403).
- **Submit**: the producer only — `service.run` + `service.represent` prove the
  caller owns the run; the run's agent must equal the bound task's **current**
  execution agent, with exact `execution_id`/`run_id` match, all inside the
  contract's project. Historical executions never bind (422
  `execution_binding_invalid`); a wrong agent gets 403.
- **Views**: contract get/list re-authorize the bound task; submission lists
  re-authorize the task AND every pinned artifact source, omitting (never
  emitting) anything whose evidence is no longer visible.

## Submission: exact evidence, fail closed, two-phase

`POST /v1/artifact-contracts/{id}:submit` is a detached handler
(`hivemind_detached_work`):

1. **Phase 1 (canonical lock)**: principal still exists, contract active and at
   the pinned `contract_revision` (412 otherwise), producer authority, exact
   named-output set, per-artifact existence/visibility/project/revision; snapshot
   content strings (≤ 1 MiB each, ≤ 4 MiB aggregate).
2. **Phase 2 (off the global canonical mutex)**: sha256 hashing, JSON parsing,
   schema/media/size validation, and trusted checks. Native checks are guarded
   by a **durable intent** in the commands ledger keyed by the exact request
   hash: `reserved` is written BEFORE any native run; after the checks the intent
   becomes `done` with the receipts. A `reserved`/`unknown` intent found on
   replay (crash or concurrent duplicate) is tombstoned `unknown` and refused
   (`contract_check_unknown`, 409) — uncertain native effects are never repeated.
   A `done` intent replays receipts only when the current pins still match its
   fingerprint (`contract_state_changed`, 409). The final record then goes
   through `service.mutate` with the caller's idempotency key, so a replayed
   submit returns the cached submission without re-running anything native.
3. **Phase 3 (inside `mutate`)**: re-validate principal, contract revision, the
   CURRENT execution binding, and every pin by revision plus exact canonical
   content-string equality before writing the immutable submission.

Trusted checks require a verifier exposing `run_bound(name, evidence)` whose
record echoes the exact evidence (`contract_id`, `contract_revision`, `task_id`,
output name, check name, and all artifact pins). A verifier offering only the
generic unbound `run()` is **advisory**: its passes never satisfy a contract
check (`trusted_verifier_unbound` → rejected). A returned record whose evidence
does not echo the request exactly is `check_evidence_mismatch` → rejected. Every
required check of every output runs and is receipted per output — nothing is
dropped or merged (≤ 16 outputs × ≤ 4 checks = ≤ 64 receipts).

Refusals write nothing: `contract_revoked` / `contract_revision_conflict` (412),
`unknown_output` / `contract_output_missing` (422), `not_found` (404),
`artifact_cross_project` (422), `artifact_revision_conflict` (412),
`artifact_checksum_mismatch` (422), `artifact_content_too_large` /
`submission_content_too_large` (422), `execution_binding_invalid` (422).
Data-level failures are recorded as `verdict: "rejected"` with diagnostics.

## Scheduling gate

`dependency_gate(service, principal, task, dependency)` (module-level) and the
`service.task_dependency_gates(principal, task, dependency)` adapter (registered
by `install`, returning `{'action': 'allow'|'defer', 'reason', 'diagnostics'}`)
are **pure canonical reads** called BEFORE a dependent is claimed/started.

Cycle detection is iterative DFS with gray (active path) / black (completed)
marking — a diamond DAG is valid, only a true back edge fails closed
(`dependency_cycle`); nodes ≤ 64 and edges ≤ 1024, exhaustion fails closed.

Reasons: approved — `contracts_disabled`, `no_contract`, `satisfied`. Held —
`dependency_cycle`, `dependency_unavailable`, `contract_not_visible`,
`contract_deleted_unreleased`, `contract_revoked`, `submission_missing`
(+`missing_outputs`), `contract_revised` (contract revision or exact output set
moved — old satisfied submissions never carry over), `execution_binding_stale`
(submission not bound to the dependency's CURRENT execution/run),
`contract_unsatisfied` (+diagnostics), `artifact_deleted`,
`artifact_not_visible`, `artifact_cross_project`, `artifact_superseded`,
`artifact_checksum_mismatch` (bounded re-hash: over-limit bodies fail closed
instead of being hashed unboundedly).

`GET /v1/artifact-contracts:gate?task_id=…&dependency_id=…` fences the pair:
`dependency_id` must be a declared dependency of `task_id` (body `depends_on` or
a `DEPENDS_ON` relation) in the same project; anything else is privacy-safe 404.

## Freezing

While the bound task has an active (non-terminal) execution, create and `:revise`
are refused (`contract_frozen`, 409). Revision after completion requires exact
`expected_revision` CAS. `:revoke` is always allowed for operators; whether it
releases the gate is explicit policy (`revocation_releases`), never silence.

## API

Registered only when enabled; `install` replaces only its own routes:

```
POST /v1/artifact-contracts
GET  /v1/artifact-contracts/{contract_id}
GET  /v1/projects/{project_id}/artifact-contracts[?task_id=]
POST /v1/artifact-contracts/{contract_id}:revise
POST /v1/artifact-contracts/{contract_id}:revoke
POST /v1/artifact-contracts/{contract_id}:submit        (detached, two-phase)
GET  /v1/artifact-contracts/{contract_id}/submissions
GET  /v1/artifact-contracts:gate?task_id=…&dependency_id=…
```

All request models are strict (`extra="forbid"`). Mutations go through the
conventional `command`/`service.mutate` idempotency pipeline.

## Limits

`MAX_CONTRACT_OUTPUTS=16`, `MAX_SCHEMA_BYTES=16384`, `MAX_SCHEMA_DEPTH=16`,
`MAX_SCHEMA_PROPERTIES=64`, `MAX_SCHEMA_ENUM=128`, `MAX_ARTIFACT_CONTENT_BYTES=1 MiB`,
`MAX_SUBMISSION_CONTENT_BYTES=4 MiB` (aggregate), `MAX_SUBMISSION_ARTIFACTS=32`,
`MAX_CONTRACTS_PER_TASK=16`, `MAX_DIAGNOSTICS=64`, `MAX_DEPENDENCY_DEPTH=64`,
`MAX_DEPENDENCY_EDGES=1024`, `MAX_DATA_NODES=4096`, `MAX_OUTPUT_CHECKS=4`,
`MAX_LIST=200`.

## Parent integration checklist

1. Add `"artifact_contracts"` to `capabilities.FEATURE_MODULES`; enable via
   `capability_options={"artifact_contracts": {"policy": ContractPolicy(enabled=True),
   "verifier": <optional verifier exposing run_bound(name, evidence)>}}`.
   `install` sets `app.state.artifact_contracts`, the `service.artifact_contracts`
   handle, and the `service.task_dependency_gates` adapter (cleared to `None`
   when disabled).
2. `Service.task_start` re-checks declared dependencies through
   `task_dependency_gates`; `Scheduling.dispatch` (parent) must consult the gate
   for each dependency of the selected task and defer with the returned
   `reason`/`diagnostics` when the action is `defer`.
