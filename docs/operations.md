# Local operations

Run one server per data directory, on local disk. Do not place the writable SQLite WAL
database on a network share. Agents use authenticated APIs, not database files.

Data layout:

```text
.hivemind-data/
  owner.json          private bootstrap human credential
  canonical.sqlite    authoritative records, events, versions, indexes, jobs
  canonical.sqlite-*  SQLite WAL/SHM
  graph/              rebuildable LadybugDB projection
  models/             pinned Hugging Face model cache
```

`hivemind --data-dir ABSOLUTE_DIRECTORY serve` selects an alternate data directory.
All CLI global options precede the subcommand. `serve --open` is an explicit request
to open the local browser. The CLI only binds `127.0.0.1`.

## Backup

`hivemind backup` uses SQLite's consistent online backup API, not a live WAL file copy.
The result is a canonical SQLite snapshot including sqlite-vec data. Model weights are
not copied. LadybugDB can be rebuilt from canonical relationship facts.

Backups are sensitive and are not automatically expired. A backup predating deletion
may contain deleted information. Before restoring, reconcile current deletion policy;
this milestone does not automate that security-critical step.

Stop the server before replacing canonical files. Use `hivemind rebuild-indexes` to
schedule replay, then restart the server with the intended embedding profile.
Do not edit/drop production graph/database files ad hoc.

## Event-only SQL projection maintenance

Stop the server and workers, and take an approved backup first. Maintenance uses
the local database owner's authority and is not exposed through an HTTP endpoint.

```text
hivemind --data-dir "ABSOLUTE_EXISTING_STORE_DIRECTORY" rebuild-projections
hivemind --data-dir "ABSOLUTE_EXISTING_STORE_DIRECTORY" rebuild-projections --activate
```

The first command builds and checks a durable shadow. The second stages/verifies
again and atomically activates it. Both print only counts, digests and generation
metadata, not resource bodies. Writers are serialized during maintenance; allow
temporary disk headroom. After interruption, rerun normally rather than editing
markers or replaying commands/jobs.

Migration 6 captures canonical mutations transactionally. The versioned reducer
rebuilds projects/resources/versions/relations/run-task access and lexical FTS.
New stores report `origin=genesis`; existing stores report `origin=bootstrap`
because their original complete command history is unavailable. Bootstrap imports
retained rows/versions, not fabricated historical events.

Activation requires exact equivalence and a current journal digest. Changed
canonical data invalidates staged content. Deletion also redacts retained journal
payloads; it does not physically erase SQLite/WAL pages or backup copies.
Credentials, command receipts, outbox state and runtime ledgers are preserved,
not reconstructed or re-executed. Graph/vector rebuilds remain separate.
Mismatches fail closed; there is no automatic force-repair of corrupted truth.

Canonical schema migrations now commit atomically with `synchronous=FULL`.
Evidence: `benchmarks/durability-results-2026-10-17.json` and
`benchmarks/durability-README.txt`. Tests include process kills, SQLite capacity
failures and a genuine ENOSPC on private container tmpfs. This does not establish
power-loss safety, plugin migration coverage or whole-system recovery.

## Index jobs

Canonical writes commit records and graph/embedding jobs atomically. Index workers check
the current revision before committing, ignore obsolete jobs, and preserve a contiguous
processing watermark. Graph writes are idempotent across the SQLite/LadybugDB boundary.
Failures retry up to five times before becoming dead jobs. Replay resets that state.

Inspect lag using `/v1/ledger` freshness or search/context response metadata.
When embeddings are disabled, jobs for indexable content remain pending and canonical
and lexical APIs remain available. Starting with `--embeddings` resumes that backlog.

CPU inference and graph projection are serialized in this milestone. Long encoding jobs
can temporarily delay other operations. Do not assume the plan's performance targets
have been achieved.

## Credential hygiene

The SQLite principal table stores credential hashes, not bearer tokens. Issued credential
files are created without overwriting existing files; POSIX permissions are `0600`.
Windows protection depends on a private inherited directory ACL. Never publish the
data directory, model/user configuration, browser fixture credentials, or adapter state.

The browser uses an eight-hour HttpOnly same-origin session. Writes require an in-memory
CSRF token; bearer API requests are not browser sessions. Long-lived bearer credentials
never appear in EventSource URLs or browser local storage.
