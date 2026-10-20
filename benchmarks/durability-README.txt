SQL projection durability and recovery, 2026-10-17
================================================

Evidence: durability-results-2026-10-17.json.
Implementation: hivemind/projection_rebuild.py; canonical Store migration 6.

Implemented scope
-----------------
Deterministic SQL projections: projects, resources, versions, relations,
run_task_access and derived FTS lexical rows.
SQLite triggers capture insert/update/delete afterimages in projection_events,
inside the same transaction as each canonical mutation. Standalone save/delete/
relate calls now create a transaction rather than leaving partial multi-step writes.
Existing API command transactions remain the enclosing transaction.

The public observation events table was insufficient to replay all commands.
New stores report origin=genesis. Older stores import their existing canonical
rows and retained versions as an explicit origin=bootstrap checkpoint. This
does NOT reconstruct missing original events or certify earlier historical state.
Missing capture metadata on an already-upgraded store is refused, not reimported.

Reducer version 1 streams only mutation events into durable shadow tables.
Live tables are used only for equivalence checks: ordered SHA-256/count pairs for
all six projections. Lexical content is deterministically derived from replayed
resource bodies. Schema/reducer versions and noncontiguous journal IDs are checked.
Unknown schema versions, malformed journal data, mismatches and stale shadows refuse
activation. The journal is not a cryptographic, tamper-proof audit ledger.

Activation repeats journal/equivalence checks inside BEGIN IMMEDIATE, replaces
stable table contents in ONE transaction, checks FKs, clears staging, and increments
the projection generation. Existing WAL readers see an old or committed consistent
state, never a partial switch. Destructive external FK actions and unknown triggers
on captured projection tables are refused.

Captured mutations clear staged content immediately. Deletion/history retention
redacts content-bearing mutation records as well as canonical versions; retained
live content stays replayable. This is logical deletion, not physical erasure of
old SQLite/WAL pages, backups or filesystem copies.

No messages, jobs, tools, summaries, providers or model calls are replayed.
Principals/credentials, command receipts, observation events, audit/receipt tables,
outbox state, runtime/private side ledgers, graph and vector indexes remain intact.
They are not claimed as reconstructed by this reducer. A mismatch refuses switching;
there is deliberately no force-repair or guessed recovery of corrupted truth.

Migration and fault evidence
----------------------------
Canonical Store schema creation/migrations 1-6 now share one explicit transaction;
failed initialization closes the connection and rolls back. WAL synchronous=FULL
is explicit. Unknown future base schema versions fail closed.

Final Windows regression matrix: 214 passed, 1 skipped, 110.75 seconds.
The skip is the opt-in Linux-only real ENOSPC test.
Final Linux Docker durability suite: 33 passed, 5.08 seconds, including real ENOSPC.
Ruff and scoped whitespace checks passed.

Tests cover:
- event-only replay/equivalence, ACL/privacy metadata, history and relation facts;
- repeated activation, restart-resumable staging and stale-stage invalidation;
- second-connection writes, concurrent WAL readers and foreign-key side effects;
- logical erasure, retained live content, malformed/unknown/gapped journal entries;
- injected activation errors and actual SQLITE_FULL at save/build/switch/upgrade;
- external SIGKILL (Linux) / TerminateProcess (Windows) while staging, switching,
  upgrading, and after a committed switch; abrupt os._exit boundaries as well;
- interrupted pre-v6 upgrades, rollback of DDL/bootstrap, and clean retry;
- real filesystem ENOSPC, failed canonical mutation, reopen, integrity check,
  unchanged live data/jobs, and successful subsequent replay/activation.

The ENOSPC test fills ONLY a separately mounted private 32 MiB tmpfs at
/fault-volume. It verifies the mount type and <=64 MiB capacity before filling.
The container uses UID 1000, default security restrictions, network none and no
host bind mounts, credentials or Docker socket. /tmp is a separate 512 MiB tmpfs.
No host filesystem or production store was filled, migrated, killed or rebuilt.
All owned test containers were automatically removed.

Reproduction
------------
Run maintenance offline with the server/workers stopped and an approved backup:
hivemind --data-dir "ABSOLUTE_EXISTING_STORE_DIRECTORY" rebuild-projections
hivemind --data-dir "ABSOLUTE_EXISTING_STORE_DIRECTORY" rebuild-projections --activate
The second command stages/verifies again before switching; it does not trust an
old staging result. If interrupted, rerun after SQLite recovery. Do not manually
toggle switching markers, remove journal triggers, or reset command/job ledgers.

Windows tests (from repository root):
python -m pytest tests/test_projection_rebuild.py tests/test_core.py tests/test_team_access.py tests/test_memory_validation.py tests/test_command_receipts.py tests/test_commit_signals.py tests/test_restore_release.py tests/test_restore_release_cli.py tests/test_coordinated_operations.py tests/test_memory_pipeline.py tests/test_scalable_retrieval.py tests/test_retrieval_benchmark.py tests/test_retrieval_dataset.py -q

Linux image base: hivemind-runtime-tests:2026-10-17, from runtime-linux/Dockerfile.
Copy current storage.py, projection_rebuild.py, cli.py and test_projection_rebuild.py
into a new disposable container at /opt/hivemind/{hivemind,tests}/ before starting.
Use: --network none --tmpfs /tmp:rw,noexec,nosuid,size=512m
     --tmpfs /fault-volume:rw,noexec,nosuid,size=32m,mode=1777
     --env HIVEMIND_DISK_FULL_ROOT=/fault-volume
Command: timeout --signal=TERM --kill-after=10s 150s python -u -m pytest -p no:cacheprovider tests/test_projection_rebuild.py -q -rs

Remaining gates
---------------
This is canonical SQL durability, not a whole-system restore or production release
claim. Power loss, corrupted storage devices, physical block-device ENOSPC, network
filesystems, plugin/side-ledger migrations, downgrade support, massive-journal
replay/load, and full production service/vendor restart campaigns remain open.
Build/activation serialize writers and need temporary disk headroom; schedule them
offline. Journaling increases storage and write work. Search/indexing performance
has not been re-benchmarked after this durability change.
