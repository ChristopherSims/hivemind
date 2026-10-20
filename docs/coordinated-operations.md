# Coordinated operations: snapshots, restore, retention

`hivemind/coordinated_operations.py` coordinates backup, restore, and retention across
the three state planes that must move together: the canonical SQLite database, local
Git repositories, and private journal/runtime artifacts. It builds on — and never
edits — the per-component proofs in `git_operations.py` and `recovery.py`.

All long-running actions are explicit two-phase handlers:
`hivemind_detached_work=True`, default-off, admin/trusted paths only. They must never
run inside a `Service.mutate` transaction or any whole-process request path.

## Snapshot (offline, revalidated)

```python
from hivemind.coordinated_operations import CoordinatedOperations, SnapshotSource

ops = CoordinatedOperations("data/snapshots")
manifest = ops.create_snapshot(SnapshotSource(
    db_path="data/canonical.sqlite",
    repositories=({"name": "project", "repo": "data/repos/project.git"},),
    journals=("data/journal.sqlite",),
    artifacts=({"name": "runtime-state.json", "path": "run/state.json"},),
    exclusions=("live vendor workspace (not quiescence-provable)",),
))
```

Guarantees:

* **Online backup for EVERY SQLite source** — the canonical DB and every journal are
  captured with the SQLite online backup API from a read-only connection. A raw
  `copyfile` of a WAL-mode database silently loses committed pages still in the
  `-wal` file; that bug class is structurally excluded here.
* **Coherent capture by revalidation, not by a Boolean** — every source is
  fingerprinted before capture (SQLite: `PRAGMA data_version`/page/freelist counters
  on the capturing connection PLUS the main/`-wal` file size+mtime pair; Git: full
  ref map + `HEAD`; files: lstat size/mtime + SHA-256), captured, then fingerprinted
  AGAIN before the manifest is finalized and the snapshot promoted. Any changed
  source refuses the entire snapshot (`snapshot_source_changed`) and the staging tree
  is removed. A non-quiescent journal (unknown or in-flight durable effects) refuses
  the snapshot outright (`snapshot_not_quiescent`) — there is no fence/Boolean
  override, because a user flag cannot make unknown effects safe, and nothing here
  resumes them. The exact pre/post fingerprints are recorded in the manifest as
  durable source evidence. Because each plane is captured independently, this is a
  **revalidated consistent capture**, explicitly NOT a claimed cross-plane atomic
  point in time (`manifest["coherence"] == "revalidated-consistent-capture"`).
* **Validated Git bundles** — each repository goes through
  `GitOperations.backup_repository` (bundle verify + fresh scratch clone + fsck +
  covered-ref parity + hashed `*.refs.json` sidecar). Validation scratch dirs are
  removed only when created by THIS call (before/after set difference), never by
  broad pattern deletion.
* **Reparse-safe raw paths** — sources, the managed root, destinations, and every
  manifest-derived path are checked component-by-component on the RAW (unresolved)
  path (`os.path.islink` plus the Windows `FILE_ATTRIBUTE_REPARSE_POINT` attribute,
  the same technique as `identity_attestation`), so junctions that `is_symlink`
  misses are rejected before any resolve/follow.
* **Untrusted manifests** — `load_manifest` re-checks the sidecar SHA-256,
  RECOMPUTES the internal manifest digest (the stored `digest` field is never
  trusted), and validates the schema: unique entries, permitted kinds, safe canonical
  relative names (no `..`, absolute paths, drive letters, ADS `:` separators, or
  Windows reserved device names), bounded count/bytes, and confinement to the managed
  top-level layout (`canonical.sqlite`, `git/`, `journals/`, `artifacts/`).
* **No silent collisions, no credential leakage** — duplicate journal basenames,
  repository names, or artifact names refuse the snapshot; artifacts get a bounded
  credential-shape scan and are refused when they match.
* **Serialization** — snapshot promotion, retention execution, and ledger appends
  hold an exclusive root lock file with a bounded wait (`coordinated_root_busy`).
  Generated names are short to stay well under the Windows `MAX_PATH` limit.

`verify_snapshot(dir)` re-checks everything and reports `missing` / `mismatches` /
`unexpected` (including reparse entries). It mutates nothing.

## Restore (offline command only, operator-approved)

```python
ops.restore_snapshot(snapshot_dir, "restore/target",
                     approve=lambda digest, report: digest == expected and report["ok"])
```

* Refuses existing destinations, any reparse component, system paths, the user's home
  root, tampered or schema-invalid or incomplete manifests, tombstoned snapshots, and
  any snapshot whose deletion is ledgered. Restore is an offline operator command —
  never wired into production serving paths.
* `approve(manifest_digest, verify_report)` must return `True`; it is the explicit
  operator-approval gate.
* The full manifest is verified BEFORE anything is written; restore stages into a
  sibling scratch tree and promotes by rename; reported paths are rebased to the
  promoted destination. Git repositories are restored through
  `GitOperations.restore_backup` (verified clone, fsck, exact covered-ref parity,
  hardened config, receive guard, ready marker last); the Nexus fence is never
  resurrected.
* **Canonical authority sanitization (before promotion, one transaction).** A
  byte-identical canonical DB would resurrect a live Nexus lease whose expiry is in
  the future, active delivery-lease ownership (old receipts could settle), and
  queued/acknowledged controls. After the backup hash verifies and before promotion,
  the restored copy — never the live store — is sanitized in a single
  `BEGIN IMMEDIATE` transaction: every `active` `nexus_lease` becomes
  `restored_invalidated` with `expires_at=0`; every project's monotonic `epoch` is
  bumped strictly past the snapshot (never reset or decremented), so the next fresh
  lease acquisition gets a strictly greater epoch; active delivery leases are expired
  (`claim_until=0` plus a bounded `restore_lease_expired` history entry) so old
  receipts cannot settle; queued/acknowledged controls become `expired` and
  `applying` controls become `outcome_unknown` — **unknown is never marked
  completed**, and effect/task-execution evidence and UNKNOWN holds are untouched.
  The canonical store is WAL-mode, so sanitization ends with a truncating checkpoint
  and companion cleanup before the sanitized hash is taken. A read-only verification
  pass against the actual schema (no `Store` init — its migrations write) then
  proves zero active leases, zero leased deliveries, the marker present, and
  `quick_check == ok`; any residue fails the restore (`restore_sanitization_failed`).
* **Restore hold (default).** The marker is the existing key/value meta schema:
  `meta('restore_hold')` = JSON `{snapshot_id, manifest_digest, restored_at,
  release, invalidated: {...counts}}`, plus a new-only sidecar `restore-hold.json`
  at the restore root (written at restore time, never part of a snapshot). The
  exported `RESTORE_HOLD_CONTRACT` is the offline bootstrap contract: the parent's
  Store/Service startup guard refuses to serve a store whose `restore_hold` meta row
  is present; release is a typed MANUAL offline action (delete the meta row and
  sidecar) only after fresh identity/authority verification — never auto-released.
  No physical paused/quiesced proof is claimed. Generic SQLite files without the
  Hivemind schema restore fine but report `authority_sanitization.applicable:
  False` / `restore_hold: "not_applicable"` — no universal sanitization is claimed.
* **Honest hash reporting.** The response carries `authority_sanitization` with the
  invalidated counts, per-project `{snapshot_epoch, restored_epoch}`, the ORIGINAL
  verified `backup_sha256` (what the manifest proves), and the `sanitized_sha256`
  (which differs by construction — the transform happened after verification).

## Retention / compaction (two-phase, digest-confirmed, honestly reported)

```python
plan = ops.plan_retention(keep=3)                      # dry-run; deletes nothing
result = ops.execute_retention(plan, confirmation=plan["digest"])
```

* Planning is bound to exact snapshot manifest digests over strict managed names
  only. Anything unreadable, tampered, unexpected, non-direct-child, reparse, or
  staging residue is preserved and listed under `excluded`. Unknown or pending
  journals/backups needed for consistency are never planned away.
* Execution RECOMPUTES the plan digest (a supplied digest is never trusted),
  requires it as `confirmation`, requires the plan to name this exact canonical
  managed root, and re-verifies every candidate at apply time (changed → skipped).
  Forged or traversal candidates (`..`, separators, foreign roots) are refused, never
  resolved or followed.
* **Durable deletion ledger, fail-closed** — intent (`state: "deleting"`) is ledgered
  BEFORE any removal and the verified outcome (`"deleted"` / `"failed"` with
  `residue: true`) is ledgered AFTER. A removal that leaves residue is reported under
  `failed`, never under `removed`. A corrupt or unparseable ledger refuses all
  ledger-dependent operations (`deletion_ledger_corrupt`); it is never treated as
  empty. Any ledgered name/snapshot-id — including failed deletions — refuses
  resurrection (`snapshot_name_tombstoned` / `snapshot_deleted`).
* **Honest erasure** — every plan/result carries
  `erasure: {logical: true, physical: "not-guaranteed"}`: filesystem unlink of
  managed snapshot directories only; nothing is claimed about physical erasure,
  other backups, WAL segments, a `VACUUM` of any live database, or remote effects.

## Public handle and suggested wiring

```python
from hivemind.coordinated_operations import install
handle = install(app, service, principal, command, envelope, root="data/snapshots")
```

The parent owns registry/config/CLI wiring; the extra arguments are recorded as
self-describing context only. The handle exposes `snapshot` / `verify` / `restore` /
`plan` / `execute` / `status` and `describe()` (the `COORDINATED_ACTIONS`
descriptors: purpose / input / output / authority / side effects / limits / retry
safety, each marked `hivemind_detached_work` and `default_enabled`).
`wiring_needs == []`.

Suggested parent CLI (offline subcommands only): `coordinated snapshot|verify|
restore|retention-plan|retention-execute`; restore and retention-execute require
explicit operator confirmation (manifest digest / plan digest respectively).

## Limits

* Snapshots are revalidated consistent captures, not a cross-plane atomic freeze;
  run them while the service is quiesced for a true point in time.
* A snapshot proves nothing about state outside the declared `SnapshotSource`.
* The deletion ledger is append-only and local to the snapshots root; a ledgered
  failure requires operator reconciliation.
* Single-owner journal and local durability limits of `recovery.py` apply unchanged.

## Tests

`tests/test_coordinated_operations.py` (tmp fixtures only; no user data, host
changes, or network — Git tests skip without a git executable):

* snapshot/verify/restore round-trip including a real Git bundle with fsck parity,
  promoted-path rebasing, and no fence resurrection;
* **WAL capture**: a live, open, uncheckpointed WAL journal is captured via the
  online backup API with its committed operations intact;
* **change-during-capture**: artifact mutation, a concurrent canonical-DB commit,
  and a Git push between capture and revalidation each refuse the whole snapshot;
* unknown/in-flight journal effects always refuse (and the removed fence override is
  a `TypeError`);
* duplicate journal/repo/artifact names and credential-shaped artifacts refused;
* junction/reparse ancestors refused for sources, destinations, and the managed root
  (junctions are NOT symlinks — the raw-component check is what catches them);
* manifest hardening: forged sidecar with stale internal digest, traversal/ADS/
  reserved/absolute/duplicate/unknown-kind entries all refused;
* restore approval gating; existing/system destinations refused;
* **fail-closed ledger**: a corrupt ledger refuses status/snapshot/restore;
* retention dry-run, recomputed-digest confirmation, forged/traversal/foreign-root
  plans refused with the victim untouched, changed-since-plan skips, unknown-entry
  preservation, and **partial unlink failure reported as `failed` with residue**
  (never as success) with intent/outcome ledgering and resurrection refusal;
* **restore authority sanitization against a real service database**: a backup of a
  live Nexus lease and a leased delivery receipt, restored immediately, invalidates
  exactly one lease and one delivery lease, bumps the project epoch, writes the
  `restore_hold` meta marker + sidecar, keeps the manifest's original backup hash
  while reporting the differing sanitized hash, and the REAL `Store`/`Service` on
  the restored copy refuses the old epoch (`stale_coordinator_epoch`) and the old
  receipt (`delivery_status != "leased"`, ack refused) while a fresh acquisition
  gets a strictly greater epoch. Generic SQLite fixtures restore with
  `applicable: False` / `restore_hold: "not_applicable"`.
