# Restore-hold release (hardened)

`hivemind/restore_release.py` owns the ONLY supported release path for the
durable restore hold that `coordinated_operations.restore` writes on every
Hivemind-schema restore: the meta row `restore_hold` in the restored
`canonical.sqlite` plus the new-only sidecar `restore-hold.json` at the restore
root. It replaces the old inline "delete the row, unlink the sidecar" command,
which had no authority verification and no crash coordination.

## What the hold means (unchanged)

A restored copy came from a snapshot: its Nexus lease, delivery-lease, and
control authority was invalidated offline at restore time. The hold is a
durable marker the `Store` startup guard enforces; **no physical
paused/quiesced proof is claimed** and a user Boolean is never proof of
paused/no-effects. Release is a typed MANUAL offline action only.

## Release requirements

* **Offline only — and offline exclusion is the operator's duty, not a proven
  property.** Run with no live server on the restored copy and no other host
  serving a copy of the data. The helper serializes all phases through an
  exclusive lock file inside the store directory and mutates with a short busy
  timeout, so a store that is ACTIVELY being written fails the release
  (`restore_release_store_busy`). Writer-absence under these checks is a local
  heuristic bound to THIS copy: it is **not accepted as proven quiescence**,
  cannot detect a server started after the check or on another host, and is
  never reported as such.
* **Typed approval evidence** (no bare confirmation string):
  ```json
  {
    "snapshot_id": "<exact snapshot id>",
    "manifest_digest": "<64-hex manifest digest>",
    "operator": "<verified principal id>",
    "authority": "<authority statement, e.g. change ticket>",
    "approved_store": "<exact restored store path>",
    "acknowledged": true
  }
  ```
  Unknown fields, malformed digests, credential-shaped material, reparse
  ancestors on the RAW `approved_store` path (checked before resolve), and an
  `approved_store` that is not THIS store refuse the release
  (`restore_release_evidence_invalid` / `restore_release_path_mismatch`).
  `acknowledged` is recorded in the audit trail, never treated as proof.
* **Verified current identity/authority — evidence strings prove nothing.**
  Release requires at least one of:
  * `identity=`: path to an owner-format credential file (the existing
    `hivemind init` format: `{"token", "project_id", "principal_id"}`). The
    token is hashed **in memory only** (never printed, logged, audited, or
    persisted; never placed in evidence) and matched against the CURRENT
    `principals` table of the restored database. The exact role, project
    binding, and principal binding come FROM THE DATABASE: the principal must
    have role `admin` (`restore_release_identity_forbidden` otherwise), a live
    project binding, and the evidence `operator` must equal the verified
    principal id — forging another admin's name is refused
    (`restore_release_identity_mismatch`). A credential whose token hash
    matches no current principal row — revoked, rotated, or foreign — is
    refused (`restore_release_identity_invalid`). Declared `principal_id` /
    `project_id` in the file must match the database row.
  * `verify_identity=`: a trusted callable supplied by the embedding parent,
    called with `{store, snapshot_id, manifest_digest, operator, authority,
    evidence_digest, principal}` — the hold's CURRENT identity read fresh from
    the verified marker plus the verified principal when a credential is also
    given. It must return truthy; a falsy return or an exception is a refusal
    (`restore_release_identity_unverified`).
  With neither, release is refused (`restore_release_identity_required`).
  The audit trail records the verified principal id and role, never the token
  or its hash.
* **Optional fresh isolation verification**: pass
  `isolation_check=lambda path: ...` — it must return truthy for the exact
  store path at release time, else `isolation_unverified`.

## Path safety and marker verification (fail closed)

* Symlink/reparse components (including Windows junctions) are rejected on the
  RAW path before any resolve (`path_reparse_component` /
  `restore_release_path_unsafe`); system roots, drive roots, and the user home
  root are refused as stores.
* Sidecar and meta marker must BOTH exist. A sidecar whose marker is already
  gone is an **orphan**: refused with `restore_release_orphaned`, never
  silently cleaned. A MISSING sidecar with a live marker is likewise REFUSED
  (`restore_hold_not_active`) as a missing-marker state — partial releases
  performed outside this helper are investigated manually, never tolerated.
  Both markers must parse within a 64 KiB bound and match the evidence exactly
  (`restore_hold_mismatch`, `restore_hold_marker_corrupt`).
* The database must pass `PRAGMA quick_check`, carry zero sanitization residue
  (no active `nexus_lease`, no leased delivered message —
  `restore_sanitization_residue`), and keep epoch monotonicity: every project
  epoch strictly greater than any snapshotted coordinator epoch
  (`restore_epoch_regression`).

## Crash safety

Phases: **verify → stage intent → mutate (one `BEGIN IMMEDIATE` transaction,
marker re-checked inside) → remove sidecar → audit**. Before any mutation a
durable intent `.restore-release-intent.json` is atomically staged
(temp-write + rename + fsync) carrying the evidence digest; every outcome is
appended to the durable audit log `restore-release-audit.jsonl` (never
truncated). The `Store` startup guard refuses on the sidecar OR the intent
file **before opening the database**, so a crash between "meta row deleted"
and "sidecar removed" can never enable service:

| Crash point                     | Startup guard            | Recovery (`resume_release`, same evidence digest) |
|---------------------------------|--------------------------|---------------------------------------------------|
| before/during mutation          | refuses (intent present) | re-verifies fully, completes                      |
| after marker, before sidecar    | refuses (sidecar)        | removes sidecar, audits, done                     |
| after completion, intent left   | refuses (intent)         | honest `already_released` no-op                   |

A leftover intent refuses a fresh release by default
(`restore_release_intent_present`); recovery is explicit and idempotent and
ONLY proceeds with the same evidence digest
(`restore_release_intent_mismatch` otherwise). Uncertain evidence (corrupt
sidecar, orphan state, failed audit) is never erased and unknown outcomes are
never declared released.

## API

```python
from hivemind.restore_release import (load_evidence_file, evidence_digest,
                                      release_restore_hold, resume_release)

evidence = load_evidence_file("approval.json")          # bounded, validated file load
confirm = evidence_digest(evidence)                     # stable digest for --confirm
report = release_restore_hold(restored_store, evidence,
                              identity="owner.json",    # owner-format credential file
                              verify_identity=None,     # or a trusted parent callable
                              isolation_check=None)     # resume=False by default
report = resume_release(restored_store, evidence,
                        identity="owner.json")          # explicit crash recovery
```

Report: `{restore_hold: "released", snapshot_id, manifest_digest, operator,
principal, role, store, resumed, audit, note}` — `principal`/`role` are the
VERIFIED current bindings from the restored database (never the token). The
note is explicit that release grants NO fresh Nexus/delivery authority:
re-verify identity and acquire a fresh Nexus lease (strictly greater epoch)
before serving. No remote/live authority is acquired and no model is run.

## CLI contract (for the `coordinated release-hold` action)

```
hivemind coordinated release-hold \
    --root <managed snapshots root> \
    --restored-store <exact restored store directory> \
    --approval <path to approval evidence JSON> \
    --identity <path to owner-format credential JSON> \
    --confirm <evidence_digest(approval JSON)>
```

`--identity` is the safe offline authentication argument: the credential file
is read locally, the token is hashed in memory and matched against the
restored database's CURRENT `principals` table; it is never echoed, logged,
or written to evidence/audit. (A parent embedding the library may instead
pass `verify_identity=` with its own trusted verifier; the CLI surface uses
`--identity`.)

Integration sketch (parent owns `cli.py`):

```python
from hivemind.restore_release import (evidence_digest, load_evidence_file,
                                      release_restore_hold)
evidence = load_evidence_file(args.approval)
if args.confirm != evidence_digest(evidence):
    raise ValueError("--confirm must equal the approval evidence digest")
report = release_restore_hold(args.restored_store, evidence,
                              identity=args.identity)
# On Problem("restore_release_intent_present"): require an explicit operator
# decision, then resume_release(args.restored_store, evidence,
#                               identity=args.identity) with the SAME files.
```

## Limitations

* **Offline exclusion is not proven by this module.** The exclusive lock and
  the short mutation busy-timeout only make an actively-written store FAIL;
  writer-absence is a local heuristic bound to this copy — NOT proven
  quiescence. The checks cannot detect a server started after them, a server
  on another host, or a copy of the data served elsewhere. Guaranteeing
  offline exclusion remains the operator's explicit duty.
* Identity verification proves the credential matches a CURRENT `admin`
  principal row in the restored database; it does not authenticate the human
  beyond possession of the credential file, and the restored DB is only as
  current as its snapshot (a revocation that happened after the snapshot is
  not visible until a fresh restore).
* A store whose sidecar was deleted outside this helper while the meta marker
  remains is refused as a missing-marker state; investigate manually.
* The audit log is append-only and never rotated by this module (evidence is
  never erased); manage it out of band if it grows.
