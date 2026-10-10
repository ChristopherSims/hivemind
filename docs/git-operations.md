# Hardened Git operations

`hivemind/git_operations.py` is the failure/operational hardening layer around
the native Git children that `hivemind/git_collaboration.py` drives. It is
local-only (`GIT_ALLOW_PROTOCOL=file`, no listeners, no pushes), never changes
Git identity, and never prunes, deletes, or rewrites user work, unknown
effects, or unproven refs automatically.

## Guarantees

1. **Bounded, journaled children.** `GitOperations.spawn()` runs every Git
   child under job-object / process-group containment with one deadline that
   covers process creation, stdin delivery, and output collection. Input is
   written by a dedicated thread, so a child that never reads its stdin
   cannot hang the caller past `timeout`. Containment teardown is
   unconditional (`finally: group.close()`), and a raising `spawn_observer`
   kills the child and journals the attempt before propagating. Intent
   (`begin_operation` + `mark_attempt`) is recorded in a dedicated
   `git-operations.sqlite3` recovery journal *before* the child starts, with
   exactly one terminal outcome afterwards. A killed or timed-out child
   becomes an **unknown** outcome and is never silently retried; a definite
   non-zero exit stays **pending**.
2. **Restart reconciliation.** `reconcile_repository()` re-verifies native
   integrity (`fsck`, `HEAD`, ref listing) and classifies every `*.lock`:
   `held` (a journaled operation is in flight or its PID is alive), `unknown`
   (no journal proof — foreign scope, never touched), or `stale`. Removal
   requires *positive ownership proof*: the journal shows the lock first
   appeared during one of our operations, the recorded file identity
   (size, mtime, SHA-256 of content) still matches the file on disk
   byte-for-byte (a recreated or modified lock never matches), the owning
   operation is terminal, its PID is dead, and the caller passed
   `allow_stale_lock_removal=True`. Time-window observation alone never
   justifies cleanup. Each removal is journaled (`git.reconcile`).
3. **Lease rollover.** `rollover_fence()` compare-and-swaps
   `refs/hivemind/nexus-fence` with `fence_value()` payloads byte-identical
   to `GitCollaboration.fence`; a stale expected value fails closed with
   `stale_coordinator_epoch`.
4. **Backup/restore validation.** `backup_repository()` writes a `git bundle`
   to a non-existing path, then validates by `bundle verify`, a fresh scratch
   clone, `fsck`, and exact ref parity; invalid artifacts are discarded.
   `restore_backup()` refuses any existing destination, requires the sidecar
   manifest, clones into the new path, and *verifies before publishing*:
   fsck + exact covered-ref parity + fence-absence must pass before the
   protected receive guard (`install_receive_guard` + `core.hooksPath`),
   hardened config and `HEAD -> refs/heads/main` are installed, and the
   `hivemind-ready` marker is written last. A failed restore removes the
   agent-created destination so no half-published repository remains; the
   Nexus fence is never resurrected (the next lease re-fences).
5. **Journal-aware retention.** `plan_retention()` only *proposes* refs under
   `refs/hivemind/` whose tip is proven (`merge-base --is-ancestor`) merged
   into the protected mainline; protected refs (`refs/heads/*`,
   `refs/hivemind/nexus-fence`) and unmerged refs are listed as `excluded`
   with reasons. Both phases accept an injected `guard(ref, oid, evidence)`
   callable (e.g. an audit-backed check against the canonical merge-request
   ledger); ancestry alone is never sufficient when a guard is supplied.
   `execute_retention()` requires the exact plan digest, refuses to run while
   the journal holds unknown or in-flight effects
   (`git_retention_unknown_effects`), re-verifies the mainline OID, guard and
   every candidate at apply time, and deletes only via compare-and-swap
   `update-ref -d ref oid`. Planning never deletes; execution without
   confirmation raises `git_retention_confirmation_required`.
6. **Upgrade preflight.** `upgrade_preflight()` is strictly read-only:
   Git version, bare layout, `hivemind-ready` marker, hardened receive
   config, pre-receive hook, repository/object format, `fsck`, and journal
   readability. It returns findings and mutates nothing.

## Fault-injection seams (for tests and the crash probe)

* `GitOperations(spawn_observer=callable)` — invoked with the live
  `subprocess.Popen` immediately after containment is established and before
  stdin is written; tests use it to kill the real child mid-flight.
* `spawn(..., hold_stdin=True)` — keeps the child's stdin open so blocking
  plumbing (`cat-file --batch-check`, `update-ref --stdin`) is genuinely
  in-flight when killed.
* `reconcile_repository(..., pid_probe=callable)` — overrides PID liveness
  for deterministic stale-lock tests.

## Verified crash evidence (offline, real Git)

`tests/test_git_process_crashes.py` terminates real bounded Git children and
asserts: the child was running when killed; the PID is dead afterwards;
`fsck` is clean; `refs/heads/main` and the fence ref are unchanged; the
journal records the attempt as `unknown`; a journal-proven stale lock is
reported without allowance and removed with allowance; foreign locks are
never removed; lease rollover with CAS still works after kills; three
consecutive kills never corrupt the object database. Run the standalone
probe for the same evidence as JSON:

```
python scripts/git_crash_probe.py
```

## CLI API for the parent (owned by this module)

`install_cli(subparsers)` wires the `git-ops` subcommands into the
parent-owned argparse parser in `hivemind/cli.py`; `run_cli(args,
resolve_ops=None)` dispatches and returns a JSON-serializable report:

```
git-ops preflight REPO
git-ops reconcile REPO [--remove-stale-locks]
git-ops backup REPO DEST
git-ops restore BUNDLE DEST
git-ops retention-plan REPO [--prefix refs/hivemind/...]
git-ops retention-execute REPO PLAN_JSON --confirm DIGEST
```

Parent wiring: `from .git_operations import install_cli, run_cli`, call
`install_cli(sub)` in `main()`, and for `args.command == "git-ops"` print
`json.dumps(run_cli(args), indent=2)`. `resolve_ops(args)` may inject the
service data directory; the default places the operations journal next to
the target repository.

## Integration hooks used / suggested in shared modules (not modified here)

* `hivemind/git_collaboration.py` — `install_receive_guard(repo)` (exposed by
  the parent) is reused by verified restores. Optional adoption points:
  `GitCollaboration._run` could delegate to `GitOperations.spawn` to gain
  journaled crash evidence per child; `fence()` could use
  `GitOperations.fence_value` (already byte-identical); service startup
  could call `reconcile_repository` per initialized repo and surface
  `upgrade_preflight` in a health endpoint.

## Limits

* Killed-child exit codes on Windows are indistinguishable from some normal
  failures; the journal therefore conservatively records any interrupted or
  unexpected exit as `unknown` (never auto-retried).
* `pid_alive` is subject to PID reuse after long uptimes; stale-lock removal
  additionally requires both the terminal-state journal proof and an exact
  file-identity match (size, mtime, SHA-256), and foreign or recreated locks
  are never removed.
* Lock scanning is bounded at 4096 `*.lock` entries per reconcile.
* Retention only ever covers `refs/hivemind/` refs proven merged into the
  recorded mainline OID; object pruning (`git gc`, `git prune`) is out of
  scope and never executed.
