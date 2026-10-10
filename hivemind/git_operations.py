"""Crash-hardened operational layer for native Git repositories.

This module owns failure/operational hardening around the bounded Git child
processes that ``git_collaboration`` drives. It never pushes to a network,
never changes Git identity, and never prunes, deletes, or rewrites user work,
unknown side effects, or unproven refs. Everything mutating is journaled
first through :class:`hivemind.recovery.Journal` so a crash mid-operation
leaves durable evidence that restart reconciliation can adjudicate.

Guarantees:

* every Git child is spawned bounded (job-object/process-group containment,
  deadline, capped output) and recorded in the journal with its PID;
* a killed or timed-out child is recorded as an *unknown* outcome and is
  never silently retried;
* stale lock files are removed only when the journal proves the owning
  operation terminated, the recorded PID is dead, and the caller passes an
  explicit ``allow_stale_lock_removal`` flag;
* backups are validated (bundle verify + fresh clone + fsck + ref parity)
  and restores refuse to overwrite any existing path;
* retention is a two-phase plan/execute flow: planning only *proposes* refs
  under ``refs/hivemind/`` proven merged into the protected mainline;
  execution requires the exact plan digest and re-proves safety at apply
  time with compare-and-swap ref deletion;
* upgrade preflight is strictly read-only.

Fault-injection seam: ``spawn_observer`` is called with the live
``subprocess.Popen`` immediately after containment is established and before
any stdin is written, so tests and the crash probe can kill the real child.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from .git_collaboration import git_executable
from .process_boundary import ProcessGroup
from .recovery import Journal
from .storage import Problem, canonical, digest, now, uid

__all__ = [
    "FENCE_REF",
    "MAIN_REF",
    "PROTECTED_REFS",
    "RETENTION_ROOT",
    "GitOperations",
    "install_cli",
    "pid_alive",
    "run_cli",
]

MAIN_REF = "refs/heads/main"
FENCE_REF = "refs/hivemind/nexus-fence"
PROTECTED_REFS = frozenset({MAIN_REF, FENCE_REF})
# Only refs below this root may ever appear in a retention plan.
RETENTION_ROOT = "refs/hivemind/"
MAX_LOCK_SCAN = 4096
MAX_OUTPUT = 262144


def pid_alive(pid):
    """Conservative liveness check. Never signals the target process."""
    if not pid or pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel.OpenProcess(0x1000, False, int(pid))  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == 259  # STILL_ACTIVE
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class GitOperations:
    """Bounded, journaled, crash-aware Git operations for one data root."""

    def __init__(self, root, *, journal_name="git-operations.sqlite3", spawn_observer=None):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.journal_path = self.root / journal_name
        # Fault-injection seam: callable(Popen) -> None, invoked post-spawn.
        self.spawn_observer = spawn_observer
        self._executable = None

    # ------------------------------------------------------------------ run
    def _env(self):
        env = os.environ.copy()
        for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"):
            env.pop(name, None)
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_ALLOW_PROTOCOL"] = "file"  # local transport only; no network exposure
        return env

    def _git(self):
        if self._executable is None:
            self._executable = git_executable()
        return self._executable

    @staticmethod
    def _lock_identity(path):
        """Content identity of a lock file, or ``None`` if it vanished.

        Ownership proof for cleanup: a lock is attributable to one of our
        operations only if the exact file observed during that operation is
        still byte-for-byte the file on disk (same size, mtime and digest).
        A recreated or modified lock never matches and is never removed.
        """
        try:
            data = Path(path).read_bytes()
            stat = os.stat(path)
        except OSError:
            return None
        return {"path": str(path), "bytes": len(data), "mtime_ns": stat.st_mtime_ns,
                "sha256": hashlib.sha256(data).hexdigest()}

    def _locks(self, repo):
        """Map of ``*.lock`` path to content identity, bounded by MAX_LOCK_SCAN."""
        repo = Path(repo)
        found = {}
        try:
            for index, path in enumerate(repo.rglob("*.lock")):
                if index >= MAX_LOCK_SCAN:
                    break
                identity = self._lock_identity(path)
                if identity:
                    found[str(path)] = identity
        except OSError:
            pass
        return found

    def _finish_operation(self, operation_id, repo, argv, process, started, locks_before,
                          *, timed_out, allowed, interrupted=False):
        """Journal the terminal outcome and build the spawn record. Never raises."""
        duration = time.monotonic() - started
        locks_after = self._locks(repo)
        # Ownership requires a NEW path observed during this operation; a lock
        # that already existed beforehand can never be attributed to us.
        observed = [identity for path, identity in locks_after.items() if path not in locks_before]
        record = {"operation_id": operation_id, "pid": process.pid, "returncode": process.returncode,
                  "duration_seconds": round(duration, 6), "argv": argv[1:], "repo": str(repo),
                  "timed_out": timed_out, "locks_observed": observed}
        try:
            with Journal(self.journal_path) as journal:
                journal.checkpoint(label="git.operation", operation_id=operation_id,
                                   metadata={**record, "at": now()})
                if timed_out:
                    journal.fail_operation(operation_id, "git_process_timeout")
                elif interrupted or process.returncode not in allowed:
                    if not interrupted and process.returncode is not None \
                            and process.returncode > 0 and allowed != (0,):
                        journal.fail_operation(operation_id, "git_command_failed", status=409)
                    else:
                        # Killed/interrupted child: the effect is unknown, never retried blindly.
                        journal.fail_operation(operation_id, "git_process_interrupted")
                else:
                    journal.complete_operation(operation_id, {"returncode": process.returncode})
        except Exception:
            pass  # journaling must never mask the process outcome
        return record

    def spawn(self, repo, args, *, timeout=20, input_bytes=None, allowed=(0,),
              hold_stdin=False, operation_id=None):
        """Run one bounded Git child, journaled with intent and outcome.

        The deadline covers process creation, stdin delivery and output
        collection: input is written by a dedicated thread so a child that
        never reads cannot hang the caller past ``timeout``. A raising
        ``spawn_observer`` never leaks the child — containment is torn down
        unconditionally and the interrupted attempt is journaled as unknown.
        ``hold_stdin`` keeps the child's stdin open for blocking plumbing.
        """
        repo = Path(repo)
        if input_bytes is not None and len(input_bytes) > 2 * 1024 * 1024:
            raise ValueError("Process input exceeds the configured boundary")
        operation_id = operation_id or uid("gitop")
        argv = [self._git(), "-c", "core.longpaths=true", "--literal-pathspecs", *map(str, args)]
        locks_before = self._locks(repo)
        with Journal(self.journal_path) as journal:
            journal.begin_operation(operation_id, "GIT", f"{repo}!{' '.join(map(str, args))}",
                                    body={"argv": argv[1:], "repo": str(repo)}, label="git.operation")
            journal.mark_attempt(operation_id)
        deadline = time.monotonic() + timeout  # covers spawn, input and output
        started = time.monotonic()
        process = subprocess.Popen(argv, cwd=repo, env=self._env(), shell=False,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=os.name != "nt")
        group = ProcessGroup(process)
        output = [bytearray(), bytearray()]
        cap_lock, exceeded = threading.Lock(), threading.Event()

        def drain(stream, index):
            try:
                while block := stream.read(4096):
                    with cap_lock:
                        remaining = max(0, MAX_OUTPUT - sum(map(len, output)))
                        output[index].extend(block[:remaining])
                        if len(block) > remaining:
                            exceeded.set()
                            group.kill()
            except (OSError, ValueError):
                pass
            finally:
                stream.close()

        def feed():
            try:
                if input_bytes:
                    process.stdin.write(input_bytes)  # unblocks when the deadline kills the child
                    process.stdin.flush()
            except (OSError, ValueError):
                pass
            finally:
                if not hold_stdin:
                    try:
                        process.stdin.close()
                    except (OSError, ValueError):
                        pass

        threads = [threading.Thread(target=drain, args=(process.stdout, 0), daemon=True),
                   threading.Thread(target=drain, args=(process.stderr, 1), daemon=True),
                   threading.Thread(target=feed, daemon=True)]
        running = []
        timed_out = False
        try:
            for thread in threads:
                thread.start()
                running.append(thread)
            if self.spawn_observer:
                self.spawn_observer(process)  # fault-injection seam
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            timed_out = True
        except BaseException:
            # A faulting observer (or any setup error) must not leak the child.
            group.close()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            self._finish_operation(operation_id, repo, argv, process, started, locks_before,
                                   timed_out=False, allowed=allowed, interrupted=True)
            raise
        finally:
            group.close()  # unconditional containment cleanup, including descendants
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            for thread in running:
                thread.join(10)
            try:
                process.stdin.close()
            except (OSError, ValueError):
                pass
        record = self._finish_operation(operation_id, repo, argv, process, started, locks_before,
                                        timed_out=timed_out, allowed=allowed)
        if timed_out:
            raise Problem("git_command_timeout", 409)
        if exceeded.is_set():
            raise Problem("git_transport_boundary_exceeded", 422)
        if process.returncode not in allowed:
            raise Problem("git_command_failed", 409)
        return {**record, "stdout": bytes(output[0]).decode("utf-8", errors="strict").strip(),
                "stderr": bytes(output[1]).decode("utf-8", errors="replace").strip()}

    def git(self, repo, *args, **kwargs):
        return self.spawn(repo, list(args), **kwargs)["stdout"]

    # ------------------------------------------------------------ reconcile
    def reconcile_repository(self, repo, *, allow_stale_lock_removal=False, pid_probe=pid_alive):
        """Restart reconciliation: verify integrity, classify locks, repair only proven scope.

        Read-only by default. Removes a ``*.lock`` file only when the journal
        proves the file appeared during one of *our* operations, that operation
        reached a terminal state, its recorded PID is dead, and the caller
        explicitly allows stale-lock removal. Unknown locks are left untouched.
        """
        repo = Path(repo)
        if not repo.is_dir():
            raise Problem("git_repository_not_initialized", 409)
        report = {"repo": str(repo), "at": now(), "refs": {}, "locks": [], "removed_locks": [],
                  "fsck_ok": None, "head": None, "ready": (repo / "hivemind-ready").exists()}
        try:
            report["head"] = self.git(repo, "rev-parse", "--verify", "HEAD")
        except Problem:
            report["head"] = None
        try:
            fsck = self.spawn(repo, ["fsck", "--no-progress"], timeout=60, allowed=(0, 1))
            report["fsck_ok"] = fsck["returncode"] == 0
            report["fsck_output"] = fsck["stdout"].splitlines()[:64]
        except Problem as error:
            report["fsck_ok"] = False
            report["fsck_error"] = error.code
        listing = self.spawn(repo, ["for-each-ref", "--format=%(refname) %(objectname)"],
                             allowed=(0,))["stdout"]
        for line in filter(None, listing.splitlines()):
            name, _, oid = line.partition(" ")
            report["refs"][name] = oid
        # Journal evidence: which operations touched this repo and how they ended.
        operations = {}
        if self.journal_path.exists():
            with Journal(self.journal_path) as journal:
                rows = journal.db.execute(
                    "SELECT operation_id, state, attempting FROM operations WHERE path LIKE ?",
                    (f"{repo}!%",)).fetchall()
                for row in rows:
                    operations[row["operation_id"]] = {"state": row["state"], "attempting": row["attempting"]}
                checkpoints = journal.db.execute(
                    "SELECT operation_id, metadata FROM checkpoints WHERE label='git.operation'").fetchall()
            pids, owned_locks = {}, {}
            for row in checkpoints:
                metadata = json.loads(row["metadata"])
                if metadata.get("repo") != str(repo):
                    continue
                pids[row["operation_id"]] = metadata.get("pid")
                for item in metadata.get("locks_observed") or []:
                    if isinstance(item, dict) and item.get("path"):
                        owned_locks.setdefault(row["operation_id"], {})[item["path"]] = item
        else:
            pids, owned_locks = {}, {}
        busy = any(op["attempting"] for op in operations.values())
        for path, identity in sorted(self._locks(repo).items()):
            entry = {"path": path, "classification": "unknown", "removed": False}
            # Positive ownership proof: a terminal operation observed THIS exact
            # file (same size, mtime and content digest). A lock merely seen in
            # a time window, or recreated since, is never attributable to us.
            owners = [op for op, locks in owned_locks.items()
                      if path in locks
                      and all(locks[path][key] == identity[key] for key in ("bytes", "mtime_ns", "sha256"))]
            if busy:
                entry["classification"] = "held"  # a journaled operation is in flight
            elif not owners:
                entry["classification"] = "unknown"  # foreign or unprovable scope: never touched
            elif owners and all(operations.get(op, {}).get("state") in ("completed", "unknown")
                                for op in owners):
                live = [pids[op] for op in owners if pid_probe(pids.get(op) or -1)]
                if live:
                    entry["classification"] = "held"
                    entry["live_pids"] = live
                elif allow_stale_lock_removal:
                    try:
                        os.unlink(path)
                    except OSError:
                        entry["classification"] = "removal_failed"
                    else:
                        entry.update(classification="stale", removed=True)
                        report["removed_locks"].append(path)
                else:
                    entry["classification"] = "stale"  # proven dead, awaiting explicit allowance
            report["locks"].append(entry)
        if report["removed_locks"]:
            with Journal(self.journal_path) as journal:
                journal.checkpoint(label="git.reconcile", metadata={
                    "repo": str(repo), "removed_locks": report["removed_locks"], "at": now()})
        return report

    # ---------------------------------------------------------------- fence
    @staticmethod
    def fence_value(lease_id, epoch):
        """Byte-identical to ``GitCollaboration.fence``'s canonical payload."""
        return canonical({"lease_id": lease_id, "epoch": epoch}).encode()

    def fence_oid(self, repo, lease_id, epoch):
        return self.git(repo, "hash-object", "-w", "--stdin",
                        input_bytes=self.fence_value(lease_id, epoch))

    def rollover_fence(self, repo, lease_id, epoch, *, expected_oid=None):
        """Lease rollover: CAS the Nexus fencing ref to a new lease/epoch.

        ``expected_oid`` (or ``"0" * 40`` for first creation) makes rollover
        fail closed against a concurrent fence writer.
        """
        new_oid = self.fence_oid(repo, lease_id, epoch)
        current = self.spawn(repo, ["rev-parse", "--verify", FENCE_REF],
                             allowed=(0, 1, 128))["stdout"]
        old = current if current else "0" * 40
        if expected_oid is not None and old != expected_oid:
            raise Problem("stale_coordinator_epoch", 409)
        self.git(repo, "update-ref", FENCE_REF, new_oid, old)
        return {"ref": FENCE_REF, "oid": new_oid, "previous_oid": old}

    # --------------------------------------------------------------- backup
    @staticmethod
    def _manifest_path(bundle):
        bundle = Path(bundle)
        return bundle.with_name(bundle.name + ".refs.json")

    def backup_repository(self, repo, destination):
        """Create and validate a bundle backup. Never overwrites an existing path.

        A Git bundle carries commit history only, so refs pointing at
        non-commit objects (for example the Nexus fence blob) are recorded in
        a hashed ``<bundle>.refs.json`` sidecar manifest instead of being
        silently dropped. The fence itself is re-established by lease
        acquisition after a restore and is intentionally not resurrected.
        """
        repo, destination = Path(repo), Path(destination)
        manifest = self._manifest_path(destination)
        if destination.exists() or manifest.exists():
            raise Problem("git_backup_destination_exists", 409)
        destination.parent.mkdir(parents=True, exist_ok=True)
        refs = self._ref_map(repo)
        covered = {ref: oid for ref, oid in refs.items()
                   if self.git(repo, "cat-file", "-t", oid) in ("commit", "tag")}
        if not covered:
            raise Problem("git_backup_empty", 409)
        self.spawn(repo, ["bundle", "create", str(destination), "HEAD", *sorted(covered)],
                   timeout=120)
        manifest.write_text(canonical({"repo": str(repo), "created_at": now(), "refs": refs,
                                       "covered_refs": sorted(covered)}), encoding="utf-8")
        report = self.validate_backup(repo, destination)
        if not report["ok"]:
            # Remove only artifacts this call just created.
            destination.unlink(missing_ok=True)
            manifest.unlink(missing_ok=True)
            raise Problem("git_backup_validation_failed", 409)
        return report

    def validate_backup(self, repo, bundle):
        """Verify a bundle: bundle verify, fresh scratch clone, fsck, covered-ref parity.

        When a sidecar manifest exists, parity is checked against its
        ``covered_refs`` and the manifest is hashed into the report.
        """
        repo, bundle = Path(repo), Path(bundle)
        verify = self.spawn(repo, ["bundle", "verify", str(bundle)], allowed=(0, 1), timeout=60)
        report = {"repo": str(repo), "bundle": str(bundle), "ok": False,
                  "bundle_verify_ok": verify["returncode"] == 0,
                  "bundle_sha256": hashlib.sha256(bundle.read_bytes()).hexdigest()}
        if not report["bundle_verify_ok"]:
            report["error"] = "bundle_verify_failed"
            return report
        manifest = self._manifest_path(bundle)
        expected = None
        if manifest.exists():
            document = json.loads(manifest.read_text(encoding="utf-8"))
            report["manifest_sha256"] = hashlib.sha256(
                manifest.read_bytes()).hexdigest()
            report["manifest_refs"] = document["refs"]
            expected = {ref: document["refs"][ref] for ref in document["covered_refs"]}
        source_refs = expected or self._ref_map(repo)
        scratch = Path(tempfile.mkdtemp(prefix="hivemind-backup-validate-", dir=bundle.parent))
        try:
            self.git(bundle.parent, "clone", "--bare", "--no-local", str(bundle), str(scratch),
                     timeout=120)
            clone_refs = self._ref_map(scratch)
            fsck = self.spawn(scratch, ["fsck", "--no-progress"], allowed=(0, 1), timeout=60)
            report.update(fsck_ok=fsck["returncode"] == 0, source_refs=source_refs,
                          restored_refs=clone_refs,
                          missing_refs=sorted(set(source_refs) - set(clone_refs)),
                          diverged_refs=sorted(name for name in set(source_refs) & set(clone_refs)
                                               if source_refs[name] != clone_refs[name]))
            report["ok"] = bool(report["fsck_ok"]) and not report["missing_refs"] \
                and not report["diverged_refs"]
        finally:
            shutil.rmtree(scratch, ignore_errors=True)  # agent-created scratch clone only
        return report

    def restore_backup(self, bundle, destination):
        """Restore into a NEW path, verified before anything is publishable.

        Sequence: refuse existing destination → require the sidecar manifest →
        clone the bundle → prove fsck clean and exact covered-ref parity →
        only then install the protected-receive guard (``core.hooksPath`` +
        pre-receive hook via the parent's ``install_receive_guard``), hardened
        config and ``HEAD -> refs/heads/main`` → write the ``hivemind-ready``
        marker LAST. A stale Nexus fence is never resurrected; the next lease
        acquisition re-fences. Any failure removes the agent-created
        destination so a retry is possible and no half-published repo remains.
        """
        bundle, destination = Path(bundle), Path(destination)
        manifest = self._manifest_path(bundle)
        if destination.exists():
            raise Problem("git_restore_destination_exists", 409)
        if not manifest.exists():
            raise Problem("git_backup_manifest_missing", 409)
        document = json.loads(manifest.read_text(encoding="utf-8"))
        expected = {ref: document["refs"][ref] for ref in document["covered_refs"]}
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.git(destination.parent, "clone", "--bare", "--no-local", str(bundle), str(destination),
                 timeout=120)
        try:
            refs = self._ref_map(destination)
            missing = sorted(set(expected) - set(refs))
            diverged = sorted(ref for ref in set(expected) & set(refs) if expected[ref] != refs[ref])
            fsck = self.spawn(destination, ["fsck", "--no-progress"], allowed=(0, 1), timeout=60)
            if missing or diverged or fsck["returncode"] or FENCE_REF in refs:
                raise Problem("git_restore_integrity_failed", 409)
            # Content is verified: now publish the guard and service layout.
            from .git_collaboration import install_receive_guard
            hooks = install_receive_guard(destination)
            self.git(destination, "config", "core.hooksPath", str(hooks))
            for key, value in (("receive.denyNonFastForwards", "true"),
                               ("receive.denyDeletes", "true"),
                               ("receive.fsckObjects", "true"),
                               ("core.logAllRefUpdates", "true")):
                self.git(destination, "config", key, value)
            self.git(destination, "symbolic-ref", "HEAD", MAIN_REF)
            hook = hooks / "pre-receive"
            final = self.spawn(destination, ["fsck", "--no-progress"], allowed=(0, 1), timeout=60)
            if final["returncode"] or not hook.is_file() \
                    or "Protected refs" not in hook.read_text(encoding="utf-8", errors="replace"):
                raise Problem("git_restore_integrity_failed", 409)
            (destination / "hivemind-ready").write_text("local-only\n", encoding="utf-8")
        except BaseException:
            (destination / "hivemind-ready").unlink(missing_ok=True)
            shutil.rmtree(destination, ignore_errors=True)  # agent-created failed restore only
            raise
        with Journal(self.journal_path) as journal:
            journal.checkpoint(label="git.restore", metadata={
                "bundle": str(bundle), "destination": str(destination),
                "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
                "refs": refs, "at": now()})
        return {"restored": str(destination), "refs": refs, "fsck_ok": True,
                "receive_guard": True, "ready": True, "fence_present": False}

    def _ref_map(self, repo):
        out = self.spawn(repo, ["for-each-ref", "--format=%(refname) %(objectname)"],
                         allowed=(0,))["stdout"]
        return dict(line.split(" ", 1) for line in filter(None, out.splitlines()))

    # ------------------------------------------------------------ retention
    def plan_retention(self, repo, *, prefixes=("refs/hivemind/candidates/",),
                       merged_into=MAIN_REF, guard=None):
        """Build a retention plan. Proposes nothing unproven; deletes nothing.

        Eligible refs must live under ``refs/hivemind/`` (never ``refs/heads/``),
        must not be protected, and their tip must be an ancestor of the
        protected mainline (proof the work is durably published). ``guard`` is
        an optional injected callable ``(ref, oid, evidence) -> bool`` — e.g. an
        audit-backed check that the canonical merge-request ledger reconciled
        this exact candidate; a rejected ref is excluded with its reason.
        Ancestry alone is never sufficient when a guard is supplied.
        """
        repo = Path(repo)
        for prefix in prefixes:
            if not prefix.startswith(RETENTION_ROOT) or prefix.rstrip("/") == FENCE_REF:
                raise Problem("git_retention_scope_not_provable", 403)
        main = self.git(repo, "rev-parse", "--verify", merged_into)
        candidates, excluded = [], []
        for prefix in prefixes:
            listing = self.spawn(repo, ["for-each-ref", "--format=%(refname) %(objectname)", prefix],
                                 allowed=(0,))["stdout"]
            for line in filter(None, listing.splitlines()):
                ref, oid = line.split(" ", 1)
                if ref in PROTECTED_REFS or ref.startswith("refs/heads/"):
                    excluded.append({"ref": ref, "oid": oid, "reason": "protected"})
                    continue
                ancestor = self.spawn(repo, ["merge-base", "--is-ancestor", oid, main],
                                      allowed=(0, 1))["returncode"]
                if ancestor:
                    excluded.append({"ref": ref, "oid": oid, "reason": "not_merged"})
                    continue
                evidence = f"ancestor_of:{merged_into}"
                if guard is not None and not guard(ref, oid, evidence):
                    excluded.append({"ref": ref, "oid": oid, "reason": "guard_rejected"})
                    continue
                candidates.append({"ref": ref, "oid": oid, "evidence": evidence})
        plan = {"plan_id": uid("retention"), "repo": str(repo), "created_at": now(),
                "merged_into": merged_into, "main_oid": main,
                "candidates": sorted(candidates, key=lambda c: c["ref"]),
                "excluded": sorted(excluded, key=lambda c: c["ref"])}
        plan["digest"] = digest({key: plan[key] for key in plan})
        with Journal(self.journal_path) as journal:
            journal.checkpoint(label="retention.plan", metadata=plan)
        return plan

    def _block_on_unknown_effects(self):
        """Refuse destructive flows while any journaled effect is unknown or in flight."""
        if not self.journal_path.exists():
            return
        with Journal(self.journal_path) as journal:
            row = journal.db.execute(
                "SELECT count(*) AS c FROM operations WHERE state='unknown' OR attempting=1"
            ).fetchone()
        if row["c"]:
            raise Problem("git_retention_unknown_effects", 409)

    def execute_retention(self, repo, plan, *, confirmation, guard=None):
        """Apply a retention plan. Requires the exact plan digest and re-proves every ref.

        Blocked entirely while the journal holds unknown or in-flight effects
        (``git_retention_unknown_effects``); every candidate is also re-checked
        against the injected ``guard`` at apply time.
        """
        repo = Path(repo)
        if not confirmation or confirmation != plan.get("digest"):
            raise Problem("git_retention_confirmation_required", 409)
        self._block_on_unknown_effects()
        main = self.git(repo, "rev-parse", "--verify", plan["merged_into"])
        if main != plan["main_oid"]:
            raise Problem("git_head_changed", 409)  # proof baseline moved: re-plan
        removed, skipped = [], []
        for candidate in plan["candidates"]:
            ref, oid = candidate["ref"], candidate["oid"]
            if ref in PROTECTED_REFS or ref.startswith("refs/heads/") \
                    or not ref.startswith(RETENTION_ROOT):
                skipped.append({"ref": ref, "reason": "protected"})
                continue
            current = self.spawn(repo, ["rev-parse", "--verify", ref], allowed=(0, 1, 128))["stdout"]
            if current != oid:
                skipped.append({"ref": ref, "reason": "changed_since_plan"})
                continue
            ancestor = self.spawn(repo, ["merge-base", "--is-ancestor", oid, main],
                                  allowed=(0, 1))["returncode"]
            if ancestor:
                skipped.append({"ref": ref, "reason": "not_merged"})
                continue
            if guard is not None and not guard(ref, oid, candidate.get("evidence")):
                skipped.append({"ref": ref, "reason": "guard_rejected"})
                continue
            self.git(repo, "update-ref", "-d", ref, oid)  # CAS delete of the proven ref only
            removed.append(ref)
        with Journal(self.journal_path) as journal:
            journal.checkpoint(label="retention.executed", metadata={
                "plan_id": plan["plan_id"], "repo": str(repo), "removed": removed,
                "skipped": skipped, "at": now()})
        return {"plan_id": plan["plan_id"], "removed": removed, "skipped": skipped}

    # ------------------------------------------------------------- preflight
    def upgrade_preflight(self, repo):
        """Read-only upgrade compatibility check. Reports findings; mutates nothing."""
        repo = Path(repo)
        findings = []

        def check(name, ok, detail=""):
            findings.append({"check": name, "ok": bool(ok), "detail": detail})

        try:
            executable = git_executable()
            check("git_version", True, executable)
        except Problem as error:
            check("git_version", False, error.code)
            return {"repo": str(repo), "ready": False, "findings": findings}
        check("repository_present", repo.is_dir())
        if not repo.is_dir():
            return {"repo": str(repo), "ready": False, "findings": findings}
        bare = self.spawn(repo, ["rev-parse", "--is-bare-repository"], allowed=(0, 1))["stdout"]
        check("bare_repository", bare == "true", bare)
        check("ready_marker", (repo / "hivemind-ready").exists())
        for key, wanted in (("receive.denyNonFastForwards", "true"), ("receive.denyDeletes", "true"),
                            ("receive.fsckObjects", "true"), ("core.logAllRefUpdates", "true")):
            value = self.spawn(repo, ["config", "--get", key], allowed=(0, 1))["stdout"]
            check(f"config:{key}", value == wanted, value or "unset")
        hook = repo / "hooks" / "pre-receive"
        check("pre_receive_hook", hook.is_file() and "Protected refs" in hook.read_text(
            encoding="utf-8", errors="replace"))
        format_version = self.spawn(repo, ["config", "--get", "core.repositoryformatversion"],
                                    allowed=(0, 1))["stdout"]
        check("repository_format", format_version in ("", "0"), format_version or "default")
        object_format = self.spawn(repo, ["config", "--get", "extensions.objectformat"],
                                   allowed=(0, 1))["stdout"]
        check("object_format_sha1", object_format in ("", "sha1"), object_format or "default")
        fsck = self.spawn(repo, ["fsck", "--no-progress"], allowed=(0, 1), timeout=60)
        check("fsck", fsck["returncode"] == 0)
        if self.journal_path.exists():
            try:
                with Journal(self.journal_path) as journal:
                    journal.db.execute("SELECT count(*) FROM operations").fetchone()
                check("journal_readable", True)
            except Exception:
                check("journal_readable", False)
        return {"repo": str(repo), "ready": all(item["ok"] for item in findings),
                "findings": findings}


# --------------------------------------------------------------------- CLI
def install_cli(subparsers):
    """Wire the ``git-ops`` subcommands into the parent-owned argparse CLI.

    Called from ``hivemind.cli.main`` (parent-owned) as
    ``install_cli(sub)``; dispatch with ``run_cli(args)`` and print the
    returned JSON-serializable report. This module owns the handlers; the
    parent owns the parser lifecycle.
    """
    git_ops = subparsers.add_parser(
        "git-ops", help="Hardened native Git operations (preflight/reconcile/backup/restore/retention)")
    sub = git_ops.add_subparsers(dest="git_ops_command", required=True)
    for name in ("preflight", "reconcile", "retention-plan", "retention-execute"):
        sub.add_parser(name).add_argument("repo")
    sub.choices["reconcile"].add_argument("--remove-stale-locks", action="store_true",
        help="Remove only journal-proven stale locks owned by dead children")
    backup = sub.add_parser("backup")
    backup.add_argument("repo")
    backup.add_argument("destination")
    restore = sub.add_parser("restore")
    restore.add_argument("bundle")
    restore.add_argument("destination")
    sub.choices["retention-plan"].add_argument(
        "--prefix", action="append", help="refs/hivemind/ prefix to plan for (repeatable)")
    execute = sub.choices["retention-execute"]
    execute.add_argument("plan", help="Path to the retention plan JSON produced by retention-plan")
    execute.add_argument("--confirm", required=True, help="Exact plan digest; required to execute")
    return git_ops


def run_cli(args, resolve_ops=None):
    """Dispatch a parsed ``git-ops`` command. Returns a JSON-serializable report.

    ``resolve_ops(args) -> GitOperations`` lets the parent inject its data
    directory; the default keeps the operations journal next to the target.
    """
    def ops_for(path):
        if resolve_ops is not None:
            return resolve_ops(args)
        return GitOperations(Path(path).resolve().parent / "git-operations")

    command = args.git_ops_command
    if command == "preflight":
        return ops_for(args.repo).upgrade_preflight(args.repo)
    if command == "reconcile":
        return ops_for(args.repo).reconcile_repository(
            args.repo, allow_stale_lock_removal=args.remove_stale_locks)
    if command == "backup":
        return ops_for(args.repo).backup_repository(args.repo, args.destination)
    if command == "restore":
        return ops_for(args.destination).restore_backup(args.bundle, args.destination)
    if command == "retention-plan":
        return ops_for(args.repo).plan_retention(
            args.repo, prefixes=tuple(args.prefix or ("refs/hivemind/candidates/",)))
    if command == "retention-execute":
        plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
        return ops_for(args.repo).execute_retention(args.repo, plan, confirmation=args.confirm)
    raise Problem("git_ops_command_unknown", 400)
