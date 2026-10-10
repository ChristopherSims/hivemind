"""Local standard-Git collaboration. No network listener or custom Git protocol."""
import hashlib
import json
import os
import re
import shutil
import time
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal
from types import SimpleNamespace

from fastapi import Depends, Header, Query
from pydantic import Field

from . import models as m
from .process_boundary import run_bounded
from .recovery import Journal
from .sdk import excluded
from .storage import Problem, canonical, now, uid

OID = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
COAUTHOR = "Co-authored-by: factory-droid[bot] <138933559+factory-droid[bot]@users.noreply.github.com>"


def install_receive_guard(repo):
    """Install the protected-ref guard in a newly created service-owned repository."""
    hooks = Path(repo) / "hooks"
    hooks.mkdir(exist_ok=True)
    hook = hooks / "pre-receive"
    hook.write_text(
        "#!/bin/sh\nwhile read old new ref; do\n"
        "  case \"$ref\" in\n"
        "    refs/heads/agents/agent_*) ;;\n"
        "    *) echo 'Protected refs require a fenced Nexus API merge' >&2; exit 1 ;;\n"
        "  esac\n"
        "done\n", encoding="utf-8")
    hook.chmod(0o700)
    return hooks


class GitWorkspaceCreate(m.Model):
    project_id: m.ID
    agent_id: m.ID


class GitCommit(m.Model):
    expected_revision: int = Field(ge=1)
    task_id: m.ID
    run_id: m.ID
    execution_id: m.ID
    message: str = Field(min_length=1, max_length=4096)
    paths: list[str] = Field(min_length=1, max_length=64)


class GitPush(m.Model):
    task_id: m.ID
    run_id: m.ID
    execution_id: m.ID
    source_oid: OID


class GitWorkspaceReconcile(GitPush):
    expected_revision: int = Field(ge=1)


class GitWorkspaceFetch(m.Model):
    expected_revision: int = Field(ge=1)
    task_id: m.ID
    run_id: m.ID
    execution_id: m.ID
    target_oid: OID


class MergeRequestCreate(m.Model):
    project_id: m.ID
    workspace_id: m.ID
    task_id: m.ID
    source_oid: OID
    target_oid: OID
    title: str = Field(min_length=1, max_length=512)


class MergeRequestReview(m.Model):
    expected_revision: int = Field(ge=1)
    source_oid: OID
    target_oid: OID
    candidate_oid: OID
    decision: Literal["approve", "reject"]
    reason: str = Field(min_length=1, max_length=4096)


class MergeRequestCheck(m.Model):
    expected_revision: int = Field(ge=1)
    check_name: str = Field(min_length=1, max_length=128)


class MergeRequestMerge(m.Model):
    expected_revision: int = Field(ge=1)
    source_oid: OID
    target_oid: OID
    candidate_oid: OID
    coordinator_epoch: int = Field(ge=1)
    lease_id: m.ID
    run_id: m.ID


class GitReconcile(m.Model):
    expected_revision: int = Field(ge=1)


def git_executable():
    executable = shutil.which("git")
    if not executable:
        raise Problem("git_not_installed", 503)
    code, output, _ = run_bounded([executable, "--version"], Path.cwd(), os.environ.copy(), 10)
    match = re.search(rb"git version (\d+)\.(\d+)", output)
    if code or not match or tuple(map(int, match.groups())) < (2, 38):
        raise Problem("git_version_unsupported", 503)
    return executable


class GitCollaboration:
    def __init__(self, service, *, checks=None, require_os_isolation=False):
        self.service, self.store = service, service.store
        self.root = self.store.directory.resolve() / "git-collaboration"
        self.checks = dict(checks or {})
        self.require_os_isolation = require_os_isolation is True
        if self.checks:
            from .verification import TrustedVerifier
            self.checks = TrustedVerifier(self.store.directory, self.checks).checks
        self._executable = None

    def _run(self, directory, *args, data=None, allowed=(0,), timeout=20):
        env = os.environ.copy()
        for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"):
            env.pop(name, None)
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_ALLOW_PROTOCOL"] = "file"  # No SSH, HTTP(S), git://, or arbitrary remote helpers.
        if self._executable is None:
            self._executable = git_executable()
        try:
            code, out, _err = run_bounded([self._executable, "-c", "core.longpaths=true",
                                         "--literal-pathspecs", *args], directory, env, timeout,
                                         input_bytes=data, max_output=262144)
        except TimeoutError:
            raise Problem("git_command_timeout", 409) from None
        except ValueError:
            raise Problem("git_transport_boundary_exceeded", 422) from None
        except OSError:
            raise Problem("git_process_unavailable", 503) from None
        if code not in allowed:
            raise Problem("git_command_failed", 409)
        return code, out.decode("utf-8", errors="strict").strip()

    def _git(self, directory, *args, **kwargs):
        return self._run(directory, *args, **kwargs)[1]

    def _repo(self, project):
        self.store.project(project)
        if self.root.drive.startswith("\\\\"):
            raise Problem("git_network_filesystem_not_supported", 403)
        return self.root / (hashlib.sha256(project.encode()).hexdigest() + ".git")

    def _audit(self, principal, action, data):
        # Independent durable ledger survives rollback of the canonical API command.
        self.root.mkdir(parents=True, exist_ok=True)
        with Journal(self.root / "audit.sqlite3") as journal:
            journal.checkpoint(label=action, metadata={"principal_id": principal["id"], **data})
        self.store.db.execute("INSERT INTO audit VALUES(?,?,?,?,?)",
            (uid("audit"), principal["id"], "git:" + action, data.get("merge_request_id") or data["project_id"], now()))

    def _save(self, kind, body):
        row = self.store.raw(body["id"], kind)
        return self.store.save(kind, row["project"], row["owner"], body, expected=body["revision"])

    def initialize(self, principal, project):
        self.service.project_access(principal, project)
        if principal["role"] not in ("admin", "human"):
            raise Problem("operator_required", 403)
        repo = self._repo(project)
        if repo.exists():
            if not (repo / "hivemind-ready").exists():
                raise Problem("git_initialization_incomplete", 409)
            return {"project_id": project, "main_oid": self._ref(repo, "refs/heads/main")}
        self.root.mkdir(parents=True, exist_ok=True)
        self._identity(self.root)
        self._git(self.root, "init", "--bare", "--object-format=sha1", "--initial-branch=main", str(repo))
        self._git(repo, "config", "receive.denyNonFastForwards", "true")
        self._git(repo, "config", "receive.denyDeletes", "true")
        self._git(repo, "config", "receive.fsckObjects", "true")
        self._git(repo, "config", "core.longpaths", "true")
        self._git(repo, "config", "core.logAllRefUpdates", "true")
        hooks = install_receive_guard(repo)
        self._git(repo, "config", "core.hooksPath", str(hooks))
        tree = self._git(repo, "mktree", data=b"")
        self._git(repo, "ls-tree", tree)
        commit = self._git(repo, "commit-tree", tree, data=f"Initialize project repository\n\n{COAUTHOR}\n".encode())
        self._git(repo, "update-ref", "refs/heads/main", commit, "0" * 40)
        (repo / "hivemind-ready").write_text("local-only\n", encoding="utf-8")
        lease = self.service.lease(principal, project)
        if lease:
            self.fence(lease)
        self._audit(principal, "repository.initialized", {"project_id": project, "target_oid": commit})
        return {"project_id": project, "main_oid": commit}

    def _ready(self, principal, project):
        self.service.project_access(principal, project)
        repo = self._repo(project)
        if not (repo / "hivemind-ready").exists():
            raise Problem("git_repository_not_initialized", 409)
        return repo

    def _ref(self, repo, ref):
        return self._git(repo, "rev-parse", "--verify", ref)

    def _commit_oid(self, repo, oid):
        if self._git(repo, "cat-file", "-t", oid) != "commit":
            raise Problem("git_commit_required", 422)
        return oid

    def _identity(self, directory):
        code, _ = self._run(directory, "var", "GIT_AUTHOR_IDENT", allowed=(0, 128))
        if code:
            raise Problem("git_author_identity_missing", 503)

    def fence(self, lease):
        repo = self._repo(lease["project_id"])
        if not (repo / "hivemind-ready").exists():
            return
        value = canonical({"lease_id": lease["id"], "epoch": lease["coordinator_epoch"]}).encode()
        oid = self._git(repo, "hash-object", "-w", "--stdin", data=value)
        self._git(repo, "update-ref", "refs/hivemind/nexus-fence", oid)

    def workspace(self, principal, request):
        repo = self._ready(principal, request.project_id)
        self.service.represent(principal, request.agent_id)
        self.service.reference(principal, request.project_id, request.agent_id, "agent")
        for workspace in self.store.list("git_workspace", request.project_id, principal, 1000):
            if workspace["agent_id"] == request.agent_id:
                return workspace
        path = self.root / "workspaces" / hashlib.sha256(
            f"{request.project_id}:{request.agent_id}".encode()).hexdigest()
        if path.exists():
            raise Problem("git_workspace_recovery_required", 409)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._git(path.parent, "clone", "--no-local", str(repo), str(path))
        self._git(path, "config", "core.longpaths", "true")
        branch = "agents/" + request.agent_id
        code, _ = self._run(repo, "show-ref", "--verify", "--quiet", "refs/heads/" + branch, allowed=(0, 1))
        if code == 0:
            self._git(path, "switch", "--track", "-c", branch, "origin/" + branch)
        else:
            self._git(path, "switch", "-c", branch)
        body = self.service.create(principal, "git_workspace", {
            "project_id": request.project_id, "agent_id": request.agent_id, "branch": branch,
            "workspace_path": str(path), "head_oid": self._ref(path, "HEAD")}, private=request.agent_id)
        self._audit(principal, "workspace.created", {"project_id": request.project_id, "agent_id": request.agent_id})
        return body

    def _workspace(self, principal, identity):
        workspace = self.store.get(identity, principal, "git_workspace")
        self.service.represent(principal, workspace["agent_id"])
        repo = self._ready(principal, workspace["project_id"])
        path = Path(workspace["workspace_path"]).resolve(strict=True)
        path.relative_to(self.root / "workspaces")
        if self._git(path, "symbolic-ref", "--short", "HEAD") != workspace["branch"]:
            raise Problem("git_workspace_branch_changed", 409)
        if self._git(path, "remote", "get-url", "origin") != str(repo):
            raise Problem("git_remote_changed", 409)
        return workspace, path, repo

    def _permit(self, principal, workspace, request, paths):
        if len(paths) > 64:
            raise Problem("git_change_set_too_large", 422)
        task = self.service.reference(principal, workspace["project_id"], request.task_id, "task")
        run = self.service.run(principal, request.run_id)
        execution = task.get("execution") or {}
        if (run["agent_id"] != workspace["agent_id"] or run.get("task_id") != task["id"]
                or execution.get("id") != request.execution_id or execution.get("run_id") != run["id"]):
            raise Problem("stale_task_execution", 409)
        if not task.get("scope_ids"):
            raise Problem("git_file_scope_required", 403)
        for item in paths:
            parsed = PurePosixPath(item)
            if (not item or item.strip() != item or parsed.is_absolute() or ".." in parsed.parts
                    or any(character in item for character in ("\\", "\0", "\n", "\r")) or excluded(parsed)):
                raise Problem("git_path_not_permitted", 403)
        self.service.permits(principal, run["id"], m.PermitRequest(scope_ids=task["scope_ids"], paths=paths))
        return task

    def _new_paths(self, repo, target, source):
        commits = self._git(repo, "rev-list", "--max-count=257", target + ".." + source).splitlines()
        if len(commits) > 256:
            raise Problem("git_change_set_too_large", 422)
        paths = set()
        for commit in commits:
            parents = self._git(repo, "show", "--no-patch", "--format=%P", commit).split()
            baseline_parents = []
            if len(parents) > 1:
                for parent in parents:
                    code, _ = self._run(repo, "merge-base", "--is-ancestor", parent, target, allowed=(0, 1))
                    if code == 0:
                        baseline_parents.append(parent)
            if baseline_parents:
                # Imported protected history is a baseline, not a new agent edit outside its scope.
                for parent in baseline_parents:
                    paths.update(filter(None, self._git(repo, "diff", "--name-only", "-z", parent, commit).split("\0")))
                continue
            paths.update(filter(None, self._git(repo, "diff-tree", "--root", "-m", "-r",
                "--no-commit-id", "--name-only", "-z", commit).split("\0")))
        return sorted(paths)

    def sync_workspace(self, principal, identity, request):
        workspace, path, repo = self._workspace(principal, identity)
        if workspace["revision"] != request.expected_revision or self._ref(path, "HEAD") != request.source_oid:
            raise Problem("git_head_changed", 409)
        paths = self._new_paths(path, self._ref(repo, "refs/heads/main"), request.source_oid)
        self._permit(principal, workspace, request, paths)
        workspace["head_oid"] = request.source_oid
        self._audit(principal, "workspace.reconciled", {"project_id": workspace["project_id"],
            "task_id": request.task_id, "source_oid": request.source_oid})
        return self._save("git_workspace", workspace)

    def fetch_workspace(self, principal, identity, request):
        workspace, path, repo = self._workspace(principal, identity)
        if workspace["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        self._permit(principal, workspace, request, [])
        protected = self._ref(repo, "refs/heads/main")
        if protected != request.target_oid:
            raise Problem("git_head_changed", 409)
        if self._ref(path, "HEAD") != workspace["head_oid"]:
            raise Problem("git_workspace_recovery_required", 409)
        if self._git(path, "status", "--porcelain"):
            raise Problem("git_workspace_dirty", 409)
        self._audit(principal, "fetch.intent", {"project_id": workspace["project_id"],
            "task_id": request.task_id, "target_oid": protected, "workspace_id": identity})
        self._git(path, "fetch", str(repo), "refs/heads/main")
        code, _ = self._run(path, "merge-base", "--is-ancestor", "HEAD", protected, allowed=(0, 1))
        if code == 0 and workspace["head_oid"] != protected:
            self._git(path, "merge", "--ff-only", "--no-overwrite-ignore", protected)
            workspace["head_oid"] = protected
            workspace = self._save("git_workspace", workspace)
        self._audit(principal, "fetch.completed", {"project_id": workspace["project_id"],
            "task_id": request.task_id, "target_oid": protected, "workspace_id": identity,
            "fast_forwarded": code == 0})
        return {**workspace, "protected_oid": protected, "pending_unmerged": code != 0}

    def commit(self, principal, identity, request):
        workspace, path, repo = self._workspace(principal, identity)
        if workspace["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        self._permit(principal, workspace, request, request.paths)
        self._identity(path)
        for item in request.paths:
            (path / item).resolve().relative_to(path)
        staged = self._git(path, "diff", "--cached", "--name-only", "-z").split("\0")
        extra = set(filter(None, staged)) - set(request.paths)
        merge_code, merge_head = self._run(path, "rev-parse", "-q", "--verify", "MERGE_HEAD", allowed=(0, 1))
        protected = self._ref(repo, "refs/heads/main")
        baseline = set()
        if merge_code == 0:
            if merge_head != protected or self._git(path, "ls-files", "-u"):
                raise Problem("git_unverified_merge_baseline", 409)
            if extra and self._git(path, "diff", "--cached", "--name-only", "-z", protected, "--", *sorted(extra)):
                raise Problem("git_unrelated_staged_changes", 409)
            baseline = extra
        elif extra:
            raise Problem("git_unrelated_staged_changes", 409)
        old_head = self._ref(path, "HEAD")
        if old_head != workspace["head_oid"]:
            raise Problem("git_workspace_recovery_required", 409)
        self._git(path, "add", "--", *request.paths)
        staged = list(filter(None, self._git(path, "diff", "--cached", "--name-only", "-z").split("\0")))
        self._permit(principal, workspace, request, [item for item in staged if item not in baseline])
        # Review the exact index/status before invoking the ordinary configured Git commit.
        diff = self._git(path, "diff", "--cached", "--no-ext-diff", "--binary")
        self._git(path, "diff", "--cached", "--stat")
        self._git(path, "status", "--porcelain=v1")
        self._audit(principal, "commit.intent", {"project_id": workspace["project_id"], "task_id": request.task_id,
            "agent_id": workspace["agent_id"], "previous_oid": old_head, "paths": staged,
            "protected_baseline_oid": protected if merge_code == 0 else None,
            "staged_diff_sha256": hashlib.sha256(diff.encode()).hexdigest()})
        message = request.message.rstrip() + "\n\n" + COAUTHOR + "\n"
        self._git(path, "commit", "--file=-", data=message.encode())
        workspace["head_oid"] = self._ref(path, "HEAD")
        result = self._save("git_workspace", workspace)
        self._audit(principal, "commit.created", {"project_id": workspace["project_id"],
            "task_id": request.task_id, "source_oid": result["head_oid"]})
        return result

    def push(self, principal, identity, request):
        workspace, path, repo = self._workspace(principal, identity)
        if self._ref(path, "HEAD") != request.source_oid:
            raise Problem("git_source_changed", 409)
        target = self._ref(repo, "refs/heads/main")
        paths = self._new_paths(path, target, request.source_oid)
        self._permit(principal, workspace, request, paths)
        self._audit(principal, "push.intent", {"project_id": workspace["project_id"], "task_id": request.task_id,
            "source_oid": request.source_oid, "branch": workspace["branch"]})
        self._git(path, "push", str(repo), request.source_oid + ":refs/heads/" + workspace["branch"])
        self._audit(principal, "push.completed", {"project_id": workspace["project_id"],
            "task_id": request.task_id, "source_oid": request.source_oid})
        return {"project_id": workspace["project_id"], "source_oid": request.source_oid, "branch": workspace["branch"]}

    def submit(self, principal, request):
        workspace, _path, repo = self._workspace(principal, request.workspace_id)
        if workspace["project_id"] != request.project_id:
            raise Problem("not_found", 404)
        task = self.service.reference(principal, request.project_id, request.task_id, "task")
        if (task.get("execution") or {}).get("agent_id") != workspace["agent_id"]:
            raise Problem("git_task_owner_required", 403)
        if (self._ref(repo, "refs/heads/" + workspace["branch"]) != request.source_oid
                or self._ref(repo, "refs/heads/main") != request.target_oid):
            raise Problem("git_head_changed", 409)
        if request.source_oid == request.target_oid:
            raise Problem("git_no_changes", 409)
        self._commit_oid(repo, request.source_oid)
        paths = self._new_paths(repo, request.target_oid, request.source_oid)
        allowed = [prefix for scope in task.get("scope_ids", []) for prefix in
                   self.service.reference(principal, request.project_id, scope, "scope").get("allowed_paths", [])]
        sensitive = self.store.project(request.project_id)["path_case_sensitive"]
        for path in paths:
            value = path if sensitive else path.casefold()
            prefixes = allowed if sensitive else [prefix.casefold() for prefix in allowed]
            if excluded(PurePosixPath(path)) or not any(value == prefix or value.startswith(prefix + "/")
                                                      for prefix in prefixes):
                raise Problem("path_outside_scope", 403)
        identity = uid("merge_request")
        code, output = self._run(repo, "merge-tree", "--write-tree", request.target_oid, request.source_oid,
                                 allowed=(0, 1))
        body = {**request.model_dump(), "id": identity, "agent_id": workspace["agent_id"], "branch": workspace["branch"],
                "task_execution_id": task["execution"]["id"],
                "status": "conflicts" if code else "open", "candidate_oid": None,
                "conflict_paths": [], "reviews": [], "checks": []}
        if code:
            # Git reports conflicts; agents receive filenames, not arbitrary stdout instructions.
            lines = output.splitlines()
            body["conflict_paths"] = [line.split("\t")[-1] for line in lines[1:] if "\t" in line][:256]
            notice = self.service.create(principal, "notification", {
                "project_id": request.project_id, "recipient_agent_id": workspace["agent_id"],
                "type": "git_merge_conflict", "merge_request_id": identity,
                "source_oid": request.source_oid, "target_oid": request.target_oid,
                "conflict_paths": body["conflict_paths"], "state": "unread", "expires_at": time.time() + 86400},
                private=workspace["agent_id"])
            body["notification_id"] = notice["id"]
        else:
            tree = output.splitlines()[0]
            self._identity(repo)
            self._git(repo, "diff", "--stat", request.target_oid, tree)
            message = f"Merge task {task['id']} via {identity}\n\n{COAUTHOR}\n".encode()
            body["candidate_oid"] = self._git(repo, "commit-tree", tree, "-p", request.target_oid,
                                             "-p", request.source_oid, data=message)
            self._git(repo, "update-ref", "refs/hivemind/candidates/" + identity, body["candidate_oid"])
        result = self.service.create(principal, "merge_request", body)
        self.store.relate(request.project_id, result["id"], request.task_id, "SUBMITS_FOR_TASK")
        self._audit(principal, "submission", {"project_id": request.project_id, "merge_request_id": identity,
            "task_id": request.task_id, "source_oid": request.source_oid, "target_oid": request.target_oid,
            "candidate_oid": body["candidate_oid"], "status": body["status"]})
        return result

    def _fresh(self, principal, identity, revision):
        body = self.store.get(identity, principal, "merge_request")
        repo = self._ready(principal, body["project_id"])
        if body["revision"] != revision:
            raise Problem("revision_conflict", 412)
        if body["status"] != "open":
            raise Problem("merge_request_not_open", 409)
        task = self.service.reference(principal, body["project_id"], body["task_id"], "task")
        if (task.get("execution") or {}).get("id") != body["task_execution_id"]:
            raise Problem("stale_task_execution", 409)
        if (self._ref(repo, "refs/heads/main") != body["target_oid"]
                or self._ref(repo, "refs/heads/" + body["branch"]) != body["source_oid"]):
            raise Problem("git_head_changed", 409)
        return body, repo

    def review(self, principal, identity, request):
        body, _ = self._fresh(principal, identity, request.expected_revision)
        if principal["role"] not in ("admin", "human", "agent") or principal.get("agent") == body["agent_id"]:
            raise Problem("independent_reviewer_required", 403)
        if any(getattr(request, key) != body[key] for key in ("source_oid", "target_oid", "candidate_oid")):
            raise Problem("git_review_hash_mismatch", 409)
        review = {"principal_id": principal["id"], "agent_id": principal.get("agent"),
            "decision": request.decision, "reason": request.reason, "at": now(),
            **{key: body[key] for key in ("source_oid", "target_oid", "candidate_oid")}}
        body["reviews"] = [item for item in body["reviews"] if item["principal_id"] != principal["id"]] + [review]
        self._audit(principal, "review", {"project_id": body["project_id"], "merge_request_id": identity, **review})
        return self._save("merge_request", body)

    def _check_policy(self, name):
        return hashlib.sha256(canonical({
            "argv": self.checks[name], "require_os_isolation": self.require_os_isolation,
        }).encode()).hexdigest()

    def check(self, principal, identity, request):
        if principal["role"] not in ("admin", "human"):
            raise Problem("trusted_verifier_required", 403)
        body, repo = self._fresh(principal, identity, request.expected_revision)
        if request.check_name not in self.checks:
            raise Problem("git_check_not_configured", 503)
        check_root = self.root / "checks" / uid("checkout")
        check_root.parent.mkdir(parents=True, exist_ok=True)
        self._git(check_root.parent, "clone", "--no-local", "--no-checkout", str(repo), str(check_root))
        self._git(check_root, "fetch", str(repo), "refs/hivemind/candidates/" + identity)
        self._git(check_root, "checkout", "--detach", body["candidate_oid"])
        from .verification import TrustedVerifier
        verifier = TrustedVerifier(check_root, {request.check_name: self.checks[request.check_name]},
                                   require_os_isolation=self.require_os_isolation)
        self._audit(principal, "check.intent", {"project_id": body["project_id"], "merge_request_id": identity,
            "candidate_oid": body["candidate_oid"], "check_name": request.check_name})
        from .agent_session import HandlerJournal
        from .recovery import UnknownOutcome
        with Journal(self.root / "check-effects.sqlite3") as journal:
            gate = HandlerJournal(journal, body["project_id"], principal["id"])
            fingerprint = hashlib.sha256(canonical({"id": identity, "revision": body["revision"],
                "candidate": body["candidate_oid"], "check": request.check_name,
                "argv": self.checks[request.check_name]}).encode()).hexdigest()
            try:
                receipt = gate.handle("git-check", fingerprint,
                    lambda: verifier.run(request.check_name).as_record())
                result = receipt["result"].get("value")
            except UnknownOutcome:
                raise Problem("git_check_effect_unknown", 409) from None
            if not result:
                raise Problem("git_check_effect_unknown", 409)
        if (self._ref(check_root, "HEAD") != body["candidate_oid"]
                or self._git(check_root, "status", "--porcelain", "--untracked-files=no")):
            result["ok"] = False
            result["error"] = "check_modified_snapshot"
        record = {"name": request.check_name, "ok": result["ok"], "at": now(),
            "result_sha256": hashlib.sha256(canonical(result).encode()).hexdigest(),
            "policy_sha256": self._check_policy(request.check_name),
            **{key: body[key] for key in ("source_oid", "target_oid", "candidate_oid")}}
        body["checks"] = [item for item in body["checks"] if item["name"] != request.check_name] + [record]
        self._audit(principal, "check.completed", {"project_id": body["project_id"], "merge_request_id": identity, **record})
        return self._save("merge_request", body)

    def merge(self, principal, identity, request):
        body, repo = self._fresh(principal, identity, request.expected_revision)
        lease = self.service.coordinator(principal, body["project_id"], request.coordinator_epoch)
        if (principal["role"] != "agent" or principal.get("agent") != lease["agent_id"]
                or lease["id"] != request.lease_id or lease["run_id"] != request.run_id):
            raise Problem("current_nexus_required", 403)
        run = self.service.run(principal, request.run_id)
        self.service.permits(principal, request.run_id, SimpleNamespace(scope_ids=[], paths=[]))
        if run["status"] not in ("running", "idle") or run["last_seen_at"] < time.time() - 30:
            raise Problem("nexus_run_not_live", 409)
        if lease["expires_at"] < time.time() + 6:
            raise Problem("nexus_lease_too_short", 409)
        if any(getattr(request, key) != body[key] for key in ("source_oid", "target_oid", "candidate_oid")):
            raise Problem("git_merge_hash_mismatch", 409)
        if not self.checks or any(not any(item["name"] == name and item["ok"]
                and item.get("policy_sha256") == self._check_policy(name)
                and all(item[key] == body[key] for key in ("source_oid", "target_oid", "candidate_oid"))
                for item in body["checks"])
                                  for name in self.checks):
            raise Problem("git_passing_checks_required", 409)
        if (not any(item["decision"] == "approve" for item in body["reviews"])
                or any(item["decision"] == "reject" for item in body["reviews"])):
            raise Problem("git_approval_required", 409)
        fence = self._git(repo, "hash-object", "--stdin", data=canonical(
            {"lease_id": lease["id"], "epoch": lease["coordinator_epoch"]}).encode())
        if self._ref(repo, "refs/hivemind/nexus-fence") != fence:
            raise Problem("stale_coordinator_epoch", 409)
        self._audit(principal, "merge.intent", {"project_id": body["project_id"], "merge_request_id": identity,
            "source_oid": body["source_oid"], "target_oid": body["target_oid"], "candidate_oid": body["candidate_oid"],
            "lease_id": lease["id"], "coordinator_epoch": lease["coordinator_epoch"],
            "nexus_agent_id": lease["agent_id"]})
        # One native Git transaction checks both the branch CAS and Nexus fencing ref.
        transaction = (f"start\nverify refs/hivemind/nexus-fence {fence}\n"
            f"update refs/heads/main {body['candidate_oid']} {body['target_oid']}\nprepare\ncommit\n").encode()
        current = self.service.coordinator(principal, body["project_id"], request.coordinator_epoch)
        if current["id"] != lease["id"] or current["expires_at"] < time.time() + 6:
            raise Problem("stale_coordinator_epoch", 409)
        self._git(repo, "update-ref", "-m", f"Hivemind {identity} epoch {lease['coordinator_epoch']}",
                  "--stdin", data=transaction, timeout=5)
        body.update(status="merged", merged_oid=body["candidate_oid"], merged_at=now(),
                    merged_by_agent_id=lease["agent_id"], coordinator_epoch=lease["coordinator_epoch"],
                    lease_id=lease["id"])
        self._audit(principal, "merge.completed", {"project_id": body["project_id"], "merge_request_id": identity,
            "merged_oid": body["candidate_oid"], "lease_id": lease["id"],
            "coordinator_epoch": lease["coordinator_epoch"]})
        return self._save("merge_request", body)

    def reconcile(self, principal, identity, request):
        if principal["role"] not in ("admin", "human"):
            raise Problem("operator_required", 403)
        body = self.store.get(identity, principal, "merge_request")
        if body["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        if body["status"] != "open" or not body.get("candidate_oid"):
            raise Problem("merge_request_not_open", 409)
        intent = None
        with Journal(self.root / "audit.sqlite3") as journal:
            rows = journal.db.execute("SELECT metadata FROM checkpoints WHERE label='merge.intent' "
                                      "ORDER BY rowid DESC").fetchall()
            for row in rows:
                value = json.loads(row["metadata"])
                if value.get("merge_request_id") == identity and all(
                        value.get(key) == body[key] for key in ("source_oid", "target_oid", "candidate_oid")):
                    intent = value
                    break
        if not intent:
            raise Problem("git_merge_evidence_unavailable", 409)
        repo = self._ready(principal, body["project_id"])
        code, _ = self._run(repo, "merge-base", "--is-ancestor", body["candidate_oid"], "refs/heads/main",
                            allowed=(0, 1))
        expected = f"{body['candidate_oid']}\0Hivemind {identity} epoch {intent['coordinator_epoch']}"
        reflog = self._git(repo, "reflog", "show", "--format=%H%x00%gs", "refs/heads/main").splitlines()
        if code or expected not in reflog:
            raise Problem("git_merge_evidence_unavailable", 409)
        body.update(status="merged", merged_oid=body["candidate_oid"], reconciled_at=now(),
                    coordinator_epoch=intent["coordinator_epoch"], lease_id=intent["lease_id"],
                    merged_by_agent_id=intent["nexus_agent_id"])
        self._audit(principal, "merge.reconciled", {"project_id": body["project_id"],
            "merge_request_id": identity, "merged_oid": body["candidate_oid"],
            "coordinator_epoch": intent["coordinator_epoch"], "lease_id": intent["lease_id"]})
        return self._save("merge_request", body)


def install(app, service, principal, command, envelope, checks=None, require_os_isolation=False):
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    domain = GitCollaboration(service, checks=checks, require_os_isolation=require_os_isolation)
    app.state.git_collaboration = domain
    service.git_collaboration = domain

    @app.post("/v0.1/projects/{project_id}/git:initialize")
    async def initialize(project_id: str, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "git-initialize:" + project_id, idempotency_key, {},
                       lambda: domain.initialize(p, project_id))

    @app.post("/v0.1/git-workspaces")
    async def workspace(body: GitWorkspaceCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "git-workspace", idempotency_key, body, lambda: domain.workspace(p, body))

    @app.get("/v0.1/projects/{project_id}/git")
    async def repository(project_id: str, p=Depends(principal)):
        repo = domain._ready(p, project_id)
        return envelope({"project_id": project_id, "protected_branch": "main",
                         "main_oid": domain._ref(repo, "refs/heads/main"), "transport": "local-only"})

    @app.get("/v0.1/git-workspaces/{identity}")
    async def workspace_status(identity: str, p=Depends(principal)):
        body, path, _ = domain._workspace(p, identity)
        return envelope({**body, "observed_head_oid": domain._ref(path, "HEAD"),
                         "dirty": bool(domain._git(path, "status", "--porcelain"))})

    @app.post("/v0.1/git-workspaces/{identity}:commit")
    async def commit(identity: str, body: GitCommit, p=Depends(principal),
                     idempotency_key: str | None = Header(None)):
        return command(p, "git-commit:" + identity, idempotency_key, body, lambda: domain.commit(p, identity, body))

    @app.post("/v0.1/git-workspaces/{identity}:push")
    async def push(identity: str, body: GitPush, p=Depends(principal),
                   idempotency_key: str | None = Header(None)):
        return command(p, "git-push:" + identity, idempotency_key, body, lambda: domain.push(p, identity, body))

    @app.post("/v0.1/git-workspaces/{identity}:fetch")
    async def fetch(identity: str, body: GitWorkspaceFetch, p=Depends(principal),
                    idempotency_key: str | None = Header(None)):
        return command(p, "git-fetch:" + identity, idempotency_key, body,
                       lambda: domain.fetch_workspace(p, identity, body))

    @app.post("/v0.1/git-workspaces/{identity}:reconcile")
    async def sync(identity: str, body: GitWorkspaceReconcile, p=Depends(principal),
                   idempotency_key: str | None = Header(None)):
        return command(p, "git-workspace-sync:" + identity, idempotency_key, body,
                       lambda: domain.sync_workspace(p, identity, body))

    @app.post("/v0.1/merge-requests")
    async def submit(body: MergeRequestCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "git-submit", idempotency_key, body, lambda: domain.submit(p, body))

    @app.get("/v0.1/merge-requests")
    async def requests(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        return envelope(service.store.list("merge_request", project_id, p, 1000))

    @app.get("/v0.1/projects/{project_id}/git:audit")
    async def audit(project_id: str, limit: int = Query(100, ge=1, le=500), p=Depends(principal)):
        domain._ready(p, project_id)
        with Journal(domain.root / "audit.sqlite3") as journal:
            rows = journal.db.execute("SELECT checkpoint_id,label,metadata,created_at FROM checkpoints "
                "WHERE json_extract(metadata,'$.project_id')=? ORDER BY rowid DESC LIMIT ?",
                (project_id, limit)).fetchall()
        return envelope([{"id": row["checkpoint_id"], "action": row["label"],
                          "at": row["created_at"], "data": json.loads(row["metadata"])} for row in rows])

    @app.get("/v0.1/merge-requests/{identity}")
    async def request(identity: str, p=Depends(principal)):
        return envelope(service.store.get(identity, p, "merge_request"))

    @app.post("/v0.1/merge-requests/{identity}:review")
    async def review(identity: str, body: MergeRequestReview, p=Depends(principal),
                     idempotency_key: str | None = Header(None)):
        return command(p, "git-review:" + identity, idempotency_key, body, lambda: domain.review(p, identity, body))

    @app.post("/v0.1/merge-requests/{identity}:check")
    async def check(identity: str, body: MergeRequestCheck, p=Depends(principal),
                    idempotency_key: str | None = Header(None)):
        return command(p, "git-check:" + identity, idempotency_key, body, lambda: domain.check(p, identity, body))

    @app.post("/v0.1/merge-requests/{identity}:merge")
    async def merge(identity: str, body: MergeRequestMerge, p=Depends(principal),
                    idempotency_key: str | None = Header(None)):
        return command(p, "git-merge:" + identity, idempotency_key, body, lambda: domain.merge(p, identity, body))

    @app.post("/v0.1/merge-requests/{identity}:reconcile")
    async def reconcile(identity: str, body: GitReconcile, p=Depends(principal),
                        idempotency_key: str | None = Header(None)):
        return command(p, "git-reconcile:" + identity, idempotency_key, body,
                       lambda: domain.reconcile(p, identity, body))
