"""Real local Git setup and fake vendor launch: no model or network calls."""
import hashlib
import shutil
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from hivemind.worker_repository import RepositoryCheckout, _validate_tree
from hivemind.storage import Problem
from test_worker_provisioning import create, enabled as provisioning_enabled

enabled = provisioning_enabled


def git(root, *args, data=None):
    executable = shutil.which("git")
    if executable is None:
        pytest.skip("Git not installed")
    result = subprocess.run([executable, "-C", str(root), *args], input=data,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=30)
    return result.stdout.decode().strip()


def source_repository(enabled):
    roots, policy_file, _, handle = enabled
    source = roots["repository"]
    git(source, "init", "--template=")
    blob = git(source, "hash-object", "-w", "--stdin", data=b"committed contents\n")
    tree = git(source, "mktree", data=f"100644 blob {blob}\tapp.txt\n".encode())
    # Use the configured Git identity, never override author/committer settings.
    commit = git(source, "commit-tree", "--no-gpg-sign", tree, data=b"Seed repository\n")
    git(source, "update-ref", "refs/heads/main", commit)
    git(source, "symbolic-ref", "HEAD", "refs/heads/main")
    git(source, "reset", "--hard", commit)
    policy_file.write_bytes(b'{"version":1,"max_runtime_seconds":60}')
    profile = replace(handle.profiles["local"], workspace_mode="git_clone",
                      allow_native_git=True, policy_sha256=hashlib.sha256(policy_file.read_bytes()).hexdigest())
    handle.profiles["local"] = profile
    return source, commit, profile


def test_checkout_is_exact_independent_and_preserves_dirty_source(world, enabled):
    source, commit, _ = source_repository(enabled)
    (source / "app.txt").write_text("staged change\n")
    git(source, "add", "app.txt")
    (source / "app.txt").write_text("unstaged change\n")
    (source / "untracked.txt").write_text("do not copy\n")
    before_index = (source / ".git/index").read_bytes()
    before_head = git(source, "rev-parse", "HEAD")
    worker = create(world)
    roots, _, calls, _ = enabled
    target = roots["workspaces"] / worker["id"]
    assert (target / "app.txt").read_bytes() == b"committed contents\n"
    assert not (target / "untracked.txt").exists()
    assert (source / "app.txt").read_text() == "unstaged change\n"
    assert (source / ".git/index").read_bytes() == before_index
    assert git(source, "rev-parse", "HEAD") == before_head
    assert git(target, "rev-parse", "HEAD") == commit
    assert git(target, "symbolic-ref", "--short", "HEAD") == worker["workspace"]["branch"]
    assert worker["workspace"]["source_commit"] == commit
    assert worker["workspace"]["working_tree_bytes"] == len(b"committed contents\n")
    assert calls[0][0]["workspace_source"]["source_commit"] == commit
    assert not (target / ".git/objects/info/alternates").exists()
    assert git(target, "remote") == ""
    (target / "app.txt").write_text("worker only\n")
    assert (source / "app.txt").read_text() == "unstaged change\n"
    assert create(world)["id"] == worker["id"]
    assert len(calls) == 1


def test_snapshot_survives_source_branch_movement(world, enabled, monkeypatch):
    from hivemind.worker_repository import RepositoryCheckout

    source, commit, _ = source_repository(enabled)
    original = RepositoryCheckout.materialize

    def moved(self, selected, identity):
        tree = git(source, "rev-parse", f"{commit}^{{tree}}")
        next_commit = git(source, "commit-tree", "--no-gpg-sign", tree, "-p", commit,
                          data=b"Move source head\n")
        git(source, "update-ref", "refs/heads/main", next_commit)
        return original(self, selected, identity)

    monkeypatch.setattr(RepositoryCheckout, "materialize", moved)
    worker = create(world)
    assert worker["workspace"]["source_commit"] == commit
    assert git(enabled[0]["workspaces"] / worker["id"], "rev-parse", "HEAD") == commit
    assert git(source, "rev-parse", "HEAD") != commit


def test_source_replace_refs_do_not_change_exact_commit_tree(world, enabled):
    source, commit, _ = source_repository(enabled)
    blob = git(source, "hash-object", "-w", "--stdin", data=b"replacement content\n")
    tree = git(source, "mktree", data=f"100644 blob {blob}\tapp.txt\n".encode())
    replacement = git(source, "commit-tree", "--no-gpg-sign", tree, data=b"Replacement\n")
    git(source, "replace", commit, replacement)
    worker = create(world)
    assert worker["workspace"]["source_commit"] == commit
    target = enabled[0]["workspaces"] / worker["id"]
    assert (target / "app.txt").read_bytes() == b"committed contents\n"
    assert worker["workspace"]["tracked_bytes"] == len(b"committed contents\n")


@pytest.mark.parametrize("mode", ["120000", "160000"])
def test_symlinks_and_submodules_refuse_before_vendor_launch(world, enabled, mode):
    source, commit, _ = source_repository(enabled)
    oid = commit if mode == "160000" else git(
        source, "hash-object", "-w", "--stdin", data=b"../private/credential\n")
    kind = "commit" if mode == "160000" else "blob"
    tree = git(source, "mktree", data=f"{mode} {kind} {oid}\tunsafe\n".encode())
    unsafe = git(source, "commit-tree", "--no-gpg-sign", tree, data=b"Unsafe tree\n")
    git(source, "update-ref", "refs/heads/main", unsafe)
    create(world, status=409)
    assert not enabled[2]
    assert not list(enabled[0]["private"].glob("*/worker.json"))
    # The cached intent cannot silently retry even after the source is corrected.
    git(source, "update-ref", "refs/heads/main", commit)
    replay = create(world)
    assert replay["state"] == "unknown"
    assert not enabled[2]


def test_no_source_filters_hooks_or_config_are_inherited(world, enabled):
    source, _, _ = source_repository(enabled)
    attributes = git(source, "hash-object", "-w", "--stdin", data=b"app.txt filter=trap\n")
    app = git(source, "rev-parse", "HEAD:app.txt")
    tree = git(source, "mktree", data=(
        f"100644 blob {attributes}\t.gitattributes\n100644 blob {app}\tapp.txt\n").encode())
    commit = git(source, "commit-tree", "--no-gpg-sign", tree, data=b"Filter declaration\n")
    git(source, "update-ref", "refs/heads/main", commit)
    marker = enabled[0]["private"] / "filter-ran"
    git(source, "config", "filter.trap.smudge", f'cmd /c echo unsafe > "{marker}"')
    # Config is intentionally ignored in the independent target repository.
    (source / ".git/hooks").mkdir()
    (source / ".git/hooks/post-checkout").write_text("exit 99\n")
    worker = create(world)
    target = enabled[0]["workspaces"] / worker["id"]
    assert not marker.exists()
    assert not (target / ".git/hooks/post-checkout").exists()
    assert subprocess.run([shutil.which("git"), "-C", str(target), "config", "--local",
                           "--get", "filter.trap.smudge"], capture_output=True).returncode == 1


def test_checkout_limit_failure_holds_and_never_launches(world, enabled):
    _, _, profile = source_repository(enabled)
    enabled[3].profiles["local"] = replace(profile, checkout_max_bytes=1)
    create(world, status=409)
    assert not enabled[2]
    assert create(world)["state"] == "unknown"


def test_materialized_eol_expansion_respects_byte_limit(world, enabled):
    source, _, profile = source_repository(enabled)
    attributes = b"app.txt text eol=crlf\n"
    contents = b"x\n" * 100
    attr_blob = git(source, "hash-object", "-w", "--stdin", data=attributes)
    app_blob = git(source, "hash-object", "-w", "--stdin", data=contents)
    tree = git(source, "mktree", data=(
        f"100644 blob {attr_blob}\t.gitattributes\n100644 blob {app_blob}\tapp.txt\n").encode())
    commit = git(source, "commit-tree", "--no-gpg-sign", tree, data=b"EOL attributes\n")
    git(source, "update-ref", "refs/heads/main", commit)
    enabled[3].profiles["local"] = replace(
        profile, checkout_max_bytes=len(attributes) + len(contents))
    create(world, status=409)
    assert not enabled[2]
    assert create(world)["state"] == "unknown"


def test_prepared_request_cannot_switch_repository_profile(world, enabled):
    source_repository(enabled)
    worker = create(world, start=False)
    profile = enabled[3].profiles["local"]
    enabled[3].profiles["local"] = replace(profile, repository_ref="refs/heads/main")
    world.call("POST", f"workers/{worker['id']}:start",
               {"expected_revision": worker["revision"]}, status=409)
    assert not enabled[2]
    assert not list(enabled[0]["workspaces"].iterdir())


def test_expired_checkout_deadline_holds_without_vendor_call(world, enabled, monkeypatch):
    from hivemind.worker_repository import RepositoryCheckout

    source_repository(enabled)
    original = RepositoryCheckout.resolve

    def expired(self):
        self.deadline = 0
        return original(self)

    monkeypatch.setattr(RepositoryCheckout, "resolve", expired)
    create(world, status=409)
    assert not enabled[2]
    assert create(world)["state"] == "unknown"


def test_checkout_rechecks_nexus_authority_after_materialization(world, enabled, monkeypatch):
    from hivemind.worker_repository import RepositoryCheckout

    source_repository(enabled)
    original = RepositoryCheckout.materialize

    def revoked(self, commit, identity):
        result = original(self, commit, identity)
        with world.app.state.store.transaction():
            world.app.state.store.db.execute("DELETE FROM principals WHERE role='admin'")
        return result

    monkeypatch.setattr(RepositoryCheckout, "materialize", revoked)
    create(world, status=409)
    assert not enabled[2]
    assert world.app.state.store.db.execute("SELECT 1 FROM principals WHERE role='agent'").fetchone() is None


@pytest.mark.parametrize("path", [
    "../escape", ".git/config", "a\\b", "CON.txt", "a:stream", "trailing.", "a/../b",
])
def test_cross_platform_unsafe_tree_paths_are_rejected(path):
    entry = f"100644 blob {'0' * 40} 1\t{path}\0".encode()
    with pytest.raises(Problem):
        _validate_tree(entry, 10, 100)


@pytest.mark.parametrize("paths", [
    ("App.py", "app.py"), ("Dir/a.py", "dir/b.py"), ("file", "FILE/child"),
])
def test_case_colliding_paths_are_rejected(paths):
    data = b"".join(f"100644 blob {'0' * 40} 1\t{path}\0".encode()
                    for path in paths)
    with pytest.raises(Problem):
        _validate_tree(data, 10, 100)


@pytest.mark.parametrize("changes", [
    {"workspace_mode": "git_clone", "allow_native_git": False},
    {"allow_native_git": 1}, {"repository_ref": "--upload-pack=anything"},
    {"checkout_timeout_seconds": True}, {"checkout_max_files": 0},
])
def test_checkout_profile_requires_explicit_safe_configuration(enabled, changes):
    with pytest.raises(ValueError):
        replace(enabled[3].profiles["local"], **changes)


def test_linked_worktree_requires_explicit_metadata_trust_and_preserves_index(world, enabled):
    source, commit, profile = source_repository(enabled)
    linked = source.parent / "linked"
    git(source, "worktree", "add", "--detach", str(linked), commit)
    (linked / "hivemind-worker-policy.json").write_bytes(enabled[1].read_bytes())
    (linked / "app.txt").write_text("linked user edits")
    metadata = git(linked, "rev-parse", "--absolute-git-dir")
    before_index = (Path(metadata) / "index").read_bytes()
    enabled[3].profiles["local"] = replace(profile, repository_root=str(linked),
                                          repository_metadata_roots=(str(source / ".git"),))
    worker = create(world)
    target = enabled[0]["workspaces"] / worker["id"]
    assert worker["workspace"]["source_kind"] == "linked_worktree"
    assert (target / "app.txt").read_bytes() == b"committed contents\n"
    assert (linked / "app.txt").read_text() == "linked user edits"
    assert (Path(metadata) / "index").read_bytes() == before_index
    assert git(target, "rev-parse", "HEAD") == commit


def test_unapproved_linked_metadata_refuses_before_launch(world, enabled):
    source, commit, profile = source_repository(enabled)
    linked = source.parent / "unapproved-linked"
    git(source, "worktree", "add", "--detach", str(linked), commit)
    (linked / "hivemind-worker-policy.json").write_bytes(enabled[1].read_bytes())
    enabled[3].profiles["local"] = replace(profile, repository_root=str(linked))
    create(world, status=409)
    assert not enabled[2]


def test_bare_source_has_exact_private_checkout_without_source_mutation(world, enabled):
    source, commit, profile = source_repository(enabled)
    bare = source.parent / "bare"
    git(source, "clone", "--bare", "--no-hardlinks", "--", str(source), str(bare))
    (bare / "hivemind-worker-policy.json").write_bytes(enabled[1].read_bytes())
    before_refs = git(bare, "show-ref")
    enabled[3].profiles["local"] = replace(profile, repository_root=str(bare))
    worker = create(world)
    target = enabled[0]["workspaces"] / worker["id"]
    assert worker["workspace"]["source_kind"] == "bare"
    assert git(target, "rev-parse", "HEAD") == commit
    assert git(bare, "show-ref") == before_refs
    assert git(target, "remote") == ""


def dependency_repository(parent, name, text=b"dependency contents\n"):
    root = parent / name
    root.mkdir()
    git(root, "init", "--template=")
    blob = git(root, "hash-object", "-w", "--stdin", data=text)
    tree = git(root, "mktree", data=f"100644 blob {blob}\tdependency.txt\n".encode())
    commit = git(root, "commit-tree", "--no-gpg-sign", tree, data=b"Dependency\n")
    git(root, "update-ref", "refs/heads/main", commit)
    git(root, "symbolic-ref", "HEAD", "refs/heads/main")
    git(root, "reset", "--hard", commit)
    return root, commit


def submodule_tree(source, dependency_commit, path="vendor", modules=None):
    data = f"160000 commit {dependency_commit}\t{path}\n".encode()
    if modules is not None:
        blob = git(source, "hash-object", "-w", "--stdin", data=modules)
        data = f"100644 blob {blob}\t.gitmodules\n".encode() + data
    tree = git(source, "mktree", data=data)
    commit = git(source, "commit-tree", "--no-gpg-sign", tree, data=b"Submodule pin\n")
    git(source, "update-ref", "refs/heads/main", commit)
    return commit


def test_local_submodule_ignores_urls_and_keeps_gitlink_pin(world, enabled):
    source, _, profile = source_repository(enabled)
    dependency, pin = dependency_repository(source.parent, "dep")
    modules = b'[submodule "vendor"]\n path = vendor\n url = https://invalid.example/never\n update = checkout\n'
    commit = submodule_tree(source, pin, modules=modules)
    enabled[3].profiles["local"] = replace(profile, submodule_sources=(("vendor", str(dependency)),))
    before_index = (dependency / ".git/index").read_bytes()
    (dependency / "dependency.txt").write_text("dirty dependency")
    worker = create(world)
    target = enabled[0]["workspaces"] / worker["id"]
    assert git(target, "rev-parse", "HEAD") == commit
    assert git(target / "vendor", "rev-parse", "HEAD") == pin
    assert (target / "vendor/dependency.txt").read_bytes() == b"dependency contents\n"
    assert (dependency / "dependency.txt").read_text() == "dirty dependency"
    assert (dependency / ".git/index").read_bytes() == before_index
    assert git(target / "vendor", "remote") == ""
    assert not (target / "vendor/.git/objects/info/alternates").exists()
    assert worker["workspace"]["submodules"] == [{"path": "vendor", "commit": pin}]


def test_nested_submodules_are_exact_and_use_full_operator_paths(world, enabled):
    source, _, profile = source_repository(enabled)
    nested, nested_pin = dependency_repository(source.parent, "nested")
    middle, _ = dependency_repository(source.parent, "middle")
    middle_pin = submodule_tree(middle, nested_pin, path="child")
    submodule_tree(source, middle_pin)
    enabled[3].profiles["local"] = replace(profile, submodule_sources=(
        ("vendor", str(middle)), ("vendor/child", str(nested))))
    worker = create(world)
    target = enabled[0]["workspaces"] / worker["id"]
    assert git(target / "vendor", "rev-parse", "HEAD") == middle_pin
    assert git(target / "vendor/child", "rev-parse", "HEAD") == nested_pin
    assert worker["workspace"]["file_count"] == 1
    assert len(worker["workspace"]["submodules"]) == 2


@pytest.mark.parametrize("failure", ["unmapped", "missing_commit", "depth", "aggregate_bytes", "objects"])
def test_dependency_failure_holds_before_vendor_launch(world, enabled, failure):
    source, _, profile = source_repository(enabled)
    dependency, pin = dependency_repository(source.parent, "bounded-dep")
    if failure == "depth":
        child, child_pin = dependency_repository(source.parent, "deeper-dep")
        pin = submodule_tree(dependency, child_pin, path="child")
        mappings = (("vendor", str(dependency)), ("vendor/child", str(child)))
        profile = replace(profile, checkout_max_submodule_depth=1)
    else:
        mappings = (("vendor", str(dependency)),)
    submodule_tree(source, pin)
    if failure == "unmapped":
        mappings = (("other", str(dependency)),)
    elif failure == "missing_commit":
        empty, _ = dependency_repository(source.parent, "wrong-dep", b"other\n")
        mappings = (("vendor", str(empty)),)
    elif failure == "aggregate_bytes":
        profile = replace(profile, checkout_max_bytes=1)
    elif failure == "objects":
        profile = replace(profile, checkout_max_object_bytes=1)
    enabled[3].profiles["local"] = replace(profile, submodule_sources=mappings)
    create(world, status=409)
    assert not enabled[2]
    assert create(world)["state"] == "unknown"


def lfs_tree(source, contents, declared_size=None):
    digest = hashlib.sha256(contents).hexdigest()
    size = len(contents) if declared_size is None else declared_size
    pointer = f"version https://git-lfs.github.com/spec/v1\noid sha256:{digest}\nsize {size}\n".encode()
    blob = git(source, "hash-object", "-w", "--stdin", data=pointer)
    config = git(source, "hash-object", "-w", "--stdin",
                 data=b"[lfs]\n url = https://invalid.example/never\n")
    tree = git(source, "mktree", data=(
        f"100644 blob {config}\t.lfsconfig\n100644 blob {blob}\tasset.bin\n").encode())
    commit = git(source, "commit-tree", "--no-gpg-sign", tree, data=b"LFS pointer\n")
    git(source, "update-ref", "refs/heads/main", commit)
    return digest, commit


@pytest.mark.parametrize("contents", [b"large asset\x00\n" * 200, b""])
def test_lfs_hydrates_only_verified_local_objects_and_preserves_pointer_commit(world, enabled, contents):
    source, _, profile = source_repository(enabled)
    digest, commit = lfs_tree(source, contents)
    cache = source.parent / "lfs-cache"
    path = cache / digest[:2] / digest[2:4] / digest
    path.parent.mkdir(parents=True)
    path.write_bytes(contents)
    enabled[3].profiles["local"] = replace(profile, materialize_lfs=True, lfs_object_roots=(str(cache),))
    worker = create(world)
    target = enabled[0]["workspaces"] / worker["id"]
    assert git(target, "rev-parse", "HEAD") == commit
    assert (target / "asset.bin").read_bytes() == contents
    assert git(target, "show", "HEAD:asset.bin").startswith("version https://git-lfs.github.com/spec/v1")
    assert path.read_bytes() == contents
    assert worker["workspace"]["lfs_objects"] == [{"path": "asset.bin", "oid": digest, "size": len(contents)}]
    assert worker["workspace"]["working_tree_bytes"] >= len(contents)


@pytest.mark.parametrize("failure", ["missing", "hash", "size", "expanded_limit"])
def test_lfs_failures_hold_before_vendor_launch(world, enabled, failure):
    source, _, profile = source_repository(enabled)
    contents = b"data" * 1000
    digest, _ = lfs_tree(source, contents)
    cache = source.parent / "bad-lfs-cache"
    cache.mkdir()
    path = cache / digest[:2] / digest[2:4] / digest
    path.parent.mkdir(parents=True)
    if failure != "missing":
        path.write_bytes(b"x" * len(contents) if failure == "hash" else contents[:-1] if failure == "size" else contents)
    enabled[3].profiles["local"] = replace(profile, materialize_lfs=True, lfs_object_roots=(str(cache),),
        checkout_max_bytes=500 if failure == "expanded_limit" else profile.checkout_max_bytes)
    create(world, status=409)
    assert not enabled[2]


def test_extended_profile_changes_fence_prepared_launch(world, enabled):
    _, _, profile = source_repository(enabled)
    worker = create(world, start=False)
    enabled[3].profiles["local"] = replace(profile, checkout_max_submodules=2)
    world.call("POST", f"workers/{worker['id']}:start", {"expected_revision": worker["revision"]}, status=409)
    assert not enabled[2]


def test_source_config_includes_and_uploadpack_helpers_are_never_loaded(world, enabled):
    source, _, _ = source_repository(enabled)
    marker = source.parent / "source-helper-ran"
    included = source.parent / "included.gitconfig"
    included.write_text('[uploadpack]\n packObjectsHook = cmd /c echo unsafe\n')
    git(source, "config", "include.path", str(included))
    git(source, "config", "uploadpack.packObjectsHook", f'cmd /c echo unsafe > "{marker}"')
    worker = create(world)
    assert worker["workspace"]["state"] == "ready"
    assert not marker.exists()


def test_gitmodules_executable_update_is_rejected_without_vendor_launch(world, enabled):
    source, _, profile = source_repository(enabled)
    dependency, pin = dependency_repository(source.parent, "unsafe-update-dep")
    submodule_tree(source, pin, modules=
        b'[submodule "vendor"]\n path = vendor\n url = https://invalid.example/never\n update = !exit 99\n')
    enabled[3].profiles["local"] = replace(profile, submodule_sources=(("vendor", str(dependency)),))
    create(world, status=409)
    assert not enabled[2]


def test_lfs_size_is_rechecked_after_cache_snapshot(world, enabled, monkeypatch):
    source, _, profile = source_repository(enabled)
    digest, _ = lfs_tree(source, b"", declared_size=32)
    cache = source.parent / "changed-lfs-cache"
    path = cache / digest[:2] / digest[2:4] / digest
    path.parent.mkdir(parents=True)
    path.write_bytes(b"x" * 32)
    enabled[3].profiles["local"] = replace(profile, materialize_lfs=True, lfs_object_roots=(str(cache),))
    original = RepositoryCheckout.materialize

    def shrink(self, commit, identity):
        path.write_bytes(b"")
        return original(self, commit, identity)

    monkeypatch.setattr(RepositoryCheckout, "materialize", shrink)
    create(world, status=409)
    assert not enabled[2]


def test_initialized_submodule_metadata_inside_trusted_parent_is_supported(world, enabled):
    source, _, profile = source_repository(enabled)
    dependency, pin = dependency_repository(source.parent, "initialized-dep")
    submodule_tree(source, pin, modules=(
        f'[submodule "vendor"]\n path = vendor\n url = {dependency.as_posix()}\n'.encode()))
    git(source, "reset", "--hard", "main")
    git(source, "-c", "protocol.file.allow=always", "submodule", "update", "--init", "vendor")
    initialized = source / "vendor"
    enabled[3].profiles["local"] = replace(profile, submodule_sources=(("vendor", str(initialized)),))
    worker = create(world)
    target = enabled[0]["workspaces"] / worker["id"] / "vendor"
    assert git(target, "rev-parse", "HEAD") == pin
    assert (target / ".git").is_dir()


def test_sha256_bare_source_format_is_preserved(world, enabled):
    original_source, _, profile = source_repository(enabled)
    bare = original_source.parent / "sha256-bare"
    bare.mkdir()
    git(bare, "init", "--bare", "--template=", "--object-format=sha256")
    (bare / "hivemind-worker-policy.json").write_bytes(enabled[1].read_bytes())
    blob = git(bare, "hash-object", "-w", "--stdin", data=b"sha256 contents\n")
    tree = git(bare, "mktree", data=f"100644 blob {blob}\tapp.txt\n".encode())
    commit = git(bare, "commit-tree", "--no-gpg-sign", tree, data=b"SHA256\n")
    git(bare, "update-ref", "refs/heads/main", commit)
    git(bare, "symbolic-ref", "HEAD", "refs/heads/main")
    enabled[3].profiles["local"] = replace(profile, repository_root=str(bare))
    worker = create(world)
    target = enabled[0]["workspaces"] / worker["id"]
    assert len(commit) == 64
    assert git(target, "rev-parse", "--show-object-format") == "sha256"
    assert git(target, "rev-parse", "HEAD") == commit


def test_shallow_source_private_view_preserves_boundary(world, enabled):
    source, commit, profile = source_repository(enabled)
    shallow = source.parent / "shallow-source"
    git(source, "clone", "--depth=1", "--no-hardlinks", source.as_uri(), str(shallow))
    (shallow / "hivemind-worker-policy.json").write_bytes(enabled[1].read_bytes())
    enabled[3].profiles["local"] = replace(profile, repository_root=str(shallow))
    worker = create(world)
    assert worker["workspace"]["source_commit"] == commit
    assert git(shallow, "rev-parse", "--is-shallow-repository") == "true"


@pytest.mark.parametrize("changes", [
    {"materialize_lfs": True}, {"materialize_lfs": 1},
    {"repository_metadata_roots": "not-a-root-list"},
    {"submodule_sources": (("vendor",),)},
    {"checkout_max_submodules": False}, {"checkout_max_submodule_depth": 0},
    {"checkout_max_object_bytes": 1073741825},
])
def test_dependency_profile_rejects_invalid_optins_and_bounds(enabled, changes):
    with pytest.raises(ValueError):
        replace(enabled[3].profiles["local"], **changes)


def test_dependency_profile_rejects_ambiguous_or_unsafe_paths(enabled):
    source = str(enabled[0]["repository"])
    for paths in (("vendor", "Vendor"), ("../escape",), (".git/objects",)):
        with pytest.raises(ValueError):
            replace(enabled[3].profiles["local"], submodule_sources=tuple((path, source) for path in paths))


def test_lfs_inside_submodule_uses_the_same_local_cache_and_aggregate_contract(world, enabled):
    source, _, profile = source_repository(enabled)
    dependency, _ = dependency_repository(source.parent, "lfs-dependency")
    contents = b"nested large asset\x00\n" * 100
    digest, pin = lfs_tree(dependency, contents)
    submodule_tree(source, pin)
    cache = source.parent / "nested-lfs-cache"
    object_path = cache / digest[:2] / digest[2:4] / digest
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(contents)
    enabled[3].profiles["local"] = replace(profile,
        submodule_sources=(("vendor", str(dependency)),),
        materialize_lfs=True, lfs_object_roots=(str(cache),))
    worker = create(world)
    target = enabled[0]["workspaces"] / worker["id"]
    assert (target / "vendor/asset.bin").read_bytes() == contents
    assert git(target / "vendor", "rev-parse", "HEAD") == pin
    assert worker["workspace"]["lfs_objects"] == [
        {"path": "vendor/asset.bin", "oid": digest, "size": len(contents)}]
