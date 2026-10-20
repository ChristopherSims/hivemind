"""Bounded, independent local Git snapshots for newly owned worker directories."""
import os
import re
import shutil
import time
import unicodedata
from pathlib import Path

from .process_boundary import run_bounded
from .storage import Problem


def _regular_tree(root, maximum=10000, *, reject_promisor=False, deadline=None):
    """Reject links/reparse points before Git reads source metadata."""
    pending, count = [root], 0
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                if deadline is not None and time.monotonic() >= deadline:
                    raise Problem("worker_repository_timeout", 409)
                count += 1
                if count > maximum:
                    raise Problem("worker_repository_metadata_limit", 409)
                info = entry.stat(follow_symlinks=False)
                if entry.is_symlink() or getattr(info, "st_file_attributes", 0) & 0x400:
                    raise Problem("worker_repository_link", 409)
                if reject_promisor and entry.name.endswith(".promisor"):
                    raise Problem("worker_repository_source_unsupported", 409)
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif not entry.is_file(follow_symlinks=False):
                    raise Problem("worker_repository_link", 409)


def _validate_tree(data, max_files, max_bytes, *, allow_submodules=False):
    seen, prefixes, size = set(), {}, 0
    for record in data.split(b"\0"):
        if not record:
            continue
        header, name = record.split(b"\t", 1)
        mode, kind, _object, length = header.split()
        submodule = mode == b"160000" and kind == b"commit" and length == b"-"
        if not (mode in (b"100644", b"100755") and kind == b"blob" or allow_submodules and submodule):
            raise Problem("worker_repository_entry_unsupported", 409)
        path = name.decode("utf-8", errors="strict")
        parts = path.split("/")
        if (not path or len(name) > 4096 or len(parts) > 64
                or any(not p or p in (".", "..") or p.rstrip(" .") != p
                            or p.casefold() == ".git"
                            or p.split(".")[0].upper() in
                            {"CON", "PRN", "AUX", "NUL", *[f"COM{i}" for i in range(1, 10)],
                             *[f"LPT{i}" for i in range(1, 10)]}
                            for p in parts)
                or any(c in '\\:<>"|?*' or ord(c) < 32 or ord(c) == 127 for c in path)):
            raise Problem("worker_repository_path_unsupported", 409)
        key = unicodedata.normalize("NFC", path).casefold()
        if key in seen:
            raise Problem("worker_repository_path_collision", 409)
        for index in range(1, len(parts) + 1):
            prefix = "/".join(parts[:index])
            folded = unicodedata.normalize("NFC", prefix).casefold()
            entry = (prefix, index == len(parts))
            if folded in prefixes and prefixes[folded] != entry:
                raise Problem("worker_repository_path_collision", 409)
            prefixes[folded] = entry
        seen.add(key)
        size += 0 if submodule else int(length)
        if len(seen) > max_files or size > max_bytes:
            raise Problem("worker_repository_content_limit", 409)
    return {"file_count": len(seen), "tracked_bytes": size}


def validate_repository_options(profile):
    from .worker_provisioning import checked_directory
    for name in ("repository_metadata_roots", "lfs_object_roots"):
        values = getattr(profile, name)
        if not isinstance(values, (tuple, list)) or len(values) > 32:
            raise ValueError("Invalid trusted repository roots")
        values = tuple(str(checked_directory(value)) for value in values)
        if len(set(values)) != len(values) or any(Path(value).drive.startswith("\\\\") for value in values):
            raise ValueError("Repository roots must be unique and local")
        object.__setattr__(profile, name, values)
    if type(profile.materialize_lfs) is not bool or profile.materialize_lfs and not profile.lfs_object_roots:
        raise ValueError("LFS materialization requires trusted local object roots")
    for value, maximum in ((profile.checkout_max_submodules, 128), (profile.checkout_max_submodule_depth, 16),
                            (profile.checkout_max_object_bytes, 1073741824)):
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError("Invalid repository dependency bound")
    if not isinstance(profile.submodule_sources, (tuple, list)) or len(profile.submodule_sources) > 128:
        raise ValueError("Invalid trusted submodule sources")
    sources, names = [], set()
    for entry in profile.submodule_sources:
        if not isinstance(entry, (tuple, list)) or len(entry) != 2 or not isinstance(entry[0], str):
            raise ValueError("Submodules require path/source pairs")
        path, source = entry
        try:
            _validate_tree(f"100644 blob {'0'*40} 0\t{path}\0".encode(), 1, 1)
        except (Problem, ValueError, UnicodeError):
            raise ValueError("Invalid trusted submodule path") from None
        folded = unicodedata.normalize("NFC", path).casefold()
        if folded in names:
            raise ValueError("Duplicate trusted submodule path")
        names.add(folded)
        source = checked_directory(source)
        if source.drive.startswith("\\\\"):
            raise ValueError("Submodule sources must be local")
        sources.append((path, str(source)))
    object.__setattr__(profile, "submodule_sources", tuple(sources))


def _plain_file(path, maximum):
    for parent in (path.parent, *path.parent.parents):
        info = parent.lstat()
        if parent.is_symlink() or getattr(info, "st_file_attributes", 0) & 0x400:
            raise Problem("worker_repository_link", 409)
    info = path.lstat()
    if (path.is_symlink() or getattr(info, "st_file_attributes", 0) & 0x400 or not path.is_file()
            or info.st_size > maximum):
        raise Problem("worker_repository_file_untrusted", 409)
    return info


class RepositoryCheckout:
    def __init__(self, profile, workspace, private, seconds):
        self.profile = profile
        self.workspace, self.private = Path(workspace), Path(private)
        self.deadline = time.monotonic() + min(seconds, profile.checkout_timeout_seconds)
        self.executable = shutil.which("git")
        if not self.executable:
            raise Problem("git_not_installed", 503)
        executable = Path(self.executable).resolve(strict=True)
        untrusted_roots = (profile.repository_root, profile.workspace_root, profile.private_root,
                           *profile.repository_metadata_roots, *profile.lfs_object_roots,
                           *(source for _, source in profile.submodule_sources))
        if any(executable.is_relative_to(Path(root)) for root in untrusted_roots):
            raise Problem("worker_repository_executable_untrusted", 409)
        self.executable = str(executable)
        # No inherited Git injection, credential helpers, global filters or templates.
        self.env = {key: value for key, value in os.environ.items() if key.upper() in
                    {"PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "HOME", "USERPROFILE"}}
        self.env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_TERMINAL_PROMPT="0", GIT_ALLOW_PROTOCOL="file",
                        GIT_OPTIONAL_LOCKS="0", GIT_LFS_SKIP_SMUDGE="1", GIT_NO_LAZY_FETCH="1",
                        GIT_NO_REPLACE_OBJECTS="1")
        self.template = self.private / "git-empty-template"
        self.template.mkdir()
        self.nodes = []
        self.object_bytes = 0
        self.tree_bytes = 0
        self.submodule_count = 0
        self.lfs_objects = []
        self.working_tree_bytes = 0
        self.sources = dict(profile.submodule_sources)
        self.metadata_roots = [Path(root) for root in profile.repository_metadata_roots]
        for source in (profile.repository_root, *self.sources.values()):
            candidate = Path(source) / ".git"
            if candidate.is_dir() and not candidate.is_symlink():
                self.metadata_roots.append(candidate)

    def _trusted_metadata(self, directory):
        return any(directory.is_relative_to(root) for root in self.metadata_roots)

    def run(self, directory, *args, allowed_codes=(0,)):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise Problem("worker_repository_timeout", 409)
        try:
            code, output, _ = run_bounded(
                [self.executable, "-c", f"core.hooksPath={self.template}",
                 "-c", "core.autocrlf=false", "-c", "core.protectNTFS=true",
                 "-c", "core.longpaths=true",
                 "-c", "core.fsmonitor=false", "-c", "maintenance.auto=false",
                 "-c", "gc.auto=0", "-c", "uploadpack.allowAnySHA1InWant=true",
                 "-c", "fetch.fsckObjects=true", "-c", "transfer.fsckObjects=true",
                 "--no-optional-locks", "--literal-pathspecs", *args],
                directory, self.env, remaining, max_output=2 * 1024 * 1024)
        except TimeoutError:
            raise Problem("worker_repository_timeout", 409) from None
        except (OSError, ValueError, RuntimeError):
            raise Problem("worker_repository_command_unconfirmed", 409) from None
        if code not in allowed_codes:
            raise Problem("worker_repository_command_failed", 409)
        return output

    def _metadata(self, source):
        from .worker_provisioning import checked_directory
        source = checked_directory(str(source))
        metadata = source / ".git"
        kind = "worktree"
        if metadata.is_file():
            _plain_file(metadata, 4096)
            directive = metadata.read_text(encoding="utf-8").strip()
            if not directive.startswith("gitdir: ") or "\n" in directive or "\0" in directive:
                raise Problem("worker_repository_source_unsupported", 409)
            target = Path(directive[8:])
            metadata = Path(os.path.abspath(target if target.is_absolute() else source / target))
            metadata = checked_directory(str(metadata))
            kind = "linked_worktree"
            if not self._trusted_metadata(metadata):
                raise Problem("worker_repository_metadata_untrusted", 409)
        elif not metadata.exists() and (source / "HEAD").is_file() and (source / "objects").is_dir():
            metadata, kind = source, "bare"
        elif not metadata.is_dir():
            raise Problem("worker_repository_source_unsupported", 409)
        metadata = checked_directory(str(metadata))
        common = metadata
        if (metadata / "commondir").exists():
            _plain_file(metadata / "commondir", 4096)
            value = (metadata / "commondir").read_text(encoding="utf-8").strip()
            if not value or "\n" in value or "\0" in value:
                raise Problem("worker_repository_source_unsupported", 409)
            common = checked_directory(os.path.abspath(metadata / value))
            if not self._trusted_metadata(common):
                raise Problem("worker_repository_metadata_untrusted", 409)
        _regular_tree(common, reject_promisor=True, deadline=self.deadline)
        if metadata != common:
            _regular_tree(metadata, reject_promisor=True, deadline=self.deadline)
        if any((common / path).exists() for path in ("objects/info/alternates", "info/grafts")):
            raise Problem("worker_repository_source_unsupported", 409)
        return metadata, common, kind

    def _copy(self, source, destination, maximum, *, metadata=True):
        if metadata:
            maximum = min(maximum, self.profile.checkout_max_object_bytes - self.object_bytes)
        before = _plain_file(source, maximum)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as incoming, destination.open("xb") as outgoing:
            copied = 0
            while block := incoming.read(65536):
                if time.monotonic() >= self.deadline:
                    raise Problem("worker_repository_timeout", 409)
                copied += len(block)
                if copied > maximum:
                    raise Problem("worker_repository_content_limit", 409)
                outgoing.write(block)
        after = _plain_file(source, maximum)
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) or copied != before.st_size):
            raise Problem("worker_repository_source_changed", 409)
        if metadata:
            self.object_bytes += copied
        return copied

    def _view(self, source, ref, path, depth):
        if depth > self.profile.checkout_max_submodule_depth:
            raise Problem("worker_repository_submodule_depth", 409)
        metadata, common, kind = self._metadata(source)
        _plain_file(common / "config", 1048576)
        object_format = self.run(self.private, "config", "--no-includes", "--file", str(common / "config"),
                                 "--get", "extensions.objectFormat", allowed_codes=(0, 1)).decode("ascii").strip() or "sha1"
        if object_format not in ("sha1", "sha256"):
            raise Problem("worker_repository_source_unsupported", 409)
        view = self.private / ("git-source-" + str(len(self.nodes)))
        self.run(self.private, "init", "--bare", f"--template={self.template}",
                 f"--object-format={object_format}", str(view))
        # Repository commands never load source configuration or source upload-pack.
        # A private plain-file copy contains only object data and ref metadata.
        for directory in ("objects", "refs"):
            root = common / directory
            for base, dirs, files in os.walk(root):
                for name in files:
                    original = Path(base) / name
                    relative = original.relative_to(common)
                    target = view / relative
                    if target.exists():
                        raise Problem("worker_repository_source_unsupported", 409)
                    self._copy(original, target, self.profile.checkout_max_object_bytes)
                for name in dirs:
                    (view / (Path(base) / name).relative_to(common)).mkdir(exist_ok=True)
        # HEAD from a linked checkout, refs/objects from its validated common dir.
        (view / "HEAD").unlink()
        self._copy(metadata / "HEAD", view / "HEAD", 4096)
        if (common / "packed-refs").exists():
            self._copy(common / "packed-refs", view / "packed-refs", 2097152)
        if (common / "shallow").exists():
            self._copy(common / "shallow", view / "shallow", 2097152)
        commit = self.run(view, "rev-parse", "--verify", "--end-of-options", ref + "^{commit}").decode("ascii").strip()
        if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit):
            raise Problem("worker_repository_commit_unproven", 409)
        tree = self.run(view, "ls-tree", "-r", "-l", "-z", "--full-tree", commit)
        self.tree_bytes += len(tree)
        if self.tree_bytes > 2 * 1024 * 1024:
            raise Problem("worker_repository_metadata_limit", 409)
        counts = _validate_tree(tree, self.profile.checkout_max_files, self.profile.checkout_max_bytes,
                                allow_submodules=bool(self.sources))
        node = {"path": path, "view": view, "commit": commit, "tree": tree, "kind": kind, "files": [], "lfs": []}
        self.nodes.append(node)
        for record in tree.split(b"\0"):
            if not record:
                continue
            header, raw_name = record.split(b"\t", 1)
            mode, entry_kind, oid, size = header.split()
            name = raw_name.decode("utf-8")
            full_name = path + "/" + name if path else name
            if mode == b"160000":
                source = self.sources.get(full_name)
                if source is None:
                    raise Problem("worker_repository_submodule_unmapped", 409)
                self.submodule_count += 1
                if self.submodule_count > self.profile.checkout_max_submodules:
                    raise Problem("worker_repository_submodule_limit", 409)
                self._view(Path(source), oid.decode("ascii"), full_name, depth + 1)
            else:
                node["files"].append(name)
                if self.profile.materialize_lfs and int(size) <= 1024:
                    blob = self.run(view, "cat-file", "blob", oid.decode("ascii"))
                    if blob.startswith(b"version https://git-lfs.github.com/spec/"):
                        match = re.fullmatch(
                            rb"version https://git-lfs.github.com/spec/v1\noid sha256:([0-9a-f]{64})\nsize (0|[1-9][0-9]*)\n", blob)
                        if not match:
                            raise Problem("worker_repository_lfs_pointer_unsupported", 409)
                        digest, length = match[1].decode("ascii"), int(match[2])
                        if length > self.profile.checkout_max_bytes:
                            raise Problem("worker_repository_content_limit", 409)
                        cached = None
                        for root in self.profile.lfs_object_roots:
                            candidate = Path(root) / digest[:2] / digest[2:4] / digest
                            if candidate.exists():
                                cached = candidate
                                break
                        if cached is None:
                            raise Problem("worker_repository_lfs_object_missing", 409)
                        _plain_file(cached, length)
                        if cached.stat().st_size != length:
                            raise Problem("worker_repository_lfs_object_invalid", 409)
                        item = {"path": full_name, "oid": digest, "size": length, "cache": cached, "name": name}
                        node["lfs"].append(item)
                        self.lfs_objects.append(item)
        return commit, counts

    def resolve(self):
        version = self.run(self.private, "--version").decode("ascii")
        match = re.search(r"git version (\d+)\.(\d+)", version)
        if not match or tuple(map(int, match.groups())) < (2, 38):
            raise Problem("git_version_unsupported", 503)
        commit, _ = self._view(Path(self.profile.repository_root), self.profile.repository_ref, "", 0)
        files = sum(len(node["files"]) for node in self.nodes)
        full_paths = b"".join(
            f"100644 blob {'0'*40} 0\t{node['path'] + '/' if node['path'] else ''}{name}\0".encode()
            for node in self.nodes for name in node["files"])
        _validate_tree(full_paths, self.profile.checkout_max_files, self.profile.checkout_max_bytes)
        tracked = sum(int(record.split(b"\t", 1)[0].split()[3])
                      for node in self.nodes for record in node["tree"].split(b"\0")
                      if record and not record.startswith(b"160000 "))
        expanded = tracked + sum(item["size"] - len(self.run(node["view"], "show",
            node["commit"] + ":" + item["name"])) for node in self.nodes for item in node["lfs"])
        if files > self.profile.checkout_max_files or max(tracked, expanded) > self.profile.checkout_max_bytes:
            raise Problem("worker_repository_content_limit", 409)
        return commit, {"file_count": files, "tracked_bytes": tracked, "source_kind": self.nodes[0]["kind"],
                        "copied_object_bytes": self.object_bytes,
                        "submodules": [{"path": node["path"], "commit": node["commit"]} for node in self.nodes[1:]],
                        "lfs_objects": [{"path": item["path"], "oid": item["oid"], "size": item["size"]}
                                        for item in self.lfs_objects]}

    def materialize(self, commit, identity):
        if any(self.workspace.iterdir()):
            raise Problem("worker_workspace_not_empty", 409)
        if not self.nodes or self.nodes[0]["commit"] != commit:
            raise Problem("worker_repository_commit_unproven", 409)
        branch = "hivemind/worker/" + identity
        for node in self.nodes:
            target = self.workspace / node["path"] if node["path"] else self.workspace
            target.mkdir(parents=True, exist_ok=True)
            if any(target.iterdir()):
                raise Problem("worker_workspace_not_empty", 409)
            self._materialize_node(node, target, branch)
        _regular_tree(self.workspace, maximum=self.profile.checkout_max_files + 10000 * len(self.nodes),
                      deadline=self.deadline)
        return branch

    def _materialize_node(self, node, target, branch):
        commit = node["commit"]
        object_format = "sha256" if len(commit) == 64 else "sha1"
        self.run(target, "init", f"--template={self.template}",
                 f"--object-format={object_format}", ".")
        self.run(target, "config", "core.longpaths", "true")
        self.run(target, "fetch", "--depth=1", "--no-tags", "--no-write-fetch-head",
                 "--no-recurse-submodules", "--no-auto-maintenance", "--", str(node["view"]), commit)
        target_tree = self.run(target, "ls-tree", "-r", "-l", "-z", "--full-tree", commit)
        if target_tree != node["tree"]:
            raise Problem("worker_repository_commit_unproven", 409)
        self.run(target, "checkout", "--no-recurse-submodules", "-b", branch, commit)
        if self.run(target, "rev-parse", "HEAD").decode("ascii").strip() != commit:
            raise Problem("worker_repository_commit_unproven", 409)
        import hashlib
        for item in node["lfs"]:
            hydrated = self.private / ("lfs-" + str(len(self.nodes)) + "-" + item["oid"])
            if hydrated.exists():
                hydrated = self.private / (hydrated.name + "-" + str(len(list(self.private.glob("lfs-*")))))
            if self._copy(item["cache"], hydrated, item["size"], metadata=False) != item["size"]:
                raise Problem("worker_repository_lfs_object_invalid", 409)
            digest = hashlib.sha256()
            with hydrated.open("rb") as stream:
                while block := stream.read(65536):
                    if time.monotonic() >= self.deadline:
                        raise Problem("worker_repository_timeout", 409)
                    digest.update(block)
            if digest.hexdigest() != item["oid"]:
                raise Problem("worker_repository_lfs_object_invalid", 409)
            destination = target / item["name"]
            mode = destination.stat().st_mode
            os.replace(hydrated, destination)
            destination.chmod(mode)
        for name in node["files"]:
            self.working_tree_bytes += (target / name).stat().st_size
            if self.working_tree_bytes > self.profile.checkout_max_bytes:
                raise Problem("worker_repository_content_limit", 409)
        if (target / ".git/objects/info/alternates").exists():
            raise Problem("worker_repository_source_unsupported", 409)
