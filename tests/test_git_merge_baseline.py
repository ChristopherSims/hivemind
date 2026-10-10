from pathlib import Path

from hivemind.git_collaboration import COAUTHOR
from test_git_collaboration import contribution


def test_scoped_merge_can_import_unchanged_protected_baseline(world):
    _, token, run, task, workspace, _ = contribution(world)
    domain = world.app.state.git_collaboration
    repo = domain._repo(world.project)
    operator = domain.root / "operator-fixture"
    domain._git(domain.root, "clone", "--no-local", str(repo), str(operator))
    domain._git(operator, "config", "core.longpaths", "true")
    readme = operator / "README.md"
    readme.write_text("Trusted protected baseline\n")
    domain._git(operator, "add", "--", "README.md")
    domain._git(operator, "diff", "--cached")
    domain._git(operator, "status", "--short")
    domain._git(operator, "commit", "--file=-", data=("Update baseline\n\n" + COAUTHOR + "\n").encode())
    protected = domain._ref(operator, "HEAD")
    domain._git(repo, "fetch", str(operator), "HEAD:refs/hivemind/test-baseline")
    domain._git(repo, "update-ref", "refs/heads/main", protected)
    path = Path(workspace["workspace_path"])
    domain._git(path, "fetch", "origin", "main")
    domain._git(path, "merge", "--no-commit", "--no-ff", "origin/main")
    committed = world.call("POST", f"git-workspaces/{workspace['id']}:commit", {
        "expected_revision": workspace["revision"], "task_id": task["id"], "run_id": run["id"],
        "execution_id": task["execution"]["id"], "message": "Integrate protected baseline",
        "paths": ["export/result.txt"]}, token)
    world.call("POST", f"git-workspaces/{workspace['id']}:push", {
        "task_id": task["id"], "run_id": run["id"], "execution_id": task["execution"]["id"],
        "source_oid": committed["head_oid"]}, token)
    assert "Trusted protected baseline" in (path / "README.md").read_text()
    assert (path / "export/result.txt").read_text() == "first"
