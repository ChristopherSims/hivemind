import sys
from pathlib import Path

from hivemind.api import hash_token
from hivemind.agent_automation import NexusReviewerBridge
from hivemind.git_reviewer_runtime import GitReviewerRuntime, ReviewerRuntimePolicy
from hivemind.nexus_git_automation import NexusGitAutomation, NexusGitPolicy
from test_git_collaboration import contribution, nexus


def test_nexus_uses_real_canonical_reviewer_without_duplicate_approval(world):
    *_, request = contribution(world)
    domain = world.app.state.git_collaboration
    reviewer = world.agent("Independent runtime reviewer")
    reviewer_token = world.credential(reviewer)
    store = world.app.state.store
    def principal(token):
        return dict(store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                                     (hash_token(token),)).fetchone())
    reviewer_principal = principal(reviewer_token)
    nexus_token, nexus_run, _ = nexus(world)
    nexus_principal = principal(nexus_token)
    operator = principal(world.owner)
    script = domain.root / "integration-check.py"
    script.write_text("from pathlib import Path\nassert Path('export/result.txt').is_file()\n")
    domain.checks = {"integration": [sys.executable, str(script)]}
    observed = []

    class Runtime:
        vendor = "droid"
        tool_names = ["Read", "Grep", "Glob", "LS"]

        def __init__(self, workspace):
            self.workspace = Path(workspace)

        def execute(self, task, context, checkpoint):
            assert not store.db.in_transaction
            assert store.raw(task["id"], "task")
            assert store.raw(context["run_id"], "run")
            assert task["execution"]["id"] == context["execution_id"]
            observed.append(context)
            return {"status": "completed", "structured": {
                "decision": "approve", "reason": "Exact candidate reviewed",
                **{key: request[key] for key in ("source_oid", "target_oid", "candidate_oid")}}}

    executor = GitReviewerRuntime(
        domain, runtime_factory=lambda workspace, context: Runtime(workspace),
        policy=ReviewerRuntimePolicy(enabled=True, allow_model_calls=True,
                                     reviewer_principal_id=reviewer_principal["id"]))
    automation = NexusGitAutomation(
        world.app.state.service, domain,
        NexusGitPolicy(enabled=True, project_id=world.project,
                       nexus_principal_id=nexus_principal["id"], nexus_run_id=nexus_run["id"],
                       operator_principal_id=operator["id"],
                       reviewer_principal_ids=(reviewer_principal["id"],), checks=("integration",)),
        NexusReviewerBridge(executor))
    result = automation.scheduled_tick()
    assert result["results"][0]["state"] == "merged"
    merged = world.call("GET", f"merge-requests/{request['id']}")
    assert len(merged["reviews"]) == 1
    assert len(observed) == 1


def test_reviewer_endpoint_cannot_impersonate_another_agent(world):
    _, producer_token, _, _, _, request = contribution(world)
    reviewer = world.agent("Private reviewer")
    token = world.credential(reviewer)
    principal = world.app.state.store.db.execute(
        "SELECT id FROM principals WHERE token_hash=?", (hash_token(token),)).fetchone()
    world.call("POST", "git-reviewer-runtime:review", {
        "merge_request_id": request["id"], "reviewer_principal_id": principal["id"],
        "reviewer_agent_id": reviewer["id"]}, producer_token, status=403)
