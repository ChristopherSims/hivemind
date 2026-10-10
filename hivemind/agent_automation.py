"""Parent-owned registration for explicitly configured agent automation."""


class NexusReviewerBridge:
    def __init__(self, executor):
        self.executor = executor

    def review_merge(self, snapshot):
        from .nexus_git_automation import MergeReviewOutcome
        result = self.executor.review_merge(
            snapshot["id"], reviewer_principal_id=snapshot["reviewer_principal_id"],
            reviewer_agent_id=snapshot["reviewer_agent_id"])
        if any(getattr(result, key) != snapshot[key]
               for key in ("source_oid", "target_oid", "candidate_oid")):
            return MergeReviewOutcome("abstain", "Review binding changed")
        reviewer = self.executor.store.db.execute(
            "SELECT * FROM principals WHERE id=?", (snapshot["reviewer_principal_id"],)).fetchone()
        body = self.executor.store.get(snapshot["id"], dict(reviewer), "merge_request")
        review = next(item for item in body["reviews"]
                      if item["principal_id"] == snapshot["reviewer_principal_id"]
                      and all(item[key] == snapshot[key]
                              for key in ("source_oid", "target_oid", "candidate_oid")))
        return MergeReviewOutcome(review["decision"], review["reason"], review["principal_id"])


def install(app, service, principal, command, envelope, nexus_policy=None, nexus_runtime=None,
            isolation_policy=None):
    from .capabilities import replace_routes
    from .nexus_git_automation import NexusGitAutomation
    replace_routes(app, __name__)
    if nexus_runtime is None and app.state.git_reviewer_runtime.policy.enabled:
        nexus_runtime = NexusReviewerBridge(app.state.git_reviewer_runtime)
    app.state.nexus_git_automation = NexusGitAutomation(
        service, app.state.git_collaboration, nexus_policy, nexus_runtime)
    from .os_isolation import IsolationPolicy, IsolationRefused, plan
    isolation = plan(isolation_policy or IsolationPolicy())
    if isolation.policy.required and any(item.status == "external" for item in isolation.decisions):
        raise IsolationRefused("Required identity/ACL controls lack automatic attestation")
    app.state.os_isolation = isolation

    from fastapi import Depends

    @app.get("/v0.1/projects/{project_id}/git-automation")
    async def status(project_id: str, p=Depends(principal)):
        service.project_access(p, project_id)
        automation = app.state.nexus_git_automation
        policy = automation.policy
        return envelope({
            "enabled": policy.enabled is True and policy.project_id == project_id,
            "review_runtime_configured": automation.runtime is not None,
            "last_error": getattr(app.state, "nexus_git_automation_error", None),
            "isolation_mode": isolation.policy.mode,
            "isolation_controls": {item.name: item.status for item in isolation.decisions},
        })
