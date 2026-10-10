"""Opt-in independent review routing. Operator checks, never producer-reported passes."""
from dataclasses import dataclass
import time

from fastapi import Depends, Header
from pydantic import Field

from . import models as m
from .storage import Problem, uid
from .verification import ReviewDecision, ReviewerAssignment, TrustedCheck


@dataclass(frozen=True)
class ReviewPolicy:
    enabled: bool = False
    reviewer_principal_ids: tuple[str, ...] = ()
    checks: tuple[str, ...] = ()
    operator_principal_id: str | None = None
    interval_seconds: float = 30


class ReviewTick(m.Model):
    project_id: m.ID
    limit: int = Field(default=4, ge=1, le=16)


class ReviewAutomation:
    def __init__(self, app, service, policy=None):
        self.app, self.service, self.store = app, service, service.store
        self.policy = policy or ReviewPolicy()
        if not 1 <= self.policy.interval_seconds <= 3600:
            raise ValueError("Invalid review interval")
        self._next_tick = 0

    def scheduled_tick(self):
        if not self.policy.enabled or not self.policy.operator_principal_id or time.monotonic() < self._next_tick:
            return
        self._next_tick = time.monotonic() + self.policy.interval_seconds
        with self.store.lock:
            row = self.store.db.execute("SELECT * FROM principals WHERE id=?",
                                        (self.policy.operator_principal_id,)).fetchone()
            if not row:
                return
            principal = dict(row)
            body = ReviewTick(project_id=principal["project"])
            return self.service.mutate(principal, "scheduled-review-tick", uid("review_tick"),
                                       body.model_dump(), lambda: self.tick(principal, body))

    def _reviewers(self, project, producer):
        result = []
        for identifier in self.policy.reviewer_principal_ids:
            row = self.store.db.execute("SELECT * FROM principals WHERE id=?", (identifier,)).fetchone()
            if row and row["role"] == "agent" and row["project"] == project and row["agent"] != producer:
                result.append(dict(row))
        return sorted(result, key=lambda item: item["agent"])

    def tick(self, principal, request):
        if principal["role"] not in ("admin", "human"):
            raise Problem("review_operator_required", 403)
        self.service.project_access(principal, request.project_id)
        if not self.policy.enabled:
            raise Problem("review_automation_disabled")
        domain = self.app.state.verification
        verifier = getattr(self.app.state, "trusted_verifier", None) or domain.verifier
        if verifier is None or not self.policy.checks:
            raise Problem("trusted_verifier_unavailable", 503)
        results = []
        for original in domain.list(principal, request.project_id, status="pending")["verifications"][:request.limit]:
            reviewers = self._reviewers(request.project_id, original["agent_id"])
            if not reviewers:
                results.append({"id": original["id"], "state": "waiting_independent_reviewer"})
                continue
            if original["evidence_unavailable"]:
                results.append({"id": original["id"], "state": "evidence_unavailable"})
                continue
            reviewer = next((item for item in reviewers if item["agent"] == original.get("reviewer_agent_id")),
                            reviewers[0])
            current = original
            if current.get("reviewer_agent_id") != reviewer["agent"]:
                current = domain.assign_reviewer(principal, current["id"], ReviewerAssignment(
                    expected_revision=current["revision"], reviewer_agent_id=reviewer["agent"]))
            names = {item["name"] for item in current.get("checks", [])}
            for name in self.policy.checks:
                if name not in names:
                    current = domain.record_check(principal, current["id"], TrustedCheck(
                        expected_revision=current["revision"], check_name=name), verifier=verifier)
            passing = (all(item["ok"] for item in current["checks"])
                       and not any(item["outcome"] == "fail" for item in current["test_evidence"]))
            decision = "verified" if passing else (
                "rejected" if current["corrections"]["cycles"] >= domain.max_cycles else "needs_corrections")
            current = domain.decide(reviewer, current["id"], ReviewDecision(
                expected_revision=current["revision"], decision=decision,
                reason="Configured independent review and trusted local checks",
                correction_items=[] if decision != "needs_corrections" else ["Resolve configured check failures"]))
            results.append({"id": current["id"], "state": current["status"],
                            "correction_ids": current["corrections"]["open_ids"]})
        return {"results": results}


def install(app, service, principal, command, envelope, *, policy=None):
    automation = ReviewAutomation(app, service, policy)
    app.state.review_automation = automation
    @app.post("/v0.1/review-automation:tick")
    async def review_tick(body: ReviewTick, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "review-automation-tick", idempotency_key, body, lambda: automation.tick(p, body))
    return automation
