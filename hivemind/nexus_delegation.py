"""Operator-managed, additive grants with a sticky fail-closed restriction ledger.

Legacy coordinators remain unchanged until their first grant. Once restricted,
deletion, revocation, expiry or invisible grant records never restore broad power.
Grants do not grant the base coordinator credential or a Nexus lease.
"""
import json
import time
from typing import Literal

from fastapi import Depends, Header
from pydantic import Field, model_validator

from . import models as m
from .storage import Problem


class NexusGrantCreate(m.Model):
    project_id: m.ID
    nexus_agent_id: m.ID
    target_agent_ids: list[m.ID] = Field(min_length=1, max_length=64)
    actions: list[Literal["steer", "pause", "resume", "cancel", "assign_task", "monitor"]] = Field(
        min_length=1, max_length=6)
    ttl_seconds: int = Field(default=3600, ge=10, le=86400)

    @model_validator(mode="after")
    def unique(self):
        if (len(set(self.target_agent_ids)) != len(self.target_agent_ids)
                or len(set(self.actions)) != len(self.actions)):
            raise ValueError("Grant targets and actions must be unique")
        return self


MODELS = {"NexusGrantCreate": NexusGrantCreate}


class NexusDelegation:
    def __init__(self, service):
        self.service, self.store = service, service.store
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS nexus_restrictions("
            "project TEXT NOT NULL,agent TEXT NOT NULL,PRIMARY KEY(project,agent))")

    def operator(self, principal, project):
        self.service.project_access(principal, project)
        if principal["role"] not in ("admin", "human"):
            raise Problem("delegation_operator_required", 403)

    def restricted(self, project, agent):
        return self.store.db.execute(
            "SELECT 1 FROM nexus_restrictions WHERE project=? AND agent=?", (project, agent)).fetchone()

    def check(self, project, nexus_agent, action=None, target_agent=None):
        if not self.restricted(project, nexus_agent):
            return
        if action == "lease_release":
            return
        rows = self.store.db.execute(
            "SELECT body FROM resources WHERE kind='nexus_grant' AND project=? AND deleted=0 "
            "AND json_extract(body,'$.nexus_agent_id')=? LIMIT 1025",
            (project, nexus_agent)).fetchall()
        if len(rows) > 1024:
            raise Problem("nexus_delegation_denied", 403)
        grants = [json.loads(row["body"]) for row in rows]
        for grant in grants:
            if grant["state"] != "active" or grant["expires_at"] <= time.time():
                continue
            if action == "lease":
                return
            if action in grant["actions"] and target_agent in grant["target_agent_ids"]:
                return grant
        raise Problem("nexus_delegation_denied", 403)

    def check_control(self, control):
        project, agent = control["project_id"], control["issuer_agent_id"]
        if not self.restricted(project, agent):
            return
        pin = control.get("delegation")
        if not pin:
            raise Problem("nexus_delegation_denied", 403)
        try:
            grant = json.loads(self.store.raw(pin["grant_id"], "nexus_grant")["body"])
        except Problem:
            raise Problem("nexus_delegation_denied", 403) from None
        if (grant["project_id"] != project or grant["nexus_agent_id"] != agent
                or grant["revision"] != pin["revision"] or grant["state"] != "active"
                or grant["expires_at"] <= time.time()
                or control["action"] not in grant["actions"]
                or control["target_agent_id"] not in grant["target_agent_ids"]):
            raise Problem("nexus_delegation_denied", 403)

    def create(self, principal, request):
        self.operator(principal, request.project_id)
        for agent in [request.nexus_agent_id, *request.target_agent_ids]:
            self.service.reference(principal, request.project_id, agent, "agent")
        count = self.store.db.execute(
            "SELECT count(*) FROM resources WHERE kind='nexus_grant' AND project=? "
            "AND json_extract(body,'$.nexus_agent_id')=?",
            (request.project_id, request.nexus_agent_id)).fetchone()[0]
        if count >= 1024:
            raise Problem("nexus_grant_limit", 422)
        self.store.db.execute("INSERT OR IGNORE INTO nexus_restrictions VALUES(?,?)",
                              (request.project_id, request.nexus_agent_id))
        result = self.service.create(principal, "nexus_grant", {
            **request.model_dump(exclude={"ttl_seconds"}),
            "expires_at": time.time() + request.ttl_seconds, "state": "active"})
        self.service.retire_controls(principal, request.project_id)
        return result

    def revoke(self, principal, grant_id, request):
        grant = self.store.get(grant_id, principal, "nexus_grant")
        self.operator(principal, grant["project_id"])
        if grant["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        grant["state"] = "revoked"
        row = self.store.raw(grant_id)
        result = self.store.save("nexus_grant", row["project"], row["owner"], grant,
                                 expected=request.expected_revision)
        self.service.retire_controls(principal, grant["project_id"])
        return result


def install(app, service, principal, command, envelope):
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    grants = service.nexus_delegation
    service.nexus_delegation = app.state.nexus_delegation = grants

    @app.post("/v1/nexus-grants")
    async def create(body: NexusGrantCreate, p=Depends(principal),
                     idempotency_key: str | None = Header(None)):
        grants.operator(p, body.project_id)
        return command(p, "nexus-grant-create", idempotency_key, body,
                       lambda: grants.create(p, body))

    @app.get("/v1/projects/{project_id}/nexus-grants")
    async def listing(project_id: str, p=Depends(principal)):
        grants.operator(p, project_id)
        return envelope(service.store.list("nexus_grant", project_id, p, 1024))

    @app.post("/v1/nexus-grants/{grant_id}:revoke")
    async def revoke(grant_id: str, body: m.Revision, p=Depends(principal),
                     idempotency_key: str | None = Header(None)):
        item = service.store.get(grant_id, p, "nexus_grant")
        grants.operator(p, item["project_id"])
        return command(p, "nexus-grant-revoke:" + grant_id, idempotency_key, body,
                       lambda: grants.revoke(p, grant_id, body))
