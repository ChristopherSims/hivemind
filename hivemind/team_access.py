"""Canonical team membership and intersecting resource access policies."""
import json
from typing import Literal

from fastapi import Depends, Header, Query
from pydantic import Field, model_validator

from .models import ID, Model
from .storage import Problem, now, uid


class TeamCreate(Model):
    project_id: ID
    display_name: str = Field(min_length=1, max_length=128)
    member_agent_ids: list[ID] = Field(default_factory=list, max_length=128,
                                       json_schema_extra={"uniqueItems": True})

    @model_validator(mode="after")
    def unique_members(self):
        if len(set(self.member_agent_ids)) != len(self.member_agent_ids):
            raise ValueError("duplicate members")
        return self


class TeamUpdate(TeamCreate):
    expected_revision: int = Field(ge=1)
    active: bool = True


class AccessRules(Model):
    public_read: bool = False
    public_contribute: bool = False
    reader_agent_ids: list[ID] = Field(default_factory=list, max_length=128,
                                       json_schema_extra={"uniqueItems": True})
    contributor_agent_ids: list[ID] = Field(default_factory=list, max_length=128,
                                            json_schema_extra={"uniqueItems": True})
    reader_team_ids: list[ID] = Field(default_factory=list, max_length=128,
                                      json_schema_extra={"uniqueItems": True})
    contributor_team_ids: list[ID] = Field(default_factory=list, max_length=128,
                                           json_schema_extra={"uniqueItems": True})

    @model_validator(mode="after")
    def unique_grants(self):
        for name in ("reader_agent_ids", "contributor_agent_ids", "reader_team_ids", "contributor_team_ids"):
            values = getattr(self, name)
            if len(set(values)) != len(values):
                raise ValueError("duplicate grants")
        return self


class AccessUpdate(AccessRules):
    expected_revision: int | None = Field(default=None, ge=1)


class BoardCreate(Model):
    project_id: ID
    title: str = Field(min_length=1, max_length=256)
    access: AccessRules | None = None


PROTECTED_KINDS = frozenset(("board", "board_post", "artifact", "memory", "task"))
OPERATOR_KINDS = frozenset(("team", "resource_access", "memory_provider_result"))


class TeamAccess:
    def __init__(self, store):
        self.store = store

    def operator(self, principal, project):
        self.store.project(project)
        if principal["role"] not in ("admin", "human"):
            raise Problem("operator_required", 403)
        if principal["role"] != "admin" and principal["project"] != project:
            raise Problem("not_found", 404)

    def _row(self, identity):
        row = self.store.db.execute(
            "SELECT * FROM resources WHERE id=? AND deleted=0", (identity,)).fetchone()
        return dict(row) if row else None

    def _audit(self, principal, action, identity):
        self.store.db.execute("INSERT INTO audit VALUES(?,?,?,?,?)",
                              (uid("aud"), principal["id"], action, identity, now()))

    def _granted(self, rules, principal, action, project):
        if principal["role"] in ("admin", "human"):
            return True
        if rules.get("public_" + action) or action == "read" and rules.get("public_contribute"):
            return True
        prefixes = ("contributor",) if action == "contribute" else ("reader", "contributor")
        agent = principal.get("agent")
        agent_row = self._row(agent) if agent else None
        if not agent_row or agent_row["kind"] != "agent" or agent_row["project"] != project:
            return False
        for prefix in prefixes:
            if agent in rules.get(prefix + "_agent_ids", []):
                return True
            for identity in rules.get(prefix + "_team_ids", []):
                row = self._row(identity)
                if not row or row["kind"] != "team" or row["project"] != project:
                    continue
                team = json.loads(row["body"])
                if team.get("active") and agent in team["member_agent_ids"]:
                    return True
        return False

    def allowed(self, row, principal, action="read", visiting=None, budget=None):
        if row["kind"] in OPERATOR_KINDS:
            if row["kind"] == "memory_provider_result":
                check = self.store.memory_provider_result_filter
                return principal["role"] in ("admin", "human") and check is not None and check(row, principal)
            return principal["role"] in ("admin", "human")
        if principal["role"] == "admin":
            return True
        budget = [128] if budget is None else budget
        budget[0] -= 1
        if budget[0] < 0:
            return False
        visiting = set() if visiting is None else visiting
        if row["id"] in visiting or len(visiting) >= 32:
            return False
        visiting = visiting | {row["id"]}
        policy = self._row("access_" + row["id"])
        if policy:
            if policy["kind"] != "resource_access" or policy["project"] != row["project"]:
                return False
            rules = json.loads(policy["body"])
            if not self._granted(rules, principal, action, row["project"]):
                return False
        body = json.loads(row["body"])
        parents = []
        if row["kind"] == "board_post":
            parents.append((body.get("board_id"), "board"))
            if body.get("parent_post_id"):
                parents.append((body["parent_post_id"], "board_post"))
        if row["kind"] == "notification":
            for key, kind in (("post_id", "board_post"), ("memory_id", "memory")):
                if body.get(key):
                    parents.append((body[key], kind))
        if row["kind"] != "task" and body.get("task_id"):
            parents.append((body["task_id"], "task"))
        if row["kind"] != "memory" and body.get("memory_id"):
            parents.append((body["memory_id"], "memory"))
        if body.get("replacement_task_id"):
            parents.append((body["replacement_task_id"], "task"))
        if body.get("target_run_id"):
            parents.append((body["target_run_id"], "run"))
        if row["kind"] in ("artifact", "decision", "event") and body.get("run_id"):
            parents.append((body["run_id"], "run"))
        if row["kind"] == "memory" and body.get("scope_kind") in ("task", "run"):
            parents.append((body.get("scope_owner_id"), body["scope_kind"]))
        for identity, kind in parents:
            parent = self._row(identity)
            if (not parent or parent["kind"] != kind or parent["project"] != row["project"]
                    or not self._base_visible(parent, principal)
                    or not self.allowed(parent, principal, action, visiting, budget)):
                return False
        if row["kind"] == "run":
            sources = self.store.db.execute(
                "SELECT task FROM run_task_access WHERE run=?", (row["id"],)).fetchall()
            if len(sources) > 64:
                return False
            for source in sources:
                task = self._row(source["task"])
                if (not task or task["kind"] != "task" or task["project"] != row["project"]
                        or not self._base_visible(task, principal)
                        or not self.allowed(task, principal, "read", visiting, budget)):
                    return False
        if action == "read":
            for ref in body.get("evidence_refs", []):
                source = self._row(ref["id"])
                if (not source or source["project"] != row["project"] or source["kind"] != ref["kind"]
                        or not self._base_visible(source, principal)
                        or not self.allowed(source, principal, "read", visiting, budget)):
                    return False
        return True

    @staticmethod
    def _base_visible(row, principal):
        if principal["role"] != "admin" and row["project"] != principal["project"]:
            return False
        if row["private_agent"] and principal["role"] != "admin" and principal.get("agent") != row["private_agent"]:
            return False
        if row["kind"] == "message":
            body = json.loads(row["body"])
            return principal["role"] in ("admin", "observer") or principal.get("agent") in (body["from"], body["to"])
        return True

    def require_contribute(self, row, principal):
        if not self._base_visible(row, principal) or not self.allowed(row, principal, "contribute"):
            raise Problem("resource_contribution_forbidden", 403)

    def _validate(self, principal, project, rules):
        self.operator(principal, project)
        for name in ("reader_agent_ids", "contributor_agent_ids", "reader_team_ids", "contributor_team_ids"):
            kind = "team" if name.endswith("team_ids") else "agent"
            for identity in getattr(rules, name):
                row = self.store.raw(identity, kind)
                if row["project"] != project:
                    raise Problem("not_found", 404)

    def policy(self, principal, identity, request):
        row = self.store.raw(identity)
        self.operator(principal, row["project"])
        if row["kind"] not in PROTECTED_KINDS:
            raise Problem("unsupported_access_resource", 422)
        self._validate(principal, row["project"], request)
        old = self._row("access_" + identity)
        if (old is None and request.expected_revision is not None
                or old is not None and request.expected_revision != old["revision"]):
            raise Problem("revision_conflict", 412)
        body = request.model_dump(exclude={"expected_revision"})
        result = self.store.save("resource_access", row["project"], principal["id"], {
            **body, "id": "access_" + identity, "project_id": row["project"], "resource_id": identity,
        }, expected=request.expected_revision)
        self._audit(principal, "resource_access.update", identity)
        return result

    def team(self, service, principal, request, identity=None):
        self.operator(principal, request.project_id)
        for agent in request.member_agent_ids:
            row = self.store.raw(agent, "agent")
            if row["project"] != request.project_id:
                raise Problem("not_found", 404)
        body = request.model_dump(exclude={"expected_revision"})
        if identity is None:
            result = service.create(principal, "team", {**body, "active": True})
            self._audit(principal, "team.create", result["id"])
            return result
        row = self.store.raw(identity, "team")
        if row["project"] != request.project_id:
            raise Problem("not_found", 404)
        result = self.store.save("team", row["project"], row["owner"], {**body, "id": identity},
                                 expected=request.expected_revision)
        self._audit(principal, "team.update", identity)
        return result

    def board(self, service, principal, request):
        self.operator(principal, request.project_id)
        board = service.create(principal, "board", request.model_dump(exclude={"access"}))
        if request.access is not None:
            self.policy(principal, board["id"], AccessUpdate(**request.access.model_dump()))
        return board


def install(app, service, principal, command, envelope):
    store = service.store
    access = store.team_access
    app.state.team_access = access

    @app.post("/v1/teams")
    async def team_create(body: TeamCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "teams", idempotency_key, body, lambda: access.team(service, p, body))

    @app.get("/v1/projects/{project_id}/teams")
    async def teams(project_id: str, p=Depends(principal)):
        access.operator(p, project_id)
        return envelope(store.list("team", project_id, p))

    @app.get("/v1/projects/{project_id}/access-resources")
    async def access_resources(project_id: str, kind: Literal[
            "board", "board_post", "artifact", "memory", "task"] = "board",
            limit: int = Query(default=200, ge=1, le=200), p=Depends(principal)):
        access.operator(p, project_id)
        items = store.list(kind, project_id, p, limit + 1)
        return envelope({"resources": [
            {"id": item["id"], "project_id": project_id, "kind": kind,
             "title": item.get("title") or item.get("display_name") or item.get("name")
                      or f"{kind.replace('_', ' ').title()} {item['id'][-8:]}"}
            for item in items[:limit]], "has_more": len(items) > limit})

    @app.patch("/v1/teams/{team_id}")
    async def team_update(team_id: str, body: TeamUpdate, p=Depends(principal),
                          idempotency_key: str | None = Header(None)):
        return command(p, "team-update:" + team_id, idempotency_key, body,
                       lambda: access.team(service, p, body, team_id))

    @app.post("/v1/boards")
    async def board_create(body: BoardCreate, p=Depends(principal), idempotency_key: str | None = Header(None)):
        return command(p, "boards", idempotency_key, body, lambda: access.board(service, p, body))

    @app.get("/v1/resources/{resource_id}/access")
    async def access_get(resource_id: str, p=Depends(principal)):
        row = store.raw(resource_id)
        access.operator(p, row["project"])
        if row["kind"] not in PROTECTED_KINDS:
            raise Problem("unsupported_access_resource", 422)
        policy = access._row("access_" + resource_id)
        return envelope(json.loads(policy["body"]) if policy else {
            "resource_id": resource_id, "revision": None, "public_read": True, "public_contribute": True})

    @app.put("/v1/resources/{resource_id}/access")
    async def access_update(resource_id: str, body: AccessUpdate, p=Depends(principal),
                            idempotency_key: str | None = Header(None)):
        return command(p, "resource-access:" + resource_id, idempotency_key, body,
                       lambda: access.policy(p, resource_id, body))
