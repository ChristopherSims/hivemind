"""Operator privacy & audit surfaces: permission review, deletion previews,
retention capability reporting and re-authorized, principal/project-bound exports.

This module deliberately reuses the canonical primitives instead of inventing
parallel ones:

* Access policies are the same ``resource_access`` rows managed by
  ``team_access`` (``GET/PUT /v1/resources/{id}/access``). This module only
  *reads* them; administration of grants stays in the team-access surface.
* Deletion is the canonical ``Store.delete`` lifecycle (logical deletion,
  synchronous content-history purge, derived index invalidation via jobs).
  This module adds only a read-only *affected-scope preview*; the destructive
  call itself stays on ``DELETE /v1/memories/{id}`` / ``DELETE /v1/posts/{id}``.
* Audit history reads the canonical ``audit`` table and returns a fixed
  whitelist of metadata. No raw payloads, command bodies, tokens, filesystem
  paths or transcript content exist in that table, and none are added here.
* Exports are canonical ``privacy_export`` descriptor resources created through
  the idempotent command path with an explicit typed confirmation. The payload
  is never persisted: it is *regenerated from currently visible, currently
  authorized canonical records* on each download, which re-authenticates and
  re-authorizes the operator in-lock.

Honest boundaries (reported, not hidden): physical erasure beyond the canonical
content-history purge, cache purges, verified external-provider deletion and
background retention sweeping are NOT supported by this build and are reported
as disabled capabilities with explanations.
"""
import json

from fastapi import Depends, Header, Query, Request
from pydantic import Field

from .models import Model
from .storage import Problem, uid, now

# Kinds whose deletion has a canonical, owner/admin-authorized endpoint.
DELETABLE_ENDPOINTS = {
    "memory": "DELETE /v1/memories/{resource_id}",
    "board_post": "DELETE /v1/posts/{resource_id}",
}

RESOURCE_LIMIT = 200
AUDIT_LIMIT = 200
DEPENDENT_LIMIT = 64
EXPORT_RESOURCE_LIMIT = 200
EXPORT_AUDIT_LIMIT = 200

# Operations recorded in the canonical audit table that this surface reports.
# Anything else is still disclosed only as its short operation string; the table
# never carries payloads, so there is nothing further to redact.
AUDIT_OPERATION_MAX = 128


class ExportCreate(Model):
    """Explicit reviewed confirmation binds the export to this exact project."""
    confirmation: str = Field(min_length=1, max_length=256)


class PrivacyAudit:
    def __init__(self, service):
        self.service = service
        self.store = service.store

    # -- shared guards ------------------------------------------------------

    def operator(self, principal, project):
        """Admin anywhere, or a human operator of exactly this project."""
        self.store.team_access.operator(principal, project)

    # -- permission review ---------------------------------------------------

    def permissions_review(self, principal, project, limit=RESOURCE_LIMIT):
        """Current canonical ACL posture for the operator's own project.

        Only resources the calling operator can currently see are listed, and
        each entry carries its canonical ``resource_access`` policy (or the
        documented default) exactly as team_access enforces it.
        """
        self.operator(principal, project)
        store = self.store
        teams = [
            {"id": team["id"], "title": team.get("title"),
             "active": bool(team.get("active")),
             "member_agent_ids": list(team.get("member_agent_ids") or []),
             "revision": team.get("revision")}
            for team in store.list("team", project, principal)
        ]
        resources, truncated = [], False
        for kind in ("board", "board_post", "artifact", "memory", "task"):
            rows = store.db.execute(
                "SELECT * FROM resources WHERE kind=? AND project=? AND deleted=0 ORDER BY id",
                (kind, project)).fetchall()
            for row in rows:
                if len(resources) >= limit:
                    truncated = True
                    break
                row = dict(row)
                if not store.visible(row, principal):
                    continue
                body = json.loads(row["body"])
                policy_row = store.team_access._row("access_" + row["id"])
                if policy_row is None:
                    policy = {"revision": None, "public_read": True, "public_contribute": True,
                              "reader_agent_ids": [], "contributor_agent_ids": [],
                              "reader_team_ids": [], "contributor_team_ids": []}
                else:
                    rules = json.loads(policy_row["body"])
                    policy = {"revision": policy_row["revision"],
                              "public_read": bool(rules.get("public_read")),
                              "public_contribute": bool(rules.get("public_contribute")),
                              "reader_agent_ids": list(rules.get("reader_agent_ids") or []),
                              "contributor_agent_ids": list(rules.get("contributor_agent_ids") or []),
                              "reader_team_ids": list(rules.get("reader_team_ids") or []),
                              "contributor_team_ids": list(rules.get("contributor_team_ids") or [])}
                resources.append({
                    "resource_id": row["id"], "kind": kind, "project_id": project,
                    "title": body.get("title") or body.get("display_name") or None,
                    "revision": row["revision"],
                    "deletable_via": DELETABLE_ENDPOINTS.get(kind),
                    "policy": policy,
                })
            if truncated:
                break
        return {
            "project_id": project,
            "generated_at": now(),
            "teams": teams,
            "resources": resources,
            "truncated": truncated,
            "policy_kinds": ["board", "board_post", "artifact", "memory", "task"],
            "administration": "Policies are changed only via "
                              "PUT /v1/resources/{resource_id}/access (expected revisions apply).",
        }

    # -- audit history ---------------------------------------------------------

    def audit_history(self, principal, project, limit=AUDIT_LIMIT):
        """Whitelisted audit metadata for this project's resources.

        An event is reported only when its resource is (or was) a canonical
        resource of exactly this project. The whitelist is: event id, operation
        string, resource id/kind/deleted flag, principal *role* and timestamp.
        """
        self.operator(principal, project)
        store = self.store
        rows = store.db.execute(
            "SELECT * FROM audit ORDER BY rowid DESC LIMIT ?", (limit + 1,)).fetchall()
        events, truncated = [], len(rows) > limit
        for row in rows[:limit]:
            resource = store.db.execute(
                "SELECT project, kind, deleted FROM resources WHERE id=?",
                (row["resource"],)).fetchone()
            if resource is None or resource["project"] != project:
                continue  # Never leak another project's (or a non-resource's) activity.
            principal_row = store.db.execute(
                "SELECT role FROM principals WHERE id=?", (row["principal"],)).fetchone()
            events.append({
                "id": row["id"],
                "operation": (row["operation"] or "")[:AUDIT_OPERATION_MAX],
                "resource_id": row["resource"],
                "resource_kind": resource["kind"],
                "resource_deleted": bool(resource["deleted"]),
                "principal_role": principal_row["role"] if principal_row else "unknown",
                "time": row["time"],
            })
        return {"project_id": project, "generated_at": now(), "events": events,
                "truncated": truncated,
                "scope": "canonical_audit_metadata_only",
                "excludes": ["command payloads", "request bodies", "tokens", "session data",
                             "filesystem paths", "transcripts"]}

    # -- retention capabilities -------------------------------------------------

    def retention_capabilities(self, principal, project):
        """Honest retention/deletion capability statement for this build."""
        self.operator(principal, project)
        return {
            "project_id": project,
            "generated_at": now(),
            "capabilities": {
                "logical_deletion": {
                    "supported": True,
                    "mechanism": "canonical Store.delete via "
                                 "DELETE /v1/memories/{id} or /v1/posts/{id} "
                                 "with expected_revision",
                    "semantics": "Marks the resource deleted, purges its versioned content "
                                 "history synchronously, and invalidates dependent memories "
                                 "(status=disputed, invalidation_reason=source_deleted).",
                },
                "source_removal": {
                    "supported": True,
                    "mechanism": "same canonical delete; relations to and from the resource "
                                 "are removed and dependents are invalidated, not erased.",
                },
                "index_invalidation": {
                    "supported": True,
                    "mechanism": "derived embedding/graph cleanup is queued as canonical jobs",
                    "semantics": "pending, not instantaneous; lexical rows are removed "
                                 "synchronously with the delete.",
                },
                "physical_erasure": {
                    "supported": False,
                    "reason": "No physical compaction or byte-level erasure of the store file "
                              "exists in this build; deletion is logical plus content-history "
                              "purge. Backend prerequisite: a verified compaction/erasure job.",
                },
                "cache_purge": {
                    "supported": False,
                    "reason": "No operator-triggered derived-cache purge endpoint exists. "
                              "Backend prerequisite: a canonical cache-invalidation command.",
                },
                "external_provider_deletion": {
                    "supported": False,
                    "reason": "Deletion of data held by external memory/model providers cannot "
                              "be verified by this build. Backend prerequisite: provider-side "
                              "verified deletion receipts.",
                },
                "background_retention_sweep": {
                    "supported": False,
                    "reason": "No background retention or archive-sweeping job runs in this "
                              "build; expiry excludes memories from retrieval but erases nothing.",
                },
                "memory_expiry": {
                    "supported": True,
                    "mechanism": "memory expires_at / validity.valid_until set through the "
                                 "reviewed memory-version flow",
                    "semantics": "expiry excludes a memory from retrieval immediately; "
                                 "it does not erase stored history.",
                },
            },
        }

    # -- deletion preview ---------------------------------------------------------

    def deletion_preview(self, principal, resource_id):
        """Read-only affected-scope review mirroring ``Store.delete`` semantics.

        Enforces the same preconditions as the canonical delete (visibility,
        contribute access, owner-or-admin) so a preview never discloses scope an
        actor could not affect, and it never performs the deletion.
        """
        store = self.store
        row = store.raw(resource_id)  # 404 for unknown/deleted
        if not store.visible(row, principal):
            raise Problem("not_found", 404)
        store.team_access.require_contribute(row, principal)
        if principal["role"] != "admin" and row["owner"] != principal["id"]:
            raise Problem("forbidden", 403)
        body = json.loads(row["body"])
        endpoint = DELETABLE_ENDPOINTS.get(row["kind"])
        dependents, truncated = [], False
        if endpoint is not None:
            pending, seen = [resource_id], {resource_id}
            while pending:
                source = pending.pop()
                rows = store.db.execute(
                    """SELECT r.* FROM resources r JOIN relations e ON e.source=r.id
                    WHERE r.kind='memory' AND r.deleted=0 AND e.kind='SUPPORTED_BY' AND e.target=?""",
                    (source,)).fetchall()
                for dependent in rows:
                    if dependent["id"] in seen:
                        continue
                    seen.add(dependent["id"])
                    if len(dependents) >= DEPENDENT_LIMIT:
                        truncated = True
                        continue
                    memory = json.loads(dependent["body"])
                    dependents.append({"id": dependent["id"], "kind": "memory",
                                       "title": memory.get("title"),
                                       "effect": "invalidated (status=disputed, "
                                                 "invalidation_reason=source_deleted)"})
                    pending.append(dependent["id"])
        return {
            "resource_id": resource_id,
            "project_id": row["project"],
            "kind": row["kind"],
            "revision": row["revision"],
            "title": body.get("title") or None,
            "deletable": endpoint is not None,
            "endpoint": endpoint.format(resource_id=resource_id) if endpoint else None,
            "unsupported_reason": None if endpoint else
                f"No canonical deletion endpoint exists for kind '{row['kind']}' in this build.",
            "dependent_memories": dependents,
            "dependent_count": len(dependents),
            "dependents_truncated": truncated,
            "consequences": [
                "logical deletion of the resource",
                "synchronous purge of its versioned content history",
                "derived index invalidation queued (pending, not instant)",
                "dependent memories invalidated as disputed, not erased",
            ] if endpoint else [],
            "not_performed": [
                "physical erasure of store bytes",
                "cache purge",
                "external provider deletion",
                "deletion of any other resource",
            ],
        }

    # -- exports -----------------------------------------------------------------

    def export_create(self, principal, project, request):
        self.operator(principal, project)
        expected = f"EXPORT {project}"
        if request.confirmation.strip() != expected:
            raise Problem("confirmation_required", 422)
        return self.service.create(principal, "privacy_export", {
            "project_id": project,
            "scope": "operator_privacy_audit",
            "state": "available",
            "created_by_role": principal["role"],
            "payload_storage": "none_regenerated_at_download",
            "confirmation": "reviewed",
        })

    def export_list(self, principal, project):
        self.operator(principal, project)
        rows = self.store.db.execute(
            "SELECT * FROM resources WHERE kind='privacy_export' AND project=? AND deleted=0 "
            "ORDER BY seq DESC LIMIT 100", (project,)).fetchall()
        exports = []
        for row in rows:
            body = json.loads(row["body"])
            exports.append({"id": row["id"], "project_id": project,
                            "created_at": body.get("created_at"),
                            "state": body.get("state"), "revision": row["revision"],
                            "scope": body.get("scope")})
        return {"project_id": project, "exports": exports}

    def export_download(self, principal, export_id):
        """Re-authorized download: the payload is regenerated from current state.

        Nothing is persisted at creation time, so a download can never serve a
        stale snapshot or records the caller has since lost access to.
        """
        row = self.store.raw(export_id, "privacy_export")  # 404 unknown/deleted
        project = row["project"]
        self.operator(principal, project)  # authorized again, now
        descriptor = json.loads(row["body"])
        permissions = self.permissions_review(principal, project, EXPORT_RESOURCE_LIMIT)
        audit = self.audit_history(principal, project, EXPORT_AUDIT_LIMIT)
        self.store.db.execute(
            "INSERT INTO audit VALUES(?,?,?,?,?)",
            (uid("aud"), principal["id"], "privacy_export.download", export_id, now()))
        return {
            "export_id": export_id,
            "project_id": project,
            "scope": descriptor.get("scope"),
            "descriptor_created_at": descriptor.get("created_at"),
            "generated_at": now(),
            "principal_bound": True,
            "regenerated": True,
            "permissions": permissions,
            "audit": audit,
            "truncated": bool(permissions["truncated"] or audit["truncated"]),
            "excludes": ["resource bodies and transcripts", "tokens and token hashes",
                         "session secrets", "credentials", "command payloads",
                         "filesystem paths", "hidden resources"],
            "external_sharing": "not_performed",
        }


def install(app, service, principal, command, envelope):
    from .capabilities import replace_routes
    replace_routes(app, __name__)
    store = service.store
    surface = PrivacyAudit(service)
    app.state.privacy_audit = surface

    @app.get("/v1/projects/{project_id}/privacy-audit/permissions")
    async def permissions(request: Request, project_id: str,
                          limit: int = Query(RESOURCE_LIMIT, ge=1, le=RESOURCE_LIMIT),
                          p=Depends(principal)):
        with store.lock:
            p = app.state.authenticate_request(request)
            return envelope(surface.permissions_review(p, project_id, limit))

    @app.get("/v1/projects/{project_id}/privacy-audit/audit")
    async def audit(request: Request, project_id: str,
                    limit: int = Query(AUDIT_LIMIT, ge=1, le=AUDIT_LIMIT),
                    p=Depends(principal)):
        with store.lock:
            p = app.state.authenticate_request(request)
            return envelope(surface.audit_history(p, project_id, limit))

    @app.get("/v1/projects/{project_id}/privacy-audit/retention")
    async def retention(request: Request, project_id: str, p=Depends(principal)):
        with store.lock:
            p = app.state.authenticate_request(request)
            return envelope(surface.retention_capabilities(p, project_id))

    @app.get("/v1/projects/{project_id}/privacy-audit/deletion-preview")
    async def preview(request: Request, project_id: str,
                      resource_id: str = Query(..., min_length=1, max_length=128),
                      p=Depends(principal)):
        with store.lock:
            p = app.state.authenticate_request(request)
            result = surface.deletion_preview(p, resource_id)
            if result["project_id"] != project_id:
                raise Problem("not_found", 404)
            return envelope(result)

    @app.post("/v1/projects/{project_id}/privacy-audit/exports")
    async def export_create_route(project_id: str, body: ExportCreate, p=Depends(principal),
                                  idempotency_key: str | None = Header(None)):
        return command(p, "privacy-export:" + project_id, idempotency_key, body,
                       lambda: surface.export_create(p, project_id, body))

    @app.get("/v1/projects/{project_id}/privacy-audit/exports")
    async def exports(request: Request, project_id: str, p=Depends(principal)):
        with store.lock:
            p = app.state.authenticate_request(request)
            return envelope(surface.export_list(p, project_id))

    @app.get("/v1/privacy-exports/{export_id}/download")
    async def export_download_route(request: Request, export_id: str, p=Depends(principal)):
        with store.lock, store.transaction():
            p = app.state.authenticate_request(request)
            return envelope(surface.export_download(p, export_id))
