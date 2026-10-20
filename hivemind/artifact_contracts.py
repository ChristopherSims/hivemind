"""Task-bound typed deliverable contracts over canonical artifacts.

A task-bound *artifact contract* declares the named, typed outputs a producer task
must deliver before its dependents may be claimed or started. Contracts ride on the
existing canonical artifact core: producers submit EXACT evidence (artifact id +
pinned revision + sha256 of the canonical content), never a self-reported claim.
Satisfaction is computed deterministically by this module and is re-verified on
every gate evaluation against CURRENT canonical state.

* **Typed named outputs.** Every contract names 1..16 outputs. ``json`` outputs
  carry a bounded, data-only JSON-Schema subset (no ``$ref``/definitions, no
  regex ``pattern`` — ReDoS-proof, no remote fetch, no external vocabulary);
  ``text`` outputs require a ``text/plain``/``text/markdown`` media type and
  optional length bounds; ``file`` outputs require an explicit media type, a
  byte bound, and optional trusted-check names. Anything else is refused at
  contract write time.
* **Exact evidence, fail closed.** A submission pins each named output to one
  artifact resource at one revision with one checksum and binds the CURRENT
  execution (exact task/execution/run/agent/project) of the bound task. Unknown
  or missing names, cross-project or invisible artifacts, stale revisions,
  checksum mismatches, wrong-agent submissions, or historical executions refuse
  the submission. Data-level failures (schema violation, media/size mismatch,
  failed trusted check) are RECORDED as a ``rejected`` verdict with bounded
  diagnostics — a producer can never turn a bad deliverable into satisfaction by
  claiming so.
* **Currency re-checks on every use.** ``dependency_gate`` is a pure canonical
  read that re-reads the binding ledger, the contract (exact revision AND exact
  current output set — revising a contract invalidates old satisfied
  submissions), the latest submission, and every pinned artifact each time. The
  submission must bind the dependency task's CURRENT execution/run. A revoked
  contract, a deleted/hidden/superseded/tampered artifact, or a dependency cycle
  fails closed. Applicability is enumerated from a durable binding ledger: a
  hidden applicable contract HOLDS the gate (never pretends absent), and a
  physically deleted contract holds as ``contract_deleted_unreleased`` — only an
  explicit authoritative revocation policy (``ContractPolicy(
  revocation_releases=True)``) lets a revoked contract release the dependency.
* **Authorization.** Create/revise/revoke require an operator (admin/human).
  Submit requires the producer: ``service.run`` + ``service.represent`` prove the
  caller owns the run, whose agent must match the task's current execution.
  Views re-authorize the bound task and every pinned artifact source before
  emitting derived ids, diagnostics, or schema enum data.
* **Heavy compute off the canonical mutex.** Submit is a two-phase detached
  handler: phase 1 (under the store lock) authorizes and snapshots exact pins +
  content; phase 2 (off lock) hashes, parses, schema-validates, and runs trusted
  checks; phase 3 (``service.mutate``) re-validates pins by revision and exact
  content-string equality before recording. Trusted checks run ONLY through a
  verifier exposing ``run_bound(name, evidence)`` whose returned record echoes
  the exact evidence (contract revision, task, execution, output name, artifact
  digest); a generic unbound ``run()`` is advisory and never satisfies a check.
  Native checks are made exactly-once-or-unknown by a durable intent record
  (reserved/done) keyed by the exact request hash BEFORE any native run; a
  reserved intent found on replay is marked ``unknown`` and refused rather than
  repeated, and a ``done`` intent's receipts are replayed only after the current
  pins are re-authorized.
* **Default off.** ``install`` registers no routes and attaches no handle unless
  ``ContractPolicy(enabled=True)`` is supplied. Install replaces only its own
  routes. The scheduling layer calls :func:`dependency_gate` (or the
  ``service.task_dependency_gates`` adapter, ``{'action': 'allow'|'defer'}``)
  BEFORE claiming/starting a dependent task; a completed producer task may exist
  while its dependents stay blocked until every bound contract is satisfied.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Literal

from fastapi import Depends, Header
from pydantic import Field, field_validator, model_validator

from . import models as m
from .service import TERMINAL
from .storage import Problem, canonical, digest, now

__all__ = [
    "ContractPolicy", "ArtifactContracts", "dependency_gate", "gate_action", "install",
    "ContractOutput", "ContractCreate", "ContractRevise", "ContractRevoke",
    "SubmissionArtifact", "SubmissionCreate", "OPERATION_DESCRIPTORS", "MODELS",
    "describe_operations", "content_checksum",
    "MAX_CONTRACT_OUTPUTS", "MAX_SCHEMA_BYTES", "MAX_SCHEMA_DEPTH",
    "MAX_ARTIFACT_CONTENT_BYTES", "MAX_SUBMISSION_ARTIFACTS",
]

# ---- bounds ---------------------------------------------------------------
MAX_CONTRACT_OUTPUTS = 16
MAX_OUTPUT_NAME = 64
MAX_SCHEMA_BYTES = 16 * 1024
MAX_SCHEMA_DEPTH = 16
MAX_SCHEMA_PROPERTIES = 64
MAX_SCHEMA_ENUM = 128
MAX_SCHEMA_REQUIRED = 64
MAX_ARTIFACT_CONTENT_BYTES = 1048576
MAX_SUBMISSION_CONTENT_BYTES = 4 * 1048576  # aggregate pins per submission/gate
MAX_SUBMISSION_ARTIFACTS = 32
MAX_CONTRACTS_PER_TASK = 16
MAX_DIAGNOSTICS = 64
MAX_DEPENDENCY_DEPTH = 64
MAX_DEPENDENCY_EDGES = 1024
MAX_DATA_NODES = 4096
MAX_LIST = 200
MAX_OUTPUT_CHECKS = 4  # 16 outputs x 4 checks = 64 total, never truncated

OUTPUT_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
MEDIA_TYPE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,63}/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,127}$")
CHECK_NAME = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
TEXT_MEDIA = frozenset({"text/plain", "text/markdown"})
SCHEMA_TYPES = frozenset({"object", "array", "string", "number", "integer", "boolean", "null"})
# Data-only keyword subset. Anything else — most importantly "$ref"/definitions
# (external/self-referential resolution) and "pattern" (regex DoS) — is refused.
SCHEMA_KEYWORDS = frozenset({
    "type", "properties", "required", "additionalProperties", "items",
    "enum", "const", "minimum", "maximum", "minLength", "maxLength",
    "minItems", "maxItems", "description",
})
OPERATOR_ROLES = ("admin", "human")
MODELS = ("ContractOutput", "ContractCreate", "ContractRevise", "ContractRevoke",
          "SubmissionArtifact", "SubmissionCreate")


# ---- bounded safe JSON-schema subset ---------------------------------------
def _validate_schema_node(node, depth):
    if not isinstance(node, dict):
        raise ValueError("Schema nodes must be objects")
    if depth > MAX_SCHEMA_DEPTH:
        raise ValueError(f"Schema depth exceeds {MAX_SCHEMA_DEPTH}")
    for key in node:
        if key not in SCHEMA_KEYWORDS:
            raise ValueError(
                f"Schema keyword {key!r} is forbidden: only a bounded data-only subset "
                "is allowed (no $ref, no pattern, no combinators, no remote resolution)")
    node_type = node.get("type")
    if node_type is not None and (not isinstance(node_type, str)
                                  or node_type not in SCHEMA_TYPES):
        raise ValueError(f"Schema type {node_type!r} is not a supported data type")
    required = node.get("required")
    if required is not None:
        if (not isinstance(required, list) or len(required) > MAX_SCHEMA_REQUIRED
                or any(not isinstance(item, str) or not item for item in required)
                or len(set(required)) != len(required)):
            raise ValueError("Schema 'required' must be a short list of unique names")
    enumeration = node.get("enum")
    if enumeration is not None and (
            not isinstance(enumeration, list) or not enumeration
            or len(enumeration) > MAX_SCHEMA_ENUM):
        raise ValueError(f"Schema 'enum' must hold 1..{MAX_SCHEMA_ENUM} values")
    for bound in ("minimum", "maximum"):
        value = node.get(bound)
        if value is not None and (not isinstance(value, (int, float))
                                  or isinstance(value, bool) or not math.isfinite(value)):
            raise ValueError(f"Schema {bound!r} must be a finite number")
    for bound in ("minLength", "maxLength", "minItems", "maxItems"):
        value = node.get(bound)
        if value is not None and (not isinstance(value, int) or isinstance(value, bool)
                                  or not 0 <= value <= MAX_ARTIFACT_CONTENT_BYTES):
            raise ValueError(f"Schema {bound!r} must be a bounded non-negative integer")
    if (node.get("minimum") is not None and node.get("maximum") is not None
            and node["minimum"] > node["maximum"]):
        raise ValueError("Schema 'minimum' cannot exceed 'maximum'")
    properties = node.get("properties")
    if properties is not None:
        if not isinstance(properties, dict) or len(properties) > MAX_SCHEMA_PROPERTIES:
            raise ValueError(f"Schema 'properties' must be an object of at most "
                             f"{MAX_SCHEMA_PROPERTIES} entries")
        for key, child in properties.items():
            if not isinstance(key, str) or len(key) > 256:
                raise ValueError("Schema property names must be short strings")
            _validate_schema_node(child, depth + 1)
    additional = node.get("additionalProperties")
    if isinstance(additional, dict):
        _validate_schema_node(additional, depth + 1)
    elif additional is not None and additional is not False:
        raise ValueError("Schema 'additionalProperties' must be false or a schema")
    items = node.get("items")
    if items is not None:
        _validate_schema_node(items, depth + 1)


def validate_schema(schema):
    """Fail-fast validation of the bounded safe schema subset (raises ValueError)."""
    if len(canonical(schema).encode("utf-8")) > MAX_SCHEMA_BYTES:
        raise ValueError(f"Schema exceeds {MAX_SCHEMA_BYTES} bytes")
    _validate_schema_node(schema, 0)
    return schema


def _reject_constant(value):
    raise ValueError("Non-JSON constant")


def check_data(schema, data, path="$", errors=None, budget=None):
    """Deterministic local validation of JSON data against the safe subset.

    Appends bounded human diagnostics; returns the error list. No regex, no
    recursion beyond the (already bounded) schema, and a hard node budget on data.
    """
    errors = [] if errors is None else errors
    budget = [MAX_DATA_NODES] if budget is None else budget
    budget[0] -= 1
    if budget[0] < 0:
        if len(errors) < MAX_DIAGNOSTICS:
            errors.append(f"{path}: data exceeds the node budget")
        return errors
    if len(errors) >= MAX_DIAGNOSTICS:
        return errors
    expected = schema.get("type")
    if expected is not None:
        ok = {
            "object": lambda v: isinstance(v, dict),
            "array": lambda v: isinstance(v, list),
            "string": lambda v: isinstance(v, str),
            "number": lambda v: (isinstance(v, (int, float))
                                 and not isinstance(v, bool) and math.isfinite(v)),
            "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
            "boolean": lambda v: isinstance(v, bool),
            "null": lambda v: v is None,
        }[expected](data)
        if not ok:
            errors.append(f"{path}: expected {expected}")
            return errors
    if "const" in schema and canonical(data) != canonical(schema["const"]):
        errors.append(f"{path}: does not equal the required constant")
    if "enum" in schema and not any(canonical(data) == canonical(option)
                                    for option in schema["enum"]):
        errors.append(f"{path}: is not one of the allowed values")
    if isinstance(data, (int, float)) and not isinstance(data, bool):
        if schema.get("minimum") is not None and data < schema["minimum"]:
            errors.append(f"{path}: below minimum {schema['minimum']}")
        if schema.get("maximum") is not None and data > schema["maximum"]:
            errors.append(f"{path}: above maximum {schema['maximum']}")
    if isinstance(data, str):
        if schema.get("minLength") is not None and len(data) < schema["minLength"]:
            errors.append(f"{path}: shorter than {schema['minLength']}")
        if schema.get("maxLength") is not None and len(data) > schema["maxLength"]:
            errors.append(f"{path}: longer than {schema['maxLength']}")
    if isinstance(data, list):
        if schema.get("minItems") is not None and len(data) < schema["minItems"]:
            errors.append(f"{path}: fewer than {schema['minItems']} items")
        if schema.get("maxItems") is not None and len(data) > schema["maxItems"]:
            errors.append(f"{path}: more than {schema['maxItems']} items")
        items = schema.get("items")
        if isinstance(items, dict):
            for index, item in enumerate(data):
                check_data(items, item, f"{path}[{index}]", errors, budget)
    if isinstance(data, dict):
        for key in schema.get("required") or []:
            if key not in data:
                errors.append(f"{path}: missing required property {key!r}")
        properties = schema.get("properties") or {}
        for key, child in properties.items():
            if key in data:
                check_data(child, data[key], f"{path}.{key}", errors, budget)
        additional = schema.get("additionalProperties")
        unexpected = [key for key in data if key not in properties]
        if additional is False and unexpected:
            errors.append(f"{path}: unexpected properties {sorted(unexpected)[:8]!r}")
        elif isinstance(additional, dict):
            for key in unexpected:
                check_data(additional, data[key], f"{path}.{key}", errors, budget)
    return errors


def content_checksum(artifact_body):
    """Exact sha256 evidence over the artifact's canonical content object."""
    content = artifact_body.get("content")
    return hashlib.sha256(canonical(content).encode("utf-8")).hexdigest()


def parse_json_text(content):
    """Parse a json output's inline text, rejecting non-JSON constants; bounded by Content."""
    text = (content or {}).get("text")
    if not isinstance(text, str):
        return None, "json output requires inline text content"
    try:
        return json.loads(text, parse_constant=_reject_constant), None
    except (ValueError, TypeError):
        return None, "content is not valid JSON"


# ---- request models ---------------------------------------------------------
class ContractOutput(m.Model):
    model_config = m.ConfigDict(extra="forbid", allow_inf_nan=False, strict=True,
                                populate_by_name=True)

    name: str = Field(min_length=1, max_length=MAX_OUTPUT_NAME)
    type: Literal["json", "text", "file"]
    media_type: str | None = Field(default=None, max_length=256)
    # Wire name "schema"; the field avoids shadowing BaseModel.schema.
    json_schema: dict | None = Field(default=None, alias="schema")
    min_length: int | None = Field(default=None, ge=0, le=65536)
    max_length: int | None = Field(default=None, ge=0, le=65536)
    max_bytes: int | None = Field(default=None, ge=1, le=MAX_ARTIFACT_CONTENT_BYTES)
    checks: list[str] = Field(default_factory=list, max_length=MAX_OUTPUT_CHECKS)

    @field_validator("name", mode="after")
    @classmethod
    def _name(cls, value):
        if not OUTPUT_NAME.match(value):
            raise ValueError("Output names must be short identifier-like strings")
        return value

    @field_validator("media_type", mode="after")
    @classmethod
    def _media(cls, value):
        if value is not None and not MEDIA_TYPE.match(value):
            raise ValueError("Media types must be well-formed type/subtype tokens")
        return value

    @field_validator("checks", mode="after")
    @classmethod
    def _checks(cls, value):
        if len(set(value)) != len(value):
            raise ValueError("Check names must be unique per output")
        for name in value:
            if not CHECK_NAME.match(name):
                raise ValueError("Check names must be short identifiers")
        return value

    @model_validator(mode="after")
    def consistent(self):
        if self.type == "json":
            if self.json_schema is None:
                raise ValueError("A json output requires a schema")
            validate_schema(self.json_schema)
            if self.media_type is not None or self.checks or self.max_bytes is not None:
                raise ValueError("json outputs carry only a schema")
        elif self.type == "text":
            if self.media_type not in TEXT_MEDIA:
                raise ValueError("text outputs require a text/plain or text/markdown media type")
            if self.json_schema is not None or self.checks or self.max_bytes is not None:
                raise ValueError("text outputs carry only media metadata and length bounds")
            if (self.min_length is not None and self.max_length is not None
                    and self.min_length > self.max_length):
                raise ValueError("min_length cannot exceed max_length")
        else:  # file
            if self.media_type is None:
                raise ValueError("file outputs require an explicit media type")
            if (self.json_schema is not None or self.min_length is not None
                    or self.max_length is not None):
                raise ValueError("file outputs carry only media metadata, a byte bound, and checks")
        return self


class ContractCreate(m.Model):
    project_id: m.ID
    task_id: m.ID
    title: str = Field(min_length=1, max_length=512)
    outputs: list[ContractOutput] = Field(min_length=1, max_length=MAX_CONTRACT_OUTPUTS)

    @model_validator(mode="after")
    def unique_outputs(self):
        names = [output.name for output in self.outputs]
        if len(set(names)) != len(names):
            raise ValueError("Contract output names must be unique")
        return self


class ContractRevise(m.Model):
    expected_revision: int = Field(ge=1)
    title: str | None = Field(default=None, min_length=1, max_length=512)
    outputs: list[ContractOutput] | None = Field(
        default=None, min_length=1, max_length=MAX_CONTRACT_OUTPUTS)

    @model_validator(mode="after")
    def unique_outputs(self):
        if self.outputs:
            names = [output.name for output in self.outputs]
            if len(set(names)) != len(names):
                raise ValueError("Contract output names must be unique")
        if self.title is None and self.outputs is None:
            raise ValueError("A revision must change the title or the outputs")
        return self


class ContractRevoke(m.Model):
    expected_revision: int = Field(ge=1)


class SubmissionArtifact(m.Model):
    name: str = Field(min_length=1, max_length=MAX_OUTPUT_NAME)
    artifact_id: m.ID
    revision: int = Field(ge=1)
    sha256: str = Field(min_length=64, max_length=64)

    @field_validator("sha256", mode="after")
    @classmethod
    def _digest(cls, value):
        if not HEX64.match(value):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        return value


class SubmissionCreate(m.Model):
    contract_revision: int = Field(ge=1)
    task_id: m.ID
    run_id: m.ID
    execution_id: m.ID
    artifacts: list[SubmissionArtifact] = Field(
        min_length=1, max_length=MAX_SUBMISSION_ARTIFACTS)

    @model_validator(mode="after")
    def unique_names(self):
        names = [artifact.name for artifact in self.artifacts]
        if len(set(names)) != len(names):
            raise ValueError("Submission artifact names must be unique")
        return self


@dataclass(frozen=True)
class ContractPolicy:
    # Strictly typed: truthy strings/ints are not a policy.
    enabled: bool = False
    # When True, an explicit authoritative revocation releases the dependency gate.
    # Default False: a revoked contract HOLDS its dependents forever.
    revocation_releases: bool = False

    def __post_init__(self):
        for field_name in ("enabled", "revocation_releases"):
            if not isinstance(getattr(self, field_name), bool):
                raise ValueError(f"ContractPolicy.{field_name} must be a strict bool")


# ---- domain -----------------------------------------------------------------
class ArtifactContracts:
    """Task-bound contract lifecycle, exact-evidence submission, and gate evaluation."""

    def __init__(self, app, service, policy=None, verifier=None):
        self.app, self.service, self.store = app, service, service.store
        self.policy = policy or ContractPolicy()
        self.verifier = verifier

    # -- helpers -------------------------------------------------------------
    def _task(self, principal, task_id):
        return self.store.get(task_id, principal, "task")

    @staticmethod
    def _frozen(task):
        return bool(task.get("execution")) and task.get("status") not in TERMINAL

    def _require_operator(self, principal):
        if principal["role"] not in OPERATOR_ROLES:
            raise Problem("forbidden", 403)

    def _require_principal_current(self, principal):
        row = self.store.db.execute("SELECT * FROM principals WHERE id=?",
                                    (principal["id"],)).fetchone()
        if not row or any(row[key] != principal[key]
                          for key in ("role", "project", "agent", "token_hash")):
            raise Problem("authentication_required", 401)

    def _contract_row(self, principal, contract_id):
        body = self.store.get(contract_id, principal, "artifact_contract")
        # Re-authorize the bound task before emitting derived contract data.
        self._task(principal, body["task_id"])
        return body

    def _contracts_for(self, principal, project_id, task_id):
        rows = self.store.db.execute(
            "SELECT * FROM resources WHERE kind='artifact_contract' AND project=? AND deleted=0 "
            "AND json_extract(body,'$.task_id')=? ORDER BY seq DESC LIMIT ?",
            (project_id, task_id, MAX_CONTRACTS_PER_TASK + 1)).fetchall()
        return [json.loads(row["body"]) for row in rows if self.store.visible(row, principal)]

    def _bindings_for(self, project_id, task_id):
        """Raw applicability ledger rows; NEVER visibility-filtered (gate use)."""
        rows = self.store.db.execute(
            "SELECT * FROM resources WHERE kind='artifact_contract_binding' AND project=? "
            "AND deleted=0 AND json_extract(body,'$.task_id')=? ORDER BY seq DESC LIMIT ?",
            (project_id, task_id, MAX_CONTRACTS_PER_TASK + 1)).fetchall()
        return [json.loads(row["body"]) for row in rows]

    def _submissions_for(self, principal, contract_id, limit=MAX_LIST):
        rows = self.store.db.execute(
            "SELECT * FROM resources WHERE kind='artifact_submission' AND deleted=0 "
            "AND json_extract(body,'$.contract_id')=? ORDER BY seq DESC LIMIT ?",
            (contract_id, min(limit, MAX_LIST))).fetchall()
        return [json.loads(row["body"]) for row in rows if self.store.visible(row, principal)]

    def _submission_viewable(self, principal, submission):
        """Re-authorize every derived source before emitting submission data."""
        try:
            self._task(principal, submission["task_id"])
            for entry in submission.get("artifacts", []):
                row = self.store.raw(entry["artifact_id"], "artifact")
                if not self.store.visible(row, principal):
                    return False
        except Problem:
            return False
        return True

    # -- lifecycle -----------------------------------------------------------
    def create_contract(self, principal, request):
        self._require_operator(principal)
        self.service.project_access(principal, request.project_id)
        task = self._task(principal, request.task_id)
        if task["project_id"] != request.project_id:
            raise Problem("not_found", 404)
        if self._frozen(task):
            raise Problem("contract_frozen")
        if len(self._contracts_for(principal, request.project_id, request.task_id)
               ) >= MAX_CONTRACTS_PER_TASK:
            raise Problem("too_many_contracts", 422)
        outputs = [output.model_dump(exclude_none=True) for output in request.outputs]
        contract = self.service.create(principal, "artifact_contract", {
            "project_id": request.project_id, "task_id": request.task_id,
            "title": request.title, "outputs": outputs, "status": "active",
            "created_by": principal["id"]})
        # Durable applicability ledger: a later physical deletion of the contract
        # can never silently liberate the planned dependency.
        self.service.create(principal, "artifact_contract_binding", {
            "project_id": request.project_id, "task_id": request.task_id,
            "contract_id": contract["id"], "state": "bound",
            "created_by": principal["id"]})
        return contract

    def get_contract(self, principal, contract_id):
        return self._contract_row(principal, contract_id)

    def list_contracts(self, principal, project_id, task_id=None):
        self.service.project_access(principal, project_id)
        rows = self.store.db.execute(
            "SELECT * FROM resources WHERE kind='artifact_contract' AND project=? "
            "AND deleted=0 ORDER BY seq DESC LIMIT ?", (project_id, MAX_LIST)).fetchall()
        result = []
        for row in rows:
            if not self.store.visible(row, principal):
                continue
            body = json.loads(row["body"])
            if task_id and body["task_id"] != task_id:
                continue
            try:
                self._task(principal, body["task_id"])
            except Problem:
                continue  # never leak a contract whose task is not visible
            result.append(body)
        return result

    def revise_contract(self, principal, contract_id, request):
        self._require_operator(principal)
        contract = self._contract_row(principal, contract_id)
        if contract["status"] != "active":
            raise Problem("contract_revoked")
        if contract["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        task = self._task(principal, contract["task_id"])
        if self._frozen(task):
            raise Problem("contract_frozen")
        if request.outputs is not None:
            contract["outputs"] = [output.model_dump(exclude_none=True)
                                   for output in request.outputs]
        if request.title is not None:
            contract["title"] = request.title
        row = self.store.raw(contract_id, "artifact_contract")
        return self.store.save("artifact_contract", row["project"], row["owner"],
                               contract, private=row["private_agent"], expected=row["revision"])

    def revoke_contract(self, principal, contract_id, request):
        # Revocation is explicit and authoritative; whether it RELEASES the gate
        # is operator policy (ContractPolicy.revocation_releases), never silence.
        self._require_operator(principal)
        contract = self._contract_row(principal, contract_id)
        if contract["revision"] != request.expected_revision:
            raise Problem("revision_conflict", 412)
        contract["status"] = "revoked"
        row = self.store.raw(contract_id, "artifact_contract")
        return self.store.save("artifact_contract", row["project"], row["owner"],
                               contract, private=row["private_agent"], expected=row["revision"])

    # -- submission (two-phase; submit route is detached) --------------------
    def _prepare_locked(self, principal, contract_id, request):
        """Phase 1 (canonical lock): authority + exact pins + content snapshot."""
        self._require_principal_current(principal)
        contract = self.store.get(contract_id, principal, "artifact_contract")
        if contract["status"] != "active":
            raise Problem("contract_revoked")
        if contract["revision"] != request.contract_revision:
            raise Problem("contract_revision_conflict", 412)
        task = self._task(principal, contract["task_id"])
        self.store.team_access.require_contribute(self.store.raw(task["id"], "task"), principal)
        if request.task_id != contract["task_id"]:
            raise Problem("execution_binding_invalid", 422)
        # Producer authority: the caller owns the run; the run's agent owns the
        # task's CURRENT execution; everything stays inside the contract project.
        run = self.service.run(principal, request.run_id)
        execution = task.get("execution") or {}
        if (execution.get("id") != request.execution_id
                or execution.get("run_id") != request.run_id
                or execution.get("agent_id") != run["agent_id"]
                or run["project_id"] != contract["project_id"]):
            raise Problem("execution_binding_invalid", 422)
        names = {output["name"] for output in contract["outputs"]}
        submitted = {artifact.name for artifact in request.artifacts}
        if submitted - names:
            raise Problem("unknown_output", 422)
        if names - submitted:
            raise Problem("contract_output_missing", 422)
        snapshot = []
        for entry in request.artifacts:
            try:
                row = self.store.raw(entry.artifact_id, "artifact")
            except Problem:
                raise Problem("not_found", 404) from None
            if not self.store.visible(row, principal):
                raise Problem("not_found", 404)
            if row["project"] != contract["project_id"]:
                raise Problem("artifact_cross_project", 422)
            if row["revision"] != entry.revision:
                raise Problem("artifact_revision_conflict", 412)
            body = json.loads(row["body"])
            encoded = canonical(body.get("content"))
            if len(encoded.encode("utf-8")) > MAX_ARTIFACT_CONTENT_BYTES:
                raise Problem("artifact_content_too_large", 422)
            snapshot.append({"name": entry.name, "artifact_id": entry.artifact_id,
                             "revision": entry.revision, "sha256": entry.sha256,
                             "content": body.get("content"), "encoded": encoded})
        if sum(len(item["encoded"].encode("utf-8")) for item in snapshot
               ) > MAX_SUBMISSION_CONTENT_BYTES:
            raise Problem("submission_content_too_large", 422)
        check_plan = [(output["name"], check)
                      for output in contract["outputs"] for check in output.get("checks", [])]
        fingerprint = hashlib.sha256(canonical({
            "contract_id": contract_id, "contract_revision": contract["revision"],
            "execution_id": request.execution_id, "run_id": request.run_id,
            "artifacts": [{"name": item["name"], "artifact_id": item["artifact_id"],
                           "revision": item["revision"], "sha256": item["sha256"]}
                          for item in snapshot],
        }).encode("utf-8")).hexdigest()
        return {"contract": contract, "snapshot": snapshot, "check_plan": check_plan,
                "fingerprint": fingerprint, "execution": execution, "run": run}

    def _compute(self, prepared):
        """Phase 2a (off lock): hashing, JSON parsing, schema/media/size validation."""
        contract, errors = prepared["contract"], []
        by_name = {item["name"]: item for item in prepared["snapshot"]}
        for item in prepared["snapshot"]:
            actual = hashlib.sha256(item["encoded"].encode("utf-8")).hexdigest()
            if actual != item["sha256"]:
                raise Problem("artifact_checksum_mismatch", 422)
        for output in contract["outputs"]:
            if len(errors) >= MAX_DIAGNOSTICS:
                break
            item = by_name[output["name"]]
            content = item["content"] or {}
            label = f"output {output['name']!r}"
            if output["type"] == "json":
                data, error = parse_json_text(content)
                if error:
                    errors.append(f"{label}: {error}")
                else:
                    for entry in check_data(output["json_schema"], data):
                        errors.append(f"{label}: {entry}")
                        if len(errors) >= MAX_DIAGNOSTICS:
                            break
            else:
                if content.get("media_type") != output["media_type"]:
                    errors.append(f"{label}: media_type {content.get('media_type')!r} "
                                  f"does not match contract {output['media_type']!r}")
                text = content.get("text")
                size = len(text.encode("utf-8")) if isinstance(text, str) else None
                if size is None:
                    errors.append(f"{label}: requires inline text content")
                    continue
                if output["type"] == "text":
                    if output.get("min_length") is not None and len(text) < output["min_length"]:
                        errors.append(f"{label}: shorter than {output['min_length']}")
                    if output.get("max_length") is not None and len(text) > output["max_length"]:
                        errors.append(f"{label}: longer than {output['max_length']}")
                elif size > output.get("max_bytes", MAX_ARTIFACT_CONTENT_BYTES):
                    errors.append(f"{label}: exceeds {output.get('max_bytes')} bytes")
        return errors[:MAX_DIAGNOSTICS]

    def _run_checks(self, prepared):
        """Phase 2b (off lock): trusted checks bound to the exact evidence snapshot."""
        contract = prepared["contract"]
        verifier = self.verifier or getattr(self.app.state, "trusted_verifier", None)
        bound = getattr(verifier, "run_bound", None) if verifier is not None else None
        receipts = []
        for output_name, check_name in prepared["check_plan"]:
            evidence = {
                "project_id": contract["project_id"],
                "execution_id": prepared["execution"]["id"],
                "run_id": prepared["run"]["id"],
                "agent_id": prepared["run"]["agent_id"],
                "contract_id": contract["id"],
                "contract_revision": contract["revision"],
                "task_id": contract["task_id"],
                "output": output_name, "check": check_name,
                "artifacts": [{"name": item["name"], "artifact_id": item["artifact_id"],
                               "revision": item["revision"], "sha256": item["sha256"]}
                              for item in prepared["snapshot"]],
                "inputs": [{"name": item["name"], "encoded": item["encoded"]}
                           for item in prepared["snapshot"] if item["name"] == output_name],
            }
            if bound is None:
                # A generic unbound run() is advisory only; it can never satisfy
                # an artifact-bound contract check.
                receipts.append({"name": check_name, "output": output_name,
                                 "kind": "trusted_check", "ok": False,
                                 "detail": "trusted_verifier_unbound"})
                continue
            result = bound(check_name, evidence)
            record = result.as_record() if hasattr(result, "as_record") else dict(result)
            ok = record.get("ok") is True and record.get("evidence") == evidence
            detail = None
            if not ok:
                detail = ("check_evidence_mismatch" if record.get("ok") is True
                          else "check_failed")
            receipts.append({"name": check_name, "output": output_name,
                             "kind": "trusted_check", "ok": ok, "detail": detail,
                             "digest": hashlib.sha256(canonical(
                                 {"record": record, "evidence": evidence}
                             ).encode("utf-8")).hexdigest()})
        return receipts

    def _checks_with_intent(self, principal, contract_id, request, prepared,
                            idempotency_key):
        """Durable exactly-once-or-unknown intent around native verification.

        The intent (reserved -> done) is keyed by the exact request hash and
        recorded BEFORE any native run. A reserved/unknown intent on replay is
        never repeated: it is marked unknown and refused. A done intent replays
        its receipts only when the current pins still match its fingerprint.
        """
        if not prepared["check_plan"]:
            return []
        payload = {"contract_id": contract_id, **request.model_dump(exclude_none=True),
                   "idempotency_key": idempotency_key}
        check_key = hashlib.sha256(canonical(payload).encode("utf-8")).hexdigest()
        operation = f"artifact-contract-checks:{contract_id}"
        with self.store.transaction():
            row = self.store.db.execute(
                "SELECT * FROM commands WHERE principal=? AND operation=? AND key=?",
                (principal["id"], operation, check_key)).fetchone()
            if row:
                record = json.loads(row["response"])
                if record.get("state") == "done":
                    if record.get("fingerprint") != prepared["fingerprint"]:
                        raise Problem("contract_state_changed", 409)
                    return record["receipts"]
                unknown = dict(record, state="unknown")
            else:
                record = {"state": "reserved", "fingerprint": prepared["fingerprint"],
                          "at": now(),
                          "refs": [{"artifact_id": item["artifact_id"],
                                    "revision": item["revision"], "sha256": item["sha256"]}
                                   for item in prepared["snapshot"]]}
                self.store.db.execute(
                    "INSERT INTO commands VALUES(?,?,?,?,?)",
                    (principal["id"], operation, check_key, digest(payload), canonical(record)))
                unknown = None
        if unknown is not None:
            # Crash or concurrent duplicate: the native effect is uncertain, so it
            # is never repeated. The tombstone commits BEFORE the refusal (the
            # transaction above already committed it via this second write).
            with self.store.transaction():
                self.store.db.execute(
                    "UPDATE commands SET response=? WHERE principal=? AND operation=? AND key=?",
                    (canonical(unknown), principal["id"], operation, check_key))
            raise Problem("contract_check_unknown", 409)
        receipts = self._run_checks(prepared)
        with self.store.transaction():
            self.store.db.execute(
                "UPDATE commands SET response=? WHERE principal=? AND operation=? AND key=?",
                (canonical({"state": "done", "fingerprint": prepared["fingerprint"],
                            "receipts": receipts, "at": now()}),
                 principal["id"], operation, check_key))
        return receipts

    def submit(self, principal, contract_id, request, idempotency_key):
        with self.store.lock:
            prepared = self._prepare_locked(principal, contract_id, request)
        errors = self._compute(prepared)  # heavy parse/validate, off the lock
        receipts = self._checks_with_intent(principal, contract_id, request, prepared,
                                            idempotency_key)
        payload = {"contract_id": contract_id, **request.model_dump(exclude_none=True)}
        return self.service.mutate(
            principal, f"artifact-contract-submit:{contract_id}", idempotency_key, payload,
            lambda: self._finalize(principal, contract_id, request, prepared, errors,
                                   receipts))

    def _finalize(self, principal, contract_id, request, prepared, errors, receipts):
        # Re-validate against current canonical state; the world may have moved
        # while compute and native checks ran off-lock.
        self._require_principal_current(principal)
        contract = self.store.get(contract_id, principal, "artifact_contract")
        if contract["status"] != "active":
            raise Problem("contract_revoked")
        if contract["revision"] != prepared["contract"]["revision"]:
            raise Problem("contract_revision_conflict", 412)
        task = self._task(principal, contract["task_id"])
        execution = task.get("execution") or {}
        if (execution.get("id") != request.execution_id
                or execution.get("run_id") != request.run_id):
            raise Problem("execution_binding_invalid", 422)
        for item in prepared["snapshot"]:
            row = self.store.raw(item["artifact_id"], "artifact")
            if not self.store.visible(row, principal):
                raise Problem("not_found", 404)
            if row["revision"] != item["revision"]:
                raise Problem("artifact_revision_conflict", 412)
            # Exact re-validation: identical canonical content => phase-2 hashes hold.
            current = canonical(json.loads(row["body"]).get("content"))
            if current != item["encoded"]:
                raise Problem("contract_state_changed", 409)
        errors = list(errors)
        for receipt in receipts:
            if not receipt["ok"]:
                errors.append(f"check {receipt['name']!r} on output "
                              f"{receipt['output']!r}: {receipt['detail']}")
        errors = errors[:MAX_DIAGNOSTICS]
        for item in prepared["snapshot"]:
            receipts.append({
                "name": item["name"], "kind": "exact_evidence",
                "ok": not any(error.startswith(f"output {item['name']!r}:")
                              for error in errors),
                "project_id": contract["project_id"], "principal_id": principal["id"],
                "contract_revision": contract["revision"],
                "execution_id": request.execution_id, "digest": item["sha256"]})
        body = {
            "project_id": contract["project_id"], "contract_id": contract_id,
            "contract_revision": contract["revision"], "task_id": request.task_id,
            "run_id": request.run_id, "execution_id": request.execution_id,
            "artifacts": [{"name": item["name"], "artifact_id": item["artifact_id"],
                           "revision": item["revision"], "sha256": item["sha256"]}
                          for item in prepared["snapshot"]],
            "verdict": "satisfied" if not errors else "rejected",
            "diagnostics": errors, "receipts": receipts,
            "submitted_by": principal["id"],
            "self_report": False,  # verdicts are computed, never claimed
        }
        return self.service.create(principal, "artifact_submission", body)

    def list_submissions(self, principal, contract_id):
        self._contract_row(principal, contract_id)
        return [body for body in self._submissions_for(principal, contract_id)
                if self._submission_viewable(principal, body)]

    # -- scheduling gate -------------------------------------------------------
    def _dep_edges(self, project_id, task_id):
        rows = self.store.db.execute(
            "SELECT target FROM relations WHERE project=? AND source=? AND kind='DEPENDS_ON' "
            "LIMIT 64", (project_id, task_id)).fetchall()
        return [row["target"] for row in rows]

    def _dependency_cycle(self, project_id, task_id):
        """Iterative DFS: gray = on the active path, black = fully explored.

        A diamond (revisiting a black node) is NOT a cycle; only a back edge to a
        gray node is. Nodes and edges are hard-bounded; exhaustion fails closed.
        """
        color = {task_id: 1}  # 1 = gray, 2 = black
        stack = [(task_id, self._dep_edges(project_id, task_id))]
        edges_seen = 0
        while stack:
            node, edges = stack[-1]
            descended = False
            for target in edges:
                edges_seen += 1
                if edges_seen > MAX_DEPENDENCY_EDGES:
                    return True
                state = color.get(target, 0)
                if state == 1:
                    return True
                if state == 0:
                    if len(color) >= MAX_DEPENDENCY_DEPTH:
                        return True
                    color[target] = 1
                    stack.append((target, self._dep_edges(project_id, target)))
                    descended = True
                    break
            if not descended:
                color[node] = 2
                stack.pop()
        return False

    def dependency_gate(self, principal, task, dependency):
        """Pure canonical read: approval diagnostics for one (task, dependency) pair.

        Called by scheduling BEFORE a dependent is claimed/started. Applicability
        comes from the durable binding ledger and is NEVER visibility-filtered:
        a hidden applicable contract holds the gate without leaking its id, and a
        physically deleted contract holds as ``contract_deleted_unreleased``.
        """
        dependency_id = dependency["id"] if isinstance(dependency, dict) else dependency
        base = {"task_id": task["id"], "dependency_id": dependency_id, "diagnostics": []}
        if self._dependency_cycle(task["project_id"], task["id"]):
            return {**base, "approved": False, "reason": "dependency_cycle"}
        bindings = [binding for binding in self._bindings_for(task["project_id"], dependency_id)
                    if binding.get("state") == "bound"]
        if not bindings:
            return {**base, "approved": True, "reason": "no_contract"}
        try:
            dependency_task = self._task(principal, dependency_id)
        except Problem:
            return {**base, "approved": False, "reason": "dependency_unavailable"}
        if dependency_task["project_id"] != task["project_id"]:
            return {**base, "approved": False, "reason": "dependency_unavailable"}
        execution = dependency_task.get("execution") or {}
        satisfied = []
        for binding in bindings[:MAX_CONTRACTS_PER_TASK]:
            try:
                row = self.store.raw(binding["contract_id"], "artifact_contract")
            except Problem:
                return {**base, "approved": False, "reason": "contract_deleted_unreleased"}
            if not self.store.visible(row, principal):
                # Hold without leaking the hidden contract's identity.
                return {**base, "approved": False, "reason": "contract_not_visible"}
            contract = json.loads(row["body"])
            label = {"contract_id": contract["id"], "contract_revision": contract["revision"]}
            if contract["status"] != "active":
                if self.policy.revocation_releases:
                    continue  # explicit authoritative release policy
                return {**base, **label, "approved": False, "reason": "contract_revoked"}
            submissions = self._submissions_for(principal, contract["id"], limit=1)
            if not submissions:
                return {**base, **label, "approved": False, "reason": "submission_missing",
                        "missing_outputs": [output["name"] for output in contract["outputs"]]}
            submission = submissions[0]
            if (submission["contract_revision"] != contract["revision"]
                    or {entry["name"] for entry in submission.get("artifacts", [])}
                    != {output["name"] for output in contract["outputs"]}):
                # The contract moved (revision or exact output set): old
                # satisfaction never carries over to a revised contract.
                return {**base, **label, "approved": False, "reason": "contract_revised",
                        "submission_id": submission["id"]}
            if (submission["execution_id"] != execution.get("id")
                    or submission["run_id"] != execution.get("run_id")):
                return {**base, **label, "approved": False,
                        "reason": "execution_binding_stale",
                        "submission_id": submission["id"]}
            if submission["verdict"] != "satisfied":
                return {**base, **label, "approved": False, "reason": "contract_unsatisfied",
                        "diagnostics": submission.get("diagnostics", [])[:MAX_DIAGNOSTICS],
                        "submission_id": submission["id"]}
            for entry in submission.get("artifacts", []):
                try:
                    artifact_row = self.store.raw(entry["artifact_id"], "artifact")
                except Problem:
                    return {**base, **label, "approved": False, "reason": "artifact_deleted",
                            "submission_id": submission["id"], "output": entry["name"]}
                if not self.store.visible(artifact_row, principal):
                    return {**base, **label, "approved": False,
                            "reason": "artifact_not_visible",
                            "submission_id": submission["id"], "output": entry["name"]}
                if artifact_row["project"] != contract["project_id"]:
                    return {**base, **label, "approved": False,
                            "reason": "artifact_cross_project",
                            "submission_id": submission["id"], "output": entry["name"]}
                if artifact_row["revision"] != entry["revision"]:
                    return {**base, **label, "approved": False, "reason": "artifact_superseded",
                            "submission_id": submission["id"], "output": entry["name"],
                            "pinned_revision": entry["revision"],
                            "current_revision": artifact_row["revision"]}
                # Bounded re-hash: submissions are capped at
                # MAX_SUBMISSION_CONTENT_BYTES aggregate; an over-limit body is
                # treated as tampered rather than hashed unboundedly.
                if (len(artifact_row["body"].encode("utf-8")) > MAX_ARTIFACT_CONTENT_BYTES + 4096
                        or content_checksum(json.loads(artifact_row["body"]))
                        != entry["sha256"]):
                    return {**base, **label, "approved": False,
                            "reason": "artifact_checksum_mismatch",
                            "submission_id": submission["id"], "output": entry["name"]}
            satisfied.append({"contract_id": contract["id"],
                              "contract_revision": contract["revision"]})
        return {**base, "approved": True, "reason": "satisfied", "contracts": satisfied}

    def gate_view(self, principal, task_id, dependency_id):
        task = self._task(principal, task_id)
        # The dependency must be a declared dependency of THIS task; anything
        # else is privacy-safe 404 (no cross-project or hidden source leaks).
        declared = set(task.get("depends_on") or [])
        if dependency_id not in declared:
            edge = self.store.db.execute(
                "SELECT 1 FROM relations WHERE project=? AND source=? AND target=? "
                "AND kind='DEPENDS_ON'",
                (task["project_id"], task_id, dependency_id)).fetchone()
            if not edge:
                raise Problem("not_found", 404)
        dependency = self._task(principal, dependency_id)
        if dependency["project_id"] != task["project_id"]:
            raise Problem("not_found", 404)
        return self.dependency_gate(principal, task, dependency_id)


def dependency_gate(service, principal, task, dependency):
    """Parent hook: gate one (task, dependency) pair before claim/start.

    With no installed/enabled contracts this is a pass-through approval, keeping
    the default-off feature a strict no-op.
    """
    handle = getattr(service, "artifact_contracts", None)
    if handle is None:
        return {"approved": True, "reason": "contracts_disabled",
                "task_id": task["id"],
                "dependency_id": dependency["id"] if isinstance(dependency, dict) else dependency,
                "diagnostics": []}
    return handle.dependency_gate(principal, task, dependency)


def gate_action(verdict):
    """Map gate diagnostics to the parent's {'action': 'allow'|'defer'} contract."""
    return {"action": "allow" if verdict.get("approved") else "defer",
            "reason": verdict.get("reason"),
            "diagnostics": verdict.get("diagnostics", [])[:MAX_DIAGNOSTICS]}


OPERATION_DESCRIPTORS = [
    {
        "name": "artifact_contracts.manage",
        "purpose": "create/revise/revoke task-bound typed deliverable contracts",
        "implementation": "hivemind.artifact_contracts.ArtifactContracts",
        "input": "ContractCreate/ContractRevise/ContractRevoke (strict schema, operator only)",
        "output": "canonical artifact_contract + durable artifact_contract_binding ledger",
        "exactness": "bounded data-only JSON-schema subset; freezing while the bound task "
                     "executes; revocation is explicit and policy-gated, deletion never "
                     "silently releases a dependency",
        "limits": f"outputs<={MAX_CONTRACT_OUTPUTS}, schema<={MAX_SCHEMA_BYTES}B/"
                  f"depth<={MAX_SCHEMA_DEPTH}, contracts per task<={MAX_CONTRACTS_PER_TASK}",
        "default_enabled": False,
    },
    {
        "name": "artifact_contracts.submit",
        "purpose": "record exact artifact/revision/checksum evidence and a computed verdict",
        "implementation": "hivemind.artifact_contracts.ArtifactContracts.submit",
        "input": "SubmissionCreate {contract_revision, task_id, run_id, execution_id, pins}",
        "output": "immutable artifact_submission with verdict and scoped validation receipts",
        "exactness": "producer authority via run/represent and the CURRENT execution; "
                     "heavy compute off the canonical lock; trusted checks exactly-once-"
                     "or-unknown via a durable intent and evidence-bound run_bound records",
        "limits": f"artifacts<={MAX_SUBMISSION_ARTIFACTS}, aggregate content<="
                  f"{MAX_SUBMISSION_CONTENT_BYTES}B, checks<={MAX_CONTRACT_OUTPUTS * MAX_OUTPUT_CHECKS}",
        "hivemind_detached_work": True,
        "default_enabled": False,
    },
    {
        "name": "artifact_contracts.gate",
        "purpose": "dependency approval diagnostics consulted before dependent claim/start",
        "implementation": "hivemind.artifact_contracts.dependency_gate",
        "input": "(principal, task, dependency); adapter service.task_dependency_gates "
                 "returns {'action': 'allow'|'defer', 'reason'}",
        "output": "{approved, reason, diagnostics, missing_outputs?}",
        "exactness": "pure canonical read; binding-ledger applicability (hidden holds, "
                     "deletion never releases); exact current contract revision, output "
                     "set, execution binding, visibility, revision, and checksum re-checked "
                     "on every use",
        "limits": f"DFS gray/black cycle detection, nodes<={MAX_DEPENDENCY_DEPTH}, "
                  f"edges<={MAX_DEPENDENCY_EDGES}, fail closed on exhaustion",
        "default_enabled": False,
    },
]


def describe_operations():
    return [dict(descriptor) for descriptor in OPERATION_DESCRIPTORS]


def install(app, service, principal, command, envelope, *, policy=None, verifier=None):
    """Opt-in install. Default off: no routes, no handle, scheduling gate passes through.

    Reinstall replaces only this module's own routes, so an explicit re-install
    can never shadow another module (or be shadowed by a stale copy of itself).
    """
    from .capabilities import replace_routes
    from .agent_feature_gates import set_feature_gates, set_resource_projection
    replace_routes(app, __name__)
    policy = policy or ContractPolicy()
    if policy.enabled is not True:
        app.state.artifact_contracts = None
        service.artifact_contracts = None
        set_resource_projection(service, "artifact_contract", None)
        set_resource_projection(service, "artifact_submission", None)
        if "artifact_contracts" in getattr(service, "_agent_feature_gates", {}):
            set_feature_gates(service, "artifact_contracts")
        return None
    contracts = ArtifactContracts(app, service, policy, verifier)
    app.state.artifact_contracts = contracts
    service.artifact_contracts = contracts
    set_resource_projection(service, "artifact_contract", contracts.get_contract)

    def submission_view(p, identity):
        body = service.store.get(identity, p, "artifact_submission")
        candidates = contracts.list_submissions(p, body["contract_id"])
        visible = next((item for item in candidates if item["id"] == identity), None)
        if visible is None:
            raise Problem("not_found", 404)
        return visible
    set_resource_projection(service, "artifact_submission", submission_view)
    set_feature_gates(service, "artifact_contracts", dependency=(
        lambda p, task, dependency: gate_action(contracts.dependency_gate(p, task, dependency))))
    endpoints = []

    @app.post("/v1/artifact-contracts")
    async def contract_create(body: ContractCreate, p=Depends(principal),
                              idempotency_key: str | None = Header(None)):
        return command(p, "artifact-contracts", idempotency_key, body,
                       lambda: contracts.create_contract(p, body))

    @app.get("/v1/artifact-contracts/{contract_id}")
    async def contract_get(contract_id: str, p=Depends(principal)):
        return envelope(contracts.get_contract(p, contract_id))

    @app.get("/v1/projects/{project_id}/artifact-contracts")
    async def contract_list(project_id: str, task_id: str | None = None, p=Depends(principal)):
        return envelope(contracts.list_contracts(p, project_id, task_id))

    @app.post("/v1/artifact-contracts/{contract_id}:revise")
    async def contract_revise(contract_id: str, body: ContractRevise, p=Depends(principal),
                              idempotency_key: str | None = Header(None)):
        return command(p, f"artifact-contract-revise:{contract_id}", idempotency_key, body,
                       lambda: contracts.revise_contract(p, contract_id, body))

    @app.post("/v1/artifact-contracts/{contract_id}:revoke")
    async def contract_revoke(contract_id: str, body: ContractRevoke, p=Depends(principal),
                              idempotency_key: str | None = Header(None)):
        return command(p, f"artifact-contract-revoke:{contract_id}", idempotency_key, body,
                       lambda: contracts.revoke_contract(p, contract_id, body))

    @app.post("/v1/artifact-contracts/{contract_id}:submit")
    async def contract_submit(contract_id: str, body: SubmissionCreate, p=Depends(principal),
                              idempotency_key: str | None = Header(None)):
        # Two-phase detached handler: authorize + snapshot under the store lock,
        # hash/parse/validate + trusted checks off the global canonical mutex
        # (behind a durable exactly-once intent), durable record via mutate.
        run_detached = getattr(app.state, "run_detached", None)
        if run_detached is None:
            import asyncio
            result = await asyncio.to_thread(contracts.submit, p, contract_id, body, idempotency_key)
        else:
            result = await run_detached(contracts.submit, p, contract_id, body, idempotency_key)
        return envelope(result)

    contract_submit.hivemind_detached_work = True

    @app.get("/v1/artifact-contracts/{contract_id}/submissions")
    async def submission_list(contract_id: str, p=Depends(principal)):
        return envelope(contracts.list_submissions(p, contract_id))

    @app.get("/v1/artifact-contracts:gate")
    async def contract_gate(task_id: str, dependency_id: str, p=Depends(principal)):
        return envelope(contracts.gate_view(p, task_id, dependency_id))

    endpoints.extend([
        "POST /v1/artifact-contracts",
        "GET /v1/artifact-contracts/{contract_id}",
        "GET /v1/projects/{project_id}/artifact-contracts",
        "POST /v1/artifact-contracts/{contract_id}:revise",
        "POST /v1/artifact-contracts/{contract_id}:revoke",
        "POST /v1/artifact-contracts/{contract_id}:submit",
        "GET /v1/artifact-contracts/{contract_id}/submissions",
        "GET /v1/artifact-contracts:gate",
    ])
    return {"handle": contracts, "endpoints": endpoints, "models": list(MODELS)}
