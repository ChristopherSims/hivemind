from datetime import datetime
import math
from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ID = Annotated[str, Field(min_length=1, max_length=128)]
Text = Annotated[str, Field(min_length=1, max_length=65536)]
UTCDateTime = Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$",
                                   json_schema_extra={"format": "date-time"})]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, strict=True)

    @model_validator(mode="after")
    def byte_limits(self):
        pending = list(self.__dict__.values())
        while pending:
            value = pending.pop()
            if isinstance(value, str) and len(value.encode()) > 65536:
                raise ValueError("Inline string exceeds UTF-8 byte limit")
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("Nonfinite values are forbidden")
            if isinstance(value, dict):
                pending.extend(value.values())
            if isinstance(value, list):
                pending.extend(value)
        return self


class Ref(Model):
    kind: Literal[
        "agent", "run", "task", "event", "artifact", "board_post", "memory",
        "scope", "work_claim", "conflict", "control", "decision",
    ]
    id: ID
    revision: int | None = Field(default=None, ge=1)


class Segment(Model):
    kind: Literal["text", "agent_mention"]
    text: Text | None = None
    agent_id: ID | None = None

    @model_validator(mode="after")
    def variant(self):
        if self.kind == "text" and (self.text is None or self.agent_id is not None):
            raise ValueError("Text segment requires text only")
        if self.kind == "agent_mention" and (self.agent_id is None or self.text is not None):
            raise ValueError("Mention segment requires agent_id only")
        return self


class Content(Model):
    media_type: Literal["text/plain", "text/markdown", "application/vnd.hivemind.chat+json"]
    text: Text | None = None
    segments: list[Segment] | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def variant(self):
        rich = self.media_type == "application/vnd.hivemind.chat+json"
        if rich != (self.segments is not None) or rich == (self.text is not None):
            raise ValueError("Content fields do not match media type")
        size = len((self.text or "".join(s.text or s.agent_id or "" for s in self.segments or [])).encode())
        if size > 65536:
            raise ValueError("Inline content too large")
        if len({s.agent_id for s in self.segments or [] if s.agent_id}) > 10:
            raise ValueError("Too many mention targets")
        return self


class AgentCreate(Model):
    project_id: ID
    display_name: Annotated[str, Field(min_length=1, max_length=120)]
    description: str = Field(default="", max_length=4096)
    runtime: str = Field(default="generic", max_length=128)
    capabilities: list[Annotated[str, Field(min_length=1, max_length=128)]] | None = Field(default=None, max_length=32)
    supported_actions: list[Literal["steer", "pause", "resume", "cancel", "assign_task"]] = []
    scope_permit_enforcement: bool = False


class AgentUpdate(Model):
    expected_revision: int = Field(ge=1)
    display_name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=4096)


class RunCreate(Model):
    project_id: ID
    agent_id: ID
    workspace_id: ID | None = None
    task_id: ID | None = None


class Heartbeat(Model):
    capture_health: dict = Field(default_factory=lambda: {"state": "healthy"})


def safe_path(value: str) -> str:
    path = value.replace("\\", "/")
    if not path or "\x00" in path or ":" in path or path.startswith("/") or ".." in path.split("/"):
        raise ValueError("Path must be workspace-relative without traversal")
    return str(PurePosixPath(path))


class Source(Model):
    source_id: ID
    kind: Literal["runtime_hook", "tool_wrapper", "filesystem", "sdk"]
    sequence: int = Field(ge=0)
    attribution: Literal["confirmed", "reported", "unattributed"] = "reported"


class Event(Model):
    protocol: Literal["hivemind"] = "hivemind"
    protocol_version: Literal["0.1.0"] = "0.1.0"
    event_id: ID
    type: Literal[
        "hivemind.context.agent.status_reported", "hivemind.context.tool.started",
        "hivemind.context.tool.completed", "hivemind.context.file.changed",
        "hivemind.context.run.status_changed", "hivemind.context.run.ended",
        "hivemind.context.capture.gap_detected", "hivemind.context.decision.recorded",
    ]
    agent_id: ID
    run_id: ID
    project_id: ID
    occurred_at: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
    source: Source
    data: dict

    @field_validator("occurred_at")
    @classmethod
    def timestamp(cls, v):
        if not v.endswith("Z"):
            raise ValueError("Timestamp must be UTC RFC3339")
        datetime.fromisoformat(v)
        return v

    @model_validator(mode="after")
    def payload(self):
        # Separate strict payload models are also exported as normative schemas.
        classes = {
            "agent.status_reported": StatusData, "tool.started": ToolStart,
            "tool.completed": ToolEnd, "file.changed": FileData,
            "run.status_changed": RunStatus, "run.ended": RunStatus,
            "capture.gap_detected": GapData, "decision.recorded": DecisionData,
        }
        suffix = self.type.removeprefix("hivemind.context.")
        self.data = classes[suffix].model_validate(self.data).model_dump(exclude_none=True)
        if suffix == "run.ended" and self.data["status"] not in ("completed", "failed", "cancelled"):
            raise ValueError("run.ended requires a terminal status")
        return self


class StatusData(Model):
    text: Text


class ToolStart(Model):
    call_id: ID
    name: ID


class ToolEnd(Model):
    call_id: ID
    success: bool | None = None


class FileData(Model):
    path: str = Field(min_length=1, max_length=4096)
    change: Literal["created", "modified", "deleted"]

    _path = field_validator("path")(safe_path)


class RunStatus(Model):
    status: Literal["running", "idle", "waiting_input", "blocked", "paused", "completed", "failed", "cancelled"]
    expected_revision: int = Field(ge=1)


class GapData(Model):
    reason: Text
    dropped_observation_count: int = Field(default=0, ge=0)


class DecisionData(Model):
    text: Text
    evidence_refs: list[Ref] = Field(default_factory=list, max_length=32)


class Batch(Model):
    events: list[dict] = Field(min_length=1, max_length=100)


class TaskCreate(Model):
    project_id: ID
    title: str = Field(min_length=1, max_length=512)
    description: str = Field(default="", max_length=65536)
    scope_ids: list[ID] = Field(default_factory=list, max_length=32)
class TaskStart(Model):
    expected_revision: int = Field(ge=1)
    run_id: ID
    recover: bool = False
class TaskEvidenceRef(Ref):
    kind: Literal["board_post", "artifact"]
class TaskProgress(Model):
    expected_revision: int = Field(ge=1)
    run_id: ID
    execution_id: ID
    checkpoint_id: ID
    status: Literal["running", "paused", "completed", "failed"]
    progress: int = Field(ge=0, le=100)
    summary: str = Field(min_length=1, max_length=4096)
    evidence_refs: list[TaskEvidenceRef] = Field(default_factory=list, max_length=32)


class ScopeCreate(Model):
    project_id: ID
    canonical_key: str = Field(min_length=1, max_length=512)
    display_name: str = Field(min_length=1, max_length=256)
    parent_scope_id: ID | None = None
    allowed_paths: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("allowed_paths")
    @classmethod
    def paths(cls, values):
        return [safe_path(v) for v in values]


class ClaimCreate(Model):
    project_id: ID
    agent_id: ID
    run_id: ID
    task_id: ID | None = None
    scope_ids: list[ID] = Field(min_length=1, max_length=32)
    mode: Literal["exclusive_owner", "collaborator", "observer"] = "exclusive_owner"
    intent: str = Field(min_length=1, max_length=4096)
    ttl_seconds: int = Field(default=120, ge=10, le=600)


class MessageRetryPolicy(Model):
    max_attempts: int = Field(default=5, ge=1, le=20)
    base_delay_seconds: int = Field(default=2, ge=1, le=60)
    max_delay_seconds: int = Field(default=60, ge=1, le=600)
    @model_validator(mode="after")
    def delays(self):
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("Maximum delay must not be less than base delay")
        return self
class MessageCreate(Model):
    protocol: Literal["hivemind"] = "hivemind"
    protocol_version: Literal["0.1.0"] = "0.1.0"
    message_id: ID
    project_id: ID
    from_agent: ID = Field(alias="from")
    to: ID
    sender_run_id: ID | None = None
    type: Literal["request", "response", "question", "inform", "ack", "error", "cancel"]
    thread_id: ID
    correlation_id: ID | None = None
    in_reply_to: ID | None = None
    content: Content
    ttl_seconds: int = Field(default=3600, ge=1, le=86400)
    retry_policy: MessageRetryPolicy | None = None

    @model_validator(mode="after")
    def reply(self):
        if self.type in ("response", "ack", "error", "cancel") and not self.in_reply_to:
            raise ValueError("Reply target required")
        if self.content.segments is not None:
            raise ValueError("Direct messages accept plain text or Markdown only")
        return self


class PostCreate(Model):
    client_post_id: ID
    kind: Literal["message", "question", "finding", "decision", "warning", "request", "status", "answer"] = "message"
    thread_id: ID | None = None
    parent_post_id: ID | None = None
    content: Content
    evidence_refs: list[Ref] = Field(default_factory=list, max_length=32)


class PostUpdate(Model):
    expected_revision: int = Field(ge=1)
    content: Content


class MemoryValidity(Model):
    valid_from: UTCDateTime | None = None
    valid_until: UTCDateTime | None = None
    repository_id: ID | None = None
    branch: str | None = Field(default=None, max_length=256)
    observed_commit: str | None = Field(default=None, max_length=256)

    @field_validator("valid_from", "valid_until")
    @classmethod
    def utc(cls, value):
        if value is not None:
            utc_timestamp(value)
        return value

    @model_validator(mode="after")
    def ordered(self):
        if self.valid_from and self.valid_until and utc_timestamp(self.valid_until) <= utc_timestamp(self.valid_from):
            raise ValueError("Validity end must follow its start")
        return self


def utc_timestamp(value):
    import re
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", value):
        raise ValueError("UTC RFC3339 timestamp required")
    return datetime.fromisoformat(value).timestamp()


class MemoryFields(Model):
    title: str = Field(min_length=1, max_length=512)
    content: Content
    evidence_refs: list[Ref] = Field(min_length=1, max_length=32)
    validity: MemoryValidity = Field(default_factory=MemoryValidity)
    expires_at: UTCDateTime | None = None

    @field_validator("expires_at")
    @classmethod
    def expiry(cls, value):
        if value is not None:
            utc_timestamp(value)
        return value

    @model_validator(mode="after")
    def interval(self):
        if self.expires_at and self.validity.valid_from and (
                utc_timestamp(self.expires_at) <= utc_timestamp(self.validity.valid_from)):
            raise ValueError("Expiry must follow validity start")
        return self


class MemoryCreate(MemoryFields):
    project_id: ID
    category: Literal["working", "episodic", "semantic", "procedural", "preference"] = "semantic"
    scope_kind: Literal["run", "agent", "task", "project"] = "project"
    scope_owner_id: ID
    private: bool = False
class MemoryExtractionRef(Ref):
    kind: Literal["board_post", "artifact"]
class MemoryExtract(Model):
    project_id: ID
    agent_id: ID
    requester_run_id: ID | None = None
    source_refs: list[MemoryExtractionRef] = Field(min_length=1, max_length=16)
    scope_kind: Literal["run", "agent", "task", "project"] = "agent"
    scope_owner_id: ID | None = None
    max_candidates: int = Field(default=8, ge=1, le=16)
    max_source_bytes: int = Field(default=4096, ge=1, le=16384)


class MemoryVersionProposal(MemoryFields):
    expected_revision: int = Field(ge=1)


class MemoryReview(Model):
    expected_revision: int = Field(ge=1)
    decision: Literal["verify", "reject"]
    reason: str = Field(min_length=1, max_length=4096)


class MemoryProposalReview(Model):
    expected_revision: int = Field(ge=1)
    decision: Literal["approve", "reject"]
    reason: str = Field(min_length=1, max_length=4096)


class MemoryDisputeCreate(Model):
    project_id: ID
    left_memory_id: ID
    right_memory_id: ID
    left_revision: int = Field(ge=1)
    right_revision: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=4096)
    evidence_refs: list[Ref] = Field(min_length=1, max_length=32)


class MemoryDisputeReview(Model):
    expected_revision: int = Field(ge=1)
    left_expected_revision: int = Field(ge=1)
    right_expected_revision: int = Field(ge=1)
    decision: Literal["confirm", "dismiss", "prefer_left", "prefer_right"]
    reason: str = Field(min_length=1, max_length=4096)


class Revision(Model):
    expected_revision: int = Field(ge=1)


class Selector(Model):
    agent_ids: list[ID] | None = Field(default=None, min_length=1, max_length=50)
    semantic_query: str | None = Field(default=None, min_length=1, max_length=4096)
    match: Literal["union", "intersection"] = "union"
    active_only: bool = True
    max_agents: int = Field(default=10, ge=1, le=50)

    @model_validator(mode="after")
    def selector(self):
        if not self.agent_ids and not (self.semantic_query and self.semantic_query.strip()):
            raise ValueError("An explicit ID or nonblank semantic query is required")
        if self.semantic_query is not None and not self.semantic_query.strip():
            raise ValueError("Semantic query must not be blank")
        if self.match == "intersection" and not (self.agent_ids and self.semantic_query):
            raise ValueError("Intersection requires both selectors")
        if self.agent_ids and (len(set(self.agent_ids)) != len(self.agent_ids) or len(self.agent_ids) > self.max_agents):
            raise ValueError("Explicit IDs must be unique and within max_agents")
        return self


class ContextQuery(Model):
    project_id: ID
    agent_selector: Selector
    query: str = Field(default="", max_length=4096)
    token_budget: int = Field(default=6000, ge=100, le=32000)
    require_verified_memory: bool = True
    include: list[Literal["ledger", "work_claims", "tasks", "board", "memory", "artifacts", "graph",
                          "messages", "controls"]] = Field(
        default_factory=lambda: ["ledger", "work_claims", "tasks", "board", "memory", "artifacts", "graph"])
    requester_run_id: ID | None = None
    target_tokenizer: str | None = Field(default=None, max_length=128)
    min_ingestion_seq: int | None = Field(default=None, ge=0)
    graph_max_hops: int = Field(default=2, ge=1, le=3)
    timeout_ms: int = Field(default=5000, ge=0, le=10000)


class SearchQuery(Model):
    project_id: ID
    query: str = Field(min_length=1, max_length=4096)
    mode: Literal["lexical", "semantic", "hybrid"] = "hybrid"
    limit: int = Field(default=10, ge=1, le=50)
    resource_kinds: list[Literal["agent", "board_post", "memory", "task", "work_claim", "artifact"]] = []
    min_ingestion_seq: int | None = Field(default=None, ge=0)


class NexusRequest(Model):
    agent_id: ID
    run_id: ID
    requested_ttl_seconds: int = Field(default=60, ge=10, le=120)


class ControlCreate(Model):
    project_id: ID
    target_run_id: ID
    action: Literal["steer", "pause", "resume", "cancel", "assign_task"]
    replacement_task_id: ID | None = None
    target_run_revision: int = Field(ge=1)
    coordinator_epoch: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=4096)

    @model_validator(mode="after")
    def action_fields(self):
        if (self.action in ("steer", "assign_task")) != (self.replacement_task_id is not None):
            raise ValueError("Replacement task is required only for assignment or steering")
        return self


class ControlResult(Model):
    outcome: Literal["applied", "failed", "outcome_unknown"]
    checkpoint_id: ID | None = None
    observed_status: Literal["running", "idle", "paused", "cancelled"] | None = None


class Checkpoint(Model):
    checkpoint_id: ID


class DeliveryClaim(Model):
    run_id: ID
class DeliveryReceipt(DeliveryClaim):
    delivery_id: ID | None = None
class DeliveryFailure(DeliveryClaim):
    delivery_id: ID
    reason: Literal["handler_failed", "interrupted", "temporary_unavailable"]
class MessageRequeue(Model):
    expected_revision: int = Field(ge=1)
class ConversationsRead(Model):
    message_ids: list[ID] = Field(min_length=1, max_length=100)


class GraphQuery(Model):
    project_id: ID
    seed_ids: list[ID] = Field(min_length=1, max_length=20)
    max_hops: int = Field(default=2, ge=1, le=3)
    max_nodes: int = Field(default=100, ge=1, le=100)
    max_edges: int = Field(default=200, ge=1, le=200)


class PermitRequest(Model):
    scope_ids: list[ID] = Field(min_length=1, max_length=32)
    paths: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("paths")
    @classmethod
    def paths_valid(cls, values):
        return [safe_path(v) for v in values]


class ArtifactCreate(Model):
    project_id: ID
    title: str = Field(min_length=1, max_length=512)
    content: Content
    run_id: ID | None = None
