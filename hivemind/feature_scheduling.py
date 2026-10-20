"""Explicit operator-configured coordination ticks under fresh canonical authority."""
import time

from pydantic import Field, StrictBool, model_validator

from . import models as m
from .coordination_policy import PolicyTick
from .storage import Problem


class CoordinationSchedule(m.Model):
    enabled: StrictBool = False
    operator_principal_id: m.ID | None = None
    project_ids: list[m.ID] = Field(default_factory=list, max_length=20)
    interval_seconds: float = Field(default=5, ge=1, le=300, allow_inf_nan=False)
    task_limit: int = Field(default=50, strict=True, ge=1, le=100)

    @model_validator(mode="after")
    def identity(self):
        if self.enabled and (not self.operator_principal_id or not self.project_ids):
            raise ValueError("Enabled scheduling requires an operator and explicit projects")
        if len(self.project_ids) != len(set(self.project_ids)):
            raise ValueError("Scheduled projects must be unique")
        return self


class CoordinationRunner:
    def __init__(self, service, policies, config):
        self.service, self.policies, self.config = service, policies, config
        self.last = None

    def scheduled_tick(self):
        now = time.monotonic()
        if not self.config.enabled or (
                self.last is not None and now - self.last < self.config.interval_seconds):
            return
        self.last = now
        for project in self.config.project_ids:
            with self.service.store.transaction():
                row = self.service.store.db.execute(
                    "SELECT * FROM principals WHERE id=?", (self.config.operator_principal_id,)).fetchone()
                if row is None:
                    raise Problem("configured_operator_unavailable", 403)
                principal = dict(row)
                epoch = None
                if principal["role"] not in ("admin", "human"):
                    lease = self.service.lease(principal, project)
                    if lease is not None:
                        epoch = lease["coordinator_epoch"]
                self.policies.tick(principal, project,
                                   PolicyTick(limit=self.config.task_limit, coordinator_epoch=epoch))
