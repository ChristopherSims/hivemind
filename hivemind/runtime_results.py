"""Explicit bounded public result contracts, never raw vendor transcripts."""
import json
from typing import Literal

from pydantic import Field, StrictBool

from . import models as m
from .storage import canonical


class ReviewResult(m.Model):
    decision: Literal["approve", "reject"]
    reason: str = Field(min_length=1, max_length=4096)
    source_oid: str = Field(pattern=r"^[0-9a-f]{40}$")
    target_oid: str = Field(pattern=r"^[0-9a-f]{40}$")
    candidate_oid: str = Field(pattern=r"^[0-9a-f]{40}$")
    findings: list[str] = Field(default_factory=list, max_length=32)


class MessageResult(m.Model):
    handled: StrictBool
    reply_text: str | None = Field(default=None, max_length=8192)


def structured_result(contract, text, redact):
    if len(text.encode()) > 16384:
        raise ValueError("Structured runtime result exceeds its boundary")
    value = json.loads(text)
    model = {"git_review": ReviewResult, "message": MessageResult}[contract]
    result = model.model_validate(value).model_dump()

    def scrub(item):
        if isinstance(item, str):
            return redact(item)
        if isinstance(item, list):
            return [scrub(entry) for entry in item]
        if isinstance(item, dict):
            return {key: scrub(entry) for key, entry in item.items()}
        return item

    result = scrub(result)
    model.model_validate(result)
    if len(canonical(result).encode()) > 16384:
        raise ValueError("Structured runtime result exceeds its boundary")
    return result
