"""Operator-configured, fresh process and protected-storage isolation checks."""
import math
from dataclasses import dataclass
from pathlib import Path

from .identity_attestation import (AttestationFailed, attest_distinct_identities,
                                   attest_process_identity, attest_protected_directory)


@dataclass(frozen=True)
class IsolationAttestationConfig:
    service_identity: str
    agent_identity: str | None = None
    protected_root: str | None = None
    max_age_seconds: float = 60

    def __post_init__(self):
        if not isinstance(self.service_identity, str) or not self.service_identity:
            raise ValueError("Service OS identity is required")
        if self.agent_identity is not None and not isinstance(self.agent_identity, str):
            raise ValueError("Agent OS identity must be a name")
        if self.protected_root is not None and not isinstance(self.protected_root, str):
            raise ValueError("Protected storage root must be a path")
        if (type(self.max_age_seconds) not in (int, float)
                or not math.isfinite(self.max_age_seconds)
                or not 0 < self.max_age_seconds <= 300):
            raise ValueError("Attestation freshness must be between 0 and 300 seconds")


class IsolationRuntimeGuard:
    """Read-only checks at startup and native Git boundaries, not a sandbox claim."""

    def __init__(self, policy, config):
        self.policy, self.config = policy, config
        self.last = ()
        if (policy.separate_agent_identity or policy.separate_service_identity
                or policy.protect_bare_directories) and not config.agent_identity:
            raise AttestationFailed("Identity/ACL separation requires the agent OS account")
        if policy.protect_bare_directories and not config.protected_root:
            raise AttestationFailed("Protected bare storage requires its exact root")
        if policy.mode == "disabled":
            raise ValueError("Disabled isolation cannot carry an attestation configuration")

    def check(self):
        records = [attest_process_identity(self.config.service_identity, require_unprivileged=True)]
        if self.config.agent_identity:
            records.append(attest_distinct_identities(
                self.config.service_identity, self.config.agent_identity))
        if self.config.protected_root:
            records.append(attest_protected_directory(
                self.config.protected_root, owner_identity=self.config.service_identity,
                forbidden_writers=(self.config.agent_identity,) if self.config.agent_identity else ()))
        for record in records:
            record.assert_fresh(self.config.max_age_seconds)
        self.last = tuple(records)
        return self.last

    def bind_resources(self, data_root, git_root):
        if self.config.protected_root:
            expected = Path(self.config.protected_root).resolve(strict=True)
            for value in (data_root, git_root):
                actual = Path(value).resolve()
                if not actual.is_relative_to(expected):
                    raise AttestationFailed("Service/Git storage escapes the attested root")
        self.check()


def agent_identity_guard(options):
    """Called before any vendor session/turn; reads the real current process token."""
    expected = options.get("os_identity")
    if expected is None:
        return None
    if not isinstance(expected, str) or not expected:
        raise ValueError("Vendor OS identity must be a nonempty account name")
    evidence = attest_process_identity(expected, require_unprivileged=True)
    evidence.assert_fresh()
    return evidence
