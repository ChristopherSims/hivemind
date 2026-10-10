"""Opt-in autonomous Nexus merge loop over the canonical Git collaboration APIs.

Default disabled. When enabled, a bounded scheduled tick discovers open merge
requests, schedules the configured trusted checks and an independent review,
conservatively acquires or renews the project Nexus lease, and publishes exact
hash-fenced merges through ``GitCollaboration.merge``.

Every canonical mutation (lease acquire/renew, check, review, merge) runs in its
own idempotent ``Service.mutate`` command with a deterministic, revision-pinned
key, so no long-lived transaction or store lock is ever held across reviewer
runtime callbacks, and a retried tick replays rather than duplicates effects.
The injected review runtime is always invoked with no active SQLite transaction.
Unknown or uncertain effects abort the item instead of retrying.
"""
from dataclasses import dataclass
import hashlib
import json
import threading
import time
from typing import Any, Literal, Mapping, NamedTuple, Protocol

from pydantic import Field

from . import models as m
from .git_collaboration import MergeRequestCheck, MergeRequestMerge, MergeRequestReview
from .storage import Problem, canonical

HASH_KEYS = ("source_oid", "target_oid", "candidate_oid")
REVIEW_REASON_MAX = 4096


class MergeReviewOutcome(NamedTuple):
    """Decision returned by the pluggable review runtime for one exact candidate.

    ``reviewer_principal_id`` is optional self-identification: when set it must
    equal the principal the approval will be recorded under, otherwise the
    outcome is treated as ``abstain`` — an approval is never recorded under a
    different reviewer than the one the runtime answered for."""
    decision: Literal["approve", "reject", "abstain"]
    reason: str
    reviewer_principal_id: str | None = None


class MergeReviewRuntime(Protocol):
    """Injectable review runtime, invoked with NO active store transaction.

    A runtime may optionally bind itself to one reviewer by exposing a
    ``reviewer_principal_id`` attribute; a mismatch with the selected reviewer
    disables the review instead of impersonating anyone."""

    def review_merge(self, request: Mapping[str, Any]) -> MergeReviewOutcome:
        """Return the independent review decision for the given snapshot, which
        carries exactly: id, project_id, task_id, agent_id, title, the
        source_oid/target_oid/candidate_oid pins, and the reviewer_principal_id
        plus reviewer_agent_id the decision will be recorded under."""
        ...  # pragma: no cover - protocol declaration


@dataclass(frozen=True)
class NexusGitPolicy:
    enabled: bool = False
    project_id: str | None = None
    nexus_principal_id: str | None = None     # agent principal holding the coordinator grant
    nexus_run_id: str | None = None           # live run of that Nexus agent
    operator_principal_id: str | None = None  # admin/human principal that records checks
    reviewer_principal_ids: tuple[str, ...] = ()
    checks: tuple[str, ...] = ()              # names must exist in GitCollaboration.checks
    require_independent_review: bool = True
    requested_ttl_seconds: int = 60
    renew_margin_seconds: float = 15
    interval_seconds: float = 30
    max_merges_per_tick: int = 4
    max_listing: int = 64


class NexusGitTick(m.Model):
    project_id: m.ID
    limit: int = Field(default=4, ge=1, le=16)


class NexusGitAutomation:
    """Bounded autonomous Nexus merger. One tick never exceeds policy limits and
    never impersonates another principal: the Nexus agent merges, the configured
    operator records checks, and a configured independent reviewer approves."""

    def __init__(self, service, git, policy=None, runtime=None, clock=time.monotonic):
        self.service, self.git, self.store = service, git, service.store
        self.policy = policy or NexusGitPolicy()
        self.runtime, self.clock = runtime, clock
        policy = self.policy
        if not isinstance(policy.enabled, bool):
            raise ValueError("policy.enabled must be a boolean")
        if not 1 <= policy.interval_seconds <= 3600:
            raise ValueError("Invalid tick interval")
        if not 10 <= policy.requested_ttl_seconds <= 120:
            raise ValueError("Invalid lease TTL")
        if not 0 <= policy.renew_margin_seconds < policy.requested_ttl_seconds:
            raise ValueError("Invalid renewal margin")
        if not 1 <= policy.max_merges_per_tick <= 16 or not 1 <= policy.max_listing <= 1000:
            raise ValueError("Invalid tick bounds")
        if policy.enabled and not all((policy.project_id, policy.nexus_principal_id,
                                       policy.nexus_run_id, policy.operator_principal_id)):
            raise ValueError("Enabled automation requires project, nexus, and operator principals")
        self._next_tick = 0.0

    def _principal(self, identifier, roles=None):
        row = self.store.db.execute("SELECT * FROM principals WHERE id=?", (identifier,)).fetchone()
        if not row or (roles and row["role"] not in roles):
            raise Problem("nexus_git_principal_unavailable", 503)
        return dict(row)

    def scheduled_tick(self):
        """Throttle-aware entry point for a supervisor or app worker loop.

        Performs no enclosing transaction: each canonical mutation inside the
        tick is its own short idempotent command, so heartbeats and reviewer
        runtime callbacks are never blocked behind a store write lock."""
        policy = self.policy
        if not policy.enabled or self.clock() < self._next_tick:
            return None
        self._next_tick = self.clock() + policy.interval_seconds
        return self.tick(self._principal(policy.nexus_principal_id, roles=("agent",)))

    def _mutate(self, principal, operation, key, payload, function):
        """One canonical mutation as one short idempotent command."""
        return self.service.mutate(principal, operation, key, payload, function)

    # ------------------------------------------------------------------ lease
    def _acquire(self, principal, run):
        project = self.policy.project_id
        epoch = self.store.project(project)["epoch"]  # Changes on every acquisition.
        return self._mutate(principal, "nexus-git-acquire", f"{project}:{run['id']}:{epoch}",
                            {"project_id": project, "run_id": run["id"]},
                            lambda: self.service.acquire_nexus(principal, project, m.NexusRequest(
                                agent_id=principal["agent"], run_id=run["id"],
                                requested_ttl_seconds=self.policy.requested_ttl_seconds)))

    def _renew(self, principal, lease):
        project = self.policy.project_id

        def renew():
            active = self.service.lease(principal, project)
            if not active or (active["id"], active["run_id"]) != (lease["id"], lease["run_id"]):
                raise Problem("stale_coordinator_epoch")
            self.service.coordinator(principal, project, active["coordinator_epoch"])
            active["expires_at"] = time.time() + self.policy.requested_ttl_seconds
            row = self.store.raw(active["id"])
            return self.store.save("nexus_lease", project, row["owner"], active,
                                   expected=active["revision"])

        return self._mutate(principal, "nexus-git-renew", f"{lease['id']}:{lease['revision']}",
                            {"lease_id": lease["id"], "coordinator_epoch": lease["coordinator_epoch"]},
                            renew)

    def _lease(self, principal, run):
        """Reuse, renew, or acquire the Nexus lease; yield to any other coordinator."""
        lease = self.service.lease(principal, self.policy.project_id)
        if lease and (lease["agent_id"], lease["run_id"]) != (principal["agent"], run["id"]):
            return None  # Another live coordinator holds the project; never preempt it.
        if lease and lease["expires_at"] < time.time() + self.policy.renew_margin_seconds:
            lease = self._renew(principal, lease)
        return lease or self._acquire(principal, run)

    def _revalidate(self, principal, lease):
        """Re-read the lease after a potentially long reviewer runtime call.

        Returns the (possibly renewed) own lease, or None when the lease was
        replaced or expired — the caller must yield rather than publish."""
        current = self.service.lease(principal, self.policy.project_id)
        if not current or current["id"] != lease["id"]:
            return None
        if current["expires_at"] < time.time() + self.policy.renew_margin_seconds:
            current = self._renew(principal, current)
        return current

    def _live_run(self, principal):
        """Fresh liveness from the owning run only; never fabricated for others."""
        run = self.service.run(principal, self.policy.nexus_run_id)
        if run["agent_id"] != principal["agent"] or run["project_id"] != self.policy.project_id \
                or run["status"] not in ("running", "idle"):
            raise Problem("nexus_run_not_live", 409)
        return run

    def _control_pending(self, principal, run):
        """A queued/applying control targeting our own run must be applied first."""
        return any(item["state"] in ("queued", "acknowledged", "applying")
                   and item["target_run_id"] == run["id"]
                   for item in self.store.list("control", self.policy.project_id, principal, 200))

    def _heartbeat(self, run):
        """Volatile presence for the automation's own run (mirrors runs/:heartbeat).

        Never touches another agent's run and never revives a terminal one."""
        with self.store.transaction():
            row = self.store.raw(run["id"], "run")
            body = json.loads(row["body"])
            if body["agent_id"] != run["agent_id"] \
                    or body["status"] in ("completed", "failed", "cancelled"):
                return
            body["last_seen_at"] = time.time()
            self.store.db.execute("UPDATE resources SET body=? WHERE id=?",
                                  (canonical(body), run["id"]))

    def _maintain(self, principal, lease, run, stop, state):
        """Bounded owner-only lease/heartbeat upkeep while the reviewer runtime runs.

        Stops on lease replacement, a paused/terminal own run, or a pending
        control targeting our run; every iteration is a short command, never a
        transaction spanning model execution."""
        interval = min(5.0, max(1.0, self.policy.renew_margin_seconds / 2))
        while not stop.wait(interval):
            try:
                current = self.service.lease(principal, self.policy.project_id)
                if not current or current["id"] != lease["id"]:
                    state["stopped"] = "lease_lost"
                    return
                live = self.service.run(principal, run["id"])
                if live["agent_id"] != principal["agent"] or live["status"] not in ("running", "idle"):
                    state["stopped"] = "run_inactive"
                    return
                if self._control_pending(principal, live):
                    state["stopped"] = "control_pending"
                    return
                if current["expires_at"] < time.time() + self.policy.renew_margin_seconds:
                    current = self._renew(principal, current)
                    state["renewals"] += 1
                self._heartbeat(live)
                state["heartbeats"] += 1
            except Exception:
                state["stopped"] = "error"
                return

    def _run_runtime(self, body, reviewer, principal, lease, run):
        """Invoke the reviewer runtime with owner-only maintenance around it.

        No transaction is held during the call; the maintenance thread is
        always stopped and joined before returning."""
        stop, state = threading.Event(), {"stopped": None, "renewals": 0, "heartbeats": 0}
        thread = threading.Thread(target=self._maintain, args=(principal, lease, run, stop, state),
                                  name="nexus-git-maintenance", daemon=True)
        thread.start()
        try:
            return self._decide(body, reviewer), state
        finally:
            stop.set()
            thread.join()

    # ------------------------------------------------------------------- tick
    def tick(self, principal=None):
        """One bounded pass. Each merge request is advanced independently; a
        failure on one never retries another, and a lost lease stops the pass."""
        policy = self.policy
        if not policy.enabled:
            raise Problem("nexus_git_automation_disabled")
        if principal is None:
            principal = self._principal(policy.nexus_principal_id, roles=("agent",))
        if principal["id"] != policy.nexus_principal_id or principal["role"] != "agent":
            raise Problem("current_nexus_required", 403)
        self.service.project_access(principal, policy.project_id)
        run = self._live_run(principal)
        operator = self._principal(policy.operator_principal_id, roles=("admin", "human"))
        lease = self._lease(principal, run)
        if not lease:
            return {"state": "yield", "results": []}
        pending = [item for item in self.store.list("merge_request", policy.project_id,
                                                    principal, policy.max_listing)
                   if item["status"] == "open" and item.get("candidate_oid")]
        results = []
        for body in pending[::-1][:policy.max_merges_per_tick]:  # oldest first
            try:
                result = self._advance(principal, operator, body, run, lease)
            except Problem as error:
                result = {"id": body["id"], "state": "blocked", "code": error.code}
            except Exception as error:  # Unknown effect: report and stop, never retry blindly.
                results.append({"id": body["id"], "state": "error", "code": type(error).__name__})
                break
            results.append(result)
            if result["state"] in ("lease_lost", "control_pending") or result.get("code") in (
                    "stale_coordinator_epoch", "nexus_lease_too_short"):
                break  # Bounded continuation: never publish further items on a stale
                # lease or while a control targeting our own run is pending.
        return {"state": "active", "lease_id": lease["id"],
                "coordinator_epoch": lease["coordinator_epoch"], "results": results}

    def _reviewer(self, body):
        for identifier in self.policy.reviewer_principal_ids:
            row = self.store.db.execute("SELECT * FROM principals WHERE id=?", (identifier,)).fetchone()
            if row and row["role"] == "agent" and row["project"] == body["project_id"] \
                    and row["agent"] != body["agent_id"]:
                return dict(row)
        return None

    def _decide(self, body, reviewer):
        """Invoke the runtime with no active transaction and validate identity."""
        if self.runtime is None:
            # Never invent an approval: without an explicit runtime the
            # independent review slot stays open and the merge waits.
            return MergeReviewOutcome("abstain", "No review runtime configured")
        bound = getattr(self.runtime, "reviewer_principal_id", None)
        if bound is not None and bound != reviewer["id"]:
            return MergeReviewOutcome("abstain", "Review runtime bound to a different reviewer")
        snapshot = {key: body[key] for key in
                    ("id", "project_id", "task_id", "agent_id", "title", *HASH_KEYS)}
        snapshot["reviewer_principal_id"] = reviewer["id"]
        snapshot["reviewer_agent_id"] = reviewer["agent"]
        outcome = self.runtime.review_merge(snapshot)
        if not isinstance(outcome, MergeReviewOutcome) or outcome.decision not in ("approve", "reject", "abstain") \
                or not isinstance(outcome.reason, str) or not 1 <= len(outcome.reason) <= REVIEW_REASON_MAX:
            return MergeReviewOutcome("abstain", "Review runtime returned an invalid outcome")
        if outcome.reviewer_principal_id is not None \
                and outcome.reviewer_principal_id != reviewer["id"]:
            return MergeReviewOutcome("abstain", "Review runtime answered for a different reviewer")
        return outcome

    def _check_fingerprint(self, name):
        """Current policy fingerprint, computed exactly like the merge gate does."""
        policy = getattr(self.git, "_check_policy", None)
        if callable(policy):
            return policy(name)
        return hashlib.sha256(canonical(self.git.checks[name]).encode()).hexdigest()

    def _check_obsolete(self, item, body, name):
        """A recorded check is only trustworthy for the current policy and pins."""
        if name not in self.git.checks:
            return False  # Unknown configured check stays blocked, never refreshed blindly.
        return item.get("policy_sha256") != self._check_fingerprint(name) \
            or any(item.get(key) != body[key] for key in HASH_KEYS)

    def _advance(self, principal, operator, body, run, lease):
        identity = body["id"]
        records = {item["name"]: item for item in body.get("checks", [])}
        for name in self.policy.checks:
            item = records.get(name)
            if item is not None and not self._check_obsolete(item, body, name):
                continue  # Fresh receipt for the current policy and exact hashes.
            if item is not None and name not in self.git.checks:
                raise Problem("git_check_not_configured", 503)
            fingerprint = (self._check_fingerprint(name) if name in self.git.checks
                           else hashlib.sha256(canonical(None).encode()).hexdigest())[:16]
            body = self._mutate(operator, "nexus-git-check",
                                f"{identity}:{body['revision']}:{name}:{fingerprint}",
                                {"merge_request_id": identity, "check_name": name,
                                 "policy_fingerprint": fingerprint},
                                lambda name=name, body=body: self.git.check(
                                    operator, identity, MergeRequestCheck(
                                        expected_revision=body["revision"], check_name=name)))
        reviewer = self._reviewer(body)
        if reviewer is None and self.policy.require_independent_review:
            return {"id": identity, "state": "waiting_independent_reviewer"}
        if reviewer is not None and not any(item["principal_id"] == reviewer["id"]
                                            for item in body.get("reviews", [])):
            # No transaction is held while the runtime runs; owner-only
            # maintenance keeps our own lease/heartbeat fresh meanwhile.
            outcome, _state = self._run_runtime(body, reviewer, principal, lease, run)
            # The runtime bridge may itself have canonically recorded the
            # review: re-read and recognize it instead of duplicating.
            fresh = self.store.get(identity, principal, "merge_request")
            existing = next((item for item in fresh.get("reviews", [])
                             if item["principal_id"] == reviewer["id"]), None)
            if existing is not None:
                if any(existing[key] != fresh[key] for key in HASH_KEYS):
                    return {"id": identity, "state": "blocked",
                            "code": "git_review_hash_mismatch"}  # Fail closed on pin mismatch.
                body = fresh
                if existing["decision"] == "reject":
                    return {"id": identity, "state": "rejected"}
            else:
                if outcome.decision == "abstain":
                    return {"id": identity, "state": "review_abstained"}
                # Gate before recording anything: fresh owning run, no pending
                # control, and a revalidated/renewed own lease.
                run = self._live_run(principal)
                if self._control_pending(principal, run):
                    return {"id": identity, "state": "control_pending"}
                lease = self._revalidate(principal, lease)
                if lease is None:
                    return {"id": identity, "state": "lease_lost"}
                body = self._mutate(reviewer, "nexus-git-review",
                                    f"{identity}:{body['revision']}:{reviewer['id']}",
                                    {"merge_request_id": identity, "decision": outcome.decision,
                                     "reviewer_principal_id": reviewer["id"]},
                                    lambda: self.git.review(reviewer, identity, MergeRequestReview(
                                        expected_revision=body["revision"], reason=outcome.reason,
                                        decision=outcome.decision,
                                        **{key: body[key] for key in HASH_KEYS})))
                if outcome.decision == "reject":
                    return {"id": identity, "state": "rejected"}
        # Pre-merge gate after any long review: fresh owning-run liveness, no
        # pending control targeting our run, and a revalidated/renewed own lease.
        run = self._live_run(principal)
        if self._control_pending(principal, run):
            return {"id": identity, "state": "control_pending"}
        lease = self._revalidate(principal, lease)
        if lease is None:
            return {"id": identity, "state": "lease_lost"}
        body = self._mutate(principal, "nexus-git-merge",
                            f"{identity}:{body['revision']}:{lease['id']}",
                            {"merge_request_id": identity, "lease_id": lease["id"],
                             "coordinator_epoch": lease["coordinator_epoch"]},
                            lambda: self.git.merge(principal, identity, MergeRequestMerge(
                                expected_revision=body["revision"],
                                coordinator_epoch=lease["coordinator_epoch"],
                                lease_id=lease["id"], run_id=run["id"],
                                **{key: body[key] for key in HASH_KEYS})))
        return {"id": identity, "state": "merged", "merged_oid": body["merged_oid"]}
