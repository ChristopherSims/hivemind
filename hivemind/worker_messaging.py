"""Parent-owned lifecycle wiring for bounded auxiliary messaging slots.

``WorkerMessaging`` owns every auxiliary runtime and stops them all before the
worker's journal is closed. With ``slots > 1`` each slot gets its own canonical
auxiliary run (via the supervisor), its own private runtime built from its own
stable per-slot options (a derived per-slot ``state_file``), and its own lease
lifecycle; cleanup iterates every slot's runtime.
"""
from pathlib import Path
from types import MappingProxyType

from .messaging_concurrency import MAX_IN_FLIGHT_LIMIT, AssignedRunMessagingSupervisor
from .recovery import UnknownOutcome, command_key, derive_operation_id
from .runtime_messaging import (MESSAGE_EFFECT_KIND, NOTIFICATION_EFFECT_KIND,
                                NOTIFICATION_TASK_KIND, NOTIFICATION_TURN_KIND,
                                TASK_OPERATION_KIND, TURN_OPERATION_KIND)
from .storage import digest

WORKER_MESSAGING_DESCRIPTOR = {
    "name": "worker_messaging.WorkerMessaging",
    "purpose": "Own one or more bounded auxiliary messaging slots (run + private "
               "runtime + lease lifecycle) beside an assigned primary run, and stop "
               "them all before the worker journal closes.",
    "input": "canonical client, worker config (project_id/agent_id/run_id), durable "
             "journal, vendor runtime factory, runtime options, and explicit opt-ins "
             "(enabled, sender/type allowlists, replies, slots, push_signal, "
             "recover_message_effects)",
    "output": "start() -> supervisor status; close() -> cleanup_report with stop, "
              "cleanup errors, and recovery counts",
    "authority": "creates real canonical auxiliary runs and scope-less message tasks "
                 "for the configured agent only; never touches the primary task",
    "side_effects": "canonical runs/tasks/replies, per-slot private state files, "
                    "vendor sessions; all cleaned up or honestly reported at close",
    "limits": f"slots 1..{MAX_IN_FLIGHT_LIMIT}, per-slot queue capacity bounds are "
              "enforced by the supervisor; shutdown is deadline-bounded",
    "retry_safety": "replies and tasks are deterministic/idempotent; unknown outcomes "
                    "are never acked and require explicit evidence-driven recovery",
}


def describe():
    """Self-describing metadata for parent discovery/MCP registration."""
    return dict(WORKER_MESSAGING_DESCRIPTOR)


class WorkerMessaging:
    """Own auxiliary runtimes and stop them before the worker's journal is closed."""

    def __init__(self, client, config, journal, factory, options, *, enabled=False,
                 allowed_senders=(), allowed_notification_types=(), allow_replies=False,
                 queue_capacity=16, poll_interval=5, shutdown_timeout=30,
                 recover_message_effects=False, slots=1, push_signal=None,
                 max_reconcile_seconds=60.0):
        slots = int(slots)
        if not 1 <= slots <= MAX_IN_FLIGHT_LIMIT:
            raise ValueError(f"slots must be between 1 and {MAX_IN_FLIGHT_LIMIT}")
        self.runtimes = []
        self.closed = False
        self.cleanup_report = None
        self.client, self.config, self.journal = client, config, journal
        self.slots = slots
        self.recover_message_effects = recover_message_effects is True
        self.allow_replies = allow_replies is True
        self.allowed_senders = frozenset(allowed_senders)
        self.recovery_report = {"recovered": 0, "held": 0}
        # Filled by the start()-integrated wedged-auxiliary sweep (opt-in).
        self.startup_recovery_report = None
        # Stable per-slot private options: slot 0 keeps the exact configured
        # state_file (restart-stable for existing single-slot deployments); each
        # additional slot gets its own derived path so no vendor session or state
        # file is ever shared between slots or with the primary runtime.
        self.slot_options = [self._slot_options(options, index)
                             for index in range(slots)]
        self.slot_state_files = [slot.get("state_file") for slot in self.slot_options]
        factories = [self._slot_factory(factory, index) for index in range(slots)]
        self.supervisor = AssignedRunMessagingSupervisor(
            client, config["project_id"], config["agent_id"],
            primary_run_id=config["run_id"], journal=journal,
            enabled=enabled, allowed_senders=allowed_senders,
            allowed_notification_types=allowed_notification_types,
            allow_replies=allow_replies, max_in_flight=slots,
            queue_capacity=queue_capacity, poll_interval=poll_interval,
            shutdown_timeout=shutdown_timeout, push_signal=push_signal,
            max_reconcile_seconds=max_reconcile_seconds,
            runtime_factory=factories,
            on_unknown=self._recover_unknown if self.recover_message_effects else None,
            # Recovery runs after the canonical slot runs/gateways are bound (so
            # receipts, run identity, and the recovery gate are current) and before
            # any consumer thread can dispatch the same unknown records.
            before_dispatch=(self._startup_recovery
                             if self.recover_message_effects else None))

    @staticmethod
    def _slot_options(options, index):
        derived = dict(options or {})
        state_file = derived.get("state_file")
        if state_file and index > 0:
            path = Path(str(state_file))
            derived["state_file"] = str(
                path.with_name(f"{path.stem}-slot{index}{path.suffix}"))
        return derived

    def _slot_factory(self, factory, index):
        def build():
            # No sharing with the primary task session or other slots; one stable
            # slot binding on restart.
            runtime = factory({**self.slot_options[index],
                               "result_contract": "message", "no_tools": True})
            self.runtimes.append(runtime)
            return runtime

        return build

    def _recovery_run_id(self, identifier):
        """Choose the owning auxiliary run from evidence for this exact item, never
        from a shared slot-0 default: the journaled message task's canonical
        execution run must be one of this supervisor's own auxiliary runs."""
        journal = getattr(self.journal, "journal", self.journal)
        owned = list(getattr(self.supervisor, "auxiliary_run_ids", None)
                     or [self.supervisor.auxiliary_run_id])
        run_id = None
        operation_id = derive_operation_id(
            "handler", TASK_OPERATION_KIND,
            f"{self.config['project_id']}:{self.config['agent_id']}:{identifier}")
        record = journal.operation(operation_id) if hasattr(journal, "operation") else None
        task_id = ((record or {}).get("result") or {}).get("task_id")
        if task_id:
            task = self.client.request("GET", f"tasks/{task_id}")
            run_id = ((task.get("execution") or {}).get("run_id"))
        if run_id is None and len(owned) == 1:
            # Single slot: the auxiliary run is the only possible owner.
            run_id = owned[0]
        if run_id not in owned:
            raise UnknownOutcome(
                "Recovery run binding is not provable for this item; held")
        return run_id

    def _recover_unknown(self, kind, identifier, error):
        """No model, no delivery acknowledgement, and no inference from exception text."""
        if kind != "message" or self.closed:
            return
        from .recovery_automation import Reconciler, RecoveryPolicy
        try:
            message = self.client.request("GET", f"messages/{identifier}")
            if message.get("from") not in self.allowed_senders:
                raise PermissionError("Recovery sender is outside the current allowlist")
            reconciler = Reconciler(
                self.journal.journal, self.client, project_id=self.config["project_id"],
                agent_id=self.config["agent_id"],
                run_id=self._recovery_run_id(identifier),
                policy=RecoveryPolicy(enabled=True, allow_reply_replay=self.allow_replies))
            reconciler.recover_message_effect(
                message, effect_gate=self.supervisor.authorize_recovery_effect)
            self.recovery_report["recovered"] += 1
        except Exception:
            # Missing decisions/evidence or stale controls remain held for explicit review.
            self.recovery_report["held"] += 1

    def _auxiliary_gate(self, item_id):
        """Authorize one resolution only while THIS item's lease/controls/run are current.

        Wraps the supervisor's public ``authorize_recovery_effect`` (fresh project/
        agent/slot-run/task/execution binding plus the exact current delivery lease
        and control fences, checked at the moment of mutation). Anything but an
        affirm — including a raised definite refusal — resolves nothing.
        """
        def gate(phase, context):
            try:
                return self.supervisor.authorize_recovery_effect(
                    phase, {**context, "item_id": item_id}) is True
            except Exception:
                return False
        return gate

    def recover_wedged_auxiliaries(self, *, limit=None):
        """Resolve wedged auxiliary handler operations from durable evidence only.

        Opt-in (``recover_message_effects=True``); a parent CLI/supervisor invokes
        this on startup/recovery. No model, runtime, or vendor session is ever
        invoked, no result is fabricated, and a bare operator claim is never
        authority. Per durable ``unknown`` handler record bound to THIS
        project/agent and one of THIS supervisor's own auxiliary runs:

        * ``runtime-message-task``/``runtime-notification-task`` (wedged canonical
          task phase): proved only from the journaled ``POST tasks``/``:start``
          command records for the same deterministic keys — the create record is
          first resolved from authoritative command receipts when needed — cross-
          checked against the CURRENT canonical task view, then resolved via
          ``Reconciler.resolve_auxiliary`` with a ``durable_result`` proof hashed
          from those durable records;
        * ``runtime-message-turn``/``runtime-notification-turn`` (wedged vendor
          turn): proved only by the vendor runtime's own quiescent checkpoint
          carrying authoritative pre-dispatch non-invocation (``dispatched is
          False``) for the exact run/task/execution — a mere paused/cancelled
          checkpoint is interrupted-after-dispatch and stays held;
        * ``runtime-message-effect``: delegated to the message-effect recovery
          path (completed validated decision + canonical proof); notification
          effects stay held (notifications hold no delivery lease to fence).

        Every resolution passes the live identity/controls/lease gate at the moment
        of mutation. Cross-slot runs, stale/current mismatches, missing proof and
        gate refusals stay ``unknown`` (held). Returns
        ``{"resolved": [...], "held": [...], "skipped": int}``.
        """
        if not self.recover_message_effects:
            from .recovery_automation import RecoveryError
            raise RecoveryError(
                "Wedged auxiliary recovery requires recover_message_effects=True")
        from .recovery_automation import (Reconciler, RecoveryError, RecoveryPolicy,
                                          UnknownOutcome as _Unknown)
        journal = getattr(self.journal, "journal", self.journal)
        project_id, agent_id = self.config["project_id"], self.config["agent_id"]
        owned = list(getattr(self.supervisor, "auxiliary_run_ids", None)
                     or ([self.supervisor.auxiliary_run_id]
                         if getattr(self.supervisor, "auxiliary_run_id", None) else []))
        policy = RecoveryPolicy(enabled=True, allow_reply_replay=self.allow_replies)
        budget = min(policy.max_operations, int(limit) if limit else policy.max_operations)
        report = {"resolved": [], "held": [], "skipped": 0}
        with journal.lock:  # serialize the sweep; per-op audits/mutations stay atomic
            for record in journal.operations(state="unknown", limit=budget):
                operation_id = record["operation_id"]
                context = {"operation_id": operation_id, "kind": record["path"]}
                if record["method"] != "HANDLER":
                    report["skipped"] += 1
                    continue
                body = dict(record.get("body") or {})
                if body.get("project_id") not in (None, project_id) \
                        or body.get("agent_id") not in (None, agent_id):
                    report["skipped"] += 1  # foreign identity: never touched here
                    continue
                run_id = body.get("run_id")
                item_id = body.get("item_id")
                if run_id not in owned or not item_id:
                    # Cross-slot/foreign run or unbound item: isolation preserved.
                    report["held"].append({**context, "reason": "cross_slot_or_unbound"})
                    continue
                reconciler = Reconciler(journal, self.client, project_id=project_id,
                                        agent_id=agent_id, run_id=run_id, policy=policy)
                try:
                    outcome = self._recover_wedged_operation(
                        reconciler, journal, record, run_id, item_id, owned)
                except (_Unknown, RecoveryError) as error:
                    report["held"].append({**context, "item_id": item_id,
                                           "run_id": run_id,
                                           "reason": type(error).__name__})
                except Exception as error:  # noqa: BLE001 - any shortfall is a hold
                    report["held"].append({**context, "item_id": item_id,
                                           "run_id": run_id,
                                           "reason": "error:" + type(error).__name__})
                else:
                    report["resolved"].append({**context, "item_id": item_id,
                                               "run_id": run_id, "outcome": outcome})
        return report

    def _recover_wedged_operation(self, reconciler, journal, record, run_id,
                                  item_id, owned):
        """One wedged handler operation; raises (held) unless fully proved."""
        from .recovery_automation import (PROVED_APPLIED, PROVED_NOT_ATTEMPTED,
                                          UnknownOutcome as _Unknown,
                                          handler_operation_ids,
                                          vendor_quiescence_evidence)
        kind = record["path"]
        notification = kind in (NOTIFICATION_TASK_KIND, NOTIFICATION_TURN_KIND,
                                NOTIFICATION_EFFECT_KIND)
        operation_id = record["operation_id"]
        if kind in (TURN_OPERATION_KIND, NOTIFICATION_TURN_KIND):
            task_op = handler_operation_ids(
                self.config["project_id"], self.config["agent_id"], item_id,
                notification=notification)[0]
            task_record = journal.operation(task_op)
            task_result = dict((task_record or {}).get("result") or {}) \
                if (task_record or {}).get("state") == "completed" else {}
            task_id = task_result.get("task_id")
            execution_id = task_result.get("execution_id")
            # Durable provenance is mandatory: the pre-dispatch checkpoint must
            # bind the EXACT journaled run/task/execution of this item. A mere
            # checkpoint file (or a caller-supplied flag) is never proof.
            if not task_id or not execution_id:
                raise _Unknown("No durable task/execution provenance for this "
                               "turn; a bare checkpoint is never proof")
            slot = owned.index(run_id)
            state_path = self.slot_state_files[slot] if slot < len(
                self.slot_state_files) else None
            if not state_path:
                raise _Unknown("No slot state file bound to this run; held")
            evidence = vendor_quiescence_evidence(
                state_path, run_id=run_id, task_id=task_id, execution_id=execution_id)
            ev = dict(evidence.get("evidence") or {})
            verified = (
                evidence.get("outcome") == PROVED_NOT_ATTEMPTED
                and ev.get("pre_dispatch") is True and ev.get("quiescent") is True
                and ev.get("run_id") == run_id and ev.get("task_id") == task_id
                and ev.get("execution_id") == execution_id
                and Path(str(state_path)) == Path(str(self.slot_state_files[slot])))
            if not verified:
                # Interrupted-after-dispatch, lost result, or incidental/mismatched
                # checkpoint: held, never replayed.
                raise _Unknown("Vendor turn is not proved pre-dispatch "
                               "non-invoked for the exact durable binding; held")
            finding = {"operation_id": operation_id, "outcome": PROVED_NOT_ATTEMPTED,
                       "evidence": MappingProxyType({
                           **ev, "project_id": self.config["project_id"],
                           "agent_id": self.config["agent_id"],
                           "run_id": run_id, "task_id": task_id,
                           "execution_id": execution_id,
                           "proof": {"kind": "pre_dispatch_non_invocation",
                                     "quiescent": True}})}
            reconciler.resolve_auxiliary(operation_id, finding,
                                         gate=self._auxiliary_gate(item_id))
            return PROVED_NOT_ATTEMPTED
        if kind in (TASK_OPERATION_KIND, NOTIFICATION_TASK_KIND):
            task_ref = self._wedged_task_reference(
                reconciler, journal, run_id, item_id, notification)
            finding = {"operation_id": operation_id, "outcome": PROVED_APPLIED,
                       "result": task_ref,
                       "evidence": {"project_id": self.config["project_id"],
                                    "agent_id": self.config["agent_id"],
                                    "run_id": run_id, "task_id": task_ref["task_id"],
                                    "execution_id": task_ref["execution_id"],
                                    "proof": {
                                        "kind": "durable_result",
                                        "result_sha256": digest(task_ref),
                                        "source": "journaled_command_records"}}}
            reconciler.resolve_auxiliary(operation_id, finding,
                                         gate=self._auxiliary_gate(item_id))
            return PROVED_APPLIED
        if kind == MESSAGE_EFFECT_KIND:
            before = self.recovery_report["recovered"]
            self._recover_unknown("message", item_id, None)
            if self.recovery_report["recovered"] > before:
                return PROVED_APPLIED
            raise _Unknown("Message effect recovery could not prove the effect")
        raise _Unknown("Unsupported wedged auxiliary kind; held")

    def _wedged_task_reference(self, reconciler, journal, run_id, item_id,
                               notification):
        """Reconstruct the exact task_ref from durable command records + current state.

        Both canonical commands (``POST tasks`` and ``tasks/{id}:start``) must be
        durably completed under their deterministic keys — the create record is
        resolved from authoritative command receipts first when it is still
        ``unknown`` — and the CURRENT canonical task must still show the exact
        execution under the exact run. Anything less raises (held); an ID is never
        fabricated and a stale/current mismatch never resolves.
        """
        from .recovery_automation import (PROVED_APPLIED, TASK_CREATE_COMMAND_OPERATION,
                                          UnknownOutcome as _Unknown,
                                          command_receipts, task_create_normalize)
        project_id, agent_id = self.config["project_id"], self.config["agent_id"]
        family = "notification" if notification else "message"
        create_key = command_key(derive_operation_id(
            "runtime-message", family + "-task-create", project_id, agent_id, item_id))
        create_op = derive_operation_id("POST", "tasks", create_key)
        create_record = journal.operation(create_op)
        if create_record is None:
            raise _Unknown("No durable task-create command record; held")
        if create_record["state"] == "unknown":
            recovered = reconciler.recover_task_create(
                create_op,
                receipts=command_receipts(
                    self.client,
                    operation_map={"task-create": TASK_CREATE_COMMAND_OPERATION}),
                normalize=task_create_normalize)
            if recovered["outcome"] != PROVED_APPLIED:
                raise _Unknown("Task-create command is not proved applied; held")
            create_record = journal.operation(create_op)
        if create_record["state"] != "completed":
            raise _Unknown("Task-create command record is not durably completed")
        created = dict(create_record.get("result") or {})
        task_id = created.get("id")
        if not task_id:
            raise _Unknown("Durable task-create result carries no canonical ID")
        start_key = command_key(derive_operation_id(
            "runtime-message", family + "-task-start", project_id, agent_id, item_id))
        start_record = journal.operation(
            derive_operation_id("POST", f"tasks/{task_id}:start", start_key))
        if (start_record or {}).get("state") != "completed":
            raise _Unknown("Task-start command record is not durably completed; held")
        started = dict(start_record.get("result") or {})
        execution = dict(started.get("execution") or {})
        if not execution.get("id") or execution.get("run_id") != run_id \
                or not started.get("id") or started.get("revision") is None:
            raise _Unknown("Durable task-start result is not bound to this run")
        task_ref = {"task_id": started["id"], "execution_id": execution["id"],
                    "revision": started["revision"]}
        # Current canonical cross-check: a replaced execution/run is stale, never proof.
        current = self.client.request("GET", f"tasks/{task_id}")
        current_execution = dict((current or {}).get("execution") or {})
        if current_execution.get("id") != task_ref["execution_id"] \
                or current_execution.get("run_id") != run_id:
            raise _Unknown("Canonical task execution is stale or replaced; held")
        return task_ref

    def _startup_recovery(self):
        """start() hook: wedged-auxiliary recovery with current bindings, pre-dispatch.

        Runs exactly once per start, after the supervisor's canonical auxiliary
        runs and gateways exist (current receipts/identity/recovery gate) and
        before any consumer thread dispatches. Held records stay ``unknown``; a
        hard failure aborts the start before any thread runs (fail-closed).
        """
        self.startup_recovery_report = self.recover_wedged_auxiliaries()

    def start(self):
        status = self.supervisor.start()
        return {**status,
                "bindings": {"auxiliary_run_ids": list(
                                 getattr(self.supervisor, "auxiliary_run_ids", None) or []),
                             "slot_state_files": list(self.slot_state_files)},
                "recovery": (dict(self.startup_recovery_report)
                             if self.startup_recovery_report is not None else None)}

    def close(self):
        if self.closed:
            return
        report = self.supervisor.stop(reason="worker_shutdown")
        errors = []
        try:
            if not report["stopped"]:
                # Observe a bounded vendor cancellation; never post a synthetic verdict.
                for runtime in self.runtimes:
                    cancel = getattr(runtime, "cancel", None)
                    run_id = getattr(runtime, "state", {}).get("run_id")
                    if callable(cancel) and run_id:
                        try:
                            cancel({"id": run_id})
                        except Exception:
                            pass  # Unknown remains journaled; close the owned process.
            for runtime in self.runtimes:
                close = getattr(runtime, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception as error:
                        errors.append(type(error).__name__)
            if not report["stopped"]:
                report = self.supervisor.stop(timeout=30)
            if not report["stopped"]:
                raise UnknownOutcome("Auxiliary messaging did not stop before journal shutdown")
            if errors:
                raise UnknownOutcome("Auxiliary vendor cleanup could not be confirmed")
        finally:
            self.cleanup_report = {**report, "cleanup_errors": errors,
                                   "slots": self.slots,
                                   "recovery": dict(self.recovery_report)}
            self.closed = True
