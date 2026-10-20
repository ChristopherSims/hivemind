"""Assigned-run messaging concurrency: a dedicated auxiliary run handles an agent's
messages and allowed notifications while the primary run's task execution is in flight.

Why a separate run: the canonical service fences one task per run
(``tasks:{id}:start`` rejects ``run_already_assigned``) and message-handling tasks are
real canonical tasks. Handling mail under the *same* run while it is assigned would
either steal/replace its task or require pseudo task/run IDs — both forbidden here.
Instead this module creates (or reuses) a **real canonical auxiliary run** for the same
agent, obtained through ``POST /v1/runs`` under a deterministic idempotency key, and
binds one :class:`hivemind.runtime_messaging.VendorMessagingExecutor` per in-flight
slot to that run with:

- **scope-less canonical message tasks** (no file grants, no scope claims),
- a **dedicated private vendor runtime** per slot (``runtime_factory`` is invoked once
  per slot; a single shared runtime instance is only accepted when
  ``max_in_flight == 1``, so no vendor session ever executes two turns concurrently),
- the **same durable handler journal** as the primary worker (pass the worker's
  ``recovery.Journal``), so stable-ID dedup and the per-operation OS ``SessionLock``
  exclusion are shared across both loops: a redelivery handled by either loop is a
  recorded no-op in the other,
- **bounded per-slot dispatch queues**: one producer thread claims deliveries only
  when a slot's queue has room and the message is not already outstanding; each slot
  has its own consumer thread, its own canonical auxiliary run, its own private
  vendor runtime, and its own queue. ``max_in_flight`` may be up to
  ``MAX_IN_FLIGHT_LIMIT`` only when every slot is **genuinely independent**: a
  factory is invoked once per slot and a shared instance across slots is rejected at
  materialization; a prebuilt single runtime is only accepted for one slot, or as an
  explicit ``{slot_index: runtime}`` mapping whose values are validated to be
  distinct instances. A delivery's claim/renew/ack/fail always names the run of the
  slot that claimed it — never slot 0's run — and no vendor session is ever shared
  or executed concurrently.

Control fencing without stealing the primary's work:

- A queued/acknowledged/applying ``pause``/``steer`` control targeting the primary or
  auxiliary run, any primary/auxiliary run status outside ``running``/``idle``
  (including an *applied* pause of the auxiliary run), or a terminal primary/auxiliary
  run **blocks new claims immediately** and **gates effects**: the executor's
  ``effect_gate`` runs after the confirmed turn decision is journaled, immediately
  before the deterministic reply, and immediately before the canonical settle, raising
  a definite :class:`EffectsBlocked` while the fence is closed. Before any effect the
  gated record stays cleanly retryable (reopened ``pending``, never poisoned unknown);
  a refusal after the reply is an honest ``unknown`` for the reconciler.
- A pending ``cancel`` control, or a terminal/missing primary or auxiliary run, halts
  the supervisor permanently.

Lease discipline:

- Every claimed receipt (queued or in-flight) is tracked in a bounded outstanding map
  (unique by stable message ID, at most ``queue_capacity + 1`` entries) and renewed by
  the producer each tick while the fence is open. A consumer verifies the exact current
  lease (claiming run, delivery ID, unexpired) with a fresh read **before beginning a
  turn**, so stale queued work never reaches the runtime; the effect gate re-verifies
  it before any reply/settle. A definite renewal failure (4xx) drops the lease; a
  transient renewal error blocks effects until a renewal succeeds. While fenced off,
  outstanding leases are deliberately not renewed and lapse for redelivery.

Maintenance never blocks the primary loop and never holds a lock through a model call:
heartbeat/lease-renewal/control polling run on separate threads doing only short HTTP
calls; journal records are short SQLite transactions; vendor execution happens outside
every journal, boundary, or queue lock.

Everything is opt-in (``enabled=True``), every bound is validated, ``stop()`` is
bounded (in-flight turns finish or the report says they did not; queued receipts are
failed back to ``available`` so nothing is lost and no effect runs after shutdown), and
``UnknownOutcome`` is never acked — it is surfaced through ``on_unknown`` and
:meth:`unknown_operations` for explicit, evidence-driven reconciliation (the recovery
seam documented in ``docs/runtime-messaging.md``).

Idle-run behavior is unchanged: with no supervisor constructed, nothing here runs, and
the primary worker's own message polling is untouched unless it is constructed with
``messaging_delegate=supervisor``.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from queue import Full, Queue

from .agent_session import HandlerJournal
from .recovery import UnknownOutcome, command_key, derive_operation_id
from .runtime_messaging import (MAX_REPLY_CHARS, TASK_OPERATION_KIND, MessagingDisabled,
                                MessagingError, MessagingPolicy, RuntimeMessageGateway,
                                VendorMessagingExecutor)

CONTROL_BLOCK_ACTIONS = ("pause", "steer")
"""Control actions that block concurrent message work while pending. ``cancel`` halts."""

RUN_LIVE_STATUSES = ("running", "idle")
RUN_TERMINAL_STATUSES = ("completed", "failed", "cancelled")

QUEUE_CAPACITY_LIMIT = 256
MAX_SHUTDOWN_SECONDS = 300.0
MAX_IN_FLIGHT_LIMIT = 8
"""Upper bound on auxiliary messaging slots. Every slot is a real canonical run plus
a private runtime, so the bound also caps server-side run churn per worker."""


class EffectsBlocked(MessagingError):
    """Definite block raised by the effect gate: the delivery is failed, never acked,
    no reply/settle runs, and redelivery after the control clears resumes at the
    journaled effect phase."""


class AssignedRunMessagingSupervisor:
    """Concurrent, fenced message/notification handling beside an assigned primary run.

    Parameters of note:

    - ``primary_run_id``: the run whose task execution must not be disturbed. Controls
      targeting it (or the auxiliary run) block/halt this supervisor.
    - ``journal``: the worker's ``recovery.Journal``. Sharing it makes handler dedup
      and OS exclusion common to both loops. A distinct journal would double-apply
      effects on cross-loop redelivery and is rejected implicitly by policy: pass the
      same one.
    - ``runtime_factory``: lazily builds one private vendor runtime per slot (invoked
      on first use, so ``start()`` opens no session). Accepts a single callable
      (invoked once per slot, identity collisions rejected at materialization) or a
      sequence of exactly ``max_in_flight`` per-slot callables. A prebuilt ``runtime``
      is accepted as a single instance only for one slot, or as an explicit
      ``{slot_index: runtime}`` mapping validated to hold distinct instances, so no
      vendor session is ever shared or executed concurrently with the primary task
      runtime or another slot.
    - ``max_in_flight``: number of genuinely independent auxiliary slots (1 to
      ``MAX_IN_FLIGHT_LIMIT``). Each slot binds its own canonical run, queue,
      runtime, and lease lifecycle; slot 0's lease is never used for another slot.
    - ``push_signal``: optional ``threading.Event``-compatible wake (see
      ``hivemind.message_push``). With it, dispatch reconciles the canonical inbox at
      startup and on each signal instead of polling every tick; heartbeats, lease
      renewals, and control fencing remain timer-driven.
    - ``auxiliary_run_id``: reuse existing auxiliary runs — a single run ID (slot 0)
      or a sequence of up to ``max_in_flight`` IDs (e.g. after restart the
      deterministic creation keys replay to the same runs; passing IDs explicitly
      validates and reuses them directly).
    - ``on_unknown``: optional ``callback(kind, identifier, error)`` invoked whenever a
      delivery surfaces ``UnknownOutcome``; the receipt is left unacked either way.
    """

    def __init__(self, client, project_id, agent_id, *, primary_run_id, journal,
                 runtime=None, runtime_factory=None, enabled=False,
                 auxiliary_run_id=None, allowed_senders=(), allowed_notification_types=(),
                 allow_replies=False, max_reply_chars=MAX_REPLY_CHARS,
                 allow_tool_mutation=False, max_in_flight=1, queue_capacity=16,
                 poll_interval=5.0, lease_ttl_seconds=30, shutdown_timeout=30.0,
                 on_unknown=None, push_signal=None, max_reconcile_seconds=60.0,
                 before_dispatch=None):
        if (runtime is None) == (runtime_factory is None):
            raise ValueError("Provide exactly one of runtime or runtime_factory")
        max_in_flight = int(max_in_flight)
        if not 1 <= max_in_flight <= MAX_IN_FLIGHT_LIMIT:
            raise ValueError(
                f"max_in_flight must be between 1 and {MAX_IN_FLIGHT_LIMIT}: every "
                "slot is a real canonical run with a private runtime")
        # Runtime sources are resolved per slot. A single prebuilt instance is only
        # valid for one slot; for several slots pass an explicit mapping
        # {slot_index: runtime} whose instances must be provably distinct (no shared
        # mutable vendor session), or a factory (a single callable invoked once per
        # slot, or a sequence of per-slot callables) whose products are checked for
        # identity collisions when each slot materializes its runtime.
        self._slot_runtimes = None
        self._slot_factories = None
        if runtime is not None:
            if max_in_flight == 1 and not isinstance(runtime, Mapping):
                self._slot_runtimes = [runtime]
            else:
                if not isinstance(runtime, Mapping):
                    raise ValueError(
                        "A single prebuilt runtime cannot serve multiple slots; pass "
                        "an explicit {slot_index: runtime} mapping of independent "
                        "instances or use runtime_factory")
                if set(runtime.keys()) != set(range(max_in_flight)):
                    raise ValueError(
                        "Runtime mapping keys must be exactly 0..max_in_flight-1")
                values = [runtime[index] for index in range(max_in_flight)]
                if len({id(instance) for instance in values}) != max_in_flight:
                    raise ValueError(
                        "Slots never share a vendor session: mapped runtimes must be "
                        "distinct instances")
                self._slot_runtimes = values
        else:
            if isinstance(runtime_factory, (list, tuple)):
                if len(runtime_factory) != max_in_flight:
                    raise ValueError(
                        "A factory sequence must provide exactly one factory per slot")
                factories = list(runtime_factory)
                if not all(callable(factory) for factory in factories):
                    raise ValueError("Every per-slot factory must be callable")
            else:
                factories = [runtime_factory] * max_in_flight
            self._created_runtimes = []
            self._slot_factories = [self._recording_factory(factory)
                                    for factory in factories]
        if not 1 <= int(queue_capacity) <= QUEUE_CAPACITY_LIMIT:
            raise ValueError("queue_capacity out of bounds")
        if not 0.1 <= float(poll_interval) <= 600:
            raise ValueError("poll_interval must be between 0.1 and 600 seconds")
        if not 1 <= int(lease_ttl_seconds) <= 300:
            raise ValueError("lease_ttl_seconds must be between 1 and 300 seconds")
        if not 0 <= float(shutdown_timeout) <= MAX_SHUTDOWN_SECONDS:
            raise ValueError("shutdown_timeout out of bounds")
        if journal is None:
            raise ValueError("A durable journal shared with the primary worker is required")
        if not primary_run_id:
            raise ValueError("primary_run_id is required for control fencing")
        if auxiliary_run_id is None:
            auxiliary_run_ids = []
        elif isinstance(auxiliary_run_id, str):
            auxiliary_run_ids = [auxiliary_run_id]
        else:
            auxiliary_run_ids = list(auxiliary_run_id)
        if len(auxiliary_run_ids) > max_in_flight or not all(
                isinstance(run_id, str) and run_id for run_id in auxiliary_run_ids):
            raise ValueError("auxiliary_run_id must be a run ID or a sequence of up "
                             "to max_in_flight run IDs")
        if push_signal is not None and not all(
                hasattr(push_signal, attr) for attr in ("wait", "clear", "is_set")):
            raise ValueError("push_signal must be a threading.Event-compatible object")
        if not 1.0 <= float(max_reconcile_seconds) <= 3600.0:
            raise ValueError("max_reconcile_seconds must be between 1 and 3600 seconds")
        self.client = client
        self.project_id = project_id
        self.agent_id = agent_id
        self.primary_run_id = primary_run_id
        self.journal = journal
        self.enabled = enabled is True
        self.auxiliary_run_ids = auxiliary_run_ids
        self.auxiliary_run_id = auxiliary_run_ids[0] if auxiliary_run_ids else None
        # Optional push wake (e.g. hivemind.message_push.PushWakeSource.event): when
        # set, dispatch reconciles the inbox on startup and on each push signal, while
        # heartbeats, lease renewals, and control fencing stay on the timer tick. A
        # lost publication can never silence dispatch: a canonical reconcile runs at
        # least every ``max_reconcile_seconds`` regardless of push.
        self.push_signal = push_signal
        self.max_reconcile_seconds = float(max_reconcile_seconds)
        self._last_reconcile = 0.0  # monotonic clock of the last canonical reconcile
        self.policy = MessagingPolicy(
            enabled=True, allowed_senders=allowed_senders,
            allowed_notification_types=allowed_notification_types,
            allow_replies=allow_replies, max_reply_chars=max_reply_chars)
        self._runtime = runtime
        self._runtime_factory = runtime_factory
        self._allow_tool_mutation = allow_tool_mutation is True
        self.max_in_flight = int(max_in_flight)
        self.queue_capacity = int(queue_capacity)
        self.poll_interval = float(poll_interval)
        self.lease_ttl_seconds = int(lease_ttl_seconds)
        self.shutdown_timeout = float(shutdown_timeout)
        self.on_unknown = on_unknown if callable(on_unknown) else None
        # Startup prep seam: invoked by start() after the canonical slot runs and
        # gateways are bound but BEFORE any producer/consumer thread is launched,
        # so evidence-driven recovery never races dispatch of the same records.
        # A raising hook aborts the start before any thread runs (fail-closed).
        if before_dispatch is not None and not callable(before_dispatch):
            raise ValueError("before_dispatch must be callable")
        self.before_dispatch = before_dispatch

        self._handler_journal = HandlerJournal(journal, project_id, agent_id)
        # One bounded queue per slot; `self._queue` stays the slot-0 alias.
        self._queues = [Queue(maxsize=self.queue_capacity)
                        for _ in range(self.max_in_flight)]
        self._queue = self._queues[0]
        self._pending_notices = set()
        # Outstanding claimed receipts (queued + in-flight), keyed by stable message
        # ID: {"receipt", "run_id", "slot", "renewal_error"}. The run/slot that
        # claimed a delivery owns its renew/ack/fail forever — never another slot's
        # run. Bounded by slots * (queue_capacity + 1)
        # (claims require queue room), so memory stays bounded; the producer renews
        # every entry's lease each tick, and the consumer/gate verify the exact
        # current lease before any turn, reply, or settle. Short lock sections only.
        self._outstanding = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._started = False
        self._halt_reason = None
        self._blocked = None  # {"action", "control_id" | "run_status"} while blocked
        self._counts = {"handled": 0, "failed": 0, "unknown": 0, "notices": 0,
                        "dropped": 0, "blocked_turns": 0, "stale": 0}
        self._producer = None
        self._consumers = []
        self._gateways = []  # one gateway/executor per in-flight slot

    # ------------------------------------------------------------------ run identity
    def _run_key(self, index):
        return command_key(derive_operation_id(
            "auxiliary-messaging-run", self.project_id, self.agent_id,
            self.primary_run_id, str(index)))

    def _validate_run(self, run):
        if run.get("agent_id") != self.agent_id or run.get("project_id") != self.project_id:
            raise MessagingError("Auxiliary run belongs to another agent or project")
        if run.get("status") not in RUN_LIVE_STATUSES:
            raise MessagingError("Auxiliary run is not live")
        return run

    def _recording_factory(self, factory):
        """Per-slot factory wrapper: rejects a factory that hands two slots the same
        instance, so a mutable vendor session is never shared between slots."""

        def build():
            instance = factory()
            if any(instance is prior for prior in self._created_runtimes):
                raise MessagingError(
                    "runtime_factory returned a shared instance; slots never share a "
                    "vendor session")
            self._created_runtimes.append(instance)
            return instance

        return build

    def _validate_reused_run(self, run):
        """An explicitly reused auxiliary run must additionally have no active
        control: resuming over a queued/applying pause, steer, or cancel would
        silently override a deliberate operator fence."""
        run = self._validate_run(run)
        for control in self._controls():
            if control.get("target_run_id") != run["id"]:
                continue
            if control.get("state") not in ("queued", "acknowledged", "applying"):
                continue
            if control.get("expires_at", 0) <= time.time():
                continue
            raise MessagingError(
                "Reused auxiliary run has an active control; refusing to resume "
                "over it: " + str(control.get("action")))
        return run

    def _acquire_runs(self):
        """One real canonical auxiliary run per slot. IDs are always the
        server-returned records; the deterministic keys make restart creation
        replay to the same runs. Explicitly reused runs are validated against
        identity, liveness, and active controls; all slot runs must be distinct."""
        runs = [self._validate_reused_run(self.client.request(
            "GET", f"runs/{run_id}")) for run_id in self.auxiliary_run_ids]
        for index in range(len(runs), self.max_in_flight):
            created = self.client.request("POST", "runs", {
                "project_id": self.project_id, "agent_id": self.agent_id},
                key=self._run_key(index))
            runs.append(self._validate_run(created))
        if len({run["id"] for run in runs}) != len(runs):
            raise MessagingError("Auxiliary slots require distinct canonical runs")
        self.auxiliary_run_ids = [run["id"] for run in runs]
        self.auxiliary_run_id = self.auxiliary_run_ids[0]
        return runs

    # ------------------------------------------------------------------ lifecycle
    def start(self):
        """Create/reuse the auxiliary run(s), build executors, and start the threads."""
        if not self.enabled:
            raise MessagingDisabled(
                "Assigned-run messaging concurrency requires enabled=True")
        with self._lock:
            if self._started:
                raise MessagingError("Supervisor was already started")
            self._started = True
        runs = self._acquire_runs()
        for index, run in enumerate(runs):
            # Exactly one runtime source per slot: a validated distinct prebuilt
            # instance, or the slot's own factory (invoked lazily by the executor,
            # so start() opens no vendor session).
            slot = ({"runtime": self._slot_runtimes[index]}
                    if self._slot_runtimes is not None
                    else {"runtime_factory": self._slot_factories[index]})
            executor = VendorMessagingExecutor(
                self.client, self.project_id, self.agent_id, run["id"],
                **slot,
                handler_journal=self._handler_journal,
                allowed_senders=self.policy.allowed_senders,
                allowed_notification_types=self.policy.allowed_notification_types,
                allow_replies=self.policy.allow_replies,
                max_reply_chars=self.policy.max_reply_chars,
                allow_tool_mutation=self._allow_tool_mutation,
                effect_gate=self._effect_gate)
            self._gateways.append(RuntimeMessageGateway(
                self.client, self.project_id, self.agent_id, run["id"], adapter=executor,
                policy=self.policy, handler_journal=self._handler_journal))
        if self.before_dispatch is not None:
            self.before_dispatch()
        self._producer = threading.Thread(target=self._produce, daemon=True,
                                          name="hivemind-messaging-producer")
        self._producer.start()
        self._consumers = [threading.Thread(target=self._consume, args=(index,),
                                            daemon=True,
                                            name=f"hivemind-messaging-consumer-{index}")
                           for index in range(self.max_in_flight)]
        for consumer in self._consumers:
            consumer.start()
        return self.status()

    @property
    def stopped(self):
        return self._stop.is_set()

    def stop(self, reason=None, timeout=None):
        """Bounded cooperative shutdown. Queued receipts are failed back to
        ``available`` (no effects ran on them); an in-flight turn is not interrupted,
        but its reply/settle is blocked by the effect gate. The report is honest:
        ``stopped`` is False when a thread outlived the budget."""
        if reason:
            self._halt_reason = reason
        self._stop.set()
        budget = self.shutdown_timeout if timeout is None else max(0.0, float(timeout))
        deadline = time.time() + budget
        if self._producer is not None:
            self._producer.join(timeout=max(0.0, deadline - time.time()))
        for consumer in self._consumers:
            consumer.join(timeout=max(0.0, deadline - time.time()))
        self._drain_queues()
        alive = [t.name for t in [self._producer, *self._consumers]
                 if t is not None and t.is_alive()]
        return {"stopped": not alive, "alive_threads": alive,
                "halt_reason": self._halt_reason, **self._counter_snapshot()}

    def status(self):
        return {"enabled": self.enabled, "started": self._started,
                "stopped": self.stopped, "halt_reason": self._halt_reason,
                "primary_run_id": self.primary_run_id,
                "auxiliary_run_id": self.auxiliary_run_id,
                "auxiliary_run_ids": list(self.auxiliary_run_ids),
                "slots": self.max_in_flight,
                "push": self.push_signal is not None,
                "blocked": dict(self._blocked) if self._blocked else None,
                "queued": sum(queue.qsize() for queue in self._queues),
                **self._counter_snapshot()}

    def _counter_snapshot(self):
        with self._lock:
            return dict(self._counts)

    # ------------------------------------------------------------------ recovery seam
    def unknown_operations(self, limit=200):
        """Durable unknown handler operations, as evidence for explicit reconciliation.

        The recovery layer (e.g. a ``recovery_automation`` reconciler) should verify
        each record against canonical state and then call
        ``journal.resolve_unknown(operation_id, decision, reason, evidence)``; this
        supervisor never auto-resolves an uncertain effect.
        """
        return [op for op in self.journal.operations(state="unknown", limit=limit)
                if op.get("method") == "HANDLER"]

    def _note_unknown(self, kind, identifier, error):
        with self._lock:
            self._counts["unknown"] += 1
        if self.on_unknown is not None:
            try:
                self.on_unknown(kind, identifier, error)
            except Exception:
                pass  # A recovery callback never changes delivery semantics.

    # ------------------------------------------------------------------ control fencing
    def _controls(self):
        controls = self.client.request(
            "GET", f"agents/{self.agent_id}/controls?project_id={self.project_id}")
        return controls or []

    def _run_view(self, run_id):
        try:
            return self.client.request("GET", f"runs/{run_id}") or None
        except Exception:
            return None  # Unknown: treated as blocked/halted below, never as healthy.

    def _run_status(self, run_id):
        view = self._run_view(run_id)
        return view.get("status") if view else None

    def _refresh_fence(self):
        """Recompute the control fence. Sets ``_blocked``; a terminal primary or
        auxiliary run halts the supervisor. Returns the blocked mapping or None."""
        watched = {self.primary_run_id, *self._run_ids()}
        for control in self._controls():
            if control.get("target_run_id") not in watched:
                continue
            if control.get("state") not in ("queued", "acknowledged", "applying"):
                continue
            if control.get("expires_at", 0) <= time.time():
                continue
            action = control.get("action")
            if action == "cancel":
                self._halt_reason = "control:cancel"
                self._stop.set()
                self._blocked = {"action": "cancel", "control_id": control.get("id")}
                return self._blocked
            if action in CONTROL_BLOCK_ACTIONS:
                self._blocked = {"action": action, "control_id": control.get("id")}
                return self._blocked
        primary = self._run_status(self.primary_run_id)
        if primary in RUN_TERMINAL_STATUSES or primary is None:
            self._halt_reason = f"primary_run_{primary or 'unknown'}"
            self._stop.set()
            self._blocked = {"action": "halt", "run_status": primary}
            return self._blocked
        if primary not in RUN_LIVE_STATUSES:
            # paused/waiting_input/blocked: no new claims, effects gated.
            self._blocked = {"action": "pause", "run_status": primary}
            return self._blocked
        for run_id in self._run_ids():
            status = self._run_status(run_id)
            if status in RUN_TERMINAL_STATUSES or status is None:
                self._halt_reason = f"auxiliary_run_{status or 'unknown'}"
                self._stop.set()
                self._blocked = {"action": "halt", "run_status": status}
                return self._blocked
            if status not in RUN_LIVE_STATUSES:
                # An applied pause (or any non-live status) on the auxiliary run
                # blocks exactly like a primary pause.
                self._blocked = {"action": "pause", "run_status": status,
                                 "auxiliary_run_id": run_id}
                return self._blocked
        self._blocked = None
        return None

    def _run_ids(self):
        return ([gateway.run_id for gateway in self._gateways]
                or list(self.auxiliary_run_ids))

    # ------------------------------------------------------------------ lease tracking
    @staticmethod
    def _receipt_id(receipt):
        return receipt.get("message_id") or receipt.get("id")

    def _track(self, receipt, slot, run_id):
        with self._lock:
            self._outstanding[self._receipt_id(receipt)] = {
                "receipt": receipt, "run_id": run_id, "slot": slot,
                "renewal_error": False}

    def _untrack(self, message_id):
        with self._lock:
            self._outstanding.pop(message_id, None)

    def _outstanding_snapshot(self):
        with self._lock:
            return {mid: dict(entry) for mid, entry in self._outstanding.items()}

    def _renew_outstanding(self):
        """Renew every outstanding (queued or in-flight) lease. A definite 4xx means
        the lease is lost/expired: the entry is dropped so the consumer discards the
        stale receipt instead of starting a turn. Any other failure flags the entry;
        the effect gate refuses reply/settle until a renewal succeeds again."""
        for message_id, entry in self._outstanding_snapshot().items():
            try:
                self.client.renew_message_lease(
                    message_id, entry["run_id"], entry["receipt"]["delivery_id"],
                    ttl_seconds=self.lease_ttl_seconds)
            except Exception as error:
                status = getattr(getattr(error, "response", None), "status_code", None)
                with self._lock:
                    current = self._outstanding.get(message_id)
                    if current is None:
                        continue
                    if isinstance(status, int) and 400 <= status < 500:
                        del self._outstanding[message_id]  # lease provably lost
                    else:
                        current["renewal_error"] = True
            else:
                with self._lock:
                    current = self._outstanding.get(message_id)
                    if current is not None:
                        current["renewal_error"] = False

    def _lease_held(self, message_id, run_id=None):
        """Freshly verify the exact current lease for one outstanding message.

        Returns True only while the server still shows the tracked claiming run
        holding this exact delivery, unexpired. ``run_id`` additionally pins the
        lease to a specific slot's run (used by the recovery gate so one slot's
        lease can never authorize another slot's effect). Never raises.
        """
        with self._lock:
            entry = self._outstanding.get(message_id)
            entry = dict(entry) if entry else None
        if entry is None or entry["renewal_error"]:
            return False
        if run_id is not None and entry["run_id"] != run_id:
            return False
        try:
            message = self.client.request("GET", f"messages/{message_id}")
        except Exception:
            return False
        return (message.get("state") == "delivered"
                and message.get("claim_run_id") == entry["run_id"]
                and message.get("delivery_id") == entry["receipt"].get("delivery_id")
                and message.get("claim_until", 0) > time.time())

    def _effect_gate(self, item, contract):
        """Executor hook: runs after the journaled turn decision, and again immediately
        before the deterministic reply and before the canonical settle.

        Raises :class:`EffectsBlocked` (definite) on shutdown, a closed control fence,
        or a lost/expired/unrenewed lease, so no effect runs while the delivery is not
        provably owned; redelivery after the fence clears dedups task+turn and resumes
        at the effect phase without a new model call. Notifications carry no lease and
        are fenced by stop/control only.
        """
        if self.stopped:
            with self._lock:
                self._counts["blocked_turns"] += 1
            raise EffectsBlocked("Supervisor is stopped; message effects are blocked")
        if self._blocked is not None or self._refresh_fence() is not None:
            with self._lock:
                self._counts["blocked_turns"] += 1
            raise EffectsBlocked("Control fence blocks message effects: "
                                 + str((self._blocked or {}).get("action")))
        if "from" in item and "to" in item:  # a message; notifications hold no lease
            if not self._lease_held(item["id"]):
                with self._lock:
                    self._counts["blocked_turns"] += 1
                raise EffectsBlocked("Message lease lost, expired, or unrenewed; "
                                     "effects are blocked")

    # ------------------------------------------------------------------ recovery gate
    def authorize_recovery_effect(self, phase, context):
        """Public authorization for replaying one journaled message effect (used by
        ``recovery_automation.Reconciler.recover_message_effect``).

        Freshly verifies, at call time: the supervisor is not stopped, no control
        fence is closed, ``context['run_id']`` is one of this supervisor's own
        auxiliary slot runs (a foreign or primary run is refused), that run is live
        (running/idle) and still owns ``context['task_id']`` as its current task,
        and the task's current execution matches ``context['execution_id']`` under
        the same run. The ``context['item_id']`` must hold the exact unexpired
        delivery lease of *that same slot's run*, without renewal errors. When
        ``context['operation_id']`` is given and a journal record exists, its recorded
        identity must match this project/agent/run. Returns True; raises
        :class:`MessagingError` (definite) otherwise. These are fresh client-side
        checks, not an atomic server authorization: the canonical checkpoint fencing
        remains the final arbiter (see docs/runtime-messaging.md).
        """
        if not isinstance(context, Mapping):
            raise MessagingError("Recovery context must be a mapping")
        run_id = context.get("run_id")
        task_id = context.get("task_id")
        execution_id = context.get("execution_id")
        if not run_id or not task_id or not execution_id:
            raise MessagingError("Recovery context requires run_id/task_id/execution_id")
        if run_id not in self._run_ids():
            raise MessagingError("Recovery effect belongs to a foreign run")
        if self.stopped:
            raise EffectsBlocked("Supervisor is stopped; recovery effects are blocked")
        if self._blocked is not None or self._refresh_fence() is not None:
            raise EffectsBlocked("Control fence blocks recovery effects: "
                                 + str((self._blocked or {}).get("action")))
        run = self._run_view(run_id)
        if not run or run.get("status") not in RUN_LIVE_STATUSES:
            raise MessagingError("Auxiliary run is not live")
        if run.get("task_id") != task_id:
            raise MessagingError("Task is not the auxiliary run's current task")
        try:
            task = self.client.request("GET", f"tasks/{task_id}")
        except Exception as error:
            raise MessagingError("Recovery task is unreadable") from error
        execution = task.get("execution") or {}
        if execution.get("id") != execution_id or execution.get("run_id") != run_id:
            raise MessagingError("Task execution is stale or foreign")
        item_id = context.get("item_id")
        if not item_id or not self._lease_held(item_id, run_id=run_id):
            raise EffectsBlocked("Recovery requires the exact current message "
                                 "delivery lease held by that same slot's run")
        operation_id = context.get("operation_id")
        if operation_id:
            record = self.journal.operation(operation_id)
            if record is not None:
                body = record.get("body") or {}
                if (body.get("run_id") not in (None, run_id)
                        or body.get("agent_id") not in (None, self.agent_id)
                        or body.get("project_id") not in (None, self.project_id)):
                    raise MessagingError("Recovery operation belongs to another identity")
        return True

    # ------------------------------------------------------------------ producer
    def _produce(self):
        # Startup (and any reconnect/restart) always reconciles the canonical inbox
        # first; with a push signal configured, later dispatches wake on push instead
        # of polling the inbox, while heartbeats, lease renewals, and control fencing
        # stay on the timer tick.
        reconcile = True
        while not self._stop.is_set():
            try:
                self._heartbeat()
                if self._refresh_fence() is not None:
                    # While fenced off, outstanding leases are deliberately not renewed:
                    # they lapse and redeliver after the fence clears. Reconcile again
                    # once the fence reopens so nothing signaled mid-fence is missed.
                    reconcile = True
                    self._wait()
                    continue
                self._renew_outstanding()
                # Independent of push: a bounded periodic canonical reconcile means
                # a lost push publication degrades to slow polling, never silence.
                if reconcile or (time.monotonic() - self._last_reconcile
                                 >= self.max_reconcile_seconds):
                    reconcile = True
                    self._poll_messages()
                    self._poll_notifications()
                    self._last_reconcile = time.monotonic()
            except Exception:
                # A polling/transport failure must not kill the loop or ack anything;
                # the next tick retries from canonical state.
                reconcile = True
            signaled = self._wait()
            reconcile = reconcile or signaled

    def _wait(self):
        """One timer tick. Returns True when the next tick should reconcile the inbox:
        always without a push signal (pure polling, unchanged); with a push signal,
        only when the signal fired during the wait."""
        if self.push_signal is None:
            self._stop.wait(min(self.poll_interval, 30.0))
            return True
        deadline = time.time() + min(self.poll_interval, 30.0)
        while not self._stop.is_set():
            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            if self.push_signal.wait(min(0.2, remaining)):
                self.push_signal.clear()
                return True
        return False

    def _heartbeat(self):
        for run_id in self._run_ids():
            try:
                self.client.request("POST", f"runs/{run_id}/heartbeat",
                                    {"capture_health": {"state": "unknown"}})
            except Exception:
                pass  # The fence refresh independently evaluates run liveness.

    def _slot_for_dispatch(self):
        """Pick the least-loaded slot whose queue has room, so independent slots
        actually run concurrently instead of stacking behind slot 0's turn."""
        with self._lock:
            load = {index: 0 for index in range(len(self._queues))}
            for entry in self._outstanding.values():
                load[entry["slot"]] += 1
        candidates = [(load[index], index)
                      for index, queue in enumerate(self._queues)
                      if not queue.full()]
        return min(candidates)[1] if candidates else None

    def _poll_messages(self):
        inbox = self.client.message_inbox(self.agent_id, self.project_id) or []
        outstanding = self._outstanding_snapshot()
        for message in inbox:
            if self._stop.is_set() or self._blocked is not None:
                return
            if message["id"] in outstanding:
                continue  # Already claimed and queued/in-flight: never double-claim.
            slot = self._slot_for_dispatch()
            if slot is None:
                return  # Claim only with queue room: a claimed receipt must be worked.
            # The claim is bound to THIS slot's run for its whole lifecycle.
            run_id = self._gateway_for(slot).run_id
            try:
                receipt = self.client.claim_message(message["id"], run_id)
            except Exception:
                continue  # 409 lease race or transient error: skip; later polls retry.
            self._track(receipt, slot, run_id)
            try:
                self._queues[slot].put_nowait(("message", receipt))
            except Full:
                self._fail_receipt(receipt, reason="queue_full")

    def _gateway_for(self, slot=0):
        return self._gateways[slot]

    def _poll_notifications(self):
        if not self.policy.allowed_notification_types:
            return
        notices = self.client.notifications(self.agent_id, self.project_id) or []
        for notice in notices:
            if self._stop.is_set() or self._blocked is not None:
                return
            if notice["id"] in self._pending_notices:
                continue
            slot = self._slot_for_dispatch()
            if slot is None:
                return
            try:
                self._queues[slot].put_nowait(("notification", notice))
            except Full:
                return
            self._pending_notices.add(notice["id"])

    # ------------------------------------------------------------------ consumer
    def _consume(self, index):
        gateway = self._gateways[index]
        queue = self._queues[index]
        while True:
            try:
                item = queue.get(timeout=0.2)
            except Exception:
                if self._stop.is_set():
                    return
                continue
            kind, payload = item
            try:
                if self._stop.is_set():
                    self._drop(item, reason="shutdown")
                elif kind == "message":
                    self._deliver_message(gateway, payload)
                else:
                    self._deliver_notification(gateway, payload)
            finally:
                queue.task_done()

    def _drop(self, item, reason):
        kind, payload = item
        with self._lock:
            self._counts["dropped"] += 1
        if kind == "message":
            # No effects ran on this receipt: failing is definite and safe.
            self._fail_receipt(payload, reason=reason)
        else:
            self._pending_notices.discard(payload["id"])

    def _drain_queues(self):
        for queue in self._queues:
            while True:
                try:
                    item = queue.get_nowait()
                except Exception:
                    break
                self._drop(item, reason="shutdown")
                queue.task_done()

    _FAIL_REASONS = {"handler_failed", "interrupted", "temporary_unavailable"}

    def _fail_receipt(self, receipt, reason="handler_failed"):
        # The canonical failure surface accepts a closed reason enum; map internal
        # causes onto it (details stay in the local journal, not on the wire).
        mapped = {"shutdown": "interrupted", "queue_full": "temporary_unavailable",
                  "control_pending": "temporary_unavailable",
                  "run_busy": "temporary_unavailable"}.get(reason, reason)
        if mapped not in self._FAIL_REASONS:
            mapped = "handler_failed"
        message_id = self._receipt_id(receipt)
        self._untrack(message_id)
        try:
            self.client.fail_message(message_id, self._claim_run(receipt),
                                     receipt.get("delivery_id"), reason=mapped)
        except Exception:
            pass  # The lease expires server-side; never escalate a cleanup failure.
        with self._lock:
            self._counts["failed"] += 1

    def _claim_run(self, receipt):
        # Renew/ack/fail must name the exact slot run that claimed the delivery.
        # The tracked outstanding entry is authoritative; the receipt's own fields
        # are the fallback, and slot 0 is only the last resort for receipts this
        # supervisor never tracked (defensive; nothing is acked from here).
        with self._lock:
            entry = self._outstanding.get(self._receipt_id(receipt))
        if entry is not None:
            return entry["run_id"]
        return (receipt.get("run_id") or receipt.get("claim_run_id")
                or self._gateway_for(0).run_id)

    def _slot_busy_for_other(self, gateway, receipt):
        """True when this slot's run still owns a DIFFERENT message's handling task.

        A gated/blocked delivery leaves its canonical task running under the auxiliary
        run, and the service fences one task per run (``run_already_assigned``), so a
        different message must be failed back (definite, before any effect) and
        redelivered once the slot settles. The busy task being THIS message's own
        journaled task is the gated-resume case and proceeds.
        """
        run = self._run_view(gateway.run_id)
        if not run or not run.get("task_id"):
            return False
        message_id = receipt.get("message_id") or receipt.get("id")
        operation_id = derive_operation_id(
            "handler", TASK_OPERATION_KIND,
            f"{self.project_id}:{self.agent_id}:{message_id}")
        record = self.journal.operation(operation_id)
        return not (record is not None and record["state"] == "completed"
                    and (record.get("result") or {}).get("task_id") == run["task_id"])

    def _deliver_message(self, gateway, receipt):
        """One message: verified lease, maintained turn, then ack / definite fail /
        no-ack-on-unknown."""
        message_id = self._receipt_id(receipt)
        if self._blocked is not None:
            self._fail_receipt(receipt, reason="control_pending")
            return
        if self._slot_busy_for_other(gateway, receipt):
            self._fail_receipt(receipt, reason="run_busy")
            return
        if not self._lease_held(message_id):
            # Stale queued work: the lease expired, was lost, or a renewal failed.
            # Never begin a turn (no new model call); drop without fail/ack — the
            # canonical redelivery path owns whatever happens next.
            self._untrack(message_id)
            with self._lock:
                self._counts["stale"] += 1
            return
        errors = []
        maintenance_stop = threading.Event()

        def maintain():
            # Short HTTP calls only (heartbeat, fence refresh); no journal/queue/
            # boundary lock is held here and nothing touches the vendor session.
            # Lease renewal is centralized in the producer's `_renew_outstanding`.
            while not maintenance_stop.wait(min(5.0, max(0.1, self.poll_interval))):
                try:
                    self._heartbeat()
                    self._refresh_fence()
                except Exception as error:
                    errors.append(type(error).__name__)
                    return

        thread = threading.Thread(target=maintain, daemon=True,
                                  name="hivemind-messaging-maintenance")
        thread.start()
        try:
            gateway.handle_message(receipt)
        except UnknownOutcome as error:
            self._note_unknown("message", message_id, error)
            return  # No ack and no fail while the outcome is uncertain.
        except MessagingError:
            self._fail_receipt(receipt)
            return
        except Exception as error:
            self._note_unknown("message", message_id, error)
            return
        finally:
            maintenance_stop.set()
            thread.join(timeout=20)
            # Handled, failed, or unknown: stop renewing. An acked message is gone; a
            # failed one redelivers on a fresh claim; an unknown one must expire so
            # redelivery stays blocked by the durable journal record.
            self._untrack(message_id)
        if errors or thread.is_alive():
            self._note_unknown("message", message_id, "maintenance_unknown")
            return  # Lease state is uncertain: leave the receipt to expire.
        try:
            self.client.ack_message(message_id,
                                    self._claim_run(receipt), receipt["delivery_id"])
        except Exception as error:
            self._note_unknown("message", message_id, error)
            return
        with self._lock:
            self._counts["handled"] += 1

    def _deliver_notification(self, gateway, notice):
        if self._blocked is not None:
            # Fenced off: leave the notice unacked and let a later poll re-enqueue it.
            self._pending_notices.discard(notice["id"])
            with self._lock:
                self._counts["dropped"] += 1
            return
        try:
            gateway.review_notification(notice)
        except UnknownOutcome as error:
            self._note_unknown("notification", notice["id"], error)
            return  # No ack while the outcome is uncertain.
        except MessagingError:
            return  # Definite refusal: stays available for a later poll, never acked.
        except Exception as error:
            self._note_unknown("notification", notice["id"], error)
            return
        try:
            self.client.ack_notification(notice["id"], key=f"ack-{notice['id']}")
        except Exception as error:
            self._note_unknown("notification", notice["id"], error)
            return
        self._pending_notices.discard(notice["id"])
        with self._lock:
            self._counts["notices"] += 1
