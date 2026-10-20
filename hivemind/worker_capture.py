"""Worker-bound continuous capture lifecycle and automatic safe-pause controls.

This module is additive and opt-in. It composes, without modifying, the existing
continuous watcher (:class:`hivemind.control_capture.ContinuousWatcher`), the
canonical capture spool (:class:`hivemind.sdk.Capture`), the capability contract
(:func:`hivemind.control_capture.runtime_capabilities`), and the durable
safe-pause controller (:class:`hivemind.control_capture.SafePauseController`).

Two independent pieces are provided:

* :class:`WorkerCaptureLifecycle` binds one agent's *main* run to a dedicated
  *observer* run whose capture spool receives the watcher's hash-only events.
  Startup creates the observer run once under a stable idempotency key and
  records it in fenced durable state, so restarts reuse it and never mint a
  duplicate. Shutdown is strictly ordered and fail-safe: a watcher thread that
  cannot be stopped keeps its dependent spool open (never closed under a live
  writer), the close report says ``shutdown_pending``, and the context-manager
  exit raises :class:`CleanupError` so the parent keeps the journal open and
  retries instead of unwinding past live threads.

* :class:`AutomaticControls` evaluates operator-configured observed-change /
  capture-gap / drift / conflict signals and drives :class:`SafePauseController`
  with a *fresh* run revision and a *current* coordinator epoch on every
  dispatch. Unsupported adapters are held, never applied. Automatic resume
  happens only under an explicit ``auto_resume`` policy, from the exact
  applied-pause quiescent checkpoint recorded for this run, and only when the
  tick carried an explicit, complete, non-reset signal set — absent or
  newly-reset signals never imply "problem cleared".

Signal honesty:

* Watcher ``file.changed`` events are *observed changes*, not drift. They feed
  the ``observed_changes`` signal only; ``drift_findings`` is populated solely
  by an explicit trusted signal provider (for example a reconciler pass that
  correlates watcher events with runtime observations). Nothing here infers
  unobserved drift from filesystem events.
* Signal accounting is durable and delivery-based: a cursor over the watcher's
  stable event ids (``<source>-<seq>``) is advanced from the union of the
  watcher outbox and the capture spool, so events drained by the watcher's own
  background thread are accounted exactly like events drained by
  :meth:`WorkerCaptureLifecycle.drain`. Pending signals survive restart and are
  acknowledged (:meth:`WorkerCaptureLifecycle.ack_signals`) only after the
  policy has durably recorded its decision (a persisted intent). A source or
  sequence discontinuity raises an explicit ``reset`` marker, never a silent
  under-count.
"""

import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from filelock import FileLock

from .control_capture import (APPLIED, HELD,
                              ContinuousWatcher, ControlPolicy, OperationDescriptor,
                              SafePauseController, WatcherPolicy, _state_key,
                              _strict_bool, _strict_float, _strict_int,
                              runtime_capabilities)
from .runtime_lifecycle import ScanLimits, secret_path
from .sdk import Capture
from .storage import canonical, digest

__all__ = [
    "WORKER_CAPTURE_DESCRIPTORS", "CleanupError",
    "WorkerCaptureConfig", "WorkerCaptureLifecycle",
    "AutomaticPolicy", "AutomaticControls", "TrustedSignalCollector",
    "capture_config_from_mapping", "automatic_policy_from_mapping",
    "validate_signal_sources",
]

MAX_PENDING_SIGNALS = 4096
MAX_OBSERVED_PATHS = 4096
MAX_PENDING_CONFLICTS = 256
_MAX_STRING = 512

# Canonical service rejection codes that name real conflicts. The worker relays
# these from actual permit/claim/checkpoint replies; they are never synthetic.
CONFLICT_CODES = {
    "revision_conflict": "revision_conflict",
    "stale_task_execution": "revision_conflict",
    "claim_conflict": "claim_conflict",
    "claim_lost": "claim_conflict",
    "permit_denied": "permit_conflict",
    "permit_conflict": "permit_conflict",
}

# Operation metadata: purpose, authority, side effects, retry safety, and limits.
# These descriptors never grant authority; the parent CLI binds trusted config,
# principals, and adapters against them.
WORKER_CAPTURE_DESCRIPTORS = {
    "worker_capture.start": OperationDescriptor(
        name="worker_capture.start",
        purpose="Bind the observer run, open the capture spool, and start the watcher thread.",
        input="WorkerCaptureConfig (trusted), main-run fence, optional journal",
        output="startup receipt with the observer run id (deduplicated on restart)",
        authority="main run's project/agent; observer run creation under a stable idempotency key",
        side_effects=("POST runs (idempotent by stable key)", "creates spool+watcher SQLite stores",
                      "starts flush and poll threads", "journal entry"),
        limits={"poll_interval_seconds": "0.1..60", "flush_interval": "0.1..60",
                "max_queue": "WatcherPolicy"},
        retry_safe=True),  # Restart reuses the recorded observer run; threads dedup; a
    # failed stage cleans up every resource started before it before raising.
    "worker_capture.drain": OperationDescriptor(
        name="worker_capture.drain",
        purpose="Transfer watcher outbox rows into the spool, flush the spool, account signals.",
        input="bound watcher and capture",
        output="transfer/flush counters",
        authority="observer run's capture spool",
        side_effects=("capture spool inserts with stable event IDs", "events:batch POST",
                      "durable signal cursor advance"),
        limits={"batch": 256},
        retry_safe=True),
    "worker_capture.signals": OperationDescriptor(
        name="worker_capture.signals",
        purpose="Read pending observed-change/gap signals without consuming them.",
        input="durable signal ledger (outbox + spool union over stable event ids)",
        output="signal counts plus an explicit reset marker on discontinuity",
        authority="local durable lifecycle state",
        side_effects=("durable signal cursor advance",),
        limits={"max_pending_signals": MAX_PENDING_SIGNALS},
        retry_safe=True),
    "worker_capture.ack_signals": OperationDescriptor(
        name="worker_capture.ack_signals",
        purpose="Durably acknowledge pending signals after a persisted policy decision.",
        input="durable signal ledger",
        output="none",
        authority="local durable lifecycle state",
        side_effects=("clears pending signals",),
        limits={},
        retry_safe=False),  # Ack only after the controller persisted its intent.
    "worker_capture.close": OperationDescriptor(
        name="worker_capture.close",
        purpose="Ordered shutdown: watcher stop+drain, spool flush+close, journal entry last.",
        input="live lifecycle resources",
        output="close report; shutdown_pending means live resources were retained",
        authority="local resources owned by this lifecycle",
        side_effects=("thread joins", "final spool flush", "journal entry when fully closed"),
        limits={"join_timeout_seconds": "watcher/capture defaults"},
        retry_safe=True),  # A failed step retains its resource for a retry.
    "controls.tick": OperationDescriptor(
        name="controls.tick",
        purpose="Evaluate observed signals and dispatch or hold a safe pause.",
        input="caller-supplied observed signals, current run revision, current coordinator epoch",
        output="tick receipt (clear | applied | held | disabled) plus the durable hold state",
        authority="coordinator lease + run revision, enforced by the service; supported adapters only",
        side_effects=("durable intent transition before dispatch", "POST controls (idempotent)",
                      "adapter control call"),
        limits={"max_intents": "SafePauseController"},
        retry_safe=True),
    "controls.resume": OperationDescriptor(
        name="controls.resume",
        purpose="Auto-resume only under explicit policy from the exact applied-pause checkpoint.",
        input="current run revision, recorded quiescent checkpoint, complete non-reset signals",
        output="resume receipt or denial",
        authority="checkpoint bound to this run's applied pause intent; consumed exactly once",
        side_effects=("durable consumption marker before dispatch",),
        limits={"max_consumed_checkpoints": "SafePauseController"},
        retry_safe=False),
}


class CleanupError(RuntimeError):
    """Structured shutdown failure: resources remain live and were retained.

    Raised by :meth:`WorkerCaptureLifecycle.__exit__` (and available for the
    parent to check via the ``close()`` report) when the watcher thread could
    not be stopped or the spool could not be flushed/closed. The parent must
    keep the journal open and retry ``close()``; the ``report`` attribute
    carries per-step errors.
    """

    def __init__(self, message, report):
        super().__init__(message)
        self.report = report
        self.errors = list(report.get("errors", []))


def _str_tuple(value, name, max_length=_MAX_STRING):
    """Strictly validate a collection of bounded strings; a bare string rejects
    rather than exploding into a tuple of characters."""
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        raise ValueError(f"{name} must be a list or tuple of strings")
    for item in value:
        if type(item) is not str or not 1 <= len(item) <= max_length:
            raise ValueError(f"{name} entries must be strings of 1..{max_length} characters")
    return tuple(value)


def _checked_state_path(value, name):
    """Resolve a config path and reject secret/ignored targets after resolve."""
    resolved = Path(value).resolve()
    if secret_path(resolved):
        raise ValueError(f"{name} resolves to a secret or ignored path")
    return str(resolved)


# --------------------------------------------------------------------------
# Configuration (strictly typed, default-off)
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class WorkerCaptureConfig:
    """Trusted operator configuration for the worker capture lifecycle.

    ``enabled`` defaults to ``False``: constructing a lifecycle performs no
    requests, opens no spool, and starts no threads. ``state_dir`` holds the
    fenced lifecycle state, the observer-run capture spool, and the watcher
    outbox; it is created operator-private (0o700) and state files are 0o600
    on POSIX. ``workspace_root`` and ``state_dir`` are resolved and rejected
    when they land on secret or ignored paths.
    """

    enabled: bool = False
    workspace_root: str | None = None
    state_dir: str | None = None
    poll_interval_seconds: float = 2.0
    flush_interval: float = 5.0
    limits: ScanLimits = field(default_factory=ScanLimits)
    exclude: tuple = ()
    max_queue: int = 256
    max_events_per_scan: int = 500

    def __post_init__(self):
        _strict_bool(self.enabled, "enabled")
        _strict_float(self.poll_interval_seconds, "poll_interval_seconds", 0.1, 60.0)
        _strict_float(self.flush_interval, "flush_interval", 0.1, 60.0)
        _strict_int(self.max_queue, "max_queue", 1, 10000)
        _strict_int(self.max_events_per_scan, "max_events_per_scan", 1, 10000)
        if not isinstance(self.limits, ScanLimits):
            raise ValueError("limits must be a ScanLimits instance")
        object.__setattr__(self, "exclude", _str_tuple(self.exclude, "exclude"))
        for name in ("workspace_root", "state_dir"):
            value = getattr(self, name)
            if value is not None:
                if type(value) is not str:
                    raise ValueError(f"{name} must be a string")
                object.__setattr__(self, name, _checked_state_path(value, name))
        if self.enabled and (not self.workspace_root or not self.state_dir):
            raise ValueError("workspace_root and state_dir are required when capture is enabled")


def capture_config_from_mapping(config):
    """Build a :class:`WorkerCaptureConfig` from a trusted mapping; unknown keys reject."""
    if not isinstance(config, dict):
        raise ValueError("Worker capture config must be an object")
    allowed = {"enabled", "workspace_root", "state_dir", "poll_interval_seconds",
               "flush_interval", "limits", "exclude", "max_queue", "max_events_per_scan"}
    unknown = set(config) - allowed
    if unknown:
        raise ValueError("Unknown worker capture config keys: " + ",".join(sorted(unknown)))
    limits = config.get("limits")
    if limits is not None and not isinstance(limits, ScanLimits):
        if not isinstance(limits, dict):
            raise ValueError("limits must be a ScanLimits or an object")
        limits = ScanLimits(**limits)
    return WorkerCaptureConfig(
        enabled=config.get("enabled", False),
        workspace_root=config.get("workspace_root"),
        state_dir=config.get("state_dir"),
        poll_interval_seconds=config.get("poll_interval_seconds", 2.0),
        flush_interval=config.get("flush_interval", 5.0),
        limits=limits or ScanLimits(),
        exclude=_str_tuple(config.get("exclude", ()), "exclude"),
        max_queue=config.get("max_queue", 256),
        max_events_per_scan=config.get("max_events_per_scan", 500))


@dataclass(frozen=True)
class AutomaticPolicy:
    """Trusted operator policy for automatic safe pause/resume.

    Every trigger is opt-in (``None`` disables it). ``observed_changes`` counts
    watcher-observed filesystem events; ``drift_findings`` is only ever fed by
    an explicit trusted signal provider, never renamed from watcher events.
    ``auto_resume`` is the only path that resumes without an explicit operator
    command, and it still requires the exact applied-pause quiescent checkpoint
    recorded for this run plus an explicit, complete, non-reset signal set.
    """

    enabled: bool = False
    observed_changes_threshold: int | None = None
    capture_gap_threshold: int | None = None
    drift_findings_threshold: int | None = None
    conflict_signals: tuple = ()
    auto_resume: bool = False

    def __post_init__(self):
        _strict_bool(self.enabled, "enabled")
        _strict_bool(self.auto_resume, "auto_resume")
        for name in ("observed_changes_threshold", "capture_gap_threshold",
                     "drift_findings_threshold"):
            value = getattr(self, name)
            if value is not None:
                _strict_int(value, name, 1, 1000000)
        object.__setattr__(self, "conflict_signals",
                           _str_tuple(self.conflict_signals, "conflict_signals", 128))
        if self.auto_resume and not self.enabled:
            raise ValueError("auto_resume requires an enabled policy")


def automatic_policy_from_mapping(config):
    """Build an :class:`AutomaticPolicy` from a trusted mapping; unknown keys reject."""
    if not isinstance(config, dict):
        raise ValueError("Automatic control policy must be an object")
    allowed = {"enabled", "observed_changes_threshold", "capture_gap_threshold",
               "drift_findings_threshold", "conflict_signals", "auto_resume"}
    unknown = set(config) - allowed
    if unknown:
        raise ValueError("Unknown automatic policy keys: " + ",".join(sorted(unknown)))
    return AutomaticPolicy(
        enabled=config.get("enabled", False),
        observed_changes_threshold=config.get("observed_changes_threshold"),
        capture_gap_threshold=config.get("capture_gap_threshold"),
        drift_findings_threshold=config.get("drift_findings_threshold"),
        conflict_signals=_str_tuple(config.get("conflict_signals", ()), "conflict_signals", 128),
        auto_resume=config.get("auto_resume", False))


# --------------------------------------------------------------------------
# Worker capture lifecycle
# --------------------------------------------------------------------------

class _AccountingWatcher(ContinuousWatcher):
    """ContinuousWatcher that reports each delivered batch to a callback.

    Delivery accounting happens here — synchronously after the spool commit and
    outbox acknowledgement, before any flush can remove the rows — so events
    drained by the watcher's own background thread are recorded exactly like
    events drained through the lifecycle's manual :meth:`drain`.
    """

    def __init__(self, *args, on_deliver=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_deliver = on_deliver

    def drain(self, batch=256):
        transferred = super().drain(batch)
        if transferred and self._on_deliver is not None:
            self._on_deliver(transferred)
        return transferred


class WorkerCaptureLifecycle:
    """Owns the observer run, its capture spool, and the watcher for one main run.

    Fencing and identity:

    * The lifecycle state file records ``bound_to`` (project/agent/main run) and
      the observer run id. Corrupt or foreign state refuses to load; it is never
      silently reset, so a restart can never accidentally re-baseline or bind the
      wrong spool.
    * The observer run is created once with the stable idempotency key
      ``worker-observer-<digest(project, agent, run)>`` and then re-read and
      fenced on every start: a restart dedups instead of minting a second
      observer.
    * Startup is stage-wise: the observer run binds first, then the spool opens,
      then the watcher is constructed and started. A failure at any stage closes
      every resource started before it before re-raising — a watcher constructor
      failure never leaks a capture flush thread or an open spool.
    * Shutdown is strictly ordered: watcher stop, watcher drain + signal
      accounting, spool flush, spool close, journal entry last. A watcher thread
      that fails to stop keeps its dependent spool *open* (closing it under a
      live writer would corrupt accounting); the report says
      ``shutdown_pending`` and the context-manager exit raises
      :class:`CleanupError`. The journal is only ever appended to, never closed;
      the final ``capture_stop`` entry is written only when every resource was
      released.
    """

    def __init__(self, client, project_id, agent_id, run_id, config, *,
                 journal=None, clock=time.time):
        if not isinstance(config, WorkerCaptureConfig):
            raise ValueError("config must be a WorkerCaptureConfig")
        self.client = client
        self.project_id, self.agent_id, self.run_id = project_id, agent_id, run_id
        self.config = config
        self.journal = journal
        self.clock = clock
        self._lock = threading.RLock()
        self._watcher = None
        self._capture = None
        self._observer = None
        self._closed = False
        if config.state_dir:
            self.state_dir = Path(config.state_dir)
            self.state_dir.mkdir(parents=True, exist_ok=True)
            if os.name != "nt":
                os.chmod(self.state_dir, 0o700)
            self.key = _state_key(f"{project_id}-{agent_id}-{run_id}")
            self.state_path = self.state_dir / f"{self.key}-worker-capture.json"
            self._file_lock = FileLock(str(self.state_path) + ".lock")
            self.state = self._load()
        else:
            self.state_dir = None
            self.state_path = None
            self._file_lock = None
            self.state = {"bound_to": None, "observer_run_id": None,
                          "signal_cursor": {"source": None, "seq": 0},
                          "pending_signals": [], "signal_reset_pending": False,
                          "signal_reset_count": 0, "observed_paths": []}

    # ------------------------------------------------------------------ state

    def _load(self):
        with self._file_lock:
            if self.state_path.exists():
                try:
                    state = json.loads(self.state_path.read_text(encoding="utf-8"))
                except (OSError, ValueError) as error:
                    raise ValueError("Worker capture state is unreadable; refusing to "
                                     "re-baseline without the observer binding") from error
                if not isinstance(state, dict):
                    raise ValueError("Worker capture state must be an object")
                expected = {"project_id": self.project_id, "agent_id": self.agent_id,
                            "run_id": self.run_id}
                bound = state.get("bound_to")
                if bound is not None and bound != expected:
                    raise ValueError("Worker capture state belongs to another project/agent/run")
            else:
                state = {}
            state.setdefault("bound_to", {"project_id": self.project_id,
                                          "agent_id": self.agent_id, "run_id": self.run_id})
            state.setdefault("observer_run_id", None)
            state.setdefault("starts", 0)
            state.setdefault("signal_cursor", {"source": None, "seq": 0})
            state.setdefault("pending_signals", [])
            state.setdefault("signal_reset_pending", False)
            state.setdefault("signal_reset_count", 0)
            state.setdefault("observed_paths", [])
            return state

    def _save(self):
        with self._file_lock:
            temporary = self.state_path.with_suffix(".tmp")
            temporary.write_text(canonical(self.state), encoding="utf-8")
            if os.name != "nt":
                temporary.chmod(0o600)
            os.replace(temporary, self.state_path)

    def _journal(self, entry):
        append = getattr(self.journal, "append", None) or getattr(self.journal, "record", None)
        if callable(append):
            append({**entry, "project_id": self.project_id, "agent_id": self.agent_id,
                    "run_id": self.run_id, "at": self.clock()})

    # ----------------------------------------------------------- observer run

    def observer_run(self, refresh=False):
        """Return the fenced observer run, creating it once under a stable key.

        The observer run belongs to the same project and agent as the main run;
        anything else refuses rather than capturing into a foreign run's spool.
        """
        if self._observer is not None and not refresh:
            return self._observer

        def fence(run):
            if (run.get("project_id") != self.project_id
                    or run.get("agent_id") != self.agent_id):
                raise ValueError("Observer run failed the project/agent fence")
            return run

        recorded = self.state.get("observer_run_id")
        if recorded:
            self._observer = fence(self.client.request("GET", f"runs/{recorded}"))
            return self._observer
        # Stable creation key: a crash after creation retries under the same
        # idempotency key, so a restart never mints a duplicate observer run.
        # Bounded to the service's 128-character key limit via a fence digest.
        key = "worker-observer-" + digest({"project_id": self.project_id,
                                           "agent_id": self.agent_id,
                                           "run_id": self.run_id})
        run = fence(self.client.request("POST", "runs",
                                        {"project_id": self.project_id,
                                         "agent_id": self.agent_id}, key=key))
        self.state["observer_run_id"] = run["id"]
        self._save()
        self._observer = run
        return run

    # -------------------------------------------------------------- lifecycle

    @property
    def running(self):
        return self._watcher is not None and self._watcher.running

    def start(self):
        """Explicit opt-in startup. Already-running starts dedup into a receipt.

        Stage-wise cleanup: any failure closes every resource started before it
        (a watcher construction or start failure closes the spool and stops its
        flush thread) and leaves the lifecycle startable for a retry.
        """
        if not self.config.enabled:
            raise RuntimeError("Worker capture is not enabled; set WorkerCaptureConfig(enabled=True)")
        with self._lock:
            if self._closed:
                raise RuntimeError("Worker capture lifecycle is closed")
            if self.running:
                return {"status": "already_running",
                        "observer_run_id": self.state["observer_run_id"]}
            self._journal({"kind": "capture_start_intent"})
            observer = self.observer_run()  # Fence refusal here owns no resources.
            capture, watcher = None, None
            try:
                capture = Capture(self.client, self.project_id, self.agent_id, observer["id"],
                                  self.state_dir / f"{self.key}-observer-spool.sqlite",
                                  flush_interval=self.config.flush_interval, start=True)
                watcher = _AccountingWatcher(
                    WatcherPolicy(enabled=True, workspace_root=self.config.workspace_root,
                                  limits=self.config.limits, exclude=self.config.exclude,
                                  poll_interval_seconds=self.config.poll_interval_seconds,
                                  max_queue=self.config.max_queue,
                                  max_events_per_scan=self.config.max_events_per_scan,
                                  state_dir=str(self.state_dir / "watcher")),
                    capture=capture, on_deliver=self._record_delivery)
                watcher.start()
            except BaseException:
                if capture is not None:
                    try:
                        capture.close()  # Stops the flush thread and closes the spool.
                    except Exception:  # noqa: BLE001 - the original error is authoritative
                        pass
                raise
            self._capture, self._watcher = capture, watcher
            self.state["starts"] = int(self.state.get("starts", 0)) + 1
            self._save()
            return {"status": "started", "observer_run_id": observer["id"],
                    "starts": self.state["starts"]}

    # --------------------------------------------------------- signal ledger

    def _mark_reset(self):
        """Declare a ledger discontinuity. Caller holds the lock."""
        self.state["signal_reset_pending"] = True
        self.state["signal_reset_count"] = int(self.state.get("signal_reset_count", 0)) + 1

    def _fold(self, event_id, suffix, path=None):
        """Fold one delivered event into the durable ledger. Caller holds the lock.

        The cursor advances only forward over the watcher's stable event ids
        (``<source>-<seq>``). A source change or a sequence jump raises the
        explicit ``reset`` marker instead of silently under-counting.
        """
        cursor = self.state["signal_cursor"]
        source, _, seq_text = str(event_id).rpartition("-")
        if not seq_text.isdigit():
            return False
        seq = int(seq_text)
        if cursor.get("source") is None:
            cursor["source"], cursor["seq"] = source, 0
        elif cursor["source"] != source:
            # A new watcher source means the previous ledger is incomplete.
            self._mark_reset()
            cursor["source"], cursor["seq"] = source, 0
        previous = int(cursor.get("seq", 0))
        if seq <= previous:
            return False
        if seq > previous + 1:
            # A delivery was flushed before accounting saw it: declare the
            # discontinuity instead of guessing the missed count.
            self._mark_reset()
        kind = "gap" if suffix == "capture.gap_detected" else "change"
        self.state["pending_signals"].append({"seq": seq, "kind": kind, "path": path})
        if kind == "change" and path:
            observed = self.state["observed_paths"]
            observed.append(path)
            if len(observed) > MAX_OBSERVED_PATHS:
                del observed[: len(observed) - MAX_OBSERVED_PATHS]
        cursor["seq"] = seq
        return True

    def _record_delivery(self, transferred):
        """Delivery callback for every drained batch, durable before any flush."""
        with self._lock:
            added = 0
            for event_id, suffix, payload in transferred:
                path = (payload or {}).get("data", {}).get("path")
                if self._fold(event_id, suffix, path=path if isinstance(path, str) else None):
                    added += 1
            self._bound_pending()
            if added or self.state["signal_reset_pending"]:
                self._save()

    def _bound_pending(self):
        pending = self.state["pending_signals"]
        if len(pending) > MAX_PENDING_SIGNALS:
            del pending[: len(pending) - MAX_PENDING_SIGNALS]
            self.state["signal_reset_pending"] = True  # Bounded, never silent.

    def account_signals(self):
        """Backstop scan: fold spool/outbox rows the delivery callback missed.

        The delivery callback is authoritative and durable at delivery time;
        this scan covers the crash window where rows reached the spool but the
        callback never ran (a crash means the flush never ran, so the rows are
        still present). If the watcher's high-water mark is ahead of the cursor
        with no candidates left, the delivery is unrecoverable for accounting
        and the ``reset`` marker declares it.
        """
        with self._lock:
            if self._watcher is None:
                return {"accounted": 0}
            candidates = {}
            for event_id, suffix, payload in self._watcher.pending(MAX_PENDING_SIGNALS):
                candidates[event_id] = (suffix, (payload or {}).get("data", {}).get("path"))
            prefix = self._watcher.source + "-"
            if self._capture is not None:
                for row_id, raw in self._capture.db.execute(
                        "SELECT id, payload FROM spool").fetchall():
                    if row_id.startswith(prefix) and row_id not in candidates:
                        try:
                            payload = json.loads(raw)
                            suffix = payload.get("type", "").replace(
                                "hivemind.context.", "", 1)
                        except ValueError:
                            continue
                        candidates[row_id] = (suffix, (payload.get("data") or {}).get("path"))
            added = 0
            for event_id, (suffix, path) in candidates.items():
                if self._fold(event_id, suffix,
                              path=path if isinstance(path, str) else None):
                    added += 1
            self._bound_pending()
            cursor = self.state["signal_cursor"]
            high_water = self._watcher.audit()["sequence"]
            if (cursor.get("source") == self._watcher.source
                    and high_water > int(cursor.get("seq", 0))):
                # Events existed but left both stores unaccounted: never silent.
                self._mark_reset()
            if added or self.state["signal_reset_pending"]:
                self._save()
            return {"accounted": added}

    def _signal_counts(self):
        counts = {"observed_changes": 0, "capture_gaps": 0}
        for entry in self.state["pending_signals"]:
            key = "capture_gaps" if entry["kind"] == "gap" else "observed_changes"
            counts[key] += 1
        if self.state.get("signal_reset_pending"):
            counts["reset"] = True
        return counts

    def pending_signals(self):
        """Read pending signals without consuming them.

        Returns ``{"observed_changes": n, "capture_gaps": n}`` plus
        ``"reset": True`` when a source/sequence discontinuity was declared.
        Nothing is cleared here: signals remain durable until
        :meth:`ack_signals` runs after a persisted policy decision.
        """
        with self._lock:
            self.account_signals()
            return self._signal_counts()

    def signal_snapshot(self):
        """A stable signal snapshot plus the exact ledger token it consumed.

        The token names the pending event sequences and the reset counter at
        snapshot time; :meth:`ack_signals` consumes exactly those, so a gap or
        change that arrives between the policy evaluation and the ack is never
        lost to the race.
        """
        with self._lock:
            self.account_signals()
            return {"signals": self._signal_counts(),
                    "ids": [entry["seq"] for entry in self.state["pending_signals"]],
                    "reset_count": int(self.state.get("signal_reset_count", 0))}

    def ack_signals(self, token=None):
        """Durably acknowledge pending signals after the policy recorded a decision.

        Call this only once :class:`AutomaticControls` has durably persisted an
        intent (its receipt is present in the tick outcome). With a ``token``
        from :meth:`signal_snapshot`, exactly the snapshotted sequences are
        consumed and the reset marker clears only when no new discontinuity was
        declared since the snapshot; without a token (operator/legacy use) the
        whole ledger clears. Acknowledging on a mere "clear" evaluation would
        drop below-threshold evidence across a crash, so below-threshold
        signals stay pending and accumulate.
        """
        with self._lock:
            if token is None:
                self.state["pending_signals"] = []
                self.state["signal_reset_pending"] = False
                self._save()
                return
            consumed = set(token.get("ids") or ())
            self.state["pending_signals"] = [
                entry for entry in self.state["pending_signals"]
                if entry["seq"] not in consumed]
            if token.get("reset_count") == int(self.state.get("signal_reset_count", 0)):
                self.state["signal_reset_pending"] = False
            self._save()

    def observed_paths(self):
        """Recently observed changed paths (bounded); evidence for drift exclusion."""
        with self._lock:
            return list(self.state.get("observed_paths", []))

    def drain(self):
        """Transfer watcher outbox rows into the spool, flush, and account signals."""
        with self._lock:
            if self._watcher is None or self._capture is None:
                raise RuntimeError("Worker capture is not started")
            transferred = self._watcher.drain()
            self._capture.flush()
            self.account_signals()
            return {"transferred": len(transferred), **self._spool_states()}

    def _spool_states(self):
        rows = self._capture.db.execute(
            "SELECT state, count(*) FROM spool GROUP BY state").fetchall()
        return {state: count for state, count in rows}

    def capture_health(self):
        """Health snapshot for the run heartbeat; counters only, never content."""
        with self._lock:
            if self._capture is None:
                return {"state": "unknown"}
            states = self._spool_states()
            quarantined = states.get("quarantined", 0)
            return {"state": "gaps" if quarantined else "healthy",
                    "quarantined_event_count": quarantined,
                    "outbox": self._watcher.audit()["outbox"] if self._watcher else 0}

    def audit(self):
        with self._lock:
            return {"enabled": self.config.enabled, "running": self.running,
                    "observer_run_id": self.state.get("observer_run_id"),
                    "starts": self.state.get("starts", 0),
                    "pending_signals": len(self.state.get("pending_signals", [])),
                    "signal_reset_pending": bool(self.state.get("signal_reset_pending")),
                    "watcher": self._watcher.audit() if self._watcher else None,
                    "spool": self._spool_states() if self._capture else None}

    def close(self):
        """Ordered shutdown: watcher -> spool -> journal. Failures are reported.

        A watcher thread that fails to stop keeps the dependent spool open —
        closing it under a live writer would corrupt delivery accounting. Only a
        fully released lifecycle writes the final journal entry and reports
        ``closed``. Anything else reports ``shutdown_pending`` and retains every
        live resource for a retry.
        """
        with self._lock:
            errors = []
            watcher, capture = self._watcher, self._capture
            watcher_alive = False
            if watcher is not None:
                try:
                    watcher.stop()  # 1. Stop the poll thread first.
                except (RuntimeError, OSError) as error:
                    errors.append(f"watcher_stop:{type(error).__name__}")
                watcher_alive = watcher.running
                if not watcher_alive:
                    try:
                        watcher.drain()  # 2. Drain retained rows into the spool.
                        self.account_signals()
                    except Exception as error:  # noqa: BLE001 - reported, never hidden
                        errors.append(f"watcher_drain:{type(error).__name__}")
                    try:
                        watcher.db.close()
                    except Exception:  # noqa: BLE001 - close failure retains no handle risk
                        pass
                    self._watcher = None
            if capture is not None:
                if watcher_alive:
                    # Never close the spool under a live writer thread.
                    errors.append("capture_retained:watcher_alive")
                else:
                    # 3. Flush first: a failed flush retains the open spool (and
                    # its pending rows) for a shutdown retry. ``Capture.close``
                    # always closes its DB, so it is only called once the final
                    # flush succeeded.
                    try:
                        capture.flush()
                    except Exception as error:  # noqa: BLE001 - spool retained for retry
                        errors.append(f"capture_flush:{type(error).__name__}")
                    else:
                        try:
                            capture.close()
                        except Exception as error:  # noqa: BLE001
                            errors.append(f"capture_close:{type(error).__name__}")
                        else:
                            self._capture = None
            closed = self._watcher is None and self._capture is None
            # 4. The journal entry is always last and always attempted; the
            # journal itself is the parent's and is never closed here.
            try:
                if closed:
                    self._journal({"kind": "capture_stop",
                                   "observer_run_id": self.state.get("observer_run_id")})
                else:
                    self._journal({"kind": "capture_stop_pending",
                                   "errors": list(errors),
                                   "observer_run_id": self.state.get("observer_run_id")})
            except Exception as error:  # noqa: BLE001 - journal faults never mask earlier errors
                errors.append(f"journal:{type(error).__name__}")
            if closed:
                self._closed = True
            return {"closed": closed, "shutdown_pending": not closed, "errors": errors}

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        report = self.close()
        if report["shutdown_pending"] and exc_type is None:
            # Do not let an ExitStack unwind past live threads: the parent keeps
            # the journal open and retries close().
            raise CleanupError("Worker capture shutdown pending; resources retained",
                               report)
        return False


# --------------------------------------------------------------------------
# Automatic safe-pause controls
# --------------------------------------------------------------------------

class AutomaticControls:
    """Policy-driven automatic pause/resume over a :class:`SafePauseController`.

    Authority inputs are refreshed on every action: the run revision is re-read
    from the service (never cached across ticks), and ``coordinator_epoch`` is a
    callable so the parent can re-acquire the project lease before each dispatch.
    ``control_client`` should be a *separate* coordinator identity; the pause
    intent is durable and stable-keyed by the controller, transport-unknown
    outcomes hold (never fail, never replay), and unsupported adapters are held
    without any canonical control being created.

    The hold exposed to the worker seam derives only from durable controller
    intents: an applied or unknown-outcome pause holds until an applied resume;
    an applied or held cancel holds permanently for this run. Heartbeat silence
    is never an input.
    """

    def __init__(self, client, project_id, agent_id, run_id, state_file, *,
                 adapter=None, policy=None, control_client=None,
                 coordinator_epoch=None, clock=time.time):
        self.policy = policy or AutomaticPolicy()
        if not isinstance(self.policy, AutomaticPolicy):
            raise ValueError("policy must be an AutomaticPolicy")
        if coordinator_epoch is not None and not callable(coordinator_epoch):
            raise ValueError("coordinator_epoch must be a callable returning the current epoch")
        self.coordinator_epoch = coordinator_epoch
        self.adapter = adapter
        self.controller = SafePauseController(
            client, project_id, agent_id, run_id, state_file, adapter=adapter,
            control_client=control_client, clock=clock,
            policy=ControlPolicy(enabled=self.policy.enabled))
        self.clock = clock
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ holds

    def hold(self):
        """Derive the current hold from durable controller intents only."""
        state, checkpoint_id, intent_key = "clear", None, None
        for record in self.controller.state["intents"].values():
            intent = record.get("intent", {})
            status = record.get("status")
            action = intent.get("action")
            if action == "pause" and status == APPLIED:
                state = "pause_applied"
                checkpoint_id = (record.get("evidence", {}).get("observation") or {}
                                 ).get("checkpoint_id")
                intent_key = intent.get("intent_key")
            elif action == "pause" and status in (HELD, "dispatching"):
                state, checkpoint_id, intent_key = "pause_held", None, intent.get("intent_key")
            elif action == "cancel" and status in (APPLIED, HELD, "dispatching"):
                state = "cancel_applied" if status == APPLIED else "cancel_held"
                checkpoint_id, intent_key = None, intent.get("intent_key")
            elif action == "resume" and status == APPLIED and state == "pause_applied":
                state, checkpoint_id = "clear", None
        return {"state": state, "checkpoint_id": checkpoint_id, "intent_key": intent_key}

    @property
    def held(self):
        """True while any hold is active; the worker must not issue model work."""
        return self.hold()["state"] != "clear"

    # ------------------------------------------------------------------ ticks

    def evaluate(self, signals):
        """Map observed signals to a pause trigger, or ``None``.

        ``signals`` is caller-supplied evidence: ``observed_changes`` (watcher
        events actually delivered), ``capture_gaps`` (explicit gap events),
        ``drift_findings`` (only from an explicit trusted provider, never
        renamed watcher events), and ``conflicts`` (service-reported conflict
        kinds). Nothing is inferred here; absent signals never trigger and never
        clear an existing hold.
        """
        if not self.policy.enabled:
            return None
        reasons = []
        for key, threshold in (
                ("observed_changes", self.policy.observed_changes_threshold),
                ("capture_gaps", self.policy.capture_gap_threshold),
                ("drift_findings", self.policy.drift_findings_threshold)):
            if threshold is not None and int(signals.get(key, 0)) >= threshold:
                reasons.append(f"{key}>={threshold}")
        for conflict in signals.get("conflicts", ()):
            if conflict in self.policy.conflict_signals:
                reasons.append(f"conflict:{conflict}")
        if not reasons:
            return None
        return {"action": "pause", "reason": ";".join(reasons)[:512]}

    def _signals_clear(self, signals):
        """True only for an explicit, complete, non-reset signal set below thresholds.

        Absent signals (``None``), a declared reset/discontinuity, or a missing
        value for any configured threshold never prove the problem cleared.
        """
        if not isinstance(signals, dict) or signals.get("reset"):
            return False
        required = []
        if self.policy.observed_changes_threshold is not None:
            required.append("observed_changes")
        if self.policy.capture_gap_threshold is not None:
            required.append("capture_gaps")
        if self.policy.drift_findings_threshold is not None:
            required.append("drift_findings")
        if self.policy.conflict_signals:
            required.append("conflicts")
        for key in required:
            value = signals.get(key)
            if key == "conflicts":
                if not isinstance(value, (list, tuple)):
                    return False
            elif type(value) is not int:
                return False
        return self.evaluate(signals) is None

    def _current_run(self):
        """Re-read the run for the current revision and terminal fence."""
        run = self.controller.client.request("GET", f"runs/{self.controller.run}")
        if run.get("agent_id") != self.controller.agent \
                or run.get("project_id") != self.controller.project:
            raise ValueError("Run identity changed; refusing to act on a foreign run")
        return run

    def _epoch(self):
        return self.coordinator_epoch() if self.coordinator_epoch is not None else None

    def tick(self, signals=None):
        """One bounded policy cycle: pause on trigger, auto-resume only per policy.

        ``signals=None`` means "no evidence this tick": it can never trigger a
        pause and can never clear one for auto-resume.
        """
        with self._lock:
            hold = self.hold()
            if not self.policy.enabled:
                return {"status": "disabled", "hold": hold["state"]}
            if hold["state"] == "pause_applied":
                if self.policy.auto_resume and hold["checkpoint_id"] \
                        and self._signals_clear(signals):
                    run = self._current_run()
                    if run.get("status") in ("completed", "failed", "cancelled"):
                        return {"status": "terminal", "hold": hold["state"]}
                    result = self.controller.resume(
                        run["control_revision"], hold["checkpoint_id"],
                        coordinator_epoch=self._epoch())
                    return {"status": "resume_" + result["status"],
                            "hold": self.hold()["state"], "receipt": result}
                return {"status": "held", "hold": hold["state"],
                        "checkpoint_id": hold["checkpoint_id"]}
            if hold["state"] != "clear":
                # A held unknown/cancel outcome never auto-resolves; the worker
                # stays stopped at the boundary until an explicit resolution.
                return {"status": "held", "hold": hold["state"]}
            trigger = self.evaluate(signals or {})
            if trigger is None:
                return {"status": "clear", "hold": "clear"}
            capabilities = runtime_capabilities(self.adapter)
            if "pause" not in capabilities:
                # Dispatch anyway: the controller records a durable held intent
                # without creating a canonical control, and the hold blocks work.
                receipt = self.controller.dispatch("pause", 0, trigger["reason"]
                                                   + ":unsupported_adapter_preflight")
                return {"status": receipt["status"], "hold": self.hold()["state"],
                        "receipt": receipt}
            run = self._current_run()
            if run.get("status") in ("completed", "failed", "cancelled"):
                return {"status": "terminal", "hold": "clear"}
            receipt = self.controller.dispatch("pause", run["control_revision"],
                                               trigger["reason"],
                                               coordinator_epoch=self._epoch())
            return {"status": receipt["status"], "hold": self.hold()["state"],
                    "receipt": receipt}

    def audit(self):
        return {"policy_enabled": self.policy.enabled,
                "auto_resume": self.policy.auto_resume,
                "hold": self.hold(),
                "controller": self.controller.audit()}


# --------------------------------------------------------------------------
# Trusted automatic signal collector
# --------------------------------------------------------------------------

#: Signal kinds each provider can supply, for ``validate_signal_sources``.
LIFECYCLE_SIGNALS = frozenset({"observed_changes", "capture_gaps"})


def validate_signal_sources(policy, *providers):
    """Fail wiring when an enabled policy declares triggers nobody can feed.

    ``providers`` are the installed signal sources: a
    :class:`WorkerCaptureLifecycle` (observed changes + capture gaps), a
    :class:`TrustedSignalCollector` (its ``provides`` set), or any callable
    provider (an explicit installed source, trusted to supply what it returns).
    A policy that names a drift threshold or conflict signals with no installed
    source raises rather than silently doing nothing.
    """
    if policy is None or not getattr(policy, "enabled", False):
        return frozenset()
    needs = set()
    if getattr(policy, "observed_changes_threshold", None) is not None:
        needs.add("observed_changes")
    if getattr(policy, "capture_gap_threshold", None) is not None:
        needs.add("capture_gaps")
    if getattr(policy, "drift_findings_threshold", None) is not None:
        needs.add("drift_findings")
    if getattr(policy, "conflict_signals", None):
        needs.add("conflicts")
    available = set()
    for provider in providers:
        if provider is None:
            continue
        if isinstance(provider, WorkerCaptureLifecycle):
            available |= LIFECYCLE_SIGNALS
        elif getattr(provider, "provides", None) is not None:
            available |= set(provider.provides)
        elif callable(provider):
            return frozenset()  # An explicit callable source is installed; trusted.
    missing = needs - available
    if missing:
        raise ValueError("Automatic control policy requires signal sources that are not "
                         "installed: " + ",".join(sorted(missing)))
    return frozenset(needs)


class TrustedSignalCollector:
    """Trusted signal provider: reconciler drift minus canonical observed events,
    plus named conflicts from actual canonical permit/claim/checkpoint replies.

    * Drift comes from a bounded :class:`hivemind.runtime_lifecycle.RuntimeReconciler`
      pass (hash-only, explicit baseline). A finding whose path was delivered to
      the observer spool as a watcher ``file.changed`` event is a *known tool
      change* — expected, never drift. Only the remaining findings count.
    * Conflicts are recorded via :meth:`record_conflict` (the worker relays real
      canonical rejection codes through ``CONFLICT_CODES``); nothing is
      fabricated from heartbeat silence or timeouts.
    * Snapshot/ack mirror the lifecycle ledger: :meth:`snapshot` returns
      ``(signals, token)`` and :meth:`ack` consumes exactly the snapshotted
      conflicts and ledger ids, so arrivals between evaluation and ack survive.

    Without a reconciler the collector does not provide ``drift_findings``;
    :func:`validate_signal_sources` refuses a drift-threshold policy against it.
    """

    def __init__(self, lifecycle, reconciler=None, *, max_conflicts=MAX_PENDING_CONFLICTS,
                 clock=time.time):
        if not isinstance(lifecycle, WorkerCaptureLifecycle):
            raise ValueError("lifecycle must be a WorkerCaptureLifecycle")
        if reconciler is not None and not callable(getattr(reconciler, "drift", None)):
            raise ValueError("reconciler must provide a bounded drift pass")
        _strict_int(max_conflicts, "max_conflicts", 1, 100000)
        self.lifecycle = lifecycle
        self.reconciler = reconciler
        self.max_conflicts = max_conflicts
        self.clock = clock
        self._lock = threading.RLock()
        self._conflicts = []  # [{"id", "name", "at"}]
        self._conflict_seq = 0
        self.provides = frozenset(
            {"observed_changes", "capture_gaps", "conflicts"}
            | ({"drift_findings"} if reconciler is not None else set()))

    # ------------------------------------------------------------- conflicts

    def record_conflict(self, name):
        """Record a named conflict from an actual canonical reply. Bounded."""
        if type(name) is not str or not 1 <= len(name) <= 128:
            raise ValueError("conflict name must be a string of 1..128 characters")
        with self._lock:
            if len(self._conflicts) >= self.max_conflicts:
                return None  # Bounded: the oldest evidence stands; never unbounded.
            self._conflict_seq += 1
            entry = {"id": self._conflict_seq, "name": name, "at": self.clock()}
            self._conflicts.append(entry)
            return entry["id"]

    # --------------------------------------------------------------- signals

    def _drift_count(self):
        """One bounded reconciler pass; watcher-observed paths are not drift."""
        findings, _snapshot = self.reconciler.drift(update_baseline=True)
        observed = set(self.lifecycle.observed_paths())
        return sum(1 for finding in findings if finding.path not in observed)

    def snapshot(self):
        """Return ``(signals, token)``: a stable view plus its consumption token."""
        with self._lock:
            ledger = self.lifecycle.signal_snapshot()
            signals = dict(ledger["signals"])
            if self.reconciler is not None:
                signals["drift_findings"] = self._drift_count()
            signals["conflicts"] = [entry["name"] for entry in self._conflicts]
            token = {"ledger": {"ids": ledger["ids"], "reset_count": ledger["reset_count"]},
                     "conflicts": [entry["id"] for entry in self._conflicts]}
            return signals, token

    def ack(self, token):
        """Consume exactly the snapshotted conflicts and ledger ids."""
        with self._lock:
            consumed = set((token or {}).get("conflicts") or ())
            self._conflicts = [entry for entry in self._conflicts
                               if entry["id"] not in consumed]
        self.lifecycle.ack_signals((token or {}).get("ledger"))

    def __call__(self):
        """Plain callable form: a snapshot without an ack token (never auto-acked)."""
        return self.snapshot()[0]
