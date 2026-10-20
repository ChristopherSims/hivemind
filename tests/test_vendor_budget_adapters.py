"""Production vendor budget adapters: probed capability honesty, fail-closed
preparation, governed-turn mechanics with documented SDK/RPC shapes, usage and
currency normalization, output-bound verification. Pure doubles; no vendor process,
no model calls, no network."""
import json
import queue
from types import SimpleNamespace

import pytest

from hivemind.recovery import UnknownOutcome
from hivemind.vendor_budget_adapters import (
    AdapterCapability,
    BudgetAdapterError,
    OutputCapViolation,
    _build_turn,
    _factory_turn,
    _omp_turn,
    capability_status,
    describe_adapters,
    factory_capability,
    factory_usage_report,
    omp_capability,
    omp_usage_report,
    prepare_factory_budgeted_turn,
    prepare_omp_budgeted_turn,
    verify_output_bound,
)
from hivemind.vendor_runtimes import FactoryRuntime, OhMyPiRuntime
from hivemind.worker_budget_governance import PreparedBudgetTurn
from test_vendor_runtimes import assignment, options


def enabled(tmp_path, **overrides):
    tmp_path.mkdir(parents=True, exist_ok=True)
    return {**options(tmp_path), "enabled": True, "allow_model_calls": True,
            "model": "vendor-model-1", **overrides}


SUPPORTED = AdapterCapability("test", True, (), ())


class Session:
    created = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.cwd, self.id = kwargs.get("cwd"), "vendor-session"
        self.prompts = []
        Session.created.append(self)

    async def open(self):
        pass

    async def close(self):
        pass

    async def interrupt(self):
        pass

    def stream(self, prompt, timeout):
        self.prompts.append(prompt)

        class Stream:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                self.result = SimpleNamespace(
                    interrupted=False, success=True, text="Local result",
                    usage=SimpleNamespace(input_tokens=11, output_tokens=3,
                                          cache_creation_tokens=2, cache_read_tokens=4,
                                          thinking_tokens=0, factory_credits=0.25))
                raise StopAsyncIteration
        return Stream()


SDK = SimpleNamespace(Session=Session, Runtime=lambda **kw: kw,
                      SessionConfig=lambda **kw: kw, Autonomy=SimpleNamespace(LOW="low"))


class OmpTransport:
    """Documented rpc-wire shapes: prompt/prompt_result/session_settled/get_state."""
    stats = None
    answer_prompt = True

    def __init__(self, argv, cwd, env):
        self.argv, self.messages, self.frames = argv, [], queue.Queue()

    def send(self, request):
        self.messages.append(request)
        kind = request["type"]
        if kind == "get_state":
            data = {"sessionId": "omp-session", "isStreaming": False, "isSettled": True}
        elif kind == "get_session_stats":
            data = self.stats
        else:
            data = {}
        self.frames.put({"type": "response", "id": request["id"], "success": True,
                         "data": data})
        if kind == "prompt" and type(self).answer_prompt:
            self.frames.put({"type": "prompt_result", "id": request["id"],
                             "status": "completed", "sessionSettled": True,
                             "agentInvoked": True})

    def receive(self, timeout):
        try:
            return self.frames.get(timeout=timeout)
        except queue.Empty:
            raise ConnectionError("Vendor transport closed") from None

    def close(self):
        pass


# -- capability probes ---------------------------------------------------------


def test_factory_capability_probes_installed_sdk_and_stays_closed():
    import droid_sdk
    capability = factory_capability(droid_sdk)
    assert capability.supported is False
    assert any("output-token limit" in reason for reason in capability.reasons)
    assert any("exact full model input" in reason for reason in capability.reasons)
    evidence = dict(item.split("=", 1) for item in capability.evidence)
    assert evidence["droid_sdk_version"] == "0.5.0"
    assert "max" not in evidence.get("stream_params", "")


def test_unverified_cap_like_parameters_never_count_as_enforcement():
    class FutureSDK(SDK.__class__):
        pass

    sdk = SimpleNamespace(
        __version__="9.9.9",
        Session=SimpleNamespace(stream=lambda self, prompt, max_output_tokens=None: None),
        SessionConfig=None, SessionSettings=None)
    capability = factory_capability(sdk)
    assert capability.supported is False
    assert any("max_output_tokens" in reason for reason in capability.reasons)


def test_omp_capability_documents_rpc_surface_and_fitter_floor():
    capability = omp_capability()
    assert capability.supported is False
    text = " ".join(capability.reasons)
    assert "message, images, streamingBehavior" in text
    assert "1024" in text
    assert "model.maxTokens" in text
    assert capability.status()["vendor"] == "oh-my-pi"


def test_capability_status_and_describe_contract():
    status = capability_status({"vendor": "oh-my-pi"})
    assert status["bounded_turn_supported"] is False
    report = describe_adapters()
    assert report["factory"]["bounded_turn_supported"] is False
    assert "PreparedBudgetTurn" in report["contract"]["prepare"]


# -- fail-closed preparation ----------------------------------------------------


def test_factory_prepare_refuses_before_any_sdk_resource_or_state(tmp_path):
    before = len(Session.created)
    runtime = FactoryRuntime(enabled(tmp_path), sdk=SDK)
    task, context = assignment()
    with pytest.raises(BudgetAdapterError) as failure:
        prepare_factory_budgeted_turn(runtime, task, context, output_max_tokens=16)
    assert failure.value.reason == "budget_capability_missing"
    assert failure.value.capability.supported is False
    assert len(Session.created) == before
    assert not runtime.path.exists()
    runtime.close()


def test_omp_prepare_refuses_without_spawning_a_transport(tmp_path):
    runtime = OhMyPiRuntime(enabled(tmp_path), transport_factory=OmpTransport)
    task, context = assignment()
    with pytest.raises(BudgetAdapterError) as failure:
        prepare_omp_budgeted_turn(runtime, task, context, output_max_tokens=16)
    assert failure.value.reason == "budget_capability_missing"
    assert runtime.transport is None
    assert not runtime.path.exists()


def test_configuration_revalidation_precedes_vendor_contact(tmp_path):
    runtime = FactoryRuntime(enabled(tmp_path), sdk=SDK)
    task, context = assignment()
    with pytest.raises(BudgetAdapterError) as failure:
        _build_turn(runtime, task, context, output_max_tokens="16",
                    capability=SUPPORTED, runner=_factory_turn)
    assert failure.value.reason == "output_reserve_invalid"
    unpinned = FactoryRuntime({**enabled(tmp_path / "b"), "model": None}, sdk=SDK)
    with pytest.raises(BudgetAdapterError) as failure:
        _build_turn(unpinned, task, context, output_max_tokens=16,
                    capability=SUPPORTED, runner=_factory_turn)
    assert failure.value.reason == "budget_model_unspecified"
    assert not unpinned.path.exists()  # refused before _prepare's durable intent


# -- governed turn assembly and invocation --------------------------------------


def test_factory_governed_turn_exact_prompt_usage_and_settlement(tmp_path):
    runtime = FactoryRuntime(enabled(tmp_path), sdk=SDK)
    task, context = assignment()
    turn = _build_turn(runtime, task, context, output_max_tokens=16,
                       capability=SUPPORTED, runner=_factory_turn)
    assert isinstance(turn, PreparedBudgetTurn)
    assert isinstance(turn.serialized_prompt, str) and task["id"] in turn.serialized_prompt
    assert turn.model_id == "vendor-model-1"
    assert turn.counter == runtime.context_tokenizer_id == "vendor-model-1"
    assert turn.output_max_tokens == 16 and turn.enforces_output_max is True
    checkpoints = []
    outcome = turn.invoke(lambda: checkpoints.append(1))
    assert checkpoints == [1]
    assert outcome["status"] == "completed"
    assert Session.created[-1].prompts == [turn.serialized_prompt]
    assert outcome["usage"]["currency"] == "factory_credits"
    assert outcome["usage"]["cost"] == 0.25
    assert outcome["usage"]["input_tokens"] == 11
    checkpoint = json.loads((runtime.path.parent / (runtime.path.name + ".checkpoint.json"))
                            .read_text())
    assert checkpoint["status"] == "completed" and checkpoint["quiescent"] is True
    runtime.close()


def test_output_cap_violation_marks_unknown_and_holds(tmp_path):
    class Overrun(Session):
        def stream(self, prompt, timeout):
            self.prompts.append(prompt)

            class Stream:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

                def __aiter__(self):
                    return self

                async def __anext__(self):
                    self.result = SimpleNamespace(
                        interrupted=False, success=True, text="Local result",
                        usage=SimpleNamespace(input_tokens=11, output_tokens=99,
                                              cache_creation_tokens=0, cache_read_tokens=0,
                                              thinking_tokens=0, factory_credits=0.25))
                    raise StopAsyncIteration
            return Stream()

    sdk = SimpleNamespace(**{**vars(SDK), "Session": Overrun})
    runtime = FactoryRuntime(enabled(tmp_path), sdk=sdk)
    task, context = assignment()
    turn = _build_turn(runtime, task, context, output_max_tokens=16,
                       capability=SUPPORTED, runner=_factory_turn)
    with pytest.raises(OutputCapViolation) as failure:
        turn.invoke(lambda: None)
    assert failure.value.reason == "output_bound_exceeded"
    assert failure.value.reported == 99 and failure.value.declared == 16
    assert json.loads(runtime.path.read_text())["outcome"] == "unknown"
    runtime.close()


def test_time_gate_checkpoint_runs_before_any_vendor_call(tmp_path):
    runtime = FactoryRuntime(enabled(tmp_path), sdk=SDK)
    task, context = assignment()
    turn = _build_turn(runtime, task, context, output_max_tokens=16,
                       capability=SUPPORTED, runner=_factory_turn)
    before = len(Session.created)

    def exhausted():
        raise RuntimeError("time_budget_exhausted")

    with pytest.raises(RuntimeError):
        turn.invoke(exhausted)
    assert len(Session.created) == before  # aggregate-time refusal spent nothing
    runtime.close()


def test_omp_governed_turn_usage_and_output_bound(tmp_path):
    class StatsTransport(OmpTransport):
        stats = {"tokens": {"input": 100, "output": 10, "reasoning": 4,
                            "cacheRead": 40, "cacheWrite": 3, "total": 157},
                 "cost": 0.00002, "toolCalls": 0, "toolResults": 0}

    runtime = OhMyPiRuntime(enabled(tmp_path, collect_usage=True,
                                    budget_cost_currency="USD"),
                            transport_factory=StatsTransport)
    task, context = assignment()
    turn = _build_turn(runtime, task, context, output_max_tokens=16,
                       capability=SUPPORTED, runner=_omp_turn)
    outcome = turn.invoke(lambda: None)
    assert outcome["status"] == "completed"
    assert outcome["usage"]["output_tokens"] == 14  # provider-billed reasoning counts
    assert outcome["usage"]["cost"] == 0.00002 and outcome["usage"]["currency"] == "USD"
    sent = [message["type"] for message in runtime.transport.messages]
    assert sent == ["get_state", "set_auto_retry", "prompt", "get_state", "get_session_stats"]
    runtime.close()


def test_omp_output_cap_violation_and_lost_response_hold(tmp_path):
    class Overrun(OmpTransport):
        stats = {"tokens": {"input": 100, "output": 20, "reasoning": 0,
                            "cacheRead": 0, "cacheWrite": 0, "total": 120}, "cost": 0.0}

    runtime = OhMyPiRuntime(enabled(tmp_path / "a", collect_usage=True),
                            transport_factory=Overrun)
    task, context = assignment()
    turn = _build_turn(runtime, task, context, output_max_tokens=16,
                       capability=SUPPORTED, runner=_omp_turn)
    with pytest.raises(OutputCapViolation):
        turn.invoke(lambda: None)
    assert json.loads(runtime.path.read_text())["outcome"] == "unknown"
    runtime.close()

    class Silent(OmpTransport):
        answer_prompt = False

    lost = OhMyPiRuntime(enabled(tmp_path / "b", timeout_seconds=2),
                         transport_factory=Silent)
    turn = _build_turn(lost, task, {**context, "execution_id": "execution-2"},
                       output_max_tokens=16, capability=SUPPORTED, runner=_omp_turn)
    with pytest.raises(ConnectionError):
        turn.invoke(lambda: None)
    # A lost response is unknown: never replayed, the reservation stays held.
    assert [m["type"] for m in lost.transport.messages].count("prompt") == 1
    assert json.loads(lost.path.read_text())["outcome"] == "unknown"
    lost.close()


def test_reprepare_after_prepared_turn_fails_closed_for_reconciliation(tmp_path):
    runtime = OhMyPiRuntime(enabled(tmp_path), transport_factory=OmpTransport)
    task, context = assignment()
    _build_turn(runtime, task, context, output_max_tokens=16,
                capability=SUPPORTED, runner=_omp_turn)
    with pytest.raises(UnknownOutcome):
        _build_turn(runtime, task, context, output_max_tokens=16,
                    capability=SUPPORTED, runner=_omp_turn)
    runtime.close()


# -- usage normalization ---------------------------------------------------------


def test_factory_usage_normalization_currency_cache_thinking_and_gaps():
    report = factory_usage_report({"input_tokens": 5, "output_tokens": 2,
                                   "cache_creation_tokens": 0, "cache_read_tokens": 9,
                                   "thinking_tokens": 7, "factory_credits": 1.5})
    assert report["source"] == "provider" and report["currency"] == "factory_credits"
    assert "cache_read=9" in report["vendor_overhead_note"]
    assert "thinking=7" in report["vendor_overhead_note"]
    missing = factory_usage_report({"input_tokens": 5})
    assert missing["output_tokens"] is None and missing["cost"] is None
    assert missing["currency"] is None  # incomplete and unpriced: the ledger holds
    assert factory_usage_report(None) is None
    with pytest.raises(BudgetAdapterError):
        factory_usage_report({"input_tokens": "5"})
    with pytest.raises(BudgetAdapterError):
        factory_usage_report({"factory_credits": -1})


def test_omp_usage_requires_explicit_currency_attestation():
    stats = {"tokens": {"input": 3, "output": 1, "reasoning": 2, "cacheRead": 0,
                        "cacheWrite": 0, "total": 6}, "cost": 0.01}
    unattested = omp_usage_report(stats)
    assert unattested["cost"] is None and unattested["currency"] is None
    assert unattested["output_tokens"] == 3
    attested = omp_usage_report(stats, cost_currency="EUR")
    assert attested["cost"] == 0.01 and attested["currency"] == "EUR"
    assert omp_usage_report({"tokens": {}})["input_tokens"] is None
    assert omp_usage_report(None) is None
    with pytest.raises(BudgetAdapterError):
        omp_usage_report("not-a-mapping")
    with pytest.raises(BudgetAdapterError):
        omp_usage_report({"cost": -0.1}, cost_currency="USD")


def test_verify_output_bound():
    verify_output_bound(None, 16, vendor="droid")
    verify_output_bound(16, 16, vendor="droid")
    with pytest.raises(OutputCapViolation):
        verify_output_bound(17, 16, vendor="droid")
