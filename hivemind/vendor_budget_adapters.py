"""Production budget-adapter helpers for the stock vendor runtimes.

These helpers are the trusted seam behind ``runtime.prepare_budgeted_turn`` for
``FactoryRuntime`` and ``OhMyPiRuntime`` (wired by the integration layer; the stock
runtime classes themselves are unchanged here). They serve
``worker_budget_governance.WorkerBudgetGovernance``, which requires a
``PreparedBudgetTurn`` with an exact serialized prompt, the budget's exact model id
and named local counter, and a turn that genuinely enforces the declared output
maximum.

Honest capability status (probed, never declared):

- Factory (``droid exec`` CLI 0.233.0 / ``droid_sdk`` 0.5.0): **unsupported**. No
  output-token limit exists on any documented surface -- ``Session.stream`` accepts
  only ``prompt/images/files/output/timeout/include_partial_messages``,
  ``SessionConfig``/``SessionSettings`` carry model and tool configuration only, the
  ``droid exec`` CLI exposes no token cap flag, and the JSON-RPC client has no such
  method. The prompt string is additionally not the exact full model input: the
  vendor prepends its system prompt, tool schemas and hook frames, so a local count
  of it would undercount input tokens. The adapter fails closed with explicit
  reasons instead of pretending a bounded turn exists.
- Oh My Pi (``omp`` 18.8.9, ``--mode rpc``): **unsupported** through the wired RPC.
  The documented ``prompt`` params are ``{message, images, streamingBehavior}``
  (rpc-wire schema); no per-turn output cap command exists. The internal
  ``runEphemeralTurn`` ``maxTokens`` used by ``/btw`` is not RPC-exposed, and the
  catalog ``model.maxTokens`` is policy metadata: ``fitOutputTokensToContextWindow``
  can only lower it but its 1024-token floor can raise a smaller cap near a full
  context window, ``ensureMaxTokensForThinking`` raises it for budget-thinking
  models, and ``omitMaxOutputTokens`` compat policies can drop it silently. Hard
  enforcement is never inferred from such metadata.

What IS implemented and exercised:

- Configuration-source revalidation and exact governed-turn assembly
  (``_build_turn``) that would produce the ``PreparedBudgetTurn`` the moment a
  probed vendor surface genuinely supports the contract.
- Turn runners (``_factory_turn`` / ``_omp_turn``) driving the runtimes through
  their own documented SDK/RPC shapes, with lost-response/unknown-outcome holds,
  post-turn output-bound verification, and normalized usage for settlement.
- Usage normalizers that never fabricate: missing tokens stay missing (the ledger
  holds the reserved worst case), factory credits are reported as
  ``factory_credits`` (a governed unit, never converted to dollars), Oh My Pi cost
  is only attached when the operator explicitly attests its governed currency, and
  cache/thinking/hidden-framing uncertainty is recorded as an overhead note.

Cancellation of aggregate task time is cooperative (checkpoints at turn
boundaries plus the worker maintenance thread); there is no invoice-hard-cap
guarantee. No retried prompt is ever replayed after a lost response.
"""
import inspect
import re
import time
from dataclasses import dataclass

from .worker_budget_governance import PreparedBudgetTurn

# Oh My Pi pi-agent-core output-budget.ts: the fitter never returns below this
# floor, so a smaller declared cap could be exceeded near a full context window.
OMP_MIN_FITTED_OUTPUT_TOKENS = 1024

# Parameter/field names that would claim an output-token cap. Presence alone is
# never evidence of enforcement: semantics must be verified before a surface is
# attested, so the probes below report such names as evidence and stay closed.
_CAP_NAME = re.compile(r"(?i)(max.*token|token.*limit|output.*cap|max.*output)")

USAGE_OVERHEAD_NOTE = {
    "droid": ("factory usage counts tokens plus factory_credits (not USD); vendor system "
              "prompt, tool schemas and hook frames are not observable in the serialized "
              "user message; cache/thinking tokens reported separately"),
    "oh-my-pi": ("omp get_session_stats tokens are session-cumulative vendor reports; "
                 "reasoning tokens are added to the generated-output bound; vendor framing "
                 "overhead is not observable"),
}


class BudgetAdapterError(Exception):
    """Fail-closed refusal or violation. ``reason`` is a stable machine code."""
    def __init__(self, reason, detail, *, capability=None):
        super().__init__(f"{reason}: {detail}")
        self.reason, self.detail, self.capability = reason, detail, capability


class OutputCapViolation(BudgetAdapterError):
    """Reported generated tokens exceeded the declared hard output bound."""
    def __init__(self, vendor, reported, declared):
        super().__init__("output_bound_exceeded",
                         f"{vendor} reported {reported} generated tokens against a "
                         f"declared hard bound of {declared}; the cap was not enforced")
        self.reported, self.declared = reported, declared


@dataclass(frozen=True)
class AdapterCapability:
    """Probed bounded-turn capability. ``supported`` is only ever set by a probe in
    this module after the enforcement semantics of a concrete vendor surface have
    been verified; callers must never construct an affirmative attestation."""
    vendor: str
    supported: bool
    reasons: tuple
    evidence: tuple

    def status(self):
        return {"vendor": self.vendor, "bounded_turn_supported": self.supported,
                "reasons": list(self.reasons), "evidence": list(self.evidence)}


def _cap_like_names(names):
    return sorted(name for name in names if isinstance(name, str) and _CAP_NAME.search(name))


def factory_capability(sdk=None):
    """Probe the Factory SDK surface for a verifiable hard output-token limit.

    Pure introspection of the (pinned) ``droid_sdk`` package or an injected test
    double with the same shape; never spawns the CLI or contacts a model."""
    if sdk is None:
        import droid_sdk as sdk  # pinned optional dependency (pyproject [runtimes])
    version = getattr(sdk, "__version__", None)
    evidence = [f"droid_sdk_version={version or 'unknown'}"]
    stream_names, config_names, settings_names = (), (), ()
    try:
        stream_names = tuple(inspect.signature(sdk.Session.stream).parameters)
    except (AttributeError, TypeError, ValueError) as error:
        evidence.append(f"stream_signature_unreadable={type(error).__name__}")
    for label, holder in (("session_config", getattr(sdk, "SessionConfig", None)),
                          ("session_settings", getattr(sdk, "SessionSettings", None))):
        fields = getattr(holder, "__dataclass_fields__", None)
        names = tuple(fields) if fields else ()
        evidence.append(f"{label}_fields={','.join(names) or 'unavailable'}")
        if label == "session_config":
            config_names = names
        else:
            settings_names = names
    evidence.append(f"stream_params={','.join(stream_names) or 'unavailable'}")
    suspects = _cap_like_names(stream_names + config_names + settings_names)
    reasons = [
        "droid_sdk Session.stream accepts only prompt/images/files/output/timeout/"
        "include_partial_messages; no hard output-token limit exists on any documented "
        "session, settings, CLI or JSON-RPC surface (verified against droid_sdk 0.5.0 / "
        "droid exec 0.233.0)",
        "the serialized prompt is the user-message text only; the vendor prepends its "
        "system prompt, tool schemas and hook frames, so it is not the exact full model "
        "input a local counter must price",
    ]
    if suspects:
        reasons.append("unverified cap-like parameters present (never inferred as "
                       "enforcement): " + ",".join(suspects))
    return AdapterCapability("droid", False, tuple(reasons), tuple(evidence))


def omp_capability():
    """Report the Oh My Pi RPC bounded-turn capability.

    The installed ``omp`` 18.8.9 rpc-wire schema documents ``prompt`` params of
    ``{message, images, streamingBehavior}`` and exposes no per-turn output cap;
    the only internal cap (``runEphemeralTurn.maxTokens``) is not RPC-exposed and
    catalog ``model.maxTokens`` is policy metadata, not attested enforcement."""
    reasons = [
        "omp 18.8.9 RPC prompt params are {message, images, streamingBehavior}; no "
        "per-turn output-token limit command exists in the rpc-wire schema",
        "the internal runEphemeralTurn maxTokens (used by /btw) is not exposed over "
        "RPC, and catalog model.maxTokens is metadata: fitOutputTokensToContextWindow "
        f"only lowers it but floors at {OMP_MIN_FITTED_OUTPUT_TOKENS} tokens, "
        "ensureMaxTokensForThinking raises it for budget-thinking models, and "
        "omitMaxOutputTokens compat policies may drop it silently -- hard enforcement "
        "is never inferred from such metadata",
        "the serialized prompt is the RPC message string; the vendor adds system "
        "prompt sections, tool descriptors and session history, so it is not the "
        "exact full model input a local counter must price",
    ]
    evidence = ["omp_version=18.8.9",
                "rpc_prompt_params=message,images,streamingBehavior",
                "rpc_commands_checked=prompt,abort_and_prompt,set_model,set_thinking_level,"
                "get_state,get_session_stats",
                f"min_fitted_output_tokens={OMP_MIN_FITTED_OUTPUT_TOKENS}"]
    return AdapterCapability("oh-my-pi", False, tuple(reasons), tuple(evidence))


def capability_status(options):
    """Operator-facing status for one runtime configuration (pure; no vendor I/O)."""
    vendor = (options or {}).get("vendor")
    capability = omp_capability() if vendor == "oh-my-pi" else factory_capability(
        (options or {}).get("sdk"))
    return capability.status()


# -- usage normalization -------------------------------------------------------


def _optional_int(container, key, source):
    if isinstance(container, dict):
        present, value = key in container, container.get(key)
    else:
        present, value = hasattr(container, key), getattr(container, key, None)
    if not present or value is None:
        return None
    if type(value) is not int or not 0 <= value <= 10**9:
        raise BudgetAdapterError("usage_shape_invalid",
                                 f"{source}.{key} must be an explicit non-negative int")
    return value


def verify_output_bound(generated_tokens, output_max_tokens, *, vendor):
    """Post-turn verification: reported generated tokens must fit the declared bound.

    A violation means the cap was not enforced; callers must fail closed (durable
    unknown hold), never settle as if the bound held."""
    if generated_tokens is None:
        return
    if generated_tokens > output_max_tokens:
        raise OutputCapViolation(vendor, generated_tokens, output_max_tokens)


def factory_usage_report(usage):
    """Normalize a droid_sdk ``Usage`` (or same-shaped mapping) for settlement.

    ``input_tokens``/``output_tokens`` stay ``None`` when absent (the ledger holds
    the reserved worst case). ``factory_credits`` is a governed currency unit, never
    converted: it settles without hold only against a ``factory_credits`` budget.
    Cache creation/read and thinking tokens are billed by the vendor but priced
    separately from in/out tokens; they are recorded in the overhead note."""
    if usage is None:
        return None
    input_tokens = _optional_int(usage, "input_tokens", "factory usage")
    output_tokens = _optional_int(usage, "output_tokens", "factory usage")
    cache_creation = _optional_int(usage, "cache_creation_tokens", "factory usage")
    cache_read = _optional_int(usage, "cache_read_tokens", "factory usage")
    thinking = _optional_int(usage, "thinking_tokens", "factory usage")
    credits = usage.get("factory_credits") if isinstance(usage, dict) \
        else getattr(usage, "factory_credits", None)
    if credits is not None and (isinstance(credits, bool)
                                or type(credits) not in (int, float) or not 0 <= credits):
        raise BudgetAdapterError("usage_shape_invalid",
                                 "factory_credits must be a non-negative number")
    note = (f"cache_creation={cache_creation},cache_read={cache_read},thinking={thinking}; "
            + USAGE_OVERHEAD_NOTE["droid"])
    return {"source": "provider", "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": float(credits) if credits is not None else None,
            "currency": "factory_credits" if credits is not None else None,
            "vendor_overhead_note": note}


def omp_usage_report(stats, *, cost_currency=None):
    """Normalize an omp ``get_session_stats`` frame for settlement.

    ``stats`` must match the documented SessionStats shape (``tokens`` with
    input/output/cacheRead/cacheWrite/total/reasoning, ``cost``, ``credits``).
    Generated tokens bound the output reserve, so provider-billed ``reasoning``
    tokens are added to ``output``. ``cost`` is a vendor-computed estimate in an
    unspecified unit: it is attached only when the operator explicitly attests its
    governed currency via ``cost_currency``; otherwise cost stays unknown and the
    ledger holds. Stats are session-cumulative, never per-turn; the caller treats
    them as evidence for one settled turn only on a freshly opened session."""
    if stats is None:
        return None
    if not isinstance(stats, dict):
        raise BudgetAdapterError("usage_shape_invalid", "omp session stats must be a mapping")
    tokens = stats.get("tokens")
    input_tokens = output_tokens = None
    if tokens is not None:
        if not isinstance(tokens, dict):
            raise BudgetAdapterError("usage_shape_invalid", "omp tokens must be a mapping")
        input_tokens = _optional_int(tokens, "input", "omp tokens")
        output = _optional_int(tokens, "output", "omp tokens")
        reasoning = _optional_int(tokens, "reasoning", "omp tokens") or 0
        output_tokens = None if output is None else output + reasoning
        cache_note = (f"cache_read={_optional_int(tokens, 'cacheRead', 'omp tokens')},"
                      f"cache_write={_optional_int(tokens, 'cacheWrite', 'omp tokens')}")
    else:
        cache_note = "tokens=unreported"
    cost = stats.get("cost")
    if cost is not None and (isinstance(cost, bool)
                             or type(cost) not in (int, float) or not 0 <= cost):
        raise BudgetAdapterError("usage_shape_invalid", "omp cost must be a non-negative number")
    if cost_currency is not None and (not isinstance(cost_currency, str) or not cost_currency):
        raise BudgetAdapterError("usage_shape_invalid", "cost currency must be an explicit unit")
    attach_cost = cost is not None and cost_currency is not None
    return {"source": "provider", "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": float(cost) if attach_cost else None,
            "currency": cost_currency if attach_cost else None,
            "vendor_overhead_note": cache_note + "; " + USAGE_OVERHEAD_NOTE["oh-my-pi"]}


# -- governed turn assembly ----------------------------------------------------


def _validate_turn_request(runtime, output_max_tokens):
    """Configuration-source revalidation before any vendor contact or state write."""
    if type(output_max_tokens) is not int or not 1 <= output_max_tokens <= 10**9:
        raise BudgetAdapterError("output_reserve_invalid",
                                 "output bound must be an explicit int in [1, 10**9]")
    options = getattr(runtime, "options", None)
    if not isinstance(options, dict):
        raise BudgetAdapterError("adapter_config_invalid", "runtime options must be a mapping")
    model = options.get("model")
    if not isinstance(model, str) or not model.strip():
        raise BudgetAdapterError("budget_model_unspecified",
                                 "the exact vendor model id must be pinned in runtime options; "
                                 "a vendor default cannot be matched to the budget model_id")
    counter = getattr(runtime, "context_tokenizer_id", None)
    if not isinstance(counter, str) or not counter:
        raise BudgetAdapterError("budget_counter_unspecified",
                                 "no named exact local token counter is bound to this runtime")
    return model, counter


def _factory_turn(runtime, prompt, checkpoint, output_max_tokens):
    """Drive one Factory turn through the runtime's own SDK machinery.

    Only reached after a probe attests support. Pre-turn checkpoint gates aggregate
    time before spending; any failure marks the durable outcome ``unknown`` and
    leaves the reservation held (never silently retried)."""
    checkpoint()
    runtime._start_loop()
    try:
        result = runtime._await(runtime._turn(prompt), runtime.timeout + 30)
        usage = factory_usage_report(getattr(result, "usage", None))
        if usage is not None:
            verify_output_bound(usage["output_tokens"], output_max_tokens, vendor="droid")
        if result.interrupted:
            status = "paused" if runtime._requested == "pause" else \
                "cancelled" if runtime._requested else "paused"
        else:
            status = "completed" if result.success else "failed"
        outcome = runtime._settled(status, result.text)
        outcome["usage"] = usage or {"source": "reported"}
        return outcome
    except Exception:
        runtime._active = False
        runtime._save(outcome="unknown")
        raise


def _omp_turn(runtime, prompt, checkpoint, output_max_tokens):
    """Drive one Oh My Pi turn over the documented RPC frame shapes.

    Mirrors ``OhMyPiRuntime.execute``: ``prompt`` -> ``prompt_result`` (+ optional
    ``session_settled``) -> ``get_state`` quiescence. A lost or malformed response
    is an unknown outcome: the turn is never replayed, the reservation stays held.
    Retry of a lost prompt is impossible to distinguish from a duplicate model
    call, so none is attempted."""
    from .recovery import UnknownOutcome
    from .vendor_runtimes import uid
    checkpoint()
    try:
        runtime._open()
        identifier = uid("turn")
        runtime.transport.send({"type": "prompt", "id": identifier, "message": prompt})
        deadline = time.monotonic() + runtime.timeout
        while time.monotonic() < deadline:
            frame = runtime.transport.receive(max(.01, deadline - time.monotonic()))
            if frame.get("type") == "prompt_result" and frame.get("id") == identifier:
                status = frame.get("status")
                if status == "completed" and frame.get("agentInvoked") is not True:
                    raise UnknownOutcome("No vendor agent executed the task")
                if status == "aborted":
                    status = "paused" if runtime._requested == "pause" else "cancelled"
                elif status == "error":
                    status = "failed"
                elif status != "completed":
                    raise ValueError("Unknown vendor turn outcome")
                if frame.get("sessionSettled") is not True:
                    while time.monotonic() < deadline:
                        settled = runtime.transport.receive(max(.01, deadline - time.monotonic()))
                        if settled.get("type") == "session_settled":
                            break
                    else:
                        raise TimeoutError("Vendor still has pending work")
                current = runtime._request("get_state")
                if current.get("isSettled") is not True:
                    raise UnknownOutcome("Vendor session is not quiescent")
                runtime._save(session_file=current.get("sessionFile"),
                              session_id=current["sessionId"])
                result = ""
                if status == "completed" and runtime.options.get("result_contract"):
                    result = runtime._request("get_last_assistant_text").get("text") or ""
                usage = None
                if runtime.options.get("collect_usage") is True:
                    stats = runtime._request("get_session_stats")
                    usage = omp_usage_report(
                        stats, cost_currency=runtime.options.get("budget_cost_currency"))
                    verify_output_bound(usage["output_tokens"], output_max_tokens,
                                        vendor="oh-my-pi")
                outcome = runtime._settled(status, result)
                outcome["usage"] = usage or {"source": "reported"}
                return outcome
        raise TimeoutError("Vendor turn timed out")
    except Exception:
        runtime._active = False
        runtime._save(outcome="unknown")
        raise


def _build_turn(runtime, task, context, *, output_max_tokens, capability, runner):
    """Exact governed preparation shared by both vendor adapters.

    ``runtime._prepare`` performs the identity, isolation and configuration-source
    revalidation (private config outside the workspace, agent identity matching the
    assignment) and records the durable ``running`` intent before any vendor call;
    if the subsequent reservation is refused the runtime stays in that intent and
    fails closed on the next preparation until reconciled -- no model call occurred.
    The returned prompt is the exact serialized user message; it is never described
    as the vendor's full model input."""
    if not capability.supported:
        raise BudgetAdapterError("budget_capability_missing",
                                 "; ".join(capability.reasons), capability=capability)
    model, counter = _validate_turn_request(runtime, output_max_tokens)
    prompt = runtime._prepare(task, context)
    if not isinstance(prompt, str) or not prompt:
        raise BudgetAdapterError("adapter_config_invalid", "runtime produced no prompt")

    def invoke(checkpoint):
        return runner(runtime, prompt, checkpoint, output_max_tokens)

    return PreparedBudgetTurn(prompt, model, counter, output_max_tokens, invoke,
                              enforces_output_max=True)


def prepare_factory_budgeted_turn(runtime, task, context, *, output_max_tokens):
    """FactoryRuntime bounded-turn preparation. Currently fails closed: see
    ``factory_capability`` for the probed reasons and evidence."""
    capability = factory_capability(getattr(runtime, "sdk", None))
    return _build_turn(runtime, task, context, output_max_tokens=output_max_tokens,
                       capability=capability, runner=_factory_turn)


def prepare_omp_budgeted_turn(runtime, task, context, *, output_max_tokens):
    """OhMyPiRuntime bounded-turn preparation. Currently fails closed: see
    ``omp_capability`` for the probed reasons and evidence."""
    return _build_turn(runtime, task, context, output_max_tokens=output_max_tokens,
                       capability=omp_capability(), runner=_omp_turn)


def describe_adapters():
    """Concise operator-facing integration contract for both stock adapters."""
    return {"factory": factory_capability().status(), "oh_my_pi": omp_capability().status(),
            "contract": {
                "prepare": "runtime.prepare_budgeted_turn(task, context, "
                           "output_max_tokens=int) -> PreparedBudgetTurn",
                "refusal": "BudgetAdapterError(reason=...) before any vendor call; "
                           "nothing is invoked, counted or written when unsupported",
                "usage": "result['usage'] is a UsageReport-shaped dict; missing tokens or "
                         "unattested currency settle as explicit unknown holds, never zero",
                "cancellation": "aggregate task time is cooperative (checkpoints at turn "
                                "boundaries plus the maintenance thread); no invoice "
                                "hard-cap guarantee"}}
