"""Operator-only, opt-in process extraction and local runtime tokenizer configuration."""
import hashlib
import hmac
import json
import os
from pathlib import Path

from .context_assembly import MAX_SCAN_LIMIT, Counter
from .indexes import RANK_DEPTH_MAX
from .process_boundary import run_bounded


class AuthorizedProcessExtractor:
    def __init__(self, *, argv, workspace, model_id, enabled=False, allow_model_calls=False,
                 timeout_seconds=60, require_os_isolation=True):
        if not isinstance(argv, list) or not argv or any(not isinstance(arg, str) or not arg for arg in argv):
            raise ValueError("Extractor requires an operator-configured argv array")
        if not 0 < timeout_seconds <= 300 or not model_id:
            raise ValueError("Extractor identity/deadline is invalid")
        self.argv, self.workspace, self.model_id = tuple(argv), Path(workspace).resolve(strict=True), model_id
        self.enabled, self.allowed = enabled is True, allow_model_calls is True
        self.timeout, self.require_os_isolation = timeout_seconds, require_os_isolation

    def __call__(self, prompt, *, max_output_bytes=16384):
        if not self.enabled or not self.allowed:
            raise PermissionError("Extraction requires explicit execution and model-call opt-ins")
        environment = {key: os.environ[key] for key in ("PATH", "SystemRoot", "WINDIR", "LANG")
                       if key in os.environ}
        environment.update(HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                           HF_DATASETS_OFFLINE="1", PYTHONDONTWRITEBYTECODE="1")
        code, stdout, _stderr = run_bounded(self.argv, self.workspace, environment, self.timeout,
            input_bytes=json.dumps({"model_id": self.model_id, "prompt": prompt,
                                    "max_output_bytes": max_output_bytes}).encode(),
            max_output=max_output_bytes, require_os_isolation=self.require_os_isolation)
        if code != 0:
            raise RuntimeError("Configured extractor process failed")
        return stdout.decode("utf-8", errors="strict")


def runtime_counter(model_id, tokenizer_file, expected_sha256):
    """Load exact tokenizer data, never downloaded code or a guessed embedding tokenizer.

    The pinned file's bytes are read once, hashed, and the tokenizer is parsed from
    *those same verified bytes* (``Tokenizer.from_buffer``) -- TOCTOU-safe: a file
    swapped between hashing and parsing cannot slip in. No network access, no
    downloads, and no fallback to any other file when the pin fails.
    """
    if not isinstance(model_id, str) or not model_id or len(model_id) > 128:
        raise ValueError("Runtime tokenizer identity/hash mismatch")
    if (not isinstance(expected_sha256, str) or len(expected_sha256) != 64
            or any(char not in "0123456789abcdef" for char in expected_sha256.lower())):
        raise ValueError("Runtime tokenizer identity/hash mismatch")
    if not isinstance(tokenizer_file, (str, os.PathLike)) or not str(tokenizer_file):
        raise ValueError("Runtime tokenizer requires an explicit local file path")
    path = Path(tokenizer_file).resolve(strict=True)
    if path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("Runtime tokenizer exceeds the local loading limit")
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if not hmac.compare_digest(digest, expected_sha256.lower()):
        raise ValueError("Runtime tokenizer identity/hash mismatch")
    from tokenizers import Tokenizer
    tokenizer = Tokenizer.from_buffer(data)  # parsed from the exact hashed bytes
    tokenizer.no_padding()
    tokenizer.no_truncation()
    return Counter(model_id, lambda text: len(tokenizer.encode(text, add_special_tokens=False).ids),
                   "operator_bound_local_runtime_tokenizer", True)


def _bounded_policy_int(section, key, maximum):
    """Strict finite operator bound: a real JSON int (never a bool/float) in range."""
    value = section.get(key)
    if value is None:
        return None
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"Runtime tokenizer policy {key} must be an integer in 1..{maximum}")
    return value


def _runtime_tokenizer_options(section):
    """Wire pinned runtime tokenizer counters into context assembly and retrieval.

    Two accepted shapes, both fully offline and operator-fixed:

    * legacy list -- ``[{"model_id", "tokenizer_file", "expected_sha256"}]``; behavior
      unchanged: counters are passed to context assembly as ``{"counters": ...}``.
    * strict object -- ``{"tokenizers": [...], "require_exact": bool,
      "scan_limit": int, "rank_depth": int}``; unknown keys are denied, bounds are
      finite ints, ``require_exact`` a real boolean. One shared
      ``retrieval_runtime.CounterRegistry`` carries the pinned counters into both the
      context assembly and retrieval surfaces, with the strict ``require_exact``
      policy and operator-fixed scan/rank bounds that clients can never widen.
    """
    legacy = isinstance(section, list)
    if not legacy:
        if not isinstance(section, dict):
            raise ValueError("Runtime tokenizer configuration must be a list or an object")
        if set(section) - {"tokenizers", "require_exact", "scan_limit", "rank_depth"}:
            raise ValueError("Unknown runtime tokenizer configuration key")
        items = section.get("tokenizers", [])
    else:
        items = section
    if not isinstance(items, list) or len(items) > 64:
        raise ValueError("Runtime tokenizers must be a bounded explicit list")
    counters = {}
    for item in items:
        if not isinstance(item, dict) or set(item) != {"model_id", "tokenizer_file",
                                                       "expected_sha256"}:
            raise ValueError("Runtime tokenizer entries require exactly model_id, "
                             "tokenizer_file, and expected_sha256")
        if item["model_id"] in counters:
            raise ValueError("Duplicate runtime tokenizer model binding")
        counters[item["model_id"]] = runtime_counter(**item)
    if legacy:
        return {"context_assembly": {"counters": counters}}
    require_exact = section.get("require_exact", False)
    if type(require_exact) is not bool:
        raise ValueError("Runtime tokenizer require_exact policy must be a boolean")
    scan_limit = _bounded_policy_int(section, "scan_limit", MAX_SCAN_LIMIT)
    rank_depth = _bounded_policy_int(section, "rank_depth", RANK_DEPTH_MAX)
    from .retrieval_runtime import CounterRegistry
    registry = CounterRegistry()
    for target, counter in counters.items():
        registry.register_counter(target, counter)
    context = {"registry": registry, "require_exact": require_exact}
    if scan_limit is not None:
        context["scan_limit"] = scan_limit
    retrieval = {"registry": registry}
    if scan_limit is not None:
        retrieval["scan_limit"] = scan_limit
    if rank_depth is not None:
        retrieval["rank_depth"] = rank_depth
    return {"context_assembly": context, "retrieval_runtime": retrieval}


def integration_options(filename):
    """Explicit local operator configuration; no module discovery or API-controlled argv."""
    path = Path(filename).resolve(strict=True)
    if path.stat().st_size > 131072:
        raise ValueError("Integration configuration exceeds its loading limit")
    def reject_constant(value):
        raise ValueError("Integration configuration requires finite numbers")

    config = json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_constant)
    if not isinstance(config, dict):
        raise ValueError("Integration configuration must be an object")
    if set(config) - {"memory_pipeline", "runtime_tokenizers", "review_automation", "verification",
                      "git_collaboration", "git_reviewer_runtime", "nexus_git_automation", "os_isolation",
                      "telemetry", "coordination_policy", "control_capture", "memory_lifecycle",
                      "message_push", "goal_orchestration", "structured_handoffs", "capability_routing",
                      "artifact_contracts", "task_budgets", "activity_stream", "memory_event_bridge",
                      "blocked_tasks", "worker_provisioning", "native_artifact_verifiers"}:
        raise ValueError("Unknown integration configuration section")
    options = {}
    if "native_artifact_verifiers" in config:
        from .native_artifact_verifier import NativeArtifactVerifier, NativeCheckProfile
        item = config["native_artifact_verifiers"]
        if not isinstance(item, dict):
            raise ValueError("Invalid native artifact verifier configuration")
        values = dict(item)
        profiles = values.pop("profiles", [])
        if not isinstance(profiles, list) or len(profiles) > 64:
            raise ValueError("Native verifier profiles must be a bounded list")
        values["profiles"] = tuple(NativeCheckProfile(**profile) for profile in profiles)
        options.setdefault("artifact_contracts", {})["verifier"] = NativeArtifactVerifier(**values)
    if "worker_provisioning" in config:
        from .worker_provisioning import ProvisioningPolicy, WorkerProfile
        item = config["worker_provisioning"]
        if not isinstance(item, dict) or set(item) - {
                "enabled", "api_base_url", "max_active_workers", "profiles", "linux_cgroup_root"}:
            raise ValueError("Invalid worker provisioning configuration")
        values = dict(item)
        profiles = values.pop("profiles", [])
        if not isinstance(profiles, list) or len(profiles) > 32:
            raise ValueError("Worker profiles require a bounded list")
        try:
            values["profiles"] = tuple(WorkerProfile(**profile) for profile in profiles)
            options["worker_provisioning"] = {"policy": ProvisioningPolicy(**values)}
        except (TypeError, OSError):
            raise ValueError("Invalid worker provisioning configuration") from None
    if "blocked_tasks" in config:
        from .blocked_tasks import BlockedTasksPolicy
        item = config["blocked_tasks"]
        if not isinstance(item, dict) or set(item) - {"explain_enabled", "requests_enabled"}:
            raise ValueError("Invalid blocked task configuration")
        if any(type(value) is not bool for value in item.values()):
            raise ValueError("Blocked task opt-ins require booleans")
        if item.get("requests_enabled", False) and not item.get("explain_enabled", True):
            raise ValueError("Blocked task requests require explanations")
        options["blocked_tasks"] = {"policy": BlockedTasksPolicy(**item)}
    if "memory_event_bridge" in config:
        from .memory_event_bridge import BridgePolicy
        item = config["memory_event_bridge"]
        if not isinstance(item, dict):
            raise ValueError("Invalid memory event bridge configuration")
        try:
            options["memory_event_bridge"] = {"policy": BridgePolicy(**item)}
        except TypeError:
            raise ValueError("Invalid memory event bridge configuration") from None
    if "activity_stream" in config:
        from .activity_stream import ActivityStreamPolicy
        item = config["activity_stream"]
        if not isinstance(item, dict):
            raise ValueError("Invalid activity stream configuration")
        try:
            options["activity_stream"] = {"policy": ActivityStreamPolicy(**item)}
        except TypeError:
            raise ValueError("Invalid activity stream configuration") from None
    if "artifact_contracts" in config:
        from .artifact_contracts import ContractPolicy
        item = config["artifact_contracts"]
        if not isinstance(item, dict) or set(item) - {"enabled", "revocation_releases"}:
            raise ValueError("Invalid artifact contract configuration")
        options.setdefault("artifact_contracts", {})["policy"] = ContractPolicy(**item)
    if "capability_routing" in config:
        from .capability_routing import RoutingPolicy
        item = config["capability_routing"]
        allowed = {"enabled", "max_candidates", "max_heartbeat_gap_seconds",
                   "allow_busy_agents", "require_declaration", "allowed_runtimes",
                   "require_worker_run", "worker_run_ids"}
        if not isinstance(item, dict) or set(item) - allowed:
            raise ValueError("Invalid capability routing configuration")
        options["capability_routing"] = {"policy": RoutingPolicy(**item)}
    if "structured_handoffs" in config:
        from .structured_handoffs import StructuredHandoffPolicy
        item = config["structured_handoffs"]
        if not isinstance(item, dict) or set(item) - {
                "allow_ownership_transfer", "max_open_offers_per_task"}:
            raise ValueError("Invalid structured handoff configuration")
        options["structured_handoffs"] = {"policy": StructuredHandoffPolicy(**item)}
    if "goal_orchestration" in config:
        from .goal_orchestration import GoalSchedule
        item = config["goal_orchestration"]
        if not isinstance(item, dict) or set(item) - {"background"}:
            raise ValueError("Invalid goal orchestration configuration")
        background = item.get("background", {})
        if not isinstance(background, dict):
            raise ValueError("Goal background configuration must be an object")
        options["goal_orchestration"] = {"background": GoalSchedule(**background)}
    if "message_push" in config:
        from .message_push import MessagePushPolicy
        if not isinstance(config["message_push"], dict):
            raise ValueError("Message push configuration must be an object")
        options["message_push"] = {"policy": MessagePushPolicy(**config["message_push"])}
    if "memory_lifecycle" in config:
        from .memory_lifecycle_automation import LifecyclePolicy, RepositoryBinding
        item = config["memory_lifecycle"]
        if not isinstance(item, dict) or set(item) - {"policy", "bindings", "use_local_semantic"}:
            raise ValueError("Invalid memory lifecycle configuration")
        bindings = item.get("bindings", [])
        if not isinstance(bindings, list) or len(bindings) > 64:
            raise ValueError("Repository bindings must be a bounded explicit list")
        semantic = item.get("use_local_semantic", False)
        if type(semantic) is not bool:
            raise ValueError("Local semantic selection requires a boolean")
        values = dict(item.get("policy", {}))
        if "secret_path_markers" in values:
            values["secret_path_markers"] = tuple(values["secret_path_markers"])
        options["memory_lifecycle_automation"] = {
            "policy": LifecyclePolicy(**values),
            "bindings": tuple(RepositoryBinding(**binding) for binding in bindings),
            "use_local_semantic": semantic}
    if "control_capture" in config:
        from .control_capture import GatewayPolicy
        item = config["control_capture"]
        if not isinstance(item, dict) or set(item) - {"gateway_policy"}:
            raise ValueError("Invalid control capture configuration")
        values = dict(item.get("gateway_policy", {}))
        for name in ("allowed_tools", "allowed_namespaces"):
            if name in values and values[name] is not None:
                if not isinstance(values[name], list) or not all(isinstance(v, str) for v in values[name]):
                    raise ValueError("Gateway allowlists must be explicit string lists")
                values[name] = frozenset(values[name])
        if "allowed_mcp_tools" in values:
            values["allowed_mcp_tools"] = tuple(values["allowed_mcp_tools"])
        options["control_capture"] = {"gateway_policy": GatewayPolicy(**values)}
    if "coordination_policy" in config:
        from .coordination_policy import PolicyUpdate
        item = config["coordination_policy"]
        from .feature_scheduling import CoordinationSchedule
        if not isinstance(item, dict) or set(item) - {"policy", "background"}:
            raise ValueError("Invalid coordination policy configuration")
        options["coordination_policy"] = {
            "policy": PolicyUpdate(**item.get("policy", {})).model_dump(),
            "background": CoordinationSchedule(**item.get("background", {})).model_dump()}
    if "telemetry" in config:
        from .telemetry import TelemetryPolicy
        if not isinstance(config["telemetry"], dict):
            raise ValueError("Telemetry configuration must be an object")
        options["telemetry"] = {"policy": TelemetryPolicy(**config["telemetry"])}
    if "git_reviewer_runtime" in config:
        from .git_reviewer_runtime import ReviewerRuntimePolicy
        item = config["git_reviewer_runtime"]
        if not isinstance(item, dict) or set(item) - {"policy", "runtime"}:
            raise ValueError("Invalid reviewer configuration")
        policy_config = item.get("policy", {})
        if any(type(policy_config.get(key, False)) is not bool for key in ("enabled", "allow_model_calls")):
            raise ValueError("Reviewer execution requires boolean opt-ins")
        runtime_config = item.get("runtime", {})
        adapter = runtime_config.get("adapter")
        if adapter not in (None, "factory", "oh_my_pi", "a2a", "acp"):
            raise ValueError("Unsupported reviewer adapter")
        base_options = dict(runtime_config.get("options", {}))

        def reviewer_factory(*, workspace, context):
            from .runtime_registry import create_runtime
            if adapter is None:
                raise ValueError("No reviewer adapter configured")
            private_root = Path(base_options["state_file"]).resolve().parent / "reviewer-executions"
            runtime_options = {**base_options, "workspace": str(workspace),
                               "state_file": str(private_root / (context["execution_id"] + ".json")),
                               "result_contract": "git_review", "read_only_tools": True}
            return create_runtime(adapter, runtime_options)

        options["git_reviewer_runtime"] = {
            "policy": ReviewerRuntimePolicy(**policy_config),
            "runtime_factory": reviewer_factory if adapter else None}
    if "nexus_git_automation" in config:
        from .nexus_git_automation import NexusGitPolicy
        item = config["nexus_git_automation"]
        if not isinstance(item, dict) or type(item.get("enabled", False)) is not bool:
            raise ValueError("Nexus automation requires an explicit boolean opt-in")
        options["agent_automation"] = {"nexus_policy": NexusGitPolicy(**item)}
    if "git_collaboration" in config:
        options["git_collaboration"] = config["git_collaboration"]
    if "runtime_tokenizers" in config:
        options.update(_runtime_tokenizer_options(config["runtime_tokenizers"]))
    if "task_budgets" in config:
        from .task_budgets import BudgetPolicy, ExactLocalCounter, PriceSchedule
        item = config["task_budgets"]
        if not isinstance(item, dict) or set(item) - {"policy", "prices", "counters"}:
            raise ValueError("Invalid task budget configuration")
        values = item.get("policy", {})
        allowed = {"enabled", "directory", "lease_seconds", "allow_reported_usage",
                   "strict_cost", "allow_concurrent_open"}
        if not isinstance(values, dict) or set(values) - allowed:
            raise ValueError("Invalid task budget policy")
        policy = BudgetPolicy(**values)
        prices = item.get("prices")
        prices = PriceSchedule(**prices) if prices is not None else None
        bindings = item.get("counters", {})
        if (not isinstance(bindings, dict) or len(bindings) > 64
                or any(not isinstance(name, str) or not 1 <= len(name) <= 64
                       or not isinstance(target, str) or not 1 <= len(target) <= 128
                       for name, target in bindings.items())):
            raise ValueError("Budget counters must be bounded pinned-tokenizer bindings")
        assembly = options.get("context_assembly", {})
        declared = config.get("runtime_tokenizers", [])
        declared = declared.get("tokenizers", []) if isinstance(declared, dict) else declared
        targets = {entry["model_id"] for entry in declared}
        counters = {}
        for name, target in bindings.items():
            if target not in targets:
                raise ValueError("Budget counter requires an explicit pinned runtime tokenizer")
            registry = assembly.get("registry")
            counter = (registry.resolve(target, require_exact=True) if registry is not None
                       else assembly.get("counters", {}).get(target))
            if counter is None or not counter.exact_for_target:
                raise ValueError("Budget counter must be exact for its pinned runtime tokenizer")
            counters[name] = ExactLocalCounter(counter)
        if policy.enabled and not counters:
            raise ValueError("Enabled task budgets require pinned exact counters")
        options["task_budgets"] = {"policy": policy, "prices": prices, "counters": counters}
    if "memory_pipeline" in config:
        from .memory_pipeline import PipelineConfig
        item = config["memory_pipeline"]
        options["memory_pipeline"] = {
            "config": PipelineConfig(**item.get("config", {})),
            "extractor": AuthorizedProcessExtractor(**item["extractor"]),
            "background": item.get("background", False)}
    if "review_automation" in config:
        from .review_automation import ReviewPolicy
        options["review_automation"] = {"policy": ReviewPolicy(**config["review_automation"])}
    if "verification" in config:
        from .verification import TrustedVerifier
        options["verification"] = {"verifier": TrustedVerifier(**config["verification"])}
    if "os_isolation" in config:
        from .os_isolation import IsolationPolicy, IsolationRefused, plan
        item = config["os_isolation"]
        if not isinstance(item, dict):
            raise ValueError("Isolation configuration must be an object")
        attestation_config = item.get("attestation")
        policy = IsolationPolicy(**{key: value for key, value in item.items() if key != "attestation"})
        result = plan(policy)
        guard = None
        if attestation_config is not None:
            from .isolation_runtime import IsolationAttestationConfig, IsolationRuntimeGuard
            if not isinstance(attestation_config, dict):
                raise ValueError("Isolation attestation must be an object")
            guard = IsolationRuntimeGuard(policy, IsolationAttestationConfig(**attestation_config))
            guard.check()
        if policy.required and any(item.status == "external" for item in result.decisions):
            if guard is None:
                raise IsolationRefused("Required identity/ACL controls lack automatic attestation; "
                                       "operator provisioning alone is not a verified runtime boundary")
        if (policy.required and policy.sandbox_processes
                and options.get("git_reviewer_runtime", {}).get("policy")
                and options["git_reviewer_runtime"]["policy"].enabled):
            raise IsolationRefused("Vendor reviewer sessions do not support required OS sandboxing")
        options.setdefault("agent_automation", {})["isolation_policy"] = policy
        if guard:
            options["agent_automation"]["isolation_guard"] = guard
        enforced = policy.sandbox_processes and result.capabilities.sandbox_filesystem_network
        if enforced:
            options.setdefault("git_collaboration", {})["require_os_isolation"] = True
            verifier = options.get("verification", {}).get("verifier")
            if verifier is not None:
                verifier.require_os_isolation = True
            extractor = options.get("memory_pipeline", {}).get("extractor")
            if extractor is not None:
                extractor.require_os_isolation = True
    return options
