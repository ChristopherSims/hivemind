"""Operator-only, opt-in process extraction and local runtime tokenizer configuration."""
import hashlib
import json
import os
from pathlib import Path

from .context_assembly import Counter
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
    """Load exact tokenizer data, never downloaded code or a guessed embedding tokenizer."""
    path = Path(tokenizer_file).resolve(strict=True)
    if path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("Runtime tokenizer exceeds the local loading limit")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if not model_id or digest != expected_sha256:
        raise ValueError("Runtime tokenizer identity/hash mismatch")
    from tokenizers import Tokenizer
    tokenizer = Tokenizer.from_file(str(path))
    tokenizer.no_padding()
    tokenizer.no_truncation()
    return Counter(model_id, lambda text: len(tokenizer.encode(text, add_special_tokens=False).ids),
                   "operator_bound_local_runtime_tokenizer", True)


def integration_options(filename):
    """Explicit local operator configuration; no module discovery or API-controlled argv."""
    path = Path(filename).resolve(strict=True)
    if path.stat().st_size > 131072:
        raise ValueError("Integration configuration exceeds its loading limit")
    config = json.loads(path.read_text(encoding="utf-8"))
    if set(config) - {"memory_pipeline", "runtime_tokenizers", "review_automation", "verification",
                      "git_collaboration", "git_reviewer_runtime", "nexus_git_automation", "os_isolation"}:
        raise ValueError("Unknown integration configuration section")
    options = {}
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
        if adapter not in (None, "factory", "oh_my_pi"):
            raise ValueError("Unsupported reviewer adapter")
        base_options = dict(runtime_config.get("options", {}))

        def reviewer_factory(*, workspace, context):
            from .vendor_runtimes import FactoryRuntime, OhMyPiRuntime
            if adapter is None:
                raise ValueError("No reviewer adapter configured")
            private_root = Path(base_options["state_file"]).resolve().parent / "reviewer-executions"
            runtime_options = {**base_options, "workspace": str(workspace),
                               "state_file": str(private_root / (context["execution_id"] + ".json")),
                               "result_contract": "git_review", "read_only_tools": True}
            return (FactoryRuntime if adapter == "factory" else OhMyPiRuntime)(runtime_options)

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
        options["context_assembly"] = {"counters": {
            item["model_id"]: runtime_counter(**item) for item in config["runtime_tokenizers"]}}
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
        policy = IsolationPolicy(**config["os_isolation"])
        result = plan(policy)
        if policy.required and any(item.status == "external" for item in result.decisions):
            raise IsolationRefused("Required identity/ACL controls lack automatic attestation; "
                                   "operator provisioning alone is not a verified runtime boundary")
        if (policy.required and policy.sandbox_processes
                and options.get("git_reviewer_runtime", {}).get("policy")
                and options["git_reviewer_runtime"]["policy"].enabled):
            raise IsolationRefused("Vendor reviewer sessions do not support required OS sandboxing")
        options.setdefault("agent_automation", {})["isolation_policy"] = policy
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
