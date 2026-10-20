"""Shared registration for agent capability modules, owned by the integration layer."""
from importlib import import_module

FEATURE_MODULES = (
    "team_access",
    "nexus_delegation",
    "scheduling", "conversations", "verification", "context_assembly", "memory_pipeline",
    "runtime_lifecycle",
    "review_automation",
    "git_collaboration",
    "git_reviewer_runtime",
    "agent_automation",
    "telemetry",
    "coordination_policy",
    "control_capture",
    "memory_lifecycle_automation",
    "message_push",
    "retrieval_runtime",
    "goal_orchestration",
    "structured_handoffs",
    "capability_routing",
    "task_budgets",
    "artifact_contracts",
    "activity_stream",
    "memory_event_bridge",
    "blocked_tasks",
    "worker_provisioning",
    "memory_adapters",
    "thread_pane",
    "mention_feedback",
    "board_reads",
    "credential_management",
    "context_explorer",
    "operations_dashboard",
    "task_run_workspace",
    "conflict_control_console",
    "privacy_audit",
    "agent_evidence",
)
INSTALLED_FEATURE_MODULES = FEATURE_MODULES


def install(app, service, principal, command, envelope, options=None):
    for name in INSTALLED_FEATURE_MODULES:
        module = import_module(f"hivemind.{name}")
        module_options = dict((options or {}).get(name, {}))
        if name == "blocked_tasks":
            module_options.setdefault("policy", module.BlockedTasksPolicy(explain_enabled=False))
        if name == "memory_event_bridge":
            module.install(app, service, app.state.memory_lifecycle, principal, command,
                           envelope, **module_options)
            continue
        if name == "message_push":
            # Default-off without registering a route: an explicit later install
            # (tests, operators) must not be shadowed by a disabled placeholder.
            policy = module_options.get("policy")
            if policy is None or getattr(policy, "enabled", False) is not True:
                app.state.message_push_hub = None
                continue
        module.install(app, service, principal, command, envelope, **module_options)


def model_modules():
    return [import_module(f"hivemind.{name}") for name in (*FEATURE_MODULES, "memory_provider_sync")]


def replace_routes(app, module_name):
    """An explicit reinstall replaces its own handlers, never another module's routes."""
    app.router.routes[:] = [
        route for route in app.router.routes
        if getattr(getattr(route, "endpoint", None), "__module__", None) != module_name
    ]
    app.openapi_schema = None
