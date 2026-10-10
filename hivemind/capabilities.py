"""Shared registration for agent capability modules, owned by the integration layer."""
from importlib import import_module

FEATURE_MODULES = (
    "scheduling", "conversations", "verification", "context_assembly", "memory_pipeline",
    "runtime_lifecycle",
    "review_automation",
    "git_collaboration",
    "git_reviewer_runtime",
    "agent_automation",
)
INSTALLED_FEATURE_MODULES = FEATURE_MODULES


def install(app, service, principal, command, envelope, options=None):
    for name in INSTALLED_FEATURE_MODULES:
        module = import_module(f"hivemind.{name}")
        module.install(app, service, principal, command, envelope, **(options or {}).get(name, {}))


def model_modules():
    return [import_module(f"hivemind.{name}") for name in FEATURE_MODULES]


def replace_routes(app, module_name):
    """An explicit reinstall replaces its own handlers, never another module's routes."""
    app.router.routes[:] = [
        route for route in app.router.routes
        if getattr(getattr(route, "endpoint", None), "__module__", None) != module_name
    ]
    app.openapi_schema = None
