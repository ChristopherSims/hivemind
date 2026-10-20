"""Integration-owned registry of bounded, read-only canonical scheduling gates."""


def set_feature_gates(service, name, *, dispatch=None, start=None, dependency=None):
    registry = getattr(service, "_agent_feature_gates", {})
    if dispatch is None and start is None and dependency is None:
        registry.pop(name, None)
    else:
        for callback in (dispatch, start, dependency):
            if callback is not None and not callable(callback):
                raise ValueError("Feature gates must be trusted callables")
        registry[name] = {"dispatch": dispatch, "start": start, "dependency": dependency}
    service._agent_feature_gates = registry
    for phase, attribute in (("dispatch", "task_dispatch_gates"),
                             ("start", "task_start_gates"),
                             ("dependency", "task_dependency_gates")):
        callbacks = tuple(registry[name][phase] for name in sorted(registry)
                          if registry[name][phase] is not None)
        setattr(service, attribute, callbacks)


def set_resource_projection(service, kind, callback):
    """Use source-aware views for committed-command receipts too."""
    projections = getattr(service, "resource_projections", {})
    if callback is None:
        projections.pop(kind, None)
    elif callable(callback):
        projections[kind] = callback
    else:
        raise ValueError("Resource projections must be trusted callables")
    service.resource_projections = projections
