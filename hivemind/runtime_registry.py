"""Explicit operator runtime choices; no discovery or API-selected Python imports."""
from importlib import import_module

RUNTIME_FACTORIES = {
    "factory": "hivemind.vendor_runtimes:factory_runtime",
    "oh_my_pi": "hivemind.vendor_runtimes:oh_my_pi_runtime",
    "a2a": "hivemind.protocol_runtimes:a2a_runtime",
    "acp": "hivemind.protocol_runtimes:acp_runtime",
}


def factory_path(name):
    if name not in RUNTIME_FACTORIES:
        raise ValueError("Unsupported explicitly configured runtime")
    return RUNTIME_FACTORIES[name]


def create_runtime(name, options):
    module, factory = factory_path(name).split(":")
    return getattr(import_module(module), factory)(dict(options))


def descriptions():
    return {
        "factory": {"purpose": "Official local Factory SDK task/structured-result execution.",
                    "authority": "Explicit operator execution/model opt-ins and canonical scopes.",
                    "limits": "Resume workspace discovery can fail closed; Windows vendor sandbox unsupported."},
        "oh_my_pi": {"purpose": "Documented local OMP RPC task/structured-result execution.",
                      "authority": "Explicit operator execution/model opt-ins and canonical scopes.",
                      "limits": "No uncertain prompt replay; sandbox support is filesystem-only Linux."},
        "a2a": {"purpose": "Pinned A2A JSON-RPC remote task/message interoperability.",
                "authority": "Approved endpoint and separate execution/model opt-ins; no new remote tool grants.",
                "limits": "Pause/resume and strict OS sandbox controls unsupported; interrupted effects held."},
        "acp": {"purpose": "Agent Client Protocol stdio session/turn interoperability.",
                "authority": "Trusted operator argv and execution/model opt-ins; permission requests denied.",
                "limits": "No native hostile-process sandbox, unadvertised filesystem/terminal methods denied; "
                          "pause/resume unsupported."},
    }
