import pytest

from hivemind.local_semantics import LocalSemanticEncoder
from hivemind.memory_lifecycle_automation import install
from hivemind.model_wiring import integration_options
from hivemind.storage import canonical


def test_local_semantic_encoder_uses_loaded_model_without_downloads():
    class StubEncoder:
        def __init__(self):
            self.queries = []

        def query(self, text):
            self.queries.append(text)
            import struct
            return struct.pack("<768f", *([0.5] * 768))

    stub = StubEncoder()
    encoder = LocalSemanticEncoder(stub)
    vectors = encoder.encode(["first memory", "second memory"])
    assert stub.queries == ["first memory", "second memory"]
    assert len(vectors) == 2 and len(vectors[0]) == 768
    with pytest.raises(ValueError):
        LocalSemanticEncoder(None)


def test_memory_lifecycle_config_validates_trusted_bindings(tmp_path):
    config = tmp_path / "integration.json"
    config.write_text(canonical({"memory_lifecycle": {
        "policy": {"enabled": False},
        "bindings": [{"project_id": "proj-a", "workspace": str(tmp_path)}]}}))
    result = integration_options(config)
    options = result["memory_lifecycle_automation"]
    assert options["policy"].enabled is False
    assert len(options["bindings"]) == 1
    assert options["use_local_semantic"] is False
    with pytest.raises(ValueError):
        (tmp_path / "bad.json").write_text(canonical({"memory_lifecycle": {"use_local_semantic": "yes"}}))
        integration_options(tmp_path / "bad.json")


def test_install_rejects_conflicting_encoder_selection(world):
    with pytest.raises(ValueError, match="not both"):
        install(world.app, world.app.state.service, lambda: None, None, lambda d: d,
                encoder=object(), use_local_semantic=True)


def test_install_with_local_semantic_requires_loaded_encoder(world):
    world.app.state.service.indexes.encoder = None
    with pytest.raises(ValueError, match="loaded local encoder"):
        install(world.app, world.app.state.service, lambda: None, None, lambda d: d,
                use_local_semantic=True)
