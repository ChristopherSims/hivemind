"""Tests for runtime-specific exact token budgeting (``hivemind/retrieval_runtime.py``).

All counters here are local fixtures: a tiny WordLevel ``tokenizer.json`` written into a
throwaway HF-style cache directory. No network access, no model downloads.
"""

import hashlib
import json

import pytest

from hivemind import context_assembly as ca
from hivemind import retrieval_runtime as rr
from hivemind.storage import Problem, canonical

from test_context_assembly import memory_body, principal_for, request_for

FIXTURE_MODEL = "fixture/tiny-wordlevel"
FIXTURE_TOKENIZER = {
    "version": "1.0", "truncation": None, "padding": None, "added_tokens": [],
    "normalizer": None, "pre_tokenizer": {"type": "Whitespace"}, "post_processor": None,
    "decoder": {"type": "WordPiece", "prefix": "##", "cleanup": True},
    "model": {"type": "WordLevel",
              "vocab": {"hello": 0, "world": 1, "zebra": 2, "token": 3, "[UNK]": 4},
              "unk_token": "[UNK]"},
}


@pytest.fixture
def fixture_cache(tmp_path, monkeypatch):
    """A local HF-style cache holding exactly one tiny tokenizer, hash known."""
    snapshot = tmp_path / "hub" / "models--fixture--tiny-wordlevel" / "snapshots" / "abc123"
    snapshot.mkdir(parents=True)
    path = snapshot / "tokenizer.json"
    path.write_text(json.dumps(FIXTURE_TOKENIZER))
    monkeypatch.setenv("HIVEMIND_TOKENIZER_CACHE", str(tmp_path / "hub"))
    monkeypatch.setenv("HF_HUB_CACHE", str(tmp_path / "hub"))
    # Hide any ambient developer-machine HF cache so resolution is fully fixture-bound.
    monkeypatch.setenv("HF_HOME", str(tmp_path / "empty-home"))
    monkeypatch.setattr(ca.Path, "home", staticmethod(lambda: tmp_path / "no-home"))
    ca.reset_tokenizer_cache()
    rr.reset_registry()
    yield rr.default_registry()
    ca.reset_tokenizer_cache()
    rr.reset_registry()


def _fixture_digest():
    return hashlib.sha256(json.dumps(FIXTURE_TOKENIZER).encode()).hexdigest()


def test_local_fixture_counter_is_exact_and_model_bound(fixture_cache):
    registry = fixture_cache
    counter = registry.resolve(FIXTURE_MODEL, require_exact=True)
    assert counter.method == "local_tokenizer"
    assert counter.exact_for_target is True
    assert counter.fallback is False
    assert counter("hello world") == 2
    assert counter("") == 0

    # Operator hash binding: correct pin resolves, wrong pin refuses.
    pinned = registry.resolve(FIXTURE_MODEL, model_hash=_fixture_digest(), require_exact=True)
    assert pinned("zebra") == 1
    with pytest.raises(Problem) as failure:
        registry.resolve(FIXTURE_MODEL, model_hash="0" * 64, require_exact=True)
    assert failure.value.code == "exact_counter_unavailable"
    assert failure.value.status == 422


def test_exact_count_covers_unicode_and_differs_from_bytes(fixture_cache):
    registry = fixture_cache
    counter = registry.resolve(FIXTURE_MODEL, require_exact=True)
    # Unknown Unicode maps to the fixture's [UNK] token: one token, many UTF-8 bytes.
    assert counter("🦄") == 1
    assert ca.utf8_counter("🦄") == 4
    assert counter("hello 🦄 zebra") == 3
    # count_exact measures the canonical serialization, including JSON syntax.
    value = {"b": [1, "hello"], "a": "zebra"}
    expected = counter(canonical(value))
    assert registry.count_exact(value, FIXTURE_MODEL) == expected
    assert canonical(value) == '{"a":"zebra","b":[1,"hello"]}'


def test_exact_refused_before_execution_when_unavailable(fixture_cache):
    registry = fixture_cache
    for target in ("vendor/hidden-model", "anthropic/claude", "cl100k_base", None):
        # No counter can be bound to these targets: no prepared encodings are
        # registered, Anthropic counting needs an external API (never called), and the
        # registry never invokes tiktoken constructors that could lazily download.
        with pytest.raises(Problem) as failure:
            registry.resolve(target, require_exact=True)
        assert failure.value.code == "exact_counter_unavailable"
        with pytest.raises(Problem):
            registry.count_exact({"prompt": "hello"}, target)
    # Without require_exact the resolver degrades to the honest labeled fallback.
    fallback = registry.resolve("vendor/hidden-model")
    assert fallback.fallback is True
    assert fallback.exact_for_target is False
    assert fallback.method == "conservative_utf8_bytes_upper_bound"


class FakeEncoding:
    """Stand-in for an operator-prepared tiktoken.Encoding (whitespace tokens)."""

    def __init__(self):
        self.encoded = []

    def encode(self, text):
        tokens = text.split()
        self.encoded.append(text)
        return list(range(len(tokens)))


class CorruptEncoding:
    def encode(self, text):
        raise OSError("rank data corrupt")


def test_prepared_vendor_encodings_bind_exactly_without_network(fixture_cache, monkeypatch):
    """Operator-prepared encodings resolve exactly; resolution performs no network and
    no filesystem cache reads/writes/deletes."""

    def no_network(*args, **kwargs):
        raise AssertionError("network access attempted")

    import socket
    import urllib.request
    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    monkeypatch.setattr(urllib.request, "urlopen", no_network)

    registry = rr.CounterRegistry()
    registry.register_encoding("cl100k_base", FakeEncoding(),
                               models=("gpt-4", "gpt-4o-mini"))
    counter = registry.resolve("gpt-4", require_exact=True)
    assert counter.method == "vendor_prepared_encoding"
    assert counter.exact_for_target is True
    assert counter("hello world zebra") == 3
    assert registry.count_exact({"a": "hello"}, "gpt-4") == counter(canonical({"a": "hello"}))
    # Unregistered encoding names/models stay honestly unavailable.
    with pytest.raises(Problem):
        registry.resolve("gpt-3.5-turbo", require_exact=True)


def test_corrupt_or_incompatible_encodings_refused_at_registration():
    registry = rr.CounterRegistry()
    with pytest.raises(Problem) as failure:
        registry.register_encoding("broken", CorruptEncoding())
    assert failure.value.code == "encoding_invalid"
    with pytest.raises(Problem):
        registry.register_encoding("empty", object())
    assert "broken" not in registry._encodings
    # A corrupt encoding can never be executed later: registration failed before any
    # counter could be built from it.
    with pytest.raises(Problem) as failure:
        registry.resolve("broken", require_exact=True)
    assert failure.value.code == "exact_counter_unavailable"


def test_resolution_never_imports_or_constructs_tiktoken(fixture_cache, monkeypatch):
    import builtins
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name.split(".")[0] == "tiktoken":
            raise AssertionError("registry must never import tiktoken")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    registry = rr.CounterRegistry()
    with pytest.raises(Problem):
        registry.resolve("cl100k_base", require_exact=True)
    fallback = registry.resolve("cl100k_base")
    assert fallback.fallback is True


def test_injected_counter_must_be_exact_when_required(fixture_cache):
    registry = fixture_cache
    exact = registry.resolve(FIXTURE_MODEL, require_exact=True)
    assert registry.resolve(FIXTURE_MODEL, require_exact=True, counter=exact) is exact
    # Bare callables are trusted as exact by the injector (context_assembly semantics).
    assert registry.resolve(FIXTURE_MODEL, require_exact=True, counter=len)("ab") == 2
    # But a counter that declares itself non-exact for the target is refused.
    inexact = ca.Counter("other", len, "injected", False, True)
    with pytest.raises(Problem) as failure:
        registry.resolve(FIXTURE_MODEL, require_exact=True, counter=inexact)
    assert failure.value.code == "exact_counter_unavailable"


def test_budget_serialized_distinguishes_packet_from_vendor_overhead(fixture_cache):
    registry = fixture_cache
    packet = {"prompt": "hello world", "items": [{"text": "zebra"}]}
    result = rr.budget_serialized(packet, target_tokenizer=FIXTURE_MODEL, budget=1000)
    assert result["exact_for_target"] is True
    assert result["covers_serialized_canonical_packet"] is True
    assert result["vendor_overhead"]["included"] is False
    assert "hidden" in result["vendor_overhead"]["note"]
    assert result["token_count"] == registry.resolve(FIXTURE_MODEL)(canonical(packet))
    assert result["fits"] is True
    tight = rr.budget_serialized(packet, target_tokenizer=FIXTURE_MODEL, budget=1)
    assert tight["fits"] is False
    open_ended = rr.budget_serialized(packet, target_tokenizer=FIXTURE_MODEL)
    assert open_ended["fits"] is None
    # The wrap seam is measured, not just the payload.
    wrapped = rr.budget_serialized(packet, target_tokenizer=FIXTURE_MODEL,
                                   wrap=lambda data: {"protocol": "hivemind", "data": data})
    assert wrapped["token_count"] > result["token_count"]


def test_describe_operations_is_self_describing(fixture_cache):
    operations = {op["name"]: op for op in rr.describe_operations()}
    assert {"retrieval.search", "retrieval.search_page", "counter.resolve",
            "counter.count_exact", "counter.budget_serialized",
            "context.assemble"} <= set(operations)
    for op in operations.values():
        for key in ("purpose", "model_binding", "privacy", "input", "output",
                    "exactness", "limits"):
            assert op[key], (op["name"], key)
    counters = {entry["name"]: entry for entry in fixture_cache.describe_counters()}
    assert counters["local_hf_tokenizer"]["available"] is True
    assert counters["anthropic"]["available"] is False
    assert "external API" in counters["anthropic"]["limits"]
    assert "operator-registered" in counters["tiktoken"]["model_binding"]
    assert counters["utf8_bytes_upper_bound"]["exactness"].startswith("never exact")


def test_pinned_hash_loads_exact_verified_bytes(fixture_cache, tmp_path, monkeypatch):
    """TOCTOU: the tokenizer is parsed from the same bytes that were hashed, never
    re-read from the path afterwards, and a swapped file is refused."""
    import tokenizers

    def forbidden_from_file(*args, **kwargs):
        raise AssertionError("from_file must not be used for pinned loads")

    monkeypatch.setattr(tokenizers.Tokenizer, "from_file", staticmethod(forbidden_from_file))
    registry = rr.CounterRegistry()
    counter = registry.resolve(FIXTURE_MODEL, model_hash=_fixture_digest(),
                               require_exact=True)
    assert counter("hello world") == 2  # parsed from the verified bytes

    # Swap the file after cache reset: the pinned hash no longer matches and there is
    # no fallback to any other candidate path.
    snapshot = tmp_path / "hub" / "models--fixture--tiny-wordlevel" / "snapshots" / "abc123"
    swapped = json.loads(json.dumps(FIXTURE_TOKENIZER))
    swapped["model"]["vocab"]["extra"] = 5
    (snapshot / "tokenizer.json").write_text(json.dumps(swapped))
    rr.reset_registry()
    ca.reset_tokenizer_cache()
    registry = rr.CounterRegistry()
    with pytest.raises(Problem) as failure:
        registry.resolve(FIXTURE_MODEL, model_hash=_fixture_digest(), require_exact=True)
    assert failure.value.code == "exact_counter_unavailable"

    # Corrupt bytes whose hash matches the pin still refuse: parse failure, no fallback.
    corrupt = b'{"not": "a tokenizer'
    digest = hashlib.sha256(corrupt).hexdigest()
    (snapshot / "tokenizer.json").write_bytes(corrupt)
    rr.reset_registry()
    ca.reset_tokenizer_cache()
    with pytest.raises(Problem):
        rr.CounterRegistry().resolve(FIXTURE_MODEL, model_hash=digest, require_exact=True)


def test_context_install_uses_operator_registry_for_exact_policy(world, fixture_cache):
    """ca.install(registry=..., require_exact=True) resolves through the configured
    registry (not the process default) and fails closed for unbound targets."""
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.responses import JSONResponse
    from fastapi.testclient import TestClient

    from hivemind.api import envelope, hash_token as api_hash_token
    from test_context_assembly import put

    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Registry seam")
    run = world.run(agent)
    put(store, "memory", world.project, principal["id"], memory_body(
        world.project, run, title="Seam memory", text="hello world seam"))

    app = FastAPI()

    @app.exception_handler(Problem)
    async def problem_handler(request, exc):
        return JSONResponse({"error": {"code": exc.code}}, status_code=exc.status)

    def principal_dependency(request: Request):
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="authentication_required")
        row = store.db.execute("SELECT * FROM principals WHERE token_hash=?",
                               (api_hash_token(auth[7:]),)).fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="authentication_required")
        return dict(row)

    registry = fixture_cache  # operator-configured registry bound to the fixture model
    ca.install(app, world.app.state.service, principal_dependency, None, envelope,
               require_exact=True, registry=registry)
    client = TestClient(app)
    headers = {"Authorization": "Bearer " + world.owner}
    body = {"project_id": world.project,
            "agent_selector": {"agent_ids": [agent["id"]], "active_only": False},
            "token_budget": 8000, "include": ["memory"]}

    ok = client.post("/v1/context:assemble", headers=headers,
                     json={**body, "target_tokenizer": FIXTURE_MODEL})
    assert ok.status_code == 200
    assert ok.json()["hivemind_detached_work"] is True
    packet = ok.json()["data"]
    assert packet["tokenization"]["exact_for_target_tokenizer"] is True
    assert packet["tokenization"]["method"] == "local_tokenizer"
    # The marker is part of the measured envelope: token_count covers the exact
    # response bytes.
    counter = registry.resolve(FIXTURE_MODEL, require_exact=True)
    assert packet["token_count"] == counter(canonical(ok.json()))

    # Unbound target: strict policy fails before execution, not a silent fallback.
    refused = client.post("/v1/context:assemble", headers=headers,
                          json={**body, "target_tokenizer": "vendor/hidden-model"})
    assert refused.status_code == 422
    assert refused.json()["error"]["code"] == "exact_counter_unavailable"

    # No target at all under strict policy: refused (nothing to be exact for).
    refused_none = client.post("/v1/context:assemble", headers=headers, json=body)
    assert refused_none.status_code == 422


def test_assemble_require_exact_fails_before_any_scan(world, fixture_cache, monkeypatch):
    principal = principal_for(world)
    agent = world.agent("Exact refuser")

    def forbidden_scan(*args, **kwargs):
        raise AssertionError("candidate scan executed despite missing exact counter")

    monkeypatch.setattr(ca, "_scan", forbidden_scan)
    request = request_for(world, agent["id"], target_tokenizer="vendor/hidden-model")
    with pytest.raises(Problem) as failure:
        ca.assemble(world.app.state.service, principal, request, require_exact=True)
    assert failure.value.code == "exact_counter_unavailable"
    assert failure.value.status == 422


def test_assemble_require_exact_measures_packet_exactly(world, fixture_cache):
    store = world.app.state.store
    principal = principal_for(world)
    agent = world.agent("Exact assembler")
    run = world.run(agent)
    from test_context_assembly import put
    for index in range(5):
        put(store, "memory", world.project, principal["id"], memory_body(
            world.project, run, title=f"Memory {index}", text="hello world"))

    request = request_for(world, agent["id"], target_tokenizer=FIXTURE_MODEL, budget=6000)
    packet = ca.assemble(world.app.state.service, principal, request, require_exact=True)
    assert packet["tokenization"]["exact_for_target_tokenizer"] is True
    assert packet["tokenization"]["method"] == "local_tokenizer"
    assert packet["tokenization"]["vendor_overhead"]["included"] is False
    counter = rr.default_registry().resolve(FIXTURE_MODEL, require_exact=True)
    assert packet["token_count"] == counter(canonical(ca.default_envelope(packet)))
    assert packet["token_count"] <= request.token_budget
    assert packet["items"]
