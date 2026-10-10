import hashlib
import json
import sys

import pytest

from hivemind.context_assembly import default_counter, reset_tokenizer_cache
from hivemind.model_wiring import AuthorizedProcessExtractor, runtime_counter


def test_authorized_offline_extractor_protocol_and_disabled_inertness(tmp_path):
    script = "import json,sys; data=json.load(sys.stdin); print(json.dumps({'proposals': []}))"
    options = {"argv": [sys.executable, "-c", script], "workspace": tmp_path,
               "model_id": "operator-local-model", "require_os_isolation": False}
    with pytest.raises(PermissionError):
        AuthorizedProcessExtractor(**options)("Untrusted evidence")
    result = AuthorizedProcessExtractor(**options, enabled=True, allow_model_calls=True)("Untrusted evidence")
    assert json.loads(result) == {"proposals": []}


def test_hash_bound_runtime_tokenizer_and_unavailable_target_fallback(tmp_path, monkeypatch):
    from tokenizers import Tokenizer, models, pre_tokenizers
    tokenizer = Tokenizer(models.WordLevel({"[UNK]": 0, "evidence": 1}, unk_token="[UNK]"))
    tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()
    path = tmp_path / "tokenizer.json"
    tokenizer.save(str(path))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    counter = runtime_counter("runtime/model", path, digest)
    assert counter("evidence evidence") == 2 and counter.exact_for_target
    with pytest.raises(ValueError):
        runtime_counter("runtime/model", path, "0" * 64)
    monkeypatch.setenv("HF_HOME", str(tmp_path))
    reset_tokenizer_cache()
    fallback = default_counter("not-installed/runtime-model")
    assert fallback.fallback and not fallback.exact_for_target
