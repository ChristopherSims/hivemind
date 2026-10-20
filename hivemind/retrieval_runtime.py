"""Runtime-specific exact token budgeting for retrieval and context surfaces.

This module owns the counter registry used to budget retrieval results and assembled
context packets *exactly* where a verified tokenizer is available, and to refuse exact
budgeting -- before any retrieval or assembly work executes -- where it is not.

Guarantees and honest limits
----------------------------
* Exactness is always relative to a named, verifiable tokenizer. A counter is only
  reported ``exact_for_target`` when the tokenizer it uses is bound to the requested
  target by the *operator*, never selected from client/API input:
  - a locally cached Hugging Face ``tokenizer.json`` for the requested model id,
    optionally pinned by an operator-supplied SHA-256 of the tokenizer file (the exact
    hashed file is the file loaded); or
  - a vendor encoding supplied by the operator as a *prepared encoding object* (for
    example a ``tiktoken.Encoding`` constructed under operator control). This module
    never imports tiktoken, never calls ``get_encoding``/``encoding_for_model`` or any
    other constructor that can lazily download encoding data, and never touches,
    deletes, or repairs cache files. Vendors without a locally verifiable offline
    tokenizer (for example Anthropic, whose counting requires an external API call that
    this module never performs) are reported unavailable.
* No network access, no model or tokenizer downloads, no argv/module selection from
  API input, anywhere in this module.
* An exact count covers the *serialized canonical packet* (``storage.canonical`` JSON)
  exactly as returned by the query/assembly seam. Vendor-side hidden system prompts,
  tool schemas, and other API overhead are never measurable locally and are always
  reported separately as excluded.
* The conservative UTF-8 byte upper bound is a non-exact fallback; it never satisfies
  ``require_exact``. The embedding tokenizer is never substituted for a runtime
  tokenizer.

Parent wiring seams
-------------------
* ``CounterRegistry(encodings={...})`` / ``register_encoding(name, obj, models=...)``
  is how an operator binds prepared vendor encodings.
* ``default_registry().resolve(target, require_exact=...)`` returns a
  ``context_assembly.Counter`` suitable for ``context_assembly.assemble(..., counter=)``
  / ``install(..., counters={target: counter})``.
* ``context_assembly.assemble(..., require_exact=True)`` resolves through this module
  and raises before scanning when no exact counter exists for the target.
* ``budget_serialized(value, ...)`` measures any fully serialized prompt/context object
  at an existing seam and reports exactness and overhead scope.
* ``install(app, service, principal, command, envelope, ...)`` registers the
  read-only retrieval routes (``POST /v1/retrieval:search-page`` with operator-fixed
  scan policy, ``GET /v1/retrieval:operations``) on a FastAPI app.
* ``OPERATION_DESCRIPTORS`` / ``describe_operations()`` /
  ``CounterRegistry.describe_counters()`` are the self-describing listings for parent
  discovery surfaces (tool manifests, MCP).
"""

from __future__ import annotations

import hashlib
import threading

from pydantic import Field

from . import models as m
from . import context_assembly as ca
from .storage import Problem, canonical

# Documented vendor counter availability. This module never performs external API
# calls and never constructs vendor encodings itself.
VENDOR_COUNTERS = {
    "tiktoken": {
        "purpose": "OpenAI token encodings (cl100k_base, o200k_base, named OpenAI models)",
        "model_binding": "operator-registered prepared tiktoken.Encoding objects only; "
                         "this module never calls get_encoding/encoding_for_model because "
                         "they can lazily download encoding data",
        "privacy": "fully local; no network, no cache reads/writes by this module",
        "exactness": "exact for the registered encoding of the serialized text; excludes "
                     "hidden vendor system/tool overhead",
        "unavailable_reason": "no operator-registered prepared encoding for the target",
    },
    "anthropic": {
        "purpose": "Anthropic Claude token counting",
        "model_binding": "none available offline",
        "privacy": "n/a",
        "exactness": "not exact offline: token counting requires an external API call, "
                     "which this module never performs",
        "unavailable_reason": "no offline-verifiable tokenizer; external API unsupported",
    },
}

_REGISTRY_LOCK = threading.Lock()
_REGISTRY = None


def _constant_time_equal(a, b):
    import hmac
    return hmac.compare_digest(a, b)


def _load_exact_tokenizer_file(path, label, model_hash=None):
    """Load the *exact* tokenizer bytes whose hash was verified (TOCTOU-safe).

    The file is read once, hashed, and the tokenizer is parsed *from the same verified
    bytes* (``Tokenizer.from_buffer``) -- never from the path afterwards, so a file
    swapped between hashing and parsing cannot slip in. When ``model_hash`` is None the
    bytes are still parsed from a single read. No fallback to other candidate files; a
    mismatch or parse failure means unavailable.
    """
    from tokenizers import Tokenizer
    data = path.read_bytes()
    if model_hash is not None:
        digest = hashlib.sha256(data).hexdigest()
        if not _constant_time_equal(digest, model_hash.lower()):
            return None
    tokenizer = Tokenizer.from_buffer(data)
    for disable in ("no_padding", "no_truncation"):
        try:
            getattr(tokenizer, disable)()
        except Exception:
            pass

    def count(text, tokenizer=tokenizer):
        return len(tokenizer.encode(text, add_special_tokens=False).ids)

    return ca.Counter(label, count, "local_tokenizer", True, False)


class CounterRegistry:
    """Registry of token counters with explicit operator-side model binding."""

    def __init__(self, encodings=None):
        self._encodings = {}
        self._counters = {}
        self._tokenizer_cache = {}
        for name, spec in (encodings or {}).items():
            if isinstance(spec, dict):
                self.register_encoding(name, spec["encoding"], models=spec.get("models", ()))
            else:
                self.register_encoding(name, spec)

    # -- operator bindings ---------------------------------------------------

    def register_encoding(self, name, encoding, models=()):
        """Bind an operator-prepared encoding object (e.g. ``tiktoken.Encoding``).

        The object must expose ``encode(text) -> list[int]``; a trivial self-test encode
        runs at registration so corrupt/incompatible objects are refused before they can
        be used. No vendor constructors are called here, so no lazy downloads, cache
        reads, or cache deletions can occur.
        """
        encode = getattr(encoding, "encode", None)
        if not callable(encode):
            raise Problem("encoding_invalid", 422)
        try:
            probe = encode("health check")
        except Exception:
            raise Problem("encoding_invalid", 422) from None
        if not isinstance(probe, (list, tuple)) or not all(
                isinstance(t, int) for t in probe) or not probe:
            raise Problem("encoding_invalid", 422)
        self._encodings[name] = {"encoding": encoding, "models": tuple(models or ())}

    def register_counter(self, target_tokenizer, counter):
        """Bind an operator-approved ``Counter`` to a target tokenizer id."""
        self._counters[target_tokenizer] = counter

    # -- counter factories ---------------------------------------------------

    def _local_tokenizer(self, target_tokenizer, model_hash=None):
        """Operator hash/model-bound local tokenizer from the HF cache (no network).

        Only a concrete ``target_tokenizer`` can bind: without one there is nothing the
        count could be exact *for*, so ``None`` is returned. When ``model_hash`` pins
        the tokenizer, the exact file whose SHA-256 matched is the file loaded.
        """
        if not target_tokenizer:
            return None
        for label, path in ca._candidate_tokenizer_files(target_tokenizer):
            if label != target_tokenizer:
                continue
            key = (str(path), (model_hash or "").lower())
            if key not in self._tokenizer_cache:
                try:
                    self._tokenizer_cache[key] = _load_exact_tokenizer_file(
                        path, label, model_hash)
                except Exception:
                    self._tokenizer_cache[key] = None
            counter = self._tokenizer_cache[key]
            if counter is not None:
                return counter
            if model_hash is not None:
                return None  # the pinned file failed; never fall back to another file
        return None

    def _prepared_encoding_counter(self, target_tokenizer):
        """Resolve an operator-registered prepared encoding for the target. Local only."""
        if not target_tokenizer:
            return None
        for name, entry in self._encodings.items():
            if target_tokenizer != name and target_tokenizer not in entry["models"]:
                continue
            encoding = entry["encoding"]

            def count(text, encoding=encoding):
                return len(encoding.encode(text))

            return ca.Counter("prepared_encoding:" + name, count,
                              "vendor_prepared_encoding", True, False)
        return None

    # -- public API ----------------------------------------------------------

    def resolve(self, target_tokenizer=None, *, model_hash=None, require_exact=False,
                counter=None):
        """Resolve the best counter for ``target_tokenizer``.

        ``counter`` (callable or ``Counter``) short-circuits resolution as an injected
        counter, matching ``context_assembly`` semantics (a bare callable is trusted as
        exact by its injector; a ``Counter`` self-declared non-exact is refused under
        ``require_exact``). When ``require_exact`` is set and no counter bound to the
        requested target exists, raises ``Problem("exact_counter_unavailable", 422)``
        *before* any counting or retrieval work -- callers must treat this as
        fail-before-execution.
        """
        wrapped = ca.wrap_counter(counter)
        if wrapped is not None:
            if require_exact and not wrapped.exact_for_target:
                raise Problem("exact_counter_unavailable", 422)
            return wrapped
        resolved = self._counters.get(target_tokenizer)
        if resolved is None:
            resolved = self._local_tokenizer(target_tokenizer, model_hash)
        if resolved is None:
            resolved = self._prepared_encoding_counter(target_tokenizer)
        if resolved is not None and require_exact and not resolved.exact_for_target:
            # A tokenizer that is not bound to the requested target is not exact for it.
            raise Problem("exact_counter_unavailable", 422)
        if resolved is None:
            if require_exact:
                raise Problem("exact_counter_unavailable", 422)
            resolved = ca.Counter("utf8_bytes_upper_bound", ca.utf8_counter,
                                  "conservative_utf8_bytes_upper_bound", False, True)
        return resolved

    def count_exact(self, value, target_tokenizer, *, model_hash=None):
        """Exact token count of the canonical serialization of ``value``.

        Raises ``Problem("exact_counter_unavailable", 422)`` when no verified counter
        exists for ``target_tokenizer``; nothing is counted or executed in that case.
        """
        counter = self.resolve(target_tokenizer, model_hash=model_hash, require_exact=True)
        text = value if isinstance(value, str) else canonical(value)
        return counter(text)

    def describe_counters(self):
        """Self-describing counter listing (plain data) for parent discovery surfaces."""
        entries = [{
            "name": "local_hf_tokenizer",
            "purpose": "exact counting with a locally cached Hugging Face tokenizer.json",
            "model_binding": "target tokenizer id (org/model); optional operator SHA-256 pin "
                             "of the tokenizer.json file binds the count to one exact "
                             "artifact, and the hashed file is the file loaded",
            "privacy": "fully local; files read only from the local HF cache; no network",
            "available": ca.local_tokenizer_available(),
            "exactness": "exact for the bound tokenizer; unknown for any other model",
            "limits": "requires the tokenizer to already exist in the local cache; never "
                      "downloads",
        }]
        for name, spec in VENDOR_COUNTERS.items():
            available = bool(self._encodings) if name == "tiktoken" else False
            entries.append({"name": name, "purpose": spec["purpose"],
                            "model_binding": spec["model_binding"], "privacy": spec["privacy"],
                            "available": available, "exactness": spec["exactness"],
                            "limits": spec["unavailable_reason"] if not available else
                            "covers serialized text only; vendor hidden overhead excluded"})
        entries.append({
            "name": "utf8_bytes_upper_bound",
            "purpose": "conservative fallback upper bound (one token per UTF-8 byte)",
            "model_binding": "none", "privacy": "fully local", "available": True,
            "exactness": "never exact; over-counts. Cannot satisfy require_exact.",
            "limits": "fallback only",
        })
        return entries


def default_registry():
    global _REGISTRY
    with _REGISTRY_LOCK:
        if _REGISTRY is None:
            _REGISTRY = CounterRegistry()
        return _REGISTRY


def reset_registry():
    """Drop the process-wide registry (tests)."""
    global _REGISTRY
    with _REGISTRY_LOCK:
        _REGISTRY = None


def budget_serialized(value, *, counter=None, target_tokenizer=None, model_hash=None,
                      budget=None, wrap=None, registry=None):
    """Measure the exact token cost of a fully serialized prompt/context packet.

    Measures the canonical serialization of ``wrap(value)`` (``wrap`` defaults to
    identity; pass the response/prompt envelope used at the seam). The returned
    interface distinguishes the serialized canonical packet count from vendor hidden
    overhead, which is always excluded and never estimated.
    """
    registry = registry or default_registry()
    selected = registry.resolve(target_tokenizer, model_hash=model_hash, counter=counter)
    payload = wrap(value) if wrap else value
    text = payload if isinstance(payload, str) else canonical(payload)
    measured = selected(text)
    return {
        "token_count": measured,
        "counter_id": selected.id,
        "method": selected.method,
        "exact_for_target": bool(selected.exact_for_target),
        "fallback": bool(selected.fallback),
        "budget": budget,
        "fits": (measured <= budget) if budget is not None else None,
        "covers_serialized_canonical_packet": True,
        "vendor_overhead": {
            "included": False,
            "note": ("Count covers only the serialized packet exactly as passed. Vendor-side "
                     "hidden system prompts, tool schemas, and API overhead are not observable "
                     "locally and are excluded; reserve headroom separately."),
        },
    }


OPERATION_DESCRIPTORS = [
    {
        "name": "retrieval.search",
        "purpose": "hybrid lexical+semantic authorized retrieval over project resources",
        "implementation": "hivemind.indexes.Indexes.search",
        "model_binding": "semantic ranking uses the pinned local EmbeddingGemma profile "
                         "eg2-text-768-v1; lexical mode needs no model",
        "privacy": "candidates are pre-authorized with visibility pushed into canonical "
                   "SQL before ranking; every result is rechecked against canonical "
                   "authorization and revision at materialization; scan counts and "
                   "truncation flags cover only rows the principal may see",
        "input": "SearchQuery {project_id, query, mode, limit<=50, resource_kinds, "
                 "min_ingestion_seq}; optional scan_limit",
        "output": "{results, freshness, embedding_profile, lexical_fallback, scan, "
                  "warnings}; scan.authorized_candidates counts authorized candidates only",
        "exactness": "exact sqlite-vec cosine over the authorized set (no ANN); RRF fusion; "
                     "deterministic (-score, resource_id) order",
        "limits": "candidate history bounded by scan_limit (default 20000, max 200000 per "
                  "kind, visible rows only), ranking window by rank_depth (default 4096, "
                  "max 65536), wall clock by a scan time budget; all exhaustion is flagged "
                  "via scan.scan_truncated / scan.rank_truncated / scan.time_budget_exceeded",
    },
    {
        "name": "retrieval.search_page",
        "purpose": "stable keyset-paginated authorized top-k over ranked history (>1000)",
        "implementation": "hivemind.indexes.Indexes.search_page",
        "model_binding": "same as retrieval.search",
        "privacy": "cursors are HMAC-signed with a process-local key and bound to the "
                   "authenticated principal, project, query, and scan policy; tamper, "
                   "cross-principal replay, or cross-query reuse fails with "
                   "cursor_invalid/cursor_query_mismatch before any work; cursors carry "
                   "only the last emitted (score, authorized id)",
        "input": "SearchQuery + optional cursor + optional scan_limit/rank_depth",
        "output": "{results, next_cursor, page, has_more, exhausted, freshness, scan, "
                  "warnings}",
        "exactness": "deterministic order (-score, resource_id); identical pages for "
                     "identical canonical state; pages are not a snapshot -- results "
                     "reflect canonical state rechecked at materialization",
        "limits": "cursors are ephemeral and fail closed after a service restart; "
                  "has_more is a hint -- a later page may be empty if candidates fail "
                  "canonical recheck; ranking beyond rank_depth is flagged via "
                  "exhausted.rank_truncated",
    },
    {
        "name": "counter.resolve",
        "purpose": "resolve a token counter bound to a target tokenizer/model",
        "implementation": "hivemind.retrieval_runtime.CounterRegistry.resolve",
        "model_binding": "operator-side only: local HF tokenizer id with optional SHA-256 "
                         "pin of the exact tokenizer.json, or operator-registered prepared "
                         "vendor encoding objects; API input can never select tokenizer "
                         "files, modules, or hashes",
        "privacy": "local only; no network, no downloads, no cache mutation, no telemetry",
        "input": "target_tokenizer, optional model_hash, require_exact flag",
        "output": "context_assembly.Counter with id/method/exact_for_target/fallback",
        "exactness": "exact only when bound to the requested target; require_exact raises "
                     "exact_counter_unavailable before execution otherwise",
        "limits": "vendors without a locally verifiable offline tokenizer (e.g. Anthropic) "
                  "are unavailable; external counting APIs are never called",
    },
    {
        "name": "counter.count_exact",
        "purpose": "exact token count of a canonically serialized value",
        "implementation": "hivemind.retrieval_runtime.CounterRegistry.count_exact",
        "model_binding": "as counter.resolve",
        "privacy": "local only",
        "input": "JSON-serializable value or string + target_tokenizer (+ model_hash)",
        "output": "integer token count",
        "exactness": "exact for the serialized canonical packet; excludes vendor hidden "
                     "overhead",
        "limits": "raises exact_counter_unavailable before any counting when no exact "
                  "counter exists",
    },
    {
        "name": "counter.budget_serialized",
        "purpose": "measure a fully serialized prompt/context packet against a budget",
        "implementation": "hivemind.retrieval_runtime.budget_serialized",
        "model_binding": "as counter.resolve",
        "privacy": "local only",
        "input": "value, optional wrap envelope, counter, target_tokenizer, budget",
        "output": "{token_count, counter_id, method, exact_for_target, budget, fits, "
                  "covers_serialized_canonical_packet, vendor_overhead}",
        "exactness": "reports whether the count is exact for the target; vendor overhead "
                     "is always excluded and labeled",
        "limits": "fits is null without a budget",
    },
    {
        "name": "context.assemble",
        "purpose": "bounded, envelope-inclusive exact-budgeted context packet assembly",
        "implementation": "hivemind.context_assembly.assemble/install",
        "model_binding": "counter resolved via counter.resolve; require_exact=True fails "
                         "before scanning when no exact counter is bound to "
                         "target_tokenizer",
        "privacy": "candidates stream from canonical SQL with visibility pushed into the "
                   "query (scan counts never include hidden rows); candidate stubs carry "
                   "no bodies; bodies are materialized with canonical recheck only for "
                   "placed items; items are labeled untrusted evidence",
        "input": "ContextQuery; assemble/install accept counter/counters, scan_limit, "
                 "wrap, require_exact",
        "output": "packet with token_count measured on the fully wrapped packet and "
                  "tokenization metadata including vendor_overhead scope",
        "exactness": "token_count equals the counter measurement of the returned packet; "
                     "exact_for_target_tokenizer reports tokenizer binding",
        "limits": "scan_limit default 20000, max 200000 per kind; candidate ranking window "
                  "bounded to 4096 stubs (flagged via scan.candidates_truncated); "
                  "token_budget 100-32000",
    },
]


def describe_operations():
    """Self-describing query/counter operations for parent discovery/MCP manifests."""
    return [dict(op) for op in OPERATION_DESCRIPTORS]


class SearchPageBody(m.SearchQuery):
    """``search_page`` request. Scan policy is operator-fixed by ``install``; clients
    can only supply the cursor, never scan limits, tokenizer files, or hashes."""
    cursor: str | None = Field(default=None, max_length=512)


def install(app, service, principal, command, envelope, counters=None, scan_limit=None,
            rank_depth=None, registry=None):
    """Register the read-only retrieval surface on ``app``.

    * ``POST /v1/retrieval:search-page`` -- paginated authorized search. The operator
      fixes ``scan_limit``/``rank_depth`` policy; clients cannot widen it. The search
      runs in a worker thread (``asyncio.to_thread``) and the response carries
      ``hivemind_detached_work: True`` so the parent can confirm no canonical/global
      request mutex was held during compute.
    * ``GET /v1/retrieval:operations`` -- self-describing operation/counter listing
      for discovery.

    ``counters`` (mapping of target tokenizer id -> Counter/callable) binds
    operator-approved counters into the registry; ``registry`` overrides the process
    default. ``command`` is accepted for signature parity with
    ``context_assembly.install`` (retrieval is read-only).
    """
    import asyncio

    from fastapi import Depends

    reg = registry or default_registry()
    for target, counter in (counters or {}).items():
        # Operator-bound counters, keyed by target; never selected from API input.
        reg.register_counter(target, ca.wrap_counter(counter))

    @app.post("/v1/retrieval:search-page")
    async def retrieval_search_page(body: SearchPageBody, p=Depends(principal)):
        service.project_access(p, body.project_id)
        result = await asyncio.to_thread(
            service.indexes.search_page, p, body, body.cursor, scan_limit, rank_depth)
        result["hivemind_detached_work"] = True
        return envelope(result)

    @app.get("/v1/retrieval:operations")
    async def retrieval_operations(p=Depends(principal)):
        return envelope({"operations": describe_operations(),
                         "counters": reg.describe_counters()})

    # Two-phase detached handler: the parent API middleware skips its global
    # request/store lock for marked endpoints; indexes.search_page takes its own
    # short canonical snapshots, so marking prevents a self-deadlock where the
    # middleware-held lock blocks the worker thread.
    retrieval_search_page.hivemind_detached_work = True

    return app

