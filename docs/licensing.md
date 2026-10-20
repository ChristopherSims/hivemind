# Dependency and model notes

Project code is MIT. This implementation was written independently; no code was copied
from the prior-art agent-mail/memory repositories in the plan.

Pinned storage dependencies:

| Dependency | Version | Distribution evidence |
| --- | --- | --- |
| sqlite-vec | 0.1.9 | Installed metadata declares MIT / Apache-2.0; MIT option selected |
| ladybug | 0.21.2 | Installed LICENSE is MIT, copyright Kùzu Inc. 2022–2025 and Ladybug Memory Inc. 2025–2026 |

The sqlite-vec wheel's metadata declares the licenses but does not bundle their full text.
NOTICE now includes the exact pinned upstream release's MIT text
(asg017/sqlite-vec v0.1.9 `LICENSE-MIT`). Ladybug's complete upstream notice ships inside
its installed distribution and is summarized in NOTICE. Remaining gate: audit
sqlite-vec's bundled binary dependencies before a wheel distribution (a source
installation like this repository's `uv sync` path does not redistribute its wheel).

Embedding profile `eg2-text-768-v1`:

- Model: `google/embeddinggemma-2`.
- Revision: `914f7f89142e33e77833254d9c9b90c3cef7303b`.
- Model card: https://huggingface.co/google/embeddinggemma-2
- Model card declares Apache 2.0 and references the Gemma Prohibited Use Policy.
- Review the model's complete applicable terms before redistribution or a new deployment.
- Text-only encoders; float32 on this CPU, bfloat16 only where CUDA supports it; no float16.
- Compatible tested libraries: SentenceTransformers 6.1.0, Transformers 5.19.0,
  Torch 2.14.1, TorchVision 0.29.1, Pillow 12.3.0.

Frontend exact dependency resolution is recorded in `dashboard/package-lock.json`.
Python dependency resolution is recorded in `uv.lock`; optional model and MCP extras are
separate from the canonical core.

No package/domain/trademark clearance has been performed for “Hivemind” or “Nexus”.
The Python distribution name is provisional and has not been published.
