"""Hivemind reference implementation."""

from pathlib import Path


def _read_version() -> str:
    """The release version lives in the repository root's VERSION file."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "VERSION"
        if candidate.is_file():
            return candidate.read_text(encoding="utf-8").strip()
    return "0.0.0+unknown"


__version__ = _read_version()