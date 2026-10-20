"""Offline evidence tools. No provider writes or retained-data erasure."""
import argparse
import hashlib
import json
import os
import sqlite3
import stat
import tempfile
from contextlib import closing
from pathlib import Path

from .model_wiring import runtime_counter
from .storage import canonical


def validate_tokenizer(manifest, tokenizer_file):
    """Compare a pinned counter with independently supplied golden text counts.

    The manifest is operator evidence, not proof that a vendor uses these bytes.
    Never generate expected counts from the counter under test.
    """
    if (manifest.get("schema_version") != 1 or not manifest.get("model_revision")
            or not manifest.get("reference_source") or not manifest.get("samples")):
        raise ValueError("Model revision, independent reference source and samples are required")
    samples = manifest["samples"]
    if not isinstance(samples, list) or len(samples) > 256:
        raise ValueError("Tokenizer validation needs 1..256 samples")
    ids = set()
    for sample in samples:
        if (not isinstance(sample.get("id"), str) or not sample["id"] or sample["id"] in ids
                or not isinstance(sample.get("text"), str) or len(sample["text"].encode()) > 262144
                or type(sample.get("expected_tokens")) is not int or sample["expected_tokens"] < 0):
            raise ValueError("Invalid tokenizer golden sample")
        ids.add(sample["id"])
    counter = runtime_counter(manifest["model_id"], tokenizer_file, manifest["tokenizer_sha256"])
    rows = []
    for sample in samples:
        observed = counter(sample["text"])
        rows.append({"id": sample["id"], "expected_tokens": sample["expected_tokens"],
                     "observed_tokens": observed, "matched": observed == sample["expected_tokens"]})
    return {"schema_version": 1, "model_id": manifest["model_id"],
            "model_revision": manifest["model_revision"], "tokenizer_sha256": manifest["tokenizer_sha256"],
            "reference_source": manifest["reference_source"], "samples": rows,
            "passed": all(row["matched"] for row in rows),
            "scope": "Raw text, add_special_tokens=False; no chat template, tools, "
                     "hidden vendor framing or generated-token budget equivalence.",
            "vendor_model_binding_verified": False}


def provider_absence(adapter, receipt):
    """Read back bounded receipt IDs. Absence is not provider-wide physical erasure."""
    ids = receipt.get("ids") if adapter.name == "mem0" else [receipt.get("id")]
    if (not isinstance(ids, list) or not 1 <= len(ids) <= 32
            or any(not isinstance(identity, str) or not 1 <= len(identity) <= 256 for identity in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError("Invalid bounded provider receipt")
    states = []
    for identity in ids:
        try:
            record = adapter.get(identity)
            state = "absent" if record is None else "present" if record["id"] == identity else "identity_mismatch"
        except Exception:
            state = "unknown"
        states.append(state)
    return {"adapter": adapter.name, "records_checked": len(states), "states": states,
            "absence_verified": all(state == "absent" for state in states),
            "physical_erasure_verified": False, "provider_writes": 0}


def _regular_file(path):
    if not Path(path).is_absolute():
        raise ValueError("Audit inputs must be absolute paths")
    path = Path(os.path.abspath(path))
    # Reject symbolic links and Windows junction/reparse ancestors, not just the leaf.
    for part in (path, *path.parents):
        info = part.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("Symlink/reparse paths are not audit inputs")
    if not path.is_file():
        raise ValueError("Audit input must be a regular file")
    return path


def audit_retained_files(entries, marker, *, max_bytes=256 * 1024 * 1024, chunk_size=65536):
    """Read only explicitly inventoried files; never report marker/content/path.

    A marker scan only establishes presence/absence of those encodings in readable
    bytes. Encryption, compression, remapped blocks, snapshots and missing files
    remain outside this verification. Quiesce files before auditing.
    """
    if not isinstance(marker, str) or not 8 <= len(marker.encode()) <= 4096:
        raise ValueError("Use an 8..4096 byte non-secret audit marker")
    if not 1 <= len(entries) <= 1024 or type(max_bytes) is not int or max_bytes < 1 or chunk_size < 1:
        raise ValueError("Invalid audit bounds")
    patterns = [marker.encode("utf-8"), marker.encode("utf-16-le"), marker.encode("utf-16-be")]
    overlap = max(map(len, patterns)) - 1
    rows = []
    labels = set()
    for entry in entries:
        label = entry["label"]
        if not isinstance(label, str) or not label or len(label) > 128 or label in labels:
            raise ValueError("Unique short inventory labels are required")
        labels.add(label)
        row = {"label": label, "category": entry["category"], "status": "unknown", "marker_found": None}
        try:
            path = _regular_file(entry["path"])
            before = path.stat()
            if before.st_size > max_bytes:
                row["status"] = "size_limit"
            else:
                digest, carry, found, total = hashlib.sha256(), b"", False, 0
                with path.open("rb") as file:
                    while block := file.read(min(chunk_size, max_bytes - total + 1)):
                        total += len(block)
                        if total > max_bytes:
                            raise ValueError("Audit input grew beyond bound")
                        digest.update(block)
                        window = carry + block
                        found |= any(pattern in window for pattern in patterns)
                        carry = window[-overlap:]
                after = path.stat()
                stable = (before.st_size, before.st_mtime_ns, before.st_ino) == (
                    after.st_size, after.st_mtime_ns, after.st_ino)
                row.update(status="scanned" if stable else "changed_during_scan",
                           marker_found=found, bytes_scanned=total, sha256=digest.hexdigest())
        except (OSError, ValueError):
            row["status"] = "unreadable_or_unsafe"
        rows.append(row)
    complete = all(row["status"] == "scanned" for row in rows)
    return {"files": rows, "inventory_scan_complete": complete,
            "marker_absent_from_inventory": complete and all(not row["marker_found"] for row in rows),
            "physical_erasure_verified": False,
            "scope": "Explicit files only, UTF-8/UTF-16 plaintext marker scan. Not a "
                     "whole-device, encrypted/compressed backup or provider erasure proof."}


def erasure_fixture():
    """Exercise deletion/compaction without ever opening an existing data directory."""
    from .indexes import Indexes
    from .retrieval_benchmark import SyntheticEncoder, add_document, project_fixture
    from .storage import Store
    marker = "hivemind-disposable-erasure-canary-47bdf18c"
    with tempfile.TemporaryDirectory(prefix="hivemind-erasure-") as directory:
        root = Path(directory)
        store, indexes = Store(root / "store"), None
        try:
            project_fixture(store)
            admin = {"id": "reader", "role": "admin", "agent": None, "project": "benchmark"}
            source = add_document(store, "evidence", "Non-sensitive supporting source")
            body = {"id": "erase-memory", "project_id": "benchmark", "title": "Erasure canary",
                    "content": {"media_type": "text/plain", "text": marker},
                    "status": "verified", "scope_kind": "project", "scope_owner_id": "benchmark",
                    "evidence_refs": [{"id": source["id"], "kind": "board_post", "revision": 1}],
                    "validity": {}, "expires_at": None, "category": "semantic", "origin": "explicit"}
            with store.transaction():
                store.save("memory", "benchmark", "reader", body)
                memory = store.save("memory", "benchmark", "reader", body, expected=1)
            indexes = Indexes(store, SyntheticEncoder())
            indexes.drain()
            backup = root / "retained-backup.sqlite"
            with closing(sqlite3.connect(backup)) as target:
                store.db.backup(target)
            spool = root / "retained-spool.jsonl"
            spool.write_text(canonical({"text": marker}) + "\n", encoding="utf-8")
            store.db.execute("PRAGMA secure_delete=ON")
            with store.transaction():
                store.delete(memory["id"], admin, expected=memory["revision"])
            indexes.drain()
            versions = store.db.execute("SELECT count(*) FROM versions WHERE id='erase-memory'").fetchone()[0]
            chunks = store.db.execute("SELECT count(*) FROM vector_chunks WHERE resource='erase-memory'").fetchone()[0]
            indexes.close()
            indexes = None
            store.db.execute("INSERT INTO lexical(lexical) VALUES('optimize')")
            checkpoint = store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint[0]:
                raise RuntimeError("Fixture WAL checkpoint blocked")
            store.db.execute("VACUUM")
            checkpoint = store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint[0]:
                raise RuntimeError("Fixture post-compaction checkpoint blocked")
            entries = [{"label": "canonical", "category": "sqlite", "path": store.directory / "canonical.sqlite"},
                       {"label": "backup", "category": "backup", "path": backup},
                       {"label": "spool", "category": "spool", "path": spool}]
            wal = store.directory / "canonical.sqlite-wal"
            if wal.exists():
                entries.append({"label": "wal", "category": "wal", "path": wal})
            audit = audit_retained_files(entries, marker)
            files = {item["label"]: item for item in audit["files"]}
            passed = (versions == chunks == 0 and files["canonical"]["marker_found"] is False
                      and files["backup"]["marker_found"] is True and files["spool"]["marker_found"] is True
                      and audit["inventory_scan_complete"] and not files.get("wal", {}).get("marker_found", False))
            return {"fixture_passed": passed, "remaining_versions": versions, "remaining_vector_chunks": chunks,
                    "audit": audit, "production_data_modified": False,
                    "meaning": "Deleted memory history and derived chunks are removed; "
                               "compacted fixture SQLite/WAL pass the marker scan. "
                               "Retained backup and spool are correctly detected, not erased."}
        finally:
            if indexes:
                indexes.close()
            store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New report, never overwritten")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("erasure-fixture")
    tokenizer = sub.add_parser("tokenizer")
    tokenizer.add_argument("--manifest", type=Path, required=True)
    tokenizer.add_argument("--tokenizer-file", type=Path, required=True)
    audit = sub.add_parser("audit")
    audit.add_argument("--inventory", type=Path, required=True, help="JSON list of label/category/absolute path")
    audit.add_argument("--marker-file", type=Path, required=True, help="Non-secret UTF-8 canary; not a credential")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Report already exists")
    if args.command == "erasure-fixture":
        report = erasure_fixture()
        success = report["fixture_passed"]
    elif args.command == "tokenizer":
        report = validate_tokenizer(json.loads(args.manifest.read_text(encoding="utf-8")), args.tokenizer_file)
        success = report["passed"]
    else:
        report = audit_retained_files(json.loads(args.inventory.read_text(encoding="utf-8")),
                                      args.marker_file.read_text(encoding="utf-8"))
        success = report["marker_absent_from_inventory"]
    with args.output.open("x", encoding="utf-8") as file:
        json.dump(report, file, indent=2, allow_nan=False)
        file.write("\n")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
