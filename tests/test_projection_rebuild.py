import json
import errno
import os
import queue
import sqlite3
import subprocess
import sys
import threading
import tempfile
from pathlib import Path

import pytest

from hivemind.projection_rebuild import activate, stage
from hivemind.storage import Problem, Store


def fixture_store(path):
    store = Store(path)
    with store.transaction():
        store.db.execute("INSERT INTO projects(id,title) VALUES('project','Fixture')")
        store.save("task", "project", "owner", {"id": "task", "title": "original"})
        store.save("run", "project", "owner", {"id": "run", "task_id": "task"})
        store.relate("project", "run", "task", "ASSIGNED_TO")
    return store


def fingerprint(store):
    return {table: [tuple(row) for row in store.db.execute(f"SELECT * FROM {table} ORDER BY 1,2")]
            for table in ("projects", "resources", "versions", "relations", "run_task_access", "lexical", "jobs")}


def test_replay_and_switch_preserve_history_acl_facts_and_jobs(tmp_path):
    store = fixture_store(tmp_path)
    try:
        with store.transaction():
            store.save("task", "project", "owner", {"id": "task", "title": "revised"}, expected=1)
            store.db.execute("UPDATE resources SET private_agent='worker' WHERE id='task'")
        before = fingerprint(store)
        report = stage(store)
        assert report["origin"] == "genesis" and report["equivalence"]["versions"]["rows"] == 3
        assert activate(store)["generation"] == 1
        assert fingerprint(store) == before
        assert stage(store)["equivalence"] == report["equivalence"]
        assert activate(store)["generation"] == 2
        with pytest.raises(Problem, match="projection_stage_stale"):
            activate(store)
    finally:
        store.close()


def test_shadow_is_rebuilt_from_events_not_live_rows(tmp_path):
    store = fixture_store(tmp_path)
    try:
        # Deliberately damage ONLY a disposable projection, bypassing capture.
        store.db.execute("UPDATE projection_state SET switching=1")
        store.db.execute("UPDATE resources SET body='{}' WHERE id='task'")
        store.db.execute("UPDATE projection_state SET switching=0")
        with pytest.raises(Problem, match="projection_equivalence_failed"):
            stage(store)
        assert store.raw("task")["body"] == "{}"  # mismatch never silently repaired
        assert store.db.execute("SELECT staged_head FROM projection_state").fetchone()[0] is None
    finally:
        store.close()


def test_stage_survives_restart_but_new_mutation_invalidates_it(tmp_path):
    store = fixture_store(tmp_path)
    stage(store)
    store.close()
    store = Store(tmp_path)
    try:
        assert activate(store)["generation"] == 1
        stage(store)
        with store.transaction():
            store.db.execute("UPDATE resources SET private_agent='revoked' WHERE id='task'")
        assert store.db.execute("SELECT count(*) FROM projection_shadow_resources").fetchone()[0] == 0
        with pytest.raises(Problem, match="projection_stage_stale"):
            activate(store)
    finally:
        store.close()


def test_erasure_redacts_journal_and_invalidates_private_shadows(tmp_path):
    store = fixture_store(tmp_path)
    try:
        stage(store)
        with store.transaction():
            store.delete("task", {"id": "owner", "role": "admin", "project": "project", "agent": None}, 1)
        for row in store.db.execute("SELECT record FROM projection_events WHERE projection IN ('resources','versions')"):
            if row[0] and json.loads(row[0])["id"] == "task":
                assert json.loads(row[0])["body"] == "{}"
        assert not store.db.execute("SELECT 1 FROM projection_shadow_resources").fetchone()
        stage(store)
        activate(store)
        assert store.db.execute("SELECT deleted,body FROM resources WHERE id='task'").fetchone()[:] == (1, "{}")
        assert not store.db.execute("SELECT 1 FROM versions WHERE id='task'").fetchone()
    finally:
        store.close()


@pytest.mark.parametrize("damage", [
    "DELETE FROM projection_events WHERE id=2",
    "UPDATE projection_events SET version=999 WHERE id=2",
    "UPDATE projection_events SET record='{}' WHERE projection='resources'",
])
def test_corrupt_or_unknown_journal_fails_closed(tmp_path, damage):
    store = fixture_store(tmp_path)
    try:
        before = fingerprint(store)
        store.db.execute(damage)
        with pytest.raises(Problem):
            stage(store)
        assert fingerprint(store) == before
    finally:
        store.close()


def test_shadow_tampering_and_post_stage_journal_edits_are_refused(tmp_path):
    store = fixture_store(tmp_path)
    try:
        stage(store)
        store.db.execute("UPDATE projection_shadow_resources SET body='{}' WHERE id='task'")
        with pytest.raises(Problem, match="projection_equivalence_failed"):
            activate(store)
        stage(store)
        store.db.execute("UPDATE projection_events SET record=replace(record,'original','changed')")
        with pytest.raises(Problem, match="projection_stage_stale"):
            activate(store)
    finally:
        store.close()


class FaultConnection:
    def __init__(self, real, match, action):
        self.real, self.match, self.action = real, match, action

    def __getattr__(self, name):
        return getattr(self.real, name)

    def execute(self, sql, *args):
        result = self.real.execute(sql, *args)
        if self.match(sql):
            self.action()
        return result


@pytest.mark.parametrize("statement", ["DELETE FROM resources", "INSERT INTO resources SELECT", "UPDATE projection_state SET switching=0"])
def test_activation_fault_rolls_back_every_projection(tmp_path, statement):
    store = fixture_store(tmp_path)
    try:
        stage(store)
        before = fingerprint(store)
        def fail():
            raise OSError("injected storage failure")
        store.db = FaultConnection(store.db, lambda sql: sql.startswith(statement), fail)
        with pytest.raises(OSError):
            activate(store)
        store.db = store.db.real
        assert fingerprint(store) == before
        assert store.db.execute("SELECT switching,generation FROM projection_state").fetchone()[:] == (0, 0)
        assert activate(store)["generation"] == 1
    finally:
        store.close()


@pytest.mark.parametrize("phase", ["stage", "activation", "migration"])
def test_process_exit_during_uncommitted_work_is_recoverable(tmp_path, phase):
    store = fixture_store(tmp_path)
    before = fingerprint(store)
    if phase == "activation":
        stage(store)
    if phase == "migration":
        remove_journal(store)
    store.close()
    child = """
import os,sys
from hivemind.storage import Store
from hivemind.projection_rebuild import stage,activate
from unittest.mock import patch
path,phase=sys.argv[1:]
if phase=='migration':
    from hivemind import projection_rebuild
    original=projection_rebuild.install
    def crash(db):
        original(db)
        os._exit(73)
    with patch.object(projection_rebuild,'install',crash): Store(path)
else:
    s=Store(path)
    if phase=='stage':
        s.db.set_trace_callback(lambda sql: os._exit(73) if sql.startswith('INSERT INTO projection_shadow_') else None)
        stage(s)
    else:
        s.db.set_trace_callback(lambda sql: os._exit(73) if sql=='DELETE FROM versions' else None)
        activate(s)
raise SystemExit(99)
"""
    result = subprocess.run([sys.executable, "-c", child, str(tmp_path), phase],
                            cwd=Path(__file__).resolve().parents[1], timeout=30)
    assert result.returncode == 73
    store = Store(tmp_path)
    try:
        assert fingerprint(store) == before
        assert store.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert not store.db.execute("PRAGMA foreign_key_check").fetchone()
        assert stage(store)["origin"] == ("bootstrap" if phase == "migration" else "genesis")
        activate(store)
    finally:
        store.close()


def remove_journal(store):
    # Disposable pre-v6 fixture, not a production downgrade operation.
    with store.transaction():
        for row in store.db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'projection_capture_%'").fetchall():
            store.db.execute(f'DROP TRIGGER "{row[0]}"')
        for row in store.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'projection_%'").fetchall():
            store.db.execute(f'DROP TABLE "{row[0]}"')
        store.db.execute("DELETE FROM migrations WHERE version=6")


def test_legacy_bootstrap_is_explicit_and_future_schema_refused(tmp_path):
    store = fixture_store(tmp_path)
    remove_journal(store)
    store.close()
    store = Store(tmp_path)
    assert stage(store)["origin"] == "bootstrap"
    store.db.execute("INSERT INTO migrations VALUES(999)")
    store.close()
    with pytest.raises(Problem, match="schema_version_unsupported"):
        Store(tmp_path)


def test_real_sqlite_full_rolls_back_mutations_and_journal(tmp_path):
    store = fixture_store(tmp_path)
    try:
        before = fingerprint(store)
        head = store.db.execute("SELECT max(id) FROM projection_events").fetchone()[0]
        pages = store.db.execute("PRAGMA page_count").fetchone()[0]
        store.db.execute(f"PRAGMA max_page_count={pages}")
        with pytest.raises(sqlite3.OperationalError, match="full"):
            with store.transaction():
                store.save("task", "project", "owner", {"id": "large", "title": "x" * 2000000})
        assert fingerprint(store) == before
        assert store.db.execute("SELECT max(id) FROM projection_events").fetchone()[0] == head
        assert not store.db.in_transaction
        store.db.execute("PRAGMA max_page_count=2147483646")
        stage(store)
        activate(store)
    finally:
        store.close()


@pytest.mark.parametrize("phase", ["stage", "activation", "committed", "migration"])
def test_external_process_kill_recovers_old_or_committed_generation(tmp_path, phase):
    store = fixture_store(tmp_path)
    before = fingerprint(store)
    if phase in ("activation", "committed"):
        stage(store)
    if phase == "migration":
        remove_journal(store)
        store.db.execute("ALTER TABLE projects DROP COLUMN path_case_sensitive")
        store.db.execute("DELETE FROM migrations WHERE version>=2")
    store.close()
    child = """
import sys
from unittest.mock import patch
from hivemind.storage import Store
from hivemind.projection_rebuild import stage,activate
path,phase=sys.argv[1:]
def stop():
    print('READY',flush=True)
    sys.stdin.buffer.read(1)
if phase=='migration':
    from hivemind import projection_rebuild
    original=projection_rebuild.install
    def pause(db):
        original(db)
        stop()
    with patch.object(projection_rebuild,'install',pause): Store(path)
else:
    s=Store(path)
    if phase=='stage':
        s.db.set_trace_callback(lambda sql: stop() if sql.startswith('INSERT INTO projection_shadow_') else None)
        stage(s)
    elif phase=='activation':
        s.db.set_trace_callback(lambda sql: stop() if sql=='DELETE FROM versions' else None)
        activate(s)
    else:
        activate(s)
        stop()
"""
    process = subprocess.Popen([sys.executable, "-u", "-c", child, str(tmp_path), phase],
                               cwd=Path(__file__).resolve().parents[1],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ready = queue.Queue()
    reader = threading.Thread(target=lambda: ready.put(process.stdout.readline()), daemon=True)
    reader.start()
    try:
        assert ready.get(timeout=20).strip() == b"READY"
        process.kill()  # SIGKILL on Linux; TerminateProcess on Windows.
        process.communicate(timeout=10)
        assert process.returncode != 0
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)
        reader.join(timeout=2)
    store = Store(tmp_path)
    try:
        assert fingerprint(store) == before
        assert store.db.execute("SELECT generation FROM projection_state").fetchone()[0] == int(phase == "committed")
        assert store.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        stage(store)
        activate(store)
    finally:
        store.close()


def test_concurrent_reader_never_sees_half_switched_projection(tmp_path):
    store = fixture_store(tmp_path)
    stage(store)
    before = fingerprint(store)
    deleted, release = threading.Event(), threading.Event()
    errors = []
    def pause():
        deleted.set()
        assert release.wait(timeout=10)
    store.db = FaultConnection(store.db, lambda sql: sql == "DELETE FROM resources", pause)
    def switch():
        try:
            activate(store)
        except BaseException as exc:
            errors.append(exc)
    worker = threading.Thread(target=switch)
    worker.start()
    try:
        assert deleted.wait(timeout=10)
        with sqlite3.connect(tmp_path / "canonical.sqlite") as reader:
            assert [tuple(row) for row in reader.execute("SELECT * FROM resources ORDER BY 1,2")] == before["resources"]
        release.set()
        worker.join(timeout=15)
        assert not worker.is_alive() and not errors
        assert fingerprint(store) == before
    finally:
        release.set()
        worker.join(timeout=15)
        store.db = store.db.real
        store.close()


@pytest.mark.parametrize("phase", ["stage", "activation"])
def test_sqlite_full_during_rebuild_preserves_live_generation(tmp_path, phase):
    store = fixture_store(tmp_path)
    try:
        with store.transaction():
            store.save("task", "project", "owner", {"id": "bulk", "title": "large", "content": {"text": "data " * 60000}})
        if phase == "activation":
            stage(store)
        before = fingerprint(store)
        pages = store.db.execute("PRAGMA page_count").fetchone()[0]
        store.db.execute(f"PRAGMA max_page_count={pages}")
        if phase == "activation":
            real = store.db
            def allocate():
                real.execute("CREATE TABLE fault_fill(data BLOB)")
                real.execute("INSERT INTO fault_fill VALUES(zeroblob(20000000))")
            store.db = FaultConnection(real, lambda sql: sql == "DELETE FROM resources", allocate)
        with pytest.raises(sqlite3.OperationalError, match="full"):
            (activate if phase == "activation" else stage)(store)
        if phase == "activation":
            store.db = store.db.real
        assert fingerprint(store) == before
        assert store.db.execute("SELECT switching,generation FROM projection_state").fetchone()[:] == (0, 0)
        store.db.execute("PRAGMA max_page_count=2147483646")
        stage(store)
        activate(store)
    finally:
        store.close()


def test_sqlite_full_during_upgrade_rolls_back_schema_and_bootstrap(tmp_path, monkeypatch):
    store = fixture_store(tmp_path)
    remove_journal(store)
    store.db.execute("VACUUM")
    pages = store.db.execute("PRAGMA page_count").fetchone()[0]
    store.close()
    original = sqlite3.connect
    def limited(*args, **kwargs):
        connection = original(*args, **kwargs)
        connection.execute(f"PRAGMA max_page_count={pages}")
        return connection
    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", limited)
        with pytest.raises(sqlite3.OperationalError, match="full"):
            Store(tmp_path)
    with original(tmp_path / "canonical.sqlite") as connection:
        assert not connection.execute("SELECT 1 FROM migrations WHERE version=6").fetchone()
        assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name='projection_events'").fetchone()
    store = Store(tmp_path)
    try:
        assert stage(store)["origin"] == "bootstrap"
    finally:
        store.close()


def test_missing_capture_is_not_silently_rebootstrapped(tmp_path):
    store = fixture_store(tmp_path)
    store.db.execute("DROP TRIGGER projection_capture_resources_update")
    store.close()
    with pytest.raises(Problem, match="projection_capture_incomplete"):
        Store(tmp_path)


def test_destructive_extension_fk_is_refused_without_ledger_changes(tmp_path):
    store = fixture_store(tmp_path)
    try:
        store.db.execute("CREATE TABLE extension_ledger(resource TEXT REFERENCES resources(id) ON DELETE CASCADE)")
        store.db.execute("INSERT INTO extension_ledger VALUES('task')")
        stage(store)
        with pytest.raises(Problem, match="projection_dependency_unsupported"):
            activate(store)
        assert store.db.execute("SELECT resource FROM extension_ledger").fetchone()[0] == "task"
    finally:
        store.close()


def test_cli_stages_and_activates_without_disclosing_resource_content(tmp_path, monkeypatch, capsys):
    from hivemind.cli import main
    store = fixture_store(tmp_path)
    store.close()
    monkeypatch.setattr(sys, "argv", ["hivemind", "--data-dir", str(tmp_path), "rebuild-projections", "--activate"])
    assert main() == 0
    output = capsys.readouterr().out
    assert json.loads(output)["activation"]["generation"] == 1
    assert "original" not in output and "body" not in output


def test_another_connection_cannot_sneak_a_mutation_past_activation(tmp_path):
    store = fixture_store(tmp_path)
    other = Store(tmp_path)
    try:
        stage(store)
        with other.transaction():
            other.db.execute("UPDATE projects SET epoch=epoch+1 WHERE id='project'")
        with pytest.raises(Problem, match="projection_stage_stale"):
            activate(store)
        assert store.project("project")["epoch"] == 1
        stage(store)
        activate(store)
        assert store.project("project")["epoch"] == 1
    finally:
        other.close()
        store.close()


def test_version_retention_preserves_replay_of_still_live_content(tmp_path):
    store = fixture_store(tmp_path)
    try:
        with store.transaction():
            store.db.execute("DELETE FROM versions WHERE id='task'")
        assert stage(store)["equivalence"]["versions"]["rows"] == 1
        activate(store)
        assert json.loads(store.raw("task")["body"])["title"] == "original"
    finally:
        store.close()


def test_real_filesystem_enospc_in_explicit_disposable_tmpfs():
    root = os.environ.get("HIVEMIND_DISK_FULL_ROOT")
    if sys.platform != "linux" or root != "/fault-volume":
        pytest.skip("Requires explicitly mounted private /fault-volume tmpfs")
    root = Path(root)
    info = os.statvfs(root)
    assert not root.is_symlink() and info.f_blocks * info.f_frsize <= 64 * 1024 * 1024
    mounts = Path("/proc/self/mountinfo").read_text()
    assert any(" /fault-volume " in line and " - tmpfs " in line for line in mounts.splitlines())
    with tempfile.TemporaryDirectory(dir=root, prefix="hivemind-enospc-") as directory:
        store = fixture_store(Path(directory) / "store")
        filler = Path(directory) / "owned-fill"
        before = fingerprint(store)
        try:
            with filler.open("wb", buffering=0) as file:
                with pytest.raises(OSError) as full:
                    for _ in range(65):
                        file.write(b"x" * 1048576)
            assert full.value.errno == errno.ENOSPC
            with pytest.raises(sqlite3.OperationalError):
                with store.transaction():
                    store.save("task", "project", "owner", {"id": "too-large", "title": "x" * 4000000})
        finally:
            filler.unlink(missing_ok=True)
            store.close()
        store = Store(Path(directory) / "store")
        try:
            assert fingerprint(store) == before
            assert store.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            stage(store)
            activate(store)
        finally:
            store.close()


def test_standalone_save_is_atomic_without_an_explicit_transaction(tmp_path):
    store = fixture_store(tmp_path)
    try:
        before = fingerprint(store)
        head = store.db.execute("SELECT max(id) FROM projection_events").fetchone()[0]
        assert store.db.execute("PRAGMA synchronous").fetchone()[0] == 2
        pages = store.db.execute("PRAGMA page_count").fetchone()[0]
        store.db.execute(f"PRAGMA max_page_count={pages}")
        with pytest.raises(sqlite3.OperationalError, match="full"):
            store.save("task", "project", "owner", {"id": "large", "title": "x" * 2000000})
        assert fingerprint(store) == before
        assert store.db.execute("SELECT max(id) FROM projection_events").fetchone()[0] == head
    finally:
        store.close()


def test_standalone_relation_failure_does_not_commit_a_partial_fact(tmp_path):
    store = fixture_store(tmp_path)
    try:
        before = fingerprint(store)
        with pytest.raises(Problem, match="not_found"):
            store.relate("project", "missing", "task", "BAD")
        assert fingerprint(store) == before
    finally:
        store.close()


def test_trigger_added_after_staging_cannot_modify_external_ledger(tmp_path):
    store = fixture_store(tmp_path)
    try:
        stage(store)
        store.db.execute("CREATE TRIGGER extension_side_effect AFTER DELETE ON resources BEGIN DELETE FROM jobs; END")
        before = fingerprint(store)
        with pytest.raises(Problem, match="projection_dependency_unsupported"):
            activate(store)
        assert fingerprint(store) == before
    finally:
        store.close()
