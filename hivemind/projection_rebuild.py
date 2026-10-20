"""Versioned event-only SQL projection replay and transactional activation.

Mutation events are internal canonical data, not public observation events. Old
stores start with an explicitly identified bootstrap, not fabricated history.
Credential, receipt, outbox and runtime authority tables are never replayed.
"""
import hashlib
import json

from .storage import Problem, canonical, searchable

VERSION = 1
KEYS = {"projects": ("id",), "resources": ("id",), "versions": ("id", "revision"),
        "relations": ("project", "source", "target", "kind"), "run_task_access": ("run", "task")}
INDEXED = {"agent", "board_post", "memory", "task", "work_claim", "artifact"}


def _columns(db, table):
    return tuple(row["name"] for row in db.execute(f"PRAGMA table_info({table})"))


def install(db):
    """Called inside the schema migration transaction, before serving."""
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='projection_state'").fetchone()
    if exists:
        state = db.execute("SELECT * FROM projection_state WHERE id=1").fetchone()
        if not state or state["version"] != VERSION or state["switching"]:
            raise Problem("projection_version_unsupported")
        expected = {f"projection_capture_{table}_{operation}" for table in KEYS
                    for operation in ("insert", "update", "delete")}
        actual = {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'projection_capture_%'")}
        if actual != expected or not db.execute(
                "SELECT 1 FROM sqlite_master WHERE name='projection_events' AND type='table'").fetchone():
            raise Problem("projection_capture_incomplete")
        return
    if db.execute("SELECT 1 FROM migrations WHERE version=6").fetchone():
        raise Problem("projection_capture_incomplete")
    db.execute("""CREATE TABLE projection_state(
        id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,origin TEXT NOT NULL,
        switching INTEGER NOT NULL DEFAULT 0,generation INTEGER NOT NULL DEFAULT 0,
        staged_head INTEGER,staged_digest TEXT)""")
    db.execute("""CREATE TABLE projection_events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,version INTEGER NOT NULL,
        projection TEXT NOT NULL,operation TEXT NOT NULL,row_key TEXT NOT NULL,record TEXT)""")
    db.execute("""CREATE INDEX projection_events_resource_revision ON projection_events(
        projection,json_extract(record,'$.id'),json_extract(record,'$.revision'))""")
    legacy = any(db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() for table in KEYS)
    db.execute("INSERT INTO projection_state(id,version,origin) VALUES(1,?,?)",
               (VERSION, "bootstrap" if legacy else "genesis"))
    for table in (*KEYS, "lexical"):
        db.execute(f"CREATE TABLE projection_shadow_{table} AS SELECT * FROM {table} WHERE 0")
    invalidation = "".join(
        f"DELETE FROM projection_shadow_{table} WHERE "
        "(SELECT staged_head FROM projection_state WHERE id=1) IS NOT NULL;"
        for table in (*KEYS, "lexical"))
    invalidation += ("UPDATE projection_state SET staged_head=NULL,staged_digest=NULL "
                     "WHERE id=1 AND staged_head IS NOT NULL;")
    for table, keys in KEYS.items():
        columns = _columns(db, table)
        for row in db.execute(f"SELECT * FROM {table}"):
            record = dict(row)
            db.execute("INSERT INTO projection_events(version,projection,operation,row_key,record) "
                       "VALUES(?,?,'insert',?,?)",
                       (VERSION, table, canonical([record[key] for key in keys]), canonical(record)))
        for operation, prefix in (("insert", "NEW"), ("update", "NEW"), ("delete", "OLD")):
            key_prefix = "OLD" if operation == "update" else prefix
            key = "json_array(" + ",".join(f'{key_prefix}."{name}"' for name in keys) + ")"
            record = ("NULL" if operation == "delete" else
                      "json_object(" + ",".join(f"'{name}',{prefix}.\"{name}\"" for name in columns) + ")")
            redact = ""
            if table == "versions" and operation == "delete":
                # Erasure must not leave content in the mutation journal.
                redact = """UPDATE projection_events SET record=json_set(record,'$.body','{}')
                    WHERE projection='versions'
                    AND json_extract(record,'$.id')=OLD.id
                    AND json_extract(record,'$.revision')=OLD.revision;
                    UPDATE projection_events SET record=json_set(record,'$.body',COALESCE(
                        (SELECT body FROM resources WHERE id=OLD.id AND revision=OLD.revision AND deleted=0),'{}'))
                    WHERE projection='resources' AND json_extract(record,'$.id')=OLD.id
                    AND json_extract(record,'$.revision')=OLD.revision;"""
            if table == "resources" and operation in ("delete", "update"):
                condition = "1" if operation == "delete" else "NEW.deleted=1"
                redact = f"""UPDATE projection_events SET record=json_set(record,'$.body','{{}}')
                    WHERE {condition} AND projection='resources'
                    AND json_extract(record,'$.id')=OLD.id;"""
            db.execute(f"""CREATE TRIGGER projection_capture_{table}_{operation}
                AFTER {operation.upper()} ON {table}
                WHEN (SELECT switching FROM projection_state WHERE id=1)=0
                BEGIN {invalidation}{redact}
                INSERT INTO projection_events(version,projection,operation,row_key,record)
                VALUES({VERSION},'{table}','{operation}',{key},{record}); END""")


def _journal(db):
    digest = hashlib.sha256()
    head = 0
    for event in db.execute("SELECT * FROM projection_events ORDER BY id"):
        if event["id"] != head + 1 or event["version"] != VERSION or event["projection"] not in KEYS:
            raise Problem("projection_journal_invalid")
        head = event["id"]
        digest.update(canonical(dict(event)).encode())
        digest.update(b"\n")
    return head, digest.hexdigest()


def _fingerprint(db, table, keys):
    digest = hashlib.sha256()
    count = 0
    for row in db.execute(f"SELECT * FROM {table} ORDER BY " + ",".join(keys)):
        digest.update(canonical(dict(row)).encode())
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def _equivalence(db):
    result = {}
    for table in (*KEYS, "lexical"):
        keys = KEYS.get(table, ("id", "project", "text"))
        actual = _fingerprint(db, table, keys)
        rebuilt = _fingerprint(db, "projection_shadow_" + table, keys)
        if actual != rebuilt:
            raise Problem("projection_equivalence_failed")
        result[table] = {"rows": actual[0], "sha256": actual[1]}
    return result


def _check_triggers(db):
    expected = {f"projection_capture_{table}_{operation}" for table in KEYS
                for operation in ("insert", "update", "delete")}
    actual = {row["name"] for row in db.execute(
        "SELECT name,tbl_name FROM sqlite_master WHERE type='trigger'") if row["tbl_name"] in KEYS}
    if not expected <= actual:
        raise Problem("projection_capture_incomplete")
    if actual != expected:
        raise Problem("projection_dependency_unsupported")


def stage(store):
    """Replay only the mutation journal; live rows are used solely for comparison.

    Offline maintenance: SQLite serializes writers while the shadow is built.
    Any subsequent captured mutation immediately clears staged private content.
    """
    db = store.db
    with store.transaction():
        state = db.execute("SELECT * FROM projection_state WHERE id=1").fetchone()
        if state["version"] != VERSION or state["switching"]:
            raise Problem("projection_version_unsupported")
        _check_triggers(db)
        head, journal_digest = _journal(db)
        for table in (*KEYS, "lexical"):
            db.execute(f"DELETE FROM projection_shadow_{table}")
        schemas = {table: _columns(db, table) for table in KEYS}
        for event in db.execute("SELECT * FROM projection_events ORDER BY id"):
            table = event["projection"]
            target = "projection_shadow_" + table
            columns = schemas[table]
            keys = json.loads(event["row_key"])
            if not isinstance(keys, list) or len(keys) != len(KEYS[table]):
                raise Problem("projection_journal_invalid")
            where = " AND ".join(f"{key} IS ?" for key in KEYS[table])
            present = db.execute(f"SELECT 1 FROM {target} WHERE {where}", keys).fetchone()
            operation = event["operation"]
            if operation not in ("insert", "update", "delete") or (operation != "insert" and not present):
                raise Problem("projection_journal_invalid")
            if operation == "insert" and present:
                raise Problem("projection_journal_invalid")
            if operation != "insert":
                db.execute(f"DELETE FROM {target} WHERE {where}", keys)
            if operation != "delete":
                record = json.loads(event["record"])
                if not isinstance(record, dict) or set(record) != set(columns):
                    raise Problem("projection_journal_invalid")
                db.execute(f"INSERT INTO {target} VALUES(" + ",".join("?" for _ in columns) + ")",
                           [record[column] for column in columns])
        for row in db.execute("SELECT * FROM projection_shadow_resources WHERE deleted=0"):
            if row["kind"] in INDEXED:
                db.execute("INSERT INTO projection_shadow_lexical VALUES(?,?,?)",
                           (row["id"], row["project"], searchable(json.loads(row["body"]))))
        equivalent = _equivalence(db)
        db.execute("UPDATE projection_state SET staged_head=?,staged_digest=? WHERE id=1",
                   (head, journal_digest))
        state = dict(db.execute("SELECT * FROM projection_state WHERE id=1").fetchone())
    return {"head": head, "journal_sha256": journal_digest, "origin": state["origin"],
            "reducer_version": VERSION, "equivalence": equivalent}


def activate(store):
    """Atomically install a verified shadow, preserving all external authority."""
    db = store.db
    with store.transaction():
        state = db.execute("SELECT * FROM projection_state WHERE id=1").fetchone()
        if state["version"] != VERSION or state["switching"]:
            raise Problem("projection_version_unsupported")
        _check_triggers(db)
        head, journal_digest = _journal(db)
        if state["staged_head"] != head or state["staged_digest"] != journal_digest:
            raise Problem("projection_stage_stale")
        equivalent = _equivalence(db)
        # Deferring FK checks does not suppress CASCADE/SET NULL side effects.
        # Unknown extensions with destructive dependencies must not be switched.
        for table in db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            for foreign in db.execute("SELECT * FROM pragma_foreign_key_list(?)", (table[0],)):
                if foreign["table"] in KEYS and foreign["on_delete"] not in ("NO ACTION", "RESTRICT"):
                    raise Problem("projection_dependency_unsupported")
        db.execute("PRAGMA defer_foreign_keys=ON")
        db.execute("UPDATE projection_state SET switching=1 WHERE id=1")
        # Stable table identities preserve FKs, triggers and existing reader SQL.
        for table in KEYS:
            db.execute(f"DELETE FROM {table}")
            db.execute(f"INSERT INTO {table} SELECT * FROM projection_shadow_{table}")
        db.execute("DELETE FROM lexical")
        db.execute("INSERT INTO lexical SELECT * FROM projection_shadow_lexical")
        if db.execute("PRAGMA foreign_key_check").fetchone():
            raise Problem("projection_foreign_key_failed")
        for table in (*KEYS, "lexical"):
            db.execute(f"DELETE FROM projection_shadow_{table}")
        db.execute("""UPDATE projection_state SET switching=0,generation=generation+1,
            staged_head=NULL,staged_digest=NULL WHERE id=1""")
    return {"generation": state["generation"] + 1, "head": head, "equivalence": equivalent}
