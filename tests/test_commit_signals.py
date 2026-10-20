import pytest

from hivemind.storage import Store


def test_signals_are_committed_minimal_and_rollback_silent(tmp_path):
    store = Store(tmp_path)
    seen = []

    def signal(changes):
        assert not store.db.in_transaction
        seen.append(changes)

    unsubscribe = store.subscribe_commits(signal)
    try:
        with store.transaction():
            store.db.execute("INSERT INTO projects(id,title) VALUES('p','Private title')")
            store.tick("p")
            store.tick("p")
            assert not seen
        assert seen == [{"p": 2}]
        with pytest.raises(RuntimeError), store.transaction():
            store.tick("p")
            raise RuntimeError("rolled back")
        assert seen == [{"p": 2}] and store.project("p")["seq"] == 2
        unsubscribe()
        unsubscribe()
        with store.transaction():
            store.tick("p")
        assert seen == [{"p": 2}]
    finally:
        store.close()


def test_bad_listener_cannot_undo_commit_or_starve_other_subscribers(tmp_path):
    store = Store(tmp_path)
    seen = []

    def broken(changes):
        raise ValueError("not forwarded")

    store.subscribe_commits(broken)
    store.subscribe_commits(seen.append)
    try:
        with store.transaction():
            store.db.execute("INSERT INTO projects(id,title) VALUES('p','Project')")
            store.tick("p")
        assert store.project("p")["seq"] == 1
        assert store.commit_listener_failures == 1
        assert seen == [{"p": 1}]
    finally:
        store.close()
