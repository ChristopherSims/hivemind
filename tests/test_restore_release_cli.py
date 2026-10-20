import json

from hivemind import cli, restore_release


def test_cli_release_delegates_exact_approval_and_private_identity(monkeypatch, tmp_path, capsys):
    evidence = {"snapshot_id": "fixture", "manifest_digest": "a" * 64}
    approval = tmp_path / "approval.json"
    approval.write_text(json.dumps(evidence), encoding="utf-8")
    identity = tmp_path / "owner.json"
    store = tmp_path / "restored"
    calls = []

    def release(path, body, **options):
        calls.append((path, body, options))
        return {"restore_hold": "released"}

    monkeypatch.setattr(restore_release, "release_restore_hold", release)
    monkeypatch.setattr("sys.argv", ["hivemind", "coordinated", "release-hold",
        "--root", str(tmp_path / "snapshots"), "--restored-store", str(store),
        "--approval", str(approval), "--identity", str(identity),
        "--confirm", restore_release.evidence_digest(evidence), "--resume-release"])
    cli.main()
    assert calls == [(str(store), evidence, {"identity": str(identity), "resume": True})]
    assert json.loads(capsys.readouterr().out)["restore_hold"] == "released"
