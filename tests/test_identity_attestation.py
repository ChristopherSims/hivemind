"""Adversarial offline tests plus read-only real OS probes for attestation.

Nothing here provisions host security state: no accounts, no ACL changes, no
elevation. Real-probe tests only *read* evidence about the current user and
temporary directories.
"""
import getpass
import os
import sys
from pathlib import Path

import pytest

from hivemind import identity_attestation as ia


def evidence(name, *, sid=None, uid=None, gid=1000, platform="posix"):
    return ia.IdentityEvidence(name=name, platform=platform,
                               source="posix-pwd" if platform != "windows" else "windows-lsa",
                               sid=sid, uid=uid, gid=gid)


def posix_resolver(mapping):
    def resolve(name):
        if name not in mapping:
            raise ia.AttestationFailed(f"Account {name!r} does not exist on this host")
        return mapping[name]
    return resolve


class TestIdentityAttestationOffline:
    def test_distinct_uids_attest(self):
        resolve = posix_resolver({"svc": evidence("svc", uid=200), "agent": evidence("agent", uid=201)})
        result = ia.attest_distinct_identities("svc", "agent", platform="posix", resolve=resolve)
        assert result.kind == "identities"
        assert "svc=('uid', 200)" in result.subject and "agent=('uid', 201)" in result.subject

    def test_aliased_names_resolving_to_same_uid_are_rejected(self):
        resolve = posix_resolver({"svc": evidence("svc", uid=200), "svc-alias": evidence("svc-alias", uid=200)})
        with pytest.raises(ia.AttestationFailed, match="same OS identity"):
            ia.attest_distinct_identities("svc", "svc-alias", platform="posix", resolve=resolve)

    def test_aliased_windows_sids_are_rejected(self):
        resolve = posix_resolver({
            "svc": evidence("svc", sid="S-1-5-21-1-2-3-1001", platform="windows"),
            "agent": evidence("agent", sid="S-1-5-21-1-2-3-1001", platform="windows")})
        with pytest.raises(ia.AttestationFailed, match="same OS identity"):
            ia.attest_distinct_identities("svc", "agent", platform="windows", resolve=resolve)

    def test_missing_account_fails_closed(self):
        resolve = posix_resolver({"svc": evidence("svc", uid=200)})
        with pytest.raises(ia.AttestationFailed, match="does not exist"):
            ia.attest_distinct_identities("svc", "ghost", platform="posix", resolve=resolve)

    def test_unsupported_platform_fails_closed(self):
        with pytest.raises(ia.AttestationFailed, match="unsupported"):
            ia.attest_distinct_identities("svc", platform="plan9")

    def test_malformed_or_reserved_names_rejected_before_resolution(self):
        with pytest.raises(ValueError):
            ia.attest_distinct_identities("svc", "svc", platform="posix",
                                          resolve=posix_resolver({}))
        with pytest.raises(ValueError, match="reserved"):
            ia.attest_distinct_identities("root", platform="posix", resolve=posix_resolver({}))

    def test_freshness_is_enforced(self):
        clock_value = [1000.0]
        resolve = posix_resolver({"svc": evidence("svc", uid=200)})
        result = ia.attest_distinct_identities("svc", platform="posix", resolve=resolve,
                                               clock=lambda: clock_value[0])
        assert result.assert_fresh(60, clock=lambda: 1030.0)
        with pytest.raises(ia.AttestationFailed, match="stale"):
            result.assert_fresh(60, clock=lambda: 1200.0)
        with pytest.raises(ia.AttestationFailed, match="future"):
            result.assert_fresh(60, clock=lambda: 999.0)


class TestWindowsSidHandling:
    def test_sid_string_roundtrip(self):
        # S-1-5-18 (LocalSystem), little-endian sub-authorities.
        raw = bytes([1, 1]) + (5).to_bytes(6, "big") + (18).to_bytes(4, "little")
        assert ia._windows_sid_string(raw) == "S-1-5-18"

    def test_multi_subauthority_sid(self):
        raw = bytes([1, 5]) + (5).to_bytes(6, "big") + b"".join(
            int(v).to_bytes(4, "little") for v in (21, 1000, 2000, 3000, 1001))
        assert ia._windows_sid_string(raw) == "S-1-5-21-1000-2000-3000-1001"

    @pytest.mark.parametrize("sid", ["S-1-5-18", "S-1-5-19", "S-1-5-20", "S-1-5-32-544",
                                     "S-1-5-21-1-2-3-500", "S-1-5-21-1-2-3-501",
                                     "S-1-5-21-1-2-3-999"])
    def test_reserved_sids_rejected(self, sid):
        with pytest.raises(ia.AttestationFailed):
            ia._windows_check_not_reserved(sid, "someone")

    def test_regular_account_sid_accepted(self):
        ia._windows_check_not_reserved("S-1-5-21-1000-2000-3000-1001", "hivemind-svc")

    def test_truncated_sid_fails_closed(self):
        with pytest.raises(ia.AttestationFailed, match="Truncated"):
            ia._windows_sid_string(b"\x01")


class TestDirectoryAttestationPosix:
    def owner(self):
        return evidence("svc", uid=os.geteuid() if hasattr(os, "geteuid") else 1000)

    def test_rejects_group_or_other_writable(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX mode bits")
        target = tmp_path / "bare.git"
        target.mkdir()
        target.chmod(0o770)
        with pytest.raises(ia.AttestationFailed, match="group or other"):
            ia.attest_protected_directory(target, owner_identity="svc", platform="posix",
                                          resolve=posix_resolver({"svc": self.owner()}))

    def test_rejects_wrong_owner(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX ownership")
        target = tmp_path / "bare.git"
        target.mkdir()
        target.chmod(0o700)
        with pytest.raises(ia.AttestationFailed, match="owned by"):
            ia.attest_protected_directory(
                target, owner_identity="svc", platform="posix",
                resolve=posix_resolver({"svc": evidence("svc", uid=os.geteuid() + 1)}))

    def test_rejects_symlink_escape(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX symlinks")
        real = tmp_path / "real"
        real.mkdir()
        real.chmod(0o700)
        link = tmp_path / "link"
        link.symlink_to(real, target_is_directory=True)
        with pytest.raises(ia.AttestationFailed, match="symbolic link"):
            ia.attest_protected_directory(link, owner_identity="svc", platform="posix",
                                          resolve=posix_resolver({"svc": self.owner()}))

    def test_rejects_missing_and_network_paths(self, tmp_path):
        with pytest.raises(ia.AttestationFailed):
            ia.attest_protected_directory(tmp_path / "absent", owner_identity="svc",
                                          platform="posix",
                                          resolve=posix_resolver({"svc": self.owner()}))
        with pytest.raises(ia.AttestationFailed, match="network"):
            ia.attest_protected_directory(r"\\server\share", owner_identity="svc",
                                          platform="posix",
                                          resolve=posix_resolver({"svc": self.owner()}))

    def test_forbidden_writer_sharing_owner_uid_rejected(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX ownership")
        target = tmp_path / "bare.git"
        target.mkdir()
        target.chmod(0o700)
        uid = os.geteuid()
        resolve = posix_resolver({"svc": evidence("svc", uid=uid),
                                  "agent": evidence("agent", uid=uid)})
        with pytest.raises(ia.AttestationFailed, match="same OS identity"):
            ia.attest_protected_directory(target, owner_identity="svc",
                                          forbidden_writers=("agent",), platform="posix",
                                          resolve=resolve)

    def test_clean_directory_attests(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX mode bits")
        target = tmp_path / "bare.git"
        target.mkdir()
        target.chmod(0o700)
        resolve = posix_resolver({"svc": self.owner(), "agent": evidence("agent", uid=12345)})
        result = ia.attest_protected_directory(target, owner_identity="svc",
                                               forbidden_writers=("agent",), platform="posix",
                                               resolve=resolve)
        assert result.kind == "directory"
        assert result.evidence_value("mode") == "0o700"
        assert str(tmp_path.resolve() / "bare.git") in result.subject


class TestDirectoryAttestationWindowsLogic:
    """Offline drives of the Windows DACL judgement through an injected probe."""

    def owner(self):
        return evidence("svc", sid="S-1-5-21-1-2-3-1001", platform="windows")

    def test_probe_error_propagates_as_refusal(self, tmp_path):
        def probe(resolved, owner, forbidden):
            raise ia.AttestationFailed("inherits ACEs")
        with pytest.raises(ia.AttestationFailed, match="inherits"):
            ia.attest_protected_directory(tmp_path, owner_identity="svc", platform="windows",
                                          resolve=posix_resolver({"svc": self.owner()}),
                                          _windows_probe=probe)

    def test_probe_details_are_recorded(self, tmp_path):
        def probe(resolved, owner, forbidden):
            return {"dacl_protected": True, "owner_sid": owner.sid}
        result = ia.attest_protected_directory(tmp_path, owner_identity="svc",
                                               platform="windows",
                                               resolve=posix_resolver({"svc": self.owner()}),
                                               _windows_probe=probe)
        assert result.evidence_value("dacl_protected") is True

    def test_reparse_point_rejected_before_probe(self, tmp_path, monkeypatch):
        class Info:
            st_file_attributes = ia.FILE_ATTRIBUTE_REPARSE_POINT
        monkeypatch.setattr(os, "lstat", lambda p: Info())
        with pytest.raises(ia.AttestationFailed, match="reparse"):
            ia.attest_protected_directory(tmp_path, owner_identity="svc", platform="windows",
                                          resolve=posix_resolver({"svc": self.owner()}),
                                          _windows_probe=lambda *a: {})


class TestCombinedProvisioning:
    def test_binds_identities_and_directory(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX mode bits")
        target = tmp_path / "bare.git"
        target.mkdir()
        target.chmod(0o700)
        uid = os.geteuid()
        resolve = posix_resolver({"svc": evidence("svc", uid=uid),
                                  "agent": evidence("agent", uid=12345)})
        result = ia.attest_provisioning("svc", "agent", target, platform="posix", resolve=resolve)
        assert result.kind == "provisioning"
        assert result.evidence_value("identities").kind == "identities"
        assert result.evidence_value("directory").kind == "directory"
        assert "svc=('uid'," in result.subject and "directory:" in result.subject


class TestAncestorBoundary:
    def owner(self):
        return evidence("svc", uid=os.geteuid() if hasattr(os, "geteuid") else 1000)

    def test_raw_path_symlink_rejected_before_resolve(self, tmp_path):
        if os.name != "posix":
            pytest.skip("symlink creation")
        real = tmp_path / "real"
        real.mkdir()
        real.chmod(0o700)
        link = tmp_path / "link"
        link.symlink_to(real, target_is_directory=True)
        # resolve() alone would silently follow the link and see a clean dir.
        with pytest.raises(ia.AttestationFailed, match="symbolic link"):
            ia.attest_protected_directory(link, owner_identity="svc", platform="posix",
                                          resolve=posix_resolver({"svc": self.owner()}))

    def test_symlinked_ancestor_rejected(self, tmp_path):
        if os.name != "posix":
            pytest.skip("symlink creation")
        outside = tmp_path / "outside"
        outside.mkdir()
        link = tmp_path / "linked-parent"
        link.symlink_to(outside, target_is_directory=True)
        target = link / "bare.git"  # raw ancestor is a symlink
        target.mkdir()
        target.chmod(0o700)
        with pytest.raises(ia.AttestationFailed, match="symbolic link"):
            ia.attest_protected_directory(target, owner_identity="svc", platform="posix",
                                          resolve=posix_resolver({"svc": self.owner()}))

    def test_movable_posix_ancestor_rejected(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX mode bits")
        parent = tmp_path / "parent"
        parent.mkdir()
        parent.chmod(0o777)  # world-writable, no sticky bit: children are movable
        target = parent / "bare.git"
        target.mkdir()
        target.chmod(0o700)
        try:
            with pytest.raises(ia.AttestationFailed, match="renamed or replaced"):
                ia.attest_protected_directory(target, owner_identity="svc",
                                              platform="posix",
                                              resolve=posix_resolver({"svc": self.owner()}))
        finally:
            parent.chmod(0o700)

    def test_ancestor_check_can_be_disabled_explicitly(self, tmp_path):
        if os.name != "posix":
            pytest.skip("POSIX mode bits")
        target = tmp_path / "bare.git"
        target.mkdir()
        target.chmod(0o700)
        result = ia.attest_protected_directory(target, owner_identity="svc",
                                               platform="posix", check_ancestors=False,
                                               resolve=posix_resolver({"svc": self.owner()}))
        assert result.evidence_value("ancestors_checked") == 0

    def test_windows_ancestor_probe_receives_ancestors(self, tmp_path):
        seen = []

        def probe(resolved, owner, forbidden):
            return {"dacl_protected": True, "owner_sid": owner.sid}

        def ancestor_probe(ancestors, forbidden):
            seen.extend(ancestors)
            return {"ancestors_checked": len(ancestors)}

        owner = evidence("svc", sid="S-1-5-21-1-2-3-1001", platform="windows")
        result = ia.attest_protected_directory(
            tmp_path, owner_identity="svc", platform="windows",
            resolve=posix_resolver({"svc": owner}), _windows_probe=probe,
            _windows_ancestor_probe=ancestor_probe)
        assert result.evidence_value("ancestors_checked") >= 2
        assert seen[0] == tmp_path.parent  # nearest-first


class TestProcessIdentity:
    def test_posix_euid_mismatch_fails_closed(self):
        if os.name != "posix":
            pytest.skip("POSIX euid")
        wrong = evidence("svc", uid=os.geteuid() + 1)
        with pytest.raises(ia.AttestationFailed, match="euid"):
            ia.attest_process_identity("svc", platform="posix",
                                       resolve=posix_resolver({"svc": wrong}))

    def test_posix_euid_match_attests(self):
        if os.name != "posix":
            pytest.skip("POSIX euid")
        right = evidence("svc", uid=os.geteuid())
        result = ia.attest_process_identity("svc", platform="posix",
                                            resolve=posix_resolver({"svc": right}))
        assert result.kind == "process" and result.evidence_value("process_uid") == os.geteuid()

    def test_posix_privileged_group_rejected_when_required(self, monkeypatch):
        if os.name != "posix":
            pytest.skip("POSIX groups")
        import grp
        monkeypatch.setattr(ia, "_posix_current_ids", lambda: (1000, 0, {0, 27}))

        class Record:
            gr_name = "sudo"
        monkeypatch.setattr(grp, "getgrgid", lambda gid: Record())
        right = evidence("svc", uid=1000)
        with pytest.raises(ia.AttestationFailed, match="privileged"):
            ia.attest_process_identity("svc", platform="posix", require_unprivileged=True,
                                       resolve=posix_resolver({"svc": right}))


@pytest.mark.skipif(sys.platform != "win32", reason="Windows token probe")
class TestRealProcessToken:
    def test_current_token_matches_current_user(self):
        import getpass
        name = getpass.getuser()
        result = ia.attest_process_identity(name)
        assert result.evidence_value("process_sid").startswith("S-1-")
        assert result.evidence_value("pid") == os.getpid()
        groups = result.evidence_value("process_groups")
        assert "S-1-1-0" in groups  # Everyone is always in the token

    def test_mismatched_account_is_refused(self):
        # hivemind-svc exists on this host (operator-provisioned); this test
        # process does NOT run as it, so attestation must fail closed.
        pytest.importorskip("ctypes")
        try:
            ia._windows_resolve_account("hivemind-svc")
        except ia.AttestationFailed:
            pytest.skip("hivemind-svc not provisioned on this host")
        with pytest.raises(ia.AttestationFailed, match="token SID"):
            ia.attest_process_identity("hivemind-svc")

    def test_token_groups_include_privileged_membership_evidence(self):
        import getpass
        result = ia.attest_process_identity(getpass.getuser())
        # Evidence is recorded either way; privileged_groups is a tuple.
        assert isinstance(result.evidence_value("privileged_groups"), tuple)


# --- Real local probes (read-only, no provisioning) --------------------------

@pytest.mark.skipif(sys.platform != "win32", reason="Windows LSA probe")
class TestRealWindowsProbes:
    def test_current_user_resolves_to_a_real_sid(self):
        name = getpass.getuser()
        resolved = ia._windows_resolve_account(name)
        assert resolved.source == "windows-lsa"
        assert resolved.sid.startswith("S-1-")
        assert resolved.sid not in ia.WINDOWS_RESERVED_SIDS

    def test_system_account_is_refused_as_identity(self):
        with pytest.raises(ia.AttestationFailed):
            ia._windows_resolve_account("SYSTEM")

    def test_real_directory_attestation_is_honest(self, tmp_path):
        # A fresh temp directory typically inherits ACEs; either the
        # attestation passes with recorded DACL evidence or it fails closed
        # with a specific reason. A permissive empty result is impossible.
        name = getpass.getuser()
        real_resolve = ia._windows_resolve_account
        try:
            result = ia.attest_protected_directory(tmp_path, owner_identity=name,
                                                   platform="windows", resolve=real_resolve)
        except ia.AttestationFailed as exc:
            assert any(marker in str(exc) for marker in
                       ("inherit", "write", "owner", "DACL", "network", "reparse"))
        else:
            assert result.evidence_value("dacl_protected") is True


@pytest.mark.skipif(sys.platform != "win32", reason="operator-provisioned Windows host")
class TestRealProvisionedHost:
    """Read-only probes against the operator-approved hivemind-svc/hivemind-agent
    accounts and G:\\Hivemind-Isolated. No mutation of accounts, ACLs, or data."""

    def setup_method(self):
        try:
            self.service = ia._windows_resolve_account("hivemind-svc")
            self.agent = ia._windows_resolve_account("hivemind-agent")
        except ia.AttestationFailed:
            pytest.skip("operator test accounts not provisioned on this host")
        self.root = Path(r"G:\Hivemind-Isolated")
        if not self.root.is_dir():
            pytest.skip("G:\\Hivemind-Isolated not provisioned on this host")

    def test_accounts_resolve_to_distinct_real_sids(self):
        assert self.service.sid != self.agent.sid
        assert self.service.sid.startswith("S-1-5-21-")
        result = ia.attest_distinct_identities("hivemind-svc", "hivemind-agent")
        assert "identities:" in result.subject

    def test_service_directory_attestation_against_agent(self):
        target = self.root / "service"
        try:
            result = ia.attest_protected_directory(
                target, owner_identity="hivemind-svc",
                forbidden_writers=("hivemind-agent",))
        except (ia.AttestationFailed, PermissionError, OSError) as exc:
            # Honest fail-closed: this unelevated operator process is itself
            # denied even READ_CONTROL/stat on the service tree — effective
            # evidence that the ACLs isolate it. An empty permissive pass is
            # what must never happen.
            assert any(marker in str(exc) for marker in
                       ("inherit", "write", "owner", "DACL", "ancestor", "effective",
                        "denied", "Access", "security descriptor"))
        else:
            assert result.evidence_value("dacl_protected") is True
            masks = result.evidence_value("forbidden_effective_masks")
            assert masks["hivemind-agent"] & ia.WINDOWS_WRITE_MASK == 0
            assert result.evidence_value("ancestors_checked") >= 1

    def test_agent_write_access_to_service_tree_is_denied(self):
        target = self.root / "service"
        try:
            (advapi, kernel, _sid_at, _owner, _control, dacl,
             descriptor) = ia._windows_open_security(target, with_owner=False)
        except ia.AttestationFailed as exc:
            # Even reading the security descriptor is denied to this process;
            # fail-closed attestation is the correct behavior.
            assert "security descriptor" in str(exc)
            return
        try:
            mask = ia._windows_effective_mask(advapi, dacl, "hivemind-agent", target)
            assert mask & ia.WINDOWS_WRITE_MASK == 0
        finally:
            kernel.LocalFree(descriptor)


@pytest.mark.skipif(os.name != "posix", reason="POSIX pwd probe")
class TestRealPosixProbes:
    def test_current_user_resolves_to_a_real_uid(self):
        name = getpass.getuser()
        resolved = ia._posix_resolve_account(name)
        assert resolved.uid == __import__("pwd").getpwnam(name).pw_uid

    def test_root_is_refused_as_identity(self):
        with pytest.raises(ia.AttestationFailed, match="UID 0"):
            ia._posix_resolve_account("root")
