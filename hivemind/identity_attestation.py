"""Read-only identity and permission attestation backed by real OS evidence.

This module turns operator-provisioned isolation state into *verified* state.
It never mutates the host: no account creation, no ACL or permission changes,
no privilege elevation, no firewall or credential changes. Provisioning stays
an explicit operator action (``scripts/provision-isolation.ps1`` /
``scripts/provision-isolation.sh``); attestation only *observes* it.

Fail closed everywhere: an unsupported platform, an OS API error, or any
uncertainty about effective access raises :class:`AttestationFailed` instead of
returning a permissive result.

Evidence gathered:

* Windows: account SIDs from ``LookupAccountNameW`` (LSA), DACL ownership and
  inheritance-protection flags from ``GetNamedSecurityInfoW`` /
  ``GetSecurityDescriptorControl``, ACE write masks via ``GetAce``, and
  effective access via the documented ``GetEffectiveRightsFromAclW``. Reparse
  points (junctions/symlinks) are rejected via ``st_file_attributes``.
* POSIX: account UIDs/GIDs from ``pwd``, ownership and mode from ``lstat``,
  plus an ``os.access(..., effective_ids=True)`` effective-access probe.

Every attestation is bound to the exact identity (SID/UID, not just a name —
aliased accounts resolving to the same SID/UID are rejected), to the exact
resolved resource path, and to a ``checked_at`` timestamp; callers must enforce
freshness with :meth:`Attestation.assert_fresh`.
"""
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from .os_isolation import validate_identities


class AttestationFailed(PermissionError):
    """Attestation could not prove the required isolation. Fail closed."""


# --- Shared constants -------------------------------------------------------

# Write-equivalent access bits (Win32 file access mask; reused conceptually on
# POSIX for reporting). Includes control rights that can be turned into write.
WINDOWS_WRITE_MASK = (
    0x00000002  # FILE_WRITE_DATA
    | 0x00000004  # FILE_APPEND_DATA
    | 0x00000010  # FILE_WRITE_EA
    | 0x00000100  # FILE_WRITE_ATTRIBUTES
    | 0x00010000  # DELETE
    | 0x00040000  # WRITE_DAC
    | 0x00080000  # WRITE_OWNER
    | 0x40000000  # GENERIC_WRITE
    | 0x10000000  # GENERIC_ALL
)

# Principals whose write access on a protected directory is never acceptable.
WINDOWS_BROAD_SIDS = frozenset({
    "S-1-1-0",        # Everyone
    "S-1-5-7",        # Anonymous
    "S-1-5-11",       # Authenticated Users
    "S-1-5-15",       # This Organization
    "S-1-5-32-545",   # BUILTIN\Users
    "S-1-5-32-547",   # BUILTIN\Power Users
})

# Accounts that must never be used as isolation identities.
WINDOWS_RESERVED_SIDS = frozenset({
    "S-1-5-18",  # LocalSystem
    "S-1-5-19",  # LocalService
    "S-1-5-20",  # NetworkService
})
WINDOWS_RESERVED_RIDS = frozenset({500, 501, 502, 503})  # Administrator, Guest, ...

# An ancestor only needs to forbid the rights that can *move, replace, or
# re-ACL* the protected directory: FILE_DELETE_CHILD | DELETE | WRITE_DAC |
# WRITE_OWNER | GENERIC_ALL. Plain "create files here" rights (e.g. drive-root
# folder creation) cannot relocate an existing protected child, so they are
# deliberately not part of the ancestor mask.
WINDOWS_ANCESTOR_WRITE_MASK = (0x00000040 | 0x00010000 | 0x00040000
                               | 0x00080000 | 0x10000000)

# Groups whose membership defeats agent-side isolation.
WINDOWS_PRIVILEGED_GROUP_SIDS = frozenset({
    "S-1-5-32-544",   # BUILTIN\Administrators
    "S-1-5-32-548",   # Account Operators
    "S-1-5-32-549",   # Server Operators
    "S-1-5-32-551",   # Backup Operators
})
POSIX_PRIVILEGED_GROUPS = frozenset({"root", "wheel", "sudo", "admin"})

FILE_ATTRIBUTE_REPARSE_POINT = 0x400
FILE_ATTRIBUTE_DIRECTORY = 0x10

DEFAULT_MAX_AGE_SECONDS = 300.0


# --- Attestation record -----------------------------------------------------

@dataclass(frozen=True)
class IdentityEvidence:
    """One account resolved to its OS-level security identifier."""
    name: str
    platform: str
    source: str           # "windows-lsa" | "posix-pwd"
    sid: str = None       # Windows string SID
    uid: int = None       # POSIX
    gid: int = None       # POSIX

    @property
    def key(self):
        """The value that actually distinguishes identities on this platform."""
        return self.sid if self.sid is not None else ("uid", self.uid)


@dataclass(frozen=True)
class Attestation:
    """A point-in-time, resource-bound proof. Enforce freshness before trusting."""
    kind: str             # "identities" | "directory" | "provisioning"
    platform: str
    subject: str          # canonical binding: exact identities + resolved path
    checked_at: float
    evidence: tuple = field(default_factory=tuple)  # (key, value) pairs

    def evidence_value(self, key):
        for item_key, value in self.evidence:
            if item_key == key:
                return value
        raise KeyError(key)

    def assert_fresh(self, max_age_seconds=DEFAULT_MAX_AGE_SECONDS, *, clock=time.time):
        """Raise unless this attestation is younger than ``max_age_seconds``."""
        age = clock() - self.checked_at
        if age < 0:
            raise AttestationFailed("Attestation timestamp is in the future; clock skew suspected")
        if age > max_age_seconds:
            raise AttestationFailed(
                f"Attestation for {self.subject!r} is stale ({age:.1f}s > {max_age_seconds:.1f}s); "
                "re-attest before relying on it")
        return True


# --- Platform account resolvers (read-only) ---------------------------------

def _resolve_platform(platform=None):
    if platform is None:
        platform = {"win32": "windows", "linux": "linux"}.get(
            sys.platform, "posix" if os.name == "posix" else "other")
    if platform == "windows":
        return _windows_resolve_account
    if platform in ("linux", "posix"):
        return _posix_resolve_account
    raise AttestationFailed(f"Identity attestation is unsupported on platform {platform!r}")


def _posix_resolve_account(name):
    import pwd
    try:
        record = pwd.getpwnam(name)
    except KeyError:
        raise AttestationFailed(f"Account {name!r} does not exist on this host") from None
    if record.pw_uid == 0:
        raise AttestationFailed(f"Account {name!r} maps to UID 0; root can never be an isolation identity")
    return IdentityEvidence(name=name, platform="posix", source="posix-pwd",
                            uid=record.pw_uid, gid=record.pw_gid)


def _windows_sid_string(sid_bytes):
    """Convert a raw SID to its string form (pure Python; no OS call needed)."""
    if len(sid_bytes) < 8:
        raise AttestationFailed("Truncated SID returned by the OS")
    revision = sid_bytes[0]
    count = sid_bytes[1]
    authority = int.from_bytes(sid_bytes[2:8], "big")
    parts = [f"S-{revision}-{authority}"]
    for index in range(count):
        offset = 8 + 4 * index
        parts.append(str(int.from_bytes(sid_bytes[offset:offset + 4], "little")))
    return "-".join(parts)


def _windows_check_not_reserved(sid_string, name):
    if sid_string in WINDOWS_RESERVED_SIDS:
        raise AttestationFailed(f"Account {name!r} is a built-in service account ({sid_string})")
    if sid_string.startswith("S-1-5-32-"):
        raise AttestationFailed(f"Account {name!r} lives in the builtin domain ({sid_string})")
    try:
        rid = int(sid_string.rsplit("-", 1)[1])
    except (ValueError, IndexError):
        raise AttestationFailed(f"Unparseable SID for account {name!r}: {sid_string!r}") from None
    if rid in WINDOWS_RESERVED_RIDS or (sid_string.startswith("S-1-5-21-") and rid < 1000):
        raise AttestationFailed(
            f"Account {name!r} is a built-in or system account (RID {rid})")


def _windows_resolve_account(name):
    """Resolve a local/domain account to its SID via LSA. Read-only."""
    import ctypes
    from ctypes import wintypes
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    advapi.LookupAccountNameW.argtypes = [
        wintypes.LPCWSTR, wintypes.LPCWSTR, ctypes.c_void_p,
        ctypes.POINTER(wintypes.DWORD), wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD)]
    sid_size = wintypes.DWORD(0)
    domain_size = wintypes.DWORD(0)
    sid_use = wintypes.DWORD(0)
    advapi.LookupAccountNameW(None, name, None, ctypes.byref(sid_size),
                              None, ctypes.byref(domain_size), ctypes.byref(sid_use))
    if sid_size.value == 0:
        raise AttestationFailed(f"Account {name!r} does not exist on this host")
    sid = ctypes.create_string_buffer(sid_size.value)
    domain = ctypes.create_unicode_buffer(domain_size.value)
    if not advapi.LookupAccountNameW(None, name, sid, ctypes.byref(sid_size),
                                     domain, ctypes.byref(domain_size), ctypes.byref(sid_use)):
        raise AttestationFailed(f"Could not resolve account {name!r} (error {ctypes.get_last_error()})")
    # SID_NAME_USE: 1=user, 2=group, 5=well-known group, 4=alias, ...
    if sid_use.value != 1:
        raise AttestationFailed(
            f"Account {name!r} is not a plain user account (SID_NAME_USE={sid_use.value})")
    sid_string = _windows_sid_string(sid.raw[:sid_size.value])
    _windows_check_not_reserved(sid_string, name)
    return IdentityEvidence(name=name, platform="windows", source="windows-lsa", sid=sid_string)


def attest_distinct_identities(service_identity, agent_identity=None, *, platform=None,
                               resolve=None, clock=time.time):
    """Prove the service/agent accounts exist and are genuinely distinct.

    Distinctness is decided by the OS security identifier (SID/UID), so two
    different spellings that resolve to the same account are rejected. Name
    shape is pre-validated by ``os_isolation.validate_identities``.
    """
    validate_identities(service_identity, agent_identity)
    resolve = resolve or _resolve_platform(platform)
    service = resolve(service_identity)
    evidence = [("service_identity", service)]
    if agent_identity is not None:
        agent = resolve(agent_identity)
        if agent.key == service.key:
            raise AttestationFailed(
                "Service and agent accounts resolve to the same OS identity "
                f"({service.key}); isolation between them is impossible")
        evidence.append(("agent_identity", agent))
    platform_name = service.platform
    subject = "identities:" + ",".join(f"{item.name}={item.key}" for _, item in evidence)
    return Attestation(kind="identities", platform=platform_name, subject=subject,
                       checked_at=clock(), evidence=tuple(evidence))


# --- Directory protection attestation ---------------------------------------

def _check_reparse_or_symlink(path):
    """Reject symlink/reparse escapes before any ACL or mode judgement."""
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise AttestationFailed(f"Cannot stat protected path {path}: {exc}") from exc
    if os.path.islink(path):
        raise AttestationFailed(f"Protected path {path} is a symbolic link")
    attributes = getattr(info, "st_file_attributes", 0)
    if attributes & FILE_ATTRIBUTE_REPARSE_POINT:
        raise AttestationFailed(f"Protected path {path} is a reparse point (junction/symlink)")
    return info


def _posix_directory_evidence(resolved, owner, forbidden):
    import grp
    info = _check_reparse_or_symlink(resolved)
    if info.st_uid != owner.uid:
        raise AttestationFailed(
            f"Protected directory is owned by UID {info.st_uid}, not the attested service "
            f"account {owner.name!r} (UID {owner.uid})")
    if info.st_mode & 0o022:
        raise AttestationFailed(
            f"Protected directory mode {oct(info.st_mode & 0o777)} is writable by group or other")
    for intruder in forbidden:
        if intruder.uid == owner.uid:
            raise AttestationFailed(
                f"Agent account {intruder.name!r} shares the owner's UID; write access is inherent")
        if info.st_mode & 0o020:
            try:
                groups = {item.gr_gid for item in grp.getgrall() if intruder.name in item.gr_mem}
                groups.add(intruder.gid)
            except Exception:
                raise AttestationFailed(
                    f"Cannot enumerate groups to disprove write access for "
                    f"{intruder.name!r}") from None
            if info.st_gid in groups:
                raise AttestationFailed(
                    f"Agent account {intruder.name!r} has group write access to {resolved}")
    effective_write = os.access(resolved, os.W_OK, effective_ids=True)
    return {"mode": oct(info.st_mode & 0o777), "owner_uid": info.st_uid,
            "caller_effective_write": effective_write}


def _windows_open_security(path, *, with_owner=True):
    """Read a file object's security descriptor via advapi32. Read-only.

    Returns ``(advapi, kernel, sid_string_at, owner_sid, control, dacl,
    descriptor)``; the caller must ``kernel.LocalFree(descriptor)`` when done.
    """
    import ctypes
    from ctypes import wintypes
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    SE_FILE_OBJECT = 1
    DACL_SECURITY_INFORMATION = 0x00000004
    OWNER_SECURITY_INFORMATION = 0x00000001

    advapi.GetNamedSecurityInfoW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
    advapi.GetNamedSecurityInfoW.restype = wintypes.DWORD
    advapi.GetSecurityDescriptorControl.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(wintypes.WORD), ctypes.POINTER(wintypes.DWORD)]
    advapi.GetAclInformation.argtypes = [ctypes.c_void_p, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD]
    advapi.GetAce.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
    advapi.GetEffectiveRightsFromAclW.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
                                                  ctypes.POINTER(wintypes.DWORD)]
    advapi.GetEffectiveRightsFromAclW.restype = wintypes.DWORD
    advapi.GetLengthSid.argtypes = [ctypes.c_void_p]
    advapi.GetLengthSid.restype = wintypes.DWORD

    def sid_string_at(pointer):
        length = advapi.GetLengthSid(pointer)
        if not length:
            raise AttestationFailed("OS returned an invalid SID")
        return _windows_sid_string(ctypes.string_at(pointer, length))

    owner_sid = ctypes.c_void_p()
    dacl = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    info = (OWNER_SECURITY_INFORMATION if with_owner else 0) | DACL_SECURITY_INFORMATION
    error = advapi.GetNamedSecurityInfoW(
        str(path), SE_FILE_OBJECT, info,
        ctypes.byref(owner_sid) if with_owner else None, None,
        ctypes.byref(dacl), None, ctypes.byref(descriptor))
    if error != 0:
        raise AttestationFailed(f"Cannot read security descriptor for {path} (error {error})")
    control = wintypes.WORD(0)
    revision = wintypes.DWORD(0)
    if not advapi.GetSecurityDescriptorControl(descriptor, ctypes.byref(control),
                                               ctypes.byref(revision)):
        kernel.LocalFree(descriptor)
        raise AttestationFailed("Cannot read security-descriptor control flags")
    return advapi, kernel, sid_string_at, owner_sid, control, dacl, descriptor


def _windows_effective_mask(advapi, dacl, account_name, path):
    """Effective rights of ``account_name`` against ``path``'s DACL. Fails closed."""
    import ctypes
    from ctypes import wintypes

    class Trustee(ctypes.Structure):
        _fields_ = [("pMultipleTrustee", ctypes.c_void_p),
                    ("MultipleTrusteeOperation", wintypes.DWORD),
                    ("TrusteeForm", wintypes.DWORD),
                    ("TrusteeType", wintypes.DWORD),
                    ("ptstrName", wintypes.LPWSTR)]
    trustee = Trustee(None, 0, 0, 1, account_name)  # TRUSTEE_IS_NAME, TRUSTEE_IS_USER
    mask = wintypes.DWORD(0)
    error = advapi.GetEffectiveRightsFromAclW(dacl, ctypes.byref(trustee), ctypes.byref(mask))
    if error != 0:
        raise AttestationFailed(
            f"Cannot determine effective rights of {account_name!r} on {path} "
            f"(error {error}); refusing to assume safety")
    return mask.value


def _windows_directory_evidence(resolved, owner, forbidden):
    """DACL evidence via advapi32. Read-only; any ambiguity fails closed."""
    import ctypes
    from ctypes import wintypes
    _check_reparse_or_symlink(resolved)

    SE_DACL_PRESENT = 0x0004
    SE_DACL_PROTECTED = 0x1000
    INHERITED_ACE = 0x10

    (advapi, kernel, sid_string_at, owner_sid, control, dacl,
     descriptor) = _windows_open_security(resolved, with_owner=True)
    try:
        if not owner_sid.value:
            raise AttestationFailed(f"Protected directory {resolved} has no owner SID")
        actual_owner = sid_string_at(owner_sid.value)
        # The string conversion above trusts the OS buffer; validate shape.
        if not actual_owner.startswith("S-1-"):
            raise AttestationFailed("Unparseable owner SID on protected directory")
        if actual_owner != owner.sid:
            raise AttestationFailed(
                f"Protected directory owner is {actual_owner}, not the attested service "
                f"account {owner.name!r} ({owner.sid})")

        if not control.value & SE_DACL_PRESENT or not dacl.value:
            raise AttestationFailed(f"Protected directory {resolved} has no DACL")
        if not control.value & SE_DACL_PROTECTED:
            raise AttestationFailed(
                f"Protected directory {resolved} inherits ACEs from its parent; inherited "
                "access cannot be audited here, so protection is unproven (disable "
                "inheritance via scripts/provision-isolation.ps1)")

        class AclSizeInfo(ctypes.Structure):
            _fields_ = [("ace_count", wintypes.DWORD), ("acl_bytes_in_use", wintypes.DWORD),
                        ("acl_bytes_free", wintypes.DWORD)]
        ACL_SIZE_INFORMATION = 2
        size_info = AclSizeInfo()
        if not advapi.GetAclInformation(dacl, ctypes.byref(size_info),
                                        ctypes.sizeof(size_info), ACL_SIZE_INFORMATION):
            raise AttestationFailed("Cannot inspect DACL size")
        allow_write_trustees = []
        for index in range(size_info.ace_count):
            ace = ctypes.c_void_p()
            if not advapi.GetAce(dacl, index, ctypes.byref(ace)):
                raise AttestationFailed("Cannot walk DACL ACEs")
            ace_type, ace_flags = ctypes.c_uint8.from_address(ace.value).value, \
                ctypes.c_uint8.from_address(ace.value + 1).value
            if ace_type != 0:  # ACCESS_ALLOWED_ACE_TYPE only; denies/object ACEs do not grant
                continue
            mask = ctypes.c_uint32.from_address(ace.value + 4).value
            if not mask & WINDOWS_WRITE_MASK:
                continue
            trustee = sid_string_at(ace.value + 8)
            inherited = bool(ace_flags & INHERITED_ACE)
            allow_write_trustees.append((trustee, inherited))
            if trustee in WINDOWS_BROAD_SIDS:
                raise AttestationFailed(
                    f"Protected directory grants write access to broad principal {trustee}")
            if any(intruder.sid == trustee for intruder in forbidden):
                raise AttestationFailed(
                    f"Protected directory grants write access to agent account SID {trustee}")
            if inherited:
                raise AttestationFailed(
                    f"Protected directory has an inherited write ACE for {trustee}")
    finally:
        kernel.LocalFree(descriptor)

    effective = {}
    for intruder in forbidden:
        mask = _windows_effective_mask(advapi, dacl, intruder.name, resolved)
        effective[intruder.name] = mask
        if mask & WINDOWS_WRITE_MASK:
            raise AttestationFailed(
                f"Agent account {intruder.name!r} has effective write access to {resolved} "
                f"(mask {mask:#x})")
    return {"dacl_protected": True, "owner_sid": owner.sid,
            "allow_write_trustees": tuple(allow_write_trustees),
            "forbidden_effective_masks": effective}


def attest_protected_directory(path, *, owner_identity, forbidden_writers=(), platform=None,
                               resolve=None, clock=time.time, check_ancestors=True,
                               _windows_probe=None, _posix_probe=None,
                               _windows_ancestor_probe=None, _posix_ancestor_probe=None):
    """Prove a bare-repository directory is protected against the agents.

    ``owner_identity`` must be the service account; ``forbidden_writers`` are
    the agent accounts that must hold no (effective) write access. Fails closed
    on symlinks/reparse points (checked on the *raw* path and every ancestor
    before resolution, so a junction cannot hide behind ``resolve()``),
    network roots, inheritance-enabled DACLs, broad-principal write ACEs,
    wrong ownership, agent-writable/movable ancestors (``check_ancestors``),
    and any API uncertainty.
    """
    raw = str(path)
    if raw.startswith("\\\\"):
        raise AttestationFailed("Protected directories must not live on a network filesystem")
    resolve = resolve or _resolve_platform(platform)
    owner = resolve(owner_identity)
    forbidden = tuple(resolve(name) for name in forbidden_writers)
    if any(item.key == owner.key for item in forbidden):
        raise AttestationFailed(
            "A forbidden-writer account resolves to the same OS identity as the owner")
    root = Path(raw)
    if not root.is_dir():
        raise AttestationFailed(f"Protected path {raw} must be an existing directory")
    # Check the RAW path and every ancestor for symlink/reparse escapes BEFORE
    # resolving: resolve() would silently follow a junction and hide it.
    _check_reparse_or_symlink(root)
    ancestors = _raw_ancestors(root)
    resolved = root.resolve()
    if str(resolved).startswith("\\\\"):
        raise AttestationFailed("Protected directories must not live on a network filesystem")
    _check_reparse_or_symlink(resolved)  # reject escapes before any trust judgement

    if owner.platform == "windows":
        probe = _windows_probe or _windows_directory_evidence
        ancestor_probe = _windows_ancestor_probe or _windows_ancestor_evidence
    elif owner.platform in ("posix", "linux"):
        probe = _posix_probe or _posix_directory_evidence
        ancestor_probe = _posix_ancestor_probe or _posix_ancestor_evidence
    else:
        raise AttestationFailed(f"Directory attestation is unsupported on {owner.platform!r}")
    if check_ancestors:
        ancestor_details = (ancestor_probe(ancestors, forbidden)
                            if owner.platform == "windows" else ancestor_probe(ancestors))
    else:
        ancestor_details = {"ancestors_checked": 0}
    details = {**ancestor_details, **probe(resolved, owner, forbidden)}

    subject = f"directory:{resolved}:owner={owner.key}"
    evidence = [("path", str(resolved)), ("owner_identity", owner),
                ("forbidden_writers", forbidden)]
    evidence.extend(sorted(details.items(), key=lambda item: item[0]))
    return Attestation(kind="directory", platform=owner.platform, subject=subject,
                       checked_at=clock(), evidence=tuple(evidence))


# --- Ancestor boundary checks ------------------------------------------------

def _raw_ancestors(path):
    """Every ancestor of ``path`` (nearest-first, anchor last), unresolved."""
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    chain = []
    current = candidate.parent
    while True:
        chain.append(current)
        if current.parent == current:
            break
        current = current.parent
    return chain


def _posix_ancestor_evidence(ancestors):
    for ancestor in ancestors:
        _check_reparse_or_symlink(ancestor)
        mode = os.lstat(ancestor).st_mode
        if mode & 0o022 and not mode & 0o1000:
            raise AttestationFailed(
                f"Ancestor {ancestor} is group/other-writable without a sticky bit; "
                "the protected directory could be renamed or replaced through it")
    return {"ancestors_checked": len(ancestors)}


def _windows_ancestor_evidence(ancestors, forbidden):
    SE_DACL_PRESENT = 0x0004
    for ancestor in ancestors:
        _check_reparse_or_symlink(ancestor)
        if not forbidden:
            continue
        (advapi, kernel, _sid_at, _owner, control, dacl,
         descriptor) = _windows_open_security(ancestor, with_owner=False)
        try:
            if not control.value & SE_DACL_PRESENT or not dacl.value:
                raise AttestationFailed(
                    f"Ancestor {ancestor} has no readable DACL; cannot disprove agent "
                    "rename/replace access")
            for intruder in forbidden:
                mask = _windows_effective_mask(advapi, dacl, intruder.name, ancestor)
                if mask & WINDOWS_ANCESTOR_WRITE_MASK:
                    raise AttestationFailed(
                        f"Agent account {intruder.name!r} has effective rename/replace "
                        f"access to ancestor {ancestor} (mask {mask:#x}); the protected "
                        "directory could be moved or re-ACL'd through it")
        finally:
            kernel.LocalFree(descriptor)
    return {"ancestors_checked": len(ancestors)}


# --- Current-process identity attestation ------------------------------------

def _windows_current_token():
    """Real current-process token evidence via OpenProcessToken/GetTokenInformation.

    Returns ``(user_sid, group_sids, elevated)``. No whoami/name assumptions.
    """
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    advapi.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD,
                                        ctypes.POINTER(wintypes.HANDLE)]
    advapi.GetTokenInformation.argtypes = [wintypes.HANDLE, wintypes.DWORD,
                                           ctypes.c_void_p, wintypes.DWORD,
                                           ctypes.POINTER(wintypes.DWORD)]
    advapi.GetLengthSid.argtypes = [ctypes.c_void_p]
    advapi.GetLengthSid.restype = wintypes.DWORD
    TOKEN_QUERY = 0x0008
    TOKEN_USER, TOKEN_GROUPS, TOKEN_ELEVATION = 1, 2, 20

    token = wintypes.HANDLE()
    if not advapi.OpenProcessToken(kernel.GetCurrentProcess(), TOKEN_QUERY,
                                   ctypes.byref(token)):
        raise AttestationFailed(
            f"Cannot open the current process token (error {ctypes.get_last_error()})")
    try:
        def token_info(class_id, what):
            size = wintypes.DWORD(0)
            advapi.GetTokenInformation(token, class_id, None, 0, ctypes.byref(size))
            if size.value == 0:
                raise AttestationFailed(f"Cannot size the process token {what}")
            buffer = ctypes.create_string_buffer(size.value)
            if not advapi.GetTokenInformation(token, class_id, buffer, size,
                                              ctypes.byref(size)):
                raise AttestationFailed(
                    f"Cannot read the process token {what} (error {ctypes.get_last_error()})")
            return buffer

        def sid_at(pointer):
            length = advapi.GetLengthSid(pointer)
            if not length:
                raise AttestationFailed("OS returned an invalid SID in the process token")
            return _windows_sid_string(ctypes.string_at(pointer, length))

        user_buffer = token_info(TOKEN_USER, "user")
        user_pointer = ctypes.c_void_p.from_buffer(user_buffer, 0).value
        user_sid = sid_at(user_pointer)

        class SidAndAttributes(ctypes.Structure):
            _fields_ = [("sid", ctypes.c_void_p), ("attributes", wintypes.DWORD)]
        groups_buffer = token_info(TOKEN_GROUPS, "groups")
        count = ctypes.c_uint32.from_buffer(groups_buffer, 0).value
        base = ctypes.addressof(groups_buffer) + 4
        base += -base % ctypes.alignment(SidAndAttributes)
        group_sids = set()
        for index in range(count):
            entry = SidAndAttributes.from_address(
                base + index * ctypes.sizeof(SidAndAttributes))
            if entry.sid:
                group_sids.add(sid_at(entry.sid))

        elevation_buffer = token_info(TOKEN_ELEVATION, "elevation")
        elevated = bool(ctypes.c_uint32.from_buffer(elevation_buffer, 0).value)
        return user_sid, group_sids, elevated
    finally:
        kernel.CloseHandle(token)


def _posix_current_ids():
    return os.geteuid(), os.getegid(), set(os.getgroups())


def attest_process_identity(expected_identity, *, platform=None, resolve=None,
                            clock=time.time, require_unprivileged=False):
    """Prove the *current process* actually runs as ``expected_identity``.

    Name-level resolution alone cannot prove this: the service could be
    started under the wrong account. Windows uses the real process token
    (``OpenProcessToken`` / ``GetTokenInformation``: user SID, group SIDs,
    elevation); POSIX uses the real ``euid``/``egid``/supplementary groups.
    Mismatch, alias-equivalence surprises, and (with ``require_unprivileged``)
    privileged-group membership or elevation all fail closed.
    """
    resolve = resolve or _resolve_platform(platform)
    expected = resolve(expected_identity)
    if expected.platform == "windows":
        actual_sid, groups, elevated = _windows_current_token()
        if actual_sid != expected.sid:
            raise AttestationFailed(
                f"Current process token SID is {actual_sid}, not the expected identity "
                f"{expected_identity!r} ({expected.sid})")
        privileged = sorted(set(groups) & WINDOWS_PRIVILEGED_GROUP_SIDS)
        if require_unprivileged and (privileged or elevated):
            raise AttestationFailed(
                f"Process runs as {expected_identity!r} but holds privileged access "
                f"(groups={privileged}, elevated={elevated}); it cannot act as an "
                "isolated agent identity")
        evidence = (("expected_identity", expected), ("process_sid", actual_sid),
                    ("process_groups", tuple(sorted(groups))), ("elevated", elevated),
                    ("privileged_groups", tuple(privileged)), ("pid", os.getpid()))
        return Attestation(kind="process", platform="windows",
                           subject=f"process:{os.getpid()}:sid={actual_sid}",
                           checked_at=clock(), evidence=evidence)
    if expected.platform in ("posix", "linux"):
        import grp
        euid, egid, groups = _posix_current_ids()
        if euid != expected.uid:
            raise AttestationFailed(
                f"Current process euid is {euid}, not the expected identity "
                f"{expected_identity!r} (UID {expected.uid})")
        names = set()
        for gid in set(groups) | {egid}:
            try:
                names.add(grp.getgrgid(gid).gr_name)
            except KeyError:
                continue
        privileged = sorted(names & POSIX_PRIVILEGED_GROUPS)
        if require_unprivileged and (privileged or euid == 0):
            raise AttestationFailed(
                f"Process runs as {expected_identity!r} but holds privileged access "
                f"(groups={privileged}, euid={euid}); it cannot act as an isolated "
                "agent identity")
        evidence = (("expected_identity", expected), ("process_uid", euid),
                    ("process_egid", egid), ("process_groups", tuple(sorted(groups))),
                    ("privileged_groups", tuple(privileged)), ("pid", os.getpid()))
        return Attestation(kind="process", platform="posix",
                           subject=f"process:{os.getpid()}:uid={euid}",
                           checked_at=clock(), evidence=evidence)
    raise AttestationFailed(
        f"Process identity attestation is unsupported on {expected.platform!r}")


def attest_provisioning(service_identity, agent_identity, bare_root, *, platform=None,
                        resolve=None, clock=time.time):
    """One attestation binding identities and the protected directory together.

    This is the preflight a required-isolation startup should run and cache
    (with :meth:`Attestation.assert_fresh` on reuse).
    """
    identities = attest_distinct_identities(service_identity, agent_identity,
                                            platform=platform, resolve=resolve, clock=clock)
    directory = attest_protected_directory(
        bare_root, owner_identity=service_identity,
        forbidden_writers=(agent_identity,) if agent_identity else (),
        platform=platform, resolve=resolve, clock=clock)
    subject = f"provisioning:{identities.subject}|{directory.subject}"
    return Attestation(kind="provisioning", platform=identities.platform, subject=subject,
                       checked_at=clock(),
                       evidence=(("identities", identities), ("directory", directory)))
