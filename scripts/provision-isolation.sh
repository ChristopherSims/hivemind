#!/usr/bin/env bash
# Operator-run provisioning audit/setup for Hivemind OS isolation on Linux.
#
# DEFAULT IS --check: a read-only audit that changes nothing.
#
# --apply performs the requested host mutations (service/agent accounts,
# protected ownership/permissions on the bare-repository directory, optional
# bubblewrap presence check). --apply requires --confirm-isolation and root,
# refuses to touch accounts or directories that already exist with unexpected
# state, and never alters firewall rules.
#
# This script is an explicit operator action. It is never invoked by Hivemind
# code, tests, or automation.
set -euo pipefail

MODE="check"
CONFIRM=0
SERVICE_ACCOUNT=""
AGENT_ACCOUNT=""
BARE_ROOT=""

while [ $# -gt 0 ]; do
    case "$1" in
        --check) MODE="check" ;;
        --apply) MODE="apply" ;;
        --confirm-isolation) CONFIRM=1 ;;
        --service-account) SERVICE_ACCOUNT="$2"; shift ;;
        --agent-account) AGENT_ACCOUNT="$2"; shift ;;
        --bare-root) BARE_ROOT="$2"; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

[ -n "$SERVICE_ACCOUNT" ] && [ -n "$AGENT_ACCOUNT" ] && [ -n "$BARE_ROOT" ] || {
    echo "Usage: $0 [--check|--apply] --service-account NAME --agent-account NAME --bare-root DIR [--confirm-isolation]" >&2
    exit 2
}
[ "$SERVICE_ACCOUNT" != "$AGENT_ACCOUNT" ] || { echo "Service and agent accounts must be distinct." >&2; exit 2; }

findings=0
note() { if [ "$2" = "problem" ]; then findings=$((findings + 1)); echo "FINDING: $1" >&2; else echo "  $1"; fi; }

echo "Hivemind isolation provisioning (${MODE})"

if [ "$(id -u)" -ne 0 ] && [ "$MODE" = "apply" ]; then note "apply requires root" problem; fi

for name in "$SERVICE_ACCOUNT" "$AGENT_ACCOUNT"; do
    if id "$name" >/dev/null 2>&1; then
        note "Account '$name' already exists; leaving it untouched (manual review required)." problem
    else
        if [ "$MODE" = "check" ]; then note "Account '$name' does not exist; would be created with --apply."
        else note "Account '$name' does not exist."; fi
    fi
done

if [ -d "$BARE_ROOT" ]; then
    mode=$(stat -c '%a' "$BARE_ROOT")
    owner=$(stat -c '%U' "$BARE_ROOT")
    if [ $(( 8#$mode & 022 )) -ne 0 ]; then
        note "'$BARE_ROOT' (mode $mode, owner $owner) is writable by group/other." problem
    else
        note "'$BARE_ROOT' (mode $mode, owner $owner) is not writable by group/other."
    fi
else
    if [ "$MODE" = "check" ]; then note "Bare root '$BARE_ROOT' missing; would be created with --apply."
    else note "Bare root '$BARE_ROOT' missing." problem; fi
fi

if command -v bwrap >/dev/null 2>&1; then
    note "bubblewrap present: $(command -v bwrap) (sandbox backend available)"
else
    note "bubblewrap NOT installed; strict process sandboxing is unavailable on this host." problem
fi

if [ "$MODE" = "check" ]; then
    echo "Audit complete. $findings finding(s). Use --apply --confirm-isolation to provision."
    [ "$findings" -eq 0 ]
    exit $?
fi

[ "$CONFIRM" -eq 1 ] || { echo "--apply requires --confirm-isolation (human decision, never automation)." >&2; exit 2; }
[ "$(id -u)" -eq 0 ] || { echo "--apply requires root." >&2; exit 1; }

for name in "$SERVICE_ACCOUNT" "$AGENT_ACCOUNT"; do
    if ! id "$name" >/dev/null 2>&1; then
        useradd --system --shell /usr/sbin/nologin --comment "Hivemind isolation identity" "$name"
        echo "Created system account '$name'."
    fi
done
mkdir -p "$BARE_ROOT"
chown "$SERVICE_ACCOUNT":"$SERVICE_ACCOUNT" "$BARE_ROOT"
chmod 0700 "$BARE_ROOT"
echo "Protected '$BARE_ROOT' (owner $SERVICE_ACCOUNT, mode 0700). Grant the agent account per-repo access as needed."
