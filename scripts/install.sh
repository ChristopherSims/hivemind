#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
check_only=false
if [ "${1:-}" = "--check-only" ]; then check_only=true
elif [ "$#" -ne 0 ]; then echo "Usage: sh scripts/install.sh [--check-only]" >&2; exit 2
fi
if ! command -v git >/dev/null 2>&1; then
    if "$check_only"; then echo "Git is missing; rerun without --check-only to install it." >&2; exit 1; fi
    elevate=""
    if [ "$(id -u)" -ne 0 ]; then
        if ! command -v sudo >/dev/null 2>&1; then echo "Administrator package installation is required." >&2; exit 1; fi
        elevate="sudo -n"
    fi
    if command -v apt-get >/dev/null 2>&1; then
        $elevate apt-get update
        $elevate apt-get install -y git
    elif command -v dnf >/dev/null 2>&1; then $elevate dnf install -y git
    elif command -v apk >/dev/null 2>&1; then $elevate apk add git
    elif command -v brew >/dev/null 2>&1; then brew install git
    else echo "No supported package manager; install standard Git and rerun." >&2; exit 1
    fi
fi
version=$(git --version)
major=$(printf '%s\n' "$version" | awk '{split($3,v,"."); print v[1]}')
minor=$(printf '%s\n' "$version" | awk '{split($3,v,"."); print v[2]}')
if [ "$major" -lt 2 ] || { [ "$major" -eq 2 ] && [ "$minor" -lt 38 ]; }; then
    echo "Git 2.38 or newer is required; upgrade your system Git." >&2; exit 1
fi
printf '%s\n' "$version"
if "$check_only"; then exit 0; fi
if ! command -v uv >/dev/null 2>&1; then echo "Git is ready. Install uv and rerun." >&2; exit 1; fi
uv sync --locked --extra embedding --extra mcp --extra runtimes --project "$root"
echo "Installed. Configure your existing Git author identity, then initialize Hivemind. No model was downloaded and no service was exposed."
