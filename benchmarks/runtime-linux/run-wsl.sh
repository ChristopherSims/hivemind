#!/usr/bin/env bash
# Mount test-only Python inside a private, unprivileged mount/user namespace.
# /usr/local and ld.so.cache on the actual WSL host are NEVER modified.
set -euo pipefail
root=${1:?Provide the temporary root printed by prepare-wsl.sh}
shift
if [[ "$root" != /tmp/hivemind-runtime-tests.* || ! -f "$root/ld.so.cache" ]]; then
    echo "Expected a prepared temporary WSL test root" >&2
    exit 2
fi
exec unshare --user --map-root-user --mount bash -c '
    set -euo pipefail
    root=$1
    shift
    mount --bind "$root/python" /usr/local
    mount --bind "$root/ld.so.cache" /etc/ld.so.cache
    export PATH="$root:/usr/local/bin:/usr/bin:/bin"
    export LD_LIBRARY_PATH=/usr/local/lib
    export PYTHONDONTWRITEBYTECODE=1
    cd "$root/project"
    exec timeout --signal=TERM --kill-after=10s 180s \
        /usr/local/bin/python3 -I -m pytest -p no:cacheprovider "$@"
' bash "$root" "$@"
