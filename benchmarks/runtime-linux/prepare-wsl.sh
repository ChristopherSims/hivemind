#!/usr/bin/env bash
# Extract an already-built test image into disposable WSL /tmp storage.
# No system install, sudo, mounted host directory, or security-policy changes.
set -euo pipefail
root=$(mktemp -d /tmp/hivemind-runtime-tests.XXXXXXXX)
container=$(docker create --label hivemind.purpose=runtime-test-extraction \
    hivemind-runtime-tests:2026-10-17)
trap 'docker rm "$container" >/dev/null' EXIT
docker cp "${container}:/usr/local" "$root/python"
docker cp "${container}:/usr/bin/bwrap" "$root/bwrap"
docker cp "${container}:/etc/ld.so.cache" "$root/ld.so.cache"
docker cp "${container}:/opt/hivemind" "$root/project"
export LD_LIBRARY_PATH="$root/python/lib"
"$root/python/bin/python3" -I -c 'import sys,pytest; print(sys.prefix); print(pytest.__version__)'
printf 'WSL_TEST_ROOT=%s\n' "$root"
