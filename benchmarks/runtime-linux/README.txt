Linux runtime testing
=====================

Build from the repository root:
  docker build -t hivemind-runtime-tests:2026-10-17 -f benchmarks/runtime-linux/Dockerfile .

The Dockerfile-specific ignore file admits only Python code/tests and package
metadata. Credentials, .hivemind-data, .git, dashboards and Windows environments
are excluded. Dependencies are installed in the image, not in the WSL system.
The image defaults to UID 1000. The default-mode commands below use no privileged
flags, capabilities or security exceptions. The separately approved sandbox and
cgroup modes are explicit below. No mode mounts host cgroups or the Docker socket.

Default Docker blocks bubblewrap namespace creation on this machine. Its
positive sandbox checks FAIL and are not containment evidence. Verify actual
refusal instead:
  docker run --rm --network none hivemind-runtime-tests:2026-10-17 python -m pytest -p no:cacheprovider tests/test_linux_runtime_deployment.py -q -rs

The original image CMD deliberately runs positive deployment checks; it is not
an assurance that any Docker host permits nested bubblewrap.

WSL2 permits unprivileged namespaces without security changes. Extract the
image's test environment to an automatically created /tmp directory:
  wsl -d Ubuntu -- bash /mnt/g/Droid/Hivemind/benchmarks/runtime-linux/prepare-wsl.sh

Use the WSL_TEST_ROOT printed by that command, for example:
  wsl -d Ubuntu -- bash /mnt/g/Droid/Hivemind/benchmarks/runtime-linux/run-wsl.sh /tmp/hivemind-runtime-tests.EXAMPLE tests/test_os_isolation.py tests/test_vendor_sandbox.py tests/test_vendor_gateway.py tests/test_runtime_safety_hardening.py tests/test_linux_runtime_deployment.py -q -rs

The runner enters an unprivileged user/mount namespace and binds the extracted
Python runtime and its loader cache at their expected locations ONLY INSIDE
THAT NAMESPACE. The WSL host /usr/local and /etc/ld.so.cache are unchanged.
Namespaced UID 0 maps to the existing unprivileged WSL user, not host root.
This is a synthetic test deployment, not an installation or ready service.
An inner 180-second timeout bounds tests even if a Windows wsl.exe client exits.

Real tests cover filesystem read/write limits, private network namespaces,
unbound host-file denial, direct host-loopback denial, allowlisted gateway
success, metadata-destination denial, exact-input native JSON verification,
and real refusal of missing cgroup delegation.

Default modes do not provide an owned writable cgroup subtree. The approved
privileged probe below creates a delegation only within its container namespace.
Live model turns, authenticated vendor enrollment and continuation remain gated.
No production credentials are used.

The image and WSL /tmp snapshots are retained for reproduction. Extraction
containers are removed automatically. No Docker pruning or blanket /tmp
cleanup is performed. The Docker diagnostic broad suite timed out; use the WSL
matrix for completed Linux regression evidence, not the incomplete Docker run.

Approved sandbox-enabled Docker mode
-----------------------------------
The operator separately approved container-only seccomp/AppArmor exceptions:
  docker run --rm --network none --security-opt seccomp=unconfined --security-opt apparmor=unconfined hivemind-runtime-tests:2026-10-17 timeout --signal=TERM --kill-after=10s 120s python -m pytest -p no:cacheprovider tests/test_os_isolation.py tests/test_vendor_sandbox.py tests/test_vendor_gateway.py tests/test_runtime_safety_hardening.py tests/test_linux_runtime_deployment.py -q -rs

This is still UID 1000, nonprivileged, without host directories or credentials.
Outer Docker network=none is not counted as gateway isolation proof: the real
test creates an outer-container loopback server, proves the nested sandbox
cannot connect directly, then proves its explicit Unix-socket gateway can.

Approved private cgroup kernel test
----------------------------------
The operator separately approved a privileged disposable container:
  docker run --rm --network none --privileged --cgroupns private --user 0 hivemind-runtime-tests:2026-10-17 python -I /opt/hivemind/cgroup_probe.py

Privileged containers weaken the Docker isolation boundary. Run only trusted
synthetic tests in this mode, never vendor code or arbitrary agent commands.
The setup refuses before mutation unless it is PID 1, sees cgroup membership
0::/, sees only PID 1 in that root, and sees no existing subgroups. It creates
one unique container-only subtree, delegates to UID 1000, runs the real helper
as that unprivileged user, kills/drains residual descendants and removes the
owned subtree before exit. It never delegates or mounts a host cgroup tree.

The real tests cover admission before GO, detached setsid descendants with
both live and exited parents, populated=0 drain proof, and subgroup removal.
This is not proof of production service-crash handling or protection against
malicious same-UID cgroup migration. Live/authenticated vendor tests remain gated.

Measured approved-mode results: sandbox suite 146 passed/2 branch skips; real
cgroup suite 2 passed with no skips. A separate broader Docker matrix exited
nonzero with only partial output and is NOT a pass. Its root cause remains open.
Evidence: ../runtime-safety-docker-enabled-2026-10-17.json.
