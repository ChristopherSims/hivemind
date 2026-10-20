FROM python:3.13-slim-bookworm@sha256:a1165e272e578941b84abc79e4ab38a0305cd12803a5c4247979ac7655f4d641
RUN apt-get update && apt-get install -y --no-install-recommends bubblewrap git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/hivemind
COPY pyproject.toml ./
COPY hivemind ./hivemind
RUN pip install --no-cache-dir '.[dev,mcp,runtimes]' 'droid-sdk==0.5.0'
RUN pip install --no-cache-dir 'tokenizers==0.23.3'
COPY tests ./tests
COPY benchmarks/runtime-linux/cgroup_probe.py ./cgroup_probe.py
RUN useradd --create-home --uid 1000 tester
USER tester
ENV PYTHONDONTWRITEBYTECODE=1
CMD ["python", "-m", "pytest", "-p", "no:cacheprovider", "tests/test_os_isolation.py", "tests/test_vendor_sandbox.py", "tests/test_vendor_gateway.py", "tests/test_runtime_safety_hardening.py", "-q", "-rs"]
