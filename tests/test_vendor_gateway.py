"""Offline adversarial contracts and Linux-only real namespace enforcement."""
import socket
import struct
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from hivemind import vendor_gateway as gateway
from hivemind import vendor_sandbox as sandbox
from hivemind.os_isolation import IsolationRefused


def resolver(host, port, **kwargs):
    ip = "127.0.0.1" if host == "127.0.0.1" else "8.8.8.8"
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))]


def policy():
    return gateway.GatewayPolicy(model_origins=["https://model.example"],
                                 hivemind_origin="http://127.0.0.1:8765", resolver=resolver)


@pytest.mark.parametrize("origin", [
    "http://model.example", "https://user:secret@model.example",
    "https://model.example/v1", "https://model.example?x=1",
    "https://model.example#x", "https://model.example\n", "file:///tmp/model",
])
def test_model_grants_reject_unsafe_or_ambiguous_origins(origin):
    with pytest.raises(ValueError):
        gateway.GatewayPolicy(model_origins=[origin], hivemind_origin="http://127.0.0.1:8765",
                              resolver=resolver)


@pytest.mark.parametrize("ip", ["127.0.0.1", "10.0.0.1", "169.254.169.254", "::1",
                               "::ffff:127.0.0.1", "0.0.0.0", "224.0.0.1"])
def test_model_dns_cannot_grant_private_or_metadata_destinations(ip):
    def resolve(host, port, **kwargs):
        family = socket.AF_INET6 if ":" in ip else socket.AF_INET
        return [(family, socket.SOCK_STREAM, 6, "", (ip, port))]
    with pytest.raises(ValueError):
        gateway.GatewayPolicy(model_origins=["https://model.example"],
                              hivemind_origin="http://127.0.0.1:8765", resolver=resolve)


@pytest.mark.parametrize("ip", ["169.254.169.254", "::ffff:169.254.169.254", "0.0.0.0", "224.0.0.1"])
def test_hivemind_grant_cannot_be_an_instance_metadata_or_wildcard_address(ip):
    def resolve(host, port, **kwargs):
        value = "8.8.8.8" if host == "model.example" else ip
        return [(socket.AF_INET6 if ":" in value else socket.AF_INET,
                 socket.SOCK_STREAM, 6, "", (value, port))]
    with pytest.raises(ValueError):
        gateway.GatewayPolicy(model_origins=["https://model.example"],
                              hivemind_origin="http://internal.example:8765", resolver=resolve)


@pytest.mark.parametrize("target", ["evil.example:443", "model.example:444",
                                   "169.254.169.254:80", "user@model.example:443",
                                   "model.example:443/path", "model.example:443?x=1",
                                   "model.example:443\r\nX: y"])
def test_connect_targets_must_match_the_exact_origin(target):
    with pytest.raises((ValueError, PermissionError)):
        policy().target("CONNECT", target)


def test_valid_targets_and_plain_hivemind_path():
    destination, path = policy().target("CONNECT", "MODEL.EXAMPLE:443")
    assert destination.host == "model.example" and path is None
    destination, path = policy().target("POST", "http://127.0.0.1:8765/v1/events?limit=1")
    assert destination.kind == "hivemind" and path == "/v1/events?limit=1"
    with pytest.raises(PermissionError):
        policy().target("GET", "http://model.example:443/v1")


def test_dns_addresses_are_pinned_and_connection_does_not_resolve_again(monkeypatch):
    connected = []
    class Connection:
        def settimeout(self, value):
            pass
        def connect(self, address):
            connected.append(address)
        def close(self):
            pass
    destination, _ = policy().target("CONNECT", "model.example:443")
    monkeypatch.setattr(gateway.socket, "socket", lambda family, kind: Connection())
    gateway.connect_pinned(destination)
    assert connected == [("8.8.8.8", 443)]


class FakeSocket:
    def __init__(self, body=b"", uid=123):
        self.body, self.uid, self.sent = body, uid, bytearray()
    def settimeout(self, value):
        pass
    def getsockopt(self, *args):
        return struct.pack("3i", 1, self.uid, 1)
    def recv(self, size):
        result, self.body = self.body[:size], self.body[size:]
        return result
    def sendall(self, body):
        self.sent.extend(body)
    def close(self):
        pass


def run_request(monkeypatch, body, uid=123):
    monkeypatch.setattr(gateway.socket, "SO_PEERCRED", 17, raising=False)
    request, upstream = FakeSocket(body, uid), FakeSocket()
    connections = []
    relays = []
    def connect(destination):
        connections.append(destination)
        return upstream
    monkeypatch.setattr(gateway, "connect_pinned", connect)
    monkeypatch.setattr(gateway, "relay", lambda *args, **kwargs: relays.append(kwargs))
    server = SimpleNamespace(policy=policy(), allowed_uid=123,
                             slots=threading.BoundedSemaphore(1), connections=set(),
                             connections_lock=threading.Lock(), closing=threading.Event())
    gateway._ProxyHandler(request, None, server)
    return request, upstream, connections, relays


@pytest.mark.parametrize("body", [
    b"CONNECT evil.example:443 HTTP/1.1\r\n\r\n",
    b"CONNECT model.example:443 HTTP/1.1\r\nContent-Length: 1\r\n\r\n",
    b"POST http://127.0.0.1:8765/v1 HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n",
    b"POST http://127.0.0.1:8765/v1 HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\n",
    b"POST http://127.0.0.1:8765/v1 HTTP/1.1\r\nContent-Length: 999999999\r\n\r\n",
])
def test_rejected_requests_never_open_an_upstream(monkeypatch, body):
    request, _, connections, _ = run_request(monkeypatch, body)
    assert b"403" in request.sent and not connections


def test_peer_identity_is_required_before_parsing_or_connecting(monkeypatch):
    request, _, connections, _ = run_request(
        monkeypatch, b"CONNECT model.example:443 HTTP/1.1\r\n\r\n", uid=999)
    assert b"403" in request.sent and not connections


def test_plain_http_strips_proxy_credentials_and_cannot_pipeline_to_upstream(monkeypatch):
    _, upstream, connections, relays = run_request(monkeypatch,
        b"POST http://127.0.0.1:8765/v1/events HTTP/1.1\r\n"
        b"Proxy-Authorization: synthetic-only\r\nContent-Length: 2\r\n\r\n{}"
        b"GET http://evil.example/ HTTP/1.1\r\n\r\n")
    assert len(connections) == 1 and b"synthetic-only" not in upstream.sent
    assert upstream.sent.endswith(b"{}") and b"evil.example" not in upstream.sent
    assert relays == [{"duplex": False}]


def test_filesystem_probe_cannot_authorize_network_isolation():
    probe = sandbox.ProbeEvidence("bubblewrap", "passed", "synthetic", time.time())
    result = sandbox.assess_vendor_sandbox("oh-my-pi", platform="linux",
        which=lambda name: "/usr/bin/bwrap", probe=probe, gateway=True,
        require_network_isolation=True)
    assert not result.allowed and not result.network_isolated


def test_network_decision_requires_fresh_network_probe():
    probe = sandbox.ProbeEvidence("bubblewrap", "passed", "synthetic", time.time(), True)
    result = sandbox.assess_vendor_sandbox("oh-my-pi", platform="linux",
        which=lambda name: "/usr/bin/bwrap", probe=probe, gateway=True,
        require_network_isolation=True)
    assert result.allowed and result.scope == "filesystem-and-gateway"
    assert result.network_isolated
    stale = sandbox.ProbeEvidence("bubblewrap", "passed", "synthetic", time.time() - 1000, True)
    assert not sandbox.assess_vendor_sandbox("oh-my-pi", platform="linux",
        which=lambda name: "/usr/bin/bwrap", probe=stale, gateway=True,
        require_network_isolation=True).allowed


def test_strict_namespace_never_shares_host_network(tmp_path):
    command = sandbox._bwrap_command("/usr/bin/bwrap", tmp_path, tmp_path / "config",
                                     [], [], assessing=True, network_isolated=True)
    assert "--unshare-all" in command and "--share-net" not in command


def test_gateway_read_mount_cannot_expose_worker_directory(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}")
    with pytest.raises(IsolationRefused, match="private worker"):
        sandbox._validate_gateway_reads([tmp_path], config, tmp_path / "private.sock")


def test_gateway_read_mount_cannot_contain_devices_or_other_sockets(tmp_path, monkeypatch):
    file = tmp_path / "device"
    file.write_text("synthetic")
    original = Path.stat
    def stat(path, *args, **kwargs):
        if path == file:
            return SimpleNamespace(st_mode=0o020600)
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "stat", stat)
    with pytest.raises(IsolationRefused, match="sockets or devices"):
        sandbox._validate_gateway_reads([file], tmp_path / "config", tmp_path / "private.sock")


@pytest.mark.skipif(sys.platform == "linux", reason="Non-Linux refusal contract")
def test_gateway_and_network_probe_refuse_this_host(tmp_path):
    with pytest.raises(IsolationRefused):
        gateway.ManagedGateway(policy(), tmp_path)
    assert sandbox.probe_bwrap_gateway_enforcement().status == "failed"


@pytest.mark.skipif(sys.platform != "linux", reason="Requires actual Linux kernel isolation")
def test_real_linux_namespace_and_gateway(tmp_path):
    import http.server
    import shutil
    import subprocess
    if not shutil.which("bwrap"):
        pytest.skip("bubblewrap is not installed")
    evidence = sandbox.probe_bwrap_gateway_enforcement()
    if evidence.status != "passed":
        pytest.skip("Host kernel refused bubblewrap; no enforcement claim")
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")
        def log_message(self, *args):
            pass
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    broker = None
    try:
        origin = f"http://127.0.0.1:{server.server_port}"
        rules = gateway.GatewayPolicy(model_origins=["https://model.example"],
                                      hivemind_origin=origin, resolver=resolver)
        broker = gateway.ManagedGateway(rules, tmp_path)
        workspace, session = tmp_path / "workspace", tmp_path / "sessions"
        workspace.mkdir()
        session.mkdir()
        config = tmp_path / "config.json"
        config.write_text("{}")
        hidden = tmp_path / "unbound.txt"
        hidden.write_text("synthetic")
        python = str(Path(shutil.which("python3")).resolve())
        script = (
            "import urllib.request,urllib.error,pathlib,socket\n"
            f"assert not pathlib.Path({str(hidden)!r}).exists()\n"
            "pathlib.Path('inside.txt').write_text('ok')\n"
            f"assert urllib.request.urlopen({origin!r}).read()==b'ok'\n"
            "try: urllib.request.urlopen('http://169.254.169.254/latest/meta-data')\n"
            "except urllib.error.HTTPError as error: assert error.code==403\n"
            "else: raise AssertionError('ungranted destination allowed')\n"
            "connection=socket.socket(); connection.settimeout(1)\n"
            f"try: connection.connect(('127.0.0.1',{server.server_port}))\n"
            "except OSError: pass\n"
            "else: raise AssertionError('direct host network visible')\n")
        argv = sandbox.sandbox_vendor_argv([python, "-I", "-c", script],
            workspace, config_file=config, session_dir=session, gateway_socket=broker.socket_path,
            require_network_isolation=True)
        result = subprocess.run(argv, capture_output=True, timeout=30)
        assert result.returncode == 0
        assert (workspace / "inside.txt").read_text() == "ok"
    finally:
        if broker is not None:
            broker.close()
        server.shutdown()
        server.server_close()
        thread.join(5)
