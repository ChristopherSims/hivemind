"""Bounded, origin-allowlisted Unix-socket proxy for isolated Linux vendors.

TLS remains end-to-end: HTTPS grants authorize an origin, not individual API
paths. No request bodies, credentials, or response bodies are logged.
"""
import ipaddress
import os
import select
import socket
import socketserver
import struct
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from urllib.parse import urlsplit

from .os_isolation import IsolationRefused

HEADER_LIMIT = 32768
BODY_LIMIT = 8 * 1024 * 1024
STREAM_LIMIT = 64 * 1024 * 1024
CONNECTION_LIMIT = 16
CONNECTION_SECONDS = 300


def authority(value):
    """Parse a single explicit host:port, never userinfo or a URL/path."""
    if not isinstance(value, str) or not value or any(
            char.isspace() or ord(char) < 32 for char in value):
        raise ValueError("Invalid gateway authority")
    parsed = urlsplit("//" + value)
    if (parsed.username is not None or parsed.password is not None or parsed.path
            or parsed.query or parsed.fragment or not parsed.hostname or parsed.port is None):
        raise ValueError("Gateway authority requires only a host and explicit port")
    host = parsed.hostname.lower()
    if "%" in host or "\\" in host or not 1 <= parsed.port <= 65535:
        raise ValueError("Invalid gateway host or port")
    return host, parsed.port


@dataclass(frozen=True)
class Destination:
    host: str
    port: int
    scheme: str
    kind: str
    addresses: tuple


class GatewayPolicy:
    """Resolve once and pin IPs; model origins must not resolve to private IPs."""

    def __init__(self, *, model_origins, hivemind_origin, resolver=socket.getaddrinfo):
        if not isinstance(model_origins, (list, tuple)) or not 1 <= len(model_origins) <= 16:
            raise ValueError("Declare between one and sixteen model origins")
        destinations = {}
        for kind, value in [*(("model", value) for value in model_origins),
                            ("hivemind", hivemind_origin)]:
            if not isinstance(value, str) or any(char.isspace() for char in value):
                raise ValueError("Invalid gateway origin")
            parsed = urlsplit(value)
            if (parsed.scheme not in ("http", "https") or not parsed.hostname
                    or parsed.username is not None or parsed.password is not None
                    or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
                raise ValueError("Gateway grants must be HTTP(S) origins without credentials or paths")
            if kind == "model" and parsed.scheme != "https":
                raise ValueError("Model origins require HTTPS")
            host = parsed.hostname.lower()
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            authority(f"[{host}]:{port}" if ":" in host else f"{host}:{port}")
            addresses = []
            for family, _, _, _, address in resolver(host, port, type=socket.SOCK_STREAM):
                if family not in (socket.AF_INET, socket.AF_INET6):
                    raise ValueError("Unsupported gateway address family")
                ip = ipaddress.ip_address(address[0])
                safety_ip = getattr(ip, "ipv4_mapped", None) or ip
                if safety_ip.is_unspecified or safety_ip.is_multicast or safety_ip.is_link_local or (
                        kind == "model" and not safety_ip.is_global):
                    raise ValueError("Gateway destination has a forbidden address")
                pinned = (family, str(ip), port)
                if pinned not in addresses:
                    addresses.append(pinned)
            if not addresses or len(addresses) > 16:
                raise ValueError("Gateway resolution must produce a bounded nonempty address set")
            if (host, port) in destinations:
                raise ValueError("Duplicate gateway origin")
            destinations[(host, port)] = Destination(host, port, parsed.scheme, kind, tuple(addresses))
        self.destinations = MappingProxyType(destinations)

    def target(self, method, target):
        if method == "CONNECT":
            key = authority(target)
            path = None
        else:
            parsed = urlsplit(target)
            if (parsed.scheme != "http" or not parsed.hostname or parsed.username is not None
                    or parsed.password is not None or parsed.fragment or "\\" in target
                    or any(char.isspace() or ord(char) < 32 for char in target)):
                raise ValueError("Only absolute HTTP requests or CONNECT are supported")
            key = (parsed.hostname.lower(), parsed.port or 80)
            path = (parsed.path or "/") + ("?" + parsed.query if parsed.query else "")
        destination = self.destinations.get(key)
        if destination is None:
            raise PermissionError("Gateway origin is not granted")
        if method != "CONNECT" and (destination.kind != "hivemind" or destination.scheme != "http"):
            raise PermissionError("Plain HTTP is restricted to the declared Hivemind origin")
        return destination, path


def connect_pinned(destination):
    deadline = time.monotonic() + 10
    for family, ip, port in destination.addresses:
        if time.monotonic() >= deadline:
            break
        connection = socket.socket(family, socket.SOCK_STREAM)
        connection.settimeout(max(0.01, deadline - time.monotonic()))
        try:
            connection.connect((ip, port))
            return connection
        except OSError:
            connection.close()
    raise OSError("Granted gateway destination is unavailable")


def relay(left, right, *, duplex=True):
    """Bound every tunnel by absolute lifetime and aggregate byte count."""
    deadline = time.monotonic() + CONNECTION_SECONDS
    transferred = 0
    while time.monotonic() < deadline and transferred < STREAM_LIMIT:
        readable, _, _ = select.select([left, right] if duplex else [right], [], [],
                                      max(0, min(1, deadline - time.monotonic())))
        for source in readable:
            data = source.recv(min(65536, STREAM_LIMIT - transferred))
            if not data:
                return
            transferred += len(data)
            (right if source is left else left).sendall(data)


class _ProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        if not self.server.slots.acquire(blocking=False):
            return
        upstream = None
        try:
            with self.server.connections_lock:
                if self.server.closing.is_set():
                    return
                self.server.connections.add(self.request)
            self.request.settimeout(10)
            credentials = self.request.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
            if struct.unpack("3i", credentials)[1] != self.server.allowed_uid:
                raise PermissionError("Gateway peer identity is not granted")
            header = bytearray()
            deadline = time.monotonic() + 10
            # Never read ahead into a CONNECT tunnel or a pipelined request.
            while not header.endswith(b"\r\n\r\n"):
                if len(header) >= HEADER_LIMIT:
                    raise ValueError("Gateway header limit")
                if time.monotonic() >= deadline:
                    raise ValueError("Gateway header deadline")
                self.request.settimeout(max(0.01, deadline - time.monotonic()))
                byte = self.request.recv(1)
                if not byte:
                    return
                header.extend(byte)
            lines = header.decode("ascii").split("\r\n")
            method, target, version = lines[0].split(" ")
            if method not in ("CONNECT", "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"):
                raise ValueError("Gateway method is unsupported")
            if version not in ("HTTP/1.0", "HTTP/1.1"):
                raise ValueError("Gateway HTTP version is unsupported")
            destination, path = self.server.policy.target(method, target)
            headers = {}
            for line in lines[1:-2]:
                name, value = line.split(":", 1)
                name = name.lower()
                if (not name or any(not (char.isalnum() or char == "-") for char in name)
                        or name in headers or any(ord(char) < 32 for char in value)):
                    raise ValueError("Invalid or duplicate gateway header")
                headers[name] = value.strip()
            if any(name in headers for name in ("transfer-encoding", "upgrade", "expect")):
                raise ValueError("Unsupported gateway request framing")
            length = headers.get("content-length", "0")
            if not length.isdigit() or not 0 <= int(length) <= BODY_LIMIT:
                raise ValueError("Gateway body limit")
            if method == "CONNECT" and int(length):
                raise ValueError("CONNECT cannot carry an HTTP body")
            upstream = connect_pinned(destination)
            with self.server.connections_lock:
                if self.server.closing.is_set():
                    return
                self.server.connections.add(upstream)
            if method == "CONNECT":
                self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            else:
                host = f"[{destination.host}]" if ":" in destination.host else destination.host
                outbound = [f"{method} {path} HTTP/1.1", f"Host: {host}:{destination.port}",
                            "Connection: close"]
                outbound.extend(f"{name}: {value}" for name, value in headers.items()
                                if name not in ("host", "connection", "proxy-connection",
                                                "proxy-authorization"))
                upstream.sendall(("\r\n".join(outbound) + "\r\n\r\n").encode("ascii"))
                remaining = int(length)
                deadline = time.monotonic() + 30
                while remaining:
                    if time.monotonic() >= deadline:
                        raise ValueError("Gateway body deadline")
                    self.request.settimeout(max(0.01, deadline - time.monotonic()))
                    data = self.request.recv(min(65536, remaining))
                    if not data:
                        return
                    upstream.sendall(data)
                    remaining -= len(data)
            relay(self.request, upstream, duplex=method == "CONNECT")
        except (OSError, ValueError, PermissionError):
            try:
                self.request.sendall(b"HTTP/1.1 403 Gateway refused\r\nConnection: close\r\n"
                                     b"Content-Length: 0\r\n\r\n")
            except OSError:
                pass
        finally:
            with self.server.connections_lock:
                self.server.connections.discard(self.request)
                self.server.connections.discard(upstream)
            if upstream is not None:
                upstream.close()
            self.server.slots.release()


class _UnixServer(socketserver.ThreadingMixIn,
                  getattr(socketserver, "UnixStreamServer", socketserver.TCPServer)):
    daemon_threads = True
    block_on_close = False


class ManagedGateway:
    """Private per-runtime gateway; Linux only, no host TCP listener."""

    def __init__(self, policy, parent):
        if sys.platform != "linux" or not hasattr(socket, "SO_PEERCRED"):
            raise IsolationRefused("Verified vendor gateways require Linux peer credentials")
        parent = Path(parent)
        if any(path.is_symlink() for path in (parent, *parent.parents)):
            raise IsolationRefused("Gateway state ancestors must not be symlinks")
        self.directory = tempfile.TemporaryDirectory(prefix="gateway-", dir=parent)
        self.socket_path = Path(self.directory.name) / "proxy.sock"
        try:
            self.server = _UnixServer(str(self.socket_path), _ProxyHandler)
            os.chmod(self.socket_path, 0o600)
            self.server.policy = policy
            self.server.allowed_uid = os.getuid()
            self.server.slots = threading.BoundedSemaphore(CONNECTION_LIMIT)
            self.server.connections = set()
            self.server.connections_lock = threading.Lock()
            self.server.closing = threading.Event()
            self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            self.thread.start()
        except BaseException:
            if hasattr(self, "server"):
                self.server.server_close()
            self.directory.cleanup()
            raise

    def close(self):
        self.server.closing.set()
        self.server.shutdown()
        with self.server.connections_lock:
            for connection in self.server.connections:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                connection.close()
        self.server.server_close()
        self.thread.join(5)
        self.directory.cleanup()
