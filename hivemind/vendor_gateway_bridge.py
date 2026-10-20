"""Trusted namespace entrypoint: local proxy -> one mounted host Unix socket."""
import os
import select
import socket
import subprocess
import sys
import threading


def bridge(client, socket_path):
    with client, socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as gateway:
        try:
            gateway.settimeout(10)
            gateway.connect(socket_path)
            while True:
                readable, _, _ = select.select([client, gateway], [], [], 300)
                if not readable:
                    return
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    (gateway if source is client else client).sendall(data)
        except OSError:
            return


def main():
    socket_path, port, *argv = sys.argv[1:]
    if not argv or argv[0] != "--":
        raise SystemExit("Gateway entrypoint requires an explicit vendor argv")
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", int(port)))
    listener.listen(16)
    slots = threading.BoundedSemaphore(16)

    def serve():
        while True:
            client, _ = listener.accept()
            if not slots.acquire(blocking=False):
                client.close()
                continue

            def handle(connection=client):
                try:
                    bridge(connection, socket_path)
                finally:
                    slots.release()
            threading.Thread(target=handle, daemon=True).start()

    threading.Thread(target=serve, daemon=True).start()
    proxy = f"http://127.0.0.1:{port}"
    environment = dict(os.environ)
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        environment[name] = proxy
    environment["NO_PROXY"] = environment["no_proxy"] = ""
    process = subprocess.Popen(argv[1:], env=environment)
    try:
        raise SystemExit(process.wait())
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
        listener.close()


if __name__ == "__main__":
    main()
