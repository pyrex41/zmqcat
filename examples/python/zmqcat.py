"""Tiny zmqcat client. Talks to the local sidecar (unix or tcp).

    from zmqcat import Client
    c = Client()          # unix:///tmp/zmqcat-<uid>.sock
    c.put("jobs", "run")
    print(c.take("jobs"))
"""

from __future__ import annotations

import json
import os
import socket
import struct
from typing import Any


MAGIC = b"ZMQC"


def default_listen() -> str:
    return f"unix:///tmp/zmqcat-{os.getuid()}.sock"


def _split(listen: str) -> tuple[str, str]:
    if listen.startswith("unix://"):
        return "unix", listen[len("unix://") :]
    if listen.startswith("tcp://"):
        return "tcp", listen[len("tcp://") :]
    if listen.startswith("/") or listen.startswith("."):
        return "unix", listen
    return "tcp", listen


class Client:
    def __init__(self, listen: str | None = None, name: str = "python"):
        listen = listen or os.environ.get("ZMQCAT_LISTEN") or default_listen()
        family, address = _split(listen)
        if family == "unix":
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect(address)
        else:
            host, port = address.rsplit(":", 1)
            self.sock = socket.create_connection((host, int(port)))
        self.name = name
        self.hello(name)

    def close(self) -> None:
        self.sock.close()

    def hello(self, name: str) -> dict[str, Any]:
        return self._rpc({"op": "hello", "from": name})

    def put(self, mailbox: str, text: str = "", body: bytes | None = None) -> dict[str, Any]:
        return self._rpc(self._payload("put", mailbox, text, body))

    def take(self, mailbox: str) -> dict[str, Any]:
        return self._rpc({"op": "take", "name": mailbox, "from": self.name})

    def pub(self, topic: str, text: str = "", body: bytes | None = None) -> dict[str, Any]:
        return self._rpc(self._payload("pub", topic, text, body))

    def sub(self, prefix: str = "") -> dict[str, Any]:
        return self._rpc({"op": "sub", "name": prefix, "from": self.name})

    def recv(self) -> dict[str, Any]:
        return self._read()

    def ping(self) -> dict[str, Any]:
        return self._rpc({"op": "ping"})

    def _payload(self, op: str, name: str, text: str, body: bytes | None) -> dict[str, Any]:
        f: dict[str, Any] = {"op": op, "name": name, "from": self.name, "text": text}
        if body:
            import base64

            f["body"] = base64.b64encode(body).decode("ascii")
        return f

    def _rpc(self, f: dict[str, Any]) -> dict[str, Any]:
        self._write(f)
        out = self._read()
        if out.get("op") == "err":
            raise RuntimeError(out.get("error") or "zmqcat error")
        return out

    def _write(self, f: dict[str, Any]) -> None:
        raw = json.dumps(f).encode()
        self.sock.sendall(MAGIC + struct.pack(">I", len(raw)) + raw)

    def _read(self) -> dict[str, Any]:
        hdr = _readn(self.sock, 8)
        if hdr[:4] != MAGIC:
            raise RuntimeError(f"bad magic {hdr[:4]!r}")
        (n,) = struct.unpack(">I", hdr[4:])
        raw = _readn(self.sock, n)
        return json.loads(raw)


def _readn(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("zmqcat socket closed")
        buf.extend(chunk)
    return bytes(buf)


if __name__ == "__main__":
    import sys

    c = Client()
    if len(sys.argv) < 2:
        print(c.ping())
        sys.exit(0)
    op = sys.argv[1]
    if op == "put":
        print(c.put(sys.argv[2], " ".join(sys.argv[3:])))
    elif op == "take":
        print(c.take(sys.argv[2]))
    elif op == "pub":
        print(c.pub(sys.argv[2], " ".join(sys.argv[3:])))
    elif op == "sub":
        c.sub(sys.argv[2] if len(sys.argv) > 2 else "")
        while True:
            print(c.recv())
    else:
        raise SystemExit("put|take|pub|sub")
