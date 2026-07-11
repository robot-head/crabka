#!/usr/bin/env python3
"""Payload-safe PostgreSQL startup/SET recorder used by F-1 recapture.

The recorder is deliberately lossy: it forwards the complete TCP stream but only
emits allowlisted startup settings and simple-query messages made solely of SET
statements. Identity fields and arbitrary SQL are never retained.
"""

from __future__ import annotations

import argparse
import json
import selectors
import socket
import struct
from pathlib import Path

ALLOWED_STARTUP = {
    "DateStyle",
    "TimeZone",
    "application_name",
    "client_encoding",
    "extra_float_digits",
}
IDENTITY_STARTUP = {"user", "database", "options", "replication"}


def startup_parameters(packet: bytes) -> dict[str, str]:
    if len(packet) < 8 or struct.unpack("!I", packet[4:8])[0] != 196608:
        raise ValueError("expected PostgreSQL protocol 3.0 startup packet")
    fields = packet[8:].rstrip(b"\0").split(b"\0")
    if len(fields) % 2:
        raise ValueError("malformed startup key/value sequence")
    result: dict[str, str] = {}
    for raw_key, raw_value in zip(fields[::2], fields[1::2], strict=True):
        key = raw_key.decode("ascii")
        if key in IDENTITY_STARTUP:
            continue
        if key not in ALLOWED_STARTUP:
            raise ValueError(f"unexpected startup key: {key}")
        value = raw_value.decode("utf-8")
        if any(token in value.lower() for token in ("password", "postgres://", "postgresql://")):
            raise ValueError(f"unsafe startup value for {key}")
        result[key] = value
    return result


def safe_set_batch(payload: bytes) -> str | None:
    sql = payload.rstrip(b"\0").decode("utf-8")
    statements = [statement.strip() for statement in sql.split(";") if statement.strip()]
    if not statements or not all(statement.upper().startswith("SET ") for statement in statements):
        return None
    lowered = sql.lower()
    if any(token in lowered for token in ("password", "postgres://", "postgresql://")):
        raise ValueError("unsafe SQL in SET batch")
    return sql


class Decoder:
    def __init__(self) -> None:
        self.buffer = bytearray()
        self.startup: dict[str, str] | None = None
        self.set_batches: list[str] = []

    def feed(self, data: bytes) -> None:
        self.buffer.extend(data)
        if self.startup is None:
            while True:
                if len(self.buffer) < 4:
                    return
                length = struct.unpack("!I", self.buffer[:4])[0]
                if len(self.buffer) < length:
                    return
                packet = bytes(self.buffer[:length])
                del self.buffer[:length]
                if packet == struct.pack("!II", 8, 80877103):
                    continue
                break
            self.startup = startup_parameters(packet)
        while len(self.buffer) >= 5:
            length = struct.unpack("!I", self.buffer[1:5])[0]
            total = 1 + length
            if len(self.buffer) < total:
                return
            message = bytes(self.buffer[:total])
            del self.buffer[:total]
            if message[0:1] == b"Q":
                batch = safe_set_batch(message[5:])
                if batch is not None:
                    self.set_batches.append(batch)


def record(listen: tuple[str, int], upstream: tuple[str, int]) -> dict[str, object]:
    with socket.create_server(listen) as listener:
        client, _ = listener.accept()
        with client, socket.create_connection(upstream, timeout=10) as server:
            # Keep the captured frontend leg plaintext. PgDog probes TLS even
            # when its backend proceeds without it; replying `N` is the normal
            # PostgreSQL negotiation path and avoids retaining encrypted bytes.
            initial = recv_initial_packet(client)
            if initial == struct.pack("!II", 8, 80877103):
                client.sendall(b"N")
                initial = recv_initial_packet(client)
            decoder = Decoder()
            decoder.feed(initial)
            server.sendall(initial)
            client.setblocking(False)
            server.setblocking(False)
            selector = selectors.DefaultSelector()
            selector.register(client, selectors.EVENT_READ, server)
            selector.register(server, selectors.EVENT_READ, client)
            while selector.get_map():
                for key, _ in selector.select(timeout=15):
                    source = key.fileobj
                    target = key.data
                    data = source.recv(65536)
                    if not data:
                        selector.unregister(source)
                        try:
                            target.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass
                        continue
                    if source is client:
                        decoder.feed(data)
                    target.sendall(data)
            if decoder.startup is None:
                raise RuntimeError("connection ended before a startup packet was captured")
            return {"startup_parameters": decoder.startup, "set_batches": decoder.set_batches}


def recv_initial_packet(sock: socket.socket) -> bytes:
    header = recv_exact(sock, 4)
    length = struct.unpack("!I", header)[0]
    if length < 8 or length > 100_000:
        raise ValueError("invalid initial PostgreSQL packet length")
    return header + recv_exact(sock, length - 4)


def recv_exact(sock: socket.socket, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = sock.recv(length - len(chunks))
        if not chunk:
            raise EOFError("connection ended during initial PostgreSQL packet")
        chunks.extend(chunk)
    return bytes(chunks)


def endpoint(value: str) -> tuple[str, int]:
    host, port = value.rsplit(":", 1)
    return host, int(port)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    captured = record(endpoint(args.listen), endpoint(args.upstream))
    args.out.write_text(json.dumps(captured, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
