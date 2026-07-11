#!/usr/bin/env python3
"""Payload-safe PostgreSQL startup/SET recorder used by F-1 recapture.

The recorder is deliberately lossy: it forwards the complete TCP stream but only
emits allowlisted startup settings and simple-query messages made solely of SET
statements. Identity fields and arbitrary SQL are never retained.
"""

from __future__ import annotations

import argparse
import json
import re
import selectors
import socket
import struct
import time
from pathlib import Path

ALLOWED_STARTUP_VALUES = {
    "DateStyle": {"ISO, MDY"},
    "TimeZone": {"UTC"},
    "application_name": {"PgDog"},
    "client_encoding": {"UTF8", "utf-8"},
    "extra_float_digits": {"2"},
}
IDENTITY_STARTUP = {"user", "database"}
ALLOWED_SET_VALUES = {
    "datestyle": "ISO, MDY",
    "extra_float_digits": "2",
    "timezone": "UTC",
}
SET_PATTERN = re.compile(
    r"\s*SET\s+(?:\"(?P<quoted>[a-z_]+)\"|(?P<plain>[a-z_]+))\s+TO\s+'(?P<value>[^']*)'\s*",
    re.IGNORECASE,
)


def startup_parameters(packet: bytes) -> dict[str, str]:
    if (
        len(packet) < 9
        or struct.unpack("!I", packet[:4])[0] != len(packet)
        or struct.unpack("!I", packet[4:8])[0] != 196608
    ):
        raise ValueError("expected PostgreSQL protocol 3.0 startup packet")
    body = packet[8:]
    if not body.endswith(b"\0\0") or b"\0\0" in body[:-2]:
        raise ValueError("malformed startup key/value sequence")
    fields = body[:-2].split(b"\0")
    if not fields or len(fields) % 2 or any(not field for field in fields):
        raise ValueError("malformed startup key/value sequence")
    result: dict[str, str] = {}
    seen: set[str] = set()
    for raw_key, raw_value in zip(fields[::2], fields[1::2], strict=True):
        try:
            key = raw_key.decode("ascii")
            value = raw_value.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("startup key/value is not valid text") from error
        if key in seen:
            raise ValueError("duplicate startup key")
        seen.add(key)
        if key in IDENTITY_STARTUP:
            continue
        allowed = ALLOWED_STARTUP_VALUES.get(key)
        if allowed is None:
            raise ValueError("unexpected startup key")
        if value not in allowed:
            raise ValueError("unexpected startup value")
        result[key] = value
    if "user" not in seen or "database" not in seen:
        raise ValueError("startup identity fields are incomplete")
    return result


def safe_set_batch(payload: bytes) -> str | None:
    if not payload.endswith(b"\0") or b"\0" in payload[:-1]:
        raise ValueError("malformed simple-query payload")
    try:
        sql = payload[:-1].decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("simple-query payload is not valid UTF-8") from error
    if re.match(r"\s*SET\b", sql, re.IGNORECASE) is None:
        return None
    matched = SET_PATTERN.fullmatch(sql)
    if matched is None:
        raise ValueError("SET query is outside the capture allowlist")
    guc = (matched.group("quoted") or matched.group("plain")).lower()
    value = matched.group("value")
    if ALLOWED_SET_VALUES.get(guc) != value:
        raise ValueError("SET assignment is outside the capture allowlist")
    return f'SET "{guc}" TO \'{value}\''


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


def record(
    listen: tuple[str, int],
    upstream: tuple[str, int],
    *,
    deadline_seconds: float = 30.0,
) -> dict[str, object]:
    if deadline_seconds <= 0:
        raise ValueError("deadline_seconds must be positive")
    deadline = time.monotonic() + deadline_seconds
    with socket.create_server(listen) as listener:
        listener.settimeout(remaining(deadline))
        client, _ = listener.accept()
        client.settimeout(remaining(deadline))
        with client, socket.create_connection(upstream, timeout=remaining(deadline)) as server:
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
                events = selector.select(timeout=remaining(deadline))
                if not events:
                    raise TimeoutError("wire capture reached its absolute deadline")
                for key, _ in events:
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


def remaining(deadline: float) -> float:
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError("wire capture reached its absolute deadline")
    return value


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
    parser.add_argument("--deadline-seconds", type=float, default=30.0)
    args = parser.parse_args()
    captured = record(
        endpoint(args.listen),
        endpoint(args.upstream),
        deadline_seconds=args.deadline_seconds,
    )
    args.out.write_text(json.dumps(captured, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
