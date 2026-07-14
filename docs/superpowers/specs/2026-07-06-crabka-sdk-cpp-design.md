# The C++ SDK — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Language cycle under the [application-SDK umbrella](2026-07-06-crabka-app-sdk-umbrella-design.md) — deliberately the **last** cycle, eyes open: the matrix's expensive cell. Implements contract v1 in C++; definition of done = the conformance suite green through the C++ adapter.

## Context — why this is the expensive cell

**No Connect client library exists for C++ anywhere.** The transport is net-new: unary is easy (an HTTP POST), but `Subscribe` is a Connect **bidi** stream requiring full-duplex HTTP/2 — beyond what libcurl's request/response model expresses cleanly. This cycle therefore builds a minimal transport on **nghttp2** (the reference HTTP/2 C library): direct session/stream control gives clean full-duplex, at the price of owning the event loop. Everything above the transport is the same formula as the other cycles.

The Rust cycle's discipline transfers: the framing is **oracle-tested against our own connectrpc-axum server's bytes**, and the Rust `connect_client`'s tests double as the reference vectors for the C++ codec.

## Design Goals

- **A minimal Connect transport on nghttp2:** one session per client, h2c prior-knowledge; unary = POST with `application/proto`; streaming = the Connect envelope codec (flags byte + u32 BE length; `EndStream` trailer JSON → error mapping). Scoped to exactly `Send`/`Subscribe`.
- **Modern-C++ surface, identical semantics:** C++20; `crabka::Client` with module accessors; errors as a `crabka::Error` value type (`kind` enum + `module`/`gated_on` fields — **exceptions optional**: a `Result<T>`-style `expected` return surface, since serverless C++ users frequently build `-fno-exceptions`); subscribe as a blocking `MessageStream` with `next(timeout)` / `close()`.
- **Protobuf via the standard C++ codegen** (`protoc-gen-cpp` through the repo buf config → `sdks/cpp/gen`), drift-checked like every other language.
- **CMake + vcpkg-free system deps:** nghttp2 + protobuf from apt in CI (a new `sdk-cpp` job — the named toolchain addition), Catch2 vendored via FetchContent for units.

## Non-goals

Windows/macOS support matrices (Linux-first, matching the substrate's deployment reality); a general-purpose Connect-C++ library; TLS (in-cluster plaintext v1, as everywhere); manual ack; package-manager distribution (deferred with SDK release engineering).

## Architecture

```
sdks/cpp/                       (CMake project; C++20; namespace crabka)
├── CMakeLists.txt              (nghttp2 + protobuf::libprotobuf; Catch2 FetchContent)
├── gen/                        protoc-gen-cpp output (buf.gen.yaml plugin block; drift-checked)
├── include/crabka/             the public headers (client.hpp, error.hpp, messaging.hpp, …)
├── src/
│   ├── envelope.cc             the Connect frame codec (pure — unit + oracle tested)
│   ├── transport.cc            nghttp2 session loop (own thread), unary + bidi plumbing
│   ├── client.cc / errors.cc   builder-style config; the taxonomy incl.
│   │                           Error{kind=Unimplemented, module, gated_on}
│   ├── messaging.cc            publish / publish_event (CE mapping) / subscribe → MessageStream
│   └── stubs.cc                queues / database / blob (pinned slugs); auth = config only
├── adapter/main.cc             the JSON-lines stdio conformance adapter (nlohmann/json, FetchContent)
└── test/                       Catch2: envelope codec (incl. the Rust-captured byte vectors),
                                CE mapping, taxonomy
```

Threading model (the one C++-specific design point): the nghttp2 session runs on a dedicated I/O thread owned by the `Client`; public calls marshal onto it via a lock-free-enough queue; `MessageStream::next` blocks on a bounded queue the stream callback fills — the same shape as the Java facade, chosen for serverless-consumer ergonomics over an async API nobody standardizes in C++.

## Integration

- **`buf.gen.yaml`** — the `protoc-gen-cpp` plugin block, `out: sdks/cpp/gen`; drift check extends.
- **CI (the named addition):** an `sdk-cpp` job — `apt-get install libnghttp2-dev protobuf-compiler libprotobuf-dev`, `cmake -B build && cmake --build build && ctest`, then the conformance suite with `--adapter sdks/cpp/build/adapter`.
- **The framing reference vectors** — the byte captures produced by the Rust cycle's oracle test are committed once (`sdks/cpp/test/vectors/`) so the C++ codec tests run hermetically; the live oracle remains the suite itself.
- **The harness/vectors unchanged.**

## Testing

Catch2 units: the envelope codec against the committed Rust-captured bytes (message frames, EndStream trailers, error JSON); CE mapping (the same assertions as every language); taxonomy mapping; stub slugs. **The conformance suite is the gate** — all v1 vectors green through the C++ adapter, which also exercises the nghttp2 duplex path end-to-end against the real gateway.

## Risks

- **The nghttp2 event loop is the risk concentration** — session lifecycle, flow control (WINDOW_UPDATE on the streaming read path), and clean shutdown are exactly the bugs C++ transports grow. Mitigations: the smallest possible surface (one session, two RPC shapes), the committed byte vectors, ASan/TSan in the CI job's test run, and the suite's live end-to-end.
- **Memory safety is ours again** (the only cycle where that's true) — sanitizers in CI are non-negotiable; the public API is value-semantic (no raw pointers cross it).
- **Toolchain drift** (system nghttp2/protobuf versions) — pinned via the CI image's apt snapshot; a container image can pin harder if it bites.
- **Effort honesty:** this remains the most expensive cycle even so scoped — it is last for a reason, and nothing else gates on it.

## Resolved decisions

nghttp2 transport (full-duplex control; libcurl rejected for bidi); C++20, Linux-first, `expected`-style errors with optional exceptions; blocking `MessageStream`; the dedicated I/O thread model; Rust-captured framing vectors as the hermetic codec oracle; sanitizers required in CI; last in the phase order.
