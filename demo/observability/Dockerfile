# syntax=docker/dockerfile:1
# Single image with every Crabka binary the demo needs, with debug symbols
# (for readable flamegraphs). Built on Linux, where the cfg(unix) pprof
# CPU-profiling code is active.
#
# The service binaries are built `--features heap-profiling`, which swaps in the
# jemalloc global allocator and exposes `/debug/pprof/heap` alongside the CPU
# `/debug/pprof/profile`. A prof-sampling latency race once stalled
# `crabka-client-consumer`'s group rejoin; that is fixed in `crabka-client-core`
# (independent reader/writer I/O tasks). NOTE: a separate cold-start hang — a WAL
# consumer left with an EMPTY assignment when its topic is created after it joins
# — is NOT a prof problem (reproduced with prof off; lock protocol proven
# deadlock-free by a stateright model); it is fixed in the consumer coordinator's
# metadata-change-triggered rejoin. `crabka-cli` and the demo app do not carry
# the heap-profiling feature.
FROM rust:1.96-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake && rm -rf /var/lib/apt/lists/*
COPY . .
# Keep debug symbols in release for symbolized flamegraphs.
ENV CARGO_PROFILE_RELEASE_DEBUG=true
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    for p in crabka-broker crabka-metrics crabka-metrics-service crabka-traces \
             crabka-observability crabka-profiles crabka-schema-registry; do \
      cargo build --release -p "$p" --features heap-profiling || exit 1; \
    done && \
    cargo build --release -p crabka-cli -p observability-demo-app && \
    mkdir -p /out && \
    for b in crabka-broker crabka crabka-metrics crabka-metrics-service crabka-traces \
             crabka-observability crabka-profiles crabka-schema-registry observability-demo-app; do \
      cp "target/release/$b" /out/; \
    done

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/* /usr/local/bin/
WORKDIR /data
