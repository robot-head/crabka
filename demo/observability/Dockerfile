# syntax=docker/dockerfile:1
# Single image with every Crabka binary the demo needs, with debug symbols
# (for readable flamegraphs). Built on Linux, where the cfg(unix) pprof
# CPU-profiling code is active.
#
# The service binaries are built `--features heap-profiling`, which swaps in the
# jemalloc global allocator and exposes `/debug/pprof/heap` alongside the CPU
# `/debug/pprof/profile`. Continuous jemalloc prof sampling used to deadlock
# `crabka-client-consumer`'s group rejoin (a lost wakeup in the connection I/O
# task); that is fixed in `crabka-client-core` (independent reader/writer tasks),
# so WAL consumers — block-builders, compactors, queriers — run fine under prof.
# `crabka-cli` and the demo app do not carry the feature.
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
