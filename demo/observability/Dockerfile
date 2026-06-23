# syntax=docker/dockerfile:1
# Single image with every Crabka binary the demo needs, with debug symbols
# (for readable CPU flamegraphs). Built on Linux, where the cfg(unix) pprof
# CPU-profiling code is active.
#
# NOTE: `heap-profiling` is deliberately NOT enabled. It swaps in the jemalloc
# global allocator, which (with prof active) breaks `crabka-client-consumer`'s
# group-coordination handshake — every WAL consumer (block-builders, compactors,
# queriers) then fails to join its consumer group, so no blocks get built and
# the queriers stay empty. CPU profiling (the `pprof` crate) is independent of
# the allocator and still works; only heap profiles are forgone. See the
# tracked bug "crabka-client-consumer + jemalloc group-join hang".
FROM rust:1.96-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake && rm -rf /var/lib/apt/lists/*
COPY . .
# Keep debug symbols in release for symbolized CPU profiles.
ENV CARGO_PROFILE_RELEASE_DEBUG=true
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release \
      -p crabka-broker -p crabka-cli \
      -p crabka-metrics -p crabka-metrics-service \
      -p crabka-traces \
      -p crabka-observability \
      -p crabka-profiles \
      -p crabka-schema-registry \
      -p observability-demo-app && \
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
