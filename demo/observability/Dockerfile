# syntax=docker/dockerfile:1
# Single image with every Crabka binary the demo needs, built with heap
# profiling and debug symbols (for readable flamegraphs). Built on Linux, where
# the cfg(unix) jemalloc/pprof profiling code is active.
FROM rust:1.96-bookworm AS build
WORKDIR /src
# jemalloc (tikv-jemalloc-sys) builds from source: needs a C toolchain + make.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake && rm -rf /var/lib/apt/lists/*
COPY . .
# Keep debug symbols in release for symbolized CPU/heap profiles.
ENV CARGO_PROFILE_RELEASE_DEBUG=true
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --features heap-profiling \
      -p crabka-broker \
      -p crabka-metrics -p crabka-metrics-service \
      -p crabka-traces \
      -p crabka-observability \
      -p crabka-profiles \
      -p crabka-schema-registry \
      -p observability-demo-app && \
    mkdir -p /out && \
    for b in crabka-broker crabka-metrics crabka-metrics-service crabka-traces \
             crabka-observability crabka-profiles crabka-schema-registry observability-demo-app; do \
      cp "target/release/$b" /out/; \
    done

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/* /usr/local/bin/
WORKDIR /data
