#!/usr/bin/env bash
set -euo pipefail

# Strict schema subscription filter audit gate.
#
# The plan-complete path is not the default JSON compatibility fallback: it is
# SQL parsed by DataFusion, compiled to a physical expression, evaluated over
# Arrow RecordBatches, and then applied in Subscribe/RowBridge before original
# bytes are delivered. These exact tests must run with `--features arrow` so a
# missing DataFusion/Arrow wiring fails CI instead of being hidden by default
# feature builds.

cargo test -p crabka-grpc-gateway --features arrow filter
cargo test -p crabka-grpc-gateway --test streaming --features arrow
