#!/usr/bin/env bash
set -euo pipefail

cargo run -p crabka-protocol-codegen -- \
    crates/protocol/schemas \
    crates/protocol/generated

cargo run -p crabka-protocol-codegen -- \
    --namespace kafka_3_6_2 \
    crates/protocol/schemas/versions/kafka_3_6_2 \
    crates/protocol/generated/kafka_3_6_2

# The codegen binary rustfmts each generated message file (they are include!'d,
# so cargo fmt never reaches them). cargo fmt then normalizes the real module
# files the binary writes into crates/protocol/src (mod.rs declarations, etc.).
cargo fmt -p crabka-protocol

echo "Regenerated. Review the diff with: git diff crates/protocol/generated crates/protocol/src"
