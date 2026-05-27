#!/usr/bin/env bash
set -euo pipefail

cargo run -p crabka-protocol-codegen -- \
    crates/protocol/schemas \
    crates/protocol/generated

cargo run -p crabka-protocol-codegen -- \
    --namespace kafka_3_6_2 \
    crates/protocol/schemas/versions/kafka_3_6_2 \
    crates/protocol/generated/kafka_3_6_2

echo "Regenerated. Review the diff with: git diff crates/protocol/generated crates/protocol/src"
