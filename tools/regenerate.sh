#!/usr/bin/env bash
set -euo pipefail

bazel-bin/crates/protocol-codegen/crabka-protocol-codegen__bin \
    crates/protocol/schemas \
    crates/protocol/generated

bazel-bin/crates/protocol-codegen/crabka-protocol-codegen__bin \
    --namespace kafka_3_6_2 \
    crates/protocol/schemas/versions/kafka_3_6_2 \
    crates/protocol/generated/kafka_3_6_2

# The codegen binary rustfmts each generated message file (they are include!'d,
# so the ordinary formatter target never reaches them). Re-run the hermetic
# rules_rs rustfmt binary over generated and module sources.
find crates/protocol/generated crates/protocol/src -name '*.rs' -print0 \
    | xargs -0 bazel run @default_rust_toolchains//:linux_x86_64_rustfmt_1_97_1 -- --edition 2024

echo "Regenerated. Review the diff with: git diff crates/protocol/generated crates/protocol/src"
