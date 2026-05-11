#!/usr/bin/env bash
set -euo pipefail
cargo run -p crabka-protocol-codegen -- \
    crates/protocol/schemas \
    crates/protocol/generated
echo "Regenerated. Review the diff with: git diff crates/protocol/generated"
