#!/usr/bin/env bash
set -euo pipefail

cargo run -p crabka-protocol-codegen -- \
    crates/protocol/schemas \
    crates/protocol/generated

cargo run -p crabka-protocol-codegen -- \
    --namespace kafka_3_6_2 \
    crates/protocol/schemas/versions/kafka_3_6_2 \
    crates/protocol/generated/kafka_3_6_2

# Auto-fix the machine-fixable clippy/rustc lints on the freshly emitted code
# (semicolons, range-contains, redundant closures, deref, unused, …) so the
# generated tree is idiomatic rather than blanket-#![allow]'d. clippy --fix edits
# the include!'d generated bodies in place; only the genuinely unfixable lints
# (intentional casts, must_use, always-true version comparisons) stay allowed in
# the wrapper header. Run twice — a fix can expose a follow-on lint.
cargo clippy --fix --allow-dirty --allow-staged -p crabka-protocol --all-targets >/dev/null
cargo clippy --fix --allow-dirty --allow-staged -p crabka-protocol --all-targets >/dev/null

# The codegen binary rustfmts each generated message file (they are include!'d,
# so cargo fmt never reaches them); clippy --fix may leave its edits unformatted.
# Re-run rustfmt on the generated bodies, then cargo fmt for the real module files.
find crates/protocol/generated -name '*.rs' -print0 | xargs -0 rustfmt --edition 2024
cargo +nightly fmt -p crabka-protocol

echo "Regenerated. Review the diff with: git diff crates/protocol/generated crates/protocol/src"
