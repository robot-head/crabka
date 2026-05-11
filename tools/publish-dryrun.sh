#!/usr/bin/env bash
set -euo pipefail

echo "==> cargo fmt --check"
cargo fmt --check

echo "==> cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets -- -D warnings

echo "==> cargo test --workspace"
cargo test --workspace

echo "==> cargo deny check"
cargo deny check

echo "==> cargo publish --dry-run for crabka-compression"
cargo publish -p crabka-compression --dry-run --allow-dirty

echo "==> cargo package --list for crabka-protocol (publish --dry-run requires crabka-compression on crates.io first)"
cargo package --list -p crabka-protocol --allow-dirty

echo "==> rustdoc with --cfg docsrs"
RUSTDOCFLAGS="--cfg docsrs -D warnings" \
    cargo doc --workspace --no-deps --all-features

echo "==> All publish-readiness checks passed."
