#!/usr/bin/env bash
# Build and run every documented client-streams example, then verify the website
# snippets are in sync with their source (doc-drift guard).
set -euo pipefail

echo "==> building all client-streams examples"
cargo build -p crabka-client-streams --examples --features polars,arrow

echo "==> running self-asserting examples"
cargo run -p crabka-client-streams --example format_json
cargo run -p crabka-client-streams --example format_arrow --features arrow
cargo run -p crabka-client-streams --example format_dsl
cargo run -p crabka-client-streams --example format_pipeline --features polars,arrow

echo "==> checking documentation snippets are in sync"
cargo run -p crabka-docgen -- snippets
if ! git diff --quiet -- website/content; then
  echo "ERROR: website snippets are stale. Run: cargo run -p crabka-docgen -- snippets" >&2
  git --no-pager diff -- website/content >&2
  exit 1
fi

echo "==> doc examples OK"
