#!/usr/bin/env bash
# Build the WASM consensus playground and stage it into the docs site.
#
# Compiles `crabka-playground` to `wasm32-unknown-unknown`, runs `wasm-bindgen
# --target web` to emit an ES module + `.wasm`, and drops them next to the
# hand-written front-end in `website/static/playground/`. The generated
# `crabka_playground.js` / `crabka_playground_bg.wasm` are git-ignored and
# regenerated here (locally and in CI) — only `app.js` / `playground.css` are
# committed.
#
# Requirements (the docs CI installs both):
#   rustup target add wasm32-unknown-unknown
#   wasm-bindgen-cli @ the version pinned in Cargo.lock
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out_dir="${repo_root}/website/static/playground"
profile="release"

echo "==> Building crabka-playground for wasm32-unknown-unknown (${profile})"
cargo build \
  --manifest-path "${repo_root}/Cargo.toml" \
  -p crabka-playground \
  --target wasm32-unknown-unknown \
  --release

wasm_in="${repo_root}/target/wasm32-unknown-unknown/${profile}/crabka_playground.wasm"

echo "==> Running wasm-bindgen --target web"
wasm-bindgen \
  --target web \
  --no-typescript \
  --out-dir "${out_dir}" \
  --out-name crabka_playground \
  "${wasm_in}"

# Optional size optimisation when wasm-opt (binaryen) is on PATH.
if command -v wasm-opt >/dev/null 2>&1; then
  echo "==> Optimising with wasm-opt -Oz"
  wasm-opt -Oz \
    "${out_dir}/crabka_playground_bg.wasm" \
    -o "${out_dir}/crabka_playground_bg.wasm"
fi

echo "==> Playground staged into ${out_dir}:"
ls -lh "${out_dir}/crabka_playground.js" "${out_dir}/crabka_playground_bg.wasm"
