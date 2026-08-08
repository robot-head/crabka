# crabka-playground

WebAssembly bindings that drive [Crabka](https://github.com/robot-head/crabka)'s
deterministic `KRaft` consensus engine in the browser.

This crate is a thin `wasm-bindgen` shim over
[`crabka-kraft-core`](../kraft-core)'s `sim::Sim`. `sim::Sim` is the same pure,
sans-IO multi-node simulator that the integration tests and `crabka-docgen`
use. The shim drives the interactive "simulate consensus in your browser"
playground on the docs site. In the playground you can inject partitions, drop
or reorder or duplicate messages, and append records. You then watch a cluster
elect a leader, lose it, and recover. All of this is live, with no backend.

## Building

CI runs the site build `website/build-playground.sh`. That script compiles this
crate to `wasm32-unknown-unknown` and runs `wasm-bindgen --target web`. It
writes an ES module and a `.wasm` file into `website/static/playground/`.

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version <matches Cargo.lock>
./website/build-playground.sh
```

The JavaScript front-end is in `website/static/playground/`.
