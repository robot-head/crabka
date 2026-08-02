# crabka-playground

WebAssembly bindings that drive [Crabka](https://github.com/robot-head/crabka)'s
deterministic `KRaft` consensus engine in the browser.

This crate is a thin `wasm-bindgen` shim over
[`crabka-kraft-core`](../kraft-core)'s `sim::Sim` — the same pure, sans-IO
multi-node simulator the integration tests and `crabka-docgen` use. It powers
the interactive "simulate consensus in your browser" playground on the docs
site: inject partitions, drop / reorder / duplicate messages, append records,
and watch a cluster elect a leader, lose it, and recover — live, with no
backend.

## Building

The site build (`aspect playground`, run from CI) compiles this crate
to `wasm32-unknown-unknown` and runs `wasm-bindgen --target web`, emitting an ES
module + `.wasm` into `website/static/playground/`.

```sh
aspect playground
```

The JavaScript front-end lives in `website/static/playground/`.
