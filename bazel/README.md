# Bazel build (client-focused)

Bazel is a **secondary** build path for Crabka, scoped to the lean Kafka
**client** libraries. Cargo remains the source of truth for the full workspace;
Bazel reads the same `Cargo.toml` / `Cargo.lock` through
[`crate_universe`](https://bazelbuild.github.io/rules_rust/crate_universe_bzlmod.html),
so there is no second dependency set to keep in sync.

## Layout

| File | Purpose |
| --- | --- |
| `MODULE.bazel` | bzlmod: pins `rules_rust`, the Rust toolchain (1.96.0 / edition 2024), and the `crate_universe` repo (`@crates`) spliced from the Cargo workspace. |
| `.bazelrc`, `.bazelversion` | Bazel settings + version pin. |
| `//:BUILD.bazel` | Re-exported public targets. |
| `//crates/<crate>:BUILD.bazel` | `rust_library` targets for the in-scope crates. |
| `//crates:BUILD.bazel` | Exports the remaining workspace members' `Cargo.toml`s so crate_universe can splice the whole workspace. |
| `//bazel/client_minimal` | The lean facade crate (Bazel-only; not a Cargo member). |

## Public targets

```
@crabka//:client_core        @crabka//:client_producer
@crabka//:client_consumer    @crabka//:client_admin
@crabka//:schema_serde       @crabka//:protocol
@crabka//:security           @crabka//:compression
@crabka//:metadata
@crabka//:client_minimal     # lean bundle of the above client surface
```

## `client_minimal`

`@crabka//:client_minimal` bundles the producer/consumer/admin/core clients plus
the Avro + Protobuf serdes. It deliberately **omits**:

* the columnar streams client (`polars` / `polars-arrow` / `arrow`), and
* the JSON-Schema serde path (`jsonschema` / `schemars`) — `//crates/schema-serde`
  is built with `avro,protobuf` only.

## GSSAPI / Kerberos (`sspi`)

The SASL/GSSAPI path rides on the released `sspi` crate and is part of the
default client build. Bazel gets it from the same `Cargo.toml` / `Cargo.lock`
splice as the rest of the workspace.

## Two implementation notes

* **`//crates/protocol` build script is not run under Bazel.** `build.rs` only
  *validates* that the checked-in `generated/` sources match the `schemas/` SHAs
  and emits no code, so it's dead weight here — the generated wire codecs are
  consumed directly as `compile_data`. SHA drift is still caught by the Cargo
  build and CI.
* **`CARGO_MANIFEST_DIR`.** protocol's generated sources are pulled in via
  `include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/..."))`, which needs
  an *absolute* manifest dir (a relative one resolves against each including
  file's directory). `rustc_env` sets it to `$${pwd}/crates/protocol` — the `$$`
  escapes Bazel's Make-variable pass so the literal `${pwd}` reaches rules_rust's
  process_wrapper, which substitutes the absolute exec root at action time.

## Sandbox note

This was developed behind a proxy that blocks `bcr.bazel.build` and re-signs TLS
with a private CA. The local-only `user.bazelrc` (gitignored) points Bazel at the
GitHub mirror of the Bazel Central Registry and at a truststore that trusts the
proxy CA. **Normal environments with BCR access do not need `user.bazelrc`.**
Generate and commit `MODULE.bazel.lock` on first resolve in such an environment.
