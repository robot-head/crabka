# Bazel build (client-focused)

Bazel is a **secondary** build path for Crabka. Its scope is the lean Kafka
**client** libraries. Cargo stays the authority for the full workspace. Bazel
reads the same `Cargo.toml` and `Cargo.lock` through
[`crate_universe`](https://bazelbuild.github.io/rules_rust/crate_universe_bzlmod.html),
so there is no second dependency set to keep in sync.

## Layout

| File | Purpose |
| --- | --- |
| `MODULE.bazel` | bzlmod. Pins `rules_rust`, the Rust toolchain (1.97.0 / edition 2024), and the `crate_universe` repo (`@crates`) that Bazel splices from the Cargo workspace. |
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
* the JSON-Schema serde path (`jsonschema` / `schemars`). Bazel builds
  `//crates/schema-serde` with `avro,protobuf` only.

## GSSAPI / Kerberos (`sspi`)

The SASL/GSSAPI path uses the released `sspi` crate and is part of the default
client build. Bazel gets the crate from the same `Cargo.toml` and `Cargo.lock`
splice as the rest of the workspace.

## Two implementation notes

* **Bazel does not run the `//crates/protocol` build script.** `build.rs` only
  *validates* that the checked-in `generated/` sources match the `schemas/` SHAs,
  and it emits no code. It is unnecessary here, because Bazel consumes the
  generated wire codecs directly as `compile_data`. The Cargo build and CI still
  catch SHA drift.
* **`CARGO_MANIFEST_DIR`.** The protocol crate pulls in its generated sources
  with `include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/..."))`. This
  needs an *absolute* manifest directory, because a relative one resolves against
  the directory of each file that includes it. `rustc_env` sets the variable to
  `$${pwd}/crates/protocol`. The `$$` escapes Bazel's Make-variable pass, so the
  literal `${pwd}` reaches the process_wrapper of rules_rust. The
  process_wrapper then substitutes the absolute exec root at action time.

## Sandbox note

Development of this build path happened behind a proxy that blocks
`bcr.bazel.build` and re-signs TLS with a private CA. The local-only
`user.bazelrc` file is gitignored. It points Bazel at the GitHub mirror of the
Bazel Central Registry and at a truststore that trusts the proxy CA. **Normal
environments with BCR access do not need `user.bazelrc`.** In such an
environment, generate and commit `MODULE.bazel.lock` on the first resolve.
