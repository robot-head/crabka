# Bazel build (client-focused)

Bazel is a **secondary** build path for Crabka, scoped to the lean Kafka
**client** libraries. Cargo remains the source of truth for the full workspace;
Bazel reads the same `Cargo.toml` / `Cargo.lock` through
[`crate_universe`](https://bazelbuild.github.io/rules_rust/crate_universe.html),
so there is no second dependency set to keep in sync.

## Layout

| File | Purpose |
| --- | --- |
| `MODULE.bazel` | bzlmod: pins `rules_rust`, the Rust toolchain (1.96.0 / edition 2024), and the `crate_universe` repo (`@crates`) spliced from the Cargo workspace. |
| `.bazelrc`, `.bazelversion` | Bazel settings + version pin. |
| `//:BUILD.bazel` | Re-exported public targets + the `//:gssapi` flag. |
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

The SASL/GSSAPI path rides on `sspi`, a git-patched dependency (see
`[patch.crates-io]` in the root `Cargo.toml`). It is **off by default**. Enable it
with:

```sh
bazel build @crabka//:client_minimal --//:gssapi=true   # or: --config=gssapi
```

That flips `:gssapi_enabled`, which turns on the `sspi-keytab` feature and links
the `sspi` crate in `//crates/security` and `//crates/client-core`.

## Status / caveats

This scaffold was authored without a Bazel run (the sandbox blocks
`bcr.bazel.build` and `static.crates.io`), so it has **not** been built end to
end. Expect to iterate on first build with network access:

1. **`MODULE.bazel.lock`** — generate and commit it on first resolve
   (`bazel mod deps`). The `rules_rust` version is a best-effort pin; bump to the
   latest release if resolution complains.
2. **`//crates/protocol`** — the generated wire codecs are pulled in via
   `include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/..."))`. That env var
   needs an absolute manifest dir under the Bazel sandbox; the generated/schema
   trees are wired as `compile_data` and `CARGO_MANIFEST_DIR` is set in
   `rustc_env`. If rustc can't find the files, switch those `include!`s to
   sandbox-relative paths or route them through a `genrule`.
3. **`@crates//:sspi`** — referencing an optional, default-disabled dependency
   from crate_universe may need an explicit annotation (a `crate.annotation` /
   feature opt-in in `MODULE.bazel`) so the alias is generated.
