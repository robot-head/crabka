//! `crabka-grpc-gateway` — gRPC / Connect-RPC + HTTP gateway into Crabka topics.
//!
//! Built entirely on the native client crates; the broker is never modified.

/// Generated protobuf + Connect server stubs. The actual content lives
/// in `OUT_DIR/crabka.gateway.v1.rs` and is produced by `build.rs`.
///
/// Pedantic lints are silenced here because the include is verbatim
/// codegen output; we cannot retrofit `#[must_use]` annotations or
/// shorter helper functions without forking the upstream codegen.
#[allow(clippy::pedantic, clippy::style)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/crabka.gateway.v1.rs"));
}
