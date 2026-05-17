//! Crabka rebalancer — Cruise-Control-equivalent partition placement
//! advisor (and, starting in slice 43b, executor).
//!
//! See `docs/superpowers/specs/2026-05-17-crabka-rebalancer-43a-design.md`
//! and the surrounding roadmap doc for the full slice plan.

// Module mounts come online as later tasks land them.

/// Generated protobuf + Connect server stubs. The actual content lives
/// in `OUT_DIR/crabka.rebalancer.v1.rs` and is produced by `build.rs`.
///
/// Pedantic lints are silenced here because the include is verbatim
/// codegen output; we cannot retrofit `#[must_use]` annotations or
/// shorter helper functions without forking the upstream codegen.
#[allow(clippy::pedantic, clippy::style)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/crabka.rebalancer.v1.rs"));
}

pub mod model;
pub mod goals;
pub mod optimizer;
