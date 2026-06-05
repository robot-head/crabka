//! `crabka-grpc-gateway` — gRPC / Connect-RPC + HTTP gateway into Crabka topics.
//!
//! Built entirely on the native client crates; the broker is never modified.

pub mod codec;
pub mod config;
pub mod dedup {
    //! Replaced in Task 12. Stub keeps `ProduceCore` compiling.
    use crate::error::GatewayError;
    use crate::types::{GatewayRecord, RecordOutcome};

    pub struct DedupEngine;

    impl DedupEngine {
        #[allow(clippy::unused_async)]
        pub async fn dedup_produce(
            &self,
            _rec: &GatewayRecord,
            _value: bytes::Bytes,
        ) -> Result<RecordOutcome, GatewayError> {
            Err(GatewayError::Other("dedup not wired yet".into()))
        }
    }
}
pub mod error;
pub mod produce;
pub mod state;
pub mod types;

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
