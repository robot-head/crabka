//! `crabka-grpc-gateway` — gRPC / Connect-RPC + HTTP gateway into Crabka topics.
//!
//! Built on the native client crates. The broker's Kafka wire stays byte-exact;
//! the gateway translates Connect-RPC calls into producer, consumer, schema,
//! deduplication, and authorization operations.
//!
//! ## Serving the gateway
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use axum::Router;
//! use crabka_grpc_gateway::{router, state::AppState};
//!
//! # async fn run(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
//! let app: Router = router(state);
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
//! axum::serve(listener, app).await?;
//! # Ok(())
//! # }
//! ```

pub mod authz;
pub mod codec;
pub mod config;
pub mod config_value;
pub mod consume;
pub mod dedup;
pub mod error;
pub mod forward;
pub mod handlers;
pub mod health;
pub mod ids;
pub mod metrics;
pub mod outbound;
pub mod outbound_config;
pub mod produce;
pub mod queue;
pub mod schema;
pub mod serve;
pub mod state;
pub mod streaming;
pub mod types;
pub mod webhook;
pub mod webhook_config;

/// Build the Connect-RPC [`axum::Router`] for the Gateway service.
///
/// The returned router has the shared `AppState` wired in as an
/// `Extension` layer so each handler can extract it with
/// `axum::Extension<Arc<AppState>>`.
pub fn router(state: std::sync::Arc<state::AppState>) -> axum::Router {
    pb::gateway_connect::GatewayServiceBuilder::<()>::new()
        .send(handlers::send)
        .send_stream(streaming::send_stream)
        .subscribe(streaming::subscribe)
        .queue_acquire(queue::queue_acquire)
        .queue_acknowledge(queue::queue_acknowledge)
        .queue_renew(queue::queue_renew)
        // `build_connect()` applies the `ConnectLayer` (protocol detection + per-request
        // `ConnectContext`); plain `.build()` omits it, so every Connect response falls back
        // to `application/json` regardless of the request's content-type, which breaks proto
        // connect-go clients (`invalid content-type: "application/json"; expecting
        // "application/proto"`). Compression/gRPC features are off, so this only adds the layer.
        .build_connect()
        .layer(axum::Extension(state))
}

/// Generated protobuf + Connect server stubs. The actual content lives
/// in `OUT_DIR/crabka.gateway.v1.rs` and is produced by `build.rs`.
///
/// Pedantic lints are silenced here because the include is verbatim
/// codegen output; we cannot retrofit `#[must_use]` annotations or
/// shorter helper functions without forking the upstream codegen.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/crabka.gateway.v1.rs"));
}
