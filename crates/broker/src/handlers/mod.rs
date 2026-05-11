//! Handler dispatch. One module per API key implements:
//!
//!   `pub async fn handle(broker: &Broker, version: i16, req_bytes: &[u8])
//!       -> Result<bytes::Bytes, BrokerError>`
//!
//! Handlers decode the request, do their work, encode the response, and
//! return the encoded bytes ready to ship after the response header is
//! prepended in `network::dispatch`.

#![allow(dead_code)] // handlers land per-API in Phase E.

use bytes::Bytes;

use crate::error::BrokerError;

/// Function signature every handler in this module exports.
pub type HandlerFn = fn(
    broker: &crate::broker::Broker,
    version: i16,
    correlation_id: i32,
    req_bytes: &[u8],
) -> futures_util::future::BoxFuture<'static, Result<Bytes, BrokerError>>;

/// API key → handler function. Built by `Broker::start` from the per-API
/// modules that exist after Phase E.
#[derive(Default)]
pub struct HandlerTable {
    table: std::collections::HashMap<i16, HandlerFn>,
}

impl HandlerTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, api_key: i16, handler: HandlerFn) {
        self.table.insert(api_key, handler);
    }

    #[must_use]
    pub fn get(&self, api_key: i16) -> Option<HandlerFn> {
        self.table.get(&api_key).copied()
    }
}

pub(crate) mod api_versions;
pub(crate) mod create_topics;
pub(crate) mod delete_topics;
pub(crate) mod describe_configs;
pub(crate) mod fetch;
pub(crate) mod find_coordinator;
pub(crate) mod list_offsets;
pub(crate) mod metadata;
pub(crate) mod produce;

/// Build the dispatch table. Phase E registers concrete handlers; for
/// now this is an empty table so the dispatch loop can still look up.
#[must_use]
pub(crate) fn build_table() -> HandlerTable {
    let mut t = HandlerTable::new();
    t.register(0, produce::handle);
    t.register(1, fetch::handle);
    t.register(2, list_offsets::handle);
    t.register(3, metadata::handle);
    t.register(10, find_coordinator::handle);
    t.register(18, api_versions::handle);
    t.register(19, create_topics::handle);
    t.register(20, delete_topics::handle);
    t.register(32, describe_configs::handle);
    t
}
