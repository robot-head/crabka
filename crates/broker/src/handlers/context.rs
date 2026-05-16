//! Per-request connection metadata threaded through every inline-intercept
//! handler.

use std::net::SocketAddr;

use crabka_security::Principal;

/// Per-request connection metadata. Constructed once per frame in
/// `network::dispatch` from the authenticated `ConnectionAuth`, the
/// accept-time peer `SocketAddr`, and the frame's `client_id` header.
pub(crate) struct RequestContext<'a> {
    pub principal: &'a Principal,
    pub peer: &'a SocketAddr,
    /// Frame's `client_id` header. Empty string when the wire field is
    /// null (`-1` length) or zero-length. Matches the existing
    /// `peek_client_id(frame).unwrap_or("")` convention used for the
    /// `request_percentage` quota in the dispatch loop.
    pub client_id: &'a str,
}
