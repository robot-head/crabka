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
    /// `true` when the connection can serve the fetch records region via
    /// kernel `sendfile(2)` — i.e. a plaintext `TcpStream` on a SENDFILE-alias
    /// platform (Linux + Apple + FreeBSD/DragonFly). The fetch handler uses this
    /// to emit `RecordsPayload::FileRegions` (zero-copy) instead of `Raw` for
    /// large records runs (Increments D + E). `false` on TLS, on Windows, and
    /// for every non-fetch handler (which ignore it).
    pub sendfile_capable: bool,
    /// Name of the [`crate::config::ListenerSpec`] serving this connection
    /// (e.g. `"PLAINTEXT"` / `"SSL"` / a configured listener name). This is
    /// the same string that self-registration writes as each
    /// [`crabka_metadata::BrokerEndpoint::name`], so address-projecting
    /// handlers (`Metadata`, `FindCoordinator`, `DescribeCluster`) advertise
    /// the endpoint matching the listener the request arrived on — exactly as
    /// Apache Kafka does. Handlers that don't project broker addresses ignore
    /// this field.
    pub connection_listener_name: &'a str,
}

/// Connection attributes a KIP-714 telemetry handler needs to match a
/// client to a subscription. Telemetry RPCs are unauthenticated, so this
/// carries no principal — just the wire/connection-derived fields.
pub(crate) struct TelemetryContext<'a> {
    pub client_id: &'a str,
    pub peer: &'a std::net::SocketAddr,
    pub software_name: &'a str,
    pub software_version: &'a str,
}

impl<'a> RequestContext<'a> {
    pub(crate) fn new(
        principal: &'a Principal,
        peer: &'a SocketAddr,
        client_id: &'a str,
        sendfile_capable: bool,
        connection_listener_name: &'a str,
    ) -> Self {
        Self {
            principal,
            peer,
            client_id,
            sendfile_capable,
            connection_listener_name,
        }
    }
}

impl<'a> TelemetryContext<'a> {
    pub(crate) fn new(
        peer: &'a SocketAddr,
        client_id: &'a str,
        software_name: &'a str,
        software_version: &'a str,
    ) -> Self {
        Self {
            client_id,
            peer,
            software_name,
            software_version,
        }
    }
}

#[cfg(test)]
mod tests {

    use crabka_security::{AuthMethod, Principal};

    use super::*;

    fn principal() -> Principal {
        Principal {
            name: "alice".to_string(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec!["operators".to_string()],
        }
    }

    #[test]
    fn request_context_new_preserves_connection_fields() {
        let principal = principal();
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = RequestContext::new(&principal, &peer, "client-a", true, "SASL_SSL");

        assert2::assert!(
            (
                ctx.principal.name.as_str(),
                ctx.peer,
                ctx.client_id,
                ctx.sendfile_capable,
                ctx.connection_listener_name,
            ) == ("alice", &peer, "client-a", true, "SASL_SSL")
        );
    }

    #[test]
    fn telemetry_context_new_preserves_client_identity_fields() {
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = TelemetryContext::new(&peer, "client-a", "crabka-test", "1.2.3");

        assert2::assert!(
            (
                ctx.peer,
                ctx.client_id,
                ctx.software_name,
                ctx.software_version,
            ) == (&peer, "client-a", "crabka-test", "1.2.3")
        );
    }
}
