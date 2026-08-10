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
    /// Frame's `client_id` header. It is an empty string when the wire field
    /// is null (`-1` length) or zero-length. This matches the existing
    /// `peek_client_id(frame).unwrap_or("")` convention that the dispatch loop
    /// uses for the `request_percentage` quota.
    pub client_id: &'a str,
    /// `true` when the connection can serve the fetch records region with
    /// kernel `sendfile(2)`. That means a plaintext `TcpStream` on a
    /// SENDFILE-alias platform: Linux, Apple, FreeBSD, or `DragonFly`. The fetch
    /// handler uses this to emit the zero-copy `RecordsPayload::FileRegions`
    /// instead of `Raw` for large records runs (Increments D and E). It is
    /// `false` on TLS, on Windows, and for every non-fetch handler, which all
    /// ignore it.
    pub sendfile_capable: bool,
    /// Name of the [`crate::config::ListenerSpec`] serving this connection
    /// such as `"PLAINTEXT"`, `"SSL"`, or a configured listener name. This is
    /// the same string that self-registration writes as each
    /// [`crabka_metadata::BrokerEndpoint::name`]. Address-projecting
    /// handlers (`Metadata`, `FindCoordinator`, `DescribeCluster`) therefore
    /// advertise the endpoint that matches the listener the request arrived
    /// on, exactly as Apache Kafka does. Handlers that do not project broker
    /// addresses ignore this field.
    pub connection_listener_name: &'a str,
}

/// Connection attributes a KIP-714 telemetry handler needs to match a
/// client to a subscription. Telemetry RPCs are unauthenticated, so this
/// carries no principal. It carries only the wire-derived and
/// connection-derived fields.
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

    /// Kafka's group coordinator stores `InetAddress::toString()`, which is
    /// the peer IP prefixed with `/` and does not include the connection port.
    pub(crate) fn client_host(&self) -> String {
        match self.peer {
            SocketAddr::V4(peer) => format!("/{}", peer.ip()),
            SocketAddr::V6(peer) => {
                let address = peer
                    .ip()
                    .segments()
                    .map(|segment| format!("{segment:x}"))
                    .join(":");
                let scope = match peer.scope_id() {
                    0 => String::new(),
                    scope_id => format!("%{scope_id}"),
                };
                format!("/{address}{scope}")
            }
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
    use assert2::assert;
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

        assert!(ctx.principal.name == "alice");
        assert!(ctx.peer == &peer);
        assert!(ctx.client_id == "client-a");
        assert!(ctx.sendfile_capable);
        assert!(ctx.connection_listener_name == "SASL_SSL");
        assert!(ctx.client_host() == "/127.0.0.1");
    }

    #[test]
    fn request_context_client_host_uses_java_ipv6_format() {
        let principal = principal();
        let peer = SocketAddr::V6(std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::LOCALHOST,
            9092,
            0,
            4,
        ));

        let ctx = RequestContext::new(&principal, &peer, "client-a", false, "PLAINTEXT");

        assert!(ctx.client_host() == "/0:0:0:0:0:0:0:1%4");
    }

    #[test]
    fn telemetry_context_new_preserves_client_identity_fields() {
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = TelemetryContext::new(&peer, "client-a", "crabka-test", "1.2.3");

        assert!(ctx.peer == &peer);
        assert!(ctx.client_id == "client-a");
        assert!(ctx.software_name == "crabka-test");
        assert!(ctx.software_version == "1.2.3");
    }
}
