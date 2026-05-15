//! Per-connection SASL authentication state machine.
//!
//! Slice 12. Drives `SaslHandshake` (17) and `SaslAuthenticate` (36).
//!
//! The state machine is deliberately separate from the byte-level I/O loop
//! in `dispatch.rs`: handlers (added in T13/T14) mutate `ConnectionAuth`
//! based on decoded request bodies; the dispatcher only consults the state
//! to gate non-allowlisted requests before authentication completes.

// T12 lands the state machine + gate. Several variants and the `principal`
// accessor are exercised by T13 (PLAIN), T14 (SCRAM), and T15 (admin) — keep
// the surface in one place so those tasks add no churn here.
#![allow(dead_code)]

use crabka_security::{Principal, SaslMechanism, ScramServerExchange};

/// Per-connection SASL state. Transitions:
/// `Anonymous` -> (`SaslHandshake`) -> `Negotiating` -> (`SaslAuthenticate` ok)
///   -> `Authenticated`.
///
/// For PLAINTEXT/SSL listeners the dispatcher initialises the connection
/// directly to `Authenticated { principal: ANONYMOUS }` so the pre-auth
/// gate is a no-op.
#[derive(Debug)]
pub enum ConnectionAuth {
    /// PLAINTEXT / SSL listener, or pre-handshake on a SASL listener.
    Anonymous,
    /// `SaslHandshake` received; awaiting (possibly multiple) `SaslAuthenticate`.
    Negotiating {
        mechanism: SaslMechanism,
        exchange: SaslExchange,
    },
    Authenticated {
        principal: Principal,
    },
}

/// In-flight SASL exchange. `Plain` carries no state because PLAIN is a
/// single round-trip; `ScramPending` is the post-handshake / pre-client-first
/// state for SCRAM (we need the client's `username` to materialise a
/// `ScramServerExchange`, so the real exchange is built lazily in T14);
/// `Scram` wraps the live RFC 5802 server state machine once the first
/// client message arrives.
#[derive(Debug)]
pub enum SaslExchange {
    Plain,
    ScramPending,
    Scram(ScramServerExchange),
}

impl ConnectionAuth {
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Authenticated { .. })
    }

    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        if let Self::Authenticated { principal } = self {
            Some(principal)
        } else {
            None
        }
    }
}

/// Pre-auth allowlist: `api_key`s clients may send before completing SASL.
///
/// Mirrors Apache Kafka's pre-auth allowlist: a client must be able to
/// negotiate the mechanism (`SaslHandshake` = 17), run the SASL exchange
/// (`SaslAuthenticate` = 36), and discover supported APIs
/// (`ApiVersions` = 18) before authenticating. Everything else is rejected
/// with `ILLEGAL_SASL_STATE` (34) and the connection is closed.
#[must_use]
pub fn is_pre_auth_allowed(api_key: i16) -> bool {
    matches!(api_key, 17 | 36 | 18)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_auth_allowlist_accepts_handshake_authenticate_apiversions() {
        assert!(is_pre_auth_allowed(17), "SaslHandshake");
        assert!(is_pre_auth_allowed(36), "SaslAuthenticate");
        assert!(is_pre_auth_allowed(18), "ApiVersions");
    }

    #[test]
    fn pre_auth_allowlist_rejects_data_plane_apis() {
        assert!(!is_pre_auth_allowed(0), "Produce");
        assert!(!is_pre_auth_allowed(1), "Fetch");
        assert!(!is_pre_auth_allowed(3), "Metadata");
        assert!(!is_pre_auth_allowed(19), "CreateTopics");
    }

    #[test]
    fn anonymous_is_not_authenticated() {
        let a = ConnectionAuth::Anonymous;
        assert!(!a.is_authenticated());
        assert!(a.principal().is_none());
    }

    #[test]
    fn negotiating_is_not_authenticated() {
        let a = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Plain,
            exchange: SaslExchange::Plain,
        };
        assert!(!a.is_authenticated());
        assert!(a.principal().is_none());
    }

    #[test]
    fn negotiating_scram_pending_is_not_authenticated() {
        let a = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::ScramSha512,
            exchange: SaslExchange::ScramPending,
        };
        assert!(!a.is_authenticated());
        assert!(a.principal().is_none());
    }

    #[test]
    fn authenticated_returns_principal() {
        let a = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
            },
        };
        assert!(a.is_authenticated());
        let p = a.principal().expect("principal");
        assert_eq!(p.name, "alice");
        assert_eq!(p.mechanism, SaslMechanism::ScramSha512);
    }
}
