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

use std::collections::HashMap;
use std::hash::BuildHasher;

use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
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
    /// OAUTHBEARER, post-handshake / pre-token. The bearer token arrives in
    /// the first (and on success only) `SaslAuthenticate`.
    OAuthBearer,
    /// OAUTHBEARER token validation failed: the broker has returned the RFC
    /// 7628 error JSON (with `error_code = 0`, connection still open) and is
    /// awaiting the client's single-`\x01` final message before failing the
    /// connection with `SASL_AUTHENTICATION_FAILED`.
    OAuthBearerFailed,
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

/// `UNSUPPORTED_SASL_MECHANISM` (33) — peer requested a mechanism the broker
/// does not advertise. The connection stays open per Kafka behaviour so the
/// client can retry with a supported mechanism from the returned list.
const UNSUPPORTED_SASL_MECHANISM: i16 = 33;

/// `SASL_AUTHENTICATION_FAILED` (58) — credential check rejected by the
/// broker. The caller closes the connection after writing the response.
const SASL_AUTHENTICATION_FAILED: i16 = 58;

/// Handles `SaslHandshake` (`api_key` 17).
///
/// On a mechanism the broker advertises, transitions `auth` to
/// `Negotiating` and returns a success response carrying the enabled list.
/// On any unknown / disabled mechanism returns
/// [`UNSUPPORTED_SASL_MECHANISM`] (33) with the enabled list; the connection
/// stays open so the client can retry with a supported mechanism.
pub fn handle_handshake(
    req: &SaslHandshakeRequest,
    auth: &mut ConnectionAuth,
    enabled: &[SaslMechanism],
) -> SaslHandshakeResponse {
    let enabled_names: Vec<String> = enabled.iter().map(|m| m.wire_name().to_string()).collect();
    let requested = SaslMechanism::from_wire(&req.mechanism);
    match requested {
        Some(m) if enabled.contains(&m) => {
            let exchange = match m {
                SaslMechanism::Plain => SaslExchange::Plain,
                // SCRAM exchange is built lazily on the first SaslAuthenticate
                // round (T14), once the username is known. Until then we sit
                // in `ScramPending`. SHA-256 and SHA-512 share the same
                // dispatch state; the mechanism is preserved on the outer
                // `Negotiating` variant.
                SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                    SaslExchange::ScramPending
                }
                // The token arrives in the first SaslAuthenticate; no
                // pre-built state needed (slice 49).
                SaslMechanism::OAuthBearer => SaslExchange::OAuthBearer,
            };
            *auth = ConnectionAuth::Negotiating {
                mechanism: m,
                exchange,
            };
            SaslHandshakeResponse {
                error_code: 0,
                mechanisms: enabled_names,
                ..Default::default()
            }
        }
        _ => {
            tracing::debug!(
                requested = %req.mechanism,
                "SaslHandshake: unsupported mechanism"
            );
            SaslHandshakeResponse {
                error_code: UNSUPPORTED_SASL_MECHANISM,
                mechanisms: enabled_names,
                ..Default::default()
            }
        }
    }
}

/// Handles `SaslAuthenticate` (`api_key` 36) for the PLAIN mechanism.
///
/// On wire format: `auth_bytes` carries `\0<authzid>\0<authcid>\0<password>`.
/// `authzid` is ignored (RFC 4616 leaves it free-form and Kafka clients
/// typically send it empty); the username is `authcid`.
///
/// On a credential match this transitions `auth` to
/// [`ConnectionAuth::Authenticated`]. The caller closes the connection if
/// the returned `error_code` is non-zero.
pub fn handle_authenticate_plain<S: BuildHasher>(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    plain_credentials: &HashMap<String, String, S>,
) -> SaslAuthenticateResponse {
    let parts: Vec<&[u8]> = req.auth_bytes.split(|&b| b == 0).collect();
    if parts.len() != 3 {
        return fail_authenticate("malformed PLAIN payload");
    }
    let Ok(user) = std::str::from_utf8(parts[1]) else {
        return fail_authenticate("non-utf8 username");
    };
    let password = parts[2];
    match crabka_security::verify_plain(plain_credentials, user, password) {
        Ok(p) => {
            *auth = ConnectionAuth::Authenticated { principal: p };
            SaslAuthenticateResponse {
                error_code: 0,
                error_message: None,
                auth_bytes: bytes::Bytes::new(),
                session_lifetime_ms: 0,
                ..Default::default()
            }
        }
        Err(_) => fail_authenticate("authentication failed"),
    }
}

/// SCRAM-SHA-512 `SaslAuthenticate` handler. Implements the two-round RFC 5802
/// dance over Kafka's `SaslAuthenticate` (`api_key` 36) wire envelope.
///
/// Round 1 (client-first):
///   - `auth_bytes` = `n,,n=<user>,r=<client-nonce>` (raw SCRAM client-first
///     message). We parse the username, look up the credential in the
///     metadata image, and instantiate a [`ScramServerExchange`]. The
///     exchange consumes the same client-first bytes and emits the
///     server-first message (`r=…,s=…,i=…`), which becomes the response
///     `auth_bytes`. `auth` transitions from
///     `Negotiating { exchange: ScramPending }` to
///     `Negotiating { exchange: Scram(server) }` — still unauthenticated.
///
/// Round 2 (client-final):
///   - `auth_bytes` = `c=biws,r=<combined-nonce>,p=<proof>`. The exchange
///     verifies the client proof and emits the server-final message
///     (`v=<server-signature>`). On success `auth` transitions to
///     `Authenticated { principal }`; on any error the response carries
///     `error_code = 58` and the dispatcher closes the connection.
pub fn handle_authenticate_scram(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    controller: &crabka_raft::ControllerHandle,
) -> SaslAuthenticateResponse {
    // Round-1 case: still in `ScramPending` — build the exchange now that
    // we have the client-first bytes (and thus the username).
    if let ConnectionAuth::Negotiating {
        exchange: SaslExchange::ScramPending,
        mechanism,
    } = auth
    {
        let mech = *mechanism;
        let Some(username) = parse_scram_username(&req.auth_bytes) else {
            return fail_authenticate("malformed SCRAM client-first");
        };
        let Some(cred) = controller
            .current_image()
            .scram_credential(&username, mech)
            .cloned()
        else {
            return fail_authenticate("unknown user");
        };
        let mut server = ScramServerExchange::new(username, cred);
        // Feed the same client-first bytes; on success the exchange emits
        // the server-first message and advances its own internal state.
        match server.step(&req.auth_bytes) {
            crabka_security::StepResult::Continue(bytes) => {
                *auth = ConnectionAuth::Negotiating {
                    mechanism: mech,
                    exchange: SaslExchange::Scram(server),
                };
                SaslAuthenticateResponse {
                    error_code: 0,
                    error_message: None,
                    auth_bytes: bytes::Bytes::from(bytes),
                    session_lifetime_ms: 0,
                    ..Default::default()
                }
            }
            // Done on the first round would be a server bug — SCRAM is
            // always two round trips for SHA-512. Treat as auth failure.
            crabka_security::StepResult::Done(_, _) => {
                fail_authenticate("SCRAM server completed in one round")
            }
            crabka_security::StepResult::Failed(_) => fail_authenticate("SCRAM step failed"),
        }
    } else if let ConnectionAuth::Negotiating {
        exchange: SaslExchange::Scram(server),
        ..
    } = auth
    {
        // Round 2: exchange already exists. Step it with the client-final
        // bytes; on success extract the principal + server-final bytes and
        // transition to `Authenticated`.
        match server.step(&req.auth_bytes) {
            crabka_security::StepResult::Continue(_) => {
                // Two-round SCRAM-SHA-512: an extra `Continue` here is a bug.
                fail_authenticate("SCRAM second round expected Done")
            }
            crabka_security::StepResult::Done(principal, bytes) => {
                *auth = ConnectionAuth::Authenticated { principal };
                SaslAuthenticateResponse {
                    error_code: 0,
                    error_message: None,
                    auth_bytes: bytes::Bytes::from(bytes),
                    session_lifetime_ms: 0,
                    ..Default::default()
                }
            }
            crabka_security::StepResult::Failed(_) => fail_authenticate("SCRAM proof failed"),
        }
    } else {
        fail_authenticate("not in SCRAM negotiation")
    }
}

/// SASL/OAUTHBEARER `SaslAuthenticate` handler (KIP-255 / RFC 7628).
///
/// Round 1 (client initial response):
///   - `auth_bytes` = `n,,\x01auth=Bearer <token>\x01\x01`. We parse the
///     bearer token and validate it with `validator` against `now_ms`. On
///     success `auth` transitions to `Authenticated` and the response carries
///     empty `auth_bytes` with `error_code = 0` (single-round success).
///   - On any parse / validation failure we return the RFC 7628
///     `{"status":"invalid_token"}` JSON in `auth_bytes` with `error_code = 0`
///     (the connection stays open) and move to `OAuthBearerFailed`.
///
/// Round 2 (failure only): the JVM client replies to the error JSON with a
/// single `\x01`. We return `SASL_AUTHENTICATION_FAILED` (58); the dispatcher
/// closes the connection.
pub async fn handle_authenticate_oauthbearer(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &crabka_security::OAuthBearerValidator,
    now_ms: i64,
) -> SaslAuthenticateResponse {
    match auth {
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::OAuthBearer,
            mechanism,
        } => {
            let mech = *mechanism;
            match validate_bearer(&req.auth_bytes, validator, now_ms).await {
                Ok(outcome) => {
                    // T1: surface the OAUTHBEARER token expiry as
                    // session_lifetime_ms. The connection state still only
                    // carries the principal — T2 extends `Authenticated` to
                    // carry the session window for the per-connection re-auth
                    // timer (KIP-368).
                    let session_lifetime_ms =
                        outcome.expires_at_ms.map_or(0, |e| (e - now_ms).max(0));
                    *auth = ConnectionAuth::Authenticated {
                        principal: outcome.principal,
                    };
                    SaslAuthenticateResponse {
                        error_code: 0,
                        error_message: None,
                        auth_bytes: bytes::Bytes::new(),
                        session_lifetime_ms,
                        ..Default::default()
                    }
                }
                Err(reason) => {
                    tracing::debug!(reason, "OAUTHBEARER token rejected");
                    *auth = ConnectionAuth::Negotiating {
                        mechanism: mech,
                        exchange: SaslExchange::OAuthBearerFailed,
                    };
                    SaslAuthenticateResponse {
                        error_code: 0,
                        error_message: None,
                        auth_bytes: bytes::Bytes::from(
                            crabka_security::invalid_token_json().into_bytes(),
                        ),
                        session_lifetime_ms: 0,
                        ..Default::default()
                    }
                }
            }
        }
        // The client's `\x01` final message after a rejected token: complete
        // the RFC 7628 failure handshake by closing with code 58.
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::OAuthBearerFailed,
            ..
        } => fail_authenticate("oauthbearer token rejected"),
        _ => fail_authenticate("not in oauthbearer negotiation"),
    }
}

/// Parse + validate an OAUTHBEARER client initial response. The authzid, when
/// present, must equal the token principal (RFC 7628 / Kafka behaviour).
async fn validate_bearer(
    auth_bytes: &[u8],
    validator: &crabka_security::OAuthBearerValidator,
    now_ms: i64,
) -> Result<crabka_security::AuthOutcome, &'static str> {
    let parsed = crabka_security::parse_client_initial_response(auth_bytes)
        .map_err(|_| "malformed OAUTHBEARER client response")?;
    let outcome = validator
        .validate(&parsed.token, now_ms)
        .await
        .map_err(|_| "token validation failed")?;
    if let Some(authzid) = parsed.authzid
        && authzid != outcome.principal.name
    {
        return Err("authzid does not match token principal");
    }
    Ok(outcome)
}

/// Parse the username from a SCRAM client-first message.
///
/// Format (RFC 5802): `n,,n=<user>,r=<nonce>[,extensions...]`. The leading
/// `n,,` is the GS2 header (no channel binding, no authzid); the bare body
/// is a comma-separated attribute list. Returns the first `n=` value, or
/// `None` on any parse failure.
fn parse_scram_username(bytes: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(bytes).ok()?;
    let bare = s.strip_prefix("n,,")?;
    for attr in bare.split(',') {
        if let Some(v) = attr.strip_prefix("n=") {
            return Some(v.to_string());
        }
    }
    None
}

/// Build a [`SASL_AUTHENTICATION_FAILED`] response. `reason` is logged at
/// `debug` (never returned over the wire — auth failures are intentionally
/// opaque so attackers can't distinguish "no such user" from "bad password").
fn fail_authenticate(reason: &str) -> SaslAuthenticateResponse {
    tracing::debug!(reason, "SASL authenticate failed");
    SaslAuthenticateResponse {
        error_code: SASL_AUTHENTICATION_FAILED,
        error_message: Some("authentication failed".to_string()),
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms: 0,
        ..Default::default()
    }
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

    fn unsecured_token(sub: &str, exp_s: i64) -> String {
        use base64::Engine;
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        format!(
            "{}.{}.",
            B64.encode(b"{\"alg\":\"none\"}"),
            B64.encode(format!("{{\"sub\":\"{sub}\",\"exp\":{exp_s}}}").as_bytes())
        )
    }

    fn oauthbearer_client_response(token: &str) -> SaslAuthenticateRequest {
        SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(
                format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn handshake_oauthbearer_transitions_to_negotiating() {
        let mut auth = ConnectionAuth::Anonymous;
        let req = SaslHandshakeRequest {
            mechanism: "OAUTHBEARER".to_string(),
            ..Default::default()
        };
        let resp = handle_handshake(&req, &mut auth, &[SaslMechanism::OAuthBearer]);
        assert_eq!(resp.error_code, 0);
        assert!(matches!(
            auth,
            ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::OAuthBearer,
                exchange: SaslExchange::OAuthBearer,
            }
        ));
    }

    #[tokio::test]
    async fn oauthbearer_valid_token_authenticates() {
        let validator = crabka_security::OAuthBearerValidator::default();
        let now_ms = 1_000_000_000_000;
        let token = unsecured_token("svc-account", 1_000_000_900); // exp seconds → future of now
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::OAuthBearer,
            exchange: SaslExchange::OAuthBearer,
        };
        let resp = handle_authenticate_oauthbearer(
            &oauthbearer_client_response(&token),
            &mut auth,
            &validator,
            now_ms,
        )
        .await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.auth_bytes.is_empty());
        let p = auth.principal().expect("authenticated");
        assert_eq!(p.name, "svc-account");
        assert_eq!(p.auth_method, crabka_security::AuthMethod::SaslOAuthBearer);
    }

    #[tokio::test]
    async fn oauthbearer_invalid_token_returns_error_json_then_fails_on_dummy() {
        let validator = crabka_security::OAuthBearerValidator::Unsecured(
            crabka_security::UnsecuredJwsValidator {
                allowable_clock_skew_ms: 0,
                ..Default::default()
            },
        );
        let now_ms = 5_000_000_000_000;
        // exp far in the past → expired.
        let token = unsecured_token("admin", 1_000_000_000);
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::OAuthBearer,
            exchange: SaslExchange::OAuthBearer,
        };
        // Round 1: rejected → error JSON, error_code 0, connection stays open.
        let resp = handle_authenticate_oauthbearer(
            &oauthbearer_client_response(&token),
            &mut auth,
            &validator,
            now_ms,
        )
        .await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(&resp.auth_bytes[..], br#"{"status":"invalid_token"}"#);
        assert!(matches!(
            auth,
            ConnectionAuth::Negotiating {
                exchange: SaslExchange::OAuthBearerFailed,
                ..
            }
        ));
        // Round 2: the client's `\x01` dummy → SASL_AUTHENTICATION_FAILED (58).
        let dummy = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from_static(&[1u8]),
            ..Default::default()
        };
        let resp2 = handle_authenticate_oauthbearer(&dummy, &mut auth, &validator, now_ms).await;
        assert_eq!(resp2.error_code, SASL_AUTHENTICATION_FAILED);
        assert!(!auth.is_authenticated());
    }

    #[tokio::test]
    async fn oauthbearer_malformed_response_returns_error_json() {
        let validator = crabka_security::OAuthBearerValidator::default();
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::OAuthBearer,
            exchange: SaslExchange::OAuthBearer,
        };
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from_static(b"not-a-valid-gs2-message"),
            ..Default::default()
        };
        let resp =
            handle_authenticate_oauthbearer(&req, &mut auth, &validator, 1_000_000_000_000).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(&resp.auth_bytes[..], br#"{"status":"invalid_token"}"#);
    }

    #[tokio::test]
    async fn oauthbearer_authzid_mismatch_fails() {
        let validator = crabka_security::OAuthBearerValidator::default();
        let now_ms = 1_000_000_000_000;
        let token = unsecured_token("alice", 1_000_000_900);
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::OAuthBearer,
            exchange: SaslExchange::OAuthBearer,
        };
        // authzid "bob" != token principal "alice".
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(
                format!("n,a=bob,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes(),
            ),
            ..Default::default()
        };
        let resp = handle_authenticate_oauthbearer(&req, &mut auth, &validator, now_ms).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(&resp.auth_bytes[..], br#"{"status":"invalid_token"}"#);
        assert!(!auth.is_authenticated());
    }

    #[test]
    fn authenticated_returns_principal() {
        let a = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".into(),
                auth_method: crabka_security::AuthMethod::SaslScramSha512,
            },
        };
        assert!(a.is_authenticated());
        let p = a.principal().expect("principal");
        assert_eq!(p.name, "alice");
        assert_eq!(p.auth_method, crabka_security::AuthMethod::SaslScramSha512);
    }
}
