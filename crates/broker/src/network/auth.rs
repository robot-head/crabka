//! Per-connection SASL authentication state machine.
//!
//! Drives `SaslHandshake` (17) and `SaslAuthenticate` (36).
//!
//! The state machine is deliberately separate from the byte-level I/O loop
//! in `dispatch.rs`: handlers mutate `ConnectionAuth`
//! based on decoded request bodies; the dispatcher only consults the state
//! to gate non-allowlisted requests before authentication completes.

// Several variants and the `principal` accessor are exercised by the PLAIN,
// SCRAM, and admin paths — keep the surface in one place.
#![allow(dead_code)]

use std::{collections::HashMap, hash::BuildHasher};

use crabka_protocol::{
    ApiKey,
    owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use crabka_security::{Principal, SaslMechanism, ScramServerExchange};
use crabka_units::{ByteSize, Time, convert::TimeExt as _, kibibytes};

use crate::{
    codes::{ILLEGAL_SASL_STATE, UNSUPPORTED_SASL_MECHANISM},
    handlers::ApiKeyCode,
};

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
        /// KIP-48 side-channel: when the SCRAM round-1
        /// lookup falls back to a delegation token, the token's
        /// `expiry_timestamp_ms` is captured here so the round-2
        /// success arm can:
        /// 1. Set `ConnectionAuth::Authenticated.expires_at_ms` (the
        ///    KIP-368 re-auth ceiling), and
        /// 2. Set `authenticated_via_token: true` (the KIP-48
        ///    token-to-token chain guard read by `CreateDelegationToken`).
        ///
        /// `None` for every non-token-SCRAM negotiation (PLAIN,
        /// regular SCRAM, OAUTHBEARER, plus token-SCRAM round 1
        /// before the lookup fires). The presence of `Some(_)` is
        /// the unambiguous "token-authed session" marker.
        pending_token_expiry_ms: Option<i64>,
    },
    Authenticated {
        principal: Principal,
        /// SASL mechanism this connection authenticated with. Used by KIP-368
        /// in-band re-auth to reject a fresh `SaslHandshake` that
        /// switches mechanisms mid-connection. For mTLS / anonymous
        /// connections (no SASL), this is `SaslMechanism::Plain` as a
        /// don't-care default (the in-band reauth path is unreachable since
        /// the listener doesn't accept `SaslHandshake` at all).
        mechanism: SaslMechanism,
        /// Session expiry as Unix epoch ms. `None` = no expiry / no re-auth
        /// timer (PLAIN/SCRAM/mTLS/anonymous). `Some` = OAUTHBEARER token's
        /// `exp`; the dispatch loop closes the connection when this elapses.
        expires_at_ms: Option<i64>,
        /// KIP-48: whether this connection authenticated via a
        /// delegation token (SCRAM-SHA-256 with the token's HMAC as the
        /// password equivalent) rather than a "real" principal credential.
        /// The delegation-token RPCs check this flag — `CreateDelegationToken`
        /// rejects token-authed callers with
        /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (KIP-48 forbids
        /// token-creating-token chains), and `DescribeDelegationToken`
        /// restricts a token-authed caller to their own owned tokens
        /// regardless of any owner filter. Set to `true` only by the
        /// token-auth path; every other construction site defaults
        /// to `false`.
        authenticated_via_token: bool,
    },
    /// In-band re-authentication in progress: a `SaslHandshake` from a
    /// previously `Authenticated` OAuth connection. Holds the previous
    /// session snapshot so the post-validate equality check (same principal
    /// name, same mechanism) has something to compare against, and so a
    /// failed re-auth's error message can reference the still-current
    /// principal. (KIP-368.)
    Reauthenticating {
        previous: AuthenticatedSnapshot,
        exchange: SaslExchange,
    },
}

/// Snapshot of an `Authenticated` connection at the moment a re-auth
/// `SaslHandshake` arrives. Used by the `SaslAuthenticate` handler during
/// re-auth to enforce same-mechanism + same-principal-name semantics
/// (KIP-368).
#[derive(Debug, Clone)]
pub struct AuthenticatedSnapshot {
    pub principal: Principal,
    pub mechanism: SaslMechanism,
    pub expires_at_ms: Option<i64>,
}

/// In-flight SASL exchange. `Plain` carries no state because PLAIN is a
/// single round-trip; `ScramPending` is the post-handshake / pre-client-first
/// state for SCRAM (we need the client's `username` to materialise a
/// `ScramServerExchange`, so the real exchange is built lazily);
/// `Scram` wraps the live RFC 5802 server state machine once the first
/// client message arrives.
#[derive(Debug)]
pub enum SaslExchange {
    Plain,
    ScramPending,
    /// Boxed because `ScramServerExchange` grew past clippy's 200-byte
    /// `large_enum_variant` threshold once the
    /// `principal_override: Option<Principal>` field was added for delegation-token
    /// SCRAM fallback. Keeps the cold path off the hot enum size.
    Scram(Box<ScramServerExchange>),
    /// OAUTHBEARER, post-handshake / pre-token. The bearer token arrives in
    /// the first (and on success only) `SaslAuthenticate`.
    OAuthBearer,
    /// OAUTHBEARER token validation failed: the broker has returned the RFC
    /// 7628 error JSON (with `error_code = 0`, connection still open) and is
    /// awaiting the client's single-`\x01` final message before failing the
    /// connection with `SASL_AUTHENTICATION_FAILED`.
    OAuthBearerFailed,
    /// GSSAPI post-handshake / pre-first-token. The acceptor (and thus the
    /// live `GssapiServerExchange`) is built lazily on the first
    /// `SaslAuthenticate` round, once the client's AP-REQ arrives — mirroring
    /// the SCRAM `ScramPending` pattern (we don't want to read the keytab
    /// until a client actually starts a GSSAPI exchange).
    GssapiPending,
    /// GSSAPI multi-round in flight: the live RFC 4752 server state machine
    /// (GSS context establishment → security-layer negotiation). Boxed to
    /// keep the `sspi`-backed acceptor off the hot enum size.
    Gssapi(Box<crabka_security::gssapi::server::GssapiServerExchange>),
}

impl ConnectionAuth {
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Authenticated { .. })
    }

    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        if let Self::Authenticated { principal, .. } = self {
            Some(principal)
        } else {
            None
        }
    }

    /// KIP-48: whether the current session authenticated via a
    /// delegation token. Used by the four delegation-token RPC handlers
    /// to gate token-creating-token (`Create`) and visibility restriction
    /// (`Describe`). `false` for any non-`Authenticated` state.
    #[must_use]
    pub fn authenticated_via_token(&self) -> bool {
        matches!(
            self,
            Self::Authenticated {
                authenticated_via_token: true,
                ..
            }
        )
    }

    /// Whether `api_key` may be served given the current auth state.
    /// - `Anonymous` / `Negotiating`: allow the pre-auth allowlist
    ///   (ApiVersions=18, SaslHandshake=17, SaslAuthenticate=36).
    /// - `Reauthenticating`: allow only `SaslAuthenticate=36`. Any other
    ///   request during in-band re-auth is a protocol violation and the
    ///   dispatch layer closes the connection (KIP-368).
    /// - `Authenticated`: allow everything.
    #[must_use]
    pub fn allows_request(&self, api_key: ApiKeyCode) -> bool {
        match self {
            Self::Anonymous | Self::Negotiating { .. } => is_pre_auth_allowed(api_key),
            Self::Reauthenticating { .. } => api_key == ApiKey::SaslAuthenticate as i16,
            Self::Authenticated { .. } => true,
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
pub fn is_pre_auth_allowed(api_key: ApiKeyCode) -> bool {
    matches!(
        ApiKey::from_i16(api_key),
        Some(ApiKey::SaslHandshake | ApiKey::SaslAuthenticate | ApiKey::ApiVersions)
    )
}

/// `SASL_AUTHENTICATION_FAILED` (58) — credential check rejected by the
/// broker. The caller closes the connection after writing the response.
/// (Not yet in `crate::codes` — this is its only use site.)
const SASL_AUTHENTICATION_FAILED: i16 = 58;

/// RFC 4752 server "maximum message size" advertised in the auth-only
/// security-layer offer. 64 KiB matches the JVM broker's default SASL receive
/// buffer; with confidentiality/integrity disabled it only bounds the size of
/// the (empty) wrapped payloads, so the exact value is not load-bearing.
const GSSAPI_MAX_RECV: ByteSize = kibibytes(64);

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

    // In-band re-auth on an already-authenticated connection.
    // Per KIP-368, only the same mechanism is allowed; a mismatch is
    // ILLEGAL_SASL_STATE and the previous session stays in force (no
    // transition).
    if let ConnectionAuth::Authenticated {
        mechanism: current, ..
    } = auth
    {
        let current = *current;
        match requested {
            Some(m) if m == current => {
                // OK: snapshot the previous Authenticated and transition.
                let prev = std::mem::replace(auth, ConnectionAuth::Anonymous);
                let ConnectionAuth::Authenticated {
                    principal,
                    mechanism,
                    expires_at_ms,
                    authenticated_via_token: _,
                } = prev
                else {
                    unreachable!("matched Authenticated above");
                };
                let exchange = exchange_for_mechanism(m);
                *auth = ConnectionAuth::Reauthenticating {
                    previous: AuthenticatedSnapshot {
                        principal,
                        mechanism,
                        expires_at_ms,
                    },
                    exchange,
                };
                return SaslHandshakeResponse {
                    error_code: 0,
                    mechanisms: enabled_names,
                    ..Default::default()
                };
            }
            _ => {
                // Mechanism switch attempted — reject without transition.
                tracing::debug!(
                    requested = %req.mechanism,
                    "SaslHandshake: mechanism switch on authenticated connection (ILLEGAL_SASL_STATE)"
                );
                return SaslHandshakeResponse {
                    error_code: ILLEGAL_SASL_STATE,
                    mechanisms: enabled_names,
                    ..Default::default()
                };
            }
        }
    }

    match requested {
        Some(m) if enabled.contains(&m) => {
            let exchange = exchange_for_mechanism(m);
            *auth = ConnectionAuth::Negotiating {
                mechanism: m,
                exchange,
                // Fresh handshake; the token-fallback in
                // `handle_authenticate_scram` may populate this later
                // during SCRAM round 1.
                pending_token_expiry_ms: None,
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

/// Build the per-mechanism `SaslExchange` initial state. Extracted from
/// `handle_handshake` so both the initial-auth path and the re-auth path
/// can construct it identically.
fn exchange_for_mechanism(m: SaslMechanism) -> SaslExchange {
    match m {
        SaslMechanism::Plain => SaslExchange::Plain,
        // SCRAM exchange is built lazily on the first SaslAuthenticate
        // round, once the username is known. Until then we sit in
        // `ScramPending`. SHA-256 and SHA-512 share the same dispatch
        // state; the mechanism is preserved on the outer `Negotiating` /
        // `Reauthenticating` variant.
        SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => SaslExchange::ScramPending,
        // The token arrives in the first SaslAuthenticate; no pre-built
        // state needed.
        SaslMechanism::OAuthBearer => SaslExchange::OAuthBearer,
        // GSSAPI exchange is built lazily on the first SaslAuthenticate
        // round, once the client's AP-REQ arrives (we defer reading the
        // keytab until then). Until then we sit in `GssapiPending`.
        SaslMechanism::Gssapi => SaslExchange::GssapiPending,
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
            *auth = ConnectionAuth::Authenticated {
                principal: p,
                mechanism: SaslMechanism::Plain,
                expires_at_ms: None,
                // PLAIN never auths via a delegation token.
                authenticated_via_token: false,
            };
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
    controller: &dyn crate::metadata_source::MetadataSource,
) -> SaslAuthenticateResponse {
    // Round-1 case: still in `ScramPending` — build the exchange now that
    // we have the client-first bytes (and thus the username).
    if let ConnectionAuth::Negotiating {
        exchange: SaslExchange::ScramPending,
        mechanism,
        pending_token_expiry_ms: _,
    } = auth
    {
        let mech = *mechanism;
        let Some(username) = parse_scram_username(&req.auth_bytes) else {
            return fail_authenticate("malformed SCRAM client-first");
        };

        // Look up the SCRAM credential. KIP-48: when the
        // user is unknown AND the mechanism is SCRAM-SHA-256, fall
        // back to the delegation-token table (KIP-48 scopes
        // token-SCRAM to SHA-256 only). On a token hit, synthesize a
        // SCRAM credential whose stored/server keys are derived from
        // the token's HMAC bytes (see
        // `synthesize_token_scram_credential`), capture the owner
        // principal so the `Done` arm surfaces the caller as
        // `User:<owner>` rather than `User:<token-uuid>`, and capture
        // the token's `expiry_timestamp_ms` for the
        // KIP-368 re-auth ceiling.
        let image = controller.current_image();
        let (cred, principal_override, token_expiry_ms) =
            if let Some(scram_cred) = image.scram_credential(&username, mech) {
                (scram_cred.clone(), None, None)
            } else if mech == SaslMechanism::ScramSha256 {
                if let Some(token) = image.delegation_token_by_id(&username) {
                    let synth = synthesize_token_scram_credential(token);
                    let owner = Principal {
                        name: token.owner.name.clone(),
                        auth_method: crabka_security::AuthMethod::SaslScramSha256,
                        groups: vec![],
                    };
                    (synth, Some(owner), Some(token.expiry_timestamp_ms))
                } else {
                    return fail_authenticate("unknown user");
                }
            } else {
                return fail_authenticate("unknown user");
            };

        let server = match principal_override {
            Some(p) => ScramServerExchange::new_with_principal(username, cred, p),
            None => ScramServerExchange::new(username, cred),
        };
        // Feed the same client-first bytes; on success the exchange emits
        // the server-first message and yields the next phase.
        match server.step(&req.auth_bytes) {
            crabka_security::StepResult::Continue(bytes, next) => {
                *auth = ConnectionAuth::Negotiating {
                    mechanism: mech,
                    exchange: SaslExchange::Scram(Box::new(next)),
                    // Side-channel — `Some` here is the
                    // unambiguous "this is a token-authed session"
                    // signal that the round-2 success arm consumes
                    // to set `Authenticated.authenticated_via_token`
                    // + `expires_at_ms`.
                    pending_token_expiry_ms: token_expiry_ms,
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
        exchange: SaslExchange::Scram(_),
        ..
    } = auth
    {
        // Round 2: exchange already exists. `step` consumes the exchange, so
        // extract it by value (mirroring `handle_handshake`'s re-auth
        // snapshot swap) before stepping it with the client-final bytes; on
        // success extract the principal + server-final bytes and transition
        // to `Authenticated`.
        let ConnectionAuth::Negotiating {
            mechanism,
            exchange: SaslExchange::Scram(server),
            pending_token_expiry_ms,
        } = std::mem::replace(auth, ConnectionAuth::Anonymous)
        else {
            unreachable!("matched Negotiating{{Scram}} above");
        };
        match server.step(&req.auth_bytes) {
            crabka_security::StepResult::Continue(_, _) => {
                // Two-round SCRAM-SHA-512: an extra `Continue` here is a bug.
                fail_authenticate("SCRAM second round expected Done")
            }
            crabka_security::StepResult::Done(principal, bytes) => {
                // When round-1 fell back to a delegation
                // token, `pending_token_expiry_ms` is `Some(expiry)`
                // — its presence is both the marker for
                // `authenticated_via_token: true` and the value of
                // `expires_at_ms` (the KIP-368 re-auth ceiling).
                // For regular SCRAM, it's `None` and the
                // session has no expiry.
                let session_lifetime_ms =
                    pending_token_expiry_ms.map_or(0, |e| (e - crate::time_util::now_ms()).max(0));
                *auth = ConnectionAuth::Authenticated {
                    principal,
                    mechanism,
                    expires_at_ms: pending_token_expiry_ms,
                    authenticated_via_token: pending_token_expiry_ms.is_some(),
                };
                SaslAuthenticateResponse {
                    error_code: 0,
                    error_message: None,
                    auth_bytes: bytes::Bytes::from(bytes),
                    session_lifetime_ms,
                    ..Default::default()
                }
            }
            crabka_security::StepResult::Failed(_) => fail_authenticate("SCRAM proof failed"),
        }
    } else {
        fail_authenticate("not in SCRAM negotiation")
    }
}

/// KIP-48: fixed SCRAM iteration count for delegation-token
/// credentials. Specified by KIP-48 §"Token Format".
const TOKEN_SCRAM_ITERS: u32 = 4096;

/// KIP-48: build a synthetic SCRAM-SHA-256 credential for
/// authenticating callers against a delegation token. KIP-48 fixes:
///   - mechanism = SCRAM-SHA-256 (the only token-SCRAM mechanism)
///   - "password" = base64-encoded token HMAC bytes (the same value
///     `CreateDelegationToken` returns to the client and that clients
///     present as the SCRAM password)
///   - salt = UTF-8 bytes of `token_id` (the token UUID is already
///     uniformly random — no extra randomness needed)
///   - iters = [`TOKEN_SCRAM_ITERS`]
///
/// The result is identical to what `hash_scram_password_with_salt`
/// would produce for those inputs — computed on every auth attempt
/// rather than stored per-token in the metadata image.
fn synthesize_token_scram_credential(
    token: &crabka_metadata::DelegationToken,
) -> crabka_security::ScramCredential {
    use base64::Engine;
    let password = base64::engine::general_purpose::STANDARD.encode(&token.hmac);
    let salt = token.token_id.as_bytes().to_vec();
    crabka_security::scram::hash_scram_password_with_salt(
        password.as_bytes(),
        SaslMechanism::ScramSha256,
        TOKEN_SCRAM_ITERS,
        salt,
    )
}

/// SASL/GSSAPI (Kerberos, RFC 4752) `SaslAuthenticate` handler.
///
/// Multi-round, driven over Kafka's `SaslAuthenticate` (`api_key` 36) wire
/// envelope. The opaque GSS/SASL tokens ride in `auth_bytes` both ways:
///
/// Round 1 (client AP-REQ):
///   - `auth_bytes` = the GSS initial context token (AP-REQ). We build the
///     `sspi`-backed acceptor from the broker's keytab now that an exchange
///     has actually started, feed the token to a fresh
///     [`GssapiServerExchange`], and emit the server's context token (AP-REP)
///     as the response `auth_bytes`. `auth` transitions
///     `Negotiating { exchange: GssapiPending }` →
///     `Negotiating { exchange: Gssapi(..) }`, still unauthenticated.
///
/// Middle round(s) (security-layer negotiation, RFC 4752):
///   - the server emits its GSS-wrapped auth-only offer, the client replies
///     with its GSS-wrapped choice. Each `ServerStep::Challenge` becomes a
///     success response carrying the next token; `auth` stays `Negotiating`.
///
/// Final round (client layer choice):
///   - the exchange yields `ServerStep::Done { principal }`. We map the raw
///     Kerberos principal through `auth_to_local`, transition to
///     `Authenticated`, and reply with empty `auth_bytes` + `error_code = 0`.
///
/// Any GSS/codec error returns `SASL_AUTHENTICATION_FAILED` (58) and the
/// dispatcher closes the connection.
pub fn handle_authenticate_gssapi(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    config: &crabka_security::gssapi::GssapiConfig,
) -> SaslAuthenticateResponse {
    use crabka_security::gssapi::server::{GssapiServerExchange, ServerStep};

    // Round 1: still `GssapiPending` — build the acceptor-backed exchange now
    // that the first client token (AP-REQ) has arrived.
    if let ConnectionAuth::Negotiating {
        exchange: SaslExchange::GssapiPending,
        mechanism,
        pending_token_expiry_ms: _,
    } = auth
    {
        let mech = *mechanism;
        let keytab = config.keytab_path.to_string_lossy();
        let acceptor = match crabka_security::gssapi::provider::SspiAcceptor::new(
            &keytab,
            &config.service_name,
        ) {
            Ok(a) => a,
            Err(e) => return fail_authenticate(&format!("GSSAPI acceptor init failed: {e}")),
        };
        let exchange = GssapiServerExchange::new(Box::new(acceptor), GSSAPI_MAX_RECV);
        let step = match exchange.step(&req.auth_bytes) {
            Ok(s) => s,
            Err(e) => return fail_authenticate(&format!("GSSAPI accept failed: {e}")),
        };
        return match step {
            ServerStep::Challenge(token, next) => {
                *auth = ConnectionAuth::Negotiating {
                    mechanism: mech,
                    exchange: SaslExchange::Gssapi(Box::new(next)),
                    pending_token_expiry_ms: None,
                };
                gssapi_challenge_response(token)
            }
            // GSSAPI always negotiates the security layer after context
            // establishment, so round 1 never completes the exchange.
            ServerStep::Done { principal } => finish_gssapi(&principal, mech, config, auth),
        };
    }

    // Subsequent rounds: the exchange already exists. `step` consumes it, so
    // extract it by value (mirroring `handle_handshake`'s re-auth snapshot
    // swap) before stepping it with the client's token.
    if let ConnectionAuth::Negotiating {
        exchange: SaslExchange::Gssapi(_),
        ..
    } = auth
    {
        let ConnectionAuth::Negotiating {
            mechanism,
            exchange: SaslExchange::Gssapi(exchange),
            pending_token_expiry_ms: _,
        } = std::mem::replace(auth, ConnectionAuth::Anonymous)
        else {
            unreachable!("matched Negotiating{{Gssapi}} above");
        };
        let step = match exchange.step(&req.auth_bytes) {
            Ok(s) => s,
            Err(e) => return fail_authenticate(&format!("GSSAPI step failed: {e}")),
        };
        return match step {
            ServerStep::Challenge(token, next) => {
                *auth = ConnectionAuth::Negotiating {
                    mechanism,
                    exchange: SaslExchange::Gssapi(Box::new(next)),
                    pending_token_expiry_ms: None,
                };
                gssapi_challenge_response(token)
            }
            ServerStep::Done { principal } => finish_gssapi(&principal, mechanism, config, auth),
        };
    }

    fail_authenticate("not in GSSAPI negotiation")
}

/// A non-terminal GSSAPI round: hand the next token back to the client with
/// `error_code = 0`; the connection stays open and `auth` stays `Negotiating`.
fn gssapi_challenge_response(token: Vec<u8>) -> SaslAuthenticateResponse {
    SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::from(token),
        session_lifetime_ms: 0,
        ..Default::default()
    }
}

/// Map the authenticated Kerberos principal through `auth_to_local` and, on
/// success, transition `auth` to `Authenticated`.
fn finish_gssapi(
    raw_principal: &str,
    mech: SaslMechanism,
    config: &crabka_security::gssapi::GssapiConfig,
    auth: &mut ConnectionAuth,
) -> SaslAuthenticateResponse {
    let short = match map_gssapi_principal(raw_principal, config) {
        Ok(s) => s,
        Err(e) => return fail_authenticate(&format!("GSSAPI principal mapping failed: {e}")),
    };
    *auth = ConnectionAuth::Authenticated {
        principal: Principal {
            name: short,
            auth_method: crabka_security::AuthMethod::SaslGssapi,
            groups: vec![],
        },
        mechanism: mech,
        // GSSAPI sessions have no broker-imposed expiry (the ticket lifetime is
        // enforced by the KDC at context-establishment time, not re-checked
        // mid-session). KIP-368 re-auth, if configured, rides the same
        // SaslHandshake path as the other mechanisms.
        expires_at_ms: None,
        authenticated_via_token: false,
    };
    SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms: 0,
        ..Default::default()
    }
}

/// Apply the configured `auth_to_local` rules to a raw Kerberos principal.
///
/// `sspi` recovers the principal lower-cased (e.g. `alice@crabka.test`); we
/// re-canonicalise the realm to upper-case before matching because Kerberos
/// realms are conventionally upper-case and both the configured default realm
/// and the `auth_to_local` rules are written against the upper-case form. When
/// no default realm is configured we fall back to the principal's own realm,
/// so a single-component principal in its own realm maps to its primary via
/// the implicit `DEFAULT` rule.
fn map_gssapi_principal(
    raw: &str,
    config: &crabka_security::gssapi::GssapiConfig,
) -> Result<String, crabka_security::gssapi::name::NameError> {
    let (head, realm_raw) = raw.rsplit_once('@').unwrap_or((raw, ""));
    let realm = realm_raw.to_uppercase();
    let components: Vec<&str> = head.split('/').collect();
    let default_realm = config.realm.as_deref().unwrap_or(&realm);
    crabka_security::gssapi::name::apply(
        &config.principal_to_local_rules,
        &realm,
        &components,
        default_realm,
    )
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
// Single state-machine dispatch: Negotiating-success / Negotiating-failure /
// Reauth-success / Reauth-failure / fall-through. Extracting per-arm helpers
// would obscure the shape and force ferrying `mech` / `prev_mech` / now_ms /
// the cap through a parameter wall.
pub async fn handle_authenticate_oauthbearer(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &crabka_security::OAuthBearerValidator,
    now_ms: i64,
    max_session_lifetime: Option<Time>,
) -> SaslAuthenticateResponse {
    match auth {
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::OAuthBearer,
            mechanism,
            // OAUTHBEARER never carries a delegation-token expiry;
            // this side-channel is only ever populated by the SCRAM round-1
            // token-fallback path. Ignore here.
            pending_token_expiry_ms: _,
        } => {
            let mech = *mechanism;
            match validate_bearer(&req.auth_bytes, validator, now_ms).await {
                Ok(outcome) => {
                    // Clamp `session_lifetime_ms` to the optional
                    // broker cap, then anchor `Authenticated.expires_at_ms`
                    // to the CLAMPED value. The dispatch loop reads
                    // `expires_at_ms` to schedule the re-auth deadline — if
                    // we stored the raw token exp here, the broker would
                    // tolerate the connection past the value reported to
                    // the client.
                    let (session_lifetime_ms, effective_expires_at_ms) =
                        oauth_session_lifetime(outcome.expires_at_ms, now_ms, max_session_lifetime);
                    *auth = ConnectionAuth::Authenticated {
                        principal: outcome.principal,
                        mechanism: mech,
                        expires_at_ms: effective_expires_at_ms,
                        // OAUTHBEARER is a real SASL mechanism,
                        // never a delegation token.
                        authenticated_via_token: false,
                    };
                    successful_authentication(session_lifetime_ms)
                }
                Err(reason) => {
                    tracing::debug!(reason, "OAUTHBEARER token rejected");
                    *auth = ConnectionAuth::Negotiating {
                        mechanism: mech,
                        exchange: SaslExchange::OAuthBearerFailed,
                        // OAUTHBEARER failure path never
                        // involves a delegation token.
                        pending_token_expiry_ms: None,
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
        // In-band re-authentication. Validate the new token and,
        // on success, require the principal name to match the previous
        // session (KIP-368 forbids principal switches mid-connection).
        ConnectionAuth::Reauthenticating {
            previous,
            exchange: SaslExchange::OAuthBearer,
        } => {
            let prev_mech = previous.mechanism;
            let prev_name = previous.principal.name.clone();
            match validate_bearer(&req.auth_bytes, validator, now_ms).await {
                Ok(outcome) => {
                    if outcome.principal.name != prev_name {
                        tracing::debug!(
                            previous = %prev_name,
                            attempted = %outcome.principal.name,
                            "OAUTHBEARER re-auth principal mismatch"
                        );
                        // Principal switch — reject; dispatch closes the
                        // connection on non-zero error_code.
                        return SaslAuthenticateResponse {
                            error_code: SASL_AUTHENTICATION_FAILED,
                            error_message: Some(
                                "re-authentication may not change the principal".to_string(),
                            ),
                            auth_bytes: bytes::Bytes::new(),
                            session_lifetime_ms: 0,
                            ..Default::default()
                        };
                    }
                    // Same clamp as the Negotiating-success arm
                    // so re-auth respects the broker cap.
                    let (session_lifetime_ms, effective_expires_at_ms) =
                        oauth_session_lifetime(outcome.expires_at_ms, now_ms, max_session_lifetime);
                    *auth = ConnectionAuth::Authenticated {
                        principal: outcome.principal,
                        mechanism: prev_mech,
                        expires_at_ms: effective_expires_at_ms,
                        // OAUTHBEARER re-auth never produces a
                        // token-authed session.
                        authenticated_via_token: false,
                    };
                    successful_authentication(session_lifetime_ms)
                }
                Err(reason) => {
                    tracing::debug!(reason, "OAUTHBEARER re-auth token rejected");
                    SaslAuthenticateResponse {
                        error_code: SASL_AUTHENTICATION_FAILED,
                        error_message: Some("re-authentication failed".to_string()),
                        auth_bytes: bytes::Bytes::new(),
                        session_lifetime_ms: 0,
                        ..Default::default()
                    }
                }
            }
        }
        _ => fail_authenticate("not in oauthbearer negotiation"),
    }
}

fn oauth_session_lifetime(
    expires_at_ms: Option<i64>,
    now_ms: i64,
    max_session_lifetime: Option<Time>,
) -> (i64, Option<i64>) {
    let raw_session_ms = expires_at_ms.map_or(0, |expires| (expires - now_ms).max(0));
    let session_lifetime_ms =
        max_session_lifetime.map_or(raw_session_ms, |cap| raw_session_ms.min(cap.millis_i64()));
    (session_lifetime_ms, Some(now_ms + session_lifetime_ms))
}

fn successful_authentication(session_lifetime_ms: i64) -> SaslAuthenticateResponse {
    SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms,
        ..Default::default()
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
    use assert2::{assert, check};
    use crabka_units::secs;

    use super::*;

    fn assert_success_authenticate_response(
        resp: &SaslAuthenticateResponse,
        expected_auth_bytes: &[u8],
        expected_session_lifetime_ms: i64,
    ) {
        let expected = SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: bytes::Bytes::copy_from_slice(expected_auth_bytes),
            session_lifetime_ms: expected_session_lifetime_ms,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(*resp == expected);
    }

    fn assert_failed_authenticate_response(resp: &SaslAuthenticateResponse) {
        let expected = SaslAuthenticateResponse {
            error_code: SASL_AUTHENTICATION_FAILED,
            error_message: Some("authentication failed".to_string()),
            auth_bytes: bytes::Bytes::new(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(*resp == expected);
    }

    #[test]
    fn pre_auth_allowlist_accepts_sasl_apis_and_rejects_data_plane() {
        let cases = [
            (17, true),  // SaslHandshake
            (36, true),  // SaslAuthenticate
            (18, true),  // ApiVersions
            (0, false),  // Produce
            (1, false),  // Fetch
            (3, false),  // Metadata
            (19, false), // CreateTopics
        ];
        for (api_key, allowed) in cases {
            assert!(is_pre_auth_allowed(api_key) == allowed, "api key {api_key}");
        }
    }

    #[test]
    fn unauthenticated_states_have_no_principal() {
        let cases = [
            ("anonymous", ConnectionAuth::Anonymous),
            (
                "negotiating_plain",
                ConnectionAuth::Negotiating {
                    mechanism: SaslMechanism::Plain,
                    exchange: SaslExchange::Plain,
                    pending_token_expiry_ms: None,
                },
            ),
            (
                "negotiating_scram_pending",
                ConnectionAuth::Negotiating {
                    mechanism: SaslMechanism::ScramSha512,
                    exchange: SaslExchange::ScramPending,
                    pending_token_expiry_ms: None,
                },
            ),
        ];
        for (name, a) in cases {
            assert!(!a.is_authenticated(), "{name}");
            assert!(a.principal().is_none(), "{name}");
        }
    }

    fn unsecured_token(sub: &str, exp_s: i64) -> String {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
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
        assert!(resp.error_code == 0);
        assert!(matches!(
            auth,
            ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::OAuthBearer,
                exchange: SaslExchange::OAuthBearer,
                ..
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
            pending_token_expiry_ms: None,
        };
        let resp = handle_authenticate_oauthbearer(
            &oauthbearer_client_response(&token),
            &mut auth,
            &validator,
            now_ms,
            None,
        )
        .await;
        assert_success_authenticate_response(&resp, b"", 900_000);
        let p = auth.principal().expect("authenticated");
        assert!(p.name == "svc-account");
        assert!(p.auth_method == crabka_security::AuthMethod::SaslOAuthBearer);
        match auth {
            ConnectionAuth::Authenticated {
                expires_at_ms,
                authenticated_via_token,
                ..
            } => {
                assert!(expires_at_ms == Some(1_000_000_900_000));
                assert!(!authenticated_via_token);
            }
            _ => panic!("expected authenticated state"),
        }
    }

    #[tokio::test]
    async fn oauthbearer_invalid_token_returns_error_json_then_fails_on_dummy() {
        let validator = crabka_security::OAuthBearerValidator::Unsecured(
            crabka_security::UnsecuredJwsValidator {
                allowable_clock_skew: secs(0),
                ..Default::default()
            },
        );
        let now_ms = 5_000_000_000_000;
        // exp far in the past → expired.
        let token = unsecured_token("admin", 1_000_000_000);
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::OAuthBearer,
            exchange: SaslExchange::OAuthBearer,
            pending_token_expiry_ms: None,
        };
        // Round 1: rejected → error JSON, error_code 0, connection stays open.
        let resp = handle_authenticate_oauthbearer(
            &oauthbearer_client_response(&token),
            &mut auth,
            &validator,
            now_ms,
            None,
        )
        .await;
        let expected = SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: bytes::Bytes::from_static(br#"{"status":"invalid_token"}"#),
            session_lifetime_ms: 0,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
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
        let resp2 =
            handle_authenticate_oauthbearer(&dummy, &mut auth, &validator, now_ms, None).await;
        assert_failed_authenticate_response(&resp2);
        assert!(!auth.is_authenticated());
    }

    #[tokio::test]
    async fn oauthbearer_malformed_response_returns_error_json() {
        let validator = crabka_security::OAuthBearerValidator::default();
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::OAuthBearer,
            exchange: SaslExchange::OAuthBearer,
            pending_token_expiry_ms: None,
        };
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from_static(b"not-a-valid-gs2-message"),
            ..Default::default()
        };
        let resp =
            handle_authenticate_oauthbearer(&req, &mut auth, &validator, 1_000_000_000_000, None)
                .await;
        let expected = SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: bytes::Bytes::from_static(br#"{"status":"invalid_token"}"#),
            session_lifetime_ms: 0,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn oauthbearer_authzid_mismatch_fails() {
        let validator = crabka_security::OAuthBearerValidator::default();
        let now_ms = 1_000_000_000_000;
        let token = unsecured_token("alice", 1_000_000_900);
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::OAuthBearer,
            exchange: SaslExchange::OAuthBearer,
            pending_token_expiry_ms: None,
        };
        // authzid "bob" != token principal "alice".
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(
                format!("n,a=bob,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes(),
            ),
            ..Default::default()
        };
        let resp = handle_authenticate_oauthbearer(&req, &mut auth, &validator, now_ms, None).await;
        let expected = SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: bytes::Bytes::from_static(br#"{"status":"invalid_token"}"#),
            session_lifetime_ms: 0,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        assert!(!auth.is_authenticated());
    }

    #[test]
    fn gssapi_challenge_response_carries_token_and_zero_lifetime() {
        let resp = gssapi_challenge_response(vec![1, 2, 3, 4]);
        assert_success_authenticate_response(&resp, &[1, 2, 3, 4], 0);
    }

    #[test]
    fn finish_gssapi_maps_principal_and_returns_empty_success() {
        let config = crabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/unused.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![crabka_security::gssapi::name::Rule::Default],
            realm: Some("CRABKA.TEST".to_string()),
            kdc: None,
        };
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Gssapi,
            exchange: SaslExchange::GssapiPending,
            pending_token_expiry_ms: None,
        };

        let resp = finish_gssapi(
            "alice@crabka.test",
            SaslMechanism::Gssapi,
            &config,
            &mut auth,
        );

        assert_success_authenticate_response(&resp, b"", 0);
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                expires_at_ms,
                authenticated_via_token,
            } => {
                check!(principal.name.as_str() == "alice");
                check!(principal.auth_method == crabka_security::AuthMethod::SaslGssapi);
                check!(mechanism == SaslMechanism::Gssapi);
                check!(expires_at_ms == None);
                check!(!authenticated_via_token);
            }
            _ => panic!("expected GSSAPI authenticated state"),
        }
    }

    #[test]
    fn finish_gssapi_mapping_error_returns_auth_failure() {
        let config = crabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/unused.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![crabka_security::gssapi::name::Rule::Default],
            realm: Some("OTHER.REALM".to_string()),
            kdc: None,
        };
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Gssapi,
            exchange: SaslExchange::GssapiPending,
            pending_token_expiry_ms: None,
        };

        let resp = finish_gssapi(
            "alice@crabka.test",
            SaslMechanism::Gssapi,
            &config,
            &mut auth,
        );

        assert_failed_authenticate_response(&resp);
        assert!(matches!(auth, ConnectionAuth::Negotiating { .. }));
    }

    #[test]
    fn handle_authenticate_gssapi_round1_bad_keytab_fails_and_leaves_state_untouched() {
        let config = crabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/nonexistent.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![crabka_security::gssapi::name::Rule::Default],
            realm: Some("CRABKA.TEST".to_string()),
            kdc: None,
        };
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Gssapi,
            exchange: SaslExchange::GssapiPending,
            pending_token_expiry_ms: None,
        };
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from_static(b"AP-REQ"),
            ..Default::default()
        };

        let resp = handle_authenticate_gssapi(&req, &mut auth, &config);

        assert_failed_authenticate_response(&resp);
        assert!(matches!(
            auth,
            ConnectionAuth::Negotiating {
                exchange: SaslExchange::GssapiPending,
                ..
            }
        ));
    }

    /// Establishes the GSS context on the first token with no trailing
    /// AP-REP, so a single `step()` reaches `AwaitingChoice` directly —
    /// mirrors `crabka-security`'s own `gssapi::server` unit tests.
    struct FakeAcceptor;

    impl crabka_security::gssapi::GssAcceptor for FakeAcceptor {
        fn accept(
            &mut self,
            _client_token: &[u8],
        ) -> Result<crabka_security::gssapi::AcceptStep, crabka_security::gssapi::GssError>
        {
            Ok(crabka_security::gssapi::AcceptStep::Established(None))
        }
        fn wrap(
            &self,
            plaintext: &[u8],
            _confidential: bool,
        ) -> Result<Vec<u8>, crabka_security::gssapi::GssError> {
            Ok(plaintext.to_vec())
        }
        fn unwrap(&self, token: &[u8]) -> Result<Vec<u8>, crabka_security::gssapi::GssError> {
            Ok(token.to_vec())
        }
        fn src_principal(&self) -> Result<String, crabka_security::gssapi::GssError> {
            Ok("alice@CRABKA.TEST".to_string())
        }
    }

    #[test]
    fn handle_authenticate_gssapi_subsequent_round_completes_and_authenticates() {
        use crabka_security::gssapi::server::{GssapiServerExchange, ServerStep};

        let config = crabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/unused.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![crabka_security::gssapi::name::Rule::Default],
            realm: Some("CRABKA.TEST".to_string()),
            kdc: None,
        };

        // Drive the exchange to `AwaitingChoice` up front (mirroring round
        // 1's work), so this test targets `handle_authenticate_gssapi`'s
        // *subsequent round* branch specifically.
        let exchange = GssapiServerExchange::new(Box::new(FakeAcceptor), kibibytes(64));
        let exchange = match exchange.step(b"AP-REQ").expect("round 1 step") {
            ServerStep::Challenge(_, next) => next,
            ServerStep::Done { .. } => panic!("expected challenge"),
        };

        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Gssapi,
            exchange: SaslExchange::Gssapi(Box::new(exchange)),
            pending_token_expiry_ms: None,
        };

        let mut choice = vec![0x01u8, 0x00, 0x10, 0x00];
        choice.extend_from_slice(b"alice");
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(choice),
            ..Default::default()
        };

        let resp = handle_authenticate_gssapi(&req, &mut auth, &config);

        assert_success_authenticate_response(&resp, b"", 0);
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                ..
            } => {
                check!(principal.name.as_str() == "alice");
                check!(mechanism == SaslMechanism::Gssapi);
            }
            _ => panic!("expected GSSAPI authenticated state"),
        }
    }

    #[test]
    fn map_gssapi_principal_uppercases_realm_before_default_rule() {
        let config = crabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/unused.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![crabka_security::gssapi::name::Rule::Default],
            realm: Some("CRABKA.TEST".to_string()),
            kdc: None,
        };

        let short = map_gssapi_principal("alice@crabka.test", &config).expect("map principal");

        assert!(short == "alice");
    }

    #[test]
    fn fail_authenticate_has_kafka_sasl_failure_shape() {
        let resp = fail_authenticate("unit-test");
        assert_failed_authenticate_response(&resp);
    }

    #[test]
    fn authenticated_returns_principal() {
        let a = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".into(),
                auth_method: crabka_security::AuthMethod::SaslScramSha512,
                groups: vec![],
            },
            mechanism: SaslMechanism::ScramSha512,
            expires_at_ms: None,
            authenticated_via_token: false,
        };
        assert!(a.is_authenticated());
        let p = a.principal().expect("principal");
        assert!(p.name == "alice");
        assert!(p.auth_method == crabka_security::AuthMethod::SaslScramSha512);
    }

    // KIP-368: in-band re-auth tests.

    #[test]
    fn authenticated_state_carries_mechanism_and_expires_at_ms() {
        let auth = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".to_string(),
                auth_method: crabka_security::AuthMethod::SaslOAuthBearer,
                groups: vec![],
            },
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(2_000_000),
            authenticated_via_token: false,
        };
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                expires_at_ms,
                authenticated_via_token: _,
            } => {
                check!(principal.name.as_str() == "alice");
                check!(mechanism == SaslMechanism::OAuthBearer);
                check!(expires_at_ms == Some(2_000_000));
            }
            _ => panic!("expected Authenticated"),
        }
    }

    #[test]
    fn handshake_from_authenticated_with_same_mechanism_transitions_to_reauthenticating() {
        let mut auth = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".to_string(),
                auth_method: crabka_security::AuthMethod::SaslOAuthBearer,
                groups: vec![],
            },
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(2_000_000),
            authenticated_via_token: false,
        };
        let req = SaslHandshakeRequest {
            mechanism: "OAUTHBEARER".to_string(),
            ..Default::default()
        };
        let resp = handle_handshake(&req, &mut auth, &[SaslMechanism::OAuthBearer]);
        assert!(resp.error_code == 0);
        assert!(matches!(
            auth,
            ConnectionAuth::Reauthenticating {
                previous: AuthenticatedSnapshot {
                    mechanism: SaslMechanism::OAuthBearer,
                    ..
                },
                exchange: SaslExchange::OAuthBearer,
            }
        ));
    }

    #[test]
    fn handshake_from_authenticated_with_different_mechanism_rejected_with_illegal_sasl_state() {
        let mut auth = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".to_string(),
                auth_method: crabka_security::AuthMethod::SaslOAuthBearer,
                groups: vec![],
            },
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(2_000_000),
            authenticated_via_token: false,
        };
        let req = SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-512".to_string(),
            ..Default::default()
        };
        let resp = handle_handshake(
            &req,
            &mut auth,
            &[SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512],
        );
        // ILLEGAL_SASL_STATE = 34 per Apache Kafka protocol.
        assert!(resp.error_code == 34);
        // The state stays Authenticated (not transitioned).
        assert!(matches!(auth, ConnectionAuth::Authenticated { .. }));
    }

    #[tokio::test]
    async fn authenticate_during_reauth_same_principal_transitions_back_to_authenticated() {
        let validator = crabka_security::OAuthBearerValidator::default();
        let now_ms = 1_000_000_000_000;
        // Token's exp is in seconds; the validator computes expires_at_ms = exp * 1000.
        let new_token_exp_seconds: i64 = 1_000_000_900;
        let new_token_exp_millis: i64 = new_token_exp_seconds * 1000;
        let token = unsecured_token("alice", new_token_exp_seconds);
        let mut auth = ConnectionAuth::Reauthenticating {
            previous: AuthenticatedSnapshot {
                principal: Principal {
                    name: "alice".to_string(),
                    auth_method: crabka_security::AuthMethod::SaslOAuthBearer,
                    groups: vec![],
                },
                mechanism: SaslMechanism::OAuthBearer,
                expires_at_ms: Some(now_ms + 1_000), // about to expire
            },
            exchange: SaslExchange::OAuthBearer,
        };
        let resp = handle_authenticate_oauthbearer(
            &oauthbearer_client_response(&token),
            &mut auth,
            &validator,
            now_ms,
            None,
        )
        .await;
        assert_success_authenticate_response(&resp, b"", new_token_exp_millis - now_ms);
        assert!(matches!(
            auth,
            ConnectionAuth::Authenticated {
                mechanism: SaslMechanism::OAuthBearer,
                expires_at_ms: Some(_),
                ..
            }
        ));
        if let ConnectionAuth::Authenticated {
            principal,
            expires_at_ms,
            ..
        } = &auth
        {
            assert!(principal.name == "alice");
            assert!(*expires_at_ms == Some(new_token_exp_millis));
        } else {
            panic!("expected Authenticated");
        }
    }

    #[tokio::test]
    async fn authenticate_during_reauth_different_principal_rejected_with_sasl_auth_failed() {
        let validator = crabka_security::OAuthBearerValidator::default();
        let now_ms = 1_000_000_000_000;
        // Token belongs to "bob", but the prior session is "alice".
        let token = unsecured_token("bob", 1_000_000_900);
        let mut auth = ConnectionAuth::Reauthenticating {
            previous: AuthenticatedSnapshot {
                principal: Principal {
                    name: "alice".to_string(),
                    auth_method: crabka_security::AuthMethod::SaslOAuthBearer,
                    groups: vec![],
                },
                mechanism: SaslMechanism::OAuthBearer,
                expires_at_ms: Some(now_ms + 1_000),
            },
            exchange: SaslExchange::OAuthBearer,
        };
        let resp = handle_authenticate_oauthbearer(
            &oauthbearer_client_response(&token),
            &mut auth,
            &validator,
            now_ms,
            None,
        )
        .await;
        // SASL_AUTHENTICATION_FAILED = 58 per Apache Kafka protocol; the
        // error message must name the principal mismatch.
        check!(resp.error_code == SASL_AUTHENTICATION_FAILED);
        check!(
            resp.error_message
                .as_deref()
                .unwrap_or("")
                .contains("principal")
        );
        check!(resp.auth_bytes.as_ref() == b"".as_slice());
        check!(resp.session_lifetime_ms == 0);
        // Connection remained in Reauthenticating (dispatch will close).
        assert!(matches!(auth, ConnectionAuth::Reauthenticating { .. }));
    }

    #[test]
    fn allows_request_during_reauthenticating_only_sasl_authenticate() {
        let auth = ConnectionAuth::Reauthenticating {
            previous: AuthenticatedSnapshot {
                principal: Principal {
                    name: "alice".to_string(),
                    auth_method: crabka_security::AuthMethod::SaslOAuthBearer,
                    groups: vec![],
                },
                mechanism: SaslMechanism::OAuthBearer,
                expires_at_ms: Some(2_000_000),
            },
            exchange: SaslExchange::OAuthBearer,
        };
        let cases = [
            (36, true),  // SaslAuthenticate
            (17, false), // SaslHandshake
            (18, false), // ApiVersions
            (3, false),  // Metadata
        ];
        for (api_key, allowed) in cases {
            assert!(auth.allows_request(api_key) == allowed, "api key {api_key}");
        }
    }

    #[test]
    fn allows_request_anonymous_uses_pre_auth_allowlist() {
        let auth = ConnectionAuth::Anonymous;
        let cases = [(17, true), (36, true), (18, true), (0, false), (3, false)];
        for (api_key, allowed) in cases {
            assert!(auth.allows_request(api_key) == allowed, "api key {api_key}");
        }
    }

    #[test]
    fn allows_request_authenticated_allows_all() {
        let auth = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".into(),
                auth_method: crabka_security::AuthMethod::SaslScramSha512,
                groups: vec![],
            },
            mechanism: SaslMechanism::ScramSha512,
            expires_at_ms: None,
            authenticated_via_token: false,
        };
        for api_key in [0, 3, 17, 36] {
            assert!(auth.allows_request(api_key), "api key {api_key}");
        }
    }

    // KIP-368 ceiling: the server-side
    // `max_session_lifetime_seconds` cap clamps both the response field and
    // the `Authenticated.expires_at_ms` stored on the connection.

    #[tokio::test]
    async fn handle_authenticate_oauthbearer_applies_max_session_lifetime_cap() {
        let validator = crabka_security::OAuthBearerValidator::Unsecured(
            crabka_security::UnsecuredJwsValidator {
                allowable_clock_skew: secs(0),
                ..Default::default()
            },
        );
        let now_ms = 1_000_000_i64;
        let exp_ms = now_ms + 60_000; // token good for 60s
        let token = unsecured_token("alice", exp_ms / 1000);
        let req = oauthbearer_client_response(&token);

        // (server cap in seconds, expected session lifetime in ms). The
        // stored expires_at_ms must reflect the clamped value too, not the
        // raw token exp.
        let cases = [
            (Some(secs(30)), 30_000_i64), // cap below the token's 60s exp → clamped
            (None, 60_000),               // unset cap → raw token exp
            (Some(secs(600)), 60_000),    // cap above exp → no effect
        ];
        for (cap, want_lifetime_ms) in cases {
            let mut auth = ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::OAuthBearer,
                exchange: SaslExchange::OAuthBearer,
                pending_token_expiry_ms: None,
            };
            let resp =
                handle_authenticate_oauthbearer(&req, &mut auth, &validator, now_ms, cap).await;
            check!(resp.error_code == 0, "cap {cap:?}");
            check!(resp.session_lifetime_ms == want_lifetime_ms, "cap {cap:?}");
            match auth {
                ConnectionAuth::Authenticated { expires_at_ms, .. } => {
                    assert!(
                        expires_at_ms == Some(now_ms + want_lifetime_ms),
                        "cap {cap:?}: expires_at_ms must reflect the clamped value"
                    );
                }
                _ => panic!("cap {cap:?}: expected Authenticated"),
            }
        }
    }

    // KIP-48 — SCRAM-SHA-256 delegation-token fallback tests.
    //
    // The tests below spin up a single-voter raft controller so we can
    // append a `DelegationTokenRecord` and then exercise
    // `handle_authenticate_scram` against the live image.

    mod token_scram_fallback {
        use std::{sync::Arc, time::Duration};

        use assert2::{assert, check};
        use crabka_metadata::{DelegationTokenRecord, MetadataRecord};
        use crabka_security::{
            KafkaPrincipal, ScramClientExchange, scram::hash_scram_password_with_salt,
        };
        use tempfile::TempDir;

        use super::*;

        async fn test_controller(
            log_dir: std::path::PathBuf,
        ) -> Arc<crabka_raft::ControllerHandle> {
            let cfg = crabka_raft::ControllerConfig {
                election_timeout: crabka_units::millis(200),
                heartbeat_interval: crabka_units::millis(50),
                client_id: "test".into(),
                ..crabka_raft::ControllerConfig::for_tests(crabka_raft::NodeId(1), log_dir)
            };
            let handle = Arc::new(crabka_raft::Controller::start(cfg).await.unwrap());
            let mut rx = handle.watch_leader();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while rx.borrow().is_none() {
                assert!(std::time::Instant::now() < deadline, "no leader in 5s");
                let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
            }
            handle
        }

        /// Helper: append a delegation token to the controller's image.
        async fn append_token(
            controller: &crabka_raft::ControllerHandle,
            token_id: &str,
            owner_name: &str,
            hmac: Vec<u8>,
            expiry_timestamp_ms: i64,
        ) {
            let rec = MetadataRecord::V1DelegationToken(DelegationTokenRecord {
                token_id: token_id.into(),
                owner: KafkaPrincipal {
                    principal_type: "User".into(),
                    name: owner_name.into(),
                },
                hmac,
                issue_timestamp_ms: 0,
                expiry_timestamp_ms,
                max_timestamp_ms: expiry_timestamp_ms,
                renewers: vec![],
            });
            controller.submit_change(vec![rec]).await.unwrap();
        }

        /// Drive the SCRAM client through both rounds against the broker's
        /// `handle_authenticate_scram`. Returns the final `auth` state plus
        /// the round-2 server response so callers can assert on
        /// `error_code`, `session_lifetime_ms`, etc.
        fn drive_scram_to_done(
            controller: &crabka_raft::ControllerHandle,
            scram_username: &str,
            password: &[u8],
            mechanism: SaslMechanism,
        ) -> (ConnectionAuth, SaslAuthenticateResponse) {
            let mut auth = ConnectionAuth::Negotiating {
                mechanism,
                exchange: SaslExchange::ScramPending,
                pending_token_expiry_ms: None,
            };
            let client =
                ScramClientExchange::new(scram_username.into(), password.to_vec(), mechanism);

            // Round 1: client-first
            let (c1, client) = client.client_first().expect("client first");
            let resp1 = handle_authenticate_scram(
                &SaslAuthenticateRequest {
                    auth_bytes: bytes::Bytes::from(c1),
                    ..Default::default()
                },
                &mut auth,
                controller,
            );
            assert!(resp1.error_code == 0, "round 1 must succeed for happy path");

            // Round 2: client-final
            let (c2, _client) = client.step(&resp1.auth_bytes).expect("client final");
            let resp2 = handle_authenticate_scram(
                &SaslAuthenticateRequest {
                    auth_bytes: bytes::Bytes::from(c2),
                    ..Default::default()
                },
                &mut auth,
                controller,
            );
            (auth, resp2)
        }

        /// Happy path: image contains a delegation token, no matching
        /// regular SCRAM user, SCRAM-SHA-256 round-1 falls back to the
        /// token table and round-2 succeeds.
        #[tokio::test]
        async fn scram_sha256_falls_back_to_delegation_token_when_no_scram_user() {
            let dir = TempDir::new().unwrap();
            let controller = test_controller(dir.path().into()).await;
            let hmac = vec![0xABu8; 32];
            let expiry_ms = crate::time_util::now_ms() + 60_000;
            append_token(&controller, "tok-uuid", "alice", hmac.clone(), expiry_ms).await;

            let password = {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode(&hmac)
            };

            let mut auth = ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::ScramSha256,
                exchange: SaslExchange::ScramPending,
                pending_token_expiry_ms: None,
            };
            let client = ScramClientExchange::new(
                "tok-uuid".into(),
                password.as_bytes().to_vec(),
                SaslMechanism::ScramSha256,
            );
            let (c1, _client) = client.client_first().unwrap();
            let resp1 = handle_authenticate_scram(
                &SaslAuthenticateRequest {
                    auth_bytes: bytes::Bytes::from(c1),
                    ..Default::default()
                },
                &mut auth,
                &*controller,
            );
            // The server-first message is nonce-dependent, so pin
            // non-emptiness rather than exact bytes.
            let round1 = "round 1 must succeed: token-fallback synthesizes the credential";
            check!(resp1.error_code == 0, "{round1}");
            check!(resp1.error_message.as_deref() == None, "{round1}");
            check!(!resp1.auth_bytes.is_empty(), "{round1}");
            check!(resp1.session_lifetime_ms == 0, "{round1}");
            // Negotiating state now carries pending_token_expiry_ms.
            match &auth {
                ConnectionAuth::Negotiating {
                    pending_token_expiry_ms,
                    ..
                } => {
                    assert!(
                        *pending_token_expiry_ms == Some(expiry_ms),
                        "round 1 must thread the token expiry through"
                    );
                }
                other => panic!("expected Negotiating, got {other:?}"),
            }
            controller.cancel().await;
        }

        /// Round-2 success: full two-round-trip drive ends in
        /// `Authenticated` whose principal is the token's owner (`alice`),
        /// with `authenticated_via_token: true` and `expires_at_ms` set
        /// to the token's `expiry_timestamp_ms`.
        #[tokio::test]
        async fn token_authed_connection_has_authenticated_via_token_true_and_owner_principal() {
            let dir = TempDir::new().unwrap();
            let controller = test_controller(dir.path().into()).await;
            let hmac = vec![0x42u8; 32];
            let expiry_ms = crate::time_util::now_ms() + 60_000;
            append_token(&controller, "tok-xyz", "alice", hmac.clone(), expiry_ms).await;

            let password = {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode(&hmac)
            };

            let (auth, resp2) = drive_scram_to_done(
                &controller,
                "tok-xyz",
                password.as_bytes(),
                SaslMechanism::ScramSha256,
            );

            // The server-final message is nonce-dependent (non-empty), and
            // token SCRAM reports the remaining token lifetime (0, 60s].
            check!(resp2.error_code == 0, "round 2 must succeed");
            check!(
                resp2.error_message.as_deref() == None,
                "round 2 must succeed"
            );
            check!(!resp2.auth_bytes.is_empty(), "round 2 must succeed");
            check!(
                resp2.session_lifetime_ms > 0 && resp2.session_lifetime_ms <= 60_000,
                "round 2 must succeed"
            );
            match auth {
                ConnectionAuth::Authenticated {
                    principal,
                    mechanism,
                    expires_at_ms,
                    authenticated_via_token,
                } => {
                    // principal is the token OWNER, not the tokenId
                    check!(principal.name.as_str() == "alice");
                    check!(mechanism == SaslMechanism::ScramSha256);
                    // expires_at_ms = token expiry (KIP-368 ceiling)
                    check!(expires_at_ms == Some(expiry_ms));
                    // token-fallback must mark the session as token-authed
                    check!(authenticated_via_token);
                }
                other => panic!("expected Authenticated, got {other:?}"),
            }
            controller.cancel().await;
        }

        /// Token-fallback must NOT fire for an unknown SCRAM username
        /// when the image has no matching token either.
        #[tokio::test]
        async fn scram_sha256_token_fallback_does_not_fire_for_unknown_token_id() {
            let dir = TempDir::new().unwrap();
            let controller = test_controller(dir.path().into()).await;
            // No tokens appended.

            let mut auth = ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::ScramSha256,
                exchange: SaslExchange::ScramPending,
                pending_token_expiry_ms: None,
            };
            let client = ScramClientExchange::new(
                "no-such-token".into(),
                b"whatever".to_vec(),
                SaslMechanism::ScramSha256,
            );
            let (c1, _client) = client.client_first().unwrap();
            let resp = handle_authenticate_scram(
                &SaslAuthenticateRequest {
                    auth_bytes: bytes::Bytes::from(c1),
                    ..Default::default()
                },
                &mut auth,
                &*controller,
            );
            assert!(
                resp.error_code == SASL_AUTHENTICATION_FAILED,
                "no SCRAM user + no token = unknown-user failure"
            );
            assert_failed_authenticate_response(&resp);
            controller.cancel().await;
        }

        /// SCRAM-SHA-512 must NOT consult the delegation-token table
        /// even when the SCRAM username happens to match a token's id:
        /// KIP-48 scopes token-SCRAM to SHA-256 only.
        #[tokio::test]
        async fn scram_sha512_does_not_fall_back_to_token() {
            let dir = TempDir::new().unwrap();
            let controller = test_controller(dir.path().into()).await;
            // Image has a token with id "tok-xyz".
            let hmac = vec![0x55u8; 32];
            let expiry_ms = crate::time_util::now_ms() + 60_000;
            append_token(&controller, "tok-xyz", "alice", hmac, expiry_ms).await;

            // Client requests SHA-512 with the tokenId as the username.
            let mut auth = ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::ScramSha512,
                exchange: SaslExchange::ScramPending,
                pending_token_expiry_ms: None,
            };
            let client = ScramClientExchange::new(
                "tok-xyz".into(),
                b"whatever".to_vec(),
                SaslMechanism::ScramSha512,
            );
            let (c1, _client) = client.client_first().unwrap();
            let resp = handle_authenticate_scram(
                &SaslAuthenticateRequest {
                    auth_bytes: bytes::Bytes::from(c1),
                    ..Default::default()
                },
                &mut auth,
                &*controller,
            );
            assert!(
                resp.error_code == SASL_AUTHENTICATION_FAILED,
                "SCRAM-SHA-512 must not consult the delegation-token table"
            );
            assert_failed_authenticate_response(&resp);
            controller.cancel().await;
        }

        /// Regular SCRAM (non-token) preserves the existing semantics:
        /// `Authenticated.authenticated_via_token = false` and
        /// `expires_at_ms = None`.
        #[tokio::test]
        async fn regular_scram_user_authentication_does_not_set_token_flag() {
            let dir = TempDir::new().unwrap();
            let controller = test_controller(dir.path().into()).await;
            // Append a regular SCRAM credential for `alice` directly via
            // metadata records. PBKDF2 is deterministic for a fixed salt.
            let salt = (0..16).collect::<Vec<u8>>();
            let cred = hash_scram_password_with_salt(
                b"alice-password",
                SaslMechanism::ScramSha256,
                4096,
                salt.clone(),
            );
            let scram_rec =
                MetadataRecord::V1ScramCredential(crabka_metadata::ScramCredentialRecord {
                    user: "alice".into(),
                    mechanism: SaslMechanism::ScramSha256,
                    salt,
                    stored_key: cred.stored_key.clone(),
                    server_key: cred.server_key.clone(),
                    iterations: cred.iterations,
                });
            controller.submit_change(vec![scram_rec]).await.unwrap();

            let (auth, resp2) = drive_scram_to_done(
                &controller,
                "alice",
                b"alice-password",
                SaslMechanism::ScramSha256,
            );
            assert!(resp2.error_code == 0);
            assert!(
                resp2.session_lifetime_ms == 0,
                "regular SCRAM has no session lifetime"
            );
            match auth {
                ConnectionAuth::Authenticated {
                    principal,
                    expires_at_ms,
                    authenticated_via_token,
                    ..
                } => {
                    let msg = "regular SCRAM is NOT a token-authed session";
                    check!(principal.name.as_str() == "alice", "{msg}");
                    check!(expires_at_ms == None, "{msg}");
                    check!(!authenticated_via_token, "{msg}");
                }
                other => panic!("expected Authenticated, got {other:?}"),
            }
            controller.cancel().await;
        }
    }
}
