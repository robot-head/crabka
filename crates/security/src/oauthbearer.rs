//! SASL/OAUTHBEARER (KIP-255 / RFC 7628) — pure logic.
//!
//! Two pieces, both I/O-free so the broker can unit-test them without a
//! socket:
//!
//! 1. [`parse_client_initial_response`] — decode the RFC 7628 client initial
//!    response (`n,,\x01auth=Bearer <token>\x01\x01`) into its bearer token
//!    and optional authzid.
//! 2. [`UnsecuredJwsValidator`] — validate the bearer token as an *unsecured*
//!    JWS (`alg: none`) and extract the connection principal from a claim.
//!    This mirrors Kafka's `OAuthBearerUnsecuredValidatorCallbackHandler`,
//!    the built-in development/testing validator. Signed-token (JWKS)
//!    validation is handled separately.

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use jsonpath_rust::parser::model::JpQuery;
use jsonpath_rust::query::js_path_process;
use serde_json::Value;

use crate::jwks::JwksHandle;
use crate::{AuthError, AuthMethod, Principal};

/// Outcome of an OAUTHBEARER validation: the authenticated principal plus the
/// token's expiry. The expiry populates
/// `SaslAuthenticateResponse.session_lifetime_ms` and what the dispatch loop
/// uses to schedule per-connection re-auth deadlines (KIP-368).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthOutcome {
    pub principal: Principal,
    /// Token expiry as Unix epoch milliseconds. `None` means "no expiry / no
    /// re-auth required" — reserved for future non-OAuth paths. For
    /// OAUTHBEARER this is always `Some` (validators reject tokens without
    /// `exp`).
    pub expires_at_ms: Option<i64>,
}

/// Parsed RFC 7628 client initial response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientInitialResponse {
    /// The bearer token (the value after `auth=Bearer `).
    pub token: String,
    /// The GS2 authorization id, if the client supplied a non-empty one.
    pub authzid: Option<String>,
}

/// Parse the SASL/OAUTHBEARER client initial response (RFC 7628 §3.1).
///
/// Wire shape (`^A` = `\x01`):
/// `gs2-header ^A auth=Bearer <token> [^A key=value ...] ^A ^A`
///
/// The GS2 header is `gs2-cb-flag "," [authzid] ","` — for OAUTHBEARER over a
/// TLS / plaintext listener the channel-binding flag is `n` (none). We accept
/// `n` / `y` and ignore everything else in the header except the authzid.
/// Non-`auth` kvpairs (host / port / extensions) are ignored.
///
/// # Errors
///
/// [`AuthError::MalformedMessage`] when the GS2 header or the `auth=Bearer`
/// kvpair is missing or malformed.
pub fn parse_client_initial_response(bytes: &[u8]) -> Result<ClientInitialResponse, AuthError> {
    let s = std::str::from_utf8(bytes).map_err(|_| AuthError::MalformedMessage)?;

    // Split off the GS2 header at the first kvsep.
    let (gs2, rest) = s.split_once('\u{1}').ok_or(AuthError::MalformedMessage)?;
    let authzid = parse_gs2_header(gs2)?;

    // The remainder is kvsep-separated kvpairs terminated by an empty pair
    // (the trailing `\x01\x01`). Find the `auth` kvpair.
    let mut token = None;
    for pair in rest.split('\u{1}') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').ok_or(AuthError::MalformedMessage)?;
        if key == "auth" {
            let t = value
                .strip_prefix("Bearer ")
                .ok_or(AuthError::MalformedMessage)?;
            token = Some(t.to_string());
        }
        // Other keys (host, port, SASL extensions) are not used here.
    }

    let token = token.ok_or(AuthError::MalformedMessage)?;
    Ok(ClientInitialResponse { token, authzid })
}

/// Parse a GS2 header `cb-flag "," [authzid] ","`, returning the authzid if
/// non-empty. The `a=` prefix RFC 5801 puts on the authzid is stripped.
fn parse_gs2_header(gs2: &str) -> Result<Option<String>, AuthError> {
    // cb-flag is one of "n", "y", or "p=<name>". OAUTHBEARER never negotiates
    // channel binding, so a "p=" flag is malformed here.
    let rest = if let Some(r) = gs2.strip_prefix("n,") {
        r
    } else if let Some(r) = gs2.strip_prefix("y,") {
        r
    } else {
        return Err(AuthError::MalformedMessage);
    };
    // `rest` is `[authzid] ","` — must end with the trailing comma.
    let authzid = rest.strip_suffix(',').ok_or(AuthError::MalformedMessage)?;
    if authzid.is_empty() {
        Ok(None)
    } else {
        // RFC 5801 prefixes the authzid with `a=`.
        Ok(Some(
            authzid.strip_prefix("a=").unwrap_or(authzid).to_string(),
        ))
    }
}

/// Validates an *unsecured* JWS bearer token (`alg: none`) and derives the
/// connection principal. Mirrors Kafka's
/// `OAuthBearerUnsecuredValidatorCallbackHandler`.
#[derive(Debug, Clone, PartialEq)]
pub struct UnsecuredJwsValidator {
    /// Claim whose string value becomes the principal name. Default `sub`.
    pub principal_claim_name: String,
    /// Tolerance, in milliseconds, applied to the `exp` / `iat` temporal
    /// checks to absorb clock drift between the client and broker.
    pub allowable_clock_skew_ms: i64,
    /// Precompiled `JsonPath` expression evaluated against the
    /// token's claim set. Token is rejected when the expression yields
    /// empty/null/false. Compile once at validator construction.
    pub custom_claim_check: Option<JpQuery>,
    /// When set, the JWT `typ` header field must equal this
    /// string. Ignored when unset.
    pub valid_token_type: Option<String>,
    /// Alternate claim name to read the principal name from
    /// when `principal_claim_name` is absent or empty. Strimzi's
    /// "service-account fallback" — `sub` typically holds a UUID,
    /// `client_id` is the readable name.
    pub fallback_user_name_claim: Option<String>,
    /// Prepended to the resolved principal name ONLY when
    /// the fallback claim fires. Strimzi convention: "service-account-".
    pub fallback_user_name_prefix: Option<String>,
    /// Precompiled `JsonPath` expression extracting group
    /// memberships from the token claims. Compile-once-at-startup.
    pub groups_claim: Option<JpQuery>,
    /// When `groups_claim` resolves to a string (not an
    /// array), split on this delimiter. Common: "," or " ".
    pub groups_claim_delimiter: Option<String>,
}

impl Default for UnsecuredJwsValidator {
    fn default() -> Self {
        Self {
            principal_claim_name: "sub".to_string(),
            allowable_clock_skew_ms: 30_000,
            custom_claim_check: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
        }
    }
}

impl UnsecuredJwsValidator {
    /// Validate `token` against `now_ms` (Unix epoch milliseconds), returning
    /// the authenticated [`Principal`] on success.
    ///
    /// # Errors
    ///
    /// [`AuthError::InvalidToken`] for any structural, signature, temporal,
    /// scope, or principal-claim failure. The caller maps this onto the RFC
    /// 7628 `invalid_token` server error status.
    pub fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
        // JWS compact serialization: header.payload.signature. For `alg:none`
        // the signature segment is empty.
        let mut segs = token.split('.');
        let header_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
        let payload_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
        let sig = segs.next().ok_or(AuthError::InvalidToken)?;
        if segs.next().is_some() {
            return Err(AuthError::InvalidToken);
        }
        if !sig.is_empty() {
            // Signed token — needs JWKS signature verification.
            return Err(AuthError::InvalidToken);
        }

        let header: Value = decode_json_segment(header_b64)?;
        if header.get("alg").and_then(Value::as_str) != Some("none") {
            return Err(AuthError::InvalidToken);
        }
        // Optional JWT `typ` header check (JWT-mode validator only).
        if let Some(expected_typ) = &self.valid_token_type
            && header.get("typ").and_then(Value::as_str) != Some(expected_typ.as_str())
        {
            return Err(AuthError::InvalidToken);
        }

        let claims: Value = decode_json_segment(payload_b64)?;

        // `exp` is required and must be in the future (within skew).
        let exp_ms = numeric_date_ms(&claims, "exp").ok_or(AuthError::InvalidToken)?;
        if exp_ms + self.allowable_clock_skew_ms <= now_ms {
            return Err(AuthError::InvalidToken);
        }
        // `iat` is optional; if present it must not be in the future (within skew).
        if let Some(iat_ms) = numeric_date_ms(&claims, "iat")
            && iat_ms - self.allowable_clock_skew_ms > now_ms
        {
            return Err(AuthError::InvalidToken);
        }

        // Optional JsonPath custom_claim_check.
        if let Some(path) = &self.custom_claim_check
            && !evaluate_custom_claim_check(path, &claims)
        {
            return Err(AuthError::InvalidToken);
        }

        // Primary → fallback → reject. Prefix applied only
        // when fallback fires.
        let (raw_name, used_fallback) = if let Some(n) = claims
            .get(&self.principal_claim_name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            (n.to_string(), false)
        } else {
            let fallback_claim = self
                .fallback_user_name_claim
                .as_deref()
                .ok_or(AuthError::InvalidToken)?;
            let raw = claims
                .get(fallback_claim)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or(AuthError::InvalidToken)?;
            (raw.to_string(), true)
        };
        let name = if used_fallback {
            match &self.fallback_user_name_prefix {
                Some(prefix) => format!("{prefix}{raw_name}"),
                None => raw_name,
            }
        } else {
            raw_name
        };

        // Groups extraction.
        let groups = match &self.groups_claim {
            Some(path) => extract_groups(path, &claims, self.groups_claim_delimiter.as_deref()),
            None => Vec::new(),
        };

        Ok(AuthOutcome {
            principal: Principal {
                name,
                auth_method: AuthMethod::SaslOAuthBearer,
                groups,
            },
            expires_at_ms: Some(exp_ms),
        })
    }
}

/// Extract group memberships from token claims using a
/// precompiled `JsonPath`. Each result element is interpreted per its
/// JSON type:
/// - `String`: if `delimiter` is set, split + trim + drop empty;
///   otherwise the whole string becomes one group.
/// - `Array`: each string element becomes a group.
/// - `Number` / `Object` / `Null`: ignored (no error).
///
/// Returns `vec![]` for empty matches (no groups extracted is not an
/// error — the token may legitimately have no groups).
fn extract_groups(path: &JpQuery, claims: &Value, delimiter: Option<&str>) -> Vec<String> {
    let Ok(refs) = js_path_process(path, claims) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for r in refs {
        match r.val() {
            Value::String(s) => match delimiter {
                Some(d) => out.extend(
                    s.split(d)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(String::from),
                ),
                None => out.push(s.clone()),
            },
            Value::Array(items) => {
                out.extend(items.iter().filter_map(Value::as_str).map(String::from));
            }
            _ => {} // ignore numbers, objects, nulls
        }
    }
    out
}

/// Evaluate a precompiled `JsonPath` expression against the token claims.
/// Returns true when the result is truthy (non-empty, with no element being
/// null or false); false otherwise. Matches Strimzi's "expression yields
/// truthy" semantics. Errors during evaluation count as falsy (rejection).
fn evaluate_custom_claim_check(path: &JpQuery, claims: &Value) -> bool {
    let Ok(refs) = js_path_process(path, claims) else {
        return false;
    };
    if refs.is_empty() {
        return false;
    }
    for r in refs {
        match r.val() {
            Value::Null | Value::Bool(false) => return false,
            _ => {}
        }
    }
    true
}

/// Whether the JWT `aud` claim contains `expected`. `aud` is a single string
/// or an array of strings (RFC 7519 §4.1.3).
fn audience_contains(claims: &Value, expected: &str) -> bool {
    match claims.get("aud") {
        Some(Value::String(s)) => s == expected,
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .any(|a| a == expected),
        _ => false,
    }
}

/// base64url-decode a JWS segment and parse it as JSON.
fn decode_json_segment(seg: &str) -> Result<Value, AuthError> {
    let bytes = B64URL.decode(seg).map_err(|_| AuthError::InvalidToken)?;
    serde_json::from_slice(&bytes).map_err(|_| AuthError::InvalidToken)
}

/// Read a JWT `NumericDate` claim (seconds since the epoch, possibly
/// fractional) and convert it to integer milliseconds.
fn numeric_date_ms(claims: &Value, key: &str) -> Option<i64> {
    let v = claims.get(key)?;
    // The common case: an integer second count. Avoids any float rounding.
    if let Some(secs) = v.as_i64() {
        return secs.checked_mul(1000);
    }
    // Fractional NumericDate (rare): truncate to whole milliseconds.
    let ms = v.as_f64()? * 1000.0;
    if ms.is_finite() {
        #[allow(clippy::cast_possible_truncation)]
        Some(ms as i64)
    } else {
        None
    }
}

/// The RFC 7628 server error response body for a rejected token. The JVM
/// `OAuthBearerSaslClient` treats any non-empty first server message as an
/// error and replies with a single `\x01` kvsep, after which the broker
/// completes the failure handshake.
#[must_use]
pub fn invalid_token_json() -> String {
    "{\"status\":\"invalid_token\"}".to_string()
}

/// Validates a *signed* JWS bearer token (`RS256` / `ES256`) against a JWKS
/// key set fetched from the identity provider, then checks the standard JWT
/// claims and derives the connection principal.
///
/// The key set lives behind a [`JwksHandle`] so the broker's background
/// refresher can rotate keys without restarting the broker or taking a lock;
/// each [`validate`](Self::validate) reads the current set.
#[derive(Debug, Clone)]
pub struct SignedJwsValidator {
    /// Claim whose string value becomes the principal name. Default `sub`.
    pub principal_claim_name: String,
    /// Tolerance, in milliseconds, applied to `exp` / `iat` / `nbf`.
    pub allowable_clock_skew_ms: i64,
    /// When set, the token `iss` claim must equal this exactly.
    pub valid_issuer: Option<String>,
    /// When set, the token `aud` claim must contain this value.
    pub expected_audience: Option<String>,
    /// Precompiled `JsonPath` `custom_claim_check`. See
    /// [`UnsecuredJwsValidator`] for semantics.
    pub custom_claim_check: Option<JpQuery>,
    /// JWT `typ` header check. Ignored when unset.
    pub valid_token_type: Option<String>,
    /// Alternate principal claim. See [`UnsecuredJwsValidator`].
    pub fallback_user_name_claim: Option<String>,
    /// Prepended to the principal name only on fallback.
    pub fallback_user_name_prefix: Option<String>,
    /// Precompiled `JsonPath` extracting group memberships.
    pub groups_claim: Option<JpQuery>,
    /// Delimiter when `groups_claim` resolves to a string.
    pub groups_claim_delimiter: Option<String>,
    /// Hard cache-expiry threshold, in milliseconds. When set,
    /// the validator rejects tokens if the paired refresher has not had a
    /// successful fetch within this window (using
    /// [`JwksHandle::last_successful_fetch_ms`]). `None` = no expiry check.
    /// Fails closed on prolonged `IdP` outage so a
    /// rotated-out key can't keep signing valid tokens indefinitely.
    pub expiry_ms: Option<i64>,
    /// The live JWKS, swapped in by the broker's refresher.
    keys: JwksHandle,
}

impl SignedJwsValidator {
    /// A validator backed by `keys`, with the same claim/skew defaults as the
    /// unsecured validator and no issuer / audience constraint.
    #[must_use]
    pub fn new(keys: JwksHandle) -> Self {
        Self {
            principal_claim_name: "sub".to_string(),
            allowable_clock_skew_ms: 30_000,
            valid_issuer: None,
            expected_audience: None,
            custom_claim_check: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            expiry_ms: None,
            keys,
        }
    }

    /// The shared key-set handle, so the broker can hand the same cell to its
    /// JWKS refresher task.
    #[must_use]
    pub fn key_handle(&self) -> JwksHandle {
        self.keys.clone()
    }

    /// Validate a signed bearer token against `now_ms` (Unix epoch ms).
    ///
    /// # Errors
    ///
    /// [`AuthError::InvalidToken`] for any structural, signature, temporal,
    /// issuer, audience, scope, or principal-claim failure.
    pub fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
        // JWS compact serialization: header.payload.signature, all non-empty.
        let mut segs = token.split('.');
        let header_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
        let payload_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
        let sig_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
        if segs.next().is_some() || sig_b64.is_empty() {
            return Err(AuthError::InvalidToken);
        }

        let header: Value = decode_json_segment(header_b64)?;
        let alg = header
            .get("alg")
            .and_then(Value::as_str)
            .ok_or(AuthError::InvalidToken)?;
        if alg != "RS256" && alg != "ES256" {
            return Err(AuthError::InvalidToken);
        }
        // Optional JWT `typ` check (JWT-mode validator only).
        if let Some(expected_typ) = &self.valid_token_type
            && header.get("typ").and_then(Value::as_str) != Some(expected_typ.as_str())
        {
            return Err(AuthError::InvalidToken);
        }

        // Hard cache-expiry. If the last successful refresh is
        // older than `expiry_ms`, reject all tokens until the refresher
        // succeeds again. The `last_fetch > 0` guard skips this on a
        // never-fetched handle so the "broker is still starting
        // up" path stays open (the verify-level check below will reject
        // anyway because the key set is empty).
        if let Some(expiry_ms) = self.expiry_ms {
            let last_fetch = self.keys.last_successful_fetch_ms();
            if last_fetch > 0 && now_ms.saturating_sub(last_fetch) > expiry_ms {
                tracing::debug!(
                    last_fetch_ms = last_fetch,
                    now_ms,
                    expiry_ms,
                    "JWKS cache expired; rejecting token until next successful refresh",
                );
                return Err(AuthError::InvalidToken);
            }
        }

        let kid = header.get("kid").and_then(Value::as_str);

        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig = B64URL
            .decode(sig_b64)
            .map_err(|_| AuthError::InvalidToken)?;
        // On any verify failure (unknown kid or bad signature)
        // signal the refresher to attempt an on-demand JWKS fetch — the
        // signing key may have rotated since the last periodic refresh.
        // The current token still rejects; a subsequent reconnect will
        // see the rotated keys. The refresher's `min_on_demand_pause`
        // caps the signal-storm cost.
        if let Err(e) = self
            .keys
            .load()
            .verify(kid, alg, signing_input.as_bytes(), &sig)
        {
            self.keys.signal_refresh();
            return Err(e);
        }

        let claims: Value = decode_json_segment(payload_b64)?;
        self.check_claims(&claims, now_ms)
    }

    /// Apply the claim policy (temporal, issuer, audience, scope, principal) to
    /// already-signature-verified `claims`. Split out so the policy is
    /// unit-testable without minting signed tokens.
    fn check_claims(&self, claims: &Value, now_ms: i64) -> Result<AuthOutcome, AuthError> {
        // `exp` required and in the future (within skew).
        let exp_ms = numeric_date_ms(claims, "exp").ok_or(AuthError::InvalidToken)?;
        if exp_ms + self.allowable_clock_skew_ms <= now_ms {
            return Err(AuthError::InvalidToken);
        }
        // `iat` optional: must not be in the future (within skew).
        if let Some(iat_ms) = numeric_date_ms(claims, "iat")
            && iat_ms - self.allowable_clock_skew_ms > now_ms
        {
            return Err(AuthError::InvalidToken);
        }
        // `nbf` optional: token not valid before it (within skew).
        if let Some(nbf_ms) = numeric_date_ms(claims, "nbf")
            && nbf_ms - self.allowable_clock_skew_ms > now_ms
        {
            return Err(AuthError::InvalidToken);
        }

        if let Some(expected) = &self.valid_issuer
            && claims.get("iss").and_then(Value::as_str) != Some(expected.as_str())
        {
            return Err(AuthError::InvalidToken);
        }

        if let Some(expected) = &self.expected_audience
            && !audience_contains(claims, expected)
        {
            return Err(AuthError::InvalidToken);
        }

        // Optional JsonPath custom_claim_check.
        if let Some(path) = &self.custom_claim_check
            && !evaluate_custom_claim_check(path, claims)
        {
            return Err(AuthError::InvalidToken);
        }

        // Primary → fallback → reject. Prefix on fallback only.
        let (raw_name, used_fallback) = if let Some(n) = claims
            .get(&self.principal_claim_name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            (n.to_string(), false)
        } else {
            let fallback_claim = self
                .fallback_user_name_claim
                .as_deref()
                .ok_or(AuthError::InvalidToken)?;
            let raw = claims
                .get(fallback_claim)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or(AuthError::InvalidToken)?;
            (raw.to_string(), true)
        };
        let name = if used_fallback {
            match &self.fallback_user_name_prefix {
                Some(prefix) => format!("{prefix}{raw_name}"),
                None => raw_name,
            }
        } else {
            raw_name
        };

        let groups = match &self.groups_claim {
            Some(path) => extract_groups(path, claims, self.groups_claim_delimiter.as_deref()),
            None => Vec::new(),
        };

        Ok(AuthOutcome {
            principal: Principal {
                name,
                auth_method: AuthMethod::SaslOAuthBearer,
                groups,
            },
            expires_at_ms: Some(exp_ms),
        })
    }
}

/// The broker's configured OAUTHBEARER token validator: the
/// development-only unsecured-JWS path, production signed-JWT
/// validation against a JWKS endpoint, or RFC 7662 opaque-token
/// introspection. Defaults to unsecured.
#[derive(Debug, Clone)]
pub enum OAuthBearerValidator {
    /// Unsecured JWS (`alg:none`) — development / testing only.
    Unsecured(UnsecuredJwsValidator),
    /// Signed JWS verified against a JWKS key set.
    Signed(SignedJwsValidator),
    /// RFC 7662 opaque-token introspection.
    Introspection(IntrospectionValidator),
}

impl Default for OAuthBearerValidator {
    fn default() -> Self {
        Self::Unsecured(UnsecuredJwsValidator::default())
    }
}

impl OAuthBearerValidator {
    /// Validate `token` against `now_ms`, dispatching to the configured path.
    ///
    /// # Errors
    ///
    /// - [`AuthError::InvalidToken`] when the token fails validation.
    /// - [`AuthError::IntrospectionTransport`] when the introspection variant's
    ///   HTTP call fails at the transport layer.
    pub async fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
        match self {
            Self::Unsecured(v) => v.validate(token, now_ms),
            Self::Signed(v) => v.validate(token, now_ms),
            Self::Introspection(v) => v.validate(token, now_ms).await,
        }
    }

    /// The JWKS handle when this is a signed validator, so the broker can wire
    /// a refresher to the same key cell. `None` for the unsecured + introspection paths.
    #[must_use]
    pub fn jwks_handle(&self) -> Option<JwksHandle> {
        match self {
            Self::Unsecured(_) | Self::Introspection(_) => None,
            Self::Signed(v) => Some(v.key_handle()),
        }
    }
}

/// HTTP transport contract for RFC 7662 introspection + OIDC userinfo.
/// Lives in this crate to keep `crates/security` as the validator surface;
/// the concrete reqwest-backed impl lives in `crates/broker`
/// (`oauth_introspection.rs`) so this crate stays I/O-free.
#[async_trait::async_trait]
pub trait IntrospectionClient: Send + Sync + std::fmt::Debug {
    /// POST the `IdP`'s introspection endpoint with `token` in a
    /// form-encoded body. Caller checks `active` + claims.
    async fn introspect(&self, token: &str) -> Result<serde_json::Value, IntrospectionError>;

    /// GET the `IdP`'s userinfo endpoint with `Authorization: Bearer
    /// <token>`. `Ok(None)` when the validator is configured without
    /// userinfo enrichment.
    async fn userinfo(&self, token: &str) -> Result<Option<serde_json::Value>, IntrospectionError>;
}

/// Transport-layer failures surfaced by [`IntrospectionClient`]. The
/// validator maps these onto [`AuthError::IntrospectionTransport`] for
/// the SASL handler.
#[derive(Debug, thiserror::Error)]
pub enum IntrospectionError {
    #[error("transport: {0}")]
    Transport(String),
    #[error("non-2xx response: {0}")]
    Status(u16),
    #[error("invalid JSON body")]
    Parse,
}

/// RFC 7662 opaque-token introspection validator. Calls the
/// introspection endpoint per token (no caching — RFC 7662 §4 discourages
/// caching without explicit lifetime info; SASL is once per connection so
/// the cost is acceptable). Optionally calls OIDC userinfo after a
/// successful introspection and merges the profile claims over the
/// introspection claims.
#[derive(Debug, Clone)]
pub struct IntrospectionValidator {
    pub client: Arc<dyn IntrospectionClient>,
    /// Claim whose string value becomes the principal name. Default `sub`
    /// for generic OAuth flows; commonly `client_id` for Keycloak
    /// client-credentials.
    pub principal_claim_name: String,
    /// Precompiled `JsonPath` `custom_claim_check`. See
    /// [`UnsecuredJwsValidator`] for semantics. Introspection has no JWT
    /// header, so there is no `valid_token_type` field here.
    pub custom_claim_check: Option<JpQuery>,
    /// `true` iff a `userinfo_endpoint_uri` is configured; the validator
    /// calls `client.userinfo(token)` after a successful introspection and
    /// merges the response over the introspection claims.
    pub call_userinfo: bool,
    /// Clock-skew tolerance for `exp`/`iat`/`nbf` checks on
    /// introspection-response timestamps (when present).
    pub allowable_clock_skew_ms: i64,
    /// When set, the introspection-response `aud` claim (RFC 7662 §2.2) must
    /// contain this value. Defaults to `None` (no audience restriction).
    /// Prevents a token minted for another resource server of the same `IdP`,
    /// which still introspects as `active: true`, from authenticating here.
    pub expected_audience: Option<String>,
    /// Alternate principal claim. See [`UnsecuredJwsValidator`].
    pub fallback_user_name_claim: Option<String>,
    /// Prepended to the principal name only on fallback.
    pub fallback_user_name_prefix: Option<String>,
    /// Precompiled `JsonPath` extracting group memberships,
    /// evaluated against the merged claims (introspection + optional
    /// userinfo).
    pub groups_claim: Option<JpQuery>,
    /// Delimiter when `groups_claim` resolves to a string.
    pub groups_claim_delimiter: Option<String>,
}

impl IntrospectionValidator {
    /// Validate a bearer token via RFC 7662 introspection + optional
    /// userinfo enrichment.
    ///
    /// # Errors
    ///
    /// - [`AuthError::IntrospectionTransport`] on HTTP transport / parse failures.
    /// - [`AuthError::InvalidToken`] on `active != true`, missing `exp`,
    ///   missing principal claim, scope mismatch, or temporal-claim failure.
    ///   `exp` is required so the SASL handler can populate
    ///   `session_lifetime_ms` for KIP-368 re-authentication.
    pub async fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
        let mut claims = self
            .client
            .introspect(token)
            .await
            .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?;
        if claims.get("active").and_then(Value::as_bool) != Some(true) {
            return Err(AuthError::InvalidToken);
        }
        // Audience restriction. When configured, the introspection-response
        // `aud` claim (RFC 7662 §2.2) must contain the expected value, mirroring
        // the signed-JWS path. Guards against an `IdP` that serves multiple
        // resource servers handing out a token that introspects as active here
        // but was minted for a different audience.
        if let Some(expected) = &self.expected_audience
            && !audience_contains(&claims, expected)
        {
            return Err(AuthError::InvalidToken);
        }
        check_temporal_claims(&claims, now_ms, self.allowable_clock_skew_ms)?;
        // Capture `exp_ms` from the introspection response BEFORE any userinfo
        // merge. Introspection's `exp` is the authoritative session expiry
        // (RFC 7662); userinfo typically doesn't carry `exp`, and the
        // `merge_userinfo_over_introspection` precedence already reserves
        // `exp` to introspection, but pulling it out here makes the
        // ordering explicit. Required for OAUTHBEARER (validators reject
        // tokens without `exp`).
        let exp_ms = numeric_date_ms(&claims, "exp").ok_or(AuthError::InvalidToken)?;
        if self.call_userinfo
            && let Some(ui) = self
                .client
                .userinfo(token)
                .await
                .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?
        {
            merge_userinfo_over_introspection(&mut claims, ui);
        }
        // Optional JsonPath custom_claim_check. Evaluated against the merged
        // claims (introspection plus optional userinfo).
        if let Some(path) = &self.custom_claim_check
            && !evaluate_custom_claim_check(path, &claims)
        {
            return Err(AuthError::InvalidToken);
        }
        // Primary → fallback → reject. Prefix on fallback only.
        let (raw_name, used_fallback) = if let Some(n) = claims
            .get(&self.principal_claim_name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            (n.to_string(), false)
        } else {
            let fallback_claim = self
                .fallback_user_name_claim
                .as_deref()
                .ok_or(AuthError::InvalidToken)?;
            let raw = claims
                .get(fallback_claim)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or(AuthError::InvalidToken)?;
            (raw.to_string(), true)
        };
        let name = if used_fallback {
            match &self.fallback_user_name_prefix {
                Some(prefix) => format!("{prefix}{raw_name}"),
                None => raw_name,
            }
        } else {
            raw_name
        };

        let groups = match &self.groups_claim {
            Some(path) => extract_groups(path, &claims, self.groups_claim_delimiter.as_deref()),
            None => Vec::new(),
        };

        Ok(AuthOutcome {
            principal: Principal {
                name,
                auth_method: AuthMethod::SaslOAuthBearer,
                groups,
            },
            expires_at_ms: Some(exp_ms),
        })
    }
}

/// Skew-tolerant temporal-claims check for introspection responses.
/// RFC 7662 doesn't mandate exp/iat/nbf, but honor them when present.
fn check_temporal_claims(claims: &Value, now_ms: i64, skew_ms: i64) -> Result<(), AuthError> {
    if let Some(exp_s) = claims.get("exp").and_then(Value::as_i64) {
        let exp_ms = exp_s.saturating_mul(1000);
        if now_ms.saturating_sub(skew_ms) > exp_ms {
            return Err(AuthError::InvalidToken);
        }
    }
    if let Some(iat_s) = claims.get("iat").and_then(Value::as_i64) {
        let iat_ms = iat_s.saturating_mul(1000);
        if iat_ms.saturating_sub(skew_ms) > now_ms {
            return Err(AuthError::InvalidToken);
        }
    }
    if let Some(nbf_s) = claims.get("nbf").and_then(Value::as_i64) {
        let nbf_ms = nbf_s.saturating_mul(1000);
        if nbf_ms.saturating_sub(skew_ms) > now_ms {
            return Err(AuthError::InvalidToken);
        }
    }
    Ok(())
}

/// Merge userinfo response over introspection claims. Userinfo wins for
/// profile-style claims (`preferred_username`, email, name, `given_name`,
/// `family_name`, ...); introspection wins for the small set of
/// authorization claims listed in `RESERVED`.
fn merge_userinfo_over_introspection(introspection: &mut Value, userinfo: Value) {
    const RESERVED: &[&str] = &["active", "exp", "iat", "nbf", "scope", "client_id", "sub"];
    let (Some(obj), Value::Object(ui_map)) = (introspection.as_object_mut(), userinfo) else {
        return;
    };
    for (k, v) in ui_map {
        if !RESERVED.contains(&k.as_str()) {
            obj.insert(k, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use jsonpath_rust::parser::parse_json_path;

    fn jws(header: &str, claims: &str) -> String {
        format!(
            "{}.{}.",
            B64URL.encode(header.as_bytes()),
            B64URL.encode(claims.as_bytes())
        )
    }

    fn unsecured(sub: &str, iat_s: i64, exp_s: i64) -> String {
        jws(
            "{\"alg\":\"none\"}",
            &format!("{{\"sub\":\"{sub}\",\"iat\":{iat_s},\"exp\":{exp_s}}}"),
        )
    }

    /// Build an unsecured-JWS from an explicit header + claim object (so
    /// callers can drive the `typ` header for `typ`-check tests). The
    /// signature segment is left empty per `alg:none`.
    fn make_unsecured_jws_with_header(
        header: &serde_json::Value,
        claims: &serde_json::Value,
    ) -> String {
        format!(
            "{}.{}.",
            B64URL.encode(serde_json::to_vec(header).unwrap()),
            B64URL.encode(serde_json::to_vec(claims).unwrap()),
        )
    }

    fn make_unsecured_jws(claims: &serde_json::Value) -> String {
        make_unsecured_jws_with_header(&serde_json::json!({"alg": "none"}), claims)
    }

    fn parse_jp(expr: &str) -> JpQuery {
        parse_json_path(expr).expect("expression compiles")
    }

    fn client_resp(token: &str) -> Vec<u8> {
        format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes()
    }

    #[test]
    fn parse_happy_path_empty_authzid() {
        let r = parse_client_initial_response(&client_resp("tok.en.")).unwrap();
        assert!(r.token == "tok.en.");
        assert!(r.authzid == None);
    }

    #[test]
    fn parse_extracts_authzid_and_ignores_extra_kvpairs() {
        let bytes =
            b"n,a=alice,\x01host=example.com\x01auth=Bearer abc\x01port=443\x01\x01".to_vec();
        let r = parse_client_initial_response(&bytes).unwrap();
        assert!(r.token == "abc");
        assert!(r.authzid == Some("alice".to_string()));
    }

    #[test]
    fn parse_rejects_missing_auth_kvpair() {
        let bytes = b"n,,\x01host=example.com\x01\x01".to_vec();
        assert!(parse_client_initial_response(&bytes) == Err(AuthError::MalformedMessage));
    }

    #[test]
    fn parse_rejects_missing_bearer_prefix() {
        let bytes = b"n,,\x01auth=Basic abc\x01\x01".to_vec();
        assert!(parse_client_initial_response(&bytes) == Err(AuthError::MalformedMessage));
    }

    #[test]
    fn parse_rejects_bad_gs2_header() {
        let bytes = b"z,,\x01auth=Bearer abc\x01\x01".to_vec();
        assert!(parse_client_initial_response(&bytes) == Err(AuthError::MalformedMessage));
    }

    #[test]
    fn validate_accepts_fresh_unsecured_token() {
        let v = UnsecuredJwsValidator::default();
        let now = 1_000_000_000_000;
        let token = unsecured("admin", 999_999_000, 1_000_000_900); // seconds
        let outcome = v.validate(&token, now).unwrap();
        assert!(outcome.principal.name == "admin");
        assert!(outcome.principal.auth_method == AuthMethod::SaslOAuthBearer);
    }

    #[test]
    fn unsecured_validate_surfaces_exp_in_auth_outcome() {
        // exp = 2000 sec = 2_000_000 ms; now = 1_000_000 ms.
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = unsecured("alice", 999, exp_secs);
        let v = UnsecuredJwsValidator::default();
        let outcome = v.validate(&token, now_ms).expect("token valid");
        assert!(outcome.principal.name == "alice");
        assert!(outcome.expires_at_ms == Some(exp_secs * 1000));
    }

    #[test]
    fn validate_rejects_expired_token() {
        let v = UnsecuredJwsValidator {
            allowable_clock_skew_ms: 0,
            ..Default::default()
        };
        let now = 2_000_000_000_000;
        let token = unsecured("admin", 1_000_000_000, 1_000_000_100);
        assert!(v.validate(&token, now) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn validate_rejects_future_iat() {
        let v = UnsecuredJwsValidator {
            allowable_clock_skew_ms: 0,
            ..Default::default()
        };
        let now = 1_000_000_000_000;
        // iat far in the future, exp even further.
        let token = unsecured("admin", 5_000_000_000, 5_000_000_100);
        assert!(v.validate(&token, now) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn validate_accepts_exp_and_iat_at_skew_boundaries() {
        let v = UnsecuredJwsValidator {
            allowable_clock_skew_ms: 1_000,
            ..Default::default()
        };

        let exp_boundary = make_unsecured_jws(&serde_json::json!({
            "sub": "admin",
            "exp": 1_999,
        }));
        assert!(v.validate(&exp_boundary, 2_000_000).is_err());
        let exp_inside = make_unsecured_jws(&serde_json::json!({
            "sub": "admin",
            "exp": 2_000,
        }));
        assert!(v.validate(&exp_inside, 2_000_000).is_ok());

        let iat_boundary = make_unsecured_jws(&serde_json::json!({
            "sub": "admin",
            "iat": 2_001,
            "exp": 3_000,
        }));
        assert!(v.validate(&iat_boundary, 2_000_000).is_ok());
        let iat_outside = make_unsecured_jws(&serde_json::json!({
            "sub": "admin",
            "iat": 2_002,
            "exp": 3_000,
        }));
        assert!(v.validate(&iat_outside, 2_000_000).is_err());
    }

    #[test]
    fn validate_rejects_signed_token() {
        let v = UnsecuredJwsValidator::default();
        let now = 1_000_000_000_000;
        // alg RS256 + a non-empty signature segment.
        let token = format!(
            "{}.{}.{}",
            B64URL.encode(b"{\"alg\":\"RS256\"}"),
            B64URL.encode(b"{\"sub\":\"admin\",\"exp\":1000000900}"),
            B64URL.encode(b"sig")
        );
        assert!(v.validate(&token, now) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn validate_rejects_missing_exp() {
        let v = UnsecuredJwsValidator::default();
        let token = jws("{\"alg\":\"none\"}", "{\"sub\":\"admin\"}");
        assert!(v.validate(&token, 1_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn validate_rejects_missing_principal_claim() {
        let v = UnsecuredJwsValidator::default();
        let token = jws("{\"alg\":\"none\"}", "{\"exp\":5000000000}");
        assert!(v.validate(&token, 1_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    // ---- custom_claim_check (JsonPath) + valid_token_type ---

    #[test]
    fn unsecured_validate_rejects_when_custom_claim_check_fails() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws(&serde_json::json!({
            "sub": "alice",
            "exp": exp_secs,
            "scope": ["kafka.read"],
        }));
        let v = UnsecuredJwsValidator {
            custom_claim_check: Some(parse_jp("$.scope[?@ == 'kafka.admin']")),
            ..Default::default()
        };
        let result = v.validate(&token, now_ms);
        assert!(result.unwrap_err() == AuthError::InvalidToken);
    }

    #[test]
    fn custom_claim_check_rejects_false_and_null_matches() {
        assert!(!evaluate_custom_claim_check(
            &parse_jp("$.enabled"),
            &serde_json::json!({"enabled": false}),
        ));
        assert!(!evaluate_custom_claim_check(
            &parse_jp("$.enabled"),
            &serde_json::json!({"enabled": null}),
        ));
        assert!(evaluate_custom_claim_check(
            &parse_jp("$.enabled"),
            &serde_json::json!({"enabled": true}),
        ));
    }

    #[test]
    fn unsecured_validate_accepts_when_custom_claim_check_passes() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws(&serde_json::json!({
            "sub": "alice",
            "exp": exp_secs,
            "scope": ["kafka.admin", "kafka.read"],
        }));
        let v = UnsecuredJwsValidator {
            custom_claim_check: Some(parse_jp("$.scope[?@ == 'kafka.admin']")),
            ..Default::default()
        };
        let outcome = v.validate(&token, now_ms).expect("valid token");
        assert!(outcome.principal.name == "alice");
    }

    #[test]
    fn unsecured_validate_rejects_when_valid_token_type_mismatch() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        // Token with typ=OPAQUE in the header.
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "OPAQUE"}),
            &serde_json::json!({"sub": "alice", "exp": exp_secs}),
        );
        let v = UnsecuredJwsValidator {
            valid_token_type: Some("JWT".into()),
            ..Default::default()
        };
        let result = v.validate(&token, now_ms);
        assert!(result.unwrap_err() == AuthError::InvalidToken);
    }

    #[test]
    fn unsecured_validate_accepts_when_valid_token_type_match() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            &serde_json::json!({"sub": "alice", "exp": exp_secs}),
        );
        let v = UnsecuredJwsValidator {
            valid_token_type: Some("JWT".into()),
            ..Default::default()
        };
        assert!(v.validate(&token, now_ms).is_ok());
    }

    #[test]
    fn unsecured_validate_accepts_when_valid_token_type_unset_regardless_of_header() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "OPAQUE"}),
            &serde_json::json!({"sub": "alice", "exp": exp_secs}),
        );
        let v = UnsecuredJwsValidator::default();
        // No valid_token_type set → header `typ` ignored.
        assert!(v.validate(&token, now_ms).is_ok());
    }

    // ---- name fallback chain + groups extraction --------------

    #[test]
    fn unsecured_validate_uses_primary_principal_claim_when_present() {
        // Regression: primary claim present → use primary, no prefix.
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            &serde_json::json!({"sub": "alice", "exp": exp_secs}),
        );
        let v = UnsecuredJwsValidator {
            fallback_user_name_claim: Some("client_id".into()),
            fallback_user_name_prefix: Some("service-account-".into()),
            ..Default::default()
        };
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(outcome.principal.name == "alice"); // primary, no prefix
    }

    #[test]
    fn unsecured_validate_falls_back_to_alt_claim_when_primary_absent() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            // No `sub` — primary lookup fails.
            &serde_json::json!({"client_id": "svc1", "exp": exp_secs}),
        );
        let v = UnsecuredJwsValidator {
            fallback_user_name_claim: Some("client_id".into()),
            ..Default::default()
        };
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(outcome.principal.name == "svc1"); // fallback, no prefix
    }

    #[test]
    fn unsecured_validate_applies_fallback_prefix_only_on_fallback() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            &serde_json::json!({"client_id": "svc1", "exp": exp_secs}),
        );
        let v = UnsecuredJwsValidator {
            fallback_user_name_claim: Some("client_id".into()),
            fallback_user_name_prefix: Some("service-account-".into()),
            ..Default::default()
        };
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(outcome.principal.name == "service-account-svc1");
    }

    #[test]
    fn unsecured_validate_rejects_when_neither_primary_nor_fallback_present() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            // Neither sub nor client_id.
            &serde_json::json!({"exp": exp_secs}),
        );
        let v = UnsecuredJwsValidator {
            fallback_user_name_claim: Some("client_id".into()),
            ..Default::default()
        };
        assert!(v.validate(&token, now_ms) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn unsecured_validate_extracts_groups_from_array_claim() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            &serde_json::json!({
                "sub": "alice",
                "exp": exp_secs,
                "groups": ["admin", "ops"],
            }),
        );
        let v = UnsecuredJwsValidator {
            groups_claim: Some(parse_jp("$.groups")),
            ..Default::default()
        };
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(outcome.principal.groups == vec!["admin".to_string(), "ops".to_string()]);
    }

    #[test]
    fn unsecured_validate_extracts_groups_from_delimited_string() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            &serde_json::json!({
                "sub": "alice",
                "exp": exp_secs,
                "groups": "admin,ops, kafka",
            }),
        );
        let v = UnsecuredJwsValidator {
            groups_claim: Some(parse_jp("$.groups")),
            groups_claim_delimiter: Some(",".into()),
            ..Default::default()
        };
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(
            outcome.principal.groups
                == vec!["admin".to_string(), "ops".to_string(), "kafka".to_string()]
        );
    }

    #[test]
    fn unsecured_validate_extracts_groups_from_nested_claim_via_jsonpath() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            &serde_json::json!({
                "sub": "alice",
                "exp": exp_secs,
                "realm_access": { "roles": ["admin", "ops"] },
            }),
        );
        let v = UnsecuredJwsValidator {
            groups_claim: Some(parse_jp("$.realm_access.roles[*]")),
            ..Default::default()
        };
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(outcome.principal.groups == vec!["admin".to_string(), "ops".to_string()]);
    }

    #[test]
    fn unsecured_validate_returns_empty_groups_when_claim_unset() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            &serde_json::json!({
                "sub": "alice",
                "exp": exp_secs,
                "groups": ["admin"],
            }),
        );
        let v = UnsecuredJwsValidator::default(); // no groups_claim
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(outcome.principal.groups == Vec::<String>::new());
    }

    #[test]
    fn unsecured_validate_returns_empty_groups_when_claim_resolves_to_empty() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let token = make_unsecured_jws_with_header(
            &serde_json::json!({"alg": "none", "typ": "JWT"}),
            &serde_json::json!({
                "sub": "alice",
                "exp": exp_secs,
            }),
        );
        let v = UnsecuredJwsValidator {
            groups_claim: Some(parse_jp("$.nonexistent")),
            ..Default::default()
        };
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(outcome.principal.groups == Vec::<String>::new());
    }

    #[test]
    fn validate_custom_principal_claim() {
        let v = UnsecuredJwsValidator {
            principal_claim_name: "client_id".to_string(),
            ..Default::default()
        };
        let token = jws(
            "{\"alg\":\"none\"}",
            "{\"client_id\":\"svc-1\",\"exp\":5000000000}",
        );
        let outcome = v.validate(&token, 1_000_000_000_000).unwrap();
        assert!(outcome.principal.name == "svc-1");
    }

    #[test]
    fn invalid_token_json_is_rfc7628_shape() {
        assert!(invalid_token_json() == "{\"status\":\"invalid_token\"}");
    }

    // ---- SignedJwsValidator -------------------------------------

    use crate::jwks::{Jwks, JwksHandle, mint_es256, mint_rs256, mint_rs256_with_header};

    /// Build a `SignedJwsValidator` whose key set is populated from `jwks_json`.
    fn signed(jwks_json: &str) -> (SignedJwsValidator, JwksHandle) {
        let handle = JwksHandle::new(Jwks::from_json(jwks_json, false).unwrap());
        (SignedJwsValidator::new(handle.clone()), handle)
    }

    #[test]
    fn signed_accepts_fresh_rs256_token() {
        let (token, jwks) = mint_rs256("k1", "{\"sub\":\"admin\",\"exp\":9999999999}");
        let (v, _h) = signed(&jwks);
        let outcome = v.validate(&token, 1_000_000_000_000).unwrap();
        assert!(outcome.principal.name == "admin");
        assert!(outcome.principal.auth_method == AuthMethod::SaslOAuthBearer);
    }

    #[test]
    fn signed_validate_surfaces_exp_in_auth_outcome() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let (token, jwks) = mint_rs256("k1", &format!("{{\"sub\":\"alice\",\"exp\":{exp_secs}}}"));
        let (v, _h) = signed(&jwks);
        let outcome = v.validate(&token, now_ms).expect("token valid");
        assert!(outcome.principal.name == "alice");
        assert!(outcome.expires_at_ms == Some(exp_secs * 1000));
    }

    #[test]
    fn signed_rejects_unsecured_alg_none() {
        // An `alg:none` token must never pass the signed validator even if a
        // key happens to be present.
        let (_token, jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let (v, _h) = signed(&jwks);
        let unsecured = jws("{\"alg\":\"none\"}", "{\"sub\":\"a\",\"exp\":9999999999}");
        assert!(v.validate(&unsecured, 1_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn signed_rejects_malformed_compact_jws_segments() {
        let (token, jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let (v, _h) = signed(&jwks);
        let header = B64URL.encode(b"{\"alg\":\"RS256\",\"kid\":\"k1\"}");
        let payload = B64URL.encode(b"{\"sub\":\"a\",\"exp\":9999999999}");

        let empty_signature = format!("{header}.{payload}.");
        assert!(v.validate(&empty_signature, 1_000_000_000_000).is_err());

        let extra_segment = format!("{token}.extra");
        assert!(v.validate(&extra_segment, 1_000_000_000_000).is_err());
    }

    #[test]
    fn signed_rejects_expired() {
        let (token, jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":1000}");
        let (mut v, _h) = signed(&jwks);
        v.allowable_clock_skew_ms = 0;
        // now (ms) far past exp (1000 s).
        assert!(v.validate(&token, 5_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn signed_rejects_future_nbf() {
        let (token, jwks) = mint_rs256(
            "k1",
            "{\"sub\":\"a\",\"exp\":9999999999,\"nbf\":5000000000}",
        );
        let (mut v, _h) = signed(&jwks);
        v.allowable_clock_skew_ms = 0;
        // now = 1e12 ms = 1e9 s, which is before nbf (5e9 s).
        assert!(v.validate(&token, 1_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn signed_honors_issuer() {
        let (token, jwks) = mint_rs256(
            "k1",
            "{\"sub\":\"a\",\"exp\":9999999999,\"iss\":\"https://idp\"}",
        );
        let (mut v, _h) = signed(&jwks);
        v.valid_issuer = Some("https://idp".to_string());
        assert!(v.validate(&token, 1_000_000_000_000).is_ok());
        v.valid_issuer = Some("https://other".to_string());
        assert!(v.validate(&token, 1_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn signed_rejects_missing_issuer_when_required() {
        let (token, jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let (mut v, _h) = signed(&jwks);
        v.valid_issuer = Some("https://idp".to_string());
        assert!(v.validate(&token, 1_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn signed_honors_audience_string_and_array() {
        let (tok_str, jwks) =
            mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999,\"aud\":\"kafka\"}");
        let (mut v, _h) = signed(&jwks);
        v.expected_audience = Some("kafka".to_string());
        assert!(v.validate(&tok_str, 1_000_000_000_000).is_ok());

        let (tok_arr, jwks2) = mint_rs256(
            "k1",
            "{\"sub\":\"a\",\"exp\":9999999999,\"aud\":[\"other\",\"kafka\"]}",
        );
        let (mut v2, _h2) = signed(&jwks2);
        v2.expected_audience = Some("kafka".to_string());
        assert!(v2.validate(&tok_arr, 1_000_000_000_000).is_ok());

        let (tok_bad, jwks3) =
            mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999,\"aud\":\"web\"}");
        let (mut v3, _h3) = signed(&jwks3);
        v3.expected_audience = Some("kafka".to_string());
        assert!(v3.validate(&tok_bad, 1_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    // ---- SignedJwsValidator custom_claim_check + valid_token_type

    #[test]
    fn signed_validate_rejects_when_custom_claim_check_fails() {
        let (token, jwks) = mint_rs256(
            "k1",
            "{\"sub\":\"alice\",\"exp\":9999999999,\"scope\":[\"kafka.read\"]}",
        );
        let (mut v, _h) = signed(&jwks);
        v.custom_claim_check = Some(parse_jp("$.scope[?@ == 'kafka.admin']"));
        let result = v.validate(&token, 1_000_000_000_000);
        assert!(result.unwrap_err() == AuthError::InvalidToken);
    }

    #[test]
    fn signed_validate_accepts_when_custom_claim_check_passes() {
        let (token, jwks) = mint_rs256(
            "k1",
            "{\"sub\":\"alice\",\"exp\":9999999999,\"scope\":[\"kafka.admin\",\"kafka.read\"]}",
        );
        let (mut v, _h) = signed(&jwks);
        v.custom_claim_check = Some(parse_jp("$.scope[?@ == 'kafka.admin']"));
        let outcome = v.validate(&token, 1_000_000_000_000).expect("valid token");
        assert!(outcome.principal.name == "alice");
    }

    #[test]
    fn signed_validate_rejects_when_valid_token_type_mismatch() {
        let (token, jwks) = mint_rs256_with_header(
            "{\"alg\":\"RS256\",\"kid\":\"k1\",\"typ\":\"OPAQUE\"}",
            "{\"sub\":\"alice\",\"exp\":9999999999}",
        );
        let (mut v, _h) = signed(&jwks);
        v.valid_token_type = Some("JWT".into());
        let result = v.validate(&token, 1_000_000_000_000);
        assert!(result.unwrap_err() == AuthError::InvalidToken);
    }

    #[test]
    fn signed_validate_accepts_when_valid_token_type_match() {
        let (token, jwks) = mint_rs256_with_header(
            "{\"alg\":\"RS256\",\"kid\":\"k1\",\"typ\":\"JWT\"}",
            "{\"sub\":\"alice\",\"exp\":9999999999}",
        );
        let (mut v, _h) = signed(&jwks);
        v.valid_token_type = Some("JWT".into());
        assert!(v.validate(&token, 1_000_000_000_000).is_ok());
    }

    #[test]
    fn signed_validate_accepts_when_valid_token_type_unset_regardless_of_header() {
        let (token, jwks) = mint_rs256_with_header(
            "{\"alg\":\"RS256\",\"kid\":\"k1\",\"typ\":\"OPAQUE\"}",
            "{\"sub\":\"alice\",\"exp\":9999999999}",
        );
        let (v, _h) = signed(&jwks);
        // No valid_token_type set → header `typ` ignored.
        assert!(v.validate(&token, 1_000_000_000_000).is_ok());
    }

    #[test]
    fn signed_rejects_missing_principal() {
        let (token, jwks) = mint_rs256("k1", "{\"exp\":9999999999}");
        let (v, _h) = signed(&jwks);
        assert!(v.validate(&token, 1_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn signed_custom_principal_claim() {
        let (token, jwks) = mint_rs256("k1", "{\"client_id\":\"svc-1\",\"exp\":9999999999}");
        let (mut v, _h) = signed(&jwks);
        v.principal_claim_name = "client_id".to_string();
        assert!(
            v.validate(&token, 1_000_000_000_000)
                .unwrap()
                .principal
                .name
                == "svc-1"
        );
    }

    #[test]
    fn signed_key_rotation_via_handle() {
        // Token A verifies under key set A. ES256 with a fresh key per mint, so
        // set B is a genuinely different key (RS256's fixed test key can't be).
        let (token_a, jwks_a) = mint_es256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let (v, handle) = signed(&jwks_a);
        assert!(v.validate(&token_a, 1_000_000_000_000).is_ok());

        // Rotate to a fresh key set (same kid, new key). Token A no longer
        // verifies; a token under the new key does. Same validator instance.
        let (token_b, jwks_b) = mint_es256("k1", "{\"sub\":\"b\",\"exp\":9999999999}");
        handle.store(Jwks::from_json(&jwks_b, false).unwrap());
        assert!(v.validate(&token_a, 1_000_000_000_000) == Err(AuthError::InvalidToken));
        assert!(
            v.validate(&token_b, 1_000_000_000_000)
                .unwrap()
                .principal
                .name
                == "b"
        );
    }

    #[test]
    fn signed_key_handle_shares_validator_key_cell() {
        let (token_a, jwks_a) = mint_es256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let (v, _handle) = signed(&jwks_a);
        assert!(v.validate(&token_a, 1_000_000_000_000).is_ok());

        let handle = v.key_handle();
        let (token_b, jwks_b) = mint_es256("k1", "{\"sub\":\"b\",\"exp\":9999999999}");
        handle.store(Jwks::from_json(&jwks_b, false).expect("parse rotated jwks"));

        assert!(v.validate(&token_a, 1_000_000_000_000).is_err());
        assert!(
            v.validate(&token_b, 1_000_000_000_000)
                .expect("rotated key validates")
                .principal
                .name
                == "b"
        );
    }

    #[test]
    fn signed_rejects_tokens_with_expected_issuer_or_audience_missing() {
        let (token, jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let (mut issuer_validator, _h) = signed(&jwks);
        issuer_validator.valid_issuer = Some("https://idp".to_string());
        assert!(
            issuer_validator.validate(&token, 1_000_000_000_000) == Err(AuthError::InvalidToken)
        );

        let (mut audience_validator, _h) = signed(&jwks);
        audience_validator.expected_audience = Some("kafka".to_string());
        assert!(
            audience_validator.validate(&token, 1_000_000_000_000) == Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn signed_accepts_temporal_claims_at_skew_boundaries() {
        let (token, jwks) = mint_rs256(
            "k1",
            "{\"sub\":\"a\",\"exp\":2000,\"iat\":2001,\"nbf\":2001}",
        );
        let (mut v, _h) = signed(&jwks);
        v.allowable_clock_skew_ms = 1_000;
        assert!(v.validate(&token, 2_000_000).is_ok());
    }

    #[test]
    fn signed_rejects_temporal_claims_outside_skew_boundaries() {
        let (expired, expired_jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":1999}");
        let (mut expired_v, _h) = signed(&expired_jwks);
        expired_v.allowable_clock_skew_ms = 1_000;
        assert!(expired_v.validate(&expired, 2_000_000).is_err());

        let (future_iat, iat_jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":3000,\"iat\":2002}");
        let (mut iat_v, _h) = signed(&iat_jwks);
        iat_v.allowable_clock_skew_ms = 1_000;
        assert!(iat_v.validate(&future_iat, 2_000_000).is_err());

        let (future_nbf, nbf_jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":3000,\"nbf\":2002}");
        let (mut nbf_v, _h) = signed(&nbf_jwks);
        nbf_v.allowable_clock_skew_ms = 1_000;
        assert!(nbf_v.validate(&future_nbf, 2_000_000).is_err());
    }

    #[test]
    fn signed_rejects_when_keyset_empty() {
        let (token, _jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let v = SignedJwsValidator::new(JwksHandle::default());
        assert!(v.validate(&token, 1_000_000_000_000) == Err(AuthError::InvalidToken));
    }

    // ---- signed-validator parity --------------------------------

    #[test]
    fn signed_validate_falls_back_to_alt_claim_when_primary_absent() {
        // Mirror the unsecured fallback test using the signed validator.
        // No `sub` claim → fallback to `client_id`, then apply prefix.
        let (token, jwks) = mint_rs256_with_header(
            "{\"alg\":\"RS256\",\"kid\":\"k1\",\"typ\":\"JWT\"}",
            "{\"client_id\":\"svc1\",\"exp\":9999999999,\"iss\":\"https://test.example\"}",
        );
        let (mut v, _h) = signed(&jwks);
        v.fallback_user_name_claim = Some("client_id".into());
        v.fallback_user_name_prefix = Some("service-account-".into());
        let outcome = v.validate(&token, 1_000_000_000_000).expect("valid");
        assert!(outcome.principal.name == "service-account-svc1");
    }

    #[test]
    fn signed_validate_extracts_groups_from_array_claim() {
        let (token, jwks) = mint_rs256(
            "k1",
            "{\"sub\":\"alice\",\"exp\":9999999999,\"groups\":[\"admin\",\"ops\"]}",
        );
        let (mut v, _h) = signed(&jwks);
        v.groups_claim = Some(parse_jp("$.groups"));
        let outcome = v.validate(&token, 1_000_000_000_000).expect("valid");
        assert!(outcome.principal.groups == vec!["admin".to_string(), "ops".to_string()]);
    }

    // ---- cache expiry + signal-on-verify-failure ----------------

    /// Build a signed validator whose paired `JwksHandle` carries explicit
    /// `last_successful_fetch_ms` and a fresh signal channel. Returns the
    /// validator and the receiver so tests can assert on emitted signals.
    fn signed_with_handles(
        jwks_json: &str,
        last_successful_fetch_ms: i64,
    ) -> (SignedJwsValidator, tokio::sync::mpsc::Receiver<()>) {
        use std::sync::Arc;
        use std::sync::atomic::AtomicI64;
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);
        let ts = Arc::new(AtomicI64::new(last_successful_fetch_ms));
        let handle = JwksHandle::new_with_refresher_handles(
            Jwks::from_json(jwks_json, false).unwrap(),
            ts,
            tx,
        );
        (SignedJwsValidator::new(handle), rx)
    }

    #[test]
    fn signed_validate_rejects_when_jwks_cache_expired() {
        let now_ms: i64 = 10_000_000;
        let exp_secs: i64 = (now_ms / 1000) + 60;
        let (token, jwks) = mint_rs256("k1", &format!("{{\"sub\":\"alice\",\"exp\":{exp_secs}}}"));
        // Last fetch 2s ago; expiry threshold 1s ⇒ expired.
        let (mut v, _rx) = signed_with_handles(&jwks, now_ms - 2_000);
        v.expiry_ms = Some(1_000);
        assert!(v.validate(&token, now_ms) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn signed_validate_accepts_when_jwks_cache_within_expiry() {
        let now_ms: i64 = 10_000_000;
        let exp_secs: i64 = (now_ms / 1000) + 60;
        let (token, jwks) = mint_rs256("k1", &format!("{{\"sub\":\"alice\",\"exp\":{exp_secs}}}"));
        // Last fetch 500ms ago; expiry threshold 1s ⇒ still fresh.
        let (mut v, _rx) = signed_with_handles(&jwks, now_ms - 500);
        v.expiry_ms = Some(1_000);
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(outcome.principal.name == "alice");
    }

    #[test]
    fn signed_validate_accepts_when_jwks_cache_age_equals_expiry() {
        let now_ms: i64 = 10_000_000;
        let exp_secs: i64 = (now_ms / 1000) + 60;
        let (token, jwks) = mint_rs256("k1", &format!("{{\"sub\":\"alice\",\"exp\":{exp_secs}}}"));
        let (mut v, _rx) = signed_with_handles(&jwks, now_ms - 1_000);
        v.expiry_ms = Some(1_000);
        let outcome = v.validate(&token, now_ms).expect("valid");
        assert!(outcome.principal.name == "alice");
    }

    #[test]
    fn signed_validate_accepts_when_expiry_unset_regardless_of_cache_age() {
        let now_ms: i64 = 10_000_000;
        let exp_secs: i64 = (now_ms / 1000) + 60;
        let (token, jwks) = mint_rs256("k1", &format!("{{\"sub\":\"alice\",\"exp\":{exp_secs}}}"));
        // Cache "is" very stale, but expiry_ms = None ⇒ no check fires.
        let (v, _rx) = signed_with_handles(&jwks, now_ms - 999_999_999);
        // expiry_ms left at default None.
        assert!(v.validate(&token, now_ms).is_ok());
    }

    #[test]
    fn signed_validate_skips_expiry_check_when_never_fetched() {
        // last_successful_fetch_ms still at 0 ⇒ no expiry math runs (the
        // never-fetched broker-startup window stays open; verify will fail
        // anyway because the served key set is intact, but the expiry path
        // must not preempt with a spurious rejection).
        let now_ms: i64 = 10_000_000;
        let exp_secs: i64 = (now_ms / 1000) + 60;
        let (token, jwks) = mint_rs256("k1", &format!("{{\"sub\":\"alice\",\"exp\":{exp_secs}}}"));
        let (mut v, _rx) = signed_with_handles(&jwks, 0);
        v.expiry_ms = Some(1);
        // Cache "age" math would say "very expired" if we ran it, but the
        // sentinel-zero guard skips. Verification succeeds.
        assert!(v.validate(&token, now_ms).is_ok());
    }

    #[test]
    fn signed_validate_signals_refresh_on_unknown_kid() {
        let now_ms: i64 = 10_000_000;
        let exp_secs: i64 = (now_ms / 1000) + 60;
        // Token's header advertises kid="k1" (the default rs256 mint), but the
        // served JWKS has a different kid ⇒ verify() returns InvalidToken AND
        // signal_refresh() fires.
        let (token, _jwks_with_k1) =
            mint_rs256("k1", &format!("{{\"sub\":\"alice\",\"exp\":{exp_secs}}}"));
        // Hand-craft a JWKS with a different kid so verify can't find k1.
        let mismatched_jwks =
            r#"{"keys":[{"kty":"RSA","kid":"other","n":"AQAB","e":"AQAB"}]}"#.to_string();
        let (v, mut rx) = signed_with_handles(&mismatched_jwks, now_ms);
        assert!(v.validate(&token, now_ms) == Err(AuthError::InvalidToken));
        assert!(
            rx.try_recv().is_ok(),
            "validator should signal refresh on verify failure",
        );
    }

    #[test]
    fn signed_validate_does_not_signal_when_verification_succeeds() {
        let now_ms: i64 = 10_000_000;
        let exp_secs: i64 = (now_ms / 1000) + 60;
        let (token, jwks) = mint_rs256("k1", &format!("{{\"sub\":\"alice\",\"exp\":{exp_secs}}}"));
        let (v, mut rx) = signed_with_handles(&jwks, now_ms);
        assert!(v.validate(&token, now_ms).is_ok());
        assert!(
            rx.try_recv().is_err(),
            "happy-path verification should not signal a refresh",
        );
    }

    #[tokio::test]
    async fn enum_dispatches_unsecured_and_signed() {
        // Unsecured default.
        let unsecured = OAuthBearerValidator::default();
        assert!(unsecured.jwks_handle().is_none());
        let tok = unsecured_token("admin", 999_999_000, 9_999_999_999);
        assert!(unsecured.validate(&tok, 1_000_000_000_000).await.is_ok());

        // Signed.
        let (token, jwks) = mint_rs256("k1", "{\"sub\":\"x\",\"exp\":9999999999}");
        let (sv, _h) = signed(&jwks);
        let signed_enum = OAuthBearerValidator::Signed(sv);
        assert!(signed_enum.jwks_handle().is_some());
        assert!(
            signed_enum
                .validate(&token, 1_000_000_000_000)
                .await
                .unwrap()
                .principal
                .name
                == "x"
        );
    }

    fn unsecured_token(sub: &str, iat_s: i64, exp_s: i64) -> String {
        jws(
            "{\"alg\":\"none\"}",
            &format!("{{\"sub\":\"{sub}\",\"iat\":{iat_s},\"exp\":{exp_s}}}"),
        )
    }

    #[test]
    fn custom_claim_check_compile_error_at_validator_construction() {
        // Operators paste a malformed expression. We catch it at parse
        // time (validator construction), not per-token validation.
        let result = parse_json_path("@.unterminated");
        assert!(result.is_err(), "malformed expression must fail to parse");
    }
}

#[cfg(test)]
mod introspection_tests {
    use super::*;
    use crate::{AuthError, AuthMethod};
    use assert2::assert;
    use jsonpath_rust::parser::parse_json_path;
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::sync::Mutex;

    // --- pure temporal/claim helpers (no IdP/JWKS fixtures needed) ----------

    #[test]
    fn temporal_claims_exp_boundary_is_inclusive() {
        // now - skew == exp_ms is NOT expired (the guard is `>`, not `>=`).
        let claims = json!({ "exp": 10 }); // exp_ms = 10_000
        assert!(check_temporal_claims(&claims, 10_000, 0).is_ok());
        assert!(check_temporal_claims(&claims, 10_001, 0).is_err());
    }

    #[test]
    fn temporal_claims_iat_and_nbf_future_rejected() {
        // iat: at the boundary it's valid; issued in the future is rejected.
        let iat = json!({ "iat": 10 }); // iat_ms = 10_000
        assert!(check_temporal_claims(&iat, 10_000, 0).is_ok());
        assert!(check_temporal_claims(&iat, 9_999, 0).is_err());
        // nbf (not-before): same shape.
        let nbf = json!({ "nbf": 10 });
        assert!(check_temporal_claims(&nbf, 10_000, 0).is_ok());
        assert!(check_temporal_claims(&nbf, 9_999, 0).is_err());
    }

    #[test]
    fn numeric_date_fractional_seconds_to_ms() {
        // Fractional NumericDate path: 10.5 s -> 10_500 ms (pins the `* 1000`).
        assert!(numeric_date_ms(&json!({ "k": 10.5 }), "k") == Some(10_500));
    }

    #[test]
    fn audience_contains_array_membership() {
        // `aud` array membership: `any(a == expected)` (kills `==` -> `!=`).
        assert!(audience_contains(&json!({ "aud": ["svc-a"] }), "svc-a"));
        assert!(!audience_contains(&json!({ "aud": ["svc-a"] }), "svc-b"));
    }

    /// Per-token canned responses. `introspect` returns the entry for
    /// the matching token (or a Transport error if absent so a test can
    /// exercise the transport-error path).
    #[derive(Debug, Default)]
    struct MockIntrospectionClient {
        introspect_responses: Mutex<HashMap<String, Result<Value, IntrospectionError>>>,
        userinfo_responses: Mutex<HashMap<String, Result<Option<Value>, IntrospectionError>>>,
    }

    impl MockIntrospectionClient {
        fn arc() -> Arc<Self> {
            Arc::new(Self::default())
        }
        fn set_introspect(&self, token: &str, resp: Result<Value, IntrospectionError>) {
            self.introspect_responses
                .lock()
                .unwrap()
                .insert(token.into(), resp);
        }
        fn set_userinfo(&self, token: &str, resp: Result<Option<Value>, IntrospectionError>) {
            self.userinfo_responses
                .lock()
                .unwrap()
                .insert(token.into(), resp);
        }
    }

    #[async_trait::async_trait]
    impl IntrospectionClient for MockIntrospectionClient {
        async fn introspect(&self, token: &str) -> Result<Value, IntrospectionError> {
            self.introspect_responses
                .lock()
                .unwrap()
                .remove(token)
                .unwrap_or(Err(IntrospectionError::Transport(
                    "no canned response".into(),
                )))
        }
        async fn userinfo(&self, token: &str) -> Result<Option<Value>, IntrospectionError> {
            self.userinfo_responses
                .lock()
                .unwrap()
                .remove(token)
                .unwrap_or(Ok(None))
        }
    }

    fn validator(client: Arc<MockIntrospectionClient>) -> IntrospectionValidator {
        IntrospectionValidator {
            client,
            principal_claim_name: "sub".into(),
            custom_claim_check: None,
            call_userinfo: false,
            allowable_clock_skew_ms: 30_000,
            expected_audience: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
        }
    }

    fn parse_jp(expr: &str) -> JpQuery {
        parse_json_path(expr).expect("expression compiles")
    }

    const NOW_MS: i64 = 1_700_000_000_000;

    #[tokio::test]
    async fn introspection_active_true_with_principal_returns_ok() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "alice", "exp": NOW_MS/1000 + 60})),
        );
        let v = validator(mock.clone());
        let outcome = v.validate("tok", NOW_MS).await.unwrap();
        assert!(outcome.principal.name == "alice");
        assert!(outcome.principal.auth_method == AuthMethod::SaslOAuthBearer);
    }

    #[tokio::test]
    async fn introspection_active_false_rejected() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect("tok", Ok(json!({"active": false})));
        let v = validator(mock.clone());
        assert!(matches!(
            v.validate("tok", NOW_MS).await,
            Err(AuthError::InvalidToken)
        ));
    }

    #[tokio::test]
    async fn introspection_missing_active_field_rejected() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect("tok", Ok(json!({"sub": "alice"})));
        let v = validator(mock.clone());
        assert!(matches!(
            v.validate("tok", NOW_MS).await,
            Err(AuthError::InvalidToken)
        ));
    }

    #[tokio::test]
    async fn introspection_expired_exp_rejected() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "alice", "exp": NOW_MS/1000 - 3600})),
        );
        let v = validator(mock.clone());
        assert!(matches!(
            v.validate("tok", NOW_MS).await,
            Err(AuthError::InvalidToken)
        ));
    }

    // ---- IntrospectionValidator custom_claim_check -------------

    #[tokio::test]
    async fn introspection_validate_rejects_when_custom_claim_check_fails() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({
                "active": true,
                "sub": "alice",
                "exp": NOW_MS/1000 + 60,
                "scope": ["kafka.read"],
            })),
        );
        let mut v = validator(mock.clone());
        v.custom_claim_check = Some(parse_jp("$.scope[?@ == 'kafka.admin']"));
        let result = v.validate("tok", NOW_MS).await;
        assert!(result.unwrap_err() == AuthError::InvalidToken);
    }

    #[tokio::test]
    async fn introspection_validate_accepts_when_custom_claim_check_passes() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({
                "active": true,
                "sub": "alice",
                "exp": NOW_MS/1000 + 60,
                "scope": ["kafka.admin", "kafka.read"],
            })),
        );
        let mut v = validator(mock.clone());
        v.custom_claim_check = Some(parse_jp("$.scope[?@ == 'kafka.admin']"));
        let outcome = v.validate("tok", NOW_MS).await.expect("valid");
        assert!(outcome.principal.name == "alice");
    }

    // ---- IntrospectionValidator expected_audience --------------

    #[tokio::test]
    async fn introspection_honors_audience_string_and_array() {
        // Matching string `aud`.
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "a", "exp": NOW_MS/1000 + 60, "aud": "kafka"})),
        );
        let mut v = validator(mock.clone());
        v.expected_audience = Some("kafka".to_string());
        assert!(v.validate("tok", NOW_MS).await.is_ok());

        // Matching value inside an `aud` array.
        let mock2 = MockIntrospectionClient::arc();
        mock2.set_introspect(
            "tok",
            Ok(json!({
                "active": true, "sub": "a", "exp": NOW_MS/1000 + 60,
                "aud": ["other", "kafka"],
            })),
        );
        let mut v2 = validator(mock2.clone());
        v2.expected_audience = Some("kafka".to_string());
        assert!(v2.validate("tok", NOW_MS).await.is_ok());
    }

    #[tokio::test]
    async fn introspection_rejects_non_matching_audience() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "a", "exp": NOW_MS/1000 + 60, "aud": "web"})),
        );
        let mut v = validator(mock.clone());
        v.expected_audience = Some("kafka".to_string());
        assert!(v.validate("tok", NOW_MS).await == Err(AuthError::InvalidToken));
    }

    #[tokio::test]
    async fn introspection_rejects_missing_audience_when_expected() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "a", "exp": NOW_MS/1000 + 60})),
        );
        let mut v = validator(mock.clone());
        v.expected_audience = Some("kafka".to_string());
        assert!(v.validate("tok", NOW_MS).await == Err(AuthError::InvalidToken));
    }

    #[tokio::test]
    async fn introspection_ignores_audience_when_unset() {
        // Default (expected_audience == None): any `aud` is accepted.
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "a", "exp": NOW_MS/1000 + 60, "aud": "web"})),
        );
        let v = validator(mock.clone());
        assert!(v.validate("tok", NOW_MS).await.is_ok());
    }

    #[tokio::test]
    async fn introspection_validate_does_not_check_valid_token_type() {
        // Introspection responses have no JWT header → typ check is N/A.
        // The struct doesn't even expose a valid_token_type field; this
        // is a regression test that validation passes regardless of any
        // hypothetical typ in the response (introspection responses
        // don't carry `typ`).
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({
                "active": true,
                "sub": "alice",
                "exp": NOW_MS/1000 + 60,
            })),
        );
        let v = validator(mock.clone());
        let outcome = v.validate("tok", NOW_MS).await.expect("valid");
        assert!(outcome.principal.name == "alice");
    }

    #[tokio::test]
    async fn introspection_userinfo_claims_override_introspection_for_profile_keys() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({
                "active": true,
                "sub": "alice",
                "exp": NOW_MS/1000 + 60,
                "preferred_username": "intros-name",
            })),
        );
        mock.set_userinfo(
            "tok",
            Ok(Some(
                json!({"preferred_username": "userinfo-name", "email": "a@b.c"}),
            )),
        );
        let mut v = validator(mock.clone());
        v.call_userinfo = true;
        v.principal_claim_name = "preferred_username".into();
        let outcome = v.validate("tok", NOW_MS).await.unwrap();
        assert!(outcome.principal.name == "userinfo-name");
    }

    #[tokio::test]
    async fn introspection_userinfo_does_not_override_authorization_keys() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "alice", "exp": NOW_MS/1000 + 60})),
        );
        mock.set_userinfo("tok", Ok(Some(json!({"active": false, "sub": "mallory"}))));
        let mut v = validator(mock.clone());
        v.call_userinfo = true;
        let outcome = v.validate("tok", NOW_MS).await.unwrap();
        assert!(
            outcome.principal.name == "alice",
            "sub from introspection wins over userinfo"
        );
    }

    #[tokio::test]
    async fn introspection_userinfo_disabled_when_call_userinfo_false() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "alice", "exp": NOW_MS/1000 + 60})),
        );
        // Deliberately set a userinfo response — should be ignored.
        mock.set_userinfo("tok", Ok(Some(json!({"preferred_username": "ignored"}))));
        let v = validator(mock.clone()); // call_userinfo: false (default)
        let outcome = v.validate("tok", NOW_MS).await.unwrap();
        assert!(outcome.principal.name == "alice");
    }

    #[tokio::test]
    async fn introspection_transport_error_becomes_introspection_transport() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Err(IntrospectionError::Transport("connection refused".into())),
        );
        let v = validator(mock.clone());
        let err = v.validate("tok", NOW_MS).await.unwrap_err();
        assert!(
            matches!(err, AuthError::IntrospectionTransport(ref msg) if msg.contains("connection refused")),
            "got {err:?}",
        );
    }

    #[tokio::test]
    async fn introspection_default_principal_claim_sub() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "sub-name", "exp": NOW_MS/1000 + 60})),
        );
        let v = validator(mock.clone());
        assert!(v.validate("tok", NOW_MS).await.unwrap().principal.name == "sub-name");
    }

    #[tokio::test]
    async fn introspection_custom_principal_claim_client_id() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({
                "active": true,
                "sub": "sub-name",
                "exp": NOW_MS/1000 + 60,
                "client_id": "my-client",
            })),
        );
        let mut v = validator(mock.clone());
        v.principal_claim_name = "client_id".into();
        assert!(v.validate("tok", NOW_MS).await.unwrap().principal.name == "my-client");
    }

    #[tokio::test]
    async fn introspection_rejects_empty_fallback_principal_claim() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({
                "active": true,
                "exp": NOW_MS/1000 + 60,
                "client_id": "",
            })),
        );
        let mut v = validator(mock.clone());
        v.fallback_user_name_claim = Some("client_id".into());
        assert!(v.validate("tok", NOW_MS).await == Err(AuthError::InvalidToken));
    }

    #[tokio::test]
    async fn enum_dispatch_introspection_async() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "alice", "exp": NOW_MS/1000 + 60})),
        );
        let v = validator(mock.clone());
        let enum_v = OAuthBearerValidator::Introspection(v);
        let outcome = enum_v.validate("tok", NOW_MS).await.unwrap();
        assert!(outcome.principal.name == "alice");
    }

    #[tokio::test]
    async fn introspection_validate_surfaces_exp_from_introspection_response() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "opaque-token",
            Ok(json!({
                "active": true,
                "sub": "alice",
                "exp": exp_secs,
                "scope": "kafka.write",
            })),
        );
        let v = validator(mock.clone());
        let outcome = v
            .validate("opaque-token", now_ms)
            .await
            .expect("token valid");
        assert!(outcome.principal.name == "alice");
        assert!(outcome.expires_at_ms == Some(exp_secs * 1000));
    }

    // ---- introspection parity -----------------------------------

    #[tokio::test]
    async fn introspection_validate_extracts_groups_from_introspection_response() {
        let exp_secs: i64 = 2_000;
        let now_ms: i64 = 1_000_000;
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "opaque-token",
            Ok(json!({
                "active": true,
                "sub": "alice",
                "exp": exp_secs,
                "groups": ["admin", "ops"],
            })),
        );
        let mut v = validator(mock.clone());
        v.groups_claim = Some(parse_jp("$.groups"));
        let outcome = v.validate("opaque-token", now_ms).await.expect("valid");
        assert!(outcome.principal.groups == vec!["admin".to_string(), "ops".to_string()]);
    }
}
