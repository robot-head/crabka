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
//!    validation is a follow-up slice.

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use serde_json::Value;

use crate::jwks::JwksHandle;
use crate::{AuthError, AuthMethod, Principal};

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
        // Other keys (host, port, SASL extensions) are not used this slice.
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsecuredJwsValidator {
    /// Claim whose string value becomes the principal name. Default `sub`.
    pub principal_claim_name: String,
    /// Claim carrying the token scope (string or array of strings). Default
    /// `scope`. Only consulted when `required_scope` is set.
    pub scope_claim_name: String,
    /// When set, the token scope must contain this value or validation fails.
    pub required_scope: Option<String>,
    /// Tolerance, in milliseconds, applied to the `exp` / `iat` temporal
    /// checks to absorb clock drift between the client and broker.
    pub allowable_clock_skew_ms: i64,
}

impl Default for UnsecuredJwsValidator {
    fn default() -> Self {
        Self {
            principal_claim_name: "sub".to_string(),
            scope_claim_name: "scope".to_string(),
            required_scope: None,
            allowable_clock_skew_ms: 30_000,
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
    pub fn validate(&self, token: &str, now_ms: i64) -> Result<Principal, AuthError> {
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
            // Signed token — needs JWKS signature verification (slice 49b).
            return Err(AuthError::InvalidToken);
        }

        let header: Value = decode_json_segment(header_b64)?;
        if header.get("alg").and_then(Value::as_str) != Some("none") {
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

        if let Some(required) = &self.required_scope
            && !self.scope_contains(&claims, required)
        {
            return Err(AuthError::InvalidToken);
        }

        let name = claims
            .get(&self.principal_claim_name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or(AuthError::InvalidToken)?
            .to_string();

        Ok(Principal {
            name,
            auth_method: AuthMethod::SaslOAuthBearer,
        })
    }

    /// Whether the token scope claim contains `required`. The scope claim may
    /// be a space-delimited string or a JSON array of strings (both per
    /// Kafka's `OAuthBearerUnsecuredJws`).
    fn scope_contains(&self, claims: &Value, required: &str) -> bool {
        scope_claim_contains(claims, &self.scope_claim_name, required)
    }
}

/// Whether the `scope_claim_name` claim of `claims` contains `required`. The
/// scope claim may be a space-delimited string or a JSON array of strings.
/// Shared by the unsecured and signed validators.
fn scope_claim_contains(claims: &Value, scope_claim_name: &str, required: &str) -> bool {
    match claims.get(scope_claim_name) {
        Some(Value::String(s)) => s.split_whitespace().any(|sc| sc == required),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .any(|sc| sc == required),
        _ => false,
    }
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
/// claims and derives the connection principal (slice 49b).
///
/// The key set lives behind a [`JwksHandle`] so the broker's background
/// refresher can rotate keys without restarting the broker or taking a lock;
/// each [`validate`](Self::validate) reads the current set.
#[derive(Debug, Clone)]
pub struct SignedJwsValidator {
    /// Claim whose string value becomes the principal name. Default `sub`.
    pub principal_claim_name: String,
    /// Claim carrying the token scope (string or array). Default `scope`.
    pub scope_claim_name: String,
    /// When set, the token scope must contain this value.
    pub required_scope: Option<String>,
    /// Tolerance, in milliseconds, applied to `exp` / `iat` / `nbf`.
    pub allowable_clock_skew_ms: i64,
    /// When set, the token `iss` claim must equal this exactly.
    pub valid_issuer: Option<String>,
    /// When set, the token `aud` claim must contain this value.
    pub expected_audience: Option<String>,
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
            scope_claim_name: "scope".to_string(),
            required_scope: None,
            allowable_clock_skew_ms: 30_000,
            valid_issuer: None,
            expected_audience: None,
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
    pub fn validate(&self, token: &str, now_ms: i64) -> Result<Principal, AuthError> {
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
        let kid = header.get("kid").and_then(Value::as_str);

        let signing_input = format!("{header_b64}.{payload_b64}");
        let sig = B64URL
            .decode(sig_b64)
            .map_err(|_| AuthError::InvalidToken)?;
        self.keys
            .load()
            .verify(kid, alg, signing_input.as_bytes(), &sig)?;

        let claims: Value = decode_json_segment(payload_b64)?;
        self.check_claims(&claims, now_ms)
    }

    /// Apply the claim policy (temporal, issuer, audience, scope, principal) to
    /// already-signature-verified `claims`. Split out so the policy is
    /// unit-testable without minting signed tokens.
    fn check_claims(&self, claims: &Value, now_ms: i64) -> Result<Principal, AuthError> {
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

        if let Some(required) = &self.required_scope
            && !scope_claim_contains(claims, &self.scope_claim_name, required)
        {
            return Err(AuthError::InvalidToken);
        }

        let name = claims
            .get(&self.principal_claim_name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or(AuthError::InvalidToken)?
            .to_string();

        Ok(Principal {
            name,
            auth_method: AuthMethod::SaslOAuthBearer,
        })
    }
}

/// The broker's configured OAUTHBEARER token validator: the
/// development-only unsecured-JWS path (slice 49), production signed-JWT
/// validation against a JWKS endpoint (slice 49b), or RFC 7662 opaque-token
/// introspection (slice 49d). Defaults to unsecured.
#[derive(Debug, Clone)]
pub enum OAuthBearerValidator {
    /// Unsecured JWS (`alg:none`) — development / testing only.
    Unsecured(UnsecuredJwsValidator),
    /// Signed JWS verified against a JWKS key set.
    Signed(SignedJwsValidator),
    /// RFC 7662 opaque-token introspection (slice 49d).
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
    pub async fn validate(&self, token: &str, now_ms: i64) -> Result<Principal, AuthError> {
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

/// RFC 7662 opaque-token introspection validator (slice 49d). Calls the
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
    /// Claim carrying the token scope (string or array). Default `scope`.
    pub scope_claim_name: String,
    /// When set, the merged scope claim must contain this value.
    pub required_scope: Option<String>,
    /// `true` iff a `userinfo_endpoint_uri` is configured; the validator
    /// calls `client.userinfo(token)` after a successful introspection and
    /// merges the response over the introspection claims.
    pub call_userinfo: bool,
    /// Clock-skew tolerance for `exp`/`iat`/`nbf` checks on
    /// introspection-response timestamps (when present).
    pub allowable_clock_skew_ms: i64,
}

impl IntrospectionValidator {
    /// Validate a bearer token via RFC 7662 introspection + optional
    /// userinfo enrichment.
    ///
    /// # Errors
    ///
    /// - [`AuthError::IntrospectionTransport`] on HTTP transport / parse failures.
    /// - [`AuthError::InvalidToken`] on `active != true`, missing principal
    ///   claim, scope mismatch, or temporal-claim failure.
    pub async fn validate(&self, token: &str, now_ms: i64) -> Result<Principal, AuthError> {
        let mut claims = self
            .client
            .introspect(token)
            .await
            .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?;
        if claims.get("active").and_then(Value::as_bool) != Some(true) {
            return Err(AuthError::InvalidToken);
        }
        check_temporal_claims(&claims, now_ms, self.allowable_clock_skew_ms)?;
        if self.call_userinfo
            && let Some(ui) = self
                .client
                .userinfo(token)
                .await
                .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?
        {
            merge_userinfo_over_introspection(&mut claims, ui);
        }
        check_required_scope(
            &claims,
            &self.scope_claim_name,
            self.required_scope.as_deref(),
        )?;
        let name = claims
            .get(&self.principal_claim_name)
            .and_then(Value::as_str)
            .ok_or(AuthError::InvalidToken)?
            .to_string();
        Ok(Principal {
            name,
            auth_method: AuthMethod::SaslOAuthBearer,
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

/// Required-scope check honoring both string-scope (space-separated, RFC
/// 6749 §3.3) and array-scope (Keycloak / some `IdPs`) forms. Pure helper.
fn check_required_scope(
    claims: &Value,
    scope_claim_name: &str,
    required: Option<&str>,
) -> Result<(), AuthError> {
    let Some(required) = required else {
        return Ok(());
    };
    let claim = claims
        .get(scope_claim_name)
        .ok_or(AuthError::InvalidToken)?;
    let granted: Vec<&str> = match claim {
        Value::String(s) => s.split_whitespace().collect(),
        Value::Array(arr) => arr.iter().filter_map(Value::as_str).collect(),
        _ => return Err(AuthError::InvalidToken),
    };
    if granted.contains(&required) {
        Ok(())
    } else {
        Err(AuthError::InvalidToken)
    }
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

    fn client_resp(token: &str) -> Vec<u8> {
        format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes()
    }

    #[test]
    fn parse_happy_path_empty_authzid() {
        let r = parse_client_initial_response(&client_resp("tok.en.")).unwrap();
        assert_eq!(r.token, "tok.en.");
        assert_eq!(r.authzid, None);
    }

    #[test]
    fn parse_extracts_authzid_and_ignores_extra_kvpairs() {
        let bytes =
            b"n,a=alice,\x01host=example.com\x01auth=Bearer abc\x01port=443\x01\x01".to_vec();
        let r = parse_client_initial_response(&bytes).unwrap();
        assert_eq!(r.token, "abc");
        assert_eq!(r.authzid, Some("alice".to_string()));
    }

    #[test]
    fn parse_rejects_missing_auth_kvpair() {
        let bytes = b"n,,\x01host=example.com\x01\x01".to_vec();
        assert_eq!(
            parse_client_initial_response(&bytes),
            Err(AuthError::MalformedMessage)
        );
    }

    #[test]
    fn parse_rejects_missing_bearer_prefix() {
        let bytes = b"n,,\x01auth=Basic abc\x01\x01".to_vec();
        assert_eq!(
            parse_client_initial_response(&bytes),
            Err(AuthError::MalformedMessage)
        );
    }

    #[test]
    fn parse_rejects_bad_gs2_header() {
        let bytes = b"z,,\x01auth=Bearer abc\x01\x01".to_vec();
        assert_eq!(
            parse_client_initial_response(&bytes),
            Err(AuthError::MalformedMessage)
        );
    }

    #[test]
    fn validate_accepts_fresh_unsecured_token() {
        let v = UnsecuredJwsValidator::default();
        let now = 1_000_000_000_000;
        let token = unsecured("admin", 999_999_000, 1_000_000_900); // seconds
        let p = v.validate(&token, now).unwrap();
        assert_eq!(p.name, "admin");
        assert_eq!(p.auth_method, AuthMethod::SaslOAuthBearer);
    }

    #[test]
    fn validate_rejects_expired_token() {
        let v = UnsecuredJwsValidator {
            allowable_clock_skew_ms: 0,
            ..Default::default()
        };
        let now = 2_000_000_000_000;
        let token = unsecured("admin", 1_000_000_000, 1_000_000_100);
        assert_eq!(v.validate(&token, now), Err(AuthError::InvalidToken));
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
        assert_eq!(v.validate(&token, now), Err(AuthError::InvalidToken));
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
        assert_eq!(v.validate(&token, now), Err(AuthError::InvalidToken));
    }

    #[test]
    fn validate_rejects_missing_exp() {
        let v = UnsecuredJwsValidator::default();
        let token = jws("{\"alg\":\"none\"}", "{\"sub\":\"admin\"}");
        assert_eq!(
            v.validate(&token, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn validate_rejects_missing_principal_claim() {
        let v = UnsecuredJwsValidator::default();
        let token = jws("{\"alg\":\"none\"}", "{\"exp\":5000000000}");
        assert_eq!(
            v.validate(&token, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn validate_honors_required_scope_string() {
        let v = UnsecuredJwsValidator {
            required_scope: Some("kafka".to_string()),
            ..Default::default()
        };
        let now = 1_000_000_000_000;
        let ok = jws(
            "{\"alg\":\"none\"}",
            "{\"sub\":\"a\",\"exp\":5000000000,\"scope\":\"read kafka write\"}",
        );
        assert!(v.validate(&ok, now).is_ok());
        let bad = jws(
            "{\"alg\":\"none\"}",
            "{\"sub\":\"a\",\"exp\":5000000000,\"scope\":\"read write\"}",
        );
        assert_eq!(v.validate(&bad, now), Err(AuthError::InvalidToken));
    }

    #[test]
    fn validate_honors_required_scope_array() {
        let v = UnsecuredJwsValidator {
            required_scope: Some("kafka".to_string()),
            ..Default::default()
        };
        let now = 1_000_000_000_000;
        let ok = jws(
            "{\"alg\":\"none\"}",
            "{\"sub\":\"a\",\"exp\":5000000000,\"scope\":[\"read\",\"kafka\"]}",
        );
        assert!(v.validate(&ok, now).is_ok());
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
        let p = v.validate(&token, 1_000_000_000_000).unwrap();
        assert_eq!(p.name, "svc-1");
    }

    #[test]
    fn invalid_token_json_is_rfc7628_shape() {
        assert_eq!(invalid_token_json(), "{\"status\":\"invalid_token\"}");
    }

    // ---- SignedJwsValidator (slice 49b) -------------------------------------

    use crate::jwks::{Jwks, JwksHandle, mint_es256, mint_rs256};

    /// Build a `SignedJwsValidator` whose key set is populated from `jwks_json`.
    fn signed(jwks_json: &str) -> (SignedJwsValidator, JwksHandle) {
        let handle = JwksHandle::new(Jwks::from_json(jwks_json).unwrap());
        (SignedJwsValidator::new(handle.clone()), handle)
    }

    #[test]
    fn signed_accepts_fresh_rs256_token() {
        let (token, jwks) = mint_rs256("k1", "{\"sub\":\"admin\",\"exp\":9999999999}");
        let (v, _h) = signed(&jwks);
        let p = v.validate(&token, 1_000_000_000_000).unwrap();
        assert_eq!(p.name, "admin");
        assert_eq!(p.auth_method, AuthMethod::SaslOAuthBearer);
    }

    #[test]
    fn signed_rejects_unsecured_alg_none() {
        // An `alg:none` token must never pass the signed validator even if a
        // key happens to be present.
        let (_token, jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let (v, _h) = signed(&jwks);
        let unsecured = jws("{\"alg\":\"none\"}", "{\"sub\":\"a\",\"exp\":9999999999}");
        assert_eq!(
            v.validate(&unsecured, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn signed_rejects_expired() {
        let (token, jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":1000}");
        let (mut v, _h) = signed(&jwks);
        v.allowable_clock_skew_ms = 0;
        // now (ms) far past exp (1000 s).
        assert_eq!(
            v.validate(&token, 5_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
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
        assert_eq!(
            v.validate(&token, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
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
        assert_eq!(
            v.validate(&token, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn signed_rejects_missing_issuer_when_required() {
        let (token, jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let (mut v, _h) = signed(&jwks);
        v.valid_issuer = Some("https://idp".to_string());
        assert_eq!(
            v.validate(&token, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
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
        assert_eq!(
            v3.validate(&tok_bad, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn signed_honors_required_scope() {
        let (token, jwks) = mint_rs256(
            "k1",
            "{\"sub\":\"a\",\"exp\":9999999999,\"scope\":\"read kafka\"}",
        );
        let (mut v, _h) = signed(&jwks);
        v.required_scope = Some("kafka".to_string());
        assert!(v.validate(&token, 1_000_000_000_000).is_ok());
        v.required_scope = Some("admin".to_string());
        assert_eq!(
            v.validate(&token, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn signed_rejects_missing_principal() {
        let (token, jwks) = mint_rs256("k1", "{\"exp\":9999999999}");
        let (v, _h) = signed(&jwks);
        assert_eq!(
            v.validate(&token, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn signed_custom_principal_claim() {
        let (token, jwks) = mint_rs256("k1", "{\"client_id\":\"svc-1\",\"exp\":9999999999}");
        let (mut v, _h) = signed(&jwks);
        v.principal_claim_name = "client_id".to_string();
        assert_eq!(v.validate(&token, 1_000_000_000_000).unwrap().name, "svc-1");
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
        handle.store(Jwks::from_json(&jwks_b).unwrap());
        assert_eq!(
            v.validate(&token_a, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
        );
        assert_eq!(v.validate(&token_b, 1_000_000_000_000).unwrap().name, "b");
    }

    #[test]
    fn signed_rejects_when_keyset_empty() {
        let (token, _jwks) = mint_rs256("k1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let v = SignedJwsValidator::new(JwksHandle::default());
        assert_eq!(
            v.validate(&token, 1_000_000_000_000),
            Err(AuthError::InvalidToken)
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
        assert_eq!(
            signed_enum
                .validate(&token, 1_000_000_000_000)
                .await
                .unwrap()
                .name,
            "x"
        );
    }

    fn unsecured_token(sub: &str, iat_s: i64, exp_s: i64) -> String {
        jws(
            "{\"alg\":\"none\"}",
            &format!("{{\"sub\":\"{sub}\",\"iat\":{iat_s},\"exp\":{exp_s}}}"),
        )
    }
}

#[cfg(test)]
mod introspection_tests {
    use super::*;
    use crate::{AuthError, AuthMethod};
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::sync::Mutex;

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
            scope_claim_name: "scope".into(),
            required_scope: None,
            call_userinfo: false,
            allowable_clock_skew_ms: 30_000,
        }
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
        let p = v.validate("tok", NOW_MS).await.unwrap();
        assert_eq!(p.name, "alice");
        assert_eq!(p.auth_method, AuthMethod::SaslOAuthBearer);
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

    #[tokio::test]
    async fn introspection_required_scope_honored_string() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "alice", "scope": "kafka.read kafka.write"})),
        );
        let mut v = validator(mock.clone());
        v.required_scope = Some("kafka.write".into());
        assert!(v.validate("tok", NOW_MS).await.is_ok());
    }

    #[tokio::test]
    async fn introspection_required_scope_honored_array() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "alice", "scope": ["kafka.read", "kafka.write"]})),
        );
        let mut v = validator(mock.clone());
        v.required_scope = Some("kafka.write".into());
        assert!(v.validate("tok", NOW_MS).await.is_ok());
    }

    #[tokio::test]
    async fn introspection_required_scope_missing_rejected() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "alice", "scope": "kafka.read"})),
        );
        let mut v = validator(mock.clone());
        v.required_scope = Some("kafka.write".into());
        assert!(matches!(
            v.validate("tok", NOW_MS).await,
            Err(AuthError::InvalidToken)
        ));
    }

    #[tokio::test]
    async fn introspection_userinfo_claims_override_introspection_for_profile_keys() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "alice", "preferred_username": "intros-name"})),
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
        let p = v.validate("tok", NOW_MS).await.unwrap();
        assert_eq!(p.name, "userinfo-name");
    }

    #[tokio::test]
    async fn introspection_userinfo_does_not_override_authorization_keys() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect("tok", Ok(json!({"active": true, "sub": "alice"})));
        mock.set_userinfo("tok", Ok(Some(json!({"active": false, "sub": "mallory"}))));
        let mut v = validator(mock.clone());
        v.call_userinfo = true;
        let p = v.validate("tok", NOW_MS).await.unwrap();
        assert_eq!(p.name, "alice", "sub from introspection wins over userinfo");
    }

    #[tokio::test]
    async fn introspection_userinfo_disabled_when_call_userinfo_false() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect("tok", Ok(json!({"active": true, "sub": "alice"})));
        // Deliberately set a userinfo response — should be ignored.
        mock.set_userinfo("tok", Ok(Some(json!({"preferred_username": "ignored"}))));
        let v = validator(mock.clone()); // call_userinfo: false (default)
        let p = v.validate("tok", NOW_MS).await.unwrap();
        assert_eq!(p.name, "alice");
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
        mock.set_introspect("tok", Ok(json!({"active": true, "sub": "sub-name"})));
        let v = validator(mock.clone());
        assert_eq!(v.validate("tok", NOW_MS).await.unwrap().name, "sub-name");
    }

    #[tokio::test]
    async fn introspection_custom_principal_claim_client_id() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect(
            "tok",
            Ok(json!({"active": true, "sub": "sub-name", "client_id": "my-client"})),
        );
        let mut v = validator(mock.clone());
        v.principal_claim_name = "client_id".into();
        assert_eq!(v.validate("tok", NOW_MS).await.unwrap().name, "my-client");
    }

    #[tokio::test]
    async fn enum_dispatch_introspection_async() {
        let mock = MockIntrospectionClient::arc();
        mock.set_introspect("tok", Ok(json!({"active": true, "sub": "alice"})));
        let v = validator(mock.clone());
        let enum_v = OAuthBearerValidator::Introspection(v);
        let p = enum_v.validate("tok", NOW_MS).await.unwrap();
        assert_eq!(p.name, "alice");
    }
}
