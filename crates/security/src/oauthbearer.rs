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

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use serde_json::Value;

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
        match claims.get(&self.scope_claim_name) {
            Some(Value::String(s)) => s.split_whitespace().any(|sc| sc == required),
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .any(|sc| sc == required),
            _ => false,
        }
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
}
