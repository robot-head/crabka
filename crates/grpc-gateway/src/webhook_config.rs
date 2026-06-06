//! Operator-supplied webhook-endpoint config (TOML), compiled at load time:
//! `JSONPath` expressions are parsed once, signature settings validated. Mirrors
//! the broker's `file_config` pattern.

use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64STD;
use hmac::{Hmac, KeyInit, Mac};
use jsonpath_rust::parser::model::JpQuery;
use jsonpath_rust::query::js_path_process;
use serde::Deserialize;
use sha2::Sha256;
use subtle::ConstantTimeEq;

/// Raw TOML form (one entry in `[[endpoints]]` per named endpoint).
#[derive(Debug, Clone, Deserialize)]
pub struct WebhooksFile {
    #[serde(default)]
    pub endpoints: Vec<WebhookEndpoint>,
}

/// One named webhook endpoint as written in the TOML config file.
#[derive(Debug, Clone, Deserialize)]
pub struct WebhookEndpoint {
    pub name: String,
    pub target_topic: String,
    /// Service principal this endpoint produces as (authz). Default `webhook:{name}`.
    pub principal: Option<String>,
    /// HMAC-SHA256 shared secret. If set, `signature_header` is required.
    pub secret: Option<String>,
    /// HTTP header that carries the HMAC signature (e.g. `X-Hub-Signature-256`).
    pub signature_header: Option<String>,
    /// `"hex"` (default) or `"base64"`.
    pub signature_encoding: Option<String>,
    /// Optional literal prefix stripped before decoding (e.g. `"sha256="` for GitHub).
    pub signature_prefix: Option<String>,
    /// Optional replay guard: header that carries the request timestamp.
    pub timestamp_header: Option<String>,
    /// Max age of a valid timestamp in seconds (default 300).
    pub timestamp_tolerance_secs: Option<i64>,
    /// `header:<Name>` or `json:<JSONPath expr>`. Absent ⇒ no dedup (plain produce).
    pub idempotency_source: Option<String>,
    /// Optional record-key source: `header:<Name>` or `json:<JSONPath expr>`.
    pub key_source: Option<String>,
    /// Maximum accepted body size in bytes (default 1 MiB).
    pub max_body_bytes: Option<usize>,
}

/// A value source: an HTTP header or a compiled `JSONPath` into the JSON body.
#[derive(Debug, Clone)]
pub enum Source {
    Header(String),
    JsonPath(JpQuery),
}

impl Source {
    /// Parse a `header:<Name>` or `json:<expr>` spec, compiling `JSONPath` at load time.
    fn parse(spec: &str, ctx: &str) -> Result<Self, String> {
        if let Some(h) = spec.strip_prefix("header:") {
            Ok(Source::Header(h.to_string()))
        } else if let Some(jp) = spec.strip_prefix("json:") {
            let q = jsonpath_rust::parser::parse_json_path(jp)
                .map_err(|e| format!("{ctx}: invalid JSONPath {jp:?}: {e}"))?;
            Ok(Source::JsonPath(q))
        } else {
            Err(format!("{ctx}: must start with 'header:' or 'json:'"))
        }
    }
}

/// How the HMAC digest is encoded in the signature header value.
#[derive(Debug, Clone)]
pub enum SigEncoding {
    Hex,
    Base64,
}

/// Validated and compiled endpoint config — the runtime form.
#[derive(Debug, Clone)]
pub struct CompiledWebhook {
    pub target_topic: String,
    pub principal: String,
    /// Raw secret bytes. `None` means signature verification is disabled.
    pub secret: Option<Vec<u8>>,
    /// HTTP header carrying the HMAC signature.
    pub signature_header: Option<String>,
    pub signature_encoding: SigEncoding,
    /// Literal prefix stripped before hex/base64 decoding (e.g. `"sha256="`).
    pub signature_prefix: Option<String>,
    pub timestamp_header: Option<String>,
    pub timestamp_tolerance_secs: i64,
    pub idempotency_source: Option<Source>,
    pub key_source: Option<Source>,
    pub max_body_bytes: usize,
}

impl WebhooksFile {
    /// Compile + validate every endpoint. Returns `name -> CompiledWebhook`.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message describing the first invalid endpoint.
    pub fn compile(&self) -> Result<HashMap<String, CompiledWebhook>, String> {
        let mut out = HashMap::new();
        for e in &self.endpoints {
            let ctx = format!("[webhooks {}]", e.name);

            // `secret` and `signature_header` must both be present or both absent.
            if e.secret.is_some() != e.signature_header.is_some() {
                return Err(format!(
                    "{ctx}: `secret` and `signature_header` must be set together"
                ));
            }

            let signature_encoding = match e.signature_encoding.as_deref() {
                None | Some("hex") => SigEncoding::Hex,
                Some("base64") => SigEncoding::Base64,
                Some(o) => {
                    return Err(format!(
                        "{ctx}: signature_encoding must be 'hex' or 'base64', got {o:?}"
                    ));
                }
            };

            let idempotency_source = e
                .idempotency_source
                .as_deref()
                .map(|s| Source::parse(s, &format!("{ctx}.idempotency_source")))
                .transpose()?;

            let key_source = e
                .key_source
                .as_deref()
                .map(|s| Source::parse(s, &format!("{ctx}.key_source")))
                .transpose()?;

            out.insert(
                e.name.clone(),
                CompiledWebhook {
                    target_topic: e.target_topic.clone(),
                    principal: e
                        .principal
                        .clone()
                        .unwrap_or_else(|| format!("webhook:{}", e.name)),
                    secret: e.secret.as_ref().map(|s| s.clone().into_bytes()),
                    signature_header: e.signature_header.clone(),
                    signature_encoding,
                    signature_prefix: e.signature_prefix.clone(),
                    timestamp_header: e.timestamp_header.clone(),
                    timestamp_tolerance_secs: e.timestamp_tolerance_secs.unwrap_or(300),
                    idempotency_source,
                    key_source,
                    max_body_bytes: e.max_body_bytes.unwrap_or(1024 * 1024),
                },
            );
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Runtime helpers (pub(crate) — used by webhook.rs)
// ---------------------------------------------------------------------------

/// Verify an HMAC-SHA256 signature over `body` using `secret`.
///
/// * `provided` is the raw header value (possibly prefixed).
/// * `prefix` is an optional literal to strip before decoding (e.g. `"sha256="`).
/// * The comparison is constant-time to prevent timing side-channels.
///
/// Returns `false` on any decoding failure so callers can treat it as
/// an authentication failure without distinguishing error kinds.
#[allow(dead_code)] // used by webhook.rs (Task 2)
pub(crate) fn verify_signature(
    secret: &[u8],
    body: &[u8],
    provided: &str,
    encoding: &SigEncoding,
    prefix: Option<&str>,
) -> bool {
    // Compute HMAC-SHA256(secret, body).
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    let computed = mac.finalize().into_bytes();

    // Strip the optional prefix (e.g. "sha256=").
    let sig_str = if let Some(p) = prefix {
        match provided.strip_prefix(p) {
            Some(s) => s,
            None => return false,
        }
    } else {
        provided
    };

    // Decode the provided signature.
    let decoded = match encoding {
        SigEncoding::Hex => match hex::decode(sig_str) {
            Ok(b) => b,
            Err(_) => return false,
        },
        SigEncoding::Base64 => match B64STD.decode(sig_str) {
            Ok(b) => b,
            Err(_) => return false,
        },
    };

    // Constant-time compare — wrong length also returns false without branching
    // on secret material.
    if computed.len() != decoded.len() {
        return false;
    }
    computed.as_slice().ct_eq(decoded.as_slice()).unwrap_u8() == 1
}

/// Extract a value from an HTTP header or a `JSONPath` expression over the body.
///
/// Returns `None` when the header is absent/non-UTF-8 or the `JSONPath` yields
/// no string result.
#[allow(dead_code)] // used by webhook.rs (Task 2)
pub(crate) fn extract_source(
    src: &Source,
    headers: &axum::http::HeaderMap,
    body_json: Option<&serde_json::Value>,
) -> Option<String> {
    match src {
        Source::Header(h) => headers.get(h)?.to_str().ok().map(str::to_string),
        Source::JsonPath(q) => {
            let json = body_json?;
            let refs = js_path_process(q, json).ok()?;
            for r in refs {
                if let Some(s) = r.val().as_str() {
                    return Some(s.to_string());
                }
            }
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    use super::*;

    // Helper: compute HMAC-SHA256(secret, body) as a hex string.
    fn hmac_hex(secret: &[u8], body: &[u8]) -> String {
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    // Helper: compute HMAC-SHA256(secret, body) as a base64 string.
    fn hmac_b64(secret: &[u8], body: &[u8]) -> String {
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret).unwrap();
        mac.update(body);
        B64STD.encode(mac.finalize().into_bytes())
    }

    // -----------------------------------------------------------------------
    // verify_signature tests
    // -----------------------------------------------------------------------

    #[test]
    fn verify_hex_correct() {
        let secret = b"my-secret";
        let body = b"{\"event\":\"push\"}";
        let sig = hmac_hex(secret, body);
        assert!(verify_signature(
            secret,
            body,
            &sig,
            &SigEncoding::Hex,
            None
        ));
    }

    #[test]
    fn verify_hex_tampered_body_rejected() {
        let secret = b"my-secret";
        let body = b"{\"event\":\"push\"}";
        let sig = hmac_hex(secret, body);
        // Change a single byte in the body.
        let bad_body = b"{\"event\":\"pull\"}";
        assert!(!verify_signature(
            secret,
            bad_body,
            &sig,
            &SigEncoding::Hex,
            None
        ));
    }

    #[test]
    fn verify_hex_garbage_sig_rejected() {
        let secret = b"my-secret";
        let body = b"hello";
        assert!(!verify_signature(
            secret,
            body,
            "zzznothex!!",
            &SigEncoding::Hex,
            None
        ));
    }

    #[test]
    fn verify_hex_wrong_length_rejected() {
        let secret = b"my-secret";
        let body = b"hello";
        // A valid hex string but only 4 bytes — wrong length.
        assert!(!verify_signature(
            secret,
            body,
            "deadbeef",
            &SigEncoding::Hex,
            None
        ));
    }

    #[test]
    fn verify_base64_correct() {
        let secret = b"s3cr3t";
        let body = b"payload";
        let sig = hmac_b64(secret, body);
        assert!(verify_signature(
            secret,
            body,
            &sig,
            &SigEncoding::Base64,
            None
        ));
    }

    #[test]
    fn verify_base64_tampered_rejected() {
        let secret = b"s3cr3t";
        let body = b"payload";
        let sig = hmac_b64(secret, body);
        assert!(!verify_signature(
            secret,
            b"different",
            &sig,
            &SigEncoding::Base64,
            None
        ));
    }

    #[test]
    fn verify_with_prefix_stripped() {
        let secret = b"key";
        let body = b"data";
        let raw_sig = hmac_hex(secret, body);
        let with_prefix = format!("sha256={raw_sig}");
        assert!(verify_signature(
            secret,
            body,
            &with_prefix,
            &SigEncoding::Hex,
            Some("sha256=")
        ));
    }

    #[test]
    fn verify_missing_prefix_rejected() {
        let secret = b"key";
        let body = b"data";
        // Provide the sig without the prefix — should fail because strip_prefix fails.
        let raw_sig = hmac_hex(secret, body);
        assert!(!verify_signature(
            secret,
            body,
            &raw_sig,
            &SigEncoding::Hex,
            Some("sha256=")
        ));
    }

    // -----------------------------------------------------------------------
    // WebhooksFile::compile tests
    // -----------------------------------------------------------------------

    #[test]
    fn compile_full_endpoint() {
        let toml = r#"
[[endpoints]]
name = "github"
target_topic = "events"
secret = "s3cr3t"
signature_header = "X-Hub-Signature-256"
signature_prefix = "sha256="
idempotency_source = "json:$.id"
key_source = "header:X-Delivery"
"#;
        let file: WebhooksFile = toml::from_str(toml).expect("parse");
        let compiled = file.compile().expect("compile");
        let ep = compiled.get("github").expect("key present");

        assert_eq!(ep.target_topic, "events");
        // principal defaults to webhook:{name}
        assert_eq!(ep.principal, "webhook:github");
        assert_eq!(ep.secret.as_deref(), Some(b"s3cr3t".as_slice()));
        assert_eq!(ep.signature_header.as_deref(), Some("X-Hub-Signature-256"));
        assert_eq!(ep.signature_prefix.as_deref(), Some("sha256="));
        assert_eq!(ep.timestamp_tolerance_secs, 300);
        assert_eq!(ep.max_body_bytes, 1024 * 1024);
        assert!(ep.idempotency_source.is_some());
        assert!(ep.key_source.is_some());
    }

    #[test]
    fn compile_explicit_principal() {
        let toml = r#"
[[endpoints]]
name = "stripe"
target_topic = "payments"
principal = "svc:stripe-ingest"
"#;
        let file: WebhooksFile = toml::from_str(toml).expect("parse");
        let compiled = file.compile().expect("compile");
        assert_eq!(compiled["stripe"].principal, "svc:stripe-ingest");
    }

    #[test]
    fn compile_error_secret_without_signature_header() {
        let toml = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
secret = "oops"
"#;
        let file: WebhooksFile = toml::from_str(toml).expect("parse");
        let err = file.compile().expect_err("must error");
        assert!(
            err.contains("signature_header"),
            "error should mention signature_header, got: {err}"
        );
    }

    #[test]
    fn compile_error_signature_header_without_secret() {
        let toml = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
signature_header = "X-Sig"
"#;
        let file: WebhooksFile = toml::from_str(toml).expect("parse");
        let err = file.compile().expect_err("must error");
        assert!(err.contains("signature_header"), "{err}");
    }

    #[test]
    fn compile_error_invalid_jsonpath() {
        let toml = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
idempotency_source = "json:@.unterminated["
"#;
        let file: WebhooksFile = toml::from_str(toml).expect("parse");
        let err = file.compile().expect_err("must error");
        assert!(
            err.contains("JSONPath"),
            "error should mention JSONPath, got: {err}"
        );
    }

    #[test]
    fn compile_error_bad_encoding() {
        let toml = r#"
[[endpoints]]
name = "bad"
target_topic = "t"
secret = "s"
signature_header = "X-Sig"
signature_encoding = "md5"
"#;
        let file: WebhooksFile = toml::from_str(toml).expect("parse");
        let err = file.compile().expect_err("must error");
        assert!(err.contains("signature_encoding"), "{err}");
    }

    #[test]
    fn compile_empty_file() {
        let file: WebhooksFile = toml::from_str("").expect("parse");
        let compiled = file.compile().expect("compile");
        assert!(compiled.is_empty());
    }

    // -----------------------------------------------------------------------
    // extract_source tests
    // -----------------------------------------------------------------------

    #[test]
    fn extract_header_source() {
        use axum::http::HeaderMap;
        let mut headers = HeaderMap::new();
        headers.insert("x-delivery", "abc-123".parse().unwrap());
        let src = Source::Header("x-delivery".to_string());
        assert_eq!(
            extract_source(&src, &headers, None),
            Some("abc-123".to_string())
        );
    }

    #[test]
    fn extract_header_missing() {
        use axum::http::HeaderMap;
        let headers = HeaderMap::new();
        let src = Source::Header("x-missing".to_string());
        assert_eq!(extract_source(&src, &headers, None), None);
    }

    #[test]
    fn extract_jsonpath_source() {
        use axum::http::HeaderMap;
        use jsonpath_rust::parser::parse_json_path;

        let q = parse_json_path("$.id").expect("compile");
        let src = Source::JsonPath(q);
        let json: serde_json::Value = serde_json::json!({"id": "event-42", "type": "push"});
        let headers = HeaderMap::new();
        assert_eq!(
            extract_source(&src, &headers, Some(&json)),
            Some("event-42".to_string())
        );
    }

    #[test]
    fn extract_jsonpath_no_match() {
        use axum::http::HeaderMap;
        use jsonpath_rust::parser::parse_json_path;

        let q = parse_json_path("$.missing_field").expect("compile");
        let src = Source::JsonPath(q);
        let json: serde_json::Value = serde_json::json!({"id": "event-42"});
        let headers = HeaderMap::new();
        assert_eq!(extract_source(&src, &headers, Some(&json)), None);
    }

    #[test]
    fn extract_jsonpath_no_body() {
        use axum::http::HeaderMap;
        use jsonpath_rust::parser::parse_json_path;

        let q = parse_json_path("$.id").expect("compile");
        let src = Source::JsonPath(q);
        let headers = HeaderMap::new();
        // body_json is None — should return None without panicking.
        assert_eq!(extract_source(&src, &headers, None), None);
    }
}
