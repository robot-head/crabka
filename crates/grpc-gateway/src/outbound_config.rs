//! Operator-supplied outbound webhook subscriptions (TOML), compiled at load:
//! the target URL's scheme/host is checked against an allow-list (SSRF guard)
//! and any filter `JSONPath` is parsed once.

use jsonpath_rust::parser::model::JpQuery;
use serde::Deserialize;

const CONTENT_TYPE_HEADER: &str = "content-type";
const CE_HTTP_PREFIX: &str = "ce-";

/// Top-level structure of the outbound webhook TOML config file.
#[derive(Debug, Clone, Deserialize)]
pub struct OutboundFile {
    #[serde(default)]
    pub subscriptions: Vec<OutboundSubscription>,
    /// Allowed `scheme://host` targets (SSRF allow-list). A target is permitted
    /// iff its `scheme` + `host` matches an entry. Empty ⇒ deny all (fail-closed).
    #[serde(default)]
    pub allowed_targets: Vec<AllowedTarget>,
}

/// One entry in `[[allowed_targets]]`: an exact `scheme` + `host` pair.
#[derive(Debug, Clone, Deserialize)]
pub struct AllowedTarget {
    /// URL scheme, e.g. `"https"` (recommended) or `"http"`.
    pub scheme: String,
    /// Exact hostname match, e.g. `"hooks.example.com"`.
    pub host: String,
}

/// One outbound subscription as written in `[[subscriptions]]`.
#[derive(Debug, Clone, Deserialize)]
pub struct OutboundSubscription {
    /// Unique name; used as the consumer-group suffix
    /// (`__crabka_grpc_wh_{name}`).
    pub name: String,
    /// Topics this subscription tails.
    pub source_topics: Vec<String>,
    /// URL to POST each record to. Must match an entry in `allowed_targets`.
    pub target_url: String,
    /// HMAC-SHA256 signing secret. When set, every POST carries an
    /// `X-Crabka-Signature` header computed over the JSON envelope body.
    pub signing_secret: Option<String>,
    /// Topic to produce exhausted records to (dead-letter queue). If absent,
    /// exhausted records are logged and dropped after `max_attempts`.
    pub dead_letter_topic: Option<String>,
    /// Maximum delivery attempts per record (clamped to ≥ 1). Default 5.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Base exponential-backoff delay in milliseconds (clamped to ≥ 1).
    /// Default 500 ms.
    #[serde(default = "default_base_backoff_ms")]
    pub base_backoff_ms: u64,
    /// Maximum backoff cap in milliseconds (clamped to ≥ `base_backoff_ms`).
    /// Default 30 000 ms.
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    /// Per-request HTTP timeout in milliseconds (clamped to ≥ 1).
    /// Default 10 000 ms.
    #[serde(default = "default_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Optional delivery filter:
    /// - `json:<JSONPath>` — record delivered iff the path yields a non-null/
    ///   non-false value.
    /// - `header:<Name>` — **rejected**: records carry no headers.
    pub filter: Option<String>,
    /// Extra static HTTP headers added to every POST (e.g. `Authorization`).
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// HTTP body/header representation for outbound deliveries. Default keeps
    /// the Crabka JSON envelope. `CloudEvents` modes forward records that carry
    /// `ce_*` Kafka headers using the HTTP binding.
    #[serde(default)]
    pub content_mode: OutboundContentMode,
    /// When `true`, each record value is run through the injected codec's
    /// `decode` before delivery: a Confluent-framed value is de-framed to its
    /// JSON view and delivered as `application/json`. With `RawCodec` (no
    /// registry) this is inert (decode yields no JSON ⇒ raw delivery). Default
    /// `false`.
    #[serde(default)]
    pub decode_to_json: bool,
}

/// Outbound HTTP content mode.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutboundContentMode {
    /// Existing Crabka JSON envelope.
    #[default]
    Envelope,
    /// `CloudEvents` HTTP binary binding: CE attributes in HTTP headers, record
    /// value as the raw HTTP body.
    CloudEventsBinary,
    /// `CloudEvents` HTTP structured binding: complete `CloudEvent` JSON in body.
    CloudEventsStructured,
}

fn default_max_attempts() -> u32 {
    5
}
fn default_base_backoff_ms() -> u64 {
    500
}
fn default_max_backoff_ms() -> u64 {
    30_000
}
fn default_timeout_ms() -> u64 {
    10_000
}

/// Validated and compiled form of [`OutboundSubscription`] — the runtime form.
///
/// `JpQuery` is parsed once at load so each delivery doesn't re-parse.
/// Secret is stored as raw bytes so callers don't have to re-encode.
#[derive(Debug, Clone)]
pub struct CompiledSubscription {
    pub name: String,
    pub source_topics: Vec<String>,
    /// Validated (parseable, scheme+host allowed) target URL string.
    pub target_url: String,
    /// HMAC-SHA256 signing key bytes, or `None` when signing is disabled.
    pub signing_secret: Option<Vec<u8>>,
    pub dead_letter_topic: Option<String>,
    /// Clamped to ≥ 1.
    pub max_attempts: u32,
    /// Clamped to ≥ 1.
    pub base_backoff_ms: u64,
    /// Clamped to ≥ `base_backoff_ms`.
    pub max_backoff_ms: u64,
    /// Clamped to ≥ 1.
    pub request_timeout_ms: u64,
    /// Compiled `JSONPath` filter, or `None` (deliver all records).
    pub filter: Option<JpQuery>,
    /// Static extra headers as `(name, value)` pairs.
    pub headers: Vec<(String, String)>,
    pub content_mode: OutboundContentMode,
    /// Decode each record value to JSON via the injected codec before delivery
    /// (inert under `RawCodec`). See [`OutboundSubscription::decode_to_json`].
    pub decode_to_json: bool,
}

impl OutboundFile {
    /// Validate + compile every subscription.
    ///
    /// SSRF check: every `target_url` must parse successfully via
    /// [`reqwest::Url::parse`] and its `(scheme, host)` must match at least one
    /// entry in `allowed_targets`. An **empty** `allowed_targets` list denies
    /// everything (fail-closed).
    ///
    /// Filter check: `header:<X>` filters are rejected because
    /// `ConsumerRecord` exposes no record headers. Only `json:<path>` is
    /// supported.
    ///
    /// # Errors
    ///
    /// Returns a human-readable error message for the first invalid
    /// subscription.
    pub fn compile(&self) -> Result<Vec<CompiledSubscription>, String> {
        let mut out = Vec::new();
        for s in &self.subscriptions {
            let ctx = format!("[outbound {}]", s.name);

            // Parse and SSRF-check the target URL.
            let url = reqwest::Url::parse(&s.target_url)
                .map_err(|e| format!("{ctx}: invalid target_url {:?}: {e}", s.target_url))?;
            let host = url
                .host_str()
                .ok_or_else(|| format!("{ctx}: target_url has no host"))?;
            let scheme = url.scheme();
            let allowed = self.allowed_targets.iter().any(|a| {
                a.scheme.eq_ignore_ascii_case(scheme) && a.host.eq_ignore_ascii_case(host)
            });
            if !allowed {
                return Err(format!(
                    "{ctx}: target {scheme}://{host} not in allowed_targets (SSRF guard)"
                ));
            }

            // Compile the optional filter.
            let filter = match s.filter.as_deref() {
                None => None,
                Some(f) if f.starts_with("json:") => Some(
                    jsonpath_rust::parser::parse_json_path(&f["json:".len()..])
                        .map_err(|e| format!("{ctx}: invalid filter JSONPath: {e}"))?,
                ),
                Some(_) => {
                    return Err(format!(
                        "{ctx}: filter must be 'json:<path>' (records carry no headers)"
                    ));
                }
            };

            if s.content_mode.is_cloudevents() {
                reject_cloudevents_header_overrides(&ctx, &s.headers)?;
            }

            out.push(CompiledSubscription {
                name: s.name.clone(),
                source_topics: s.source_topics.clone(),
                target_url: s.target_url.clone(),
                signing_secret: s.signing_secret.as_ref().map(|x| x.clone().into_bytes()),
                dead_letter_topic: s.dead_letter_topic.clone(),
                max_attempts: s.max_attempts.max(1),
                base_backoff_ms: s.base_backoff_ms.max(1),
                max_backoff_ms: s.max_backoff_ms.max(s.base_backoff_ms),
                request_timeout_ms: s.request_timeout_ms.max(1),
                filter,
                headers: s
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                content_mode: s.content_mode,
                decode_to_json: s.decode_to_json,
            });
        }
        Ok(out)
    }
}

impl OutboundContentMode {
    fn is_cloudevents(self) -> bool {
        matches!(self, Self::CloudEventsBinary | Self::CloudEventsStructured)
    }
}

fn reject_cloudevents_header_overrides(
    ctx: &str,
    headers: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    for header_name in headers.keys() {
        let normalized_header_name = header_name.to_ascii_lowercase();
        if normalized_header_name == CONTENT_TYPE_HEADER
            || normalized_header_name.starts_with(CE_HTTP_PREFIX)
        {
            return Err(format!(
                "{ctx}: CloudEvents content_mode reserves static header {header_name:?}"
            ));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_TOML: &str = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "my-sub"
source_topics = ["events"]
target_url    = "https://hooks.example.com/deliver"
signing_secret = "s3cr3t"
filter         = "json:$.type"
"#;

    #[test]
    fn compile_valid_subscription() {
        let file: OutboundFile = toml::from_str(VALID_TOML).expect("parse TOML");
        let compiled = file.compile().expect("compile");
        assert_eq!(compiled.len(), 1);
        let sub = &compiled[0];
        assert_eq!(sub.name, "my-sub");
        assert_eq!(sub.source_topics, ["events"]);
        assert_eq!(sub.target_url, "https://hooks.example.com/deliver");
        assert_eq!(sub.signing_secret.as_deref(), Some(b"s3cr3t".as_slice()));
        assert!(sub.filter.is_some(), "filter should be compiled");
        // Default values clamped correctly.
        assert_eq!(sub.max_attempts, 5);
        assert_eq!(sub.base_backoff_ms, 500);
        assert_eq!(sub.max_backoff_ms, 30_000);
        assert_eq!(sub.request_timeout_ms, 10_000);
        assert_eq!(sub.content_mode, OutboundContentMode::Envelope);
    }

    #[test]
    fn cloudevents_content_mode_compiles() {
        let toml = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "ce-sub"
source_topics = ["events"]
target_url    = "https://hooks.example.com/deliver"
content_mode  = "cloud_events_binary"
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let compiled = file.compile().expect("compile");

        assert_eq!(
            compiled[0].content_mode,
            OutboundContentMode::CloudEventsBinary
        );
    }

    #[test]
    fn cloudevents_content_mode_rejects_reserved_static_headers() {
        let toml = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "ce-sub"
source_topics = ["events"]
target_url    = "https://hooks.example.com/deliver"
content_mode  = "cloud_events_structured"

[subscriptions.headers]
Authorization = "Bearer ok"
"Ce-Id" = "bad"
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let err = file.compile().expect_err("reserved header must fail");

        assert!(
            err.contains("reserves static header"),
            "error must mention reserved header, got: {err}"
        );
    }

    #[test]
    fn ssrf_target_not_in_allowed_targets_errors() {
        let toml = r#"
[[allowed_targets]]
scheme = "https"
host   = "allowed.example.com"

[[subscriptions]]
name          = "bad"
source_topics = ["t"]
target_url    = "https://evil.attacker.com/exfil"
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let err = file.compile().expect_err("must fail SSRF check");
        assert!(
            err.contains("SSRF guard"),
            "error must mention SSRF guard, got: {err}"
        );
    }

    #[test]
    fn ssrf_empty_allowed_targets_denies_all() {
        // No `[[allowed_targets]]` at all → deny everything (fail-closed).
        let toml = r#"
[[subscriptions]]
name          = "bad"
source_topics = ["t"]
target_url    = "https://hooks.example.com/deliver"
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let err = file.compile().expect_err("empty allow-list must deny");
        assert!(
            err.contains("SSRF guard"),
            "error must mention SSRF guard, got: {err}"
        );
    }

    #[test]
    fn header_filter_is_rejected() {
        let toml = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "bad-filter"
source_topics = ["t"]
target_url    = "https://hooks.example.com/deliver"
filter        = "header:X-Custom"
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let err = file.compile().expect_err("header: filter must be rejected");
        assert!(
            err.contains("records carry no headers"),
            "error must mention missing headers, got: {err}"
        );
    }

    #[test]
    fn json_filter_compiles() {
        let toml = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "filter-sub"
source_topics = ["t"]
target_url    = "https://hooks.example.com/deliver"
filter        = "json:$.type"
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let compiled = file.compile().expect("compile");
        assert!(
            compiled[0].filter.is_some(),
            "json:$.type filter must compile to Some(JpQuery)"
        );
    }

    #[test]
    fn unparseable_target_url_errors() {
        let toml = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "bad-url"
source_topics = ["t"]
target_url    = "not a valid url %%"
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let err = file.compile().expect_err("invalid URL must fail");
        assert!(
            err.contains("invalid target_url"),
            "error must mention invalid target_url, got: {err}"
        );
    }

    #[test]
    fn clamp_max_attempts_minimum_one() {
        let toml = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "clamp"
source_topics = ["t"]
target_url    = "https://hooks.example.com/deliver"
max_attempts  = 0
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let compiled = file.compile().expect("compile");
        assert_eq!(
            compiled[0].max_attempts, 1,
            "max_attempts must be clamped to 1"
        );
    }

    #[test]
    fn max_backoff_clamped_to_base_when_smaller() {
        let toml = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name              = "backoff"
source_topics     = ["t"]
target_url        = "https://hooks.example.com/deliver"
base_backoff_ms   = 1000
max_backoff_ms    = 100
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let compiled = file.compile().expect("compile");
        assert_eq!(
            compiled[0].max_backoff_ms, 1000,
            "max_backoff_ms must be clamped to base_backoff_ms when smaller"
        );
    }

    #[test]
    fn empty_file_compiles_to_empty_vec() {
        let file: OutboundFile = toml::from_str("").expect("parse empty TOML");
        let compiled = file.compile().expect("compile empty");
        assert!(compiled.is_empty());
    }
}
