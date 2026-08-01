//! Operator-supplied outbound webhook subscriptions (TOML), compiled at load:
//! the target URL's scheme/host is checked against an allow-list (SSRF guard)
//! and any filter `JSONPath` is parsed once.

use crabka_units::prelude::*;
use jsonpath_rust::parser::model::JpQuery;
use serde::{Deserialize, Serialize};

use crate::config_value::{PositiveU32, positive_time};

/// Top-level structure of the outbound webhook TOML config file.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutboundFile {
    #[serde(default)]
    pub subscriptions: Vec<OutboundSubscription>,
    /// Allowed `scheme://host` targets (SSRF allow-list). A target is permitted
    /// iff its `scheme` + `host` matches an entry. Empty ⇒ deny all (fail-closed).
    #[serde(default)]
    pub allowed_targets: Vec<AllowedTarget>,
}

/// One entry in `[[allowed_targets]]`: an exact `scheme` + `host` pair.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AllowedTarget {
    /// URL scheme, e.g. `"https"` (recommended) or `"http"`.
    pub scheme: String,
    /// Exact hostname match, e.g. `"hooks.example.com"`.
    pub host: String,
}

/// One outbound subscription as written in `[[subscriptions]]`.
///
/// The delivery-policy durations carry their unit: `base_backoff = "500ms"`,
/// `max_backoff = "30s"`, `request_timeout = "10s"`. A bare number is rejected.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct OutboundSubscription {
    /// Unique name; used as the consumer-group suffix
    /// (`__crabka_grpc_wh_{name}`).
    pub name: String,
    /// Consumer group override. Defaults to `__crabka_grpc_wh_{name}`.
    pub group_id: Option<String>,
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
    /// Maximum delivery attempts per record. Must be greater than zero. Default 5.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Base exponential-backoff delay, e.g. `"500ms"`. Must be greater than
    /// zero. Default 500 ms.
    #[serde(
        default = "default_base_backoff",
        with = "crabka_units::serde_units::human::time"
    )]
    pub base_backoff: Time,
    /// Maximum backoff cap, e.g. `"30s"`. Must be at least `base_backoff`.
    /// Default 30 s.
    #[serde(
        default = "default_max_backoff",
        with = "crabka_units::serde_units::human::time"
    )]
    pub max_backoff: Time,
    /// Per-request HTTP timeout, e.g. `"10s"`. Must be greater than zero.
    /// Default 10 s.
    #[serde(
        default = "default_request_timeout",
        with = "crabka_units::serde_units::human::time"
    )]
    pub request_timeout: Time,
    /// Optional delivery filter:
    /// - `json:<JSONPath>` — record delivered iff the path yields a non-null/
    ///   non-false value.
    /// - `header:<Name>` — **rejected**: records carry no headers.
    pub filter: Option<String>,
    /// Extra static HTTP headers added to every POST (e.g. `Authorization`).
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// When `true`, each record value is run through the injected codec's
    /// `decode` before delivery: a Confluent-framed value is de-framed to its
    /// JSON view and delivered as `application/json`. With `RawCodec` (no
    /// registry) this is inert (decode yields no JSON ⇒ raw delivery). Default
    /// `false`.
    #[serde(default)]
    pub decode_to_json: bool,
}

fn default_max_attempts() -> u32 {
    5
}
fn default_base_backoff() -> Time {
    millis(500)
}
fn default_max_backoff() -> Time {
    secs(30)
}
fn default_request_timeout() -> Time {
    secs(10)
}

/// Validated and compiled form of [`OutboundSubscription`] — the runtime form.
///
/// `JpQuery` is parsed once at load so each delivery doesn't re-parse.
/// Secret is stored as raw bytes so callers don't have to re-encode.
#[derive(Debug, Clone)]
pub struct CompiledSubscription {
    pub name: String,
    pub group_id: String,
    pub source_topics: Vec<String>,
    /// Validated (parseable, scheme+host allowed) target URL string.
    pub target_url: String,
    /// HMAC-SHA256 signing key bytes, or `None` when signing is disabled.
    pub signing_secret: Option<Vec<u8>>,
    pub dead_letter_topic: Option<String>,
    pub max_attempts: u32,
    pub base_backoff: Time,
    pub max_backoff: Time,
    pub request_timeout: Time,
    /// Compiled `JSONPath` filter, or `None` (deliver all records).
    pub filter: Option<JpQuery>,
    /// Static extra headers as `(name, value)` pairs.
    pub headers: Vec<(String, String)>,
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
            let group_id = match &s.group_id {
                Some(value) => refined_type::rule::NonEmptyString::new(value.clone())
                    .map_err(|error| format!("{ctx}: group_id: {error}"))?
                    .into_value(),
                None => format!("__crabka_grpc_wh_{}", s.name),
            };
            let max_attempts = PositiveU32::new(s.max_attempts)
                .map_err(|error| format!("{ctx}: max_attempts: {error}"))?
                .into_value();
            let base_backoff = positive_time("base_backoff", s.base_backoff)
                .map_err(|error| format!("{ctx}: {error}"))?;
            let max_backoff = positive_time("max_backoff", s.max_backoff)
                .map_err(|error| format!("{ctx}: {error}"))?;
            let request_timeout = positive_time("request_timeout", s.request_timeout)
                .map_err(|error| format!("{ctx}: {error}"))?;
            if max_backoff < base_backoff {
                return Err(format!(
                    "{ctx}: max_backoff must be greater than or equal to base_backoff"
                ));
            }

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

            out.push(CompiledSubscription {
                name: s.name.clone(),
                group_id,
                source_topics: s.source_topics.clone(),
                target_url: s.target_url.clone(),
                signing_secret: s.signing_secret.as_ref().map(|x| x.clone().into_bytes()),
                dead_letter_topic: s.dead_letter_topic.clone(),
                max_attempts,
                base_backoff,
                max_backoff,
                request_timeout,
                filter,
                headers: s
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                decode_to_json: s.decode_to_json,
            });
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use assert2::check;

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
        let sub = &compiled[0];
        assert2::assert!(compiled.len() == 1);
        assert2::assert!(sub.name.as_str() == "my-sub");
        assert2::assert!(sub.group_id.as_str() == "__crabka_grpc_wh_my-sub");
        assert2::assert!(
            sub.source_topics
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                == vec!["events"]
        );
        assert2::assert!(sub.target_url.as_str() == "https://hooks.example.com/deliver");
        assert2::assert!(sub.signing_secret.as_deref() == Some(b"s3cr3t".as_slice()));
        assert2::assert!(sub.filter.is_some());
        assert2::assert!(sub.dead_letter_topic.as_deref() == None);
        assert2::assert!(sub.max_attempts == 5);
        assert2::assert!(sub.base_backoff == millis(500));
        assert2::assert!(sub.max_backoff == secs(30));
        assert2::assert!(sub.request_timeout == secs(10));
        assert2::assert!(sub.headers.is_empty());
        assert2::assert!(!sub.decode_to_json);
    }

    #[test]
    fn configured_group_id_reaches_compiled_subscription() {
        let input = VALID_TOML.replace(
            "name          = \"my-sub\"",
            "name          = \"my-sub\"\ngroup_id      = \"deliver-custom\"",
        );
        let file: OutboundFile = toml::from_str(&input).expect("parse TOML");
        let compiled = file.compile().expect("compile");
        assert2::assert!(compiled[0].group_id.as_str() == "deliver-custom");
    }

    #[test]
    fn explicitly_empty_group_id_is_rejected() {
        let input = VALID_TOML.replace(
            "name          = \"my-sub\"",
            "name          = \"my-sub\"\ngroup_id      = \"\"",
        );
        let file: OutboundFile = toml::from_str(&input).expect("parse TOML");
        let error = file.compile().expect_err("empty group_id");
        assert2::assert!(error.contains("group_id"));
    }

    #[test]
    fn compile_error_cases() {
        let ssrf_mismatch = r#"
[[allowed_targets]]
scheme = "https"
host   = "allowed.example.com"

[[subscriptions]]
name          = "bad"
source_topics = ["t"]
target_url    = "https://evil.attacker.com/exfil"
"#;
        let ssrf_empty = r#"
[[subscriptions]]
name          = "bad"
source_topics = ["t"]
target_url    = "https://hooks.example.com/deliver"
"#;
        let header_filter = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "bad-filter"
source_topics = ["t"]
target_url    = "https://hooks.example.com/deliver"
filter        = "header:X-Custom"
"#;
        let invalid_url = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "bad-url"
source_topics = ["t"]
target_url    = "not a valid url %%"
"#;
        for (_name, input, needle) in [
            ("ssrf_mismatch", ssrf_mismatch, "SSRF guard"),
            ("ssrf_empty", ssrf_empty, "SSRF guard"),
            ("header_filter", header_filter, "records carry no headers"),
            ("invalid_url", invalid_url, "invalid target_url"),
        ] {
            let file: OutboundFile = toml::from_str(input).expect("parse TOML");
            let err = file.compile().expect_err("case must fail");
            assert2::assert!(err.contains(needle));
        }
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
        assert2::assert!(compiled[0].filter.is_some());
    }

    #[test]
    fn rejects_non_positive_delivery_policy() {
        let cases = [
            ("max_attempts", "max_attempts = 0"),
            ("base_backoff", r#"base_backoff = "0ms""#),
            ("max_backoff", r#"max_backoff = "0s""#),
            ("request_timeout", r#"request_timeout = "0s""#),
        ];
        for (field, value) in cases {
            let input = format!(
                r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name          = "invalid"
source_topics = ["t"]
target_url    = "https://hooks.example.com/deliver"
{value}
"#
            );
            let file: OutboundFile = toml::from_str(&input).expect("parse TOML");
            let error = file.compile().expect_err("zero must be rejected");
            assert2::assert!(error.contains(field));
        }
    }

    #[test]
    fn max_backoff_smaller_than_base_is_rejected() {
        let toml = r#"
[[allowed_targets]]
scheme = "https"
host   = "hooks.example.com"

[[subscriptions]]
name              = "backoff"
source_topics     = ["t"]
target_url        = "https://hooks.example.com/deliver"
base_backoff      = "1s"
max_backoff       = "100ms"
"#;
        let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
        let error = file.compile().expect_err("invalid backoff relationship");
        assert2::assert!(error.contains("max_backoff"));
    }

    #[test]
    fn empty_file_compiles_to_empty_vec() {
        let file: OutboundFile = toml::from_str("").expect("parse empty TOML");
        let compiled = file.compile().expect("compile empty");
        assert2::assert!(compiled.is_empty());
    }

    // -----------------------------------------------------------------------
    // Dimensioned config encoding
    // -----------------------------------------------------------------------

    #[test]
    fn delivery_policy_is_read_in_its_human_form() {
        let input = VALID_TOML.replace(
            "filter         = \"json:$.type\"",
            "base_backoff = \"250ms\"\nmax_backoff = \"1m\"\nrequest_timeout = \"2.5s\"",
        );
        let file: OutboundFile = toml::from_str(&input).expect("parse TOML");
        let compiled = file.compile().expect("compile");

        check!(compiled[0].base_backoff == millis(250));
        check!(compiled[0].max_backoff == minutes(1));
        check!(compiled[0].request_timeout == millis(2_500));
    }

    /// A duration must carry its unit: `500` is neither milliseconds nor
    /// seconds until it says so.
    #[test]
    fn unitless_delivery_policy_is_rejected() {
        for field in ["base_backoff", "max_backoff", "request_timeout"] {
            let input = VALID_TOML.replace(
                "filter         = \"json:$.type\"",
                &format!("{field} = 500"),
            );
            check!(toml::from_str::<OutboundFile>(&input).is_err());
        }
    }

    #[test]
    fn subscription_round_trips_through_its_serde_encoding() {
        let subscription = OutboundSubscription {
            name: "round-trip".to_string(),
            group_id: None,
            source_topics: vec!["events".to_string()],
            target_url: "https://hooks.example.com/deliver".to_string(),
            signing_secret: None,
            dead_letter_topic: None,
            max_attempts: 5,
            base_backoff: millis(500),
            max_backoff: secs(30),
            request_timeout: secs(10),
            filter: None,
            headers: std::collections::HashMap::new(),
            decode_to_json: false,
        };

        let encoded = serde_json::to_string(&subscription).expect("serialize");
        let decoded: OutboundSubscription = serde_json::from_str(&encoded).expect("deserialize");

        check!(encoded.contains(r#""base_backoff":"500ms""#));
        check!(encoded.contains(r#""max_backoff":"30s""#));
        check!(encoded.contains(r#""request_timeout":"10s""#));
        check!(decoded == subscription);
    }
}
