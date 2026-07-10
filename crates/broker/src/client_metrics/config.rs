//! Validation + parsing for KIP-714 `CLIENT_METRICS` config resources.
//!
//! Three keys only (matching `org.apache.kafka.server.metrics.ClientMetricsConfigs`):
//! `metrics` (CSV prefix list; the single token `"*"` = all), `interval.ms`
//! (int, `100..=3_600_000`, default 300000), and `match` (CSV of `selector=regex`).

use std::collections::BTreeMap;

use regex::Regex;

pub(crate) const KEY_METRICS: &str = "metrics";
pub(crate) const KEY_INTERVAL_MS: &str = "interval.ms";
pub(crate) const KEY_MATCH: &str = "match";

pub(crate) const DEFAULT_INTERVAL_MS: i32 = 300_000;
pub(crate) const MIN_INTERVAL_MS: i64 = 100;
pub(crate) const MAX_INTERVAL_MS: i64 = 3_600_000;
pub(crate) const ALL_METRICS: &str = "*";

// Variant names mirror the KIP-714 selector field names exactly (client_id,
// client_software_name, …); keeping the Client prefix is intentional.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MatchSelector {
    ClientInstanceId,
    ClientId,
    ClientSoftwareName,
    ClientSoftwareVersion,
    ClientSourceAddress,
    ClientSourcePort,
}

impl MatchSelector {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "client_instance_id" => Self::ClientInstanceId,
            "client_id" => Self::ClientId,
            "client_software_name" => Self::ClientSoftwareName,
            "client_software_version" => Self::ClientSoftwareVersion,
            "client_source_address" => Self::ClientSourceAddress,
            "client_source_port" => Self::ClientSourcePort,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MatchRule {
    pub selector: MatchSelector,
    pub pattern: Regex,
}

/// Validate a single `(key, value)` against KIP-714 rules. Returns a
/// human-readable reason on failure (surfaced as `INVALID_CONFIG`).
pub(crate) fn validate(key: &str, value: &str) -> Result<(), String> {
    match key {
        KEY_METRICS => Ok(()),
        KEY_INTERVAL_MS => {
            let n: i64 = value
                .parse()
                .map_err(|_| format!("interval.ms must be an integer, got `{value}`"))?;
            if (MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&n) {
                Ok(())
            } else {
                Err(format!(
                    "interval.ms must be in [{MIN_INTERVAL_MS}, {MAX_INTERVAL_MS}], got {n}"
                ))
            }
        }
        KEY_MATCH => parse_match_rules(value).map(|_| ()),
        other => Err(format!("unknown client-metrics config key `{other}`")),
    }
}

/// True if `key` is one of the three recognized client-metrics keys.
pub(crate) fn is_recognized(key: &str) -> bool {
    matches!(key, KEY_METRICS | KEY_INTERVAL_MS | KEY_MATCH)
}

/// Effective push interval for a subscription's override map (default when unset).
pub(crate) fn effective_interval_ms(configs: &BTreeMap<String, String>) -> i32 {
    configs
        .get(KEY_INTERVAL_MS)
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(DEFAULT_INTERVAL_MS)
}

/// Parse the `metrics` value into prefixes. `"*"` collapses to `["*"]`;
/// empty string yields an empty list (no metrics).
pub(crate) fn parse_metrics(value: &str) -> Vec<String> {
    if value.trim().is_empty() {
        return Vec::new();
    }
    value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Parse the `match` value into compiled selector rules. Empty = match-all.
pub(crate) fn parse_match_rules(value: &str) -> Result<Vec<MatchRule>, String> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut rules = Vec::new();
    for entry in value.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (sel, pat) = entry
            .split_once('=')
            .ok_or_else(|| format!("match entry `{entry}` is not `selector=regex`"))?;
        let selector = MatchSelector::parse(sel.trim())
            .ok_or_else(|| format!("unknown match selector `{}`", sel.trim()))?;
        let pattern = Regex::new(pat.trim())
            .map_err(|e| format!("invalid regex for `{}`: {e}", sel.trim()))?;
        rules.push(MatchRule { selector, pattern });
    }
    Ok(rules)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_bounds_enforced() {
        for (value, ok) in [
            ("300000", true),
            ("100", true),
            ("3600000", true),
            ("99", false),
            ("3600001", false),
            ("not-a-number", false),
        ] {
            assert!(
                validate("interval.ms", value).is_ok() == ok,
                "interval.ms={value}"
            );
        }
    }

    #[test]
    fn unknown_key_rejected() {
        assert!(validate("bogus.key", "x").is_err());
    }

    #[test]
    fn match_selectors_validated() {
        for (value, ok) in [
            ("client_id=my-app.*", true),
            (
                "client_software_name=apache-kafka-java,client_id=svc-.*",
                true,
            ),
            ("client_foo=x", false),
            ("client_id", false),
            ("client_id=[unclosed", false),
        ] {
            assert!(validate("match", value).is_ok() == ok, "match={value}");
        }
    }

    #[test]
    fn metrics_list_accepts_star_and_prefixes() {
        for value in [
            "*",
            "org.apache.kafka.consumer.,org.apache.kafka.producer.",
            "",
        ] {
            assert!(validate("metrics", value).is_ok(), "metrics={value}");
        }
    }

    #[test]
    fn effective_interval_defaults_and_clamps() {
        for (case, interval, expected) in [
            ("default", None, DEFAULT_INTERVAL_MS),
            ("configured", Some("60000"), 60_000),
        ] {
            let mut configs = std::collections::BTreeMap::new();
            if let Some(interval) = interval {
                configs.insert("interval.ms".to_string(), interval.to_string());
            }
            assert_eq!(effective_interval_ms(&configs), expected, "case {case}");
        }
    }

    #[test]
    fn parse_match_rules_roundtrip() {
        let rules = parse_match_rules("client_id=svc-.*,client_software_name=java").unwrap();
        let parsed: Vec<_> = rules
            .iter()
            .map(|rule| (rule.selector, rule.pattern.as_str()))
            .collect();
        assert_eq!(
            parsed,
            vec![
                (MatchSelector::ClientId, "svc-.*"),
                (MatchSelector::ClientSoftwareName, "java"),
            ]
        );
    }

    #[test]
    fn parse_metrics_collapses_star() {
        for (case, input, expected) in [
            ("all metrics", "*", vec!["*".to_string()]),
            ("empty", "", Vec::new()),
            (
                "prefixes",
                "a.,b.",
                vec!["a.".to_string(), "b.".to_string()],
            ),
        ] {
            assert_eq!(parse_metrics(input), expected, "case {case}");
        }
    }
}
