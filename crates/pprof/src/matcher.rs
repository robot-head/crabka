//! Prometheus label-selector helper.
//!
//! This is not a profiles query language. It only parses the matcher subset
//! carried in Pyroscope/Grafana requests.

use crabka_blockstore::{LabelMatcher, MatchOp};
use regex::Regex;

use crate::error::ProfileError;

pub fn parse_label_selector(input: &str) -> Result<Vec<LabelMatcher>, ProfileError> {
    let body = trim_selector(input)?;
    if body.trim().is_empty() {
        return Ok(Vec::new());
    }

    let matcher_re =
        Regex::new(r#"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"\s*$"#)
            .map_err(|err| ProfileError::Plan(format!("matcher regex failed to compile: {err}")))?;

    let mut out = Vec::new();
    for part in split_top_level_commas(body)? {
        let captures = matcher_re
            .captures(part)
            .ok_or_else(|| ProfileError::Plan(format!("invalid label matcher {part:?}")))?;
        let name = captures.get(1).expect("capture").as_str().to_string();
        let op = match captures.get(2).expect("capture").as_str() {
            "=" => MatchOp::Eq,
            "!=" => MatchOp::Neq,
            "=~" => MatchOp::Re,
            "!~" => MatchOp::Nre,
            other => {
                return Err(ProfileError::Plan(format!(
                    "invalid matcher operator {other:?}"
                )));
            }
        };
        let value = unescape_quoted(captures.get(3).expect("capture").as_str())?;
        out.push(LabelMatcher::new(name, op, value));
    }
    Ok(out)
}

#[allow(dead_code)]
fn trim_selector(input: &str) -> Result<&str, ProfileError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok("");
    }
    if let Some(inner) = trimmed.strip_prefix('{') {
        return inner
            .strip_suffix('}')
            .ok_or_else(|| ProfileError::Plan("unclosed label selector".to_string()));
    }
    if trimmed.ends_with('}') {
        return Err(ProfileError::Plan(
            "label selector has closing brace without opening brace".to_string(),
        ));
    }
    Ok(trimmed)
}

#[allow(dead_code)]
fn split_top_level_commas(input: &str) -> Result<Vec<&str>, ProfileError> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut escaped = false;
    for (idx, ch) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quote => escaped = true,
            '"' => in_quote = !in_quote,
            ',' if !in_quote => {
                // Tolerate empty parts from stray/leading/trailing/double commas
                // (e.g. `{,service_name="x"}`). Grafana's pyroscope app builds its
                // selector by concatenating an often-empty base filter with the
                // service name, yielding a leading comma; Prometheus/Pyroscope
                // accept these, so skip the empty part rather than erroring.
                let part = input[start..idx].trim();
                if !part.is_empty() {
                    parts.push(part);
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    if in_quote {
        return Err(ProfileError::Plan(
            "unterminated quoted matcher value".to_string(),
        ));
    }
    let tail = input[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    Ok(parts)
}

#[allow(dead_code)]
fn unescape_quoted(input: &str) -> Result<String, ProfileError> {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(next) = chars.next() else {
            return Err(ProfileError::Plan(
                "trailing escape in matcher value".to_string(),
            ));
        };
        match next {
            '"' | '\\' => out.push(next),
            'n' => out.push('\n'),
            't' => out.push('\t'),
            other => {
                out.push('\\');
                out.push(other);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_blockstore::MatchOp;

    use super::*;

    #[test]
    fn parses_braced_matchers() {
        let ms =
            parse_label_selector(r#"{service_name="checkout", env=~"prod|stage", region!="eu"}"#)
                .unwrap();
        assert!(ms.len() == 3);
        assert!(
            ms[0].name == "service_name" && ms[0].op == MatchOp::Eq && ms[0].value == "checkout"
        );
        assert!(ms[1].op == MatchOp::Re);
        assert!(ms[2].op == MatchOp::Neq);
    }

    #[test]
    fn empty_selector_is_empty() {
        assert!(parse_label_selector("{}").unwrap().is_empty());
        assert!(parse_label_selector("").unwrap().is_empty());
    }

    // Grafana's pyroscope drilldown app concatenates an often-empty base filter
    // with the service name, producing leading/trailing/double commas. Real
    // Pyroscope tolerates these; we skip the empty parts rather than rejecting
    // the whole query with "empty label matcher" (which blanked every tile).
    #[test]
    fn tolerates_stray_commas() {
        for sel in [
            r#"{,service_name="broker"}"#,
            r#"{service_name="broker",}"#,
            r#"{ , service_name="broker" , }"#,
        ] {
            let ms = parse_label_selector(sel).unwrap();
            assert!(ms.len() == 1, "{sel}");
            assert!(ms[0].name == "service_name" && ms[0].value == "broker");
        }
        // Real matchers on either side of a double comma are both kept.
        assert!(
            parse_label_selector(r#"{service_name="a",,instance="i"}"#)
                .unwrap()
                .len()
                == 2
        );
        // A selector that is only commas/whitespace means match-all.
        assert!(parse_label_selector("{,}").unwrap().is_empty());
    }

    #[test]
    fn keeps_commas_inside_quoted_matcher_values() {
        let ms = parse_label_selector(r#"{service_name="api,primary",instance="pod-1"}"#).unwrap();

        assert!(ms.len() == 2);
        assert!(ms[0].name == "service_name");
        assert!(ms[0].value == "api,primary");
        assert!(ms[1].name == "instance");
        assert!(ms[1].value == "pod-1");
    }

    #[test]
    fn escaped_quotes_do_not_toggle_comma_splitting() {
        let ms = parse_label_selector(r#"{note="say \"hi, there\"",service_name="api"}"#).unwrap();

        assert!(ms.len() == 2);
        assert!(ms[0].name == "note");
        assert!(ms[0].value == "say \"hi, there\"");
        assert!(ms[1].name == "service_name");
        assert!(ms[1].value == "api");
    }

    #[test]
    fn splitter_does_not_treat_backslash_outside_quotes_as_escape() {
        assert!(
            split_top_level_commas("left\\,right,tail").unwrap() == vec!["left\\", "right", "tail"]
        );
    }

    #[test]
    fn rejects_malformed() {
        assert!(parse_label_selector(r"{service_name=}").is_err());
        assert!(parse_label_selector(r#"{=~"x"}"#).is_err());
    }
}
