//! `sasl.kerberos.principal.to.local.rules` (`auth_to_local`) DSL.
//!
//! Maps a Kerberos principal (`primary/instance@REALM`) to a short ACL name
//! using the same `RULE:`/`DEFAULT` grammar as the JVM `KerberosName`. Pure
//! logic — no KDC required.

use regex::Regex;

/// One `auth_to_local` rule.
#[derive(Debug, Clone)]
pub enum Rule {
    /// `DEFAULT`: matches a 1-component principal whose realm == default realm;
    /// result is the first component.
    Default,
    /// `RULE:[n:format](match)s/from/to/[g][/L]`
    Translate {
        num_components: usize,
        format: String,
        match_re: Option<Regex>,
        subst: Option<Subst>,
        lowercase: bool,
    },
}

#[derive(Debug, Clone)]
pub struct Subst {
    from: Regex,
    to: String,
    global: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum NameError {
    #[error("malformed auth_to_local rule: {0}")]
    Parse(String),
    #[error("no auth_to_local rule matched principal")]
    NoMatch,
}

impl Rule {
    pub fn parse(spec: &str) -> Result<Rule, NameError> {
        let spec = spec.trim();
        if spec == "DEFAULT" {
            return Ok(Rule::Default);
        }
        let body = spec
            .strip_prefix("RULE:")
            .ok_or_else(|| NameError::Parse(spec.to_string()))?;
        let body = body
            .strip_prefix('[')
            .ok_or_else(|| NameError::Parse(spec.into()))?;
        let (n_str, rest) = body
            .split_once(':')
            .ok_or_else(|| NameError::Parse(spec.into()))?;
        let num_components: usize = n_str
            .trim()
            .parse()
            .map_err(|_| NameError::Parse(spec.into()))?;
        let (format, mut rest) = rest
            .split_once(']')
            .ok_or_else(|| NameError::Parse(spec.into()))?;
        let format = format.to_string();

        let mut match_re = None;
        if let Some(after) = rest.strip_prefix('(') {
            let (m, r) = after
                .split_once(')')
                .ok_or_else(|| NameError::Parse(spec.into()))?;
            match_re = Some(Regex::new(m).map_err(|e| NameError::Parse(e.to_string()))?);
            rest = r;
        }

        // The trailing `/L` lowercase modifier (Hadoop KerberosName grammar:
        // `...(g)?)?/?(L)?`) sits after the optional substitution. When a subst
        // is present the `L` rides in its flags segment (e.g. `s/x/y/L` or
        // `s/x/y/gL`); without one it trails the regex as `/L`. Detect it by the
        // presence of the `L` flag in either position rather than matching `/L`.
        let mut subst = None;
        let lowercase;
        if let Some(after) = rest.strip_prefix("s/") {
            let parts: Vec<&str> = after.splitn(3, '/').collect();
            if parts.len() < 2 {
                return Err(NameError::Parse(spec.into()));
            }
            let from = Regex::new(parts[0]).map_err(|e| NameError::Parse(e.to_string()))?;
            let to = parts[1].to_string();
            let flags = parts.get(2).copied().unwrap_or("");
            subst = Some(Subst {
                from,
                to,
                global: flags.contains('g'),
            });
            lowercase = flags.contains('L');
        } else {
            lowercase = rest.contains('L');
        }

        Ok(Rule::Translate {
            num_components,
            format,
            match_re,
            subst,
            lowercase,
        })
    }
}

/// Build the candidate string for a Translate rule from realm + components.
/// `$0` => realm, `$1`.. => components[0]..
fn expand_format(format: &str, components: &[&str], realm: &str) -> String {
    let mut out = String::new();
    let mut chars = format.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' {
            let mut num = String::new();
            while let Some(d) = chars.peek() {
                if d.is_ascii_digit() {
                    num.push(*d);
                    chars.next();
                } else {
                    break;
                }
            }
            let idx: usize = num.parse().unwrap_or(usize::MAX);
            if idx == 0 {
                out.push_str(realm);
            } else if let Some(comp) = components.get(idx - 1) {
                out.push_str(comp);
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Apply rules in order; first match wins.
pub fn apply(
    rules: &[Rule],
    realm: &str,
    components: &[&str],
    default_realm: &str,
) -> Result<String, NameError> {
    for rule in rules {
        match rule {
            Rule::Default => {
                if components.len() == 1 && realm == default_realm {
                    return Ok(components[0].to_string());
                }
            }
            Rule::Translate {
                num_components,
                format,
                match_re,
                subst,
                lowercase,
            } => {
                if *num_components != components.len() {
                    continue;
                }
                let candidate = expand_format(format, components, realm);
                if let Some(re) = match_re
                    && !re.is_match(&candidate)
                {
                    continue;
                }
                let mut result = candidate;
                if let Some(s) = subst {
                    result = if s.global {
                        s.from.replace_all(&result, s.to.as_str()).into_owned()
                    } else {
                        s.from.replace(&result, s.to.as_str()).into_owned()
                    };
                }
                if *lowercase {
                    result = result.to_lowercase();
                }
                return Ok(result);
            }
        }
    }
    Err(NameError::NoMatch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn rules(specs: &[&str]) -> Vec<Rule> {
        specs.iter().map(|s| Rule::parse(s).unwrap()).collect()
    }

    #[test]
    fn default_rule_strips_realm_single_component() {
        let r = rules(&["DEFAULT"]);
        // DEFAULT only matches when the principal realm equals the default
        // realm (matching the JVM `KerberosName`), so realm == default_realm.
        assert!(apply(&r, "REALM", &["alice"], "REALM").unwrap() == "alice");
    }

    #[test]
    fn default_rule_rejects_multi_component() {
        let r = rules(&["DEFAULT"]);
        assert!(apply(&r, "REALM", &["kafka", "host"], "REALM").is_err());
    }

    #[test]
    fn rule_substitutes_and_matches_regex() {
        let r = rules(&["RULE:[2:$1](kafka.*)s/^.*$/kafka/", "DEFAULT"]);
        assert!(apply(&r, "REALM", &["kafka", "host"], "REALM").unwrap() == "kafka");
    }

    #[test]
    fn rule_lowercase_modifier() {
        let r = rules(&["RULE:[1:$1]/L"]);
        assert!(apply(&r, "REALM", &["Alice"], "REALM").unwrap() == "alice");
    }

    #[test]
    fn rule_lowercase_modifier_after_substitution() {
        // `/L` riding in the substitution flags must still lowercase the result.
        let r = rules(&["RULE:[1:$1](.*)s/$/-X/L"]);
        assert!(apply(&r, "REALM", &["Alice"], "REALM").unwrap() == "alice-x");
    }

    #[test]
    fn rule_global_and_lowercase_flags_combined() {
        let r = rules(&["RULE:[1:$1](.*)s/A/a/gL"]);
        assert!(apply(&r, "REALM", &["BANANA"], "REALM").unwrap() == "banana");
    }

    #[test]
    fn first_matching_rule_wins() {
        let r = rules(&["RULE:[1:$1](nomatch)s/x/y/", "RULE:[1:$1]/L"]);
        assert!(apply(&r, "REALM", &["BOB"], "REALM").unwrap() == "bob");
    }

    #[test]
    fn no_matching_rule_is_error() {
        let r = rules(&["RULE:[1:$1](nope)s/a/b/"]);
        assert!(apply(&r, "REALM", &["alice"], "REALM").is_err());
    }

    #[test]
    fn parse_round_trips_two_component_format_string() {
        let rule = Rule::parse("RULE:[2:$1@$0](.*@REALM)s/@REALM//").unwrap();
        match rule {
            Rule::Translate { num_components, .. } => assert!(num_components == 2),
            Rule::Default => panic!("expected Translate"),
        }
    }
}
