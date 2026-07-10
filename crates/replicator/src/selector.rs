//! Include/exclude name selectors. `*` is a glob wildcard; patterns are anchored.

use regex::Regex;

use crate::error::ReplicatorError;

/// Matches topic or consumer-group names against include/exclude pattern lists.
///
/// Rules:
/// - `*` is a glob wildcard (maps to `.*` in regex).
/// - Patterns are fully anchored (`^...$`).
/// - Exclude wins over include.
/// - An empty include list matches nothing.
#[derive(Debug, Clone)]
pub struct Selector {
    include: Vec<Regex>,
    exclude: Vec<Regex>,
}

fn glob_to_regex(pat: &str) -> Result<Regex, ReplicatorError> {
    let mut re = String::from("^");
    for ch in pat.chars() {
        match ch {
            '*' => re.push_str(".*"),
            c if "\\.[]{}()+?^$|".contains(c) => {
                re.push('\\');
                re.push(c);
            }
            c => re.push(c),
        }
    }
    re.push('$');
    Regex::new(&re).map_err(|e| ReplicatorError::Config(format!("bad selector `{pat}`: {e}")))
}

impl Selector {
    /// Compile include and exclude glob patterns into a `Selector`.
    ///
    /// # Errors
    /// Returns [`ReplicatorError::Config`] if any pattern produces an invalid regex.
    pub fn compile(include: &[String], exclude: &[String]) -> Result<Self, ReplicatorError> {
        Ok(Self {
            include: include
                .iter()
                .map(|p| glob_to_regex(p))
                .collect::<Result<_, _>>()?,
            exclude: exclude
                .iter()
                .map(|p| glob_to_regex(p))
                .collect::<Result<_, _>>()?,
        })
    }

    /// Returns `true` if `name` is matched by the include list and not the exclude list.
    #[must_use]
    pub fn matches(&self, name: &str) -> bool {
        if self.exclude.iter().any(|r| r.is_match(name)) {
            return false;
        }
        self.include.iter().any(|r| r.is_match(name))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn include_exclude_with_glob_and_regex() {
        let s = Selector::compile(
            &["orders".into(), "telemetry.*".into()],
            &["*.internal".into()],
        )
        .unwrap();
        for (name, want) in [
            ("orders", true),
            ("telemetry.cpu", true),
            ("payments", false),
            ("telemetry.internal", false), // excluded wins
        ] {
            assert2::assert!(s.matches(name) == want);
        }
    }

    #[test]
    fn empty_include_matches_nothing() {
        let s = Selector::compile(&[], &[]).unwrap();
        assert2::assert!(!s.matches("anything"));
    }

    #[test]
    fn regex_metacharacters_are_escaped_to_literals() {
        // A literal `.` in a pattern must match only a literal `.`, never an
        // arbitrary character. If the metachar-escape guard is disabled, `a.b`
        // compiles to the regex `^a.b$` and would wrongly match `axb`.
        let s = Selector::compile(&["a.b".into()], &[]).unwrap();
        assert2::assert!(s.matches("a.b"));
        assert2::assert!(!s.matches("axb"));
    }
}
