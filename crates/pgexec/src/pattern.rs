//! `SIMILAR TO` pattern matching and the `ESCAPE` clause shared by `LIKE`,
//! `ILIKE`, and `SIMILAR TO`.
//!
//! `PostgreSQL` does not implement SQL's `SIMILAR TO` directly: it rewrites the
//! pattern into a POSIX regular expression and hands that to its regexp engine
//! (`similar_to_escape` in `backend/utils/adt/regexp.c`). This module performs
//! the same rewrite, so the wildcard/metacharacter split — `%` and `_` are SQL
//! wildcards, while `| * + ? {} () []` keep their regexp meaning and everything
//! else is literal — comes out identical.

use crabka_pgtypes::{Datum, TypeError};

use crate::error::ExecError;

/// The escape character an `ESCAPE` clause supplies. `Ok(None)` is `ESCAPE ''`,
/// which disables escaping entirely; a string of more than one character is
/// 22025, exactly as `PostgreSQL` reports it.
pub(crate) fn escape_char(escape: &Datum) -> Result<Option<char>, ExecError> {
    let Datum::Text(s) = escape else {
        return Err(ExecError::TypeMismatch(
            "ESCAPE string must be type text".into(),
        ));
    };
    let mut chars = s.chars();
    match (chars.next(), chars.next()) {
        (None, _) => Ok(None),
        (Some(c), None) => Ok(Some(c)),
        (Some(_), Some(_)) => Err(ExecError::Type(TypeError::InvalidEscapeString)),
    }
}

/// `s SIMILAR TO pattern` — true iff the whole of `s` matches.
pub(crate) fn similar_match(
    s: &str,
    pattern: &str,
    escape: Option<char>,
) -> Result<bool, ExecError> {
    let regex = regex::RegexBuilder::new(&similar_to_regex(pattern, escape, false))
        // PostgreSQL's regexps are non-newline-sensitive by default, so `.` and
        // therefore `_`/`%` match a newline too.
        .dot_matches_new_line(true)
        .build()
        .map_err(|_| {
            ExecError::Type(TypeError::Domain {
                sqlstate: "2201B",
                message: "invalid regular expression",
            })
        })?;
    Ok(regex.is_match(s))
}

/// Rewrite a SQL `SIMILAR TO` pattern as the POSIX regexp `PostgreSQL` would run.
///
/// The whole pattern must match, so the output is wrapped in `^(?:…)$`.
/// `%` becomes `.*` and `_` becomes `.`; a `(` becomes a non-capturing
/// `(?:` (`PostgreSQL` reserves capture groups for `substring()`); the regexp
/// metacharacters SQL does *not* give a meaning to — `. ^ $ \` — are escaped so
/// they stay literal; and a bracket expression is copied through verbatim.
/// `SUBSTRING(s SIMILAR pattern ESCAPE esc)` — the SQL-regex extraction form.
///
/// The region to return is delimited by the escape character followed by a
/// double quote, so with `ESCAPE '#'` the pattern `%#"b_d#"%` returns whatever
/// `b_d` matched. `None` means the pattern did not match the whole string.
///
/// # Errors
///
/// 2201B when the translated pattern is not a valid regular expression.
pub(crate) fn similar_substring(
    s: &str,
    pattern: &str,
    escape: Option<char>,
) -> Result<Option<String>, ExecError> {
    let regex = regex::RegexBuilder::new(&similar_to_regex(pattern, escape, true))
        .dot_matches_new_line(true)
        .build()
        .map_err(|_| {
            ExecError::Type(TypeError::Domain {
                sqlstate: "2201B",
                message: "invalid regular expression",
            })
        })?;
    let Some(captures) = regex.captures(s) else {
        return Ok(None);
    };
    // Without a `#"…"#` region the whole match is the result, which is what
    // PostgreSQL returns for a pattern that marks no region.
    Ok(Some(
        captures
            .get(1)
            .or_else(|| captures.get(0))
            .map_or_else(String::new, |m| m.as_str().to_string()),
    ))
}

/// `capture_quoted` turns each `<escape>"` into a capture-group delimiter rather
/// than a literal quote, which is how `SUBSTRING(… SIMILAR …)` marks the region
/// to extract. Everything else translates identically.
fn similar_to_regex(pattern: &str, escape: Option<char>, capture_quoted: bool) -> String {
    let mut out = String::with_capacity(pattern.len() + 8);
    out.push_str("^(?:");
    let mut after_escape = false;
    let mut in_char_class = false;
    let mut open_capture = false;
    for c in pattern.chars() {
        if after_escape {
            if capture_quoted && c == '"' {
                // The opening delimiter becomes the capture group, the closing
                // one ends it. PostgreSQL allows only one such region.
                out.push_str(if open_capture { ")" } else { "(" });
                open_capture = !open_capture;
            } else {
                // An escaped character is always literal, inside a bracket
                // expression or out.
                out.push('\\');
                out.push(c);
            }
            after_escape = false;
        } else if escape == Some(c) {
            after_escape = true;
        } else if in_char_class {
            if c == '\\' {
                out.push('\\');
            }
            out.push(c);
            in_char_class = c != ']';
        } else {
            match c {
                '[' => {
                    out.push('[');
                    in_char_class = true;
                }
                '%' => out.push_str(".*"),
                '_' => out.push('.'),
                '(' => out.push_str("(?:"),
                '\\' | '.' | '^' | '$' => {
                    out.push('\\');
                    out.push(c);
                }
                _ => out.push(c),
            }
        }
    }
    // A pattern ending in a lone escape character contributes nothing, exactly
    // as in PostgreSQL's `similar_to_escape` — unlike `LIKE`, `SIMILAR TO` does
    // not reject it.
    out.push_str(")$");
    out
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn similar_to_translates_sql_wildcards_and_keeps_regexp_metacharacters() {
        // (pattern, subject, matches) — verified against PostgreSQL 18.4.
        let cases: &[(&str, &str, bool)] = &[
            ("a%", "abc", true),
            ("_b_", "abc", true),
            ("_b_", "ab", false),
            ("a|b", "abc", false),
            ("a|b", "a", true),
            ("(a|b)bc", "abc", true),
            ("a{1,2}bc", "abc", true),
            ("(ab)*c", "ababc", true),
            ("ab+c", "abbc", true),
            ("ab?c", "ac", true),
            ("[abc]bc", "abc", true),
            ("[^a]bc", "abc", false),
            // `.` is NOT a SIMILAR TO metacharacter: it matches itself only.
            ("a.c", "abc", false),
            ("a.c", "a.c", true),
            ("", "", true),
            ("%", "anything at all", true),
        ];
        for (pattern, subject, expected) in cases {
            let got = similar_match(subject, pattern, Some('\\')).expect("pattern is valid");
            assert!(got == *expected, "{subject:?} SIMILAR TO {pattern:?}");
        }
    }

    #[test]
    fn escape_character_makes_the_next_wildcard_literal() {
        assert!(similar_match("a%c", "a#%c", Some('#')).expect("valid") == true);
        assert!(similar_match("abc", "a#%c", Some('#')).expect("valid") == false);
        // The default escape is a backslash.
        assert!(similar_match("a%c", "a\\%c", Some('\\')).expect("valid") == true);
        // `ESCAPE ''` disables escaping, so `%` stays a wildcard.
        assert!(similar_match("abc", "a%c", None).expect("valid") == true);
    }

    #[test]
    fn escape_clause_accepts_only_an_empty_or_one_character_string() {
        assert!(escape_char(&Datum::Text(String::new())).expect("empty is valid") == None);
        assert!(escape_char(&Datum::Text("#".into())).expect("one char is valid") == Some('#'));
        assert!(
            escape_char(&Datum::Text("ab".into()))
                == Err(ExecError::Type(TypeError::InvalidEscapeString))
        );
    }

    #[test]
    fn a_pattern_ending_in_a_lone_escape_contributes_nothing() {
        // PostgreSQL's similar_to_escape drops it rather than reporting 22025,
        // so the pattern is simply one character shorter.
        assert!(similar_match("a", "a#", Some('#')).expect("valid") == true);
        assert!(similar_match("ab", "#", Some('#')).expect("valid") == false);
        assert!(similar_match("", "#", Some('#')).expect("valid") == true);
    }
}
