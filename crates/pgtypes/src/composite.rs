//! `PostgreSQL`'s composite (record) text format — `record_out` and the field
//! splitter behind `record_in`.
//!
//! The output side renders `(f1,f2,…)`, writing a NULL field as nothing and
//! double-quoting any field that is empty or contains a quote, a backslash, a
//! parenthesis, a comma or whitespace. The input side is the exact inverse: it
//! splits a literal into per-field strings (`None` for a NULL field) without
//! knowing the field types, leaving each field's own input function to the
//! caller.

use crate::error::TypeError;

/// `PostgreSQL`'s `record_out`, given each field already rendered to text
/// (`None` for a NULL field).
#[must_use]
pub fn record_out(fields: &[Option<String>]) -> String {
    let mut out = String::from("(");
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let Some(text) = field else { continue };
        if needs_quoting(text) {
            out.push('"');
            for c in text.chars() {
                if c == '"' || c == '\\' {
                    out.push(c);
                }
                out.push(c);
            }
            out.push('"');
        } else {
            out.push_str(text);
        }
    }
    out.push(')');
    out
}

/// Whether `record_out` wraps this field in double quotes: `PostgreSQL` quotes
/// an empty field (so it is not read back as NULL) and any field holding a
/// character that would otherwise be structural.
#[must_use]
pub fn needs_quoting(text: &str) -> bool {
    text.is_empty()
        || text
            .chars()
            .any(|c| matches!(c, '"' | '\\' | '(' | ')' | ',') || c.is_whitespace())
}

/// Split a composite literal into its fields, `None` for a NULL field.
///
/// This is the structural half of `record_in`: `PostgreSQL` requires the outer
/// parentheses (whitespace either side of them is allowed), reads a zero-length
/// field as NULL, and un-doubles `""` / un-escapes `\x` inside a quoted field.
/// It does **not** trim whitespace around a field — `'( 1 , a )'::t_rec` is
/// `(1," a ")`, because `int4in` trims its own input and `textin` does not — so
/// each field's text arrives here exactly as written and the caller's per-field
/// input function decides what to do with it.
///
/// A zero-field composite cannot be told apart from a one-NULL-field one by the
/// literal alone (`PostgreSQL` drives the split from the target type's column
/// count), so `()` yields one NULL field and the caller reconciles it against a
/// zero-column composite.
///
/// # Errors
///
/// [`TypeError::Coded`] carrying `PostgreSQL`'s 22P02 `malformed record
/// literal` when the literal has no leading `(`, no closing `)`, or junk after
/// the closing parenthesis.
pub fn record_fields(literal: &str) -> Result<Vec<Option<String>>, TypeError> {
    let body = literal
        .trim_start()
        .strip_prefix('(')
        .ok_or_else(|| malformed(literal, "Missing left parenthesis."))?;
    let mut chars = body.chars().peekable();
    let mut fields: Vec<Option<String>> = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut saw_quotes = false;
    loop {
        let Some(c) = chars.next() else {
            return Err(malformed(literal, "Unexpected end of input."));
        };
        if quoted {
            match c {
                '\\' => match chars.next() {
                    Some(escaped) => field.push(escaped),
                    None => return Err(malformed(literal, "Unexpected end of input.")),
                },
                // A doubled quote inside a quoted field is one literal quote;
                // a lone one closes the field.
                '"' if chars.peek() == Some(&'"') => {
                    chars.next();
                    field.push('"');
                }
                '"' => quoted = false,
                other => field.push(other),
            }
            continue;
        }
        match c {
            '"' => {
                quoted = true;
                saw_quotes = true;
            }
            '\\' => match chars.next() {
                Some(escaped) => {
                    saw_quotes = true;
                    field.push(escaped);
                }
                None => return Err(malformed(literal, "Unexpected end of input.")),
            },
            ',' | ')' => {
                fields.push(finish_field(std::mem::take(&mut field), saw_quotes));
                saw_quotes = false;
                if c == ')' {
                    break;
                }
            }
            other => field.push(other),
        }
    }
    // Trailing whitespace after the closing parenthesis is allowed; anything
    // else is junk.
    if chars.any(|c| !c.is_whitespace()) {
        return Err(malformed(literal, "Junk after right parenthesis."));
    }
    Ok(fields)
}

/// A field that carried no quoting at all and is zero-length is NULL; every
/// other field is its text exactly as written, `""` included.
fn finish_field(field: String, saw_quotes: bool) -> Option<String> {
    if !saw_quotes && field.is_empty() {
        None
    } else {
        Some(field)
    }
}

/// `PostgreSQL`'s `malformed record literal` (22P02). The `DETAIL` line it also
/// emits is not part of the wire message crabka reports, so the detail argument
/// documents which of `record_in`'s failures this is rather than being printed.
#[must_use]
pub fn malformed(literal: &str, _detail: &str) -> TypeError {
    TypeError::Coded {
        sqlstate: "22P02",
        message: format!("malformed record literal: \"{literal}\""),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// Every expectation here is `SELECT ROW(…)::text` on `PostgreSQL` 18.4.
    #[test]
    fn record_out_quotes_exactly_what_postgres_quotes() {
        let cases: &[(&[Option<&str>], &str)] = &[
            (&[Some("1"), Some("2")], "(1,2)"),
            (&[Some("1"), None, Some("t")], "(1,,t)"),
            (&[Some("a b")], "(\"a b\")"),
            (&[Some("a,b")], "(\"a,b\")"),
            (&[Some("c\"d")], "(\"c\"\"d\")"),
            (&[Some("a\\b")], "(\"a\\\\b\")"),
            (&[Some("a(b")], "(\"a(b\")"),
            (&[Some("")], "(\"\")"),
            (&[], "()"),
        ];
        for (fields, expected) in cases {
            let owned: Vec<Option<String>> =
                fields.iter().map(|f| f.map(ToString::to_string)).collect();
            assert!(record_out(&owned) == *expected, "record_out({fields:?})");
        }
    }

    /// The inverse: each literal is one `PostgreSQL` renders, read back into the
    /// fields it came from.
    #[test]
    fn record_fields_is_the_inverse_of_record_out() {
        let cases: &[(&str, &[Option<&str>])] = &[
            ("(1,2)", &[Some("1"), Some("2")]),
            ("(1,,t)", &[Some("1"), None, Some("t")]),
            ("(\"a b\")", &[Some("a b")]),
            ("(\"a,b\")", &[Some("a,b")]),
            ("(\"c\"\"d\")", &[Some("c\"d")]),
            ("(\"a\\\\b\")", &[Some("a\\b")]),
            ("(\"\")", &[Some("")]),
            // `()` cannot be told from one NULL field by the literal alone.
            ("()", &[None]),
            // Whitespace around an unquoted field IS part of it: PostgreSQL
            // renders `'( 1 , a )'::t_rec` as `(1," a ")`, leaving `int4in` to
            // trim its own input and `textin` to keep the spaces.
            ("( 1 , 2 )", &[Some(" 1 "), Some(" 2 ")]),
            ("(\" 1 \")", &[Some(" 1 ")]),
            // A quoted run followed by an unquoted one concatenates.
            ("(1,\"a\" )", &[Some("1"), Some("a ")]),
            // Whitespace outside the parentheses is allowed.
            (" (1,2) ", &[Some("1"), Some("2")]),
        ];
        for (literal, expected) in cases {
            let parsed = record_fields(literal).expect("valid composite literal");
            let expected: Vec<Option<String>> = expected
                .iter()
                .map(|f| f.map(ToString::to_string))
                .collect();
            assert!(parsed == expected, "record_fields({literal})");
        }
    }

    #[test]
    fn malformed_composite_literals_are_rejected() {
        for literal in ["1,2)", "(1,2", "(1,2))", "(\"a"] {
            assert!(
                record_fields(literal).is_err(),
                "record_fields({literal}) must fail"
            );
        }
    }
}
