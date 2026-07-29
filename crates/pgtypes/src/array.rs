//! The `PostgreSQL` array **text** literal format — `{a,"quoted, thing",NULL}`.
//!
//! Both directions are element-type agnostic: [`parse_literal`] splits a literal
//! into raw element texts (`None` for an unquoted `NULL`) which the caller feeds
//! to the element type's input function, and [`literal_text`] re-quotes already
//! rendered element texts. Only one dimension is supported — a nested `{` is a
//! 0A000 feature error, matching the deferred multidimensional-array scope.

use crate::TypeError;

/// Split an array text literal into its element texts, `None` for NULL.
///
/// Divergence: a backslash outside a quoted element is rejected rather than
/// escaping the next character. `PostgreSQL`'s `array_out` always quotes an
/// element containing a backslash, so this only affects hand-written literals.
///
/// # Errors
///
/// Returns 22P02 for a malformed literal (no braces, a stray delimiter, an
/// unterminated quote, trailing garbage) and 0A000 for a nested `{`, which would
/// be a multidimensional array.
pub fn parse_literal(input: &str) -> Result<Vec<Option<String>>, TypeError> {
    let bytes = input.as_bytes();
    let mut pos = skip_ws(bytes, 0);
    if bytes.get(pos) != Some(&b'{') {
        return Err(invalid(input));
    }
    pos += 1;
    let mut elements: Vec<Option<String>> = Vec::new();
    pos = skip_ws(bytes, pos);
    if bytes.get(pos) == Some(&b'}') {
        pos += 1;
        return finish(input, bytes, pos, elements);
    }
    loop {
        pos = skip_ws(bytes, pos);
        let (element, next) = parse_element(input, bytes, pos)?;
        elements.push(element);
        pos = skip_ws(bytes, next);
        match bytes.get(pos) {
            Some(b',') => pos += 1,
            Some(b'}') => {
                pos += 1;
                return finish(input, bytes, pos, elements);
            }
            _ => return Err(invalid(input)),
        }
    }
}

/// Reject trailing non-whitespace after the closing brace.
fn finish(
    input: &str,
    bytes: &[u8],
    pos: usize,
    elements: Vec<Option<String>>,
) -> Result<Vec<Option<String>>, TypeError> {
    if skip_ws(bytes, pos) == bytes.len() {
        Ok(elements)
    } else {
        Err(invalid(input))
    }
}

/// Parse one element starting at `pos`, returning it and the position just past
/// it (trailing whitespace of an unquoted element is trimmed, as `PostgreSQL` does).
fn parse_element(
    input: &str,
    bytes: &[u8],
    mut pos: usize,
) -> Result<(Option<String>, usize), TypeError> {
    if bytes.get(pos) == Some(&b'"') {
        pos += 1;
        let mut out = String::new();
        loop {
            match bytes.get(pos) {
                None => return Err(invalid(input)),
                Some(b'"') => return Ok((Some(out), pos + 1)),
                Some(b'\\') => {
                    // A backslash escapes the next character verbatim.
                    let start = pos + 1;
                    if start >= bytes.len() {
                        return Err(invalid(input));
                    }
                    pos = next_char(bytes, start);
                    out.push_str(&input[start..pos]);
                }
                Some(_) => {
                    let start = pos;
                    pos = next_char(bytes, pos);
                    out.push_str(&input[start..pos]);
                }
            }
        }
    } else {
        let start = pos;
        while let Some(byte) = bytes.get(pos) {
            match byte {
                b',' | b'}' => break,
                b'{' => {
                    return Err(TypeError::FeatureNotSupported {
                        message: "multidimensional arrays are not supported".into(),
                    });
                }
                b'"' | b'\\' => return Err(invalid(input)),
                _ => pos = next_char(bytes, pos),
            }
        }
        let raw = input[start..pos].trim_end();
        if raw.is_empty() {
            return Err(invalid(input));
        }
        // Only an UNQUOTED, case-insensitive `NULL` is the null element.
        Ok((
            (!raw.eq_ignore_ascii_case("NULL")).then(|| raw.to_string()),
            pos,
        ))
    }
}

/// Render element texts (`None` = NULL) as an array literal, quoting exactly
/// when `PostgreSQL`'s `array_out` would.
#[must_use]
pub fn literal_text(elements: &[Option<String>]) -> String {
    let mut out = String::from("{");
    for (i, element) in elements.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match element {
            None => out.push_str("NULL"),
            Some(text) => push_element(text, &mut out),
        }
    }
    out.push('}');
    out
}

/// Append one element, quoted (and backslash-escaped) if it is empty, spells
/// `NULL`, or contains a character that would otherwise be structural.
fn push_element(text: &str, out: &mut String) {
    if needs_quotes(text) {
        out.push('"');
        for ch in text.chars() {
            if ch == '"' || ch == '\\' {
                out.push('\\');
            }
            out.push(ch);
        }
        out.push('"');
    } else {
        out.push_str(text);
    }
}

/// Does `text` need quoting inside an array literal?
fn needs_quotes(text: &str) -> bool {
    text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .chars()
            .any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_whitespace())
}

/// Advance one UTF-8 scalar from `pos` (the caller's bytes come from a `&str`).
fn next_char(bytes: &[u8], mut pos: usize) -> usize {
    pos += 1;
    while matches!(bytes.get(pos), Some(byte) if (byte & 0xc0) == 0x80) {
        pos += 1;
    }
    pos
}

fn skip_ws(bytes: &[u8], mut pos: usize) -> usize {
    while matches!(bytes.get(pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        pos += 1;
    }
    pos
}

fn invalid(input: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "array",
        value: input.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn some(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|v| Some((*v).to_string())).collect()
    }

    #[test]
    fn parses_the_array_literal_grammar() {
        let cases: &[(&str, Vec<Option<String>>)] = &[
            ("{}", vec![]),
            ("  {  }  ", vec![]),
            ("{a,b}", some(&["a", "b"])),
            ("{ 1 , 2 }", some(&["1", "2"])),
            ("{NULL}", vec![None]),
            ("{null}", vec![None]),
            ("{nUlL}", vec![None]),
            // Quoted NULL is the four-character string.
            (r#"{"NULL"}"#, some(&["NULL"])),
            (r#"{"a,b"}"#, some(&["a,b"])),
            (r#"{"a b","{}"}"#, some(&["a b", "{}"])),
            (r#"{""}"#, some(&[""])),
            (r#"{"\""}"#, some(&["\""])),
            (r#"{"a\\b"}"#, some(&["a\\b"])),
            ("{1,NULL,3}", vec![Some("1".into()), None, Some("3".into())]),
            // Unquoted elements have surrounding whitespace trimmed.
            ("{  hi  ,  there  }", some(&["hi", "there"])),
            // Quoted elements keep theirs.
            (r#"{"  hi  "}"#, some(&["  hi  "])),
            ("{héllo}", some(&["héllo"])),
        ];
        for (input, expected) in cases {
            assert!(
                parse_literal(input).as_ref() == Ok(expected),
                "parsing {input:?}"
            );
        }
    }

    #[test]
    fn rejects_malformed_array_literals() {
        for input in [
            "",
            "{",
            "}",
            "abc",
            "{a",
            "{a,}",
            "{,a}",
            "{a}}",
            "{a},",
            r#"{"a}"#,
            r#"{"a"b}"#,
            r#"{a"b}"#,
            r"{a\b}",
            "{a,,b}",
        ] {
            assert!(parse_literal(input).is_err(), "expected {input:?} rejected");
        }
    }

    #[test]
    fn nested_braces_are_a_feature_error() {
        let err = parse_literal("{{1,2},{3,4}}").expect_err("multidim");
        assert!(err.sqlstate() == "0A000");
    }

    #[test]
    fn quotes_exactly_the_elements_postgres_quotes() {
        let cases: &[(Vec<Option<String>>, &str)] = &[
            (vec![], "{}"),
            (vec![None], "{NULL}"),
            (some(&["1", "2"]), "{1,2}"),
            (some(&["NULL"]), r#"{"NULL"}"#),
            (some(&["null"]), r#"{"null"}"#),
            (some(&[""]), r#"{""}"#),
            (some(&["a b"]), r#"{"a b"}"#),
            (some(&["a,b"]), r#"{"a,b"}"#),
            (some(&["{}"]), r#"{"{}"}"#),
            (some(&["\""]), r#"{"\""}"#),
            (some(&["a\\b"]), r#"{"a\\b"}"#),
            (some(&["plain"]), "{plain}"),
            (some(&["héllo"]), "{héllo}"),
        ];
        for (elements, expected) in cases {
            assert!(literal_text(elements) == *expected, "{elements:?}");
        }
    }

    #[test]
    fn literals_round_trip_through_the_parser() {
        let rows: &[Vec<Option<String>>] = &[
            vec![],
            vec![None],
            some(&["NULL"]),
            some(&["a,b", "", " ", "\"", "\\", "{x}"]),
            vec![Some("1".into()), None, Some("plain".into())],
        ];
        for elements in rows {
            let text = literal_text(elements);
            assert!(
                parse_literal(&text).as_ref() == Ok(elements),
                "round trip of {text:?}"
            );
        }
    }
}
