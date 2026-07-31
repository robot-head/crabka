//! The `PostgreSQL` array **text** literal format — `array_in` and `array_out`.
//!
//! Both directions are element-type agnostic: [`parse_literal`] splits a literal
//! into a dimension header plus raw element texts (`None` for an unquoted
//! `NULL`) which the caller feeds to the element type's input function, and
//! [`literal_text`] re-quotes already rendered element texts back into the same
//! shape.
//!
//! The grammar is `PostgreSQL`'s in full: nested braces give multiple
//! dimensions, and an optional `[l:u][l:u]…=` prefix gives non-default lower
//! bounds. Elements are stored flat in row-major order — the same layout
//! `PostgreSQL` uses — with the extents in [`ArrayDim`]s alongside.

use crate::{MAX_ARRAY_DIM, TypeError, datum::ArrayDim};

/// A parsed array literal: the dimension header and the flat, row-major element
/// texts (`None` for an unquoted `NULL`).
///
/// `dims` is empty exactly when `elements` is — `PostgreSQL` collapses every
/// element-free literal (`{}`, `{{}}`, `{{},{}}`) to the same zero-dimensional
/// empty array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayLiteral {
    /// One entry per dimension, outermost first.
    pub dims: Vec<ArrayDim>,
    /// The elements in row-major order; `None` is a NULL element.
    pub elements: Vec<Option<String>>,
}

/// Parse an array text literal into its dimensions and element texts.
///
/// # Errors
///
/// Returns 22P02 for a malformed literal (no braces, a stray delimiter, an
/// unterminated quote, trailing junk, sub-arrays of differing length, or a
/// `[l:u]=` header that disagrees with the braces), 2202E for a header whose
/// upper bound precedes its lower bound, and 54000 for more than six dimensions.
pub fn parse_literal(input: &str) -> Result<ArrayLiteral, TypeError> {
    let bytes = input.as_bytes();
    let mut pos = skip_ws(bytes, 0);
    let explicit = read_dim_header(input, bytes, &mut pos)?;
    if bytes.get(pos) != Some(&b'{') {
        return Err(malformed(input));
    }
    let mut state = Scan {
        input,
        elements: Vec::new(),
        extents: Vec::new(),
        leaf_depth: None,
    };
    pos = state.level(bytes, pos, 0)?;
    if skip_ws(bytes, pos) != bytes.len() {
        return Err(malformed(input));
    }
    let Scan {
        elements, extents, ..
    } = state;
    if elements.is_empty() {
        return Ok(ArrayLiteral {
            dims: Vec::new(),
            elements,
        });
    }
    let extents: Vec<usize> = extents.into_iter().map(Option::unwrap_or_default).collect();
    let dims = match explicit {
        None => extents.iter().map(|len| ArrayDim::from_len(*len)).collect(),
        Some(header) => {
            let matches_braces = header.len() == extents.len()
                && header
                    .iter()
                    .zip(&extents)
                    .all(|(d, len)| usize::try_from(d.len) == Ok(*len));
            if !matches_braces {
                return Err(malformed(input));
            }
            header
        }
    };
    Ok(ArrayLiteral { dims, elements })
}

/// Read the optional `[l:u][l:u]…=` prefix, leaving `pos` just past its `=`.
/// `None` means the literal has no prefix at all.
fn read_dim_header(
    input: &str,
    bytes: &[u8],
    pos: &mut usize,
) -> Result<Option<Vec<ArrayDim>>, TypeError> {
    if bytes.get(*pos) != Some(&b'[') {
        return Ok(None);
    }
    let mut dims = Vec::new();
    while bytes.get(*pos) == Some(&b'[') {
        *pos += 1;
        let first = read_bound(input, bytes, pos)?;
        // `[n]` is shorthand for `[1:n]`, exactly as `ReadDimensionInt` treats it.
        let (lower, upper) = if bytes.get(*pos) == Some(&b':') {
            *pos += 1;
            (first, read_bound(input, bytes, pos)?)
        } else {
            (1, first)
        };
        if bytes.get(*pos) != Some(&b']') {
            return Err(malformed(input));
        }
        *pos += 1;
        if upper < lower {
            return Err(TypeError::array_subscript(
                "upper bound cannot be less than lower bound",
            ));
        }
        // The upper bound must leave room for the array's own length, so
        // PostgreSQL refuses a dimension that would end at `INT_MAX`.
        if upper == i32::MAX {
            return Err(TypeError::Coded {
                sqlstate: "54000",
                message: format!("array upper bound is too large: {upper}"),
            });
        }
        let len = i64::from(upper) - i64::from(lower) + 1;
        let len = i32::try_from(len).map_err(|_| TypeError::Overflow)?;
        dims.push(ArrayDim::new(lower, len));
        if dims.len() > MAX_ARRAY_DIM {
            return Err(too_many_dims(dims.len()));
        }
    }
    *pos = skip_ws(bytes, *pos);
    if bytes.get(*pos) != Some(&b'=') {
        return Err(malformed(input));
    }
    *pos = skip_ws(bytes, *pos + 1);
    Ok(Some(dims))
}

/// One signed integer inside a `[l:u]` header entry.
fn read_bound(input: &str, bytes: &[u8], pos: &mut usize) -> Result<i32, TypeError> {
    let start = *pos;
    if matches!(bytes.get(*pos), Some(b'-' | b'+')) {
        *pos += 1;
    }
    let digits = *pos;
    while matches!(bytes.get(*pos), Some(byte) if byte.is_ascii_digit()) {
        *pos += 1;
    }
    if *pos == digits {
        return Err(malformed(input));
    }
    input[start..*pos]
        .parse::<i32>()
        .map_err(|_| TypeError::Coded {
            sqlstate: "54000",
            message: "array bound is out of integer range".into(),
        })
}

/// The brace-scanning state: the flat elements and the extent agreed for each
/// depth so far, so a later sub-array of a different length is rejected.
struct Scan<'a> {
    input: &'a str,
    elements: Vec<Option<String>>,
    extents: Vec<Option<usize>>,
    /// The depth at which scalar elements were found, once one has been.
    leaf_depth: Option<usize>,
}

impl Scan<'_> {
    /// Consume one `{ … }` level starting at `pos`, returning the position just
    /// past its closing brace.
    fn level(&mut self, bytes: &[u8], mut pos: usize, depth: usize) -> Result<usize, TypeError> {
        if depth >= MAX_ARRAY_DIM {
            return Err(too_many_dims(depth + 1));
        }
        if bytes.get(pos) != Some(&b'{') {
            return Err(malformed(self.input));
        }
        pos = skip_ws(bytes, pos + 1);
        if bytes.get(pos) == Some(&b'}') {
            self.record(depth, 0)?;
            return Ok(pos + 1);
        }
        let mut count = 0usize;
        loop {
            pos = skip_ws(bytes, pos);
            if bytes.get(pos) == Some(&b'{') {
                if self.leaf_depth == Some(depth) {
                    return Err(mismatched_subarrays(self.input));
                }
                pos = self.level(bytes, pos, depth + 1)?;
            } else {
                if self.leaf_depth.is_some_and(|leaf| leaf != depth) {
                    return Err(mismatched_subarrays(self.input));
                }
                self.leaf_depth = Some(depth);
                let (element, next) = parse_element(self.input, bytes, pos)?;
                self.elements.push(element);
                pos = next;
            }
            count += 1;
            pos = skip_ws(bytes, pos);
            match bytes.get(pos) {
                Some(b',') => pos += 1,
                Some(b'}') => {
                    self.record(depth, count)?;
                    return Ok(pos + 1);
                }
                _ => return Err(malformed(self.input)),
            }
        }
    }

    /// Agree this level's length with every sibling already seen at `depth`.
    ///
    /// Levels close innermost-first, so `extents` is grown to reach `depth`
    /// rather than pushed onto — the slot's index IS the dimension.
    fn record(&mut self, depth: usize, count: usize) -> Result<(), TypeError> {
        if self.extents.len() <= depth {
            self.extents.resize(depth + 1, None);
        }
        match self.extents[depth] {
            None => {
                self.extents[depth] = Some(count);
                Ok(())
            }
            Some(seen) if seen == count => Ok(()),
            Some(_) => Err(mismatched_subarrays(self.input)),
        }
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
                None => return Err(malformed(input)),
                Some(b'"') => return Ok((Some(out), pos + 1)),
                Some(b'\\') => {
                    // A backslash escapes the next character verbatim.
                    let start = pos + 1;
                    if start >= bytes.len() {
                        return Err(malformed(input));
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
        let mut out = String::new();
        // A backslash anywhere in an unquoted element also stops it spelling
        // NULL: PostgreSQL reads `n\ull` as the four-character string.
        let mut escaped = false;
        // Whitespace is only dropped at the *ends* — `{a b}` is one element.
        let mut trailing_ws = 0usize;
        while let Some(byte) = bytes.get(pos) {
            match byte {
                b',' | b'}' => break,
                b'{' | b'"' => return Err(malformed(input)),
                b'\\' => {
                    let start = pos + 1;
                    if start >= bytes.len() {
                        return Err(malformed(input));
                    }
                    pos = next_char(bytes, start);
                    out.push_str(&input[start..pos]);
                    escaped = true;
                    trailing_ws = 0;
                }
                _ => {
                    let start = pos;
                    pos = next_char(bytes, pos);
                    let text = &input[start..pos];
                    out.push_str(text);
                    if text.chars().all(char::is_whitespace) {
                        trailing_ws += text.len();
                    } else {
                        trailing_ws = 0;
                    }
                }
            }
        }
        out.truncate(out.len() - trailing_ws);
        if out.is_empty() && !escaped {
            return Err(malformed(input));
        }
        // Only an UNQUOTED, unescaped, case-insensitive `NULL` is the null element.
        Ok((
            (escaped || !out.eq_ignore_ascii_case("NULL")).then_some(out),
            pos,
        ))
    }
}

/// Render element texts (`None` = NULL) as an array literal with `dims`'
/// nesting, quoting exactly when `PostgreSQL`'s `array_out` would and emitting
/// the `[l:u]=` header exactly when some lower bound is not 1.
#[must_use]
pub fn literal_text(dims: &[ArrayDim], elements: &[Option<String>]) -> String {
    if dims.is_empty() || elements.is_empty() {
        return "{}".to_string();
    }
    let mut out = String::new();
    if dims.iter().any(|d| d.lower != 1) {
        for dim in dims {
            out.push('[');
            out.push_str(&dim.lower.to_string());
            out.push(':');
            out.push_str(&dim.upper().to_string());
            out.push(']');
        }
        out.push('=');
    }
    push_level(dims, elements, &mut out);
    out
}

/// Render one nesting level: the outermost dimension of `dims` over the slice
/// `elements`, recursing into equal-sized chunks for the inner dimensions.
fn push_level(dims: &[ArrayDim], elements: &[Option<String>], out: &mut String) {
    out.push('{');
    match dims {
        [] | [_] => {
            for (i, element) in elements.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                match element {
                    None => out.push_str("NULL"),
                    Some(text) => push_element(text, out),
                }
            }
        }
        [_, rest @ ..] => {
            let stride: usize = rest
                .iter()
                .map(|d| usize::try_from(d.len).unwrap_or(0))
                .product();
            for (i, slice) in elements.chunks(stride.max(1)).enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_level(rest, slice, out);
            }
        }
    }
    out.push('}');
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

fn malformed(input: &str) -> TypeError {
    TypeError::Coded {
        sqlstate: "22P02",
        message: format!("malformed array literal: \"{input}\""),
    }
}

fn mismatched_subarrays(input: &str) -> TypeError {
    malformed(input)
}

fn too_many_dims(ndims: usize) -> TypeError {
    TypeError::Coded {
        sqlstate: "54000",
        message: format!(
            "number of array dimensions ({ndims}) exceeds the maximum allowed ({MAX_ARRAY_DIM})"
        ),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn some(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|v| Some((*v).to_string())).collect()
    }

    fn flat(elements: Vec<Option<String>>) -> ArrayLiteral {
        let dims = if elements.is_empty() {
            Vec::new()
        } else {
            vec![ArrayDim::from_len(elements.len())]
        };
        ArrayLiteral { dims, elements }
    }

    #[test]
    fn parses_the_one_dimensional_array_literal_grammar() {
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
            // Unquoted elements have surrounding whitespace trimmed, but keep
            // whitespace in the middle: `{a b}` is the one element `a b`.
            ("{  hi  ,  there  }", some(&["hi", "there"])),
            ("{a b}", some(&["a b"])),
            ("{a  b , c}", some(&["a  b", "c"])),
            // Quoted elements keep theirs.
            (r#"{"  hi  "}"#, some(&["  hi  "])),
            ("{héllo}", some(&["héllo"])),
            // A backslash outside quotes escapes the next character, and stops
            // the element spelling NULL.
            (r"{ ab\c }", some(&["abc"])),
            (r"{n\ull}", some(&["null"])),
        ];
        for (input, expected) in cases {
            assert!(
                parse_literal(input).as_ref() == Ok(&flat(expected.clone())),
                "parsing {input:?}"
            );
        }
    }

    #[test]
    fn parses_nested_braces_into_dimensions() {
        let cases: &[(&str, Vec<ArrayDim>, Vec<&str>)] = &[
            (
                "{{1,2},{3,4}}",
                vec![ArrayDim::new(1, 2), ArrayDim::new(1, 2)],
                vec!["1", "2", "3", "4"],
            ),
            (
                "{{{1,2}}}",
                vec![
                    ArrayDim::new(1, 1),
                    ArrayDim::new(1, 1),
                    ArrayDim::new(1, 2),
                ],
                vec!["1", "2"],
            ),
            (
                "  {  {  1 , 2 } ,  { 3,4 }  }  ",
                vec![ArrayDim::new(1, 2), ArrayDim::new(1, 2)],
                vec!["1", "2", "3", "4"],
            ),
            (
                "{{1,2,3},{4,5,6}}",
                vec![ArrayDim::new(1, 2), ArrayDim::new(1, 3)],
                vec!["1", "2", "3", "4", "5", "6"],
            ),
        ];
        for (input, dims, elements) in cases {
            let expected = ArrayLiteral {
                dims: dims.clone(),
                elements: some(elements),
            };
            assert!(parse_literal(input).as_ref() == Ok(&expected), "{input:?}");
        }
        // Every element-free literal collapses to the zero-dimensional array.
        for input in ["{}", "{{}}", "{{},{}}"] {
            assert!(
                parse_literal(input).as_ref() == Ok(&flat(vec![])),
                "{input:?}"
            );
        }
    }

    #[test]
    fn parses_an_explicit_dimension_header() {
        let cases: &[(&str, Vec<ArrayDim>, Vec<&str>)] = &[
            (
                "[2:4]={1,2,3}",
                vec![ArrayDim::new(2, 3)],
                vec!["1", "2", "3"],
            ),
            // `[n]` is shorthand for `[1:n]`.
            ("[2]={1,7}", vec![ArrayDim::new(1, 2)], vec!["1", "7"]),
            ("[-1:0]={7,1}", vec![ArrayDim::new(-1, 2)], vec!["7", "1"]),
            (
                "[0:1][0:1]={{1,2},{3,4}}",
                vec![ArrayDim::new(0, 2), ArrayDim::new(0, 2)],
                vec!["1", "2", "3", "4"],
            ),
            (" [1:2] = {1,2} ", vec![ArrayDim::new(1, 2)], vec!["1", "2"]),
        ];
        for (input, dims, elements) in cases {
            let expected = ArrayLiteral {
                dims: dims.clone(),
                elements: some(elements),
            };
            assert!(parse_literal(input).as_ref() == Ok(&expected), "{input:?}");
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
            "{a,,b}",
            // Sub-arrays of differing length, and elements mixed with them.
            "{{1,2},{3}}",
            "{1,{2}}",
            "{{1},2}",
            // A header that disagrees with the braces.
            "[1:2]={1,2,3}",
            "[1:3]={1,2}",
            "[1:2]={{1,2},{3,4}}",
        ] {
            let error = parse_literal(input).expect_err("rejected");
            assert!(error.sqlstate() == "22P02", "expected 22P02 for {input:?}");
        }
    }

    #[test]
    fn rejects_inverted_bounds_and_excess_dimensions() {
        for input in ["[1:0]={}", "[1:-1]={}"] {
            let error = parse_literal(input).expect_err("rejected");
            assert!(error.sqlstate() == "2202E", "{input:?}");
        }
        let deep = format!("{}1{}", "{".repeat(7), "}".repeat(7));
        assert!(parse_literal(&deep).expect_err("too deep").sqlstate() == "54000");
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
            let dims = flat(elements.clone()).dims;
            assert!(literal_text(&dims, elements) == *expected, "{elements:?}");
        }
    }

    #[test]
    fn renders_dimensions_and_lower_bounds() {
        let cases: &[(Vec<ArrayDim>, Vec<&str>, &str)] = &[
            (
                vec![ArrayDim::new(1, 2), ArrayDim::new(1, 2)],
                vec!["1", "2", "3", "4"],
                "{{1,2},{3,4}}",
            ),
            (
                vec![ArrayDim::new(1, 2), ArrayDim::new(1, 3)],
                vec!["1", "2", "3", "4", "5", "6"],
                "{{1,2,3},{4,5,6}}",
            ),
            (
                vec![ArrayDim::new(2, 3)],
                vec!["1", "2", "3"],
                "[2:4]={1,2,3}",
            ),
            (
                vec![ArrayDim::new(0, 2), ArrayDim::new(0, 2)],
                vec!["1", "2", "3", "4"],
                "[0:1][0:1]={{1,2},{3,4}}",
            ),
            (
                vec![
                    ArrayDim::new(1, 2),
                    ArrayDim::new(1, 2),
                    ArrayDim::new(1, 2),
                ],
                vec!["1", "2", "3", "4", "5", "6", "7", "8"],
                "{{{1,2},{3,4}},{{5,6},{7,8}}}",
            ),
        ];
        for (dims, elements, expected) in cases {
            assert!(literal_text(dims, &some(elements)) == *expected, "{dims:?}");
        }
    }

    #[test]
    fn literals_round_trip_through_the_parser() {
        let rows: &[ArrayLiteral] = &[
            flat(vec![]),
            flat(vec![None]),
            flat(some(&["NULL"])),
            flat(some(&["a,b", "", " ", "\"", "\\", "{x}"])),
            flat(vec![Some("1".into()), None, Some("plain".into())]),
            ArrayLiteral {
                dims: vec![ArrayDim::new(1, 2), ArrayDim::new(1, 2)],
                elements: some(&["1", "2", "3", "4"]),
            },
            ArrayLiteral {
                dims: vec![ArrayDim::new(-2, 2), ArrayDim::new(5, 2)],
                elements: vec![Some("a".into()), None, Some("c".into()), Some("d".into())],
            },
        ];
        for literal in rows {
            let text = literal_text(&literal.dims, &literal.elements);
            assert!(
                parse_literal(&text).as_ref() == Ok(literal),
                "round trip of {text:?}"
            );
        }
    }
}
