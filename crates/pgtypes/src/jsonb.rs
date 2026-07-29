//! `PostgreSQL` `jsonb`: a decomposed, canonically-ordered JSON value.
//!
//! `jsonb` is not stored as source text — `PostgreSQL` parses the input, keeps
//! numbers as `numeric`, discards insignificant whitespace, drops duplicate
//! object keys (last one wins) and stores object keys in a canonical order
//! (shorter keys first, then bytewise). Output is therefore re-rendered from
//! the decomposed form, which is why `'{"b":1,"a":2}'::jsonb` prints as
//! `{"a": 2, "b": 1}`.
//!
//! This module owns that representation ([`JsonbValue`]), a hand-written RFC
//! 8259 parser ([`parse`]) and the canonical serializer ([`JsonbValue::to_text`]).
//! The parser is hand-written rather than delegated to `serde_json` because
//! `PostgreSQL`'s numbers are arbitrary-precision `numeric` values that preserve
//! their input scale (`'1.00'::jsonb` prints `1.00`), which `serde_json::Number`
//! cannot represent without changing that type workspace-wide.

use std::{cmp::Ordering, fmt::Write as _};

use bigdecimal::BigDecimal;

use crate::TypeError;

/// The maximum nesting depth accepted by [`parse`]. `PostgreSQL` guards its
/// recursive descent parser with a stack-depth check; this is the equivalent,
/// so adversarial input cannot overflow the parser's stack.
const MAX_DEPTH: u32 = 512;

/// A decomposed `jsonb` value.
///
/// Objects are **always** held in `PostgreSQL`'s canonical key order (key byte
/// length first, then bytewise) with duplicate keys already resolved, so two
/// `jsonb` values that `PostgreSQL` considers equal are structurally equal here
/// — the property index keys and `GROUP BY` rely on.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonbValue {
    /// The JSON `null` literal — distinct from a SQL NULL.
    Null,
    /// `true` / `false`.
    Bool(bool),
    /// A JSON number, held as `numeric` so scale survives (`1.00` stays `1.00`).
    Number(BigDecimal),
    /// A JSON string (already unescaped).
    String(String),
    /// A JSON array, in input order.
    Array(Vec<JsonbValue>),
    /// A JSON object, in canonical key order with duplicates resolved.
    Object(Vec<(String, JsonbValue)>),
}

// Sound: `BigDecimal`'s `PartialEq` is value equality (`1.0 == 1.00`), which is
// reflexive, symmetric and transitive; every other variant is structural.
impl Eq for JsonbValue {}

impl std::hash::Hash for JsonbValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            JsonbValue::Null => {}
            JsonbValue::Bool(b) => b.hash(state),
            // Scale-insensitive, matching `Eq` (`1.0` and `1.00` are one value).
            JsonbValue::Number(n) => n.normalized().to_string().hash(state),
            JsonbValue::String(s) => s.hash(state),
            JsonbValue::Array(items) => items.hash(state),
            JsonbValue::Object(pairs) => pairs.hash(state),
        }
    }
}

impl PartialOrd for JsonbValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonbValue {
    /// `PostgreSQL`'s `jsonb` btree order: `Object > Array > Boolean > Number >
    /// String > Null`. Within a type: numbers compare numerically, strings
    /// compare bytewise (a documented divergence — `PostgreSQL` uses the database
    /// collation), arrays compare by element count then element-wise, objects by
    /// pair count then key/value pairs in canonical key order.
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (JsonbValue::Null, JsonbValue::Null) => Ordering::Equal,
            (JsonbValue::Bool(a), JsonbValue::Bool(b)) => a.cmp(b),
            (JsonbValue::Number(a), JsonbValue::Number(b)) => a.cmp(b),
            (JsonbValue::String(a), JsonbValue::String(b)) => a.as_bytes().cmp(b.as_bytes()),
            (JsonbValue::Array(a), JsonbValue::Array(b)) => {
                a.len().cmp(&b.len()).then_with(|| a.cmp(b))
            }
            (JsonbValue::Object(a), JsonbValue::Object(b)) => {
                a.len().cmp(&b.len()).then_with(|| {
                    for ((ka, va), (kb, vb)) in a.iter().zip(b.iter()) {
                        let ord = compare_object_keys(ka, kb).then_with(|| va.cmp(vb));
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    Ordering::Equal
                })
            }
            _ => self.type_rank().cmp(&other.type_rank()),
        }
    }
}

impl JsonbValue {
    /// The btree rank of this value's JSON type (higher sorts later): `Object >
    /// Array > Boolean > Number > String > Null`.
    fn type_rank(&self) -> u8 {
        match self {
            JsonbValue::Null => 0,
            JsonbValue::String(_) => 1,
            JsonbValue::Number(_) => 2,
            JsonbValue::Bool(_) => 3,
            JsonbValue::Array(_) => 4,
            JsonbValue::Object(_) => 5,
        }
    }

    /// The `jsonb_typeof` spelling of this value's JSON type.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            JsonbValue::Null => "null",
            JsonbValue::Bool(_) => "boolean",
            JsonbValue::Number(_) => "number",
            JsonbValue::String(_) => "string",
            JsonbValue::Array(_) => "array",
            JsonbValue::Object(_) => "object",
        }
    }

    /// The canonical `PostgreSQL` `jsonb_out` rendering: `", "` between elements,
    /// `": "` after object keys, objects in canonical key order, numbers as
    /// plain decimals (never scientific notation).
    #[must_use]
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        self.write_text(&mut out);
        out
    }

    fn write_text(&self, out: &mut String) {
        match self {
            JsonbValue::Null => out.push_str("null"),
            JsonbValue::Bool(true) => out.push_str("true"),
            JsonbValue::Bool(false) => out.push_str("false"),
            JsonbValue::Number(n) => out.push_str(&crate::numeric::to_text(n)),
            JsonbValue::String(s) => write_json_string(s, out),
            JsonbValue::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    item.write_text(out);
                }
                out.push(']');
            }
            JsonbValue::Object(pairs) => {
                out.push('{');
                for (i, (key, value)) in pairs.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    write_json_string(key, out);
                    out.push_str(": ");
                    value.write_text(out);
                }
                out.push('}');
            }
        }
    }

    /// Build an object from `pairs`, applying `PostgreSQL`'s canonical key order
    /// and last-wins duplicate-key resolution. Use this instead of constructing
    /// [`JsonbValue::Object`] directly, which assumes the invariant already holds.
    #[must_use]
    pub fn object_from_pairs(pairs: Vec<(String, JsonbValue)>) -> Self {
        JsonbValue::Object(canonicalize_pairs(pairs))
    }

    /// Look up an object member by key (`None` for a non-object or a missing key).
    #[must_use]
    pub fn object_get(&self, key: &str) -> Option<&JsonbValue> {
        match self {
            JsonbValue::Object(pairs) => pairs
                .binary_search_by(|(k, _)| compare_object_keys(k, key))
                .ok()
                .map(|i| &pairs[i].1),
            _ => None,
        }
    }

    /// Recursively normalize every number to its shortest exact form (`1.00` →
    /// `1`, `-0.0` → `0`), returning `None` when nothing changed.
    ///
    /// Index keys are compared as raw encoded bytes, so two values that compare
    /// equal must encode identically; scale is the one part of a `jsonb` number
    /// that equality ignores but the encoding would otherwise preserve.
    #[must_use]
    pub fn normalized_numbers(&self) -> Option<Self> {
        match self {
            JsonbValue::Number(n) => {
                let normalized = crate::numeric::canonical(n.normalized());
                // Normalization only ever changes the scale (the value is equal
                // by definition), so the scale is the whole change detector.
                (normalized.fractional_digit_count() != n.fractional_digit_count())
                    .then_some(JsonbValue::Number(normalized))
            }
            JsonbValue::Array(items) => {
                let mut changed = false;
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    match item.normalized_numbers() {
                        Some(v) => {
                            changed = true;
                            out.push(v);
                        }
                        None => out.push(item.clone()),
                    }
                }
                changed.then_some(JsonbValue::Array(out))
            }
            JsonbValue::Object(pairs) => {
                let mut changed = false;
                let mut out = Vec::with_capacity(pairs.len());
                for (key, value) in pairs {
                    match value.normalized_numbers() {
                        Some(v) => {
                            changed = true;
                            out.push((key.clone(), v));
                        }
                        None => out.push((key.clone(), value.clone())),
                    }
                }
                changed.then_some(JsonbValue::Object(out))
            }
            JsonbValue::Null | JsonbValue::Bool(_) | JsonbValue::String(_) => None,
        }
    }
}

impl std::fmt::Display for JsonbValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_text())
    }
}

/// `PostgreSQL`'s object-key order: shorter keys first, then bytewise.
#[must_use]
pub fn compare_object_keys(a: &str, b: &str) -> Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

/// Sort `pairs` into canonical key order and drop duplicate keys, keeping the
/// last occurrence (`PostgreSQL`'s `jsonb` duplicate-key rule).
fn canonicalize_pairs(mut pairs: Vec<(String, JsonbValue)>) -> Vec<(String, JsonbValue)> {
    // A stable sort keeps duplicates in input order, so the last one survives.
    pairs.sort_by(|(a, _), (b, _)| compare_object_keys(a, b));
    let mut out: Vec<(String, JsonbValue)> = Vec::with_capacity(pairs.len());
    for pair in pairs {
        if out.last().is_some_and(|(k, _)| *k == pair.0) {
            out.pop();
        }
        out.push(pair);
    }
    out
}

/// Append `s` as a JSON string literal, escaping exactly the characters
/// `PostgreSQL`'s `escape_json` escapes (`"`, `\`, and the C0 controls; `/` and
/// non-ASCII are emitted raw).
fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Parse `jsonb` input text (RFC 8259, `PostgreSQL`'s `jsonb_in`).
///
/// # Errors
///
/// Returns 22P02 for malformed JSON (bad syntax, a duplicate escape, trailing
/// garbage, `NaN`/`Infinity`, a leading zero), 22003 for a number that overflows
/// the `numeric` format, and 54001 when nesting exceeds [`MAX_DEPTH`].
pub fn parse(input: &str) -> Result<JsonbValue, TypeError> {
    let mut parser = Parser {
        src: input.as_bytes(),
        pos: 0,
        input,
    };
    parser.skip_ws();
    let value = parser.value(0)?;
    parser.skip_ws();
    if parser.pos != parser.src.len() {
        return Err(parser.invalid());
    }
    Ok(value)
}

struct Parser<'a> {
    src: &'a [u8],
    pos: usize,
    input: &'a str,
}

impl Parser<'_> {
    fn invalid(&self) -> TypeError {
        TypeError::InvalidText {
            type_name: "json",
            value: self.input.to_string(),
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    /// Consume `lit` (an ASCII literal) or fail.
    fn literal(&mut self, lit: &str) -> Result<(), TypeError> {
        if self.src[self.pos..].starts_with(lit.as_bytes()) {
            self.pos += lit.len();
            Ok(())
        } else {
            Err(self.invalid())
        }
    }

    fn value(&mut self, depth: u32) -> Result<JsonbValue, TypeError> {
        if depth > MAX_DEPTH {
            return Err(TypeError::Domain {
                sqlstate: "54001",
                message: "stack depth limit exceeded",
            });
        }
        match self.peek().ok_or_else(|| self.invalid())? {
            b'n' => {
                self.literal("null")?;
                Ok(JsonbValue::Null)
            }
            b't' => {
                self.literal("true")?;
                Ok(JsonbValue::Bool(true))
            }
            b'f' => {
                self.literal("false")?;
                Ok(JsonbValue::Bool(false))
            }
            b'"' => self.string().map(JsonbValue::String),
            b'[' => self.array(depth),
            b'{' => self.object(depth),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err(self.invalid()),
        }
    }

    fn array(&mut self, depth: u32) -> Result<JsonbValue, TypeError> {
        self.pos += 1; // '['
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(JsonbValue::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value(depth + 1)?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(JsonbValue::Array(items));
                }
                _ => return Err(self.invalid()),
            }
        }
    }

    fn object(&mut self, depth: u32) -> Result<JsonbValue, TypeError> {
        self.pos += 1; // '{'
        let mut pairs = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(JsonbValue::Object(pairs));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err(self.invalid());
            }
            let key = self.string()?;
            self.skip_ws();
            if self.peek() != Some(b':') {
                return Err(self.invalid());
            }
            self.pos += 1;
            self.skip_ws();
            pairs.push((key, self.value(depth + 1)?));
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(JsonbValue::Object(canonicalize_pairs(pairs)));
                }
                _ => return Err(self.invalid()),
            }
        }
    }

    fn string(&mut self) -> Result<String, TypeError> {
        self.pos += 1; // opening quote
        let mut out = String::new();
        loop {
            let byte = self.peek().ok_or_else(|| self.invalid())?;
            match byte {
                b'"' => {
                    self.pos += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.pos += 1;
                    self.escape(&mut out)?;
                }
                // RFC 8259 forbids raw C0 controls inside a string.
                0x00..=0x1f => return Err(self.invalid()),
                _ => {
                    let start = self.pos;
                    // Advance over one whole UTF-8 scalar (the input is a `&str`,
                    // so continuation bytes are always well-formed).
                    self.pos += 1;
                    while matches!(self.peek(), Some(b) if (b & 0xc0) == 0x80) {
                        self.pos += 1;
                    }
                    out.push_str(&self.input[start..self.pos]);
                }
            }
        }
    }

    fn escape(&mut self, out: &mut String) -> Result<(), TypeError> {
        let byte = self.peek().ok_or_else(|| self.invalid())?;
        self.pos += 1;
        let ch = match byte {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{8}',
            b'f' => '\u{c}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => return self.unicode_escape(out),
            _ => return Err(self.invalid()),
        };
        out.push(ch);
        Ok(())
    }

    fn unicode_escape(&mut self, out: &mut String) -> Result<(), TypeError> {
        let first = self.hex4()?;
        let code = if (0xd800..0xdc00).contains(&first) {
            // A high surrogate must be followed by `\uXXXX` low surrogate.
            if !self.src[self.pos..].starts_with(b"\\u") {
                return Err(self.invalid());
            }
            self.pos += 2;
            let low = self.hex4()?;
            if !(0xdc00..0xe000).contains(&low) {
                return Err(self.invalid());
            }
            0x1_0000 + ((first - 0xd800) << 10) + (low - 0xdc00)
        } else if (0xdc00..0xe000).contains(&first) {
            // A lone low surrogate is invalid.
            return Err(self.invalid());
        } else {
            first
        };
        // PostgreSQL rejects `\u0000` for `jsonb`: it cannot be stored in `text`.
        let ch = char::from_u32(code)
            .filter(|c| *c != '\0')
            .ok_or_else(|| self.invalid())?;
        out.push(ch);
        Ok(())
    }

    fn hex4(&mut self) -> Result<u32, TypeError> {
        // Copy the slice reference out of `self` so bumping `self.pos` below is
        // not blocked by a borrow of `self`.
        let src = self.src;
        let raw = src
            .get(self.pos..self.pos + 4)
            .ok_or_else(|| self.invalid())?;
        let mut code = 0u32;
        for byte in raw {
            let digit = char::from(*byte)
                .to_digit(16)
                .ok_or_else(|| self.invalid())?;
            code = code * 16 + digit;
        }
        self.pos += 4;
        Ok(code)
    }

    /// RFC 8259 number: `-? (0 | [1-9][0-9]*) (\.[0-9]+)? ([eE][+-]?[0-9]+)?`.
    /// `NaN`, `Infinity`, `+1`, `01` and `1.` are all rejected.
    fn number(&mut self) -> Result<JsonbValue, TypeError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => self.digits()?,
            _ => return Err(self.invalid()),
        }
        if self.peek() == Some(b'.') {
            self.pos += 1;
            self.digits()?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            self.digits()?;
        }
        let text = &self.input[start..self.pos];
        // `numeric::parse` rejects values outside PostgreSQL's numeric format
        // (an adversarial exponent) without materializing their digits.
        crate::numeric::parse(text)
            .map(JsonbValue::Number)
            .ok_or(TypeError::Overflow)
    }

    /// Consume one or more ASCII digits.
    fn digits(&mut self) -> Result<(), TypeError> {
        let start = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        if self.pos == start {
            Err(self.invalid())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use assert2::assert;

    use super::*;

    fn num(s: &str) -> JsonbValue {
        JsonbValue::Number(crate::numeric::parse(s).expect("numeric literal"))
    }

    fn hash_of(v: &JsonbValue) -> u64 {
        let mut hasher = DefaultHasher::new();
        v.hash(&mut hasher);
        hasher.finish()
    }

    /// The equal-values-hash-equally test cannot distinguish a correct `Hash`
    /// from a constant one, so pin that the hash actually discriminates.
    /// Collisions are permitted by the `Hash` contract, but a handful of
    /// clearly-different values colliding would mean the impl stopped reading
    /// its input — the same check the `Datum` instant test already makes.
    #[test]
    fn hashing_discriminates_between_distinct_values() {
        let distinct = [
            JsonbValue::Null,
            JsonbValue::Bool(true),
            JsonbValue::Bool(false),
            num("1"),
            num("2"),
            JsonbValue::String("a".to_owned()),
            JsonbValue::String("b".to_owned()),
            JsonbValue::Array(vec![num("1")]),
            JsonbValue::Object(vec![("a".to_owned(), num("1"))]),
        ];
        for (i, left) in distinct.iter().enumerate() {
            for right in &distinct[i + 1..] {
                assert!(hash_of(left) != hash_of(right), "{left:?} vs {right:?}");
            }
        }
        // …while equal values still agree, including across numeric scale.
        assert!(hash_of(&num("1.0")) == hash_of(&num("1.00")));
    }

    /// `PartialOrd` delegates to `Ord`, but every caller in the crate reaches
    /// for `cmp` directly, so nothing else exercises the delegation or the
    /// comparison operators it powers.
    #[test]
    fn partial_ord_agrees_with_ord() {
        let cases = [
            (JsonbValue::Null, JsonbValue::Null),
            (JsonbValue::Null, JsonbValue::String("a".to_owned())),
            (num("1"), num("2")),
            (JsonbValue::Bool(false), JsonbValue::Bool(true)),
            (JsonbValue::Array(vec![]), JsonbValue::Object(vec![])),
        ];
        for (left, right) in cases {
            assert!(
                left.partial_cmp(&right) == Some(left.cmp(&right)),
                "{left:?} vs {right:?}"
            );
        }
        // The operators PartialOrd powers, which a `None` would silently break.
        assert!(JsonbValue::Null < JsonbValue::String(String::new()));
        assert!(num("2") > num("1"));
        assert!(JsonbValue::Null <= JsonbValue::Null);
    }

    #[test]
    fn parses_the_json_value_grammar() {
        let cases: &[(&str, JsonbValue)] = &[
            ("null", JsonbValue::Null),
            ("true", JsonbValue::Bool(true)),
            ("false", JsonbValue::Bool(false)),
            ("  1  ", num("1")),
            ("-2.5", num("-2.5")),
            ("1e5", num("100000")),
            ("\"hi\"", JsonbValue::String("hi".into())),
            ("[]", JsonbValue::Array(vec![])),
            ("{}", JsonbValue::Object(vec![])),
            (
                "[1, [2]]",
                JsonbValue::Array(vec![num("1"), JsonbValue::Array(vec![num("2")])]),
            ),
            (
                "{\"a\": null}",
                JsonbValue::Object(vec![("a".into(), JsonbValue::Null)]),
            ),
        ];
        for (input, expected) in cases {
            assert!(parse(input).as_ref() == Ok(expected), "parsing {input:?}");
        }
    }

    #[test]
    fn rejects_malformed_json() {
        for input in [
            "",
            "  ",
            "nul",
            "tru",
            "NaN",
            "Infinity",
            "-Infinity",
            "01",
            "-01",
            "+1",
            "1.",
            ".5",
            "1e",
            "1e+",
            "0x10",
            "[1,]",
            "[1 2]",
            "[",
            "]",
            "{\"a\":1,}",
            "{a:1}",
            "{\"a\"}",
            "{\"a\":}",
            "{}{}",
            "1 2",
            "\"unterminated",
            "\"bad\\escape\"",
            "\"\\u12\"",
            "\"\\ud800\"",
            "\"\\udc00\"",
            "\"\\u0000\"",
            "\"raw\ttab\"",
            "'single'",
        ] {
            assert!(parse(input).is_err(), "expected {input:?} to be rejected");
        }
    }

    /// Every two-character escape RFC 8259 defines, decoded and re-rendered.
    #[test]
    fn two_character_escapes_decode_and_round_trip() {
        // (input, decoded scalar, canonical rendering)
        let cases: &[(&str, &str, &str)] = &[
            (r#""\"""#, "\"", r#""\"""#),
            (r#""\\""#, "\\", r#""\\""#),
            // `/` may be escaped on input but is never escaped on output.
            (r#""\/""#, "/", r#""/""#),
            (r#""\b""#, "\u{8}", r#""\b""#),
            (r#""\f""#, "\u{c}", r#""\f""#),
            (r#""\n""#, "\n", r#""\n""#),
            (r#""\r""#, "\r", r#""\r""#),
            (r#""\t""#, "\t", r#""\t""#),
        ];
        for (input, decoded, rendered) in cases {
            let value = parse(input).expect("parse");
            assert!(
                value == JsonbValue::String((*decoded).to_owned()),
                "decoding {input}"
            );
            assert!(value.to_text() == *rendered, "rendering {input}");
        }
        // All of them together, inside one string, in one order-sensitive pass.
        assert!(
            parse(r#""a\bb\fc\rd""#).expect("parse")
                == JsonbValue::String("a\u{8}b\u{c}c\rd".to_owned())
        );
    }

    /// Each of `units` written as a `\uXXXX` escape, in the requested
    /// hex-digit case. Built rather than spelled out so the tests below can
    /// name code units directly and cover both spellings.
    fn escapes(units: &[u16], upper: bool) -> String {
        units
            .iter()
            .map(|unit| {
                if upper {
                    format!("\\u{unit:04X}")
                } else {
                    format!("\\u{unit:04x}")
                }
            })
            .collect()
    }

    /// Wrap `body` in JSON string quotes verbatim, escaping nothing.
    fn quoted(body: &str) -> String {
        format!("\"{body}\"")
    }

    /// `\uXXXX`, including the surrogate-pair arithmetic. A pair decodes to
    /// `0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)`; each case below picks
    /// code units where every operand of that expression changes the result.
    #[test]
    fn unicode_escapes_decode_including_surrogate_pairs() {
        // (UTF-16 code units of the escape sequence, decoded content)
        let cases: &[(&[u16], &str)] = &[
            (&[0x0041], "A"),
            (&[0x0020], " "),
            (&[0x00e9], "\u{e9}"),
            (&[0xfffd], "\u{fffd}"),
            (&[0xffff], "\u{ffff}"),
            // Surrogate pairs: the lowest astral scalar, U+1F600, and the
            // highest scalar there is.
            (&[0xd800, 0xdc00], "\u{10000}"),
            (&[0xd83d, 0xde00], "\u{1f600}"),
            (&[0xdbff, 0xdfff], "\u{10ffff}"),
            // Two pairs back to back, so no half leaks into the next escape.
            (&[0xd83d, 0xde00, 0xd83d, 0xde00], "\u{1f600}\u{1f600}"),
        ];
        for (units, expected) in cases {
            for upper in [false, true] {
                let input = quoted(&escapes(units, upper));
                let value = parse(&input).expect("parse");
                assert!(
                    value == JsonbValue::String((*expected).to_owned()),
                    "decoding {input}"
                );
            }
        }
        // An escape sits between the raw characters around it, and non-ASCII is
        // re-rendered raw rather than re-escaped.
        let mixed = quoted(&format!("x{}y", escapes(&[0xd83d, 0xde00], false)));
        let value = parse(&mixed).expect("parse");
        assert!(value == JsonbValue::String("x\u{1f600}y".to_owned()));
        assert!(value.to_text() == "\"x\u{1f600}y\"");
    }

    #[test]
    fn rejects_malformed_unicode_escapes() {
        let high = escapes(&[0xd83d], false);
        let inputs = [
            // Truncated, and non-hex digits.
            quoted("\\u12"),
            quoted("\\u00g0"),
            quoted("\\u 041"),
            // A lone high surrogate: at the end of the string, before raw text,
            // and before an escape that is not `\u`.
            quoted(&high),
            quoted(&format!("{high}A")),
            quoted(&format!("{high}\\n")),
            // A high surrogate followed by a non-surrogate `\u` escape, and by
            // a second high surrogate.
            quoted(&format!("{high}{}", escapes(&[0x0041], false))),
            quoted(&format!("{high}{high}")),
            // Lone low surrogates.
            quoted(&escapes(&[0xdc00], false)),
            quoted(&escapes(&[0xde00], false)),
            // NUL cannot be stored in `text`.
            quoted(&escapes(&[0x0000], false)),
        ];
        for input in &inputs {
            assert!(parse(input).is_err(), "expected {input:?} to be rejected");
        }
    }

    /// The string scanner walks raw UTF-8 by hand (one lead byte plus its
    /// continuation bytes), so multi-byte scalars must survive intact.
    #[test]
    fn raw_multi_byte_utf8_in_strings_is_preserved() {
        let cases: &[(&str, &str)] = &[
            ("\"\u{e9}\"", "\u{e9}"),
            ("\"na\u{ef}ve caf\u{e9}\"", "na\u{ef}ve caf\u{e9}"),
            ("\"\u{65e5}\u{672c}\u{8a9e}\"", "\u{65e5}\u{672c}\u{8a9e}"),
            ("\"\u{1f600} emoji\"", "\u{1f600} emoji"),
            ("\"a\u{e9}\u{1f600}b\"", "a\u{e9}\u{1f600}b"),
        ];
        for (input, expected) in cases {
            let value = parse(input).expect("parse");
            assert!(
                value == JsonbValue::String((*expected).to_owned()),
                "decoding {input}"
            );
            assert!(value.to_text() == *input, "non-ASCII is emitted raw");
        }
        // Object keys go through the same scanner.
        let obj = parse("{\"\u{e9}\u{1f600}\": 1}").expect("parse");
        assert!(obj.object_get("\u{e9}\u{1f600}") == Some(&num("1")));
    }

    /// C0 controls without a shorthand escape are written `\u00XX`; U+0020 and
    /// everything above it is emitted raw.
    #[test]
    fn control_character_escaping_stops_at_u0020() {
        for unit in [0x0000_u16, 0x0001, 0x001e, 0x001f] {
            let ch = char::from_u32(u32::from(unit)).expect("scalar value");
            let text = JsonbValue::String(ch.to_string()).to_text();
            assert!(
                text == quoted(&escapes(&[unit], false)),
                "U+{unit:04X} rendered as {text:?}"
            );
        }
        for ch in [' ', '!', '~'] {
            let text = JsonbValue::String(ch.to_string()).to_text();
            assert!(text == format!("\"{ch}\""), "{ch:?} rendered as {text:?}");
        }
        // Controls that do have a shorthand escape keep it.
        assert!(JsonbValue::String("\u{8}".to_owned()).to_text() == r#""\b""#);
        // Escaped controls survive a round trip through the parser.
        let text = JsonbValue::String("\u{1f}".to_owned()).to_text();
        assert!(parse(&text).expect("reparse") == JsonbValue::String("\u{1f}".to_owned()));
    }

    #[test]
    fn display_renders_the_canonical_text() {
        let value = parse(r#"{"b": 1, "a": [null, "x"]}"#).expect("parse");
        assert!(value.to_string() == r#"{"a": [null, "x"], "b": 1}"#);
        assert!(format!("{value}") == value.to_text());
        assert!(JsonbValue::Null.to_string() == "null");
        assert!(format!("{}", JsonbValue::String("\t".to_owned())) == r#""\t""#);
    }

    #[test]
    fn duplicate_object_keys_keep_the_last_value() {
        assert!(
            parse(r#"{"a": 1, "b": 2, "a": 3}"#)
                .expect("parse")
                .to_text()
                == r#"{"a": 3, "b": 2}"#
        );
        // Three occurrences still resolve to the final one.
        assert!(
            parse(r#"{"k": 1, "k": 2, "k": 3}"#)
                .expect("parse")
                .to_text()
                == r#"{"k": 3}"#
        );
    }

    #[test]
    fn objects_are_stored_in_postgres_key_order() {
        // Shorter keys sort first, then bytewise — NOT plain lexicographic.
        let v = parse(r#"{"bb": 1, "a": 2, "cc": 3, "b": 4}"#).expect("parse");
        assert!(v.to_text() == r#"{"a": 2, "b": 4, "bb": 1, "cc": 3}"#);
        // Key order is a storage property: two spellings of the same object are
        // the same value.
        assert!(parse(r#"{"b":2,"a":1}"#).expect("p") == parse(r#"{"a":1,"b":2}"#).expect("p"));
    }

    #[test]
    fn canonical_text_matches_postgres_jsonb_out() {
        let cases: &[(&str, &str)] = &[
            (r#"{"b":1,"a":2}"#, r#"{"a": 2, "b": 1}"#),
            (r"[1,2,  3]", "[1, 2, 3]"),
            // Scale is preserved (jsonb numbers are `numeric`).
            ("1.00", "1.00"),
            ("[1.10, 2.0]", "[1.10, 2.0]"),
            // Never scientific notation, in either direction.
            ("1e5", "100000"),
            ("1e-3", "0.001"),
            ("-0", "0"),
            (r#"{"a":{"b":[]}}"#, r#"{"a": {"b": []}}"#),
            // Escapes: `/` and non-ASCII stay raw; controls are escaped.
            (r#""a\/b""#, r#""a/b""#),
            (r#""\u00e9""#, "\"\u{e9}\""),
            ("\"tab\\there\"", r#""tab\there""#),
            (r#""\u0001""#, r#""\u0001""#),
            (r#""q\"b\\s""#, r#""q\"b\\s""#),
        ];
        for (input, expected) in cases {
            let text = parse(input).expect("parse").to_text();
            assert!(text == *expected, "{input:?} rendered as {text:?}");
        }
    }

    #[test]
    fn text_round_trips_through_the_parser() {
        for input in [
            r#"{"a": 1, "b": [1, 2, {"c": "d"}], "e": null, "f": true}"#,
            r#"[[], {}, "", 0]"#,
            r#""line\nbreak""#,
        ] {
            let once = parse(input).expect("parse").to_text();
            let twice = parse(&once).expect("reparse").to_text();
            assert!(once == twice);
            assert!(once == input, "{input:?} is already canonical");
        }
    }

    #[test]
    fn btree_order_ranks_types_like_postgres() {
        // One representative of each JSON type, in ascending jsonb order.
        let ascending = [
            JsonbValue::Null,
            JsonbValue::String("z".into()),
            num("9999"),
            JsonbValue::Bool(false),
            JsonbValue::Array(vec![]),
            JsonbValue::Object(vec![]),
        ];
        for (i, a) in ascending.iter().enumerate() {
            for (j, b) in ascending.iter().enumerate() {
                let expected = i.cmp(&j);
                assert!(a.cmp(b) == expected, "{a} vs {b}");
            }
        }
    }

    #[test]
    fn btree_order_within_each_type() {
        let cases: &[(JsonbValue, JsonbValue, Ordering)] = &[
            (
                JsonbValue::Bool(false),
                JsonbValue::Bool(true),
                Ordering::Less,
            ),
            (num("2"), num("10"), Ordering::Less),
            (num("1.0"), num("1.00"), Ordering::Equal),
            (
                JsonbValue::String("a".into()),
                JsonbValue::String("b".into()),
                Ordering::Less,
            ),
            // Arrays: fewer elements first, then element-wise.
            (
                JsonbValue::Array(vec![num("9")]),
                JsonbValue::Array(vec![num("1"), num("1")]),
                Ordering::Less,
            ),
            (
                JsonbValue::Array(vec![num("1"), num("2")]),
                JsonbValue::Array(vec![num("1"), num("3")]),
                Ordering::Less,
            ),
            // Objects: fewer pairs first, then key/value pairs in stored order.
            (
                JsonbValue::Object(vec![("a".into(), num("1"))]),
                JsonbValue::Object(vec![("a".into(), num("1")), ("b".into(), num("1"))]),
                Ordering::Less,
            ),
            (
                JsonbValue::Object(vec![("a".into(), num("1"))]),
                JsonbValue::Object(vec![("a".into(), num("2"))]),
                Ordering::Less,
            ),
            (
                JsonbValue::Object(vec![("a".into(), num("1"))]),
                JsonbValue::Object(vec![("b".into(), num("0"))]),
                Ordering::Less,
            ),
        ];
        for (a, b, expected) in cases {
            assert!(a.cmp(b) == *expected, "{a} vs {b}");
            assert!(b.cmp(a) == expected.reverse(), "{b} vs {a}");
        }
    }

    #[test]
    fn equal_values_hash_equally_regardless_of_scale_or_key_order() {
        let a = parse(r#"{"b": 1.0, "a": [2.0]}"#).expect("a");
        let b = parse(r#"{"a": [2.00], "b": 1.00}"#).expect("b");
        assert!(a == b);
        assert!(hash_of(&a) == hash_of(&b));
        let c = parse(r#"{"a": [2.0], "b": 2.0}"#).expect("c");
        assert!(a != c);
    }

    #[test]
    fn normalized_numbers_strips_scale_recursively() {
        let v = parse(r#"{"a": [1.00, 2], "b": -0.0}"#).expect("parse");
        let normalized = v.normalized_numbers().expect("scale was stripped");
        assert!(normalized.to_text() == r#"{"a": [1, 2], "b": 0}"#);
        // Already-normal values report no change (so callers can borrow).
        assert!(normalized.normalized_numbers() == None);
        assert!(parse(r#"[1, "x", null]"#).expect("p").normalized_numbers() == None);
        // Normalization preserves equality.
        assert!(normalized == v);
    }

    #[test]
    fn deeply_nested_input_errors_instead_of_overflowing_the_stack() {
        let deep = format!("{}{}", "[".repeat(2000), "]".repeat(2000));
        let err = parse(&deep).expect_err("too deep");
        assert!(err.sqlstate() == "54001");
        // Just inside the limit still parses.
        let ok = format!("{}{}", "[".repeat(100), "]".repeat(100));
        assert!(parse(&ok).is_ok());
    }

    /// The depth guard is exact: the deepest accepted value nests to
    /// [`MAX_DEPTH`], one more is 54001. Objects count towards it like arrays.
    #[test]
    fn nesting_is_accepted_up_to_max_depth_and_rejected_one_past_it() {
        // `n` nested arrays put the innermost value at depth `n - 1`; `n`
        // nested objects put their innermost member value at depth `n`.
        let arrays = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        let objects = |n: usize| format!("{}1{}", r#"{"a":"#.repeat(n), "}".repeat(n));
        let limit = MAX_DEPTH as usize;

        assert!(parse(&arrays(limit + 1)).is_ok(), "arrays at the limit");
        assert!(parse(&objects(limit)).is_ok(), "objects at the limit");

        let array_err = parse(&arrays(limit + 2)).expect_err("arrays one too deep");
        assert!(array_err.sqlstate() == "54001");
        let object_err = parse(&objects(limit + 1)).expect_err("objects one too deep");
        assert!(object_err.sqlstate() == "54001");
    }

    #[test]
    fn numbers_outside_the_numeric_format_are_out_of_range() {
        let err = parse("1e1000000000").expect_err("overflow");
        assert!(err == TypeError::Overflow);
    }

    #[test]
    fn object_get_uses_canonical_key_order() {
        let v = parse(r#"{"bb": 1, "a": 2}"#).expect("parse");
        assert!(v.object_get("a") == Some(&num("2")));
        assert!(v.object_get("bb") == Some(&num("1")));
        assert!(v.object_get("zz") == None);
        assert!(JsonbValue::Null.object_get("a") == None);
    }

    #[test]
    fn object_from_pairs_applies_the_storage_invariant() {
        let v = JsonbValue::object_from_pairs(vec![
            ("bb".into(), num("1")),
            ("a".into(), num("2")),
            ("bb".into(), num("3")),
        ]);
        assert!(v.to_text() == r#"{"a": 2, "bb": 3}"#);
    }

    #[test]
    fn type_name_matches_jsonb_typeof() {
        let cases: &[(&str, &str)] = &[
            ("null", "null"),
            ("true", "boolean"),
            ("1", "number"),
            (r#""s""#, "string"),
            ("[]", "array"),
            ("{}", "object"),
        ];
        for (input, expected) in cases {
            assert!(parse(input).expect("parse").type_name() == *expected);
        }
    }
}
