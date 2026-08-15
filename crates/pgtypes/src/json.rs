//! `PostgreSQL` `json`: the input text, validated but never rewritten.
//!
//! `json` and `jsonb` are two types over one syntax. `jsonb_in` decomposes its
//! input — numbers become `numeric`, insignificant whitespace is dropped,
//! duplicate object keys collapse last-wins, and the surviving keys are stored
//! in a canonical order — so `'{"b":1,   "a":2,  "b":3}'::jsonb` prints
//! `{"a": 2, "b": 3}`. `json_in` only *validates*: it keeps the original bytes,
//! so the same literal cast to `json` prints back exactly as written. Every
//! difference between the two types follows from that one decision, including
//! why `jsonb` has an equality operator and `json` has none.
//!
//! This module owns the syntax both types share, ported from `jsonapi.c`:
//!
//!   * [`Lexer`] — the RFC 8259 lexer, tracking `token_start`, `token_terminator`
//!     and the current line the way `JsonLexContext` does, because those three
//!     are what `json_errdetail` and `report_json_context` render into the
//!     DETAIL and CONTEXT of a syntax error. Both `json_in` and `jsonb_in`
//!     report `invalid input syntax for type json` — with the word `json` — so
//!     the message is not the place the types differ either.
//!   * [`validate`] — `json_in`, which runs the parser for its errors and
//!     discards everything else.
//!   * [`parse_jsonb`] — `jsonb_in`, the same walk with the semantic actions
//!     that build a [`JsonbValue`].
//!   * [`Scanner`] — a whitespace-preserving reader over *already validated*
//!     text, which is how `json_each`, `json_array_elements`, `->` and
//!     `json_extract_path` return sub-documents with their original spacing,
//!     key order and duplicate keys intact.
//!   * the `json`-flavoured serializers ([`write_string`], [`Layout`]), which
//!     differ from `jsonb`'s: `json_build_object` writes `{"a" : 1}`,
//!     `row_to_json` writes `{"a":1}`, and `jsonb_build_object` writes
//!     `{"a": 1}`. Three spacings, one syntax.
//!
//! The one behavioural fork inside the lexer is `need_escapes`. `json_in` never
//! needs a decoded string, so it never decodes one, and therefore never notices
//! that `"\ud800"` is an unpaired surrogate — `'"\ud800"'::json` is accepted and
//! `'"\ud800"'::jsonb` is not. That is `PostgreSQL`'s behaviour, not an accident
//! of this port.
//!
//! The fork does not stop at input. `makeJsonLexContext` takes `need_escapes` as
//! an argument, so every accessor over an *already stored* `json` value picks a
//! side of it again: `get_worker` (`->`, `->>`, `#>`, `#>>`,
//! `json_extract_path`) passes true, `each_worker` and `json_object_keys` pass
//! true, `elements_worker` passes its `as_text` flag, and `json_array_length`
//! and `json_typeof` pass false. So `'{"a":"\ud800"}'::json` is a legal value
//! that `json_typeof` reports on happily and `-> 'a'` refuses — the accessor,
//! not the value, decides. [`validate`] is the false side and
//! [`validate_escapes`] the true one; they must not be merged.

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL JSON lexer kept structurally close to jsonapi.c"
)]

use std::fmt::Write as _;

use crate::{TypeError, jsonb::JsonbValue};

/// `PostgreSQL` guards its recursive-descent JSON parser with `check_stack_depth`
/// rather than a fixed count. This is the equivalent bound, chosen so a nesting
/// depth no real document reaches cannot overflow the parser's own stack.
pub const MAX_DEPTH: u32 = 512;

// ---------------------------------------------------------------- token kinds

/// `JsonTokenType`. The initial value of a fresh lexer is [`Tok::Invalid`],
/// which matters: `report_json_context` suppresses its trailing `...` only for
/// [`Tok::End`], so an error raised before any token was completed still gets
/// the ellipsis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tok {
    Invalid,
    String,
    Number,
    ObjectStart,
    ObjectEnd,
    ArrayStart,
    ArrayEnd,
    Comma,
    Colon,
    True,
    False,
    Null,
    End,
}

/// `JsonParseErrorType`, narrowed to the cases this port can reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParseError {
    InvalidToken,
    EscapingInvalid,
    EscapingRequired,
    ExpectedEnd,
    ExpectedArrayFirst,
    ExpectedArrayNext,
    ExpectedColon,
    ExpectedJson,
    ExpectedMore,
    ExpectedObjectFirst,
    ExpectedObjectNext,
    ExpectedString,
    UnicodeEscapeFormat,
    UnicodeCodePointZero,
    UnicodeHighSurrogate,
    UnicodeLowSurrogate,
}

impl ParseError {
    /// `json_errsave_error` re-codes exactly three failures as `22P05` /
    /// `unsupported Unicode escape sequence`, and `\u0000` is the only one of
    /// the three this port can reach — the other two are frontend-only or
    /// non-UTF8-server cases. The *surrogate* failures are deliberately not in
    /// that set: they stay `22P02`, which is easy to get wrong because they read
    /// like Unicode problems.
    fn is_unicode(self) -> bool {
        matches!(self, ParseError::UnicodeCodePointZero)
    }
}

/// `JsonParseContext` — which production was being parsed, which is the only
/// input `report_parse_error` uses to pick between the "Expected …" details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ctx {
    End,
    Value,
    String,
    ArrayStart,
    ArrayNext,
    ObjectStart,
    ObjectLabel,
    ObjectNext,
}

// -------------------------------------------------------------------- errors

/// Build the `TypeError` for a JSON syntax failure, carrying `PostgreSQL`'s
/// DETAIL and CONTEXT verbatim.
fn json_error(
    sqlstate: &'static str,
    message: &'static str,
    detail: String,
    context: String,
) -> TypeError {
    TypeError::JsonSyntax {
        sqlstate,
        message,
        detail,
        context,
    }
}

// -------------------------------------------------------------------- lexer

/// `JsonLexContext`: the input, the current token's byte span, and enough line
/// bookkeeping to render `JSON data, line N: …`.
struct Lexer<'a> {
    src: &'a str,
    bytes: &'a [u8],
    /// `jsonb_in` decodes string escapes and so sees unpaired surrogates and
    /// `\u0000`; `json_in` does not decode and so does not.
    need_escapes: bool,
    token_start: Option<usize>,
    token_terminator: usize,
    token_type: Tok,
    line_number: usize,
    line_start: usize,
    /// The decoded text of the token most recently lexed as a string. Only
    /// maintained when `need_escapes` is set.
    strval: String,
}

impl<'a> Lexer<'a> {
    fn new(src: &'a str, need_escapes: bool) -> Self {
        Lexer {
            src,
            bytes: src.as_bytes(),
            need_escapes,
            token_start: Some(0),
            token_terminator: 0,
            token_type: Tok::Invalid,
            line_number: 1,
            line_start: 0,
            strval: String::new(),
        }
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    /// The raw text of the current token — the `%.*s` every token-bearing DETAIL
    /// interpolates.
    fn token_text(&self) -> &str {
        let start = self.token_start.unwrap_or(self.token_terminator);
        &self.src[start..self.token_terminator]
    }

    /// `json_errdetail`.
    fn detail(&self, error: ParseError) -> String {
        match error {
            ParseError::EscapingInvalid => {
                format!("Escape sequence \"\\{}\" is invalid.", self.token_text())
            }
            ParseError::EscapingRequired => format!(
                "Character with value 0x{:02x} must be escaped.",
                self.bytes.get(self.token_terminator).copied().unwrap_or(0)
            ),
            ParseError::ExpectedEnd => {
                format!(
                    "Expected end of input, but found \"{}\".",
                    self.token_text()
                )
            }
            ParseError::ExpectedArrayFirst => format!(
                "Expected array element or \"]\", but found \"{}\".",
                self.token_text()
            ),
            ParseError::ExpectedArrayNext => format!(
                "Expected \",\" or \"]\", but found \"{}\".",
                self.token_text()
            ),
            ParseError::ExpectedColon => {
                format!("Expected \":\", but found \"{}\".", self.token_text())
            }
            ParseError::ExpectedJson => {
                format!("Expected JSON value, but found \"{}\".", self.token_text())
            }
            ParseError::ExpectedMore => "The input string ended unexpectedly.".to_string(),
            ParseError::ExpectedObjectFirst => format!(
                "Expected string or \"}}\", but found \"{}\".",
                self.token_text()
            ),
            ParseError::ExpectedObjectNext => format!(
                "Expected \",\" or \"}}\", but found \"{}\".",
                self.token_text()
            ),
            ParseError::ExpectedString => {
                format!("Expected string, but found \"{}\".", self.token_text())
            }
            ParseError::InvalidToken => format!("Token \"{}\" is invalid.", self.token_text()),
            ParseError::UnicodeEscapeFormat => {
                "\"\\u\" must be followed by four hexadecimal digits.".to_string()
            }
            ParseError::UnicodeCodePointZero => "\\u0000 cannot be converted to text.".to_string(),
            ParseError::UnicodeHighSurrogate => {
                "Unicode high surrogate must not follow a high surrogate.".to_string()
            }
            ParseError::UnicodeLowSurrogate => {
                "Unicode low surrogate must follow a high surrogate.".to_string()
            }
        }
    }

    /// `report_json_context`: at most the last 50 bytes of the current line up
    /// to the error, elided with `...` on whichever side was cut.
    fn context(&self) -> String {
        let line_start = self.line_start;
        let context_end = self.token_terminator.min(self.len());
        let mut context_start = line_start;
        // Advance whole characters until the excerpt is under 50 bytes.
        while context_end.saturating_sub(context_start) >= 50 {
            context_start += 1;
            while context_start < context_end && !self.src.is_char_boundary(context_start) {
                context_start += 1;
            }
        }
        // Within three bytes of the line start, the ellipsis buys nothing.
        if context_start - line_start <= 3 {
            context_start = line_start;
        }
        let prefix = if context_start > line_start {
            "..."
        } else {
            ""
        };
        let suffix = if self.token_type != Tok::End
            && context_end < self.len()
            && self.bytes[context_end] != b'\n'
            && self.bytes[context_end] != b'\r'
        {
            "..."
        } else {
            ""
        };
        format!(
            "JSON data, line {}: {prefix}{}{suffix}",
            self.line_number,
            &self.src[context_start..context_end]
        )
    }

    fn fail(&self, error: ParseError) -> TypeError {
        if error.is_unicode() {
            json_error(
                "22P05",
                "unsupported Unicode escape sequence",
                self.detail(error),
                self.context(),
            )
        } else {
            json_error(
                "22P02",
                "invalid input syntax for type json",
                self.detail(error),
                self.context(),
            )
        }
    }

    /// `json_lex`: skip whitespace, then classify one token.
    fn lex(&mut self) -> Result<(), ParseError> {
        let end = self.len();
        let mut s = self.token_terminator;
        while s < end && matches!(self.bytes[s], b' ' | b'\t' | b'\n' | b'\r') {
            if self.bytes[s] == b'\n' {
                self.line_number += 1;
                self.line_start = s + 1;
            }
            s += 1;
        }
        self.token_start = Some(s);

        if s >= end {
            self.token_start = None;
            self.token_terminator = s;
            self.token_type = Tok::End;
            return Ok(());
        }

        match self.bytes[s] {
            b'{' => self.punct(s, Tok::ObjectStart),
            b'}' => self.punct(s, Tok::ObjectEnd),
            b'[' => self.punct(s, Tok::ArrayStart),
            b']' => self.punct(s, Tok::ArrayEnd),
            b',' => self.punct(s, Tok::Comma),
            b':' => self.punct(s, Tok::Colon),
            b'"' => {
                self.lex_string()?;
                self.token_type = Tok::String;
            }
            b'-' => {
                self.lex_number(s + 1)?;
                self.token_type = Tok::Number;
            }
            b'0'..=b'9' => {
                self.lex_number(s)?;
                self.token_type = Tok::Number;
            }
            _ => {
                // Not a string, number or punctuation: scan the whole
                // alphanumeric run so the error names the word, not a prefix.
                let mut p = s;
                while p < end && is_alnum(self.bytes[p]) {
                    p += 1;
                }
                if p == s {
                    self.token_terminator = s + 1;
                    return Err(ParseError::InvalidToken);
                }
                self.token_terminator = p;
                self.token_type = match &self.src[s..p] {
                    "true" => Tok::True,
                    "false" => Tok::False,
                    "null" => Tok::Null,
                    _ => return Err(ParseError::InvalidToken),
                };
            }
        }
        Ok(())
    }

    fn punct(&mut self, s: usize, tok: Tok) {
        self.token_terminator = s + 1;
        self.token_type = tok;
    }

    /// `json_lex_string`.
    fn lex_string(&mut self) -> Result<(), ParseError> {
        let end = self.len();
        if self.need_escapes {
            self.strval.clear();
        }
        let mut hi_surrogate: Option<u32> = None;
        let mut s = self.token_start.expect("string token has a start");
        loop {
            s += 1;
            if s >= end {
                self.token_terminator = s;
                return Err(ParseError::InvalidToken);
            }
            let c = self.bytes[s];
            if c == b'"' {
                break;
            }
            if c == b'\\' {
                s += 1;
                if s >= end {
                    self.token_terminator = s;
                    return Err(ParseError::InvalidToken);
                }
                if self.bytes[s] == b'u' {
                    let mut ch: u32 = 0;
                    for _ in 0..4 {
                        s += 1;
                        if s >= end {
                            self.token_terminator = s;
                            return Err(ParseError::InvalidToken);
                        }
                        match hex_val(self.bytes[s]) {
                            Some(v) => ch = ch * 16 + v,
                            None => {
                                self.token_terminator = self.char_end(s);
                                return Err(ParseError::UnicodeEscapeFormat);
                            }
                        }
                    }
                    if self.need_escapes {
                        if (0xd800..0xdc00).contains(&ch) {
                            if hi_surrogate.is_some() {
                                self.token_terminator = self.char_end(s);
                                return Err(ParseError::UnicodeHighSurrogate);
                            }
                            hi_surrogate = Some(ch);
                            continue;
                        } else if (0xdc00..0xe000).contains(&ch) {
                            let Some(hi) = hi_surrogate else {
                                self.token_terminator = self.char_end(s);
                                return Err(ParseError::UnicodeLowSurrogate);
                            };
                            ch = 0x10000 + ((hi - 0xd800) << 10) + (ch - 0xdc00);
                            hi_surrogate = None;
                        }
                        if hi_surrogate.is_some() {
                            self.token_terminator = self.char_end(s);
                            return Err(ParseError::UnicodeLowSurrogate);
                        }
                        if ch == 0 {
                            self.token_terminator = self.char_end(s);
                            return Err(ParseError::UnicodeCodePointZero);
                        }
                        // Four hex digits cannot exceed 0xFFFF and the surrogate
                        // halves were handled above, so this always maps.
                        self.strval
                            .push(char::from_u32(ch).unwrap_or(char::REPLACEMENT_CHARACTER));
                    }
                } else if self.need_escapes {
                    if hi_surrogate.is_some() {
                        self.token_terminator = self.char_end(s);
                        return Err(ParseError::UnicodeLowSurrogate);
                    }
                    match self.bytes[s] {
                        b'"' => self.strval.push('"'),
                        b'\\' => self.strval.push('\\'),
                        b'/' => self.strval.push('/'),
                        b'b' => self.strval.push('\u{8}'),
                        b'f' => self.strval.push('\u{c}'),
                        b'n' => self.strval.push('\n'),
                        b'r' => self.strval.push('\r'),
                        b't' => self.strval.push('\t'),
                        _ => {
                            // Report only the escape, not the whole string.
                            self.token_start = Some(s);
                            self.token_terminator = self.char_end(s);
                            return Err(ParseError::EscapingInvalid);
                        }
                    }
                } else if !matches!(
                    self.bytes[s],
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't'
                ) {
                    self.token_start = Some(s);
                    self.token_terminator = self.char_end(s);
                    return Err(ParseError::EscapingInvalid);
                }
            } else {
                if hi_surrogate.is_some() {
                    self.token_terminator = self.char_end(s);
                    return Err(ParseError::UnicodeLowSurrogate);
                }
                let mut p = s;
                while p < end {
                    let b = self.bytes[p];
                    if b == b'\\' || b == b'"' {
                        break;
                    }
                    if b <= 31 {
                        // RFC 4627 requires these escaped; the character itself
                        // is unprintable, so it is left out of the context.
                        self.token_terminator = p;
                        return Err(ParseError::EscapingRequired);
                    }
                    p += 1;
                }
                if self.need_escapes {
                    self.strval.push_str(&self.src[s..p]);
                }
                s = p - 1;
            }
        }
        if hi_surrogate.is_some() {
            self.token_terminator = s + 1;
            return Err(ParseError::UnicodeLowSurrogate);
        }
        self.token_terminator = s + 1;
        Ok(())
    }

    /// The byte offset just past the character starting at `s`.
    fn char_end(&self, s: usize) -> usize {
        let mut e = s + 1;
        while e < self.len() && !self.src.is_char_boundary(e) {
            e += 1;
        }
        e.min(self.len())
    }

    /// `json_lex_number`: `-? (0 | [1-9][0-9]*) (\.[0-9]+)? ([eE][+-]?[0-9]+)?`,
    /// with any trailing alphanumeric run folded into the token so the error
    /// names the whole of it.
    fn lex_number(&mut self, from: usize) -> Result<(), ParseError> {
        let end = self.len();
        let mut s = from;
        let mut error = false;

        if s < end && self.bytes[s] == b'0' {
            s += 1;
        } else if s < end && self.bytes[s].is_ascii_digit() {
            while s < end && self.bytes[s].is_ascii_digit() {
                s += 1;
            }
        } else {
            error = true;
        }

        if s < end && self.bytes[s] == b'.' {
            s += 1;
            if s >= end || !self.bytes[s].is_ascii_digit() {
                error = true;
            } else {
                while s < end && self.bytes[s].is_ascii_digit() {
                    s += 1;
                }
            }
        }

        if s < end && (self.bytes[s] == b'e' || self.bytes[s] == b'E') {
            s += 1;
            if s < end && (self.bytes[s] == b'+' || self.bytes[s] == b'-') {
                s += 1;
            }
            if s >= end || !self.bytes[s].is_ascii_digit() {
                error = true;
            } else {
                while s < end && self.bytes[s].is_ascii_digit() {
                    s += 1;
                }
            }
        }

        while s < end && is_alnum(self.bytes[s]) {
            s += 1;
            error = true;
        }

        self.token_terminator = s;
        if error {
            Err(ParseError::InvalidToken)
        } else {
            Ok(())
        }
    }

    /// `report_parse_error`: a premature end always outranks the production's
    /// own "Expected …" wording.
    fn parse_error(&self, ctx: Ctx) -> ParseError {
        if self.token_start.is_none() || self.token_type == Tok::End {
            return ParseError::ExpectedMore;
        }
        match ctx {
            Ctx::End => ParseError::ExpectedEnd,
            Ctx::Value => ParseError::ExpectedJson,
            Ctx::String => ParseError::ExpectedString,
            Ctx::ArrayStart => ParseError::ExpectedArrayFirst,
            Ctx::ArrayNext => ParseError::ExpectedArrayNext,
            Ctx::ObjectStart => ParseError::ExpectedObjectFirst,
            Ctx::ObjectLabel => ParseError::ExpectedColon,
            Ctx::ObjectNext => ParseError::ExpectedObjectNext,
        }
    }

    fn expect(&mut self, ctx: Ctx, token: Tok) -> Result<(), ParseError> {
        if self.token_type == token {
            self.lex()
        } else {
            Err(self.parse_error(ctx))
        }
    }
}

fn is_alnum(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

fn hex_val(b: u8) -> Option<u32> {
    match b {
        b'0'..=b'9' => Some(u32::from(b - b'0')),
        b'a'..=b'f' => Some(u32::from(b - b'a') + 10),
        b'A'..=b'F' => Some(u32::from(b - b'A') + 10),
        _ => None,
    }
}

// ------------------------------------------------------------------- parser

/// What the walk should produce. Validation runs the identical control flow and
/// simply drops the values, so `json_in` and `jsonb_in` cannot disagree about
/// which documents are well-formed.
struct Parser<'a> {
    lex: Lexer<'a>,
    /// Build [`JsonbValue`]s (`jsonb_in`) rather than only validating.
    build: bool,
    reject_duplicates: bool,
    saw_duplicate: bool,
}

impl Parser<'_> {
    fn parse(&mut self) -> Result<JsonbValue, TypeError> {
        self.step(|p| p.lex.lex())?;
        let value = match self.lex.token_type {
            Tok::ObjectStart => self.object(0)?,
            Tok::ArrayStart => self.array(0)?,
            _ => self.scalar()?,
        };
        // `lex_expect(JSON_PARSE_END, …)`: anything left over is `Expected end
        // of input, but found "…"`.
        self.step(|p| {
            if p.lex.token_type == Tok::End {
                Ok(())
            } else {
                Err(p.lex.parse_error(Ctx::End))
            }
        })?;
        Ok(value)
    }

    /// Run one lexer/parser step, converting its `ParseError` into the
    /// `TypeError` that carries `PostgreSQL`'s DETAIL and CONTEXT. The error is
    /// rendered from the lexer state *at the moment of failure*, so this must
    /// wrap every fallible step rather than being applied once at the end.
    fn step<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, ParseError>,
    ) -> Result<T, TypeError> {
        match f(self) {
            Ok(v) => Ok(v),
            Err(e) => Err(self.lex.fail(e)),
        }
    }

    fn depth_check(&self, depth: u32) -> Result<(), TypeError> {
        if depth > MAX_DEPTH {
            return Err(TypeError::Domain {
                sqlstate: "54001",
                message: "stack depth limit exceeded",
            });
        }
        Ok(())
    }

    fn value(&mut self, depth: u32) -> Result<JsonbValue, TypeError> {
        match self.lex.token_type {
            Tok::ObjectStart => self.object(depth),
            Tok::ArrayStart => self.array(depth),
            _ => self.scalar(),
        }
    }

    fn scalar(&mut self) -> Result<JsonbValue, TypeError> {
        let tok = self.lex.token_type;
        if !matches!(
            tok,
            Tok::String | Tok::Number | Tok::True | Tok::False | Tok::Null
        ) {
            let e = self.lex.parse_error(Ctx::Value);
            return Err(self.lex.fail(e));
        }
        let value = if self.build {
            match tok {
                Tok::String => JsonbValue::String(std::mem::take(&mut self.lex.strval)),
                Tok::Number => {
                    // `parse_finite` rejects an adversarial exponent without
                    // materializing its digits; JSON has no `NaN`/`Infinity`
                    // spelling, so the finite parser is the right one.
                    let text = self.lex.token_text();
                    let n = crate::numeric::parse_finite(text).ok_or(TypeError::Overflow)?;
                    JsonbValue::Number(n)
                }
                Tok::True => JsonbValue::Bool(true),
                Tok::False => JsonbValue::Bool(false),
                _ => JsonbValue::Null,
            }
        } else {
            JsonbValue::Null
        };
        self.step(|p| p.lex.lex())?;
        Ok(value)
    }

    fn array(&mut self, depth: u32) -> Result<JsonbValue, TypeError> {
        self.depth_check(depth)?;
        let mut items = Vec::new();
        self.step(|p| p.lex.expect(Ctx::ArrayStart, Tok::ArrayStart))?;
        if self.lex.token_type != Tok::ArrayEnd {
            items.push(self.value(depth + 1)?);
            while self.lex.token_type == Tok::Comma {
                self.step(|p| p.lex.lex())?;
                items.push(self.value(depth + 1)?);
            }
        }
        self.step(|p| p.lex.expect(Ctx::ArrayNext, Tok::ArrayEnd))?;
        Ok(JsonbValue::Array(items))
    }

    fn object(&mut self, depth: u32) -> Result<JsonbValue, TypeError> {
        self.depth_check(depth)?;
        let mut pairs: Vec<(String, JsonbValue)> = Vec::new();
        self.step(|p| p.lex.lex())?;
        match self.lex.token_type {
            Tok::String => {
                pairs.push(self.object_field(depth)?);
                while self.lex.token_type == Tok::Comma {
                    self.step(|p| p.lex.lex())?;
                    pairs.push(self.object_field(depth)?);
                }
            }
            Tok::ObjectEnd => {}
            _ => {
                let e = self.lex.parse_error(Ctx::ObjectStart);
                return Err(self.lex.fail(e));
            }
        }
        self.step(|p| p.lex.expect(Ctx::ObjectNext, Tok::ObjectEnd))?;

        if !self.build {
            return Ok(JsonbValue::Null);
        }
        let mut seen: Vec<&str> = Vec::with_capacity(pairs.len());
        for (key, _) in &pairs {
            if seen.contains(&key.as_str()) {
                if self.reject_duplicates {
                    return Err(TypeError::Coded {
                        sqlstate: "22030",
                        message: format!("duplicate JSON object key value: \"{key}\""),
                    });
                }
                self.saw_duplicate = true;
            } else {
                seen.push(key);
            }
        }
        Ok(JsonbValue::object_from_pairs(pairs))
    }

    fn object_field(&mut self, depth: u32) -> Result<(String, JsonbValue), TypeError> {
        if self.lex.token_type != Tok::String {
            let e = self.lex.parse_error(Ctx::String);
            return Err(self.lex.fail(e));
        }
        let key = if self.build {
            std::mem::take(&mut self.lex.strval)
        } else {
            String::new()
        };
        self.step(|p| p.lex.lex())?;
        self.step(|p| p.lex.expect(Ctx::ObjectLabel, Tok::Colon))?;
        let value = self.value(depth + 1)?;
        Ok((key, value))
    }
}

/// `json_in`: accept the text if it is a JSON document, and change nothing.
///
/// # Errors
///
/// 22P02 with `PostgreSQL`'s DETAIL and CONTEXT for malformed JSON, and 54001
/// past [`MAX_DEPTH`] levels of nesting.
pub fn validate(input: &str) -> Result<(), TypeError> {
    Parser {
        lex: Lexer::new(input, false),
        build: false,
        reject_duplicates: false,
        saw_duplicate: false,
    }
    .parse()
    .map(|_| ())
}

/// The same walk as [`validate`] with the lexer's `need_escapes` set — the
/// `pg_parse_json` an accessor built by `makeJsonLexContext(…, true)` runs.
///
/// This is not a stricter `json_in`, and it must not be used as one. `json_in`
/// decodes nothing, so `'"\ud800"'::json` and `'"\u0000"'::json` are both legal
/// `json` values and stay legal; this is what `->` and its relatives run over
/// that stored text *afterwards*, and it is the reason those operators reject
/// documents the cast accepted. Nothing is built: the decoded strings exist only
/// so the lexer can trip over the ones that cannot be decoded.
///
/// # Errors
///
/// 22P02 for malformed JSON or an unpaired surrogate, 22P05 for `\u0000`, and
/// 54001 past [`MAX_DEPTH`] levels of nesting.
pub fn validate_escapes(input: &str) -> Result<(), TypeError> {
    Parser {
        lex: Lexer::new(input, true),
        build: false,
        reject_duplicates: false,
        saw_duplicate: false,
    }
    .parse()
    .map(|_| ())
}

/// `jsonb_in`: the same walk, decomposing as it goes.
///
/// The second element of the pair is `PostgreSQL`'s `WITH UNIQUE KEYS`
/// observation — true when some object repeated a key. `reject_duplicates`
/// turns that into an error (22030) instead.
///
/// # Errors
///
/// 22P02 for malformed JSON, 22P05 for an escape that decodes to nothing `text`
/// can hold, 22003 for a number outside `numeric`, 54001 past [`MAX_DEPTH`], and
/// 22030 for a duplicate key when `reject_duplicates` is set.
pub fn parse_jsonb(input: &str, reject_duplicates: bool) -> Result<(JsonbValue, bool), TypeError> {
    let mut parser = Parser {
        lex: Lexer::new(input, true),
        build: true,
        reject_duplicates,
        saw_duplicate: false,
    };
    let value = parser.parse()?;
    Ok((value, parser.saw_duplicate))
}

// ------------------------------------------------------------------ scanner

/// What a `json` document is at its top level — `json_typeof`'s answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

impl Kind {
    /// `json_typeof` / `jsonb_typeof`'s spelling.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Kind::Null => "null",
            Kind::Bool => "boolean",
            Kind::Number => "number",
            Kind::String => "string",
            Kind::Array => "array",
            Kind::Object => "object",
        }
    }
}

/// A reader over *already validated* `json` text.
///
/// Every accessor returns byte spans of the original input rather than a rebuilt
/// document, which is the whole point of the type: `json_each` on
/// `'{"b":1,   "a":2,  "b":3}'` yields three fields in that order, spacing and
/// all, where `jsonb_each` yields two in sorted order.
struct Scanner<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(src: &'a str) -> Self {
        Scanner {
            src,
            bytes: src.as_bytes(),
            pos: 0,
        }
    }

    fn skip_ws(&mut self) {
        while self
            .bytes
            .get(self.pos)
            .is_some_and(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
        {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    /// Advance past one complete value, returning its span. Assumes validated
    /// input, so it never has to report a syntax error.
    fn skip_value(&mut self) -> (usize, usize) {
        self.skip_ws();
        let start = self.pos;
        match self.peek() {
            Some(b'"') => self.skip_string(),
            Some(b'[') => self.skip_bracketed(b'[', b']'),
            Some(b'{') => self.skip_bracketed(b'{', b'}'),
            _ => {
                while self.peek().is_some_and(|b| {
                    !matches!(b, b',' | b']' | b'}' | b' ' | b'\t' | b'\n' | b'\r')
                }) {
                    self.pos += 1;
                }
            }
        }
        (start, self.pos)
    }

    fn skip_string(&mut self) {
        self.pos += 1;
        while let Some(b) = self.peek() {
            self.pos += 1;
            match b {
                b'\\' => self.pos += 1,
                b'"' => break,
                _ => {}
            }
        }
    }

    fn skip_bracketed(&mut self, open: u8, close: u8) {
        let mut depth = 0usize;
        while let Some(b) = self.peek() {
            match b {
                b'"' => {
                    self.skip_string();
                    continue;
                }
                b if b == open => depth += 1,
                b if b == close => {
                    depth -= 1;
                    self.pos += 1;
                    if depth == 0 {
                        return;
                    }
                    continue;
                }
                _ => {}
            }
            self.pos += 1;
        }
    }
}

/// `json_typeof`'s answer for validated `json` text.
#[must_use]
pub fn kind(input: &str) -> Kind {
    let mut sc = Scanner::new(input);
    sc.skip_ws();
    match sc.peek() {
        Some(b'{') => Kind::Object,
        Some(b'[') => Kind::Array,
        Some(b'"') => Kind::String,
        Some(b't' | b'f') => Kind::Bool,
        Some(b'n') => Kind::Null,
        _ => Kind::Number,
    }
}

/// The elements of a JSON array, each as the original text of that element with
/// its surrounding whitespace trimmed. `None` when the document is not an array.
#[must_use]
pub fn array_elements(input: &str) -> Option<Vec<&str>> {
    let mut sc = Scanner::new(input);
    sc.skip_ws();
    if sc.peek() != Some(b'[') {
        return None;
    }
    sc.pos += 1;
    let mut out = Vec::new();
    loop {
        sc.skip_ws();
        if sc.peek() == Some(b']') {
            return Some(out);
        }
        let (start, end) = sc.skip_value();
        out.push(&sc.src[start..end]);
        sc.skip_ws();
        match sc.peek() {
            Some(b',') => sc.pos += 1,
            _ => return Some(out),
        }
    }
}

/// The fields of a JSON object, in input order, with duplicate keys kept — the
/// two properties `jsonb` discards. Keys are de-escaped; values are the original
/// text. `None` when the document is not an object.
#[must_use]
pub fn object_fields(input: &str) -> Option<Vec<(String, &str)>> {
    let mut sc = Scanner::new(input);
    sc.skip_ws();
    if sc.peek() != Some(b'{') {
        return None;
    }
    sc.pos += 1;
    let mut out = Vec::new();
    loop {
        sc.skip_ws();
        if sc.peek() == Some(b'}') {
            return Some(out);
        }
        let (ks, ke) = sc.skip_value();
        let key = unescape(&sc.src[ks..ke])?;
        sc.skip_ws();
        if sc.peek() != Some(b':') {
            return Some(out);
        }
        sc.pos += 1;
        let (vs, ve) = sc.skip_value();
        out.push((key, &sc.src[vs..ve]));
        sc.skip_ws();
        match sc.peek() {
            Some(b',') => sc.pos += 1,
            _ => return Some(out),
        }
    }
}

/// A high surrogate whose partner never arrived. It is damage, not absence, so
/// it decodes to the replacement character instead of vanishing: dropping it
/// silently made `"\ud800"` and `"\ud801"` both decode to the empty string, and
/// two distinct documents then produced one indistinguishable value.
///
/// Every accessor that decodes runs [`validate_escapes`] first and so cannot
/// reach this at all — it is the floor under a caller that forgets to.
fn flush_surrogate(hi_surrogate: &mut Option<u32>, out: &mut String) {
    if hi_surrogate.take().is_some() {
        out.push(char::REPLACEMENT_CHARACTER);
    }
}

/// The de-escaped contents of a JSON string literal (`raw` includes its quotes).
/// `None` when `raw` is not a string literal.
#[must_use]
pub fn unescape(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    if bytes.first() != Some(&b'"') {
        return None;
    }
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw[1..raw.len().saturating_sub(1)].chars();
    let mut hi_surrogate: Option<u32> = None;
    while let Some(c) = chars.next() {
        if c != '\\' {
            flush_surrogate(&mut hi_surrogate, &mut out);
            out.push(c);
            continue;
        }
        let escape = chars.next();
        if escape != Some('u') {
            flush_surrogate(&mut hi_surrogate, &mut out);
        }
        match escape {
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some('/') => out.push('/'),
            Some('b') => out.push('\u{8}'),
            Some('f') => out.push('\u{c}'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('u') => {
                let mut ch = 0u32;
                for _ in 0..4 {
                    let d = chars.next()?;
                    ch = ch * 16 + d.to_digit(16)?;
                }
                if !(0xdc00..0xe000).contains(&ch) {
                    flush_surrogate(&mut hi_surrogate, &mut out);
                }
                if (0xd800..0xdc00).contains(&ch) {
                    hi_surrogate = Some(ch);
                    continue;
                }
                if (0xdc00..0xe000).contains(&ch)
                    && let Some(hi) = hi_surrogate.take()
                {
                    ch = 0x10000 + ((hi - 0xd800) << 10) + (ch - 0xdc00);
                }
                out.push(char::from_u32(ch).unwrap_or(char::REPLACEMENT_CHARACTER));
            }
            other => out.push(other?),
        }
    }
    flush_surrogate(&mut hi_surrogate, &mut out);
    Some(out)
}

/// `->>` and friends: a JSON string yields its de-escaped contents, and anything
/// else yields its own text unchanged.
#[must_use]
pub fn as_text(raw: &str) -> String {
    unescape(raw).unwrap_or_else(|| raw.to_string())
}

/// Is this validated document the JSON `null` literal?
#[must_use]
pub fn is_null(input: &str) -> bool {
    kind(input) == Kind::Null
}

/// `json_strip_nulls`: drop every object field whose value is JSON `null`,
/// recursively. `PostgreSQL` re-serializes the survivors compactly rather than
/// preserving their spacing, and (unless `strip_in_arrays`) keeps array nulls.
#[must_use]
pub fn strip_nulls(input: &str, strip_in_arrays: bool) -> String {
    let mut out = String::with_capacity(input.len());
    write_stripped(input, strip_in_arrays, &mut out);
    out
}

fn write_stripped(input: &str, strip_in_arrays: bool, out: &mut String) {
    match kind(input) {
        Kind::Object => {
            let fields = object_fields(input).unwrap_or_default();
            out.push('{');
            let mut first = true;
            for (key, value) in fields {
                if is_null(value) {
                    continue;
                }
                if !first {
                    out.push(',');
                }
                first = false;
                write_string(&key, out);
                out.push(':');
                write_stripped(value, strip_in_arrays, out);
            }
            out.push('}');
        }
        Kind::Array => {
            let items = array_elements(input).unwrap_or_default();
            out.push('[');
            let mut first = true;
            for item in items {
                if strip_in_arrays && is_null(item) {
                    continue;
                }
                if !first {
                    out.push(',');
                }
                first = false;
                write_stripped(item, strip_in_arrays, out);
            }
            out.push(']');
        }
        _ => out.push_str(input.trim()),
    }
}

// -------------------------------------------------------------- serializers

/// Append `s` as a JSON string literal, escaping exactly what `escape_json`
/// escapes: `"`, `\` and the C0 controls. `/` and non-ASCII stay raw.
pub fn write_string(s: &str, out: &mut String) {
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

/// [`write_string`] as an owned value.
#[must_use]
pub fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    write_string(s, &mut out);
    out
}

/// How a `json` constructor spaces the document it builds.
///
/// `PostgreSQL` uses three different spacings for `json` and they are not
/// interchangeable: `row_to_json(row(1,2))` is `{"f1":1,"f2":2}`,
/// `json_build_object('a',1)` is `{"a" : 1}`, and `json_object_agg('a',1)` is
/// `{ "a" : 1 }`. `jsonb`'s single spacing (`{"a": 1}`) is a fourth, and lives
/// with [`JsonbValue::to_text`](crate::jsonb::JsonbValue::to_text).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// `{"a":1,"b":2}` / `[1,2]` — `composite_to_json` and `array_to_json`.
    Compact,
    /// `{"a" : 1, "b" : 2}` / `[1, 2]` — `json_build_object`, `json_build_array`,
    /// `json_object` and `json_agg`.
    Spaced,
    /// `{ "a" : 1, "b" : 2 }` — `json_object_agg` alone.
    Padded,
}

impl Layout {
    /// The text between a key and its value.
    #[must_use]
    pub fn colon(self) -> &'static str {
        match self {
            Layout::Compact => ":",
            Layout::Spaced | Layout::Padded => " : ",
        }
    }

    /// The text between two members.
    #[must_use]
    pub fn comma(self) -> &'static str {
        match self {
            Layout::Compact => ",",
            Layout::Spaced | Layout::Padded => ", ",
        }
    }

    /// The text just inside the braces of a non-empty object.
    #[must_use]
    pub fn pad(self) -> &'static str {
        match self {
            Layout::Padded => " ",
            Layout::Compact | Layout::Spaced => "",
        }
    }
}

/// Render a `jsonb` value as `json` text under `layout` — the conversion
/// `'{"b":1}'::jsonb::json` performs, which keeps `jsonb`'s canonical order
/// because that order is all a `jsonb` value has left.
#[must_use]
pub fn from_jsonb(value: &JsonbValue) -> String {
    value.to_text()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(input: &str) -> (String, String, String) {
        match validate(input) {
            Ok(()) => panic!("expected {input:?} to be rejected"),
            Err(e) => (
                e.sqlstate().to_string(),
                e.detail().map(|d| d.into_owned()).unwrap_or_default(),
                e.context().map(str::to_string).unwrap_or_default(),
            ),
        }
    }

    #[test]
    fn valid_documents_are_accepted_unchanged() {
        for input in [
            "{}",
            "[]",
            "null",
            "true",
            "1",
            "-1.5e10",
            r#""abc""#,
            r#"{"b":1,   "a":2,  "b":3}"#,
            "  [1,  2]  ",
            // `json_in` does not de-escape, so an unpaired surrogate is fine.
            r#""\ud800""#,
        ] {
            assert2::assert!(validate(input).is_ok(), "{input:?}");
        }
    }

    #[test]
    fn syntax_errors_carry_postgres_detail_and_context() {
        let cases = [
            (
                "x",
                "22P02",
                "Token \"x\" is invalid.",
                "JSON data, line 1: x",
            ),
            (
                "",
                "22P02",
                "The input string ended unexpectedly.",
                "JSON data, line 1: ",
            ),
            (
                "{",
                "22P02",
                "The input string ended unexpectedly.",
                "JSON data, line 1: {",
            ),
            (
                "01",
                "22P02",
                "Token \"01\" is invalid.",
                "JSON data, line 1: 01",
            ),
            (
                r#""\v""#,
                "22P02",
                "Escape sequence \"\\v\" is invalid.",
                "JSON data, line 1: \"\\v...",
            ),
            (
                "{,}",
                "22P02",
                "Expected string or \"}\", but found \",\".",
                "JSON data, line 1: {,...",
            ),
            (
                "[1,]",
                "22P02",
                "Expected JSON value, but found \"]\".",
                "JSON data, line 1: [1,]",
            ),
            (
                "{\"a\":1,}",
                "22P02",
                "Expected string, but found \"}\".",
                "JSON data, line 1: {\"a\":1,}",
            ),
            (
                "{\"a\":1}{\"b\":2}",
                "22P02",
                "Expected end of input, but found \"{\".",
                "JSON data, line 1: {\"a\":1}{...",
            ),
            (
                "{\"a\"}",
                "22P02",
                "Expected \":\", but found \"}\".",
                "JSON data, line 1: {\"a\"}",
            ),
            (
                r#""\u00""#,
                "22P02",
                "\"\\u\" must be followed by four hexadecimal digits.",
                "JSON data, line 1: \"\\u00\"",
            ),
            (
                "\"a\tb\"",
                "22P02",
                "Character with value 0x09 must be escaped.",
                "JSON data, line 1: \"a...",
            ),
            (
                "\n\n  bogus",
                "22P02",
                "Token \"bogus\" is invalid.",
                "JSON data, line 3:   bogus",
            ),
        ];
        for (input, sqlstate, detail, context) in cases {
            let got = err(input);
            assert2::assert!(
                got == (
                    sqlstate.to_string(),
                    detail.to_string(),
                    context.to_string()
                ),
                "{input:?}"
            );
        }
    }

    #[test]
    fn only_jsonb_decodes_escapes_and_so_only_jsonb_rejects_lone_surrogates() {
        // Every one of these is a valid `json` document and an invalid `jsonb`
        // one, because only `jsonb_in` decodes the escape.
        let cases = [
            (
                r#""\ud800""#,
                "22P02",
                "Unicode low surrogate must follow a high surrogate.",
            ),
            (
                r#""\ud800\ud800""#,
                "22P02",
                "Unicode high surrogate must not follow a high surrogate.",
            ),
            // `\u0000` is the one case PostgreSQL re-codes to 22P05.
            (
                r#""\u0000""#,
                "22P05",
                "\\u0000 cannot be converted to text.",
            ),
        ];
        for (input, sqlstate, detail) in cases {
            assert2::assert!(validate(input).is_ok(), "{input:?}");
            let e = parse_jsonb(input, false).expect_err(input);
            assert2::assert!(
                (e.sqlstate(), e.detail().as_deref()) == (sqlstate, Some(detail)),
                "{input:?}"
            );
        }
    }

    /// `json_encoding`'s surrogate block, which is the same five documents read
    /// three ways: `json_in` takes all five, the accessors take only the
    /// well-formed one, and neither rewrites what it accepted.
    #[test]
    fn an_accessor_decodes_the_escapes_json_in_stored_without_reading() {
        // (document, sqlstate, DETAIL, CONTEXT) for the four `validate_escapes`
        // refuses, verbatim from PostgreSQL 18.4's json_encoding.out.
        let refused = [
            (
                r#"{ "a":  "\ud83d\ud83d" }"#,
                "22P02",
                "Unicode high surrogate must not follow a high surrogate.",
                "JSON data, line 1: { \"a\":  \"\\ud83d\\ud83d...",
            ),
            (
                r#"{ "a":  "\ude04\ud83d" }"#,
                "22P02",
                "Unicode low surrogate must follow a high surrogate.",
                "JSON data, line 1: { \"a\":  \"\\ude04...",
            ),
            (
                r#"{ "a":  "\ud83dX" }"#,
                "22P02",
                "Unicode low surrogate must follow a high surrogate.",
                "JSON data, line 1: { \"a\":  \"\\ud83dX...",
            ),
            (
                r#"{ "a":  "\ude04X" }"#,
                "22P02",
                "Unicode low surrogate must follow a high surrogate.",
                "JSON data, line 1: { \"a\":  \"\\ude04...",
            ),
            // `\u0000` decodes to nothing `text` can hold, and is the one case
            // re-coded to 22P05.
            (
                r#"{ "a":  "null \u0000 escape" }"#,
                "22P05",
                "\\u0000 cannot be converted to text.",
                "JSON data, line 1: { \"a\":  \"null \\u0000...",
            ),
        ];
        for (input, sqlstate, detail, context) in refused {
            // `json_in` stores every one of them: the cast must keep working.
            assert2::assert!(validate(input).is_ok(), "{input:?}");
            let e = validate_escapes(input).expect_err(input);
            let got = (
                e.sqlstate().to_string(),
                e.detail().map(|d| d.into_owned()).unwrap_or_default(),
                e.context().map(str::to_string).unwrap_or_default(),
            );
            assert2::assert!(
                got == (
                    sqlstate.to_string(),
                    detail.to_string(),
                    context.to_string()
                ),
                "{input:?}"
            );
        }

        // The well-formed pair passes both readings, and the accessor still
        // hands back the ORIGINAL escapes rather than the decoded emoji — a fix
        // that decoded the output would satisfy the four cases above and be
        // just as wrong.
        let paired = r#"{ "a":  "\ud83d\ude04\ud83d\udc36" }"#;
        assert2::assert!(validate(paired).is_ok());
        assert2::assert!(validate_escapes(paired).is_ok());
        assert2::assert!(
            object_fields(paired).expect("object")
                == vec![("a".to_string(), r#""\ud83d\ude04\ud83d\udc36""#)]
        );
        // …and `->>`'s reading of that same field does decode it.
        assert2::assert!(as_text(r#""\ud83d\ude04\ud83d\udc36""#) == "😄🐶");
    }

    /// A high surrogate with no partner used to be dropped rather than replaced,
    /// so `"\ud800"` and `"\ud801"` decoded to the same empty string. No
    /// accessor can reach that any more, but the decoder must not be the thing
    /// that loses the distinction if one ever does.
    #[test]
    fn a_dangling_high_surrogate_decodes_to_a_character_not_to_nothing() {
        let cases = [
            (r#""\ud800""#, "\u{fffd}"),
            (r#""\ud83dX""#, "\u{fffd}X"),
            (r#""\ud83d\ud83d""#, "\u{fffd}\u{fffd}"),
            (r#""\ud83d\n""#, "\u{fffd}\n"),
            (r#""\ude04""#, "\u{fffd}"),
            // A well-formed pair still combines, and nothing else moved.
            (r#""😄""#, "😄"),
            (r#""a\tb""#, "a\tb"),
        ];
        for (raw, expected) in cases {
            assert2::assert!(unescape(raw).as_deref() == Some(expected), "{raw:?}");
        }
    }

    #[test]
    fn the_scanner_preserves_order_spacing_and_duplicates() {
        let doc = r#"{"b":1,   "a":2,  "b":3}"#;
        assert2::assert!(kind(doc) == Kind::Object);
        let fields = object_fields(doc).expect("object");
        assert2::assert!(
            fields
                == vec![
                    ("b".to_string(), "1"),
                    ("a".to_string(), "2"),
                    ("b".to_string(), "3"),
                ]
        );
        assert2::assert!(array_elements("[1,  2,   3]").expect("array") == vec!["1", "2", "3"]);
        assert2::assert!(
            object_fields(r#"{"a":{"b":  1},"c":[1,  2]}"#).expect("object")
                == vec![
                    ("a".to_string(), "{\"b\":  1}"),
                    ("c".to_string(), "[1,  2]"),
                ]
        );
        assert2::assert!(array_elements("{}").is_none());
        assert2::assert!(object_fields("[]").is_none());
    }

    #[test]
    fn strip_nulls_reserializes_compactly() {
        let cases = [
            (
                r#"[1, null,  {"a":null,  "b": 2}]"#,
                false,
                "[1,null,{\"b\":2}]",
            ),
            (r#"{"a":  {"b":  null}}"#, false, "{\"a\":{}}"),
            (r#"[1, null]"#, true, "[1]"),
        ];
        for (input, in_arrays, expected) in cases {
            assert2::assert!(strip_nulls(input, in_arrays) == expected, "{input:?}");
        }
    }

    #[test]
    fn as_text_de_escapes_only_strings() {
        assert2::assert!(as_text(r#""a\tb""#) == "a\tb");
        assert2::assert!(as_text("{\"a\":  1}") == "{\"a\":  1}");
        assert2::assert!(as_text("1.000") == "1.000");
    }

    #[test]
    fn nesting_past_the_depth_bound_is_54001() {
        let deep = "[".repeat(1000) + &"]".repeat(1000);
        let e = validate(&deep).expect_err("too deep");
        assert2::assert!(e.sqlstate() == "54001");
    }
}
