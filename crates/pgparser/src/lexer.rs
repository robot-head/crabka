//! Hand-written lexer.
//!
//! The lexer produces (Token, byte-offset) pairs. The offsets feed 42601 error
//! positions.
//!
//! The literal grammar tracks `PostgreSQL`'s `scan.l`: decimal, hexadecimal
//! (`0x…`), octal (`0o…`) and binary (`0b…`) integers, `_` digit separators,
//! `float8`/`numeric` literals with fraction and exponent, standard `'…'`
//! strings, `E'…'` escape strings, `U&'…'` / `U&"…"` Unicode-escape literals,
//! and `$tag$…$tag$` dollar quoting.

use crate::{
    error::ParseError,
    token::{Keyword, Token},
};

/// SQLSTATE 22021 (`character_not_in_repertoire`).
///
/// The escape-string escapes (`\ddd`, `\xhh`) address raw bytes, so they can
/// spell a sequence that is not valid UTF-8. `PostgreSQL` rejects those with
/// this code, not with 42601.
const UNTRANSLATABLE: &str = "22021";

/// SQLSTATE 42601 (`syntax_error`). [`ParseError::new`] already defaults to it,
/// but it also prefixes its argument with "syntax error at position N:", so a
/// lexer rule whose text `PostgreSQL` renders verbatim has to name the code.
const SYNTAX_ERROR: &str = "42601";

/// The escape character a `U&'…'` / `U&"…"` literal uses when it carries no
/// `UESCAPE` clause of its own.
const DEFAULT_UESCAPE: u8 = b'\\';

/// `PostgreSQL`'s hint under a `U&'…'` escape that no escape form completes.
const UESCAPE_HINT: &str = "Unicode escapes must be \\XXXX or \\+XXXXXX.";

/// Tokenize SQL text and preserve each token's byte offset.
///
/// # Errors
///
/// Returns a parse error for malformed literals, identifiers, comments, or
/// unsupported token forms.
pub fn lex(sql: &str) -> Result<Vec<(Token, usize)>, ParseError> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let (end, closed) = skip_block_comment(bytes, i);
                if !closed {
                    return Err(ParseError::new("unterminated block comment", i));
                }
                i = end;
            }
            b'\'' => {
                let start = i;
                let (text, end) = string_literal(bytes, i, false)?;
                i = end;
                out.push((Token::StringLit(text), start));
            }
            // `E'…'` / `e'…'` — an escape string literal, whose backslash escapes
            // are expanded here so the token is an ordinary string. Checked before
            // the identifier arm; `east'x'` is still the identifier `east` because
            // the quote must follow the `E` immediately.
            b'e' | b'E' if bytes.get(i + 1) == Some(&b'\'') => {
                let start = i;
                let (text, end) = string_literal(bytes, i + 1, true)?;
                i = end;
                out.push((Token::StringLit(text), start));
            }
            // `B'…'` / `X'…'` — a bit-string literal, binary or hexadecimal.
            // The marker is kept on the token so the parser can run PostgreSQL's
            // `bit_in`, which is the routine that decides `x` means hex. Checked
            // ahead of the identifier arm, and only when the quote follows the
            // letter immediately, so `box'x'` is still the identifier `box`.
            b'b' | b'B' | b'x' | b'X' if bytes.get(i + 1) == Some(&b'\'') => {
                let start = i;
                let marker = bytes[i].to_ascii_lowercase();
                let (text, end) = string_literal(bytes, i + 1, false)?;
                i = end;
                out.push((
                    Token::BitStringLit(format!("{}{text}", char::from(marker))),
                    start,
                ));
            }
            b'"' => {
                let start = i;
                let (name, end) = quoted_ident(sql, bytes, start, start)?;
                i = end;
                out.push((Token::Ident(name), start));
            }
            // `U&'…'` / `U&"…"` — a Unicode-escape string or quoted identifier.
            // `PostgreSQL`'s `[uU]&{quote}` admits nothing between the three
            // characters, so `u & 'x'` is still a bitwise AND of a column named
            // `u`, and `u&'x'` is still the literal (the same reading there).
            b'u' | b'U'
                if bytes.get(i + 1) == Some(&b'&')
                    && matches!(bytes.get(i + 2), Some(b'\'' | b'"')) =>
            {
                let start = i;
                let (token, end) = unicode_literal(sql, bytes, start)?;
                i = end;
                out.push((token, start));
            }
            b'$' if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                let start = i;
                i += 1;
                let ds = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if bytes.get(i).is_some_and(|&b| is_ident_cont(b) || b == b'$') {
                    return Err(ParseError::new("trailing junk after parameter", start));
                }
                let n: i32 = sql[ds..i]
                    .parse()
                    .map_err(|_| ParseError::new("parameter number too large", start))?;
                let n = u32::try_from(n)
                    .map_err(|_| ParseError::new("parameter number too large", start))?;
                out.push((Token::Param(n), start));
            }
            // `$$…$$` / `$tag$…$tag$` — dollar quoting. The body is taken verbatim
            // (no escape processing, no `''` doubling), so the token is an ordinary
            // string literal.
            b'$' => {
                let start = i;
                let (text, end) = dollar_quoted(sql, bytes, i)?;
                i = end;
                out.push((Token::StringLit(text), start));
            }
            // A numeric literal: a decimal/hex/octal/binary integer, or a
            // `numeric` literal if it has a fractional part (`.`) or an exponent
            // (`e`/`E`). A leading `.` only starts a number when a digit follows;
            // a bare `.` falls through to the SP33 Dot arm.
            c if c.is_ascii_digit()
                || (c == b'.' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit)) =>
            {
                let start = i;
                let (token, end) = scan_number(sql, bytes, i)?;
                i = end;
                out.push((token, start));
            }
            // SP33: a `.` that does not begin a number lexeme is the qualified-name
            // separator. The numeric arm above already claimed `.5`/`2.`, so any `.`
            // reaching here is a separator (`a.col`).
            b'.' => {
                out.push((Token::Dot, i));
                i += 1;
            }
            c if c == b'_' || c.is_ascii_alphabetic() => {
                let start = i;
                while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
                    i += 1;
                }
                let word = sql[start..i].to_ascii_lowercase();
                let tok = match Keyword::from_word(&word) {
                    Some(kw) => Token::Keyword(kw),
                    None => Token::Ident(word),
                };
                out.push((tok, start));
            }
            // Every operator/punctuation lexeme. Reached last so the arms
            // above keep their claim on `--`, `/*`, quotes, `$1`, numbers and
            // words; within it, maximal munch is `punctuation`'s job.
            _ => {
                let Some((token, len)) = punctuation(bytes, i) else {
                    return Err(ParseError::new(
                        format!("unexpected character {:?}", c as char),
                        i,
                    ));
                };
                out.push((token, i));
                i += len;
            }
        }
    }
    out.push((Token::Eof, sql.len()));
    Ok(out)
}

/// A byte that may start an identifier (`PostgreSQL`'s `ident_start`): a letter,
/// `_`, or any non-ASCII byte.
fn is_ident_start(b: u8) -> bool {
    b == b'_' || b.is_ascii_alphabetic() || b >= 0x80
}

/// A byte that may continue an identifier (`PostgreSQL`'s `ident_cont`).
fn is_ident_cont(b: u8) -> bool {
    is_ident_start(b) || b.is_ascii_digit()
}

/// The end of the identifier starting at `i`, or `i` itself when no identifier
/// starts there.
fn ident_end(bytes: &[u8], i: usize) -> usize {
    if !bytes.get(i).is_some_and(|&b| is_ident_start(b)) {
        return i;
    }
    let mut j = i + 1;
    while bytes.get(j).is_some_and(|&b| is_ident_cont(b)) {
        j += 1;
    }
    j
}

/// The end of `PostgreSQL`'s `{digit}(_?{digit})*` run in `radix`, i.e. digits
/// with optional SINGLE `_` separators that must sit BETWEEN two digits.
/// Returns `start` when no digit is present there.
fn digit_run(bytes: &[u8], start: usize, radix: u32) -> usize {
    let is_digit = |i: usize| bytes.get(i).is_some_and(|&b| char::from(b).is_digit(radix));
    if !is_digit(start) {
        return start;
    }
    let mut i = start + 1;
    loop {
        if bytes.get(i) == Some(&b'_') && is_digit(i + 1) {
            i += 2;
        } else if is_digit(i) {
            i += 1;
        } else {
            return i;
        }
    }
}

/// Render a radix-`radix` digit string (already separator-free) as a decimal
/// string of unbounded width.
///
/// The width matters. `PostgreSQL` does not reject an integer literal that
/// overflows `int8`. It widens the literal to `numeric`
/// (`0xFFFFFFFFFFFFFFFFF` is `295147905179352825855`). So the lexer must not
/// truncate to a machine integer here. Code downstream decides the value's
/// type from the decimal text.
fn radix_to_decimal(digits: &str, radix: u32) -> String {
    // Little-endian decimal digits, multiplied by `radix` per input digit.
    let mut out: Vec<u8> = vec![0];
    for c in digits.chars() {
        let mut carry = c
            .to_digit(radix)
            .expect("the scanner accepted only radix digits");
        for d in &mut out {
            let t = u32::from(*d) * radix + carry;
            *d = u8::try_from(t % 10).expect("a decimal digit is below 10");
            carry = t / 10;
        }
        while carry > 0 {
            out.push(u8::try_from(carry % 10).expect("a decimal digit is below 10"));
            carry /= 10;
        }
    }
    while out.len() > 1 && out.last() == Some(&0) {
        out.pop();
    }
    out.iter().rev().map(|d| char::from(b'0' + d)).collect()
}

/// Scan the numeric literal at `start` and return its token with the byte
/// offset just past it.
///
/// Mirrors `PostgreSQL`'s `scan.l` literal grammar: `0x`/`0o`/`0b` integers
/// (case-insensitive prefix), `_` digit separators anywhere a separator may sit
/// between two digits, an optional fraction, and an optional exponent. A literal
/// immediately followed by an identifier character is `PostgreSQL`'s "trailing
/// junk after numeric literal" (42601), not two tokens.
fn scan_number(sql: &str, bytes: &[u8], start: usize) -> Result<(Token, usize), ParseError> {
    // `junk` is what turns `1abc` / `1000_` / `0x1g` into one error rather than a
    // number followed by an identifier.
    let junk = |end: usize| {
        bytes
            .get(end)
            .is_some_and(|&b| is_ident_cont(b) || b == b'$')
            .then(|| ParseError::new("trailing junk after numeric literal", start))
    };
    if bytes[start] == b'0'
        && let Some(&marker) = bytes.get(start + 1)
        && let Some((radix, kind)) = match marker {
            b'x' | b'X' => Some((16, "hexadecimal")),
            b'o' | b'O' => Some((8, "octal")),
            b'b' | b'B' => Some((2, "binary")),
            _ => None,
        }
    {
        // `0x(_?{hexdigit})+` — unlike the decimal run, the FIRST group may lead
        // with a separator, so `0x_1F` is a valid 31.
        let digits_start = start + 2 + usize::from(bytes.get(start + 2) == Some(&b'_'));
        let end = digit_run(bytes, digits_start, radix);
        if end == digits_start {
            // No digits at all. `0x` is "invalid hexadecimal integer", but `0xg`
            // is longer read as the decimal `0` plus the identifier `xg`, which
            // is trailing junk — PostgreSQL's longest-match resolves the tie.
            let mut fail_end = start + 2;
            while bytes.get(fail_end) == Some(&b'_') {
                fail_end += 1;
            }
            if ident_end(bytes, start + 1) > fail_end {
                return Err(ParseError::new(
                    "trailing junk after numeric literal",
                    start,
                ));
            }
            return Err(ParseError::new(format!("invalid {kind} integer"), start));
        }
        if let Some(e) = junk(end) {
            return Err(e);
        }
        let digits: String = sql[start + 2..end].chars().filter(|c| *c != '_').collect();
        return Ok((Token::IntLit(radix_to_decimal(&digits, radix)), end));
    }

    let mut is_float = false;
    let mut end = if bytes[start] == b'.' {
        is_float = true;
        digit_run(bytes, start + 1, 10)
    } else {
        let int_end = digit_run(bytes, start, 10);
        // `1..2` is the integer `1` followed by `..`, never the float `1.`
        // (PostgreSQL throws the second dot back rather than absorbing the first).
        if bytes.get(int_end) == Some(&b'.') && bytes.get(int_end + 1) != Some(&b'.') {
            is_float = true;
            digit_run(bytes, int_end + 1, 10)
        } else {
            int_end
        }
    };
    if matches!(bytes.get(end), Some(b'e' | b'E')) {
        // The exponent is consumed only if a (signed) digit run actually follows.
        // Otherwise `e` is left in place and the junk check below rejects `1e`,
        // which is what PostgreSQL does — an exponent-less `e` never starts an
        // identifier that touches a number.
        let signed = end + 1 + usize::from(matches!(bytes.get(end + 1), Some(b'+' | b'-')));
        let exp_end = digit_run(bytes, signed, 10);
        if exp_end > signed {
            is_float = true;
            end = exp_end;
        }
    }
    if let Some(e) = junk(end) {
        return Err(e);
    }
    let text: String = sql[start..end].chars().filter(|c| *c != '_').collect();
    Ok((
        if is_float {
            Token::FloatLit(text)
        } else {
            Token::IntLit(text)
        },
        end,
    ))
}

/// Turn the accumulated literal bytes into a `String`.
///
/// This function rejects what `PostgreSQL` rejects: an embedded NUL, and any
/// byte sequence that is not valid UTF-8 (22021). Only `E'…'` escapes can
/// produce either one. The other literal forms copy already-valid input.
fn decode_utf8(bytes: Vec<u8>, position: usize) -> Result<String, ParseError> {
    if bytes.contains(&0) {
        return Err(ParseError::new_sqlstate(
            UNTRANSLATABLE,
            "invalid byte sequence for encoding \"UTF8\": 0x00",
            position,
        ));
    }
    String::from_utf8(bytes).map_err(|e| {
        let byte = e.as_bytes()[e.utf8_error().valid_up_to()];
        ParseError::new_sqlstate(
            UNTRANSLATABLE,
            format!("invalid byte sequence for encoding \"UTF8\": 0x{byte:02x}"),
            position,
        )
    })
}

/// Scan a string literal whose opening quote is at `quote` and return its
/// decoded text with the offset just past its closing quote.
///
/// `escape` selects `E'…'` semantics, where the lexer expands backslash
/// escapes. A standard literal treats `\` as an ordinary character. Both forms
/// read `''` as one embedded quote. Both also absorb `PostgreSQL`'s literal
/// continuation: two literals separated by whitespace CONTAINING A NEWLINE are
/// one literal, and the continuation keeps the escape mode of the literal it
/// continues.
fn string_literal(bytes: &[u8], quote: usize, escape: bool) -> Result<(String, usize), ParseError> {
    let mut buf: Vec<u8> = Vec::new();
    let mut i = quote;
    loop {
        i += 1;
        loop {
            match bytes.get(i) {
                None => return Err(ParseError::new("unterminated quoted string", quote)),
                Some(&b'\'') if bytes.get(i + 1) == Some(&b'\'') => {
                    buf.push(b'\'');
                    i += 2;
                }
                Some(&b'\'') => {
                    i += 1;
                    break;
                }
                Some(&b'\\') if escape => i = escape_sequence(bytes, i, &mut buf, quote)?,
                Some(&b) => {
                    buf.push(b);
                    i += 1;
                }
            }
        }
        match literal_continuation(bytes, i) {
            Some(next) => i = next,
            None => break,
        }
    }
    Ok((decode_utf8(buf, quote)?, i))
}

/// `PostgreSQL`'s `quotecontinue`.
///
/// From the offset just past a closing quote, skip whitespace and `--`
/// comments across a NEWLINE. If the next byte is a quote, the literal
/// continues there. Returns the offset of that quote, or `None` when this
/// literal ended.
///
/// The newline is necessary. `'a' 'b'` on ONE line is a syntax error in
/// `PostgreSQL`, but the same pair split across lines is the single value
/// `ab`.
///
/// BLOCK comments deliberately do not count. `PostgreSQL`'s `quotecontinue`
/// admits only `{space}` and the `--` line comment, because a separate start
/// condition scans `/* … */`. So `'a'/* c */\n'b'` is a syntax error there and
/// must be one here too.
fn literal_continuation(bytes: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    let mut newline = false;
    loop {
        match bytes.get(i) {
            Some(&b'\n') => {
                newline = true;
                i += 1;
            }
            Some(&(b' ' | b'\t' | b'\r' | b'\x0c')) => i += 1,
            Some(&b'-') if bytes.get(i + 1) == Some(&b'-') => {
                while bytes.get(i).is_some_and(|&b| b != b'\n') {
                    i += 1;
                }
            }
            _ => break,
        }
    }
    (newline && bytes.get(i) == Some(&b'\'')).then_some(i)
}

/// Scan the double-quoted identifier whose opening quote is at `quote` and
/// return its text with the offset just past the closing quote. A doubled `""`
/// inside the delimiters is one embedded quote.
///
/// `PostgreSQL`'s `scan.l` rejects a delimiter pair enclosing nothing at the
/// moment it closes the identifier (`literallen == 0`), so `""` never becomes a
/// name — the error fires wherever a quoted identifier may appear, not only in
/// the clause that would have consumed it. A doubled quote INSIDE the
/// delimiters is an escape rather than a delimiter, so this never fires for
/// `""""` (the one-character identifier `"`) or for `"a""b"`. Only the
/// accumulated name's length decides, and it is the RAW length: `U&""` is
/// rejected before any Unicode escape could have filled it.
///
/// `lexeme` is where the whole token starts, which is the quote itself except
/// under a `U&"…"` prefix. The message quotes the source from there, the way
/// `PostgreSQL`'s scanner quotes the lexeme it was reading.
fn quoted_ident(
    sql: &str,
    bytes: &[u8],
    lexeme: usize,
    quote: usize,
) -> Result<(String, usize), ParseError> {
    let mut s = Vec::new();
    let mut i = quote + 1;
    loop {
        match bytes.get(i) {
            None => return Err(ParseError::new("unterminated quoted identifier", lexeme)),
            Some(&b'"') if bytes.get(i + 1) == Some(&b'"') => {
                s.push(b'"');
                i += 2;
            }
            Some(&b'"') => {
                i += 1;
                break;
            }
            Some(&b) => {
                s.push(b);
                i += 1;
            }
        }
    }
    if s.is_empty() {
        return Err(ParseError::new_sqlstate(
            SYNTAX_ERROR,
            format!(
                "zero-length delimited identifier at or near \"{}\"",
                &sql[lexeme..i]
            ),
            lexeme,
        ));
    }
    Ok((decode_utf8(s, lexeme)?, i))
}

/// Scan the `U&'…'` string literal or `U&"…"` quoted identifier at `start`,
/// together with the `UESCAPE 'c'` clause that may follow either one.
///
/// UESCAPE is consumed HERE, in the lexer, and not in the grammar.
/// `PostgreSQL` resolves it in the same place — `base_yylex`, the one-token
/// lookahead filter it puts between the scanner and the parser — because the
/// clause belongs to the literal and to nothing else. `U&'…'` stands wherever a
/// string stands and `U&"…"` wherever a name stands, so a token that still
/// carried an unresolved escape character would have to be handled again in
/// every one of those productions. Resolving it here means the parser only ever
/// sees the ordinary [`Token::StringLit`] and [`Token::Ident`] it already
/// understands, and `U&' \' UESCAPE '!'` yields the two characters ` \` without
/// any production knowing that a Unicode literal exists.
fn unicode_literal(sql: &str, bytes: &[u8], start: usize) -> Result<(Token, usize), ParseError> {
    let quote = start + 2;
    let is_ident = bytes[quote] == b'"';
    let (raw, after_literal) = if is_ident {
        quoted_ident(sql, bytes, start, quote)?
    } else {
        string_literal(bytes, quote, false)?
    };
    let (escape, end) = uescape_clause(sql, bytes, after_literal)?;
    let text = unicode_deescape(&raw, escape, quote + 1)?;
    Ok((
        if is_ident {
            Token::Ident(text)
        } else {
            Token::StringLit(text)
        },
        end,
    ))
}

/// Read the optional `UESCAPE 'c'` that follows a `U&` literal ending at
/// `from`, and return the escape character it selects with the offset just past
/// the clause. Without a clause the escape is the default `\` and `from` is
/// unchanged.
///
/// `c` must be exactly one character, and `PostgreSQL` refuses the ones that
/// would make an escape unreadable: a hex digit, `+`, either quote, and
/// whitespace.
fn uescape_clause(sql: &str, bytes: &[u8], from: usize) -> Result<(u8, usize), ParseError> {
    let word = skip_whitespace_and_comments(bytes, from);
    let word_end = ident_end(bytes, word);
    if !sql[word..word_end].eq_ignore_ascii_case("uescape") {
        return Ok((DEFAULT_UESCAPE, from));
    }
    let at = skip_whitespace_and_comments(bytes, word_end);
    if bytes.get(at) != Some(&b'\'') {
        return Err(ParseError::new_sqlstate(
            SYNTAX_ERROR,
            format!(
                "UESCAPE must be followed by a simple string literal {}",
                at_or_near(sql, bytes, at)
            ),
            at,
        )
        .reporting_position());
    }
    let (text, end) = string_literal(bytes, at, false)?;
    let [escape] = text.as_bytes() else {
        return Err(invalid_uescape_char(sql, at, end));
    };
    if escape.is_ascii_hexdigit()
        || matches!(escape, b'+' | b'\'' | b'"')
        || escape.is_ascii_whitespace()
    {
        return Err(invalid_uescape_char(sql, at, end));
    }
    Ok((*escape, end))
}

/// The rejection of a `UESCAPE` character, quoting the string literal that
/// named it exactly as it was written.
fn invalid_uescape_char(sql: &str, at: usize, end: usize) -> ParseError {
    ParseError::new_sqlstate(
        SYNTAX_ERROR,
        format!(
            "invalid Unicode escape character at or near \"{}\"",
            &sql[at..end]
        ),
        at,
    )
    .reporting_position()
}

/// Expand the Unicode escapes in the body of a `U&'…'` / `U&"…"` literal.
///
/// `escape` introduces `XXXX` (exactly 4 hex digits) or `+XXXXXX` (exactly 6),
/// each naming one code point, and doubling it writes the character itself.
/// A high surrogate must be followed IMMEDIATELY by the escape of its low half,
/// and the pair becomes the one code point it encodes; a lone half of a pair is
/// an error. `base` is the source offset the body starts at, so the errors land
/// where `PostgreSQL` puts its cursor.
///
/// `E'…'`'s `\uXXXX` / `\UXXXXXXXX` spellings are deliberately absent. They
/// belong to the other form, and `U&'\u0061'` is an error in `PostgreSQL`
/// because `u006` is not four hex digits.
fn unicode_deescape(body: &str, escape: u8, base: usize) -> Result<String, ParseError> {
    let bytes = body.as_bytes();
    let mut out = String::new();
    // The high half of a surrogate pair, once one has been read: the next
    // escape has to complete it.
    let mut pair_first: Option<u32> = None;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != escape {
            if pair_first.is_some() {
                return Err(invalid_surrogate_pair(base + i));
            }
            let ch = body[i..]
                .chars()
                .next()
                .expect("i indexes a character boundary");
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        if bytes.get(i + 1) == Some(&escape) {
            if pair_first.is_some() {
                return Err(invalid_surrogate_pair(base + i));
            }
            out.push(char::from(escape));
            i += 2;
            continue;
        }
        let (value, next) = if let Some(value) = hex_run(bytes, i + 1, 4) {
            (value, i + 5)
        } else if bytes.get(i + 1) == Some(&b'+')
            && let Some(value) = hex_run(bytes, i + 2, 6)
        {
            (value, i + 8)
        } else {
            return Err(
                ParseError::new_sqlstate(SYNTAX_ERROR, "invalid Unicode escape", base + i)
                    .with_hint(UESCAPE_HINT)
                    .reporting_position(),
            );
        };
        if value == 0 || value > 0x10_FFFF {
            return Err(invalid_escape_value(base + i));
        }
        // A surrogate is only ever half a character, so it is held back until
        // its partner arrives rather than encoded on its own.
        let value = match (pair_first, value) {
            (Some(high), 0xdc00..0xe000) => 0x1_0000 + ((high - 0xd800) << 10) + (value - 0xdc00),
            (Some(_), _) | (None, 0xdc00..0xe000) => {
                return Err(invalid_surrogate_pair(base + i));
            }
            (None, _) => value,
        };
        pair_first = None;
        if (0xd800..0xdc00).contains(&value) {
            pair_first = Some(value);
        } else {
            out.push(char::from_u32(value).ok_or_else(|| invalid_escape_value(base + i))?);
        }
        i = next;
    }
    if pair_first.is_some() {
        return Err(invalid_surrogate_pair(base + bytes.len()));
    }
    Ok(out)
}

/// The value of exactly `width` hexadecimal digits at `at`, or `None` when
/// fewer than that many are there.
fn hex_run(bytes: &[u8], at: usize, width: usize) -> Option<u32> {
    let mut value: u32 = 0;
    for k in 0..width {
        value = value * 16 + char::from(*bytes.get(at + k)?).to_digit(16)?;
    }
    Some(value)
}

fn invalid_surrogate_pair(position: usize) -> ParseError {
    ParseError::new_sqlstate(SYNTAX_ERROR, "invalid Unicode surrogate pair", position)
        .reporting_position()
}

fn invalid_escape_value(position: usize) -> ParseError {
    ParseError::new_sqlstate(SYNTAX_ERROR, "invalid Unicode escape value", position)
        .reporting_position()
}

/// `PostgreSQL`'s `at or near "…"` clause for the token at `at`, or its `at end
/// of input` when nothing is left.
///
/// The scanner quotes source text, so the lexeme is echoed as it was written.
fn at_or_near(sql: &str, bytes: &[u8], at: usize) -> String {
    let Some(&b) = bytes.get(at) else {
        return "at end of input".to_string();
    };
    let end = if is_ident_start(b) {
        ident_end(bytes, at)
    } else if b.is_ascii_digit() {
        // A number's own scan can fail on trailing junk, and an error message is
        // no place to raise a second error; the digit run is enough to name it.
        digit_run(bytes, at, 10)
    } else {
        // Anything else is one operator lexeme, or — where even that does not
        // match — the single character sitting there.
        punctuation(bytes, at).map_or_else(
            || at + sql[at..].chars().next().map_or(1, char::len_utf8),
            |(_, len)| at + len,
        )
    };
    format!("at or near \"{}\"", &sql[at..end])
}

/// Skip the run of whitespace and comments at `from`, the way the scanner's own
/// loop does between two tokens. An unterminated block comment stops the skip
/// where it started, so [`lex`] reports it against the comment and not against
/// whatever the lookahead was after.
fn skip_whitespace_and_comments(bytes: &[u8], from: usize) -> usize {
    let mut i = from;
    loop {
        match bytes.get(i) {
            Some(&(b' ' | b'\t' | b'\r' | b'\n' | b'\x0c')) => i += 1,
            Some(&b'-') if bytes.get(i + 1) == Some(&b'-') => {
                while bytes.get(i).is_some_and(|&b| b != b'\n') {
                    i += 1;
                }
            }
            Some(&b'/') if bytes.get(i + 1) == Some(&b'*') => match skip_block_comment(bytes, i) {
                (end, true) => i = end,
                (_, false) => return i,
            },
            _ => return i,
        }
    }
}

/// Expand one `E'…'` backslash escape that starts at the `\` at `i`.
///
/// The function appends the escape's bytes to `buf` and returns the offset
/// just past it. `\ddd` (octal) and `\xhh` (hex) address RAW BYTES, so a pair
/// of them can spell one multi-byte character. [`decode_utf8`] judges validity
/// once, over the whole literal. An unrecognised escape gives the escaped
/// character itself (`\q` is `q`), which is what `PostgreSQL` does.
fn escape_sequence(
    bytes: &[u8],
    i: usize,
    buf: &mut Vec<u8>,
    quote: usize,
) -> Result<usize, ParseError> {
    let Some(&c) = bytes.get(i + 1) else {
        return Err(ParseError::new("unterminated quoted string", quote));
    };
    if let Some(b) = match c {
        b'b' => Some(0x08),
        b'f' => Some(0x0c),
        b'n' => Some(b'\n'),
        b'r' => Some(b'\r'),
        b't' => Some(b'\t'),
        b'v' => Some(0x0b),
        _ => None,
    } {
        buf.push(b);
        return Ok(i + 2);
    }
    if (b'0'..=b'7').contains(&c) {
        let mut value: u32 = 0;
        let mut j = i + 1;
        while j < i + 4 && bytes.get(j).is_some_and(|&b| (b'0'..=b'7').contains(&b)) {
            value = value * 8 + u32::from(bytes[j] - b'0');
            j += 1;
        }
        buf.push(u8::try_from(value & 0xff).expect("masked to one byte"));
        return Ok(j);
    }
    if c == b'x' {
        let mut value: u32 = 0;
        let mut j = i + 2;
        while j < i + 4
            && let Some(d) = bytes.get(j).and_then(|b| char::from(*b).to_digit(16))
        {
            value = value * 16 + d;
            j += 1;
        }
        if j == i + 2 {
            // `\x` with no hex digit is the literal `x` (PostgreSQL's fallthrough).
            buf.push(b'x');
            return Ok(i + 2);
        }
        buf.push(u8::try_from(value & 0xff).expect("masked to one byte"));
        return Ok(j);
    }
    if c == b'u' || c == b'U' {
        return unicode_escape(bytes, i, buf, quote);
    }
    // `\'`, `\\`, `\"` and every other escape are the escaped byte itself.
    buf.push(c);
    Ok(i + 2)
}

/// Expand a `\uXXXX` (4 hex digits) or `\UXXXXXXXX` (8) escape.
///
/// The function applies `PostgreSQL`'s surrogate-pair rule: a `\u` low
/// surrogate must immediately follow a high surrogate, and the pair encodes
/// one character. An unpaired surrogate is 42601 "invalid Unicode surrogate
/// pair".
fn unicode_escape(
    bytes: &[u8],
    i: usize,
    buf: &mut Vec<u8>,
    quote: usize,
) -> Result<usize, ParseError> {
    let width = if bytes[i + 1] == b'u' { 4 } else { 8 };
    let read = |at: usize| -> Option<(u32, usize)> {
        let mut value: u32 = 0;
        for k in 0..width {
            value = value * 16 + char::from(*bytes.get(at + k)?).to_digit(16)?;
        }
        Some((value, at + width))
    };
    let Some((first, mut j)) = read(i + 2) else {
        return Err(ParseError::new("invalid Unicode escape value", quote));
    };
    let code = if (0xd800..0xdc00).contains(&first) {
        // A high surrogate: the low half must follow as its own `\u` escape.
        let paired = (bytes.get(j) == Some(&b'\\') && bytes.get(j + 1) == Some(&b'u'))
            .then(|| read(j + 2))
            .flatten()
            .filter(|(low, _)| (0xdc00..0xe000).contains(low));
        let Some((low, after)) = paired else {
            return Err(ParseError::new("invalid Unicode surrogate pair", quote));
        };
        j = after;
        0x1_0000 + ((first - 0xd800) << 10) + (low - 0xdc00)
    } else if (0xdc00..0xe000).contains(&first) {
        return Err(ParseError::new("invalid Unicode surrogate pair", quote));
    } else {
        first
    };
    let Some(ch) = char::from_u32(code) else {
        return Err(ParseError::new("invalid Unicode escape value", quote));
    };
    let mut encoded = [0u8; 4];
    buf.extend_from_slice(ch.encode_utf8(&mut encoded).as_bytes());
    Ok(j)
}

/// Scan a `$tag$…$tag$` (or `$$…$$`) dollar-quoted literal at the `$` at
/// `start`.
///
/// The function returns the verbatim body and the offset just past the closing
/// delimiter. The tag follows `PostgreSQL`'s `dolqdelim`: it may be empty, and
/// otherwise it starts with a letter/`_`/non-ASCII byte. Distinct tags nest,
/// because only the exact opening delimiter closes the literal. So the body of
/// `$outer$ … $inner$ … $outer$` includes the inner delimiters verbatim.
fn dollar_quoted(sql: &str, bytes: &[u8], start: usize) -> Result<(String, usize), ParseError> {
    let tag_end = ident_end(bytes, start + 1);
    if bytes.get(tag_end) != Some(&b'$') {
        return Err(ParseError::new("unexpected character '$'", start));
    }
    let delim = &sql[start..=tag_end];
    let body_start = tag_end + 1;
    let Some(offset) = sql[body_start..].find(delim) else {
        return Err(ParseError::new("unterminated dollar-quoted string", start));
    };
    let body_end = body_start + offset;
    Ok((
        sql[body_start..body_end].to_string(),
        body_end + delim.len(),
    ))
}

/// Skip the block comment that opens at `i`, with `PostgreSQL`'s nesting rule.
/// Returns the offset just past it and whether the comment was actually
/// closed.
fn skip_block_comment(bytes: &[u8], i: usize) -> (usize, bool) {
    let mut depth = 1usize;
    let mut j = i + 2;
    while j < bytes.len() {
        if bytes[j] == b'/' && bytes.get(j + 1) == Some(&b'*') {
            depth += 1;
            j += 2;
        } else if bytes[j] == b'*' && bytes.get(j + 1) == Some(&b'/') {
            depth -= 1;
            j += 2;
            if depth == 0 {
                return (j, true);
            }
        } else {
            j += 1;
        }
    }
    (j, false)
}

/// Match the longest operator/punctuation lexeme that starts at `bytes[i]`.
///
/// MAXIMAL MUNCH is the whole contract of this function: every spelling whose
/// first byte also begins a shorter spelling is listed longest-first — `->>`
/// before `->` before `-`, `#>>` before `#>` before `##` before `#`, `?-|`
/// before `?-` and `?||` before `?|`, then `?#`/`?&` before `?`, `@-@` before
/// `@>`/`@?`/`@@`/`@`, `!~*` before `!~`, `||/` before `||` before `|/` before
/// `|`, `<<=` before `<<`, `>>=` before `>>`, `<@`/`<=`/`<>`/`<^`/`<<` before
/// `<`, `>^` before `>`, `::` before `:`. A slip re-reads `a->>'k'` as
/// `a -> >'k'`, whose tail still lexes, so the lexer tests pin each
/// neighbouring shorter spelling explicitly.
///
/// The comment arms in [`lex`] claim `--` and `/*` before this function runs,
/// so a `-` or `/` that reaches here is always the operator. The one place that
/// still matters is `||/`. `PostgreSQL` stops an operator at an embedded
/// comment opener, so `'a' ||/* c */ 'b'` is concatenation, not a cube root.
fn punctuation(bytes: &[u8], i: usize) -> Option<(Token, usize)> {
    let next_is = |byte: u8| bytes.get(i + 1) == Some(&byte);
    let next_two_are = |first: u8, second: u8| next_is(first) && bytes.get(i + 2) == Some(&second);
    let comment_at = |at: usize| bytes.get(at) == Some(&b'*');
    Some(match bytes[i] {
        b'-' if next_two_are(b'|', b'-') => (Token::Adjacent, 3),
        b'-' if next_two_are(b'>', b'>') => (Token::JsonGetText, 3),
        b'-' if next_is(b'>') => (Token::JsonGet, 2),
        b'#' if next_two_are(b'>', b'>') => (Token::JsonGetPathText, 3),
        b'#' if next_is(b'>') => (Token::JsonGetPath, 2),
        b'#' if next_is(b'#') => (Token::ClosestPoint, 2),
        // `@-@` leads the `@` family. Its second byte is unique among them, so
        // ordering is not load-bearing here — but the geometric spellings are
        // the ones a reader is least likely to expect, so they go first.
        b'@' if next_two_are(b'-', b'@') => (Token::Length, 3),
        b'@' if next_is(b'>') => (Token::Contains, 2),
        b'@' if next_is(b'?') => (Token::JsonPathExists, 2),
        b'@' if next_is(b'@') => (Token::JsonPathMatch, 2),
        b'<' if next_is(b'@') => (Token::ContainedBy, 2),
        b'?' if next_two_are(b'-', b'|') => (Token::Perpendicular, 3),
        b'?' if next_two_are(b'|', b'|') => (Token::Parallel, 3),
        b'?' if next_is(b'|') => (Token::KeyExistsAny, 2),
        b'?' if next_is(b'-') => (Token::Horizontal, 2),
        b'?' if next_is(b'#') => (Token::Intersects, 2),
        b'?' if next_is(b'&') => (Token::KeyExistsAll, 2),
        b'?' => (Token::KeyExists, 1),
        b'&' if next_is(b'&') => (Token::Overlaps, 2),
        b'&' if next_two_are(b'<', b'|') => (Token::DoesNotExtendAbove, 3),
        b'&' if next_is(b'<') => (Token::DoesNotExtendRight, 2),
        b'&' if next_is(b'>') => (Token::DoesNotExtendLeft, 2),
        b'<' if next_two_are(b'-', b'>') => (Token::Phrase, 3),
        b'!' if next_is(b'!') => (Token::TsNot, 2),
        b'!' if next_two_are(b'~', b'*') => (Token::NotTildeCi, 3),
        b'!' if next_is(b'~') => (Token::NotTilde, 2),
        // PostgreSQL accepts `!=` as a spelling of `<>`.
        b'!' if next_is(b'=') => (Token::Ne, 2),
        b'~' if next_is(b'=') => (Token::Same, 2),
        b'~' if next_is(b'*') => (Token::TildeCi, 2),
        b'~' => (Token::Tilde, 1),
        // `=>` before `=`. PostgreSQL's scanner returns EQUALS_GREATER for it
        // and refuses to let any user operator take the spelling, so the
        // two-byte reading is the only one.
        b'=' if next_is(b'>') => (Token::NamedArg, 2),
        b'<' if next_is(b'=') => (Token::Le, 2),
        b'>' if next_is(b'=') => (Token::Ge, 2),
        b'<' if next_is(b'>') => (Token::Ne, 2),
        // The geometric `<^`/`>^`. Nothing extends them, and their `^` second
        // byte is unique in both families, so they cannot disturb `<@ <= <>
        // <<| <<= << <-> <` or `>= >>= >> >`.
        b'<' if next_is(b'^') => (Token::BelowEq, 2),
        b'>' if next_is(b'^') => (Token::AboveEq, 2),
        b'<' if next_two_are(b'<', b'|') => (Token::StrictlyBelow, 3),
        b'<' if next_two_are(b'<', b'=') => (Token::ContainedByOrEq, 3),
        b'<' if next_is(b'<') => (Token::Shl, 2),
        b'|' if next_two_are(b'&', b'>') => (Token::DoesNotExtendBelow, 3),
        b'|' if next_two_are(b'>', b'>') => (Token::StrictlyAbove, 3),
        b'>' if next_two_are(b'>', b'=') => (Token::ContainsOrEq, 3),
        b'>' if next_is(b'>') => (Token::Shr, 2),
        b'|' if next_two_are(b'|', b'/') && !comment_at(i + 3) => (Token::CubeRoot, 3),
        b'|' if next_is(b'|') => (Token::Concat, 2),
        b'|' if next_is(b'/') && !comment_at(i + 2) => (Token::SquareRoot, 2),
        b'|' => (Token::Pipe, 1),
        b'&' => (Token::Amp, 1),
        b'#' => (Token::Hash, 1),
        b'^' => (Token::Caret, 1),
        b'%' => (Token::Percent, 1),
        b'@' => (Token::At, 1),
        b':' if next_is(b':') => (Token::TypeCast, 2),
        // A lone `:` is only grammatical inside an array slice (`a[1:2]`); it is
        // lexed so the parser can refuse slices by name rather than failing with
        // a character-level lexer error.
        b':' => (Token::Colon, 1),
        b'[' => (Token::LBracket, 1),
        b']' => (Token::RBracket, 1),
        b'(' => (Token::LParen, 1),
        b')' => (Token::RParen, 1),
        b',' => (Token::Comma, 1),
        b';' => (Token::Semicolon, 1),
        b'*' => (Token::Star, 1),
        b'+' => (Token::Plus, 1),
        b'-' => (Token::Minus, 1),
        b'/' => (Token::Slash, 1),
        b'=' => (Token::Eq, 1),
        b'<' => (Token::Lt, 1),
        b'>' => (Token::Gt, 1),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::token::{Keyword, Token};

    fn toks(sql: &str) -> Vec<Token> {
        lex(sql).expect("lex").into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn keywords_idents_literals() {
        assert_eq!(
            toks("SELECT id FROM t WHERE x = 'a'"),
            vec![
                Token::Keyword(Keyword::Select),
                Token::Ident("id".into()),
                Token::Keyword(Keyword::From),
                Token::Ident("t".into()),
                Token::Keyword(Keyword::Where),
                Token::Ident("x".into()),
                Token::Eq,
                Token::StringLit("a".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn keywords_are_case_insensitive_idents_lowercased() {
        assert_eq!(toks("Select FOO")[0], Token::Keyword(Keyword::Select));
        assert_eq!(toks("Select FOO")[1], Token::Ident("foo".into()));
    }

    #[test]
    fn quoted_ident_preserves_case() {
        assert_eq!(toks("\"MixedCase\"")[0], Token::Ident("MixedCase".into()));
    }

    #[test]
    fn quoted_ident_escapes_doubled_quote() {
        assert_eq!(toks("\"a\"\"b\"")[0], Token::Ident("a\"b".into()));
    }

    #[test]
    fn string_escaping_doubles_quote() {
        assert_eq!(toks("'it''s'")[0], Token::StringLit("it's".into()));
    }

    #[test]
    fn comments_are_skipped() {
        assert_eq!(
            toks("1 -- c\n+ /* x */ 2"),
            vec![
                Token::IntLit("1".into()),
                Token::Plus,
                Token::IntLit("2".into()),
                Token::Eof
            ]
        );
    }

    #[test]
    fn operators_lex() {
        assert_eq!(
            toks("<= >= <> < > = + - * / ( ) , ;"),
            vec![
                Token::Le,
                Token::Ge,
                Token::Ne,
                Token::Lt,
                Token::Gt,
                Token::Eq,
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::LParen,
                Token::RParen,
                Token::Comma,
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn concat_operator_lexes_and_a_lone_pipe_is_bitwise_or() {
        use assert2::assert;

        // `||` is one token; with no surrounding spaces a slip in the two-byte
        // advance would mis-read the next byte.
        assert!(
            toks("a||b")
                == vec![
                    Token::Ident("a".into()),
                    Token::Concat,
                    Token::Ident("b".into()),
                    Token::Eof,
                ]
        );
        // A single `|` is bitwise OR; `||` must not steal it, nor it `||`.
        assert!(
            toks("a|b")
                == vec![
                    Token::Ident("a".into()),
                    Token::Pipe,
                    Token::Ident("b".into()),
                    Token::Eof,
                ]
        );
    }

    #[test]
    fn cast_operator_wins_maximal_munch_over_the_lone_colon() {
        // `::` is one token; with no surrounding spaces a slip in the two-byte
        // advance would mis-read the next byte.
        assert_eq!(
            toks("x::int4"),
            vec![
                Token::Ident("x".into()),
                Token::TypeCast,
                Token::Ident("int4".into()),
                Token::Eof,
            ]
        );
        // A single `:` is its own token (array-slice syntax, which the parser
        // refuses by name); it must never be mis-lexed as a cast.
        assert_eq!(
            toks("a : b"),
            vec![
                Token::Ident("a".into()),
                Token::Colon,
                Token::Ident("b".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn float_literals_lex_distinctly_from_ints() {
        // Fractional, leading-dot, trailing-dot, and exponent forms are FloatLit;
        // a bare integer stays IntLit.
        assert_eq!(toks("42"), vec![Token::IntLit("42".into()), Token::Eof]);
        assert_eq!(toks("1.5"), vec![Token::FloatLit("1.5".into()), Token::Eof]);
        assert_eq!(toks(".5"), vec![Token::FloatLit(".5".into()), Token::Eof]);
        assert_eq!(toks("2."), vec![Token::FloatLit("2.".into()), Token::Eof]);
        assert_eq!(
            toks("1e10"),
            vec![Token::FloatLit("1e10".into()), Token::Eof]
        );
        assert_eq!(
            toks("1.5E-3"),
            vec![Token::FloatLit("1.5E-3".into()), Token::Eof]
        );
        assert_eq!(
            toks("6e+2"),
            vec![Token::FloatLit("6e+2".into()), Token::Eof]
        );
        // `1 + 2.5` keeps the operator separate from the float.
        assert_eq!(
            toks("1 + 2.5"),
            vec![
                Token::IntLit("1".into()),
                Token::Plus,
                Token::FloatLit("2.5".into()),
                Token::Eof,
            ]
        );
        // A bare `.` with no following digit is now the SP33 Dot token (qualified-name
        // separator), not an error. The numeric arm only claims `.` when a digit follows.
        assert_eq!(toks("."), vec![Token::Dot, Token::Eof]);
        // An `e` not followed by a (signed) digit is left for the identifier lexer.
        assert_eq!(
            toks("3 e"),
            vec![
                Token::IntLit("3".into()),
                Token::Ident("e".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn unterminated_string_errors_with_position() {
        let e = lex("'abc").expect_err("unterminated");
        assert_eq!(e.position, 0);
    }

    #[test]
    fn lexes_parameter_placeholder() {
        assert_eq!(toks("$0")[0], Token::Param(0));
        assert_eq!(toks("$1")[0], Token::Param(1));
        assert_eq!(toks("$2147483647")[0], Token::Param(2_147_483_647));

        let error = lex("select $2147483648").expect_err("parameter exceeds signed int32");
        assert_eq!(error.position, 7);
        assert_eq!(
            error.message,
            "syntax error at position 7: parameter number too large"
        );
    }

    #[test]
    fn two_char_operators_advance_exactly_two_bytes() {
        // No surrounding spaces: a position-arithmetic slip in the two-byte
        // advance would mis-read the following byte as its own token.
        assert_eq!(
            toks("1<=2"),
            vec![
                Token::IntLit("1".into()),
                Token::Le,
                Token::IntLit("2".into()),
                Token::Eof
            ]
        );
        assert_eq!(
            toks("1>=2"),
            vec![
                Token::IntLit("1".into()),
                Token::Ge,
                Token::IntLit("2".into()),
                Token::Eof
            ]
        );
        assert_eq!(
            toks("1<>2"),
            vec![
                Token::IntLit("1".into()),
                Token::Ne,
                Token::IntLit("2".into()),
                Token::Eof
            ]
        );
    }

    #[test]
    fn line_comment_runs_to_eof_without_a_newline() {
        // A `--` comment with no trailing newline must end cleanly at EOF, never
        // reading one byte past the buffer.
        assert_eq!(toks("1 --eof"), vec![Token::IntLit("1".into()), Token::Eof]);
    }

    #[test]
    fn block_comment_at_start_of_input() {
        // A `/* */` comment at offset 0 exercises the comment-open advance from
        // the very first byte.
        assert_eq!(
            toks("/* c */1"),
            vec![Token::IntLit("1".into()), Token::Eof]
        );
    }

    #[test]
    fn block_comment_with_internal_star_only_closes_at_star_slash() {
        // A lone `*` inside a block comment is NOT the terminator — only `*/` is.
        assert_eq!(
            toks("/* a * b */1"),
            vec![Token::IntLit("1".into()), Token::Eof]
        );
    }

    #[test]
    fn nested_block_comments_are_skipped() {
        assert_eq!(
            toks("1 /* outer /* inner */ still outer */ + 2"),
            vec![
                Token::IntLit("1".into()),
                Token::Plus,
                Token::IntLit("2".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn unterminated_block_comment_errors_at_its_start() {
        let e = lex("/* c").expect_err("unterminated block comment");
        assert!(e.message.contains("unterminated block comment"));
        assert_eq!(e.position, 0);
    }

    #[test]
    fn unterminated_nested_block_comment_errors_at_outer_start() {
        let e = lex("1 + /* outer /* inner */ 2").expect_err("unterminated nested block comment");
        assert!(e.message.contains("unterminated block comment"));
        assert_eq!(e.position, 4);
    }

    #[test]
    fn unterminated_block_comment_ending_in_a_star_does_not_read_past_eof() {
        // A trailing `*` makes the `bytes[i] == b'*'` half of the terminator
        // check true, so the scan would read `bytes[i + 1]` — its `i + 1 < len`
        // bound is what stops that from running off the end. ("/* c" can't catch
        // this: 'c' is not `*`, so the `&&` short-circuits before bytes[i + 1].)
        let e = lex("/* *").expect_err("unterminated block comment");
        assert!(e.message.contains("unterminated block comment"));
        assert_eq!(e.position, 0);
    }

    #[test]
    fn lone_dollar_is_an_unexpected_character_not_a_bad_param() {
        // `$` only begins a parameter when a digit follows; otherwise it is an
        // unexpected character (this lexer has no dollar-quoting).
        let e = lex("$x").expect_err("$x is not a token");
        assert!(e.message.contains("unexpected character"));
        assert_eq!(e.position, 0);
    }

    #[test]
    fn dot_is_a_token_between_identifiers() {
        use crate::token::Token;
        assert_eq!(
            toks("a.col"),
            vec![
                Token::Ident("a".into()),
                Token::Dot,
                Token::Ident("col".into()),
                Token::Eof
            ]
        );
    }

    #[test]
    fn dot_does_not_disturb_numeric_literals() {
        use crate::token::Token;
        // A leading/trailing-dot float is still one FloatLit, not <int> Dot.
        assert_eq!(toks(".5"), vec![Token::FloatLit(".5".into()), Token::Eof]);
        assert_eq!(toks("2."), vec![Token::FloatLit("2.".into()), Token::Eof]);
    }

    #[test]
    fn join_keywords_lex() {
        use crate::token::{Keyword, Token};
        assert_eq!(
            toks("INNER JOIN ON USING NATURAL LEFT RIGHT FULL OUTER CROSS"),
            vec![
                Token::Keyword(Keyword::Inner),
                Token::Keyword(Keyword::Join),
                Token::Keyword(Keyword::On),
                Token::Keyword(Keyword::Using),
                Token::Keyword(Keyword::Natural),
                Token::Keyword(Keyword::Left),
                Token::Keyword(Keyword::Right),
                Token::Keyword(Keyword::Full),
                Token::Keyword(Keyword::Outer),
                Token::Keyword(Keyword::Cross),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn jsonb_and_array_operators_lex_with_maximal_munch() {
        use assert2::assert;

        // Every new operator, written WITHOUT surrounding spaces: a munch-order
        // slip would split `->>` into `->` `>` (etc.) and the trailing operand
        // would still lex, so only the exact token vector catches it.
        let cases: &[(&str, &[Token])] = &[
            ("a->'k'", &[Token::JsonGet, Token::StringLit("k".into())]),
            (
                "a->>'k'",
                &[Token::JsonGetText, Token::StringLit("k".into())],
            ),
            (
                "a#>'{k}'",
                &[Token::JsonGetPath, Token::StringLit("{k}".into())],
            ),
            (
                "a#>>'{k}'",
                &[Token::JsonGetPathText, Token::StringLit("{k}".into())],
            ),
            ("a@>b", &[Token::Contains, Token::Ident("b".into())]),
            ("a<@b", &[Token::ContainedBy, Token::Ident("b".into())]),
            ("a?'k'", &[Token::KeyExists, Token::StringLit("k".into())]),
            ("a?|b", &[Token::KeyExistsAny, Token::Ident("b".into())]),
            ("a?&b", &[Token::KeyExistsAll, Token::Ident("b".into())]),
            ("a&&b", &[Token::Overlaps, Token::Ident("b".into())]),
            (
                "a[1]",
                &[Token::LBracket, Token::IntLit("1".into()), Token::RBracket],
            ),
        ];
        for (sql, tail) in cases {
            let mut expected = vec![Token::Ident("a".into())];
            expected.extend_from_slice(tail);
            expected.push(Token::Eof);
            assert!(toks(sql) == expected, "lexing {sql:?}");
        }
    }

    #[test]
    fn new_operators_do_not_steal_the_shorter_spellings_they_share_a_prefix_with() {
        use assert2::assert;

        // `->` must not swallow the subtraction in `a-1`, `<@` must not shadow
        // `<=`/`<>`/`<`, and `||` must still be Concat now that `?|` exists.
        let cases: &[(&str, &[Token])] = &[
            ("a-1", &[Token::Minus, Token::IntLit("1".into())]),
            ("a<=b", &[Token::Le, Token::Ident("b".into())]),
            ("a<b", &[Token::Lt, Token::Ident("b".into())]),
            ("a<>b", &[Token::Ne, Token::Ident("b".into())]),
            ("a>=b", &[Token::Ge, Token::Ident("b".into())]),
            ("a>b", &[Token::Gt, Token::Ident("b".into())]),
            ("a||b", &[Token::Concat, Token::Ident("b".into())]),
            ("a::b", &[Token::TypeCast, Token::Ident("b".into())]),
        ];
        for (sql, tail) in cases {
            let mut expected = vec![Token::Ident("a".into())];
            expected.extend_from_slice(tail);
            expected.push(Token::Eof);
            assert!(toks(sql) == expected, "lexing {sql:?}");
        }
        // A `--` line comment still wins over `->`-style munching.
        assert!(toks("a --x") == vec![Token::Ident("a".into()), Token::Eof]);
    }

    #[test]
    fn operator_bytes_that_used_to_be_rejected_now_lex() {
        use assert2::assert;

        let cases: &[(&str, Token)] = &[
            ("a # b", Token::Hash),
            ("a & b", Token::Amp),
            ("a | b", Token::Pipe),
            ("a @ b", Token::At),
            ("a ~ b", Token::Tilde),
            ("a ^ b", Token::Caret),
            ("a % b", Token::Percent),
        ];
        for (sql, token) in cases {
            assert!(toks(sql)[1] == *token, "lexing {sql:?}");
        }
        // `!` is not an operator on its own — PostgreSQL dropped postfix `!`,
        // and the byte only leads `!=`, `!~` and `!~*`.
        let e = lex("a ! b").expect_err("lone bang");
        assert!(e.message.contains("unexpected character"));
    }

    #[test]
    fn new_operator_spellings_lex_with_maximal_munch() {
        use assert2::assert;

        // Written WITHOUT surrounding spaces: a munch-order slip splits `!~*`
        // into `!~` `*` (etc.) and the tail still lexes, so only the exact token
        // vector catches it. Each new spelling is paired with the SHORTER
        // spelling it shares a prefix with.
        let cases: &[(&str, &[Token])] = &[
            ("a~b", &[Token::Tilde, Token::Ident("b".into())]),
            ("a~*b", &[Token::TildeCi, Token::Ident("b".into())]),
            ("a!~b", &[Token::NotTilde, Token::Ident("b".into())]),
            ("a!~*b", &[Token::NotTildeCi, Token::Ident("b".into())]),
            ("a!=b", &[Token::Ne, Token::Ident("b".into())]),
            ("a<<b", &[Token::Shl, Token::Ident("b".into())]),
            ("a>>b", &[Token::Shr, Token::Ident("b".into())]),
            ("a<<=b", &[Token::ContainedByOrEq, Token::Ident("b".into())]),
            ("a>>=b", &[Token::ContainsOrEq, Token::Ident("b".into())]),
            ("a<=b", &[Token::Le, Token::Ident("b".into())]),
            ("a>=b", &[Token::Ge, Token::Ident("b".into())]),
            ("a<@b", &[Token::ContainedBy, Token::Ident("b".into())]),
            ("a<>b", &[Token::Ne, Token::Ident("b".into())]),
            ("a&b", &[Token::Amp, Token::Ident("b".into())]),
            ("a&&b", &[Token::Overlaps, Token::Ident("b".into())]),
            ("a#b", &[Token::Hash, Token::Ident("b".into())]),
            ("a#>b", &[Token::JsonGetPath, Token::Ident("b".into())]),
            ("a#>>b", &[Token::JsonGetPathText, Token::Ident("b".into())]),
            ("a@b", &[Token::At, Token::Ident("b".into())]),
            ("a@>b", &[Token::Contains, Token::Ident("b".into())]),
            ("a|b", &[Token::Pipe, Token::Ident("b".into())]),
            ("a||b", &[Token::Concat, Token::Ident("b".into())]),
            ("a|/b", &[Token::SquareRoot, Token::Ident("b".into())]),
            ("a||/b", &[Token::CubeRoot, Token::Ident("b".into())]),
            ("a^b", &[Token::Caret, Token::Ident("b".into())]),
            ("a%b", &[Token::Percent, Token::Ident("b".into())]),
            ("a=b", &[Token::Eq, Token::Ident("b".into())]),
            ("a=>b", &[Token::NamedArg, Token::Ident("b".into())]),
            ("a>b", &[Token::Gt, Token::Ident("b".into())]),
        ];
        for (sql, tail) in cases {
            let mut expected = vec![Token::Ident("a".into())];
            expected.extend_from_slice(tail);
            expected.push(Token::Eof);
            assert!(toks(sql) == expected, "lexing {sql:?}");
        }
    }

    #[test]
    fn an_operator_stops_at_an_embedded_comment_opener() {
        use assert2::assert;

        // PostgreSQL truncates an operator at an embedded `/*`, so `||/*` is the
        // concatenation `||` followed by a comment — NOT the cube root `||/`.
        assert!(
            toks("a ||/* c */ b")
                == vec![
                    Token::Ident("a".into()),
                    Token::Concat,
                    Token::Ident("b".into()),
                    Token::Eof,
                ]
        );
        assert!(
            toks("a |/* c */ b")
                == vec![
                    Token::Ident("a".into()),
                    Token::Pipe,
                    Token::Ident("b".into()),
                    Token::Eof,
                ]
        );
    }

    #[test]
    fn non_decimal_integer_literals_lex_as_their_decimal_value() {
        use assert2::assert;

        // The prefix is case-insensitive, `_` may separate digits (and may even
        // lead the digit run), and a value too wide for int8 keeps every digit
        // because PostgreSQL widens it to `numeric` downstream.
        let cases: &[(&str, &str)] = &[
            ("0x7FFFFFFF", "2147483647"),
            ("0X1f", "31"),
            ("0o273", "187"),
            ("0O17", "15"),
            ("0b100101", "37"),
            ("0B11", "3"),
            ("0x1_F", "31"),
            ("0x_1F", "31"),
            ("0b_1", "1"),
            ("0x0", "0"),
            ("0x80000000", "2147483648"),
            ("0x8000000000000000", "9223372036854775808"),
            ("0xFFFFFFFFFFFFFFFFF", "295147905179352825855"),
        ];
        for (sql, decimal) in cases {
            assert!(
                toks(sql) == vec![Token::IntLit((*decimal).into()), Token::Eof],
                "lexing {sql:?}"
            );
        }
    }

    #[test]
    fn underscore_separators_are_stripped_from_numeric_literals() {
        use assert2::assert;

        let cases: &[(&str, Token)] = &[
            ("1_000", Token::IntLit("1000".into())),
            ("1_000_000", Token::IntLit("1000000".into())),
            ("1_000.000_1", Token::FloatLit("1000.0001".into())),
            ("1e1_0", Token::FloatLit("1e10".into())),
            ("1_000e1_0", Token::FloatLit("1000e10".into())),
            (".5_5", Token::FloatLit(".55".into())),
        ];
        for (sql, token) in cases {
            assert!(
                toks(sql) == vec![token.clone(), Token::Eof],
                "lexing {sql:?}"
            );
        }
    }

    #[test]
    fn malformed_numeric_literals_are_rejected_the_way_postgres_rejects_them() {
        use assert2::assert;

        // A bare radix prefix names its radix; everything else that runs a
        // literal into an identifier character is "trailing junk".
        let radix_cases: &[(&str, &str)] = &[
            ("SELECT 0x", "invalid hexadecimal integer"),
            ("SELECT 0X", "invalid hexadecimal integer"),
            ("SELECT 0o", "invalid octal integer"),
            ("SELECT 0b", "invalid binary integer"),
            ("SELECT 0x_", "invalid hexadecimal integer"),
        ];
        for (sql, message) in radix_cases {
            let e = lex(sql).expect_err("bad radix literal");
            assert!(e.message.contains(message), "lexing {sql:?}: {}", e.message);
            assert!(e.sqlstate() == "42601", "lexing {sql:?}");
        }
        let junk = [
            "1abc", "1000_", "1__000", "1_.5", "1._5", "1.5_", "1e_10", "1e10_", "1e", "1e+",
            "0xg", "0x1g", "0b12", "0o18",
        ];
        for sql in junk {
            let e = lex(sql).expect_err("trailing junk");
            assert!(
                e.message.contains("trailing junk after numeric literal"),
                "lexing {sql:?}: {}",
                e.message
            );
        }
        // A separated `e` is still an identifier — only an ADJACENT one is junk.
        assert!(
            toks("3 e")
                == vec![
                    Token::IntLit("3".into()),
                    Token::Ident("e".into()),
                    Token::Eof
                ]
        );
    }

    #[test]
    fn dollar_quoted_strings_lex_as_ordinary_string_literals() {
        use assert2::assert;

        let cases: &[(&str, &str)] = &[
            ("$$hello$$", "hello"),
            ("$tag$hello$tag$", "hello"),
            ("$$it's a \\ test$$", "it's a \\ test"),
            // Distinct tags nest: only the exact opening delimiter closes it.
            ("$outer$a $inner$ b$outer$", "a $inner$ b"),
            ("$_tag1$x$_tag1$", "x"),
            ("$$$$", ""),
        ];
        for (sql, text) in cases {
            assert!(
                toks(sql) == vec![Token::StringLit((*text).into()), Token::Eof],
                "lexing {sql:?}"
            );
        }
        let e = lex("$q$unterminated").expect_err("unterminated dollar quote");
        assert!(e.message.contains("unterminated dollar-quoted string"));
        // A tag may not start with a digit — that spelling is a parameter.
        let e = lex("$1x$y$1x$").expect_err("parameter junk");
        assert!(e.message.contains("trailing junk after parameter"));
    }

    #[test]
    fn escape_string_literals_expand_backslash_escapes() {
        use assert2::assert;

        let cases: &[(&str, &str)] = &[
            (r"E'a\nb'", "a\nb"),
            (r"E'a\tb'", "a\tb"),
            (r"E'a\\b'", "a\\b"),
            (r"E'it\'s'", "it's"),
            (r"E'a''b'", "a'b"),
            (r"E'\101\102'", "AB"),
            (r"E'\x41\x42'", "AB"),
            (r"E'A'", "A"),
            (r"E'\U0001F600'", "\u{1f600}"),
            // A `\u` surrogate PAIR encodes one supplementary-plane character.
            (r"E'\uD83D\uDE00'", "\u{1f600}"),
            // Non-ASCII text passes through escape mode untouched.
            ("E'😀'", "\u{1f600}"),
            // An unrecognised escape is the escaped character itself.
            (r"E'\q'", "q"),
            (r"E'\z'", "z"),
            // `\x` with no hex digit is a literal `x`.
            (r"E'\xZ'", "xZ"),
            (r"E'\b\f\r\v'", "\u{8}\u{c}\r\u{b}"),
            (r"e'lower'", "lower"),
            // Two hex escapes can spell ONE multi-byte character.
            (r"E'\xc3\xa9'", "é"),
        ];
        for (sql, text) in cases {
            assert!(
                toks(sql) == vec![Token::StringLit((*text).into()), Token::Eof],
                "lexing {sql:?}"
            );
        }
        // A standard literal keeps the backslash and only doubles quotes.
        assert!(toks(r"'a\nb'") == vec![Token::StringLit("a\\nb".into()), Token::Eof]);
        // `east'x'` is an identifier followed by a literal, not an E-string:
        // the quote must follow the `E` immediately.
        assert!(
            toks("east'x'")
                == vec![
                    Token::Ident("east".into()),
                    Token::StringLit("x".into()),
                    Token::Eof,
                ]
        );
    }

    #[test]
    fn escape_string_errors_carry_postgres_sqlstates() {
        use assert2::assert;

        // A byte escape can spell something that is not text: NUL and an invalid
        // UTF-8 sequence are 22021 (character_not_in_repertoire), NOT 42601.
        for sql in [r"E'\0'", r"E'\x0'", r"E'\400'", r"E'\777'"] {
            let e = lex(sql).expect_err("untranslatable byte");
            assert!(e.sqlstate() == "22021", "lexing {sql:?}: {}", e.sqlstate());
        }
        // An unpaired surrogate is a syntax error, like PostgreSQL.
        for sql in [r"E'\uD800'", r"E'\uDC00'", r"E'\uD800A'"] {
            let e = lex(sql).expect_err("unpaired surrogate");
            assert!(
                e.message.contains("invalid Unicode surrogate pair"),
                "{sql}"
            );
            assert!(e.sqlstate() == "42601", "{sql}");
        }
    }

    #[test]
    fn adjacent_literals_concatenate_only_across_a_newline() {
        use assert2::assert;

        // PostgreSQL's `quotecontinue`: whitespace CONTAINING A NEWLINE joins two
        // literals into one, and the continuation keeps the escape mode.
        assert!(toks("'a'\n'b'") == vec![Token::StringLit("ab".into()), Token::Eof]);
        assert!(toks("'a' \t\n  'b'") == vec![Token::StringLit("ab".into()), Token::Eof]);
        assert!(toks("'a' --c\n'b'") == vec![Token::StringLit("ab".into()), Token::Eof]);
        assert!(toks("'a'\n--c\n'b'") == vec![Token::StringLit("ab".into()), Token::Eof]);
        assert!(toks("E'a\\n'\n'b\\t'") == vec![Token::StringLit("a\nb\t".into()), Token::Eof]);
        // These stay TWO literals (the parser then rejects the pair): the same
        // line, or a BLOCK comment anywhere in the gap — PostgreSQL's
        // `quotecontinue` admits only whitespace and `--` comments.
        let two = |a: &str, b: &str| {
            vec![
                Token::StringLit(a.into()),
                Token::StringLit(b.into()),
                Token::Eof,
            ]
        };
        assert!(toks("'a' 'b'") == two("a", "b"));
        assert!(toks("'a'/* c\n */'b'") == two("a", "b"));
        assert!(toks("'a'/* c */\n'b'") == two("a", "b"));
        assert!(toks("'a'\n/* c */'b'") == two("a", "b"));
    }

    #[test]
    fn string_literals_and_quoted_identifiers_preserve_non_ascii_text() {
        use assert2::assert;

        // The literal body is UTF-8, not Latin-1: a multi-byte character must
        // survive as ONE `char`, not as its individual bytes.
        assert!(toks("'é'") == vec![Token::StringLit("é".into()), Token::Eof]);
        assert!(toks("'日本'") == vec![Token::StringLit("日本".into()), Token::Eof]);
        assert!(toks("\"café\"") == vec![Token::Ident("café".into()), Token::Eof]);
        assert!(toks("$$é$$") == vec![Token::StringLit("é".into()), Token::Eof]);
    }

    #[test]
    fn array_keyword_lexes_as_a_keyword() {
        use assert2::assert;

        assert!(
            toks("ARRAY[1]")
                == vec![
                    Token::Keyword(Keyword::Array),
                    Token::LBracket,
                    Token::IntLit("1".into()),
                    Token::RBracket,
                    Token::Eof,
                ]
        );
    }

    #[test]
    fn geometric_operator_spellings_lex_with_maximal_munch() {
        use assert2::assert;

        // Written WITHOUT surrounding spaces, and each new spelling paired with
        // every SHORTER spelling it shares a first byte with. A munch-order slip
        // re-reads `a?-|b` as `a ?- |b` — whose tail still lexes — so only the
        // exact token vector catches it.
        let cases: &[(&str, &[Token])] = &[
            // `#`: `#>>` before `#>` before `##` before `#`.
            (
                "a#>>'{k}'",
                &[Token::JsonGetPathText, Token::StringLit("{k}".into())],
            ),
            (
                "a#>'{k}'",
                &[Token::JsonGetPath, Token::StringLit("{k}".into())],
            ),
            ("a##b", &[Token::ClosestPoint, Token::Ident("b".into())]),
            ("a#b", &[Token::Hash, Token::Ident("b".into())]),
            // `?`: `?-|` before `?-`, `?||` before `?|`, then `?#`, `?&`, `?`.
            ("a?-|b", &[Token::Perpendicular, Token::Ident("b".into())]),
            ("a?-b", &[Token::Horizontal, Token::Ident("b".into())]),
            ("a?||b", &[Token::Parallel, Token::Ident("b".into())]),
            ("a?|b", &[Token::KeyExistsAny, Token::Ident("b".into())]),
            ("a?#b", &[Token::Intersects, Token::Ident("b".into())]),
            ("a?&b", &[Token::KeyExistsAll, Token::Ident("b".into())]),
            ("a?'k'", &[Token::KeyExists, Token::StringLit("k".into())]),
            // `@`: `@-@` before `@>`, `@?`, `@@`, `@`.
            ("a@>b", &[Token::Contains, Token::Ident("b".into())]),
            ("a@?b", &[Token::JsonPathExists, Token::Ident("b".into())]),
            ("a@@b", &[Token::JsonPathMatch, Token::Ident("b".into())]),
            ("a@b", &[Token::At, Token::Ident("b".into())]),
            // `<`: `<^` must leave the rest of the family alone.
            ("a<^b", &[Token::BelowEq, Token::Ident("b".into())]),
            ("a<@b", &[Token::ContainedBy, Token::Ident("b".into())]),
            ("a<=b", &[Token::Le, Token::Ident("b".into())]),
            ("a<>b", &[Token::Ne, Token::Ident("b".into())]),
            ("a<<|b", &[Token::StrictlyBelow, Token::Ident("b".into())]),
            ("a<<=b", &[Token::ContainedByOrEq, Token::Ident("b".into())]),
            ("a<<b", &[Token::Shl, Token::Ident("b".into())]),
            ("a<->b", &[Token::Phrase, Token::Ident("b".into())]),
            ("a<b", &[Token::Lt, Token::Ident("b".into())]),
            // `>`: same for `>^`.
            ("a>^b", &[Token::AboveEq, Token::Ident("b".into())]),
            ("a>=b", &[Token::Ge, Token::Ident("b".into())]),
            ("a>>=b", &[Token::ContainsOrEq, Token::Ident("b".into())]),
            ("a>>b", &[Token::Shr, Token::Ident("b".into())]),
            ("a>b", &[Token::Gt, Token::Ident("b".into())]),
        ];
        for (sql, tail) in cases {
            let mut expected = vec![Token::Ident("a".into())];
            expected.extend_from_slice(tail);
            expected.push(Token::Eof);
            assert!(toks(sql) == expected, "lexing {sql:?}");
        }
    }

    #[test]
    fn the_prefix_only_length_spelling_is_one_token() {
        use assert2::assert;

        // `@-@` is the one geometric spelling with no infix reading, and the
        // only one whose bytes used to lex as three separate operators — as
        // `@ - @`, which parsed as a nested absolute value.
        assert!(toks("@-@p") == vec![Token::Length, Token::Ident("p".into()), Token::Eof,]);
        // The shorter `@` spellings it could have stolen from are untouched, and
        // an `@` followed by an ordinary minus still lexes as two operators.
        assert!(
            toks("@-p")
                == vec![
                    Token::At,
                    Token::Minus,
                    Token::Ident("p".into()),
                    Token::Eof,
                ]
        );
    }

    #[test]
    fn a_zero_length_delimited_identifier_is_rejected_wherever_one_can_appear() {
        use assert2::assert;

        // `PostgreSQL` raises this from `scan.l`, so it fires at the quote pair
        // itself rather than in whatever clause would have consumed the name.
        // Each case pairs a statement with the BYTE offset of its `""`; the
        // one-based column PostgreSQL's caret prints is that offset plus one.
        let cases: &[(&str, usize)] = &[
            // The `create_am` regression case: an access method name.
            ("CREATE TABLE i_am_a_failure() USING \"\";", 36),
            // A relation name, bare and schema-qualified on either side.
            ("SELECT * FROM \"\";", 14),
            ("SELECT * FROM \"\".t;", 14),
            ("SELECT * FROM pg_catalog.\"\";", 25),
            ("CREATE TABLE \"\" (a int);", 13),
            // A column name, in a target list, a definition and an insert list.
            ("SELECT \"\" FROM t;", 7),
            ("CREATE TABLE t (\"\" int);", 16),
            ("INSERT INTO t (\"\") VALUES (1);", 15),
            // An alias, a sort key, a grouping key, a function and a type.
            ("SELECT * FROM t AS \"\";", 19),
            ("SELECT 1 AS \"\";", 12),
            ("SELECT x FROM t ORDER BY \"\";", 25),
            ("SELECT x FROM t GROUP BY \"\";", 25),
            ("SELECT \"\"('a');", 7),
            ("SELECT 1::\"\";", 10),
            ("SELECT CAST(1 AS \"\");", 17),
            // Other object namespaces, and a name abutting a bare word on
            // either side — `x""` is the identifier `x` and then the error.
            ("CREATE SCHEMA \"\";", 14),
            ("CREATE INDEX \"\" ON t (a);", 13),
            ("SET \"\" = 1;", 4),
            ("SELECT \"\"x;", 7),
            ("SELECT x\"\";", 8),
        ];
        for (sql, position) in cases {
            let e = lex(sql).expect_err("a zero-length delimited identifier");
            assert!(
                e.message == "zero-length delimited identifier at or near \"\"\"\"",
                "lexing {sql:?}: {}",
                e.message
            );
            assert!(e.sqlstate() == "42601", "lexing {sql:?}");
            assert!(e.position == *position, "lexing {sql:?}");
        }
    }

    #[test]
    fn a_doubled_quote_inside_the_delimiters_is_an_escape_not_an_empty_name() {
        use assert2::assert;

        // Only the ACCUMULATED name's length decides, so a pair that is escaped
        // rather than delimiting still yields a name. `""""` is the one-character
        // identifier `"`, and the rule reaches neither the empty string literal
        // (which is legal) nor any other quoted form the lexer scans.
        let cases: &[(&str, Token)] = &[
            ("\"\"\"\"", Token::Ident("\"".into())),
            ("\"\"\"\"\"\"", Token::Ident("\"\"".into())),
            ("\"a\"\"b\"", Token::Ident("a\"b".into())),
            ("\"\"\"ab\"", Token::Ident("\"ab".into())),
            ("\"ab\"\"\"", Token::Ident("ab\"".into())),
            ("\"a\"\"\"\"b\"", Token::Ident("a\"\"b".into())),
            ("\"\"\"a\"\"\"", Token::Ident("\"a\"".into())),
            ("\" \"", Token::Ident(" ".into())),
            ("\"select\"", Token::Ident("select".into())),
            ("\"MixedCase\"", Token::Ident("MixedCase".into())),
            ("\"héllo\"", Token::Ident("héllo".into())),
            // A zero-length STRING is legal in PostgreSQL, in every spelling.
            ("''", Token::StringLit(String::new())),
            ("E''", Token::StringLit(String::new())),
            ("$$$$", Token::StringLit(String::new())),
            ("B''", Token::BitStringLit("b".into())),
            ("X''", Token::BitStringLit("x".into())),
            // And a quote pair that is DATA rather than a delimiter is untouched.
            ("'\"\"'", Token::StringLit("\"\"".into())),
            ("$$\"\"$$", Token::StringLit("\"\"".into())),
        ];
        for (sql, token) in cases {
            assert!(
                toks(sql) == vec![token.clone(), Token::Eof],
                "lexing {sql:?}"
            );
        }
    }

    #[test]
    fn the_plpgsql_label_and_directive_bytes_still_lex_as_before() {
        use assert2::assert;

        // PL/pgSQL spells `<<label>>` with the shift tokens and its compiler
        // directives with a bare `#`; `<^`/`>^`/`##` must not have disturbed
        // either. (The parse-level guarantee is pinned in the parser tests.)
        assert!(
            toks("<<lbl>>")
                == vec![
                    Token::Shl,
                    Token::Ident("lbl".into()),
                    Token::Shr,
                    Token::Eof,
                ]
        );
        assert!(
            toks("#variable_conflict use_column")
                == vec![
                    Token::Hash,
                    Token::Ident("variable_conflict".into()),
                    Token::Ident("use_column".into()),
                    Token::Eof,
                ]
        );
    }

    #[test]
    fn unicode_escape_string_literals_decode_to_their_code_points() {
        use assert2::assert;

        // `\XXXX` is exactly four hex digits and `\+XXXXXX` exactly six; the
        // escape character doubles to itself, a surrogate pair combines into one
        // character, and `''` still doubles. `UESCAPE` replaces the escape
        // character for its own literal only, which demotes `\` to ordinary text.
        let cases: &[(&str, &str)] = &[
            (r"U&'d\0061t\+000061'", "data"),
            (r"u&'d\0061t\+000061'", "data"),
            (r"U&'a\\b'", r"a\b"),
            (r"U&'\0061\0308bc'", "a\u{308}bc"),
            (r"U&'\00E4bc'", "äbc"),
            (r"U&'abc\+10FFFF'", "abc\u{10ffff}"),
            (r"U&'abc'", "abc"),
            (r"U&''", ""),
            (r"U&'it''s'", "it's"),
            // A high surrogate followed by its low half is one character.
            (r"U&'\D83D\DE00'", "\u{1f600}"),
            (r"U&'\+00D83D\+00DE00'", "\u{1f600}"),
            (r"U&' \' UESCAPE '!'", r" \"),
            (r"U&'d!0061t\+000061' UESCAPE '!'", r"dat\+000061"),
            (r"U&'!!' UESCAPE '!'", "!"),
            (r"U&'\0061' uescape '!'", r"\0061"),
        ];
        for (sql, text) in cases {
            assert!(
                toks(sql) == vec![Token::StringLit((*text).into()), Token::Eof],
                "lexing {sql:?}"
            );
        }
    }

    #[test]
    fn unicode_escape_quoted_identifiers_decode_the_same_way() {
        use assert2::assert;

        // The same prefix and the same escapes spell a NAME, whose case is kept
        // because the delimiters are quotes.
        let cases: &[(&str, &str)] = &[
            (r#"U&"d\0061t\+000061""#, "data"),
            (r#"u&"d\0061t\+000061""#, "data"),
            (r#"U&"a\\b""#, r"a\b"),
            (r#"U&"MixedCase""#, "MixedCase"),
            (r#"U&"a""b""#, "a\"b"),
            (r#"U&"\" UESCAPE '!'"#, r"\"),
            (r#"U&"d*0061t\+000061" UESCAPE '*'"#, r"dat\+000061"),
        ];
        for (sql, name) in cases {
            assert!(
                toks(sql) == vec![Token::Ident((*name).into()), Token::Eof],
                "lexing {sql:?}"
            );
        }
    }

    #[test]
    fn a_unicode_literal_is_one_token_in_the_statement_around_it() {
        use assert2::assert;

        // The `U` must not be read as a column and the `&` must not be read as
        // the bitwise operator, in either position; and the alias form takes its
        // own UESCAPE.
        assert!(
            toks(r#"SELECT U&'d\0061t\+000061' AS U&"d\0061t\+000061""#)
                == vec![
                    Token::Keyword(Keyword::Select),
                    Token::StringLit("data".into()),
                    Token::Keyword(Keyword::As),
                    Token::Ident("data".into()),
                    Token::Eof,
                ]
        );
        assert!(
            toks(r#"SELECT 'tricky' AS U&"\" UESCAPE '!'"#)
                == vec![
                    Token::Keyword(Keyword::Select),
                    Token::StringLit("tricky".into()),
                    Token::Keyword(Keyword::As),
                    Token::Ident(r"\".into()),
                    Token::Eof,
                ]
        );
        // `u` is only the prefix when the `&` and the quote follow it directly.
        assert!(
            toks("u & 'x'")
                == vec![
                    Token::Ident("u".into()),
                    Token::Amp,
                    Token::StringLit("x".into()),
                    Token::Eof,
                ]
        );
        assert!(
            toks("uu&'x'")
                == vec![
                    Token::Ident("uu".into()),
                    Token::Amp,
                    Token::StringLit("x".into()),
                    Token::Eof,
                ]
        );
    }

    #[test]
    fn unicode_escapes_reject_what_postgresql_rejects() {
        use assert2::assert;

        // Message and cursor both, because psql draws the caret from the cursor
        // and the regression corpus compares that line. The offsets are the ones
        // PostgreSQL reports: the escape itself for a malformed or out-of-range
        // one, and whatever failed to complete a surrogate pair for that.
        let cases: &[(&str, &str, usize)] = &[
            (r"SELECT U&'wrong: \061'", "invalid Unicode escape", 17),
            (r"SELECT U&'wrong: \+0061'", "invalid Unicode escape", 17),
            (
                r"SELECT U&'wrong: \db99'",
                "invalid Unicode surrogate pair",
                22,
            ),
            (
                r"SELECT U&'wrong: \db99xy'",
                "invalid Unicode surrogate pair",
                22,
            ),
            (
                r"SELECT U&'wrong: \db99\\'",
                "invalid Unicode surrogate pair",
                22,
            ),
            (
                r"SELECT U&'wrong: \db99\0061'",
                "invalid Unicode surrogate pair",
                22,
            ),
            (
                r"SELECT U&'wrong: \+00db99\+000061'",
                "invalid Unicode surrogate pair",
                25,
            ),
            (
                r"SELECT U&'wrong: \+2FFFFF'",
                "invalid Unicode escape value",
                17,
            ),
            (
                r"SELECT U&'wrong: \0000'",
                "invalid Unicode escape value",
                17,
            ),
            // A low surrogate with no high half before it is just as wrong.
            (
                r"SELECT U&'wrong: \de00'",
                "invalid Unicode surrogate pair",
                17,
            ),
            (
                r"SELECT U&'wrong: +0061' UESCAPE +",
                "UESCAPE must be followed by a simple string literal at or near \"+\"",
                32,
            ),
            (
                r"SELECT U&'wrong: +0061' UESCAPE '+'",
                "invalid Unicode escape character at or near \"'+'\"",
                32,
            ),
            (
                r"SELECT U&'x' UESCAPE 'ab'",
                "invalid Unicode escape character at or near \"'ab'\"",
                21,
            ),
            (
                // A hex digit cannot be the escape: `a0061` would be unreadable.
                r"SELECT U&'x' UESCAPE 'a'",
                "invalid Unicode escape character at or near \"'a'\"",
                21,
            ),
            (
                r"SELECT U&'x' UESCAPE ' '",
                "invalid Unicode escape character at or near \"' '\"",
                21,
            ),
            (
                r"SELECT U&'x' UESCAPE",
                "UESCAPE must be followed by a simple string literal at end of input",
                20,
            ),
            // The identifier spelling reports the identical rejections.
            (r#"SELECT U&"wrong: \061""#, "invalid Unicode escape", 17),
            (
                r#"SELECT U&"wrong: \db99""#,
                "invalid Unicode surrogate pair",
                22,
            ),
        ];
        for (sql, message, position) in cases {
            let error = lex(sql).expect_err("rejected");
            assert!(error.message == *message, "lexing {sql:?}");
            assert!(error.position == *position, "lexing {sql:?}");
            // PostgreSQL puts a cursor on every one of these, so psql echoes the
            // line with a caret under it.
            assert!(
                error.reported_position(sql) == Some(position + 1),
                "lexing {sql:?}"
            );
        }
        // The malformed-escape rejection carries PostgreSQL's remedy, and names
        // only the two forms this literal has.
        let error = lex(r"SELECT U&'wrong: \061'").expect_err("rejected");
        assert!(error.hint() == Some(r"Unicode escapes must be \XXXX or \+XXXXXX."));
    }

    #[test]
    fn a_unicode_literal_does_not_admit_the_escape_string_spellings() {
        use assert2::assert;

        // `\uXXXX` / `\UXXXXXXXX` belong to `E'…'`. In a `U&'…'` literal the
        // `u` is simply not a hex digit, so these are malformed escapes — and
        // the `E'…'` spellings must keep working where they do belong.
        for sql in [r"SELECT U&'\u0061'", r"SELECT U&'\U00000061'"] {
            let error = lex(sql).expect_err("rejected");
            assert!(error.message == "invalid Unicode escape", "lexing {sql:?}");
        }
        assert!(toks(r"E'dat\U00000061'") == vec![Token::StringLit("data".into()), Token::Eof]);
    }

    #[test]
    fn a_unicode_quoted_identifier_may_not_be_empty() {
        use assert2::assert;

        // PostgreSQL applies its zero-length rule to the RAW delimiters, before
        // any escape could have filled the name, and quotes the lexeme it read.
        let error = lex(r#"SELECT U&"""#).expect_err("rejected");
        assert!(error.message == "zero-length delimited identifier at or near \"U&\"\"\"");
        let error = lex(r#"SELECT """#).expect_err("rejected");
        assert!(error.message == "zero-length delimited identifier at or near \"\"\"\"");
    }

    proptest! {
        #[test]
        fn lex_never_panics(s: String) {
            // The lexer must never panic on arbitrary (valid-UTF-8) input —
            // it returns Ok(tokens) or Err(ParseError), never unwinds.
            let _ = lex(&s);
        }

        #[test]
        fn a_unicode_literal_never_panics(body: String, escape: String) {
            // Arbitrary input rarely spells the `U&` prefix, and the escape
            // expansion indexes BYTES into text that may hold any character, so
            // the two spellings are shaped here explicitly.
            let _ = lex(&format!("U&'{body}'"));
            let _ = lex(&format!("U&\"{body}\""));
            let _ = lex(&format!("U&'{body}' UESCAPE '{escape}'"));
        }
    }
}
