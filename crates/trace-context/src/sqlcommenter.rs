//! Reading a W3C trace context out of a [sqlcommenter] tag on a SQL statement.
//!
//! OpenTelemetry-instrumented database drivers append
//! `/*traceparent='00-<32 hex>-<16 hex>-<2 hex>'*/` to the statements they send.
//! `PostgreSQL` — and Crabka's own lexer — skip both `--` and `/* */` comments
//! without emitting a token, so the tag changes no AST and the SQL text needs no
//! rewriting before it is parsed.
//!
//! [sqlcommenter]: https://google.github.io/sqlcommenter/

use crate::{
    carrier::parse_traceparent,
    propagation::{TRACEPARENT, TRACESTATE},
};

/// A trace context read out of a sqlcommenter tag, borrowed from the statement.
///
/// `traceparent` has already been validated against the W3C format. Values are
/// returned exactly as they appeared in the comment — sqlcommenter
/// percent-encodes them, which is a no-op for a `traceparent` but may leave a
/// `tracestate` encoded; [`crate::TraceCarrier::from_w3c`] drops a `tracestate`
/// it cannot parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCommenterTrace<'a> {
    /// The `traceparent` list value, guaranteed to parse.
    pub traceparent: &'a str,
    /// The `tracestate` list value, when the comment carried one.
    pub tracestate: Option<&'a str>,
}

/// Extract a sqlcommenter trace context from `sql`, if it carries one.
///
/// Only genuine comment regions are inspected. `SELECT '/*traceparent=…*/'` is
/// a string literal, not a comment, and yields `None` — which is the whole
/// reason this walks the statement rather than pattern-matching the text.
///
/// The cost when no tag is present is a single substring search: the scan is
/// never entered unless the word `traceparent` appears somewhere in `sql`.
#[must_use]
pub fn extract_sqlcommenter(sql: &str) -> Option<SqlCommenterTrace<'_>> {
    sql.find(TRACEPARENT)?;
    scan_comments(sql)
}

/// Walk `sql`, skipping string literals, quoted identifiers, and dollar-quoted
/// bodies, and try to read a trace context out of each comment region.
///
/// Any unterminated construct abandons the scan: the statement will not parse
/// either, and guessing at the structure of a truncated string or comment is
/// exactly how a literal gets mistaken for a comment.
fn scan_comments(sql: &str) -> Option<SqlCommenterTrace<'_>> {
    let bytes = sql.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' => index = skip_quoted(bytes, index, b'\'')?,
            b'"' => index = skip_quoted(bytes, index, b'"')?,
            b'$' => match dollar_quote_delimiter(sql, index) {
                // Not a dollar quote — a positional parameter such as `$1`.
                None => index += 1,
                Some(delimiter) => {
                    let body = index + delimiter.len();
                    index = body + sql[body..].find(delimiter)? + delimiter.len();
                }
            },
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                let start = index + 2;
                let end = line_comment_end(bytes, start);
                if let Some(found) = read_comment(&sql[start..end]) {
                    return Some(found);
                }
                index = end;
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                let (end, closed) = block_comment_end(bytes, index);
                if !closed {
                    return None;
                }
                if let Some(found) = read_comment(&sql[index + 2..end - 2]) {
                    return Some(found);
                }
                index = end;
            }
            _ => index += 1,
        }
    }

    None
}

/// Read a `traceparent` (and any accompanying `tracestate`) out of one comment
/// body. `None` when the comment carries no tag, or one that fails validation.
fn read_comment(content: &str) -> Option<SqlCommenterTrace<'_>> {
    let traceparent = find_field(content, TRACEPARENT)?;
    parse_traceparent(traceparent).ok()?;
    Some(SqlCommenterTrace {
        traceparent,
        tracestate: find_field(content, TRACESTATE),
    })
}

/// Find `key=value` inside a comment body and return the value.
///
/// The key must not be preceded by an identifier byte, so `db_traceparent=…`
/// does not masquerade as `traceparent=…`. Occurrences that are not followed by
/// `=` are skipped rather than aborting the search, which is what lets a nested
/// comment (`/* /*traceparent='…'*/ */`) and the other sqlcommenter keys
/// (`route`, `db_driver`, …) sit in the same body.
fn find_field<'a>(content: &'a str, key: &str) -> Option<&'a str> {
    let bytes = content.as_bytes();
    let mut from = 0;

    while let Some(offset) = content[from..].find(key) {
        let start = from + offset;
        let end = start + key.len();
        if (start == 0 || !is_identifier_byte(bytes[start - 1]))
            && let Some(value) = read_value(content, end)
        {
            return Some(value);
        }
        from = end;
    }

    None
}

/// Read the value of a `key=value` pair whose key ends at `index`. Values are
/// single-quoted per the sqlcommenter spec; an unquoted value is also accepted
/// and runs to the next delimiter.
fn read_value(content: &str, index: usize) -> Option<&str> {
    let bytes = content.as_bytes();
    let mut cursor = skip_spaces(bytes, index);
    if bytes.get(cursor) != Some(&b'=') {
        return None;
    }
    cursor = skip_spaces(bytes, cursor + 1);

    if bytes.get(cursor) == Some(&b'\'') {
        let start = cursor + 1;
        let end = start + content[start..].find('\'')?;
        return Some(&content[start..end]);
    }

    let start = cursor;
    let mut end = cursor;
    while end < bytes.len() && !is_value_terminator(bytes[end]) {
        end += 1;
    }
    (end > start).then(|| &content[start..end])
}

fn skip_spaces(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    index
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn is_value_terminator(byte: u8) -> bool {
    byte.is_ascii_whitespace() || matches!(byte, b',' | b'*' | b'/' | b'\'' | b'"')
}

/// Skip a `'…'` string literal or a `"…"` quoted identifier starting at
/// `index`, honouring the doubled-quote escape. `None` when it is unterminated,
/// which stops the scan: the remaining text is not statement structure.
fn skip_quoted(bytes: &[u8], mut index: usize, quote: u8) -> Option<usize> {
    index += 1;
    while index < bytes.len() {
        if bytes[index] != quote {
            index += 1;
        } else if bytes.get(index + 1) == Some(&quote) {
            index += 2;
        } else {
            return Some(index + 1);
        }
    }
    None
}

/// The `$tag$` delimiter opening at `index`, or `None` when `index` does not
/// begin a dollar-quoted body — a positional parameter such as `$1`, whose tag
/// would have to start with a digit.
fn dollar_quote_delimiter(sql: &str, index: usize) -> Option<&str> {
    let bytes = sql.as_bytes();
    let mut end = index + 1;
    if bytes.get(end).is_some_and(u8::is_ascii_digit) {
        return None;
    }
    while end < bytes.len() && is_identifier_byte(bytes[end]) {
        end += 1;
    }
    (bytes.get(end) == Some(&b'$')).then(|| &sql[index..=end])
}

fn line_comment_end(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index] != b'\n' {
        index += 1;
    }
    index
}

/// Find the end of the block comment opening at `index`, honouring
/// `PostgreSQL`'s comment nesting (matching `crabka_pgparser`'s lexer).
/// Returns the offset just past the comment and whether it was actually closed.
fn block_comment_end(bytes: &[u8], index: usize) -> (usize, bool) {
    let mut depth = 1usize;
    let mut cursor = index + 2;

    while cursor < bytes.len() {
        if bytes[cursor] == b'/' && bytes.get(cursor + 1) == Some(&b'*') {
            depth += 1;
            cursor += 2;
        } else if bytes[cursor] == b'*' && bytes.get(cursor + 1) == Some(&b'/') {
            depth -= 1;
            cursor += 2;
            if depth == 0 {
                return (cursor, true);
            }
        } else {
            cursor += 1;
        }
    }

    (cursor, false)
}
