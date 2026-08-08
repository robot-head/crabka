//! Reads a W3C trace context out of a [sqlcommenter] tag on a SQL statement.
//!
//! OpenTelemetry-instrumented database drivers append
//! `/*traceparent='00-<32 hex>-<16 hex>-<2 hex>'*/` to the statements they send.
//! `PostgreSQL` and Crabka's own lexer both skip `--` comments and `/* */`
//! comments, and emit no token for them. The tag changes no AST, so nothing
//! rewrites the SQL text before the parser reads it.
//!
//! [sqlcommenter]: https://google.github.io/sqlcommenter/

use crate::{
    carrier::parse_traceparent,
    propagation::{TRACEPARENT, TRACESTATE},
};

/// A trace context read out of a sqlcommenter tag, borrowed from the statement.
///
/// The `traceparent` has already passed validation against the W3C format.
/// The fields hold the values exactly as they appeared in the comment. The
/// sqlcommenter format percent-encodes them. That does nothing to a
/// `traceparent`, but it can leave a `tracestate` encoded, and
/// [`crate::TraceCarrier::from_w3c`] drops a `tracestate` it cannot parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCommenterTrace<'a> {
    /// The `traceparent` list value, guaranteed to parse.
    pub traceparent: &'a str,
    /// The `tracestate` list value, when the comment carried one.
    pub tracestate: Option<&'a str>,
}

/// Extract a sqlcommenter trace context from `sql`, if it carries one.
///
/// This function examines only genuine comment regions.
/// `SELECT '/*traceparent=…*/'` is a string literal, not a comment, and gives
/// `None`. That is the reason this function walks the statement instead of a
/// match on the text.
///
/// The cost when no tag is present is one substring search. The scan starts
/// only when the word `traceparent` appears somewhere in `sql`.
#[must_use]
pub fn extract_sqlcommenter(sql: &str) -> Option<SqlCommenterTrace<'_>> {
    sql.find(TRACEPARENT)?;
    scan_comments(sql)
}

/// Walk `sql` and try to read a trace context out of each comment region.
///
/// The walk skips string literals, quoted identifiers, and dollar-quoted
/// bodies. Any unterminated construct stops the scan, because the statement
/// will not parse either. A guess at the structure of a truncated string or
/// comment is exactly what makes a scanner mistake a literal for a comment.
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

/// Read a `traceparent` out of one comment body.
///
/// This function also reads the `tracestate` when the body carries one. It
/// returns `None` when the comment carries no tag, or a tag that fails
/// validation.
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
/// An identifier byte must not come before the key, so `db_traceparent=…`
/// cannot pass as `traceparent=…`. The search skips an occurrence that no `=`
/// follows, and does not stop. That behaviour lets a nested comment such as
/// `/* /*traceparent='…'*/ */` sit in the same body as the tag. It also lets
/// the other sqlcommenter keys, such as `route` and `db_driver`, sit there.
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

/// Read the value of a `key=value` pair whose key ends at `index`.
///
/// The sqlcommenter specification puts values in single quotes. This function
/// also accepts an unquoted value, which runs to the next delimiter.
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

/// Skip a `'…'` string literal or a `"…"` quoted identifier at `index`.
///
/// The skip honours the doubled-quote escape. Returns `None` when the literal
/// or the identifier is unterminated, which stops the scan: the text that
/// remains is not statement structure.
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

/// The `$tag$` delimiter that opens at `index`.
///
/// Returns `None` when `index` does not begin a dollar-quoted body. A
/// positional parameter such as `$1` is one such case, because its tag would
/// have to start with a digit.
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

/// Find the end of the block comment that opens at `index`.
///
/// The search honours `PostgreSQL`'s comment nesting, which matches
/// `crabka_pgparser`'s lexer. Returns the offset just past the comment, and
/// whether the comment was closed.
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
