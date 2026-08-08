//! `COPY`'s text and CSV framing: option resolution and the byte-exact encoders
//! and decoders the two formats share.
//!
//! The parser records a [`CopyOptions`] exactly as it was written, because the
//! defaults it does not fill in depend on the format — a text copy's delimiter
//! is a tab, a CSV one's is a comma — and `None` is the only way to tell "not
//! given" from "given the default". Resolving those defaults, and running the
//! validity checks that only make sense once they are resolved, happens here.
//!
//! The encoders are transcriptions of `PostgreSQL`'s `CopyAttributeOutText` and
//! `CopyAttributeOutCSV`. Every rule they carry that is easy to get subtly wrong
//! is pinned by a test against `PostgreSQL` 18.4's observed output:
//!
//! - text escapes the delimiter byte as well as `\b \f \n \r \t \v \\`, and any
//!   *other* ASCII control character only when it is the delimiter;
//! - CSV quotes a field that holds the delimiter, the quote, `\r` or `\n`, that
//!   equals the null string, or — in a one-column copy — that is exactly `\.`;
//! - CSV escapes both the quote and the escape character inside a quoted field,
//!   and leaves them alone in an unquoted one;
//! - a NULL is the null string verbatim in both formats: never escaped, never
//!   quoted, not even under `FORCE_QUOTE *`.

use crabka_pgparser::ast::{CopyColumns, CopyFormat, CopyHeader, CopyOptions};
use crabka_pgwire::error::PgError;

use crate::error::ExecError;

/// The bytes that separate and delimit fields, once the format's defaults have
/// been applied. Shared by both directions: a `COPY FROM` reads what a `COPY TO`
/// with the same options would have written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyFraming {
    /// Always one byte, and therefore always ASCII: a single-byte character in
    /// UTF-8 is below `0x80`, and a longer one is refused as a delimiter.
    pub(crate) delimiter: u8,
    /// The string a NULL is written as and recognised by.
    pub(crate) null: String,
    /// `Some` in CSV mode, `None` in text mode.
    pub(crate) csv: Option<CsvFraming>,
}

/// The two characters only CSV has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CsvFraming {
    pub(crate) quote: u8,
    pub(crate) escape: u8,
}

/// A resolved `COPY … TO` option set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyOutFormat {
    pub(crate) framing: CopyFraming,
    pub(crate) header: bool,
    force_quote: Option<CopyColumns>,
}

/// A resolved `COPY … FROM` option set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyInFormat {
    pub(crate) framing: CopyFraming,
    /// `None` when no `HEADER` was written; otherwise the spelling, because
    /// `MATCH` checks the line it consumes and `TRUE` only discards it.
    pub(crate) header: Option<CopyHeader>,
}

fn invalid_parameter(message: &str) -> ExecError {
    ExecError::Remote(PgError::error("22023", message))
}

impl CopyFraming {
    /// Fill in the format's defaults and run `PostgreSQL`'s `ProcessCopyOptions`
    /// checks, in its order — the order is observable, because more than one of
    /// them can fire on the same option list.
    fn resolve(options: &CopyOptions) -> Result<Self, ExecError> {
        let csv = options.format == CopyFormat::Csv;
        let delimiter = options
            .delimiter
            .clone()
            .unwrap_or_else(|| if csv { ",".into() } else { "\t".into() });
        let null = options
            .null
            .clone()
            .unwrap_or_else(|| if csv { String::new() } else { r"\N".into() });

        let delimiter = single_byte(&delimiter, "delimiter")?;
        if delimiter == b'\r' || delimiter == b'\n' {
            return Err(invalid_parameter(
                "COPY delimiter cannot be newline or carriage return",
            ));
        }
        if null.contains(['\r', '\n']) {
            return Err(invalid_parameter(
                "COPY null representation cannot use newline or carriage return",
            ));
        }
        // Text mode reads a backslash as an escape and `\.` alone on a line as
        // the end of the data, and it does not quote, so a delimiter that could
        // start an escape or be mistaken for datum text would be unreadable.
        // CSV has quoting and so has no such restriction.
        if !csv && (delimiter == b'\\' || delimiter == b'.' || delimiter.is_ascii_alphanumeric()) {
            return Err(invalid_parameter(&format!(
                "COPY delimiter cannot be \"{}\"",
                char::from(delimiter)
            )));
        }

        let csv = csv
            .then(|| {
                let quote = single_byte(options.quote.as_deref().unwrap_or("\""), "quote")?;
                if delimiter == quote {
                    return Err(invalid_parameter(
                        "COPY delimiter and quote must be different",
                    ));
                }
                let escape = match options.escape.as_deref() {
                    Some(escape) => single_byte(escape, "escape")?,
                    None => quote,
                };
                Ok(CsvFraming { quote, escape })
            })
            .transpose()?;

        if null.as_bytes().contains(&delimiter) {
            return Err(invalid_parameter(
                "COPY delimiter character must not appear in the NULL specification",
            ));
        }
        if let Some(csv) = &csv
            && null.as_bytes().contains(&csv.quote)
        {
            return Err(invalid_parameter(
                "CSV quote character must not appear in the NULL specification",
            ));
        }
        Ok(Self {
            delimiter,
            null,
            csv,
        })
    }
}

/// `PostgreSQL` reports a multi-byte delimiter, quote or escape as
/// `feature_not_supported` rather than an invalid value, because the restriction
/// is an implementation limit and not a rule about the option.
fn single_byte(value: &str, option: &str) -> Result<u8, ExecError> {
    match value.as_bytes() {
        [byte] => Ok(*byte),
        _ => Err(ExecError::Unsupported(format!(
            "COPY {option} must be a single one-byte character"
        ))),
    }
}

/// The options a `COPY … FROM` cannot honour, refused rather than ignored.
///
/// Ignoring one of these would load *wrong rows* — silently, and into a table
/// the caller believes it filled correctly. A refusal is the only safe answer
/// until the option is implemented, so every option the decoder does not read is
/// listed here rather than left to fall through.
///
/// `FREEZE` is the exception, and is accepted and ignored: it asks for a
/// visibility shortcut on a relation created in the same transaction, so rows
/// loaded without it are the same rows. `pgbench -i` sends it.
fn refuse_unhandled_copy_from_options(options: &CopyOptions) -> Result<(), ExecError> {
    let unhandled = [
        ("DEFAULT", options.default.is_some()),
        ("CONVERT_SELECTIVELY", options.convert_selectively.is_some()),
        (
            "ON_ERROR",
            options
                .on_error
                .is_some_and(|on_error| on_error != crabka_pgparser::ast::CopyOnError::Stop),
        ),
        ("REJECT_LIMIT", options.reject_limit.is_some()),
    ];
    for (name, present) in unhandled {
        if present {
            return Err(ExecError::Unsupported(format!(
                "COPY {name} is not supported"
            )));
        }
    }
    refuse_unhandled_encoding(options.encoding.as_deref())
}

/// The server encoding is UTF-8 and there is no transcoding machinery, so an
/// `ENCODING` naming anything else would be read as a promise the copy cannot
/// keep. `PostgreSQL` spells the alias set out in `pg_wchar.h`; the two names
/// that reach UTF-8 are all this engine can answer to.
fn refuse_unhandled_encoding(encoding: Option<&str>) -> Result<(), ExecError> {
    match encoding {
        None => Ok(()),
        Some(name) if name.eq_ignore_ascii_case("utf8") || name.eq_ignore_ascii_case("utf-8") => {
            Ok(())
        }
        Some(name) => Err(ExecError::Unsupported(format!(
            "COPY ENCODING \"{name}\" is not supported; the server encoding is UTF8"
        ))),
    }
}

impl CopyInFormat {
    pub(crate) fn resolve(options: &CopyOptions) -> Result<Self, ExecError> {
        // Before the framing, so that an option this engine cannot honour is
        // reported as unsupported rather than as a complaint about the value of
        // an option that would have been ignored anyway.
        refuse_unhandled_copy_from_options(options)?;
        if options.format == CopyFormat::Csv {
            return Err(ExecError::Unsupported("COPY CSV is not supported".into()));
        }
        Ok(Self {
            framing: CopyFraming::resolve(options)?,
            header: options.header.filter(|header| *header != CopyHeader::False),
        })
    }
}

impl CopyOutFormat {
    pub(crate) fn resolve(options: &CopyOptions) -> Result<Self, ExecError> {
        refuse_unhandled_encoding(options.encoding.as_deref())?;
        Ok(Self {
            framing: CopyFraming::resolve(options)?,
            header: options.header == Some(CopyHeader::True),
            force_quote: options.force_quote.clone(),
        })
    }

    /// Which of `names` `FORCE_QUOTE` covers.
    ///
    /// `relation` names the copied table for the not-found message, which
    /// `PostgreSQL` words differently for the `COPY (query) TO` form because
    /// there is no relation to name.
    pub(crate) fn forced_columns(
        &self,
        names: &[String],
        relation: Option<&str>,
    ) -> Result<Vec<bool>, ExecError> {
        let mut forced = vec![false; names.len()];
        match &self.force_quote {
            None => {}
            Some(CopyColumns::All) => forced.fill(true),
            Some(CopyColumns::Named(columns)) => {
                for column in columns {
                    let index = names
                        .iter()
                        .position(|name| name == column)
                        .ok_or_else(|| undefined_copy_column(column, relation))?;
                    if std::mem::replace(&mut forced[index], true) {
                        return Err(ExecError::DuplicateOutputColumn(column.clone()));
                    }
                }
            }
        }
        Ok(forced)
    }

    /// The `HEADER` line, or `None` when no header was asked for.
    ///
    /// `FORCE_QUOTE` deliberately does not reach it: `PostgreSQL` writes the
    /// column names with the force flag hard-coded false, so a `FORCE_QUOTE *`
    /// copy has an unquoted header above quoted rows.
    pub(crate) fn header_line(&self, names: &[String]) -> Option<Vec<u8>> {
        self.header.then(|| {
            let cells: Vec<Option<&[u8]>> =
                names.iter().map(|name| Some(name.as_bytes())).collect();
            self.row_line(&cells, &vec![false; names.len()])
        })
    }

    /// One encoded row, its trailing newline included.
    ///
    /// `forced` is [`Self::forced_columns`] for this copy's column list; it is
    /// taken as an argument rather than recomputed because it is the same for
    /// every row.
    pub(crate) fn row_line(&self, cells: &[Option<&[u8]>], forced: &[bool]) -> Vec<u8> {
        let mut out = Vec::new();
        for (index, cell) in cells.iter().enumerate() {
            if index > 0 {
                out.push(self.framing.delimiter);
            }
            match cell {
                // Verbatim, in both formats: a NULL is the one field that is
                // neither escaped nor quoted, whatever FORCE_QUOTE says.
                None => out.extend_from_slice(self.framing.null.as_bytes()),
                Some(value) => match &self.framing.csv {
                    None => encode_text_field(&mut out, value, self.framing.delimiter),
                    Some(csv) => encode_csv_field(
                        &mut out,
                        value,
                        self.framing.delimiter,
                        *csv,
                        &self.framing.null,
                        forced.get(index).copied().unwrap_or(false),
                        cells.len() == 1,
                    ),
                },
            }
        }
        out.push(b'\n');
        out
    }
}

fn undefined_copy_column(column: &str, relation: Option<&str>) -> ExecError {
    ExecError::Remote(PgError::error(
        "42703",
        match relation {
            Some(relation) => {
                format!("column \"{column}\" of relation \"{relation}\" does not exist")
            }
            None => format!("column \"{column}\" does not exist"),
        },
    ))
}

/// `PostgreSQL`'s `CopyAttributeOutText`.
///
/// The C-style spellings win over the raw-byte one wherever both apply, so a
/// tab is `\t` even when the tab is also the delimiter. Every other ASCII
/// control character passes through untouched unless it is the delimiter, in
/// which case it is backslashed as itself rather than named.
fn encode_text_field(out: &mut Vec<u8>, value: &[u8], delimiter: u8) {
    for &byte in value {
        match byte {
            0x08 => out.extend_from_slice(br"\b"),
            0x0c => out.extend_from_slice(br"\f"),
            b'\n' => out.extend_from_slice(br"\n"),
            b'\r' => out.extend_from_slice(br"\r"),
            b'\t' => out.extend_from_slice(br"\t"),
            0x0b => out.extend_from_slice(br"\v"),
            b'\\' => out.extend_from_slice(br"\\"),
            byte if byte == delimiter => {
                out.push(b'\\');
                out.push(byte);
            }
            byte => out.push(byte),
        }
    }
}

/// `PostgreSQL`'s `CopyAttributeOutCSV`.
///
/// `single_attr` reproduces the rule that a one-column copy quotes a field that
/// is exactly `\.`, so that the line cannot be mistaken for the text format's
/// end-of-data marker by a reader that still honours one.
fn encode_csv_field(
    out: &mut Vec<u8>,
    value: &[u8],
    delimiter: u8,
    csv: CsvFraming,
    null: &str,
    force_quote: bool,
    single_attr: bool,
) {
    let quote = force_quote
        || value == null.as_bytes()
        || (single_attr && value == br"\.")
        || value
            .iter()
            .any(|&byte| byte == delimiter || byte == csv.quote || byte == b'\r' || byte == b'\n');
    if !quote {
        out.extend_from_slice(value);
        return;
    }
    out.push(csv.quote);
    for &byte in value {
        if byte == csv.quote || byte == csv.escape {
            out.push(csv.escape);
        }
        out.push(byte);
    }
    out.push(csv.quote);
}

/// Split a `COPY` text-format line into its raw, still-escaped fields.
///
/// The split honours backslash escapes, so a `\` followed by the delimiter byte
/// stays inside the field it belongs to. Scanning bytes is safe even though the
/// input is UTF-8: the delimiter is a single byte and therefore ASCII, no
/// continuation byte can equal it, and none can be a backslash either, so every
/// boundary this finds is a character boundary.
fn split_text_fields(line: &str, delimiter: u8) -> Vec<&str> {
    let bytes = line.as_bytes();
    let mut fields = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 2,
            byte if byte == delimiter => {
                fields.push(&line[start..index]);
                index += 1;
                start = index;
            }
            _ => index += 1,
        }
    }
    fields.push(&line[start..]);
    fields
}

/// The longest `COPY` field or line a message quotes before it is elided —
/// `PostgreSQL`'s `MAX_COPY_DATA_DISPLAY`.
const MAX_COPY_DATA_DISPLAY: usize = 100;

/// One quoted value in a `COPY` `CONTEXT` line, cut to
/// [`MAX_COPY_DATA_DISPLAY`] characters with `...` in place of the rest.
///
/// The cut is by character, not by byte, so a multi-byte character is never
/// split — `PostgreSQL` clips to a character boundary for the same reason.
pub(crate) fn copy_printout(value: &str) -> String {
    match value.char_indices().nth(MAX_COPY_DATA_DISPLAY) {
        None => value.to_string(),
        Some((cut, _)) => format!("{}...", &value[..cut]),
    }
}

/// Which `CONTEXT` line a failing `COPY … FROM` row carries.
///
/// `PostgreSQL` installs one error-context callback over the whole per-row
/// loop, and what that callback can still say narrows as the row travels. All
/// three spellings, and the fourth case of saying nothing at all, are
/// observable:
///
/// ```text
/// CONTEXT:  COPY t, line 1, column c: "toolong"
/// CONTEXT:  COPY dn, line 1, column a: null input
/// CONTEXT:  COPY t, line 1: "1<TAB>ab<TAB>extra"
/// CONTEXT:  COPY u, line 2
/// ```
#[derive(Debug, Clone, Copy)]
pub(crate) enum CopyContext<'a> {
    /// A failure inside one column's input function, which is the only point
    /// that knows both the column and the field it was handed. A field that was
    /// the null representation has no text to quote and says `null input`.
    Column {
        name: &'a str,
        value: Option<&'a str>,
    },
    /// A failure judging the assembled row — a constraint, a partition route, a
    /// `BEFORE` row trigger. The line is still in hand, so it is quoted whole.
    Line { raw: &'a str },
    /// A failure raised once the row has been handed on and the line buffer no
    /// longer describes it: the unique-index check, which `PostgreSQL` runs at
    /// its multi-insert flush. Only the counter survives.
    LineNumber,
}

/// The `CONTEXT` line a failing `COPY … FROM` reports.
///
/// The relation is named unqualified whatever the statement wrote, because this
/// is `RelationGetRelationName` and not a regclass.
pub(crate) fn copy_context(relation: &str, line: u64, at: CopyContext<'_>) -> String {
    match at {
        CopyContext::Column {
            name,
            value: Some(value),
        } => format!(
            "COPY {relation}, line {line}, column {name}: \"{}\"",
            copy_printout(value)
        ),
        CopyContext::Column { name, value: None } => {
            format!("COPY {relation}, line {line}, column {name}: null input")
        }
        CopyContext::Line { raw } => {
            format!("COPY {relation}, line {line}: \"{}\"", copy_printout(raw))
        }
        CopyContext::LineNumber => format!("COPY {relation}, line {line}"),
    }
}

/// One decoded `COPY … FROM` row, with where in the payload it came from.
///
/// The origin travels with the row because a failure anywhere in the write —
/// an input conversion, a constraint, a trigger — reports it as `CONTEXT`, and
/// by then the payload has been left behind. It costs no allocation to carry:
/// [`Self::raw`] borrows the line out of the payload the decode read, and is
/// copied only when a row actually fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyRow<'a> {
    /// The row's fields, de-escaped, `None` for the null representation.
    pub(crate) values: Vec<Option<String>>,
    /// The one-based input line this row was read from. A `HEADER` line counts,
    /// so the first row of a `COPY … WITH (HEADER)` is line 2 — `PostgreSQL`
    /// counts the lines it read, not the rows it kept.
    pub(crate) line: u64,
    /// The line as it arrived, before the fields were split or de-escaped.
    pub(crate) raw: &'a str,
}

/// The header line of a `COPY … FROM` payload, when the copy has one.
///
/// Only a `HEADER MATCH` failure asks, so this re-reads the first line rather
/// than the decode carrying it: the decode that would have returned it is the
/// one that just failed.
pub(crate) fn header_line_of<'a>(data: &'a [u8], format: &CopyInFormat) -> Option<&'a str> {
    format.header?;
    let text = std::str::from_utf8(data).ok()?;
    let line = text.split('\n').next()?;
    Some(line.strip_suffix('\r').unwrap_or(line))
}

/// Decode a `COPY … FROM` text-format payload into per-row raw values.
///
/// The NULL comparison is against the field *before* de-escaping, as
/// `PostgreSQL`'s `CopyReadAttributesText` does it: with `NULL 'NUL'` an
/// incoming `\N` is not a null but the one-character string `N`.
pub(crate) fn decode_copy_text<'a>(
    data: &'a [u8],
    format: &CopyInFormat,
    columns: &[String],
) -> Result<Vec<CopyRow<'a>>, ExecError> {
    let text = std::str::from_utf8(data)
        .map_err(|_| ExecError::Syntax("invalid byte sequence for encoding \"UTF8\"".into()))?;
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let mut header_pending = format.header.is_some();
    let mut lines = text.split('\n').peekable();
    let mut number = 0_u64;
    while let Some(raw_line) = lines.next() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        number += 1;
        // PostgreSQL's text-format end-of-data marker: clients on the old
        // PQputline/PQendcopy API (pgbench -i among them) send a final `\.`
        // line; it terminates the data and everything after it is ignored.
        if line == r"\." {
            break;
        }
        if raw_line.is_empty() && lines.peek().is_none() && text.ends_with('\n') {
            continue;
        }
        if header_pending {
            header_pending = false;
            if format.header == Some(CopyHeader::Match) {
                match_header_line(line, format, columns)?;
            }
            continue;
        }
        rows.push(CopyRow {
            values: split_text_fields(line, format.framing.delimiter)
                .into_iter()
                .map(|field| {
                    if field == format.framing.null {
                        return Ok(None);
                    }
                    decode_copy_text_field(field).map(Some)
                })
                .collect::<Result<_, _>>()?,
            line: number,
            raw: line,
        });
    }
    Ok(rows)
}

/// A malformed `COPY` payload: `PostgreSQL`'s `bad_copy_file_format`.
fn bad_copy_format(message: String) -> ExecError {
    ExecError::Remote(PgError::error("22P04", message))
}

/// `HEADER MATCH`: the header must name the copy's target columns, in order.
///
/// The width is checked before any name is, as `PostgreSQL` checks it: a header
/// of the wrong length is one error about the line rather than an error about
/// whichever column happens to sit where the two lists first diverge.
fn match_header_line(
    line: &str,
    format: &CopyInFormat,
    columns: &[String],
) -> Result<(), ExecError> {
    let fields = split_text_fields(line, format.framing.delimiter);
    if fields.len() != columns.len() {
        return Err(bad_copy_format(format!(
            "wrong number of fields in header line: got {}, expected {}",
            fields.len(),
            columns.len()
        )));
    }
    for (index, (field, expected)) in fields.iter().zip(columns).enumerate() {
        let field_number = index + 1;
        if *field == format.framing.null {
            return Err(bad_copy_format(format!(
                "column name mismatch in header line field {field_number}: got null value (\"{}\"), expected \"{expected}\"",
                format.framing.null
            )));
        }
        let got = decode_copy_text_field(field)?;
        if got != *expected {
            return Err(bad_copy_format(format!(
                "column name mismatch in header line field {field_number}: got \"{got}\", expected \"{expected}\""
            )));
        }
    }
    Ok(())
}

fn decode_copy_text_field(field: &str) -> Result<String, ExecError> {
    let mut out = String::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err(ExecError::Syntax(
                "unterminated COPY escape sequence".into(),
            ));
        };
        out.push(match escaped {
            'b' => '\u{0008}',
            'f' => '\u{000c}',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'v' => '\u{000b}',
            '\\' => '\\',
            other => other,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgparser::ast::{CopyColumns, CopyFormat, CopyHeader, CopyOptions};

    use super::{CopyInFormat, CopyOutFormat, decode_copy_text};
    use crate::error::ExecError;

    fn options(build: impl FnOnce(&mut CopyOptions)) -> CopyOptions {
        let mut options = CopyOptions::default();
        build(&mut options);
        options
    }

    fn csv(build: impl FnOnce(&mut CopyOptions)) -> CopyOptions {
        options(|o| {
            o.format = CopyFormat::Csv;
            build(o);
        })
    }

    /// Render a whole copy — header line included — the way the session does.
    fn copy_out(options: &CopyOptions, names: &[&str], rows: &[&[Option<&str>]]) -> String {
        let format = CopyOutFormat::resolve(options).expect("options resolve");
        let names: Vec<String> = names.iter().map(|name| (*name).to_string()).collect();
        let forced = format.forced_columns(&names, Some("t")).expect("columns");
        let mut out = format.header_line(&names).unwrap_or_default();
        for row in rows {
            let cells: Vec<Option<&[u8]>> =
                row.iter().map(|cell| cell.map(str::as_bytes)).collect();
            out.extend_from_slice(&format.row_line(&cells, &forced));
        }
        String::from_utf8(out).expect("utf8")
    }

    fn error_of(result: Result<impl Sized, ExecError>) -> (String, String) {
        let rendered = result
            .map(|_| ())
            .expect_err("should have failed")
            .into_pg();
        (rendered.code.clone(), rendered.message)
    }

    /// Every byte PostgreSQL 18.4 escapes in the text format, and every one it
    /// leaves alone, checked against its observed output for the same rows.
    #[test]
    fn text_output_escapes_exactly_what_postgres_escapes() {
        struct Case {
            name: &'static str,
            options: CopyOptions,
            expected: &'static str,
        }
        let rows: &[&[Option<&str>]] = &[
            &[Some("1"), Some("tab\there"), Some("x")],
            &[Some("2"), Some("nl\nhere"), None],
            &[Some("3"), Some("back\\slash"), Some("cr\rhere")],
            &[Some("4"), Some("bs\u{8} ff\u{c} vt\u{b}"), Some("q\"uote")],
            &[Some("5"), Some("comma,here"), Some("plain")],
        ];
        let cases = [
            Case {
                name: "the defaults",
                options: CopyOptions::default(),
                expected: concat!(
                    "1\ttab\\there\tx\n",
                    "2\tnl\\nhere\t\\N\n",
                    "3\tback\\\\slash\tcr\\rhere\n",
                    "4\tbs\\b ff\\f vt\\v\tq\"uote\n",
                    "5\tcomma,here\tplain\n",
                ),
            },
            Case {
                name: "an alternative null string",
                options: options(|o| o.null = Some("NUL".into())),
                expected: concat!(
                    "1\ttab\\there\tx\n",
                    "2\tnl\\nhere\tNUL\n",
                    "3\tback\\\\slash\tcr\\rhere\n",
                    "4\tbs\\b ff\\f vt\\v\tq\"uote\n",
                    "5\tcomma,here\tplain\n",
                ),
            },
            Case {
                name: "a pipe delimiter, which leaves the tabs named",
                options: options(|o| o.delimiter = Some("|".into())),
                expected: concat!(
                    "1|tab\\there|x\n",
                    "2|nl\\nhere|\\N\n",
                    "3|back\\\\slash|cr\\rhere\n",
                    "4|bs\\b ff\\f vt\\v|q\"uote\n",
                    "5|comma,here|plain\n",
                ),
            },
            Case {
                name: "a header line, escaped like any other row",
                options: options(|o| o.header = Some(CopyHeader::True)),
                expected: concat!(
                    "a\tb\tc\n",
                    "1\ttab\\there\tx\n",
                    "2\tnl\\nhere\t\\N\n",
                    "3\tback\\\\slash\tcr\\rhere\n",
                    "4\tbs\\b ff\\f vt\\v\tq\"uote\n",
                    "5\tcomma,here\tplain\n",
                ),
            },
        ];
        for case in cases {
            assert!(
                copy_out(&case.options, &["a", "b", "c"], rows) == case.expected,
                "{}",
                case.name
            );
        }
    }

    /// Text mode has no quoting, so a field holding the delimiter escapes it —
    /// in the header line as much as in the rows. PostgreSQL 18.4 writes exactly
    /// this for a relation whose column is named `we|ird,name`.
    #[test]
    fn text_output_escapes_the_delimiter_wherever_it_appears() {
        assert!(
            copy_out(
                &options(|o| {
                    o.delimiter = Some("|".into());
                    o.header = Some(CopyHeader::True);
                }),
                &["we|ird,name", "b\"q"],
                &[&[Some(""), Some("a|b")], &[None, Some("x")]]
            ) == "we\\|ird,name|b\"q\n|a\\|b\n\\N|x\n"
        );
    }

    /// The delimiter is escaped as itself when it is a control character, rather
    /// than taking the C-style spelling a named control character would.
    #[test]
    fn a_control_character_delimiter_is_backslashed_as_itself() {
        let options = options(|o| o.delimiter = Some("\u{1}".into()));
        assert!(
            copy_out(&options, &["a"], &[&[Some("x\u{1}y\u{2}z")]]) == "x\\\u{1}y\u{2}z\n",
            "the delimiter is escaped, an unrelated control character is not"
        );
    }

    /// PostgreSQL 18.4's CSV output for the same rows, option set by option set.
    #[test]
    fn csv_output_quotes_exactly_what_postgres_quotes() {
        struct Case {
            name: &'static str,
            options: CopyOptions,
            expected: &'static str,
        }
        let rows: &[&[Option<&str>]] = &[
            &[Some("1"), Some("tab\there"), Some("x")],
            &[Some("2"), Some("nl\nhere"), None],
            &[Some("3"), Some("back\\slash"), Some("cr\rhere")],
            &[Some("4"), Some("bs\u{8} ff\u{c} vt\u{b}"), Some("q\"uote")],
            &[Some("5"), Some("comma,here"), Some("plain")],
        ];
        let cases = [
            Case {
                name: "the defaults",
                options: csv(|_| {}),
                expected: concat!(
                    "1,tab\there,x\n",
                    "2,\"nl\nhere\",\n",
                    "3,back\\slash,\"cr\rhere\"\n",
                    "4,bs\u{8} ff\u{c} vt\u{b},\"q\"\"uote\"\n",
                    "5,\"comma,here\",plain\n",
                ),
            },
            Case {
                name: "a header line",
                options: csv(|o| o.header = Some(CopyHeader::True)),
                expected: concat!(
                    "a,b,c\n",
                    "1,tab\there,x\n",
                    "2,\"nl\nhere\",\n",
                    "3,back\\slash,\"cr\rhere\"\n",
                    "4,bs\u{8} ff\u{c} vt\u{b},\"q\"\"uote\"\n",
                    "5,\"comma,here\",plain\n",
                ),
            },
            Case {
                name: "a null string, which force-quotes a value that matches it",
                options: csv(|o| o.null = Some("NUL".into())),
                expected: concat!(
                    "1,tab\there,x\n",
                    "2,\"nl\nhere\",NUL\n",
                    "3,back\\slash,\"cr\rhere\"\n",
                    "4,bs\u{8} ff\u{c} vt\u{b},\"q\"\"uote\"\n",
                    "5,\"comma,here\",plain\n",
                ),
            },
            Case {
                name: "FORCE_QUOTE on one column, which leaves the null bare",
                options: csv(|o| {
                    o.force_quote = Some(CopyColumns::Named(vec!["a".into()]));
                }),
                expected: concat!(
                    "\"1\",tab\there,x\n",
                    "\"2\",\"nl\nhere\",\n",
                    "\"3\",back\\slash,\"cr\rhere\"\n",
                    "\"4\",bs\u{8} ff\u{c} vt\u{b},\"q\"\"uote\"\n",
                    "\"5\",\"comma,here\",plain\n",
                ),
            },
            Case {
                name: "FORCE_QUOTE *, which still leaves the null bare",
                options: csv(|o| o.force_quote = Some(CopyColumns::All)),
                expected: concat!(
                    "\"1\",\"tab\there\",\"x\"\n",
                    "\"2\",\"nl\nhere\",\n",
                    "\"3\",\"back\\slash\",\"cr\rhere\"\n",
                    "\"4\",\"bs\u{8} ff\u{c} vt\u{b}\",\"q\"\"uote\"\n",
                    "\"5\",\"comma,here\",\"plain\"\n",
                ),
            },
            Case {
                name: "a non-default quote and escape",
                options: csv(|o| {
                    o.quote = Some("~".into());
                    o.escape = Some("@".into());
                }),
                expected: concat!(
                    "1,tab\there,x\n",
                    "2,~nl\nhere~,\n",
                    "3,back\\slash,~cr\rhere~\n",
                    "4,bs\u{8} ff\u{c} vt\u{b},q\"uote\n",
                    "5,~comma,here~,plain\n",
                ),
            },
        ];
        for case in cases {
            assert!(
                copy_out(&case.options, &["a", "b", "c"], rows) == case.expected,
                "{}",
                case.name
            );
        }
    }

    /// The two CSV quoting rules that only fire on values shaped like the
    /// format's own metacharacters.
    #[test]
    fn csv_quotes_the_null_string_and_a_lone_end_of_data_marker() {
        assert!(
            copy_out(
                &csv(|_| {}),
                &["s"],
                &[&[Some("")], &[Some(r"\.")], &[Some("a")]]
            ) == "\"\"\n\"\\.\"\na\n",
            "a one-column copy quotes both the empty string and `\\.`"
        );
        assert!(
            copy_out(&csv(|_| {}), &["s", "u"], &[&[Some(r"\."), Some("z")]]) == "\\.,z\n",
            "with a second column `\\.` cannot be an end-of-data line, so it is bare"
        );
        assert!(
            copy_out(
                &csv(|o| {
                    o.quote = Some("~".into());
                    o.escape = Some("@".into());
                }),
                &["s", "u"],
                &[&[Some("a@b~c"), Some("p")], &[Some(""), Some("q")]]
            ) == "~a@@b@~c~,p\n~~,q\n",
            "inside a quoted field both the quote and the escape are escaped"
        );
    }

    /// The header takes the null-string quoting rule but not FORCE_QUOTE.
    #[test]
    fn the_csv_header_ignores_force_quote_but_not_the_null_string() {
        assert!(
            copy_out(
                &csv(|o| {
                    o.force_quote = Some(CopyColumns::All);
                    o.header = Some(CopyHeader::True);
                }),
                &["s", "t"],
                &[&[Some("a"), Some("b")]]
            ) == "s,t\n\"a\",\"b\"\n"
        );
        assert!(
            copy_out(
                &csv(|o| {
                    o.null = Some("s".into());
                    o.header = Some(CopyHeader::True);
                }),
                &["s", "t"],
                &[&[Some("a"), Some("b")]]
            ) == "\"s\",t\na,b\n"
        );
    }

    /// PostgreSQL's option checks, in the order it runs them, with the SQLSTATE
    /// it reports for each. A multi-byte separator is `feature_not_supported`;
    /// everything else here is an invalid parameter value.
    #[test]
    fn option_validation_matches_postgres_sqlstates_and_wording() {
        struct Case {
            options: CopyOptions,
            sqlstate: &'static str,
            message: &'static str,
        }
        let cases = [
            Case {
                options: options(|o| o.delimiter = Some("ab".into())),
                sqlstate: "0A000",
                message: "COPY delimiter must be a single one-byte character",
            },
            Case {
                options: options(|o| o.delimiter = Some("\n".into())),
                sqlstate: "22023",
                message: "COPY delimiter cannot be newline or carriage return",
            },
            Case {
                options: options(|o| o.delimiter = Some("\r".into())),
                sqlstate: "22023",
                message: "COPY delimiter cannot be newline or carriage return",
            },
            Case {
                options: options(|o| o.null = Some("a\nb".into())),
                sqlstate: "22023",
                message: "COPY null representation cannot use newline or carriage return",
            },
            Case {
                options: options(|o| o.delimiter = Some(".".into())),
                sqlstate: "22023",
                message: "COPY delimiter cannot be \".\"",
            },
            Case {
                options: options(|o| o.delimiter = Some("a".into())),
                sqlstate: "22023",
                message: "COPY delimiter cannot be \"a\"",
            },
            Case {
                options: options(|o| o.delimiter = Some("5".into())),
                sqlstate: "22023",
                message: "COPY delimiter cannot be \"5\"",
            },
            Case {
                options: options(|o| o.delimiter = Some("\\".into())),
                sqlstate: "22023",
                message: "COPY delimiter cannot be \"\\\"",
            },
            Case {
                options: csv(|o| o.quote = Some("ab".into())),
                sqlstate: "0A000",
                message: "COPY quote must be a single one-byte character",
            },
            Case {
                options: csv(|o| o.escape = Some("ab".into())),
                sqlstate: "0A000",
                message: "COPY escape must be a single one-byte character",
            },
            Case {
                options: csv(|o| o.delimiter = Some("\"".into())),
                sqlstate: "22023",
                message: "COPY delimiter and quote must be different",
            },
            Case {
                options: options(|o| o.null = Some("a\tb".into())),
                sqlstate: "22023",
                message: "COPY delimiter character must not appear in the NULL specification",
            },
            Case {
                options: csv(|o| o.null = Some("a\"b".into())),
                sqlstate: "22023",
                message: "CSV quote character must not appear in the NULL specification",
            },
            Case {
                options: options(|o| o.encoding = Some("LATIN1".into())),
                sqlstate: "0A000",
                message: "COPY ENCODING \"LATIN1\" is not supported; the server encoding is UTF8",
            },
        ];
        for case in cases {
            assert!(
                error_of(CopyOutFormat::resolve(&case.options))
                    == (case.sqlstate.to_string(), case.message.to_string()),
                "{}",
                case.message
            );
        }
    }

    /// A CSV delimiter may be one of the bytes text mode forbids, because CSV
    /// quotes rather than backslashing.
    #[test]
    fn csv_admits_the_delimiters_text_mode_refuses() {
        assert!(
            copy_out(
                &csv(|o| o.delimiter = Some(".".into())),
                &["s"],
                &[&[Some("a")]]
            ) == "a\n"
        );
    }

    /// UTF-8 is the server encoding, so its two spellings are accepted and every
    /// other encoding name is refused rather than silently ignored.
    #[test]
    fn the_server_encoding_is_the_only_encoding_accepted() {
        for name in ["UTF8", "utf-8"] {
            assert!(CopyOutFormat::resolve(&options(|o| o.encoding = Some(name.into()))).is_ok());
        }
    }

    /// The decoder reads what the encoder wrote, under the same options.
    #[test]
    fn copy_from_honours_the_delimiter_null_and_header_options() {
        struct Case {
            name: &'static str,
            options: CopyOptions,
            data: &'static str,
            expected: Vec<Vec<Option<String>>>,
        }
        let row = |values: &[Option<&str>]| -> Vec<Option<String>> {
            values
                .iter()
                .map(|value| value.map(str::to_string))
                .collect()
        };
        let cases = [
            Case {
                name: "the defaults",
                options: CopyOptions::default(),
                data: "a\t1\nb\t\\N\n",
                expected: vec![row(&[Some("a"), Some("1")]), row(&[Some("b"), None])],
            },
            Case {
                name: "a delimiter and a null string, with `\\N` no longer a null",
                options: options(|o| {
                    o.delimiter = Some("|".into());
                    o.null = Some("NUL".into());
                }),
                data: "a|1\nb|\\N\nc|NUL\n",
                expected: vec![
                    row(&[Some("a"), Some("1")]),
                    row(&[Some("b"), Some("N")]),
                    row(&[Some("c"), None]),
                ],
            },
            Case {
                name: "an escaped delimiter, which does not split its field",
                options: options(|o| o.delimiter = Some("|".into())),
                data: "a\\|b|2\n",
                expected: vec![row(&[Some("a|b"), Some("2")])],
            },
            Case {
                name: "HEADER, which discards the first line",
                options: options(|o| o.header = Some(CopyHeader::True)),
                data: "anything\tat all\na\t1\n",
                expected: vec![row(&[Some("a"), Some("1")])],
            },
            Case {
                name: "HEADER MATCH, which checks it",
                options: options(|o| o.header = Some(CopyHeader::Match)),
                data: "s\tn\na\t1\n",
                expected: vec![row(&[Some("a"), Some("1")])],
            },
            Case {
                name: "HEADER FALSE, which is no header at all",
                options: options(|o| o.header = Some(CopyHeader::False)),
                data: "a\t1\n",
                expected: vec![row(&[Some("a"), Some("1")])],
            },
        ];
        let columns = ["s".to_string(), "n".to_string()];
        for case in cases {
            let format = CopyInFormat::resolve(&case.options).expect("options resolve");
            let decoded = decode_copy_text(case.data.as_bytes(), &format, &columns)
                .expect("decode")
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>();
            assert!(decoded == case.expected, "{}", case.name);
        }
    }

    /// Old-API clients (`PQputline`/`PQendcopy` — `pgbench -i`) send a final
    /// `\.` line; it terminates the data and every later line is ignored.
    #[test]
    fn copy_from_stops_at_the_end_of_data_marker() {
        let format = CopyInFormat::resolve(&CopyOptions::default()).expect("options resolve");
        let decode = |data: &[u8]| {
            decode_copy_text(data, &format, &[])
                .expect("decode")
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>()
        };
        assert!(
            decode(b"1\t0\t\\N\n\\.\n") == vec![vec![Some("1".into()), Some("0".into()), None]]
        );
        assert!(
            decode(b"1\ta\n\\.\nignored\tafter\n")
                == vec![vec![Some("1".into()), Some("a".into())]]
        );
        assert!(decode(b"1\ta\n2\tb\n").len() == 2);
    }

    /// A mismatched `HEADER MATCH` names the field, what it got and what it
    /// wanted, exactly as PostgreSQL does.
    #[test]
    fn header_match_reports_the_first_mismatched_column() {
        let format = CopyInFormat::resolve(&options(|o| o.header = Some(CopyHeader::Match)))
            .expect("options resolve");
        let columns = ["s".to_string(), "n".to_string()];
        assert!(
            error_of(decode_copy_text(b"s\twrong\na\t1\n", &format, &columns))
                == (
                    "22P04".to_string(),
                    "column name mismatch in header line field 2: got \"wrong\", expected \"n\""
                        .to_string()
                )
        );
        assert!(
            error_of(decode_copy_text(b"s\n", &format, &columns))
                == (
                    "22P04".to_string(),
                    "wrong number of fields in header line: got 1, expected 2".to_string()
                )
        );
        assert!(
            error_of(decode_copy_text(b"s\t\\N\na\t1\n", &format, &columns))
                == (
                    "22P04".to_string(),
                    "column name mismatch in header line field 2: got null value (\"\\N\"), expected \"n\""
                        .to_string()
                )
        );
    }

    /// Every option a `COPY … FROM` cannot honour is refused rather than
    /// ignored: ignoring one loads different rows than the caller asked for.
    #[test]
    fn copy_from_refuses_the_options_it_cannot_honour() {
        struct Case {
            options: CopyOptions,
            message: &'static str,
        }
        let cases = [
            Case {
                options: csv(|_| {}),
                message: "COPY CSV is not supported",
            },
            Case {
                options: options(|o| o.default = Some("D".into())),
                message: "COPY DEFAULT is not supported",
            },
            Case {
                options: options(|o| o.convert_selectively = Some(vec!["a".into()])),
                message: "COPY CONVERT_SELECTIVELY is not supported",
            },
            Case {
                options: options(|o| {
                    o.on_error = Some(crabka_pgparser::ast::CopyOnError::Ignore);
                }),
                message: "COPY ON_ERROR is not supported",
            },
            Case {
                options: options(|o| o.reject_limit = Some(5)),
                message: "COPY REJECT_LIMIT is not supported",
            },
            Case {
                options: options(|o| o.encoding = Some("LATIN1".into())),
                message: "COPY ENCODING \"LATIN1\" is not supported; the server encoding is UTF8",
            },
        ];
        for case in cases {
            assert!(
                error_of(CopyInFormat::resolve(&case.options))
                    == ("0A000".to_string(), case.message.to_string()),
                "{}",
                case.message
            );
        }
    }

    /// `FREEZE` is a visibility shortcut, not a framing option: the rows it
    /// loads are the same rows, so it is accepted and ignored. `pgbench -i`
    /// sends it, and `ON_ERROR STOP` is the default spelled out.
    #[test]
    fn copy_from_accepts_the_options_that_change_no_row() {
        for build in [
            (|o: &mut CopyOptions| o.freeze = true) as fn(&mut CopyOptions),
            |o: &mut CopyOptions| o.on_error = Some(crabka_pgparser::ast::CopyOnError::Stop),
            |o: &mut CopyOptions| {
                o.log_verbosity = Some(crabka_pgparser::ast::CopyLogVerbosity::Verbose);
            },
        ] {
            assert!(CopyInFormat::resolve(&options(build)).is_ok());
        }
    }

    /// `FORCE_QUOTE` names columns, and a name that is not one — or is one
    /// twice — is the error PostgreSQL reports for it. The relation form and the
    /// query form are worded differently.
    #[test]
    fn force_quote_resolves_against_the_copied_column_list() {
        let format = CopyOutFormat::resolve(&csv(|o| {
            o.force_quote = Some(CopyColumns::Named(vec!["nope".into()]));
        }))
        .expect("options resolve");
        let names = ["s".to_string(), "u".to_string()];
        assert!(
            error_of(format.forced_columns(&names, Some("two")))
                == (
                    "42703".to_string(),
                    "column \"nope\" of relation \"two\" does not exist".to_string()
                )
        );
        assert!(
            error_of(format.forced_columns(&names, None))
                == (
                    "42703".to_string(),
                    "column \"nope\" does not exist".to_string()
                )
        );

        let repeated = CopyOutFormat::resolve(&csv(|o| {
            o.force_quote = Some(CopyColumns::Named(vec!["s".into(), "s".into()]));
        }))
        .expect("options resolve");
        assert!(
            error_of(repeated.forced_columns(&names, Some("two")))
                == (
                    "42701".to_string(),
                    "column \"s\" specified more than once".to_string()
                )
        );
    }
}
