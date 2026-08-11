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
    /// `FORCE_NOT_NULL`, as written. Resolved against the copy's column list
    /// by [`CopyInFormat::force_flags`] rather than here, because the names it
    /// carries mean nothing until the relation is known.
    force_not_null: Option<CopyColumns>,
    /// `FORCE_NULL`, as written.
    force_null: Option<CopyColumns>,
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
/// keep. `PostgreSQL` spells the alias set out in `pg_wchar.h`; the names that
/// ask for no conversion at all are all this engine can answer to.
///
/// `SQL_ASCII` is one of them: it is `PostgreSQL`'s "do not convert", so a copy
/// under it moves the server's own bytes in either direction, which is what
/// this engine does anyway. The one divergence is on the way in — `PostgreSQL`
/// stores whatever bytes arrive, valid UTF-8 or not, and this engine still
/// requires them to be UTF-8, so it refuses a payload `PostgreSQL` would have
/// stored and only been unable to read back.
fn refuse_unhandled_encoding(encoding: Option<&str>) -> Result<(), ExecError> {
    match encoding {
        None => Ok(()),
        Some(name)
            if name.eq_ignore_ascii_case("utf8")
                || name.eq_ignore_ascii_case("utf-8")
                || name.eq_ignore_ascii_case("sql_ascii") =>
        {
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
        Ok(Self {
            framing: CopyFraming::resolve(options)?,
            header: options.header.filter(|header| *header != CopyHeader::False),
            force_not_null: options.force_not_null.clone(),
            force_null: options.force_null.clone(),
        })
    }

    /// Whether this copy has to know the names of the columns it fills before
    /// it can read a byte: `HEADER MATCH` checks them against the first line,
    /// and the two `FORCE_` options name them.
    pub(crate) fn needs_column_names(&self) -> bool {
        self.header == Some(CopyHeader::Match)
            || self.force_not_null.is_some()
            || self.force_null.is_some()
    }

    /// Resolve `FORCE_NOT_NULL` and `FORCE_NULL` to one flag per field.
    ///
    /// `columns` is the copy's own column list, in the order the data supplies
    /// the fields; `relation_columns` is every column the relation has.
    /// `PostgreSQL` resolves each name against the *relation* first — a name
    /// that is not a column at all is an undefined column — and only then
    /// checks that it is one of the columns the copy reads, which is a
    /// different complaint. Both are raised before the copy reads any data, so
    /// neither can arrive after copy-in mode has been announced.
    pub(crate) fn force_flags(
        &self,
        columns: &[String],
        relation_columns: &[String],
        relation: &str,
    ) -> Result<CopyForceFlags, ExecError> {
        let resolve = |option: Option<&CopyColumns>, spelling: &str| match option {
            None => Ok(vec![false; columns.len()]),
            Some(CopyColumns::All) => Ok(vec![true; columns.len()]),
            Some(CopyColumns::Named(named)) => {
                let mut flags = vec![false; columns.len()];
                for name in named {
                    if !relation_columns.iter().any(|column| column == name) {
                        return Err(undefined_copy_column(name, Some(relation)));
                    }
                    let Some(index) = columns.iter().position(|column| column == name) else {
                        return Err(ExecError::Remote(PgError::error(
                            "42P10",
                            format!("{spelling} column \"{name}\" not referenced by COPY"),
                        )));
                    };
                    flags[index] = true;
                }
                Ok(flags)
            }
        };
        Ok(CopyForceFlags {
            not_null: resolve(self.force_not_null.as_ref(), "FORCE_NOT_NULL")?,
            null: resolve(self.force_null.as_ref(), "FORCE_NULL")?,
        })
    }
}

/// `FORCE_NOT_NULL` and `FORCE_NULL` resolved to one flag per copied field.
///
/// Both are CSV-only, and both act on the *raw* field rather than the decoded
/// one: `FORCE_NOT_NULL` keeps an unquoted null string as that literal string,
/// and `FORCE_NULL` turns a quoted one into a NULL. A field can carry both
/// flags, in which case neither fires — an unquoted null string is already not
/// null and a quoted one is already the string.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CopyForceFlags {
    not_null: Vec<bool>,
    null: Vec<bool>,
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

/// The end-of-line style a payload's first line establishes.
///
/// `PostgreSQL` reads the style off the first terminator it meets and then
/// holds every later line to it, so a file that mixes styles is a malformed
/// file rather than a lenient read. The state is per payload, which is why it
/// lives on the reader rather than being recomputed per line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EolStyle {
    Unknown,
    Nl,
    CrNl,
    Cr,
}

/// One step of [`CopyLineReader`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyLine<'a> {
    /// A line of data, with its terminator already consumed and not included.
    Data(&'a str),
    /// The text-format `\.` marker. Nothing after it is data.
    End,
    /// The payload is exhausted.
    Exhausted,
}

/// `PostgreSQL`'s `CopyReadLineText`, for both formats.
///
/// The two formats differ in exactly two ways, and both are here rather than in
/// the field splitters:
///
/// - CSV tracks quoting, because a `\r` or a `\n` inside a quoted field is data
///   and does not end the line. Text has no quoting and so no such state.
/// - text reads `\` as an escape, which is what makes `\.` on a line of its own
///   the end of the data. In CSV a backslash is an ordinary character and `\.`
///   is never a marker — psql strips the `\.` it sees in a script before it
///   sends anything, so a `\.` that reaches a CSV copy came from a file and is
///   a value.
struct CopyLineReader<'a> {
    text: &'a str,
    /// The byte offset of the next unread character. Always on a character
    /// boundary: every byte this reader compares against is ASCII, and no
    /// UTF-8 continuation byte is.
    pos: usize,
    eol: EolStyle,
    /// `Some((quote, escape))` in CSV mode. `escape` is `None` when it is the
    /// same character as the quote, which is the common case and needs no
    /// separate tracking.
    csv: Option<(u8, Option<u8>)>,
    /// Physical line breaks swallowed inside the last line's quoted fields.
    embedded: u64,
}

impl<'a> CopyLineReader<'a> {
    fn new(text: &'a str, format: &CopyInFormat) -> Self {
        Self {
            text,
            pos: 0,
            eol: EolStyle::Unknown,
            csv: format
                .framing
                .csv
                .map(|csv| (csv.quote, (csv.escape != csv.quote).then_some(csv.escape))),
            embedded: 0,
        }
    }

    /// How many physical line breaks the last line hid inside quoted fields,
    /// clearing the count for the next one.
    fn take_embedded_lines(&mut self) -> u64 {
        std::mem::take(&mut self.embedded)
    }

    /// The byte at `offset`, or `0` past the end — `PostgreSQL` pads its input
    /// buffer with a NUL for exactly this look-ahead, and the padding is
    /// observable: it is what makes a `\.` at the very end of a payload a
    /// marker that is not alone on its line.
    fn peek(&self, offset: usize) -> u8 {
        self.text.as_bytes().get(offset).copied().unwrap_or(0)
    }

    fn read(&mut self) -> Result<CopyLine<'a>, ExecError> {
        let bytes = self.text.as_bytes();
        let start = self.pos;
        let csv = self.csv.is_some();
        let mut in_quote = false;
        let mut last_was_esc = false;
        self.embedded = 0;
        while self.pos < bytes.len() {
            let at = self.pos;
            let c = bytes[at];
            self.pos += 1;
            if let Some((quote, escape)) = self.csv {
                if in_quote && Some(c) == escape {
                    last_was_esc = !last_was_esc;
                }
                if c == quote && !last_was_esc {
                    in_quote = !in_quote;
                }
                if Some(c) != escape {
                    last_was_esc = false;
                }
                let eol_char = if self.eol == EolStyle::Cr {
                    b'\r'
                } else {
                    b'\n'
                };
                if in_quote && c == eol_char {
                    self.embedded += 1;
                }
            }
            if c == b'\r' && !in_quote {
                match self.eol {
                    EolStyle::Unknown | EolStyle::CrNl => {
                        if self.peek(self.pos) == b'\n' {
                            self.pos += 1;
                            self.eol = EolStyle::CrNl;
                        } else if self.eol == EolStyle::CrNl {
                            return Err(stray_carriage_return(csv));
                        } else {
                            self.eol = EolStyle::Cr;
                        }
                    }
                    EolStyle::Nl => return Err(stray_carriage_return(csv)),
                    EolStyle::Cr => {}
                }
                return Ok(CopyLine::Data(&self.text[start..at]));
            }
            if c == b'\n' && !in_quote {
                if matches!(self.eol, EolStyle::Cr | EolStyle::CrNl) {
                    return Err(stray_newline(csv));
                }
                self.eol = EolStyle::Nl;
                return Ok(CopyLine::Data(&self.text[start..at]));
            }
            if c == b'\\' && !csv {
                if self.pos >= bytes.len() {
                    break;
                }
                if bytes[self.pos] != b'.' {
                    // Whatever it escapes is not a marker, and skipping it is
                    // what keeps `\\.` a backslash followed by a period.
                    self.pos += 1;
                    continue;
                }
                self.pos += 1;
                self.read_end_marker(start, at)?;
                return Ok(CopyLine::End);
            }
        }
        if self.pos == start {
            return Ok(CopyLine::Exhausted);
        }
        // A payload whose last line has no terminator still ends in a row:
        // PostgreSQL reports EOF and hands the partial line on.
        Ok(CopyLine::Data(&self.text[start..]))
    }

    /// The rest of a `\.` marker, once the `.` has been consumed.
    ///
    /// `start` is where the line began and `marker` where its backslash sits,
    /// so anything between them is data before the marker.
    fn read_end_marker(&mut self, start: usize, marker: usize) -> Result<(), ExecError> {
        if self.eol == EolStyle::CrNl {
            let next = self.peek(self.pos);
            self.pos += 1;
            if next == b'\n' {
                return Err(marker_style_mismatch());
            }
            if next != b'\r' {
                return Err(marker_not_alone());
            }
        }
        let next = self.peek(self.pos);
        self.pos += 1;
        if next != b'\r' && next != b'\n' {
            return Err(marker_not_alone());
        }
        let matches_style = match self.eol {
            EolStyle::Nl | EolStyle::CrNl => next == b'\n',
            EolStyle::Cr => next == b'\r',
            EolStyle::Unknown => true,
        };
        if !matches_style {
            return Err(marker_style_mismatch());
        }
        if marker > start {
            return Err(marker_not_alone());
        }
        Ok(())
    }
}

fn stray_carriage_return(csv: bool) -> ExecError {
    if csv {
        bad_copy_format("unquoted carriage return found in data".into())
    } else {
        bad_copy_format("literal carriage return found in data".into())
    }
}

fn stray_newline(csv: bool) -> ExecError {
    if csv {
        bad_copy_format("unquoted newline found in data".into())
    } else {
        bad_copy_format("literal newline found in data".into())
    }
}

fn marker_not_alone() -> ExecError {
    bad_copy_format("end-of-copy marker is not alone on its line".into())
}

fn marker_style_mismatch() -> ExecError {
    bad_copy_format("end-of-copy marker does not match previous newline style".into())
}

/// Decode a `COPY … FROM` payload into per-row raw values.
///
/// The NULL comparison is against the field *before* de-escaping, as
/// `PostgreSQL`'s `CopyReadAttributesText` does it: with `NULL 'NUL'` an
/// incoming `\N` is not a null but the one-character string `N`. CSV compares
/// against the raw field too, and additionally requires that no quote appeared
/// in it — `""` is the empty string even when the null string is empty, which
/// is how a CSV copy distinguishes the two at all.
pub(crate) fn decode_copy_rows<'a>(
    data: &'a [u8],
    format: &CopyInFormat,
    columns: &[String],
    force: &CopyForceFlags,
    relation: &str,
) -> Result<Vec<CopyRow<'a>>, ExecError> {
    let text = std::str::from_utf8(data)
        .map_err(|_| ExecError::Syntax("invalid byte sequence for encoding \"UTF8\"".into()))?;
    let mut reader = CopyLineReader::new(text, format);
    let mut rows = Vec::new();
    let mut header_pending = format.header.is_some();
    let mut number = 0_u64;
    // A failure inside the line reader is a failure reading the line the counter
    // has not reached yet, and PostgreSQL reports it against that line with
    // nothing quoted — the line it would have quoted is the one it could not
    // assemble.
    let at_next_line = |number: u64| {
        move |error| {
            crate::exec::with_copy_context(
                error,
                copy_context(relation, number + 1, CopyContext::LineNumber),
            )
        }
    };
    while let CopyLine::Data(line) = reader.read().map_err(at_next_line(number))? {
        number += 1;
        let at_line = |error| {
            crate::exec::with_copy_context(
                error,
                copy_context(relation, number, CopyContext::Line { raw: line }),
            )
        };
        if header_pending {
            header_pending = false;
            if format.header == Some(CopyHeader::Match) {
                match_header_line(line, format, columns).map_err(at_line)?;
            }
            number += reader.take_embedded_lines();
            continue;
        }
        rows.push(CopyRow {
            values: decode_copy_line(line, format, force).map_err(at_line)?,
            line: number,
            raw: line,
        });
        // A CSV field may hold newlines, and PostgreSQL counts the physical
        // lines it read rather than the rows it kept.
        number += reader.take_embedded_lines();
    }
    Ok(rows)
}

/// One line's fields, de-escaped, `None` for the ones that are NULL.
fn decode_copy_line(
    line: &str,
    format: &CopyInFormat,
    force: &CopyForceFlags,
) -> Result<Vec<Option<String>>, ExecError> {
    let Some(csv) = format.framing.csv else {
        return split_text_fields(line, format.framing.delimiter)
            .into_iter()
            .map(|field| {
                if field == format.framing.null {
                    return Ok(None);
                }
                decode_copy_text_field(field).map(Some)
            })
            .collect();
    };
    let fields = split_csv_fields(line, format.framing.delimiter, csv)?;
    Ok(fields
        .into_iter()
        .enumerate()
        .map(|(index, field)| {
            let forced_not_null = force.not_null.get(index).copied().unwrap_or(false);
            let forced_null = force.null.get(index).copied().unwrap_or(false);
            if !field.quoted && field.value == format.framing.null {
                // FORCE_NOT_NULL keeps it as the null string spelled out.
                return forced_not_null.then_some(field.value);
            }
            if forced_null && field.value == format.framing.null {
                return None;
            }
            Some(field.value)
        })
        .collect())
}

/// One CSV field, de-escaped.
struct CsvField {
    value: String,
    /// Whether a quote character appeared anywhere in the raw field. Only an
    /// unquoted field can be the null string, so this is what keeps `""`
    /// distinct from an empty unquoted field.
    quoted: bool,
}

/// `PostgreSQL`'s `CopyReadAttributesCSV`.
///
/// The scan alternates between "not in quote" and "in quote". Outside quotes
/// the delimiter ends the field and a quote opens one; inside, the escape
/// character consumes a following quote or escape, and an unescaped quote
/// closes. A field may open and close quotes more than once, so `a"b"c` is
/// `abc` — `PostgreSQL` accepts it rather than complaining.
fn split_csv_fields(
    line: &str,
    delimiter: u8,
    csv: CsvFraming,
) -> Result<Vec<CsvField>, ExecError> {
    let bytes = line.as_bytes();
    let mut fields = Vec::new();
    let mut index = 0;
    loop {
        let mut value = Vec::new();
        let mut quoted = false;
        let found_delimiter = loop {
            // Outside quotes: the delimiter ends the field, a quote opens one,
            // and the end of the line ends the field too.
            let opened_quote = loop {
                let Some(&c) = bytes.get(index) else {
                    break None;
                };
                index += 1;
                if c == delimiter {
                    break Some(false);
                }
                if c == csv.quote {
                    quoted = true;
                    break Some(true);
                }
                value.push(c);
            };
            match opened_quote {
                None => break false,
                Some(false) => break true,
                Some(true) => {}
            }
            // Inside quotes: only the escape and the quote are special, and
            // the escape is only special before one of the two. When the two
            // characters are the same this is what makes `""` a quote.
            loop {
                let Some(&c) = bytes.get(index) else {
                    return Err(bad_copy_format("unterminated CSV quoted field".into()));
                };
                index += 1;
                if c == csv.escape
                    && let Some(&next) = bytes.get(index)
                    && (next == csv.escape || next == csv.quote)
                {
                    value.push(next);
                    index += 1;
                    continue;
                }
                if c == csv.quote {
                    break;
                }
                value.push(c);
            }
        };
        // The line was UTF-8, and the delimiter, the quote and the escape are
        // each a single ASCII byte, so none of them can be part of a
        // multi-byte sequence and none of the bytes this splitter drops can
        // leave a partial character behind.
        fields.push(CsvField {
            value: String::from_utf8(value).expect("csv fields split on ascii boundaries"),
            quoted,
        });
        if !found_delimiter {
            return Ok(fields);
        }
    }
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
    // The header is split by the copy's own format, so a CSV header can quote
    // a name that holds the delimiter. No `FORCE_` flag reaches it: PostgreSQL
    // matches the header before it applies them.
    let fields = decode_copy_line(line, format, &CopyForceFlags::default())?;
    if fields.len() != columns.len() {
        return Err(bad_copy_format(format!(
            "wrong number of fields in header line: got {}, expected {}",
            fields.len(),
            columns.len()
        )));
    }
    for (index, (field, expected)) in fields.iter().zip(columns).enumerate() {
        let field_number = index + 1;
        let Some(got) = field else {
            return Err(bad_copy_format(format!(
                "column name mismatch in header line field {field_number}: got null value (\"{}\"), expected \"{expected}\"",
                format.framing.null
            )));
        };
        if got != expected {
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

    use super::{CopyForceFlags, CopyInFormat, CopyOutFormat, decode_copy_rows};
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
        for name in ["UTF8", "utf-8", "SQL_ASCII", "sql_ascii"] {
            assert!(CopyOutFormat::resolve(&options(|o| o.encoding = Some(name.into()))).is_ok());
        }
        assert!(CopyOutFormat::resolve(&options(|o| o.encoding = Some("LATIN1".into()))).is_err());
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
            let decoded = decode_copy_rows(
                case.data.as_bytes(),
                &format,
                &columns,
                &CopyForceFlags::default(),
                "t",
            )
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
            decode_copy_rows(data, &format, &[], &CopyForceFlags::default(), "t")
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
            error_of(decode_copy_rows(
                b"s\twrong\na\t1\n",
                &format,
                &columns,
                &CopyForceFlags::default(),
                "t"
            )) == (
                "22P04".to_string(),
                "column name mismatch in header line field 2: got \"wrong\", expected \"n\""
                    .to_string()
            )
        );
        assert!(
            error_of(decode_copy_rows(
                b"s\n",
                &format,
                &columns,
                &CopyForceFlags::default(),
                "t"
            )) == (
                "22P04".to_string(),
                "wrong number of fields in header line: got 1, expected 2".to_string()
            )
        );
        assert!(
            error_of(decode_copy_rows(b"s\t\\N\na\t1\n", &format, &columns, &CopyForceFlags::default(), "t"))
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
