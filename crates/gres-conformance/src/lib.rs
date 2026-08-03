//! Differential conformance harness: run the same SQL against real `PostgreSQL`
//! (the oracle) and Crabka Gres (the subject), diff the outcomes.

pub mod driver_goldens;
pub mod feature_manifest;
mod parser_commands;
pub mod tls;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};

const CASE_ID_TOKEN: &str = "__case_id__";
static NEXT_CASE_ID: AtomicU64 = AtomicU64::new(0);

pub use self::parser_commands::{
    PARSER_COMMAND_REPORT_FORMAT_VERSION, ParserCommandError, ParserCommandReport,
    parser_command_report,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryOutcome {
    /// Row values in text format; None = NULL.
    pub rows: Vec<Vec<Option<String>>>,
    /// SQLSTATE if the statement errored.
    pub error_code: Option<String>,
    /// The server's primary error message, when the statement errored. Parity is
    /// judged on SQLSTATE and rows alone — the message is carried only so the
    /// report can group mismatches by the engine gap that produced them.
    pub error_message: Option<String>,
}

impl QueryOutcome {
    /// A successful outcome carrying `rows`.
    #[must_use]
    pub fn success(rows: Vec<Vec<Option<String>>>) -> Self {
        Self {
            rows,
            error_code: None,
            error_message: None,
        }
    }

    /// A failed outcome whose message the harness could not observe.
    #[must_use]
    pub fn failure(error_code: String) -> Self {
        Self {
            rows: Vec::new(),
            error_code: Some(error_code),
            error_message: None,
        }
    }

    /// A failed outcome carrying the server's primary message.
    #[must_use]
    pub fn failure_with_message(error_code: String, error_message: String) -> Self {
        Self {
            rows: Vec::new(),
            error_code: Some(error_code),
            error_message: Some(error_message),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffResult {
    pub matched: bool,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct CaseResult {
    pub file: String,
    pub sql: String,
    pub matched: bool,
    pub detail: String,
    /// The subject's SQLSTATE for a mismatching statement, for root-cause grouping.
    pub subject_error_code: Option<String>,
    /// The subject's error message for a mismatching statement, for root-cause grouping.
    pub subject_error_message: Option<String>,
}

impl CaseResult {
    /// Record one diffed statement. A matching statement carries no subject
    /// diagnostics — only mismatches feed the root-cause ranking.
    #[must_use]
    pub fn new(file: String, sql: String, diff: DiffResult, subject: &QueryOutcome) -> Self {
        let (subject_error_code, subject_error_message) = if diff.matched {
            (None, None)
        } else {
            (subject.error_code.clone(), subject.error_message.clone())
        };
        Self {
            file,
            sql,
            matched: diff.matched,
            detail: diff.detail,
            subject_error_code,
            subject_error_message,
        }
    }
}

/// One statement from a corpus file, with the inline payload it owns.
///
/// Only `COPY … FROM STDIN` carries a payload: in a `pg_regress` file its data
/// follows the statement as raw lines terminated by `\.`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorpusStatement {
    pub sql: String,
    /// The `COPY … FROM STDIN` data block, newline-terminated, without its `\.`.
    pub stdin_data: Option<String>,
}

impl CorpusStatement {
    /// A statement with no inline payload.
    #[must_use]
    pub fn plain(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            stdin_data: None,
        }
    }
}

/// One root-cause bucket of the mismatching statements in a [`Report`].
///
/// Statements are grouped by the *shape* of the subject's failure rather than
/// its exact text, so "unknown type `int2`" and "unknown type `xml`" land in
/// separate buckets while two occurrences of the same gap land in one. A bucket
/// with no SQLSTATE is a wrong-answer mismatch: the subject executed the
/// statement and produced different rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RootCause {
    /// A stable, normalized signature for the failure shape.
    pub signature: String,
    /// The subject's SQLSTATE, when it errored.
    pub sqlstate: Option<String>,
    /// How many mismatching statements share this signature.
    pub count: usize,
    /// One representative statement, for triage.
    pub example: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtendedCase {
    pub name: String,
    pub sql: String,
    pub params: Vec<ExtendedParam>,
    pub setup: Vec<String>,
    pub teardown: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ExtendedParam {
    #[serde(rename = "type")]
    pub ty: ExtendedParamType,
    pub value: Option<ExtendedParamValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtendedParamType {
    Bool,
    Int4,
    Text,
    /// `jsonb` (OID 3802), sent in the binary `[0x01][canonical text]` format.
    Jsonb,
    #[serde(rename = "int4[]")]
    Int4Array,
    #[serde(rename = "text[]")]
    TextArray,
}

/// A case's parameter value.
///
/// The variants are untagged, so the JSON shape picks the variant: a `jsonb`
/// parameter carries its text in [`ExtendedParamValue::Text`], and both array
/// types carry a JSON array whose elements are checked against the declared
/// [`ExtendedParamType`] in `owned_param`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ExtendedParamValue {
    Bool(bool),
    Int4(i32),
    Text(String),
    Array(Vec<serde_json::Value>),
}

#[derive(Debug, Clone)]
pub struct ExtendedCaseFile {
    pub file: String,
    pub cases: Vec<ExtendedCase>,
}

#[derive(Debug, Error)]
#[error("cannot transform subject CREATE TABLE `{sql}`: {message}")]
pub struct SubjectDdlTransformError {
    sql: String,
    message: String,
}

/// Whether a parse refusal is a feature `crabka` deliberately does not
/// implement (`0A000`) rather than a malformed statement.
///
/// The distinction decides whether [`subject_sharded_statement`] fails closed.
/// A syntax error in table DDL means the transform would skip a table the
/// sharded leg is meant to measure, which must be loud. A deliberate refusal
/// means neither leg creates anything — `PostgreSQL` refuses `MATCH PARTIAL`
/// with `0A000` and so does the subject — so the corpus is asserting the
/// refusal itself and the statement is the same on both legs unsharded.
fn is_unimplemented_feature(error: &crabka_pgparser::ParseError) -> bool {
    error.sqlstate() == "0A000"
}

/// Rewrite one subject `CREATE TABLE` statement to use sharding.
///
/// # Errors
///
/// Returns [`SubjectDdlTransformError`] when a candidate statement cannot be
/// parsed or the rewritten statement is invalid.
pub fn subject_sharded_statement(sql: &str) -> Result<String, SubjectDdlTransformError> {
    let statements = match crabka_pgparser::parse(sql) {
        Ok(statements) => statements,
        // A `CREATE TABLE` the parser *cannot* parse is a harness gap: the
        // transform would silently skip a table the sharded leg is supposed to
        // measure, so it fails closed. The exception is a refusal the parser
        // makes deliberately — a feature it does not implement, which
        // `PostgreSQL` refuses too. That statement creates no table on either
        // leg, both refuse it identically, and the corpus exists to assert
        // exactly that, so passing it through is faithful rather than a silent
        // escape.
        Err(error) if is_create_table_candidate(sql) && !is_unimplemented_feature(&error) => {
            return Err(SubjectDdlTransformError {
                sql: sql.to_string(),
                message: error.to_string(),
            });
        }
        Err(_) => return Ok(sql.to_string()),
    };
    // A TEMP table has no sharded form either — it is session-local, so both legs
    // create an ordinary table — and appending `SHARDED BY` after a trailing
    // clause like `ON COMMIT PRESERVE ROWS` would not even parse.
    if matches!(
        statements.as_slice(),
        [crabka_pgparser::ast::Statement::CreateTable {
            temporary: true,
            ..
        }]
    ) {
        return Ok(sql.to_string());
    }
    if !matches!(
        statements.as_slice(),
        [crabka_pgparser::ast::Statement::CreateTable { .. }]
    ) {
        // `CREATE TABLE … AS` has no `SHARDED BY` spelling, so an ordinary table
        // is the only thing either leg can create; passing it through is the
        // faithful transform, not a silent escape.
        if matches!(
            statements.as_slice(),
            [crabka_pgparser::ast::Statement::CreateTableAs { .. }]
        ) {
            return Ok(sql.to_string());
        }
        // A statement that *looks* like table DDL but did not parse into a shape
        // this transform knows must not slip through unsharded, or the sharded
        // leg would silently measure an ordinary table and report parity for it.
        if is_create_table_candidate(sql) {
            return Err(SubjectDdlTransformError {
                sql: sql.to_string(),
                message: "table DDL shape is not the plain CREATE TABLE this transform rewrites"
                    .into(),
            });
        }
        return Ok(sql.to_string());
    }
    let [crabka_pgparser::ast::Statement::CreateTable { sharded, .. }] = statements.as_slice()
    else {
        unreachable!("CREATE TABLE shape checked above");
    };
    if *sharded {
        return Ok(sql.to_string());
    }
    let trailing_ws_len = sql.len() - sql.trim_end().len();
    let (body, trailing_ws) = sql.split_at(sql.len() - trailing_ws_len);
    let (body, semicolon) = body
        .strip_suffix(';')
        .map_or((body, ""), |without| (without.trim_end(), ";"));
    let transformed = format!("{body} SHARDED{semicolon}{trailing_ws}");
    let reparsed =
        crabka_pgparser::parse(&transformed).map_err(|error| SubjectDdlTransformError {
            sql: sql.to_string(),
            message: format!("rewritten statement does not parse: {error}"),
        })?;
    if !matches!(
        reparsed.as_slice(),
        [crabka_pgparser::ast::Statement::CreateTable { sharded: true, .. }]
    ) {
        return Err(SubjectDdlTransformError {
            sql: sql.to_string(),
            message: "rewritten statement is not one sharded CREATE TABLE".into(),
        });
    }
    Ok(transformed)
}

/// Rewrite the setup statements for one extended conformance case.
///
/// # Errors
///
/// Returns [`SubjectDdlTransformError`] when a setup statement cannot be
/// transformed safely.
pub fn subject_sharded_extended_case(
    case: &ExtendedCase,
) -> Result<ExtendedCase, SubjectDdlTransformError> {
    Ok(ExtendedCase {
        name: case.name.clone(),
        sql: case.sql.clone(),
        params: case.params.clone(),
        setup: case
            .setup
            .iter()
            .map(|statement| subject_sharded_statement(statement))
            .collect::<Result<_, _>>()?,
        teardown: case.teardown.clone(),
    })
}

fn is_create_table_candidate(sql: &str) -> bool {
    use crabka_pgparser::token::{Keyword, Token};

    let tokens = match crabka_pgparser::lexer::lex(sql) {
        Ok(tokens) => tokens,
        Err(error) => lex_valid_prefix(sql, error.position),
    };
    matches!(tokens.first(), Some((Token::Keyword(Keyword::Create), _)))
        && tokens
            .iter()
            .take(8)
            .any(|(token, _)| matches!(token, Token::Keyword(Keyword::Table)))
}

fn lex_valid_prefix(sql: &str, mut end: usize) -> Vec<(crabka_pgparser::token::Token, usize)> {
    loop {
        while !sql.is_char_boundary(end) {
            end -= 1;
        }
        match crabka_pgparser::lexer::lex(&sql[..end]) {
            Ok(tokens) => return tokens,
            Err(error) if error.position < end => end = error.position,
            Err(_) if end > 0 => end -= 1,
            Err(_) => return Vec::new(),
        }
    }
}

#[derive(Debug, Error)]
enum ExtendedCaseFileError {
    #[error("failed to read extended case file {}: {source}", path.display())]
    Read { path: PathBuf, source: io::Error },
    #[error("failed to parse extended case file {}: {source}", path.display())]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },
}

#[derive(Debug, Clone)]
enum OwnedParam {
    Bool(Option<bool>),
    Int4(Option<i32>),
    Text(Option<String>),
    Jsonb(Option<JsonbParam>),
    Int4Array(Option<Vec<Option<i32>>>),
    TextArray(Option<Vec<Option<String>>>),
}

/// A `jsonb` bind parameter in `PostgreSQL`'s binary format.
///
/// `tokio-postgres` only encodes `jsonb` through its optional `serde_json`
/// integration, so the harness carries the document as text and writes the
/// jsonb version byte itself. That keeps the bind on the binary path that real
/// drivers use (`[0x01][json text]`) rather than falling back to a text cast.
#[derive(Debug, Clone, PartialEq, Eq)]
struct JsonbParam(String);

impl ToSql for JsonbParam {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(&[1]);
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }

    to_sql_checked!();
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub total: usize,
    pub matched: usize,
    pub parity_percent: f64,
    pub cases: Vec<CaseResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileSummary {
    pub file: String,
    pub total: usize,
    pub matched: usize,
}

/// Machine-readable parity floor for CI.
///
/// The G-1 gate requires the vendored engine to reproduce exactly the donor
/// repository's conformance corpus size, while the match count may only ratchet
/// upward.
#[derive(Debug, Serialize, Deserialize)]
pub struct Baseline {
    pub total: usize,
    pub matched: usize,
}

/// Per-file parity floor for adopted `PostgreSQL` `pg_regress` files.
///
/// Each upstream file ratchets independently because the adopted regress corpus
/// starts far below full parity and grows by `PostgreSQL` feature area.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegressBaseline {
    pub files: Vec<RegressFileBaseline>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegressFileBaseline {
    pub file: String,
    pub total: usize,
    pub matched: usize,
}

impl Report {
    #[must_use]
    pub fn new(cases: Vec<CaseResult>) -> Self {
        let total = cases.len();
        let matched = cases.iter().filter(|c| c.matched).count();
        let parity_percent = parity_percent(matched, total);
        Self {
            total,
            matched,
            parity_percent,
            cases,
        }
    }

    /// Rank the mismatching statements by the engine gap that produced them.
    ///
    /// The ranking is the conformance work queue: the first bucket is the single
    /// change that would convert the most statements to matches.
    #[must_use]
    pub fn root_causes(&self) -> Vec<RootCause> {
        let mut buckets: BTreeMap<String, RootCause> = BTreeMap::new();
        for case in self.cases.iter().filter(|case| !case.matched) {
            let signature = root_cause_signature(case);
            buckets
                .entry(signature.clone())
                .and_modify(|bucket| bucket.count += 1)
                .or_insert_with(|| RootCause {
                    signature,
                    sqlstate: case.subject_error_code.clone(),
                    count: 1,
                    example: case.sql.clone(),
                });
        }
        let mut ranked: Vec<RootCause> = buckets.into_values().collect();
        // Descending by count, then by signature so the order is deterministic.
        ranked.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.signature.cmp(&b.signature))
        });
        ranked
    }

    #[must_use]
    pub fn markdown_summary(&self) -> String {
        let mut md = format!(
            "# crabka-gres conformance report\n\n**Parity: {:.1}%** ({} / {} statements match the oracle)\n\n",
            self.parity_percent, self.matched, self.total
        );
        let ranked = self.root_causes();
        if !ranked.is_empty() {
            md.push_str("## Mismatches by root cause\n\n");
            md.push_str("| statements | sqlstate | signature | example |\n|---|---|---|---|\n");
            for cause in &ranked {
                let example = cause.example.replace('|', "\\|").replace('\n', " ");
                let signature = cause.signature.replace('|', "\\|").replace('\n', " ");
                writeln!(
                    md,
                    "| {} | {} | {} | `{}` |",
                    cause.count,
                    cause.sqlstate.as_deref().unwrap_or("-"),
                    signature,
                    truncate_for_report(&example)
                )
                .expect("writing to a String cannot fail");
            }
            md.push('\n');
            md.push_str("## Every statement\n\n");
        }
        md.push_str("| file | statement | result |\n|---|---|---|\n");
        for c in &self.cases {
            let sql = c.sql.replace('|', "\\|").replace('\n', " ");
            let result = if c.matched {
                "match".to_string()
            } else {
                let detail = c.detail.replace('|', "\\|").replace('\n', " ");
                format!("MISMATCH: {detail}")
            };
            writeln!(md, "| {} | `{}` | {} |", c.file, sql, result)
                .expect("writing to a String cannot fail");
        }
        md
    }

    #[must_use]
    pub fn file_summaries(&self) -> Vec<FileSummary> {
        let mut summaries = BTreeMap::new();
        for case in &self.cases {
            let summary = summaries
                .entry(case.file.clone())
                .or_insert_with(|| FileSummary {
                    file: case.file.clone(),
                    total: 0,
                    matched: 0,
                });
            summary.total += 1;
            if case.matched {
                summary.matched += 1;
            }
        }
        summaries.into_values().collect()
    }

    /// Gate this report against a recorded baseline.
    ///
    /// `Err` carries a human-readable failure for CI logs.
    ///
    /// # Errors
    ///
    /// Returns an error when corpus size changes or matched parity regresses.
    pub fn check_baseline(&self, baseline: &Baseline) -> Result<(), String> {
        if self.total != baseline.total {
            return Err(format!(
                "corpus size changed: report has {} statements, baseline records {} — \
                 update crates/gres-conformance/baseline.json deliberately, never incidentally",
                self.total, baseline.total
            ));
        }
        if self.matched < baseline.matched {
            return Err(format!(
                "parity regression: {}/{} statements match the oracle, baseline requires at least {}",
                self.matched, self.total, baseline.matched
            ));
        }
        Ok(())
    }
}

impl RegressBaseline {
    /// Gate a report against per-file `pg_regress` ratchets.
    ///
    /// `Err` carries a human-readable failure for CI logs.
    ///
    /// # Errors
    ///
    /// Returns an error when the file set or corpus size changes, or matched
    /// parity regresses.
    ///
    /// # Panics
    ///
    /// Panics only if equal file-name sets unexpectedly fail a map lookup.
    pub fn check_report(&self, report: &Report) -> Result<(), String> {
        let baseline_files: BTreeMap<_, _> = self.files.iter().map(|f| (&f.file, f)).collect();
        let report_summaries = report.file_summaries();
        let report_files: BTreeMap<_, _> = report_summaries.iter().map(|f| (&f.file, f)).collect();

        let baseline_names: BTreeSet<_> = baseline_files.keys().copied().collect();
        let report_names: BTreeSet<_> = report_files.keys().copied().collect();
        if baseline_names != report_names {
            return Err(format!(
                "regress corpus file set changed: report has {report_names:?}, baseline records {baseline_names:?} — update crates/gres-conformance/corpus-regress/baseline.json deliberately"
            ));
        }

        for (file, report_summary) in report_files {
            let baseline = baseline_files
                .get(file)
                .expect("matching file sets guarantee a baseline entry");
            if report_summary.total != baseline.total {
                return Err(format!(
                    "regress corpus size changed for {file}: report has {} statements, baseline records {}",
                    report_summary.total, baseline.total
                ));
            }
            if report_summary.matched < baseline.matched {
                return Err(format!(
                    "regress parity regression for {file}: {}/{} statements match the oracle, baseline requires at least {}",
                    report_summary.matched, report_summary.total, baseline.matched
                ));
            }
        }
        Ok(())
    }
}

/// Discover SQL corpus files in deterministic path order.
///
/// # Errors
///
/// Returns an I/O error when a corpus directory cannot be read.
pub fn discover_sql_files(corpus: &Path, recursive: bool) -> Result<Vec<PathBuf>, io::Error> {
    let mut files = Vec::new();
    collect_sql_files(corpus, recursive, &mut files)?;
    files.sort();
    Ok(files)
}

/// Discover extended-case JSON files in deterministic path order.
///
/// # Errors
///
/// Returns an I/O error when a corpus directory cannot be read.
pub fn discover_extended_case_files(corpus: &Path) -> Result<Vec<PathBuf>, io::Error> {
    let mut files = Vec::new();
    collect_files_with_extension(corpus, true, "json", &mut files)?;
    files.retain(|path| path.file_name().is_none_or(|name| name != "baseline.json"));
    files.sort();
    Ok(files)
}

fn collect_sql_files(
    directory: &Path,
    recursive: bool,
    files: &mut Vec<PathBuf>,
) -> Result<(), io::Error> {
    collect_files_with_extension(directory, recursive, "sql", files)
}

fn collect_files_with_extension(
    directory: &Path,
    recursive: bool,
    extension: &str,
    files: &mut Vec<PathBuf>,
) -> Result<(), io::Error> {
    if !directory.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("corpus directory does not exist: {}", directory.display()),
        ));
    }

    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            if recursive {
                collect_files_with_extension(&path, recursive, extension, files)?;
            }
            continue;
        }
        if path
            .extension()
            .is_some_and(|file_extension| file_extension == extension)
        {
            files.push(path);
        }
    }
    Ok(())
}

/// Loads extended-protocol case files from a JSON corpus directory.
///
/// Each `.json` file except the reserved `baseline.json` metadata file contains
/// an array of [`ExtendedCase`] values. Files are discovered recursively so
/// feature areas can grow independently.
///
/// # Errors
///
/// Returns an error when corpus discovery, file reads, or JSON decoding fails.
pub fn load_extended_case_files(
    corpus: &Path,
) -> Result<Vec<ExtendedCaseFile>, Box<dyn std::error::Error>> {
    let mut case_files = Vec::new();
    for path in discover_extended_case_files(corpus)? {
        let text =
            std::fs::read_to_string(&path).map_err(|source| ExtendedCaseFileError::Read {
                path: path.clone(),
                source,
            })?;
        let cases = serde_json::from_str::<Vec<ExtendedCase>>(&text).map_err(|source| {
            ExtendedCaseFileError::Parse {
                path: path.clone(),
                source,
            }
        })?;
        case_files.push(ExtendedCaseFile {
            file: corpus_file_name(corpus, &path),
            cases,
        });
    }
    Ok(case_files)
}

#[must_use]
pub fn corpus_file_name(corpus: &Path, path: &Path) -> String {
    path.strip_prefix(corpus)
        .unwrap_or(path)
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/")
}

#[expect(
    clippy::cast_precision_loss,
    reason = "parity is a one-decimal report metric; exact integer counts are stored separately"
)]
fn parity_percent(matched: usize, total: usize) -> f64 {
    if total == 0 {
        return 0.0;
    }
    matched as f64 * 100.0 / total as f64
}

/// Collapse one mismatch into the failure shape that caused it.
///
/// Positions and bare integers vary between otherwise identical failures, so
/// they are normalized away; quoted names are kept because each one is usually
/// a distinct piece of missing work.
fn root_cause_signature(case: &CaseResult) -> String {
    let Some(message) = case.subject_error_message.as_deref() else {
        return case.subject_error_code.as_ref().map_or_else(
            || "wrong rows (subject executed the statement)".to_string(),
            |code| format!("subject error {code} (no message captured)"),
        );
    };
    let mut normalized = String::with_capacity(message.len());
    let mut digits = false;
    for ch in message.chars() {
        if ch.is_ascii_digit() {
            if !digits {
                normalized.push('N');
                digits = true;
            }
            continue;
        }
        digits = false;
        normalized.push(ch);
    }
    truncate_for_report(normalized.trim())
}

/// Keep report cells readable; the full text stays in the JSON report.
fn truncate_for_report(text: &str) -> String {
    const LIMIT: usize = 120;
    if text.chars().count() <= LIMIT {
        return text.to_string();
    }
    let kept: String = text.chars().take(LIMIT).collect();
    format!("{kept}…")
}

#[must_use]
pub fn diff(oracle: &QueryOutcome, subject: &QueryOutcome) -> DiffResult {
    if oracle.error_code != subject.error_code {
        return DiffResult {
            matched: false,
            detail: format!(
                "error code: oracle={:?} subject={:?}",
                oracle.error_code, subject.error_code
            ),
        };
    }
    if oracle.rows != subject.rows {
        return DiffResult {
            matched: false,
            detail: format!("rows: oracle={:?} subject={:?}", oracle.rows, subject.rows),
        };
    }
    DiffResult {
        matched: true,
        detail: String::new(),
    }
}

/// Executes one statement via the simple query protocol, normalizing the
/// outcome. Errors with no SQLSTATE (I/O, disconnect) map to "XXIO" so they
/// are visible as harness-level failures rather than silently matching.
pub async fn run_one(client: &tokio_postgres::Client, sql: &str) -> QueryOutcome {
    use tokio_postgres::SimpleQueryMessage;
    match client.simple_query(sql).await {
        Ok(messages) => {
            let mut rows = Vec::new();
            for m in messages {
                if let SimpleQueryMessage::Row(row) = m {
                    let mut values = Vec::with_capacity(row.len());
                    for i in 0..row.len() {
                        values.push(row.get(i).map(std::string::ToString::to_string));
                    }
                    rows.push(values);
                }
            }
            QueryOutcome::success(rows)
        }
        Err(e) => outcome_from_error(&e),
    }
}

/// Execute one corpus statement, routing `COPY` through its wire subprotocol.
///
/// A `COPY` statement sent down the simple query path leaves the connection in
/// copy mode and poisons every later statement in the file, so both directions
/// are handled explicitly here.
pub async fn run_corpus_statement(
    client: &tokio_postgres::Client,
    statement: &CorpusStatement,
) -> QueryOutcome {
    match copy_payload(&statement.sql) {
        Some(CopyPayload::Stdin) => {
            run_copy_in(client, &statement.sql, statement.stdin_data.as_deref()).await
        }
        Some(CopyPayload::Stdout) => run_copy_out(client, &statement.sql).await,
        None => run_one(client, &statement.sql).await,
    }
}

/// Feed a `COPY … FROM STDIN` data block over the copy-in subprotocol.
async fn run_copy_in(
    client: &tokio_postgres::Client,
    sql: &str,
    data: Option<&str>,
) -> QueryOutcome {
    use futures_util::SinkExt as _;

    let sink = match client.copy_in::<_, bytes::Bytes>(sql).await {
        Ok(sink) => sink,
        Err(error) => return outcome_from_error(&error),
    };
    futures_util::pin_mut!(sink);
    let payload = bytes::Bytes::copy_from_slice(data.unwrap_or_default().as_bytes());
    // A failed send means the server already rejected the COPY, so there is no
    // completion to await — `finish` would block forever on a reply that is
    // never coming. Abandoning the sink leaves the connection in copy mode; the
    // caller's reconnect-on-`XXIO` path is what restores it.
    if let Err(error) = sink.send(payload).await {
        return outcome_from_error(&error);
    }
    match sink.finish().await {
        Ok(_rows) => QueryOutcome::success(Vec::new()),
        Err(error) => outcome_from_error(&error),
    }
}

/// Collect a `COPY … TO STDOUT` stream as one text column per output line.
async fn run_copy_out(client: &tokio_postgres::Client, sql: &str) -> QueryOutcome {
    use futures_util::StreamExt as _;

    let stream = match client.copy_out(sql).await {
        Ok(stream) => stream,
        Err(error) => return outcome_from_error(&error),
    };
    futures_util::pin_mut!(stream);
    let mut payload = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => payload.extend_from_slice(&bytes),
            Err(error) => return outcome_from_error(&error),
        }
    }
    let Ok(text) = String::from_utf8(payload) else {
        return QueryOutcome::failure_with_message(
            "XXIO".into(),
            "COPY TO STDOUT payload is not valid UTF-8".into(),
        );
    };
    let rows = text
        .lines()
        .map(|line| vec![Some(line.to_string())])
        .collect();
    QueryOutcome::success(rows)
}

pub async fn run_extended_one(
    client: &mut tokio_postgres::Client,
    case: &ExtendedCase,
) -> QueryOutcome {
    let case = materialize_case(case);
    let transaction = match client.transaction().await {
        Ok(transaction) => transaction,
        Err(error) => return outcome_from_error(&error),
    };

    let mut outcome = match execute_case_statements(&transaction, &case.setup).await {
        Ok(()) => query_extended(&transaction, &case).await,
        Err(error_code) => QueryOutcome::failure(error_code),
    };
    let mut cleanup_after_rollback = outcome.error_code.is_some();
    if !cleanup_after_rollback
        && let Err(error_code) = execute_case_statements(&transaction, &case.teardown).await
    {
        outcome = QueryOutcome::failure(error_code);
        cleanup_after_rollback = true;
    }

    if let Err(error) = transaction.rollback().await
        && outcome.error_code.is_none()
    {
        outcome = outcome_from_error(&error);
    }
    if cleanup_after_rollback
        && let Err(error_code) = cleanup_case(client, &case.teardown).await
        && outcome.error_code.is_none()
    {
        outcome = QueryOutcome::failure(error_code);
    }
    outcome
}

fn materialize_case(case: &ExtendedCase) -> ExtendedCase {
    if !case.sql.contains(CASE_ID_TOKEN)
        && !case.setup.iter().any(|sql| sql.contains(CASE_ID_TOKEN))
        && !case.teardown.iter().any(|sql| sql.contains(CASE_ID_TOKEN))
    {
        return case.clone();
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let id = format!(
        "{nanos}_{}_{}",
        std::process::id(),
        NEXT_CASE_ID.fetch_add(1, Ordering::Relaxed)
    );
    let replace = |sql: &String| sql.replace(CASE_ID_TOKEN, &id);
    ExtendedCase {
        name: case.name.clone(),
        sql: case.sql.replace(CASE_ID_TOKEN, &id),
        params: case.params.clone(),
        setup: case.setup.iter().map(replace).collect(),
        teardown: case.teardown.iter().map(replace).collect(),
    }
}

async fn cleanup_case(
    client: &mut tokio_postgres::Client,
    statements: &[String],
) -> Result<(), String> {
    let mut first_error = None;
    for statement in statements {
        if statement.trim().is_empty() {
            continue;
        }
        // Recover a client whose prior transaction could not be rolled back,
        // and isolate every cleanup statement so one failure cannot suppress
        // the remaining deterministic cleanup work.
        let _ = client.batch_execute("ROLLBACK").await;
        let transaction = match client.transaction().await {
            Ok(transaction) => transaction,
            Err(error) => {
                first_error.get_or_insert_with(|| error_code_from_error(&error));
                continue;
            }
        };
        let result = transaction.batch_execute(statement).await;
        match result {
            Ok(()) => {
                if let Err(error) = transaction.commit().await {
                    first_error.get_or_insert_with(|| error_code_from_error(&error));
                }
            }
            Err(error) => {
                first_error.get_or_insert_with(|| error_code_from_error(&error));
                let _ = transaction.rollback().await;
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn query_extended(
    client: &tokio_postgres::Transaction<'_>,
    case: &ExtendedCase,
) -> QueryOutcome {
    let params = match owned_params(&case.params) {
        Ok(params) => params,
        Err(error) => {
            return QueryOutcome::failure(error);
        }
    };
    let param_refs = param_refs(&params);
    let param_types = case
        .params
        .iter()
        .map(|param| postgres_type(param.ty))
        .collect::<Vec<_>>();
    let statement = match client.prepare_typed(&case.sql, &param_types).await {
        Ok(statement) => statement,
        Err(error) => return outcome_from_error(&error),
    };
    match client.query(&statement, &param_refs).await {
        Ok(rows) => rows_to_outcome(&rows),
        Err(error) => outcome_from_error(&error),
    }
}

async fn execute_case_statements(
    client: &tokio_postgres::Transaction<'_>,
    statements: &[String],
) -> Result<(), String> {
    for statement in statements {
        if statement.trim().is_empty() {
            continue;
        }
        client
            .batch_execute(statement)
            .await
            .map_err(|error| error_code_from_error(&error))?;
    }
    Ok(())
}

fn owned_params(params: &[ExtendedParam]) -> Result<Vec<OwnedParam>, String> {
    params.iter().map(owned_param).collect()
}

fn owned_param(param: &ExtendedParam) -> Result<OwnedParam, String> {
    match (param.ty, &param.value) {
        (ExtendedParamType::Bool, None) => Ok(OwnedParam::Bool(None)),
        (ExtendedParamType::Bool, Some(ExtendedParamValue::Bool(value))) => {
            Ok(OwnedParam::Bool(Some(*value)))
        }
        (ExtendedParamType::Int4, None) => Ok(OwnedParam::Int4(None)),
        (ExtendedParamType::Int4, Some(ExtendedParamValue::Int4(value))) => {
            Ok(OwnedParam::Int4(Some(*value)))
        }
        (ExtendedParamType::Text, None) => Ok(OwnedParam::Text(None)),
        (ExtendedParamType::Text, Some(ExtendedParamValue::Text(value))) => {
            Ok(OwnedParam::Text(Some(value.clone())))
        }
        (ExtendedParamType::Jsonb, None) => Ok(OwnedParam::Jsonb(None)),
        (ExtendedParamType::Jsonb, Some(ExtendedParamValue::Text(value))) => {
            Ok(OwnedParam::Jsonb(Some(JsonbParam(value.clone()))))
        }
        (ExtendedParamType::Int4Array, None) => Ok(OwnedParam::Int4Array(None)),
        (ExtendedParamType::Int4Array, Some(ExtendedParamValue::Array(elements))) => {
            Ok(OwnedParam::Int4Array(Some(int4_elements(elements)?)))
        }
        (ExtendedParamType::TextArray, None) => Ok(OwnedParam::TextArray(None)),
        (ExtendedParamType::TextArray, Some(ExtendedParamValue::Array(elements))) => {
            Ok(OwnedParam::TextArray(Some(text_elements(elements)?)))
        }
        (ty, value) => Err(format!(
            "XXPARAM: {ty:?} parameter has incompatible value {value:?}"
        )),
    }
}

fn int4_elements(elements: &[serde_json::Value]) -> Result<Vec<Option<i32>>, String> {
    elements
        .iter()
        .map(|element| match element {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::Number(number) => number
                .as_i64()
                .and_then(|value| i32::try_from(value).ok())
                .map(Some)
                .ok_or_else(|| format!("XXPARAM: int4[] element {number} is not an int4")),
            other => Err(format!("XXPARAM: int4[] element {other} is not a number")),
        })
        .collect()
}

fn text_elements(elements: &[serde_json::Value]) -> Result<Vec<Option<String>>, String> {
    elements
        .iter()
        .map(|element| match element {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::String(value) => Ok(Some(value.clone())),
            other => Err(format!("XXPARAM: text[] element {other} is not a string")),
        })
        .collect()
}

fn param_refs(params: &[OwnedParam]) -> Vec<&(dyn ToSql + Sync)> {
    params
        .iter()
        .map(|param| match param {
            OwnedParam::Bool(value) => value as &(dyn ToSql + Sync),
            OwnedParam::Int4(value) => value as &(dyn ToSql + Sync),
            OwnedParam::Text(value) => value as &(dyn ToSql + Sync),
            OwnedParam::Jsonb(value) => value as &(dyn ToSql + Sync),
            OwnedParam::Int4Array(value) => value as &(dyn ToSql + Sync),
            OwnedParam::TextArray(value) => value as &(dyn ToSql + Sync),
        })
        .collect()
}

fn postgres_type(ty: ExtendedParamType) -> Type {
    match ty {
        ExtendedParamType::Bool => Type::BOOL,
        ExtendedParamType::Int4 => Type::INT4,
        ExtendedParamType::Text => Type::TEXT,
        ExtendedParamType::Jsonb => Type::JSONB,
        ExtendedParamType::Int4Array => Type::INT4_ARRAY,
        ExtendedParamType::TextArray => Type::TEXT_ARRAY,
    }
}

fn rows_to_outcome(rows: &[tokio_postgres::Row]) -> QueryOutcome {
    let mut normalized_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut normalized_values = Vec::with_capacity(row.len());
        for column_index in 0..row.len() {
            match cell_to_text(row, column_index) {
                Ok(value) => normalized_values.push(value),
                Err(error_code) => {
                    return QueryOutcome::failure(error_code);
                }
            }
        }
        normalized_rows.push(normalized_values);
    }
    QueryOutcome::success(normalized_rows)
}

fn cell_to_text(row: &tokio_postgres::Row, column_index: usize) -> Result<Option<String>, String> {
    let ty = row.columns()[column_index].type_();
    if *ty == Type::BOOL {
        let value = row
            .try_get::<_, Option<bool>>(column_index)
            .map_err(|error| error_code_from_error(&error))?;
        return Ok(value.map(|bool_value| bool_value.to_string()));
    }
    if *ty == Type::INT4 {
        let value = row
            .try_get::<_, Option<i32>>(column_index)
            .map_err(|error| error_code_from_error(&error))?;
        return Ok(value.map(|int_value| int_value.to_string()));
    }
    let value = row
        .try_get::<_, Option<String>>(column_index)
        .map_err(|error| error_code_from_error(&error))?;
    Ok(value)
}

fn outcome_from_error(error: &tokio_postgres::Error) -> QueryOutcome {
    match error.as_db_error() {
        Some(db) => QueryOutcome::failure_with_message(
            db.code().code().to_string(),
            db.message().to_string(),
        ),
        None => QueryOutcome::failure(error_code_from_error(error)),
    }
}

fn error_code_from_error(error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map_or_else(|| "XXIO".to_string(), |db| db.code().code().to_string())
}

/// Statement splitter: semicolons outside single/double quotes, line comments,
/// and dollar-quoted strings. Doubled quotes ('') net-cancel under the toggle
/// approach, keeping ; inside strings protected.
///
/// A `COPY … FROM STDIN` statement additionally absorbs the inline data block
/// that follows it in a `pg_regress` file, up to the `\.` terminator line. Those
/// lines are not SQL: leaving them in the statement stream both mis-measures
/// them as bogus statements and leaves the connection stuck in copy mode.
#[must_use]
pub fn split_statements(sql: &str) -> Vec<CorpusStatement> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;

    while i < sql.len() {
        let c = char_at(sql, i);
        // Line comment (outside strings).
        if !in_single && !in_double && c == '-' && sql[i..].starts_with("--") {
            i = line_end(sql, i);
            continue;
        }
        // psql meta-command lines in pg_regress files are harness controls, not
        // SQL. Skip them so the following SQL statement is still exercised.
        if !in_single && !in_double && c == '\\' && current.trim().is_empty() {
            i = line_end(sql, i);
            continue;
        }
        // Dollar-quoted string (outside other strings).
        if !in_single
            && !in_double
            && c == '$'
            && let Some(tag_len) = dollar_tag_len(&sql.as_bytes()[i..])
        {
            let tag = &sql[i..i + tag_len];
            current.push_str(tag);
            i += tag_len;
            // Consume until the matching closing tag.
            loop {
                if i >= sql.len() {
                    break; // unterminated; emit what we have
                }
                if sql[i..].starts_with(tag) {
                    current.push_str(tag);
                    i += tag_len;
                    break;
                }
                let ch = char_at(sql, i);
                current.push(ch);
                i += ch.len_utf8();
            }
            continue;
        }
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            ';' if !in_single && !in_double => {
                let statement = current.trim().to_string();
                current.clear();
                i += 1;
                if !statement.is_empty() {
                    let stdin_data = if copy_payload(&statement) == Some(CopyPayload::Stdin) {
                        let (data, resume) = take_copy_data_block(sql, i);
                        i = resume;
                        Some(data)
                    } else {
                        None
                    };
                    statements.push(CorpusStatement {
                        sql: statement,
                        stdin_data,
                    });
                }
                continue;
            }
            _ => {}
        }
        current.push(c);
        i += c.len_utf8();
    }
    let statement = current.trim().to_string();
    if !statement.is_empty() {
        statements.push(CorpusStatement {
            sql: statement,
            stdin_data: None,
        });
    }
    statements
}

/// The character starting at byte offset `i`, which is always a boundary here.
fn char_at(sql: &str, i: usize) -> char {
    sql[i..]
        .chars()
        .next()
        .expect("split_statements only indexes inside the input")
}

/// The offset of the newline ending the line containing `i`, or the input end.
fn line_end(sql: &str, i: usize) -> usize {
    sql[i..]
        .find('\n')
        .map_or_else(|| sql.len(), |offset| i + offset)
}

/// Consume a `COPY … FROM STDIN` data block starting after the statement's `;`.
///
/// Returns the data (each line newline-terminated, as the wire format requires)
/// and the offset just past the `\.` terminator line.
fn take_copy_data_block(sql: &str, after_semicolon: usize) -> (String, usize) {
    // The rest of the statement's own line is not data.
    let mut cursor = line_end(sql, after_semicolon);
    if cursor < sql.len() {
        cursor += 1;
    }
    let mut data = String::new();
    while cursor < sql.len() {
        let end = line_end(sql, cursor);
        let line = &sql[cursor..end];
        let next = if end < sql.len() { end + 1 } else { end };
        if line.trim_end_matches('\r') == "\\." {
            return (data, next);
        }
        data.push_str(line.trim_end_matches('\r'));
        data.push('\n');
        cursor = next;
    }
    (data, cursor)
}

/// Which end of a `COPY` statement the client is responsible for, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyPayload {
    /// `COPY … FROM STDIN` — the client sends the data.
    Stdin,
    /// `COPY … TO STDOUT` — the client receives the data.
    Stdout,
}

/// Classify a statement as a client-side `COPY`, ignoring case and layout.
///
/// Both forms take over the connection with a subprotocol the ordinary simple
/// query path cannot speak, so they must be routed before execution rather than
/// discovered by failing.
fn copy_payload(sql: &str) -> Option<CopyPayload> {
    // Punctuation is a token boundary in PostgreSQL's grammar but not in
    // `split_whitespace`, and `COPY t FROM stdin(on_error ignore)` is written
    // without a space. Missing that spelling routes a `COPY` down the ordinary
    // query path, which wedges the connection in copy mode.
    let normalized: String = sql
        .to_ascii_uppercase()
        .chars()
        .map(|ch| if "(),;".contains(ch) { ' ' } else { ch })
        .collect();
    let mut words = normalized.split_whitespace();
    if words.next() != Some("COPY") {
        return None;
    }
    let words: Vec<&str> = words.collect();
    // PostgreSQL's grammar accepts STDIN and STDOUT interchangeably: both mean
    // "the client end", and the direction comes from FROM/TO alone. So
    // `COPY t FROM STDOUT` really is a copy-in, and treating it as anything else
    // wedges the connection — `insert.sql` in the regression corpus relies on it.
    words.windows(2).find_map(|pair| match pair {
        ["FROM", "STDIN" | "STDOUT"] => Some(CopyPayload::Stdin),
        ["TO", "STDIN" | "STDOUT"] => Some(CopyPayload::Stdout),
        _ => None,
    })
}

/// If `s` begins with a dollar-quote opening tag (`$$` or `$tag$`), return its
/// byte length, else None. A tag body is `[A-Za-z_][A-Za-z0-9_]*`.
fn dollar_tag_len(s: &[u8]) -> Option<usize> {
    if s.first() != Some(&b'$') {
        return None;
    }
    let mut j = 1;
    if s.get(j) == Some(&b'$') {
        return Some(2); // `$$`
    }
    // First tag char must be a letter or underscore.
    match s.get(j) {
        Some(&b) if b == b'_' || b.is_ascii_alphabetic() => {}
        _ => return None,
    }
    j += 1;
    while let Some(&b) = s.get(j) {
        if b == b'_' || b.is_ascii_alphanumeric() {
            j += 1;
        } else {
            break;
        }
    }
    if s.get(j) == Some(&b'$') {
        Some(j + 1)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn splits_statements_on_semicolons_respecting_quotes_and_comments() {
        let sql = "SELECT 1;\n-- a comment; with a semicolon\nSELECT 'a;b';\nSELECT 2";
        assert!(
            split_statements(sql)
                == vec![
                    CorpusStatement::plain("SELECT 1"),
                    CorpusStatement::plain("SELECT 'a;b'"),
                    CorpusStatement::plain("SELECT 2"),
                ]
        );
    }

    #[test]
    fn identical_outcomes_match() {
        let a = QueryOutcome::success(vec![vec![Some("1".into())]]);
        assert!(diff(&a, &a.clone()).matched);
    }

    #[test]
    fn differing_rows_mismatch_with_detail() {
        let oracle = QueryOutcome::success(vec![vec![Some("1".into())]]);
        let subject = QueryOutcome::success(vec![vec![Some("2".into())]]);
        let d = diff(&oracle, &subject);
        assert!(!d.matched);
        assert!(d.detail.contains("rows"));
    }

    #[test]
    fn matching_error_codes_match() {
        // Same SQLSTATE on both sides counts as parity (e.g. both reject).
        let a = QueryOutcome::failure("42601".into());
        assert!(diff(&a, &a.clone()).matched);
        let b = QueryOutcome::failure("0A000".into());
        assert!(!diff(&a, &b).matched);
    }

    #[test]
    fn doubled_quotes_keep_semicolons_protected() {
        // SQL escapes a quote by doubling it; the toggle approach keeps the
        // in-string state net-unchanged across '' so the ; stays protected.
        let sql = "SELECT 'it''s;bad';SELECT 2";
        assert!(
            split_statements(sql)
                == vec![
                    CorpusStatement::plain("SELECT 'it''s;bad'"),
                    CorpusStatement::plain("SELECT 2"),
                ]
        );
    }

    #[test]
    fn dollar_quoted_body_is_not_split_on_inner_semicolons() {
        let sql = "SELECT 1;\nDO $$ BEGIN x; y; END $$;\nSELECT 2";
        assert!(
            split_statements(sql)
                == vec![
                    CorpusStatement::plain("SELECT 1"),
                    CorpusStatement::plain("DO $$ BEGIN x; y; END $$"),
                    CorpusStatement::plain("SELECT 2"),
                ]
        );
    }

    #[test]
    fn tagged_dollar_quote_is_matched_by_tag() {
        let sql = "SELECT $tag$a;b$tag$ ; SELECT 2";
        assert!(
            split_statements(sql)
                == vec![
                    CorpusStatement::plain("SELECT $tag$a;b$tag$"),
                    CorpusStatement::plain("SELECT 2"),
                ]
        );
    }

    #[test]
    fn psql_meta_commands_are_not_sql_statements() {
        let sql = "SELECT true;\n\\pset null '(null)'\nSELECT NULL;";
        assert!(
            split_statements(sql)
                == vec![
                    CorpusStatement::plain("SELECT true"),
                    CorpusStatement::plain("SELECT NULL"),
                ]
        );
    }

    #[test]
    fn baseline_passes_on_exact_match() {
        let r = report_with(613, 591);
        let b = Baseline {
            total: 613,
            matched: 591,
        };
        assert!(r.check_baseline(&b).is_ok());
    }

    #[test]
    fn baseline_passes_on_improvement() {
        let r = report_with(613, 600);
        let b = Baseline {
            total: 613,
            matched: 591,
        };
        assert!(r.check_baseline(&b).is_ok());
    }

    #[test]
    fn baseline_fails_on_parity_regression() {
        let r = report_with(613, 580);
        let b = Baseline {
            total: 613,
            matched: 591,
        };
        let err = r.check_baseline(&b).expect_err("regression must fail");
        assert!(err.contains("parity regression"));
    }

    #[test]
    fn baseline_fails_on_corpus_size_change() {
        let r = report_with(620, 620);
        let b = Baseline {
            total: 613,
            matched: 591,
        };
        let err = r.check_baseline(&b).expect_err("size change must fail");
        assert!(err.contains("corpus size changed"));
    }

    #[test]
    fn regress_baseline_ratchets_each_file_independently() {
        let report = Report::new(vec![
            case("boolean/boolean.sql", true),
            case("boolean/boolean.sql", false),
            case("int4/int4.sql", true),
        ]);
        let baseline = RegressBaseline {
            files: vec![
                RegressFileBaseline {
                    file: "boolean/boolean.sql".into(),
                    total: 2,
                    matched: 1,
                },
                RegressFileBaseline {
                    file: "int4/int4.sql".into(),
                    total: 1,
                    matched: 0,
                },
            ],
        };

        assert!(baseline.check_report(&report).is_ok());
    }

    #[test]
    fn regress_baseline_fails_one_file_regression() {
        let report = Report::new(vec![case("boolean/boolean.sql", false)]);
        let baseline = RegressBaseline {
            files: vec![RegressFileBaseline {
                file: "boolean/boolean.sql".into(),
                total: 1,
                matched: 1,
            }],
        };

        let err = baseline
            .check_report(&report)
            .expect_err("per-file regression must fail");
        assert!(err.contains("regress parity regression for boolean/boolean.sql"));
    }

    #[test]
    fn discovers_regress_sql_files_recursively() {
        let root = temp_corpus_dir();
        let regress_dir = root.join("boolean");
        std::fs::create_dir_all(&regress_dir).expect("create test corpus directory");
        std::fs::write(regress_dir.join("boolean.sql"), "SELECT true;")
            .expect("write test SQL file");
        std::fs::write(regress_dir.join("boolean.out"), "ignored").expect("write non-SQL file");

        let files = discover_sql_files(&root, true).expect("discover recursive corpus");
        assert_eq!(files, vec![regress_dir.join("boolean.sql")]);
        assert_eq!(
            corpus_file_name(&root, &regress_dir.join("boolean.sql")),
            "boolean/boolean.sql"
        );

        std::fs::remove_dir_all(root).expect("clean test corpus directory");
    }

    #[test]
    fn discovers_extended_case_files_recursively() {
        let root = temp_corpus_dir();
        let extended_dir = root.join("parameters");
        std::fs::create_dir_all(&extended_dir).expect("create extended corpus directory");
        std::fs::write(extended_dir.join("f0.json"), "[]").expect("write extended cases");
        std::fs::write(extended_dir.join("notes.sql"), "SELECT 1;").expect("write ignored file");

        let files = discover_extended_case_files(&root).expect("discover extended corpus");

        assert_eq!(files, vec![extended_dir.join("f0.json")]);
        assert_eq!(
            corpus_file_name(&root, &extended_dir.join("f0.json")),
            "parameters/f0.json"
        );

        std::fs::remove_dir_all(root).expect("clean extended corpus directory");
    }

    #[test]
    fn loads_extended_cases_and_reports_by_file() {
        let root = temp_corpus_dir();
        let case_file = root.join("parameters.json");
        std::fs::write(
            &case_file,
            r#"[
              {
                "name": "select_text_parameter",
                "sql": "SELECT $1::text",
                "params": [{ "type": "text", "value": "crab" }],
                "setup": [],
                "teardown": []
              }
            ]"#,
        )
        .expect("write extended case file");

        let files = load_extended_case_files(&root).expect("load extended case files");
        let report = Report::new(vec![CaseResult::new(
            files[0].file.clone(),
            files[0].cases[0].sql.clone(),
            DiffResult {
                matched: true,
                detail: String::new(),
            },
            &QueryOutcome::success(Vec::new()),
        )]);

        assert_eq!(files[0].file, "parameters.json");
        assert_eq!(files[0].cases[0].name, "select_text_parameter");
        assert_eq!(report.file_summaries()[0].file, "parameters.json");
        assert!(report.markdown_summary().contains("SELECT $1::text"));

        std::fs::remove_dir_all(root).expect("clean extended corpus directory");
    }

    #[test]
    fn loads_extended_cases_without_parsing_baseline_metadata() {
        let root = temp_corpus_dir();
        let extended_dir = root.join("parameters");
        std::fs::create_dir_all(&extended_dir).expect("create extended corpus directory");
        std::fs::write(
            extended_dir.join("f0.json"),
            r#"[
              {
                "name": "select_text_parameter",
                "sql": "SELECT $1::text",
                "params": [{ "type": "text", "value": "crab" }],
                "setup": [],
                "teardown": []
              }
            ]"#,
        )
        .expect("write extended case file");
        std::fs::write(
            root.join("baseline.json"),
            r#"{ "total": 6, "matched": 6 }"#,
        )
        .expect("write extended baseline");

        let files = load_extended_case_files(&root).expect("load extended case files");

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file, "parameters/f0.json");
        assert_eq!(files[0].cases[0].name, "select_text_parameter");

        std::fs::remove_dir_all(root).expect("clean extended corpus directory");
    }

    #[test]
    fn rejects_malformed_non_baseline_extended_case_file() {
        let root = temp_corpus_dir();
        let malformed_path = root.join("malformed.json");
        std::fs::write(&malformed_path, r#"{ "not": "cases" }"#)
            .expect("write malformed extended case file");

        let error = load_extended_case_files(&root).expect_err("reject malformed case file");
        let message = error.to_string();

        assert!(message.contains("expected a sequence"));
        assert!(message.contains(&malformed_path.display().to_string()));

        std::fs::remove_dir_all(root).expect("clean extended corpus directory");
    }

    #[test]
    fn extended_baseline_uses_standard_report_ratchet() {
        let report = report_with(6, 6);
        let baseline = Baseline {
            total: 6,
            matched: 5,
        };

        assert!(report.check_baseline(&baseline).is_ok());
    }

    #[test]
    fn materialized_extended_case_identifiers_are_unique_and_sql_safe() {
        let case = ExtendedCase {
            name: "unique".into(),
            sql: "SELECT * FROM probe___case_id__".into(),
            params: Vec::new(),
            setup: vec!["CREATE TABLE probe___case_id__ (id int4)".into()],
            teardown: vec!["DROP TABLE probe___case_id__".into()],
        };

        let first = materialize_case(&case);
        let second = materialize_case(&case);

        assert_ne!(first.sql, second.sql);
        assert!(!first.sql.contains(CASE_ID_TOKEN));
        assert!(
            first
                .sql
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || " *_".contains(character))
        );
        assert_eq!(first.name, case.name);
        assert_eq!(first.params.len(), case.params.len());
    }

    #[test]
    fn sharded_subject_transform_preserves_oracle_and_rewrites_only_create_table() {
        let oracle = "CREATE TABLE t (id int4, label text)";
        let subject = subject_sharded_statement(oracle).expect("subject transform");

        assert_eq!(oracle, "CREATE TABLE t (id int4, label text)");
        assert_eq!(subject, "CREATE TABLE t (id int4, label text) SHARDED");
        assert_eq!(
            subject_sharded_statement("CREATE INDEX i ON t (id)").expect("index unchanged"),
            "CREATE INDEX i ON t (id)"
        );
        assert_eq!(
            subject_sharded_statement("CREATE TABLESPACE ts LOCATION '/tmp/ts'")
                .expect("tablespace unchanged"),
            "CREATE TABLESPACE ts LOCATION '/tmp/ts'"
        );
    }

    #[test]
    fn sharded_subject_transform_handles_nested_defaults_constraints_and_quotes() {
        let cases = [
            "CREATE TABLE nested (id int4, amount numeric(10, 2), label text DEFAULT 'a)b')",
            "CREATE TABLE constrained (id int4 NOT NULL, label text, UNIQUE (id, label))",
            "CREATE TABLE \"Odd Table\" (\"Odd Column\" int4, note text DEFAULT '(x)')",
        ];
        for sql in cases {
            let transformed = subject_sharded_statement(sql).expect("valid CREATE TABLE");
            assert_eq!(transformed, format!("{sql} SHARDED"));
            let statements = crabka_pgparser::parse(&transformed).expect("rewritten SQL reparses");
            assert!(matches!(
                statements.as_slice(),
                [crabka_pgparser::ast::Statement::CreateTable { sharded: true, .. }]
            ));
        }
        assert_eq!(
            subject_sharded_statement("CREATE TABLE t (id int4) SHARDED").expect("already sharded"),
            "CREATE TABLE t (id int4) SHARDED"
        );
        assert_eq!(
            subject_sharded_statement("CREATE TABLE semi (id int4);  ")
                .expect("semicolon and whitespace are preserved"),
            "CREATE TABLE semi (id int4) SHARDED;  "
        );
        assert_eq!(
            subject_sharded_statement("/* setup */ CREATE TABLE commented (id int4)")
                .expect("leading comments do not hide table DDL"),
            "/* setup */ CREATE TABLE commented (id int4) SHARDED"
        );
        assert_eq!(
            subject_sharded_statement("CREATE VIEW \"table\" AS SELECT 1")
                .expect("non-table DDL remains unchanged"),
            "CREATE VIEW \"table\" AS SELECT 1"
        );
    }

    /// The two table-DDL shapes that have no `SHARDED BY` spelling at all pass
    /// through: both legs can only ever create an ordinary table, so rewriting is
    /// not merely unsupported but meaningless. Failing closed on these aborted the
    /// whole sharded run at the corpus's first `CREATE TABLE … AS`.
    #[test]
    fn sharded_subject_transform_passes_through_table_ddl_with_no_sharded_form() {
        for sql in [
            "CREATE TABLE copied AS SELECT 1",
            "CREATE TEMP TABLE tt (a int4) ON COMMIT PRESERVE ROWS",
            "CREATE TEMPORARY TABLE tt2 (a int4)",
        ] {
            assert_eq!(
                subject_sharded_statement(sql).expect("passes through"),
                sql,
                "{sql}"
            );
        }
    }

    #[test]
    fn sharded_subject_transform_fails_closed_for_malformed_create_table() {
        let error = subject_sharded_statement("CREATE TABLE broken (id int4")
            .expect_err("malformed table DDL must not pass through");
        assert!(error.to_string().contains("CREATE TABLE"));

        subject_sharded_statement("CREATE TABLE lexical (label text DEFAULT 'unterminated)")
            .expect_err("lexer-level malformed table DDL must not pass through unchanged");
        subject_sharded_statement(
            "CREATE/* gap */TABLE lexical_comment (label text DEFAULT 'unterminated)",
        )
        .expect_err("comments must not hide lexer-level malformed table DDL");
    }

    /// A `CREATE TABLE` the parser refuses *deliberately* is not a harness gap.
    /// `MATCH PARTIAL` is `0A000` here and in `PostgreSQL`, so neither leg
    /// creates a table and the corpus is asserting the refusal itself — the
    /// statement has to reach the subject unchanged to be refused there too.
    #[test]
    fn a_deliberate_feature_refusal_in_table_ddl_passes_through() {
        let sql = "CREATE TABLE fk_e8 (a int4, b int4, \
                   FOREIGN KEY (a, b) REFERENCES fk_comp_p(x, y) MATCH PARTIAL)";
        assert!(is_create_table_candidate(sql));
        let error = crabka_pgparser::parse(sql).expect_err("MATCH PARTIAL is refused");
        assert!(error.sqlstate() == "0A000");
        assert!(subject_sharded_statement(sql).expect("passes through") == sql);
    }

    #[test]
    fn sharded_extended_transform_changes_setup_only() {
        let case = ExtendedCase {
            name: "parameterized".into(),
            sql: "SELECT label FROM ext WHERE id = $1".into(),
            params: vec![ExtendedParam {
                ty: ExtendedParamType::Int4,
                value: Some(ExtendedParamValue::Int4(7)),
            }],
            setup: vec![
                "CREATE TABLE ext (id int4, label text)".into(),
                "INSERT INTO ext VALUES (7, 'seven')".into(),
            ],
            teardown: vec!["DROP TABLE ext".into()],
        };
        let transformed = subject_sharded_extended_case(&case).expect("extended transform");

        assert_eq!(transformed.name, case.name);
        assert_eq!(transformed.sql, case.sql);
        assert_eq!(transformed.params, case.params);
        assert_eq!(
            transformed.setup,
            vec![
                "CREATE TABLE ext (id int4, label text) SHARDED",
                "INSERT INTO ext VALUES (7, 'seven')",
            ]
        );
        assert_eq!(transformed.teardown, case.teardown);
    }

    #[test]
    fn typed_parameters_bind_jsonb_and_array_values() {
        let params: Vec<ExtendedParam> = serde_json::from_str(
            r#"[
              { "type": "jsonb", "value": "{\"a\": 1}" },
              { "type": "int4[]", "value": [1, null] },
              { "type": "text[]", "value": ["a", null] },
              { "type": "text[]", "value": [] },
              { "type": "int4[]", "value": null }
            ]"#,
        )
        .expect("typed parameters must deserialize");

        let owned = owned_params(&params).expect("typed parameters must bind");

        assert!(matches!(
            owned.as_slice(),
            [
                OwnedParam::Jsonb(Some(JsonbParam(document))),
                OwnedParam::Int4Array(Some(numbers)),
                OwnedParam::TextArray(Some(labels)),
                OwnedParam::TextArray(Some(empty)),
                OwnedParam::Int4Array(None),
            ] if document == "{\"a\": 1}"
                && numbers.as_slice() == [Some(1), None]
                && labels.as_slice() == [Some("a".to_string()), None]
                && empty.is_empty()
        ));
        assert!(param_refs(&owned).len() == 5);
        assert!(postgres_type(ExtendedParamType::Jsonb) == Type::JSONB);
        assert!(postgres_type(ExtendedParamType::Int4Array) == Type::INT4_ARRAY);
        assert!(postgres_type(ExtendedParamType::TextArray) == Type::TEXT_ARRAY);
    }

    #[test]
    fn mistyped_array_elements_fail_the_case_instead_of_binding() {
        let params: Vec<ExtendedParam> =
            serde_json::from_str(r#"[{ "type": "int4[]", "value": ["not a number"] }]"#)
                .expect("parameters must deserialize");

        let error = owned_params(&params).expect_err("mistyped element must not bind");

        assert!(error.contains("XXPARAM"));
    }

    #[test]
    fn jsonb_parameters_use_the_binary_version_byte() {
        let mut encoded = BytesMut::new();
        let null = JsonbParam("{\"a\": 1}".into())
            .to_sql(&Type::JSONB, &mut encoded)
            .expect("jsonb parameter must encode");

        assert!(matches!(null, IsNull::No));
        assert!(encoded.as_ref() == b"\x01{\"a\": 1}");
        assert!(<JsonbParam as ToSql>::accepts(&Type::JSONB));
        assert!(!<JsonbParam as ToSql>::accepts(&Type::TEXT));
    }

    #[test]
    fn copy_from_stdin_absorbs_its_inline_data_block() {
        let sql = "CREATE TABLE t (a int);\nCOPY t (a) FROM stdin;\n1\n2\n\\.\nSELECT * FROM t;";

        let statements = split_statements(sql);

        assert!(
            statements
                == vec![
                    CorpusStatement::plain("CREATE TABLE t (a int)"),
                    CorpusStatement {
                        sql: "COPY t (a) FROM stdin".into(),
                        stdin_data: Some("1\n2\n".into()),
                    },
                    CorpusStatement::plain("SELECT * FROM t"),
                ]
        );
    }

    #[test]
    fn copy_from_stdin_is_recognized_with_options_glued_to_the_keyword() {
        let sql = "copy t from stdin(on_error ignore);\nx\n\\.\nSELECT 1;";

        let statements = split_statements(sql);

        assert!(
            statements
                == vec![
                    CorpusStatement {
                        sql: "copy t from stdin(on_error ignore)".into(),
                        stdin_data: Some("x\n".into()),
                    },
                    CorpusStatement::plain("SELECT 1"),
                ]
        );
    }

    #[test]
    fn copy_to_stdout_takes_no_inline_data_block() {
        let statements = split_statements("COPY t TO STDOUT;\nSELECT 1;");

        assert!(
            statements
                == vec![
                    CorpusStatement::plain("COPY t TO STDOUT"),
                    CorpusStatement::plain("SELECT 1"),
                ]
        );
    }

    #[test]
    fn multibyte_text_survives_splitting() {
        let statements = split_statements("SELECT 'héllo — ✓';");

        assert!(statements == vec![CorpusStatement::plain("SELECT 'héllo — ✓'")]);
    }

    /// One mismatching case whose subject failed with `code`/`message`.
    fn failing_case(sql: &str, code: &str, message: &str) -> CaseResult {
        CaseResult::new(
            "t.sql".into(),
            sql.into(),
            DiffResult {
                matched: false,
                detail: "error code".into(),
            },
            &QueryOutcome::failure_with_message(code.into(), message.into()),
        )
    }

    #[test]
    fn root_causes_rank_shared_failure_shapes_first() {
        let report = Report::new(vec![
            failing_case(
                "SELECT row_number() OVER ()",
                "42601",
                "syntax error at position 25: expected ; or end of input, found LParen",
            ),
            failing_case(
                "SELECT sum(1) OVER ()",
                "42601",
                "syntax error at position 19: expected ; or end of input, found LParen",
            ),
            failing_case(
                "SELECT 1::int2",
                "42601",
                "syntax error: unknown type \"int2\"",
            ),
            case("t.sql", true),
        ]);

        let ranked = report.root_causes();

        assert!(
            ranked
                == vec![
                    RootCause {
                        signature:
                            "syntax error at position N: expected ; or end of input, found LParen"
                                .into(),
                        sqlstate: Some("42601".into()),
                        count: 2,
                        example: "SELECT row_number() OVER ()".into(),
                    },
                    RootCause {
                        signature: "syntax error: unknown type \"intN\"".into(),
                        sqlstate: Some("42601".into()),
                        count: 1,
                        example: "SELECT 1::int2".into(),
                    },
                ]
        );
    }

    #[test]
    fn root_causes_separate_wrong_rows_from_errors() {
        let wrong_rows = CaseResult::new(
            "t.sql".into(),
            "SELECT 1".into(),
            DiffResult {
                matched: false,
                detail: "rows".into(),
            },
            &QueryOutcome::success(vec![vec![Some("2".into())]]),
        );

        let ranked = Report::new(vec![wrong_rows]).root_causes();

        assert!(ranked.len() == 1);
        assert!(ranked[0].sqlstate.is_none());
        assert!(ranked[0].signature == "wrong rows (subject executed the statement)");
    }

    /// Report with the given counts and no per-case detail.
    fn report_with(total: usize, matched: usize) -> Report {
        Report {
            total,
            matched,
            parity_percent: parity_percent(matched, total),
            cases: Vec::new(),
        }
    }

    fn case(file: &str, matched: bool) -> CaseResult {
        CaseResult::new(
            file.into(),
            "SELECT 1".into(),
            DiffResult {
                matched,
                detail: String::new(),
            },
            &QueryOutcome::success(Vec::new()),
        )
    }

    fn temp_corpus_dir() -> PathBuf {
        tempfile::Builder::new()
            .prefix("crabka-gres-conformance-")
            .tempdir()
            .expect("create temp corpus directory")
            .keep()
    }
}
