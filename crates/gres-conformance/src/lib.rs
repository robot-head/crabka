//! Differential conformance harness: run the same SQL against real `PostgreSQL`
//! (the oracle) and Crabka Gres (the subject), diff the outcomes.

mod parser_commands;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_postgres::types::{ToSql, Type};

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
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtendedCase {
    pub name: String,
    pub sql: String,
    pub params: Vec<ExtendedParam>,
    pub setup: Vec<String>,
    pub teardown: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
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
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum ExtendedParamValue {
    Bool(bool),
    Int4(i32),
    Text(String),
}

#[derive(Debug, Clone)]
pub struct ExtendedCaseFile {
    pub file: String,
    pub cases: Vec<ExtendedCase>,
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

    #[must_use]
    pub fn markdown_summary(&self) -> String {
        let mut md = format!(
            "# crabka-gres conformance report\n\n**Parity: {:.1}%** ({} / {} statements match the oracle)\n\n",
            self.parity_percent, self.matched, self.total
        );
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

pub fn discover_sql_files(corpus: &Path, recursive: bool) -> Result<Vec<PathBuf>, io::Error> {
    let mut files = Vec::new();
    collect_sql_files(corpus, recursive, &mut files)?;
    files.sort();
    Ok(files)
}

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
            QueryOutcome {
                rows,
                error_code: None,
            }
        }
        Err(e) => QueryOutcome {
            rows: vec![],
            error_code: Some(
                e.as_db_error()
                    .map_or_else(|| "XXIO".to_string(), |db| db.code().code().to_string()),
            ),
        },
    }
}

pub async fn run_extended_one(
    client: &tokio_postgres::Client,
    case: &ExtendedCase,
) -> QueryOutcome {
    if let Err(error_code) = execute_case_statements(client, &case.setup).await {
        return QueryOutcome {
            rows: Vec::new(),
            error_code: Some(error_code),
        };
    }

    let outcome = query_extended(client, case).await;
    if outcome.error_code.is_none()
        && let Err(error_code) = execute_case_statements(client, &case.teardown).await
    {
        return QueryOutcome {
            rows: Vec::new(),
            error_code: Some(error_code),
        };
    }
    outcome
}

async fn query_extended(client: &tokio_postgres::Client, case: &ExtendedCase) -> QueryOutcome {
    let params = match owned_params(&case.params) {
        Ok(params) => params,
        Err(error) => {
            return QueryOutcome {
                rows: Vec::new(),
                error_code: Some(error),
            };
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
    client: &tokio_postgres::Client,
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
        (ty, value) => Err(format!(
            "XXPARAM: {ty:?} parameter has incompatible value {value:?}"
        )),
    }
}

fn param_refs(params: &[OwnedParam]) -> Vec<&(dyn ToSql + Sync)> {
    params
        .iter()
        .map(|param| match param {
            OwnedParam::Bool(value) => value as &(dyn ToSql + Sync),
            OwnedParam::Int4(value) => value as &(dyn ToSql + Sync),
            OwnedParam::Text(value) => value as &(dyn ToSql + Sync),
        })
        .collect()
}

fn postgres_type(ty: ExtendedParamType) -> Type {
    match ty {
        ExtendedParamType::Bool => Type::BOOL,
        ExtendedParamType::Int4 => Type::INT4,
        ExtendedParamType::Text => Type::TEXT,
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
                    return QueryOutcome {
                        rows: Vec::new(),
                        error_code: Some(error_code),
                    };
                }
            }
        }
        normalized_rows.push(normalized_values);
    }
    QueryOutcome {
        rows: normalized_rows,
        error_code: None,
    }
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
    QueryOutcome {
        rows: Vec::new(),
        error_code: Some(error_code_from_error(error)),
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
#[must_use]
pub fn split_statements(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;

    while i < bytes.len() {
        let c = bytes[i];
        // Line comment (outside strings).
        if !in_single && !in_double && c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // psql meta-command lines in pg_regress files are harness controls, not
        // SQL. Skip them so the following SQL statement is still exercised.
        if !in_single && !in_double && c == b'\\' && current.trim().is_empty() {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Dollar-quoted string (outside other strings).
        if !in_single
            && !in_double
            && c == b'$'
            && let Some(tag_len) = dollar_tag_len(&bytes[i..])
        {
            let tag = &sql[i..i + tag_len];
            current.push_str(tag);
            i += tag_len;
            // Consume until the matching closing tag.
            loop {
                if i >= bytes.len() {
                    break; // unterminated; emit what we have
                }
                if sql[i..].starts_with(tag) {
                    current.push_str(tag);
                    i += tag_len;
                    break;
                }
                current.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        match c {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            b';' if !in_single && !in_double => {
                let stmt = current.trim().to_string();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                current.clear();
                i += 1;
                continue;
            }
            _ => {}
        }
        current.push(c as char);
        i += 1;
    }
    let stmt = current.trim().to_string();
    if !stmt.is_empty() {
        statements.push(stmt);
    }
    statements
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
        assert_eq!(
            split_statements(sql),
            vec!["SELECT 1", "SELECT 'a;b'", "SELECT 2"]
        );
    }

    #[test]
    fn identical_outcomes_match() {
        let a = QueryOutcome {
            rows: vec![vec![Some("1".into())]],
            error_code: None,
        };
        assert!(diff(&a, &a.clone()).matched);
    }

    #[test]
    fn differing_rows_mismatch_with_detail() {
        let oracle = QueryOutcome {
            rows: vec![vec![Some("1".into())]],
            error_code: None,
        };
        let subject = QueryOutcome {
            rows: vec![vec![Some("2".into())]],
            error_code: None,
        };
        let d = diff(&oracle, &subject);
        assert!(!d.matched);
        assert!(d.detail.contains("rows"));
    }

    #[test]
    fn matching_error_codes_match() {
        // Same SQLSTATE on both sides counts as parity (e.g. both reject).
        let a = QueryOutcome {
            rows: vec![],
            error_code: Some("42601".into()),
        };
        assert!(diff(&a, &a.clone()).matched);
        let b = QueryOutcome {
            rows: vec![],
            error_code: Some("0A000".into()),
        };
        assert!(!diff(&a, &b).matched);
    }

    #[test]
    fn doubled_quotes_keep_semicolons_protected() {
        // SQL escapes a quote by doubling it; the toggle approach keeps the
        // in-string state net-unchanged across '' so the ; stays protected.
        let sql = "SELECT 'it''s;bad';SELECT 2";
        assert_eq!(
            split_statements(sql),
            vec!["SELECT 'it''s;bad'", "SELECT 2"]
        );
    }

    #[test]
    fn dollar_quoted_body_is_not_split_on_inner_semicolons() {
        let sql = "SELECT 1;\nDO $$ BEGIN x; y; END $$;\nSELECT 2";
        assert_eq!(
            split_statements(sql),
            vec!["SELECT 1", "DO $$ BEGIN x; y; END $$", "SELECT 2"]
        );
    }

    #[test]
    fn tagged_dollar_quote_is_matched_by_tag() {
        let sql = "SELECT $tag$a;b$tag$ ; SELECT 2";
        assert_eq!(
            split_statements(sql),
            vec!["SELECT $tag$a;b$tag$", "SELECT 2"]
        );
    }

    #[test]
    fn psql_meta_commands_are_not_sql_statements() {
        let sql = "SELECT true;\n\\pset null '(null)'\nSELECT NULL;";
        assert_eq!(split_statements(sql), vec!["SELECT true", "SELECT NULL"]);
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
        let report = Report::new(vec![CaseResult {
            file: files[0].file.clone(),
            sql: files[0].cases[0].sql.clone(),
            matched: true,
            detail: String::new(),
        }]);

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
        CaseResult {
            file: file.into(),
            sql: "SELECT 1".into(),
            matched,
            detail: String::new(),
        }
    }

    fn temp_corpus_dir() -> PathBuf {
        tempfile::Builder::new()
            .prefix("crabka-gres-conformance-")
            .tempdir()
            .expect("create temp corpus directory")
            .keep()
    }
}
