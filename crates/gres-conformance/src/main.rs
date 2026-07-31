use std::path::Path;

use clap::Parser;
use crabka_gres_conformance::{
    Baseline, CaseResult, QueryOutcome, RegressBaseline, Report, corpus_file_name, diff,
    discover_sql_files, load_extended_case_files, run_corpus_statement, run_extended_one,
    split_statements, subject_sharded_extended_case, subject_sharded_statement, tls,
};
use tokio_postgres::NoTls;

/// Differential conformance runner: oracle (real `PostgreSQL`) vs subject (Crabka Gres).
#[derive(Parser)]
struct Args {
    /// e.g. "host=127.0.0.1 port=54320 user=postgres dbname=postgres"
    #[arg(long)]
    oracle_url: String,
    /// e.g. "host=127.0.0.1 port=5433 user=crab dbname=crab"
    #[arg(long)]
    subject_url: String,
    /// Rewrite only subject-side CREATE TABLE/setup statements as SHARDED.
    #[arg(long)]
    subject_sharded_ddl: bool,
    /// Directory of .sql corpus files.
    #[arg(long, default_value = "crates/gres-conformance/corpus")]
    corpus: std::path::PathBuf,
    /// Optional adopted `PostgreSQL` `pg_regress` corpus directory.
    #[arg(long)]
    corpus_regress: Option<std::path::PathBuf>,
    #[arg(long, default_value = "parity.json")]
    out: std::path::PathBuf,
    #[arg(long, default_value = "parity.md")]
    summary: std::path::PathBuf,
    /// Optional parity baseline; when set, exit nonzero on any regression.
    #[arg(long)]
    baseline: Option<std::path::PathBuf>,
    /// Optional per-file baseline for --corpus-regress.
    #[arg(long)]
    regress_baseline: Option<std::path::PathBuf>,
    /// Optional JSON corpus for extended-protocol parameterized cases.
    #[arg(long)]
    extended_corpus: Option<std::path::PathBuf>,
    /// Optional parity baseline for --extended-corpus.
    #[arg(long)]
    extended_baseline: Option<std::path::PathBuf>,
    /// JSON report path for --extended-corpus.
    #[arg(long, default_value = "extended-parity.json")]
    extended_out: std::path::PathBuf,
    /// Markdown report path for --extended-corpus.
    #[arg(long, default_value = "extended-parity.md")]
    extended_summary: std::path::PathBuf,
    /// JSON report path for --corpus-regress.
    #[arg(long, default_value = "regress-parity.json")]
    regress_out: std::path::PathBuf,
    /// Markdown report path for --corpus-regress.
    #[arg(long, default_value = "regress-parity.md")]
    regress_summary: std::path::PathBuf,
    /// Wall-clock cap for one statement on one side of the diff, in seconds.
    ///
    /// A statement that does not answer in time is recorded as a mismatch and
    /// its connection is restored, so one wedged or pathologically slow
    /// statement costs a statement rather than the run. Lower it to sweep a
    /// large corpus quickly; raise it when a slow statement is expected.
    #[arg(long, default_value_t = 15)]
    statement_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let statement_timeout = std::time::Duration::from_secs(args.statement_timeout_secs);
    let mut oracle = Endpoint::connect(
        args.oracle_url.clone(),
        EndpointKind::Oracle,
        statement_timeout,
    )
    .await?;
    let mut subject = Endpoint::connect(
        args.subject_url.clone(),
        EndpointKind::Subject,
        statement_timeout,
    )
    .await?;

    let report = run_primary_corpus(&mut oracle, &mut subject, &args).await?;
    std::fs::write(&args.out, serde_json::to_string_pretty(&report)?)?;
    std::fs::write(&args.summary, report.markdown_summary())?;
    println!(
        "parity: {:.1}% ({} / {}) -> {} / {}",
        report.parity_percent,
        report.matched,
        report.total,
        args.out.display(),
        args.summary.display()
    );
    if let Some(path) = &args.baseline {
        let text = std::fs::read_to_string(path)?;
        let baseline: Baseline = serde_json::from_str(&text)?;
        match report.check_baseline(&baseline) {
            Ok(()) => println!(
                "baseline gate passed: {}/{} matched (floor {})",
                report.matched, report.total, baseline.matched
            ),
            Err(msg) => {
                eprintln!("baseline gate FAILED: {msg}");
                std::process::exit(1);
            }
        }
    }
    if let Some(corpus_regress) = &args.corpus_regress {
        let regress_report = run_corpus(
            &mut oracle,
            &mut subject,
            corpus_regress,
            true,
            args.subject_sharded_ddl,
        )
        .await?;
        std::fs::write(
            &args.regress_out,
            serde_json::to_string_pretty(&regress_report)?,
        )?;
        std::fs::write(&args.regress_summary, regress_report.markdown_summary())?;
        println!(
            "pg_regress parity: {:.1}% ({} / {}) -> {} / {}",
            regress_report.parity_percent,
            regress_report.matched,
            regress_report.total,
            args.regress_out.display(),
            args.regress_summary.display()
        );
        if let Some(path) = &args.regress_baseline {
            let text = std::fs::read_to_string(path)?;
            let baseline: RegressBaseline = serde_json::from_str(&text)?;
            match baseline.check_report(&regress_report) {
                Ok(()) => println!(
                    "pg_regress baseline gate passed: {}/{} matched",
                    regress_report.matched, regress_report.total
                ),
                Err(msg) => {
                    eprintln!("pg_regress baseline gate FAILED: {msg}");
                    std::process::exit(1);
                }
            }
        }
    }
    if let Some(extended_corpus) = &args.extended_corpus {
        let extended_report = run_extended_corpus(
            &mut oracle.client,
            &mut subject.client,
            extended_corpus,
            args.subject_sharded_ddl,
        )
        .await?;
        std::fs::write(
            &args.extended_out,
            serde_json::to_string_pretty(&extended_report)?,
        )?;
        std::fs::write(&args.extended_summary, extended_report.markdown_summary())?;
        println!(
            "extended parity: {:.1}% ({} / {}) -> {} / {}",
            extended_report.parity_percent,
            extended_report.matched,
            extended_report.total,
            args.extended_out.display(),
            args.extended_summary.display()
        );
        if let Some(path) = &args.extended_baseline {
            let text = std::fs::read_to_string(path)?;
            let baseline: Baseline = serde_json::from_str(&text)?;
            match extended_report.check_baseline(&baseline) {
                Ok(()) => println!(
                    "extended baseline gate passed: {}/{} matched (floor {})",
                    extended_report.matched, extended_report.total, baseline.matched
                ),
                Err(msg) => {
                    eprintln!("extended baseline gate FAILED: {msg}");
                    std::process::exit(1);
                }
            }
        }
    }
    Ok(())
}

async fn run_primary_corpus(
    oracle: &mut Endpoint,
    subject: &mut Endpoint,
    args: &Args,
) -> Result<Report, Box<dyn std::error::Error>> {
    run_corpus(
        oracle,
        subject,
        &args.corpus,
        false,
        args.subject_sharded_ddl,
    )
    .await
}

/// One side of the differential run, able to restore its own connection.
///
/// A statement can leave a connection unusable — a rejected `COPY` desynchronizes
/// the extended-protocol exchange, for instance. Without recovery the whole
/// remainder of the run scores against two dead sockets, which compare equal and
/// register as matches, so a broken connection silently *inflates* parity.
struct Endpoint {
    client: tokio_postgres::Client,
    url: String,
    kind: EndpointKind,
    statement_timeout: std::time::Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointKind {
    /// The real `PostgreSQL` oracle, reached without TLS.
    Oracle,
    /// The Crabka Gres subject, reached through the harness TLS connector.
    Subject,
}

impl Endpoint {
    async fn connect(
        url: String,
        kind: EndpointKind,
        statement_timeout: std::time::Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let client = Self::dial(&url, kind).await?;
        Ok(Self {
            client,
            url,
            kind,
            statement_timeout,
        })
    }

    async fn dial(
        url: &str,
        kind: EndpointKind,
    ) -> Result<tokio_postgres::Client, Box<dyn std::error::Error>> {
        match kind {
            EndpointKind::Oracle => {
                let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
                tokio::spawn(connection);
                Ok(client)
            }
            EndpointKind::Subject => tls::connect(url)
                .await
                .map_err(|error| error.to_string().into()),
        }
    }

    /// Return the session to a pristine state between corpus files.
    ///
    /// `ROLLBACK` closes any block the file left open (`DISCARD ALL` is refused
    /// inside one), then `DISCARD ALL` does what a fresh connection would:
    /// `RESET ALL`, drop temp tables, deallocate prepared statements, close
    /// cursors. Reconnecting would do the same, but a hundred corpus files times
    /// two endpoints exhausts the oracle's `max_connections` mid-run — so a
    /// reconnect is only the fallback for a session that cannot be reset.
    async fn reset_session(&mut self) {
        for sql in ["ROLLBACK", "DISCARD ALL"] {
            let outcome = self
                .run_once(&crabka_gres_conformance::CorpusStatement::plain(sql))
                .await;
            // A failed ROLLBACK just means there was no open block. A failed
            // DISCARD means the session is not usable, so replace it.
            if sql == "DISCARD ALL" && outcome.error_code.is_some() {
                let _ = self.reconnect().await;
            }
        }
    }

    async fn reconnect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.client = Self::dial(&self.url, self.kind).await?;
        Ok(())
    }

    /// Run one statement, restoring a dead connection and retrying once.
    ///
    /// `XXIO` is the harness's marker for "no SQLSTATE — the connection failed",
    /// which means the statement never reached the server; re-running it after a
    /// reconnect measures it rather than losing it.
    async fn run(&mut self, statement: &crabka_gres_conformance::CorpusStatement) -> QueryOutcome {
        let outcome = self.run_once(statement).await;
        match outcome.error_code.as_deref() {
            // The connection failed: the statement never ran, so re-running it
            // after a reconnect measures it rather than losing it.
            Some("XXIO") => match self.reconnect().await {
                Ok(()) => self.run_once(statement).await,
                Err(_) => outcome,
            },
            // The statement did not answer in time. Retrying would just spend the
            // same wall clock again, but the connection is presumed wedged, so
            // restore it — otherwise every remaining statement in the run pays
            // the timeout too.
            Some("XXTO") => {
                let _ = self.reconnect().await;
                outcome
            }
            _ => outcome,
        }
    }

    /// Execute one statement under a wall-clock cap.
    ///
    /// An engine that never answers must not be able to hang the whole run: a
    /// timeout is reported as a distinct outcome so it shows up as a ranked root
    /// cause instead of an empty report.
    async fn run_once(&self, statement: &crabka_gres_conformance::CorpusStatement) -> QueryOutcome {
        let execution = run_corpus_statement(&self.client, statement);
        match tokio::time::timeout(self.statement_timeout, execution).await {
            Ok(outcome) => outcome,
            Err(_) => QueryOutcome::failure_with_message(
                "XXTO".into(),
                format!(
                    "statement did not answer within {}s",
                    self.statement_timeout.as_secs()
                ),
            ),
        }
    }
}

async fn run_corpus(
    oracle: &mut Endpoint,
    subject: &mut Endpoint,
    corpus: &Path,
    recursive: bool,
    subject_sharded_ddl: bool,
) -> Result<Report, Box<dyn std::error::Error>> {
    let mut cases = Vec::new();
    for path in discover_sql_files(corpus, recursive)? {
        let name = corpus_file_name(corpus, &path);
        let sql = std::fs::read_to_string(&path)?;
        let started = std::time::Instant::now();
        let before = cases.len();
        for statement in split_statements(&sql) {
            let o = oracle.run(&statement).await;
            let subject_statement = if subject_sharded_ddl {
                crabka_gres_conformance::CorpusStatement {
                    sql: subject_sharded_statement(&statement.sql)?,
                    stdin_data: statement.stdin_data.clone(),
                }
            } else {
                statement.clone()
            };
            let s = subject.run(&subject_statement).await;
            let d = diff(&o, &s);
            cases.push(CaseResult::new(name.clone(), statement.sql, d, &s));
        }
        // A 15k-statement run is long enough that silence is indistinguishable
        // from a wedge; per-file progress makes a slow file findable.
        eprintln!(
            "  {name}: {} statements in {:.1}s",
            cases.len() - before,
            started.elapsed().as_secs_f64()
        );
        // A pristine session per file on BOTH sides, because that is what
        // pg_regress does — it runs each file through its own psql. Holding one
        // session for the whole run lets a file's state leak into every later
        // file, and asymmetrically: `create_index.sql` ends with `SET
        // search_path = 'schema_to_reindex'` and then drops that schema without
        // resetting, so on the oracle every later unqualified CREATE was 3F000
        // while the subject, which did not implement search_path when this was
        // found, accepted it. That one leak accounted for 75 false mismatches —
        // and now that the subject does implement it, the leak would make both
        // sides wrong together instead, which is worse.
        oracle.reset_session().await;
        subject.reset_session().await;
    }
    Ok(Report::new(cases))
}

async fn run_extended_corpus(
    oracle: &mut tokio_postgres::Client,
    subject: &mut tokio_postgres::Client,
    corpus: &Path,
    subject_sharded_ddl: bool,
) -> Result<Report, Box<dyn std::error::Error>> {
    let mut cases = Vec::new();
    for case_file in load_extended_case_files(corpus)? {
        for extended_case in case_file.cases {
            let o = run_extended_one(oracle, &extended_case).await;
            let subject_case = if subject_sharded_ddl {
                subject_sharded_extended_case(&extended_case)?
            } else {
                extended_case.clone()
            };
            let s = run_extended_one(subject, &subject_case).await;
            let d = diff(&o, &s);
            cases.push(CaseResult::new(
                case_file.file.clone(),
                format!("{}: {}", extended_case.name, extended_case.sql),
                d,
                &s,
            ));
        }
    }
    Ok(Report::new(cases))
}
