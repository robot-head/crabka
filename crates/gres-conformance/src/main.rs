use std::path::Path;

use clap::Parser;
use crabka_gres_conformance::{
    Baseline, CaseResult, RegressBaseline, Report, corpus_file_name, diff, discover_sql_files,
    load_extended_case_files, run_extended_one, run_one, split_statements,
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let (mut oracle, oracle_conn) = tokio_postgres::connect(&args.oracle_url, NoTls).await?;
    tokio::spawn(oracle_conn);
    let (mut subject, subject_conn) = tokio_postgres::connect(&args.subject_url, NoTls).await?;
    tokio::spawn(subject_conn);

    let report = run_corpus(&oracle, &subject, &args.corpus, false).await?;
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
        let regress_report = run_corpus(&oracle, &subject, corpus_regress, true).await?;
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
        let extended_report =
            run_extended_corpus(&mut oracle, &mut subject, extended_corpus).await?;
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

async fn run_corpus(
    oracle: &tokio_postgres::Client,
    subject: &tokio_postgres::Client,
    corpus: &Path,
    recursive: bool,
) -> Result<Report, Box<dyn std::error::Error>> {
    let mut cases = Vec::new();
    for path in discover_sql_files(corpus, recursive)? {
        let name = corpus_file_name(corpus, &path);
        let sql = std::fs::read_to_string(&path)?;
        for stmt in split_statements(&sql) {
            let o = run_one(oracle, &stmt).await;
            let s = run_one(subject, &stmt).await;
            let d = diff(&o, &s);
            cases.push(CaseResult {
                file: name.clone(),
                sql: stmt,
                matched: d.matched,
                detail: d.detail,
            });
        }
    }
    Ok(Report::new(cases))
}

async fn run_extended_corpus(
    oracle: &mut tokio_postgres::Client,
    subject: &mut tokio_postgres::Client,
    corpus: &Path,
) -> Result<Report, Box<dyn std::error::Error>> {
    let mut cases = Vec::new();
    for case_file in load_extended_case_files(corpus)? {
        for extended_case in case_file.cases {
            let o = run_extended_one(oracle, &extended_case).await;
            let s = run_extended_one(subject, &extended_case).await;
            let d = diff(&o, &s);
            cases.push(CaseResult {
                file: case_file.file.clone(),
                sql: format!("{}: {}", extended_case.name, extended_case.sql),
                matched: d.matched,
                detail: d.detail,
            });
        }
    }
    Ok(Report::new(cases))
}
