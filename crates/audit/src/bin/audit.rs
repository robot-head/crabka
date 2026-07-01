//! `crabka-audit` — offline audit-log tools.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use crabka_audit::{TrustedKeys, verify_partition_dir};

#[derive(Parser)]
#[command(name = "crabka-audit", about = "Crabka audit-log tools (FedRAMP MLA)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Verify a partition's hash-chain and signed checkpoints offline.
    Verify {
        /// Path to the audit partition directory, e.g. `<log_dir>/__crabka_audit-0`
        #[arg(long)]
        partition_dir: PathBuf,
        /// `key_id` the trusted public key corresponds to.
        #[arg(long)]
        key_id: String,
        /// Path to the trusted Ed25519 public key (raw 32 bytes).
        #[arg(long)]
        public_key: PathBuf,
    },
}

#[tracing::instrument(
    level = "info",
    skip_all,
    fields(command = tracing::field::Empty, partition_dir = tracing::field::Empty, key_id = tracing::field::Empty)
)]
fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Verify {
            partition_dir,
            key_id,
            public_key,
        } => {
            let span = tracing::Span::current();
            span.record("command", "verify");
            span.record(
                "partition_dir",
                tracing::field::display(partition_dir.display()),
            );
            span.record("key_id", tracing::field::display(&key_id));
            let pubkey = match std::fs::read(&public_key) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("error: read public key {}: {e}", public_key.display());
                    return ExitCode::FAILURE;
                }
            };
            let trusted = TrustedKeys::single(key_id, pubkey);
            match verify_partition_dir(&partition_dir, &trusted) {
                Ok(report) if !report.ok => {
                    let b = report.first_break.expect("not ok implies a break");
                    eprintln!(
                        "TAMPER DETECTED at offset {} (seq {:?}): {}",
                        b.offset, b.seq, b.reason
                    );
                    eprintln!(
                        "verified {} records, {} checkpoints before the break",
                        report.records, report.checkpoints
                    );
                    ExitCode::FAILURE
                }
                Ok(report) if report.records == 0 => {
                    println!("OK: empty partition");
                    ExitCode::SUCCESS
                }
                Ok(report) if report.checkpoints == 0 || report.unanchored_records > 0 => {
                    eprintln!(
                        "INCOMPLETE ATTESTATION: chain continuous over {} records, but {} \
                        record(s) are not covered by a signed checkpoint ({} checkpoint(s) \
                        present). Integrity is not cryptographically attested for the unsigned \
                        portion.",
                        report.records, report.unanchored_records, report.checkpoints
                    );
                    ExitCode::FAILURE
                }
                Ok(report) => {
                    println!(
                        "OK: {} records, {} checkpoints, chain continuous, all signatures valid, \
                        fully attested",
                        report.records, report.checkpoints
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}
