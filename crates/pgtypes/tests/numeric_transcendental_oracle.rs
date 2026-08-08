//! SP34: validate the numeric transcendental functions against a live
//! PostgreSQL oracle. These tests are `#[ignore]` by default because they need
//! a running PostgreSQL. Run them locally to confirm and finalize the
//! display-scale (rscale) rules:
//!
//!   $env:PGPASSWORD="postgres"
//!   cargo nextest run -p pgtypes --test numeric_transcendental_oracle --run-ignored all
//!
//! The oracle is PostgreSQL 17.10 at localhost:5432 (user `postgres`, db
//! `postgres`). The diff is on `numeric_out` text: value AND display scale.

#![expect(
    clippy::doc_markdown,
    reason = "vendored local-only oracle notes intentionally preserve donor wording"
)]

use std::process::Command;

const PSQL: &str = r"C:\Program Files\PostgreSQL\17\bin\psql.exe";

/// Run a SQL scalar through PostgreSQL and return its trimmed text.
///
/// The text is empty on a domain error. Those rows are skipped, and the unit
/// tests cover the error surface.
fn pg(sql: &str) -> String {
    let out = Command::new(PSQL)
        .args([
            "-U",
            "postgres",
            "-h",
            "localhost",
            "-d",
            "postgres",
            "-t",
            "-A",
            "-c",
            sql,
        ])
        .output()
        .expect("run psql (is PostgreSQL 17 installed + running?)");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Our engine's `numeric_out` text for a unary transcendental.
fn ours_unary(f: &str, arg: &str) -> Option<String> {
    use crabka_pgtypes::numeric::{num_exp, num_ln, num_log10, num_sqrt, parse, to_text};
    let a = parse(arg).expect("parse arg");
    let bd = match f {
        "sqrt" => num_sqrt(&a).ok()?,
        "ln" => num_ln(&a).ok()?,
        "log" => num_log10(&a).ok()?,
        "exp" => num_exp(&a).ok()?,
        _ => unreachable!(),
    };
    Some(to_text(&bd))
}

/// Our engine's `numeric_out` text for `power(base, exp)`.
fn ours_power(base: &str, exp: &str) -> Option<String> {
    use crabka_pgtypes::numeric::{num_power, parse, to_text};
    let b = parse(base).expect("parse base");
    let e = parse(exp).expect("parse exp");
    Some(to_text(&num_power(&b, &e).ok()?))
}

#[test]
#[ignore = "requires a local PostgreSQL psql oracle"]
fn unary_transcendentals_match_oracle_battery() {
    // A broad magnitude/sign/scale battery (avoids the near-1 region's separate
    // PG branch for log/ln, which is validated narrowly in the unit tests).
    let args = [
        "2",
        "3",
        "4",
        "5",
        "7",
        "10",
        "16",
        "50",
        "99",
        "200",
        "1000",
        "999999",
        "1000000",
        "1000000000000",
        "0.5",
        "0.25",
        "0.04",
        "0.01",
        "0.0001",
        "0.000001",
        "1.5",
        "2.5",
        "3.14",
        "123.456",
        "2.00",
        "10.0",
    ];
    let mut mismatches = Vec::new();
    for f in ["sqrt", "ln", "log", "exp"] {
        for a in args {
            let want = pg(&format!("SELECT {f}({a}::numeric)::text"));
            if want.is_empty() {
                continue; // domain-invalid for this f (e.g. ln of a value PG rejects)
            }
            match ours_unary(f, a) {
                Some(got) if got == want => {}
                Some(got) => mismatches.push(format!("{f}({a}): pg={want} ours={got}")),
                None => mismatches.push(format!("{f}({a}): pg={want} ours=<domain-error>")),
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} oracle mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
#[ignore = "requires a local PostgreSQL psql oracle"]
fn power_matches_oracle_battery() {
    // (base, exp) pairs across integer/non-integer exponents, signs, magnitudes.
    let cases = [
        ("2", "10"),
        ("2", "3"),
        ("3", "4"),
        ("5", "2"),
        ("10", "5"),
        ("2", "100"),
        ("-2", "3"),
        ("-3", "2"),
        ("5", "-2"),
        ("10", "-3"),
        ("0.5", "3"),
        ("2.5", "2"),
        ("100", "3"),
        ("2", "0.5"),
        ("9", "0.5"),
        ("10", "0.5"),
        ("2", "0.1"),
        ("10", "3.5"),
        ("1.5", "2.5"),
        ("100", "0.25"),
        ("1000", "0.5"),
        ("2", "0"),
        ("7", "1"),
    ];
    let mut mismatches = Vec::new();
    for (b, e) in cases {
        let want = pg(&format!("SELECT power({b}::numeric, {e}::numeric)::text"));
        if want.is_empty() {
            continue;
        }
        match ours_power(b, e) {
            Some(got) if got == want => {}
            Some(got) => mismatches.push(format!("power({b},{e}): pg={want} ours={got}")),
            None => mismatches.push(format!("power({b},{e}): pg={want} ours=<domain-error>")),
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} power oracle mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
