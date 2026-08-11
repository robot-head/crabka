//! `oidvector`'s element reader is `oidin`, not `int4in`.
//!
//! Crabka read the elements of an `oidvector` with the `integer` parser, which
//! is wrong in four separate ways and only one of them is cosmetic.
//!
//! * **The base.** `oidin` is `strtoul(s, &endptr, 0)` — base *zero*, so a
//!   leading `0` is octal and `0x`/`0b` are prefixes. `int4in` is
//!   `pg_strtoint32`, which is base ten unless a prefix says otherwise and
//!   which also accepts `_` digit separators that `strtoul` does not.
//! * **The range.** An `oid` is unsigned. `'-1'` and `'4294967295'` are the same
//!   legal value; the `integer` parser took the first and rejected the second.
//! * **Where an element ends.** `oidvectorin` resumes the scan wherever
//!   `strtoul` stopped, so `'01XYZ'` is the octal `01` followed by a *separate*
//!   unconvertible `XYZ`. Splitting on whitespace makes it one token.
//! * **The name in the error**, which is `oid` — never `integer`, and never
//!   `oidvector`.
//!
//! The last two are what `pg_input_error_info('01 01XYZ', 'oidvector')` sees:
//! `PostgreSQL` says `invalid input syntax for type oid: "XYZ"` where crabka
//! said `invalid input syntax for type integer: "01XYZ"` — a different type
//! *and* a different token, from one substitution.
//!
//! Every expectation here is `PostgreSQL` 18.4's, and the five `oidvector` rows
//! are the ones `src/test/regress/sql/oid.sql` asserts.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

/// Every row of a result, one string per row with the columns comma-joined.
async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    let results = session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
    match &results[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell_text(cell.as_ref()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// The single cell of a one-row, one-column result.
async fn scalar(session: &mut SqlSession, sql: &str) -> String {
    let rows = query(session, sql).await;
    let [only] = rows.as_slice() else {
        panic!("expected exactly one row from {sql}, got {rows:?}");
    };
    only.clone()
}

fn session() -> SqlSession {
    SqlEngine::new().connect()
}

/// `pg_input_is_valid(value, 'oidvector')`.
///
/// The accepted rows carry as much weight as the rejected ones: an element
/// reader strict enough to reject `01XYZ` as a whole would also have to reject
/// `08`, and `oidin` accepts that as two elements.
#[tokio::test]
async fn pg_input_is_valid_accepts_what_oidvectorin_accepts() {
    let cases = [
        // From `oid.sql`.
        (" 1 2  4 ", "t"),
        ("01 01XYZ", "f"),
        ("01 9999999999", "f"),
        // Empty and all-space are the empty vector, not an error.
        ("", "t"),
        ("   ", "t"),
        // Base zero, per element.
        ("010", "t"),
        ("0x10 0x20", "t"),
        // Unsigned, so both spellings of 4294967295 are legal.
        ("-1", "t"),
        ("4294967295", "t"),
        // `strtoul` stops at the first byte that is not a digit of its base and
        // the scan resumes there, so neither of these is one bad token.
        ("08", "t"),
        ("0x", "f"),
        // `_` is a `pg_strtoint32` extension; `strtoul` has never taken it.
        ("1_0", "f"),
        ("4294967296", "f"),
        ("1,2", "f"),
        ("asdfasd", "f"),
    ];
    let mut session = session();
    for (input, want) in cases {
        let got = scalar(
            &mut session,
            &format!("SELECT pg_input_is_valid('{input}', 'oidvector')"),
        )
        .await;
        assert!(got == want, "{input:?}");
    }
}

/// `pg_input_error_info(value, 'oidvector')`, compared whole: message, detail,
/// hint and SQLSTATE in one string, because the defect moved the message and
/// the token together and pinning either alone would have missed it.
#[tokio::test]
async fn pg_input_error_info_names_oid_and_the_unconverted_remainder() {
    let cases = [
        // The two rows `oid.sql` asserts.
        (
            "01 01XYZ",
            "invalid input syntax for type oid: \"XYZ\",NULL,NULL,22P02",
        ),
        (
            "01 9999999999",
            "value \"9999999999\" is out of range for type oid,NULL,NULL,22003",
        ),
        // The quoted string is the whole remainder from the failing element's
        // first non-space byte, not the failing token.
        (
            "1 2 XY ZW",
            "invalid input syntax for type oid: \"XY ZW\",NULL,NULL,22P02",
        ),
        // A separator that is not whitespace stops the scan where it stands.
        (
            "1,2",
            "invalid input syntax for type oid: \",2\",NULL,NULL,22P02",
        ),
        // `0x` with no hex digit is the octal `0` and then a stray `x`.
        (
            "0x",
            "invalid input syntax for type oid: \"x\",NULL,NULL,22P02",
        ),
        // Past `u32` after either extension is 22003, still naming `oid`.
        (
            "4294967296",
            "value \"4294967296\" is out of range for type oid,NULL,NULL,22003",
        ),
    ];
    let mut session = session();
    for (input, want) in cases {
        let got = scalar(
            &mut session,
            &format!("SELECT * FROM pg_input_error_info('{input}', 'oidvector')"),
        )
        .await;
        assert!(got == want, "{input:?}");
    }
}

/// `oidvectorout` prints `%u`. Crabka carries an `oid` element in an `Int4`
/// because it has no `oid` element type, so the round trip is the check that
/// the bit pattern is read back unsigned rather than signed.
#[tokio::test]
async fn oidvector_text_output_is_unsigned() {
    let cases = [
        ("1 2 4", "1 2 4"),
        // Both spellings of the same oid print the same way.
        ("-1", "4294967295"),
        ("4294967295", "4294967295"),
        ("1 -1040", "1 4294966256"),
        // Base zero on the way in, decimal on the way out.
        ("010", "8"),
        ("0x10 0x20", "16 32"),
        ("08", "0 8"),
        ("", ""),
    ];
    let mut session = session();
    for (input, want) in cases {
        let got = scalar(&mut session, &format!("SELECT '{input}'::oidvector")).await;
        assert!(got == want, "{input:?}");
    }
}
