//! `"char"` in double quotes is `PostgreSQL`'s ad-hoc one-byte type (OID 18),
//! not `character(1)` (OID 1042).
//!
//! The two are separate types the grammar reaches by separate routes: unquoted
//! `char`/`character` is a keyword the parser turns into `character(1)`, and
//! the quoted spelling is an identifier looked up by `pg_type.typname`. Folding
//! them together is not a labelling slip — it changes what a value *is*.
//! `'\101'::"char"` is the byte `A`, because `charin` decodes the octal escape,
//! while `'\101'::char` is a backslash and three discarded characters; and
//! `'\377'::"char"` holds a byte no `character(1)` can hold at all.
//!
//! Every expected value here is `PostgreSQL` 18.4's, from
//! `src/test/regress/expected/char.out` and `src/backend/utils/adt/char.c`.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn session() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let s = engine.connect();
    (engine, s)
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e:?}"))
}

async fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match &run(s, sql).await[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

/// One projected column as `psql` shows it: the label from `RowDescription`,
/// the `pg_type` oid it reports, and the value's text.
#[derive(Debug, PartialEq, Eq)]
struct Column {
    label: String,
    type_oid: u32,
    value: Option<String>,
}

async fn column(s: &mut SqlSession, expr: &str) -> Column {
    let sql = format!("SELECT {expr}");
    match &run(s, &sql).await[0] {
        QueryResult::Rows { fields, rows, .. } => Column {
            label: fields[0].name.clone(),
            type_oid: fields[0].type_oid,
            value: cell_text(rows[0][0].as_ref()),
        },
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

fn col(label: &str, type_oid: u32, value: &str) -> Column {
    Column {
        label: label.to_string(),
        type_oid,
        value: Some(value.to_string()),
    }
}

/// The SQLSTATE and message a statement that must fail reports.
async fn err(s: &mut SqlSession, sql: &str) -> (String, String) {
    let e = s
        .simple_query(sql)
        .await
        .expect_err("statement should fail");
    (e.code, e.message)
}

/// `pg_type.oid` of `"char"`, of `character(n)` and of `text` — the three the
/// nine `char.out` statements move between.
const CHAR_OID: u32 = 18;
const BPCHAR_OID: u32 = 1042;
const TEXT_OID: u32 = 25;

// ---------------------------------------------------------------------------
// char.out's own cases
// ---------------------------------------------------------------------------

/// The nine `"char"` statements at the end of `char.out`, label and value both.
///
/// The label is half the diff: `PostgreSQL` names an unaliased cast column
/// after `pg_type.typname`, which is `char` for OID 18 and `bpchar` for 1042,
/// so a `"char"` cast reported as `bpchar` says the wrong type was chosen even
/// where the value happens to agree.
#[tokio::test]
async fn char_out_renders_every_form_charin_and_charout_define() {
    let (_engine, mut s) = session();

    for (expr, expected) in [
        (r#"'a'::"char""#, col("char", CHAR_OID, "a")),
        // The escape is DECODED: `\101` is octal 101, which is `A`.
        (r#"'\101'::"char""#, col("char", CHAR_OID, "A")),
        // And re-escaped on the way out, because 0xFF has no printable form.
        (r#"'\377'::"char""#, col("char", CHAR_OID, r"\377")),
        (r#"'a'::"char"::text"#, col("text", TEXT_OID, "a")),
        (r#"'\377'::"char"::text"#, col("text", TEXT_OID, r"\377")),
        // `char_text` is honest about NUL where `charout` is merely silent:
        // both reach the empty string.
        (r#"'\000'::"char"::text"#, col("text", TEXT_OID, "")),
        (r#"'a'::text::"char""#, col("char", CHAR_OID, "a")),
        (r#"'\377'::text::"char""#, col("char", CHAR_OID, r"\377")),
        (r#"''::text::"char""#, col("char", CHAR_OID, "")),
    ] {
        assert!(column(&mut s, expr).await == expected, "SELECT {expr}");
    }
}

// ---------------------------------------------------------------------------
// The separation
// ---------------------------------------------------------------------------

/// `'a'::char(1)` and `'a'::"char"` side by side. They agree on the value and
/// on nothing else, which is how folding them together survived this long.
#[tokio::test]
async fn the_quoted_and_unquoted_spellings_are_different_types() {
    let (_engine, mut s) = session();

    for (expr, expected) in [
        (r#"'a'::"char""#, col("char", CHAR_OID, "a")),
        ("'a'::char(1)", col("bpchar", BPCHAR_OID, "a")),
        ("'a'::char", col("bpchar", BPCHAR_OID, "a")),
        ("'a'::character(1)", col("bpchar", BPCHAR_OID, "a")),
        ("'a'::character varying(1)", col("varchar", 1043, "a")),
        ("'a'::bpchar", col("bpchar", BPCHAR_OID, "a")),
    ] {
        assert!(column(&mut s, expr).await == expected, "SELECT {expr}");
    }
}

/// The same separation where it changes the value rather than the label: a
/// `bpchar` keeps the four characters of `\101`, a `"char"` decodes them to one
/// byte, and `char(1)` keeps only the backslash.
#[tokio::test]
async fn an_octal_escape_is_one_byte_to_char_and_four_characters_to_bpchar() {
    let (_engine, mut s) = session();

    for (expr, expected) in [
        (r#"'\101'::"char""#, "A"),
        (r"'\101'::bpchar", r"\101"),
        (r"'\101'::char(1)", r"\"),
        (r"'\101'::text", r"\101"),
    ] {
        assert!(
            column(&mut s, expr).await.value == Some(expected.to_string()),
            "SELECT {expr}"
        );
    }
}

/// `pg_catalog."char"` reaches the same type; `pg_catalog.char` still reaches
/// `character(1)`, because the qualification does not make the keyword quoted.
#[tokio::test]
async fn a_pg_catalog_qualified_quoted_char_is_the_one_byte_type() {
    let (_engine, mut s) = session();

    assert!(column(&mut s, r#"'\101'::pg_catalog."char""#).await == col("char", CHAR_OID, "A"));
    assert!(column(&mut s, r"'\101'::pg_catalog.char").await == col("bpchar", BPCHAR_OID, r"\"));
}

// ---------------------------------------------------------------------------
// Ordering, storage and the integer pair
// ---------------------------------------------------------------------------

/// `charlt` and friends compare the byte **unsigned**, so `\377` is the largest
/// value of the type. Compared as the text `\377` it would be one of the
/// smallest, since a backslash sorts below every letter.
#[tokio::test]
async fn comparison_is_unsigned_over_the_byte() {
    let (_engine, mut s) = session();

    for (expr, expected) in [
        (r#"'\377'::"char" > 'a'::"char""#, "t"),
        (r#"'\377'::"char" > '\176'::"char""#, "t"),
        (r#"'\000'::"char" < 'a'::"char""#, "t"),
        (r#"'a'::"char" = 'a'::"char""#, "t"),
        // The same three values as text sort the other way round, which is the
        // order a `bpchar` fold would have given.
        (r"'\377' > 'a'", "f"),
    ] {
        assert!(
            column(&mut s, expr).await.value == Some(expected.to_string()),
            "SELECT {expr}"
        );
    }
}

/// A bare literal beside a `"char"` is `unknown` and adopts the type, so
/// `relkind = 'r'` compares two bytes.
///
/// This is the one place the separation costs something rather than buying it:
/// while `"char"` was `bpchar` the literal was already the right type by
/// accident, and a one-byte type with no `unknown` rule of its own answers
/// 42804 to the most ordinary query anyone writes against it.
#[tokio::test]
async fn a_bare_literal_beside_a_char_adopts_the_one_byte_type() {
    let (_engine, mut s) = session();

    run(&mut s, r#"CREATE TABLE codes (k int4, c "char")"#).await;
    run(
        &mut s,
        r"INSERT INTO codes VALUES (1, 'r'), (2, 'v'), (3, '\377')",
    )
    .await;

    for (sql, expected) in [
        ("SELECT count(*) FROM codes WHERE c = 'r'", "1"),
        ("SELECT count(*) FROM codes WHERE c IN ('r','v')", "2"),
        // `\377` is above every letter, so the unsigned order puts all three
        // rows past `'a'` and keeps the escaped one out of `'a'`..`'z'`.
        ("SELECT count(*) FROM codes WHERE c > 'a'", "3"),
        (
            "SELECT count(*) FROM codes WHERE c BETWEEN 'a' AND 'z'",
            "2",
        ),
    ] {
        assert!(
            rows(&mut s, sql).await == vec![vec![Some(expected.to_string())]],
            "{sql}"
        );
    }
    assert!(column(&mut s, r#"'x' = 'x'::"char""#).await.value == Some("t".into()));
    // `||` deliberately does not adopt: `anynonarray || text` is the only
    // candidate, so the byte's text is concatenated with an ordinary `text`.
    assert!(column(&mut s, r#"'x'::"char" || 'y'"#).await == col("?column?", TEXT_OID, "xy"));
}

/// `chartoi4` reads the byte **signed** while the comparisons read it unsigned.
/// `char.c` says "You wanted consistency?" about exactly this pair.
#[tokio::test]
async fn the_integer_conversions_are_signed_and_range_checked() {
    let (_engine, mut s) = session();

    for (expr, expected) in [
        (r#"'a'::"char"::int4"#, "97"),
        (r#"'\377'::"char"::int4"#, "-1"),
        (r#"'\200'::"char"::int4"#, "-128"),
        (r#"'\000'::"char"::int4"#, "0"),
        (r#"97::int4::"char""#, "a"),
        (r#"(-1)::int4::"char""#, r"\377"),
        (r#"0::int4::"char""#, ""),
    ] {
        assert!(
            column(&mut s, expr).await.value == Some(expected.to_string()),
            "SELECT {expr}"
        );
    }

    for sql in [
        r#"SELECT 128::int4::"char""#,
        r#"SELECT (-129)::int4::"char""#,
    ] {
        assert!(
            err(&mut s, sql).await == ("22003".to_string(), "\"char\" out of range".to_string()),
            "{sql}"
        );
    }
}

/// A stored `"char"` column round-trips the byte, including the two values no
/// `character(1)` can hold: a high-bit byte and NUL.
#[tokio::test]
async fn a_stored_char_column_round_trips_every_byte_form() {
    let (_engine, mut s) = session();

    run(&mut s, r#"CREATE TABLE codes (k int4, c "char")"#).await;
    run(
        &mut s,
        r"INSERT INTO codes VALUES (1, 'r'), (2, '\377'), (3, '\000'), (4, '\101')",
    )
    .await;

    assert!(
        rows(&mut s, "SELECT k, c FROM codes ORDER BY k").await
            == vec![
                vec![Some("1".into()), Some("r".into())],
                vec![Some("2".into()), Some(r"\377".into())],
                vec![Some("3".into()), Some(String::new())],
                vec![Some("4".into()), Some("A".into())],
            ]
    );
    // Ordering by the column is the unsigned byte order, so NUL leads and
    // `\377` trails — not the text order the escaped spelling would give, where
    // the backslash would put `\377` first.
    assert!(
        rows(&mut s, "SELECT k FROM codes ORDER BY c").await
            == vec![
                vec![Some("3".into())],
                vec![Some("4".into())],
                vec![Some("1".into())],
                vec![Some("2".into())],
            ]
    );
    // A `WHERE` on the stored byte finds the row a text comparison would miss.
    assert!(
        rows(&mut s, r#"SELECT k FROM codes WHERE c = '\377'::"char""#).await
            == vec![vec![Some("2".into())]]
    );
}

/// The declared type survives DDL: every catalog reader sees OID 18 and
/// `typlen` 1, where `character(1)` is 1042 and -1.
#[tokio::test]
async fn a_char_column_declares_the_one_byte_type_in_the_catalog() {
    let (_engine, mut s) = session();

    run(
        &mut s,
        r#"CREATE TABLE widths (a "char", b char(1), c character varying(1))"#,
    )
    .await;

    assert!(
        rows(
            &mut s,
            "SELECT a.attname, t.typname, t.typlen, a.attlen \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE c.relname = 'widths' AND a.attnum > 0 ORDER BY a.attnum",
        )
        .await
            == vec![
                vec![
                    Some("a".into()),
                    Some("char".into()),
                    Some("1".into()),
                    Some("1".into()),
                ],
                vec![
                    Some("b".into()),
                    Some("bpchar".into()),
                    Some("-1".into()),
                    Some("-1".into()),
                ],
                vec![
                    Some("c".into()),
                    Some("varchar".into()),
                    Some("-1".into()),
                    Some("-1".into()),
                ],
            ]
    );
}

/// `pg_type` has the row upstream has, array link included.
#[tokio::test]
async fn pg_type_carries_the_char_row_and_its_array() {
    let (_engine, mut s) = session();

    assert!(
        rows(
            &mut s,
            "SELECT oid, typname, typlen, typcategory, typelem, typarray FROM pg_type \
             WHERE oid IN (18, 1002) ORDER BY oid",
        )
        .await
            == vec![
                vec![
                    Some("18".into()),
                    Some("char".into()),
                    Some("1".into()),
                    Some("Z".into()),
                    Some("0".into()),
                    Some("1002".into()),
                ],
                vec![
                    Some("1002".into()),
                    Some("_char".into()),
                    Some("-1".into()),
                    Some("A".into()),
                    Some("18".into()),
                    Some("0".into()),
                ],
            ]
    );
}

/// The empty string and one space are two different `"char"` values — bytes
/// 0x00 and 0x20 — and a UNIQUE index has to admit both rows.
///
/// This is what the `bpchar` fold cost. `character(1)` blank-pads, so both
/// literals arrived as the single stored value `' '`, the second `INSERT` was a
/// 23505 duplicate-key violation, and a row `PostgreSQL` stores was refused.
#[tokio::test]
async fn nul_and_space_are_two_values_a_unique_index_must_keep_apart() {
    let (_engine, mut s) = session();

    run(&mut s, r#"CREATE TABLE codes (c "char" UNIQUE)"#).await;
    run(&mut s, "INSERT INTO codes VALUES ('')").await;
    run(&mut s, "INSERT INTO codes VALUES (' ')").await;

    assert!(
        rows(&mut s, "SELECT c::int4 FROM codes ORDER BY 1").await
            == vec![vec![Some("0".into())], vec![Some("32".into())]]
    );
    assert!(column(&mut s, r#"''::"char" = ' '::"char""#).await.value == Some("f".into()));
    // `character(1)` is the type that folds them, and still does.
    assert!(column(&mut s, "''::char(1) = ' '::char(1)").await.value == Some("t".into()));
}

/// `charin` takes the first byte of a longer input and silently discards the
/// rest — `char.c`'s backwards-compatibility provision for multibyte text.
///
/// Under the `bpchar` fold every one of these writes was a 22001 instead, so a
/// `"char"` column refused the whole escaped half of its own range: `'\377'` is
/// four characters to `character(1)` and one byte to `"char"`.
#[tokio::test]
async fn a_write_wider_than_one_byte_keeps_the_first_and_is_not_refused() {
    let (_engine, mut s) = session();

    run(&mut s, r#"CREATE TABLE codes (k int4, c "char")"#).await;
    run(
        &mut s,
        r"INSERT INTO codes VALUES (1, 'cd'), (2, '\377'), (3, 'c     ')",
    )
    .await;

    assert!(
        rows(&mut s, "SELECT k, c, c::int4 FROM codes ORDER BY k").await
            == vec![
                vec![Some("1".into()), Some("c".into()), Some("99".into())],
                vec![Some("2".into()), Some(r"\377".into()), Some("-1".into())],
                vec![Some("3".into()), Some("c".into()), Some("99".into())],
            ]
    );
    // `character(1)` still refuses `'cd'` and still accepts `'c     '`, which
    // is `char.out`'s own expectation for the unquoted spelling — and was what
    // the quoted one got.
    run(&mut s, "CREATE TABLE padded (c char(1))").await;
    assert!(
        err(&mut s, "INSERT INTO padded VALUES ('cd')").await
            == (
                "22001".to_string(),
                "value too long for type character(1)".to_string()
            )
    );
    assert!(err(&mut s, r"INSERT INTO padded VALUES ('\377')").await.0 == "22001");
    run(&mut s, "INSERT INTO padded VALUES ('c     ')").await;
}

/// `"char"` casts to the string family and to `int4`, and to nothing else. The
/// absences are as load-bearing as the presences: each is a 42846 upstream, and
/// the numeric fall-through would otherwise grant them all.
#[tokio::test]
async fn char_casts_only_where_pg_cast_has_an_entry() {
    let (_engine, mut s) = session();

    for target in ["float8", "int8", "int2", "numeric", "bool", "date", "bytea"] {
        let sql = format!(r#"SELECT 'a'::"char"::{target}"#);
        let (code, _) = err(&mut s, &sql).await;
        assert!(code == "42846", "{sql} reported {code}");
    }
}
