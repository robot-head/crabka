//! Two things `PostgreSQL` does that Crabka did not: drop `character(n)`'s blank
//! padding on the way to `text`, and point a caret at the reference that failed
//! a query's parse analysis.
//!
//! # The padding is not a display detail
//!
//! `character(8)` stores `'xxxx'` as `'xxxx    '`, and every string type but
//! `character` is reached from it through one implicit cast, `text(bpchar)`,
//! which is `rtrim`. Carrying the padding past that cast is not a wider column;
//! it is a different value. `length` answered 8, `lower(c)` answered
//! `'xxxx    '`, and — the one that costs data rather than looks — `c = 'xxxx'`
//! answered false, so **no predicate written against a short literal could ever
//! find a row**. `SELECT … WHERE c = 'xxxx'` returned nothing, and `DELETE …
//! WHERE c = 'xxxx'` reported `DELETE 0` while the row it named stayed. A
//! `CHECK (c <> 'bad')` constraint could never fire, and a row-security policy
//! written `USING (tenant = 'acme')` could never grant.
//!
//! The cast is not universal, which is why the negative cases below carry as
//! much weight as the positive ones. `PostgreSQL` keeps the padding wherever it
//! has a `character` overload that reads the stored datum — `bpcharout` (a
//! projected column still prints eight wide), `bpcharoctetlen`, `bpcharlike`,
//! `bpcharregexeq` — and wherever an argument is rendered by its own output
//! function rather than cast: `concat`, `format`, an array or record member.
//! Every expectation here was measured against `PostgreSQL` 18.4.
//!
//! # The caret has to be earned in both directions
//!
//! `PostgreSQL` prints the offending line and a caret for its grouping and
//! name-resolution errors. Adding one costs two lines of output, so it pays only
//! where Crabka raises the same error at the same statement; on an error Crabka
//! raises and `PostgreSQL` does not, a caret is two more lines of divergence,
//! not fewer. So the caret cases are paired with cases that must stay bare.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Cell, Engine, QueryResult, Session},
    error::PgError,
};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

async fn fail(session: &mut SqlSession, sql: &str) -> PgError {
    session
        .simple_query(sql)
        .await
        .expect_err(&format!("{sql} should fail"))
}

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

/// Every row of a result, one string per row with the columns comma-joined.
async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match &run(session, sql).await[0] {
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

async fn tag(session: &mut SqlSession, sql: &str) -> String {
    match &run(session, sql).await[0] {
        QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag.clone(),
        QueryResult::Empty => panic!("expected a command tag from {sql}"),
    }
}

/// One `character(8)` value `'xxxx'`, stored padded, beside the same four
/// characters in the two string types that do not pad.
const PADDED: &str = r"
CREATE TABLE bp (c char(8), v varchar(8), t text);
INSERT INTO bp VALUES ('xxxx', 'xxxx', 'xxxx');
";

async fn padded() -> SqlSession {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, PADDED).await;
    session
}

/// The `bpchar → text` cast, at every spelling that reaches it.
///
/// The right-hand column is what `PostgreSQL` 18.4 answers. Brackets fence the
/// value so a trailing blank is visible in a failure message.
#[tokio::test]
async fn a_character_value_loses_its_padding_on_the_way_to_text() {
    let cases = [
        // Written casts.
        ("SELECT '[' || (c::text) || ']' FROM bp", "[xxxx]"),
        ("SELECT '[' || (c::varchar) || ']' FROM bp", "[xxxx]"),
        // Functions declared over `text`.
        ("SELECT '[' || lower(c) || ']' FROM bp", "[xxxx]"),
        ("SELECT '[' || upper(c) || ']' FROM bp", "[XXXX]"),
        ("SELECT '[' || btrim(c) || ']' FROM bp", "[xxxx]"),
        (
            "SELECT '[' || replace(c, 'x', 'y') || ']' FROM bp",
            "[yyyy]",
        ),
        ("SELECT '[' || substr(c, 1, 8) || ']' FROM bp", "[xxxx]"),
        (
            "SELECT '[' || lpad(c, 10, '.') || ']' FROM bp",
            "[......xxxx]",
        ),
        ("SELECT quote_literal(c) FROM bp", "'xxxx'"),
        // `bpcharlen` measures with `bcTruelen`, which is the same answer.
        ("SELECT length(c) FROM bp", "4"),
        ("SELECT length(upper(c)) FROM bp", "4"),
        // The concatenation operator has no `character` overload.
        ("SELECT '[' || (c || 'Z') || ']' FROM bp", "[xxxxZ]"),
    ];
    let mut session = padded().await;
    for (sql, expected) in cases {
        assert!(scalar(&mut session, sql).await == expected, "{sql}");
    }
}

/// Where `PostgreSQL` keeps the padding, so must Crabka.
///
/// These are not oversights in the fix; they are the shape of `pg_cast`. A
/// `character` overload (`bpcharoctetlen`, `bpcharlike`, `bpcharregexeq`) reads
/// the stored datum, and a `VARIADIC "any"` argument is rendered by `bpcharout`
/// rather than cast at all — which is the whole difference between
/// `concat(c, 'Z')` and `c || 'Z'`.
#[tokio::test]
async fn the_padding_survives_where_postgresql_keeps_it() {
    let cases = [
        ("SELECT '[' || concat(c, 'Z') || ']' FROM bp", "[xxxx    Z]"),
        ("SELECT '[' || format('%s', c) || ']' FROM bp", "[xxxx    ]"),
        ("SELECT octet_length(c) FROM bp", "8"),
        ("SELECT (c LIKE 'xxxx') FROM bp", "f"),
        ("SELECT (c ~ 'xxxx$') FROM bp", "f"),
        ("SELECT (ARRAY[c])::text FROM bp", "{\"xxxx    \"}"),
        // The projected column itself: `bpcharout` returns eight characters.
        ("SELECT c FROM bp", "xxxx    "),
        // And a `character` target re-pads rather than trimming.
        ("SELECT (c::text)::char(6) FROM bp", "xxxx  "),
    ];
    let mut session = padded().await;
    for (sql, expected) in cases {
        assert!(scalar(&mut session, sql).await == expected, "{sql}");
    }
}

/// A `text` value that really ends in a blank keeps it.
///
/// The cast is chosen by the operand's *static* type, not by what the value
/// looks like, so nothing here may change. This is the test that fails if the
/// trim is ever moved down into the value layer, where `character`, `varchar`
/// and `text` are one and the same `Datum`.
#[tokio::test]
async fn a_text_value_keeps_a_trailing_blank_it_owns() {
    let cases = [
        ("SELECT ('a '::text = 'a')", "f"),
        ("SELECT length('a '::text)", "2"),
        ("SELECT '[' || lower('A '::text) || ']'", "[a ]"),
        ("SELECT ('a '::varchar = 'a')", "f"),
        ("SELECT length('a '::varchar(4))", "2"),
    ];
    let mut session = padded().await;
    for (sql, expected) in cases {
        assert!(scalar(&mut session, sql).await == expected, "{sql}");
    }
}

/// A comparison against a shorter value has to find the row.
///
/// Both operands are cast: two `character` values of different declared widths
/// compare equal because `bpchareq` measures with `bcTruelen`, and a `character`
/// against a `text` compares equal because only the `character` side is
/// trimmed.
#[tokio::test]
async fn a_character_column_compares_equal_to_the_value_it_holds() {
    let cases = [
        ("SELECT (c = 'xxxx') FROM bp", "t"),
        ("SELECT (c = 'xxxx'::text) FROM bp", "t"),
        ("SELECT (c = v) FROM bp", "t"),
        ("SELECT (c <> 'xxxx') FROM bp", "f"),
        ("SELECT (c IN ('nope', 'xxxx')) FROM bp", "t"),
        ("SELECT (c BETWEEN 'xxxx' AND 'xxxx') FROM bp", "t"),
        ("SELECT ('xxxx'::char(4) = 'xxxx'::char(8))", "t"),
        // Only the `character` side loses blanks, so a `text` value that owns
        // one still differs.
        ("SELECT (c = 'xxxx '::text) FROM bp", "f"),
    ];
    let mut session = padded().await;
    for (sql, expected) in cases {
        assert!(scalar(&mut session, sql).await == expected, "{sql}");
    }
}

/// The rows such a predicate governs — the reason this is a data bug and not a
/// formatting one.
///
/// A `SELECT` found nothing, a `CHECK` could not reject, and a `DELETE`
/// reported success having removed nothing.
#[tokio::test]
async fn a_predicate_on_a_character_column_reaches_its_rows() {
    let mut session = padded().await;
    assert!(scalar(&mut session, "SELECT count(*) FROM bp WHERE c = 'xxxx'").await == "1");
    assert!(query(&mut session, "SELECT t FROM bp WHERE c = 'xxxx'").await == ["xxxx"]);

    run(
        &mut session,
        "CREATE TABLE guarded (c char(8) CHECK (c <> 'bad'))",
    )
    .await;
    let rejected = fail(&mut session, "INSERT INTO guarded VALUES ('bad')").await;
    assert!(rejected.code == "23514", "{rejected:?}");
    assert!(query(&mut session, "SELECT c FROM guarded").await == Vec::<String>::new());

    assert!(tag(&mut session, "DELETE FROM bp WHERE c = 'xxxx'").await == "DELETE 1");
    assert!(query(&mut session, "SELECT t FROM bp").await == Vec::<String>::new());
}

/// A `character` column grouped as itself keeps its padding in the group key;
/// grouped through a `text` function it does not.
///
/// `GROUP BY c` was never wrong — every row of one column carries the same
/// padding — and this pins that the fix did not start trimming the projected
/// key. `GROUP BY lower(c)` is the case that was wrong, and it collapses
/// `'ABAB'` and `'abab'` into one group whose key is four characters, not eight.
#[tokio::test]
async fn a_grouping_key_pads_as_the_grouped_expression_does() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE g (c char(8));
         INSERT INTO g VALUES ('ABAB'), ('abab'), ('cccc');",
    )
    .await;
    assert!(
        query(
            &mut session,
            "SELECT c, count(*) FROM g GROUP BY c ORDER BY c"
        )
        .await
            == ["ABAB    ,1", "abab    ,1", "cccc    ,1"]
    );
    assert!(
        query(
            &mut session,
            "SELECT lower(c), count(*) FROM g GROUP BY lower(c) ORDER BY lower(c)"
        )
        .await
            == ["abab,2", "cccc,1"]
    );
}

/// A caret, at the column `PostgreSQL` 18.4 puts it, for each error family.
///
/// The position is a one-based character offset into the statement; the client
/// renders it as the `LINE`/`^` pair. Each expected offset below was read off
/// `PostgreSQL`'s own output for the same statement.
#[tokio::test]
async fn a_query_analysis_error_carries_postgresqls_caret() {
    let cases = [
        // The ungrouped-column check walks the target list — which carries the
        // `ORDER BY` expressions by then — and only afterwards the `HAVING`
        // qualification.
        (
            "SELECT count(*) FROM t GROUP BY a ORDER BY b",
            "42803",
            44_usize,
        ),
        ("SELECT a FROM t HAVING min(a) < max(a)", "42803", 8),
        ("SELECT 1 AS one FROM t HAVING a > 1", "42803", 31),
        ("SELECT b, count(*) FROM t GROUP BY 3", "42P10", 36),
        // Ambiguity is resolved in clause order: target list, WHERE, HAVING,
        // ORDER BY, and GROUP BY last — so the `ORDER BY` reference is blamed
        // even though `GROUP BY` wrote one first.
        (
            "SELECT count(*) FROM t x, t y WHERE x.a = y.a GROUP BY b ORDER BY b",
            "42702",
            67,
        ),
        (
            "SELECT count(b) FROM t x, t y WHERE x.a = y.a GROUP BY x.b",
            "42702",
            14,
        ),
        // The unreachable FROM-clause entry is blamed where the query named it.
        (
            "SELECT a, g FROM t alias, (SELECT alias.a AS g) ss",
            "42P01",
            35,
        ),
    ];
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE t (a int, b int)").await;
    for (sql, code, position) in cases {
        let error = fail(&mut session, sql).await;
        assert!(error.code == code, "{sql}: {error:?}");
        let found = error
            .diagnostics
            .as_ref()
            .and_then(|diagnostics| diagnostics.position);
        assert!(found == Some(position), "{sql}: {error:?}");
    }
}

/// No caret on an error Crabka raises where `PostgreSQL` raises none, or none
/// there.
///
/// A caret is two extra lines of output. On a report `PostgreSQL` does not make
/// — Crabka's `MERGE` and `ON CONFLICT` name resolution is stricter than
/// `PostgreSQL`'s — those two lines are divergence added, not removed. `EXECUTE`
/// is the other shape: `PostgreSQL` does raise the grouping error there, from a
/// prepared plan whose source the statement no longer carries, and reports no
/// position for it.
#[tokio::test]
async fn an_error_postgresql_does_not_position_stays_bare() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE t (a int, b int);
         CREATE TABLE u (key int PRIMARY KEY, data text);
         PREPARE p AS SELECT a, b FROM t GROUP BY a;",
    )
    .await;
    let cases = [
        // 18.4 answers this one `name \"t\" specified more than once`, so the
        // ambiguity report is Crabka's alone.
        "MERGE INTO t USING t ON a = a WHEN MATCHED THEN DO NOTHING",
        // The prepared statement's own source is gone by the time the grouping
        // check runs, so `PostgreSQL` reports the message with no position.
        "EXECUTE p",
    ];
    for sql in cases {
        let error = fail(&mut session, sql).await;
        let found = error
            .diagnostics
            .as_ref()
            .and_then(|diagnostics| diagnostics.position);
        assert!(found == None, "{sql}: {error:?}");
    }
}

/// A `COPY ( query )` that does not go `TO` carries the caret its grammar rule
/// earns.
///
/// `PostgreSQL` positions every syntax error its grammar raises. Crabka's parser
/// knows an offset for all of its own, but most of what it refuses `PostgreSQL`
/// accepts, so the offset is only `PostgreSQL`'s where the rule rejects the same
/// text — and `COPY ( query )` is such a rule. Both offsets below are the ones
/// 18.4 reports for `copyselect.sql`'s two failing statements, and the character
/// count is what makes the multi-byte case land in the same place.
#[tokio::test]
async fn a_copy_grammar_error_carries_postgresqls_caret() {
    let cases = [
        ("copy (select * from t) from stdin;", 24_usize),
        ("copy (select * from t) (a,b) to stdout;", 24),
        // The `P` field counts characters, not bytes, so text before the error
        // must not shift the caret by its encoded width.
        ("select 'éé'; copy (select * from t) from stdin;", 37),
    ];
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE t (a int, b int)").await;
    for (sql, position) in cases {
        let error = fail(&mut session, sql).await;
        assert!(error.code == "42601", "{sql}: {error:?}");
        let found = error
            .diagnostics
            .as_ref()
            .and_then(|diagnostics| diagnostics.position);
        assert!(found == Some(position), "{sql}: {error:?}");
    }
}

/// A syntax error from a rule that did not claim `PostgreSQL`'s offset stays
/// bare, whether or not the parser knows where it stopped.
///
/// Every one of these is grammar `PostgreSQL` accepts and Crabka has not
/// implemented, so the report itself is Crabka's; a caret under it would spell a
/// coverage gap as a typing mistake and cost two more lines of divergence.
#[tokio::test]
async fn a_syntax_error_postgresql_does_not_raise_stays_bare() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE t (a int, b int)").await;
    let cases = [
        "CREATE RULE r AS ON INSERT TO t DO INSTEAD NOTHING",
        "COPY t FROM stdin WITH (on_error ignore)",
        "SELECT a FROM t WHERE a IN (SELECT a FROM t) FOR UPDATE OF nosuch",
    ];
    for sql in cases {
        let error = fail(&mut session, sql).await;
        let found = error
            .diagnostics
            .as_ref()
            .and_then(|diagnostics| diagnostics.position);
        assert!(found == None, "{sql}: {error:?}");
    }
}
