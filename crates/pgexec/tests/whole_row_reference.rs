//! Whole-row references — `SELECT t FROM t`, `PostgreSQL`'s `Var` with
//! `varattno` 0.
//!
//! A bare name that matches no column is looked up in the range table, and a
//! match there is the entire row as one composite value. It is the same
//! resolution step everywhere a relation can be named, so these cases cover the
//! ordinary relations as well as the transient one a trigger's `REFERENCING …
//! TABLE` clause introduces, which has no catalog row at all.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut SqlSession, sql: &str) {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

async fn cells(s: &mut SqlSession, sql: &str) -> Vec<Option<String>> {
    match &s
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))[0]
    {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row[0]
                    .as_ref()
                    .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8"))
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

async fn column_label(s: &mut SqlSession, sql: &str) -> String {
    match &s
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))[0]
    {
        QueryResult::Rows { fields, .. } => fields[0].name.clone(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

async fn error(s: &mut SqlSession, sql: &str) -> String {
    match s.simple_query(sql).await {
        Err(error) => format!("{error}"),
        Ok(ok) => panic!("expected {sql} to fail, got {ok:?}"),
    }
}

/// The composite value, its field order, its NULL rendering, and the label the
/// reference gets — over a base table, an alias, a subquery and a `VALUES` list,
/// which are four different ways a range-table entry reaches a scope.
#[tokio::test]
async fn a_bare_relation_name_is_the_whole_row_as_a_composite() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE wr (a int, b text)").await;
    run(&mut s, "INSERT INTO wr VALUES (1, 'x'), (2, NULL)").await;
    run(
        &mut s,
        "CREATE FUNCTION whole_row_arg(wr) RETURNS TABLE(result text) LANGUAGE sql AS 'SELECT $1.b'",
    )
    .await;

    let cases: Vec<(&str, Vec<Option<String>>)> = vec![
        // The relation's own name, in declaration order, with a NULL field
        // rendered as nothing between the separators.
        (
            "SELECT wr FROM wr ORDER BY a",
            vec![Some("(1,x)".into()), Some("(2,)".into())],
        ),
        // An alias replaces the name: the range-table entry is what is named.
        (
            "SELECT q FROM wr q ORDER BY a",
            vec![Some("(1,x)".into()), Some("(2,)".into())],
        ),
        // A cast is the corpus's spelling, and it must see the same value.
        (
            "SELECT wr::text FROM wr ORDER BY a",
            vec![Some("(1,x)".into()), Some("(2,)".into())],
        ),
        // A subquery's alias is a range-table entry like any other.
        (
            "SELECT s FROM (SELECT 1 AS x, 'y'::text AS y) s",
            vec![Some("(1,y)".into())],
        ),
        (
            "SELECT v FROM (VALUES (1, 2)) v(a, b)",
            vec![Some("(1,2)".into())],
        ),
        (
            "SELECT row_to_json(wr.*)::text FROM wr ORDER BY a",
            vec![
                Some(r#"{"a":1,"b":"x"}"#.into()),
                Some(r#"{"a":2,"b":null}"#.into()),
            ],
        ),
        (
            "SELECT row_to_json(s.*)::text FROM generate_series(11, 12) WITH ORDINALITY s ORDER BY ordinality",
            vec![
                Some(r#"{"s":11,"ordinality":1}"#.into()),
                Some(r#"{"s":12,"ordinality":2}"#.into()),
            ],
        ),
        // A FROM function is implicitly LATERAL. Its whole-row argument must
        // therefore be substituted from the preceding relation.
        (
            "SELECT result FROM wr, whole_row_arg(wr) ORDER BY a",
            vec![Some("x".into()), None],
        ),
        // It is an ordinary value, so it aggregates, orders and groups.
        (
            "SELECT string_agg(wr::text, ', ' ORDER BY a) FROM wr",
            vec![Some("(1,x), (2,)".into())],
        ),
        (
            "SELECT wr FROM wr ORDER BY wr DESC",
            vec![Some("(2,)".into()), Some("(1,x)".into())],
        ),
        (
            "SELECT wr FROM wr GROUP BY wr ORDER BY wr",
            vec![Some("(1,x)".into()), Some("(2,)".into())],
        ),
        // Field selection off the composite.
        (
            "SELECT (wr).a FROM wr ORDER BY 1",
            vec![Some("1".into()), Some("2".into())],
        ),
        // `IS [NOT] NULL` on a composite is field-wise, so the row with one
        // NULL field satisfies neither test.
        (
            "SELECT count(*)::text FROM wr WHERE wr IS NOT NULL",
            vec![Some("1".into())],
        ),
        (
            "SELECT count(*)::text FROM wr WHERE wr IS NULL",
            vec![Some("0".into())],
        ),
        // RETURNING resolves against the same range table.
        (
            "INSERT INTO wr VALUES (3, 'z') RETURNING wr",
            vec![Some("(3,z)".into())],
        ),
        (
            "DELETE FROM wr WHERE a = 3 RETURNING wr",
            vec![Some("(3,z)".into())],
        ),
    ];

    for (sql, expected) in cases {
        let got = cells(&mut s, sql).await;
        assert!(got == expected, "{sql}");
    }

    // The reference is labelled by the name that was written, as a column
    // reference is.
    assert!(column_label(&mut s, "SELECT wr FROM wr").await == "wr");
    assert!(column_label(&mut s, "SELECT q FROM wr q").await == "q");
}

/// A column of that name wins, and a qualified spelling is never a whole row.
#[tokio::test]
async fn a_column_outranks_the_relation_and_a_qualified_name_is_never_a_whole_row() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE shadow (shadow int)").await;
    run(&mut s, "INSERT INTO shadow VALUES (7)").await;
    assert!(cells(&mut s, "SELECT shadow FROM shadow").await == vec![Some("7".into())]);

    // `s.wr` is "column wr of range-table entry s", so the missing entry is the
    // error — the whole row of `s.wr` is not a spelling that exists.
    run(&mut s, "CREATE SCHEMA sc").await;
    run(&mut s, "CREATE TABLE sc.wr (a int)").await;
    assert!(
        error(&mut s, "SELECT sc.wr FROM sc.wr")
            .await
            .contains("missing FROM-clause entry for table \"sc\"")
    );
    // An alias hides the relation's own name, exactly as it does for columns.
    assert!(
        error(&mut s, "SELECT wr FROM sc.wr q")
            .await
            .contains("column \"wr\" does not exist")
    );
    // A name that is neither column nor relation still reports 42703.
    assert!(
        error(&mut s, "SELECT nosuch FROM sc.wr")
            .await
            .contains("column \"nosuch\" does not exist")
    );
}

/// An inherited parent projects its children into the parent's row format, so
/// the parent's whole row has the parent's fields whichever child a row is in.
#[tokio::test]
async fn an_inherited_parents_whole_row_is_in_the_parents_format() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE par (a text, b int)").await;
    run(&mut s, "CREATE TABLE chi (c text) INHERITS (par)").await;
    run(&mut s, "INSERT INTO par VALUES ('p', 1)").await;
    run(&mut s, "INSERT INTO chi VALUES ('c', 2, 'extra')").await;

    // The child's extra column is not part of the parent's row type.
    assert!(
        cells(&mut s, "SELECT par FROM par ORDER BY b").await
            == vec![Some("(p,1)".into()), Some("(c,2)".into())]
    );
    assert!(cells(&mut s, "SELECT par FROM ONLY par").await == vec![Some("(p,1)".into())]);
    // Named directly, the child's own row type includes it.
    assert!(cells(&mut s, "SELECT chi FROM chi").await == vec![Some("(c,2,extra)".into())]);
}

/// The transient relation a `REFERENCING … TABLE` clause introduces behaves like
/// any other range-table entry: its whole row is a composite in the *triggering*
/// relation's format, collected across the whole statement.
#[tokio::test]
async fn a_transition_table_supports_the_same_whole_row_reference() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE tt (a int, v text)").await;
    run(&mut s, "CREATE TABLE tt_log (what text)").await;
    run(
        &mut s,
        "CREATE FUNCTION tt_dump() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO tt_log
             SELECT coalesce((SELECT string_agg(nt::text, ', ' ORDER BY a) FROM nt), '<none>');
           RETURN NULL;
         END $$",
    )
    .await;
    run(
        &mut s,
        "CREATE TRIGGER tt_i AFTER INSERT ON tt REFERENCING NEW TABLE AS nt
         FOR EACH STATEMENT EXECUTE FUNCTION tt_dump()",
    )
    .await;

    // Every row the statement wrote, once — not once per row.
    run(&mut s, "INSERT INTO tt VALUES (1, 'a'), (2, 'b')").await;
    assert!(cells(&mut s, "SELECT what FROM tt_log").await == vec![Some("(1,a), (2,b)".into())]);

    // A statement that writes nothing still fires, over an empty relation.
    run(&mut s, "DELETE FROM tt_log").await;
    run(&mut s, "INSERT INTO tt SELECT 9, 'z' WHERE false").await;
    assert!(cells(&mut s, "SELECT what FROM tt_log").await == vec![Some("<none>".into())]);

    // The name is gone once the trigger returns.
    assert!(
        error(&mut s, "SELECT * FROM nt")
            .await
            .contains("relation \"nt\" does not exist")
    );
}

/// A leaf partition's trigger sees leaf-format rows and the parent's sees
/// parent-format rows, even when the two disagree on column order.
#[tokio::test]
async fn a_transition_table_carries_the_triggering_relations_row_format() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE ph (a text, b int) PARTITION BY LIST (a)",
    )
    .await;
    // The leaf declares its columns in the opposite order.
    run(&mut s, "CREATE TABLE ph_leaf (b int, a text)").await;
    run(
        &mut s,
        "ALTER TABLE ph ATTACH PARTITION ph_leaf FOR VALUES IN ('AAA')",
    )
    .await;
    run(&mut s, "CREATE TABLE ph_log (who text, what text)").await;
    run(
        &mut s,
        "CREATE FUNCTION ph_dump() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO ph_log
             SELECT TG_NAME, (SELECT string_agg(nt::text, ', ') FROM nt);
           RETURN NULL;
         END $$",
    )
    .await;
    run(
        &mut s,
        "CREATE TRIGGER ph_parent AFTER INSERT ON ph REFERENCING NEW TABLE AS nt
         FOR EACH STATEMENT EXECUTE FUNCTION ph_dump()",
    )
    .await;
    run(
        &mut s,
        "CREATE TRIGGER ph_child AFTER INSERT ON ph_leaf REFERENCING NEW TABLE AS nt
         FOR EACH STATEMENT EXECUTE FUNCTION ph_dump()",
    )
    .await;

    // Routed through the parent: the parent's trigger fires, in parent format.
    run(&mut s, "INSERT INTO ph VALUES ('AAA', 42)").await;
    assert!(
        cells(&mut s, "SELECT what FROM ph_log WHERE who = 'ph_parent'").await
            == vec![Some("(AAA,42)".into())]
    );
    // Written straight to the leaf: the leaf's trigger fires, in leaf format.
    run(&mut s, "INSERT INTO ph_leaf VALUES (7, 'AAA')").await;
    assert!(
        cells(&mut s, "SELECT what FROM ph_log WHERE who = 'ph_child'").await
            == vec![Some("(7,AAA)".into())]
    );
}

/// `REFERENCING` on a `TRUNCATE` trigger is rejected as unsupported, before the
/// OLD/NEW event rules get a chance to report the wrong reason.
#[tokio::test]
async fn a_truncate_trigger_cannot_have_transition_tables() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE tr (a int)").await;
    run(
        &mut s,
        "CREATE FUNCTION tr_noop() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RETURN NULL; END $$",
    )
    .await;
    for clause in ["OLD TABLE AS ot", "NEW TABLE AS nt"] {
        let sql = format!(
            "CREATE TRIGGER tr_t AFTER TRUNCATE ON tr REFERENCING {clause}
             FOR EACH STATEMENT EXECUTE FUNCTION tr_noop()"
        );
        assert!(
            error(&mut s, &sql)
                .await
                .contains("TRUNCATE triggers with transition tables are not supported"),
            "{clause}"
        );
    }
}
