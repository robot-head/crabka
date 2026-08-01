use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn execute(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error:?}"))
}

async fn scalar(session: &mut SqlSession, sql: &str) -> String {
    let results = execute(session, sql).await;
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("expected rows from `{sql}`");
    };
    let cell = rows[0][0].as_ref().expect("non-null scalar");
    String::from_utf8(cell.text.to_vec()).expect("UTF-8 cell")
}

fn text(cell: &Cell) -> String {
    String::from_utf8(cell.text.to_vec()).expect("UTF-8 cell")
}

#[tokio::test]
async fn default_conflict_policy_rejects_ambiguous_select_and_update() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE vc_default (n int4); INSERT INTO vc_default VALUES (1)",
    )
    .await;

    for body in [
        "DO $$ DECLARE n int4 := 9; BEGIN PERFORM n FROM vc_default; END $$",
        "DO $$ DECLARE n int4 := 9; BEGIN UPDATE vc_default SET n = n + 1 RETURNING n INTO n; END $$",
    ] {
        let error = session
            .simple_query(body)
            .await
            .expect_err("the default policy must reject an ambiguous name");
        assert!(error.code == "42702", "{error:?}");
    }
}

#[tokio::test]
async fn directives_choose_variable_or_column_in_select_and_returning() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE vc_pick (n int4);
        CREATE TABLE vc_result (policy text, selected int4, returned int4);
        INSERT INTO vc_pick VALUES (1);
        DO $$
        #variable_conflict use_variable
        DECLARE n int4 := 40; selected int4; returned int4;
        BEGIN
          SELECT n INTO selected FROM vc_pick;
          UPDATE vc_pick SET n = n + 1 RETURNING n INTO returned;
          INSERT INTO vc_result VALUES ('variable', selected, returned);
        END
        $$;
        UPDATE vc_pick SET n = 1;
        DO $$
        #variable_conflict use_column
        DECLARE n int4 := 40; selected int4; returned int4;
        BEGIN
          SELECT n INTO selected FROM vc_pick;
          UPDATE vc_pick SET n = n + 1 RETURNING n INTO returned;
          INSERT INTO vc_result VALUES ('column', selected, returned);
        END
        $$
        ",
    )
    .await;

    let results = execute(
        &mut session,
        "SELECT policy, selected, returned FROM vc_result ORDER BY policy",
    )
    .await;
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("expected rows");
    };
    let actual: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| text(cell.as_ref().expect("non-null")))
                .collect()
        })
        .collect();
    assert!(
        actual
            == vec![
                vec!["column".to_string(), "1".to_string(), "2".to_string()],
                vec!["variable".to_string(), "40".to_string(), "40".to_string(),],
            ]
    );
}

#[tokio::test]
async fn nested_and_qualified_names_resolve_in_their_own_scope() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE vc_nested (id int4, n int4);
        CREATE TABLE vc_nested_result (nested int4, qualified int4, block_var int4);
        INSERT INTO vc_nested VALUES (1, 7);
        DO $$
        #variable_conflict use_column
        <<pl>>
        DECLARE n int4 := 99; source record; nested_value int4; qualified_value int4;
        BEGIN
          SELECT (SELECT n FROM vc_nested WHERE id = 1), source.n, pl.n
            INTO nested_value, qualified_value, n
            FROM vc_nested AS source
            WHERE source.id = 1;
          INSERT INTO vc_nested_result VALUES (nested_value, qualified_value, n);
        END pl
        $$
        ",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT nested || ',' || qualified || ',' || block_var FROM vc_nested_result",
        )
        .await
            == "7,7,99"
    );
}

#[tokio::test]
async fn conflict_binding_covers_correlated_windows_upsert_and_merge() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE vc_outer (id int4, n int4);
        CREATE TABLE vc_inner (value int4);
        CREATE TABLE vc_upsert (k int4 PRIMARY KEY, n int4);
        CREATE TABLE vc_merge_source (id int4);
        INSERT INTO vc_outer VALUES (1, 7);
        INSERT INTO vc_inner VALUES (1);
        INSERT INTO vc_upsert VALUES (1, 1);
        INSERT INTO vc_merge_source VALUES (1)
        ",
    )
    .await;

    for body in [
        "DO $$ DECLARE id int4 := 9; BEGIN PERFORM (SELECT 1 FROM vc_inner WHERE id = 1) FROM vc_outer; END $$",
        "DO $$ DECLARE id int4 := 9; BEGIN PERFORM (SELECT (SELECT 1 FROM vc_inner WHERE id = 1)) FROM vc_outer; END $$",
        "DO $$ DECLARE n int4 := 9; BEGIN PERFORM sum(n) OVER (PARTITION BY n ORDER BY n ROWS BETWEEN n PRECEDING AND CURRENT ROW) FROM vc_outer; END $$",
        "DO $$ DECLARE chosen int4 := 9; n int4; BEGIN SELECT vc_outer.n AS chosen INTO n FROM vc_outer ORDER BY chosen; END $$",
    ] {
        let error = session
            .simple_query(body)
            .await
            .expect_err("the SQL scope must expose the conflict");
        assert!(error.code == "42702", "{error:?}");
    }

    execute(
        &mut session,
        r"
        DO $$
        #variable_conflict use_variable
        DECLARE n int4 := 40;
        BEGIN
          INSERT INTO vc_upsert VALUES (1, 2)
            ON CONFLICT (k) DO UPDATE SET n = n;
          MERGE INTO vc_outer AS target USING vc_merge_source AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET n = n;
        END
        $$
        ",
    )
    .await;

    assert!(scalar(&mut session, "SELECT n FROM vc_upsert").await == "40");
    assert!(scalar(&mut session, "SELECT n FROM vc_outer").await == "40");
}
