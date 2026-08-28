//! Q1 statement completeness: `INSERT … <query>`, `UPDATE … FROM`,
//! `DELETE … USING`, `MERGE`, `CREATE TABLE … AS`, the standalone `TABLE`
//! statement, data-modifying CTEs, and `PostgreSQL` 18's `RETURNING` `OLD`/`NEW`
//! aliases. Every expectation here was taken from a live `PostgreSQL` 18.4
//! oracle.

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut impl Session, sql: &str) -> QueryResult {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .into_iter()
        .next()
        .expect("one result")
}

async fn run_err(s: &mut impl Session, sql: &str) -> String {
    match s.simple_query(sql).await {
        Ok(results) => panic!("{sql} unexpectedly succeeded: {results:?}"),
        Err(e) => e.code.clone(),
    }
}

fn tag(result: &QueryResult) -> String {
    match result {
        QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag.clone(),
        QueryResult::Empty => panic!("expected a tagged result"),
    }
}

fn grid(result: &QueryResult) -> Vec<Vec<Option<String>>> {
    match result {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| {
                        cell.as_ref()
                            .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8"))
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn names(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Rows { fields, .. } => fields.iter().map(|f| f.name.clone()).collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn cells(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

async fn seeded() -> (SqlEngine, impl Session) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE src (id int4, label text, n int4)").await;
    run(
        &mut s,
        "INSERT INTO src VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)",
    )
    .await;
    (engine, s)
}

#[tokio::test]
async fn insert_from_a_query_materializes_before_writing() {
    let (_engine, mut s) = seeded().await;
    run(&mut s, "CREATE TABLE dst (id int4, label text, n int4)").await;
    let inserted = run(&mut s, "INSERT INTO dst SELECT id, label, n FROM src").await;
    assert!(tag(&inserted) == "INSERT 0 3");

    // The feeding query reads the pre-insert snapshot, so this doubles the
    // table rather than looping.
    run(&mut s, "INSERT INTO dst SELECT id + 10, label, n FROM dst").await;
    let count = run(&mut s, "SELECT count(*) FROM dst").await;
    assert!(grid(&count) == vec![cells(&["6"])]);

    // A column list narrows the target; the rest take their defaults.
    run(
        &mut s,
        "INSERT INTO dst (id) SELECT id + 100 FROM src ORDER BY id",
    )
    .await;
    let narrowed = run(
        &mut s,
        "SELECT id, label, n FROM dst WHERE id > 100 ORDER BY id",
    )
    .await;
    assert!(
        grid(&narrowed)
            == vec![
                vec![Some("101".into()), None, None],
                vec![Some("102".into()), None, None],
                vec![Some("103".into()), None, None],
            ]
    );

    // Arity mismatches use PostgreSQL's two distinct messages, both 42601.
    assert!(
        run_err(
            &mut s,
            "INSERT INTO dst (id, label) SELECT id, label, n FROM src"
        )
        .await
            == "42601"
    );
    assert!(
        run_err(
            &mut s,
            "INSERT INTO dst (id, label, n) SELECT id, label FROM src"
        )
        .await
            == "42601"
    );
}

#[tokio::test]
async fn insert_default_values_and_explicit_defaults() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE d (id int4 DEFAULT 7, label text DEFAULT 'dflt', n int4)",
    )
    .await;
    assert!(tag(&run(&mut s, "INSERT INTO d DEFAULT VALUES").await) == "INSERT 0 1");
    run(
        &mut s,
        "INSERT INTO d VALUES (1, DEFAULT, 1), (DEFAULT, 'set', 2)",
    )
    .await;
    let all = run(&mut s, "SELECT id, label, n FROM d ORDER BY id, n").await;
    assert!(
        grid(&all)
            == vec![
                vec![Some("1".into()), Some("dflt".into()), Some("1".into())],
                vec![Some("7".into()), Some("set".into()), Some("2".into())],
                vec![Some("7".into()), Some("dflt".into()), None],
            ]
    );
}

#[tokio::test]
async fn update_from_joins_the_target_and_updates_each_row_once() {
    let (_engine, mut s) = seeded().await;
    run(&mut s, "CREATE TABLE ext (id int4, w int4, tag text)").await;
    run(
        &mut s,
        "INSERT INTO ext VALUES (1, 100, 'x'), (2, 200, 'y'), (9, 900, 'z')",
    )
    .await;

    let updated = run(
        &mut s,
        "UPDATE src SET n = n + ext.w, label = ext.tag FROM ext WHERE src.id = ext.id",
    )
    .await;
    assert!(tag(&updated) == "UPDATE 2");
    let after = run(&mut s, "SELECT id, label, n FROM src ORDER BY id").await;
    assert!(
        grid(&after)
            == vec![
                cells(&["1", "x", "110"]),
                cells(&["2", "y", "220"]),
                cells(&["3", "three", "30"]),
            ]
    );

    // Two source rows matching one target row still update it once.
    run(&mut s, "INSERT INTO ext VALUES (3, 1, 'p'), (3, 2, 'q')").await;
    let once = run(
        &mut s,
        "UPDATE src SET n = 0 FROM ext WHERE src.id = ext.id AND ext.id = 3",
    )
    .await;
    assert!(tag(&once) == "UPDATE 1");

    // An alias hides the table name.
    run(
        &mut s,
        "UPDATE src AS t SET n = 1 FROM ext AS e WHERE t.id = e.id AND e.id = 1",
    )
    .await;
    assert!(run_err(&mut s, "UPDATE src AS t SET n = src.n WHERE t.id = 1").await == "42P01");
    // RETURNING may project the joined relation.
    let returned = run(
        &mut s,
        "UPDATE src SET n = n FROM ext WHERE src.id = ext.id AND ext.id = 1 RETURNING src.id, ext.w, ext.tag",
    )
    .await;
    assert!(grid(&returned) == vec![cells(&["1", "100", "x"])]);
}

#[tokio::test]
async fn multi_column_set_covers_row_pair_and_subquery_forms() {
    let (_engine, mut s) = seeded().await;
    run(
        &mut s,
        "UPDATE src SET (label, n) = ROW('row', 1) WHERE id = 1",
    )
    .await;
    run(
        &mut s,
        "UPDATE src SET (label, n) = ('pair', 2) WHERE id = 2",
    )
    .await;
    run(
        &mut s,
        "UPDATE src SET (label, n) = (SELECT 'sub', 3) WHERE id = 3",
    )
    .await;
    let after = run(&mut s, "SELECT id, label, n FROM src ORDER BY id").await;
    assert!(
        grid(&after)
            == vec![
                cells(&["1", "row", "1"]),
                cells(&["2", "pair", "2"]),
                cells(&["3", "sub", "3"]),
            ]
    );

    // A zero-row sub-select assigns NULL to every target.
    run(
        &mut s,
        "UPDATE src SET (label, n) = (SELECT label, n FROM src WHERE id = 999) WHERE id = 1",
    )
    .await;
    let nulled = run(&mut s, "SELECT label, n FROM src WHERE id = 1").await;
    assert!(grid(&nulled) == vec![vec![None, None]]);

    let cases = [
        ("UPDATE src SET (label, n) = ROW('a', 1, 2)", "42601"),
        ("UPDATE src SET (label, n) = (SELECT 1)", "42601"),
        ("UPDATE src SET nope = 1", "42703"),
        ("UPDATE src SET n = 1, n = 2", "42601"),
    ];
    for (sql, expected) in cases {
        assert!(run_err(&mut s, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn delete_using_joins_and_returns_both_relations() {
    let (_engine, mut s) = seeded().await;
    run(&mut s, "CREATE TABLE keys (id int4, tag text)").await;
    run(&mut s, "INSERT INTO keys VALUES (1, 'x'), (3, 'z')").await;
    let deleted = run(
        &mut s,
        "DELETE FROM src USING keys WHERE src.id = keys.id AND keys.id = 1 RETURNING src.id, keys.tag",
    )
    .await;
    assert!(tag(&deleted) == "DELETE 1");
    assert!(grid(&deleted) == vec![cells(&["1", "x"])]);
    let left = run(&mut s, "SELECT id FROM src ORDER BY id").await;
    assert!(grid(&left) == vec![cells(&["2"]), cells(&["3"])]);

    let none = run(
        &mut s,
        "DELETE FROM src USING keys WHERE src.id = keys.id AND keys.id = 42",
    )
    .await;
    assert!(tag(&none) == "DELETE 0");
}

#[tokio::test]
async fn returning_old_and_new_images_follow_pg18() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE r (id int4, v int4)").await;

    // INSERT: the pre-image is all NULL.
    let inserted = run(
        &mut s,
        "INSERT INTO r VALUES (1, 10) RETURNING old.v, new.v, old, new",
    )
    .await;
    assert!(grid(&inserted) == vec![vec![None, Some("10".into()), None, Some("(1,10)".into())]]);

    // UPDATE: a bare name means the post-image; `*` never expands the images.
    let updated = run(
        &mut s,
        "UPDATE r SET v = v + 1 WHERE id = 1 RETURNING v, old.v, new.v",
    )
    .await;
    assert!(grid(&updated) == vec![cells(&["11", "10", "11"])]);
    let starred = run(&mut s, "UPDATE r SET v = v + 1 WHERE id = 1 RETURNING *").await;
    assert!(grid(&starred) == vec![cells(&["1", "12"])]);
    assert!(names(&starred) == vec!["id".to_string(), "v".to_string()]);

    // The explicit alias list, including one that names only OLD.
    let aliased = run(
        &mut s,
        "UPDATE r SET v = v + 1 WHERE id = 1 RETURNING WITH (OLD AS o, NEW AS n) o.v, n.v, n.v - o.v",
    )
    .await;
    assert!(grid(&aliased) == vec![cells(&["12", "13", "1"])]);
    let old_only = run(
        &mut s,
        "UPDATE r SET v = v + 1 WHERE id = 1 RETURNING WITH (OLD AS b) b.v, new.v",
    )
    .await;
    assert!(grid(&old_only) == vec![cells(&["13", "14"])]);

    // A bare image name is the whole composite row, unless an ordinary output
    // column of that name takes precedence.
    let whole_images = run(
        &mut s,
        "UPDATE r SET v = v + 1 WHERE id = 1 RETURNING old, new",
    )
    .await;
    assert!(grid(&whole_images) == vec![cells(&["(1,14)", "(1,15)"])]);
    assert!(names(&whole_images) == vec!["old".to_string(), "new".to_string()]);

    // DELETE: the post-image is all NULL.
    let deleted = run(
        &mut s,
        "DELETE FROM r WHERE id = 1 RETURNING old.id, old.v, new.v, old, new",
    )
    .await;
    assert!(
        grid(&deleted)
            == vec![vec![
                Some("1".into()),
                Some("15".into()),
                None,
                Some("(1,15)".into()),
                None,
            ]]
    );

    run(&mut s, "INSERT INTO r VALUES (2, 20)").await;
    assert!(run_err(&mut s, "UPDATE r SET v = 1 RETURNING old.nope").await == "42703");
}

#[tokio::test]
async fn merge_covers_every_when_clause_and_merge_action() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, v int4, label text)").await;
    run(&mut s, "CREATE TABLE u (id int4, w int4, tag text)").await;
    run(
        &mut s,
        "INSERT INTO t VALUES (1, 10, 'one'), (2, 20, 'two'), (3, 30, 'three')",
    )
    .await;
    run(
        &mut s,
        "INSERT INTO u VALUES (1, 100, 'x'), (3, 300, 'y'), (5, 500, 'z')",
    )
    .await;

    let merged = run(
        &mut s,
        "MERGE INTO t USING u ON t.id = u.id \
         WHEN MATCHED THEN UPDATE SET v = u.w, label = u.tag \
         WHEN NOT MATCHED THEN INSERT (id, v, label) VALUES (u.id, u.w, u.tag)",
    )
    .await;
    assert!(tag(&merged) == "MERGE 3");
    let after = run(&mut s, "SELECT id, v, label FROM t ORDER BY id").await;
    assert!(
        grid(&after)
            == vec![
                cells(&["1", "100", "x"]),
                cells(&["2", "20", "two"]),
                cells(&["3", "300", "y"]),
                cells(&["5", "500", "z"]),
            ]
    );

    // WHEN MATCHED AND / DELETE / DO NOTHING, first matching clause wins.
    run(
        &mut s,
        "MERGE INTO t USING u ON t.id = u.id \
         WHEN MATCHED AND u.w >= 500 THEN DELETE \
         WHEN MATCHED AND u.w >= 300 THEN UPDATE SET v = t.v + 1 \
         WHEN MATCHED THEN DO NOTHING",
    )
    .await;
    let after = run(&mut s, "SELECT id, v FROM t ORDER BY id").await;
    assert!(
        grid(&after)
            == vec![
                cells(&["1", "100"]),
                cells(&["2", "20"]),
                cells(&["3", "301"])
            ]
    );

    // WHEN NOT MATCHED BY SOURCE fires for target rows no source row joined.
    let orphans = run(
        &mut s,
        "MERGE INTO t USING u ON t.id = u.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET label = 'orphan'",
    )
    .await;
    assert!(tag(&orphans) == "MERGE 1");
    let after = run(&mut s, "SELECT id, label FROM t ORDER BY id").await;
    assert!(
        grid(&after)
            == vec![
                cells(&["1", "x"]),
                cells(&["2", "orphan"]),
                cells(&["3", "y"]),
            ]
    );

    // RETURNING projects merge_action(), the source, and the OLD/NEW images.
    let returned = run(
        &mut s,
        "MERGE INTO t AS a USING (SELECT 1 AS id, 999 AS w) AS b ON a.id = b.id \
         WHEN MATCHED THEN UPDATE SET v = b.w \
         RETURNING merge_action(), b.w, old.v, new.v",
    )
    .await;
    assert!(grid(&returned) == vec![cells(&["UPDATE", "999", "100", "999"])]);
    assert!(names(&returned)[0] == "merge_action");

    let inserted = run(
        &mut s,
        "MERGE INTO t AS a USING (SELECT 21 AS id) AS b ON a.id = b.id \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (b.id, 1) \
         RETURNING merge_action(), a.id, a.v",
    )
    .await;
    assert!(grid(&inserted) == vec![cells(&["INSERT", "21", "1"])]);

    let removed = run(
        &mut s,
        "MERGE INTO t AS a USING (SELECT 21 AS id) AS b ON a.id = b.id \
         WHEN MATCHED THEN DELETE RETURNING merge_action(), a.id, a.v",
    )
    .await;
    assert!(grid(&removed) == vec![cells(&["DELETE", "21", "1"])]);

    // `RETURNING *` lists the source relation before the target.
    let starred = run(
        &mut s,
        "MERGE INTO t AS a USING (SELECT 1 AS id, 5 AS w) AS b ON a.id = b.id \
         WHEN MATCHED THEN UPDATE SET v = b.w RETURNING *",
    )
    .await;
    assert!(names(&starred) == vec!["id", "w", "id", "v", "label"]);
}

#[tokio::test]
async fn create_table_as_and_select_into_populate_and_type_the_new_table() {
    let (_engine, mut s) = seeded().await;
    let created = run(&mut s, "CREATE TABLE copy1 AS SELECT id, label FROM src").await;
    assert!(tag(&created) == "SELECT 3");
    let copied = run(&mut s, "SELECT id, label FROM copy1 ORDER BY id").await;
    assert!(grid(&copied).len() == 3);

    let empty = run(
        &mut s,
        "CREATE TABLE copy2 AS SELECT id FROM src WITH NO DATA",
    )
    .await;
    assert!(tag(&empty) == "CREATE TABLE AS");
    assert!(grid(&run(&mut s, "SELECT count(*) FROM copy2").await) == vec![cells(&["0"])]);

    // An explicit column list renames the leading columns.
    run(
        &mut s,
        "CREATE TABLE named (a, b) AS SELECT id, label FROM src",
    )
    .await;
    let renamed = run(&mut s, "SELECT a, b FROM named ORDER BY a").await;
    assert!(names(&renamed) == vec!["a", "b"]);
    assert!(grid(&renamed).len() == 3);
    assert!(
        run_err(
            &mut s,
            "CREATE TABLE toomany (a, b, c) AS SELECT id, label FROM src"
        )
        .await
            == "42601"
    );

    // IF NOT EXISTS over an existing relation is a notice, not a 42P07.
    assert!(run_err(&mut s, "CREATE TABLE copy1 AS SELECT 1").await == "42P07");
    let skipped = run(&mut s, "CREATE TABLE IF NOT EXISTS copy1 AS SELECT 1").await;
    assert!(tag(&skipped) == "CREATE TABLE AS");
    assert!(grid(&run(&mut s, "SELECT count(*) FROM copy1").await) == vec![cells(&["3"])]);

    // SELECT ... INTO is the same statement, tagged `SELECT n`.
    let into = run(&mut s, "SELECT id, label INTO copy3 FROM src WHERE id <= 2").await;
    assert!(tag(&into) == "SELECT 2");
    assert!(grid(&run(&mut s, "SELECT count(*) FROM copy3").await) == vec![cells(&["2"])]);

    // A failed source query leaves no relation behind.
    assert!(run_err(&mut s, "CREATE TABLE bad AS SELECT nope FROM src").await == "42703");
    assert!(run_err(&mut s, "SELECT count(*) FROM bad").await == "42P01");
}

#[tokio::test]
async fn table_statement_works_wherever_a_query_expression_does() {
    let (_engine, mut s) = seeded().await;
    let direct = run(&mut s, "TABLE src").await;
    assert!(grid(&direct).len() == 3);
    assert!(names(&direct) == vec!["id", "label", "n"]);

    let tailed = run(&mut s, "TABLE src ORDER BY id DESC LIMIT 1").await;
    assert!(grid(&tailed) == vec![cells(&["3", "three", "30"])]);

    let union = run(&mut s, "TABLE src UNION ALL TABLE src ORDER BY id, label").await;
    assert!(grid(&union).len() == 6);

    let in_cte = run(&mut s, "WITH c AS (TABLE src) SELECT id FROM c ORDER BY id").await;
    assert!(grid(&in_cte) == vec![cells(&["1"]), cells(&["2"]), cells(&["3"])]);

    run(&mut s, "CREATE TABLE mirror (id int4, label text, n int4)").await;
    assert!(tag(&run(&mut s, "INSERT INTO mirror TABLE src").await) == "INSERT 0 3");
    assert!(run_err(&mut s, "TABLE nope").await == "42P01");
}

#[tokio::test]
async fn data_modifying_ctes_run_once_and_never_see_their_own_effects() {
    let (_engine, mut s) = seeded().await;

    // The body sees the CTE's RETURNING output but not its rows in the table.
    let inserted = run(
        &mut s,
        "WITH i AS (INSERT INTO src VALUES (4, 'four', 40) RETURNING id, label) \
         SELECT id, label FROM i",
    )
    .await;
    assert!(grid(&inserted) == vec![cells(&["4", "four"])]);
    let hidden = run(
        &mut s,
        "WITH i AS (INSERT INTO src VALUES (5, 'five', 50) RETURNING id) SELECT count(*) FROM src",
    )
    .await;
    assert!(grid(&hidden) == vec![cells(&["4"])]);
    assert!(grid(&run(&mut s, "SELECT count(*) FROM src").await) == vec![cells(&["5"])]);

    // An unreferenced data-modifying CTE still runs exactly once.
    run(
        &mut s,
        "WITH i AS (INSERT INTO src VALUES (6, 'six', 60) RETURNING id) SELECT 1",
    )
    .await;
    assert!(grid(&run(&mut s, "SELECT count(*) FROM src").await) == vec![cells(&["6"])]);

    // UPDATE and DELETE CTEs, plus a column alias list.
    let updated = run(
        &mut s,
        "WITH u AS (UPDATE src SET n = n + 1 WHERE id = 1 RETURNING id, n) SELECT id, n FROM u",
    )
    .await;
    assert!(grid(&updated) == vec![cells(&["1", "11"])]);
    let deleted = run(
        &mut s,
        "WITH d (k, m) AS (DELETE FROM src WHERE id >= 5 RETURNING id, n) SELECT k, m FROM d ORDER BY k",
    )
    .await;
    assert!(grid(&deleted) == vec![cells(&["5", "50"]), cells(&["6", "60"])]);

    // A CTE without RETURNING cannot be referenced, but still runs.
    assert!(
        run_err(
            &mut s,
            "WITH i AS (INSERT INTO src VALUES (7, 'seven', 70)) SELECT * FROM i"
        )
        .await
            == "0A000"
    );
    run(
        &mut s,
        "WITH i AS (INSERT INTO src VALUES (8, 'eight', 80)) SELECT 1",
    )
    .await;
    let ids = run(&mut s, "SELECT id FROM src ORDER BY id").await;
    assert!(
        grid(&ids)
            == vec![
                cells(&["1"]),
                cells(&["2"]),
                cells(&["3"]),
                cells(&["4"]),
                cells(&["8"]),
            ]
    );

    // A DML body under a WITH: the move-rows idiom.
    run(&mut s, "CREATE TABLE archive (id int4, label text)").await;
    let moved = run(
        &mut s,
        "WITH m AS (DELETE FROM src WHERE id >= 4 RETURNING id, label) \
         INSERT INTO archive SELECT id, label FROM m",
    )
    .await;
    assert!(tag(&moved) == "INSERT 0 2");
    let archived = run(&mut s, "SELECT id FROM archive ORDER BY id").await;
    assert!(grid(&archived) == vec![cells(&["4"]), cells(&["8"])]);
    assert!(grid(&run(&mut s, "SELECT count(*) FROM src").await) == vec![cells(&["3"])]);

    // An error in the body discards the CTE's writes too.
    assert!(
        run_err(
            &mut s,
            "WITH i AS (INSERT INTO archive VALUES (99, 'x') RETURNING id) SELECT nope FROM i"
        )
        .await
            == "42703"
    );
    assert!(grid(&run(&mut s, "SELECT count(*) FROM archive").await) == vec![cells(&["2"])]);
}

#[tokio::test]
async fn merge_inside_a_with_list_runs_on_the_write_path() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    let merged = run(
        &mut s,
        "WITH m AS (MERGE INTO t USING (SELECT 3 AS id) AS x ON t.id = x.id \
                      WHEN NOT MATCHED THEN INSERT (id) VALUES (x.id) \
                      RETURNING merge_action(), t.id) \
         SELECT * FROM m",
    )
    .await;
    assert!(grid(&merged) == vec![cells(&["INSERT", "3"])]);
    assert!(grid(&run(&mut s, "SELECT id FROM t").await) == vec![cells(&["3"])]);
}

/// A statement's `WITH` items and its body are one command, so a unique index is
/// enforced across all of them. Before this, each part kept its own pending-key
/// set and two parts could both write the same primary key.
#[tokio::test]
async fn unique_keys_are_enforced_across_every_part_of_one_statement() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE pk (id int4 PRIMARY KEY, v int4)").await;

    // A WITH item's INSERT and the body's INSERT collide.
    assert!(
        run_err(
            &mut s,
            "WITH i AS (INSERT INTO pk VALUES (50, 1) RETURNING id) INSERT INTO pk VALUES (50, 2)"
        )
        .await
            == "23505"
    );
    assert!(grid(&run(&mut s, "SELECT count(*) FROM pk").await) == vec![cells(&["0"])]);

    // Two WITH items collide with each other, with no DML in the body at all.
    assert!(
        run_err(
            &mut s,
            "WITH a AS (INSERT INTO pk VALUES (1, 1) RETURNING id), \
                  b AS (INSERT INTO pk VALUES (1, 2) RETURNING id) \
             SELECT (SELECT count(*) FROM a) + (SELECT count(*) FROM b)"
        )
        .await
            == "23505"
    );
    assert!(grid(&run(&mut s, "SELECT count(*) FROM pk").await) == vec![cells(&["0"])]);

    // An UPDATE item re-keying a row collides with the body's INSERT of that key.
    run(&mut s, "INSERT INTO pk VALUES (1, 1), (2, 2)").await;
    assert!(
        run_err(
            &mut s,
            "WITH u AS (UPDATE pk SET id = 5 WHERE id = 1 RETURNING id) \
             INSERT INTO pk SELECT 5, 9 FROM u"
        )
        .await
            == "23505"
    );
    assert!(
        grid(&run(&mut s, "SELECT id, v FROM pk ORDER BY id").await)
            == vec![cells(&["1", "1"]), cells(&["2", "2"])]
    );

    // The key a part frees IS available to a later part: PostgreSQL's uniqueness
    // check ignores a tuple its own command has already superseded.
    let moved = run(
        &mut s,
        "WITH d AS (DELETE FROM pk WHERE id = 1 RETURNING id) INSERT INTO pk SELECT 1, 7 FROM d",
    )
    .await;
    assert!(tag(&moved) == "INSERT 0 1");
    assert!(
        grid(&run(&mut s, "SELECT id, v FROM pk ORDER BY id").await)
            == vec![cells(&["1", "7"]), cells(&["2", "2"])]
    );

    // Re-keying every row downwards in one UPDATE is legal for the same reason.
    run(&mut s, "CREATE TABLE shift (id int4 PRIMARY KEY)").await;
    run(&mut s, "INSERT INTO shift VALUES (2), (3)").await;
    let shifted = run(&mut s, "UPDATE shift SET id = id - 1").await;
    assert!(tag(&shifted) == "UPDATE 2");
    assert!(
        grid(&run(&mut s, "SELECT id FROM shift ORDER BY id").await)
            == vec![cells(&["1"]), cells(&["2"])]
    );
    // Re-keying upwards still collides, because the row holding the new key has
    // not been reached yet.
    assert!(run_err(&mut s, "UPDATE shift SET id = id + 1").await == "23505");
}

/// A row one part of a statement modified is never modified again by another.
#[tokio::test]
async fn a_row_is_modified_at_most_once_per_statement() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE ov (id int4)").await;
    run(&mut s, "INSERT INTO ov VALUES (1), (2), (3)").await;

    // Both items are demanded by the body, so they run in list order: the second
    // DELETE finds id = 2 already deleted and neither deletes nor returns it.
    let counts = run(
        &mut s,
        "WITH a AS (DELETE FROM ov WHERE id <= 2 RETURNING id), \
              b AS (DELETE FROM ov WHERE id >= 2 RETURNING id) \
         SELECT (SELECT count(*) FROM a) AS na, (SELECT count(*) FROM b) AS nb",
    )
    .await;
    assert!(grid(&counts) == vec![cells(&["2", "1"])]);
    assert!(grid(&run(&mut s, "SELECT count(*) FROM ov").await) == vec![cells(&["0"])]);

    // The same rule over two UPDATEs: the second leaves the shared row alone.
    run(&mut s, "CREATE TABLE m (id int4, v int4)").await;
    run(&mut s, "INSERT INTO m VALUES (1, 10), (2, 20), (3, 30)").await;
    let updated = run(
        &mut s,
        "WITH a AS (UPDATE m SET v = v + 1 WHERE id <= 2 RETURNING id), \
              b AS (UPDATE m SET v = v + 100 WHERE id >= 2 RETURNING id) \
         SELECT (SELECT count(*) FROM a) AS na, (SELECT count(*) FROM b) AS nb",
    )
    .await;
    assert!(grid(&updated) == vec![cells(&["2", "1"])]);
    assert!(
        grid(&run(&mut s, "SELECT id, v FROM m ORDER BY id").await)
            == vec![
                cells(&["1", "11"]),
                cells(&["2", "21"]),
                cells(&["3", "130"]),
            ]
    );

    // A demanded DELETE item runs before the body, which then skips the row.
    run(&mut s, "CREATE TABLE d (id int4, v int4)").await;
    run(&mut s, "INSERT INTO d VALUES (1, 10), (2, 20), (3, 30)").await;
    let body = run(
        &mut s,
        "WITH a AS (DELETE FROM d WHERE id <= 2 RETURNING id) \
         UPDATE d SET v = v + 1 FROM a WHERE d.id >= 2 AND a.id = 2 RETURNING d.id",
    )
    .await;
    assert!(grid(&body) == vec![cells(&["3"])]);
    assert!(
        grid(&run(&mut s, "SELECT id, v FROM d ORDER BY id").await) == vec![cells(&["3", "31"])]
    );
}

/// `ON CONFLICT DO UPDATE` and `MERGE` report 21000 rather than skipping when the
/// row they would touch was already modified by another part of the statement.
#[tokio::test]
async fn a_second_touch_from_an_upsert_or_merge_is_21000() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE oc (id int4 PRIMARY KEY, v int4)").await;
    run(&mut s, "INSERT INTO oc VALUES (1, 1), (2, 2)").await;
    assert!(
        run_err(
            &mut s,
            "WITH a AS (UPDATE oc SET v = 10 WHERE id = 1 RETURNING id) \
             INSERT INTO oc SELECT 1, 99 FROM a ON CONFLICT (id) DO UPDATE SET v = 100"
        )
        .await
            == "21000"
    );
    assert!(
        run_err(
            &mut s,
            "WITH a AS (UPDATE oc SET v = 10 WHERE id = 1 RETURNING id) \
             MERGE INTO oc USING (SELECT id AS k FROM a) s ON oc.id = s.k \
             WHEN MATCHED THEN UPDATE SET v = 50"
        )
        .await
            == "21000"
    );
    run(&mut s, "CREATE TABLE merge_source (id int4)").await;
    run(&mut s, "INSERT INTO merge_source VALUES (2), (2)").await;
    let error = s
        .simple_query(
            "MERGE INTO oc USING merge_source ON oc.id = merge_source.id \
             WHEN MATCHED THEN UPDATE SET v = 50",
        )
        .await
        .expect_err("a repeated MERGE match must fail");
    assert!(error.code == "21000");
    assert!(
        error
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.hint.as_deref())
            == Some("Ensure that not more than one source row matches any one target row.")
    );
    let duplicate_name = s
        .simple_query("MERGE INTO oc USING oc ON id = id WHEN MATCHED THEN DO NOTHING")
        .await
        .expect_err("MERGE target and source names must not collide");
    assert!(duplicate_name.code == "42712");
    assert!(duplicate_name.message == "name \"oc\" specified more than once");
    assert!(
        duplicate_name
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.detail.as_deref())
            == Some("The name is used both as MERGE target table and data source.")
    );
    let system_column = s
        .simple_query(
            "MERGE INTO oc t USING merge_source s ON t.id = s.id \
             WHEN MATCHED AND t.xmin = t.xmax THEN DO NOTHING",
        )
        .await
        .expect_err("MERGE WHEN may not read a target MVCC system column");
    assert!(system_column.message == "cannot use system column \"xmin\" in MERGE WHEN condition");
    let source_target = s
        .simple_query(
            "MERGE INTO oc t USING (SELECT id FROM merge_source WHERE t.id > id) s \
             ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, 0)",
        )
        .await
        .expect_err("a MERGE source query cannot reference its target");
    assert!(source_target.code == "42P01");
    assert!(source_target.message == "invalid reference to FROM-clause entry for table \"t\"");
    assert!(
        source_target
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.detail.as_deref())
            == Some(
                "There is an entry for table \"t\", but it cannot be referenced from this part of the query."
            )
    );
    run(&mut s, "CREATE TABLE merge_unreachable_source (id int4)").await;
    run(&mut s, "INSERT INTO merge_unreachable_source VALUES (1)").await;
    run(
        &mut s,
        "MERGE INTO oc t USING merge_unreachable_source s ON t.id = s.id \
         WHEN MATCHED AND t.tableoid >= 0 THEN UPDATE SET v = 7",
    )
    .await;
    let unreachable = s
        .simple_query(
            "MERGE INTO oc t USING merge_unreachable_source s ON t.id = s.id \
             WHEN MATCHED THEN DELETE WHEN MATCHED THEN UPDATE SET v = 0",
        )
        .await
        .expect_err("a MERGE clause after an unconditional clause is unreachable");
    assert!(unreachable.code == "42601");
    assert!(
        unreachable.message == "unreachable WHEN clause specified after unconditional WHEN clause"
    );
    for (sql, table) in [
        (
            "MERGE INTO oc t USING merge_source s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (t.id, s.id)",
            "t",
        ),
        (
            "MERGE INTO oc t USING merge_source s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE AND s.id = 2 THEN DELETE",
            "s",
        ),
    ] {
        let inaccessible = s
            .simple_query(sql)
            .await
            .expect_err("MERGE clause must reject its absent join relation");
        assert!(inaccessible.code == "42P01");
        assert!(
            inaccessible.message
                == format!("invalid reference to FROM-clause entry for table \"{table}\"")
        );
        let expected_detail = format!(
            "There is an entry for table \"{table}\", but it cannot be referenced from this part of the query."
        );
        assert!(
            inaccessible
                .diagnostics
                .as_deref()
                .and_then(|diagnostics| diagnostics.detail.as_deref())
                == Some(expected_detail.as_str())
        );
    }
    run(&mut s, "CREATE TABLE merge_scope_source (id int4, v int4)").await;
    run(&mut s, "INSERT INTO merge_scope_source VALUES (3, 30)").await;
    run(
        &mut s,
        "MERGE INTO oc t USING merge_scope_source s ON false \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, v)",
    )
    .await;
    run(
        &mut s,
        "MERGE INTO oc t USING merge_scope_source s ON false \
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = v + 1",
    )
    .await;
    assert!(
        grid(&run(&mut s, "SELECT id, v FROM oc ORDER BY id").await)
            == vec![cells(&["1", "8"]), cells(&["2", "3"]), cells(&["3", "31"]),]
    );
}

/// A `WITH` item nothing demands runs after the body, in reverse list order.
/// That is `PostgreSQL`'s `ExecPostprocessPlan` order, and it decides whose
/// change survives.
#[tokio::test]
async fn undemanded_with_items_run_after_the_body_in_reverse_order() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE r (id int4, v int4)").await;
    run(&mut s, "INSERT INTO r VALUES (1, 10), (2, 20), (3, 30)").await;

    // Neither item is demanded, so b runs first and wins the shared row.
    run(
        &mut s,
        "WITH a AS (UPDATE r SET v = v + 1 WHERE id <= 2 RETURNING id), \
              b AS (UPDATE r SET v = v + 100 WHERE id >= 2 RETURNING id) \
         SELECT 0",
    )
    .await;
    assert!(
        grid(&run(&mut s, "SELECT id, v FROM r ORDER BY id").await)
            == vec![
                cells(&["1", "11"]),
                cells(&["2", "120"]),
                cells(&["3", "130"]),
            ]
    );

    // The body runs before an undemanded item, so the upsert updates the row and
    // the item then leaves it alone.
    run(&mut s, "CREATE TABLE u (id int4 PRIMARY KEY, v int4)").await;
    run(&mut s, "INSERT INTO u VALUES (1, 1), (2, 2)").await;
    let upserted = run(
        &mut s,
        "WITH a AS (UPDATE u SET v = 10 WHERE id = 1 RETURNING id) \
         INSERT INTO u VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET v = 100",
    )
    .await;
    assert!(tag(&upserted) == "INSERT 0 1");
    assert!(
        grid(&run(&mut s, "SELECT id, v FROM u ORDER BY id").await)
            == vec![cells(&["1", "100"]), cells(&["2", "2"])]
    );
}

/// With no explicit column list, the engine truncates the implicit target list
/// to the source width. Only an explicit list makes "too few expressions" an
/// error.
#[tokio::test]
async fn an_implicit_insert_target_list_is_truncated_to_the_source_width() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE wide (a int4, b text, c int4)").await;
    run(&mut s, "CREATE TABLE narrow (a int4, b text)").await;
    run(&mut s, "INSERT INTO narrow VALUES (1, 'one')").await;

    for sql in [
        "INSERT INTO wide SELECT a, b FROM narrow",
        "INSERT INTO wide VALUES (2, 'two')",
        "INSERT INTO wide (a, b) VALUES (3, 'three')",
    ] {
        assert!(tag(&run(&mut s, sql).await) == "INSERT 0 1");
    }
    assert!(
        grid(&run(&mut s, "SELECT a, b, c FROM wide ORDER BY a").await)
            == vec![
                vec![Some("1".into()), Some("one".into()), None],
                vec![Some("2".into()), Some("two".into()), None],
                vec![Some("3".into()), Some("three".into()), None],
            ]
    );

    // Both error directions, on the VALUES and query paths alike.
    let rejected = [
        ("INSERT INTO wide SELECT a, b, 1, 2 FROM narrow", "42601"),
        ("INSERT INTO wide VALUES (1, 'x', 2, 3)", "42601"),
        (
            "INSERT INTO wide (a, b, c) SELECT a, b FROM narrow",
            "42601",
        ),
        ("INSERT INTO wide (a, b, c) VALUES (1, 'x')", "42601"),
        ("INSERT INTO wide (a) SELECT a, b FROM narrow", "42601"),
        ("INSERT INTO wide VALUES (1, 'x'), (2)", "42601"),
    ];
    for (sql, code) in rejected {
        assert!(run_err(&mut s, sql).await == code, "{sql}");
    }
}

/// A `MERGE` insert action obeys the same arity rule as a plain `INSERT`.
#[tokio::test]
async fn a_merge_insert_action_fills_the_columns_its_values_omit() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE mt (a int4, b text, c int4)").await;
    run(&mut s, "CREATE TABLE ms (k int4)").await;
    run(&mut s, "INSERT INTO ms VALUES (1)").await;
    let merged = run(
        &mut s,
        "MERGE INTO mt USING ms ON mt.a = ms.k WHEN NOT MATCHED THEN INSERT VALUES (5, 'five')",
    )
    .await;
    assert!(tag(&merged) == "MERGE 1");
    assert!(
        grid(&run(&mut s, "SELECT a, b, c FROM mt").await)
            == vec![vec![Some("5".into()), Some("five".into()), None]]
    );
    for sql in [
        "MERGE INTO mt USING ms ON mt.a = ms.k WHEN NOT MATCHED THEN INSERT (a) VALUES (7, 'x')",
        "MERGE INTO mt USING ms ON mt.a = ms.k WHEN NOT MATCHED THEN INSERT (a, b) VALUES (8)",
    ] {
        assert!(run_err(&mut s, sql).await == "42601", "{sql}");
    }
}

/// `CREATE TABLE … AS` is all-or-nothing from the client's point of view. A
/// runtime failure in the source query leaves no relation behind, so the
/// ordinary fix-and-retry works.
#[tokio::test]
async fn create_table_as_leaves_no_relation_behind_when_the_query_fails() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    assert!(run_err(&mut s, "CREATE TABLE boom AS SELECT 1 / 0 AS b").await == "22012");
    assert!(run_err(&mut s, "SELECT count(*) FROM boom").await == "42P01");

    run(&mut s, "CREATE TABLE src2 (n int4)").await;
    run(&mut s, "INSERT INTO src2 VALUES (2), (1), (0)").await;
    assert!(run_err(&mut s, "CREATE TABLE q AS SELECT 100 / n AS r FROM src2").await == "22012");
    run(&mut s, "DELETE FROM src2 WHERE n = 0").await;
    let retried = run(&mut s, "CREATE TABLE q AS SELECT 100 / n AS r FROM src2").await;
    assert!(tag(&retried) == "SELECT 2");
}

/// Two output columns of the same name would build a relation that cannot be
/// read. `PostgreSQL` rejects the definition instead.
#[tokio::test]
async fn create_table_as_rejects_duplicate_output_column_names() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE base (id int4)").await;
    run(&mut s, "INSERT INTO base VALUES (1)").await;
    for sql in [
        "CREATE TABLE dup1 AS SELECT id, id FROM base",
        "CREATE TABLE dup2 AS SELECT 1 + 1, 2 + 2",
        "CREATE TABLE dup3 (x, x) AS SELECT id, id + 1 FROM base",
    ] {
        assert!(run_err(&mut s, sql).await == "42701", "{sql}");
    }
    for sql in [
        "SELECT * FROM dup1",
        "SELECT * FROM dup2",
        "SELECT * FROM dup3",
    ] {
        assert!(run_err(&mut s, sql).await == "42P01", "{sql}");
    }
}

/// An explicit `RETURNING WITH` image alias is a relation name. It must not
/// collide with another relation in scope or with the other image, and it
/// suppresses the other image's default spelling.
#[tokio::test]
async fn returning_image_aliases_follow_postgresql_naming_rules() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE im (v int4)").await;
    run(&mut s, "INSERT INTO im VALUES (1)").await;

    for sql in [
        "UPDATE im SET v = v RETURNING WITH (OLD AS o, NEW AS o) o.v",
        "UPDATE im SET v = v RETURNING WITH (OLD AS im) im.v",
        "UPDATE im AS t SET v = v RETURNING WITH (NEW AS t) t.v",
        "INSERT INTO im VALUES (2) RETURNING WITH (OLD AS z, NEW AS z) z.v",
        "DELETE FROM im WHERE v = 99 RETURNING WITH (OLD AS y, NEW AS y) y.v",
    ] {
        assert!(run_err(&mut s, sql).await == "42712", "{sql}");
    }

    // `NEW AS old` binds `old` to the POST-image: an explicit alias wins over
    // the other image's default spelling.
    let renamed = run(
        &mut s,
        "UPDATE im SET v = v + 1 RETURNING WITH (NEW AS old) old.v",
    )
    .await;
    assert!(grid(&renamed) == vec![cells(&["2"])]);
    let flipped = run(
        &mut s,
        "UPDATE im SET v = v + 1 RETURNING WITH (OLD AS new) new.v",
    )
    .await;
    assert!(grid(&flipped) == vec![cells(&["2"])]);
}

/// Error paths whose SQLSTATE `PostgreSQL` fixes by context rather than by shape.
#[tokio::test]
async fn misplaced_merge_action_and_qualified_set_targets_report_postgresqls_sqlstate() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE q (a int4, b int4)").await;
    let cases = [
        ("SELECT merge_action()", "42601"),
        ("SELECT merge_action() FROM q", "42601"),
        ("UPDATE q SET q.a = 1", "42703"),
        ("UPDATE q AS t SET t.a = 1", "42703"),
        (
            "MERGE INTO q USING (SELECT 1 AS k) s ON q.a = s.k WHEN MATCHED THEN UPDATE SET q.a = 1",
            "42703",
        ),
        ("UPDATE q SET a = 1, a = 2", "42601"),
        ("UPDATE q SET (a, a) = ROW(1, 2)", "42601"),
    ];
    for (sql, code) in cases {
        assert!(run_err(&mut s, sql).await == code, "{sql}");
    }
}
