//! Row triggers implemented by the static PostgreSQL regression C module.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8"))
}

async fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match &run(session, sql).await[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|cell| cell_text(cell.as_ref())).collect())
            .collect(),
        result => panic!("expected rows, got {result:?}"),
    }
}

#[tokio::test]
async fn trigger_return_old_uses_the_c_trigger_tuple() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE FUNCTION trigger_return_old() RETURNS trigger AS 'regress' LANGUAGE C;
         CREATE TABLE t (id int, value text);
         CREATE TRIGGER t_return_old BEFORE INSERT OR UPDATE OR DELETE ON t
         FOR EACH ROW EXECUTE FUNCTION trigger_return_old()",
    )
    .await;

    run(&mut session, "INSERT INTO t VALUES (1, 'before')").await;
    run(&mut session, "UPDATE t SET value = 'after'").await;
    assert!(
        rows(&mut session, "SELECT id, value FROM t").await
            == vec![vec![Some("1".into()), Some("before".into())]]
    );
    run(&mut session, "DELETE FROM t").await;
    assert!(rows(&mut session, "SELECT * FROM t").await.is_empty());
}

#[tokio::test]
async fn trigger_comments_are_catalogued_and_clearable() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE FUNCTION trigger_comment_fn() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN RETURN NEW; END $$;
         CREATE TABLE trigger_comment_table (id int);
         CREATE TRIGGER trigger_comment BEFORE INSERT ON trigger_comment_table
         FOR EACH ROW EXECUTE FUNCTION trigger_comment_fn()",
    )
    .await;

    let missing = session
        .simple_query("COMMENT ON TRIGGER missing ON trigger_comment_table IS 'nope'")
        .await
        .expect_err("missing trigger is rejected");
    assert!(missing.code == "42704");
    assert!(
        missing.message == "trigger \"missing\" for table \"trigger_comment_table\" does not exist"
    );

    run(
        &mut session,
        "COMMENT ON TRIGGER trigger_comment ON trigger_comment_table IS 'catalogued'",
    )
    .await;
    assert!(
        rows(
            &mut session,
            "SELECT description FROM pg_description WHERE classoid = 'pg_trigger'::regclass
             AND objoid = (SELECT oid FROM pg_trigger WHERE tgname = 'trigger_comment')",
        )
        .await
            == vec![vec![Some("catalogued".into())]]
    );

    run(
        &mut session,
        "COMMENT ON TRIGGER trigger_comment ON trigger_comment_table IS NULL",
    )
    .await;
    assert!(
        rows(
            &mut session,
            "SELECT description FROM pg_description WHERE classoid = 'pg_trigger'::regclass
             AND objoid = (SELECT oid FROM pg_trigger WHERE tgname = 'trigger_comment')",
        )
        .await
        .is_empty()
    );
}

#[tokio::test]
async fn copy_from_stdin_runs_plpgsql_triggers() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE FUNCTION copy_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN NEW.value := upper(NEW.value); RETURN NEW; END $$;
         CREATE TABLE copy_trigger_table (value text);
         CREATE TABLE copy_trigger_log (value text);
         CREATE FUNCTION copy_after_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN INSERT INTO copy_trigger_log VALUES (NEW.value); RETURN NEW; END $$;
         CREATE TRIGGER copy_trigger BEFORE INSERT ON copy_trigger_table
         FOR EACH ROW EXECUTE FUNCTION copy_trigger_fn();
         CREATE TRIGGER copy_after_trigger AFTER INSERT ON copy_trigger_table
         FOR EACH ROW EXECUTE FUNCTION copy_after_trigger_fn()",
    )
    .await;

    session
        .copy_in(
            "COPY copy_trigger_table FROM STDIN",
            0,
            vec![bytes::Bytes::from_static(b"one\ntwo\n")],
        )
        .await
        .expect("COPY succeeds through the trigger actor");
    assert!(
        rows(
            &mut session,
            "SELECT value FROM copy_trigger_table ORDER BY value",
        )
        .await
            == vec![vec![Some("ONE".into())], vec![Some("TWO".into())]]
    );
    assert!(
        rows(
            &mut session,
            "SELECT value FROM copy_trigger_log ORDER BY value",
        )
        .await
            == vec![vec![Some("ONE".into())], vec![Some("TWO".into())]]
    );
}

#[tokio::test]
async fn drop_column_refuses_a_dependent_trigger_without_cascade() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE FUNCTION drop_column_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN RETURN NEW; END $$;
         CREATE TABLE drop_column_trigger_table (id int, watched int);
         CREATE TRIGGER watched_trigger BEFORE UPDATE OF watched ON drop_column_trigger_table
         FOR EACH ROW EXECUTE FUNCTION drop_column_trigger_fn();
         CREATE TRIGGER watched_trigger_later BEFORE UPDATE OF watched ON drop_column_trigger_table
         FOR EACH ROW EXECUTE FUNCTION drop_column_trigger_fn()",
    )
    .await;

    let error = session
        .simple_query("ALTER TABLE drop_column_trigger_table DROP COLUMN watched")
        .await
        .expect_err("dependent trigger blocks DROP COLUMN");
    assert!(error.code == "2BP01");
    assert!(
        error.message
            == "cannot drop column watched of table drop_column_trigger_table because other objects depend on it"
    );
    assert!(
        error.diagnostics.expect("dependency detail").detail
            == Some(
                "trigger watched_trigger on table drop_column_trigger_table depends on column watched of table drop_column_trigger_table\ntrigger watched_trigger_later on table drop_column_trigger_table depends on column watched of table drop_column_trigger_table".into()
            )
    );
    assert!(
        rows(
            &mut session,
            "SELECT tgname FROM pg_trigger WHERE tgname LIKE 'watched_trigger%' ORDER BY tgname",
        )
        .await
            == vec![
                vec![Some("watched_trigger".into())],
                vec![Some("watched_trigger_later".into())],
            ]
    );

    run(
        &mut session,
        "ALTER TABLE drop_column_trigger_table DROP COLUMN watched CASCADE",
    )
    .await;
    assert!(
        rows(
            &mut session,
            "SELECT tgname FROM pg_trigger WHERE tgname LIKE 'watched_trigger%'",
        )
        .await
        .is_empty()
    );
}

#[tokio::test]
async fn trigger_when_rejects_unavailable_row_images_and_duplicate_columns() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE FUNCTION trigger_when_fn() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN RETURN NEW; END $$;
         CREATE TABLE trigger_when_table (id int)",
    )
    .await;

    for (sql, message) in [
        (
            "CREATE TRIGGER old_on_insert BEFORE INSERT ON trigger_when_table FOR EACH ROW \
             WHEN (OLD.id = 1) EXECUTE FUNCTION trigger_when_fn()",
            "INSERT trigger's WHEN condition cannot reference OLD values",
        ),
        (
            "CREATE TRIGGER new_on_delete BEFORE DELETE ON trigger_when_table FOR EACH ROW \
             WHEN (NEW.id = 1) EXECUTE FUNCTION trigger_when_fn()",
            "DELETE trigger's WHEN condition cannot reference NEW values",
        ),
        (
            "CREATE TRIGGER system_before BEFORE UPDATE ON trigger_when_table FOR EACH ROW \
             WHEN (NEW.tableoid <> 0) EXECUTE FUNCTION trigger_when_fn()",
            "BEFORE trigger's WHEN condition cannot reference NEW system columns",
        ),
        (
            "CREATE TRIGGER statement_when BEFORE UPDATE ON trigger_when_table FOR EACH STATEMENT \
             WHEN (OLD.id = NEW.id) EXECUTE FUNCTION trigger_when_fn()",
            "statement trigger's WHEN condition cannot reference column values",
        ),
        (
            "CREATE TRIGGER duplicate_column BEFORE UPDATE OF id, id ON trigger_when_table \
             FOR EACH ROW EXECUTE FUNCTION trigger_when_fn()",
            "column \"id\" specified more than once",
        ),
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("invalid trigger");
        assert!(
            error.code
                == if message.starts_with("column") {
                    "42701"
                } else {
                    "42P17"
                }
        );
        assert!(error.message == message);
    }
    run(
        &mut session,
        "CREATE TRIGGER literal_old BEFORE INSERT ON trigger_when_table FOR EACH ROW
         WHEN ('OLD.id' = 'OLD.id') EXECUTE FUNCTION trigger_when_fn()",
    )
    .await;
    assert!(
        rows(
            &mut session,
            "SELECT tgname FROM pg_trigger WHERE tgrelid = 'trigger_when_table'::regclass"
        )
        .await
            == vec![vec![Some("literal_old".to_string())]]
    );
}

#[tokio::test]
async fn generated_columns_drive_update_of_and_are_refused_in_before_when() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE generated_trigger_log (value int);
         CREATE TABLE generated_trigger_table (
             a int,
             c int,
             b int GENERATED ALWAYS AS (a * 2) STORED
         );
         CREATE FUNCTION generated_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN INSERT INTO generated_trigger_log VALUES (NEW.a); RETURN NEW; END $$;
         CREATE TRIGGER generated_update BEFORE UPDATE OF b ON generated_trigger_table
         FOR EACH ROW EXECUTE FUNCTION generated_trigger_fn();
         INSERT INTO generated_trigger_table (a) VALUES (1);
         UPDATE generated_trigger_table SET a = 2;
         UPDATE generated_trigger_table SET c = 3",
    )
    .await;
    assert!(
        rows(&mut session, "SELECT value FROM generated_trigger_log").await
            == vec![vec![Some("2".to_string())]]
    );

    let error = session
        .simple_query(
            "CREATE TRIGGER generated_when BEFORE UPDATE ON generated_trigger_table FOR EACH ROW \
             WHEN (NEW.b > 0) EXECUTE FUNCTION generated_trigger_fn()",
        )
        .await
        .expect_err("generated column in BEFORE WHEN");
    assert!(error.code == "42P17");
    assert!(
        error.message == "BEFORE trigger's WHEN condition cannot reference NEW generated columns"
    );
    assert!(
        error.diagnostics.and_then(|fields| fields.detail)
            == Some("Column \"b\" is a generated column.".to_string())
    );
}

#[tokio::test]
async fn trigger_kind_checks_preserve_the_relation_specific_diagnostic() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE trigger_kind_table (id int);
         CREATE VIEW trigger_kind_view AS SELECT id FROM trigger_kind_table;
         CREATE FUNCTION trigger_kind_fn() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN RETURN NEW; END $$",
    )
    .await;

    for (sql, message, detail) in [
        (
            "CREATE TRIGGER table_instead INSTEAD OF INSERT ON trigger_kind_table FOR EACH ROW \
             EXECUTE FUNCTION trigger_kind_fn()",
            "\"trigger_kind_table\" is a table",
            "Tables cannot have INSTEAD OF triggers.",
        ),
        (
            "CREATE TRIGGER view_before BEFORE INSERT ON trigger_kind_view FOR EACH ROW \
             EXECUTE FUNCTION trigger_kind_fn()",
            "\"trigger_kind_view\" is a view",
            "Views cannot have row-level BEFORE or AFTER triggers.",
        ),
        (
            "CREATE TRIGGER view_truncate BEFORE TRUNCATE ON trigger_kind_view \
             EXECUTE FUNCTION trigger_kind_fn()",
            "\"trigger_kind_view\" is a view",
            "Views cannot have TRUNCATE triggers.",
        ),
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("wrong relation kind");
        assert!(error.code == "42809");
        assert!(error.message == message);
        assert!(error.diagnostics.and_then(|fields| fields.detail) == Some(detail.to_string()));
    }

    let error = session
        .simple_query(
            "CREATE TRIGGER view_instead_statement INSTEAD OF INSERT ON trigger_kind_view \
             EXECUTE FUNCTION trigger_kind_fn()",
        )
        .await
        .expect_err("statement-level INSTEAD OF trigger");
    assert!(error.code == "42P17");
    assert!(error.message == "INSTEAD OF triggers must be FOR EACH ROW");
}
