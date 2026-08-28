//! Rewrite rules apply `NEW.*` actions after the base statement's rows exist.

use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};
use tokio::sync::mpsc::error::TryRecvError;

async fn run(session: &mut SqlSession, sql: &str) {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

async fn scalar(session: &mut SqlSession, sql: &str) -> Option<String> {
    match &session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))[0]
    {
        QueryResult::Rows { rows, .. } => rows[0][0]
            .as_ref()
            .map(|cell: &Cell| String::from_utf8(cell.text.to_vec()).expect("utf8")),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// The rule action sees each inserted row as `NEW.*` after the base INSERT.
///
/// Its source expression is expanded separately, so the source serial default
/// runs again while the rule action is constructed.
#[tokio::test]
async fn an_insert_also_rule_copies_new_rows_after_the_base_insert() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE source_rows (id serial, value text)",
    )
    .await;
    run(&mut session, "CREATE TABLE rule_log (id int, value text)").await;
    run(
        &mut session,
        "CREATE RULE source_rows_insert AS ON INSERT TO source_rows \
         DO ALSO INSERT INTO rule_log VALUES (NEW.*)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE source_rows_update AS ON UPDATE TO source_rows \
         DO ALSO INSERT INTO rule_log VALUES (100, 'update')",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO source_rows (value) VALUES ('first'), ('second')",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT string_agg(id::text || ':' || value, ',' ORDER BY id) FROM source_rows",
        )
        .await
            == Some("1:first,2:second".to_string())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT string_agg(id::text || ':' || value, ',' ORDER BY id) FROM rule_log",
        )
        .await
            == Some("3:first,4:second".to_string())
    );
}

#[tokio::test]
async fn an_image_free_rule_action_runs_once_per_statement() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE statement_rule_source (value int)",
    )
    .await;
    run(&mut session, "CREATE TABLE statement_rule_log (value int)").await;
    run(
        &mut session,
        "INSERT INTO statement_rule_source VALUES (1), (2)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE statement_rule AS ON UPDATE TO statement_rule_source \
         DO ALSO INSERT INTO statement_rule_log VALUES (1)",
    )
    .await;

    run(
        &mut session,
        "UPDATE statement_rule_source SET value = value",
    )
    .await;
    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM statement_rule_log"
        )
        .await
            == Some("1".into())
    );
}

#[tokio::test]
async fn an_also_values_rule_action_returns_bound_old_and_new_wildcards() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE values_rule_source (id int, value int)",
    )
    .await;
    run(&mut session, "INSERT INTO values_rule_source VALUES (1, 2)").await;
    run(
        &mut session,
        "CREATE RULE values_rule AS ON UPDATE TO values_rule_source \
         DO ALSO VALUES (OLD.*, 'old'), (NEW.*, 'new')",
    )
    .await;

    let result = session
        .simple_query("UPDATE values_rule_source SET value = 3")
        .await
        .expect("UPDATE runs its VALUES rule action");
    let [QueryResult::Rows { rows, .. }] = result.as_slice() else {
        panic!("expected VALUES action rows, got {result:?}");
    };
    assert!(rows.len() == 2);
    assert!(rows[0][0].as_ref().map(|cell| cell.text.as_ref()) == Some(&b"1"[..]));
    assert!(rows[0][1].as_ref().map(|cell| cell.text.as_ref()) == Some(&b"2"[..]));
    assert!(rows[0][2].as_ref().map(|cell| cell.text.as_ref()) == Some(&b"old"[..]));
    assert!(rows[1][0].as_ref().map(|cell| cell.text.as_ref()) == Some(&b"1"[..]));
    assert!(rows[1][1].as_ref().map(|cell| cell.text.as_ref()) == Some(&b"3"[..]));
    assert!(rows[1][2].as_ref().map(|cell| cell.text.as_ref()) == Some(&b"new"[..]));
}

#[tokio::test]
async fn an_instead_insert_rule_action_accepts_a_target_alias_in_returning() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE alias_rule_source (id int, value text)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE alias_rule_log (id int, value text)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE alias_rule AS ON INSERT TO alias_rule_source \
         DO INSTEAD INSERT INTO alias_rule_log AS target SELECT NEW.* \
         RETURNING target.id, target.value",
    )
    .await;

    let result = session
        .simple_query("INSERT INTO alias_rule_source VALUES (7, 'alias') RETURNING *")
        .await
        .expect("INSERT runs its aliased rule action");
    let [QueryResult::Rows { rows, .. }] = result.as_slice() else {
        panic!("expected aliased rule action rows, got {result:?}");
    };
    assert!(rows.len() == 1);
    assert!(rows[0][0].as_ref().map(|cell| cell.text.as_ref()) == Some(&b"7"[..]));
    assert!(rows[0][1].as_ref().map(|cell| cell.text.as_ref()) == Some(&b"alias"[..]));
    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'alias_rule'",
        )
        .await
        .expect("rule definition")
        .contains("INSERT INTO alias_rule_log AS target")
    );
}

#[tokio::test]
async fn a_rule_action_allows_a_subscripted_multi_column_set_from_a_subquery() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE subscript_rule_source (id int)").await;
    run(
        &mut session,
        "CREATE TABLE subscript_rule_target (values int[], id int)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO subscript_rule_target VALUES ('{1, 1}', 1)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE subscript_rule AS ON UPDATE TO subscript_rule_source \
         DO INSTEAD UPDATE subscript_rule_target target \
         SET (values[1], id) = (SELECT 2, NEW.id) WHERE target.id = OLD.id",
    )
    .await;
    run(&mut session, "INSERT INTO subscript_rule_source VALUES (1)").await;
    run(
        &mut session,
        "UPDATE subscript_rule_source SET id = 3 WHERE id = 1",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT values::text || ':' || id::text FROM subscript_rule_target"
        )
        .await
            == Some("{2,1}:3".into())
    );
}

#[tokio::test]
async fn an_implicit_also_rule_copies_new_rows_after_the_base_insert() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE implicit_rule_source (value int)",
    )
    .await;
    run(&mut session, "CREATE TABLE implicit_rule_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE implicit_rule AS ON INSERT TO implicit_rule_source \
         DO INSERT INTO implicit_rule_log VALUES (NEW.value)",
    )
    .await;
    run(&mut session, "INSERT INTO implicit_rule_source VALUES (7)").await;

    assert!(
        scalar(&mut session, "SELECT value::text FROM implicit_rule_source").await
            == Some("7".into())
    );
    assert!(
        scalar(&mut session, "SELECT value::text FROM implicit_rule_log").await == Some("7".into())
    );
}

#[tokio::test]
async fn a_rule_action_binds_new_inside_a_multi_column_assignment_subquery() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE rule_subquery_source (a int, b int)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE rule_subquery_log (a int, b int)",
    )
    .await;
    run(&mut session, "INSERT INTO rule_subquery_log VALUES (0, 0)").await;
    run(
        &mut session,
        "CREATE RULE rule_subquery_insert AS ON INSERT TO rule_subquery_source \
         DO ALSO UPDATE rule_subquery_log SET (a, b) = (SELECT NEW.a, NEW.b)",
    )
    .await;

    run(
        &mut session,
        "INSERT INTO rule_subquery_source VALUES (7, 9)",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT a::text || ':' || b::text FROM rule_subquery_log",
        )
        .await
            == Some("7:9".into())
    );
}

#[tokio::test]
async fn a_rule_action_refuses_old_or_new_inside_a_cte() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE cte_rule_source (value int)").await;
    run(&mut session, "CREATE TABLE cte_rule_log (value int)").await;
    run(
        &mut session,
        "CREATE TABLE cte_rule_conflict_log (value int PRIMARY KEY)",
    )
    .await;
    let error = session
        .simple_query(
            "CREATE RULE cte_rule AS ON UPDATE TO cte_rule_source DO INSTEAD \
             WITH copied AS (SELECT OLD.value) UPDATE cte_rule_log SET value = copied.value FROM copied",
        )
        .await
        .expect_err("OLD in a rule-action CTE must fail");

    assert!(format!("{error:?}").contains("cannot refer to OLD within WITH query"));

    let error = session
        .simple_query(
            "CREATE RULE cte_rule_wildcard AS ON UPDATE TO cte_rule_source DO INSTEAD \
             WITH copied AS (SELECT OLD.*) UPDATE cte_rule_log SET value = copied.value FROM copied",
        )
        .await
        .expect_err("OLD.* in a rule-action CTE must fail");

    assert!(format!("{error:?}").contains("cannot refer to OLD within WITH query"));

    let error = session
        .simple_query(
            "CREATE RULE cte_rule_dml AS ON UPDATE TO cte_rule_source DO INSTEAD \
             WITH copied AS (INSERT INTO cte_rule_log SELECT OLD.value RETURNING *) \
             DELETE FROM cte_rule_log WHERE false",
        )
        .await
        .expect_err("OLD in a data-modifying rule-action CTE must fail");

    assert!(format!("{error:?}").contains("cannot refer to OLD within WITH query"));

    for (name, event, image, cte_body) in [
        (
            "cte_rule_dml_update",
            "UPDATE",
            "NEW",
            "UPDATE cte_rule_log SET value = NEW.value RETURNING *",
        ),
        (
            "cte_rule_dml_delete",
            "DELETE",
            "OLD",
            "DELETE FROM cte_rule_log WHERE value = OLD.value RETURNING *",
        ),
    ] {
        let error = session
            .simple_query(&format!(
                "CREATE RULE {name} AS ON {event} TO cte_rule_source DO INSTEAD \
                 WITH copied AS ({cte_body}) \
                 DELETE FROM cte_rule_log WHERE false"
            ))
            .await
            .expect_err("rule image in a data-modifying rule-action CTE must fail");
        assert!(
            format!("{error:?}").contains(&format!("cannot refer to {image} within WITH query"))
        );
    }

    let error = session
        .simple_query(
            "CREATE RULE cte_rule_dml_from AS ON UPDATE TO cte_rule_source DO INSTEAD \
             WITH copied AS (UPDATE cte_rule_log SET value = source.value \
             FROM (SELECT NEW.value) source RETURNING *) DELETE FROM cte_rule_log WHERE false",
        )
        .await
        .expect_err("NEW in a derived FROM query inside a rule-action CTE must fail");
    assert!(format!("{error:?}").contains("cannot refer to NEW within WITH query"));

    let error = session
        .simple_query(
            "CREATE RULE cte_rule_dml_conflict AS ON UPDATE TO cte_rule_source DO INSTEAD \
             WITH copied AS (INSERT INTO cte_rule_conflict_log VALUES (0) ON CONFLICT (value) \
             DO UPDATE SET value = NEW.value RETURNING *) DELETE FROM cte_rule_log WHERE false",
        )
        .await
        .expect_err("NEW in a conflict action inside a rule-action CTE must fail");
    assert!(format!("{error:?}").contains("cannot refer to NEW within WITH query"));

    let error = session
        .simple_query(
            "CREATE RULE cte_rule_dml_function AS ON UPDATE TO cte_rule_source DO INSTEAD \
             WITH copied AS (UPDATE cte_rule_log SET value = series.value \
             FROM generate_series(NEW.value, NEW.value) AS series(value) RETURNING *) \
             DELETE FROM cte_rule_log WHERE false",
        )
        .await
        .expect_err("NEW in a function FROM item inside a rule-action CTE must fail");
    assert!(format!("{error:?}").contains("cannot refer to NEW within WITH query"));

    let error = session
        .simple_query(
            "CREATE RULE cte_rule_dml_json_table AS ON UPDATE TO cte_rule_source DO INSTEAD \
             WITH copied AS (UPDATE cte_rule_log SET value = json_row.value \
             FROM JSON_TABLE(NEW.value::text::json, '$' COLUMNS (value int PATH '$')) \
             AS json_row RETURNING *) DELETE FROM cte_rule_log WHERE false",
        )
        .await
        .expect_err("NEW in a JSON_TABLE FROM item inside a rule-action CTE must fail");
    assert!(format!("{error:?}").contains("cannot refer to NEW within WITH query"));
}

#[tokio::test]
async fn a_data_modifying_cte_refuses_a_conditional_instead_rule() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE cte_dml_rule_target (value int)").await;
    run(
        &mut session,
        "CREATE RULE cte_dml_conditional AS ON INSERT TO cte_dml_rule_target \
         WHERE NEW.value = 0 DO INSTEAD NOTHING",
    )
    .await;
    let error = session
        .simple_query(
            "WITH inserted AS (INSERT INTO cte_dml_rule_target VALUES (0)) VALUES (FALSE)",
        )
        .await
        .expect_err("conditional INSTEAD rule in a data-modifying CTE must fail");

    assert!(format!("{error:?}").contains(
        "conditional DO INSTEAD rules are not supported for data-modifying statements in WITH"
    ));

    run(
        &mut session,
        "CREATE TABLE cte_dml_disabled_target (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE cte_dml_disabled AS ON INSERT TO cte_dml_disabled_target \
         WHERE NEW.value = 0 DO INSTEAD NOTHING",
    )
    .await;
    run(
        &mut session,
        "ALTER TABLE cte_dml_disabled_target DISABLE RULE cte_dml_disabled",
    )
    .await;
    assert!(
        scalar(
            &mut session,
            "WITH inserted AS (INSERT INTO cte_dml_disabled_target VALUES (0)) VALUES (FALSE)",
        )
        .await
            == Some("f".into())
    );

    run(
        &mut session,
        "CREATE TABLE cte_dml_event_target (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE cte_dml_update AS ON UPDATE TO cte_dml_event_target \
         WHERE NEW.value = 0 DO INSTEAD NOTHING",
    )
    .await;
    assert!(
        scalar(
            &mut session,
            "WITH inserted AS (INSERT INTO cte_dml_event_target VALUES (0)) VALUES (FALSE)",
        )
        .await
            == Some("f".into())
    );

    run(
        &mut session,
        "CREATE TABLE cte_dml_update_target (value int)",
    )
    .await;
    run(&mut session, "INSERT INTO cte_dml_update_target VALUES (0)").await;
    run(
        &mut session,
        "CREATE RULE cte_dml_update_conditional AS ON UPDATE TO cte_dml_update_target \
         WHERE NEW.value = 1 DO INSTEAD NOTHING",
    )
    .await;
    let error = session
        .simple_query("WITH changed AS (UPDATE cte_dml_update_target SET value = 1) VALUES (FALSE)")
        .await
        .expect_err("conditional UPDATE rule in a data-modifying CTE must fail");
    assert!(format!("{error:?}").contains(
        "conditional DO INSTEAD rules are not supported for data-modifying statements in WITH"
    ));

    run(
        &mut session,
        "CREATE TABLE cte_dml_delete_target (value int)",
    )
    .await;
    run(&mut session, "INSERT INTO cte_dml_delete_target VALUES (0)").await;
    run(
        &mut session,
        "CREATE RULE cte_dml_delete_conditional AS ON DELETE TO cte_dml_delete_target \
         WHERE OLD.value = 0 DO INSTEAD NOTHING",
    )
    .await;
    let error = session
        .simple_query("WITH removed AS (DELETE FROM cte_dml_delete_target) VALUES (FALSE)")
        .await
        .expect_err("conditional DELETE rule in a data-modifying CTE must fail");
    assert!(format!("{error:?}").contains(
        "conditional DO INSTEAD rules are not supported for data-modifying statements in WITH"
    ));

    let cases = [
        (
            "cte_dml_nothing_target",
            "cte_dml_nothing",
            "DO INSTEAD NOTHING",
            "DO INSTEAD NOTHING rules are not supported for data-modifying statements in WITH",
        ),
        (
            "cte_dml_instead_notify_target",
            "cte_dml_instead_notify",
            "DO INSTEAD NOTIFY cte_dml_events",
            "DO INSTEAD NOTIFY rules are not supported for data-modifying statements in WITH",
        ),
        (
            "cte_dml_also_notify_target",
            "cte_dml_also_notify",
            "DO ALSO NOTIFY cte_dml_events",
            "DO ALSO rules are not supported for data-modifying statements in WITH",
        ),
        (
            "cte_dml_multi_target",
            "cte_dml_multi",
            "DO INSTEAD (NOTIFY cte_dml_events; NOTIFY cte_dml_events)",
            "multi-statement DO INSTEAD rules are not supported for data-modifying statements in WITH",
        ),
    ];
    for (table, rule, action, message) in cases {
        run(&mut session, &format!("CREATE TABLE {table} (value int)")).await;
        run(
            &mut session,
            &format!("CREATE RULE {rule} AS ON INSERT TO {table} {action}"),
        )
        .await;
        let error = session
            .simple_query(&format!(
                "WITH inserted AS (INSERT INTO {table} VALUES (0)) VALUES (FALSE)"
            ))
            .await
            .expect_err("unsupported rule in a data-modifying CTE must fail");
        assert!(format!("{error:?}").contains(message));
    }

    run(
        &mut session,
        "CREATE TABLE cte_dml_select_target (value int)",
    )
    .await;
    run(&mut session, "CREATE TABLE cte_dml_select_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE cte_dml_select AS ON INSERT TO cte_dml_select_target \
         DO INSTEAD INSERT INTO cte_dml_select_log SELECT NEW.value",
    )
    .await;
    let error = session
        .simple_query(
            "WITH inserted AS (INSERT INTO cte_dml_select_target VALUES (0)) VALUES (FALSE)",
        )
        .await
        .expect_err("INSERT SELECT rule action in a data-modifying CTE must fail");
    assert!(
        format!("{error:?}").contains(
            "INSERT ... SELECT rule actions are not supported for queries having data-modifying statements in WITH"
        )
    );

    run(
        &mut session,
        "CREATE TABLE cte_dml_allowed_target (value int)",
    )
    .await;
    run(&mut session, "CREATE TABLE cte_dml_allowed_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE cte_dml_allowed AS ON INSERT TO cte_dml_allowed_target \
         DO INSTEAD INSERT INTO cte_dml_allowed_log VALUES (NEW.value)",
    )
    .await;
    assert!(
        scalar(
            &mut session,
            "WITH inserted AS (INSERT INTO cte_dml_allowed_target VALUES (9)) VALUES (FALSE)",
        )
        .await
            == Some("f".into())
    );
    assert!(
        scalar(&mut session, "SELECT value::text FROM cte_dml_allowed_log").await
            == Some("9".into())
    );
}

#[tokio::test]
async fn a_rule_action_queues_notify_at_statement_commit() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut listener = engine.connect_with_pid(101);
    let mut notifications = listener
        .take_notifications()
        .expect("listener notification receiver");
    let mut writer = engine.connect_with_pid(202);
    run(&mut listener, "LISTEN rule_events").await;
    run(&mut writer, "CREATE TABLE rule_notify_source (value int)").await;
    run(
        &mut writer,
        "CREATE RULE rule_notify_insert AS ON INSERT TO rule_notify_source \
         DO ALSO NOTIFY rule_events, 'inserted'",
    )
    .await;

    assert!(
        scalar(
            &mut writer,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'rule_notify_insert'",
        )
        .await
            == Some(
                "CREATE RULE rule_notify_insert AS\n    ON INSERT TO public.rule_notify_source DO\n \
                 NOTIFY rule_events, 'inserted';"
                    .into(),
            )
    );

    run(&mut writer, "INSERT INTO rule_notify_source VALUES (1)").await;

    let notification = notifications.try_recv().expect("rule notification");
    assert!(notification.process_id == 202);
    assert!(notification.channel == "rule_events");
    assert!(notification.payload == "inserted");

    run(&mut writer, "BEGIN").await;
    run(&mut writer, "INSERT INTO rule_notify_source VALUES (2)").await;
    run(&mut writer, "ROLLBACK").await;
    assert!(notifications.try_recv() == Err(TryRecvError::Empty));
}

#[tokio::test]
async fn pg_get_ruledef_reconstructs_a_durable_rule() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE rule_source (value int)").await;
    run(&mut session, "CREATE TABLE rule_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE rule_source_insert AS ON INSERT TO rule_source \
         DO ALSO INSERT INTO rule_log VALUES (NEW.*)",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'rule_source_insert'",
        )
        .await
            == Some(
                "CREATE RULE rule_source_insert AS\n    ON INSERT TO public.rule_source DO  \
                 INSERT INTO rule_log (value)\n  VALUES (new.value);"
                    .into(),
            )
    );
}

#[tokio::test]
async fn pg_get_ruledef_reconstructs_a_view_return_rule() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE VIEW return_rule_view AS SELECT 1 AS value",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite \
             WHERE rulename = '_RETURN' AND ev_class = 'return_rule_view'::regclass",
        )
        .await
            == Some(
                "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO return_rule_view DO INSTEAD  SELECT 1 AS value;"
                    .into(),
            )
    );
}

#[tokio::test]
async fn a_view_return_rule_cannot_be_dropped_directly() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE VIEW protected_return_rule AS SELECT 1 AS value",
    )
    .await;
    let error = session
        .simple_query("DROP RULE \"_RETURN\" ON protected_return_rule")
        .await
        .expect_err("a view owns its _RETURN rule");

    let debug = format!("{error:?}");
    assert!(debug.contains("cannot drop rule _RETURN on view protected_return_rule"));
    assert!(debug.contains("You can drop view protected_return_rule instead."));
}

#[tokio::test]
async fn a_view_return_rule_cannot_be_renamed() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE VIEW protected_return_rename AS SELECT 1 AS value",
    )
    .await;
    let error = session
        .simple_query("ALTER RULE \"_RETURN\" ON protected_return_rename RENAME TO changed")
        .await
        .expect_err("a view owns its ON SELECT rule");

    let debug = format!("{error:?}");
    assert!(debug.contains("code: \"42P17\""));
    assert!(debug.contains("renaming an ON SELECT rule is not allowed"));
}

#[tokio::test]
async fn a_table_has_no_protected_return_rule() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE ordinary_return_rule_target (value int)",
    )
    .await;
    let error = session
        .simple_query("DROP RULE \"_RETURN\" ON ordinary_return_rule_target")
        .await
        .expect_err("a table does not own a _RETURN rule");

    assert!(!format!("{error:?}").contains("requires it"));
}

#[tokio::test]
async fn a_table_has_no_protected_return_rule_to_rename() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE ordinary_return_rename_target (value int)",
    )
    .await;
    let error = session
        .simple_query("ALTER RULE \"_RETURN\" ON ordinary_return_rename_target RENAME TO changed")
        .await
        .expect_err("a table does not own a _RETURN rule");

    assert!(!format!("{error:?}").contains("renaming an ON SELECT rule"));
}

#[tokio::test]
async fn on_select_rules_are_reserved_for_view_creation() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE select_rule_table (value int)").await;
    let table_error = session
        .simple_query(
            "CREATE RULE select_rule AS ON SELECT TO select_rule_table \
             DO INSTEAD SELECT value FROM select_rule_table",
        )
        .await
        .expect_err("tables cannot have ON SELECT rules");
    let table_debug = format!("{table_error:?}");
    assert!(table_debug.contains("code: \"42809\""));
    assert!(table_debug.contains("cannot have ON SELECT rules"));
    assert!(table_debug.contains("This operation is not supported for tables."));

    run(
        &mut session,
        "CREATE VIEW select_rule_view AS SELECT 1 AS value",
    )
    .await;
    let view_error = session
        .simple_query(
            "CREATE RULE select_rule AS ON SELECT TO select_rule_view \
             DO INSTEAD SELECT 1 AS value",
        )
        .await
        .expect_err("a view already owns its ON SELECT rule");
    let view_debug = format!("{view_error:?}");
    assert!(view_debug.contains("code: \"55000\""));
}

#[tokio::test]
async fn a_view_created_from_select_star_accepts_write_rules() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE rule_view_base (a int, b int)").await;
    run(
        &mut session,
        "CREATE VIEW rule_view AS SELECT * FROM rule_view_base",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE rule_view_ins AS ON INSERT TO rule_view DO INSTEAD \
         INSERT INTO rule_view_base VALUES (NEW.a, NEW.b)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE rule_view_upd AS ON UPDATE TO rule_view DO INSTEAD \
         UPDATE rule_view_base SET a = NEW.a, b = NEW.b WHERE a = OLD.a",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE rule_view_del AS ON DELETE TO rule_view DO INSTEAD \
         DELETE FROM rule_view_base WHERE a = OLD.a",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM pg_rules WHERE tablename = 'rule_view'",
        )
        .await
            == Some("3".into())
    );

    run(
        &mut session,
        "CREATE TABLE rule_view_runtime_base (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE VIEW rule_view_runtime AS SELECT * FROM rule_view_runtime_base",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE rule_view_insert_log (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE rule_view_update_log (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE rule_view_delete_log (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE rule_view_runtime_ins AS ON INSERT TO rule_view_runtime DO INSTEAD \
         INSERT INTO rule_view_insert_log VALUES (NEW.value)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE rule_view_runtime_upd AS ON UPDATE TO rule_view_runtime DO INSTEAD \
         INSERT INTO rule_view_update_log VALUES (NEW.value)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE rule_view_runtime_del AS ON DELETE TO rule_view_runtime DO INSTEAD \
         INSERT INTO rule_view_delete_log VALUES (OLD.value)",
    )
    .await;
    run(&mut session, "INSERT INTO rule_view_runtime VALUES (7)").await;
    run(
        &mut session,
        "INSERT INTO rule_view_runtime_base VALUES (1)",
    )
    .await;
    run(&mut session, "UPDATE rule_view_runtime SET value = 2").await;
    run(&mut session, "DELETE FROM rule_view_runtime").await;

    assert!(
        scalar(&mut session, "SELECT value::text FROM rule_view_insert_log").await
            == Some("7".into())
    );
    assert!(
        scalar(&mut session, "SELECT value::text FROM rule_view_update_log").await
            == Some("2".into())
    );
    assert!(
        scalar(&mut session, "SELECT value::text FROM rule_view_delete_log").await
            == Some("1".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT value::text FROM rule_view_runtime_base"
        )
        .await
            == Some("1".into())
    );
}

#[tokio::test]
async fn pg_rules_lists_durable_user_rules() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE listed_rules (value int)").await;
    run(&mut session, "CREATE TABLE later_rules (value int)").await;
    run(
        &mut session,
        "CREATE RULE z_listed_insert AS ON INSERT TO listed_rules DO INSTEAD NOTHING",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE a_later_insert AS ON INSERT TO later_rules DO INSTEAD NOTHING",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT definition FROM pg_rules WHERE rulename = 'z_listed_insert'",
        )
        .await
            == Some(
                "CREATE RULE z_listed_insert AS\n    ON INSERT TO public.listed_rules DO INSTEAD NOTHING;"
                    .into(),
            )
    );
    assert!(
        scalar(
            &mut session,
            "SELECT string_agg(rulename, ',') FROM pg_rules"
        )
        .await
            == Some("a_later_insert,z_listed_insert".into())
    );
}

#[tokio::test]
async fn pg_class_reports_durable_user_rules() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE class_rule_source (value int)").await;
    run(
        &mut session,
        "CREATE RULE class_rule AS ON INSERT TO class_rule_source DO INSTEAD NOTHING",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT relhasrules::text FROM pg_class WHERE oid = 'class_rule_source'::regclass",
        )
        .await
            == Some("true".into())
    );
}

#[tokio::test]
async fn an_insert_instead_nothing_rule_suppresses_the_base_write() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE ignored_rows (value int)").await;
    run(
        &mut session,
        "CREATE RULE ignore_insert AS ON INSERT TO ignored_rows DO INSTEAD NOTHING",
    )
    .await;
    run(&mut session, "INSERT INTO ignored_rows VALUES (1), (2)").await;

    assert!(scalar(&mut session, "SELECT count(*) FROM ignored_rows").await == Some("0".into()));
}

#[tokio::test]
async fn on_conflict_is_refused_when_the_target_has_a_rewrite_rule() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE conflict_rule_target (id int primary key)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE conflict_rule AS ON INSERT TO conflict_rule_target DO ALSO NOTHING",
    )
    .await;
    let error = session
        .simple_query("INSERT INTO conflict_rule_target VALUES (1) ON CONFLICT DO NOTHING")
        .await
        .expect_err("ON CONFLICT with a rule must fail");

    assert!(
        format!("{error:?}").contains("cannot be used with table that has INSERT or UPDATE rules")
    );
}

#[tokio::test]
async fn merge_is_refused_when_the_target_has_a_rewrite_rule() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE merge_rule_target (id int primary key)",
    )
    .await;
    run(&mut session, "CREATE TABLE merge_rule_source (id int)").await;
    run(
        &mut session,
        "CREATE RULE merge_rule AS ON UPDATE TO merge_rule_target DO ALSO NOTHING",
    )
    .await;
    let error = session
        .simple_query(
            "MERGE INTO merge_rule_target USING merge_rule_source \
             ON merge_rule_target.id = merge_rule_source.id \
             WHEN MATCHED THEN UPDATE SET id = merge_rule_source.id",
        )
        .await
        .expect_err("MERGE with a rule must fail");

    assert!(format!("{error:?}").contains("cannot execute MERGE on relation"));
}

#[tokio::test]
async fn merge_insert_assigns_a_subscripted_target() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE merge_subscript_target (id int primary key, filling int[])",
    )
    .await;
    run(&mut session, "CREATE TABLE merge_subscript_source (id int)").await;
    run(
        &mut session,
        "INSERT INTO merge_subscript_source VALUES (1)",
    )
    .await;
    run(
        &mut session,
        "MERGE INTO merge_subscript_target target USING merge_subscript_source source \
         ON target.id = source.id WHEN NOT MATCHED THEN \
         INSERT (filling[1], id) VALUES (source.id, source.id)",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT filling::text || ':' || id::text FROM merge_subscript_target",
        )
        .await
            == Some("{1}:1".into())
    );
}

#[tokio::test]
async fn merge_insert_uses_a_default_target_value() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE merge_default_target (id int primary key, filling int[] DEFAULT '{9}')",
    )
    .await;
    run(&mut session, "CREATE TABLE merge_default_source (id int)").await;
    run(&mut session, "INSERT INTO merge_default_source VALUES (1)").await;
    run(
        &mut session,
        "MERGE INTO merge_default_target target USING merge_default_source source \
         ON target.id = source.id WHEN NOT MATCHED THEN \
         INSERT (filling, id) VALUES (DEFAULT, source.id)",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT filling::text || ':' || id::text FROM merge_default_target",
        )
        .await
            == Some("{9}:1".into())
    );
}

#[tokio::test]
async fn insert_assigns_array_and_jsonb_target_subscripts() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE insert_subscript_target (values int[], doc jsonb)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO insert_subscript_target (values[2], doc['flag']) \
         VALUES (7, 'true'::jsonb)",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT values::text FROM insert_subscript_target"
        )
        .await
            == Some("[2:2]={7}".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT doc->>'flag' FROM insert_subscript_target"
        )
        .await
            == Some("true".into())
    );
    run(
        &mut session,
        "INSERT INTO insert_subscript_target (values[1], values[3]) VALUES (11, 22)",
    )
    .await;
    assert!(
        scalar(
            &mut session,
            "SELECT values::text FROM insert_subscript_target WHERE doc IS NULL",
        )
        .await
            == Some("{11,NULL,22}".into())
    );
    run(
        &mut session,
        "CREATE DOMAIN insert_subscript_domain AS int[]",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE insert_subscript_domain_target (values insert_subscript_domain)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO insert_subscript_domain_target (values[1], values[3]) VALUES (11, 22)",
    )
    .await;
    assert!(
        scalar(
            &mut session,
            "SELECT values::text FROM insert_subscript_domain_target",
        )
        .await
            == Some("{11,NULL,22}".into())
    );
    run(
        &mut session,
        "UPDATE insert_subscript_domain_target SET values[2] = 33",
    )
    .await;
    assert!(
        scalar(
            &mut session,
            "SELECT values::text FROM insert_subscript_domain_target",
        )
        .await
            == Some("{11,33,22}".into())
    );
}

#[tokio::test]
async fn an_insert_instead_rule_runs_its_action_without_writing_the_base_table() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE redirected_rows (value int)").await;
    run(&mut session, "CREATE TABLE redirect_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE redirect_insert AS ON INSERT TO redirected_rows \
         DO INSTEAD INSERT INTO redirect_log VALUES (NEW.*)",
    )
    .await;
    run(&mut session, "INSERT INTO redirected_rows VALUES (1), (2)").await;

    assert!(scalar(&mut session, "SELECT count(*) FROM redirected_rows").await == Some("0".into()));
    assert!(
        scalar(
            &mut session,
            "SELECT string_agg(value::text, ',' ORDER BY value) FROM redirect_log",
        )
        .await
            == Some("1,2".into())
    );
}

#[tokio::test]
async fn a_conditional_insert_instead_rule_suppresses_only_matching_rows() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE conditional_rows (value int)").await;
    run(
        &mut session,
        "CREATE RULE ignore_one AS ON INSERT TO conditional_rows \
         WHERE NEW.value = 1 DO INSTEAD NOTHING",
    )
    .await;

    run(&mut session, "INSERT INTO conditional_rows VALUES (1), (2)").await;
    assert!(
        scalar(
            &mut session,
            "SELECT string_agg(value::text, ',' ORDER BY value) FROM conditional_rows",
        )
        .await
            == Some("2".into())
    );
}

#[tokio::test]
async fn an_insert_also_rule_runs_each_parenthesized_action() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE action_source (value int)").await;
    run(&mut session, "CREATE TABLE first_log (value int)").await;
    run(&mut session, "CREATE TABLE second_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE copy_twice AS ON INSERT TO action_source DO ALSO (\
         INSERT INTO first_log VALUES (NEW.*); \
         INSERT INTO second_log VALUES (NEW.*))",
    )
    .await;
    run(&mut session, "INSERT INTO action_source VALUES (7)").await;

    assert!(scalar(&mut session, "SELECT value::text FROM first_log").await == Some("7".into()));
    assert!(scalar(&mut session, "SELECT value::text FROM second_log").await == Some("7".into()));
}

#[tokio::test]
async fn pg_get_ruledef_normalizes_each_parenthesized_insert_action() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE action_source (value int)").await;
    run(&mut session, "CREATE TABLE first_log (value int)").await;
    run(&mut session, "CREATE TABLE second_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE copy_twice AS ON INSERT TO action_source DO ALSO (\
         INSERT INTO first_log VALUES (NEW.*); \
         INSERT INTO second_log VALUES (NEW.*))",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'copy_twice'",
        )
        .await
            == Some(
                "CREATE RULE copy_twice AS\n    ON INSERT TO public.action_source DO ( \
                 INSERT INTO first_log (value)\n  VALUES (new.value);\n \
                 INSERT INTO second_log (value)\n  VALUES (new.value);\n);"
                    .into(),
            )
    );
}

#[tokio::test]
async fn pg_get_ruledef_separates_a_grouped_notify_action() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE grouped_source (value int)").await;
    run(&mut session, "CREATE TABLE grouped_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE grouped_actions AS ON INSERT TO grouped_source DO ALSO (\
         INSERT INTO grouped_log VALUES (NEW.*); \
         NOTIFY grouped_events, 'inserted')",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'grouped_actions'",
        )
        .await
            == Some(
                "CREATE RULE grouped_actions AS\n    ON INSERT TO public.grouped_source DO ( \
                 INSERT INTO grouped_log (value)\n  VALUES (new.value);\n\n \
                 NOTIFY grouped_events, 'inserted';\n);"
                    .into(),
            )
    );
}

#[tokio::test]
async fn pg_get_ruledef_collapses_a_one_action_notify_list() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE one_notify_source (value int)").await;
    run(
        &mut session,
        "CREATE RULE one_notify AS ON INSERT TO one_notify_source \
         DO ALSO (NOTIFY one_notify_events, 'inserted')",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'one_notify'",
        )
        .await
            == Some(
                "CREATE RULE one_notify AS\n    ON INSERT TO public.one_notify_source DO\n \
                 NOTIFY one_notify_events, 'inserted';"
                    .into(),
            )
    );
}

#[tokio::test]
async fn pg_get_ruledef_collapses_a_one_action_insert_list() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE one_insert_source (value int)").await;
    run(&mut session, "CREATE TABLE one_insert_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE one_insert AS ON INSERT TO one_insert_source \
         DO ALSO (INSERT INTO one_insert_log VALUES (NEW.*))",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'one_insert'",
        )
        .await
            == Some(
                "CREATE RULE one_insert AS\n    ON INSERT TO public.one_insert_source DO  \
                 INSERT INTO one_insert_log (value)\n  VALUES (new.value);"
                    .into(),
            )
    );
}

#[tokio::test]
async fn pg_get_ruledef_breaks_before_a_notify_first_grouped_action() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE notify_first_source (value int)").await;
    run(&mut session, "CREATE TABLE notify_first_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE notify_first AS ON INSERT TO notify_first_source DO ALSO (\
         NOTIFY notify_first_events, 'inserted'; \
         INSERT INTO notify_first_log VALUES (NEW.*))",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'notify_first'",
        )
        .await
            == Some(
                "CREATE RULE notify_first AS\n    ON INSERT TO public.notify_first_source DO (\n \
                 NOTIFY notify_first_events, 'inserted';\n INSERT INTO notify_first_log (value)\n  \
                 VALUES (new.value);\n);"
                    .into(),
            )
    );
}

#[tokio::test]
async fn pg_get_ruledef_keeps_a_new_column_as_one_qualified_value() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE source_columns (value int, ignored int)",
    )
    .await;
    run(&mut session, "CREATE TABLE value_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE copy_value AS ON INSERT TO source_columns \
         DO ALSO INSERT INTO value_log VALUES (NEW.value)",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'copy_value'",
        )
        .await
            == Some(
                "CREATE RULE copy_value AS\n    ON INSERT TO public.source_columns DO  \
                 INSERT INTO value_log (value)\n  VALUES (new.value);"
                    .into(),
            )
    );
}

#[tokio::test]
async fn pg_get_ruledef_keeps_new_qualified_in_a_rule_predicate() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE conditional_source (value int)").await;
    run(&mut session, "CREATE TABLE conditional_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE copy_larger_values AS ON INSERT TO conditional_source \
         WHERE NEW.value > 1 DO ALSO INSERT INTO conditional_log VALUES (NEW.value)",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'copy_larger_values'",
        )
        .await
            == Some(
                "CREATE RULE copy_larger_values AS\n    ON INSERT TO public.conditional_source\n   \
                 WHERE (new.value > 1) DO  INSERT INTO conditional_log (value)\n  \
                 VALUES (new.value);"
                    .into(),
            )
    );
}

#[tokio::test]
async fn disabling_a_rule_stops_it_and_preserves_its_comment() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE enable_source (value int)").await;
    run(&mut session, "CREATE TABLE enable_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE copy_enabled AS ON INSERT TO enable_source \
         DO ALSO INSERT INTO enable_log VALUES (NEW.*)",
    )
    .await;
    run(
        &mut session,
        "COMMENT ON RULE copy_enabled ON enable_source IS 'copy audit'",
    )
    .await;
    run(
        &mut session,
        "ALTER TABLE enable_source DISABLE RULE copy_enabled",
    )
    .await;
    run(&mut session, "INSERT INTO enable_source VALUES (1)").await;

    assert!(scalar(&mut session, "SELECT count(*) FROM enable_log").await == Some("0".into()));
    assert!(
        scalar(
            &mut session,
            "SELECT ev_enabled FROM pg_rewrite WHERE rulename = 'copy_enabled'",
        )
        .await
            == Some("D".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM pg_description \
             WHERE classoid = 'pg_rewrite'::regclass AND objoid = \
             (SELECT oid FROM pg_rewrite WHERE rulename = 'copy_enabled') AND description = 'copy audit'",
        )
        .await
            == Some("1".into())
    );

    run(
        &mut session,
        "ALTER TABLE enable_source ENABLE REPLICA RULE copy_enabled",
    )
    .await;
    run(&mut session, "INSERT INTO enable_source VALUES (2)").await;
    assert!(scalar(&mut session, "SELECT count(*) FROM enable_log").await == Some("0".into()));
    assert!(
        scalar(
            &mut session,
            "SELECT ev_enabled FROM pg_rewrite WHERE rulename = 'copy_enabled'",
        )
        .await
            == Some("R".into())
    );
    run(&mut session, "SET session_replication_role = replica").await;
    run(&mut session, "INSERT INTO enable_source VALUES (3)").await;
    assert!(scalar(&mut session, "SELECT value::text FROM enable_log").await == Some("3".into()));
}

#[tokio::test]
async fn replacing_a_rule_preserves_its_enabled_state_and_comment() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE replace_source (value int)").await;
    run(&mut session, "CREATE TABLE replace_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE replace_audit AS ON INSERT TO replace_source DO ALSO NOTHING",
    )
    .await;
    run(
        &mut session,
        "COMMENT ON RULE replace_audit ON replace_source IS 'keep me'",
    )
    .await;
    run(
        &mut session,
        "ALTER TABLE replace_source DISABLE RULE replace_audit",
    )
    .await;
    let oid = scalar(
        &mut session,
        "SELECT oid::text FROM pg_rewrite WHERE rulename = 'replace_audit'",
    )
    .await;

    run(
        &mut session,
        "CREATE OR REPLACE RULE replace_audit AS ON INSERT TO replace_source \
         DO ALSO INSERT INTO replace_log VALUES (NEW.value)",
    )
    .await;
    run(&mut session, "INSERT INTO replace_source VALUES (7)").await;

    assert!(
        scalar(
            &mut session,
            "SELECT oid::text FROM pg_rewrite WHERE rulename = 'replace_audit'",
        )
        .await
            == oid
    );
    assert!(
        scalar(
            &mut session,
            "SELECT ev_enabled FROM pg_rewrite WHERE rulename = 'replace_audit'",
        )
        .await
            == Some("D".into())
    );
    assert!(
        scalar(&mut session, "SELECT count(*)::text FROM replace_log").await == Some("0".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM pg_description \
             WHERE classoid = 'pg_rewrite'::regclass AND objoid = \
             (SELECT oid FROM pg_rewrite WHERE rulename = 'replace_audit') AND description = 'keep me'",
        )
        .await
            == Some("1".into())
    );
}

#[tokio::test]
async fn update_and_delete_instead_nothing_rules_suppress_their_base_writes() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE update_ignored (value int)").await;
    run(&mut session, "INSERT INTO update_ignored VALUES (1)").await;
    run(
        &mut session,
        "CREATE RULE ignore_update AS ON UPDATE TO update_ignored DO INSTEAD NOTHING",
    )
    .await;
    run(&mut session, "UPDATE update_ignored SET value = 2").await;
    assert!(
        scalar(&mut session, "SELECT value::text FROM update_ignored").await == Some("1".into())
    );

    run(&mut session, "CREATE TABLE delete_ignored (value int)").await;
    run(&mut session, "INSERT INTO delete_ignored VALUES (1)").await;
    run(
        &mut session,
        "CREATE RULE ignore_delete AS ON DELETE TO delete_ignored DO INSTEAD NOTHING",
    )
    .await;
    run(&mut session, "DELETE FROM delete_ignored").await;
    assert!(scalar(&mut session, "SELECT count(*) FROM delete_ignored").await == Some("1".into()));

    run(&mut session, "CREATE TABLE truncate_ignored (value int)").await;
    run(&mut session, "INSERT INTO truncate_ignored VALUES (1)").await;
    run(
        &mut session,
        "CREATE RULE ignore_truncate_delete AS ON DELETE TO truncate_ignored DO INSTEAD NOTHING",
    )
    .await;
    run(&mut session, "TRUNCATE TABLE truncate_ignored").await;
    assert!(
        scalar(&mut session, "SELECT count(*) FROM truncate_ignored").await == Some("0".into())
    );
}

#[tokio::test]
async fn conditional_update_and_delete_nothing_rules_test_new_and_old_rows() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE conditional_update (value int)").await;
    run(
        &mut session,
        "INSERT INTO conditional_update VALUES (1), (2)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE suppress_twenty AS ON UPDATE TO conditional_update \
         WHERE NEW.value = 20 DO INSTEAD NOTHING",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE ignored_delete_event AS ON DELETE TO conditional_update DO INSTEAD NOTHING",
    )
    .await;
    run(
        &mut session,
        "UPDATE conditional_update SET value = value * 10",
    )
    .await;
    assert!(
        scalar(
            &mut session,
            "SELECT string_agg(value::text, ',' ORDER BY value) FROM conditional_update",
        )
        .await
            == Some("2,10".into())
    );

    run(&mut session, "CREATE TABLE conditional_delete (value int)").await;
    run(
        &mut session,
        "INSERT INTO conditional_delete VALUES (1), (2)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE keep_one AS ON DELETE TO conditional_delete \
         WHERE OLD.value = 1 DO INSTEAD NOTHING",
    )
    .await;
    run(&mut session, "DELETE FROM conditional_delete").await;
    assert!(
        scalar(
            &mut session,
            "SELECT string_agg(value::text, ',' ORDER BY value) FROM conditional_delete",
        )
        .await
            == Some("1".into())
    );
}

#[tokio::test]
async fn update_and_delete_instead_rules_bind_old_and_new_rows_in_their_actions() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE update_source (value int)").await;
    run(
        &mut session,
        "CREATE TABLE update_log (old_value int, new_value int)",
    )
    .await;
    run(&mut session, "INSERT INTO update_source VALUES (1)").await;
    run(
        &mut session,
        "CREATE RULE log_update AS ON UPDATE TO update_source \
         DO INSTEAD INSERT INTO update_log VALUES (OLD.*, NEW.*)",
    )
    .await;
    run(&mut session, "UPDATE update_source SET value = 2").await;
    assert!(
        scalar(&mut session, "SELECT value::text FROM update_source").await == Some("1".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT old_value::text || ':' || new_value::text FROM update_log"
        )
        .await
            == Some("1:2".into())
    );

    run(&mut session, "CREATE TABLE delete_source (value int)").await;
    run(&mut session, "CREATE TABLE delete_log (value int)").await;
    run(&mut session, "INSERT INTO delete_source VALUES (3)").await;
    run(
        &mut session,
        "CREATE RULE log_delete AS ON DELETE TO delete_source \
         DO INSTEAD INSERT INTO delete_log VALUES (OLD.*)",
    )
    .await;
    run(&mut session, "DELETE FROM delete_source").await;
    assert!(scalar(&mut session, "SELECT count(*) FROM delete_source").await == Some("1".into()));
    assert!(scalar(&mut session, "SELECT value::text FROM delete_log").await == Some("3".into()));
}

#[tokio::test]
async fn update_rule_actions_bind_individual_old_and_new_columns() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE detail_source (value int)").await;
    run(
        &mut session,
        "CREATE TABLE detail_log (old_value int, new_value int)",
    )
    .await;
    run(&mut session, "INSERT INTO detail_source VALUES (1)").await;
    run(
        &mut session,
        "CREATE RULE detail_update AS ON UPDATE TO detail_source \
         DO ALSO INSERT INTO detail_log VALUES (OLD.value, NEW.value)",
    )
    .await;
    run(&mut session, "UPDATE detail_source SET value = 2").await;

    assert!(
        scalar(
            &mut session,
            "SELECT old_value::text || ':' || new_value::text FROM detail_log",
        )
        .await
            == Some("1:2".into())
    );
}

#[tokio::test]
async fn update_and_delete_rule_actions_use_bound_images_in_write_clauses() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE rule_action_source (value int)").await;
    run(
        &mut session,
        "CREATE TABLE rule_action_update_log (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE rule_action_delete_log (value int)",
    )
    .await;
    run(&mut session, "INSERT INTO rule_action_source VALUES (1)").await;
    run(
        &mut session,
        "INSERT INTO rule_action_update_log VALUES (0)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO rule_action_delete_log VALUES (1), (2)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE update_log_value AS ON UPDATE TO rule_action_source \
         DO ALSO UPDATE rule_action_update_log SET value = NEW.value",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE delete_log_value AS ON DELETE TO rule_action_source \
         DO ALSO DELETE FROM rule_action_delete_log WHERE value = OLD.value",
    )
    .await;

    run(&mut session, "UPDATE rule_action_source SET value = 2").await;
    assert!(
        scalar(
            &mut session,
            "SELECT value::text FROM rule_action_update_log"
        )
        .await
            == Some("2".into())
    );
    run(&mut session, "DELETE FROM rule_action_source").await;
    assert!(
        scalar(
            &mut session,
            "SELECT string_agg(value::text, ',' ORDER BY value) FROM rule_action_delete_log",
        )
        .await
            == Some("1".into())
    );
}

#[tokio::test]
async fn update_and_delete_rule_actions_bind_images_in_from_and_using_items() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE rule_from_source (value int)").await;
    run(&mut session, "CREATE TABLE rule_from_log (value int)").await;
    run(&mut session, "CREATE TABLE rule_using_log (value int)").await;
    run(&mut session, "INSERT INTO rule_from_source VALUES (1)").await;
    run(&mut session, "INSERT INTO rule_from_log VALUES (0)").await;
    run(&mut session, "INSERT INTO rule_using_log VALUES (1), (2)").await;
    run(
        &mut session,
        "CREATE RULE update_from_image AS ON UPDATE TO rule_from_source \
         DO ALSO UPDATE rule_from_log SET value = image.value \
         FROM (SELECT NEW.value AS value) AS image",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE delete_using_image AS ON DELETE TO rule_from_source \
         DO ALSO DELETE FROM rule_using_log USING generate_series(OLD.value, OLD.value) \
         AS image(value) WHERE rule_using_log.value = image.value",
    )
    .await;

    run(&mut session, "UPDATE rule_from_source SET value = 2").await;
    assert!(
        scalar(&mut session, "SELECT value::text FROM rule_from_log").await == Some("2".into())
    );
    run(&mut session, "DELETE FROM rule_from_source").await;
    assert!(
        scalar(
            &mut session,
            "SELECT string_agg(value::text, ',' ORDER BY value) FROM rule_using_log",
        )
        .await
            == Some("1".into())
    );
}

#[tokio::test]
async fn an_instead_rule_action_expands_an_image_wildcard_in_returning() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE rule_returning_source (value int)",
    )
    .await;
    run(&mut session, "CREATE TABLE rule_returning_log (value int)").await;
    run(&mut session, "INSERT INTO rule_returning_source VALUES (1)").await;
    run(&mut session, "INSERT INTO rule_returning_log VALUES (0)").await;
    run(
        &mut session,
        "CREATE RULE update_returning_image AS ON UPDATE TO rule_returning_source \
         DO INSTEAD UPDATE rule_returning_log SET value = 99 RETURNING NEW.*",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "UPDATE rule_returning_source SET value = 2 RETURNING value::text",
        )
        .await
            == Some("2".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM rule_returning_source"
        )
        .await
            == Some("1".into())
    );

    run(
        &mut session,
        "CREATE TABLE rule_returning_old_source (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE rule_returning_old_log (value int)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO rule_returning_old_source VALUES (1)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO rule_returning_old_log VALUES (0)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE update_returning_old_image AS ON UPDATE TO rule_returning_old_source \
         DO INSTEAD UPDATE rule_returning_old_log SET value = 99 RETURNING OLD.*",
    )
    .await;
    assert!(
        scalar(
            &mut session,
            "UPDATE rule_returning_old_source SET value = 2 RETURNING value::text",
        )
        .await
            == Some("1".into())
    );
}

#[tokio::test]
async fn an_insert_default_values_rule_action_binds_its_returning_image() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE rule_default_source (value int)").await;
    run(
        &mut session,
        "CREATE TABLE rule_default_log (value int DEFAULT 42)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE insert_default_returning AS ON INSERT TO rule_default_source \
         DO INSTEAD INSERT INTO rule_default_log DEFAULT VALUES RETURNING NEW.*",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "INSERT INTO rule_default_source VALUES (7) RETURNING value::text",
        )
        .await
            == Some("7".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM rule_default_source"
        )
        .await
            == Some("0".into())
    );
    assert!(
        scalar(&mut session, "SELECT value::text FROM rule_default_log").await == Some("42".into())
    );
}

#[tokio::test]
async fn pg_get_ruledef_normalizes_simple_update_and_delete_actions() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE action_source (value int)").await;
    run(&mut session, "CREATE TABLE action_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE update_insert AS ON INSERT TO action_source \
         DO ALSO UPDATE action_log SET value = NEW.value WHERE action_log.value = 0 RETURNING value",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE delete_insert AS ON INSERT TO action_source \
         DO ALSO DELETE FROM action_log WHERE action_log.value = NEW.value RETURNING value",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE update_returning_expression AS ON INSERT TO action_source \
         DO ALSO UPDATE action_log SET value = NEW.value RETURNING value + 1",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'update_insert'",
        )
        .await
            == Some(
                "CREATE RULE update_insert AS\n    ON INSERT TO public.action_source DO  \
                 UPDATE action_log SET value = new.value\n  WHERE (action_log.value = 0)\n  RETURNING action_log.value;"
                    .into(),
            )
    );
    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'update_returning_expression'",
        )
        .await
            == Some(
                "CREATE RULE update_returning_expression AS\n    ON INSERT TO public.action_source DO \
                 UPDATE action_log SET value = NEW.value RETURNING value + 1;"
                    .into(),
            )
    );
    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'delete_insert'",
        )
        .await
            == Some(
                "CREATE RULE delete_insert AS\n    ON INSERT TO public.action_source DO  \
                 DELETE FROM action_log\n  WHERE (action_log.value = new.value)\n  RETURNING action_log.value;"
                    .into(),
            )
    );
}

#[tokio::test]
async fn an_instead_rule_returns_its_action_rows() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE returning_source (value int)").await;
    run(&mut session, "CREATE TABLE returning_log (value int)").await;
    run(&mut session, "INSERT INTO returning_source VALUES (7)").await;
    run(
        &mut session,
        "CREATE RULE redirect_returning_delete AS ON DELETE TO returning_source \
         DO INSTEAD INSERT INTO returning_log VALUES (OLD.value) RETURNING value",
    )
    .await;

    assert!(
        scalar(&mut session, "DELETE FROM returning_source RETURNING value").await
            == Some("7".into())
    );
    assert!(
        scalar(&mut session, "SELECT count(*)::text FROM returning_source").await
            == Some("1".into())
    );
    assert!(
        scalar(&mut session, "SELECT value::text FROM returning_log").await == Some("7".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'redirect_returning_delete'",
        )
        .await
            == Some(
                "CREATE RULE redirect_returning_delete AS\n    ON DELETE TO public.returning_source DO INSTEAD  \
                 INSERT INTO returning_log (value)\n  VALUES (old.value)\n  RETURNING returning_log.value;"
                    .into(),
            )
    );
}

#[tokio::test]
async fn an_instead_action_returning_clause_does_not_change_a_command_only_statement() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE command_returning_source (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE command_returning_log (value int)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO command_returning_source VALUES (7)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE command_returning_delete AS ON DELETE TO command_returning_source \
         DO INSTEAD INSERT INTO command_returning_log VALUES (OLD.value) RETURNING value",
    )
    .await;

    let result = session
        .simple_query("DELETE FROM command_returning_source")
        .await
        .expect("command-only delete succeeds");
    assert!(matches!(result.as_slice(), [QueryResult::Command { .. }]));
    assert!(
        scalar(
            &mut session,
            "SELECT value::text FROM command_returning_log"
        )
        .await
            == Some("7".into())
    );
}

#[tokio::test]
async fn an_insert_rule_action_can_select_from_new() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE query_action_source (value int)").await;
    run(&mut session, "CREATE TABLE query_action_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE query_action AS ON INSERT TO query_action_source \
         DO INSTEAD INSERT INTO query_action_log SELECT NEW.value",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'query_action'",
        )
        .await
            == Some(
                "CREATE RULE query_action AS\n    ON INSERT TO public.query_action_source DO INSTEAD  \
                 INSERT INTO query_action_log (value)  SELECT new.value;"
                    .into(),
            )
    );
    run(&mut session, "INSERT INTO query_action_source VALUES (9)").await;

    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM query_action_source"
        )
        .await
            == Some("0".into())
    );
    assert!(
        scalar(&mut session, "SELECT value::text FROM query_action_log").await == Some("9".into())
    );
}

#[tokio::test]
async fn an_instead_select_rule_action_returns_its_bound_new_row() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE select_action_source (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE select_action_lock_target (value int)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO select_action_lock_target VALUES (9)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE select_action AS ON INSERT TO select_action_source \
         DO INSTEAD SELECT NEW.value FROM select_action_lock_target \
         FOR UPDATE OF select_action_lock_target",
    )
    .await;

    let result = session
        .simple_query("INSERT INTO select_action_source VALUES (9)")
        .await
        .expect("INSERT uses its SELECT rule action");
    let [QueryResult::Rows { rows, .. }] = result.as_slice() else {
        panic!("expected rule action rows, got {result:?}");
    };
    assert!(rows.len() == 1);
    assert!(rows[0][0].as_ref().map(|cell| cell.text.as_ref()) == Some(&b"9"[..]));
    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM select_action_source"
        )
        .await
            == Some("0".into())
    );
}

#[tokio::test]
async fn a_rule_action_cannot_lock_the_old_or_new_image() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE rule_lock_image_source (value int)",
    )
    .await;
    let sql = "CREATE RULE invalid_lock_image AS ON UPDATE TO rule_lock_image_source \
               DO INSTEAD SELECT * FROM rule_lock_image_source FOR UPDATE OF old";
    let error = session
        .simple_query(sql)
        .await
        .expect_err("OLD cannot name a locked relation");
    assert!(format!("{error:?}").contains("FOR UPDATE clause not found in FROM clause"));
    assert!(
        error
            .diagnostics
            .as_ref()
            .and_then(|diagnostics| diagnostics.position)
            == Some(sql.rfind("old").expect("OLD") + 1)
    );
}

#[tokio::test]
async fn a_rule_values_action_requires_a_qualified_column() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE rule_values_source (value int)").await;
    run(&mut session, "CREATE TABLE rule_values_target (value int)").await;
    let sql = "CREATE RULE invalid_rule_values AS ON INSERT TO rule_values_source\n\
               DO INSTEAD INSERT INTO rule_values_target VALUES (value)";
    let error = session
        .simple_query(sql)
        .await
        .expect_err("unqualified rule column is rejected");
    let diagnostics = error.diagnostics.expect("detail and hint");
    assert!(
        diagnostics.detail.as_deref()
            == Some(
                "There are columns named \"value\", but they are in tables that cannot be referenced from this part of the query."
            )
    );
    assert!(diagnostics.hint.as_deref() == Some("Try using a table-qualified name."));
    assert!(diagnostics.position == Some(sql.find("(value)").expect("column") + 2));
}

#[tokio::test]
async fn an_insert_rule_action_can_select_all_new_columns() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE query_wildcard_source (id int, value text)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE query_wildcard_log (id int, value text)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE query_wildcard AS ON INSERT TO query_wildcard_source \
         DO INSTEAD INSERT INTO query_wildcard_log SELECT NEW.*",
    )
    .await;

    run(
        &mut session,
        "INSERT INTO query_wildcard_source VALUES (9, 'copied')",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM query_wildcard_source"
        )
        .await
            == Some("0".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT id::text || ':' || value FROM query_wildcard_log",
        )
        .await
            == Some("9:copied".into())
    );
}

#[tokio::test]
async fn a_delete_rule_action_can_select_all_old_columns() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE old_wildcard_source (id int, value text)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE old_wildcard_log (id int, value text)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO old_wildcard_source VALUES (4, 'removed')",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE old_wildcard AS ON DELETE TO old_wildcard_source \
         DO INSTEAD INSERT INTO old_wildcard_log SELECT OLD.*",
    )
    .await;

    run(&mut session, "DELETE FROM old_wildcard_source").await;

    assert!(
        scalar(
            &mut session,
            "SELECT count(*)::text FROM old_wildcard_source"
        )
        .await
            == Some("1".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT id::text || ':' || value FROM old_wildcard_log",
        )
        .await
            == Some("4:removed".into())
    );
}

#[tokio::test]
async fn pg_get_ruledef_normalizes_default_values_actions() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE default_source (value int)").await;
    run(
        &mut session,
        "CREATE TABLE default_log (value int DEFAULT 7)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE default_action AS ON INSERT TO default_source \
         DO ALSO INSERT INTO default_log DEFAULT VALUES",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'default_action'",
        )
        .await
            == Some(
                "CREATE RULE default_action AS\n    ON INSERT TO public.default_source DO  \
                 INSERT INTO default_log DEFAULT VALUES;"
                    .into(),
            )
    );
}

#[tokio::test]
async fn pg_get_ruledef_normalizes_simple_returning_aliases() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE returning_alias_source (value int)",
    )
    .await;
    run(&mut session, "CREATE TABLE returning_alias_log (value int)").await;
    run(
        &mut session,
        "CREATE RULE returning_item_alias AS ON DELETE TO returning_alias_source \
         DO INSTEAD INSERT INTO returning_alias_log VALUES (OLD.value) RETURNING value AS copied",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE returning_image_alias AS ON UPDATE TO returning_alias_source \
         DO INSTEAD INSERT INTO returning_alias_log VALUES (NEW.value) \
         RETURNING WITH (NEW AS action_new) value",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE returning_expression AS ON DELETE TO returning_alias_source \
         DO INSTEAD INSERT INTO returning_alias_log VALUES (OLD.value) RETURNING value + 1",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE returning_wildcard_source (value int)",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE returning_wildcard_log (value int)",
    )
    .await;
    run(
        &mut session,
        "INSERT INTO returning_wildcard_source VALUES (11)",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE returning_wildcard AS ON DELETE TO returning_wildcard_source \
         DO INSTEAD INSERT INTO returning_wildcard_log VALUES (OLD.value) RETURNING *",
    )
    .await;
    run(
        &mut session,
        "CREATE RULE returning_qualified_wildcard AS ON UPDATE TO returning_wildcard_source \
         DO INSTEAD INSERT INTO returning_wildcard_log VALUES (NEW.value) \
         RETURNING returning_wildcard_log.*",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'returning_item_alias'",
        )
        .await
            == Some(
                "CREATE RULE returning_item_alias AS\n    ON DELETE TO public.returning_alias_source DO INSTEAD  \
                 INSERT INTO returning_alias_log (value)\n  VALUES (old.value)\n  RETURNING returning_alias_log.value AS copied;"
                    .into(),
            )
    );
    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'returning_qualified_wildcard'",
        )
        .await
            == Some(
                "CREATE RULE returning_qualified_wildcard AS\n    ON UPDATE TO public.returning_wildcard_source DO INSTEAD  \
                 INSERT INTO returning_wildcard_log (value)\n  VALUES (new.value)\n  RETURNING returning_wildcard_log.value;"
                    .into(),
            )
    );
    assert!(
        scalar(
            &mut session,
            "DELETE FROM returning_wildcard_source RETURNING value",
        )
        .await
            == Some("11".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'returning_wildcard'",
        )
        .await
            == Some(
                "CREATE RULE returning_wildcard AS\n    ON DELETE TO public.returning_wildcard_source DO INSTEAD  \
                 INSERT INTO returning_wildcard_log (value)\n  VALUES (old.value)\n  RETURNING returning_wildcard_log.value;"
                    .into(),
            )
    );
    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'returning_image_alias'",
        )
        .await
            == Some(
                "CREATE RULE returning_image_alias AS\n    ON UPDATE TO public.returning_alias_source DO INSTEAD  \
                 INSERT INTO returning_alias_log (value)\n  VALUES (new.value)\n  RETURNING WITH (NEW AS action_new) returning_alias_log.value;"
                    .into(),
            )
    );
    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'returning_expression'",
        )
        .await
            == Some(
                "CREATE RULE returning_expression AS\n    ON DELETE TO public.returning_alias_source DO INSTEAD \
                 INSERT INTO returning_alias_log VALUES (OLD.value) RETURNING value + 1;"
                    .into(),
            )
    );
}

#[tokio::test]
async fn update_and_delete_also_rules_run_after_the_base_write() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE update_source (value int)").await;
    run(
        &mut session,
        "CREATE TABLE update_log (old_value int, new_value int)",
    )
    .await;
    run(&mut session, "INSERT INTO update_source VALUES (1)").await;
    run(
        &mut session,
        "CREATE RULE log_update AS ON UPDATE TO update_source \
         DO ALSO INSERT INTO update_log VALUES (OLD.*, NEW.*)",
    )
    .await;
    run(&mut session, "UPDATE update_source SET value = 2").await;
    assert!(
        scalar(&mut session, "SELECT value::text FROM update_source").await == Some("2".into())
    );
    assert!(
        scalar(
            &mut session,
            "SELECT old_value::text || ':' || new_value::text FROM update_log"
        )
        .await
            == Some("1:2".into())
    );

    run(&mut session, "CREATE TABLE delete_source (value int)").await;
    run(&mut session, "CREATE TABLE delete_log (value int)").await;
    run(&mut session, "INSERT INTO delete_source VALUES (3)").await;
    run(
        &mut session,
        "CREATE RULE log_delete AS ON DELETE TO delete_source \
         DO ALSO INSERT INTO delete_log VALUES (OLD.*)",
    )
    .await;
    run(&mut session, "DELETE FROM delete_source").await;
    assert!(scalar(&mut session, "SELECT count(*) FROM delete_source").await == Some("0".into()));
    assert!(scalar(&mut session, "SELECT value::text FROM delete_log").await == Some("3".into()));
}

#[tokio::test]
async fn insert_and_update_follow_composite_field_and_array_paths() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TYPE write_pair AS (r int, s int)").await;
    run(
        &mut session,
        "CREATE DOMAIN write_pair_domain AS write_pair",
    )
    .await;
    run(
        &mut session,
        "CREATE DOMAIN write_pair_array_domain AS write_pair[]",
    )
    .await;
    run(
        &mut session,
        "CREATE TABLE composite_targets (d write_pair_domain, a write_pair[], b write_pair_array_domain)",
    )
    .await;

    run(
        &mut session,
        "INSERT INTO composite_targets (d.r, a[1].r, b[1].r) VALUES (11, 21, 31)",
    )
    .await;
    run(
        &mut session,
        "UPDATE composite_targets SET d.r = (d).r + 1, a[1].r = a[1].r + 1, b[1].r = b[1].r + 1",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT (d).r::text FROM composite_targets").await
            == Some("12".into())
    );
    assert!(
        scalar(&mut session, "SELECT a[1].r::text FROM composite_targets").await
            == Some("22".into())
    );
    assert!(
        scalar(&mut session, "SELECT b[1].r::text FROM composite_targets").await
            == Some("32".into())
    );
}
