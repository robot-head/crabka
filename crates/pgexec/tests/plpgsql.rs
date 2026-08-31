//! Behavior-level PL/pgSQL conformance, adapted from `PostgreSQL` 18's
//! `plpgsql_simple`, `plpgsql_control`, and `plpgsql_trap` coverage to SQL
//! features Crabka already exposes.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, FieldDescription, QueryResult, Session};

async fn execute(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error:?}"))
}

async fn query(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    let results = execute(session, sql).await;
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("`{sql}` did not return rows: {:?}", results[0]);
    };
    rows.iter()
        .map(|row| row.iter().map(|cell| cell.as_ref().map(text)).collect())
        .collect()
}

async fn scalar(session: &mut SqlSession, sql: &str) -> Option<String> {
    let rows = query(session, sql).await;
    assert!(rows.len() == 1, "`{sql}` returned {rows:?}");
    assert!(rows[0].len() == 1, "`{sql}` returned {rows:?}");
    rows[0][0].clone()
}

fn text(cell: &Cell) -> String {
    String::from_utf8(cell.text.to_vec()).expect("server text is UTF-8")
}

/// How `sql` describes its single column, and the rows it answers with.
async fn described(
    session: &mut SqlSession,
    sql: &str,
) -> (FieldDescription, Vec<Vec<Option<String>>>) {
    let results = execute(session, sql).await;
    let QueryResult::Rows { fields, rows, .. } = &results[0] else {
        panic!("`{sql}` did not return rows: {:?}", results[0]);
    };
    assert!(fields.len() == 1, "`{sql}` returned {fields:?}");
    (
        fields[0].clone(),
        rows.iter()
            .map(|row| row.iter().map(|cell| cell.as_ref().map(text)).collect())
            .collect(),
    )
}

/// A `text` column of the given name, as the wire describes it.
fn text_field(name: &str) -> FieldDescription {
    FieldDescription {
        name: name.into(),
        table_oid: 0,
        column_id: 0,
        type_oid: 25,
        type_size: -1,
        type_modifier: -1,
        format: 0,
    }
}

fn row(values: &[&str]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|value| Some((*value).to_string()))
        .collect()
}

#[tokio::test]
async fn scalar_functions_are_evaluated_for_each_input_row() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE pl_scalar_input (n int4);
        INSERT INTO pl_scalar_input VALUES (1), (2), (3);
        CREATE FUNCTION pl_transform(x int4) RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          IF x % 2 = 0 THEN
            RETURN x * 10;
          END IF;
          RETURN x + 1;
        END
        $$
        ",
    )
    .await;

    assert!(
        query(
            &mut session,
            "SELECT n, pl_transform(n) FROM pl_scalar_input ORDER BY n",
        )
        .await
            == vec![row(&["1", "2"]), row(&["2", "20"]), row(&["3", "4"])]
    );
}

#[tokio::test]
async fn labeled_control_flow_and_simple_case_choose_the_postgres_branch() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(&mut session, "CREATE TABLE pl_control_result (total int4)").await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE total int4 := 0;
        BEGIN
          <<numbers>>
          FOR i IN 1..5 LOOP
            CONTINUE numbers WHEN i = 2;
            CASE i
              WHEN 1 THEN total := total + 1;
              WHEN 3 THEN total := total + 30;
              ELSE total := total + 100;
            END CASE;
            EXIT numbers WHEN i = 3;
          END LOOP numbers;
          INSERT INTO pl_control_result VALUES (total);
        END
        $$
        ",
    )
    .await;

    assert!(scalar(&mut session, "SELECT total FROM pl_control_result").await == Some("31".into()));
}

#[tokio::test]
async fn case_without_a_matching_arm_raises_case_not_found() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let error = session
        .simple_query("DO $$ BEGIN CASE 7 WHEN 1 THEN NULL; END CASE; END $$")
        .await
        .expect_err("CASE without ELSE must fail");

    assert!(error.code == "20000", "{error:?}");
}

#[tokio::test]
async fn strict_into_reports_postgres_diagnostics_and_extra_checks() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let mut notices = session.take_notices().expect("notice receiver");
    execute(
        &mut session,
        r"
        CREATE TABLE strict_source (value int4);
        INSERT INTO strict_source VALUES (1), (2);
        SET plpgsql.print_strict_params = on;
        CREATE FUNCTION strict_detail() RETURNS void LANGUAGE plpgsql AS $$
        DECLARE selected int4; needle int4 := 0;
        BEGIN
          SELECT value FROM strict_source WHERE value > needle INTO STRICT selected;
        END
        $$;
        CREATE FUNCTION strict_detail_off() RETURNS void LANGUAGE plpgsql AS $$
        #print_strict_params off
        DECLARE selected int4; needle int4 := 0;
        BEGIN
          SELECT value FROM strict_source WHERE value > needle INTO STRICT selected;
        END
        $$
        ",
    )
    .await;

    let error = session
        .simple_query("SELECT strict_detail()")
        .await
        .expect_err("STRICT SELECT must reject multiple rows");
    assert!(error.code == "P0003", "{error:?}");
    let diagnostics = error.diagnostics.expect("strict diagnostics");
    assert!(diagnostics.detail.as_deref() == Some("parameters: needle = '0'"));
    assert!(
        diagnostics.hint.as_deref()
            == Some("Make sure the query returns a single row, or use LIMIT 1.")
    );

    let error = session
        .simple_query("SELECT strict_detail_off()")
        .await
        .expect_err("directive still keeps STRICT behavior");
    assert!(
        error
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.detail.as_deref())
            .is_none(),
        "{error:?}"
    );

    let error = session
        .simple_query(
            "DO $$ DECLARE selected int4; BEGIN EXECUTE 'SELECT value FROM strict_source WHERE value > $1' INTO STRICT selected USING 0; END $$",
        )
        .await
        .expect_err("dynamic STRICT SELECT must reject multiple rows");
    let diagnostics = error.diagnostics.expect("dynamic strict diagnostics");
    assert!(diagnostics.detail.as_deref() == Some("parameters: $1 = '0'"));
    assert!(diagnostics.hint.is_none(), "{diagnostics:?}");

    execute(
        &mut session,
        "SET plpgsql.extra_warnings = 'strict_multi_assignment'",
    )
    .await;
    execute(
        &mut session,
        "DO $$ DECLARE left_value int4; right_value int4; BEGIN SELECT 1 INTO left_value, right_value; END $$",
    )
    .await;
    let warning = notices.try_recv().expect("assignment warning");
    assert!(warning.message == "number of source and target fields in assignment does not match");
    let diagnostics = warning.diagnostics.expect("warning diagnostics");
    assert!(
        diagnostics.detail.as_deref()
            == Some("strict_multi_assignment check of extra_warnings is active.")
    );

    execute(
        &mut session,
        "SET plpgsql.extra_errors = 'strict_multi_assignment'",
    )
    .await;
    let error = session
        .simple_query(
            "DO $$ DECLARE left_value int4; right_value int4; BEGIN SELECT 1 INTO left_value, right_value; END $$",
        )
        .await
        .expect_err("error-level assignment check must reject the mismatch");
    let diagnostics = error.diagnostics.expect("assignment error diagnostics");
    assert!(
        diagnostics.detail.as_deref()
            == Some("strict_multi_assignment check of extra_errors is active.")
    );

    execute(
        &mut session,
        "CREATE TABLE strict_pair (first_value int4, second_value int4)",
    )
    .await;
    let error = session
        .simple_query(
            "DO $$ DECLARE pair_value strict_pair; BEGIN SELECT 1, 2, 3 INTO pair_value; END $$",
        )
        .await
        .expect_err("named composite assignment must use its field count");
    let diagnostics = error.diagnostics.expect("composite assignment diagnostics");
    assert!(
        diagnostics.detail.as_deref()
            == Some("strict_multi_assignment check of extra_errors is active.")
    );

    execute(&mut session, "SET plpgsql.extra_errors = 'too_many_rows'").await;
    let error = session
        .simple_query("DO $$ DECLARE selected int4; BEGIN SELECT value FROM strict_source INTO selected; END $$")
        .await
        .expect_err("too_many_rows extra check must reject multiple rows");
    assert!(error.code == "P0003", "{error:?}");
}

#[tokio::test]
async fn query_for_assigns_each_selected_column_to_scalar_targets() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_query_input (n int4, weight int4); \
         INSERT INTO pl_query_input VALUES (1, 10), (2, 20), (3, 30); \
         CREATE TABLE pl_query_result (total int4)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE query_n int4; query_weight int4; total int4 := 0;
        BEGIN
          FOR query_n, query_weight IN
            SELECT source.n, source.weight
            FROM pl_query_input AS source
            ORDER BY source.n
          LOOP
            total := total + query_n * query_weight;
          END LOOP;
          INSERT INTO pl_query_result VALUES (total);
        END
        $$
        ",
    )
    .await;

    assert!(scalar(&mut session, "SELECT total FROM pl_query_result").await == Some("140".into()));
}

#[tokio::test]
async fn query_for_populates_record_fields() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_record_input (n int4, weight int4); \
         INSERT INTO pl_record_input VALUES (1, 10), (2, 20), (3, 30); \
         CREATE TABLE pl_record_result (total int4)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE r record; total int4 := 0;
        BEGIN
          FOR r IN
            SELECT source.n, source.weight
            FROM pl_record_input AS source
            ORDER BY source.n
          LOOP
            total := total + r.n * r.weight;
          END LOOP;
          INSERT INTO pl_record_result VALUES (total);
        END
        $$
        ",
    )
    .await;

    assert!(scalar(&mut session, "SELECT total FROM pl_record_result").await == Some("140".into()));
}

#[tokio::test]
async fn dynamic_execute_binds_using_parameters_once() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(&mut session, "CREATE TABLE pl_dynamic_result (answer int4)").await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE answer int4;
        BEGIN
          EXECUTE 'SELECT $1::int4 + $2::int4'
            INTO STRICT answer USING 19, 23;
          INSERT INTO pl_dynamic_result VALUES (answer);
        END
        $$
        ",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT answer FROM pl_dynamic_result").await == Some("42".into())
    );
}

#[tokio::test]
async fn dynamic_for_binds_using_parameters_and_iterates_rows() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_dynamic_input (n int4); \
         INSERT INTO pl_dynamic_input VALUES (1), (2), (3), (4); \
         CREATE TABLE pl_dynamic_for_result (total int4)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE n int4; total int4 := 0;
        BEGIN
          FOR n IN EXECUTE
            'SELECT n FROM pl_dynamic_input WHERE n >= $1 ORDER BY n'
            USING 2
          LOOP
            total := total + n;
          END LOOP;
          INSERT INTO pl_dynamic_for_result VALUES (total);
        END
        $$
        ",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT total FROM pl_dynamic_for_result").await == Some("9".into())
    );
}

#[tokio::test]
async fn declared_cursors_open_fetch_move_and_close() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_cursor_input (n int4); \
         INSERT INTO pl_cursor_input VALUES (10), (20), (30); \
         CREATE TABLE pl_cursor_result (total int4)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE
          values_cursor SCROLL CURSOR FOR
            SELECT n FROM pl_cursor_input ORDER BY n;
          value int4;
          total int4 := 0;
        BEGIN
          OPEN values_cursor;
          FETCH NEXT FROM values_cursor INTO value;
          total := total + value;
          MOVE NEXT FROM values_cursor;
          FETCH NEXT FROM values_cursor INTO value;
          total := total + value;
          FETCH PRIOR FROM values_cursor INTO value;
          total := total + value;
          CLOSE values_cursor;
          INSERT INTO pl_cursor_result VALUES (total);
        END
        $$
        ",
    )
    .await;

    assert!(scalar(&mut session, "SELECT total FROM pl_cursor_result").await == Some("60".into()));
}

#[tokio::test]
async fn caught_exceptions_roll_back_only_the_protected_block() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(&mut session, "CREATE TABLE pl_trap (n int4 PRIMARY KEY)").await;
    execute(
        &mut session,
        r"
        DO $$
        BEGIN
          INSERT INTO pl_trap VALUES (1);
          BEGIN
            INSERT INTO pl_trap VALUES (2);
            INSERT INTO pl_trap VALUES (1);
          EXCEPTION WHEN unique_violation THEN
            INSERT INTO pl_trap VALUES (3);
          END;
        END
        $$
        ",
    )
    .await;

    assert!(
        query(&mut session, "SELECT n FROM pl_trap ORDER BY n").await
            == vec![row(&["1"]), row(&["3"])]
    );
}

#[tokio::test]
async fn exception_condition_categories_match_member_sqlstates() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_category_result (caught bool)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        BEGIN
          PERFORM 1 / 0;
        EXCEPTION WHEN data_exception THEN
          INSERT INTO pl_category_result VALUES (true);
        END
        $$
        ",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT caught FROM pl_category_result").await == Some("t".into())
    );
}

#[tokio::test]
async fn current_diagnostics_reports_the_previous_statement_row_count() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_diag_input (n int4); \
         INSERT INTO pl_diag_input VALUES (1), (2), (3); \
         CREATE TABLE pl_diag_result (affected int8)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE affected int8;
        BEGIN
          UPDATE pl_diag_input SET n = n + 10 WHERE n >= 2;
          GET CURRENT DIAGNOSTICS affected = ROW_COUNT;
          INSERT INTO pl_diag_result VALUES (affected);
        END
        $$
        ",
    )
    .await;

    assert!(scalar(&mut session, "SELECT affected FROM pl_diag_result").await == Some("2".into()));
}

#[tokio::test]
async fn stacked_diagnostics_exposes_the_caught_error() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_stacked_result (state text, message text)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE state text; message text;
        BEGIN
          RAISE EXCEPTION 'bad math' USING ERRCODE = '22012';
        EXCEPTION WHEN division_by_zero THEN
          GET STACKED DIAGNOSTICS
            state = RETURNED_SQLSTATE,
            message = MESSAGE_TEXT;
          INSERT INTO pl_stacked_result VALUES (state, message);
        END
        $$
        ",
    )
    .await;

    assert!(
        query(&mut session, "SELECT state, message FROM pl_stacked_result").await
            == vec![row(&["22012", "bad math"])]
    );
}

#[tokio::test]
async fn exception_variables_are_scoped_to_the_handler() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let error = session
        .simple_query(
            "CREATE FUNCTION pl_no_exception_variables() RETURNS void LANGUAGE plpgsql AS $$ \
             BEGIN RAISE NOTICE '%', SQLSTATE; END $$; \
             SELECT pl_no_exception_variables()",
        )
        .await
        .expect_err("SQLSTATE is unavailable outside an exception handler");
    assert!(error.code == "42703", "{error:?}");
    let diagnostics = error
        .diagnostics
        .as_deref()
        .expect("internal query diagnostics");
    assert!(diagnostics.internal_position == Some(1), "{error:?}");
    assert!(
        diagnostics.internal_query.as_deref() == Some("SQLSTATE"),
        "{error:?}"
    );

    execute(
        &mut session,
        "CREATE FUNCTION pl_exception_variables() RETURNS text LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION 'caught'; \
         EXCEPTION WHEN OTHERS THEN RETURN SQLSTATE || ':' || SQLERRM; END $$",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT pl_exception_variables()").await
            == Some("P0001:caught".into())
    );
}

#[tokio::test]
async fn raise_notice_preserves_message_code_detail_and_hint() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let mut notices = session.take_notices().expect("notice receiver");

    execute(
        &mut session,
        "DO $$ BEGIN RAISE NOTICE 'processed %', 3 \
         USING DETAIL = 'three rows', HINT = 'keep going'; END $$",
    )
    .await;

    let notice = notices.try_recv().expect("one notice");
    assert!(notice.code == "00000", "{notice:?}");
    assert!(notice.message == "processed 3", "{notice:?}");
    let fields = notice.diagnostics.expect("structured fields");
    assert!(fields.detail.as_deref() == Some("three rows"), "{fields:?}");
    assert!(fields.hint.as_deref() == Some("keep going"), "{fields:?}");
}

#[tokio::test]
async fn assert_accepts_true_and_raises_p0004_with_the_supplied_message() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(&mut session, "DO $$ BEGIN ASSERT 2 + 2 = 4; END $$").await;

    let error = session
        .simple_query("DO $$ BEGIN ASSERT false, 'broken invariant'; END $$")
        .await
        .expect_err("false ASSERT must fail");
    assert!(error.code == "P0004", "{error:?}");
    assert!(error.message == "broken invariant", "{error:?}");
}

#[tokio::test]
async fn scalar_out_and_inout_parameters_are_return_values() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_out_value(input int4, OUT doubled int4)
        LANGUAGE plpgsql AS $$
        BEGIN
          doubled := input * 2;
        END
        $$;
        CREATE FUNCTION pl_inout_value(INOUT value int4)
        LANGUAGE plpgsql AS $$
        BEGIN
          value := value + 1;
        END
        $$
        ",
    )
    .await;

    assert!(
        query(&mut session, "SELECT pl_out_value(9), pl_inout_value(9)").await
            == vec![row(&["18", "10"])]
    );
}

#[tokio::test]
async fn procedures_return_inout_parameters_as_a_call_row() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE PROCEDURE pl_inout_procedure(INOUT value int4)
        LANGUAGE plpgsql AS $$
        BEGIN
          value := value + 5;
        END
        $$
        ",
    )
    .await;

    assert!(query(&mut session, "CALL pl_inout_procedure(7)").await == vec![row(&["12"])]);
}

#[tokio::test]
async fn procedure_out_parameters_use_full_call_arity_and_assign_nested_targets() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE PROCEDURE pl_mixed_procedure(
          IN seed int4, OUT doubled int4, INOUT total int4
        )
        LANGUAGE plpgsql AS $$
        BEGIN
          doubled := seed * 2;
          total := total + seed;
        END
        $$;
        CREATE PROCEDURE pl_out_only(OUT answer int4)
        LANGUAGE plpgsql AS $$
        BEGIN
          answer := 42;
        END
        $$;
        CREATE TABLE pl_call_results (doubled int4, total int4, answer int4);
        ",
    )
    .await;

    assert!(
        query(&mut session, "CALL pl_mixed_procedure(3, NULL, 5)").await == vec![row(&["6", "8"])]
    );
    assert!(query(&mut session, "CALL pl_out_only(NULL)").await == vec![row(&["42"])]);

    execute(
        &mut session,
        r"
        DO $$
        DECLARE doubled int4; total int4 := 10; answer int4;
        BEGIN
          CALL pl_mixed_procedure(4, doubled, total);
          CALL pl_out_only(answer);
          INSERT INTO pl_call_results VALUES (doubled, total, answer);
        END
        $$
        ",
    )
    .await;
    assert!(
        query(
            &mut session,
            "SELECT doubled, total, answer FROM pl_call_results",
        )
        .await
            == vec![row(&["8", "14", "42"])]
    );
}

#[tokio::test]
async fn nested_procedure_output_requires_a_writable_target() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE PROCEDURE pl_requires_target(OUT value int4)
        LANGUAGE plpgsql AS $$ BEGIN value := 1; END $$
        ",
    )
    .await;

    let error = session
        .simple_query("DO $$ BEGIN CALL pl_requires_target(NULL); END $$")
        .await
        .expect_err("PL/pgSQL output arguments must be writable");
    assert!(error.code == "42601", "{error:?}");
    assert!(error.message.contains("not writable"), "{error:?}");
}

#[tokio::test]
async fn setof_functions_append_return_next_and_return_query_rows() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_emit(limit_value int4) RETURNS SETOF int4
        LANGUAGE plpgsql AS $$
        BEGIN
          FOR i IN 1..limit_value LOOP
            RETURN NEXT i;
          END LOOP;
          RETURN QUERY SELECT limit_value * 10;
          RETURN;
        END
        $$
        ",
    )
    .await;

    assert!(
        query(&mut session, "SELECT * FROM pl_emit(3)").await
            == vec![row(&["1"]), row(&["2"]), row(&["3"]), row(&["30"])]
    );
}

#[tokio::test]
async fn table_functions_preserve_next_query_and_dynamic_query_order() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_table_rows(seed int4)
        RETURNS TABLE(value int4, source text)
        LANGUAGE plpgsql AS $$
        BEGIN
          value := seed;
          source := 'next';
          RETURN NEXT;
          RETURN QUERY SELECT seed + 1, 'query'::text;
          RETURN QUERY EXECUTE 'SELECT $1::int4, $2::text'
            USING seed + 2, 'dynamic';
          RETURN;
        END
        $$
        ",
    )
    .await;

    assert!(
        query(
            &mut session,
            "SELECT * FROM pl_table_rows(5) WITH ORDINALITY AS emitted(v, kind, position)",
        )
        .await
            == vec![
                row(&["5", "next", "1"]),
                row(&["6", "query", "2"]),
                row(&["7", "dynamic", "3"]),
            ]
    );
}

#[tokio::test]
async fn out_functions_form_from_rows_and_set_functions_can_return_no_rows() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_out_row(input int4, OUT doubled int4, OUT label text)
        LANGUAGE plpgsql AS $$
        BEGIN
          doubled := input * 2;
          label := 'done';
        END
        $$;
        CREATE FUNCTION pl_no_rows() RETURNS SETOF int4
        LANGUAGE plpgsql AS $$
        BEGIN
          RETURN;
        END
        $$;
        CREATE FUNCTION pl_out_rows(limit_value int4, OUT doubled int4, OUT label text)
        RETURNS SETOF record LANGUAGE plpgsql AS $$
        BEGIN
          FOR value IN 1..limit_value LOOP
            doubled := value * 2;
            label := 'row';
            RETURN NEXT;
          END LOOP;
        END
        $$;
        CREATE FUNCTION pl_out_scalar_rows(input int4, OUT doubled int4)
        RETURNS SETOF int4 LANGUAGE plpgsql AS $$
        BEGIN
          doubled := input * 2;
          RETURN NEXT;
        END
        $$
        ",
    )
    .await;

    assert!(
        query(
            &mut session,
            "SELECT * FROM pl_out_row(4) AS result(value, status)",
        )
        .await
            == vec![row(&["8", "done"])]
    );
    assert!(
        query(&mut session, "SELECT * FROM pl_no_rows()")
            .await
            .is_empty()
    );
    assert!(
        query(&mut session, "SELECT * FROM pl_out_rows(2)").await
            == vec![row(&["2", "row"]), row(&["4", "row"])]
    );
    let (field, rows) = described(&mut session, "SELECT * FROM pl_out_scalar_rows(4)").await;
    assert!(field.name == "doubled", "{field:?}");
    assert!(rows == vec![row(&["8"])]);
}

#[tokio::test]
async fn set_functions_are_implicitly_lateral_to_prior_from_items() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE pl_lateral_input (n int4);
        INSERT INTO pl_lateral_input VALUES (1), (2);
        CREATE FUNCTION pl_lateral_emit(limit_value int4) RETURNS SETOF int4
        LANGUAGE plpgsql AS $$
        BEGIN
          FOR i IN 1..limit_value LOOP
            RETURN NEXT i;
          END LOOP;
          RETURN;
        END
        $$
        ",
    )
    .await;

    assert!(
        query(
            &mut session,
            "SELECT source.n, emitted.value \
             FROM pl_lateral_input AS source, \
                  pl_lateral_emit(source.n) AS emitted(value) \
             ORDER BY source.n, emitted.value",
        )
        .await
            == vec![row(&["1", "1"]), row(&["2", "1"]), row(&["2", "2"])]
    );
}

#[test]
fn set_functions_keep_default_strict_and_recursive_call_semantics() {
    std::thread::Builder::new()
        .name("plpgsql-recursion-test".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(async {
                    let engine = SqlEngine::new();
                    let mut session = engine.connect();
                    execute(
                        &mut session,
                        r"
        CREATE FUNCTION pl_default_rows(seed int4 DEFAULT 4) RETURNS SETOF int4
        LANGUAGE plpgsql AS $$
        BEGIN
          RETURN NEXT seed;
          RETURN;
        END
        $$;
        CREATE FUNCTION pl_strict_rows(seed int4) RETURNS SETOF int4
        LANGUAGE plpgsql STRICT AS $$
        BEGIN
          PERFORM 1 / seed;
          RETURN NEXT seed;
          RETURN;
        END
        $$;
        CREATE FUNCTION pl_recursive_rows(seed int4) RETURNS SETOF int4
        LANGUAGE plpgsql AS $$
        BEGIN
          IF seed <= 0 THEN
            RETURN;
          END IF;
          RETURN NEXT seed;
          RETURN QUERY SELECT * FROM pl_recursive_rows(seed - 1);
          RETURN;
        END
        $$
        ",
                    )
                    .await;

                    assert!(
                        query(&mut session, "SELECT * FROM pl_default_rows()").await
                            == vec![row(&["4"])]
                    );
                    assert!(
                        query(&mut session, "SELECT * FROM pl_strict_rows(NULL)")
                            .await
                            .is_empty()
                    );
                    assert_eq!(
                        query(&mut session, "SELECT * FROM pl_recursive_rows(3)").await,
                        vec![row(&["3"]), row(&["2"]), row(&["1"])]
                    );
                });
        })
        .expect("start recursion test")
        .join()
        .expect("recursion test");
}

#[tokio::test]
async fn strict_from_functions_do_not_execute_for_null_inputs() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE pl_strict_log (kind text);
        CREATE FUNCTION pl_strict_scalar(seed int4) RETURNS int4
        LANGUAGE plpgsql STRICT AS $$
        BEGIN
          INSERT INTO pl_strict_log VALUES ('scalar');
          RETURN seed;
        END
        $$;
        CREATE FUNCTION pl_strict_set(seed int4) RETURNS SETOF int4
        LANGUAGE plpgsql STRICT AS $$
        BEGIN
          INSERT INTO pl_strict_log VALUES ('set');
          RETURN NEXT seed;
          RETURN;
        END
        $$
        ",
    )
    .await;

    assert!(query(&mut session, "SELECT * FROM pl_strict_scalar(NULL)").await == vec![vec![None]]);
    assert!(
        query(&mut session, "SELECT * FROM pl_strict_set(NULL)")
            .await
            .is_empty()
    );
    assert!(scalar(&mut session, "SELECT count(*) FROM pl_strict_log").await == Some("0".into()));
}

#[tokio::test]
async fn set_function_side_effects_share_the_outer_statement_transaction() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE pl_set_log (value int4);
        CREATE FUNCTION pl_atomic_rows() RETURNS SETOF int4
        LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO pl_set_log VALUES (1);
          RETURN NEXT 1;
          RETURN NEXT 2;
          RETURN;
        END
        $$
        ",
    )
    .await;

    let error = session
        .simple_query(
            "SELECT CASE WHEN value = 2 THEN 1 / 0 ELSE value END \
             FROM pl_atomic_rows() AS emitted(value)",
        )
        .await
        .expect_err("later output row must fail the whole statement");
    assert!(error.code == "22012", "{error:?}");
    assert!(scalar(&mut session, "SELECT count(*) FROM pl_set_log").await == Some("0".into()));
}

#[tokio::test]
async fn found_tracks_perform_select_update_and_query_for() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_found_input (n int4); \
         INSERT INTO pl_found_input VALUES (1), (2); \
         CREATE TABLE pl_found_result \
           (after_perform bool, after_select bool, after_update bool, after_loop bool)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE
          selected int4;
          after_perform bool;
          after_select bool;
          after_update bool;
          after_loop bool;
        BEGIN
          PERFORM 1;
          after_perform := FOUND;
          SELECT source.n INTO selected
            FROM pl_found_input AS source WHERE source.n = 99;
          after_select := FOUND;
          UPDATE pl_found_input SET n = n + 10;
          after_update := FOUND;
          FOR selected IN SELECT source.n FROM pl_found_input AS source LOOP
            NULL;
          END LOOP;
          after_loop := FOUND;
          INSERT INTO pl_found_result VALUES
            (after_perform, after_select, after_update, after_loop);
        END
        $$
        ",
    )
    .await;

    assert!(
        query(
            &mut session,
            "SELECT after_perform, after_select, after_update, after_loop \
             FROM pl_found_result",
        )
        .await
            == vec![row(&["t", "f", "t", "t"])]
    );
}

#[tokio::test]
async fn procedure_commit_and_rollback_start_fresh_transactions() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE pl_tx_result (n int4);
        CREATE PROCEDURE pl_tx_control() LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO pl_tx_result VALUES (1);
          COMMIT;
          INSERT INTO pl_tx_result VALUES (2);
          ROLLBACK;
          INSERT INTO pl_tx_result VALUES (3);
        END
        $$
        ",
    )
    .await;
    execute(&mut session, "CALL pl_tx_control()").await;

    assert!(
        query(&mut session, "SELECT n FROM pl_tx_result ORDER BY n").await
            == vec![row(&["1"]), row(&["3"])]
    );
}

#[tokio::test]
async fn foreach_iterates_arrays_and_sets_found() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_foreach_result (total int4, iterated bool)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE value int4; total int4 := 0; iterated bool;
        BEGIN
          FOREACH value IN ARRAY ARRAY[2, 3, 5] LOOP
            total := total + value;
          END LOOP;
          iterated := FOUND;
          INSERT INTO pl_foreach_result VALUES (total, iterated);
        END
        $$
        ",
    )
    .await;

    assert!(
        query(
            &mut session,
            "SELECT total, iterated FROM pl_foreach_result"
        )
        .await
            == vec![row(&["10", "t"])]
    );
}

#[tokio::test]
async fn record_fields_can_be_read_and_assigned() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_record_assignment_input (n int4); \
         INSERT INTO pl_record_assignment_input VALUES (41); \
         CREATE TABLE pl_record_assignment_result (n int4)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE r record;
        BEGIN
          SELECT source.n INTO r FROM pl_record_assignment_input AS source;
          r.n := r.n + 1;
          INSERT INTO pl_record_assignment_result VALUES (r.n);
        END
        $$
        ",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT n FROM pl_record_assignment_result").await
            == Some("42".into())
    );
}

#[tokio::test]
async fn array_elements_can_be_assignment_targets() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_array_target_result \
           (array_value int4, json_value text, record_array int4, record_json text)",
    )
    .await;
    execute(
        &mut session,
        r#"
        DO $$
        DECLARE
          values_ int4[] := ARRAY[1, 2, 3];
          payload jsonb := '{"answer": 0}'::jsonb;
          index_ int4 := 2;
          r record;
        BEGIN
          values_[index_] := 41;
          values_[index_] := values_[index_] + 1;
          payload['answer'] := '42'::jsonb;
          SELECT ARRAY[1, 2] AS items, '{"answer": 1}'::jsonb AS document INTO r;
          r.items[1] := 42;
          r.document['answer'] := '42'::jsonb;
          INSERT INTO pl_array_target_result VALUES
            (values_[2], payload ->> 'answer', r.items[1], r.document ->> 'answer');
        END
        $$
        "#,
    )
    .await;

    assert!(
        query(
            &mut session,
            "SELECT array_value, json_value, record_array, record_json \
             FROM pl_array_target_result",
        )
        .await
            == vec![row(&["42", "42", "42", "42"])]
    );
}

#[tokio::test]
async fn cursor_arguments_and_dynamic_open_using_are_bound() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_open_input (n int4); \
         INSERT INTO pl_open_input VALUES (1), (2), (3); \
         CREATE TABLE pl_open_result (declared int4, dynamic int4)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE
          declared_cursor CURSOR(bound_ int4) FOR
            SELECT n FROM pl_open_input WHERE n >= bound_ ORDER BY n;
          dynamic_cursor refcursor;
          declared_value int4;
          dynamic_value int4;
        BEGIN
          OPEN declared_cursor(2);
          FETCH NEXT FROM declared_cursor INTO declared_value;
          CLOSE declared_cursor;
          OPEN dynamic_cursor FOR EXECUTE
            'SELECT n FROM pl_open_input WHERE n = $1' USING 3;
          FETCH NEXT FROM dynamic_cursor INTO dynamic_value;
          CLOSE dynamic_cursor;
          INSERT INTO pl_open_result VALUES (declared_value, dynamic_value);
        END
        $$
        ",
    )
    .await;

    assert!(
        query(&mut session, "SELECT declared, dynamic FROM pl_open_result").await
            == vec![row(&["2", "3"])]
    );
}

#[tokio::test]
async fn execute_discards_rows_and_dml_returning_rejects_multiple_rows() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_returning_input (n int4); \
         INSERT INTO pl_returning_input VALUES (1), (2); \
         CREATE TABLE pl_returning_result (state text)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE picked int4;
        BEGIN
          EXECUTE 'SELECT n FROM pl_returning_input ORDER BY n';
          BEGIN
            EXECUTE 'UPDATE pl_returning_input SET n = n + 10 RETURNING n' INTO picked;
          EXCEPTION WHEN too_many_rows THEN
            INSERT INTO pl_returning_result VALUES (SQLSTATE);
          END;
        END
        $$
        ",
    )
    .await;

    assert!(
        scalar(&mut session, "SELECT state FROM pl_returning_result").await == Some("P0003".into())
    );
    assert!(
        query(&mut session, "SELECT n FROM pl_returning_input ORDER BY n").await
            == vec![row(&["1"]), row(&["2"])]
    );
}

#[tokio::test]
async fn call_keeps_unknown_literals_and_transaction_eligibility() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE pl_call_result (value text);
        CREATE TABLE pl_call_atomic (value int4 PRIMARY KEY);
        CREATE PROCEDURE pl_overloaded(value int4) LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO pl_call_result VALUES ('integer'); END
        $$;
        CREATE PROCEDURE pl_overloaded(value text) LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO pl_call_result VALUES ('text'); END
        $$;
        CREATE PROCEDURE pl_inner_commit() LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO pl_call_result VALUES ('before commit'); COMMIT; END
        $$;
        CREATE PROCEDURE pl_outer_commit() LANGUAGE plpgsql AS $$
        BEGIN CALL pl_inner_commit(); INSERT INTO pl_call_result VALUES ('after commit'); END
        $$;
        CREATE FUNCTION pl_sql_call_argument(value int4) RETURNS int4 LANGUAGE plpgsql AS $$
        DECLARE computed int4;
        BEGIN SELECT value + 1 INTO computed; RETURN computed; END
        $$;
        CREATE PROCEDURE pl_accept_call_argument(value int4) LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO pl_call_result VALUES (value::text); END
        $$;
        CREATE FUNCTION pl_atomic_call_argument(value int4) RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO pl_call_atomic VALUES (1); RETURN value; END
        $$;
        CREATE PROCEDURE pl_fail_after_argument(value int4) LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO pl_call_atomic VALUES (2);
          INSERT INTO pl_call_atomic VALUES (2);
        END
        $$
        ",
    )
    .await;
    execute(
        &mut session,
        "CALL pl_overloaded('unknown literal'); \
         CALL pl_outer_commit(); \
         CALL pl_accept_call_argument(pl_sql_call_argument(41))",
    )
    .await;

    assert!(
        query(
            &mut session,
            "SELECT value FROM pl_call_result ORDER BY value"
        )
        .await
            == vec![
                row(&["42"]),
                row(&["after commit"]),
                row(&["before commit"]),
                row(&["text"])
            ]
    );
    let error = session
        .simple_query("CALL pl_fail_after_argument(pl_atomic_call_argument(42))")
        .await
        .expect_err("procedure failure must roll back argument side effects");
    assert!(error.code == "23505", "{error:?}");
    assert!(scalar(&mut session, "SELECT count(*) FROM pl_call_atomic").await == Some("0".into()));
}

#[tokio::test]
async fn restricted_procedures_reject_transaction_control() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE PROCEDURE pl_security_commit() LANGUAGE plpgsql SECURITY DEFINER AS $$
        BEGIN COMMIT; END
        $$;
        CREATE PROCEDURE pl_config_commit() LANGUAGE plpgsql SET work_mem = '64MB' AS $$
        BEGIN COMMIT; END
        $$
        ",
    )
    .await;

    for name in ["pl_security_commit", "pl_config_commit"] {
        let error = session
            .simple_query(&format!("CALL {name}()"))
            .await
            .expect_err("restricted procedure must reject COMMIT");
        assert!(error.code == "25001", "{name}: {error:?}");
    }
}

#[tokio::test]
async fn diagnostics_include_plpgsql_context() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE TABLE pl_context_result (current_context text, exception_context text)",
    )
    .await;
    execute(
        &mut session,
        r"
        DO $$
        DECLARE current_context text; exception_context text;
        BEGIN
          GET CURRENT DIAGNOSTICS current_context = PG_CONTEXT;
          BEGIN
            RAISE EXCEPTION 'context source';
          EXCEPTION WHEN raise_exception THEN
            GET STACKED DIAGNOSTICS exception_context = PG_EXCEPTION_CONTEXT;
          END;
          INSERT INTO pl_context_result VALUES (current_context, exception_context);
        END
        $$
        ",
    )
    .await;

    assert!(
        query(
            &mut session,
            "SELECT current_context, exception_context FROM pl_context_result",
        )
        .await
            == vec![row(&[
                "PL/pgSQL function inline_code_block line 4 at GET DIAGNOSTICS",
                "PL/pgSQL function inline_code_block line 6 at RAISE",
            ])]
    );
}

#[tokio::test]
async fn current_diagnostics_includes_nested_call_frames() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(&mut session, "CREATE TABLE pl_call_context (value text)").await;
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_call_context_inner() RETURNS int4 LANGUAGE plpgsql AS $$
        DECLARE context text;
        BEGIN
          GET CURRENT DIAGNOSTICS context = PG_CONTEXT;
          INSERT INTO pl_call_context VALUES (context);
          RETURN 1;
        END
        $$;
        CREATE FUNCTION pl_call_context_outer() RETURNS void LANGUAGE plpgsql AS $$
        DECLARE value int4;
        BEGIN
          value := pl_call_context_inner();
        END
        $$;
        ",
    )
    .await;

    execute(&mut session, "SELECT pl_call_context_outer()").await;
    assert!(
        scalar(&mut session, "SELECT value FROM pl_call_context").await
            == Some(
                "PL/pgSQL function pl_call_context_inner() line 4 at GET DIAGNOSTICS\nPL/pgSQL function pl_call_context_outer() line 4 at assignment"
                    .into()
            )
    );
}

#[tokio::test]
async fn scalar_raise_exception_formats_percent_and_using_diagnostics() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_scalar_raise(value int4) RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'value %% %', value
            USING ERRCODE = '22012', DETAIL = 'scalar detail', HINT = 'scalar hint';
        END
        $$
        ",
    )
    .await;

    let error = session
        .simple_query("SELECT pl_scalar_raise(7)")
        .await
        .expect_err("scalar RAISE EXCEPTION must fail");
    assert!(error.code == "22012", "{error:?}");
    assert!(error.message == "value % 7", "{error:?}");
    let diagnostics = error.diagnostics.expect("RAISE diagnostics");
    assert!(diagnostics.detail.as_deref() == Some("scalar detail"));
    assert!(diagnostics.hint.as_deref() == Some("scalar hint"));
}

#[tokio::test]
async fn scalar_raise_expressions_can_call_sql_bearing_plpgsql_functions_atomically() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE pl_raise_audit (value int4);
        CREATE FUNCTION pl_raise_argument() RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          INSERT INTO pl_raise_audit VALUES (7);
          RETURN 7;
        END
        $$;
        CREATE FUNCTION pl_raise_outer() RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'value %', pl_raise_argument();
        END
        $$
        ",
    )
    .await;

    let error = session
        .simple_query("SELECT pl_raise_outer()")
        .await
        .expect_err("RAISE must propagate its SQL-bearing argument's value");
    assert!(error.code == "P0001", "{error:?}");
    assert!(error.message == "value 7", "{error:?}");
    assert!(scalar(&mut session, "SELECT count(*) FROM pl_raise_audit").await == Some("0".into()));
}

#[tokio::test]
async fn sql_json_expressions_bind_plpgsql_variables() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_json_scalar(value int4) RETURNS jsonb LANGUAGE plpgsql AS $$
        BEGIN
          RETURN JSON_SCALAR(value);
        END
        $$
        ",
    )
    .await;

    assert!(scalar(&mut session, "SELECT pl_json_scalar(42)").await == Some("42".into()));
}

#[tokio::test]
async fn scalar_expression_subqueries_use_the_async_session_interpreter() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TABLE pl_subquery_source (value int4);
        INSERT INTO pl_subquery_source VALUES (42);
        CREATE FUNCTION pl_return_subquery() RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          RETURN (SELECT value FROM pl_subquery_source);
        END
        $$;
        CREATE FUNCTION pl_default_subquery() RETURNS int4 LANGUAGE plpgsql AS $$
        DECLARE picked int4 := (SELECT value FROM pl_subquery_source);
        BEGIN
          RETURN picked;
        END
        $$
        ",
    )
    .await;

    assert!(scalar(&mut session, "SELECT pl_return_subquery()").await == Some("42".into()));
    assert!(scalar(&mut session, "SELECT pl_default_subquery()").await == Some("42".into()));
}

#[tokio::test]
async fn current_diagnostics_reports_the_routine_oid() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_current_oid() RETURNS int4 LANGUAGE plpgsql AS $$
        DECLARE result int4;
        BEGIN
          GET DIAGNOSTICS result = PG_ROUTINE_OID;
          RETURN result;
        END
        $$
        ",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pl_current_oid() = oid FROM pg_proc WHERE proname = 'pl_current_oid'",
        )
        .await
            == Some("t".into())
    );
}

/// A NULL reaching a `RAISE` is a rendering question, not an abort.
///
/// `PostgreSQL` substitutes `<NULL>` for a NULL format parameter, refuses a NULL
/// `USING` option outright, and falls back to the default text for a NULL
/// `ASSERT` message. NULL travels out of band on the wire, so the text output
/// functions never see one — handing a NULL to one of them aborted the whole
/// server rather than the statement.
#[tokio::test]
async fn a_null_reaching_a_raise_is_rendered_rather_than_fatal() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let mut notices = session.take_notices().expect("notice receiver");

    execute(&mut session, "DO $$ BEGIN RAISE NOTICE 'v=%', NULL; END $$").await;
    assert!(notices.try_recv().expect("one notice").message == "v=<NULL>");

    let error = session
        .simple_query("DO $$ BEGIN RAISE EXCEPTION 'v=%', NULL; END $$")
        .await
        .expect_err("RAISE EXCEPTION must fail");
    assert!(error.message == "v=<NULL>", "{error:?}");

    // A NULL option is an error rather than the word `<NULL>` in the DETAIL.
    for option in ["DETAIL", "HINT"] {
        let sql = format!("DO $$ BEGIN RAISE EXCEPTION 'boom' USING {option} = NULL; END $$");
        let error = session
            .simple_query(&sql)
            .await
            .expect_err("a NULL RAISE option must fail");
        assert!(error.code == "22004", "{option}: {error:?}");
        assert!(
            error.message == "RAISE statement option cannot be null",
            "{option}: {error:?}"
        );
    }

    // A NULL ASSERT message is not rendered at all.
    let error = session
        .simple_query("DO $$ BEGIN ASSERT false, NULL; END $$")
        .await
        .expect_err("false ASSERT must fail");
    assert!(error.code == "P0004", "{error:?}");
    assert!(error.message == "assertion failed", "{error:?}");
    assert!(
        error
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.context.as_deref())
            == Some("PL/pgSQL function inline_code_block line 1 at ASSERT"),
        "{error:?}"
    );

    // The session is still usable, which a panicking backend would not be.
    assert!(scalar(&mut session, "SELECT 1").await == Some("1".into()));
}

/// The range-table entry a DML statement adds is aliased to the relation's bare
/// name, whatever schema the statement reached it through.
#[tokio::test]
async fn returning_binds_a_schema_qualified_target_under_its_bare_name() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(&mut session, "CREATE SCHEMA rq").await;
    execute(&mut session, "CREATE TABLE rq.t (a int, b text)").await;

    let cases = [
        ("INSERT INTO rq.t VALUES (1, 'x') RETURNING t.a", "1"),
        ("UPDATE rq.t SET b = 'y' RETURNING t.b", "y"),
        ("DELETE FROM rq.t RETURNING t.a", "1"),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&mut session, sql).await == Some(expected.into()),
            "{sql}"
        );
    }
}

/// A `RETURNS void` call in a select list answers one column named for the
/// function, one row, and a blank value.
///
/// Crabka has no `void` column type, so a void routine answers the empty
/// `text` its built-in void functions already answer -- blank like
/// `PostgreSQL`'s void, and *not* NULL, so a `\pset null` marker does not
/// appear where `PostgreSQL` leaves a blank. The three bodies cover the shapes
/// that reach different runtimes: falling off the end (`temp`'s helper), a bare
/// `RETURN`, and a SQL-bearing body, which the session interpreter runs rather
/// than the row evaluator.
#[tokio::test]
async fn a_void_function_answers_one_blank_column_named_for_itself() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(&mut session, "CREATE TABLE void_log (n int4)").await;

    let bodies = [
        ("void_falls_off", "RAISE NOTICE 'no RETURN here';"),
        ("void_bare_return", "RETURN;"),
        ("void_runs_sql", "INSERT INTO void_log VALUES (1);"),
    ];
    for (name, body) in bodies {
        execute(
            &mut session,
            &format!(
                "CREATE FUNCTION {name}() RETURNS void LANGUAGE plpgsql AS $$ BEGIN {body} END $$"
            ),
        )
        .await;

        let (field, rows) = described(&mut session, &format!("SELECT {name}()")).await;

        assert!(field == text_field(name), "{name}");
        assert!(rows == vec![vec![Some(String::new())]], "{name}");
    }

    // Blank, but not NULL: PostgreSQL's void is a value.
    assert!(scalar(&mut session, "SELECT void_bare_return() IS NULL").await == Some("f".into()));
    assert!(
        scalar(&mut session, "SELECT length(void_bare_return()::text)").await == Some("0".into())
    );
}

/// A void call is evaluated once per input row, and its side effects survive.
#[tokio::test]
async fn a_void_function_runs_once_for_each_input_row() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(&mut session, "CREATE TABLE void_src (n int4)").await;
    execute(&mut session, "INSERT INTO void_src VALUES (1), (2), (3)").await;
    execute(&mut session, "CREATE TABLE void_seen (n int4)").await;
    execute(
        &mut session,
        "CREATE FUNCTION void_record(a int4) RETURNS void LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO void_seen VALUES (a); END $$",
    )
    .await;

    let (field, rows) = described(&mut session, "SELECT void_record(n) FROM void_src").await;

    assert!(field == text_field("void_record"));
    assert!(rows == vec![vec![Some(String::new())]; 3]);
    assert!(scalar(&mut session, "SELECT count(*) FROM void_seen").await == Some("3".into()));
}

/// A void call inside a transaction leaves the transaction usable. Refusing
/// the call aborted it instead, and every later statement in the block then
/// answered `current transaction is aborted` rather than its own result.
#[tokio::test]
async fn a_void_call_does_not_abort_the_enclosing_transaction() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(&mut session, "CREATE TABLE void_txn (n int4)").await;
    execute(&mut session, "INSERT INTO void_txn VALUES (7)").await;
    execute(
        &mut session,
        "CREATE FUNCTION void_touch() RETURNS void LANGUAGE plpgsql AS $$ BEGIN END $$",
    )
    .await;

    execute(&mut session, "BEGIN").await;
    assert!(scalar(&mut session, "SELECT void_touch()").await == Some(String::new()));
    assert!(scalar(&mut session, "SELECT count(*) FROM void_txn").await == Some("1".into()));
    execute(&mut session, "COMMIT").await;
}

/// Only `void` may fall off the end. A function that owes a value still has to
/// return one, and PL/pgSQL's own 2F005 is what says so.
#[tokio::test]
async fn a_function_that_owes_a_value_still_needs_a_return() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        "CREATE FUNCTION owes_a_value() RETURNS int4 LANGUAGE plpgsql AS $$ BEGIN END $$",
    )
    .await;

    let error = session
        .simple_query("SELECT owes_a_value()")
        .await
        .expect_err("a non-void function must return a value");

    assert!(error.code == "2F005", "{error:?}");
    assert!(
        error.message == "control reached end of function without RETURN",
        "{error:?}"
    );
    assert!(
        error
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.context.as_deref())
            == Some("PL/pgSQL function owes_a_value()"),
        "{error:?}"
    );
}

#[tokio::test]
async fn assignment_and_return_errors_stack_plpgsql_statement_contexts() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_context_inner() RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'boom';
        END
        $$;
        CREATE FUNCTION pl_context_outer() RETURNS void LANGUAGE plpgsql AS $$
        DECLARE value int4;
        BEGIN
          value := pl_context_inner();
        END
        $$;
        CREATE FUNCTION pl_return_context() RETURNS int4 LANGUAGE plpgsql AS $$
        BEGIN
          RETURN 1 / 0;
        END
        $$;
        ",
    )
    .await;

    let assignment = session
        .simple_query("SELECT pl_context_outer()")
        .await
        .expect_err("the assignment must retain both PL/pgSQL frames");
    assert!(
        assignment
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.context.as_deref())
            == Some(
                "PL/pgSQL function pl_context_inner() line 3 at RAISE\nPL/pgSQL function pl_context_outer() line 4 at assignment"
            ),
        "{assignment:?}"
    );

    let returned = session
        .simple_query("SELECT pl_return_context()")
        .await
        .expect_err("the return expression must carry its PL/pgSQL frame");
    assert!(
        returned
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.context.as_deref())
            == Some(
                "PL/pgSQL expression \"1 / 0\"\nPL/pgSQL function pl_return_context() line 3 at RETURN"
            ),
        "{returned:?}"
    );
}

#[tokio::test]
async fn reraise_preserves_the_original_statement_context() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE FUNCTION pl_reraise() RETURNS void LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'boom';
        EXCEPTION WHEN OTHERS THEN
          RAISE;
        END
        $$
        ",
    )
    .await;

    let error = session
        .simple_query("SELECT pl_reraise()")
        .await
        .expect_err("the original exception must be rethrown");
    assert!(error.code == "P0001", "{error:?}");
    assert!(
        error
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.context.as_deref())
            == Some("PL/pgSQL function pl_reraise() line 3 at RAISE"),
        "{error:?}"
    );
}

#[tokio::test]
async fn composite_functions_reject_scalar_return_values() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TYPE pl_return_pair AS (id int4, label text);
        CREATE FUNCTION pl_return_scalar() RETURNS pl_return_pair LANGUAGE plpgsql AS $$
        BEGIN
          RETURN 7;
        END
        $$
        ",
    )
    .await;

    for sql in [
        "SELECT pl_return_scalar()",
        "SELECT * FROM pl_return_scalar()",
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("a composite function must reject a scalar return value");
        assert!(error.code == "42804", "{error:?}");
        assert!(
            error.message
                == "cannot return non-composite value from function returning composite type",
            "{error:?}"
        );
    }
}

#[tokio::test]
async fn set_functions_expand_composite_return_next_values() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    execute(
        &mut session,
        r"
        CREATE TYPE pl_return_pair AS (id int4, label text);
        CREATE FUNCTION pl_return_pairs() RETURNS SETOF pl_return_pair LANGUAGE plpgsql AS $$
        BEGIN
          RETURN NEXT (1, 'one'::text);
          RETURN NEXT NULL::pl_return_pair;
        END
        $$
        ",
    )
    .await;

    assert!(
        query(&mut session, "SELECT * FROM pl_return_pairs()").await
            == vec![row(&["1", "one"]), vec![None, None]]
    );
}

#[tokio::test]
async fn get_stacked_diagnostics_errors_name_the_statement_line() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let error = session
        .simple_query("DO $$\nBEGIN\n  GET STACKED DIAGNOSTICS ignored = MESSAGE_TEXT;\nEND\n$$")
        .await
        .expect_err("GET STACKED DIAGNOSTICS needs an exception handler");

    assert!(error.code == "0Z002", "{error:?}");
    assert!(
        error
            .diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.context.as_deref())
            == Some("PL/pgSQL function inline_code_block line 3 at GET STACKED DIAGNOSTICS"),
        "{error:?}"
    );
}
