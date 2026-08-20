//! P6: user-defined aggregates, exercised through the SQL session a client
//! reaches them by.
//!
//! Every expectation here was taken from `PostgreSQL` 18.4 — the pinned oracle
//! for this engine — either directly or from `src/test/regress/expected`.

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// Run `sql` as one simple query and return its final result.
async fn run(engine: &SqlEngine, sql: &str) -> QueryResult {
    engine
        .connect()
        .simple_query(sql)
        .await
        .expect("query succeeds")
        .into_iter()
        .next_back()
        .expect("at least one result")
}

/// The whole result as text.
async fn grid(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<String>>> {
    match run(engine, sql).await {
        QueryResult::Rows { rows, .. } => rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|cell| cell.as_ref().map(text_of))
                    .collect()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn text_of(cell: &Cell) -> String {
    String::from_utf8(cell.text.to_vec()).expect("valid text cell")
}

/// The error message a failing statement produces.
async fn error(engine: &SqlEngine, sql: &str) -> String {
    let outcome = engine.connect().simple_query(sql).await;
    match outcome {
        Ok(results) => panic!("expected a failure, got {results:?}"),
        Err(error) => error.to_string(),
    }
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

/// A session with the transition/final functions the regression corpus builds
/// its polymorphic aggregates from, plus a small table to aggregate.
async fn fixture() -> SqlEngine {
    let engine = SqlEngine::new();
    for sql in [
        "CREATE TABLE t (f1 int4, f3 text)",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        "CREATE FUNCTION addint(int4, int4) RETURNS int4 LANGUAGE sql AS 'select $1 + $2'",
        "CREATE FUNCTION stfnp(int4[]) RETURNS int4[] LANGUAGE sql AS 'select $1'",
        "CREATE FUNCTION tfnp(int4[], int4) RETURNS int4[] LANGUAGE sql AS 'select $1 || $2'",
        "CREATE FUNCTION tfp(anyarray, anyelement) RETURNS anyarray LANGUAGE sql AS \
         'select $1 || $2'",
        "CREATE FUNCTION tf1p(anyarray, int4) RETURNS anyarray LANGUAGE sql AS 'select $1'",
        "CREATE FUNCTION ffp(anyarray) RETURNS anyarray LANGUAGE sql AS 'select $1'",
    ] {
        run(&engine, sql).await;
    }
    engine
}

#[tokio::test]
async fn a_monomorphic_aggregate_folds_every_row_and_groups() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE mysum (int4) (SFUNC = addint, STYPE = int4, INITCOND = '0')",
    )
    .await;

    assert!(grid(&engine, "SELECT mysum(f1) FROM t").await == vec![some(&["6"])]);
    assert!(grid(&engine, "SELECT mysum(f1) FROM t WHERE f1 > 1").await == vec![some(&["5"])]);
    assert!(
        grid(
            &engine,
            "SELECT f1, mysum(f1) FROM t GROUP BY f1 ORDER BY f1"
        )
        .await
            == vec![some(&["1", "1"]), some(&["2", "2"]), some(&["3", "3"])]
    );
    // FILTER and DISTINCT are the shared accumulator's, so they apply here too.
    assert!(
        grid(&engine, "SELECT mysum(f1) FILTER (WHERE f1 <> 2) FROM t").await == vec![some(&["4"])]
    );
}

#[tokio::test]
async fn a_builtin_transition_function_defines_a_user_aggregate() {
    let engine = fixture().await;
    run(&engine, "INSERT INTO t VALUES (NULL, 'd')").await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_sum (int4) (SFUNC = int4pl, STYPE = int4, INITCOND = '0')",
    )
    .await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_count (*) (SFUNC = int8inc, STYPE = int8, INITCOND = '0')",
    )
    .await;

    assert!(
        grid(&engine, "SELECT builtin_sum(f1), builtin_count(*) FROM t").await
            == vec![some(&["6", "4"])]
    );
}

#[tokio::test]
async fn comment_on_aggregate_resolves_its_signature() {
    let engine = fixture().await;
    assert!(
        error(
            &engine,
            "COMMENT ON AGGREGATE missing_aggregate_comment (int4) IS 'missing'",
        )
        .await
            == "ERROR: aggregate missing_aggregate_comment(integer) does not exist (42883)"
    );

    run(
        &engine,
        "CREATE AGGREGATE aggregate_with_comment (int4) (SFUNC = int4pl, STYPE = int4, \
         INITCOND = '0')",
    )
    .await;
    run(
        &engine,
        "COMMENT ON AGGREGATE aggregate_with_comment (int4) IS 'an aggregate comment'",
    )
    .await;
    run(
        &engine,
        "COMMENT ON AGGREGATE aggregate_with_comment (int4) IS NULL",
    )
    .await;
}

#[tokio::test]
async fn int4_sum_transition_defines_a_user_aggregate() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_int4_sum (int4) (SFUNC = int4_sum, STYPE = int8)",
    )
    .await;

    assert!(grid(&engine, "SELECT builtin_int4_sum(f1) FROM t").await == vec![some(&["6"])]);
}

#[tokio::test]
async fn int4larger_transition_defines_a_user_aggregate() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_int4_max (int4) (SFUNC = int4larger, STYPE = int4)",
    )
    .await;

    assert!(grid(&engine, "SELECT builtin_int4_max(f1) FROM t").await == vec![some(&["3"])]);
}

#[tokio::test]
async fn boolean_transitions_define_user_aggregates() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_bool_and (bool) (SFUNC = booland_statefunc, STYPE = bool)",
    )
    .await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_bool_or (bool) (SFUNC = boolor_statefunc, STYPE = bool)",
    )
    .await;

    assert!(
        grid(
            &engine,
            "SELECT builtin_bool_and(f1 > 1), builtin_bool_or(f1 > 1) FROM t",
        )
        .await
            == vec![some(&["f", "t"])]
    );
}

#[tokio::test]
async fn float8mi_transition_defines_a_user_aggregate() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_float8_difference (float8) (SFUNC = float8mi, \
         STYPE = float8, INITCOND = '0')",
    )
    .await;

    assert!(
        grid(&engine, "SELECT builtin_float8_difference(f1::float8) FROM t").await
            == vec![some(&["-6"])]
    );
}

#[tokio::test]
async fn array_larger_transition_defines_a_user_aggregate() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_array_max (int4[]) (SFUNC = array_larger, STYPE = int4[])",
    )
    .await;

    assert!(
        grid(&engine, "SELECT builtin_array_max(ARRAY[f1]) FROM t").await
            == vec![some(&["{3}"])]
    );
}

#[tokio::test]
async fn array_append_transition_resolves_an_array_element_signature() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_array_collect (int4) (SFUNC = array_append, \
         STYPE = int4[], INITCOND = '{}')",
    )
    .await;
    run(&engine, "INSERT INTO t VALUES (NULL, 'd')").await;

    assert!(
        grid(&engine, "SELECT builtin_array_collect(f1) FROM t").await
            == vec![some(&["{1,2,3,NULL}"])]
    );
}

#[tokio::test]
async fn array_cat_transition_resolves_compatible_array_signatures() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_array_concat (int4[]) (SFUNC = array_cat, STYPE = int4[], \
         INITCOND = '{}')",
    )
    .await;

    assert!(
        grid(&engine, "SELECT builtin_array_concat(ARRAY[f1]) FROM t").await
            == vec![some(&["{1,2,3}"])]
    );
}

#[tokio::test]
async fn float8_accumulator_and_finalizer_define_a_user_average() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_float8_avg (float8) (SFUNC = float8_accum, \
         STYPE = float8[], FINALFUNC = float8_avg, INITCOND = '{0,0,0}')",
    )
    .await;

    assert!(
        grid(&engine, "SELECT builtin_float8_avg(f1::float8) FROM t").await
            == vec![some(&["2"])]
    );
}

#[tokio::test]
async fn int4_accumulator_and_finalizer_define_a_user_average() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE builtin_int4_avg (int4) (SFUNC = int4_avg_accum, \
         STYPE = int8[], FINALFUNC = int8_avg, INITCOND = '{0,0}')",
    )
    .await;

    assert!(
        grid(&engine, "SELECT builtin_int4_avg(f1) FROM t").await
            == vec![some(&["2.0000000000000000"])]
    );
}

#[tokio::test]
async fn a_zero_argument_aggregate_is_called_with_a_star() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE myaggp01a(*) (SFUNC = stfnp, STYPE = int4[], FINALFUNC = ffp, \
         INITCOND = '{}')",
    )
    .await;
    assert!(grid(&engine, "SELECT myaggp01a(*) FROM t").await == vec![some(&["{}"])]);
}

#[tokio::test]
async fn a_polymorphic_state_resolves_from_the_calls_own_argument() {
    let engine = fixture().await;
    // polymorphism.sql's myaggp20a: BASETYPE and STYPE are both polymorphic, so
    // the state type is whatever the call's argument pins it to.
    run(
        &engine,
        "CREATE AGGREGATE myaggp20a(BASETYPE = anyelement, SFUNC = tfp, STYPE = anyarray, \
         FINALFUNC = ffp, INITCOND = '{}')",
    )
    .await;
    assert!(grid(&engine, "SELECT myaggp20a(f1) FROM t").await == vec![some(&["{1,2,3}"])]);
    assert!(grid(&engine, "SELECT myaggp20a(f3) FROM t").await == vec![some(&["{a,b,c}"])]);
}

#[tokio::test]
async fn the_old_style_spellings_define_the_same_aggregate() {
    let engine = fixture().await;
    for sql in [
        "CREATE AGGREGATE old1 (SFUNC = addint, BASETYPE = int4, STYPE = int4, INITCOND = '0')",
        "CREATE AGGREGATE old2 (SFUNC1 = addint, BASETYPE = int4, STYPE1 = int4, INITCOND1 = '0')",
        "CREATE AGGREGATE new1 (int4) (SFUNC = addint, STYPE = int4, INITCOND = '0')",
    ] {
        run(&engine, sql).await;
    }
    assert!(
        grid(&engine, "SELECT old1(f1), old2(f1), new1(f1) FROM t").await
            == vec![some(&["6", "6", "6"])]
    );
}

#[tokio::test]
async fn a_definition_is_refused_the_way_postgresql_refuses_it() {
    let engine = fixture().await;
    let cases = [
        (
            "CREATE AGGREGATE bad (int4) (STYPE = int4)",
            "aggregate sfunc must be specified",
        ),
        (
            "CREATE AGGREGATE bad (int4) (SFUNC = addint)",
            "aggregate stype must be specified",
        ),
        // polymorphism.sql: a polymorphic state with no polymorphic argument
        // can never be pinned down.
        (
            "CREATE AGGREGATE bad (int4) (SFUNC = tfp, STYPE = anyarray)",
            "cannot determine transition data type",
        ),
        // A concrete parameter does not accept a pseudo-type argument, which is
        // what makes this one fail against tfnp(int4[], int4).
        (
            "CREATE AGGREGATE bad (BASETYPE = anyelement, SFUNC = tfnp, STYPE = int4[], \
             INITCOND = '{}')",
            "function tfnp(integer[], anyelement) does not exist",
        ),
        (
            "CREATE AGGREGATE bad (BASETYPE = anyelement, SFUNC = tf1p, STYPE = int4[], \
             INITCOND = '{}')",
            "function tf1p(integer[], anyelement) does not exist",
        ),
        (
            "CREATE AGGREGATE bad (int4) (SFUNC = nosuchfn, STYPE = int4)",
            "function nosuchfn(integer, integer) does not exist",
        ),
        (
            "CREATE AGGREGATE bad (int4) (SFUNC = array_larger, STYPE = int4)",
            "function array_larger(integer, integer) does not exist",
        ),
    ];
    for (sql, expected) in cases {
        let message = error(&engine, sql).await;
        assert!(message.contains(expected), "{sql}\ngave: {message}");
    }
}

#[tokio::test]
async fn ordered_set_aggregates_sort_transition_rows_and_bind_direct_final_args() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE FUNCTION n16_ordered_trans(text, int4) RETURNS text LANGUAGE sql AS \
         'select $1 || $2::text'",
    )
    .await;
    run(
        &engine,
        "CREATE FUNCTION n16_ordered_final(text, float8, int4) RETURNS text LANGUAGE sql AS \
         'select $2::text'",
    )
    .await;
    run(
        &engine,
        "CREATE AGGREGATE n16_ordered(float8 ORDER BY int4) (SFUNC = n16_ordered_trans, \
         STYPE = text, INITCOND = '', FINALFUNC = n16_ordered_final, FINALFUNC_EXTRA = true)",
    )
    .await;
    run(
        &engine,
        "CREATE AGGREGATE n16_ordered_state(float8 ORDER BY int4) (SFUNC = n16_ordered_trans, \
         STYPE = text, INITCOND = '')",
    )
    .await;

    assert!(
        grid(
            &engine,
            "SELECT n16_ordered_state(0.5) WITHIN GROUP (ORDER BY f1 DESC), \
             n16_ordered(0.5) WITHIN GROUP (ORDER BY f1) FROM t",
        )
        .await
            == vec![some(&["321", "0.5"])]
    );
    assert!(
        error(
            &engine,
            "SELECT n16_ordered(DISTINCT 0.5) WITHIN GROUP (ORDER BY f1) FROM t",
        )
        .await
        .contains("DISTINCT is not implemented for ordered-set aggregates")
    );

    run(
        &engine,
        "CREATE AGGREGATE n16_plain(*) (SFUNC = int8inc, STYPE = int8, INITCOND = '0')",
    )
    .await;
    let plain_error = error(
        &engine,
        "SELECT n16_plain(*) WITHIN GROUP (ORDER BY f1) FROM t",
    )
    .await;
    assert!(
        plain_error.contains("function n16_plain(...) does not exist"),
        "{plain_error}"
    );
}

#[tokio::test]
async fn an_aggregate_lives_in_the_catalog_as_a_routine_of_its_own_kind() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE mysum (int4) (SFUNC = addint, STYPE = int4, INITCOND = '7')",
    )
    .await;

    // psql's \da asks pg_proc alone, and needs prokind = 'a'.
    assert!(
        grid(
            &engine,
            "SELECT proname, prokind, pg_get_function_arguments(oid), \
             format_type(prorettype, NULL) FROM pg_proc WHERE proname = 'mysum'"
        )
        .await
            == vec![some(&["mysum", "a", "integer", "integer"])]
    );
    // pg_aggregate carries the definition.
    assert!(
        grid(
            &engine,
            "SELECT agginitval FROM pg_aggregate a JOIN pg_proc p ON p.oid = a.aggfnoid \
             WHERE p.proname = 'mysum'"
        )
        .await
            == vec![some(&["7"])]
    );
}

#[tokio::test]
async fn rename_and_drop_go_through_the_routine_catalog() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE mysum (int4) (SFUNC = addint, STYPE = int4, INITCOND = '0')",
    )
    .await;
    run(&engine, "ALTER AGGREGATE mysum(int4) RENAME TO mytotal").await;
    assert!(grid(&engine, "SELECT mytotal(f1) FROM t").await == vec![some(&["6"])]);

    run(&engine, "DROP AGGREGATE mytotal(int4)").await;
    assert!(
        grid(
            &engine,
            "SELECT count(*) FROM pg_proc WHERE proname = 'mytotal'"
        )
        .await
            == vec![some(&["0"])]
    );
    assert!(
        error(&engine, "DROP AGGREGATE mytotal(int4)")
            .await
            .contains("aggregate mytotal(integer) does not exist")
    );
    run(&engine, "DROP AGGREGATE IF EXISTS mytotal(int4)").await;
}

#[tokio::test]
async fn a_second_definition_needs_or_replace() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE mysum (int4) (SFUNC = addint, STYPE = int4, INITCOND = '0')",
    )
    .await;
    assert!(
        error(
            &engine,
            "CREATE AGGREGATE mysum (int4) (SFUNC = addint, STYPE = int4, INITCOND = '1')"
        )
        .await
        .contains("already exists with same argument types")
    );
    run(
        &engine,
        "CREATE OR REPLACE AGGREGATE mysum (int4) (SFUNC = addint, STYPE = int4, INITCOND = '10')",
    )
    .await;
    assert!(grid(&engine, "SELECT mysum(f1) FROM t").await == vec![some(&["16"])]);
}

/// A user aggregate must win over a same-named ordinary function, or the query
/// silently returns one row per input instead of aggregating.
///
/// The two are told apart by arity. A function and an aggregate that share a
/// name *and* an arity are a documented divergence: this engine resolves the
/// aggregate, where `PostgreSQL` would pick by argument type.
#[tokio::test]
async fn an_aggregate_is_not_shadowed_by_a_same_named_function() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE acc (int4) (SFUNC = addint, STYPE = int4, INITCOND = '0')",
    )
    .await;
    run(
        &engine,
        "CREATE FUNCTION acc(text, text) RETURNS int4 LANGUAGE sql AS 'select 1'",
    )
    .await;
    let aggregate = grid(&engine, "SELECT acc(f1) FROM t").await;
    assert!(aggregate == vec![some(&["6"])]);
    let function = grid(&engine, "SELECT acc('x', 'y')").await;
    assert!(function == vec![some(&["1"])]);
    let nested_function = grid(&engine, "SELECT max(acc('x', 'y')) FROM t").await;
    assert!(nested_function == vec![some(&["1"])]);
    let sibling_function = grid(&engine, "SELECT count(*), acc('x', 'y') FROM t").await;
    assert!(sibling_function == vec![some(&["3", "1"])]);
}

/// The `Aggregate` plan node is decided by the executor's own resolver, so
/// every built-in aggregate gets one — not just the fourteen a stale private
/// list used to name — and a user aggregate does too.
#[tokio::test]
async fn explain_names_an_aggregate_node_for_every_aggregate() {
    let engine = fixture().await;
    run(
        &engine,
        "CREATE AGGREGATE mysum (int4) (SFUNC = addint, STYPE = int4, INITCOND = '0')",
    )
    .await;
    for call in [
        "sum(f1)",
        "corr(f1, f1)",
        "var_pop(f1)",
        "stddev_pop(f1)",
        "bit_or(f1)",
        "json_agg(f1)",
        "regr_count(f1, f1)",
        "covar_pop(f1, f1)",
        "range_agg(int4range(f1, f1 + 1))",
        "mysum(f1)",
    ] {
        let plan = grid(
            &engine,
            &format!("EXPLAIN (COSTS OFF) SELECT {call} FROM t"),
        )
        .await;
        assert!(
            plan.first() == Some(&some(&["Aggregate"])),
            "{call} planned as {plan:?}"
        );
    }
}
