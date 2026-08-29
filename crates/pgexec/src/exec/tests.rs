use std::sync::Arc;

/// A predicate only crosses below a security policy when it cannot leak the
/// rows the policy hides. The upstream `rowsecurity` test proves the point
/// with an `f_leak(text)` that `RAISE NOTICE`s its argument: pushed under
/// the policy, it prints the titles of rows the user may not read.
#[test]
fn only_a_leakproof_predicate_may_cross_a_security_policy() {
    // (predicate, may it be pushed below the policy?)
    let cases: &[(&str, bool)] = &[
        ("a.unique2 < 10", true),
        ("a.x = 3 AND a.y > 1", true),
        ("a.x IS NULL", true),
        ("a.x + 1 = a.y", true),
        // A function call can observe — and re-emit — what it is handed.
        ("f_leak(a.title)", false),
        ("a.x < 10 AND f_leak(a.title)", false),
        // A cast and a division leak through their error messages instead.
        ("a.text_col::int = 1", false),
        ("a.x / a.y = 1", false),
    ];
    for (sql, expected) in cases {
        let expr = crabka_pgparser::parser::parse_expr_for_test(sql).expect("parse");
        assert2::assert!(super::leakproof_predicate(&expr) == *expected, "{sql}");
    }
}

#[test]
fn immutable_row_predicate_rejects_volatile_calls() {
    let cases: &[(&str, bool)] = &[
        ("1 = 1", true),
        ("random() = 0.5", false),
        ("count(*) > 0", false),
    ];
    for (sql, expected) in cases {
        let expr = crabka_pgparser::parser::parse_expr_for_test(sql).expect("parse");
        assert2::assert!(super::immutable_row_predicate(&expr) == *expected, "{sql}");
    }
}

#[test]
fn structural_figure_colnames_match_parser_rules() {
    let cases: &[(&str, &str)] = &[
        ("CASE WHEN true THEN 1 ELSE (SELECT 2 AS x) END", "x"),
        ("CASE WHEN true THEN 1 END", "case"),
        ("ARRAY[1, 2]", "array"),
        ("ROW(1, 2)", "row"),
        ("(ARRAY[1, 2])[1]", "array"),
        ("(SELECT x FROM (VALUES (1)) AS v(x))", "x"),
        ("EXISTS (SELECT 1)", "exists"),
    ];
    for (sql, expected) in cases {
        let expr = crabka_pgparser::parser::parse_expr_for_test(sql).expect("parse");
        assert2::assert!(super::derived_name(&expr) == *expected, "{sql}");
    }

    let column = || Expr::Column {
        table: None,
        name: "items".into(),
    };
    let field = Expr::FieldSelect {
        base: Box::new(column()),
        field: "price".into(),
    };
    let array_ref = Expr::ArrayRef {
        base: Box::new(column()),
        subscripts: Vec::new(),
    };
    assert2::assert!(super::derived_name(&field) == "price");
    assert2::assert!(super::derived_name(&array_ref) == "items");
}

#[test]
fn pruning_a_relation_keeps_its_visible_shape() {
    let live = ColumnBinding {
        exposure: Exposure::Output,
        qualifier: Some("v".into()),
        name: "live".into(),
        ty: crabka_pgtypes::ColumnType::Int4,
    };
    let dead = ColumnBinding {
        exposure: Exposure::Output,
        qualifier: Some("v".into()),
        name: "dead".into(),
        ty: crabka_pgtypes::ColumnType::Text,
    };
    let relation = super::Relation {
        scope: Scope {
            columns: vec![live.clone(), dead],
            ..Default::default()
        },
        rows: vec![vec![
            crabka_pgtypes::Datum::Int4(1),
            crabka_pgtypes::Datum::Text("unused".into()),
        ]],
    };

    let pruned = super::prune_relation_columns(relation, Some(&[live]));

    assert2::assert!(
        pruned.rows
            == vec![vec![
                crabka_pgtypes::Datum::Int4(1),
                crabka_pgtypes::Datum::Null
            ]]
    );
    assert2::assert!(pruned.scope.width() == 2);
}

#[test]
fn lateral_filter_pushdown_distinguishes_security_free_relations() {
    let mut relation = super::Relation {
        scope: Scope {
            columns: vec![ColumnBinding {
                exposure: Exposure::Output,
                qualifier: Some("t".into()),
                name: "id".into(),
                ty: crabka_pgtypes::ColumnType::Int4,
            }],
            ..Default::default()
        },
        rows: vec![vec![crabka_pgtypes::Datum::Int4(2)]],
    };
    let ctx = crate::clock::EvalCtx::test_default();

    let non_leakproof = crabka_pgparser::parser::parse_expr_for_test("t.id::text = '1'")
        .expect("parse cast predicate");
    super::push_left_where(
        &mut relation,
        crabka_pgparser::ast::JoinKind::Cross,
        &non_leakproof,
        &ctx,
        false,
    )
    .expect("do not push a cast");
    assert_eq!(relation.rows, vec![vec![crabka_pgtypes::Datum::Int4(2)]]);

    super::push_left_where(
        &mut relation,
        crabka_pgparser::ast::JoinKind::Cross,
        &non_leakproof,
        &ctx,
        true,
    )
    .expect("push a cast for a security-free virtual catalog relation");
    assert_eq!(relation.rows, Vec::<Vec<crabka_pgtypes::Datum>>::new());

    let unresolved = crabka_pgparser::parser::parse_expr_for_test("t.id = missing")
        .expect("parse unresolved predicate");
    super::push_left_where(
        &mut relation,
        crabka_pgparser::ast::JoinKind::Cross,
        &unresolved,
        &ctx,
        false,
    )
    .expect("do not evaluate an unresolved predicate");
}

#[test]
fn local_filter_pushdown_requires_security_free_right_scope() {
    let relation = |qualifier: &str, values: &[i32]| super::Relation {
        scope: Scope {
            columns: vec![ColumnBinding {
                exposure: Exposure::Output,
                qualifier: Some(qualifier.into()),
                name: "id".into(),
                ty: crabka_pgtypes::ColumnType::Int4,
            }],
            ..Default::default()
        },
        rows: values
            .iter()
            .map(|value| vec![crabka_pgtypes::Datum::Int4(*value)])
            .collect(),
    };
    let ctx = crate::clock::EvalCtx::test_default();
    let cast = crabka_pgparser::parser::parse_expr_for_test("r.id::text = '1'")
        .expect("parse right cast predicate");
    let mut left = relation("l", &[1, 2]);
    let mut right = relation("r", &[1, 2]);

    super::push_local_where(
        &mut left,
        &mut right,
        crabka_pgparser::ast::JoinKind::Cross,
        &cast,
        &ctx,
        false,
        false,
    )
    .expect("do not push a cast across a security boundary");
    assert_eq!(right.rows.len(), 2);

    super::push_local_where(
        &mut left,
        &mut right,
        crabka_pgparser::ast::JoinKind::Cross,
        &cast,
        &ctx,
        false,
        true,
    )
    .expect("push a cast for a security-free right relation");
    assert_eq!(right.rows, vec![vec![crabka_pgtypes::Datum::Int4(1)]]);

    let left_only =
        crabka_pgparser::parser::parse_expr_for_test("l.id = 1").expect("parse left predicate");
    let mut right = relation("r", &[1, 2]);
    super::push_local_where(
        &mut left,
        &mut right,
        crabka_pgparser::ast::JoinKind::Cross,
        &left_only,
        &ctx,
        false,
        true,
    )
    .expect("do not apply a left predicate to the right relation");
    assert_eq!(right.rows.len(), 2);
}

#[test]
fn security_free_from_item_requires_only_virtual_catalog_relations() {
    use crabka_units::convert::ByteSizeExt;

    let kv = crabka_pgkv::MemKv::new();
    let snapshot = crabka_pgmvcc::visibility::Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    };
    let ctes = crate::cte::CteContext::empty();
    let eval_ctx = crate::clock::EvalCtx::test_default();
    let scanner = crate::scanner::LocalRangeScanner;
    let policy_stack = crate::rls::PolicyStack::default();
    let refs = crate::scope::StatementRefs::default();
    let ctx = crate::subquery::SubCtx {
        catalog_kv: &kv,
        kv: &kv,
        global: &kv,
        gsnap: &snapshot,
        snapshot: &snapshot,
        own: None,
        command_id: None,
        ctes: &ctes,
        eval_ctx: &eval_ctx,
        fctx: crate::exec::ForeignCtx::none(),
        range_scanner: &scanner,
        blocking_query_memory: crabka_units::ByteSize::from_bytes(1),
        statement_memory: crate::scanner::StatementMemory::new(crabka_units::ByteSize::from_bytes(
            1,
        )),
        security_role: "owner",
        policy_stack: &policy_stack,
        refs: Some(&refs),
        explain_plan_state: None,
    };

    let virtual_only = parsed_select("SELECT * FROM pg_catalog.pg_type");
    let unqualified_virtual = parsed_select("SELECT * FROM pg_type");
    let mixed_join = parsed_select(
        "SELECT * FROM pg_catalog.pg_type AS t JOIN public.missing_local AS l ON true",
    );
    let virtual_join = parsed_select(
        "SELECT * FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_namespace AS n ON true",
    );

    assert2::assert!(super::security_free_from_item(&ctx, &virtual_only.from[0]));
    assert2::assert!(super::security_free_from_item(
        &ctx,
        &unqualified_virtual.from[0]
    ));
    assert2::assert!(!super::security_free_from_item(&ctx, &mixed_join.from[0]));
    assert2::assert!(super::security_free_from_item(&ctx, &virtual_join.from[0]));

    let mut shadowing_ctes = crate::cte::CteContext::empty();
    shadowing_ctes.insert(
        "pg_type".into(),
        crate::join::Relation {
            scope: crate::scope::Scope::empty(),
            rows: Vec::new(),
        },
    );
    assert2::assert!(!super::security_free_from_item(
        &ctx.with_ctes(&shadowing_ctes),
        &unqualified_virtual.from[0]
    ));
}

#[tokio::test]
async fn virtual_catalog_filters_do_not_relax_prior_user_tables() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE local_t (value text)").await;
    run(&engine, "INSERT INTO local_t VALUES ('not-an-int')").await;

    assert2::assert!(
        cells(
            &engine,
            "SELECT local_t.value
         FROM pg_catalog.pg_type AS t, local_t, pg_catalog.pg_namespace AS n
         WHERE local_t.value::int = 1 AND n.oid = 0",
        )
        .await
        .is_empty()
    );
}

#[tokio::test]
async fn pg_class_statistics_updates_persist() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE stats_target (id int)").await;
    run_s(
        &mut session,
        "UPDATE pg_class \
         SET reltuples = 23, relpages = 4, relallvisible = 3 \
         WHERE oid = 'stats_target'::regclass",
    )
    .await;

    assert_eq!(
        text_rows_of(
            &mut session,
            "SELECT reltuples::text, relpages::text, relallvisible::text \
             FROM pg_class WHERE oid = 'stats_target'::regclass",
        )
        .await,
        vec![text_row(&["23", "4", "3"])]
    );
}

use crabka_pgcatalog::RelationName;
use crabka_pgparser::ast::{Expr, QueryBody, SelectStmt, SetExpr, Statement};
use crabka_pgwire::engine::{Cell, Engine, FieldDescription, QueryResult, Session};

use crate::{
    ExecError, PartialAggregateFunction, PartialAggregateSpec, SqlEngine, SqlSession, TopKColumn,
    TopKSpec,
    plan_dist::DistributedScanPlan,
    scanner::{PredicatePushdown, ProjectionPushdown, ScanRequest, ScannedRow},
    scope::{ColumnBinding, Exposure, Scope},
};

struct RejectingRangeScanner;

impl crate::RangeScanner for RejectingRangeScanner {
    fn scan(&self, _request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError> {
        Err(ExecError::Unsupported(
            "test scanner rejects table scans".into(),
        ))
    }
}

#[test]
fn scan_pushdown_retry_is_limited_to_optional_predicate_or_projection() {
    let error = ExecError::Unsupported("predicate pushdown unsupported".into());

    assert!(super::should_retry_without_scan_pushdown(
        &error,
        &DistributedScanPlan {
            predicate: PredicatePushdown::Conjunctive(Vec::new()),
            projection: ProjectionPushdown::All,
            partial_aggregate: None,
            top_k: None,
            text_search: None,
        },
    ));

    assert!(!super::should_retry_without_scan_pushdown(
        &error,
        &DistributedScanPlan {
            predicate: PredicatePushdown::FullScan,
            projection: ProjectionPushdown::All,
            partial_aggregate: Some(PartialAggregateSpec {
                function: PartialAggregateFunction::Sum,
                column: Some(0),
                group_by: Vec::new(),
            }),
            top_k: None,
            text_search: None,
        },
    ));

    assert!(!super::should_retry_without_scan_pushdown(
        &error,
        &DistributedScanPlan {
            predicate: PredicatePushdown::FullScan,
            projection: ProjectionPushdown::All,
            partial_aggregate: None,
            top_k: Some(TopKSpec {
                order_by: vec![TopKColumn {
                    column: 0,
                    asc: true,
                }],
                limit: 1,
            }),
            text_search: None,
        },
    ));
}

#[test]
fn global_status_derefs_prepared_to_range0_global_clog() {
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgmvcc::{
        clog::{XidStatus, put_op},
        xid::GLOBAL_XID_BASE,
    };

    use super::global_status;
    let (local, global) = (MemKv::new(), MemKv::new());
    let li = 5u64;
    let g = GLOBAL_XID_BASE + 1;
    local
        .write_batch(&[put_op(li, XidStatus::Prepared(g))])
        .expect("put prepared marker");
    // G in-doubt (not in global clog, gsnap says running) => InProgress (invisible)
    let running = crabka_pgmvcc::visibility::Snapshot {
        xmin: g,
        xmax: g + 1,
        xip: vec![g],
    };
    assert_eq!(
        global_status(&local, &global, &running)(li).expect("resolve in-doubt"),
        XidStatus::InProgress
    );
    // G committed + settled (gsnap moved past it) => Committed (visible)
    global
        .write_batch(&[put_op(g, XidStatus::Committed)])
        .expect("put global commit");
    let settled = crabka_pgmvcc::visibility::Snapshot {
        xmin: g + 2,
        xmax: g + 2,
        xip: vec![],
    };
    assert_eq!(
        global_status(&local, &global, &settled)(li).expect("resolve settled"),
        XidStatus::Committed
    );
    // A plain local xid is unaffected.
    local
        .write_batch(&[put_op(3, XidStatus::Committed)])
        .expect("put local commit");
    assert_eq!(
        global_status(&local, &global, &settled)(3).expect("resolve local"),
        XidStatus::Committed
    );
}

#[test]
fn durable_global_snapshot_resolves_committed_against_range0() {
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgmvcc::{
        clog::{XidStatus, put_op},
        xid::GLOBAL_XID_BASE,
    };
    let local = MemKv::new(); // this range's clog
    let global = MemKv::new(); // range 0's global clog + meta
    let g = GLOBAL_XID_BASE + 5;

    local
        .write_batch(&[put_op(3, XidStatus::Prepared(g))])
        .expect("local prepared");
    // Range 0: g committed, next_global persisted past g — BIG-ENDIAN, the exact
    // on-disk layout the GTM allocator writes (correction C1).
    global
        .write_batch(&[put_op(g, XidStatus::Committed)])
        .expect("global committed");
    global
        .write_batch(&[crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::meta_next_global_xid_key(),
            value: (g + 1).to_be_bytes().to_vec(),
        }])
        .expect("persist next_global");

    let gsnap = crate::session::durable_global_snapshot(&global).expect("rebuild gsnap");
    let resolve = crate::exec::global_status(&local, &global, &gsnap);
    assert_eq!(
        resolve(3).expect("resolve"),
        XidStatus::Committed,
        "committed cross-range deleter resolves Committed via range 0's durable clog"
    );

    let g2 = GLOBAL_XID_BASE + 6;
    local
        .write_batch(&[put_op(4, XidStatus::Prepared(g2))])
        .expect("local prepared 2");
    global
        .write_batch(&[crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::meta_next_global_xid_key(),
            value: (g2 + 1).to_be_bytes().to_vec(),
        }])
        .expect("advance next_global past g2");
    let gsnap2 = crate::session::durable_global_snapshot(&global).expect("rebuild gsnap2");
    let resolve2 = crate::exec::global_status(&local, &global, &gsnap2);
    assert_eq!(
        resolve2(4).expect("resolve g2"),
        XidStatus::InProgress,
        "allocated-but-undecided cross-range deleter is invisible"
    );
}

async fn run_s(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("ok")
}

/// The rows a query returns, as text, so a DDL test can state the whole
/// expected table rather than probing it field by field.
async fn text_rows_of(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    let results = session.simple_query(sql).await.expect(sql);
    results
        .into_iter()
        .flat_map(|result| match result {
            QueryResult::Rows { rows, .. } => rows,
            QueryResult::Command { .. } | QueryResult::Empty => Vec::new(),
        })
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.map(|cell| String::from_utf8_lossy(&cell.text).into_owned()))
                .collect()
        })
        .collect()
}

async fn sqlstate_of(session: &mut SqlSession, sql: &str) -> String {
    session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} must fail"))
        .code
}

fn text_row(values: &[&str]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|value| Some((*value).to_string()))
        .collect()
}

#[tokio::test]
async fn statistics_import_resolves_the_pg_temp_alias() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE stats_public (id int4)").await;
    run_s(&mut session, "CREATE TEMP TABLE stats_temp (id int4)").await;
    run_s(
        &mut session,
        "SELECT pg_restore_relation_stats(\
            'schemaname', 'pg_temp', 'relname', 'stats_temp', 'relpages', 7::int4)",
    )
    .await;
    run_s(
        &mut session,
        "SELECT pg_restore_attribute_stats(\
            'schemaname', 'pg_temp', 'relname', 'stats_temp', 'attname', 'id', \
            'inherited', false::boolean, 'null_frac', 0.25::real)",
    )
    .await;

    assert!(
        text_rows_of(
            &mut session,
            "SELECT relpages::text FROM pg_class \
             WHERE oid = 'pg_temp.stats_temp'::regclass",
        )
        .await
            == vec![text_row(&["7"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT null_frac::text || ',' || avg_width::text || ',' || n_distinct::text \
             FROM pg_stats \
             WHERE tablename = 'stats_temp' AND attname = 'id'",
        )
        .await
            == vec![text_row(&["0.25,0,0"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT stanullfrac::text || ',' || stawidth::text || ',' || stadistinct::text \
             FROM pg_statistic WHERE starelid = 'pg_temp.stats_temp'::regclass",
        )
        .await
            == vec![text_row(&["0.25,0,0"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT count(*)::text FROM pg_class \
             WHERE oid = 'public.stats_public'::regclass",
        )
        .await
            == vec![text_row(&["1"])]
    );
}

#[tokio::test]
async fn restored_attribute_statistics_are_visible_through_both_catalogs() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE stats_test (id int4)").await;
    run_s(&mut session, "BEGIN").await;
    run_s(
        &mut session,
        "SELECT pg_restore_attribute_stats(\
            'schemaname', 'public', 'relname', 'stats_test', 'attnum', 1::smallint, \
            'inherited', false::boolean, 'null_frac', 0.25::real, \
            'most_common_vals', '{2,1}'::text, \
            'most_common_freqs', '{0.6,0.4}'::real[])",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT attname, null_frac, most_common_vals::text, most_common_freqs::text \
             FROM pg_stats WHERE tablename = 'stats_test'",
        )
        .await
            == vec![text_row(&["id", "0.25", "{2,1}", "{0.6,0.4}"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT staattnum, stakind1, stanumbers1::text, stavalues1::text \
             FROM pg_statistic WHERE starelid = 'stats_test'::regclass",
        )
        .await
            == vec![text_row(&["1", "1", "{0.6,0.4}", "{2,1}"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT mode FROM pg_locks \
             WHERE relation = 'stats_test'::regclass \
             AND mode = 'ShareUpdateExclusiveLock'",
        )
        .await
            == vec![text_row(&["ShareUpdateExclusiveLock"])]
    );
    run_s(&mut session, "ROLLBACK").await;

    run_s(
        &mut session,
        "CREATE INDEX stats_test_expr ON stats_test ((id % 2 = 1))",
    )
    .await;
    run_s(&mut session, "BEGIN").await;
    run_s(
        &mut session,
        "SELECT pg_restore_attribute_stats(\
            'schemaname', 'public', 'relname', 'stats_test_expr', 'attname', 'expr', \
            'inherited', false::boolean, 'null_frac', 0.5::real, \
            'most_common_vals', '{t,f}'::text, \
            'most_common_freqs', '{0.75,0.25}'::real[])",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT attname, atttypid::regtype::text, attnum FROM pg_attribute \
             WHERE attrelid = 'stats_test_expr'::regclass",
        )
        .await
            == vec![text_row(&["expr", "boolean", "1"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT attname, null_frac, most_common_vals::text, most_common_freqs::text \
             FROM pg_stats WHERE tablename = 'stats_test_expr'",
        )
        .await
            == vec![text_row(&["expr", "0.5", "{t,f}", "{0.75,0.25}"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT staattnum, stakind1, stanumbers1::text, stavalues1::text \
             FROM pg_statistic WHERE starelid = 'stats_test_expr'::regclass",
        )
        .await
            == vec![text_row(&["1", "1", "{0.75,0.25}", "{t,f}"])]
    );
    run_s(&mut session, "ROLLBACK").await;

    run_s(&mut session, "CREATE INDEX stats_test_i ON stats_test (id)").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT relpages, reltuples FROM pg_class WHERE oid = 'stats_test_i'::regclass",
        )
        .await
            == vec![text_row(&["1", "0"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT attname, atttypid::regtype::text, attnum FROM pg_attribute \
             WHERE attrelid = 'stats_test_i'::regclass",
        )
        .await
            == vec![text_row(&["id", "integer", "1"])]
    );
    run_s(&mut session, "BEGIN").await;
    run_s(
        &mut session,
        "SELECT pg_restore_relation_stats(\
            'schemaname', 'public', 'relname', 'stats_test_i', 'relpages', 1::integer)",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT relation::regclass::text, mode FROM pg_locks \
             WHERE relation = 'stats_test'::regclass \
             AND mode = 'ShareUpdateExclusiveLock'",
        )
        .await
            == vec![text_row(&["stats_test", "ShareUpdateExclusiveLock"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT relation::regclass::text, mode FROM pg_locks \
             WHERE relation = 'stats_test_i'::regclass \
             AND mode = 'ShareUpdateExclusiveLock'",
        )
        .await
            == vec![text_row(&["stats_test_i", "ShareUpdateExclusiveLock"])]
    );
    run_s(&mut session, "ROLLBACK").await;
}

/// Two output columns of the same name would define a relation whose columns
/// cannot be told apart, so `CREATE VIEW` refuses it with `PostgreSQL`'s
/// 42701 before it creates anything. `CREATE TABLE AS` applies the same
/// rule.
#[tokio::test]
async fn create_view_refuses_duplicate_output_column_names() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (id int4, label text)").await;
    run_s(&mut session, "INSERT INTO t VALUES (1, 'one')").await;

    for sql in [
        "CREATE VIEW v AS SELECT id, id FROM t",
        "CREATE VIEW v AS SELECT id AS k, label AS k FROM t",
        // Two unnamed expressions both label `?column?`.
        "CREATE VIEW v AS SELECT 1 + 1, 2 + 2 FROM t",
    ] {
        assert!(sqlstate_of(&mut session, sql).await == "42701", "{sql}");
        // Nothing was created, so the name is still free.
        assert!(
            sqlstate_of(&mut session, "SELECT * FROM v").await == "42P01",
            "{sql}"
        );
    }

    // Names that only differ by quoting do not collide.
    run_s(
        &mut session,
        "CREATE VIEW v AS SELECT id AS k, label AS \"K\" FROM t",
    )
    .await;
    assert!(
        text_rows_of(&mut session, "SELECT k, \"K\" FROM v").await == vec![text_row(&["1", "one"])]
    );
}

/// A `UNIQUE` key written on a partitioned parent is enforced by the copy
/// each partition carries, not by the parent's own index: the parent stores
/// no rows, so nothing ever reaches that index. The copy is what
/// `pg_constraint` reports and what the 23505 names.
#[tokio::test]
async fn a_partitioned_unique_key_is_cloned_onto_every_partition() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE parted_uniq_tbl (i int UNIQUE DEFERRABLE) PARTITION BY RANGE (i)",
        "CREATE TABLE parted_uniq_tbl_1 PARTITION OF parted_uniq_tbl \
             FOR VALUES FROM (0) TO (10)",
        "CREATE TABLE parted_uniq_tbl_2 PARTITION OF parted_uniq_tbl \
             FOR VALUES FROM (20) TO (30)",
    ] {
        run_s(&mut session, sql).await;
    }

    assert!(
        text_rows_of(
            &mut session,
            "SELECT conname, conrelid::regclass::text FROM pg_constraint \
                 WHERE conname LIKE 'parted_uniq%' ORDER BY conname",
        )
        .await
            == vec![
                text_row(&["parted_uniq_tbl_1_i_key", "parted_uniq_tbl_1"]),
                text_row(&["parted_uniq_tbl_2_i_key", "parted_uniq_tbl_2"]),
                text_row(&["parted_uniq_tbl_i_key", "parted_uniq_tbl"]),
            ]
    );

    run_s(&mut session, "INSERT INTO parted_uniq_tbl VALUES (1)").await;
    assert!(
        error_of(&mut session, "INSERT INTO parted_uniq_tbl VALUES (1)").await
            == (
                "23505".to_string(),
                "duplicate key value violates unique constraint \"parted_uniq_tbl_1_i_key\""
                    .to_string(),
            )
    );
    // The same key in another partition's range is a different key, so the
    // clone constrains its own partition and nothing else.
    run_s(&mut session, "INSERT INTO parted_uniq_tbl VALUES (21)").await;
}

/// The clone keeps the parent's deferral. `UNIQUE DEFERRABLE` is checked at
/// the end of the statement, so two conflicting rows inserted by one
/// statement are fine and two inserted by two statements are not.
#[tokio::test]
async fn a_cloned_partition_key_keeps_the_parents_deferral() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE swap (i int UNIQUE DEFERRABLE) PARTITION BY RANGE (i)",
        "CREATE TABLE swap_1 PARTITION OF swap FOR VALUES FROM (0) TO (10)",
        "INSERT INTO swap VALUES (1), (2)",
        // Deferred to statement end, so the pair crossing over is legal.
        "UPDATE swap SET i = 3 - i",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        text_rows_of(&mut session, "SELECT i FROM swap ORDER BY i").await
            == vec![text_row(&["1"]), text_row(&["2"])]
    );
    assert!(sqlstate_of(&mut session, "INSERT INTO swap VALUES (1)").await == "23505");
}

/// `ATTACH PARTITION` copies the parent's indexes onto the candidate, and
/// the copy is built over the rows the candidate already holds — so a
/// candidate whose rows break the parent's key cannot be attached.
#[tokio::test]
async fn attach_partition_builds_the_parents_indexes_over_existing_rows() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE att (i int, j int, PRIMARY KEY (i)) PARTITION BY RANGE (i)",
        "CREATE TABLE att_bad (i int NOT NULL, j int)",
        "INSERT INTO att_bad VALUES (1, 10), (1, 20)",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        sqlstate_of(
            &mut session,
            "ALTER TABLE att ATTACH PARTITION att_bad FOR VALUES FROM (0) TO (10)",
        )
        .await
            == "23505"
    );

    run_s(&mut session, "CREATE TABLE att_ok (i int NOT NULL, j int)").await;
    run_s(&mut session, "INSERT INTO att_ok VALUES (1, 10), (2, 20)").await;
    run_s(
        &mut session,
        "ALTER TABLE att ATTACH PARTITION att_ok FOR VALUES FROM (0) TO (10)",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT conname, conrelid::regclass::text FROM pg_constraint \
                 WHERE conname LIKE 'att%pkey' ORDER BY conname",
        )
        .await
            == vec![
                text_row(&["att_ok_pkey", "att_ok"]),
                text_row(&["att_pkey", "att"]),
            ]
    );
    // The copy enforces the parent's key over rows written after the attach.
    assert!(sqlstate_of(&mut session, "INSERT INTO att VALUES (1, 30)").await == "23505");
}

/// A candidate that already carries an equivalent constraint keeps it:
/// `PostgreSQL` matches an existing index rather than building a second one.
#[tokio::test]
async fn attach_partition_reuses_an_equivalent_index_the_candidate_already_has() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE reuse (i int, PRIMARY KEY (i)) PARTITION BY RANGE (i)",
        "CREATE TABLE reuse_1 (i int NOT NULL, CONSTRAINT mine PRIMARY KEY (i))",
        "ALTER TABLE reuse ATTACH PARTITION reuse_1 FOR VALUES FROM (0) TO (10)",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        text_rows_of(
            &mut session,
            "SELECT conname, conrelid::regclass::text FROM pg_constraint \
                 WHERE conname IN ('mine', 'reuse_pkey', 'reuse_1_pkey') ORDER BY conname",
        )
        .await
            == vec![
                text_row(&["mine", "reuse_1"]),
                text_row(&["reuse_pkey", "reuse"]),
            ]
    );
}

/// A key added to a partitioned parent after its partitions exist is
/// copied onto each of them and built over the rows they already hold. A
/// key that leaves a partition-key column out is refused instead: no copy
/// could enforce it, because two partitions never see each other's rows.
#[tokio::test]
async fn adding_a_key_to_a_partitioned_parent_reaches_its_partitions() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE late (i int NOT NULL, j int) PARTITION BY RANGE (i)",
        "CREATE TABLE late_1 PARTITION OF late FOR VALUES FROM (0) TO (10)",
        "INSERT INTO late VALUES (1, 10), (2, 20)",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        sqlstate_of(&mut session, "ALTER TABLE late ADD UNIQUE (j)").await == "0A000",
        "a key without the partition key cannot be enforced"
    );

    run_s(&mut session, "ALTER TABLE late ADD PRIMARY KEY (i)").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT conname, conrelid::regclass::text FROM pg_constraint \
                 WHERE conname LIKE 'late%pkey' ORDER BY conname",
        )
        .await
            == vec![
                text_row(&["late_1_pkey", "late_1"]),
                text_row(&["late_pkey", "late"]),
            ]
    );
    assert!(sqlstate_of(&mut session, "INSERT INTO late VALUES (1, 30)").await == "23505");

    // And the copy is built over the rows already stored, so a key the
    // partition's existing rows break is refused.
    run_s(
        &mut session,
        "CREATE TABLE dupes (i int NOT NULL) PARTITION BY RANGE (i)",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TABLE dupes_1 PARTITION OF dupes FOR VALUES FROM (0) TO (10)",
    )
    .await;
    run_s(&mut session, "INSERT INTO dupes VALUES (1), (1)").await;
    assert!(sqlstate_of(&mut session, "ALTER TABLE dupes ADD UNIQUE (i)").await == "23505");
}

/// An unnamed `CREATE INDEX` takes the next free `_idx` label, so a second
/// index on the same key is legal. `PostgreSQL`'s `ChooseRelationName`
/// counts the label up until nothing in the schema answers to the name. An
/// index the statement names itself is never renamed: that name is the
/// user's, and a collision in it is an error.
#[tokio::test]
async fn a_second_unnamed_index_on_one_key_takes_the_next_free_name() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE named (a int, b int)",
        "CREATE INDEX ON named (a)",
        "CREATE INDEX ON named (a)",
        "CREATE INDEX ON named (a)",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        text_rows_of(
            &mut session,
            "SELECT indexname FROM pg_indexes WHERE tablename = 'named' ORDER BY indexname",
        )
        .await
            == vec![
                text_row(&["named_a_idx"]),
                text_row(&["named_a_idx1"]),
                text_row(&["named_a_idx2"]),
            ]
    );
    assert!(sqlstate_of(&mut session, "CREATE INDEX named_a_idx ON named (b)").await == "42P07");
}

/// A comment dies with the column it describes. `PostgreSQL` deletes the
/// `pg_description` row keyed on the column's `attnum` as the column goes,
/// so a later column of the same name starts with no comment. The
/// relation's own comment, and every other column's, are untouched.
#[tokio::test]
async fn a_dropped_column_takes_its_comment_with_it() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE described (a int, b int)",
        "COMMENT ON TABLE described IS 'the table'",
        "COMMENT ON COLUMN described.a IS 'the first'",
        "COMMENT ON COLUMN described.b IS 'the second'",
        "ALTER TABLE described DROP COLUMN a",
        "ALTER TABLE described ADD COLUMN a int",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        text_rows_of(
            &mut session,
            "SELECT a.attname, col_description(a.attrelid, a.attnum) \
                 FROM pg_attribute a WHERE a.attrelid = 'described'::regclass \
                 AND a.attnum > 0 ORDER BY a.attname",
        )
        .await
            == vec![
                vec![Some("a".to_string()), None],
                text_row(&["b", "the second"]),
            ]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT obj_description('described'::regclass, 'pg_class')",
        )
        .await
            == vec![text_row(&["the table"])]
    );
}

#[tokio::test]
async fn user_type_and_domain_comments_are_visible_in_pg_description() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TYPE described_type AS (id int)",
        "CREATE DOMAIN described_domain AS int",
        "COMMENT ON TYPE described_type IS 'the type'",
        "COMMENT ON DOMAIN described_domain IS 'the domain'",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        text_rows_of(
            &mut session,
            "SELECT count(DISTINCT oid) = 2 FROM pg_type \
             WHERE typname IN ('described_domain', 'described_type')",
        )
        .await
            == vec![text_row(&["t"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT t.typname, d.description FROM pg_description d \
             JOIN pg_type t ON t.oid = d.objoid \
             WHERE t.typname IN ('described_domain', 'described_type') ORDER BY t.typname",
        )
        .await
            == vec![
                text_row(&["described_domain", "the domain"]),
                text_row(&["described_type", "the type"]),
            ]
    );
    run_s(&mut session, "DROP TYPE described_type").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT description FROM pg_description d JOIN pg_type t ON t.oid = d.objoid \
             WHERE t.typname = 'described_type'",
        )
        .await
        .is_empty()
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT typname, obj_description(oid, 'pg_type') \
             FROM pg_type WHERE typname = 'described_domain'",
        )
        .await
            == vec![text_row(&["described_domain", "the domain"])]
    );
}

#[tokio::test]
async fn cast_and_access_method_comments_are_visible_in_pg_description() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE CAST (int8 AS timestamp) WITHOUT FUNCTION",
        "CREATE ACCESS METHOD described_am TYPE TABLE HANDLER heap_tableam_handler",
        "COMMENT ON CAST (int8 AS timestamp) IS 'the cast'",
        "COMMENT ON ACCESS METHOD described_am IS 'the access method'",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        text_rows_of(
            &mut session,
            "SELECT obj_description(oid, 'pg_cast') FROM pg_cast \
             WHERE castsource = 'int8'::regtype AND casttarget = 'timestamp'::regtype",
        )
        .await
            == vec![text_row(&["the cast"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT obj_description(oid, 'pg_am') FROM pg_am WHERE amname = 'described_am'",
        )
        .await
            == vec![text_row(&["the access method"])]
    );
}

#[tokio::test]
async fn a_table_of_a_composite_type_copies_its_fields() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TYPE person_type AS (id int, name text)",
    )
    .await;
    run_s(&mut session, "CREATE TABLE persons OF person_type").await;
    run_s(&mut session, "INSERT INTO persons VALUES (1, 'Ada')").await;
    assert!(
        text_rows_of(&mut session, "SELECT id, name FROM persons").await
            == vec![text_row(&["1", "Ada"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT c.reloftype = t.oid FROM pg_class c JOIN pg_type t ON t.typname = 'person_type' WHERE c.relname = 'persons'",
        )
        .await
            == vec![text_row(&["t"])]
    );
    for (sql, message) in [
        (
            "ALTER TABLE persons ADD COLUMN comment text",
            "cannot add column to typed table",
        ),
        (
            "ALTER TABLE persons DROP COLUMN name",
            "cannot drop column from typed table",
        ),
        (
            "ALTER TABLE persons RENAME COLUMN id TO num",
            "cannot rename column of typed table",
        ),
        (
            "ALTER TABLE persons ALTER COLUMN name TYPE varchar",
            "cannot alter column type of typed table",
        ),
        (
            "ALTER TABLE persons INHERIT persons",
            "cannot change inheritance of typed table",
        ),
    ] {
        assert!(error_of(&mut session, sql).await == ("42809".into(), message.into()));
    }
    run_s(
        &mut session,
        "CREATE FUNCTION person_name(person_type) RETURNS text LANGUAGE SQL AS $$ SELECT $1.name $$",
    )
    .await;
    assert!(
        text_rows_of(&mut session, "SELECT person_name(persons) FROM persons").await
            == vec![text_row(&["Ada"])]
    );
    run_s(
        &mut session,
        "CREATE TABLE keyed_people OF person_type (id WITH OPTIONS PRIMARY KEY, UNIQUE (name))",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT indexname FROM pg_indexes WHERE tablename = 'keyed_people' ORDER BY indexname",
        )
        .await
            == vec![
                text_row(&["keyed_people_name_key"]),
                text_row(&["keyed_people_pkey"]),
            ]
    );
    assert!(
        error_of(
            &mut session,
            "CREATE TABLE identity_people OF person_type \
             (id WITH OPTIONS GENERATED ALWAYS AS IDENTITY)",
        )
        .await
            == (
                "0A000".into(),
                "identity columns are not supported on typed tables".into(),
            )
    );
    assert!(
        error_of(
            &mut session,
            "CREATE TABLE generated_people OF person_type \
             (name WITH OPTIONS GENERATED ALWAYS AS ('Ada'::text) STORED)",
        )
        .await
            == (
                "0A000".into(),
                "generated columns are not supported on typed tables".into(),
            )
    );
    assert!(sqlstate_of(&mut session, "CREATE TABLE missing OF absent_type").await == "42704");
    run_s(&mut session, "CREATE TYPE label_type AS ENUM ('a')").await;
    assert!(sqlstate_of(&mut session, "CREATE TABLE labels OF label_type").await == "42809");
    assert!(sqlstate_of(&mut session, "DROP TYPE person_type RESTRICT").await == "2BP01");
    run_s(&mut session, "DROP TYPE person_type CASCADE").await;
    assert!(sqlstate_of(&mut session, "SELECT * FROM persons").await == "42P01");
}

#[tokio::test]
async fn alter_table_can_associate_and_disassociate_a_matching_row_type() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TYPE pair AS (id int4, label text)").await;
    run_s(&mut session, "CREATE TABLE items (id int4, label text)").await;
    run_s(&mut session, "ALTER TABLE IF EXISTS absent OF pair").await;
    assert!(sqlstate_of(&mut session, "ALTER TABLE absent OF pair").await == "42P01");
    run_s(&mut session, "CREATE TABLE too_short (id int4)").await;
    assert!(
        error_of(&mut session, "ALTER TABLE too_short OF pair").await
            == ("42P16".into(), "table is missing column \"label\"".into())
    );
    run_s(
        &mut session,
        "CREATE TABLE wrong_type (id text, label text)",
    )
    .await;
    assert!(
        error_of(&mut session, "ALTER TABLE wrong_type OF pair").await
            == (
                "42P16".into(),
                "table \"wrong_type\" has different type for column \"id\"".into(),
            )
    );
    run_s(
        &mut session,
        "CREATE TABLE wrong_order (label text, id int4)",
    )
    .await;
    assert!(
        error_of(&mut session, "ALTER TABLE wrong_order OF pair").await
            == (
                "42P16".into(),
                "table has column \"label\" where type requires \"id\"".into(),
            )
    );
    run_s(
        &mut session,
        "CREATE TABLE too_wide (id int4, label text, extra int4)",
    )
    .await;
    assert!(
        error_of(&mut session, "ALTER TABLE too_wide OF pair").await
            == ("42P16".into(), "table has extra column \"extra\"".into())
    );
    run_s(&mut session, "CREATE TABLE parent (id int4, label text)").await;
    run_s(&mut session, "CREATE TABLE inherited () INHERITS (parent)").await;
    assert!(
        error_of(&mut session, "ALTER TABLE inherited OF pair").await
            == ("42809".into(), "typed tables cannot inherit".into())
    );

    run_s(&mut session, "ALTER TABLE items OF pair").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT c.reloftype = t.oid FROM pg_class c JOIN pg_type t \
             ON t.typname = 'pair' WHERE c.relname = 'items'",
        )
        .await
            == vec![text_row(&["t"])]
    );
    run_s(&mut session, "ALTER TABLE items NOT OF").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT reloftype FROM pg_class WHERE relname = 'items'",
        )
        .await
            == vec![text_row(&["0"])]
    );
}

#[tokio::test]
async fn create_table_like_copies_generated_columns_only_when_requested() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE source (base int4, doubled int4 GENERATED ALWAYS AS (base * 2) STORED)",
    )
    .await;
    run_s(&mut session, "CREATE TABLE copied_default (LIKE source)").await;
    run_s(&mut session, "INSERT INTO copied_default VALUES (4, 91)").await;
    assert!(
        text_rows_of(&mut session, "SELECT * FROM copied_default").await
            == vec![text_row(&["4", "91"])]
    );

    run_s(
        &mut session,
        "CREATE TABLE copied_generated (LIKE source INCLUDING GENERATED)",
    )
    .await;
    run_s(
        &mut session,
        "INSERT INTO copied_generated (base) VALUES (4)",
    )
    .await;
    assert!(
        text_rows_of(&mut session, "SELECT * FROM copied_generated").await
            == vec![text_row(&["4", "8"])]
    );

    run_s(
        &mut session,
        "CREATE TABLE copied_all (LIKE source INCLUDING ALL)",
    )
    .await;
    run_s(&mut session, "INSERT INTO copied_all (base) VALUES (5)").await;
    assert!(
        text_rows_of(&mut session, "SELECT * FROM copied_all").await
            == vec![text_row(&["5", "10"])]
    );
}

#[tokio::test]
async fn create_table_like_identity_owns_a_new_sequence() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE source (id int4 GENERATED BY DEFAULT AS IDENTITY \
         (START WITH 7 INCREMENT BY 2))",
    )
    .await;
    run_s(&mut session, "INSERT INTO source DEFAULT VALUES").await;
    run_s(
        &mut session,
        "CREATE TABLE copied (LIKE source INCLUDING IDENTITY)",
    )
    .await;
    run_s(&mut session, "INSERT INTO copied DEFAULT VALUES").await;
    run_s(&mut session, "INSERT INTO source DEFAULT VALUES").await;
    assert!(
        text_rows_of(&mut session, "SELECT id FROM source ORDER BY id").await
            == vec![text_row(&["7"]), text_row(&["9"])]
    );
    assert!(text_rows_of(&mut session, "SELECT id FROM copied").await == vec![text_row(&["7"])]);
    run_s(&mut session, "CREATE TABLE plain (id int4)").await;
    run_s(
        &mut session,
        "CREATE TABLE copied_plain (LIKE plain INCLUDING IDENTITY)",
    )
    .await;
    run_s(&mut session, "INSERT INTO copied_plain VALUES (1)").await;
}

#[tokio::test]
async fn create_table_like_including_indexes_copies_ordinary_indexes() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE source (id int4, label text)").await;
    run_s(&mut session, "CREATE INDEX ON source (label)").await;
    run_s(
        &mut session,
        "CREATE TABLE copied (LIKE source INCLUDING INDEXES)",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT indexname FROM pg_indexes WHERE tablename = 'copied'",
        )
        .await
            == vec![text_row(&["copied_label_idx"])]
    );
}

#[tokio::test]
async fn create_table_like_keeps_the_written_column_position() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE source (copied int4)").await;
    run_s(
        &mut session,
        "CREATE TABLE target (before int4, LIKE source, after text)",
    )
    .await;
    run_s(&mut session, "INSERT INTO target VALUES (1, 2, 'tail')").await;
    assert!(
        text_rows_of(&mut session, "SELECT before, copied, after FROM target").await
            == vec![text_row(&["1", "2", "tail"])]
    );
    run_s(
        &mut session,
        "CREATE TABLE target_at_end (before int4, LIKE source)",
    )
    .await;
    run_s(&mut session, "INSERT INTO target_at_end VALUES (3, 4)").await;
    assert!(
        text_rows_of(&mut session, "SELECT before, copied FROM target_at_end").await
            == vec![text_row(&["3", "4"])]
    );
}

#[tokio::test]
async fn create_table_like_accepts_relation_and_composite_sources() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE source (a int4, b text)").await;
    run_s(
        &mut session,
        "CREATE VIEW source_view AS SELECT * FROM source",
    )
    .await;
    run_s(
        &mut session,
        "CREATE MATERIALIZED VIEW source_matview AS SELECT * FROM source",
    )
    .await;
    run_s(&mut session, "CREATE TYPE source_type AS (a int4, b text)").await;
    for (target, definition, source) in [
        ("from_view", "LIKE source_view INCLUDING ALL", "source_view"),
        ("from_matview", "LIKE source_matview", "source_matview"),
        ("from_type", "LIKE source_type", "source_type"),
    ] {
        run_s(
            &mut session,
            &format!("CREATE TABLE {target} ({definition})"),
        )
        .await;
        run_s(
            &mut session,
            &format!("INSERT INTO {target} VALUES (1, 'copied')"),
        )
        .await;
        assert!(
            text_rows_of(&mut session, &format!("SELECT a, b FROM {target}")).await
                == vec![text_row(&["1", "copied"])],
            "{source}"
        );
    }

    run_s(&mut session, "CREATE INDEX source_index ON source (a)").await;
    run_s(&mut session, "CREATE SEQUENCE source_sequence").await;
    for (source, detail) in [
        ("source_index", "indexes"),
        ("source_sequence", "sequences"),
    ] {
        let expected_detail = format!("This operation is not supported for {detail}.");
        let error = session
            .simple_query(&format!("CREATE TABLE wrong_source (LIKE {source})"))
            .await
            .expect_err("invalid LIKE source");
        assert!(error.code == "42809");
        assert!(error.message == format!("relation \"{source}\" is invalid in LIKE clause"));
        assert!(
            error
                .diagnostics
                .as_ref()
                .and_then(|diagnostics| diagnostics.detail.as_deref())
                == Some(expected_detail.as_str())
        );
    }

    for sql in [
        "CREATE TABLE duplicate_local (a int4, LIKE source)",
        "CREATE TABLE duplicate_like (LIKE source, LIKE source)",
    ] {
        let (code, message) = error_of(&mut session, sql).await;
        assert!(code == "42701", "{sql}");
        assert!(message == "column \"a\" specified more than once", "{sql}");
    }
}

#[tokio::test]
async fn create_foreign_table_like_keeps_the_written_column_position() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE source (a int4 DEFAULT 7, b text CHECK (b <> 'bad'))",
    )
    .await;
    run_s(
        &mut session,
        "CREATE SERVER like_server FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'b:9092')",
    )
    .await;
    run_s(
        &mut session,
        "CREATE FOREIGN TABLE target (before int4, LIKE source INCLUDING ALL, after text) \
         SERVER like_server OPTIONS (topic 'target')",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT attname FROM pg_attribute WHERE attrelid = 'target'::regclass \
             AND attname IN ('before', 'a', 'b', 'after') ORDER BY attnum",
        )
        .await
            == vec![
                text_row(&["before"]),
                text_row(&["a"]),
                text_row(&["b"]),
                text_row(&["after"]),
            ]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef \
             WHERE adrelid = 'target'::regclass",
        )
        .await
            == vec![text_row(&["7"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT conname FROM pg_constraint WHERE conrelid = 'target'::regclass",
        )
        .await
            == vec![text_row(&["source_b_check"])]
    );
}

/// A row-security policy that reads a column depends on it. `PostgreSQL`
/// refuses the drop and names the policy, and `CASCADE` takes the whole
/// policy — never a policy left reading a column that is gone.
#[tokio::test]
async fn a_policy_that_reads_a_column_blocks_dropping_it() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE guarded (owner text, payload text, abc int)",
        "ALTER TABLE guarded ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY p ON guarded USING (owner = current_user)",
        "CREATE POLICY q ON guarded FOR INSERT WITH CHECK (owner = 'me')",
        // Neither policy reads `payload` or `abc`; `owner` occurs inside
        // neither the identifier `abc` nor any literal.
        "CREATE POLICY r ON guarded FOR SELECT USING (abc > 0)",
    ] {
        run_s(&mut session, sql).await;
    }

    assert!(
        error_of(&mut session, "ALTER TABLE guarded DROP COLUMN owner").await
            == (
                "2BP01".to_string(),
                "cannot drop column owner of table guarded because other objects depend on \
                     it\nDETAIL:  policy p on table guarded depends on column owner of table \
                     guarded\npolicy q on table guarded depends on column owner of table \
                     guarded\nHINT:  Use DROP ... CASCADE to drop the dependent objects too."
                    .to_string(),
            )
    );

    // A column no policy reads drops without a word, and the refusal above
    // wrote nothing.
    run_s(&mut session, "ALTER TABLE guarded DROP COLUMN payload").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT polname FROM pg_policy ORDER BY polname",
        )
        .await
            == vec![text_row(&["p"]), text_row(&["q"]), text_row(&["r"])]
    );

    run_s(
        &mut session,
        "ALTER TABLE guarded DROP COLUMN owner CASCADE",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT polname FROM pg_policy ORDER BY polname",
        )
        .await
            == vec![text_row(&["r"])]
    );
}

#[tokio::test]
async fn user_access_methods_are_catalogued_and_validate_handlers() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE ACCESS METHOD gist2 TYPE INDEX HANDLER gisthandler",
    )
    .await;
    run_s(
        &mut session,
        "CREATE ACCESS METHOD heap2 TYPE TABLE HANDLER heap_tableam_handler",
    )
    .await;
    run_s(&mut session, "CREATE TABLE am_table (a int) USING heap2").await;
    run_s(
        &mut session,
        "CREATE TABLE am_ctas USING heap2 AS SELECT 1 AS a",
    )
    .await;
    run_s(
        &mut session,
        "CREATE MATERIALIZED VIEW am_mat USING heap2 AS SELECT 1 AS a WITH NO DATA",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT amname, amhandler, amtype FROM pg_am \
             WHERE amname IN ('gist2', 'heap2') ORDER BY amname",
        )
        .await
            == vec![
                text_row(&["gist2", "gisthandler", "i"]),
                text_row(&["heap2", "heap_tableam_handler", "t"]),
            ]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT c.relname, am.amname FROM pg_class c JOIN pg_am am ON am.oid = c.relam \
             WHERE c.relname IN ('am_ctas', 'am_mat', 'am_table') ORDER BY c.relname",
        )
        .await
            == vec![
                text_row(&["am_ctas", "heap2"]),
                text_row(&["am_mat", "heap2"]),
                text_row(&["am_table", "heap2"]),
            ]
    );
    assert!(
        error_of(&mut session, "CREATE TABLE wrong_kind (a int) USING gist2").await
            == (
                "42809".into(),
                "access method \"gist2\" is not of type TABLE".into(),
            )
    );
    assert!(
        error_of(
            &mut session,
            "CREATE TABLE missing_method (a int) USING missing_am"
        )
        .await
            == (
                "42704".into(),
                "access method \"missing_am\" does not exist".into(),
            )
    );
    assert!(
        error_of(&mut session, "SET default_table_access_method = ''").await
            == (
                "22023".into(),
                "invalid value for parameter \"default_table_access_method\": \"\"".into(),
            )
    );
    assert!(
        error_of(
            &mut session,
            "SET default_table_access_method = 'missing_am'",
        )
        .await
            == (
                "22023".into(),
                "invalid value for parameter \"default_table_access_method\": \"missing_am\""
                    .into(),
            )
    );
    assert!(
        error_of(&mut session, "SET default_table_access_method = btree").await
            == (
                "42809".into(),
                "access method \"btree\" is not of type TABLE".into(),
            )
    );
    run_s(&mut session, "SET default_table_access_method = heap2").await;
    run_s(&mut session, "CREATE TABLE am_default (a int)").await;
    run_s(
        &mut session,
        "CREATE TABLE am_default_parent (a int) PARTITION BY LIST (a)",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TABLE am_default_child PARTITION OF am_default_parent FOR VALUES IN (1)",
    )
    .await;
    run_s(&mut session, "RESET default_table_access_method").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT c.relname, am.amname FROM pg_class c JOIN pg_am am ON am.oid = c.relam \
             WHERE c.relname IN ('am_default', 'am_default_child') ORDER BY c.relname",
        )
        .await
            == vec![
                text_row(&["am_default", "heap2"]),
                text_row(&["am_default_child", "heap2"]),
            ]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT relam FROM pg_class WHERE relname = 'am_default_parent'",
        )
        .await
            == vec![text_row(&["0"])]
    );
    run_s(&mut session, "ALTER TABLE am_table SET ACCESS METHOD heap").await;
    run_s(
        &mut session,
        "ALTER MATERIALIZED VIEW am_mat SET ACCESS METHOD heap",
    )
    .await;
    run_s(
        &mut session,
        "ALTER TABLE am_default_parent SET ACCESS METHOD heap2",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT c.relname, am.amname FROM pg_class c JOIN pg_am am ON am.oid = c.relam \
             WHERE c.relname IN ('am_default_parent', 'am_mat', 'am_table') ORDER BY c.relname",
        )
        .await
            == vec![
                text_row(&["am_default_parent", "heap2"]),
                text_row(&["am_mat", "heap"]),
                text_row(&["am_table", "heap"]),
            ]
    );
    run_s(
        &mut session,
        "ALTER TABLE am_default_parent SET ACCESS METHOD DEFAULT",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT relam FROM pg_class WHERE relname = 'am_default_parent'",
        )
        .await
            == vec![text_row(&["0"])]
    );
    assert!(
        error_of(
            &mut session,
            "ALTER TABLE am_table SET ACCESS METHOD heap, SET ACCESS METHOD heap2",
        )
        .await
            == (
                "42601".into(),
                "cannot have multiple SET ACCESS METHOD subcommands".into(),
            )
    );
    assert!(
        error_of(
            &mut session,
            "CREATE ACCESS METHOD bad_index TYPE INDEX HANDLER heap_tableam_handler",
        )
        .await
            == (
                "42804".into(),
                "function heap_tableam_handler must return type index_am_handler".into(),
            )
    );
    assert!(
        error_of(
            &mut session,
            "CREATE ACCESS METHOD bad_table TYPE TABLE HANDLER int4in",
        )
        .await
            == (
                "42883".into(),
                "function int4in(internal) does not exist".into(),
            )
    );
}

#[tokio::test]
async fn a_function_return_type_autocreates_a_shell() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let mut notices = session.take_notices().expect("notice receiver");

    run_s(
        &mut session,
        "CREATE FUNCTION autoshell_in(cstring) RETURNS autoshell \
         LANGUAGE internal AS 'int4in'",
    )
    .await;
    let notice = notices.try_recv().expect("shell creation notice");
    assert!(notice.message == "type \"autoshell\" is not yet defined");
    assert!(
        notice
            .diagnostics
            .as_ref()
            .and_then(|d| d.detail.as_deref())
            == Some("Creating a shell type definition.")
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT typisdefined FROM pg_type WHERE typname = 'autoshell'",
        )
        .await
            == vec![text_row(&["f"])]
    );
    run_s(
        &mut session,
        "CREATE FUNCTION autoshell_out(autoshell) RETURNS cstring \
         LANGUAGE internal AS 'int4out'",
    )
    .await;
    assert!(
        notices.try_recv().expect("argument shell notice").message
            == "argument type autoshell is only a shell"
    );
    run_s(
        &mut session,
        "CREATE TYPE autoshell (input = autoshell_in, output = autoshell_out, like = int4)",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT typisdefined FROM pg_type WHERE typname = 'autoshell'",
        )
        .await
            == vec![text_row(&["t"])]
    );
}

/// The `float4` regression suite builds a type from scratch to read a
/// `float4`'s bit pattern: a shell, an I/O pair bound to the built-in
/// `int4in`/`int4out`, a base type completing the shell, and two
/// binary-coercible casts. `1::xfloat4::float4` then has to be the float
/// whose *bits* are 1, not the float 1.
#[tokio::test]
async fn a_shell_completed_by_a_base_type_reinterprets_its_bits() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    // The shell exists, and taking the name twice is 42710.
    run_s(&mut session, "CREATE TYPE xfloat4").await;
    assert!(sqlstate_of(&mut session, "CREATE TYPE xfloat4").await == "42710");
    // A shell has no values: it resolves in a routine signature and nowhere
    // else, so it can never reach a column or an expression.
    assert!(sqlstate_of(&mut session, "CREATE TABLE t (c xfloat4)").await == "42704");

    let mut notices = session.take_notices().expect("notice receiver");
    run_s(
        &mut session,
        "create function xfloat4in(cstring) returns xfloat4 \
             immutable strict language internal as 'int4in'",
    )
    .await;
    // A shell named as the *return* type has no parse location, so the
    // notice carries no position; an argument type does, so it does.
    let notice = notices.try_recv().expect("return shell notice");
    assert!(notice.message == "return type xfloat4 is only a shell");
    assert!(
        notice
            .diagnostics
            .as_ref()
            .and_then(|d| d.position)
            .is_none()
    );
    run_s(
        &mut session,
        "create function xfloat4out(xfloat4) returns cstring \
             immutable strict language internal as 'int4out'",
    )
    .await;
    let notice = notices.try_recv().expect("argument shell notice");
    assert!(notice.message == "argument type xfloat4 is only a shell");
    // `create function xfloat4out(` is 27 characters, so the argument type
    // starts at the 28th — the column the client draws its caret under.
    assert!(notice.diagnostics.as_ref().and_then(|d| d.position) == Some(28));
    run_s(
        &mut session,
        "CREATE TYPE xfloat4 (input = xfloat4in, output = xfloat4out, like = float4)",
    )
    .await;

    // Before any cast is declared the type is an island: nothing converts.
    assert!(sqlstate_of(&mut session, "SELECT 1::xfloat4").await == "42846");

    run_s(
        &mut session,
        "CREATE CAST (xfloat4 AS float4) WITHOUT FUNCTION",
    )
    .await;
    run_s(
        &mut session,
        "CREATE CAST (integer AS xfloat4) WITHOUT FUNCTION",
    )
    .await;
    // A declared cast is visible in `pg_cast`, so the catalog and the cast
    // path agree about what conversions exist.
    assert!(
        text_rows_of(
            &mut session,
            "SELECT castcontext, castmethod FROM pg_cast \
                 WHERE castsource = 23 AND casttarget = \
                 (SELECT oid FROM pg_type WHERE typname = 'xfloat4')"
        )
        .await
            == vec![text_row(&["e", "b"])]
    );

    // The bit patterns from `float4.sql`: the smallest subnormals, and one
    // ordinary value.
    assert!(
        text_rows_of(
            &mut session,
            "SELECT 1::xfloat4::float4, 2::xfloat4::float4, 1065353216::xfloat4::float4"
        )
        .await
            == vec![text_row(&["1e-45", "3e-45", "1"])]
    );
    // And the round trip back out through `float4send` is the input word.
    assert!(
        text_rows_of(&mut session, "SELECT float4send(8388608::xfloat4::float4)").await
            == vec![text_row(&["\\x00800000"])]
    );

    // The I/O pair and the casts depend on the type. Dropping it without
    // CASCADE is 2BP01, and with CASCADE it names every one of them, in
    // creation order.
    assert!(sqlstate_of(&mut session, "DROP TYPE xfloat4").await == "2BP01");
    run_s(&mut session, "DROP TYPE xfloat4 CASCADE").await;
    let notice = notices.try_recv().expect("cascade notice");
    assert!(notice.message == "drop cascades to 4 other objects");
    assert!(
        notice
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.detail.as_deref())
            == Some(
                "drop cascades to function xfloat4in(cstring)\n\
                     drop cascades to function xfloat4out(xfloat4)\n\
                     drop cascades to cast from xfloat4 to real\n\
                     drop cascades to cast from integer to xfloat4"
            )
    );
    // Nothing is left behind: the name is free and the casts are gone.
    run_s(&mut session, "CREATE TYPE xfloat4").await;
    assert!(sqlstate_of(&mut session, "DROP CAST (xfloat4 AS float4)").await == "42704");
}

/// A schema owns every direct member, while an attached partition is an
/// internal dependent. The cascade notice follows the members' shared create
/// order even though tables, views, sequences, and types use separate catalog
/// families.
#[tokio::test]
async fn dropping_a_schema_reports_direct_members_in_creation_order() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE SCHEMA cascade_order",
        "CREATE TYPE cascade_order.ty AS (i int)",
        "CREATE TABLE cascade_order.t (i int)",
        "CREATE TABLE cascade_order.parent (i int) PARTITION BY RANGE (i)",
        "CREATE TABLE cascade_order.child PARTITION OF cascade_order.parent FOR VALUES FROM (0) TO (10)",
        "CREATE SEQUENCE cascade_order.s",
        "CREATE VIEW cascade_order.v AS SELECT * FROM cascade_order.t",
        "CREATE TABLE cascade_order.late (i int)",
    ] {
        run_s(&mut session, sql).await;
    }

    let mut notices = session.take_notices().expect("notice receiver");
    run_s(&mut session, "DROP SCHEMA cascade_order CASCADE").await;
    let notice = notices.try_recv().expect("cascade notice");
    assert!(notice.message == "drop cascades to 6 other objects");
    assert!(
        notice
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.detail.as_deref())
            == Some(
                "drop cascades to type cascade_order.ty\n\
                 drop cascades to table cascade_order.t\n\
                 drop cascades to table cascade_order.parent\n\
                 drop cascades to sequence cascade_order.s\n\
                 drop cascades to view cascade_order.v\n\
                 drop cascades to table cascade_order.late"
            )
    );
}

#[tokio::test]
async fn dropping_a_schema_prints_a_visible_type_without_qualification() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE SCHEMA cascade_type_visibility",
        "CREATE TYPE public.cascade_type_visibility_shadow AS (i int)",
        "CREATE TYPE cascade_type_visibility.ty AS (i int)",
        "SET search_path TO public, cascade_type_visibility",
    ] {
        run_s(&mut session, sql).await;
    }

    let mut notices = session.take_notices().expect("notice receiver");
    run_s(&mut session, "DROP SCHEMA cascade_type_visibility CASCADE").await;
    let notice = notices.try_recv().expect("cascade notice");
    assert!(notice.message == "drop cascades to type ty");
}

/// `CREATE CAST … WITHOUT FUNCTION` is a claim that two types are the same
/// bytes, and PostgreSQL checks it crudely rather than taking the word for
/// it. Every refusal below is one of `CreateCast`'s.
#[tokio::test]
async fn a_binary_cast_is_refused_when_the_types_are_not_the_same_bytes() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TYPE colour AS ENUM ('red', 'green')").await;
    run_s(
        &mut session,
        "CREATE DOMAIN small AS int4 CHECK (VALUE < 10)",
    )
    .await;
    run_s(&mut session, "CREATE TYPE pair AS (a int4, b int4)").await;

    for (sql, message) in [
        (
            "CREATE CAST (int4 AS int8) WITHOUT FUNCTION",
            "source and target data types are not physically compatible",
        ),
        (
            "CREATE CAST (int4 AS text) WITHOUT FUNCTION",
            "source and target data types are not physically compatible",
        ),
        (
            "CREATE CAST (colour AS int4) WITHOUT FUNCTION",
            "enum data types are not binary-compatible",
        ),
        (
            "CREATE CAST (small AS int4) WITHOUT FUNCTION",
            "domain data types must not be marked binary-compatible",
        ),
        // A composite is varlena, so PostgreSQL's physical check catches it
        // before the composite check ever runs.
        (
            "CREATE CAST (pair AS int4) WITHOUT FUNCTION",
            "source and target data types are not physically compatible",
        ),
        (
            "CREATE CAST (int4 AS int4) WITHOUT FUNCTION",
            "source data type and target data type are the same",
        ),
    ] {
        let (state, reported) = error_of(&mut session, sql).await;
        assert!(state == "42P17", "{sql}");
        assert!(reported == message, "{sql}");
    }

    // A recorded cast is unique on its type pair, and a second one is 42710.
    run_s(&mut session, "CREATE CAST (int4 AS date) WITHOUT FUNCTION").await;
    assert!(
        sqlstate_of(&mut session, "CREATE CAST (int4 AS date) WITHOUT FUNCTION").await == "42710"
    );
    // Dropping it takes the conversion away again.
    run_s(&mut session, "DROP CAST (int4 AS date)").await;
    assert!(sqlstate_of(&mut session, "DROP CAST (int4 AS date)").await == "42704");
    run_s(&mut session, "DROP CAST IF EXISTS (int4 AS date)").await;
}

/// A function cast is only recorded after its routine resolves, so a failed
/// `CREATE CAST` never leaves an inert catalog row behind.
#[tokio::test]
async fn a_missing_cast_function_is_not_recorded() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let sql = "CREATE CAST (int4 AS text) WITH FUNCTION no_such_cast_fn(int4)";
    assert!(sqlstate_of(&mut session, sql).await == "42883");
    // Nothing was written, so the pair is still free.
    assert!(sqlstate_of(&mut session, "DROP CAST (int4 AS text)").await == "42704");
}

/// `CREATE TYPE name (…)` needs both I/O functions, and gres additionally
/// needs a representation it can name — from `LIKE`, or from a layout it
/// carries a built-in for. A layout it carries nothing for is 0A000 rather
/// than a type that exists and cannot hold anything.
#[tokio::test]
async fn a_base_type_needs_its_io_pair_and_a_representation() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TYPE t").await;
    run_s(
        &mut session,
        "CREATE FUNCTION t_in(cstring) RETURNS t LANGUAGE internal AS 'int4in'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE FUNCTION t_out(t) RETURNS cstring LANGUAGE internal AS 'int4out'",
    )
    .await;

    let (state, message) = error_of(&mut session, "CREATE TYPE t (output = t_out)").await;
    assert!(state == "42P17");
    assert!(message == "type input function must be specified");
    let (state, message) = error_of(&mut session, "CREATE TYPE t (input = t_in)").await;
    assert!(state == "42P17");
    assert!(message == "type output function must be specified");
    // `widget`'s 24 bytes and `city_budget`'s 16 name a layout gres carries
    // no built-in for, and stay refused.
    assert!(
        sqlstate_of(
            &mut session,
            "CREATE TYPE t (input = t_in, output = t_out, internallength = 24, \
                 alignment = double)"
        )
        .await
            == "0A000"
    );
    // `LIKE` and the layout triple are two spellings of one thing, and gres
    // has no `typbyval`/`typalign` of its own to apply one over the other.
    assert!(
        sqlstate_of(
            &mut session,
            "CREATE TYPE t (input = t_in, output = t_out, like = float4, alignment = int4)"
        )
        .await
            == "0A000"
    );
    // A malformed layout is 22023, as `defGetTypeLength` and `DefineType`
    // have it, and not the 0A000 of one gres merely cannot carry.
    let (state, message) = error_of(
        &mut session,
        "CREATE TYPE t (input = t_in, output = t_out, alignment = quadruple)",
    )
    .await;
    assert!(state == "22023");
    assert!(message == "alignment \"quadruple\" not recognized");
    assert!(
        sqlstate_of(
            &mut session,
            "CREATE TYPE t (input = t_in, output = t_out, internallength = 0)"
        )
        .await
            == "22023"
    );
    // A layout `TypeCreate` itself rejects is 42P17 with PostgreSQL's own
    // message, which keeps it apart from the 0A000 that says only that gres
    // has no carrier.
    let (state, message) = error_of(
        &mut session,
        "CREATE TYPE t (input = t_in, output = t_out, internallength = 3, passedbyvalue)",
    )
    .await;
    assert!(state == "42P17");
    assert!(message == "internal size 3 is invalid for passed-by-value type");
    let (state, message) = error_of(
        &mut session,
        "CREATE TYPE t (input = t_in, output = t_out, internallength = 4, passedbyvalue, \
             alignment = double)",
    )
    .await;
    assert!(state == "42P17");
    assert!(message == "alignment \"d\" is invalid for passed-by-value type of size 4");
    let (state, message) = error_of(
        &mut session,
        "CREATE TYPE t (input = t_in, output = t_out, internallength = variable, \
             alignment = char)",
    )
    .await;
    assert!(state == "42P17");
    assert!(message == "alignment \"c\" is invalid for variable-length type");
    // An I/O function that does not exist is 42883.
    assert!(
        sqlstate_of(
            &mut session,
            "CREATE TYPE t (input = nosuch_in, output = t_out, like = float4)"
        )
        .await
            == "42883"
    );
    // `DefineType`'s own defaults are variable length, by reference, int
    // alignment — a varlena, which gres carries as `text`.
    run_s(&mut session, "CREATE TYPE t (input = t_in, output = t_out)").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT typlen, typtype, typstorage FROM pg_type WHERE typname = 't'"
        )
        .await
            == vec![text_row(&["-1", "b", "x"])]
    );
}

/// The two C-backed fixed-width regression types retain their PostgreSQL
/// layouts even though their Rust adapters store the values as text.
#[tokio::test]
async fn regression_c_base_types_keep_their_declared_layouts() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE FUNCTION widget_in(cstring) RETURNS widget AS 'regress' LANGUAGE C STRICT",
        "CREATE FUNCTION widget_out(widget) RETURNS cstring AS 'regress' LANGUAGE C STRICT",
        "CREATE FUNCTION pt_in_widget(point, widget) RETURNS bool AS 'regress' LANGUAGE C STRICT",
        "CREATE FUNCTION int44in(cstring) RETURNS city_budget AS 'regress' LANGUAGE C STRICT",
        "CREATE FUNCTION int44out(city_budget) RETURNS cstring AS 'regress' LANGUAGE C STRICT",
        "CREATE TYPE widget (internallength = 24, input = widget_in, output = widget_out, alignment = double)",
        "CREATE TYPE city_budget (internallength = 16, input = int44in, output = int44out)",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        text_rows_of(
            &mut session,
            "SELECT typname, typlen, typbyval, typalign FROM pg_type \
             WHERE typname IN ('city_budget', 'widget') ORDER BY typname",
        )
        .await
            == vec![
                text_row(&["city_budget", "16", "f", "i"]),
                text_row(&["widget", "24", "f", "d"]),
            ]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT typname, typalign FROM pg_type \
             WHERE typname IN ('char', 'int2', 'int4', 'int8') ORDER BY typname",
        )
        .await
            == vec![
                text_row(&["char", "c"]),
                text_row(&["int2", "s"]),
                text_row(&["int4", "i"]),
                text_row(&["int8", "d"]),
            ]
    );
    run_s(&mut session, "CREATE TABLE widget_values (value widget)").await;
    run_s(
        &mut session,
        "INSERT INTO widget_values VALUES ('(1,2,3)'), ('(-44,5.5,12)'), ('(1.0,2.00,3.000)')",
    )
    .await;
    assert!(
        text_rows_of(&mut session, "SELECT value FROM widget_values").await
            == vec![
                text_row(&["(1,2,3)"]),
                text_row(&["(-44,5.5,12)"]),
                text_row(&["(1,2,3)"]),
            ]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT pt_in_widget('(2,2)'::point, '(1,1,1.5)'::widget)",
        )
        .await
            == vec![text_row(&["t"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT attlen, attbyval, attalign FROM pg_attribute \
             WHERE attrelid = 'widget_values'::regclass AND attname = 'value'",
        )
        .await
            == vec![text_row(&["24", "f", "d"])]
    );
    run_s(&mut session, "CREATE TABLE builtin_layout (value int)").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT attlen, attbyval, attalign FROM pg_attribute \
             WHERE attrelid = 'builtin_layout'::regclass AND attname = 'value'",
        )
        .await
            == vec![text_row(&["4", "t", "i"])]
    );
    let widget = crabka_pgtypes::usertype::lookup("widget")
        .and_then(|definition| definition.column_type())
        .expect("widget type");
    assert!(super::result_types::field("value", widget).type_size == 24);
    run_s(&mut session, "CREATE TABLE city (budget city_budget)").await;
    run_s(
        &mut session,
        "INSERT INTO city VALUES ('100,127,1000'), ('123456,127,-1000,6789')",
    )
    .await;
    assert!(
        text_rows_of(&mut session, "SELECT budget FROM city ORDER BY budget").await
            == vec![
                text_row(&["100,127,1000,0"]),
                text_row(&["123456,127,-1000,6789"]),
            ]
    );
}

/// `INTERNALLENGTH`/`PASSEDBYVALUE`/`ALIGNMENT` describe the same layout
/// `LIKE` copies, so a type built either way is the same type: it carries
/// its values in the built-in of that layout, and a `WITHOUT FUNCTION` cast
/// to that built-in is the pass-through `pg_cast` says it is.
#[tokio::test]
async fn a_base_type_takes_its_layout_written_out() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TYPE vt").await;
    run_s(
        &mut session,
        "CREATE FUNCTION vt_in(cstring) RETURNS vt LANGUAGE internal AS 'textin'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE FUNCTION vt_out(vt) RETURNS cstring LANGUAGE internal AS 'textout'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE FUNCTION vt_mod_in(cstring) RETURNS int4 LANGUAGE internal AS 'numerictypmodin'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE FUNCTION vt_mod_out(int4) RETURNS cstring LANGUAGE internal AS 'numerictypmodout'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TYPE vt (internallength = variable, input = vt_in, output = vt_out, \
             typmod_in = vt_mod_in, typmod_out = vt_mod_out, element = int4, alignment = int4, \
             default = 'zippo', storage = main)",
    )
    .await;
    // A varlena pair needs no reinterpretation, so the cast is recorded and
    // the value passes through unchanged.
    run_s(&mut session, "CREATE CAST (text AS vt) WITHOUT FUNCTION").await;
    assert!(text_rows_of(&mut session, "SELECT 'foo'::text::vt").await == vec![text_row(&["foo"])]);

    run_s(&mut session, "CREATE TYPE ft").await;
    run_s(
        &mut session,
        "CREATE FUNCTION ft_in(cstring) RETURNS ft LANGUAGE internal AS 'int4in'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE FUNCTION ft_out(ft) RETURNS cstring LANGUAGE internal AS 'int4out'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TYPE ft (internallength = 4, input = ft_in, output = ft_out, \
             alignment = int4, default = 42, passedbyvalue)",
    )
    .await;
    run_s(&mut session, "CREATE CAST (int4 AS ft) WITHOUT FUNCTION").await;
    assert!(text_rows_of(&mut session, "SELECT 42::int4::ft").await == vec![text_row(&["42"])]);
    // `typlen` is the layout's own, not the carrier's by coincidence: the
    // 4-byte type reports 4 and the varlena reports -1.
    assert!(
        text_rows_of(
            &mut session,
            "SELECT typname, typlen, typtype, typstorage, typelem FROM pg_type \
                 WHERE typname IN ('vt', 'ft') ORDER BY typname"
        )
        .await
            == vec![
                text_row(&["ft", "4", "b", "p", "0"]),
                text_row(&["vt", "-1", "b", "m", "23"]),
            ]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT typinput, typoutput, typmodin, typmodout, typdefault FROM pg_type WHERE typname = 'vt'"
        )
        .await
            == vec![text_row(&[
                "vt_in",
                "vt_out",
                "vt_mod_in",
                "vt_mod_out",
                "'zippo'",
            ])]
    );
    run_s(&mut session, "CREATE TABLE base_defaults (v vt, f ft)").await;
    run_s(&mut session, "INSERT INTO base_defaults DEFAULT VALUES").await;
    assert!(
        text_rows_of(&mut session, "SELECT v, f FROM base_defaults").await
            == vec![text_row(&["zippo", "42"])]
    );
}

/// Opening a second engine over an existing catalog republishes durable
/// casts after the first process registry has gone away.
#[tokio::test]
async fn reopening_a_catalog_hydrates_its_user_casts() {
    use crabka_pgkv::{Kv, MemKv};

    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TYPE cast_hydration_probe").await;
    run_s(
        &mut session,
        "CREATE FUNCTION cast_hydration_probe_in(cstring) RETURNS cast_hydration_probe \
             LANGUAGE internal AS 'textin'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE FUNCTION cast_hydration_probe_out(cast_hydration_probe) RETURNS cstring \
             LANGUAGE internal AS 'textout'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TYPE cast_hydration_probe (internallength = variable, \
             input = cast_hydration_probe_in, output = cast_hydration_probe_out, \
             alignment = int4)",
    )
    .await;
    run_s(
        &mut session,
        "CREATE CAST (text AS cast_hydration_probe) WITHOUT FUNCTION",
    )
    .await;

    let durable = crabka_pgcatalog::list_user_casts(kv.as_ref()).expect("durable cast");
    let removed = durable
        .iter()
        .map(|cast| crabka_pgtypes::usercast::DeclaredCast {
            source: cast.source,
            target: cast.target,
            method: crabka_pgtypes::usercast::CastMethod::Binary,
        })
        .collect::<Vec<_>>();
    crabka_pgtypes::usercast::publish_catalog_delta(&removed, &[]);

    let reopened = SqlEngine::with_kv(kv).expect("reopened engine");
    let mut reopened_session = reopened.connect();
    assert2::assert!(
        text_rows_of(
            &mut reopened_session,
            "SELECT 'persisted'::text::cast_hydration_probe",
        )
        .await
            == vec![text_row(&["persisted"])]
    );
}

#[tokio::test]
async fn reopening_a_catalog_hydrates_a_domain_over_a_relation_rowtype() {
    use crabka_pgkv::{Kv, MemKv};

    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE durable_rowtype_source (id int4)",
    )
    .await;
    run_s(
        &mut session,
        "CREATE DOMAIN durable_rowtype_domain AS durable_rowtype_source",
    )
    .await;

    crabka_pgtypes::usertype::unregister_in("public", "durable_rowtype_domain");
    crabka_pgtypes::usertype::unregister_in("public", "durable_rowtype_source");

    let reopened = SqlEngine::with_kv(kv).expect("reopened engine");
    let mut reopened_session = reopened.connect();
    run_s(
        &mut reopened_session,
        "CREATE TABLE durable_rowtype_consumer (value durable_rowtype_domain)",
    )
    .await;
}

/// `CREATE CAST … WITH INOUT` is `typoutput` then `typinput`: the source's
/// text form, read as the target. It needs no shared layout, which is what
/// separates it from `WITHOUT FUNCTION` — `integer` and a varlena base type
/// have nothing physical in common and convert here anyway.
#[tokio::test]
async fn an_inout_cast_converts_through_the_text_form() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TYPE vt").await;
    run_s(
        &mut session,
        "CREATE FUNCTION vt_in(cstring) RETURNS vt LANGUAGE internal AS 'textin'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE FUNCTION vt_out(vt) RETURNS cstring LANGUAGE internal AS 'textout'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TYPE vt (internallength = variable, input = vt_in, output = vt_out, \
             alignment = int4)",
    )
    .await;
    // A binary cast over this pair is refused for the width mismatch, and
    // an I/O one is recorded for the same pair.
    assert!(
        sqlstate_of(&mut session, "CREATE CAST (int4 AS vt) WITHOUT FUNCTION").await == "42P17"
    );
    run_s(&mut session, "CREATE CAST (int4 AS vt) WITH INOUT").await;
    assert!(text_rows_of(&mut session, "SELECT 1234::int4::vt").await == vec![text_row(&["1234"])]);
    assert!(
        text_rows_of(&mut session, "SELECT (NULL::int4::vt) IS NULL").await
            == vec![text_row(&["t"])]
    );
    // A second cast over the same pair is 42710 whatever its method.
    assert!(sqlstate_of(&mut session, "CREATE CAST (int4 AS vt) WITH INOUT").await == "42710");
    run_s(&mut session, "DROP CAST (int4 AS vt)").await;
    assert!(sqlstate_of(&mut session, "SELECT 1234::int4::vt").await == "42846");

    // The target's own text input is what reads the form, so a value the
    // target cannot parse is an error and not some other value.
    run_s(&mut session, "CREATE TYPE it").await;
    run_s(
        &mut session,
        "CREATE FUNCTION it_in(cstring) RETURNS it LANGUAGE internal AS 'int4in'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE FUNCTION it_out(it) RETURNS cstring LANGUAGE internal AS 'int4out'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TYPE it (internallength = 4, input = it_in, output = it_out, \
             alignment = int4, passedbyvalue)",
    )
    .await;
    run_s(
        &mut session,
        "CREATE CAST (text AS it) WITH INOUT AS IMPLICIT",
    )
    .await;
    assert!(text_rows_of(&mut session, "SELECT '42'::text::it").await == vec![text_row(&["42"])]);
    assert!(sqlstate_of(&mut session, "SELECT 'abc'::text::it").await == "22P02");
    run_s(
        &mut session,
        "CREATE FUNCTION int_to_it(int4) RETURNS it LANGUAGE sql AS \
         'SELECT NULL::it'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE CAST (int4 AS it) WITH FUNCTION int_to_it(int4) AS IMPLICIT",
    )
    .await;
    assert!(
        text_rows_of(&mut session, "SELECT (42::int4::it) IS NULL").await == vec![text_row(&["t"])]
    );
    run_s(
        &mut session,
        "CREATE FUNCTION int8_to_text(int8) RETURNS text LANGUAGE sql AS \
         'SELECT $1::text'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE CAST (int8 AS it) WITH FUNCTION int8_to_text(int8) AS IMPLICIT",
    )
    .await;
    assert!(text_rows_of(&mut session, "SELECT 42::int8::it").await == vec![text_row(&["42"])]);
    assert!(
        text_rows_of(
            &mut session,
            "SELECT castfunc <> 0 FROM pg_cast \
             WHERE castsource = 'int4'::regtype AND casttarget = 'it'::regtype"
        )
        .await
            == vec![text_row(&["t"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT deptype FROM pg_depend WHERE classid = 2605 AND refclassid = 1255 \
             AND objid = (SELECT oid FROM pg_cast WHERE castsource = 'int4'::regtype \
                          AND casttarget = 'it'::regtype)",
        )
        .await
            == vec![text_row(&["n"])]
    );
    assert!(sqlstate_of(&mut session, "DROP FUNCTION int_to_it(int4)").await == "2BP01");
    run_s(&mut session, "DROP FUNCTION int_to_it(int4) CASCADE").await;
    assert!(sqlstate_of(&mut session, "SELECT 42::int4::it").await == "42846");
    assert!(text_rows_of(&mut session, "SELECT 42::int8::it").await == vec![text_row(&["42"])]);
}

/// `COLLATE` is a postfix operator on the collated types. Every collation
/// this engine has orders text by byte value, so a supported one is a no-op;
/// an unsupported name is 42704 and a non-collatable operand is 42804, both
/// as in `PostgreSQL`.
#[tokio::test]
async fn collate_is_typed_like_postgresql() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4, b text)").await;
    run_s(&mut session, "INSERT INTO t VALUES (2, 'b'), (1, 'a')").await;

    assert!(
        text_rows_of(&mut session, "SELECT b FROM t ORDER BY b COLLATE \"C\"").await
            == vec![text_row(&["a"]), text_row(&["b"])]
    );
    assert!(
        text_rows_of(&mut session, "SELECT b COLLATE \"POSIX\" FROM t ORDER BY 1").await
            == vec![text_row(&["a"]), text_row(&["b"])]
    );
    assert!(
        text_rows_of(&mut session, "SELECT b FROM t WHERE b COLLATE \"C\" = 'a'").await
            == vec![text_row(&["a"])]
    );

    for sql in [
        "SELECT a COLLATE \"C\" FROM t",
        "SELECT a FROM t ORDER BY a COLLATE \"C\"",
    ] {
        assert!(sqlstate_of(&mut session, sql).await == "42804", "{sql}");
    }
    for sql in [
        "SELECT b COLLATE \"en_US\" FROM t",
        "SELECT b COLLATE c FROM t",
    ] {
        assert!(sqlstate_of(&mut session, sql).await == "42704", "{sql}");
    }
}

/// The `\d` Collation column: psql prints a column's collation exactly when
/// `pg_attribute.attcollation` differs from the type's `typcollation`, so
/// this asks the catalog the same question psql does.
/// `attrelid` is a SQL expression for the relation's oid rather than a name,
/// so a composite type — whose attributes hang off `pg_type.typrelid` — can
/// be asked the same question a table can.
async fn collation_shown_by_backslash_d(
    session: &mut SqlSession,
    attrelid: &str,
) -> Vec<Vec<Option<String>>> {
    text_rows_of(
        session,
        &format!(
            "SELECT a.attname, (SELECT c.collname FROM pg_collation c, pg_type t \
                 WHERE c.oid = a.attcollation AND t.oid = a.atttypid \
                 AND a.attcollation <> t.typcollation) \
                 FROM pg_attribute a WHERE a.attrelid = ({attrelid}) \
                 AND a.attnum > 0 ORDER BY a.attnum"
        ),
    )
    .await
}

/// A column-level `COLLATE` is accepted, recorded and reported. Every
/// collation this engine has orders text by byte value, so the clause never
/// changes how rows compare — what it changes is what `pg_attribute` and so
/// `\d` say about the column. The type rule (42804) and the unknown-name
/// rule (42704) are the same ones the postfix operator applies.
#[tokio::test]
async fn a_column_collate_clause_is_recorded_and_reported() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE DOMAIN dtext AS text").await;
    run_s(
        &mut session,
        "CREATE TABLE t (id int4, plain text, c text COLLATE \"C\", \
             p varchar(8) COLLATE \"POSIX\", d text COLLATE \"default\", \
             dom dtext COLLATE \"C\", arr text[] COLLATE \"C\")",
    )
    .await;

    // PostgreSQL prints nothing for a column with no clause and nothing for
    // one that wrote the database default; it prints the name for the rest.
    assert!(
        collation_shown_by_backslash_d(&mut session, "'t'::regclass").await
            == vec![
                vec![Some("id".into()), None],
                vec![Some("plain".into()), None],
                text_row(&["c", "C"]),
                text_row(&["p", "POSIX"]),
                vec![Some("d".into()), None],
                text_row(&["dom", "C"]),
                text_row(&["arr", "C"]),
            ]
    );

    // The clause is semantically a no-op: the column stores, compares and
    // orders exactly as an uncollated one does.
    run_s(
        &mut session,
        "INSERT INTO t (id, c) VALUES (2, 'b'), (1, 'a')",
    )
    .await;
    assert!(
        text_rows_of(&mut session, "SELECT c FROM t ORDER BY c").await
            == vec![text_row(&["a"]), text_row(&["b"])]
    );
    assert!(
        text_rows_of(&mut session, "SELECT c FROM t WHERE c = 'a'").await == vec![text_row(&["a"])]
    );

    // `LIKE` copies the whole column, collation included.
    run_s(&mut session, "CREATE TABLE copied (LIKE t)").await;
    assert!(
        collation_shown_by_backslash_d(&mut session, "'copied'::regclass").await
            == collation_shown_by_backslash_d(&mut session, "'t'::regclass").await
    );

    // `ADD COLUMN` takes the clause; a retype names the collation the column
    // keeps, and omitting it resets the column to the type's own.
    run_s(
        &mut session,
        "ALTER TABLE t ADD COLUMN added text COLLATE \"POSIX\"",
    )
    .await;
    run_s(
        &mut session,
        "ALTER TABLE t ALTER COLUMN plain TYPE text COLLATE \"C\"",
    )
    .await;
    run_s(&mut session, "ALTER TABLE t ALTER COLUMN c TYPE text").await;
    let after = collation_shown_by_backslash_d(&mut session, "'t'::regclass").await;
    assert!(after[1] == text_row(&["plain", "C"]));
    assert!(after[2] == vec![Some("c".into()), None]);
    assert!(after[7] == text_row(&["added", "POSIX"]));

    // A collation is only meaningful on a collatable type, and only a
    // collation `pg_collation` holds can be named — in every place a column
    // is declared.
    let refusals: &[(&str, &str)] = &[
        ("CREATE TABLE bad (a int4 COLLATE \"C\")", "42804"),
        ("CREATE TABLE bad (a int4[] COLLATE \"C\")", "42804"),
        ("ALTER TABLE t ADD COLUMN bad int4 COLLATE \"C\"", "42804"),
        (
            "ALTER TABLE t ALTER COLUMN id TYPE int4 COLLATE \"C\"",
            "42804",
        ),
        ("CREATE TABLE bad (a text COLLATE \"en_US\")", "42704"),
        (
            "CREATE TABLE bad (a text COLLATE \"C\" COLLATE \"POSIX\")",
            "42601",
        ),
    ];
    for (sql, expected) in refusals {
        assert!(sqlstate_of(&mut session, sql).await == *expected, "{sql}");
    }
    // A refused statement created nothing.
    assert!(sqlstate_of(&mut session, "SELECT * FROM bad").await == "42P01");
    // The same type rule guards a domain's own clause, which the parser used
    // to consume and throw away.
    assert!(
        sqlstate_of(&mut session, "CREATE DOMAIN di AS int4 COLLATE \"POSIX\"").await == "42804"
    );
    run_s(&mut session, "CREATE DOMAIN dp AS text COLLATE \"POSIX\"").await;
}

/// A partition has to declare its columns exactly as its parent does, and
/// the collation is part of that declaration — PostgreSQL compares the two
/// written collations rather than what they do, so a `POSIX` child cannot
/// join a `C` parent even though both order text by byte value here.
#[tokio::test]
async fn a_partition_must_declare_the_parents_collation() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE lp (a int4, b text COLLATE \"C\") PARTITION BY LIST (a)",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TABLE lp_bad (a int4, b text COLLATE \"POSIX\")",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TABLE lp_ok (a int4, b text COLLATE \"C\")",
    )
    .await;

    assert!(
        sqlstate_of(
            &mut session,
            "ALTER TABLE lp ATTACH PARTITION lp_bad FOR VALUES IN (1)",
        )
        .await
            == "42P21"
    );
    run_s(
        &mut session,
        "ALTER TABLE lp ATTACH PARTITION lp_ok FOR VALUES IN (2)",
    )
    .await;

    // A partition declared through `PARTITION OF` may write the clause, and
    // PostgreSQL parses it and then ignores it: the column keeps what the
    // parent declared.
    run_s(
        &mut session,
        "CREATE TABLE lp_of PARTITION OF lp (b COLLATE \"POSIX\") FOR VALUES IN (3)",
    )
    .await;
    assert!(
        collation_shown_by_backslash_d(&mut session, "'lp_of'::regclass").await
            == collation_shown_by_backslash_d(&mut session, "'lp'::regclass").await
    );
}

/// `pg_get_viewdef` has to write a collated expression back, not swallow it.
/// The deparser's catch-all renders an unknown node as the literal
/// `?column?`, which turns `WHERE (b COLLATE "C") >= 'bbc'` into a body that
/// no longer says what the view does.
#[tokio::test]
async fn a_view_body_keeps_the_collate_it_was_written_with() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4, b text, x text)").await;
    run_s(
        &mut session,
        "CREATE VIEW v1 AS SELECT a, b FROM t WHERE b COLLATE \"C\" >= 'bbc'",
    )
    .await;
    run_s(
        &mut session,
        "CREATE VIEW v2 AS SELECT a, lower((x || x) COLLATE \"POSIX\") FROM t",
    )
    .await;

    let bodies = text_rows_of(
        &mut session,
        "SELECT table_name, view_definition FROM information_schema.views \
             WHERE table_name LIKE 'v%' ORDER BY 1",
    )
    .await;
    let body = |row: &Vec<Option<String>>| row[1].clone().expect("a definition");
    assert!(
        body(&bodies[0]).contains("WHERE ((b COLLATE \"C\") >= 'bbc'::text)"),
        "{}",
        body(&bodies[0])
    );
    assert!(
        body(&bodies[1]).contains("lower(((x || x) COLLATE \"POSIX\"))"),
        "{}",
        body(&bodies[1])
    );
}

/// `pg_attribute` is built by one function for every kind of relation that
/// has columns, and `attcollation` is the field a column-level `COLLATE`
/// changed. None of these relations can carry one, so every one of them has
/// to keep reporting the collation its column's *type* implies — the
/// database default for the string types and 0 for everything else — or
/// `\d` starts printing a Collation column for relations that have none.
#[tokio::test]
async fn attcollation_follows_the_type_for_every_relation_without_a_collate() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE base (id int4, label text)").await;
    run_s(&mut session, "CREATE VIEW v AS SELECT id, label FROM base").await;
    run_s(&mut session, "CREATE TYPE pair AS (n int4, s text)").await;
    run_s(
        &mut session,
        "CREATE MATERIALIZED VIEW m AS SELECT id, label FROM base",
    )
    .await;
    run_s(
        &mut session,
        "CREATE TABLE ctas AS SELECT id, label FROM base",
    )
    .await;

    // A table, a view, a materialized view, a CREATE TABLE AS, a composite
    // type and a catalog relation of the engine's own — every one reports
    // the type's collation and nothing else, so `\d` prints no Collation.
    let relations = [
        "'base'::regclass",
        "'v'::regclass",
        "'m'::regclass",
        "'ctas'::regclass",
        "SELECT typrelid FROM pg_type WHERE typname = 'pair'",
        "'pg_class'::regclass",
    ];
    for relation in relations {
        let printed = collation_shown_by_backslash_d(&mut session, relation).await;
        assert!(!printed.is_empty(), "{relation} has no attributes");
        assert!(
            printed.iter().all(|row| row[1].is_none()),
            "{relation} prints a collation: {printed:?}"
        );
    }

    // And the underlying value is the database default for a text column,
    // not 0 and not a named collation.
    assert!(
        text_rows_of(
            &mut session,
            "SELECT attname, attcollation FROM pg_attribute \
                 WHERE attrelid = 'base'::regclass AND attnum > 0 ORDER BY attnum",
        )
        .await
            == vec![text_row(&["id", "0"]), text_row(&["label", "100"])]
    );
}

/// A `CHECK` constraint is persisted and enforced on INSERT, UPDATE and
/// COPY, with PostgreSQL's SQLSTATE and its three-valued NULL rule.
#[tokio::test]
async fn check_constraints_are_enforced_on_every_write_path() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE t (a int4, b int4 CHECK (b > 0), CONSTRAINT ck CHECK (a + b < 100))",
    )
    .await;

    let cases: &[(&str, &str)] = &[
        ("INSERT INTO t VALUES (1, -1)", "23514"),
        ("INSERT INTO t VALUES (60, 60)", "23514"),
    ];
    for (sql, expected) in cases {
        assert!(sqlstate_of(&mut session, sql).await == *expected, "{sql}");
    }

    // A NULL predicate is not false, so the row is accepted.
    run_s(&mut session, "INSERT INTO t VALUES (1, NULL)").await;
    run_s(&mut session, "INSERT INTO t VALUES (2, 3)").await;
    assert!(
        text_rows_of(&mut session, "SELECT a, b FROM t ORDER BY a").await
            == vec![vec![Some("1".into()), None], text_row(&["2", "3"]),]
    );

    assert!(sqlstate_of(&mut session, "UPDATE t SET b = -5 WHERE a = 2").await == "23514");
    assert!(
        text_rows_of(&mut session, "SELECT a, b FROM t WHERE a = 2").await
            == vec![text_row(&["2", "3"])]
    );
}

/// PostgreSQL's default `CHECK` names: `<table>_<column>_check` when the
/// predicate references exactly one column, `<table>_check` otherwise, and
/// a numeric suffix on collision.
#[tokio::test]
async fn unnamed_check_constraints_take_postgresql_default_names() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
            &mut session,
            "CREATE TABLE t (a int4 CHECK (a > 0), b int4 CHECK (b > 0), CHECK (a < b), CHECK (a <> 5))",
        )
        .await;
    let table = crabka_pgcatalog::get_table(engine.catalog_kv(), &RelationName::public("t"))
        .expect("table");
    assert!(
        table
            .checks
            .iter()
            .map(|check| check.name.clone())
            .collect::<Vec<_>>()
            == vec!["t_a_check", "t_b_check", "t_check", "t_a_check1"]
    );
}

/// `ADD COLUMN` back-fills stored rows with the new column's default and
/// `DROP COLUMN` reclaims the position, so later reads line up.
#[tokio::test]
async fn add_and_drop_column_rewrite_stored_rows() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (id int4, label text)").await;
    run_s(&mut session, "INSERT INTO t VALUES (1, 'x'), (2, 'y')").await;

    run_s(&mut session, "ALTER TABLE t ADD COLUMN n int4 DEFAULT 7").await;
    assert!(
        text_rows_of(&mut session, "SELECT id, label, n FROM t ORDER BY id").await
            == vec![text_row(&["1", "x", "7"]), text_row(&["2", "y", "7"])]
    );

    run_s(&mut session, "ALTER TABLE t DROP COLUMN label").await;
    assert!(
        text_rows_of(&mut session, "SELECT id, n FROM t ORDER BY id").await
            == vec![text_row(&["1", "7"]), text_row(&["2", "7"])]
    );
    assert!(sqlstate_of(&mut session, "SELECT label FROM t").await == "42703");
}

/// `SET NOT NULL` and `ADD CONSTRAINT … CHECK` back-validate the stored
/// rows all-or-nothing, and only live rows count.
#[tokio::test]
async fn alter_table_back_validates_against_live_rows_only() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4)").await;
    run_s(&mut session, "INSERT INTO t VALUES (1), (NULL), (-3)").await;

    assert!(
        sqlstate_of(&mut session, "ALTER TABLE t ALTER COLUMN a SET NOT NULL").await == "23502"
    );
    assert!(
        sqlstate_of(
            &mut session,
            "ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0)"
        )
        .await
            == "23514"
    );

    run_s(&mut session, "DELETE FROM t WHERE a IS NULL OR a < 0").await;
    run_s(&mut session, "ALTER TABLE t ALTER COLUMN a SET NOT NULL").await;
    run_s(
        &mut session,
        "ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0)",
    )
    .await;
    assert!(sqlstate_of(&mut session, "INSERT INTO t VALUES (0)").await == "23514");
    assert!(sqlstate_of(&mut session, "INSERT INTO t VALUES (NULL)").await == "23502");
}

/// `RENAME COLUMN` rewrites the dependencies that name the column: the
/// table's own `CHECK` predicates keep firing, and a stored view keeps
/// returning the same rows under its original output labels.
#[tokio::test]
async fn rename_column_rewrites_check_and_view_dependencies() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE t (a int4 CHECK (a > 0), b int4)",
    )
    .await;
    run_s(&mut session, "INSERT INTO t VALUES (1, 2)").await;
    run_s(
        &mut session,
        "CREATE VIEW v AS SELECT a, b FROM t WHERE a > 0",
    )
    .await;

    run_s(&mut session, "ALTER TABLE t RENAME COLUMN a TO a2").await;
    assert!(text_rows_of(&mut session, "SELECT a2, b FROM t").await == vec![text_row(&["1", "2"])]);
    // The view keeps its own output labels and still resolves.
    assert!(text_rows_of(&mut session, "SELECT a, b FROM v").await == vec![text_row(&["1", "2"])]);
    // The renamed CHECK is still enforced.
    assert!(sqlstate_of(&mut session, "INSERT INTO t VALUES (0, 1)").await == "23514");

    run_s(&mut session, "INSERT INTO t VALUES (5, 6)").await;
    assert!(
        text_rows_of(&mut session, "SELECT a, b FROM v ORDER BY a").await
            == vec![text_row(&["1", "2"]), text_row(&["5", "6"])]
    );
}

#[tokio::test]
async fn rename_column_changes_a_views_output_label_and_public_definition() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE VIEW v AS SELECT 1 AS a, 2 AS b").await;
    run_s(&mut session, "ALTER TABLE v RENAME COLUMN b TO q2").await;

    assert!(text_rows_of(&mut session, "SELECT a, q2 FROM v").await == vec![text_row(&["1", "2"])]);
    assert!(sqlstate_of(&mut session, "SELECT b FROM v").await == "42703");
    assert!(
        single_text(
            &session
                .simple_query("SELECT pg_get_viewdef('v'::regclass)")
                .await
                .expect("view definition")
        ) == " SELECT 1 AS a,\n    2 AS q2;"
    );
}

#[tokio::test]
async fn pg_get_viewdef_deparses_values_views_as_selects() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE VIEW v0 AS VALUES (1, 2); CREATE VIEW v (x) AS VALUES (1, 2)",
    )
    .await;

    assert!(
        single_text(
            &session
                .simple_query("SELECT pg_get_viewdef('v0'::regclass)")
                .await
                .expect("default-label view definition")
        ) == " VALUES (1,2);"
    );

    assert!(
        single_text(
            &session
                .simple_query("SELECT pg_get_viewdef('v'::regclass)")
                .await
                .expect("view definition")
        ) == " SELECT column1 AS x,\n    column2\n   FROM (VALUES (1,2)) \"*VALUES*\";"
    );
}

#[tokio::test]
async fn pg_get_viewdef_preserves_derived_values_source_names() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE VIEW v (x) AS SELECT * FROM (VALUES (1, 2)) z; \
         CREATE VIEW w (x) AS SELECT * FROM (VALUES (1, 2)) z(q, w)",
    )
    .await;

    assert!(
        single_text(
            &session
                .simple_query("SELECT pg_get_viewdef('v'::regclass)")
                .await
                .expect("default derived values")
        ) == " SELECT column1 AS x,\n    column2\n   FROM ( VALUES (1,2)) z;"
    );
    assert!(
        single_text(
            &session
                .simple_query("SELECT pg_get_viewdef('w'::regclass)")
                .await
                .expect("aliased derived values")
        ) == " SELECT q AS x,\n    w\n   FROM ( VALUES (1,2)) z(q, w);"
    );
}

#[tokio::test]
async fn pg_get_viewdef_distinguishes_derived_values_and_qualified_wildcards() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE VIEW plain (x) AS SELECT * FROM (SELECT 1 AS a) z; \
         CREATE VIEW qualified (x) AS SELECT z.* FROM (VALUES (1, 2)) z; \
         CREATE VIEW multi (x) AS SELECT z.* FROM (VALUES (1, 2)) z, (VALUES (3)) y",
    )
    .await;

    assert!(
        single_text(
            &session
                .simple_query("SELECT pg_get_viewdef('plain'::regclass)")
                .await
                .expect("ordinary derived select")
        ) == " SELECT a AS x\n   FROM ( SELECT 1 AS a) z;"
    );
    assert!(
        single_text(
            &session
                .simple_query("SELECT pg_get_viewdef('qualified'::regclass)")
                .await
                .expect("qualified values wildcard")
        ) == " SELECT column1 AS x,\n    column2\n   FROM ( VALUES (1,2)) z;"
    );
    assert!(
        single_text(
            &session
                .simple_query("SELECT pg_get_viewdef('multi'::regclass)")
                .await
                .expect("multi-source values wildcard")
        ) == " SELECT z.column1,\n    z.column2\n   FROM ( VALUES (1,2)) z,\n    ( VALUES (3)) y;"
    );
}

/// The rewrite is scoped by catalog resolution, not by name matching: a
/// view over a *different* relation that happens to have a column of the
/// same name is left alone, and keeps returning that relation's rows.
#[tokio::test]
async fn rename_column_leaves_a_same_named_column_of_another_relation_alone() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4)").await;
    run_s(&mut session, "CREATE TABLE u (a int4)").await;
    run_s(&mut session, "INSERT INTO t VALUES (1)").await;
    run_s(&mut session, "INSERT INTO u VALUES (9)").await;
    run_s(&mut session, "CREATE VIEW vu AS SELECT a FROM u").await;

    run_s(&mut session, "ALTER TABLE t RENAME COLUMN a TO b").await;
    assert!(text_rows_of(&mut session, "SELECT b FROM t").await == vec![text_row(&["1"])]);
    assert!(text_rows_of(&mut session, "SELECT a FROM vu").await == vec![text_row(&["9"])]);
    assert!(text_rows_of(&mut session, "SELECT a FROM u").await == vec![text_row(&["9"])]);
}

/// Identity and generated columns compute their values on every write, and
/// a generated column is visible to a CHECK over it.
#[tokio::test]
async fn identity_and_generated_columns_are_computed_on_write() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE t (id int4 GENERATED BY DEFAULT AS IDENTITY, a int4, \
             doubled int4 GENERATED ALWAYS AS (a * 2) STORED, CHECK (doubled < 100))",
    )
    .await;
    run_s(&mut session, "INSERT INTO t (a) VALUES (3)").await;
    run_s(&mut session, "INSERT INTO t (a) VALUES (4)").await;
    assert!(
        text_rows_of(&mut session, "SELECT id, a, doubled FROM t ORDER BY id").await
            == vec![text_row(&["1", "3", "6"]), text_row(&["2", "4", "8"])]
    );

    assert!(sqlstate_of(&mut session, "INSERT INTO t (a) VALUES (60)").await == "23514");
    run_s(&mut session, "UPDATE t SET a = 10 WHERE a = 3").await;
    assert!(
        text_rows_of(&mut session, "SELECT a, doubled FROM t ORDER BY a").await
            == vec![text_row(&["4", "8"]), text_row(&["10", "20"])]
    );
}

/// Index options whose semantics the scanner cannot honor are refused;
/// non-btree access methods remain catalog metadata while scans stay exact.
#[tokio::test]
async fn unsupported_index_options_are_refused_not_silently_built() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4, b text)").await;
    for sql in [
        "CREATE INDEX i ON t (a) WHERE a > 5",
        "CREATE INDEX i ON t (a DESC)",
        "CREATE INDEX i ON t (a NULLS FIRST)",
        "CREATE INDEX i ON t (a) INCLUDE (b)",
    ] {
        assert!(sqlstate_of(&mut session, sql).await == "0A000", "{sql}");
    }
    for method in ["hash", "gist", "spgist"] {
        run_s(
            &mut session,
            &format!("CREATE INDEX t_{method}_idx ON t USING {method} (a)"),
        )
        .await;
    }
    // The supported spellings still build, including the default name.
    run_s(&mut session, "CREATE INDEX ON t (a)").await;
    assert!(
        crabka_pgcatalog::get_index(engine.catalog_kv(), &RelationName::public("t_a_idx")).is_ok()
    );

    run_s(
        &mut session,
        "CREATE INDEX ON t USING spgist (int4range(a, a + 10))",
    )
    .await;
    run_s(&mut session, "INSERT INTO t VALUES (5, 'x'), (25, 'y')").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT count(*) FROM t WHERE int4range(a, a + 10) <@ int4range(1, 20)",
        )
        .await
            == vec![text_row(&["1"])]
    );
    let expression =
        crabka_pgcatalog::get_index(engine.catalog_kv(), &RelationName::public("t_expr_idx"))
            .expect("expression index");
    assert!(
        crabka_pgcatalog::index_key_expression(&expression.columns[0])
            == Some("int4range(a, a + 10)")
    );
    run_s(&mut session, "ALTER TABLE t RENAME COLUMN a TO n").await;
    let renamed =
        crabka_pgcatalog::get_index(engine.catalog_kv(), &RelationName::public("t_expr_idx"))
            .expect("renamed expression index");
    assert!(
        crabka_pgcatalog::index_key_expression(&renamed.columns[0]) == Some("int4range(n, n + 10)")
    );
    run_s(&mut session, "ALTER TABLE t DROP COLUMN n").await;
    assert!(
        crabka_pgcatalog::get_index(engine.catalog_kv(), &RelationName::public("t_expr_idx"))
            .is_err()
    );
}

#[tokio::test]
async fn btree_expression_indexes_are_catalog_only_and_never_probed() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4)").await;
    run_s(&mut session, "INSERT INTO t VALUES (1), (2)").await;

    run_s(&mut session, "CREATE INDEX t_expr_idx ON t ((1))").await;
    let index =
        crabka_pgcatalog::get_index(engine.catalog_kv(), &RelationName::public("t_expr_idx"))
            .expect("expression index");
    assert!(index.method == crabka_pgcatalog::IndexMethod::Btree);
    assert!(crabka_pgcatalog::index_key_expression(&index.columns[0]) == Some("(1)"));

    run_s(&mut session, "INSERT INTO t VALUES (3)").await;
    run_s(&mut session, "UPDATE t SET a = 20 WHERE a = 2").await;
    run_s(&mut session, "DELETE FROM t WHERE a = 1").await;
    assert!(
        text_rows_of(&mut session, "SELECT a FROM t WHERE a = 20").await == vec![text_row(&["20"])]
    );
    assert!(
        engine
            .kv
            .scan_prefix(&crabka_pgkv::key::secondary_index_prefix(
                index.table_id,
                index.id,
            ))
            .expect("scan expression index entries")
            .is_empty()
    );
    assert!(
        crabka_pgcatalog::get_index(engine.catalog_kv(), &RelationName::public("t_expr_idx"))
            .expect("persisted expression index")
            == index
    );
}

#[tokio::test]
async fn create_index_resolves_and_validates_operator_classes() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4, b text, data int4)").await;
    run_s(&mut session, "CREATE INDEX i1 ON t (a int4_ops)").await;
    run_s(
        &mut session,
        "CREATE INDEX i2 ON t (data pg_catalog.int4_ops)",
    )
    .await;
    assert!(
        sqlstate_of(
            &mut session,
            "CREATE INDEX i3 ON t USING hash (a int4_minmax_ops)",
        )
        .await
            == "42704"
    );
    assert!(sqlstate_of(&mut session, "CREATE INDEX i4 ON t (b int4_ops)").await == "42804");
    assert!(sqlstate_of(&mut session, "CREATE INDEX i5 ON t (b name_ops)").await == "42804");
}

#[tokio::test]
async fn notification_queue_usage_has_postgres_value_type_arity_and_volatility() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    assert!(
        text_rows_of(
            &mut session,
            "SELECT pg_notification_queue_usage(), \
                 pg_typeof(pg_notification_queue_usage())",
        )
        .await
            == vec![text_row(&["0", "double precision"])]
    );
    assert!(sqlstate_of(&mut session, "SELECT pg_notification_queue_usage(1)").await == "42883");
    run_s(&mut session, "CREATE TABLE t (a int4)").await;
    assert!(
        sqlstate_of(
            &mut session,
            "CREATE INDEX i ON t USING spgist ((pg_notification_queue_usage()))",
        )
        .await
            == "42P17"
    );
}

/// The comma form applies every subcommand or none of them.
#[tokio::test]
async fn multi_subcommand_alter_table_is_atomic() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4)").await;
    run_s(&mut session, "INSERT INTO t VALUES (1)").await;

    run_s(
        &mut session,
        "ALTER TABLE t ADD COLUMN b int4 DEFAULT 2, ADD COLUMN c text DEFAULT 'c'",
    )
    .await;
    assert!(
        text_rows_of(&mut session, "SELECT a, b, c FROM t").await
            == vec![text_row(&["1", "2", "c"])]
    );

    // The second subcommand fails, so the first must not be applied.
    assert!(
        sqlstate_of(
            &mut session,
            "ALTER TABLE t ADD COLUMN d int4, DROP COLUMN nope"
        )
        .await
            == "42703"
    );
    assert!(sqlstate_of(&mut session, "SELECT d FROM t").await == "42703");
}

fn settled_snapshot() -> crabka_pgmvcc::visibility::Snapshot {
    crabka_pgmvcc::visibility::Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    }
}

fn lookup_index_text(
    engine: &SqlEngine,
    table: &crabka_pgcatalog::Table,
    index: &crabka_pgcatalog::Index,
    value: &str,
) -> Vec<Vec<crabka_pgtypes::Datum>> {
    let snapshot = engine.procarray.snapshot();
    let gsnap = settled_snapshot();
    super::lookup_local_index_equal(
        &super::MvccReadContext {
            kv: engine.kv.as_ref(),
            global: engine.kv.as_ref(),
            global_snapshot: &gsnap,
            snapshot: &snapshot,
            own: None,
            command_id: None,
        },
        table,
        index,
        &[crabka_pgtypes::Datum::Text(value.into())],
    )
    .expect("index lookup")
    .into_iter()
    .map(|row| row.row)
    .collect()
}

#[tokio::test]
async fn local_secondary_index_lookup_tracks_insert_update_delete() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create table");
    session
        .simple_query("CREATE INDEX t_name_idx ON t (name)")
        .await
        .expect("create index");
    session
        .simple_query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a')")
        .await
        .expect("insert");

    let table = crabka_pgcatalog::get_table(engine.catalog_kv.as_ref(), &RelationName::public("t"))
        .expect("table");
    let index = crabka_pgcatalog::list_table_indexes(
        engine.catalog_kv.as_ref(),
        &RelationName::public("t"),
    )
    .expect("indexes")
    .pop()
    .expect("index");
    assert_eq!(lookup_index_text(&engine, &table, &index, "a").len(), 2);
    assert_eq!(lookup_index_text(&engine, &table, &index, "b").len(), 1);

    session
        .simple_query("UPDATE t SET name = 'a' WHERE id = 2")
        .await
        .expect("update");
    assert_eq!(lookup_index_text(&engine, &table, &index, "a").len(), 3);
    assert!(lookup_index_text(&engine, &table, &index, "b").is_empty());

    session
        .simple_query("DELETE FROM t WHERE id = 1")
        .await
        .expect("delete");
    let rows = lookup_index_text(&engine, &table, &index, "a");
    let ids: Vec<_> = rows
        .iter()
        .map(|row| row.first().expect("id"))
        .cloned()
        .collect();
    assert_eq!(
        ids,
        vec![
            crabka_pgtypes::Datum::Int4(3),
            crabka_pgtypes::Datum::Int4(2)
        ]
    );
}

#[tokio::test]
async fn drop_index_removes_catalog_metadata_and_local_entries_in_one_ddl_batch() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create table");
    session
        .simple_query("CREATE INDEX t_name_idx ON t (name)")
        .await
        .expect("create index");
    session
        .simple_query("INSERT INTO t VALUES (1, 'a')")
        .await
        .expect("insert indexed row");

    let index = crabka_pgcatalog::get_index(
        engine.catalog_kv.as_ref(),
        &RelationName::public("t_name_idx"),
    )
    .expect("index metadata");
    let entry_prefix = crabka_pgkv::key::secondary_index_prefix(index.table_id, index.id);
    assert_eq!(
        engine
            .kv
            .scan_prefix(&entry_prefix)
            .expect("scan index entries")
            .len(),
        1
    );

    session
        .simple_query("DROP INDEX t_name_idx")
        .await
        .expect("drop index");

    assert_eq!(
        crabka_pgcatalog::get_index(
            engine.catalog_kv.as_ref(),
            &RelationName::public("t_name_idx")
        )
        .expect_err("metadata removed")
        .sqlstate(),
        "42704"
    );
    assert!(
        engine
            .kv
            .scan_prefix(&entry_prefix)
            .expect("scan removed entries")
            .is_empty()
    );
}

#[tokio::test]
async fn select_uses_local_index_for_simple_equality_with_residual_filter() {
    let mut engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, name text, active bool)").await;
    run(
        &engine,
        "INSERT INTO t VALUES (1, 'a', true), (2, 'a', false), (3, 'b', true)",
    )
    .await;
    run(&engine, "CREATE INDEX t_name_idx ON t (name)").await;
    engine.set_range_scanner(Arc::new(RejectingRangeScanner));

    let result = run(
        &engine,
        "SELECT id FROM t WHERE name = 'a' AND active = true ORDER BY id",
    )
    .await;

    assert_eq!(rows_of(&result[0]).len(), 1);
    assert_eq!(text(&rows_of(&result[0])[0][0]).as_deref(), Some("1"));
}

#[tokio::test]
async fn local_index_select_ignores_stale_entries_after_update_and_delete() {
    let mut engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, name text)").await;
    run(&engine, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a')").await;
    run(&engine, "CREATE INDEX t_name_idx ON t (name)").await;
    run(&engine, "UPDATE t SET name = 'a' WHERE id = 2").await;
    run(&engine, "DELETE FROM t WHERE id = 1").await;
    engine.set_range_scanner(Arc::new(RejectingRangeScanner));

    let result = run(&engine, "SELECT id FROM t WHERE name = 'a' ORDER BY id").await;
    let ids = rows_of(&result[0])
        .iter()
        .map(|row| text(&row[0]).expect("id cell"))
        .collect::<Vec<_>>();

    assert_eq!(ids, vec!["2", "3"]);
}

#[tokio::test]
async fn unsupported_index_shape_falls_back_to_table_scan_semantics() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, name text)").await;
    run(
        &engine,
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'aa')",
    )
    .await;
    run(&engine, "CREATE INDEX t_name_idx ON t (name)").await;

    let result = run(&engine, "SELECT id FROM t WHERE id > 1 ORDER BY id").await;
    let ids = rows_of(&result[0])
        .iter()
        .map(|row| text(&row[0]).expect("id cell"))
        .collect::<Vec<_>>();

    assert_eq!(ids, vec!["2", "3"]);
}

#[tokio::test]
async fn local_secondary_index_entries_survive_durable_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text)")
            .await
            .expect("create table");
        session
            .simple_query("CREATE INDEX t_name_idx ON t (name)")
            .await
            .expect("create index");
        session
            .simple_query("INSERT INTO t VALUES (1, 'persisted')")
            .await
            .expect("insert");
    }

    let reopened = SqlEngine::open(dir.path()).expect("reopen");
    let table =
        crabka_pgcatalog::get_table(reopened.catalog_kv.as_ref(), &RelationName::public("t"))
            .expect("table");
    let index = crabka_pgcatalog::list_table_indexes(
        reopened.catalog_kv.as_ref(),
        &RelationName::public("t"),
    )
    .expect("indexes")
    .pop()
    .expect("index");
    let rows = lookup_index_text(&reopened, &table, &index, "persisted");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], crabka_pgtypes::Datum::Int4(1));
}

#[tokio::test]
async fn read_your_writes_via_own_xid_in_txn() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    // Own uncommitted insert is visible to this txn (no write-set; via xid).
    assert_eq!(
        rows_of(&run_s(&mut s, "SELECT id FROM t").await[0]).len(),
        1
    );
    s.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(
        rows_of(&run_s(&mut s, "SELECT id FROM t").await[0]).len(),
        0
    );
}

#[tokio::test]
async fn another_session_cannot_see_uncommitted_rows() {
    let engine = SqlEngine::new();
    let mut writer = engine.connect();
    writer
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    writer.simple_query("BEGIN").await.expect("begin");
    writer
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    // A concurrent session must not see the in-progress row.
    let mut reader = engine.connect();
    assert_eq!(
        rows_of(&run_s(&mut reader, "SELECT id FROM t").await[0]).len(),
        0
    );
    writer.simple_query("COMMIT").await.expect("commit");
    // After commit a fresh snapshot sees it.
    assert_eq!(
        rows_of(&run_s(&mut reader, "SELECT id FROM t").await[0]).len(),
        1
    );
}

fn rows_of(r: &QueryResult) -> &Vec<Vec<Option<Cell>>> {
    match r {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

fn fields_of(r: &QueryResult) -> &Vec<FieldDescription> {
    match r {
        QueryResult::Rows { fields, .. } => fields,
        other => panic!("expected Rows, got {other:?}"),
    }
}

fn text(cell: &Option<Cell>) -> Option<String> {
    cell.as_ref()
        .map(|c| String::from_utf8(c.text.to_vec()).expect("cell text is valid UTF-8"))
}

/// The network address types end to end: a table of them stores and reads
/// back, the operators and support functions resolve through the executor,
/// and the row description carries PostgreSQL's own OIDs.
#[tokio::test]
async fn network_address_types_round_trip_through_the_engine() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(
        &mut s,
        "CREATE TABLE net (c cidr, i inet, m macaddr, m8 macaddr8)",
    )
    .await;
    run_s(
        &mut s,
        "INSERT INTO net VALUES ('192.168.1', '192.168.1.226/24', \
             '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05'), \
             ('10:23::8000/113', '10:23::ffff', '08-00-2b-01-02-04', '0800.2b01.0203')",
    )
    .await;

    // Storage round trip: `cidr` and `inet` render differently, and a
    // six-byte `macaddr8` input widened to EUI-64 on the way in.
    assert!(
        text_rows_of(&mut s, "SELECT c, i, m, m8 FROM net ORDER BY i").await
            == vec![
                text_row(&[
                    "192.168.1.0/24",
                    "192.168.1.226/24",
                    "08:00:2b:01:02:03",
                    "08:00:2b:01:02:03:04:05",
                ]),
                text_row(&[
                    "10:23::8000/113",
                    "10:23::ffff",
                    "08:00:2b:01:02:04",
                    "08:00:2b:ff:fe:01:02:03",
                ]),
            ]
    );

    // (expression, expected text) — the support functions and the operators
    // the regression suite exercises, through the whole executor.
    let cases: &[(&str, &str)] = &[
        ("host(i)", "192.168.1.226"),
        ("text(i)", "192.168.1.226/24"),
        ("text(c)", "192.168.1.0/24"),
        ("family(i)::text", "4"),
        ("abbrev(c)", "192.168.1/24"),
        ("abbrev(i)", "192.168.1.226/24"),
        ("broadcast(i)::text", "192.168.1.255/24"),
        ("network(i)::text", "192.168.1.0/24"),
        ("masklen(c)::text", "24"),
        ("netmask(i)::text", "255.255.255.0/32"),
        ("hostmask(i)::text", "0.0.0.255/32"),
        ("set_masklen(i, 16)::text", "192.168.1.226/16"),
        ("set_masklen(c, 16)::text", "192.168.0.0/16"),
        ("inet_merge(c, i)::text", "192.168.1.0/24"),
        ("inet_same_family(c, i)::text", "true"),
        ("(i << c)::text", "false"),
        ("(i <<= c)::text", "true"),
        ("(c >>= i)::text", "true"),
        ("(c >> i)::text", "false"),
        ("(i && c)::text", "true"),
        ("(i = c)::text", "false"),
        ("(~i)::text", "63.87.254.29/24"),
        ("(i & c)::text", "192.168.1.0/24"),
        ("(i | c)::text", "192.168.1.226/24"),
        ("(i + 1)::text", "192.168.1.227/24"),
        ("(i - 1)::text", "192.168.1.225/24"),
        ("(i - c)::text", "226"),
        ("(cidr(text(c)))::text", "192.168.1.0/24"),
        ("(inet(text(i)))::text", "192.168.1.226/24"),
        ("trunc(m)::text", "08:00:2b:00:00:00"),
        ("(~m)::text", "f7:ff:d4:fe:fd:fc"),
        ("(m & '00:00:00:ff:ff:ff')::text", "00:00:00:01:02:03"),
        ("(m | '01:02:03:04:05:06')::text", "09:02:2b:05:07:07"),
        ("(m < '08:00:2b:01:02:04')::text", "true"),
        ("m::macaddr8::text", "08:00:2b:ff:fe:01:02:03"),
        ("trunc(m8)::text", "08:00:2b:00:00:00:00:00"),
        ("macaddr8_set7bit(m8)::text", "0a:00:2b:01:02:03:04:05"),
    ];
    for (expression, expected) in cases {
        let sql = format!("SELECT ({expression})::text FROM net WHERE family(i) = 4");
        assert!(
            text_rows_of(&mut s, &sql).await == vec![text_row(&[expected])],
            "{expression}"
        );
    }

    // PostgreSQL's own OIDs, so `pg_typeof`, `format_type` and `\d` agree.
    assert!(
        fields_of(&run_s(&mut s, "SELECT c, i, m, m8 FROM net").await[0])
            .iter()
            .map(|field| field.type_oid)
            .collect::<Vec<_>>()
            == vec![
                crabka_pgtypes::oids::CIDR,
                crabka_pgtypes::oids::INET,
                crabka_pgtypes::oids::MACADDR,
                crabka_pgtypes::oids::MACADDR8,
            ]
    );

    // A `cidr` with a bit set to the right of its mask is 22P02; the same
    // text is a perfectly good `inet`.
    assert!(sqlstate_of(&mut s, "SELECT '192.168.1.2/30'::cidr").await == "22P02");
    assert!(sqlstate_of(&mut s, "SELECT set_masklen(i, 33) FROM net").await == "22023");
    assert!(sqlstate_of(&mut s, "SELECT i + 10000000000 FROM net").await == "22003");
    assert!(
        sqlstate_of(
            &mut s,
            "SELECT '08:00:2b:01:02:03:04:05'::macaddr8::macaddr"
        )
        .await
            == "22003"
    );
}

#[tokio::test]
async fn select_literal_no_from() {
    let engine = SqlEngine::new();
    let r = &run(&engine, "SELECT 1 + 1 AS two").await[0];
    assert_eq!(fields_of(r)[0].name, "two");
    assert_eq!(fields_of(r)[0].type_oid, crabka_pgtypes::oids::INT4);
    assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
}

#[tokio::test]
async fn select_where_order_limit() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, name text)").await;
    run(&engine, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").await;
    let r = &run(
        &engine,
        "SELECT name FROM t WHERE id > 1 ORDER BY id DESC LIMIT 5",
    )
    .await[0];
    let rows = rows_of(r);
    assert_eq!(rows.len(), 2);
    assert_eq!(text(&rows[0][0]), Some("c".into())); // id=3 first (DESC)
    assert_eq!(text(&rows[1][0]), Some("b".into()));
}

#[tokio::test]
async fn select_star_projects_all_columns() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, name text)").await;
    run(&engine, "INSERT INTO t VALUES (7,'x')").await;
    let r = &run(&engine, "SELECT * FROM t").await[0];
    assert_eq!(
        fields_of(r)
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name"]
    );
    assert_eq!(text(&rows_of(r)[0][0]), Some("7".into()));
    assert_eq!(text(&rows_of(r)[0][1]), Some("x".into()));
}

#[tokio::test]
async fn derived_table_in_from() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, v int4)").await;
    run(&engine, "INSERT INTO t VALUES (1,10),(2,20),(3,30)").await;
    let r = &run(
        &engine,
        "SELECT d.s FROM (SELECT v + 1 AS s FROM t WHERE id > 1) d ORDER BY d.s",
    )
    .await[0];
    let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert_eq!(got, vec![Some("21".into()), Some("31".into())]);
}

#[tokio::test]
async fn join_against_a_derived_table() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, v int4)").await;
    run(&engine, "INSERT INTO t VALUES (1,10),(2,20)").await;
    let r = &run(
        &engine,
        "SELECT t.id, d.mx FROM t JOIN (SELECT max(v) AS mx FROM t) d ON t.v = d.mx",
    )
    .await[0];
    assert_eq!(rows_of(r).len(), 1);
    assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
}

#[tokio::test]
async fn inner_join_on_equi_key() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE a (id int4, av text)").await;
    run(&engine, "CREATE TABLE b (id int4, bv text)").await;
    run(&engine, "INSERT INTO a VALUES (1,'a1'),(2,'a2'),(3,'a3')").await;
    run(&engine, "INSERT INTO b VALUES (2,'b2'),(3,'b3'),(4,'b4')").await;
    let r = &run(
        &engine,
        "SELECT a.av, b.bv FROM a JOIN b ON a.id = b.id ORDER BY a.id",
    )
    .await[0];
    let got: Vec<_> = rows_of(r)
        .iter()
        .map(|row| (text(&row[0]), text(&row[1])))
        .collect();
    assert_eq!(
        got,
        vec![
            (Some("a2".into()), Some("b2".into())),
            (Some("a3".into()), Some("b3".into()))
        ]
    );
}

#[tokio::test]
async fn comma_form_is_a_cross_join_filtered_by_where() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE a (id int4)").await;
    run(&engine, "CREATE TABLE b (id int4)").await;
    run(&engine, "INSERT INTO a VALUES (1),(2)").await;
    run(&engine, "INSERT INTO b VALUES (2),(3)").await;
    let r = &run(&engine, "SELECT a.id FROM a, b WHERE a.id = b.id").await[0];
    assert_eq!(rows_of(r).len(), 1);
    assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
}

#[tokio::test]
async fn comma_equality_uses_the_bounded_indexed_join() {
    let engine = SqlEngine::new();
    let r = &run(
        &engine,
        "SELECT count(*) FROM generate_series(1, 2000) a(i), \
             generate_series(1, 2000) b(i) WHERE a.i = b.i",
    )
    .await[0];
    assert_eq!(text(&rows_of(r)[0][0]), Some("2000".into()));
}

#[tokio::test]
async fn self_join_requires_distinct_aliases() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, mgr int4)").await;
    run(&engine, "INSERT INTO t VALUES (1, NULL),(2, 1)").await;
    let r = &run(
        &engine,
        "SELECT e.id, m.id FROM t e JOIN t m ON e.mgr = m.id",
    )
    .await[0];
    // Only (employee 2 -> manager 1) matches: e.id=2, m.id=1.
    assert_eq!(rows_of(r).len(), 1);
    assert_eq!(text(&rows_of(r)[0][0]), Some("2".into()));
    assert_eq!(text(&rows_of(r)[0][1]), Some("1".into()));
}

#[tokio::test]
async fn unaliased_self_join_is_duplicate_alias_42712() {
    // The same qualifier on both sides of a join is rejected (PG 42712).
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4)").await;
    run(&engine, "INSERT INTO t VALUES (1)").await;
    let err = engine
        .connect()
        .simple_query("SELECT * FROM t JOIN t ON t.id = t.id")
        .await
        .expect_err("duplicate table name");
    assert_eq!(err.code, "42712");
}

#[tokio::test]
async fn ambiguous_bare_column_is_42702() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE a (id int4)").await;
    run(&engine, "CREATE TABLE b (id int4)").await;
    let err = engine
        .connect()
        .simple_query("SELECT id FROM a JOIN b ON a.id = b.id")
        .await
        .expect_err("ambiguous");
    assert_eq!(err.code, "42702");
}

#[tokio::test]
async fn left_join_emits_nulls_for_unmatched() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE a (id int4)").await;
    run(&engine, "CREATE TABLE b (id int4, bv text)").await;
    run(&engine, "INSERT INTO a VALUES (1),(2)").await;
    run(&engine, "INSERT INTO b VALUES (2,'two')").await;
    let r = &run(
        &engine,
        "SELECT a.id, b.bv FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id",
    )
    .await[0];
    let got: Vec<_> = rows_of(r)
        .iter()
        .map(|row| (text(&row[0]), text(&row[1])))
        .collect();
    assert_eq!(
        got,
        vec![
            (Some("1".into()), None),
            (Some("2".into()), Some("two".into())),
        ]
    );
}

#[tokio::test]
async fn using_join_merges_the_key_column() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE a (id int4, av text)").await;
    run(&engine, "CREATE TABLE b (id int4, bv text)").await;
    run(&engine, "INSERT INTO a VALUES (1,'a1'),(2,'a2')").await;
    run(&engine, "INSERT INTO b VALUES (2,'b2'),(3,'b3')").await;
    // SELECT * -> merged id first, then av, then bv.
    let r = &run(&engine, "SELECT * FROM a JOIN b USING (id)").await[0];
    assert_eq!(
        fields_of(r)
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "av", "bv"]
    );
    assert_eq!(rows_of(r).len(), 1);
    // Bare `id` is unambiguous after USING/NATURAL.
    let r2 = &run(&engine, "SELECT id FROM a NATURAL JOIN b").await[0];
    assert_eq!(rows_of(r2).len(), 1);
    assert_eq!(text(&rows_of(r2)[0][0]), Some("2".into()));
}

#[tokio::test]
async fn select_command_tag_counts_rows() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4)").await;
    run(&engine, "INSERT INTO t VALUES (1),(2)").await;
    match &run(&engine, "SELECT id FROM t").await[0] {
        QueryResult::Rows { tag, .. } => assert_eq!(tag, "SELECT 2"),
        other => panic!("got {other:?}"),
    }
}

#[tokio::test]
async fn non_boolean_where_is_42804() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4)").await;
    run(&engine, "INSERT INTO t VALUES (1)").await;
    let err = engine
        .connect()
        .simple_query("SELECT id FROM t WHERE id")
        .await
        .expect_err("non-bool");
    assert_eq!(err.code, "42804");
}

#[tokio::test]
async fn null_orders_last_ascending() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4)").await;
    run(&engine, "INSERT INTO t VALUES (2),(null),(1)").await;
    let r = &run(&engine, "SELECT id FROM t ORDER BY id ASC").await[0];
    let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert_eq!(got, vec![Some("1".into()), Some("2".into()), None]); // NULLS LAST
}

#[tokio::test]
async fn order_by_mixed_width_expression_key() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (a int4)").await;
    run(&engine, "INSERT INTO t VALUES (1),(3),(2)").await;
    // a + 3000000000 promotes each key to int8; sort must still be 1,2,3.
    let r = &run(&engine, "SELECT a FROM t ORDER BY a + 3000000000 ASC").await[0];
    let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert_eq!(
        got,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
}

#[tokio::test]
async fn plain_select_order_by_position_and_alias_use_output() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (a int4, b int4, name text)").await;
    run(
        &engine,
        "INSERT INTO t VALUES (1,20,'a'),(2,10,'b'),(3,30,'c')",
    )
    .await;

    let r = &run(&engine, "SELECT name FROM t ORDER BY 1 DESC").await[0];
    let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert_eq!(
        got,
        vec![Some("c".into()), Some("b".into()), Some("a".into())]
    );

    let r = &run(&engine, "SELECT a AS b FROM t ORDER BY b").await[0];
    let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert_eq!(
        got,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );

    let r = &run(&engine, "SELECT a AS b FROM t ORDER BY t.b").await[0];
    let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert_eq!(
        got,
        vec![Some("2".into()), Some("1".into()), Some("3".into())]
    );

    let r = &run(&engine, "SELECT a AS b FROM t ORDER BY b + 0").await[0];
    let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert_eq!(
        got,
        vec![Some("2".into()), Some("1".into()), Some("3".into())]
    );
}

#[tokio::test]
async fn plain_select_order_by_pg_error_surface() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (a int4, b int4)").await;
    run(&engine, "INSERT INTO t VALUES (1,20),(2,10)").await;

    let err = engine
        .connect()
        .simple_query("SELECT a FROM t ORDER BY 0")
        .await
        .expect_err("position zero");
    assert_eq!(err.code, "42P10");

    let err = engine
        .connect()
        .simple_query("SELECT a FROM t ORDER BY 999999999999999999999999999")
        .await
        .expect_err("overflow position");
    assert_eq!(err.code, "42601");

    let err = engine
        .connect()
        .simple_query("SELECT a AS x, b AS x FROM t ORDER BY x")
        .await
        .expect_err("ambiguous output label");
    assert_eq!(err.code, "42702");
}

#[tokio::test]
async fn distinct_select_order_by_uses_output_only() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (a int4, b int4)").await;
    run(&engine, "INSERT INTO t VALUES (1,20),(1,10),(2,30)").await;

    let r = &run(&engine, "SELECT DISTINCT a AS x FROM t ORDER BY x DESC").await[0];
    let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert_eq!(got, vec![Some("2".into()), Some("1".into())]);

    let r = &run(&engine, "SELECT DISTINCT a AS x FROM t ORDER BY 1 DESC").await[0];
    let got: Vec<_> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert_eq!(got, vec![Some("2".into()), Some("1".into())]);

    let err = engine
        .connect()
        .simple_query("SELECT DISTINCT a FROM t ORDER BY b")
        .await
        .expect_err("source-only distinct key");
    assert_eq!(err.code, "42P10");
}

fn order_scope() -> Scope {
    Scope {
        columns: vec![
            ColumnBinding {
                exposure: Exposure::Output,
                qualifier: Some("t".into()),
                name: "a".into(),
                ty: crabka_pgtypes::ColumnType::Int4,
            },
            ColumnBinding {
                exposure: Exposure::Output,
                qualifier: Some("t".into()),
                name: "b".into(),
                ty: crabka_pgtypes::ColumnType::Int4,
            },
        ],
        ..Default::default()
    }
}

fn parsed_select(sql: &str) -> SelectStmt {
    match crabka_pgparser::parse(sql)
        .expect("parse")
        .pop()
        .expect("one")
    {
        Statement::Query(q) => match q.body {
            SetExpr::Query(QueryBody::Select(s)) => {
                let mut s = *s;
                s.order_by = q.order_by;
                s.limit = q.limit;
                s.offset = q.offset;
                s.locking = q.locking;
                s
            }
            other => panic!("expected select body, got {other:?}"),
        },
        other => panic!("expected select, got {other:?}"),
    }
}

#[test]
fn local_join_count_plan_deferral_requires_bare_equality() {
    let cases = [
        (
            "SELECT count(*) FROM l INNER JOIN r ON l.a = r.a",
            (true, true),
        ),
        (
            "SELECT count(*) FROM l LEFT JOIN r ON l.a = r.a",
            (true, true),
        ),
        (
            "SELECT count(*) FROM l INNER JOIN r ON l.a = r.a + 1",
            (true, false),
        ),
        (
            "SELECT count(l.a) FROM l INNER JOIN r ON l.a = r.a",
            (false, false),
        ),
        (
            "SELECT count(*) FROM l INNER JOIN r ON l.a = r.a WHERE l.a = 1",
            (false, false),
        ),
    ];
    for (sql, (local_count, defer_plan)) in cases {
        let select = parsed_select(sql);
        assert!(
            super::uses_local_join_count_shape(&select) == local_count,
            "{sql}"
        );
        assert!(
            super::should_defer_local_join_count_plan(&select) == defer_plan,
            "{sql}"
        );
    }
}

#[test]
fn ordered_rows_use_the_callers_statement_memory() {
    use assert2::assert;

    let select = parsed_select("SELECT a FROM t ORDER BY a");
    let scope = order_scope();
    let (fields, out_exprs, _) =
        super::resolve_projection(&select.projection, &scope).expect("projection");
    let statement_memory = crate::scanner::StatementMemory::new(crabka_units::bytes(1));

    let error = super::project_rows_ordered_with_memory(
        &select,
        &scope,
        &fields,
        &out_exprs,
        vec![vec![crabka_pgtypes::Datum::Int4(1)]],
        &crate::clock::EvalCtx::test_default(),
        &statement_memory,
    )
    .expect_err("sort keys must use the supplied statement limit")
    .into_pg();

    assert!(error.code == "53200");
}

#[test]
fn distinct_rows_use_the_callers_statement_memory() {
    use assert2::assert;

    let select = parsed_select("SELECT DISTINCT a FROM t");
    let scope = order_scope();
    let (fields, out_exprs, _) =
        super::resolve_projection(&select.projection, &scope).expect("projection");
    let statement_memory = crate::scanner::StatementMemory::new(crabka_units::bytes(1));

    let error = super::project_rows_ordered_with_memory(
        &select,
        &scope,
        &fields,
        &out_exprs,
        vec![vec![crabka_pgtypes::Datum::Int4(1)]],
        &crate::clock::EvalCtx::test_default(),
        &statement_memory,
    )
    .expect_err("DISTINCT rows must use the supplied statement limit")
    .into_pg();

    assert!(error.code == "53200");
}

#[test]
fn distinct_rows_are_deduplicated_and_ordered() {
    use assert2::assert;
    use crabka_pgtypes::Datum;

    let select = parsed_select("SELECT DISTINCT a FROM t ORDER BY a");
    let scope = order_scope();
    let (fields, out_exprs, _) =
        super::resolve_projection(&select.projection, &scope).expect("projection");
    let statement_memory = crate::scanner::StatementMemory::new(crabka_units::bytes(1024));

    let rows = super::project_rows_ordered_with_memory(
        &select,
        &scope,
        &fields,
        &out_exprs,
        vec![
            vec![Datum::Int4(2), Datum::Int4(0)],
            vec![Datum::Int4(1), Datum::Int4(0)],
            vec![Datum::Int4(2), Datum::Int4(0)],
        ],
        &crate::clock::EvalCtx::test_default(),
        &statement_memory,
    )
    .expect("distinct query");

    assert!(rows == vec![vec![Datum::Int4(1)], vec![Datum::Int4(2)]]);
}

#[test]
fn lateral_cache_accepts_only_a_noop_offset() {
    let select = parsed_select(
        "SELECT * FROM outer_t, LATERAL (SELECT inner_t.id FROM inner_t \
             WHERE inner_t.id = outer_t.id OFFSET 0) q",
    );
    assert!(super::lateral_cacheable(&select.from[1]));

    let select = parsed_select(
        "SELECT * FROM outer_t, LATERAL (SELECT inner_t.id FROM inner_t \
             WHERE inner_t.id = outer_t.id OFFSET 1) q",
    );
    assert!(!super::lateral_cacheable(&select.from[1]));

    let select = parsed_select(
        "SELECT * FROM outer_t, LATERAL (SELECT random() FROM inner_t \
             WHERE inner_t.id = outer_t.id OFFSET 0) q",
    );
    assert!(!super::lateral_cacheable(&select.from[1]));
}

#[test]
fn select_order_keys_resolve_positions_aliases_and_source_fallback() {
    use super::{SelectOrderKey, resolve_select_order_keys};

    let s = parsed_select("SELECT a AS x, b FROM t ORDER BY 1, x DESC, t.b, b + 0");
    let scope = order_scope();
    let (fields, out_exprs, _) =
        super::resolve_projection(&s.projection, &scope).expect("projection");
    let keys = resolve_select_order_keys(&s.order_by, &scope, &fields, &out_exprs, false)
        .expect("order keys");

    assert!(matches!(keys[0], SelectOrderKey::Output(0)));
    assert!(matches!(keys[1], SelectOrderKey::Output(0)));
    assert!(matches!(keys[2], SelectOrderKey::SourceExpr(_)));
    assert!(matches!(keys[3], SelectOrderKey::SourceExpr(_)));
}

#[test]
fn select_order_keys_report_pg_errors() {
    use super::resolve_select_order_keys;

    let scope = order_scope();

    let bad_pos = parsed_select("SELECT a FROM t ORDER BY 0");
    let (fields, out_exprs, _) =
        super::resolve_projection(&bad_pos.projection, &scope).expect("projection");
    let err = resolve_select_order_keys(&bad_pos.order_by, &scope, &fields, &out_exprs, false)
        .expect_err("bad position");
    assert_eq!(err.into_pg().code, "42P10");

    let overflow = parsed_select("SELECT a FROM t ORDER BY 999999999999999999999999999");
    let (fields, out_exprs, _) =
        super::resolve_projection(&overflow.projection, &scope).expect("projection");
    let err = resolve_select_order_keys(&overflow.order_by, &scope, &fields, &out_exprs, false)
        .expect_err("overflow");
    assert_eq!(err.into_pg().code, "42601");

    let i32_overflow = parsed_select("SELECT a FROM t ORDER BY 2147483648");
    let (fields, out_exprs, _) =
        super::resolve_projection(&i32_overflow.projection, &scope).expect("projection");
    let err = resolve_select_order_keys(&i32_overflow.order_by, &scope, &fields, &out_exprs, false)
        .expect_err("i32 overflow");
    let pg = err.into_pg();
    assert_eq!(pg.code, "42601");
    assert_eq!(pg.message, "non-integer constant in ORDER BY");

    let duplicate = parsed_select("SELECT a AS x, b AS x FROM t ORDER BY x");
    let (fields, out_exprs, _) =
        super::resolve_projection(&duplicate.projection, &scope).expect("projection");
    let err = resolve_select_order_keys(&duplicate.order_by, &scope, &fields, &out_exprs, false)
        .expect_err("ambiguous output label");
    let pg = err.into_pg();
    assert_eq!(pg.code, "42702");
    assert_eq!(pg.message, "ORDER BY \"x\" is ambiguous");
}

#[test]
fn select_order_keys_allow_identical_duplicate_output_labels() {
    use super::{SelectOrderKey, resolve_select_order_keys};

    let scope = order_scope();

    let duplicate_same_expr = parsed_select("SELECT a, a FROM t ORDER BY a");
    let (fields, out_exprs, _) =
        super::resolve_projection(&duplicate_same_expr.projection, &scope).expect("projection");
    let keys = resolve_select_order_keys(
        &duplicate_same_expr.order_by,
        &scope,
        &fields,
        &out_exprs,
        false,
    )
    .expect("identical duplicate output expressions are not ambiguous");
    assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

    let duplicate_same_alias = parsed_select("SELECT a AS x, a AS x FROM t ORDER BY x");
    let (fields, out_exprs, _) =
        super::resolve_projection(&duplicate_same_alias.projection, &scope).expect("projection");
    let keys = resolve_select_order_keys(
        &duplicate_same_alias.order_by,
        &scope,
        &fields,
        &out_exprs,
        false,
    )
    .expect("identical duplicate output aliases are not ambiguous");
    assert_eq!(keys, vec![SelectOrderKey::Output(0)]);
}

#[test]
fn select_distinct_order_keys_require_output_columns() {
    use super::{SelectOrderKey, resolve_select_order_keys};

    let scope = order_scope();

    let by_alias = parsed_select("SELECT DISTINCT a AS x FROM t ORDER BY x");
    let (fields, out_exprs, _) =
        super::resolve_projection(&by_alias.projection, &scope).expect("projection");
    let keys = resolve_select_order_keys(&by_alias.order_by, &scope, &fields, &out_exprs, true)
        .expect("alias is output");
    assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

    let by_select_expr = parsed_select("SELECT DISTINCT a AS x FROM t ORDER BY a");
    let (fields, out_exprs, _) =
        super::resolve_projection(&by_select_expr.projection, &scope).expect("projection");
    let keys =
        resolve_select_order_keys(&by_select_expr.order_by, &scope, &fields, &out_exprs, true)
            .expect("select-list expression is output");
    assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

    let by_qualified_select_expr = parsed_select("SELECT DISTINCT a FROM t ORDER BY t.a");
    let (fields, out_exprs, _) =
        super::resolve_projection(&by_qualified_select_expr.projection, &scope)
            .expect("projection");
    let keys = resolve_select_order_keys(
        &by_qualified_select_expr.order_by,
        &scope,
        &fields,
        &out_exprs,
        true,
    )
    .expect("qualified select-list expression is output");
    assert_eq!(keys, vec![SelectOrderKey::Output(0)]);

    let missing_qualifier = parsed_select("SELECT DISTINCT a FROM t ORDER BY nope.a");
    let (fields, out_exprs, _) =
        super::resolve_projection(&missing_qualifier.projection, &scope).expect("projection");
    let err = resolve_select_order_keys(
        &missing_qualifier.order_by,
        &scope,
        &fields,
        &out_exprs,
        true,
    )
    .expect_err("missing qualified table");
    assert_eq!(err.into_pg().code, "42P01");

    let source_only = parsed_select("SELECT DISTINCT a FROM t ORDER BY b");
    let (fields, out_exprs, _) =
        super::resolve_projection(&source_only.projection, &scope).expect("projection");
    let err = resolve_select_order_keys(&source_only.order_by, &scope, &fields, &out_exprs, true)
        .expect_err("source-only key");
    let pg = err.into_pg();
    assert_eq!(pg.code, "42P10");
    assert_eq!(
        pg.message,
        "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    );
}

async fn run(engine: &SqlEngine, sql: &str) -> Vec<QueryResult> {
    // Autocommit per statement: a fresh session per call preserves the same
    // semantics the old direct `engine.simple_query` had.
    engine.connect().simple_query(sql).await.expect("ok")
}

// ---- Q3: DISTINCT ON, LATERAL, ordering/limit breadth ----

/// Every output cell of one statement, row-major, with NULL as `None`.
async fn cells(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<String>>> {
    let results = run(engine, sql).await;
    rows_of(&results[results.len() - 1])
        .iter()
        .map(|row| row.iter().map(text).collect())
        .collect()
}

/// The SQLSTATE one statement fails with.
async fn sqlstate(engine: &SqlEngine, sql: &str) -> String {
    engine
        .connect()
        .simple_query(sql)
        .await
        .expect_err("expected an error")
        .code
}

fn cell_rows(rows: &[&[&str]]) -> Vec<Vec<Option<String>>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|value| (*value != "NULL").then(|| (*value).to_string()))
                .collect()
        })
        .collect()
}

async fn q3_fixture() -> SqlEngine {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE q3 (id int4, grp int4, v int4)").await;
    run(
        &engine,
        "INSERT INTO q3 VALUES (1,10,100),(2,10,300),(3,20,50),(4,20,50),(5,NULL,NULL)",
    )
    .await;
    engine
}

#[tokio::test]
async fn distinct_on_keeps_the_first_row_of_each_group() {
    use assert2::assert;
    let engine = q3_fixture().await;
    let cases: [(&str, &[&[&str]]); 4] = [
        (
            "SELECT DISTINCT ON (grp) grp, id FROM q3 ORDER BY grp, id",
            &[&["10", "1"], &["20", "3"], &["NULL", "5"]],
        ),
        (
            "SELECT DISTINCT ON (grp) grp, id FROM q3 ORDER BY grp, id DESC",
            &[&["10", "2"], &["20", "4"], &["NULL", "5"]],
        ),
        (
            "SELECT DISTINCT ON (grp) id FROM q3 ORDER BY grp DESC, id",
            &[&["5"], &["3"], &["1"]],
        ),
        (
            "SELECT DISTINCT ON (grp, v) id FROM q3 ORDER BY grp, v, id",
            &[&["1"], &["2"], &["3"], &["5"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
}

#[tokio::test]
async fn distinct_on_without_order_by_sorts_by_its_keys() {
    use assert2::assert;
    let engine = q3_fixture().await;
    assert!(
        cells(&engine, "SELECT DISTINCT ON (grp) grp FROM q3").await
            == cell_rows(&[&["10"], &["20"], &["NULL"]])
    );
}

/// `PostgreSQL`'s DISTINCT ON / ORDER BY rule is one-directional: every
/// leading ORDER BY key must be a DISTINCT ON expression, but the ON list may
/// hold expressions the ORDER BY never mentions. It is NOT a set match, and
/// the queries in the accepted half below are exactly the ones a set match
/// wrongly rejects.
#[tokio::test]
async fn distinct_on_adopts_the_leading_order_by_keys() {
    use assert2::assert;
    let engine = q3_fixture().await;
    let accepted: [(&str, &[&[&str]]); 6] = [
        // Order among the adopted keys is free.
        (
            "SELECT DISTINCT ON (grp, v) grp FROM q3 ORDER BY v, grp",
            &[&["20"], &["10"], &["10"], &["NULL"]],
        ),
        // An ON expression the ORDER BY never mentions is appended to the
        // dedup sort with default ASC NULLS LAST semantics.
        (
            "SELECT DISTINCT ON (grp, v) grp, v FROM q3 ORDER BY grp",
            &[
                &["10", "100"],
                &["10", "300"],
                &["20", "50"],
                &["NULL", "NULL"],
            ],
        ),
        (
            "SELECT DISTINCT ON (grp, id) grp, id FROM q3 ORDER BY grp DESC",
            &[
                &["NULL", "5"],
                &["20", "3"],
                &["20", "4"],
                &["10", "1"],
                &["10", "2"],
            ],
        ),
        // An output ordinal and an output alias both name the select-list
        // column they stand for, on either side of the comparison.
        (
            "SELECT DISTINCT ON (1) grp, id FROM q3 ORDER BY 1, 2",
            &[&["10", "1"], &["20", "3"], &["NULL", "5"]],
        ),
        (
            "SELECT DISTINCT ON (g) grp AS g, id FROM q3 ORDER BY g, id DESC",
            &[&["10", "2"], &["20", "4"], &["NULL", "5"]],
        ),
        // DISTINCT ON over a grouped query dedups the grouped output.
        (
            "SELECT DISTINCT ON (grp) count(*) FROM q3 GROUP BY grp ORDER BY grp",
            &[&["2"], &["2"], &["1"]],
        ),
    ];
    for (sql, want) in accepted {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
    // 42P10 fires once an ORDER BY key has been skipped: for a later key that
    // IS in the ON list, and for an ON expression still needing appending.
    for sql in [
        "SELECT DISTINCT ON (grp) grp FROM q3 ORDER BY v",
        "SELECT DISTINCT ON (grp) grp FROM q3 ORDER BY id, grp",
        "SELECT DISTINCT ON (grp, v) grp FROM q3 ORDER BY grp, id",
    ] {
        assert!(sqlstate(&engine, sql).await == "42P10", "{sql}");
    }
}

/// A bare constant in ORDER BY / GROUP BY / DISTINCT ON is an output
/// position, and `-` folds into it. Before this was modelled, `ORDER BY -1`
/// and `ORDER BY 1.0` were accepted as constant expressions and silently
/// dropped the sort.
#[tokio::test]
async fn sql92_constant_positions_are_validated() {
    use assert2::assert;
    let engine = q3_fixture().await;
    let cases: [(&str, &str); 10] = [
        ("SELECT id FROM q3 ORDER BY -1", "42P10"),
        ("SELECT id, grp FROM q3 ORDER BY -1, 1", "42P10"),
        ("SELECT id FROM q3 ORDER BY 0", "42P10"),
        ("SELECT id FROM q3 ORDER BY -0", "42P10"),
        ("SELECT id FROM q3 ORDER BY 1.0", "42601"),
        ("SELECT id FROM q3 ORDER BY 1e0", "42601"),
        ("SELECT id FROM q3 ORDER BY 'x'", "42601"),
        ("SELECT id FROM q3 ORDER BY true", "42601"),
        // Wider than int4, so a float constant in PostgreSQL — not a position.
        ("SELECT id FROM q3 ORDER BY 3000000000", "42601"),
        ("SELECT id FROM q3 GROUP BY -1", "42P10"),
    ];
    for (sql, want) in cases {
        assert!(sqlstate(&engine, sql).await == want, "{sql}");
    }
    // Unary `+` is an operator, not a sign, so `+1` is the constant 1 and
    // sorts every row equal rather than naming output column 1.
    assert!(
        cells(&engine, "SELECT id FROM q3 ORDER BY +1").await
            == cell_rows(&[&["1"], &["2"], &["3"], &["4"], &["5"]])
    );
}

#[tokio::test]
async fn order_by_null_placement_follows_postgres() {
    use assert2::assert;
    let engine = q3_fixture().await;
    let cases: [(&str, &[&[&str]]); 6] = [
        (
            "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v",
            &[&["100"], &["NULL"]],
        ),
        (
            "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v DESC",
            &[&["NULL"], &["100"]],
        ),
        (
            "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v NULLS FIRST",
            &[&["NULL"], &["100"]],
        ),
        (
            "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v NULLS LAST",
            &[&["100"], &["NULL"]],
        ),
        (
            "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v DESC NULLS LAST",
            &[&["100"], &["NULL"]],
        ),
        (
            "SELECT v FROM q3 WHERE id IN (1,5) ORDER BY v ASC NULLS FIRST",
            &[&["NULL"], &["100"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
}

#[tokio::test]
async fn row_counts_accept_arbitrary_expressions() {
    use assert2::assert;
    let engine = q3_fixture().await;
    let cases: [(&str, &[&[&str]]); 7] = [
        (
            "SELECT id FROM q3 ORDER BY id LIMIT 1 + 1",
            &[&["1"], &["2"]],
        ),
        (
            "SELECT id FROM q3 ORDER BY id LIMIT (SELECT 2)",
            &[&["1"], &["2"]],
        ),
        ("SELECT id FROM q3 ORDER BY id OFFSET 3", &[&["4"], &["5"]]),
        (
            "SELECT id FROM q3 ORDER BY id LIMIT ALL OFFSET 4",
            &[&["5"]],
        ),
        (
            "SELECT id FROM q3 ORDER BY id LIMIT NULL OFFSET 4",
            &[&["5"]],
        ),
        (
            "SELECT id FROM q3 ORDER BY id OFFSET 3 ROWS FETCH NEXT 1 ROW ONLY",
            &[&["4"]],
        ),
        (
            "SELECT id FROM q3 ORDER BY id FETCH FIRST ROW ONLY",
            &[&["1"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
    assert!(sqlstate(&engine, "SELECT id FROM q3 LIMIT -1").await == "2201W");
    assert!(sqlstate(&engine, "SELECT id FROM q3 OFFSET -1").await == "2201X");
}

#[tokio::test]
async fn fetch_with_ties_extends_the_cut_through_equal_keys() {
    use assert2::assert;
    let engine = q3_fixture().await;
    let cases: [(&str, &[&[&str]]); 3] = [
        (
            "SELECT id, v FROM q3 ORDER BY v NULLS LAST FETCH FIRST 1 ROW WITH TIES",
            &[&["3", "50"], &["4", "50"]],
        ),
        (
            "SELECT id, v FROM q3 ORDER BY v NULLS LAST FETCH FIRST 1 ROW ONLY",
            &[&["3", "50"]],
        ),
        (
            "SELECT id, v FROM q3 ORDER BY v NULLS LAST OFFSET 2 FETCH FIRST 1 ROW WITH TIES",
            &[&["1", "100"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
}

#[tokio::test]
async fn lateral_items_are_evaluated_per_outer_row() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE lat (id int4, n int4)").await;
    run(&engine, "INSERT INTO lat VALUES (1,2),(2,0),(3,1)").await;
    let cases: [(&str, &[&[&str]]); 5] = [
        (
            "SELECT t.id, u.x FROM lat t, LATERAL (SELECT t.n * 10 AS x) u ORDER BY t.id",
            &[&["1", "20"], &["2", "0"], &["3", "10"]],
        ),
        (
            "SELECT t.id, g FROM lat t, LATERAL generate_series(1, t.n) g ORDER BY t.id, g",
            &[&["1", "1"], &["1", "2"], &["3", "1"]],
        ),
        // Implicit lateral: a function argument naming an earlier FROM item.
        (
            "SELECT t.id, g FROM lat t, generate_series(1, t.n) g ORDER BY t.id, g",
            &[&["1", "1"], &["1", "2"], &["3", "1"]],
        ),
        // LEFT JOIN LATERAL keeps an outer row whose lateral side is empty.
        (
            "SELECT t.id, g FROM lat t LEFT JOIN LATERAL generate_series(1, t.n) g ON true ORDER BY t.id, g",
            &[&["1", "1"], &["1", "2"], &["2", "NULL"], &["3", "1"]],
        ),
        (
            "SELECT t.id, u.x FROM lat t LEFT JOIN LATERAL (SELECT t.n AS x WHERE t.n > 1) u ON true ORDER BY t.id",
            &[&["1", "2"], &["2", "NULL"], &["3", "NULL"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
}

#[tokio::test]
async fn lateral_filter_runs_only_for_rows_that_survive_a_safe_where_filter() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE lateral_filter (id int4)").await;
    run(&engine, "INSERT INTO lateral_filter VALUES (1),(0)").await;

    assert!(
        cells(
            &engine,
            "SELECT t.id, g FROM lateral_filter t \
             CROSS JOIN LATERAL generate_series(1, 1 / t.id) g \
             WHERE t.id = 1",
        )
        .await
            == cell_rows(&[&["1", "1"]])
    );
}

/// `PostgreSQL` resolves an unqualified name inside a lateral item against
/// the item's own FROM first, and only then against the outer row. The
/// binder used to give up whenever the inner block had a FROM at all, which
/// turned ordinary lateral queries into a spurious 42703.
#[tokio::test]
async fn lateral_unqualified_names_fall_back_to_the_outer_row() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE lo (id int4, nm text)").await;
    run(&engine, "INSERT INTO lo VALUES (1,'a'),(2,'b')").await;
    run(&engine, "CREATE TABLE li (a int4, b int4)").await;
    run(&engine, "INSERT INTO li VALUES (1,10),(1,20),(2,30)").await;
    let cases: [(&str, &[&[&str]]); 6] = [
        // `id` is not a column of `li`, so it binds to the outer row.
        (
            "SELECT o.id, q.b FROM lo o, LATERAL (SELECT b FROM li WHERE li.a = id) q \
                 ORDER BY 1, 2",
            &[&["1", "10"], &["1", "20"], &["2", "30"]],
        ),
        // `a` IS a column of `li`, so the inner one wins and nothing binds.
        (
            "SELECT o.id, q.b FROM lo o, LATERAL (SELECT b FROM li WHERE a = o.id) q \
                 ORDER BY 1, 2",
            &[&["1", "10"], &["1", "20"], &["2", "30"]],
        ),
        // A CTE inside the lateral item is walked too.
        (
            "SELECT o.id, q.v FROM lo o, LATERAL (WITH c AS (SELECT o.id AS v) \
                 SELECT * FROM c) q ORDER BY 1",
            &[&["1", "1"], &["2", "2"]],
        ),
        // With no inner FROM at all every name comes from the outer row.
        (
            "SELECT o.id, q.z FROM lo o, LATERAL (SELECT nm AS z) q ORDER BY 1",
            &[&["1", "a"], &["2", "b"]],
        ),
        // The lateral binder substitutes `id` with a constant. The derived
        // table must nevertheless retain the name FigureColname assigned
        // before that substitution.
        (
            "SELECT o.id, q.id FROM lo o, LATERAL (SELECT id) q ORDER BY 1",
            &[&["1", "1"], &["2", "2"]],
        ),
        // The FunctionScan needs `id` to be substituted before the inner
        // FROM schema is described; the projected bare `id` then falls
        // back to the same outer row.
        (
            "SELECT o.id, q.outer_id FROM lo o, LATERAL (SELECT id AS outer_id \
                 FROM (VALUES (1)) v(n) LEFT JOIN generate_series(id, id) g ON true OFFSET 0) q \
                 ORDER BY 1",
            &[&["1", "1"], &["2", "2"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
}

/// `RIGHT`/`FULL JOIN LATERAL` is legal in `PostgreSQL` whenever the lateral
/// item reads nothing from the other side; only an actual reference is the
/// error, and it is 42P10 naming the relation, not a blanket 0A000.
#[tokio::test]
async fn lateral_on_the_nullable_side_is_rejected_only_when_it_correlates() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE rj (id int4)").await;
    run(&engine, "INSERT INTO rj VALUES (1),(2)").await;
    let accepted: [(&str, &[&[&str]]); 3] = [
        (
            "SELECT * FROM rj RIGHT JOIN LATERAL (SELECT 9 AS z) q ON true ORDER BY 1",
            &[&["1", "9"], &["2", "9"]],
        ),
        (
            "SELECT * FROM rj FULL JOIN LATERAL (SELECT 9 AS z) q ON true ORDER BY 1",
            &[&["1", "9"], &["2", "9"]],
        ),
        (
            "SELECT * FROM rj RIGHT JOIN LATERAL generate_series(1,2) g ON true \
                 ORDER BY 1, 2",
            &[&["1", "1"], &["1", "2"], &["2", "1"], &["2", "2"]],
        ),
    ];
    for (sql, want) in accepted {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
    for sql in [
        "SELECT * FROM rj RIGHT JOIN LATERAL (SELECT rj.id AS z) q ON true",
        "SELECT * FROM rj FULL JOIN LATERAL (SELECT rj.id AS z) q ON true",
        "SELECT * FROM rj RIGHT JOIN LATERAL generate_series(1, rj.id) g ON true",
    ] {
        assert!(sqlstate(&engine, sql).await == "42P10", "{sql}");
    }
}

/// `SELECT *` over a relation whose column names repeat expands
/// positionally, so it works even though a bare reference to the repeated
/// name is still ambiguous.
#[tokio::test]
async fn wildcard_expands_positionally_over_repeated_column_names() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let cases: [(&str, &[&[&str]]); 4] = [
        (
            "SELECT * FROM ROWS FROM (generate_series(1,3), generate_series(1,2))",
            &[&["1", "1"], &["2", "2"], &["3", "NULL"]],
        ),
        (
            "SELECT * FROM ROWS FROM (generate_series(1,2), generate_series(1,1)) \
                 WITH ORDINALITY",
            &[&["1", "1", "1"], &["2", "NULL", "2"]],
        ),
        (
            "SELECT t.* FROM ROWS FROM (generate_series(1,2), generate_series(1,1)) t",
            &[&["1", "1"], &["2", "NULL"]],
        ),
        (
            "SELECT * FROM unnest(ARRAY[1,2], ARRAY['a','b','c'])",
            &[&["1", "a"], &["2", "b"], &["NULL", "c"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
    // A bare reference to the repeated name is still 42702, as in PostgreSQL.
    assert!(
        sqlstate(
            &engine,
            "SELECT generate_series FROM ROWS FROM (generate_series(1,2), generate_series(1,1))"
        )
        .await
            == "42702"
    );
}

/// A base-table alias may rename columns, exactly like a derived table's.
#[tokio::test]
async fn a_base_table_alias_may_carry_a_column_list() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE ba (a int4, b int4, c int4)").await;
    run(&engine, "INSERT INTO ba VALUES (1,2,3)").await;
    let cases: [(&str, &[&[&str]]); 3] = [
        ("SELECT * FROM ba AS q(x)", &[&["1", "2", "3"]]),
        ("SELECT q.x, q.y FROM ba q(x, y)", &[&["1", "2"]]),
        ("SELECT x FROM ba AS q(x, y, z) WHERE z = 3", &[&["1"]]),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
    // Too many names is 42P10, the same as for a derived table.
    assert!(sqlstate(&engine, "SELECT * FROM ba AS q(w, x, y, z)").await == "42P10");
}

/// The row-count clauses coerce to bigint by assignment, so a type with no
/// such cast is 42804 naming it, not the 42846 an explicit cast would
/// give.
#[tokio::test]
async fn limit_and_offset_reject_non_numeric_arguments() {
    use assert2::assert;
    let engine = q3_fixture().await;
    for sql in [
        "SELECT id FROM q3 LIMIT true",
        "SELECT id FROM q3 OFFSET true",
        "SELECT id FROM q3 LIMIT '2'::text",
        "SELECT id FROM q3 LIMIT '1 day'::interval",
    ] {
        assert!(sqlstate(&engine, sql).await == "42804", "{sql}");
    }
    // An untyped literal still resolves as bigint.
    assert!(
        cells(&engine, "SELECT id FROM q3 ORDER BY id LIMIT '2'")
            .await
            .len()
            == 2
    );
}

/// A null `REPEATABLE` seed is `invalid_tablesample_repeat`, which is a
/// different SQLSTATE from the `invalid_tablesample_argument` a null or
/// out-of-range percentage raises.
#[tokio::test]
async fn tablesample_null_seed_and_null_percentage_differ() {
    use assert2::assert;
    let engine = q3_fixture().await;
    let cases: [(&str, &str); 4] = [
        (
            "SELECT * FROM q3 TABLESAMPLE SYSTEM (50) REPEATABLE (NULL)",
            "2202G",
        ),
        (
            "SELECT * FROM q3 TABLESAMPLE BERNOULLI (50) REPEATABLE (NULL)",
            "2202G",
        ),
        ("SELECT * FROM q3 TABLESAMPLE SYSTEM (NULL)", "2202H"),
        ("SELECT * FROM q3 TABLESAMPLE SYSTEM (101)", "2202H"),
    ];
    for (sql, want) in cases {
        assert!(sqlstate(&engine, sql).await == want, "{sql}");
    }
}

/// `ORDER BY … USING <op>` takes its direction from the ordering operator,
/// and its NULL placement from that direction.
#[tokio::test]
async fn order_by_using_takes_its_direction_from_the_operator() {
    use assert2::assert;
    let engine = q3_fixture().await;
    let cases: [(&str, &[&[&str]]); 3] = [
        (
            "SELECT grp FROM q3 WHERE id IN (1,3,5) ORDER BY grp USING <",
            &[&["10"], &["20"], &["NULL"]],
        ),
        (
            "SELECT grp FROM q3 WHERE id IN (1,3,5) ORDER BY grp USING >",
            &[&["NULL"], &["20"], &["10"]],
        ),
        (
            "SELECT grp FROM q3 WHERE id IN (1,3,5) ORDER BY grp USING > NULLS LAST",
            &[&["20"], &["10"], &["NULL"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
    assert!(sqlstate(&engine, "SELECT grp FROM q3 ORDER BY grp USING <=").await == "42809");
    // `FOR READ ONLY` locks nothing and is accepted as a no-op.
    assert!(
        cells(&engine, "SELECT id FROM q3 ORDER BY id FOR READ ONLY")
            .await
            .len()
            == 5
    );
}

#[tokio::test]
async fn a_non_lateral_derived_table_cannot_see_an_earlier_from_item() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE lat (id int4, n int4)").await;
    run(&engine, "INSERT INTO lat VALUES (1,2)").await;
    assert!(sqlstate(&engine, "SELECT * FROM lat t, (SELECT t.n AS x) u").await == "42P01");
}

/// The full error one statement fails with: SQLSTATE, message, DETAIL, HINT.
async fn failure(
    engine: &SqlEngine,
    sql: &str,
) -> (String, String, Option<String>, Option<String>) {
    let error = engine
        .connect()
        .simple_query(sql)
        .await
        .expect_err("expected an error");
    let diagnostics = error.diagnostics.unwrap_or_default();
    (
        error.code,
        error.message,
        diagnostics.detail,
        diagnostics.hint,
    )
}

async fn agg_level_fixture() -> SqlEngine {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE al4 (f1 int4)").await;
    run(&engine, "INSERT INTO al4 VALUES (0), (5), (-5)").await;
    run(&engine, "CREATE TABLE al8 (q1 int8, q2 int8)").await;
    run(&engine, "INSERT INTO al8 VALUES (5,6), (5,7), (8,5)").await;
    engine
}

/// `PostgreSQL` gives an aggregate the query level of the innermost variable
/// its arguments read, then rejects it when that level is the one whose FROM
/// clause the aggregate is written in. It is the level that decides, not the
/// `LATERAL` keyword: `max(a.f1 + b.q1)` reads the sub-select's own `b` and
/// is therefore allowed in the very position `max(a.f1)` is not.
#[tokio::test]
async fn an_aggregate_may_not_take_its_level_from_the_from_clause_it_is_written_in() {
    use assert2::assert;
    let engine = agg_level_fixture().await;
    let rejected: [(&str, &str); 12] = [
        // The argument reads the level that owns the FROM clause.
        (
            "SELECT 1 FROM al4 a, LATERAL (SELECT max(a.f1) FROM al8 b) ss",
            "aggregate functions are not allowed in FROM clause of their own query level",
        ),
        // Anywhere in the sub-select, not just its select list.
        (
            "SELECT 1 FROM al4 a, LATERAL (SELECT 1 FROM al8 b HAVING max(a.f1) > 0) ss",
            "aggregate functions are not allowed in FROM clause of their own query level",
        ),
        (
            "SELECT 1 FROM al4 a, LATERAL (SELECT 1 FROM al8 b WHERE max(a.f1) > 0) ss",
            "aggregate functions are not allowed in FROM clause of their own query level",
        ),
        (
            "SELECT 1 FROM al4 a, LATERAL (SELECT max(a.f1) FROM al8 b LIMIT 1) ss",
            "aggregate functions are not allowed in FROM clause of their own query level",
        ),
        (
            "SELECT 1 FROM al4 a, LATERAL (SELECT max(a.f1) FROM al8 b GROUP BY b.q1) ss",
            "aggregate functions are not allowed in FROM clause of their own query level",
        ),
        // Through a set operation and through a CTE.
        (
            "SELECT 1 FROM al4 a, LATERAL (SELECT max(a.f1) FROM al8 b UNION ALL SELECT 1) ss",
            "aggregate functions are not allowed in FROM clause of their own query level",
        ),
        (
            "SELECT 1 FROM al4 a, LATERAL \
                 (WITH w AS (SELECT max(a.f1) AS m FROM al8 b) SELECT * FROM w) ss",
            "aggregate functions are not allowed in FROM clause of their own query level",
        ),
        // Written two query levels down, but still levelled out here.
        (
            "SELECT 1 FROM al4 a, LATERAL (SELECT (SELECT max(a.f1)) AS m) ss",
            "aggregate functions are not allowed in FROM clause of their own query level",
        ),
        (
            "SELECT 1 FROM al4 a, LATERAL \
                 (SELECT * FROM al4 c, LATERAL (SELECT max(a.f1) FROM al8 b) ss2) ss",
            "aggregate functions are not allowed in FROM clause of their own query level",
        ),
        // A function FROM item is its own expression kind, with its own wording.
        (
            "SELECT * FROM al4 a, generate_series(1, max(a.f1)) g",
            "aggregate functions are not allowed in functions in FROM",
        ),
        (
            "SELECT * FROM al4 a, LATERAL generate_series(1, (SELECT max(a.f1))) g",
            "aggregate functions are not allowed in functions in FROM",
        ),
        // So is a JOIN condition.
        (
            "SELECT * FROM al4 a JOIN al4 b ON a.f1 = max(b.f1)",
            "aggregate functions are not allowed in JOIN conditions",
        ),
    ];
    for (sql, message) in rejected {
        assert!(failure(&engine, sql).await.0 == "42803", "{sql}");
        assert!(failure(&engine, sql).await.1 == message, "{sql}");
    }

    // Every one of these takes its level from the sub-select it is written
    // in, and so is legal in exactly the position the cases above are not.
    let accepted: [&str; 7] = [
        "SELECT 1 FROM al4 a, LATERAL (SELECT max(b.q1) FROM al8 b) ss",
        // The *innermost* level its arguments read wins.
        "SELECT 1 FROM al4 a, LATERAL (SELECT max(a.f1 + b.q1) FROM al8 b) ss",
        "SELECT 1 FROM al4 a, LATERAL (SELECT max(b.q1) FILTER (WHERE a.f1 > 0) FROM al8 b) ss",
        // No variable at all: the aggregate keeps the level it is written at.
        "SELECT 1 FROM al4 a, LATERAL (SELECT max(1)) ss",
        "SELECT 1 FROM al4 a, LATERAL (SELECT count(*) FROM al8 b) ss",
        // A variable local to a sub-select of the argument is ignored.
        "SELECT 1 FROM al4 a, LATERAL \
             (SELECT max((SELECT z.f1 FROM al4 z WHERE z.f1 = 5)) FROM al8 b) ss",
        // The aggregate reads the derived table, not the outer relation.
        "SELECT 1 FROM al4 a, LATERAL (SELECT max(y.f1) FROM al8 b, LATERAL (SELECT b.q1 AS f1) y) ss",
    ];
    for sql in accepted {
        assert!(!cells(&engine, sql).await.is_empty(), "{sql}");
    }

    // Without LATERAL the sub-select cannot see the entry at all, so the
    // reference is what PostgreSQL reports — the aggregate never gets a level.
    assert!(
        failure(
            &engine,
            "SELECT 1 FROM al4 a, (SELECT max(a.f1) FROM al8 b) ss"
        )
        .await
        .1 == "invalid reference to FROM-clause entry for table \"a\""
    );
}

/// An entry that exists at this query level but is out of the referring
/// part's reach is a different error from one that is simply absent, and
/// `PostgreSQL` explains which in DETAIL — offering `LATERAL` as the remedy
/// only where `LATERAL` could help.
#[tokio::test]
async fn an_out_of_reach_from_entry_is_reported_apart_from_a_missing_one() {
    use assert2::assert;
    let engine = agg_level_fixture().await;
    run(&engine, "CREATE TABLE xx1 (x1 int4, x2 int4)").await;
    run(&engine, "INSERT INTO xx1 VALUES (0,0)").await;

    let entry = |table: &str| {
        format!(
            "There is an entry for table \"{table}\", but it cannot be referenced from this \
                 part of the query."
        )
    };
    let mark_lateral = "To reference that table, you must mark this subquery with LATERAL.";

    // A sibling FROM item, qualified: LATERAL would bring it into view.
    for sql in [
        "SELECT f1, g FROM al4 a, (SELECT a.f1 AS g) ss",
        "SELECT f1, g FROM al4 a CROSS JOIN (SELECT a.f1 AS g) ss",
        "SELECT * FROM al4 a JOIN (SELECT a.f1 AS g) ss ON true",
        "SELECT * FROM al4 a, al8 b, (SELECT a.f1 + b.q1 AS g) ss",
    ] {
        assert!(
            failure(&engine, sql).await
                == (
                    "42P01".into(),
                    "invalid reference to FROM-clause entry for table \"a\"".into(),
                    Some(entry("a")),
                    Some(mark_lateral.into()),
                ),
            "{sql}"
        );
    }

    // The same sibling, unqualified: the column is what is named.
    assert!(
        failure(&engine, "SELECT f1, g FROM al4 a, (SELECT f1 AS g) ss").await
            == (
                "42703".into(),
                "column \"f1\" does not exist".into(),
                Some(
                    "There is a column named \"f1\" in table \"a\", but it cannot be \
                         referenced from this part of the query."
                        .into()
                ),
                Some("To reference that column, you must mark this subquery with LATERAL.".into()),
            )
    );

    // An UPDATE/DELETE target is out of reach of its own FROM/USING items,
    // and no LATERAL can bring it back — so no remedy is offered.
    assert!(
        failure(
            &engine,
            "UPDATE xx1 SET x2 = f1 FROM (SELECT * FROM al4 WHERE f1 = xx1.x1) ss"
        )
        .await
            == (
                "42P01".into(),
                "invalid reference to FROM-clause entry for table \"xx1\"".into(),
                Some(entry("xx1")),
                None,
            )
    );
    assert!(
        failure(
            &engine,
            "DELETE FROM xx1 USING (SELECT * FROM al4 WHERE f1 = x1) ss"
        )
        .await
            == (
                "42703".into(),
                "column \"x1\" does not exist".into(),
                Some(
                    "There is a column named \"x1\" in table \"xx1\", but it cannot be \
                         referenced from this part of the query."
                        .into()
                ),
                None,
            )
    );
    // Written LATERAL, the target's name resolves and is then disallowed,
    // which PostgreSQL reports as the entry, with the sentence as a HINT.
    assert!(
        failure(
            &engine,
            "UPDATE xx1 SET x2 = f1 FROM LATERAL (SELECT * FROM al4 WHERE f1 = x1) ss"
        )
        .await
            == (
                "42P10".into(),
                "invalid reference to FROM-clause entry for table \"xx1\"".into(),
                None,
                Some(entry("xx1")),
            )
    );

    // A lateral item on the nullable side of a join is disallowed outright.
    assert!(
        failure(
            &engine,
            "SELECT * FROM al4 a RIGHT JOIN LATERAL (SELECT a.f1 AS g) ss ON true"
        )
        .await
            == (
                "42P10".into(),
                "invalid reference to FROM-clause entry for table \"a\"".into(),
                Some(
                    "The combining JOIN type must be INNER or LEFT for a LATERAL reference.".into()
                ),
                None,
            )
    );

    // A name no entry supplies keeps the bald statement of error.
    assert!(
        failure(&engine, "SELECT * FROM (SELECT a.f1 AS g) ss, al4 a").await
            == (
                "42P01".into(),
                "missing FROM-clause entry for table \"a\"".into(),
                None,
                None,
            )
    );
}

#[tokio::test]
async fn lateral_over_an_empty_outer_relation_keeps_the_columns() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE lat (id int4, n int4)").await;
    let results = run(
        &engine,
        "SELECT t.id, g FROM lat t, LATERAL generate_series(1, t.n) g",
    )
    .await;
    assert!(rows_of(&results[0]).is_empty());
    assert!(
        fields_of(&results[0])
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<_>>()
            == vec!["id".to_string(), "g".to_string()]
    );
}

#[tokio::test]
async fn with_ordinality_and_rows_from_expand_in_lockstep() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let cases: [(&str, &[&[&str]]); 5] = [
        (
            "SELECT * FROM generate_series(10, 30, 10) WITH ORDINALITY",
            &[&["10", "1"], &["20", "2"], &["30", "3"]],
        ),
        (
            "SELECT * FROM ROWS FROM (generate_series(1, 3), unnest(ARRAY['a','b'])) AS t(n, s)",
            &[&["1", "a"], &["2", "b"], &["3", "NULL"]],
        ),
        (
            "SELECT * FROM ROWS FROM (generate_series(1, 2)) WITH ORDINALITY AS t(a, b)",
            &[&["1", "1"], &["2", "2"]],
        ),
        // A bare alias renames a single-column item; ordinality keeps its name.
        (
            "SELECT g, ordinality FROM generate_series(1, 2) WITH ORDINALITY AS g",
            &[&["1", "1"], &["2", "2"]],
        ),
        // A shorter column-alias list renames only a prefix.
        (
            "SELECT a, ordinality FROM generate_series(1, 2) WITH ORDINALITY AS t(a)",
            &[&["1", "1"], &["2", "2"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
    assert!(sqlstate(&engine, "SELECT * FROM generate_series(1, 2) AS t(a, b)").await == "42P10");
    assert!(sqlstate(&engine, "SELECT * FROM generate_series(1, 2) AS t(a int4)").await == "42601");
}

#[tokio::test]
async fn tablesample_matches_postgres_at_its_deterministic_ends() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE samp (i int4)").await;
    run(&engine, "INSERT INTO samp VALUES (1),(2),(3),(4)").await;
    let cases: [(&str, &[&[&str]]); 4] = [
        (
            "SELECT count(*) FROM samp TABLESAMPLE BERNOULLI (100)",
            &[&["4"]],
        ),
        (
            "SELECT count(*) FROM samp TABLESAMPLE SYSTEM (100)",
            &[&["4"]],
        ),
        (
            "SELECT count(*) FROM samp TABLESAMPLE BERNOULLI (0)",
            &[&["0"]],
        ),
        (
            "SELECT count(*) FROM samp TABLESAMPLE SYSTEM (100) REPEATABLE (7)",
            &[&["4"]],
        ),
    ];
    for (sql, want) in cases {
        assert!(cells(&engine, sql).await == cell_rows(want), "{sql}");
    }
    let errors: [(&str, &str); 4] = [
        ("SELECT * FROM samp TABLESAMPLE FOO (50)", "42704"),
        ("SELECT * FROM samp TABLESAMPLE BERNOULLI (101)", "2202H"),
        ("SELECT * FROM samp TABLESAMPLE SYSTEM (-1)", "2202H"),
        ("SELECT * FROM samp TABLESAMPLE BERNOULLI (NULL)", "2202H"),
    ];
    for (sql, want) in errors {
        assert!(sqlstate(&engine, sql).await == want, "{sql}");
    }
}

#[tokio::test]
async fn locking_reads_accept_every_strength_and_wait_policy() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE lk (id int4)").await;
    run(&engine, "INSERT INTO lk VALUES (1),(2)").await;
    for sql in [
        "SELECT id FROM lk ORDER BY id FOR UPDATE",
        "SELECT id FROM lk ORDER BY id FOR NO KEY UPDATE",
        "SELECT id FROM lk ORDER BY id FOR SHARE",
        "SELECT id FROM lk ORDER BY id FOR KEY SHARE",
        "SELECT id FROM lk ORDER BY id FOR UPDATE OF lk",
        "SELECT id FROM lk AS t ORDER BY id FOR UPDATE OF t",
        "SELECT id FROM lk ORDER BY id FOR UPDATE NOWAIT",
        "SELECT id FROM lk ORDER BY id FOR UPDATE SKIP LOCKED",
        "SELECT id FROM lk ORDER BY id FOR SHARE OF lk SKIP LOCKED",
    ] {
        assert!(
            cells(&engine, sql).await == cell_rows(&[&["1"], &["2"]]),
            "{sql}"
        );
    }
    // Nothing to lock: PostgreSQL just runs the query.
    assert!(cells(&engine, "SELECT 1 FOR UPDATE").await == cell_rows(&[&["1"]]));
    assert!(
        cells(&engine, "SELECT g FROM generate_series(1, 1) g FOR UPDATE").await
            == cell_rows(&[&["1"]])
    );
    run(&engine, "CREATE SEQUENCE locking_fallback_seq").await;
    assert!(
        cells(
            &engine,
            "SELECT (SELECT nextval('locking_fallback_seq')) \
                 FROM (SELECT 1) d FOR UPDATE",
        )
        .await
            == cell_rows(&[&["1"]])
    );
    assert!(cells(&engine, "SELECT nextval('locking_fallback_seq')").await == cell_rows(&[&["2"]]));

    // EvalPlanQual must not re-run a volatile correlated predicate when the
    // locked tuple is the same version that the statement already tested.
    run(&engine, "CREATE SEQUENCE locking_correlated_seq").await;
    assert!(
        cells(
            &engine,
            "SELECT o.id FROM lk o \
                 WHERE EXISTS (SELECT 1 \
                               WHERE nextval('locking_correlated_seq') > 0 \
                                 AND o.id > 0) \
                 ORDER BY o.id FOR UPDATE",
        )
        .await
            == cell_rows(&[&["1"], &["2"]])
    );
    assert!(
        cells(&engine, "SELECT nextval('locking_correlated_seq')").await == cell_rows(&[&["3"]])
    );
}

#[tokio::test]
async fn locking_refusals_match_postgres_sqlstates() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE lk (id int4)").await;
    run(&engine, "INSERT INTO lk VALUES (1),(2)").await;
    let cases: [(&str, &str); 7] = [
        ("SELECT count(*) FROM lk FOR UPDATE", "0A000"),
        ("SELECT id FROM lk GROUP BY id FOR UPDATE", "0A000"),
        (
            "SELECT id FROM lk GROUP BY id HAVING count(*) > 0 FOR SHARE",
            "0A000",
        ),
        ("SELECT DISTINCT id FROM lk FOR UPDATE", "0A000"),
        ("SELECT id FROM lk UNION SELECT 3 FOR UPDATE", "0A000"),
        ("VALUES (1) FOR UPDATE", "0A000"),
        ("SELECT id FROM lk FOR UPDATE OF nosuch", "42P01"),
    ];
    for (sql, want) in cases {
        assert!(sqlstate(&engine, sql).await == want, "{sql}");
    }
}

fn single_text(result: &[QueryResult]) -> String {
    let [QueryResult::Rows { rows, .. }] = result else {
        panic!("expected rows");
    };
    let [row] = rows.as_slice() else {
        panic!("expected one row");
    };
    let [Some(cell)] = row.as_slice() else {
        panic!("expected one non-null cell");
    };
    String::from_utf8(cell.text.to_vec()).expect("cell is utf8")
}

#[tokio::test]
async fn drop_table_if_exists_skips_missing_table() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let results = run(&engine, "DROP TABLE IF EXISTS missing").await;
    assert!(tag_of(&results[0]) == "DROP TABLE");
}

#[tokio::test]
async fn drop_table_without_if_exists_errors_on_missing_table() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let err = engine
        .connect()
        .simple_query("DROP TABLE missing")
        .await
        .expect_err("missing table without IF EXISTS");
    assert!(err.code == "42P01");
}

#[tokio::test]
async fn drop_role_if_exists_skips_only_a_missing_role() {
    use assert2::assert;
    let engine = SqlEngine::new();

    let results = run(&engine, "DROP ROLE IF EXISTS missing_role").await;
    assert!(tag_of(&results[0]) == "DROP ROLE");
    assert!(sqlstate(&engine, "DROP ROLE missing_role").await == "42704");

    run(&engine, "CREATE ROLE existing_role").await;
    let results = run(&engine, "DROP ROLE IF EXISTS existing_role").await;
    assert!(tag_of(&results[0]) == "DROP ROLE");
    assert!(
        !crabka_pgcatalog::role_exists(engine.catalog_kv(), "existing_role").expect("role lookup")
    );
}

#[tokio::test]
async fn drop_user_after_rule_and_owner_cleanup_terminates() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query(
            "CREATE USER rule_user; \
             CREATE TABLE rule_source (x int4); \
             CREATE TABLE rule_sink (x int4); \
             CREATE VIEW rule_view WITH (security_invoker = true) AS SELECT * FROM rule_source; \
             GRANT INSERT ON rule_view TO rule_user; \
             CREATE RULE rule_insert AS ON INSERT TO rule_view \
                 DO INSTEAD INSERT INTO rule_sink VALUES (NEW.*)",
        )
        .await
        .expect("rule setup");
    session
        .simple_query("SET SESSION AUTHORIZATION rule_user")
        .await
        .expect("become rule user");
    session
        .simple_query("INSERT INTO rule_view VALUES (1)")
        .await
        .expect("rule action uses the view owner's privileges");
    assert!(single_text(&run(&engine, "SELECT x::text FROM rule_sink").await) == "1");
    session
        .simple_query(
            "RESET SESSION AUTHORIZATION; \
             CREATE RULE rule_update AS ON UPDATE TO rule_source \
                 DO INSTEAD INSERT INTO rule_sink VALUES (OLD.*); \
             ALTER TABLE rule_source OWNER TO rule_user; \
             DROP VIEW rule_view; \
             DROP RULE rule_update ON rule_source; \
             DROP TABLE rule_sink; \
             DROP TABLE rule_source; \
             DROP USER rule_user",
        )
        .await
        .expect("drop user after the rule cleanup");
    assert!(!crabka_pgcatalog::role_exists(engine.catalog_kv(), "rule_user").expect("role lookup"));
}

#[tokio::test]
async fn on_conflict_rejects_relations_with_insert_or_update_rules() {
    use assert2::assert;

    let engine = SqlEngine::new();
    for sql in [
        "CREATE TABLE ruled_table (id int4 PRIMARY KEY)",
        "CREATE RULE table_rule AS ON INSERT TO ruled_table DO INSTEAD NOTHING",
        "CREATE VIEW ruled_view AS SELECT * FROM ruled_table",
        "CREATE RULE view_rule AS ON INSERT TO ruled_view DO INSTEAD NOTHING",
    ] {
        run(&engine, sql).await;
    }
    for relation in ["ruled_table", "ruled_view"] {
        let error = engine
            .connect()
            .simple_query(&format!(
                "INSERT INTO {relation} VALUES (1) ON CONFLICT DO NOTHING"
            ))
            .await
            .expect_err("ON CONFLICT rejects rewrite-rule relations");
        assert!(error.code == "0A000");
        assert!(
            error.message
                == "INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules"
        );
    }
}

#[tokio::test]
async fn merge_rejects_enabled_rules_but_allows_disabled_rules() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE merge_ruled (id int4 PRIMARY KEY); \
         CREATE RULE merge_rule AS ON INSERT TO merge_ruled DO INSTEAD NOTHING",
    )
    .await;
    let sql = "MERGE INTO merge_ruled target USING (SELECT 1 AS id) source \
               ON target.id = source.id WHEN NOT MATCHED THEN INSERT VALUES (source.id)";
    let (code, message, detail, hint) = failure(&engine, sql).await;
    assert!(code == "0A000");
    assert!(message == "cannot execute MERGE on relation \"merge_ruled\"");
    assert!(detail == Some("MERGE is not supported for relations with rules.".into()));
    assert!(hint.is_none());

    run(&engine, "ALTER TABLE merge_ruled DISABLE RULE merge_rule").await;
    run(&engine, sql).await;
    assert!(single_text(&run(&engine, "SELECT id::text FROM merge_ruled").await) == "1");
}

#[tokio::test]
async fn on_select_rules_name_a_partitioned_table_in_the_detail() {
    use assert2::assert;

    let engine = SqlEngine::new();
    for (table, create, detail) in [
        (
            "on_select_plain",
            "CREATE TABLE on_select_plain (a int4)",
            "This operation is not supported for tables.",
        ),
        (
            "on_select_partitioned",
            "CREATE TABLE on_select_partitioned (a int4) PARTITION BY LIST (a)",
            "This operation is not supported for partitioned tables.",
        ),
    ] {
        run(&engine, create).await;
        let (code, message, got_detail, hint) = failure(
            &engine,
            &format!("CREATE RULE on_select_rule AS ON SELECT TO {table} DO INSTEAD SELECT 1"),
        )
        .await;
        assert!(code == "42809");
        assert!(message == format!("relation \"{table}\" cannot have ON SELECT rules"));
        assert!(got_detail == Some(detail.into()));
        assert!(hint.is_none());
    }
}

#[tokio::test]
async fn rule_values_require_old_or_new_for_rule_images() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE rule_source (f1 int4); CREATE TABLE rule_sink (f1 int4)",
    )
    .await;
    let error = engine
        .connect()
        .simple_query(
            "CREATE RULE invalid_rule AS ON INSERT TO rule_source \
             DO INSTEAD INSERT INTO rule_sink VALUES (f1)",
        )
        .await
        .expect_err("unqualified rule image is rejected");
    assert!(error.code == "42703");
    assert!(
        error
            .diagnostics
            .as_ref()
            .and_then(|diagnostics| diagnostics.hint.as_deref())
            == Some("Try using a table-qualified name.")
    );
    assert!(
        error.diagnostics.and_then(|diagnostics| diagnostics.detail)
            == Some(
                "There are columns named \"f1\", but they are in tables that cannot be referenced from this part of the query."
                    .into()
            )
    );
    run(
        &engine,
        "CREATE RULE valid_rule AS ON INSERT TO rule_source \
         DO INSTEAD INSERT INTO rule_sink VALUES (NEW.f1); INSERT INTO rule_source VALUES (2)",
    )
    .await;
    assert!(single_text(&run(&engine, "SELECT f1::text FROM rule_sink").await) == "2");
}

#[tokio::test]
async fn insert_rules_run_do_also_with_do_instead_in_name_order() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE rule_source (a int4); CREATE TABLE rule_log (step int4, label text); \
         CREATE SEQUENCE rule_order_seq; \
         CREATE RULE r0 AS ON UPDATE TO rule_source DO \
             INSERT INTO rule_log VALUES (nextval('rule_order_seq'), 'update'); \
         CREATE RULE r3 AS ON INSERT TO rule_source DO INSTEAD \
             INSERT INTO rule_log VALUES (nextval('rule_order_seq'), 'third'); \
         CREATE RULE r4 AS ON INSERT TO rule_source WHERE a < 100 DO INSTEAD \
             INSERT INTO rule_log VALUES (nextval('rule_order_seq'), 'fourth'); \
         CREATE RULE r2 AS ON INSERT TO rule_source DO \
             INSERT INTO rule_log VALUES (nextval('rule_order_seq'), 'second'); \
         CREATE RULE r1 AS ON INSERT TO rule_source DO INSTEAD \
             INSERT INTO rule_log VALUES (nextval('rule_order_seq'), 'first'); \
         INSERT INTO rule_source VALUES (1)",
    )
    .await;
    let results = run(&engine, "SELECT label FROM rule_log ORDER BY step").await;
    let [QueryResult::Rows { rows, .. }] = results.as_slice() else {
        panic!("expected rule rows");
    };
    let labels = rows
        .iter()
        .map(|row| String::from_utf8(row[0].as_ref().expect("label").text.to_vec()).expect("utf8"))
        .collect::<Vec<_>>();
    assert!(labels == ["first", "second", "third", "fourth"]);
    assert!(single_text(&run(&engine, "SELECT count(*) FROM rule_source").await) == "0");
    run(
        &engine,
        "CREATE TABLE view_sink (id int4); CREATE VIEW rule_view AS SELECT * FROM view_sink; \
         CREATE RULE view_insert AS ON INSERT TO rule_view \
         DO INSTEAD INSERT INTO view_sink VALUES (NEW.id); INSERT INTO rule_view VALUES (8)",
    )
    .await;
    assert!(single_text(&run(&engine, "SELECT count(*) FROM view_sink").await) == "1");
}

#[tokio::test]
async fn auto_updatable_views_run_do_also_rules_after_their_base_write() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE rule_source (a int4, b text DEFAULT 'xxx', c int4); \
         CREATE VIEW rule_view AS SELECT * FROM rule_source; \
         CREATE RULE view_insert AS ON INSERT TO rule_view DO ALSO \
             INSERT INTO rule_source SELECT * FROM \
             (SELECT a + 10 FROM rule_source WHERE a = NEW.a) derived; \
         CREATE RULE view_update AS ON UPDATE TO rule_view DO ALSO \
             UPDATE rule_source target SET c = derived.a * 10 FROM \
             (SELECT a FROM rule_source WHERE a = OLD.a) derived \
             WHERE target.a = derived.a; \
         INSERT INTO rule_view VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    assert!(
        text_rows_of(
            &mut engine.connect(),
            "SELECT a::text FROM rule_source ORDER BY a::int4"
        )
        .await
            == vec![
                text_row(&["1"]),
                text_row(&["2"]),
                text_row(&["11"]),
                text_row(&["12"])
            ]
    );
    run(&engine, "UPDATE rule_view SET b = upper(b)").await;
    assert!(
        text_rows_of(
            &mut engine.connect(),
            "SELECT a::text, b, c::text FROM rule_source ORDER BY a::int4"
        )
        .await
            == vec![
                text_row(&["1", "A", "10"]),
                text_row(&["2", "B", "20"]),
                text_row(&["11", "XXX", "110"]),
                text_row(&["12", "XXX", "120"]),
            ]
    );
}

#[tokio::test]
async fn instead_view_rules_report_affected_row_counts() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE rule_target (id int4); \
         CREATE VIEW rule_view AS SELECT * FROM rule_target; \
         CREATE RULE rule_insert AS ON INSERT TO rule_view DO INSTEAD \
             INSERT INTO rule_target VALUES (NEW.id); \
         CREATE RULE rule_update AS ON UPDATE TO rule_view DO INSTEAD \
             UPDATE rule_target SET id = NEW.id WHERE id = OLD.id; \
         CREATE RULE rule_delete AS ON DELETE TO rule_view DO INSTEAD \
             DELETE FROM rule_target WHERE id = OLD.id",
    )
    .await;

    let result = run(&engine, "INSERT INTO rule_view VALUES (1), (2)").await;
    assert!(tag_of(&result[0]) == "INSERT 0 2");
    let result = run(&engine, "UPDATE rule_view SET id = id + 10").await;
    assert!(tag_of(&result[0]) == "UPDATE 2");
    let result = run(&engine, "DELETE FROM rule_view").await;
    assert!(tag_of(&result[0]) == "DELETE 2");
}

#[tokio::test]
async fn view_rule_actions_preserve_inheritance_reach() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE rule_parent (id int4 PRIMARY KEY, name text); \
         CREATE TABLE rule_child () INHERITS (rule_parent); \
         INSERT INTO rule_child VALUES (1, 'before'); \
         CREATE VIEW rule_view AS SELECT * FROM rule_parent ORDER BY id; \
         CREATE RULE rule_update AS ON UPDATE TO rule_view DO INSTEAD \
             UPDATE rule_parent SET name = NEW.name WHERE id = OLD.id",
    )
    .await;
    let result = run(&engine, "UPDATE rule_view SET name = 'after' WHERE id = 1").await;
    assert!(tag_of(&result[0]) == "UPDATE 1");
    assert!(
        text_rows_of(&mut engine.connect(), "SELECT name FROM rule_child").await
            == vec![text_row(&["after"])]
    );
}

#[tokio::test]
async fn comments_on_view_rules_use_the_view_relation_identity() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query(
            "CREATE TABLE rule_sink (id int4); CREATE VIEW rule_view AS SELECT * FROM rule_sink; \
             CREATE RULE view_insert AS ON INSERT TO rule_view \
             DO INSTEAD INSERT INTO rule_sink VALUES (NEW.id); \
             COMMENT ON RULE view_insert ON rule_view IS 'write through view'",
        )
        .await
        .expect("view rule comment");
    let error = session
        .simple_query("COMMENT ON RULE missing_rule ON rule_view IS 'missing'")
        .await
        .expect_err("missing view rule");
    assert!(error.code == "42704");
    assert!(error.message == "rule \"missing_rule\" for relation \"rule_view\" does not exist");
    assert!(
        text_rows_of(
            &mut session,
            "SELECT description FROM pg_description \
             WHERE classoid = 'pg_rewrite'::regclass \
             AND objoid = (SELECT oid FROM pg_rewrite WHERE rulename = 'view_insert')",
        )
        .await
            == vec![text_row(&["write through view"])]
    );
}

#[tokio::test]
async fn correlated_dml_filters_work_for_tables_and_rule_views() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE direct_target (id int4); CREATE TABLE matches (id int4); \
         INSERT INTO direct_target VALUES (1), (2), (3); INSERT INTO matches VALUES (2); \
         DELETE FROM direct_target WHERE EXISTS \
             (SELECT 1 FROM matches WHERE matches.id = direct_target.id)",
    )
    .await;
    assert!(
        single_text(
            &run(
                &engine,
                "SELECT string_agg(id::text, ',' ORDER BY id) FROM direct_target"
            )
            .await
        ) == "1,3"
    );
    run(
        &engine,
        "CREATE TABLE direct_update (id int4); INSERT INTO direct_update VALUES (1), (2), (3); \
         UPDATE direct_update SET id = id + 10 WHERE EXISTS \
             (SELECT 1 FROM matches WHERE matches.id = direct_update.id)",
    )
    .await;
    assert!(
        single_text(
            &run(
                &engine,
                "SELECT string_agg(id::text, ',' ORDER BY id) FROM direct_update"
            )
            .await
        ) == "1,3,12"
    );

    run(
        &engine,
        "CREATE TABLE rule_target (id int4); CREATE TABLE rule_matches (id int4); \
         CREATE VIEW rule_view AS SELECT * FROM rule_target; \
         CREATE RULE delete_rule AS ON DELETE TO rule_view DO INSTEAD \
             DELETE FROM rule_target WHERE id = OLD.id; \
         INSERT INTO rule_target VALUES (1), (2), (3); INSERT INTO rule_matches VALUES (2); \
         DELETE FROM rule_view WHERE EXISTS \
             (SELECT 1 FROM rule_matches WHERE rule_matches.id = rule_view.id)",
    )
    .await;
    assert!(
        single_text(
            &run(
                &engine,
                "SELECT string_agg(id::text, ',' ORDER BY id) FROM rule_target"
            )
            .await
        ) == "1,3"
    );
    assert!(
        single_text(
            &run(
                &engine,
                "SELECT definition FROM pg_rules WHERE rulename = 'delete_rule'"
            )
            .await
        )
        .contains("DELETE")
    );
    run(
        &engine,
        "CREATE TABLE rule_update_target (id int4); \
         CREATE VIEW rule_update_view AS SELECT * FROM rule_update_target; \
         CREATE RULE update_rule AS ON UPDATE TO rule_update_view DO INSTEAD \
             UPDATE rule_update_target SET id = NEW.id WHERE id = OLD.id; \
         INSERT INTO rule_update_target VALUES (1), (2), (3); \
         UPDATE rule_update_view SET id = id + 10 WHERE EXISTS \
             (SELECT 1 FROM rule_matches WHERE rule_matches.id = rule_update_view.id)",
    )
    .await;
    assert!(
        single_text(
            &run(
                &engine,
                "SELECT string_agg(id::text, ',' ORDER BY id) FROM rule_update_target"
            )
            .await
        ) == "1,3,12"
    );
    run(
        &engine,
        "CREATE TABLE rule_action_source (id int4); \
         CREATE RULE update_view_action AS ON INSERT TO rule_action_source DO INSTEAD \
             UPDATE rule_update_view SET id = NEW.id WHERE id = 1 RETURNING *",
    )
    .await;
    assert!(
        single_text(
            &run(
                &engine,
                "SELECT definition FROM pg_rules WHERE rulename = 'update_view_action'"
            )
            .await
        )
        .contains("RETURNING rule_update_view.id")
    );
}

#[tokio::test]
async fn correlated_dml_resolves_uncorrelated_derived_projection_once() {
    use std::time::Duration;

    use assert2::assert;

    let engine = SqlEngine::new();
    let sql = "CREATE TABLE derived_target (name text); \
         CREATE TABLE derived_text (f1 text); \
         CREATE TABLE derived_int8 (q1 int8); \
         CREATE SEQUENCE derived_projection_seq; \
         INSERT INTO derived_target SELECT 'match' FROM generate_series(1, 1024); \
         INSERT INTO derived_text VALUES ('match'), ('miss'); \
         INSERT INTO derived_int8 VALUES (1), (2), (3), (4), (5); \
         DELETE FROM derived_target WHERE EXISTS ( \
           SELECT 1 FROM derived_int8 CROSS JOIN \
             (SELECT f1, ARRAY(SELECT nextval('derived_projection_seq') FROM derived_int8) AS arr \
                FROM derived_text) AS ss \
           WHERE derived_target.name = ss.f1)";
    assert!(
        tokio::time::timeout(Duration::from_secs(2), run(&engine, &sql))
            .await
            .is_ok()
    );
    assert!(single_text(&run(&engine, "SELECT nextval('derived_projection_seq')").await) == "6");
}

#[tokio::test]
async fn conditional_rules_support_correlated_subqueries() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE conditional_rule_target (id int4 PRIMARY KEY, value text); \
         CREATE RULE update_duplicate AS ON INSERT TO conditional_rule_target WHERE EXISTS \
             (SELECT 1 FROM conditional_rule_target AS existing WHERE existing.id = NEW.id) \
             DO INSTEAD UPDATE conditional_rule_target SET value = NEW.value WHERE id = NEW.id; \
         INSERT INTO conditional_rule_target VALUES (1, 'before'); \
         INSERT INTO conditional_rule_target VALUES (1, 'after'); \
         INSERT INTO conditional_rule_target VALUES (2, 'new')",
    )
    .await;
    assert!(
        text_rows_of(
            &mut engine.connect(),
            "SELECT id::text, value FROM conditional_rule_target ORDER BY id"
        )
        .await
            == vec![text_row(&["1", "after"]), text_row(&["2", "new"])]
    );
}

#[tokio::test]
async fn dropping_a_view_cascades_its_rewrite_rules() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE dropped_rule_base (id int4); \
         CREATE VIEW dropped_rule_view AS SELECT * FROM dropped_rule_base; \
         CREATE RULE dropped_rule AS ON INSERT TO dropped_rule_view DO INSTEAD \
             INSERT INTO dropped_rule_base VALUES (NEW.id); \
         DROP TABLE dropped_rule_base CASCADE; \
         CREATE VIEW dropped_rule_view AS VALUES (1)",
    )
    .await;
    assert!(
        text_rows_of(
            &mut engine.connect(),
            "SELECT rulename FROM pg_rules WHERE rulename = 'dropped_rule'"
        )
        .await
        .is_empty()
    );
}

#[tokio::test]
async fn pg_rules_deparses_unqualified_temp_rule_actions() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query(
            "CREATE TEMP TABLE temp_rule_source (id int4); \
             CREATE TEMP TABLE temp_rule_target (id int4); \
             CREATE RULE temp_rule AS ON INSERT TO temp_rule_source DO INSTEAD \
                 INSERT INTO temp_rule_target VALUES (NEW.id) RETURNING *",
        )
        .await
        .expect("temporary rule setup");
    let definition = single_text(
        &session
            .simple_query("SELECT definition FROM pg_rules WHERE rulename = 'temp_rule'")
            .await
            .expect("deparse temporary rule"),
    );
    assert!(definition.contains("INSERT INTO temp_rule_target"));
    assert!(definition.contains("RETURNING temp_rule_target.id"));
}

#[tokio::test]
async fn pg_rules_qualifies_rule_targets_once() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE public_rule_target (id int4); \
         CREATE RULE public_rule AS ON INSERT TO public_rule_target DO INSTEAD NOTHING",
    )
    .await;
    let definition = single_text(
        &run(
            &engine,
            "SELECT definition FROM pg_rules WHERE rulename = 'public_rule'",
        )
        .await,
    );
    assert!(definition.contains("ON INSERT TO public.public_rule_target"));
    let pretty = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'public_rule'",
        )
        .await,
    );
    assert!(pretty.contains("ON INSERT TO public_rule_target"));
    assert!(!pretty.contains("TO public.public_rule_target"));
}

#[tokio::test]
async fn pg_rewrite_links_rules_to_their_pg_class_row() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE rewrite_target (id int4); \
         CREATE RULE rewrite_rule AS ON INSERT TO rewrite_target DO INSTEAD NOTHING",
    )
    .await;
    let result = run(
        &engine,
        "SELECT count(*) FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class \
         WHERE r.rulename = 'rewrite_rule' AND c.relname = 'rewrite_target'",
    )
    .await;
    assert!(single_text(&result) == "1");
}

#[tokio::test]
async fn pg_get_ruledef_expands_old_and_new_wildcards_in_values_actions() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE rule_source (a int4, b int4); \
         CREATE TABLE rule_sink (old_a int4, old_b int4, old_tag text, new_a int4, new_b int4, new_tag text); \
         CREATE RULE rule_update AS ON UPDATE TO rule_source DO INSTEAD \
           INSERT INTO rule_sink VALUES (OLD.*, 'old', NEW.*, 'new'); \
         CREATE TABLE rule_specific_sink (a int4); \
         CREATE RULE rule_specific AS ON UPDATE TO rule_source DO INSTEAD \
           INSERT INTO rule_specific_sink VALUES (OLD.a); \
         CREATE TABLE rule_null_sink (a int4, b int4, label text); \
         CREATE RULE rule_null AS ON INSERT TO rule_source DO INSTEAD \
           INSERT INTO rule_null_sink VALUES (NULL, NULL, '-'), (NULL, NULL, '-'); \
         CREATE VIEW rule_layout_view AS SELECT * FROM rule_source; \
         CREATE TABLE rule_layout_sink (a int4); \
         CREATE RULE rule_layout AS ON INSERT TO rule_layout_view DO INSTEAD \
           INSERT INTO rule_layout_sink VALUES (NEW.a); \
         CREATE TABLE rule_query_sink (a int4, b int4); \
         CREATE RULE rule_query AS ON INSERT TO rule_source DO INSTEAD \
           INSERT INTO rule_query_sink AS sink SELECT NEW.* RETURNING sink.a; \
         CREATE RULE rule_query_all AS ON UPDATE TO rule_source DO INSTEAD \
           INSERT INTO rule_query_sink AS sink SELECT NEW.* RETURNING sink.*; \
         CREATE TABLE rule_return_sink (a int4, b int4, tag text); \
         CREATE RULE rule_return_images AS ON UPDATE TO rule_source DO INSTEAD \
           INSERT INTO rule_return_sink SELECT NEW.* RETURNING NEW.*; \
         CREATE RULE rule_return_target AS ON UPDATE TO rule_source DO INSTEAD \
           INSERT INTO rule_return_sink SELECT NEW.* RETURNING *; \
         CREATE RULE rule_target_update AS ON UPDATE TO rule_source DO INSTEAD \
           UPDATE rule_query_sink AS sink SET a = NEW.a WHERE sink.a = NEW.a; \
         CREATE TABLE rule_multi_sink (a int4[], b int4, tag varchar); \
         CREATE RULE rule_multi AS ON UPDATE TO rule_source DO INSTEAD \
           UPDATE rule_multi_sink AS sink SET (a[1], b, tag) = (SELECT NEW.a, NEW.b, 'updated') \
           WHERE sink.b = NEW.b RETURNING NEW.a, NEW.b; \
         CREATE TABLE rule_plain_sink (a int4); \
         CREATE RULE rule_plain AS ON INSERT TO rule_source DO INSTEAD \
           INSERT INTO rule_plain_sink SELECT 1; \
         CREATE TABLE rule_cte_stage (a int4); \
         CREATE RULE rule_cte AS ON INSERT TO rule_source DO INSTEAD \
           WITH inserted AS (INSERT INTO rule_cte_stage VALUES (1) RETURNING a) \
           INSERT INTO rule_query_sink AS sink SELECT NEW.a, NEW.b FROM inserted RETURNING sink.*; \
         CREATE RULE rule_values AS ON UPDATE TO rule_source DO \
           VALUES (OLD.*, 'old'), (NEW.*, 'new')",
    )
    .await;
    let definition = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'rule_update'",
        )
        .await,
    );
    assert!(definition.contains("old.a, old.b, 'old'::text, new.a, new.b, 'new'::text"));
    let specific = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'rule_specific'",
        )
        .await,
    );
    assert!(specific.contains("VALUES (old.a)"));
    assert!(!specific.contains("old.b"));
    let nulls = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'rule_null'",
        )
        .await,
    );
    assert!(
        nulls.contains("VALUES (NULL::integer, NULL::integer, '-'::text)"),
        "{nulls}"
    );
    let pretty_nulls = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_null'",
        )
        .await,
    );
    assert!(
        pretty_nulls.contains(
            "VALUES (NULL::integer,NULL::integer,'-'::text), (NULL::integer,NULL::integer,'-'::text)"
        ),
        "{pretty_nulls}"
    );
    let layout = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_layout'",
        )
        .await,
    );
    assert!(
        layout.contains("INSERT INTO rule_layout_sink (a)\n  VALUES (new.a)"),
        "{layout}"
    );
    let query = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_query'",
        )
        .await,
    );
    assert!(
        query.contains("INSERT INTO rule_query_sink AS sink (a, b)"),
        "{query}"
    );
    assert!(
        query.contains("SELECT new.a,\n            new.b"),
        "{query}"
    );
    assert!(!query.contains("new.*"), "{query}");
    assert!(query.contains("RETURNING sink.a"), "{query}");
    assert!(!query.contains("insert into"), "{query}");
    let query_all = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_query_all'",
        )
        .await,
    );
    assert!(
        query_all.contains("RETURNING sink.a,\n    sink.b"),
        "{query_all}"
    );
    let return_images = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_return_images'",
        )
        .await,
    );
    assert!(
        return_images.contains("RETURNING new.a,\n    new.b"),
        "{return_images}"
    );
    assert!(!return_images.contains("new.tag"), "{return_images}");
    let compact_return_images = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'rule_return_images'",
        )
        .await,
    );
    assert!(
        compact_return_images.contains("RETURNING new.a,\n    new.b"),
        "{compact_return_images}"
    );
    let return_target = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_return_target'",
        )
        .await,
    );
    assert!(
        return_target.contains(
            "RETURNING rule_return_sink.a,\n    rule_return_sink.b,\n    rule_return_sink.tag"
        ),
        "{return_target}"
    );
    let update = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_target_update'",
        )
        .await,
    );
    assert!(
        update.contains("UPDATE rule_query_sink sink SET a"),
        "{update}"
    );
    assert!(update.contains("WHERE sink.a = new.a"), "{update}");
    assert!(!update.contains("WHERE (sink.a = new.a)"), "{update}");
    let multi = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_multi'",
        )
        .await,
    );
    assert!(
        multi.contains("SET (a[1],") && multi.contains("tag) = ( SELECT new.a,"),
        "{multi}"
    );
    assert!(
        multi.contains("new.b") && multi.contains("RETURNING new.a,\n    new.b"),
        "{multi}"
    );
    let plain = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_plain'",
        )
        .await,
    );
    assert!(plain.contains("SELECT 1 AS \"?column?\""), "{plain}");
    let cte = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid, true) FROM pg_rewrite WHERE rulename = 'rule_cte'",
        )
        .await,
    );
    assert!(
        cte.contains("WITH inserted AS (\n         INSERT INTO rule_cte_stage AS trgt_1 (a)"),
        "{cte}"
    );
    assert!(
        cte.contains("\n          VALUES (1)\n          RETURNING trgt_1.a\n        )"),
        "{cte}"
    );
    assert!(
        cte.contains("\n INSERT INTO rule_query_sink AS sink (a, b)"),
        "{cte}"
    );
    let values = single_text(
        &run(
            &engine,
            "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'rule_values'",
        )
        .await,
    );
    assert!(values.contains("VALUES (old.a, old.b, 'old'::text), (new.a, new.b, 'new'::text)"));
}

#[tokio::test]
async fn view_rules_cannot_be_renamed_to_the_implicit_return_rule() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE rule_target (id int4); \
         CREATE VIEW rule_view AS SELECT * FROM rule_target; \
         CREATE RULE rule_insert AS ON INSERT TO rule_view DO INSTEAD NOTHING",
    )
    .await;
    let error = session
        .simple_query("ALTER RULE rule_insert ON rule_view RENAME TO _RETURN")
        .await
        .expect_err("implicit return rule collision");
    assert!(error.code == "42710");
    assert!(error.message == "rule \"_RETURN\" for relation \"rule_view\" already exists");
}

#[tokio::test]
async fn table_rules_can_be_renamed_to_return() {
    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TABLE rule_table (id int4); \
         CREATE RULE rule_insert AS ON INSERT TO rule_table DO INSTEAD NOTHING; \
         ALTER RULE rule_insert ON rule_table RENAME TO _RETURN",
    )
    .await;
}

#[tokio::test]
async fn pg_rules_deparses_on_conflict_rule_actions() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE conflict_rule_source (name char(10)); \
         CREATE TABLE conflict_rule_target (name char(10) PRIMARY KEY, v char(10), n int4); \
         CREATE RULE conflict_rule AS ON INSERT TO conflict_rule_source DO INSTEAD \
           INSERT INTO conflict_rule_target VALUES (NEW.name, 'candidate') \
           ON CONFLICT (name COLLATE \"C\" text_pattern_ops) WHERE v = 'green' \
           DO UPDATE SET n = '42' WHERE excluded.v <> 'blocked' \
           RETURNING *",
    )
    .await;
    let definition = single_text(
        &run_s(
            &mut session,
            "SELECT definition FROM pg_rules WHERE rulename = 'conflict_rule'",
        )
        .await,
    );
    assert!(definition.contains("ON CONFLICT(name COLLATE \"C\" text_pattern_ops)\n  WHERE"));
    assert!(definition.contains("DO UPDATE SET n = 42"));
    assert!(
        definition.contains("WHERE (v = 'green'::bpchar) DO UPDATE"),
        "{definition}"
    );
    assert!(definition.contains("DO UPDATE SET n = 42\n  WHERE (excluded.v <> 'blocked'::bpchar)"));
    assert!(
        definition.contains(
            "RETURNING conflict_rule_target.name,\n    conflict_rule_target.v,\n    conflict_rule_target.n"
        ),
        "{definition}"
    );
}

#[tokio::test]
async fn multi_table_drop_is_all_or_nothing() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE a (id int4 PRIMARY KEY)").await;
    run(&engine, "CREATE TABLE b (id int4 PRIMARY KEY)").await;

    // A missing name without IF EXISTS aborts the whole drop.
    let err = engine
        .connect()
        .simple_query("DROP TABLE a, missing, b")
        .await
        .expect_err("missing name aborts the whole drop");
    assert!(err.code == "42P01");
    run(&engine, "SELECT count(*) FROM a").await;
    run(&engine, "SELECT count(*) FROM b").await;

    // With IF EXISTS the existing names drop and the missing one is skipped.
    let results = run(&engine, "DROP TABLE IF EXISTS a, missing, b").await;
    assert!(tag_of(&results[0]) == "DROP TABLE");
    for table in ["a", "b"] {
        let err = engine
            .connect()
            .simple_query(&format!("SELECT count(*) FROM {table}"))
            .await
            .expect_err("table was dropped");
        assert!(err.code == "42P01");
    }
}

#[tokio::test]
async fn truncate_empties_multiple_tables_atomically() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE ta (id int4 PRIMARY KEY)").await;
    run(&engine, "CREATE TABLE tb (id int4 PRIMARY KEY)").await;
    run(&engine, "INSERT INTO ta VALUES (1), (2), (3)").await;
    run(&engine, "INSERT INTO tb VALUES (7)").await;

    // A missing name aborts the whole statement before any rows go.
    let err = engine
        .connect()
        .simple_query("TRUNCATE ta, missing, tb")
        .await
        .expect_err("missing table aborts the whole truncate");
    assert!(err.code == "42P01");
    assert!(single_text(&run(&engine, "SELECT count(*) FROM ta").await) == "3");

    let results = run(&engine, "TRUNCATE TABLE ta, tb").await;
    assert!(tag_of(&results[0]) == "TRUNCATE TABLE");
    assert!(single_text(&run(&engine, "SELECT count(*) FROM ta").await) == "0");
    assert!(single_text(&run(&engine, "SELECT count(*) FROM tb").await) == "0");
}

#[tokio::test]
async fn vacuum_is_an_accepted_hint_outside_transactions() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE tv (id int4 PRIMARY KEY)").await;

    let results = run(&engine, "VACUUM ANALYZE tv").await;
    assert!(tag_of(&results[0]) == "VACUUM");

    // PostgreSQL refuses VACUUM inside a transaction block (25001).
    let mut session = engine.connect();
    session.simple_query("BEGIN").await.expect("begin");
    let error = session
        .simple_query("VACUUM")
        .await
        .expect_err("vacuum in a transaction block");
    assert!(error.code == "25001");
}

#[tokio::test]
async fn truncate_rolls_back_inside_a_transaction() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE tr (id int4 PRIMARY KEY)").await;
    run(&engine, "INSERT INTO tr VALUES (1), (2)").await;

    let mut session = engine.connect();
    session.simple_query("BEGIN").await.expect("begin");
    session.simple_query("TRUNCATE tr").await.expect("truncate");
    let counted = session
        .simple_query("SELECT count(*) FROM tr")
        .await
        .expect("count inside txn");
    assert!(single_text(&counted) == "0");
    session.simple_query("ROLLBACK").await.expect("rollback");

    assert!(single_text(&run(&engine, "SELECT count(*) FROM tr").await) == "2");
}

#[tokio::test]
async fn truncate_restart_identity_fails_clear() {
    use assert2::assert;
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE ti (id int4 PRIMARY KEY)").await;
    let err = engine
        .connect()
        .simple_query("TRUNCATE ti RESTART IDENTITY")
        .await
        .expect_err("restart identity is a bounded refusal");
    assert!(err.code == "0A000");
}

#[tokio::test]
async fn drop_sequence_if_exists_skips_missing_sequence() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let results = run(&engine, "DROP SEQUENCE IF EXISTS missing_seq").await;
    assert!(tag_of(&results[0]) == "DROP SEQUENCE");
    let err = engine
        .connect()
        .simple_query("DROP SEQUENCE missing_seq")
        .await
        .expect_err("missing sequence without IF EXISTS");
    assert!(err.code == "42P01");
}

#[tokio::test]
async fn sequence_functions_and_drop_work() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE SEQUENCE s START WITH 10 INCREMENT BY 5")
        .await
        .expect("create sequence");

    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT nextval('s')")
                .await
                .expect("nextval")
        ),
        "10"
    );
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT currval('s')")
                .await
                .expect("currval")
        ),
        "10"
    );
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT nextval('s')")
                .await
                .expect("nextval")
        ),
        "15"
    );
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT setval('s', 40, false)")
                .await
                .expect("setval")
        ),
        "40"
    );
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT nextval('s')")
                .await
                .expect("nextval after setval")
        ),
        "40"
    );

    session.simple_query("DROP SEQUENCE s").await.expect("drop");
    let err = session
        .simple_query("SELECT nextval('s')")
        .await
        .expect_err("dropped sequence");
    assert_eq!(err.code, "42P01");
}

#[tokio::test]
async fn currval_requires_session_nextval() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE SEQUENCE s").await;
    let err = engine
        .connect()
        .simple_query("SELECT currval('s')")
        .await
        .expect_err("currval before nextval");
    assert_eq!(err.code, "55000");
}

#[tokio::test]
async fn sequence_bounds_and_cycle_are_enforced() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE SEQUENCE bounded START WITH 2 MAXVALUE 3 NO CYCLE")
        .await
        .expect("create bounded");
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT nextval('bounded')")
                .await
                .expect("n1")
        ),
        "2"
    );
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT nextval('bounded')")
                .await
                .expect("n2")
        ),
        "3"
    );
    let err = session
        .simple_query("SELECT nextval('bounded')")
        .await
        .expect_err("limit");
    assert_eq!(err.code, "2200H");

    session
        .simple_query("CREATE SEQUENCE cyc START WITH 2 MAXVALUE 3 CYCLE")
        .await
        .expect("create cycle");
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT nextval('cyc')")
                .await
                .expect("c1")
        ),
        "2"
    );
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT nextval('cyc')")
                .await
                .expect("c2")
        ),
        "3"
    );
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT nextval('cyc')")
                .await
                .expect("c3")
        ),
        "1"
    );
}

#[tokio::test]
async fn serial_insert_default_uses_backing_sequence() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE t (id serial, name text)")
        .await
        .expect("create serial table");
    session
        .simple_query("INSERT INTO t (name) VALUES ('a'), ('b')")
        .await
        .expect("insert defaults");
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT id FROM t ORDER BY id LIMIT 1")
                .await
                .expect("select")
        ),
        "1"
    );
    assert_eq!(
        single_text(
            &session
                .simple_query("SELECT currval('t_id_seq')")
                .await
                .expect("currval")
        ),
        "2"
    );
}

#[tokio::test]
async fn insert_then_count_via_kv() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, name text)").await;
    let r = run(&engine, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;
    assert_eq!(
        r,
        vec![QueryResult::Command {
            tag: "INSERT 0 2".into()
        }]
    );
    // A third single-row insert with explicit columns.
    let r = run(&engine, "INSERT INTO t (name, id) VALUES ('c', 3)").await;
    assert_eq!(
        r,
        vec![QueryResult::Command {
            tag: "INSERT 0 1".into()
        }]
    );
}

#[tokio::test]
async fn insert_writes_a_versioned_row_visible_to_select() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4)").await;
    run(&engine, "INSERT INTO t VALUES (1)").await;
    let r = &run(&engine, "SELECT id FROM t").await[0];
    assert_eq!(rows_of(r).len(), 1);
}

#[tokio::test]
async fn insert_widens_int4_to_int8_column() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (big int8)").await;
    run(&engine, "INSERT INTO t VALUES (5)").await;
    // Round-trips through SELECT in Task 17; here just assert no error.
}

#[tokio::test]
async fn insert_type_mismatch_is_42804() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (flag bool)").await;
    let err = engine
        .connect()
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect_err("mismatch");
    assert_eq!(err.code, "42804");
}

#[tokio::test]
#[allow(non_snake_case)]
async fn insert_into_missing_table_is_42P01() {
    let engine = SqlEngine::new();
    let err = engine
        .connect()
        .simple_query("INSERT INTO nope VALUES (1)")
        .await
        .expect_err("no table");
    assert_eq!(err.code, "42P01");
}

#[tokio::test]
/// A short `VALUES` row is legal without a column list, because PostgreSQL
/// fills the trailing columns from their defaults. But a statement that
/// names more target columns than there are expressions is `42601`. Both
/// were verified against the 18.4
/// oracle; this test previously asserted `42804` for the legal form.
async fn insert_row_shorter_than_the_table_fills_defaults() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (a int4, b int4)").await;

    // The legal form must not error, and the unnamed column must be NULL.
    run(&engine, "INSERT INTO t VALUES (1)").await;
    let rows = run(&engine, "SELECT a, b IS NULL FROM t").await;
    assert!(
        matches!(
            rows.as_slice(),
            [QueryResult::Rows { rows, .. }]
                if rows.len() == 1 && rows[0].len() == 2
        ),
        "one row of two columns: {rows:?}"
    );

    let err = engine
        .connect()
        .simple_query("INSERT INTO t (a, b) VALUES (1)")
        .await
        .expect_err("more target columns than expressions");
    assert!(err.code == "42601");
}

#[tokio::test]
async fn create_then_drop_table() {
    let engine = SqlEngine::new();
    let r = run(&engine, "CREATE TABLE t (id int4, name text)").await;
    assert_eq!(
        r,
        vec![QueryResult::Command {
            tag: "CREATE TABLE".into()
        }]
    );
    // Re-creating is a duplicate error (42P07), session survives.
    let err = engine
        .connect()
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect_err("dup");
    assert_eq!(err.code, "42P07");
    let r = run(&engine, "DROP TABLE t").await;
    assert_eq!(
        r,
        vec![QueryResult::Command {
            tag: "DROP TABLE".into()
        }]
    );
    let err = engine
        .connect()
        .simple_query("DROP TABLE t")
        .await
        .expect_err("gone");
    assert_eq!(err.code, "42P01");
}

#[tokio::test]
async fn empty_query_yields_empty_result() {
    let engine = SqlEngine::new();
    assert_eq!(run(&engine, "   ").await, vec![QueryResult::Empty]);
}

#[tokio::test]
async fn syntax_error_is_42601() {
    let engine = SqlEngine::new();
    let err = engine
        .connect()
        .simple_query("SELCT 1")
        .await
        .expect_err("syntax");
    assert_eq!(err.code, "42601");
}

#[tokio::test]
async fn describe_select_returns_field_types_without_executing() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4, name text)").await;
    let fields = engine
        .connect()
        .test_describe("SELECT id, name FROM t")
        .await
        .expect("describe");
    assert_eq!(
        fields.iter().map(|f| f.type_oid).collect::<Vec<_>>(),
        vec![crabka_pgtypes::oids::INT4, crabka_pgtypes::oids::TEXT]
    );
}

#[tokio::test]
async fn describe_non_select_has_no_fields() {
    let engine = SqlEngine::new();
    let fields = engine
        .connect()
        .test_describe("CREATE TABLE t (id int4)")
        .await
        .expect("describe");
    assert!(fields.is_empty());
}

#[tokio::test]
async fn describe_set_op_returns_first_branch_fields() {
    // Schema-only: a set-op query reports the first branch's column name(s) and
    // the unified type, without executing.
    let engine = SqlEngine::new();
    let fields = engine
        .connect()
        .test_describe("SELECT 1 AS x UNION SELECT 2")
        .await
        .expect("describe");
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name, "x"); // name from the FIRST branch
}

#[tokio::test]
async fn describe_set_op_unifies_branch_types() {
    // The Describe path must run cross-branch type unification: int4 ∪ int8 → int8.
    let engine = SqlEngine::new();
    let fields = engine
        .connect()
        .test_describe("SELECT 1 AS x UNION SELECT 2::int8")
        .await
        .expect("describe");
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name, "x");
    assert_eq!(fields[0].type_oid, crabka_pgtypes::ColumnType::Int8.oid());
}

#[tokio::test]
async fn two_inserts_are_both_visible() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4)").await;
    run(&engine, "INSERT INTO t VALUES (1)").await;
    run(&engine, "INSERT INTO t VALUES (2)").await;
    let r = &run(&engine, "SELECT id FROM t ORDER BY id").await[0];
    assert_eq!(rows_of(r).len(), 2);
}

#[tokio::test]
async fn select_on_empty_table_sees_no_rows() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4)").await;
    let r = &run(&engine, "SELECT id FROM t").await[0];
    assert_eq!(rows_of(r).len(), 0);
}

fn tag_of(r: &QueryResult) -> String {
    match r {
        QueryResult::Command { tag } => tag.clone(),
        other => panic!("expected Command, got {other:?}"),
    }
}

#[tokio::test]
async fn select_for_update_returns_rows() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4)").await;
    run(&engine, "INSERT INTO t VALUES (1),(2),(3)").await;
    let r = &run(
        &engine,
        "SELECT id FROM t WHERE id > 1 ORDER BY id FOR UPDATE",
    )
    .await[0];
    assert_eq!(rows_of(r).len(), 2);
}

#[tokio::test]
async fn for_update_in_txn_then_commit_releases() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4)").await;
    run(&engine, "INSERT INTO t VALUES (1)").await;
    let mut s = engine.connect();
    run_s(&mut s, "BEGIN").await;
    run_s(&mut s, "SELECT id FROM t FOR UPDATE").await; // takes a lock
    run_s(&mut s, "COMMIT").await; // must release; no hang
    // a fresh autocommit update of the same row must not block
    let r = run(&engine, "UPDATE t SET id = 9 WHERE id = 1").await;
    assert_eq!(tag_of(&r[0]), "UPDATE 1");
}

/// Regression test: `eval_plan_qual` must resolve a `Prepared(LA → g)` deleter
/// against the CURRENT global clog (via `settled_global`), NOT the writer's
/// pre-lock global snapshot (`gsnap`), which may still list `g` as in-flight.
///
/// Scenario (reconstructed without concurrency):
///   - Cross-range txn `LA` (local xid on this range) UPDATE-committed row R
///     from value 100 (v1) to value 70 (v2), leaving local clog entry
///     `LA → Prepared(g1)` and global clog entry `g1 → Committed`.
///   - Writer W took its global snapshot BEFORE `g1` was committed, so that
///     snapshot still lists `g1` as in-flight (stale gsnap).
///   - W now holds the row lock and calls `eval_plan_qual`.
///
/// With the fix (`settled_global`): `resolve(LA) == Committed` and the
/// snapshot already sees LA, so EvalPlanQual returns v2 (value 70).
///
/// Without the fix (using `gsnap` for resolve):
///   `resolve(LA) == InProgress` (g1 still in-doubt in stale gsnap) →
///   `changed_since_snapshot == false` → `find_visible_one` with stale snapshot
///   sees v1 as live (xmax=LA appears uncommitted) → returns v1 (value 100).
///   Lost update across the 2PC boundary.
#[test]
fn eval_plan_qual_settled_global_sees_committed_cross_range_version() {
    use std::sync::Arc;

    use crabka_pgcatalog::{Column, Table};
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgmvcc::{
        clog::{XidStatus, put_op},
        version::{encode_tuple, version_key_xid},
        visibility::Snapshot,
        xid::{FIRST_NORMAL_XID, GLOBAL_XID_BASE, INVALID_XID},
    };
    use crabka_pgtypes::{ColumnType, Datum};

    use super::eval_plan_qual;

    // ── xid assignments ─────────────────────────────────────────────────────
    let x0: u64 = FIRST_NORMAL_XID; // original inserter — settled, committed
    let la: u64 = FIRST_NORMAL_XID + 1; // cross-range txn's local xid (Prepared)
    let g1: u64 = GLOBAL_XID_BASE + 1; // global txn id
    let writer: u64 = FIRST_NORMAL_XID + 2; // writer calling eval_plan_qual

    // ── stores ──────────────────────────────────────────────────────────────
    // `kv` holds both the data range's row versions AND the local clog.
    // `global` holds only range-0's global clog.
    let kv = Arc::new(MemKv::new());
    let global = MemKv::new();

    // ── catalog table ────────────────────────────────────────────────────────
    // Table id 1, single int4 column "val".
    let table = Table {
        id: 1,
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        name: RelationName::public("t"),
        columns: vec![Column::new("val", ColumnType::Int4)],
        sharded: false,
        row_security: false,
        force_row_security: false,
        sharding: None,
        foreign: None,
        materialized: None,
        checks: Vec::new(),
    };
    let rowid: u64 = 1;

    // ── write two versions of row R ──────────────────────────────────────────
    // v1: created by x0, deleted (xmax) by la — value 100 (the old row)
    kv.write_batch(&[crabka_pgkv::WriteOp::Put {
        key: version_key_xid(table.id, rowid, x0),
        value: encode_tuple(x0, la, &[Datum::Int4(100)]),
    }])
    .expect("write v1");
    // v2: created by la, live (xmax=INVALID_XID) — value 70 (the updated row)
    kv.write_batch(&[crabka_pgkv::WriteOp::Put {
        key: version_key_xid(table.id, rowid, la),
        value: encode_tuple(la, INVALID_XID, &[Datum::Int4(70)]),
    }])
    .expect("write v2");

    // ── local clog in `kv` ───────────────────────────────────────────────────
    // x0 is settled-committed.  la is in Prepared state → g1.
    kv.write_batch(&[
        put_op(x0, XidStatus::Committed),
        put_op(la, XidStatus::Prepared(g1)),
    ])
    .expect("write local clog");

    // ── global clog in `global` ──────────────────────────────────────────────
    // g1 has committed — but writer's global snapshot is stale (lists g1 as
    // in-flight), so eval_plan_qual MUST use settled_global, not stale_gsnap.
    global
        .write_batch(&[put_op(g1, XidStatus::Committed)])
        .expect("write global clog");

    // ── stale global snapshot (what the writer held pre-lock) ────────────────
    // g1 is listed as in-flight — this is the bug trigger.
    // NOTE: eval_plan_qual no longer accepts gsnap as a parameter (the fix
    // bakes settled_global internally), so this snapshot is used as the
    // *local* snapshot below, which represents the writer's view of local xids.
    // The global staleness is expressed via the local clog's Prepared marker.

    // ── procarray: writer is running; x0 and la are not ────────────────────
    // The fresh snapshot produced by procarray.snapshot() inside eval_plan_qual
    // will have xmax=writer+1, xip=[writer] — so la is below xmax and not in xip,
    // meaning satisfies_mvcc will ask the clog for la → Prepared(g1) →
    // settled_global → Committed → v2 visible. Correct.
    let procarray = crate::procarray::ProcArray::open(
        Arc::clone(&kv) as Arc<dyn crabka_pgkv::Kv>,
        crate::PersistMode::Durable,
    )
    .expect("procarray open");
    // Advance next_xid past x0, la, and writer by allocating writer's slot.
    let _xid_x0 = procarray.begin_write().expect("alloc x0 slot");
    let _xid_la = procarray.begin_write().expect("alloc la slot");
    let _xid_w = procarray.begin_write().expect("alloc writer slot");
    assert_eq!((_xid_x0, _xid_la, _xid_w), (x0, la, writer));
    // Mark x0 and la as finished (committed) so they are not in the running set.
    procarray.finish(_xid_x0);
    procarray.finish(_xid_la);
    // writer (xid=3) remains running.

    // ── local (txn) snapshot for the writer ─────────────────────────────────
    // Taken when the writer began. At that time la (xid=2) was still running
    // in the local sense because the Prepared marker hadn't been removed yet.
    // NOTE: in the real 2PC path la is deregistered from procarray at prepare,
    // so in practice it would not appear in xip here; but eval_plan_qual's
    // staleness bug is about the GLOBAL snapshot, not the local one. We make
    // la visible in the local snapshot to keep the test simple and focused:
    // x0 is settled (xid < xmax, not in xip) and la is settled too (same).
    // The critical stale element is the global clog Prepared → g1-in-doubt path,
    // which is exercised via the kv local-clog entry `la → Prepared(g1)`.
    //
    // Writer's local snapshot: xmax = writer, only writer in xip.
    // x0 and la are below xmax and not in xip → settled.
    // This is the snapshot held when the writer started, BEFORE it blocked on
    // the row lock. la's Prepared(g1) status makes g1 the relevant global txn.
    let writer_snapshot = Snapshot {
        xmin: writer,
        xmax: writer,      // writer itself started after x0 and la settled locally
        xip: vec![writer], // writer is the only running local txn
    };

    // ── call eval_plan_qual ──────────────────────────────────────────────────
    // With the fix: eval_plan_qual uses settled_global internally, so:
    //   resolve(la) → Prepared(g1) → g1 not in-doubt in settled_global → Committed
    //   changed_since_snapshot: xmax=la, la != INVALID_XID, la != writer,
    //     resolve(la)==Committed, !snapshot_can_see(writer_snapshot, la).
    //   snapshot_can_see(writer_snapshot, la): la=2 < xmax=3, la not in xip=[3]
    //     → la IS visible → snapshot_can_see = true → !true = false → NOT changed.
    //
    // Wait — if la is visible in writer_snapshot, changed_since_snapshot is false,
    // so we go to find_visible_one with writer_snapshot and settled_global.
    // With settled_global: resolve(la) = Committed.
    // v1: xmin=x0 (committed, visible), xmax=la (committed-visible) → NOT visible.
    // v2: xmin=la (committed-visible), xmax=INVALID_XID → visible. Returns v2. Correct.
    //
    // Without the fix (using stale gsnap where g1 is in-doubt):
    //   resolve(la) → Prepared(g1) → g1 in-doubt → InProgress
    //   changed_since_snapshot: resolve(la)==InProgress, not Committed → false
    //   find_visible_one with writer_snapshot and stale resolver:
    //     v1: xmin=x0 visible, xmax=la → resolve(la)=InProgress → not committed
    //         → xmax not committed-visible → v1 appears live → visible!
    //     v2: xmin=la → committed_visible(la): la not own, la < xmax, not in xip
    //         → NOT running → asks status: InProgress → NOT committed → v2 invisible
    //   Returns v1 (value 100). Bug.
    let result = eval_plan_qual(
        &super::MutationContext {
            kv: kv.as_ref(),
            global: &global,
            procarray: &procarray,
            snapshot: &writer_snapshot,
            xid: writer,
            command_id: None,
            repeatable_read: true,
            eval_ctx: &crate::clock::EvalCtx::test_default(),
        },
        &table,
        rowid,
        crate::scope::GeneratedReads::every(),
    )
    .expect("eval_plan_qual must not error");

    // The fix: must see v2 (xmin=la, value=70), NOT v1 (value=100).
    let (_ret_rowid, _ret_key_xid, ret_xmin, _ret_cmin, _ret_cmax, ret_row) =
        result.expect("must find a version (not None)");
    assert_eq!(
        ret_xmin, la,
        "eval_plan_qual must return the cross-range committed version (xmin=la={la}), \
             not the stale pre-commit version (xmin=x0={x0})"
    );
    assert_eq!(
        ret_row,
        vec![Datum::Int4(70)],
        "eval_plan_qual must return value 70 (cross-range committed UPDATE result), \
             not value 100 (the stale pre-2PC-commit row) — lost-update bug"
    );
}

#[test]
fn eval_plan_qual_hides_own_current_command_delete() {
    use std::sync::Arc;

    use crabka_pgcatalog::{Column, Table};
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgmvcc::{
        clog::{XidStatus, put_op},
        version::{encode_tuple_with_command_ids, version_key_xid},
    };
    use crabka_pgtypes::{ColumnType, Datum};

    use super::eval_plan_qual;

    let kv = Arc::new(MemKv::new());
    let procarray = crate::procarray::ProcArray::open(
        Arc::clone(&kv) as Arc<dyn crabka_pgkv::Kv>,
        crate::PersistMode::Durable,
    )
    .expect("procarray open");
    let original = procarray.begin_write().expect("original xid");
    let writer = procarray.begin_write().expect("writer xid");
    procarray.finish(original);
    let table = Table {
        id: 1,
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        name: RelationName::public("t"),
        columns: vec![Column::new("val", ColumnType::Int4)],
        sharded: false,
        row_security: false,
        force_row_security: false,
        sharding: None,
        foreign: None,
        materialized: None,
        checks: Vec::new(),
    };
    kv.write_batch(&[
        crabka_pgkv::WriteOp::Put {
            key: version_key_xid(table.id, 1, original),
            value: encode_tuple_with_command_ids(original, writer, 0, 0, &[Datum::Int4(42)]),
        },
        crabka_pgkv::WriteOp::Put {
            key: version_key_xid(table.id, 2, original),
            value: encode_tuple_with_command_ids(original, 0, 0, 0, &[Datum::Int4(7)]),
        },
        put_op(original, XidStatus::Committed),
        put_op(writer, XidStatus::Committed),
    ])
    .expect("write rows");

    let snapshot = procarray.snapshot();
    let eval_ctx = crate::clock::EvalCtx::test_default();
    let mutation = super::MutationContext {
        kv: kv.as_ref(),
        global: kv.as_ref(),
        procarray: &procarray,
        snapshot: &snapshot,
        xid: writer,
        command_id: Some(1),
        repeatable_read: true,
        eval_ctx: &eval_ctx,
    };
    let deleted = eval_plan_qual(&mutation, &table, 1, crate::scope::GeneratedReads::every())
        .expect("eval plan qual");
    let live = eval_plan_qual(&mutation, &table, 2, crate::scope::GeneratedReads::every())
        .expect("eval plan qual");

    assert_eq!(deleted, None);
    assert_eq!(
        live.map(|(_, _, _, _, _, row)| row),
        Some(vec![Datum::Int4(7)])
    );
}

#[test]
fn eval_plan_qual_restarts_only_for_a_newly_committed_change() {
    use std::sync::Arc;

    use crabka_pgcatalog::{Column, Table};
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgmvcc::{
        clog::{XidStatus, put_op},
        version::{encode_tuple, version_key_xid},
    };
    use crabka_pgtypes::{ColumnType, Datum};

    use super::eval_plan_qual;

    let kv = Arc::new(MemKv::new());
    let procarray = crate::procarray::ProcArray::open(
        Arc::clone(&kv) as Arc<dyn crabka_pgkv::Kv>,
        crate::PersistMode::Durable,
    )
    .expect("procarray open");
    let original = procarray.begin_write().expect("original xid");
    procarray.finish(original);
    let writer = procarray.begin_write().expect("writer xid");
    let snapshot = procarray.snapshot();
    let completed = procarray.begin_write().expect("completed xid");
    procarray.finish(completed);
    let pending = procarray.begin_write().expect("pending xid");
    let table = Table {
        id: 1,
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        name: RelationName::public("t"),
        columns: vec![Column::new("val", ColumnType::Int4)],
        sharded: false,
        row_security: false,
        force_row_security: false,
        sharding: None,
        foreign: None,
        materialized: None,
        checks: Vec::new(),
    };
    kv.write_batch(&[
        crabka_pgkv::WriteOp::Put {
            key: version_key_xid(table.id, 1, original),
            value: encode_tuple(original, completed, &[Datum::Int4(1)]),
        },
        crabka_pgkv::WriteOp::Put {
            key: version_key_xid(table.id, 2, original),
            value: encode_tuple(original, pending, &[Datum::Int4(2)]),
        },
        put_op(original, XidStatus::Committed),
        put_op(completed, XidStatus::Committed),
        put_op(pending, XidStatus::InProgress),
    ])
    .expect("write rows");

    let eval_ctx = crate::clock::EvalCtx::test_default();
    let mutation = super::MutationContext {
        kv: kv.as_ref(),
        global: kv.as_ref(),
        procarray: &procarray,
        snapshot: &snapshot,
        xid: writer,
        command_id: None,
        repeatable_read: true,
        eval_ctx: &eval_ctx,
    };
    assert!(matches!(
        eval_plan_qual(&mutation, &table, 1, crate::scope::GeneratedReads::every()),
        Err(ExecError::SerializationFailure)
    ));
    let unchanged = eval_plan_qual(&mutation, &table, 2, crate::scope::GeneratedReads::every())
        .expect("in-progress update does not restart");
    assert_eq!(
        unchanged.map(|(_, _, _, _, _, row)| row),
        Some(vec![Datum::Int4(2)])
    );
}

/// SP21: after a fresh-`g'` re-attempt, a row has TWO physical versions: the
/// abandoned attempt's `Prepared(Li_old -> g)` with `g` Aborted, and the re-attempt's
/// `Prepared(Li_new -> g')` with `g'` Committed. `find_visible_one` must return the
/// committed-`g'` version (highest xmin) and never the aborted shadow; exactly one
/// version is live (the assert holds).
#[test]
fn find_visible_one_returns_committed_reattempt_over_aborted_shadow() {
    use std::sync::Arc;

    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgmvcc::{
        clog::{XidStatus, put_op},
        visibility::Snapshot,
        xid::{GLOBAL_XID_BASE, INVALID_XID},
    };
    use crabka_pgtypes::Datum;

    use super::{find_visible_one, global_status};

    let li_old: u64 = 5; // abandoned attempt's local xid
    let li_new: u64 = 9; // re-attempt's local xid (reseed -> strictly greater)
    let g: u64 = GLOBAL_XID_BASE + 1; // abandoned global xid (Aborted)
    let g2: u64 = GLOBAL_XID_BASE + 2; // fresh global xid (Committed)

    let kv = Arc::new(MemKv::new()); // holds the local clog
    let global = MemKv::new(); // range-0 global clog

    // `find_visible_one` reads ONLY the passed `versions` slice + the local/global clogs
    // (it never touches the kv row-version store), so seed just the two clogs here.
    // Local clog: both local xids are Prepared, deref to the global clog.
    kv.write_batch(&[
        put_op(li_old, XidStatus::Prepared(g)),
        put_op(li_new, XidStatus::Prepared(g2)),
    ])
    .expect("local clog");
    // Global clog: g Aborted (abandoned), g2 Committed (re-attempt).
    global
        .write_batch(&[
            put_op(g, XidStatus::Aborted),
            put_op(g2, XidStatus::Committed),
        ])
        .expect("global clog");

    // A settled snapshot: every xid is settled, so global_status reads the global clog.
    let settled = Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    };
    // The two physical versions, both live (xmax = INVALID): old value 100, new value 70.
    let versions = vec![
        (li_old, INVALID_XID, vec![Datum::Int4(100)]),
        (li_new, INVALID_XID, vec![Datum::Int4(70)]),
    ];
    let got = find_visible_one(
        kv.as_ref(),
        &global,
        &settled,
        &settled,
        None,
        None,
        &versions,
    )
    .expect("find_visible_one ok")
    .expect("a version is visible");
    assert_eq!(
        got.0, li_new,
        "the committed re-attempt version (highest xmin) wins"
    );
    assert_eq!(
        got.1,
        vec![Datum::Int4(70)],
        "value is the re-attempt's, not the aborted shadow's"
    );
    // Sanity: the aborted shadow really is invisible under this resolver.
    let resolve = global_status(kv.as_ref(), &global, &settled);
    assert!(matches!(resolve(li_old), Ok(XidStatus::Aborted)));
}

/// The explicit highest-xmin selection is order-independent, and the at-most-one-live
/// invariant is debug-asserted. Two committed, non-deleted versions of one row are an
/// artificial invariant violation: in DEBUG the assert fires (`should_panic`); in
/// RELEASE the assert is compiled out and the greater xmin is returned regardless of
/// the order the versions are presented.
///
/// Debug-profile-dependent BY DESIGN: this repo's CI runs `cargo nextest` and
/// `cargo llvm-cov nextest` in the debug profile, so the `debug_assert!` fires and the
/// `should_panic` arm is exercised. Introducing a release/opt test profile would flip
/// the expectation and require revisiting this `cfg_attr`.
#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "at-most-one-live"))]
fn find_visible_one_orders_by_xmin_and_flags_multiple_live() {
    use std::sync::Arc;

    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgmvcc::{
        clog::{XidStatus, put_op},
        visibility::Snapshot,
        xid::INVALID_XID,
    };
    use crabka_pgtypes::Datum;

    use super::find_visible_one;

    let kv = Arc::new(MemKv::new());
    let global = MemKv::new();
    kv.write_batch(&[
        put_op(5, XidStatus::Committed),
        put_op(9, XidStatus::Committed),
    ])
    .expect("clog");
    let settled = Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    };

    // Present them in DESCENDING order so last-wins would pick the LOWER xmin; the
    // explicit max must still pick 9.
    let versions = vec![
        (9u64, INVALID_XID, vec![Datum::Int4(70)]),
        (5u64, INVALID_XID, vec![Datum::Int4(100)]),
    ];
    let got = find_visible_one(
        kv.as_ref(),
        &global,
        &settled,
        &settled,
        None,
        None,
        &versions,
    )
    .expect("ok"); // only reached in release builds
    assert_eq!(
        got.expect("visible").0,
        9,
        "highest xmin regardless of presentation order"
    );
}

// ───────────────────────── SP40 Task 14: pushdown ─────────────────────────

mod pushdown {
    use std::sync::{Arc, Mutex};

    use crabka_pgcatalog::{ForeignServer, Table, UserMapping};
    use crabka_pgtypes::Datum;
    use crabka_pgwire::engine::{Engine, QueryResult, Session};

    use crate::{
        SqlEngine,
        clock::EvalCtx,
        error::ExecError,
        exec::extract_scan_bounds,
        foreign::{ForeignScanner, ImportFilter, ScanBounds},
    };

    /// Parse `where_sql` into a WHERE [`Expr`] and run it through
    /// `extract_scan_bounds`. The argument is the predicate text only.
    fn bounds_of(where_sql: &str) -> ScanBounds {
        let expr = crabka_pgparser::parser::parse_expr_for_test(where_sql)
            .expect("the WHERE predicate parses");
        extract_scan_bounds(Some(&expr))
    }

    #[test]
    fn partition_and_lower_bound_pushes_inclusive_start() {
        let b = bounds_of("_partition = 0 AND _offset >= 10");
        assert_eq!(b.start_offsets, vec![(0, 10)]);
        assert!(b.end_offsets.is_empty());
    }

    #[test]
    fn partition_and_upper_strict_pushes_exclusive_end() {
        // `_offset < 50` → exclusive end 50 (unchanged).
        let b = bounds_of("_partition = 1 AND _offset < 50");
        assert!(b.start_offsets.is_empty());
        assert_eq!(b.end_offsets, vec![(1, 50)]);
    }

    #[test]
    fn between_pushes_inclusive_start_and_exclusive_end_plus_one() {
        // BETWEEN bounds are inclusive: [5, 9] → start 5, exclusive end 10.
        let b = bounds_of("_partition = 2 AND _offset BETWEEN 5 AND 9");
        assert_eq!(b.start_offsets, vec![(2, 5)]);
        assert_eq!(b.end_offsets, vec![(2, 10)]);
    }

    #[test]
    fn strict_lower_and_inclusive_upper_apply_exclusivity_correctly() {
        // `_offset > 7` → start 8; `_offset <= 20` → exclusive end 21.
        let b = bounds_of("_partition = 3 AND _offset > 7 AND _offset <= 20");
        assert_eq!(b.start_offsets, vec![(3, 8)]);
        assert_eq!(b.end_offsets, vec![(3, 21)]);
    }

    #[test]
    fn reversed_operand_order_is_normalized() {
        // `10 <= _offset` ≡ `_offset >= 10`; `50 > _offset` ≡ `_offset < 50`.
        let b = bounds_of("_partition = 0 AND 10 <= _offset AND 50 > _offset");
        assert_eq!(b.start_offsets, vec![(0, 10)]);
        assert_eq!(b.end_offsets, vec![(0, 50)]);
    }

    #[test]
    fn timestamp_predicate_is_not_pushed() {
        // `_timestamp` cannot be represented in ScanBounds — stays residual.
        let b = bounds_of("_partition = 0 AND _timestamp > '2020-01-01'");
        assert_eq!(b, ScanBounds::default());
    }

    #[test]
    fn non_envelope_predicate_is_not_pushed() {
        let b = bounds_of("_partition = 0 AND id = 42");
        // The partition anchor exists but no offset bound → empty bounds.
        assert_eq!(b, ScanBounds::default());
    }

    #[test]
    fn bare_offset_without_partition_is_not_pushed() {
        // No `_partition =` to scope the offset to → cannot push.
        let b = bounds_of("_offset >= 10");
        assert_eq!(b, ScanBounds::default());
    }

    #[test]
    fn no_filter_yields_default_bounds() {
        assert_eq!(extract_scan_bounds(None), ScanBounds::default());
    }

    /// A scanner that RECORDS every `ScanBounds` it is handed and returns a
    /// fixed corpus of rows IGNORING the bounds. So a result-equivalence test
    /// proves the residual WHERE still filters, and a recording test proves the
    /// pushed bounds reached the scan.
    struct RecordingScanner {
        seen: Arc<Mutex<Vec<ScanBounds>>>,
        /// Fixed (partition, offset, value) corpus, returned verbatim.
        corpus: Vec<(i32, i64, i64)>,
    }

    impl ForeignScanner for RecordingScanner {
        fn scan(
            &self,
            table: &Table,
            _server: &ForeignServer,
            _mapping: Option<&UserMapping>,
            bounds: &ScanBounds,
            _ctx: &EvalCtx,
        ) -> Result<Vec<Vec<Datum>>, ExecError> {
            self.seen.lock().expect("lock").push(bounds.clone());
            // Envelope columns then one value column `v`; deliberately ignore
            // `bounds` to prove the residual WHERE re-filters.
            assert_eq!(table.columns.len(), 6, "5 envelope cols + value `v`");
            Ok(self
                .corpus
                .iter()
                .map(|&(p, off, v)| {
                    vec![
                        Datum::Int4(p),
                        Datum::Int8(off),
                        Datum::Null, // _timestamp
                        Datum::Null, // _key
                        Datum::Null, // _headers
                        Datum::Int8(v),
                    ]
                })
                .collect())
        }

        fn import_schema(
            &self,
            _server: &ForeignServer,
            _mapping: Option<&UserMapping>,
            _filter: &ImportFilter,
        ) -> Result<Vec<crate::foreign::ImportedTable>, ExecError> {
            Ok(Vec::new())
        }
    }

    async fn seed_engine(corpus: Vec<(i32, i64, i64)>) -> (SqlEngine, Arc<Mutex<Vec<ScanBounds>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut engine = SqlEngine::new();
        engine.set_foreign_scanner(Arc::new(RecordingScanner {
            seen: Arc::clone(&seen),
            corpus,
        }));
        {
            let mut s = engine.connect();
            s.simple_query(
                "CREATE SERVER k FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'b:9092')",
            )
            .await
            .expect("create server");
            s.simple_query("CREATE FOREIGN TABLE f (v int8) SERVER k OPTIONS (topic 'topic')")
                .await
                .expect("create foreign table");
        }
        (engine, seen)
    }

    fn rows_of(r: &QueryResult) -> &Vec<Vec<Option<crabka_pgwire::engine::Cell>>> {
        match r {
            QueryResult::Rows { rows, .. } => rows,
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn single_foreign_table_pushes_recorded_bounds() {
        let (engine, seen) = seed_engine(vec![(0, 10, 100)]).await;
        let mut s = engine.connect();
        s.simple_query("SELECT v FROM f WHERE _partition = 0 AND _offset >= 10")
            .await
            .expect("scan ok");
        let recorded = seen.lock().expect("lock");
        assert_eq!(recorded.len(), 1, "exactly one scan");
        assert_eq!(
            recorded[0].start_offsets,
            vec![(0, 10)],
            "the `_partition = 0 AND _offset >= 10` slice was pushed into the scan"
        );
    }

    #[tokio::test]
    async fn full_scan_when_no_pushable_predicate() {
        let (engine, seen) = seed_engine(vec![(0, 10, 100)]).await;
        let mut s = engine.connect();
        // A bare-offset predicate is NOT pushable → default (full) bounds.
        s.simple_query("SELECT v FROM f WHERE _offset >= 10")
            .await
            .expect("scan ok");
        let recorded = seen.lock().expect("lock");
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0],
            ScanBounds::default(),
            "an unanchored offset stays residual → full scan"
        );
    }

    #[tokio::test]
    async fn pushdown_does_not_change_results() {
        // The scanner returns rows OUTSIDE the pushed slice (offsets 5 and 10,
        // partitions 0 and 1) and ignores bounds; the residual WHERE must still
        // yield exactly the rows passing the full predicate.
        let corpus = vec![
            (0, 5, 50),   // _offset 5 < 10 → excluded by WHERE
            (0, 10, 100), // partition 0, offset 10, v=100 → kept
            (0, 12, 7),   // v=7, fails `v > 50` → excluded by WHERE
            (1, 10, 200), // partition 1 → excluded by `_partition = 0`
        ];
        let (engine, seen) = seed_engine(corpus).await;
        let mut s = engine.connect();
        let res = s
            .simple_query("SELECT v FROM f WHERE _partition = 0 AND _offset >= 10 AND v > 50")
            .await
            .expect("scan ok");
        // Only the (0,10,100) row passes the full predicate.
        let rows = rows_of(&res[0]);
        let got: Vec<_> = rows
            .iter()
            .map(|row| {
                String::from_utf8(row[0].as_ref().expect("v not null").text.to_vec()).expect("utf8")
            })
            .collect();
        assert_eq!(got, vec!["100".to_string()], "residual WHERE still applied");
        // And the bounds were pushed (proves it is a real pushdown, not a no-op).
        assert_eq!(seen.lock().expect("lock")[0].start_offsets, vec![(0, 10)]);
    }
}

// ─────────────────── Fix 2: CURRENT_USER / PUBLIC normalization ───────────

/// `normalize_mapping_user` must map both `"current_user"` and `"public"`
/// (any case) to `"public"`, and pass through any other user name unchanged.
#[test]
fn normalize_mapping_user_maps_current_user_and_public_to_public() {
    use super::normalize_mapping_user;
    assert_eq!(normalize_mapping_user("current_user"), "public");
    assert_eq!(normalize_mapping_user("CURRENT_USER"), "public");
    assert_eq!(normalize_mapping_user("Current_User"), "public");
    assert_eq!(normalize_mapping_user("public"), "public");
    assert_eq!(normalize_mapping_user("PUBLIC"), "public");
    // Named users pass through unchanged.
    assert_eq!(normalize_mapping_user("alice"), "alice");
    assert_eq!(normalize_mapping_user("bob"), "bob");
}

/// `CREATE USER MAPPING FOR CURRENT_USER` must be findable via
/// `crabka_pgcatalog::get_user_mapping(kv, "public", server)`, which confirms
/// the key is stored under "public", not "current_user".
#[test]
fn create_user_mapping_for_current_user_stored_under_public() {
    use crabka_pgkv::{Kv, MemKv};

    let kv = MemKv::new();
    let stmt = crabka_pgparser::parser::parse(
        "CREATE USER MAPPING FOR CURRENT_USER SERVER s OPTIONS (username 'u', password 'p')",
    )
    .expect("parse")
    .into_iter()
    .next()
    .expect("one statement");

    // execute_ddl must succeed and store under "public".
    let fctx = super::ForeignCtx::none();
    let (result, ops) = super::execute_ddl(&kv, &stmt, fctx, true).expect("execute_ddl ok");
    assert!(
        matches!(result, crabka_pgwire::engine::QueryResult::Command { tag } if tag == "CREATE USER MAPPING"),
        "expected CREATE USER MAPPING command tag"
    );
    kv.write_batch(&ops).expect("apply DDL ops");

    // The mapping must be retrievable under the "public" key.
    let mapping = crabka_pgcatalog::get_user_mapping(&kv, "public", "s")
        .expect("FOR CURRENT_USER mapping must be stored under 'public'");
    assert!(
        mapping.options.iter().any(|(k, _)| k == "username"),
        "options preserved"
    );
}

fn command_tag(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag,
        QueryResult::Empty => panic!("expected a tagged result, got Empty"),
    }
}

/// Parse `sql` (a DELETE statement) and return its WHERE clause, exercising
/// the same filter shapes the write path receives.
fn delete_filter(sql: &str) -> Option<crabka_pgparser::ast::Expr> {
    let stmt = crabka_pgparser::parser::parse(sql)
        .expect("parse")
        .into_iter()
        .next()
        .expect("one statement");
    match stmt {
        Statement::Delete { filter, .. } => filter,
        other => panic!("expected DELETE, got {other:?}"),
    }
}

#[tokio::test]
async fn choose_write_index_probe_matches_single_column_equality_conjuncts() {
    use assert2::assert;

    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE t (id int4 PRIMARY KEY, flag text)").await;
    let table = crabka_pgcatalog::get_table(engine.catalog_kv.as_ref(), &RelationName::public("t"))
        .expect("table");

    let cases: &[(&str, Option<crabka_pgtypes::Datum>)] = &[
        (
            "DELETE FROM t WHERE id = 5",
            Some(crabka_pgtypes::Datum::Int4(5)),
        ),
        (
            "DELETE FROM t WHERE id = 5 AND flag = 'x'",
            Some(crabka_pgtypes::Datum::Int4(5)),
        ),
        (
            "DELETE FROM t WHERE flag = 'x' AND id = 5",
            Some(crabka_pgtypes::Datum::Int4(5)),
        ),
        (
            "DELETE FROM t WHERE 5 = id",
            Some(crabka_pgtypes::Datum::Int4(5)),
        ),
        // Non-indexed column, disjunction, computed column, wrong-type
        // literal, range comparison, and no filter all fall back.
        ("DELETE FROM t WHERE flag = 'x'", None),
        ("DELETE FROM t WHERE id = 5 OR flag = 'x'", None),
        ("DELETE FROM t WHERE id + 1 = 5", None),
        ("DELETE FROM t WHERE id = 5.5", None),
        ("DELETE FROM t WHERE id < 5", None),
        ("DELETE FROM t", None),
    ];
    for (sql, expected) in cases {
        let filter = delete_filter(sql);
        let probe =
            super::choose_write_index_probe(engine.catalog_kv.as_ref(), &table, filter.as_ref())
                .expect("choose probe");
        match (probe, expected) {
            (Some((index, value)), Some(want)) => {
                assert!(index.columns == ["id"], "{sql}");
                assert!(value == *want, "{sql}");
            }
            (None, None) => {}
            (got, want) => panic!("{sql}: got {got:?}, want {want:?}"),
        }
    }

    // Sharded tables never probe, even with a matching filter shape.
    let mut sharded = table.clone();
    sharded.sharded = true;
    let filter = delete_filter("DELETE FROM t WHERE id = 5");
    let probe =
        super::choose_write_index_probe(engine.catalog_kv.as_ref(), &sharded, filter.as_ref())
            .expect("choose probe");
    assert!(probe.is_none());
}

#[tokio::test]
async fn insert_unique_probe_rejects_duplicates_within_and_across_statements() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE t (id int4 PRIMARY KEY, v text)").await;
    run_s(&mut s, "INSERT INTO t VALUES (1, 'a')").await;

    // Across committed rows.
    let err = s
        .simple_query("INSERT INTO t VALUES (1, 'b')")
        .await
        .expect_err("duplicate committed key");
    assert!(err.code == "23505");

    // Within one statement (rows not yet in the kv: pending-key dedup).
    let err = s
        .simple_query("INSERT INTO t VALUES (2, 'a'), (2, 'b')")
        .await
        .expect_err("duplicate within statement");
    assert!(err.code == "23505");

    // Across statements inside one transaction: the probe sees our own
    // uncommitted row via read-your-writes.
    run_s(&mut s, "BEGIN").await;
    run_s(&mut s, "INSERT INTO t VALUES (3, 'x')").await;
    let err = s
        .simple_query("INSERT INTO t VALUES (3, 'y')")
        .await
        .expect_err("duplicate of own uncommitted row");
    assert!(err.code == "23505");
    run_s(&mut s, "ROLLBACK").await;

    // The rolled-back insert left only dead index entries: the key is free.
    run_s(&mut s, "INSERT INTO t VALUES (3, 'z')").await;

    // UPDATE moving a row onto a held key is a violation too.
    let err = s
        .simple_query("UPDATE t SET id = 1 WHERE id = 3")
        .await
        .expect_err("update onto held key");
    assert!(err.code == "23505");
}

#[tokio::test]
async fn insert_unique_probe_ignores_dead_versions_of_the_key() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE t (id int4 PRIMARY KEY, v text)").await;
    run_s(&mut s, "INSERT INTO t VALUES (1, 'a')").await;

    // Move the key away: the index keeps a dead entry for id=1 pointing at
    // the superseded version, which must not count as a holder.
    run_s(&mut s, "UPDATE t SET id = 2 WHERE id = 1").await;
    run_s(&mut s, "INSERT INTO t VALUES (1, 'b')").await;

    // Delete-then-reinsert: the deleted version's entry is dead too.
    run_s(&mut s, "DELETE FROM t WHERE id = 2").await;
    run_s(&mut s, "INSERT INTO t VALUES (2, 'c')").await;

    let r = &run_s(&mut s, "SELECT id, v FROM t ORDER BY id").await[0];
    let rows = rows_of(r);
    assert!(rows.len() == 2);
    assert!(text(&rows[0][0]) == Some("1".into()));
    assert!(text(&rows[0][1]) == Some("b".into()));
    assert!(text(&rows[1][0]) == Some("2".into()));
    assert!(text(&rows[1][1]) == Some("c".into()));
}

#[tokio::test]
async fn point_update_via_index_probe_applies_residual_filter_and_returning() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(
        &mut s,
        "CREATE TABLE t (id int4 PRIMARY KEY, flag text, v text)",
    )
    .await;
    run_s(
        &mut s,
        "INSERT INTO t VALUES (1,'x','a'), (2,'x','b'), (3,'y','c')",
    )
    .await;

    // Indexed equality + residual conjunct: only id=2 matches both.
    let r = &run_s(&mut s, "UPDATE t SET v = 'z' WHERE id = 2 AND flag = 'x'").await[0];
    assert!(command_tag(r) == "UPDATE 1");

    // Residual conjunct rejects the probed row: no row is touched.
    let r = &run_s(&mut s, "UPDATE t SET v = 'w' WHERE id = 3 AND flag = 'x'").await[0];
    assert!(command_tag(r) == "UPDATE 0");

    // RETURNING reflects the updated row exactly.
    let r = &run_s(
        &mut s,
        "UPDATE t SET v = 'r' WHERE id = 1 AND flag = 'x' RETURNING id, v",
    )
    .await[0];
    assert!(command_tag(r) == "UPDATE 1");
    let returned = rows_of(r);
    assert!(returned.len() == 1);
    assert!(text(&returned[0][0]) == Some("1".into()));
    assert!(text(&returned[0][1]) == Some("r".into()));

    let r = &run_s(&mut s, "SELECT id, flag, v FROM t ORDER BY id").await[0];
    let rows = rows_of(r);
    let contents: Vec<Vec<Option<String>>> = rows
        .iter()
        .map(|row| row.iter().map(text).collect())
        .collect();
    assert!(
        contents
            == vec![
                vec![Some("1".into()), Some("x".into()), Some("r".into())],
                vec![Some("2".into()), Some("x".into()), Some("z".into())],
                vec![Some("3".into()), Some("y".into()), Some("c".into())],
            ]
    );
}

#[tokio::test]
async fn point_delete_via_index_probe_applies_residual_filter_and_returning() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(
        &mut s,
        "CREATE TABLE t (id int4 PRIMARY KEY, flag text, v text)",
    )
    .await;
    run_s(
        &mut s,
        "INSERT INTO t VALUES (1,'x','a'), (2,'x','b'), (3,'y','c')",
    )
    .await;

    // Residual conjunct rejects the probed row: nothing is deleted.
    let r = &run_s(&mut s, "DELETE FROM t WHERE id = 3 AND flag = 'x'").await[0];
    assert!(command_tag(r) == "DELETE 0");

    let r = &run_s(
        &mut s,
        "DELETE FROM t WHERE id = 1 AND flag = 'x' RETURNING id, v",
    )
    .await[0];
    assert!(command_tag(r) == "DELETE 1");
    let returned = rows_of(r);
    assert!(returned.len() == 1);
    assert!(text(&returned[0][0]) == Some("1".into()));
    assert!(text(&returned[0][1]) == Some("a".into()));

    let r = &run_s(&mut s, "SELECT id FROM t ORDER BY id").await[0];
    let rows = rows_of(r);
    assert!(rows.len() == 2);
    assert!(text(&rows[0][0]) == Some("2".into()));
    assert!(text(&rows[1][0]) == Some("3".into()));
}

#[tokio::test]
async fn update_and_delete_fall_back_to_full_scan_for_non_indexed_predicates() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(
        &mut s,
        "CREATE TABLE t (id int4 PRIMARY KEY, flag text, v text)",
    )
    .await;
    run_s(
        &mut s,
        "INSERT INTO t VALUES (1,'x','a'), (2,'x','b'), (3,'y','c')",
    )
    .await;

    // Non-indexed equality: full scan, same result as the probe would give.
    let r = &run_s(&mut s, "UPDATE t SET v = 'q' WHERE flag = 'x'").await[0];
    assert!(command_tag(r) == "UPDATE 2");

    // Disjunction on the indexed column: not a conjunctive equality, so the
    // fallback full scan must handle it.
    let r = &run_s(&mut s, "UPDATE t SET v = 'd' WHERE id = 1 OR id = 3").await[0];
    assert!(command_tag(r) == "UPDATE 2");

    let r = &run_s(&mut s, "SELECT id, v FROM t ORDER BY id").await[0];
    let rows = rows_of(r);
    let contents: Vec<Vec<Option<String>>> = rows
        .iter()
        .map(|row| row.iter().map(text).collect())
        .collect();
    assert!(
        contents
            == vec![
                vec![Some("1".into()), Some("d".into())],
                vec![Some("2".into()), Some("q".into())],
                vec![Some("3".into()), Some("d".into())],
            ]
    );

    // Range predicate on the indexed column: also a fallback.
    let r = &run_s(&mut s, "DELETE FROM t WHERE id > 1").await[0];
    assert!(command_tag(r) == "DELETE 2");
    let r = &run_s(&mut s, "SELECT id FROM t").await[0];
    assert!(rows_of(r).len() == 1);
    assert!(text(&rows_of(r)[0][0]) == Some("1".into()));
}

#[test]
fn column_type_from_oid_maps_supported_scalars_and_every_array_oid() {
    use assert2::assert;
    use crabka_pgtypes::{ColumnType, ElemType, oids};

    for (oid, expected) in [
        // `json` and `jsonb` are separate types: json keeps its input text,
        // jsonb decomposes it. Each oid must map to its own.
        (oids::JSON, ColumnType::Json),
        (oids::JSONB, ColumnType::Jsonb),
        (oids::NAME, ColumnType::Name),
        (oids::ACLITEM, ColumnType::Aclitem),
        (oids::REFCURSOR, ColumnType::Refcursor),
        (oids::BYTEA, ColumnType::Bytea),
        (oids::INT2VECTOR, ColumnType::Int2Vector),
        (oids::MONEY, ColumnType::Money),
        (oids::BIT, ColumnType::Bit(None)),
        (oids::VARBIT, ColumnType::VarBit(None)),
        (oids::TSVECTOR, ColumnType::TsVector),
        (oids::TSQUERY, ColumnType::TsQuery),
        (oids::INET, ColumnType::Inet),
        (oids::CIDR, ColumnType::Cidr),
        (oids::MACADDR, ColumnType::MacAddr),
        (oids::MACADDR8, ColumnType::MacAddr8),
        (oids::JSONARRAY, ColumnType::Array(ElemType::Json)),
        (oids::RECORD, ColumnType::Record(None)),
        (
            oids::INFORMATION_SCHEMA_CARDINAL_NUMBER,
            ColumnType::information_schema_domain("cardinal_number").expect("domain"),
        ),
    ] {
        assert!(super::column_type_from_oid(oid).expect("known oid") == expected);
    }
    for elem in ElemType::ALL {
        assert!(
            super::column_type_from_oid(elem.array_oid()).expect("array oid")
                == ColumnType::Array(elem)
        );
    }
    assert!(super::column_type_from_oid(999_999).is_err());
}

#[test]
fn pg_type_exposes_the_scalar_array_link_for_every_row() {
    use assert2::assert;
    use crabka_pgtypes::ElemType;

    let rows = super::builtin_type_rows();
    for scalar in rows.iter().filter(|row| row.array != 0) {
        let array = rows
            .iter()
            .find(|row| row.oid == scalar.array)
            .unwrap_or_else(|| panic!("{} has no array row", scalar.name));
        assert!((array.elem, array.category, array.len, array.array) == (scalar.oid, "A", -1, 0));
    }
    for array in rows.iter().filter(|row| row.category == "A") {
        assert!(
            rows.iter().any(|row| row.oid == array.elem),
            "{} has a dangling typelem",
            array.name
        );
    }
    // Every element type crabka can build an array of has a pg_type row.
    for elem in ElemType::ALL {
        let oid = i32::try_from(elem.array_oid()).expect("array oid fits in int4");
        assert!(rows.iter().any(|row| row.oid == oid), "{elem:?}");
    }
}

#[test]
fn pg_type_rows_match_the_declared_column_list() {
    use assert2::assert;
    use crabka_pgtypes::Datum;

    let columns = super::virtual_catalog_columns("pg_type");
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert!(
        names
            == [
                "oid",
                "typname",
                "typnamespace",
                "typowner",
                "typlen",
                "typbyval",
                "typtype",
                "typcategory",
                "typispreferred",
                "typisdefined",
                "typdelim",
                "typrelid",
                "typsubscript",
                "typelem",
                "typarray",
                "typinput",
                "typoutput",
                "typreceive",
                "typsend",
                "typmodin",
                "typmodout",
                "typanalyze",
                "typalign",
                "typstorage",
                "typnotnull",
                "typbasetype",
                "typtypmod",
                "typndims",
                "typcollation",
                "typdefaultbin",
                "typdefault",
                "typacl",
            ]
    );
    let rows = super::pg_type_rows(&crabka_pgkv::MemKv::default()).expect("pg_type rows");
    for row in &rows {
        assert!(row.len() == columns.len());
    }
    // `_int4` keeps PostgreSQL's scalar/array links and physical shape.
    let int4_array = rows
        .iter()
        .find(|row| row[0] == Datum::Oid(1007))
        .expect("_int4 row");
    assert!(int4_array[0] == Datum::Oid(1007));
    assert!(int4_array[1] == super::text("_int4"));
    assert!(int4_array[2] == Datum::Oid(super::PG_CATALOG_NAMESPACE_OID as u32));
    assert!(int4_array[7] == Datum::InternalChar(b'A'));
    assert!(int4_array[13] == Datum::Oid(23));
    assert!(int4_array[14] == Datum::Oid(0));
    assert!(int4_array[22] == Datum::InternalChar(b'i'));
    assert!(int4_array[23] == Datum::InternalChar(b'x'));
    // `text` is the collatable case: its `typcollation` has to be the same
    // database default `attcollation` gives a text column, or `\d` reports a
    // collation on every text column.
    let text_row = rows
        .iter()
        .find(|row| row[0] == Datum::Oid(25))
        .expect("text row");
    assert!(text_row[28] == Datum::Oid(crate::catalog_rel::DEFAULT_COLLATION_OID as u32));
}

#[test]
fn coerce_assigns_literals_and_arrays_to_jsonb_and_array_columns() {
    use assert2::assert;
    use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType};

    let ctx = crate::clock::EvalCtx::test_default();
    let jsonb = super::coerce(
        Datum::Text("{\"b\":2,\"a\":1}".into()),
        ColumnType::Jsonb,
        &ctx,
    )
    .expect("jsonb literal");
    assert!(
        jsonb
            == Datum::Jsonb(
                crabka_pgtypes::jsonb::parse("{\"a\":1,\"b\":2}").expect("canonical parse")
            )
    );
    let array = super::coerce(
        Datum::Text("{1,2}".into()),
        ColumnType::Array(ElemType::Int4),
        &ctx,
    )
    .expect("array literal");
    assert!(
        array
            == Datum::Array(ArrayValue::new(
                ElemType::Int4,
                vec![Datum::Int4(1), Datum::Int4(2)]
            ))
    );
    // An int4[] value widens element-wise into a bigint[] column.
    let widened = super::coerce(
        Datum::Array(ArrayValue::new(ElemType::Int4, vec![Datum::Int4(7)])),
        ColumnType::Array(ElemType::Int8),
        &ctx,
    )
    .expect("element-wise widening");
    assert!(widened == Datum::Array(ArrayValue::new(ElemType::Int8, vec![Datum::Int8(7)])));
    // Malformed input is the type's input-function error, not 42804.
    assert!(super::coerce(Datum::Text("{".into()), ColumnType::Jsonb, &ctx).is_err());
}

#[test]
fn assignment_temporal_input_uses_the_transaction_timestamp() {
    use assert2::assert;
    use crabka_pgtypes::{ColumnType, Datum};

    let now = "2024-03-10T05:06:07Z".parse().expect("timestamp");
    let mut ctx = crate::clock::EvalCtx::test_default();
    ctx.now = now;
    for (target, expected) in [
        (
            ColumnType::Time,
            Datum::Time(crabka_pgtypes::datetime::parse_time("05:06:07").expect("time")),
        ),
        (ColumnType::Timestamptz, Datum::Timestamptz(now)),
    ] {
        assert!(super::resolve_unknown_literal("now", target, &ctx).expect("literal") == expected);
    }
}

#[test]
fn name_assignments_truncate_at_the_postgresql_byte_limit() {
    use assert2::assert;
    use crabka_pgtypes::{ColumnType, Datum};

    let ctx = crate::clock::EvalCtx::test_default();
    for (value, expected) in [
        ("x".repeat(64), "x".repeat(63)),
        (format!("{}é", "x".repeat(62)), "x".repeat(62)),
    ] {
        assert!(
            super::resolve_unknown_literal(&value, ColumnType::Name, &ctx).expect("name literal")
                == Datum::Text(expected.clone())
        );
        assert!(
            super::coerce(Datum::Text(value), ColumnType::Name, &ctx).expect("name assignment")
                == Datum::Text(expected)
        );
    }
}

/// The DDL gate covers every column type: exactly the types whose datums
/// [`super::hash_bucket_for_row`] can hash are accepted as a hash shard key.
#[test]
fn only_hashable_column_types_are_accepted_as_a_hash_shard_key() {
    use assert2::assert;
    use crabka_pgcatalog::Column;
    use crabka_pgtypes::{ColumnType, ElemType};

    let sharding = |column: &str| {
        crabka_pgcatalog::ShardingStrategy::Hash(crabka_pgcatalog::HashSharding {
            columns: vec![column.to_string()],
            buckets: 4,
            co_location_group: None,
        })
    };
    for (ty, supported) in [
        (ColumnType::Int4, true),
        (ColumnType::Int8, true),
        (ColumnType::Text, true),
        (ColumnType::Varchar(Some(8)), true),
        (ColumnType::Char(Some(8)), true),
        (ColumnType::Bytea, true),
        (ColumnType::Uuid, true),
        (ColumnType::Regclass, true),
        (ColumnType::Bool, false),
        (ColumnType::Float8, false),
        (ColumnType::Numeric(None), false),
        (ColumnType::Date, false),
        (ColumnType::Time, false),
        (ColumnType::Timestamp, false),
        (ColumnType::Timestamptz, false),
        (ColumnType::Interval, false),
        (ColumnType::Jsonb, false),
        (ColumnType::Array(ElemType::Int4), false),
    ] {
        let columns = vec![Column::new("k", ty)];
        let result =
            super::ensure_hash_shard_key_types_are_supported(&columns, Some(&sharding("k")));
        assert!(result.is_ok() == supported, "{ty:?}");
        if !supported {
            let error = result.expect_err("unhashable key").into_pg();
            assert!(error.code == "0A000");
            assert!(error.message.contains("\"k\""), "{}", error.message);
            assert!(error.message.contains(ty.name()), "{}", error.message);
        }
    }
    // A hash column that does not exist is left to the catalog's own
    // undefined-column error, and an unsharded table has nothing to check.
    let columns = vec![Column::new("k", ColumnType::Jsonb)];
    assert!(
        super::ensure_hash_shard_key_types_are_supported(&columns, Some(&sharding("missing")))
            .is_ok()
    );
    assert!(super::ensure_hash_shard_key_types_are_supported(&columns, None).is_ok());
}

/// The write-path backstop still refuses an unhashable shard key. That is
/// reachable for a table whose sharding was attached outside CREATE TABLE.
#[test]
fn hashing_a_row_refuses_an_unhashable_shard_key() {
    use assert2::assert;
    use crabka_pgcatalog::Column;
    use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType};

    let table = crabka_pgcatalog::Table {
        id: 1,
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        name: RelationName::public("t"),
        columns: vec![Column::new("k", ColumnType::Jsonb)],
        sharded: true,
        row_security: false,
        force_row_security: false,
        sharding: Some(crabka_pgcatalog::ShardingStrategy::Hash(
            crabka_pgcatalog::HashSharding {
                columns: vec!["k".into()],
                buckets: 4,
                co_location_group: None,
            },
        )),
        foreign: None,
        materialized: None,
        checks: Vec::new(),
    };
    for value in [
        Datum::Jsonb(crabka_pgtypes::jsonb::parse("{\"a\":1}").expect("jsonb")),
        Datum::Array(ArrayValue::new(ElemType::Int4, vec![Datum::Int4(1)])),
    ] {
        let error = super::hash_bucket_for_row(&table, &[value])
            .expect_err("unhashable")
            .into_pg();
        assert!(error.code == "0A000");
        assert!(error.message == "hash shard key type is not supported");
    }
    // A hashable value in the same slot still routes.
    assert!(
        super::hash_bucket_for_row(&table, &[Datum::Int4(1)])
            .expect("hashable")
            .is_some()
    );
}

/// The DDL path builds a hash sharding only from a one-column key, the
/// arity [`super::hash_bucket_for_row`] can encode. The grammar already
/// refuses a wider `SHARDED BY HASH` list, so this covers the seam for an
/// AST built by something other than the parser. Bucket counts are still
/// checked, and a valid spec still converts.
#[test]
fn the_ddl_path_builds_a_hash_sharding_only_from_one_column() {
    use assert2::assert;
    use crabka_pgparser::ast::{HashShardingSpec, ShardingSpec};

    let spec = |columns: &[&str], buckets: u32| {
        ShardingSpec::Hash(HashShardingSpec {
            columns: columns.iter().map(|column| (*column).to_string()).collect(),
            buckets,
            co_location_group: None,
        })
    };
    let arity = Some("hash sharding requires exactly one column");
    let buckets_message = Some("hash sharding bucket count must be a power of two");
    for (columns, buckets, expected) in [
        (&[][..], 4, arity),
        (&["a"][..], 4, None),
        (&["a", "b"][..], 4, arity),
        (&["a", "b", "c"][..], 4, arity),
        (&["a"][..], 0, buckets_message),
        (&["a"][..], 6, buckets_message),
    ] {
        let converted = super::hash_sharding_from_ast(&spec(columns, buckets));
        match expected {
            Some(message) => {
                let error = converted.expect_err("refused").into_pg();
                assert!(error.code == "0A000", "{columns:?}/{buckets}");
                assert!(error.message == message, "{columns:?}/{buckets}");
            }
            None => assert!(
                converted.expect("accepted")
                    == crabka_pgcatalog::ShardingStrategy::Hash(crabka_pgcatalog::HashSharding {
                        columns: vec!["a".into()],
                        buckets,
                        co_location_group: None,
                    }),
                "{columns:?}/{buckets}"
            ),
        }
    }
}

/// The write-path backstop behind the two creation gates: a multi-column
/// hash sharding has no row encoding, so the write is refused rather than
/// placing the row under the hash of its first column, where a route
/// computed from the whole key never looks.
#[test]
fn hashing_a_row_refuses_a_multi_column_hash_shard_key() {
    use assert2::assert;
    use crabka_pgcatalog::Column;
    use crabka_pgtypes::{ColumnType, Datum};

    let table = crabka_pgcatalog::Table {
        id: 1,
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        name: RelationName::public("t"),
        columns: vec![
            Column::new("a", ColumnType::Int4),
            Column::new("b", ColumnType::Int4),
        ],
        sharded: true,
        row_security: false,
        force_row_security: false,
        sharding: Some(crabka_pgcatalog::ShardingStrategy::Hash(
            crabka_pgcatalog::HashSharding {
                columns: vec!["a".into(), "b".into()],
                buckets: 4,
                co_location_group: None,
            },
        )),
        foreign: None,
        materialized: None,
        checks: Vec::new(),
    };
    let error = super::hash_bucket_for_row(&table, &[Datum::Int4(1), Datum::Int4(2)])
        .expect_err("no row encoding for a two-column key")
        .into_pg();
    assert!(error.code == "0A000");
    assert!(error.message == "hash sharding requires exactly one hash column");
}

#[test]
fn jsonb_and_array_defaults_render_as_quoted_literals() {
    use assert2::assert;
    use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType};

    let doc = Datum::Jsonb(crabka_pgtypes::jsonb::parse("{\"a\":1}").expect("parse"));
    assert!(super::format_default_value(&doc, ColumnType::Jsonb) == "'{\"a\": 1}'::jsonb");
    let array = Datum::Array(ArrayValue::new(
        ElemType::Int4,
        vec![Datum::Int4(1), Datum::Int4(2)],
    ));
    assert!(
        super::format_default_value(&array, ColumnType::Array(ElemType::Int4))
            == "'{1,2}'::integer[]"
    );
}

#[test]
fn a_from_item_function_must_be_a_known_set_returning_function() {
    use assert2::assert;
    use crabka_pgparser::ast::Expr;
    use crabka_pgtypes::{ColumnType, Datum, ElemType};

    let call = |name: &str, arg: Expr| {
        vec![crabka_pgparser::ast::TableFuncCall {
            name: name.into(),
            args: vec![arg],
            named_args: Vec::new(),
            variadic: None,
            column_defs: None,
        }]
    };
    let array_arg = Expr::Const {
        value: Datum::Null,
        ty: ColumnType::Array(ElemType::Text),
    };
    // A name no SRF registry entry claims is 42883, PostgreSQL's failed
    // function lookup, on both the row and the schema-only path.
    let unknown = call("no_such_function", array_arg);
    let statement_memory =
        crate::scanner::StatementMemory::new(crate::scanner::BLOCKING_QUERY_MEMORY);
    for relation in [
        crate::srf::from_item_with_memory(
            &unknown,
            false,
            false,
            None,
            &None,
            &crate::clock::EvalCtx::test_default(),
            &statement_memory,
        ),
        crate::srf::from_item_schema(&unknown, false, false, None, &None),
    ] {
        assert!(matches!(relation, Err(ExecError::UndefinedFunction(_))));
    }
    // A non-array `unnest` argument resolves to no `unnest` function at all.
    let scalar = call(
        "unnest",
        Expr::Const {
            value: Datum::Int4(1),
            ty: ColumnType::Int4,
        },
    );
    assert!(matches!(
        crate::srf::from_item_schema(&scalar, false, false, None, &None),
        Err(ExecError::UndefinedFunction(_))
    ));
}

#[tokio::test]
async fn unnest_in_from_expands_an_array_argument() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    let r = &run_s(&mut s, "SELECT * FROM unnest('{3,1,2}'::int[])").await[0];
    assert!(fields_of(r)[0].name == "unnest");
    assert!(fields_of(r)[0].type_oid == crabka_pgtypes::oids::INT4);
    let values: Vec<Option<String>> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert!(values == vec![Some("3".into()), Some("1".into()), Some("2".into())]);

    let r = &run_s(
        &mut s,
        "SELECT * FROM unnest(int4multirange(int4range(1, 3), int4range(5, 7)))",
    )
    .await[0];
    let values: Vec<Option<String>> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert!(values == vec![Some("[1,3)".into()), Some("[5,7)".into())]);

    // Alias and column alias behave as they do for a derived table.
    let r = &run_s(
        &mut s,
        "SELECT u.x FROM unnest('{a,b}'::text[]) AS u(x) ORDER BY u.x",
    )
    .await[0];
    assert!(fields_of(r)[0].name == "x");
    let values: Vec<Option<String>> = rows_of(r).iter().map(|row| text(&row[0])).collect();
    assert!(values == vec![Some("a".into()), Some("b".into())]);

    // A NULL array and an empty array both expand to zero rows.
    for sql in [
        "SELECT * FROM unnest(NULL::int[])",
        "SELECT * FROM unnest('{}'::int[])",
    ] {
        assert!(rows_of(&run_s(&mut s, sql).await[0]).is_empty(), "{sql}");
    }

    // A name the SRF registry does not claim is 42883 in FROM position.
    let error = s
        .simple_query("SELECT * FROM no_such_function(1, 3)")
        .await
        .expect_err("no such table function");
    assert!(error.code == "42883", "{error:?}");
}

#[tokio::test]
async fn jsonb_and_array_columns_round_trip_through_ddl() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(
        &mut s,
        "CREATE TABLE t (id int4 PRIMARY KEY, j jsonb, a int[])",
    )
    .await;
    run_s(
        &mut s,
        "INSERT INTO t (id, j, a) VALUES (1, '{\"b\":2,\"a\":1}', '{3,4}'), (2, NULL, '{}')",
    )
    .await;
    let r = &run_s(&mut s, "SELECT j, a FROM t ORDER BY id").await[0];
    assert!(fields_of(r)[0].type_oid == crabka_pgtypes::oids::JSONB);
    assert!(fields_of(r)[1].type_oid == crabka_pgtypes::oids::INT4ARRAY);
    let values: Vec<Vec<Option<String>>> = rows_of(r)
        .iter()
        .map(|row| row.iter().map(text).collect())
        .collect();
    assert!(
        values
            == vec![
                vec![Some("{\"a\": 1, \"b\": 2}".into()), Some("{3,4}".into())],
                vec![None, Some("{}".into())],
            ]
    );

    // jsonb/array defaults persist and apply, including executable defaults
    // whose result is converted through the target type at insert time.
    run_s(
        &mut s,
        "CREATE TABLE d (id int4, j jsonb DEFAULT '{}', a int[] DEFAULT '{1}')",
    )
    .await;
    run_s(&mut s, "INSERT INTO d (id) VALUES (1)").await;
    let r = &run_s(&mut s, "SELECT j, a FROM d").await[0];
    assert!(
        rows_of(r)[0].iter().map(text).collect::<Vec<_>>()
            == vec![Some("{}".into()), Some("{1}".into())]
    );
    run_s(
        &mut s,
        "CREATE TABLE e (d date DEFAULT '2020-01-01'::date, rendered text DEFAULT now())",
    )
    .await;
    run_s(&mut s, "INSERT INTO e DEFAULT VALUES").await;
    let r = &run_s(&mut s, "SELECT d, rendered FROM e").await[0];
    assert!(text(&rows_of(r)[0][0]) == Some("2020-01-01".into()));
    assert!(text(&rows_of(r)[0][1]).is_some());
}

#[tokio::test]
async fn assignment_casts_non_string_values_to_string_columns() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(
        &mut s,
        "CREATE TABLE strings (a text, b varchar(8), c char(10), d int4)",
    )
    .await;
    run_s(
        &mut s,
        "INSERT INTO strings (a, b, c) VALUES (42, true, DATE '2020-01-02')",
    )
    .await;
    let r = &run_s(&mut s, "SELECT a, b, c FROM strings").await[0];
    assert!(
        rows_of(r)[0].iter().map(text).collect::<Vec<_>>()
            == vec![
                Some("42".into()),
                Some("true".into()),
                Some("2020-01-02".into())
            ]
    );
    assert!(sqlstate_of(&mut s, "INSERT INTO strings (d) VALUES ('42'::text)").await == "42804");
}

#[tokio::test]
async fn pg_index_marks_the_primary_key_index() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE t (id int4 PRIMARY KEY, v text)").await;
    run_s(&mut s, "CREATE UNIQUE INDEX t_v_key ON t (v)").await;
    let r = &run_s(
        &mut s,
        "SELECT i.indisunique, i.indisprimary
             FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
             WHERE c.relname = 't' ORDER BY i.indexrelid",
    )
    .await[0];
    let values: Vec<Vec<Option<String>>> = rows_of(r)
        .iter()
        .map(|row| row.iter().map(text).collect())
        .collect();
    assert!(
        values
            == vec![
                vec![Some("t".into()), Some("t".into())],
                vec![Some("t".into()), Some("f".into())],
            ]
    );
}

/// Build a table and its indexes for the arbiter-resolution tests.
/// `indexes` entries are `(name, columns, unique, is_constraint)`.
fn arbiter_fixture(
    columns: &[&str],
    indexes: &[(&str, &[&str], bool, bool)],
) -> (crabka_pgcatalog::Table, Vec<crabka_pgcatalog::Index>) {
    let table = crabka_pgcatalog::Table {
        id: 1,
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        name: RelationName::public("t"),
        columns: columns
            .iter()
            .map(|name| crabka_pgcatalog::Column::new(*name, crabka_pgtypes::ColumnType::Int4))
            .collect(),
        sharded: false,
        row_security: false,
        force_row_security: false,
        sharding: None,
        foreign: None,
        materialized: None,
        checks: Vec::new(),
    };
    let indexes = indexes
        .iter()
        .enumerate()
        .map(
            |(i, (name, cols, unique, constraint))| crabka_pgcatalog::Index {
                id: i as u32 + 1,
                name: (*name).to_string(),
                table: RelationName::public("t"),
                table_id: 1,
                columns: cols.iter().map(|c| (*c).to_string()).collect(),
                unique: *unique,
                placement: crabka_pgcatalog::IndexPlacement::Local,
                method: crabka_pgcatalog::IndexMethod::Btree,
                constraint: constraint.then_some(crabka_pgcatalog::IndexConstraint::Unique),
                without_overlaps: false,
                clustered: false,
                deferral: crabka_pgcatalog::ConstraintDeferral::Immediate,
            },
        )
        .collect();
    (table, indexes)
}

#[test]
fn arbiter_resolution_matches_column_sets_and_constraint_names() {
    use assert2::assert;
    use crabka_pgparser::ast::OnConflictTarget;

    let (table, indexes) = arbiter_fixture(
        &["a", "b", "c"],
        &[
            ("t_pkey", &["a"], true, true),
            ("t_ab_key", &["a", "b"], true, true),
            ("t_c_idx", &["c"], false, false),
            ("t_c_uniq", &["c"], true, false),
        ],
    );
    let names = |target: &OnConflictTarget| {
        super::resolve_arbiter_indexes(&table, &indexes, target)
            .map(|found| found.iter().map(|i| i.name.clone()).collect::<Vec<_>>())
    };
    let columns = |cols: &[&str]| OnConflictTarget::Columns {
        columns: cols.iter().map(|c| (*c).to_string()).collect(),
        inference_columns: cols
            .iter()
            .map(|name| crabka_pgparser::ast::OnConflictInferenceColumn {
                name: (*name).into(),
                collation: None,
                opclass: None,
            })
            .collect(),
        index_predicate: None,
    };

    // No target: every unique local index arbitrates (the non-unique one
    // never does).
    assert!(
        names(&OnConflictTarget::None)
            == Ok(vec!["t_pkey".into(), "t_ab_key".into(), "t_c_uniq".into()])
    );
    // Column-set inference, order-insensitive: `(b, a)` finds `UNIQUE (a, b)`.
    assert!(names(&columns(&["a"])) == Ok(vec!["t_pkey".into()]));
    assert!(names(&columns(&["b", "a"])) == Ok(vec!["t_ab_key".into()]));
    // A unique index needs no constraint to be inferred by columns.
    assert!(names(&columns(&["c"])) == Ok(vec!["t_c_uniq".into()]));
    // A subset/superset of an index's columns is not a match: 42P10.
    assert!(names(&columns(&["b"])) == Err(ExecError::OnConflictNoArbiter));
    assert!(
        names(&columns(&["a", "b", "c"])) == Err(ExecError::OnConflictNoArbiter),
        "no index covers all three columns"
    );
    // An unknown inference column is 42703, checked before arbitration.
    assert!(names(&columns(&["nope"])) == Err(ExecError::UndefinedColumn("nope".into())));
    // ON CONSTRAINT resolves by name, but only for constraint-backed indexes.
    assert!(
        names(&OnConflictTarget::OnConstraint("t_ab_key".into())) == Ok(vec!["t_ab_key".into()])
    );
    for name in ["t_c_uniq", "t_c_idx", "nosuch"] {
        assert!(
            names(&OnConflictTarget::OnConstraint(name.into()))
                == Err(ExecError::UndefinedConstraint {
                    name: name.into(),
                    table: "t".into(),
                }),
            "ON CONSTRAINT {name}"
        );
    }
    // A predicate can infer a regular unique index; partial-index filtering is
    // irrelevant when no partial indexes exist.
    let predicated = OnConflictTarget::Columns {
        columns: vec!["a".into()],
        inference_columns: vec![crabka_pgparser::ast::OnConflictInferenceColumn {
            name: "a".into(),
            collation: None,
            opclass: None,
        }],
        index_predicate: Some(crabka_pgparser::ast::Expr::BoolLiteral(true)),
    };
    assert!(names(&predicated) == Ok(vec!["t_pkey".into()]));
}

#[test]
fn arbiter_resolution_without_unique_indexes_is_empty_not_an_error() {
    use assert2::assert;
    use crabka_pgparser::ast::OnConflictTarget;

    // `DO NOTHING` with no target on a table with no unique index: legal,
    // and every row simply inserts.
    let (table, indexes) = arbiter_fixture(&["a"], &[("t_a_idx", &["a"], false, false)]);
    let found = super::resolve_arbiter_indexes(&table, &indexes, &OnConflictTarget::None);
    assert!(found == Ok(Vec::new()));
}

#[tokio::test]
async fn on_conflict_do_nothing_and_do_update_upsert() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE t (id int4 PRIMARY KEY, v text)").await;
    run_s(&mut s, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;

    // DO NOTHING skips the conflicting row and does not count it.
    let r = &run_s(
        &mut s,
        "INSERT INTO t VALUES (1, 'x'), (3, 'c') ON CONFLICT (id) DO NOTHING",
    )
    .await[0];
    assert!(matches!(r, QueryResult::Command { tag } if tag == "INSERT 0 1"));
    let r = &run_s(
        &mut s,
        "INSERT INTO t VALUES (2, 'ignored') ON CONFLICT (id) WHERE v = 'green' DO NOTHING",
    )
    .await[0];
    assert!(matches!(r, QueryResult::Command { tag } if tag == "INSERT 0 0"));

    // DO UPDATE upserts: the conflicting row is updated (and counted), the
    // new one inserted. RETURNING reports the post-image.
    let r = &run_s(
        &mut s,
        "INSERT INTO t VALUES (1, 'x'), (4, 'd') \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v || t.v RETURNING id, v",
    )
    .await[0];
    let values: Vec<Vec<Option<String>>> = rows_of(r)
        .iter()
        .map(|row| row.iter().map(text).collect())
        .collect();
    assert!(
        values
            == vec![
                vec![Some("1".into()), Some("xa".into())],
                vec![Some("4".into()), Some("d".into())]
            ]
    );

    // A data-modifying CTE can free an arbiter key before the body reuses it.
    // The stale heap version must not turn the body into a second update of it.
    let r = &run_s(
        &mut s,
        "WITH removed AS (DELETE FROM t WHERE id = 2) \
         INSERT INTO t VALUES (2, 'reused') \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v",
    )
    .await[0];
    assert!(matches!(r, QueryResult::Command { tag } if tag == "INSERT 0 1"));

    let r = &run_s(&mut s, "SELECT id, v FROM t ORDER BY id").await[0];
    let values: Vec<Vec<Option<String>>> = rows_of(r)
        .iter()
        .map(|row| row.iter().map(text).collect())
        .collect();
    assert!(
        values
            == vec![
                vec![Some("1".into()), Some("xa".into())],
                vec![Some("2".into()), Some("reused".into())],
                vec![Some("3".into()), Some("c".into())],
                vec![Some("4".into()), Some("d".into())],
            ]
    );

    // A repeatable-read transaction can update its own freshly inserted row,
    // but must retry if a conflicting row committed after its snapshot.
    let own_engine = SqlEngine::new();
    let mut own = own_engine.connect();
    run_s(&mut own, "CREATE TABLE own_t (id int4 PRIMARY KEY, v text)").await;
    run_s(&mut own, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    run_s(&mut own, "INSERT INTO own_t VALUES (1, 'before')").await;
    let r = &run_s(
        &mut own,
        "INSERT INTO own_t VALUES (1, 'after') \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v",
    )
    .await[0];
    assert!(matches!(r, QueryResult::Command { tag } if tag == "INSERT 0 1"));
    run_s(&mut own, "COMMIT").await;
    assert!(
        text_rows_of(&mut own, "SELECT id::text, v FROM own_t").await
            == vec![vec![Some("1".into()), Some("after".into())]]
    );

    let concurrent_engine = SqlEngine::new();
    let mut stale = concurrent_engine.connect();
    let mut writer = concurrent_engine.connect();
    run_s(
        &mut writer,
        "CREATE TABLE concurrent_t (id int4 PRIMARY KEY, v text)",
    )
    .await;
    run_s(&mut stale, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    run_s(
        &mut writer,
        "INSERT INTO concurrent_t VALUES (1, 'committed')",
    )
    .await;
    assert!(
        sqlstate_of(
            &mut stale,
            "INSERT INTO concurrent_t VALUES (1, 'candidate') \
         ON CONFLICT (id) DO UPDATE SET v = excluded.v",
        )
        .await
            == "40001"
    );

    // Under READ COMMITTED, an upsert that waits for a concurrent insert must
    // refresh its snapshot and update that newly committed row.
    let read_committed_engine = SqlEngine::new();
    let mut pending = read_committed_engine.connect();
    run_s(
        &mut pending,
        "CREATE TABLE read_committed_t (id int4 PRIMARY KEY, v text); BEGIN; \
         INSERT INTO read_committed_t VALUES (1, 'pending')",
    )
    .await;
    let upsert_engine = read_committed_engine.clone_handle();
    let upsert = tokio::spawn(async move {
        let mut concurrent = upsert_engine.connect();
        concurrent
            .simple_query(
                "INSERT INTO read_committed_t VALUES (1, 'updated') \
                 ON CONFLICT (id) DO UPDATE SET v = excluded.v",
            )
            .await
    });
    for _ in 0..100 {
        if (0..2).any(|session| {
            read_committed_engine
                .lockmgr
                .waiter_queue_len_as(crate::lockmgr::LockOwner::Session(session))
                != 0
        }) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!((0..2).any(|session| {
        read_committed_engine
            .lockmgr
            .waiter_queue_len_as(crate::lockmgr::LockOwner::Session(session))
            != 0
    }));
    run_s(&mut pending, "COMMIT").await;
    let results = upsert
        .await
        .expect("upsert task joins")
        .expect("upsert succeeds");
    assert!(matches!(&results[0], QueryResult::Command { tag } if tag == "INSERT 0 1"));
}
// ---- D6: foreign keys wired into the local write path ----

#[tokio::test]
async fn on_delete_cascade_removes_the_referencing_rows() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE p (id int4 PRIMARY KEY)").await;
    run_s(
        &mut s,
        "CREATE TABLE c (id int4 PRIMARY KEY, p int4 REFERENCES p (id) ON DELETE CASCADE)",
    )
    .await;
    run_s(&mut s, "INSERT INTO p VALUES (1), (2)").await;
    run_s(&mut s, "INSERT INTO c VALUES (10, 1), (11, 1), (12, 2)").await;
    run_s(&mut s, "DELETE FROM p WHERE id = 1").await;
    assert!(
        text_rows_of(&mut s, "SELECT id FROM c ORDER BY id").await
            == vec![vec![Some("12".to_string())]]
    );
}

#[tokio::test]
async fn a_cascade_cycle_between_two_tables_terminates() {
    use assert2::assert;

    // a -> b -> a, both ON DELETE CASCADE. The cascade comes back around to
    // the row the statement itself deleted, which the drain reads through
    // the staged batch and therefore sees as gone; a cascade that revisits a
    // row *it* deleted cannot be recognised that way and is stopped by
    // `StatementWrites` instead.
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE cyc_a (id int4 PRIMARY KEY, b int4)").await;
    run_s(
        &mut s,
        "CREATE TABLE cyc_b (id int4 PRIMARY KEY, \
             a int4 REFERENCES cyc_a (id) ON DELETE CASCADE)",
    )
    .await;
    run_s(
        &mut s,
        "ALTER TABLE cyc_a ADD CONSTRAINT cyc_a_b_fkey \
             FOREIGN KEY (b) REFERENCES cyc_b (id) ON DELETE CASCADE",
    )
    .await;
    run_s(&mut s, "INSERT INTO cyc_a VALUES (1, NULL)").await;
    run_s(&mut s, "INSERT INTO cyc_b VALUES (1, 1)").await;
    run_s(&mut s, "UPDATE cyc_a SET b = 1 WHERE id = 1").await;
    run_s(&mut s, "DELETE FROM cyc_a WHERE id = 1").await;
    assert!(text_rows_of(&mut s, "SELECT id FROM cyc_a").await == Vec::<Vec<_>>::new());
    assert!(text_rows_of(&mut s, "SELECT id FROM cyc_b").await == Vec::<Vec<_>>::new());
}

#[tokio::test]
async fn a_self_referencing_cascade_terminates_on_a_tree_and_on_a_self_loop() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(
        &mut s,
        "CREATE TABLE tree (id int4 PRIMARY KEY, \
             parent int4 REFERENCES tree (id) ON DELETE CASCADE)",
    )
    .await;
    run_s(&mut s, "INSERT INTO tree VALUES (1, NULL), (2, 1), (3, 2)").await;
    // A row that references itself: the cascade revisits the very row the
    // statement deleted, and stops there.
    run_s(&mut s, "INSERT INTO tree VALUES (4, 4)").await;
    run_s(&mut s, "DELETE FROM tree WHERE id = 4").await;
    run_s(&mut s, "DELETE FROM tree WHERE id = 1").await;
    assert!(text_rows_of(&mut s, "SELECT id FROM tree").await == Vec::<Vec<_>>::new());
}

#[tokio::test]
async fn on_update_cascade_follows_the_referenced_key() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE up (id int4 PRIMARY KEY, v int4)").await;
    run_s(
        &mut s,
        "CREATE TABLE uc (a int4 REFERENCES up (id) ON UPDATE CASCADE)",
    )
    .await;
    run_s(&mut s, "INSERT INTO up VALUES (1, 100)").await;
    run_s(&mut s, "INSERT INTO uc VALUES (1)").await;
    // A non-key update of the parent leaves the child alone and never
    // touches the key lock.
    run_s(&mut s, "UPDATE up SET v = 200 WHERE id = 1").await;
    assert!(text_rows_of(&mut s, "SELECT a FROM uc").await == vec![vec![Some("1".to_string())]]);
    run_s(&mut s, "UPDATE up SET id = 2 WHERE id = 1").await;
    assert!(text_rows_of(&mut s, "SELECT a FROM uc").await == vec![vec![Some("2".to_string())]]);
}

#[tokio::test]
async fn set_null_onto_a_not_null_column_is_the_ordinary_23502() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE np (id int4 PRIMARY KEY)").await;
    run_s(
        &mut s,
        "CREATE TABLE nc (a int4 NOT NULL REFERENCES np (id) ON DELETE SET NULL)",
    )
    .await;
    run_s(&mut s, "INSERT INTO np VALUES (1)").await;
    run_s(&mut s, "INSERT INTO nc VALUES (1)").await;
    assert!(sqlstate_of(&mut s, "DELETE FROM np WHERE id = 1").await == "23502");
}

#[tokio::test]
async fn restrict_and_no_action_report_different_sqlstates() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE rp1 (id int4 PRIMARY KEY)").await;
    run_s(&mut s, "CREATE TABLE rc1 (a int4 REFERENCES rp1 (id))").await;
    run_s(&mut s, "CREATE TABLE rp2 (id int4 PRIMARY KEY)").await;
    run_s(
        &mut s,
        "CREATE TABLE rc2 (a int4 REFERENCES rp2 (id) ON DELETE RESTRICT)",
    )
    .await;
    run_s(
        &mut s,
        "INSERT INTO rp1 VALUES (1); INSERT INTO rc1 VALUES (1)",
    )
    .await;
    run_s(
        &mut s,
        "INSERT INTO rp2 VALUES (1); INSERT INTO rc2 VALUES (1)",
    )
    .await;
    assert!(sqlstate_of(&mut s, "DELETE FROM rp1 WHERE id = 1").await == "23503");
    assert!(sqlstate_of(&mut s, "DELETE FROM rp2 WHERE id = 1").await == "23001");
}

#[tokio::test]
async fn truncate_refuses_a_child_outside_the_set_and_cascade_widens_it() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE tp (id int4 PRIMARY KEY)").await;
    run_s(
        &mut s,
        "CREATE TABLE tc (id int4 PRIMARY KEY, p int4 REFERENCES tp (id) ON DELETE CASCADE)",
    )
    .await;
    run_s(
        &mut s,
        "INSERT INTO tp VALUES (1); INSERT INTO tc VALUES (9, 1)",
    )
    .await;
    assert!(sqlstate_of(&mut s, "TRUNCATE tp").await == "0A000");
    // Naming both relations empties both, and the ON DELETE CASCADE never
    // fires: TRUNCATE widens the set, it does not run referential actions.
    run_s(&mut s, "TRUNCATE tp, tc").await;
    assert!(text_rows_of(&mut s, "SELECT id FROM tc").await == Vec::<Vec<_>>::new());
    run_s(
        &mut s,
        "INSERT INTO tp VALUES (1); INSERT INTO tc VALUES (9, 1)",
    )
    .await;
    run_s(&mut s, "TRUNCATE tp CASCADE").await;
    assert!(text_rows_of(&mut s, "SELECT id FROM tp").await == Vec::<Vec<_>>::new());
    assert!(text_rows_of(&mut s, "SELECT id FROM tc").await == Vec::<Vec<_>>::new());
}

#[tokio::test]
async fn dropping_a_referenced_object_is_2bp01_and_cascade_drops_the_constraint() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE dp (id int4 PRIMARY KEY)").await;
    run_s(&mut s, "CREATE TABLE dc (a int4 REFERENCES dp (id))").await;
    assert!(sqlstate_of(&mut s, "DROP TABLE dp").await == "2BP01");
    assert!(sqlstate_of(&mut s, "ALTER TABLE dp DROP CONSTRAINT dp_pkey").await == "2BP01");
    // CASCADE drops the referencing CONSTRAINT, not the referencing table.
    run_s(&mut s, "DROP TABLE dp CASCADE").await;
    run_s(&mut s, "INSERT INTO dc VALUES (42)").await;
    assert!(text_rows_of(&mut s, "SELECT a FROM dc").await == vec![vec![Some("42".to_string())]]);
}

#[tokio::test]
async fn a_mutually_referencing_pair_can_be_dropped_together() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE mp (id int4 PRIMARY KEY, other int4)").await;
    run_s(
        &mut s,
        "CREATE TABLE mc (id int4 PRIMARY KEY, a int4 REFERENCES mp (id))",
    )
    .await;
    run_s(
        &mut s,
        "ALTER TABLE mp ADD CONSTRAINT mp_other_fkey FOREIGN KEY (other) REFERENCES mc (id)",
    )
    .await;
    run_s(&mut s, "DROP TABLE mp, mc").await;
}

#[tokio::test]
async fn adding_a_foreign_key_back_validates_stored_rows_unless_not_valid() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE bp (id int4 PRIMARY KEY)").await;
    run_s(&mut s, "CREATE TABLE bc (a int4)").await;
    run_s(&mut s, "INSERT INTO bc VALUES (7)").await;
    assert!(
        sqlstate_of(
            &mut s,
            "ALTER TABLE bc ADD CONSTRAINT bv FOREIGN KEY (a) REFERENCES bp (id)"
        )
        .await
            == "23503"
    );
    run_s(
        &mut s,
        "ALTER TABLE bc ADD CONSTRAINT bv FOREIGN KEY (a) REFERENCES bp (id) NOT VALID",
    )
    .await;
    // NOT VALID skips the scan but still governs every later write.
    assert!(sqlstate_of(&mut s, "INSERT INTO bc VALUES (8)").await == "23503");
    assert!(sqlstate_of(&mut s, "ALTER TABLE bc VALIDATE CONSTRAINT bv").await == "23503");
    run_s(&mut s, "INSERT INTO bp VALUES (7)").await;
    run_s(&mut s, "ALTER TABLE bc VALIDATE CONSTRAINT bv").await;
}

#[tokio::test]
async fn a_foreign_key_added_beside_a_column_validates_the_rewritten_rows() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE ap (id int4 PRIMARY KEY)").await;
    run_s(&mut s, "INSERT INTO ap VALUES (5)").await;
    run_s(&mut s, "CREATE TABLE ac (x int4)").await;
    run_s(&mut s, "INSERT INTO ac VALUES (1)").await;
    // The added column fills the existing row with 5, which the constraint
    // must see — storage still holds the row without the column at all.
    run_s(
        &mut s,
        "ALTER TABLE ac ADD COLUMN a int4 DEFAULT 5, \
             ADD CONSTRAINT ac_fk FOREIGN KEY (a) REFERENCES ap (id)",
    )
    .await;
}

#[tokio::test]
async fn renaming_a_referenced_column_rewrites_the_foreign_key() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE qp (id int4 PRIMARY KEY)").await;
    run_s(&mut s, "CREATE TABLE qc (a int4 REFERENCES qp (id))").await;
    run_s(&mut s, "ALTER TABLE qp RENAME COLUMN id TO ident").await;
    run_s(&mut s, "INSERT INTO qp VALUES (1)").await;
    run_s(&mut s, "INSERT INTO qc VALUES (1)").await;
    assert!(sqlstate_of(&mut s, "INSERT INTO qc VALUES (2)").await == "23503");
}

#[tokio::test]
async fn a_foreign_key_can_be_renamed_and_dropped() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE zp (id int4 PRIMARY KEY)").await;
    run_s(&mut s, "CREATE TABLE zc (a int4 REFERENCES zp (id))").await;
    run_s(
        &mut s,
        "ALTER TABLE zc RENAME CONSTRAINT zc_a_fkey TO zc_renamed",
    )
    .await;
    assert!(sqlstate_of(&mut s, "INSERT INTO zc VALUES (1)").await == "23503");
    run_s(&mut s, "ALTER TABLE zc DROP CONSTRAINT zc_renamed").await;
    run_s(&mut s, "INSERT INTO zc VALUES (1)").await;
}

#[tokio::test]
async fn a_foreign_key_on_a_partitioned_table_is_refused_by_name() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run_s(&mut s, "CREATE TABLE pp (id int4 PRIMARY KEY)").await;
    let error = s
        .simple_query(
            "CREATE TABLE part (id int4, a int4 REFERENCES pp (id)) PARTITION BY RANGE (id)",
        )
        .await
        .expect_err("partitioned foreign key");
    assert!(error.code == "0A000");
    assert!(
        error.message
            == "foreign key constraint \"part_a_fkey\" on a partitioned table is not supported"
    );
}

/// The unqualified scan order after `CLUSTER`, in each of `PostgreSQL`'s
/// three spellings. A bare `SELECT` reads the heap in rowid order, so this
/// is the observable form of "the heap was rewritten in index order".
#[tokio::test]
async fn cluster_reorders_the_heap_into_index_order() {
    use assert2::assert;
    // (the CLUSTER spelling, the scan order it must produce)
    // Every expectation differs from the insertion order 3, 1, 2, so a
    // CLUSTER that did nothing would fail each of them.
    let cases: &[(&str, &[&str])] = &[
        ("CLUSTER t USING t_b", &["2", "3", "1"]),
        ("CLUSTER t_b ON t", &["2", "3", "1"]),
        ("CLUSTER t USING t_pkey", &["1", "2", "3"]),
        ("CLUSTER t USING t_c", &["1", "3", "2"]),
    ];
    for (sql, expected) in cases {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(
            &mut session,
            "CREATE TABLE t (a int4 PRIMARY KEY, b text, c int4)",
        )
        .await;
        run_s(&mut session, "CREATE INDEX t_b ON t (b)").await;
        run_s(&mut session, "CREATE INDEX t_c ON t (c)").await;
        run_s(
            &mut session,
            "INSERT INTO t VALUES (3, 'b', 2), (1, 'c', 1), (2, 'a', 3)",
        )
        .await;
        // Insertion order, which is what an unclustered heap reads back as.
        assert!(
            text_rows_of(&mut session, "SELECT a FROM t").await
                == vec![text_row(&["3"]), text_row(&["1"]), text_row(&["2"])],
            "{sql}"
        );
        run_s(&mut session, sql).await;
        let want: Vec<_> = expected.iter().map(|a| text_row(&[a])).collect();
        assert!(
            text_rows_of(&mut session, "SELECT a FROM t").await == want,
            "{sql}"
        );
    }
}

/// `CLUSTER` moves rows to new rowids, and everything that reaches a row by
/// rowid has to follow: the secondary indexes, the row count, and the rows
/// themselves. A row that lost its index entry would still be found by a
/// sequential scan, so both scan shapes are asserted.
#[tokio::test]
async fn cluster_preserves_rows_and_their_index_entries() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE t (a int4 PRIMARY KEY, b text UNIQUE, c int4)",
    )
    .await;
    run_s(&mut session, "CREATE INDEX t_c ON t (c)").await;
    run_s(
        &mut session,
        "INSERT INTO t VALUES (3, 'c', 30), (1, 'a', 10), (2, 'b', 20)",
    )
    .await;
    run_s(&mut session, "CLUSTER t USING t_pkey").await;

    assert!(text_rows_of(&mut session, "SELECT count(*) FROM t").await == vec![text_row(&["3"])]);
    assert!(
        text_rows_of(&mut session, "SELECT a, b, c FROM t ORDER BY a").await
            == vec![
                text_row(&["1", "a", "10"]),
                text_row(&["2", "b", "20"]),
                text_row(&["3", "c", "30"]),
            ]
    );
    // Each index still resolves to exactly the row it named, and to only
    // one of it — a stale entry pointing at a vacated rowid would either
    // vanish or double up here.
    for (sql, expected) in [
        ("SELECT a FROM t WHERE c = 20", "2"),
        ("SELECT a FROM t WHERE b = 'a'", "1"),
        ("SELECT b FROM t WHERE a = 3", "c"),
    ] {
        assert!(
            text_rows_of(&mut session, sql).await == vec![text_row(&[expected])],
            "{sql}"
        );
    }
    // The unique keys are still enforced, and still free for the values the
    // rewrite did not use.
    assert!(sqlstate_of(&mut session, "INSERT INTO t VALUES (4, 'a', 40)").await == "23505");
    assert!(sqlstate_of(&mut session, "INSERT INTO t VALUES (1, 'd', 40)").await == "23505");
    run_s(&mut session, "INSERT INTO t VALUES (4, 'd', 40)").await;
    // A row inserted after the rewrite lands past it, as it does in
    // PostgreSQL: the rowid counter only ever moves forward.
    assert!(
        text_rows_of(&mut session, "SELECT a FROM t").await
            == vec![
                text_row(&["1"]),
                text_row(&["2"]),
                text_row(&["3"]),
                text_row(&["4"]),
            ]
    );
}

/// `pg_index.indisclustered` records the index a later bare `CLUSTER
/// <table>` reorders by. At most one index per relation carries it.
#[tokio::test]
async fn cluster_records_the_index_it_ordered_by() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4 PRIMARY KEY, b text)").await;
    run_s(&mut session, "CREATE INDEX t_b ON t (b)").await;
    run_s(&mut session, "INSERT INTO t VALUES (2, 'b'), (1, 'a')").await;
    let marks = "SELECT indexrelid::regclass::text, indisclustered FROM pg_index \
                     WHERE indrelid = 't'::regclass ORDER BY 1";

    // Nothing is clustered until something says so, and the bare spelling
    // has nothing to reuse.
    assert!(
        text_rows_of(&mut session, marks).await
            == vec![text_row(&["t_b", "f"]), text_row(&["t_pkey", "f"])]
    );
    assert!(sqlstate_of(&mut session, "CLUSTER t").await == "42704");

    // (statement, the index left marked afterwards)
    let cases: &[(&str, Option<&str>)] = &[
        ("CLUSTER t USING t_b", Some("t_b")),
        ("CLUSTER t_pkey ON t", Some("t_pkey")),
        ("ALTER TABLE t CLUSTER ON t_b", Some("t_b")),
        // The bare spelling reuses the mark without moving it.
        ("CLUSTER t", Some("t_b")),
        ("ALTER TABLE t SET WITHOUT CLUSTER", None),
        ("ALTER TABLE t SET WITHOUT OIDS", None),
    ];
    for (sql, marked) in cases {
        run_s(&mut session, sql).await;
        let want = vec![
            text_row(&["t_b", if *marked == Some("t_b") { "t" } else { "f" }]),
            text_row(&["t_pkey", if *marked == Some("t_pkey") { "t" } else { "f" }]),
        ];
        assert!(text_rows_of(&mut session, marks).await == want, "{sql}");
    }
    // Cleared again, so the bare spelling has nothing to reuse.
    assert!(sqlstate_of(&mut session, "CLUSTER t").await == "42704");
}

#[tokio::test]
async fn alter_index_statistics_rejects_zero_attribute_number() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE attmp (a int4)").await;
    run_s(&mut session, "CREATE INDEX attmp_idx ON attmp (a)").await;

    assert!(
        error_of(
            &mut session,
            "ALTER INDEX attmp_idx ALTER COLUMN 0 SET STATISTICS 1000",
        )
        .await
            == (
                "22023".into(),
                "column number must be in range from 1 to 32767".into(),
            )
    );
    assert!(
        error_of(
            &mut session,
            "ALTER INDEX attmp_idx ALTER COLUMN 1 SET STATISTICS 1000",
        )
        .await
            == (
                "42809".into(),
                "cannot alter statistics on non-expression column \"a\" of index \"attmp_idx\""
                    .into(),
            )
    );
}

#[tokio::test]
async fn alter_table_set_schema_moves_the_catalogued_relation() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE SCHEMA moved").await;
    run_s(
        &mut session,
        "CREATE TABLE t (id int4 PRIMARY KEY, payload text)",
    )
    .await;
    run_s(&mut session, "CREATE INDEX t_payload ON t (payload)").await;
    run_s(&mut session, "INSERT INTO t VALUES (1, 'one')").await;

    run_s(&mut session, "ALTER TABLE t SET SCHEMA moved").await;
    assert!(sqlstate_of(&mut session, "SELECT * FROM t").await == "42P01");
    assert!(
        text_rows_of(&mut session, "SELECT id, payload FROM moved.t").await
            == vec![text_row(&["1", "one"])]
    );
    assert!(
        text_rows_of(
            &mut session,
            "SELECT indexrelid::regclass::text FROM pg_index WHERE \
             indrelid = 'moved.t'::regclass ORDER BY 1",
        )
        .await
            == vec![text_row(&["moved.t_payload"]), text_row(&["moved.t_pkey"])]
    );
    run_s(
        &mut session,
        "ALTER TABLE IF EXISTS absent SET SCHEMA moved",
    )
    .await;
    assert!(sqlstate_of(&mut session, "ALTER TABLE absent SET SCHEMA moved").await == "42P01");

    run_s(&mut session, "ALTER TABLE moved.t CLUSTER ON t_payload").await;
    let clustered = "SELECT indisclustered FROM pg_index \
                     WHERE indexrelid = 'moved.t_payload'::regclass";
    assert!(text_rows_of(&mut session, clustered).await == vec![text_row(&["t"])]);
    run_s(&mut session, "ALTER TABLE moved.t SET WITHOUT CLUSTER").await;
    assert!(text_rows_of(&mut session, clustered).await == vec![text_row(&["f"])]);

    // ALTER TABLE applies its fixed subcommand order rather than written
    // order: the drop must happen before the primary key puts NOT NULL back.
    run_s(&mut session, "CREATE TABLE action_order (id int4)").await;
    run_s(
        &mut session,
        "ALTER TABLE action_order ADD CONSTRAINT action_order_pkey PRIMARY KEY (id), \
         ALTER COLUMN id DROP NOT NULL",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT is_nullable FROM information_schema.columns \
             WHERE table_name = 'action_order' AND column_name = 'id'",
        )
        .await
            == vec![text_row(&["NO"])]
    );

    run_s(
        &mut session,
        "CREATE MATERIALIZED VIEW action_order_mv AS SELECT 1 AS id",
    )
    .await;
    let error = error_of(
        &mut session,
        "ALTER MATERIALIZED VIEW action_order_mv ADD COLUMN extra int4",
    )
    .await;
    assert!(
        error
            == (
                "42809".into(),
                "ALTER action ADD COLUMN cannot be performed on relation \"action_order_mv\""
                    .into(),
            ),
        "{error:?}"
    );
}

/// `CLUSTER` is transactional: `ROLLBACK` restores both halves of it — the
/// heap order, which rides MVCC, and the `indisclustered` mark, which is a
/// catalog record and has to be undone from a before-image.
#[tokio::test]
async fn cluster_rolls_back_with_its_transaction() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4 PRIMARY KEY)").await;
    run_s(&mut session, "INSERT INTO t VALUES (2), (1)").await;
    let marked = "SELECT indisclustered FROM pg_index WHERE indrelid = 't'::regclass";

    run_s(&mut session, "BEGIN").await;
    run_s(&mut session, "CLUSTER t USING t_pkey").await;
    assert!(
        text_rows_of(&mut session, "SELECT a FROM t").await
            == vec![text_row(&["1"]), text_row(&["2"])]
    );
    assert!(text_rows_of(&mut session, marked).await == vec![text_row(&["t"])]);
    run_s(&mut session, "ROLLBACK").await;

    assert!(
        text_rows_of(&mut session, "SELECT a FROM t").await
            == vec![text_row(&["2"]), text_row(&["1"])]
    );
    assert!(text_rows_of(&mut session, marked).await == vec![text_row(&["f"])]);

    // And it commits as one, so the same statement under COMMIT keeps both.
    run_s(&mut session, "BEGIN").await;
    run_s(&mut session, "CLUSTER t USING t_pkey").await;
    run_s(&mut session, "COMMIT").await;
    assert!(
        text_rows_of(&mut session, "SELECT a FROM t").await
            == vec![text_row(&["1"]), text_row(&["2"])]
    );
    assert!(text_rows_of(&mut session, marked).await == vec![text_row(&["t"])]);
}

/// The refusals `PostgreSQL` raises, with its SQLSTATEs.
#[tokio::test]
async fn cluster_refuses_what_postgresql_refuses() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4 PRIMARY KEY)").await;
    run_s(
        &mut session,
        "CREATE TABLE p (a int4) PARTITION BY RANGE (a)",
    )
    .await;
    run_s(&mut session, "CREATE INDEX p_a ON p (a)").await;

    // (statement, SQLSTATE)
    let cases: &[(&str, &str)] = &[
        ("CLUSTER nosuch USING t_pkey", "42P01"),
        ("CLUSTER t USING nosuch", "42704"),
        ("CLUSTER t", "42704"),
        ("CLUSTER p", "42704"),
        // A partitioned relation has no heap of its own to mark.
        ("ALTER TABLE p CLUSTER ON p_a", "0A000"),
        ("ALTER TABLE p SET WITHOUT CLUSTER", "0A000"),
    ];
    for (sql, code) in cases {
        assert!(sqlstate_of(&mut session, sql).await == *code, "{sql}");
    }
}

/// A deferred referential check names its row by rowid and re-derives the
/// key at `COMMIT`. Reordering the relation moves the row, so the check
/// would read no version, conclude the row is gone, and let the violation
/// commit. `PostgreSQL` refuses the reorder instead (55006), on either side
/// of the constraint.
#[tokio::test]
async fn cluster_refuses_a_relation_that_owes_a_deferred_check() {
    use assert2::assert;
    // (the statement that defers a check, the relation CLUSTER then names)
    let cases: &[(&str, &str, &str)] = &[
        // Child side: the referencing row has no parent yet.
        ("INSERT INTO ch VALUES (9, 999)", "ch", "ch_pkey"),
        // Parent side: the referenced key is going away.
        ("DELETE FROM par WHERE id = 1", "par", "par_pkey"),
    ];
    for (defer, relation, index) in cases {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run_s(&mut session, "CREATE TABLE par (id int4 PRIMARY KEY)").await;
        run_s(&mut session, "INSERT INTO par VALUES (1)").await;
        run_s(
            &mut session,
            "CREATE TABLE ch (id int4 PRIMARY KEY, p int4 REFERENCES par (id) \
                 DEFERRABLE INITIALLY DEFERRED)",
        )
        .await;
        run_s(&mut session, "INSERT INTO ch VALUES (5, 1), (4, 1)").await;

        run_s(&mut session, "BEGIN").await;
        run_s(&mut session, defer).await;
        let sql = format!("CLUSTER {relation} USING {index}");
        assert!(sqlstate_of(&mut session, &sql).await == "55006", "{sql}");
        run_s(&mut session, "ROLLBACK").await;

        // The constraint still bites when the reorder is not in the way, so
        // the refusal is protecting a check that really would have fired.
        run_s(&mut session, "BEGIN").await;
        run_s(&mut session, defer).await;
        assert!(
            sqlstate_of(&mut session, "COMMIT").await == "23503",
            "{sql}"
        );
    }
}

/// The bare `CLUSTER` reclusters every marked relation, so its target list
/// is not written down and cannot be locked for longer than one statement.
/// `PostgreSQL` refuses it inside a transaction block for the same reason.
#[tokio::test]
async fn bare_cluster_is_refused_inside_a_transaction_block() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4 PRIMARY KEY)").await;
    run_s(&mut session, "INSERT INTO t VALUES (2), (1)").await;
    run_s(&mut session, "CLUSTER t USING t_pkey").await;
    run_s(&mut session, "INSERT INTO t VALUES (0)").await;

    run_s(&mut session, "BEGIN").await;
    assert!(sqlstate_of(&mut session, "CLUSTER").await == "25001");
    run_s(&mut session, "ROLLBACK").await;

    // Outside a block it runs, and reaches the relation it marked earlier.
    run_s(&mut session, "CLUSTER").await;
    assert!(
        text_rows_of(&mut session, "SELECT a FROM t").await
            == vec![text_row(&["0"]), text_row(&["1"]), text_row(&["2"])]
    );
}

/// An expression index orders the heap by the expression's value, not by
/// any stored column, and NULLs sort last as they do in a btree.
#[tokio::test]
async fn cluster_orders_by_expression_keys_and_sorts_nulls_last() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE t (a int4, b int4)").await;
    run_s(&mut session, "CREATE INDEX t_minus_a ON t ((-a))").await;
    run_s(&mut session, "CREATE INDEX t_a ON t (a)").await;
    run_s(
        &mut session,
        "INSERT INTO t VALUES (1, 10), (3, 30), (NULL, 40), (2, 20)",
    )
    .await;

    run_s(&mut session, "CLUSTER t USING t_minus_a").await;
    assert!(
        text_rows_of(&mut session, "SELECT b FROM t").await
            == vec![
                text_row(&["30"]),
                text_row(&["20"]),
                text_row(&["10"]),
                text_row(&["40"]),
            ]
    );
    run_s(&mut session, "CLUSTER t USING t_a").await;
    assert!(
        text_rows_of(&mut session, "SELECT b FROM t").await
            == vec![
                text_row(&["10"]),
                text_row(&["20"]),
                text_row(&["30"]),
                text_row(&["40"]),
            ]
    );
}

/// An expression key has no table column to name, so `pg_index` reports 0
/// in `indkey` and carries the expression in `indexprs` — PostgreSQL's own
/// encoding.
///
/// Resolving the stored key as a column name instead fails the *whole*
/// projection: `pg_index` is built as one list, so a single expression
/// index makes every index on every relation unreadable. Each case below
/// therefore also names a plain index, which is the part that regressed.
#[tokio::test]
async fn pg_index_reports_expression_keys_as_zero_attnums() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE b (kind text, totamt numeric)").await;
    run_s(&mut session, "CREATE INDEX b_kind ON b (kind)").await;
    run_s(&mut session, "CREATE INDEX b_expr ON b ((totamt * 2))").await;
    run_s(
        &mut session,
        "CREATE INDEX b_mixed ON b (kind, (totamt + 1))",
    )
    .await;
    run_s(
        &mut session,
        "CREATE INDEX b_two ON b ((totamt * 3), (totamt * 4))",
    )
    .await;

    // (index, indnatts, indnkeyatts, indkey, indexprs, indpred)
    let want = vec![
        vec![
            Some("b_expr".into()),
            Some("1".into()),
            Some("1".into()),
            Some("0".into()),
            Some("(totamt * 2)".into()),
            None,
        ],
        vec![
            Some("b_kind".into()),
            Some("1".into()),
            Some("1".into()),
            Some("1".into()),
            None,
            None,
        ],
        vec![
            Some("b_mixed".into()),
            Some("2".into()),
            Some("2".into()),
            Some("1 0".into()),
            Some("(totamt + 1)".into()),
            None,
        ],
        vec![
            Some("b_two".into()),
            Some("2".into()),
            Some("2".into()),
            Some("0 0".into()),
            Some("(totamt * 3), (totamt * 4)".into()),
            None,
        ],
    ];
    assert!(
        text_rows_of(
            &mut session,
            "SELECT indexrelid::regclass::text, indnatts, indnkeyatts, indkey::text, \
                 pg_get_expr(indexprs, indrelid), pg_get_expr(indpred, indrelid) \
                 FROM pg_index WHERE indrelid = 'b'::regclass ORDER BY 1",
        )
        .await
            == want
    );

    // The table's own `pg_attribute` rows are the shared machinery behind
    // every `\d`, and an index that cannot be described must not disturb
    // them.
    assert!(
        text_rows_of(
            &mut session,
            "SELECT attname, attnum FROM pg_attribute \
                 WHERE attrelid = 'b'::regclass AND attnum > 0 ORDER BY attnum",
        )
        .await
            == vec![text_row(&["kind", "1"]), text_row(&["totamt", "2"])]
    );
}

/// `\d` on a partitioned table needs three catalog answers, and each was
/// missing or wrong: the key from `pg_get_partkeydef`, the *direct*
/// children from `pg_inherits`, and each child's bound from
/// `pg_class.relpartbound`.
///
/// The fixture is deliberately three levels deep: counting descendants or
/// leaves rather than direct children answers 5 or 4 where PostgreSQL
/// answers 3.
#[tokio::test]
async fn partitioned_table_reports_its_key_bounds_and_direct_children() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE clstrpart (a int) PARTITION BY RANGE (a)",
        "CREATE TABLE clstrpart1 PARTITION OF clstrpart FOR VALUES FROM (1) TO (10) \
             PARTITION BY RANGE (a)",
        "CREATE TABLE clstrpart11 PARTITION OF clstrpart1 FOR VALUES FROM (1) TO (5)",
        "CREATE TABLE clstrpart2 PARTITION OF clstrpart FOR VALUES FROM (10) TO (20)",
        "CREATE TABLE clstrpart3 PARTITION OF clstrpart DEFAULT PARTITION BY RANGE (a)",
    ] {
        run_s(&mut session, sql).await;
    }

    // psql's "Partition key:" line, for every partitioned relation in the
    // tree and for a relation that is not partitioned at all.
    run_s(&mut session, "CREATE TABLE plain (a int)").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT relname, pg_get_partkeydef(oid) FROM pg_class \
                 WHERE relname LIKE 'clstrpart%' OR relname = 'plain' ORDER BY 1",
        )
        .await
            == vec![
                text_row(&["clstrpart", "RANGE (a)"]),
                text_row(&["clstrpart1", "RANGE (a)"]),
                vec![Some("clstrpart11".into()), None],
                vec![Some("clstrpart2".into()), None],
                text_row(&["clstrpart3", "RANGE (a)"]),
                vec![Some("plain".into()), None],
            ]
    );

    // psql's "Number of partitions:" line counts `pg_inherits` rows whose
    // `inhparent` is the relation's own `pg_class` oid — which is the join
    // that found nothing.
    assert!(
        text_rows_of(
            &mut session,
            "SELECT c.relname, count(*)::text FROM pg_class c JOIN pg_inherits i \
                 ON i.inhparent = c.oid WHERE c.relname LIKE 'clstrpart%' \
                 GROUP BY c.relname ORDER BY 1",
        )
        .await
            == vec![
                text_row(&["clstrpart", "3"]),
                text_row(&["clstrpart1", "1"]),
            ]
    );

    // psql's "Partition of: <parent> <bound>" line.
    assert!(
        text_rows_of(
            &mut session,
            "SELECT relname, relispartition, pg_get_expr(relpartbound, oid) FROM pg_class \
                 WHERE relname LIKE 'clstrpart%' ORDER BY 1",
        )
        .await
            == vec![
                vec![Some("clstrpart".into()), Some("f".into()), None],
                text_row(&["clstrpart1", "t", "FOR VALUES FROM (1) TO (10)"]),
                text_row(&["clstrpart11", "t", "FOR VALUES FROM (1) TO (5)"]),
                text_row(&["clstrpart2", "t", "FOR VALUES FROM (10) TO (20)"]),
                text_row(&["clstrpart3", "t", "DEFAULT"]),
            ]
    );
}

/// The other bound spellings `pg_class.relpartbound` has to print, and the
/// `LIST`/`HASH` keys `pg_get_partkeydef` renders for them.
#[tokio::test]
async fn partition_bounds_print_in_postgresql_spelling() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for sql in [
        "CREATE TABLE pr (a int, b date) PARTITION BY RANGE (a, b)",
        "CREATE TABLE pr1 PARTITION OF pr FOR VALUES FROM (MINVALUE, MINVALUE) \
             TO (10, '2020-01-01')",
        "CREATE TABLE pr2 PARTITION OF pr FOR VALUES FROM (10, '2020-01-01') \
             TO (MAXVALUE, MAXVALUE)",
        "CREATE TABLE pl (a int, b text) PARTITION BY LIST (b)",
        "CREATE TABLE pl1 PARTITION OF pl FOR VALUES IN ('x', 'y''z')",
        "CREATE TABLE ph (a int) PARTITION BY HASH (a)",
        "CREATE TABLE ph1 PARTITION OF ph FOR VALUES WITH (MODULUS 4, REMAINDER 0)",
    ] {
        run_s(&mut session, sql).await;
    }
    assert!(
        text_rows_of(
            &mut session,
            "SELECT c.relname, pg_get_partkeydef(p.oid), pg_get_expr(c.relpartbound, c.oid) \
                 FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid \
                 JOIN pg_class p ON p.oid = i.inhparent ORDER BY 1",
        )
        .await
            == vec![
                text_row(&[
                    "ph1",
                    "HASH (a)",
                    "FOR VALUES WITH (modulus 4, remainder 0)"
                ]),
                text_row(&["pl1", "LIST (b)", "FOR VALUES IN ('x', 'y''z')"]),
                text_row(&[
                    "pr1",
                    "RANGE (a, b)",
                    "FOR VALUES FROM (MINVALUE, MINVALUE) TO (10, '2020-01-01')",
                ]),
                text_row(&[
                    "pr2",
                    "RANGE (a, b)",
                    "FOR VALUES FROM (10, '2020-01-01') TO (MAXVALUE, MAXVALUE)",
                ]),
            ]
    );
}

/// `pg_inherits` describes plain `INHERITS` children by the same two oids,
/// so the parent side had to be a `pg_class` oid for them too — `\d` prints
/// "Inherits:" and "Number of child tables:" from exactly this join.
#[tokio::test]
async fn pg_inherits_names_an_inheritance_parent_by_its_relation_oid() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE ip (x int)").await;
    run_s(&mut session, "CREATE TABLE ic (z int) INHERITS (ip)").await;
    run_s(&mut session, "CREATE TABLE ic2 () INHERITS (ip)").await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT ch.relname, pa.relname, i.inhseqno FROM pg_inherits i \
                 JOIN pg_class ch ON ch.oid = i.inhrelid \
                 JOIN pg_class pa ON pa.oid = i.inhparent ORDER BY 1",
        )
        .await
            == vec![text_row(&["ic", "ip", "1"]), text_row(&["ic2", "ip", "1"])]
    );
}

/// A stored parent name that resolves to no relation costs *that row* its
/// parent oid, and costs nothing else.
///
/// It used to cost the whole projection. The parent lookup raised
/// `UndefinedTable` for the statement, so one stale key took
/// `SELECT * FROM pg_inherits` — and psql's `\d` on every relation in the
/// database — down with it, including relations with no inheritance at all.
///
/// The store is built directly rather than through statements because the
/// guard has to hold for a key no statement is supposed to leave: one
/// written by a crash, a partial batch, or a producer bug not yet found.
/// Fixing a producer removes today's route to the state; it does not make
/// the projection total.
#[test]
fn an_unresolvable_parent_costs_one_row_its_oid_not_the_projection() {
    use assert2::assert;
    use crabka_pgkv::Kv as _;
    use crabka_pgtypes::{ColumnType, Datum};

    let kv = crabka_pgkv::MemKv::new();
    let column = || vec![crabka_pgcatalog::Column::new("i", ColumnType::Int4)];
    let healthy = RelationName::public("amp_parent");
    let child = RelationName::public("amp_child");
    let gone = RelationName::public("amp_departed");
    let healthy_id =
        crabka_pgcatalog::create_table(&kv, &healthy, column()).expect("create the parent");
    let child_id = crabka_pgcatalog::create_table(&kv, &child, column()).expect("create the child");
    // `amp_departed` is never created: the child's list names a relation
    // the catalog does not hold, which is the state under test.
    kv.write_batch(&crate::inheritance::attach_ops(
        &child,
        &[healthy.clone(), gone],
    ))
    .expect("link the child to both names");

    let oid = |id| crate::catalog_rel::table_relation_oid(id).expect("oid");
    assert!(
        super::pg_inherits_rows(&kv).expect("pg_inherits stays readable")
            == vec![
                vec![
                    Datum::Int4(oid(child_id)),
                    Datum::Int4(oid(healthy_id)),
                    Datum::Int4(1),
                    Datum::Bool(false),
                ],
                vec![
                    Datum::Int4(oid(child_id)),
                    Datum::Int4(super::UNRESOLVED_PARENT_OID),
                    Datum::Int4(2),
                    Datum::Bool(false),
                ],
            ]
    );
    // The substitute has to resolve to nothing, or the row would read as a
    // link to whichever relation did carry that oid.
    assert!(
        crate::catalog_rel::relation_for_oid(&kv, super::UNRESOLVED_PARENT_OID)
            .expect("reverse lookup")
            .is_none()
    );
}

/// A generated column's expression lives in `pg_attrdef` in PostgreSQL, and
/// `atthasdef` is what says so — psql's `\d` reads the body of `generated
/// always as (…) stored` through that flag and finds nothing without it.
#[tokio::test]
async fn a_generated_column_carries_its_expression_in_pg_attrdef() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(
        &mut session,
        "CREATE TABLE gen (a int, b int GENERATED ALWAYS AS (a * 2) STORED, c int DEFAULT 7)",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT a.attname, a.atthasdef, a.attgenerated, \
                 pg_get_expr(d.adbin, d.adrelid, true) \
                 FROM pg_attribute a LEFT JOIN pg_attrdef d \
                 ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
                 WHERE a.attrelid = 'gen'::regclass AND a.attnum > 0 ORDER BY a.attnum",
        )
        .await
            == vec![
                vec![
                    Some("a".into()),
                    Some("f".into()),
                    Some(String::new()),
                    None,
                ],
                text_row(&["b", "t", "s", "a * 2"]),
                text_row(&["c", "t", "", "7"]),
            ]
    );
}

/// Walk one engine's user-type oid counter past the oids the rest of this
/// binary's tests reach, then create the two user types the `unknown`
/// literal tests assign to.
///
/// The registry a type *name* resolves through is process-wide, while every
/// engine allocates type oids from its own counter starting at the same
/// base. So the first user type of any two engines in this process claim
/// one oid, and whichever test creates its type last rebinds the other's
/// name to its own definition — a `CREATE DOMAIN posint` here resolving to
/// a neighbouring test's composite `pair`, and failing with `malformed
/// record literal`. That is a defect in the registry, and is documented on
/// `crabka_pgtypes::usertype::CatalogTypes` itself; until it is keyed by
/// catalog, a test that depends on a user type has to stay off the oids its
/// neighbours use.
async fn create_private_user_types(session: &mut SqlSession) {
    for index in 0..8 {
        run_s(session, &format!("CREATE DOMAIN oidburn{index} AS int4")).await;
    }
    run_s(session, "CREATE DOMAIN posint AS int4 CHECK (VALUE > 0)").await;
    run_s(session, "CREATE TYPE mood AS ENUM ('sad', 'ok')").await;
}

/// The SQLSTATE and message one statement fails with, for a test that has a
/// session in hand.
async fn error_of(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} must fail"));
    (error.code, error.message)
}

/// `PostgreSQL` types an unadorned `'…'` `unknown`, and an `unknown` in the
/// target list of an `INSERT`'s feeding query takes the *target column's*
/// type, parsed by that type's input function. So `INSERT INTO t SELECT
/// '(0,0)'` stores a point, exactly as the `VALUES` spelling of the same row
/// does, and neither is the `text` that `SELECT '(0,0)'` alone produces.
///
/// Every literal below is spelled so that its canonical rendering differs
/// from the text that was written, wherever the type allows one: a value
/// that had been stored as text would read back as the input text.
#[tokio::test]
async fn an_insert_select_resolves_a_bare_literal_against_the_target_column() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    create_private_user_types(&mut session).await;

    // (column type, the literal written, what the column reads back as)
    let cases: &[(&str, &str, &str)] = &[
        ("point", "(0,0)", "(0,0)"),
        // The input functions strip the padding, fold the spelling, order
        // the corners and round to the column's scale; a text store could
        // do none of it.
        ("int4", " 42 ", "42"),
        ("boolean", "yes", "t"),
        ("date", "1997-02-10", "1997-02-10"),
        ("interval", "1 day 2 hours", "1 day 02:00:00"),
        ("box", "((0,0),(1,1))", "(1,1),(0,0)"),
        (
            "uuid",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        ),
        ("numeric(4,2)", "1.005", "1.01"),
        ("int4[]", "{1,2,3}", "{1,2,3}"),
        ("int4range", "[1,4)", "[1,4)"),
        ("jsonpath", "$.a", "$.\"a\""),
        ("mood", "ok", "ok"),
        // A domain literal is parsed by the base type's input function.
        ("posint", " 5 ", "5"),
    ];
    for (index, (ty, literal, stored)) in cases.iter().enumerate() {
        let table = format!("unk{index}");
        run_s(&mut session, &format!("CREATE TABLE {table} (f1 {ty})")).await;
        run_s(
            &mut session,
            &format!("INSERT INTO {table} SELECT '{literal}'"),
        )
        .await;
        // The same row through the VALUES spelling, which already resolved
        // the literal: the two paths must not disagree about one value.
        run_s(
            &mut session,
            &format!("INSERT INTO {table} VALUES ('{literal}')"),
        )
        .await;
        assert!(
            text_rows_of(&mut session, &format!("SELECT f1 FROM {table}")).await
                == vec![text_row(&[stored]), text_row(&[stored])],
            "{ty}"
        );
    }

    // The upstream `gist` case this began as: one literal, a row per row of
    // the feeding query.
    run_s(&mut session, "CREATE TABLE point_gist_tbl (f1 point)").await;
    run_s(
        &mut session,
        "INSERT INTO point_gist_tbl SELECT '(0,0)' FROM generate_series(0, 1000)",
    )
    .await;
    assert!(
        text_rows_of(
            &mut session,
            "SELECT count(*), min(f1 <-> '(3,4)'::point) FROM point_gist_tbl",
        )
        .await
            == vec![text_row(&["1001", "5"])]
    );
}

/// Only the literal itself is `unknown`. Everything else in a feeding
/// query's target list already carries a type, so assigning it to a column
/// of another type is still 42804 — the whole point of the `unknown` rule is
/// that it does not coerce a genuine `text` expression.
///
/// A set operation and a derived table are the two that look like literals
/// and are not: `PostgreSQL` resolves an all-unknown set-op column to `text`
/// through `select_common_type`, and has coerced a sub-select's unknown
/// outputs to `text` at the boundary since PostgreSQL 10.
#[tokio::test]
async fn an_insert_select_leaves_a_typed_expression_typed() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run_s(&mut session, "CREATE TABLE pt (f1 point)").await;
    run_s(&mut session, "CREATE TABLE src (t text)").await;
    run_s(&mut session, "INSERT INTO src VALUES ('(0,0)')").await;

    let refused = [
        // A genuine text expression.
        "INSERT INTO pt SELECT t FROM src",
        "INSERT INTO pt SELECT lower('(0,0)')",
        // An explicit cast types the literal itself.
        "INSERT INTO pt SELECT '(0,0)'::text",
        "INSERT INTO pt SELECT CAST('(0,0)' AS text)",
        // A CASE resolves its arms to a common type first.
        "INSERT INTO pt SELECT CASE WHEN true THEN '(0,0)' END",
        // A set operation.
        "INSERT INTO pt SELECT '(0,0)' UNION ALL SELECT '(1,1)'",
        "INSERT INTO pt SELECT '(0,0)' UNION SELECT '(1,1)'",
        // A derived table, and the wildcard that reads one.
        "INSERT INTO pt SELECT * FROM (VALUES ('(0,0)')) v",
        "INSERT INTO pt SELECT * FROM src",
    ];
    for sql in refused {
        assert!(
            error_of(&mut session, sql).await
                == (
                    "42804".to_string(),
                    "column is of type point but expression is of type text".to_string(),
                ),
            "{sql}"
        );
    }
    assert!(
        text_rows_of(&mut session, "SELECT f1 FROM pt").await == Vec::<Vec<Option<String>>>::new()
    );
}

/// The safety probe. Resolving the literal against the target column widens
/// what an `INSERT … SELECT` accepts, so every rule that used to stand
/// between a value and the table has to still stand: the input function's
/// own parse, the assignment-context length rule, a `CHECK`, a domain's
/// constraint, `NOT NULL`, and the range of the type.
///
/// Each case is one row written two ways. The `VALUES` spelling has always
/// resolved the literal, so it is the oracle: the `SELECT` spelling must
/// fail with the same SQLSTATE and the same message, and write nothing.
#[tokio::test]
async fn an_insert_select_refuses_what_the_values_spelling_refuses() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    create_private_user_types(&mut session).await;
    for ddl in [
        "CREATE TABLE vc (f1 varchar(3))",
        "CREATE TABLE chk (f1 int4 CHECK (f1 > 0))",
        "CREATE TABLE dom (f1 posint)",
        "CREATE TABLE nn (a int4, b int4 NOT NULL)",
        "CREATE TABLE i4 (f1 int4)",
        "CREATE TABLE en (f1 mood)",
        "CREATE TABLE pt (f1 point)",
        "CREATE TABLE nm (f1 numeric(4,2))",
    ] {
        run_s(&mut session, ddl).await;
    }

    // (table, the VALUES spelling, the SELECT spelling, SQLSTATE, message)
    let cases: &[(&str, &str, &str, &str, &str)] = &[
        // An assignment, so the over-long value is 22001 and NOT the silent
        // truncation `'abcd'::varchar(3)` would have done.
        (
            "vc",
            "INSERT INTO vc VALUES ('abcd')",
            "INSERT INTO vc SELECT 'abcd'",
            "22001",
            "value too long for type character varying(3)",
        ),
        (
            "chk",
            "INSERT INTO chk VALUES ('-1')",
            "INSERT INTO chk SELECT '-1'",
            "23514",
            "new row for relation \"chk\" violates check constraint \"chk_f1_check\"",
        ),
        // The literal is parsed by the domain's BASE type, which leaves the
        // domain's own constraint to `coerce` — this proves `coerce` still
        // runs on the resolved value.
        (
            "dom",
            "INSERT INTO dom VALUES ('-1')",
            "INSERT INTO dom SELECT '-1'",
            "23514",
            "value for domain posint violates check constraint \"posint_check\"",
        ),
        (
            "nn",
            "INSERT INTO nn (a, b) VALUES ('1', NULL)",
            "INSERT INTO nn (a, b) SELECT '1', NULL",
            "23502",
            "null value in column \"b\" of relation \"nn\" violates not-null constraint",
        ),
        (
            "i4",
            "INSERT INTO i4 VALUES ('99999999999')",
            "INSERT INTO i4 SELECT '99999999999'",
            "22003",
            "value \"99999999999\" is out of range for type integer",
        ),
        (
            "nm",
            "INSERT INTO nm VALUES ('123.45')",
            "INSERT INTO nm SELECT '123.45'",
            "22003",
            "integer out of range",
        ),
        // Malformed input is the input function's own 22P02, never a silent
        // accept and never the 42804 the unresolved literal used to give.
        (
            "en",
            "INSERT INTO en VALUES ('bogus')",
            "INSERT INTO en SELECT 'bogus'",
            "22P02",
            "invalid input value for enum mood: \"bogus\"",
        ),
        (
            "pt",
            "INSERT INTO pt VALUES ('asdfasdf')",
            "INSERT INTO pt SELECT 'asdfasdf'",
            "22P02",
            "invalid input syntax for type point: \"asdfasdf\"",
        ),
        (
            "i4",
            "INSERT INTO i4 VALUES ('zz')",
            "INSERT INTO i4 SELECT 'zz'",
            "22P02",
            "invalid input syntax for type integer: \"zz\"",
        ),
    ];
    for (table, values, select, code, message) in cases {
        let expected = ((*code).to_string(), (*message).to_string());
        assert!(error_of(&mut session, values).await == expected, "{values}");
        assert!(error_of(&mut session, select).await == expected, "{select}");
        assert!(
            text_rows_of(&mut session, &format!("SELECT count(*) FROM {table}")).await
                == vec![text_row(&["0"])],
            "{select} wrote a row"
        );
    }
}

/// The resolution belongs to the source rows an `INSERT` builds, so it
/// reaches every flavour that takes its rows from a query: the plain
/// statement, `ON CONFLICT`, a partitioned parent that routes the row to a
/// leaf, and an automatically updatable view.
///
/// `ORDER BY`, `LIMIT` and a `WITH` prefix wrap the target list without
/// retyping it, and a literal keeps its place among typed columns.
#[tokio::test]
async fn every_query_fed_insert_resolves_the_literal() {
    use assert2::assert;
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for ddl in [
        "CREATE TABLE plain (k int4, f1 point)",
        "CREATE TABLE oc (k int4 PRIMARY KEY, f1 point)",
        "CREATE TABLE parts (k int4, f1 point) PARTITION BY RANGE (k)",
        "CREATE TABLE parts_lo PARTITION OF parts FOR VALUES FROM (0) TO (10)",
        "CREATE TABLE base (k int4, f1 point)",
        "CREATE VIEW v AS SELECT * FROM base",
    ] {
        run_s(&mut session, ddl).await;
    }

    // (the statement, where its row lands)
    let cases: &[(&str, &str)] = &[
        ("INSERT INTO plain SELECT 1, '(0,0)'", "plain"),
        // A clause that only wraps the target list is transparent.
        ("INSERT INTO plain SELECT 2, '(0,0)' ORDER BY 1", "plain"),
        ("INSERT INTO plain SELECT 3, '(0,0)' LIMIT 1", "plain"),
        (
            "INSERT INTO plain WITH c AS (SELECT 4) SELECT k, '(0,0)' FROM c AS c(k)",
            "plain",
        ),
        (
            "INSERT INTO oc SELECT 1, '(0,0)' ON CONFLICT (k) DO NOTHING",
            "oc",
        ),
        ("INSERT INTO parts SELECT 1, '(0,0)'", "parts_lo"),
        ("INSERT INTO v SELECT 1, '(0,0)'", "base"),
    ];
    for (sql, table) in cases {
        run_s(&mut session, sql).await;
        assert!(
            text_rows_of(&mut session, &format!("SELECT f1 FROM {table}")).await
                == vec![text_row(&["(0,0)"])],
            "{sql}"
        );
        run_s(&mut session, &format!("DELETE FROM {table}")).await;
    }
}
