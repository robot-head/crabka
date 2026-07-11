use crabka_gres_ranges::{
    HashShardSpec, MultiRangeTenant, MultiRangeTenantConfig, RangeId, StatementKind, TableId,
    TenantName,
};
use crabka_pgwire::engine::{
    BoundParam, CloseTarget, Engine, ExecuteOutcome, QueryResult, Session,
};

#[tokio::test]
async fn gateway_owns_multiple_portals_cursor_close_and_sync_lifetimes() {
    let (gateway, _handles) = MultiRangeTenant::start(tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4); INSERT INTO t150 VALUES (1), (2), (3)")
        .await
        .expect("seed");

    session
        .parse("statement", "SELECT id FROM t150 ORDER BY id", &[])
        .await
        .expect("parse");
    session
        .bind("first", "statement", &[], &[0])
        .await
        .expect("bind first");
    session
        .bind("second", "statement", &[], &[1])
        .await
        .expect("bind second");

    let ExecuteOutcome::Rows { rows, completion } =
        session.execute("first", 1).await.expect("first page")
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 1);
    assert!(completion.is_none());
    let ExecuteOutcome::Rows { rows, completion } =
        session.execute("first", 0).await.expect("resume")
    else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(completion.as_deref(), Some("SELECT 3"));

    session
        .close(CloseTarget::Portal("first"))
        .await
        .expect("close portal");
    assert_eq!(
        session.execute("first", 0).await.expect_err("closed").code,
        "34000"
    );
    session.sync().await.expect("sync");
    assert_eq!(
        session.execute("second", 0).await.expect_err("synced").code,
        "34000"
    );
    session
        .describe_statement("statement")
        .await
        .expect("prepared survives sync");
}

#[tokio::test]
async fn failed_unnamed_replacements_remove_old_gateway_resources() {
    let (gateway, _handles) = MultiRangeTenant::start(tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session.parse("", "SELECT 1", &[]).await.expect("parse");
    session.bind("", "", &[], &[]).await.expect("bind");

    session
        .parse("", "SELECT FROM", &[])
        .await
        .expect_err("bad parse");
    assert_eq!(
        session
            .describe_statement("")
            .await
            .expect_err("removed")
            .code,
        "26000"
    );

    session.parse("", "SELECT 1", &[]).await.expect("reparse");
    session.bind("", "", &[], &[]).await.expect("rebind");
    session
        .bind("", "missing", &[], &[])
        .await
        .expect_err("bad bind");
    assert_eq!(
        session.describe_portal("").await.expect_err("removed").code,
        "34000"
    );
}

#[tokio::test]
async fn gateway_owned_command_portal_executes_side_effect_once() {
    let (gateway, _handles) = MultiRangeTenant::start(tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .parse("ddl", "CREATE TABLE t150 (id int4)", &[])
        .await
        .expect("parse");
    session.bind("ddl", "ddl", &[], &[]).await.expect("bind");
    session.execute("ddl", 0).await.expect("first execute");
    session.execute("ddl", 0).await.expect("cached execute");
}

#[tokio::test]
async fn gateway_transaction_parse_rejects_parameter_type_hints() {
    let (gateway, _handles) = MultiRangeTenant::start(tenant_config()).expect("tenant");
    let mut session = gateway.connect();

    let error = session
        .parse("commit", "COMMIT WORK", &[23])
        .await
        .expect_err("transaction control has no parameters");

    assert_eq!(error.code, "42P02");
    assert_eq!(
        session
            .describe_statement("commit")
            .await
            .expect_err("failed parse owns no statement")
            .code,
        "26000"
    );
}

fn tenant_config() -> MultiRangeTenantConfig {
    MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_a").expect("tenant"),
        "0,100,200",
    )
    .expect("config")
}

fn row_split_tenant_config() -> MultiRangeTenantConfig {
    MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_row_split").expect("tenant"),
        "0:0,150:100",
    )
    .expect("config")
}

fn hash_split_tenant_config() -> MultiRangeTenantConfig {
    MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_hash_split").expect("tenant"),
        "0:0:0,150:0:0,150:8:0,151:0:0",
    )
    .expect("config")
}

#[tokio::test]
async fn ddl_routes_to_range0_and_dml_routes_to_table_owner() {
    let (gateway, handles) = MultiRangeTenant::start(tenant_config()).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t150 VALUES (7)")
        .await
        .expect("insert");

    let routes = handles.route_log().await;
    assert_eq!(routes[0].kind, StatementKind::Ddl);
    assert_eq!(routes[0].range_id, RangeId::COORDINATOR);
    assert_eq!(routes[1].kind, StatementKind::Dml);
    assert_eq!(routes[1].range_id, RangeId::new(1));
}

#[tokio::test]
async fn cross_range_statement_all_sharded_tables_is_allowed() {
    let (gateway, _handles) = MultiRangeTenant::start(tenant_config()).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t50 (id int4) SHARDED")
        .await
        .expect("create t50");
    session
        .simple_query("CREATE TABLE t150 (id int4) SHARDED")
        .await
        .expect("create t150");
    session
        .simple_query("SELECT * FROM t50 JOIN t150 ON true")
        .await
        .expect("all-sharded cross-range statement is allowed");
}

#[tokio::test]
async fn cross_range_statement_mixed_sharded_tables_is_rejected() {
    let (gateway, _handles) = MultiRangeTenant::start(tenant_config()).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t50 (id int4) SHARDED")
        .await
        .expect("create t50");
    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create t150");

    let error = session
        .simple_query("SELECT * FROM t50 JOIN t150 ON true")
        .await
        .expect_err("mixed sharded cross-range statement rejected");

    assert_eq!(error.code, "0A000");
}

#[tokio::test]
async fn cross_range_statement_all_unsharded_tables_is_rejected() {
    let (gateway, _handles) = MultiRangeTenant::start(tenant_config()).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t50 (id int4)")
        .await
        .expect("create t50");
    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create t150");

    let error = session
        .simple_query("SELECT * FROM t50 JOIN t150 ON true")
        .await
        .expect_err("unsharded cross-range statement rejected");

    assert_eq!(error.code, "0A000");
}

#[tokio::test]
async fn sharded_statements_route_by_id_before_table_suffix_fallback() {
    let (gateway, handles) = MultiRangeTenant::start(row_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t150 VALUES (20, 1)")
        .await
        .expect("insert below boundary");
    session
        .simple_query("INSERT INTO t150 VALUES (120, 2)")
        .await
        .expect("insert above boundary");

    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 WHERE id = 20").await,
        vec![1]
    );
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 WHERE id = 120").await,
        vec![2]
    );
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 ORDER BY id").await,
        vec![1, 2]
    );

    let routes = handles.route_log().await;
    let routed_ranges = routes
        .iter()
        .filter(|record| matches!(record.kind, StatementKind::Dml | StatementKind::Query))
        .map(|record| record.range_id)
        .collect::<Vec<_>>();
    assert_eq!(
        routed_ranges,
        vec![
            RangeId::COORDINATOR,
            RangeId::new(1),
            RangeId::COORDINATOR,
            RangeId::new(1),
            RangeId::COORDINATOR,
        ]
    );
}

#[tokio::test]
async fn row_sharded_insert_with_explicit_columns_missing_id_fails_clear() {
    let (gateway, _handles) = MultiRangeTenant::start(row_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED")
        .await
        .expect("create");

    let error = session
        .simple_query("INSERT INTO t150 (value) VALUES (7)")
        .await
        .expect_err("missing row shard key rejected");

    assert_eq!(error.code, "0A000");
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 ORDER BY id").await,
        Vec::<i32>::new()
    );
}

#[tokio::test]
async fn hash_sharded_equality_routes_to_deterministic_bucket_range() {
    let (gateway, handles) = MultiRangeTenant::start(hash_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t150 VALUES (42, 7)")
        .await
        .expect("insert");

    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 WHERE id = 42").await,
        vec![7]
    );

    let spec =
        HashShardSpec::new(TableId::new(150), vec!["id".into()], 16, None).expect("hash spec");
    let expected_range = handles
        .range_map()
        .route_hash_equality(&spec, 42_i32.to_be_bytes())
        .expect("route")
        .range_id;
    let routes = handles.route_log().await;
    let routed_ranges = routes
        .iter()
        .filter(|record| matches!(record.kind, StatementKind::Dml | StatementKind::Query))
        .map(|record| record.range_id)
        .collect::<Vec<_>>();

    assert_eq!(routed_ranges, vec![expected_range, expected_range]);
}

#[tokio::test]
async fn hash_sharded_insert_with_explicit_columns_missing_hash_key_fails_clear() {
    let (gateway, _handles) = MultiRangeTenant::start(hash_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");

    let error = session
        .simple_query("INSERT INTO t150 (value) VALUES (7)")
        .await
        .expect_err("missing hash shard key rejected");

    assert_eq!(error.code, "0A000");
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 ORDER BY id").await,
        Vec::<i32>::new()
    );
}

#[tokio::test]
async fn hash_sharded_multi_row_insert_routes_when_all_rows_share_range() {
    let (gateway, handles) = MultiRangeTenant::start(hash_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");
    let spec =
        HashShardSpec::new(TableId::new(150), vec!["id".into()], 16, None).expect("hash spec");
    let (first_id, second_id, expected_range) = same_hash_range_ids(&handles.range_map(), &spec);

    session
        .simple_query(&format!(
            "INSERT INTO t150 VALUES ({first_id}, 10), ({second_id}, 20)"
        ))
        .await
        .expect("same-range insert");

    let routes = handles.route_log().await;
    assert_eq!(routes[1].range_id, expected_range);
}

#[tokio::test]
async fn sharded_multi_row_insert_spanning_row_ranges_commits_atomically() {
    let (gateway, _handles) = MultiRangeTenant::start(row_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED")
        .await
        .expect("create");

    session
        .simple_query("INSERT INTO t150 VALUES (20, 10), (120, 20)")
        .await
        .expect("cross-range row insert");
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 ORDER BY value").await,
        vec![10, 20]
    );
}

#[tokio::test]
async fn in_process_scanner_merges_partial_aggregates_across_row_ranges() {
    let (gateway, _handles) = MultiRangeTenant::start(row_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED")
        .await
        .expect("create");
    for statement in [
        "INSERT INTO t150 VALUES (20, 5)",
        "INSERT INTO t150 VALUES (30, 15)",
        "INSERT INTO t150 VALUES (40, 35)",
        "INSERT INTO t150 VALUES (120, 25)",
        "INSERT INTO t150 VALUES (130, NULL)",
    ] {
        session.simple_query(statement).await.expect(statement);
    }

    assert_eq!(
        select_scalar(&mut session, "SELECT AVG(value) FROM t150 WHERE id >= 30").await,
        "25.0000000000000000"
    );
    assert_eq!(
        select_scalar(&mut session, "SELECT COUNT(value) FROM t150 WHERE id >= 30").await,
        "3"
    );
    assert_eq!(
        select_scalar(&mut session, "SELECT SUM(value) FROM t150 WHERE id >= 30").await,
        "75"
    );
}

#[tokio::test]
async fn sharded_multi_row_insert_in_explicit_transaction_remains_fail_clear() {
    let (gateway, _handles) = MultiRangeTenant::start(row_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");

    let error = session
        .simple_query("INSERT INTO t150 VALUES (20, 10), (120, 20)")
        .await
        .expect_err("explicit transaction rejected");

    assert_eq!(error.code, "0A000");
    session.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 ORDER BY id").await,
        Vec::<i32>::new()
    );
}

#[tokio::test]
async fn parameterized_row_insert_is_rejected_before_binding() {
    let (gateway, _handles) = MultiRangeTenant::start(row_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED")
        .await
        .expect("create");
    let params = [text_param("20")];

    let error = session
        .extended_query_v2("INSERT INTO t150 VALUES ($1, 7)", &params)
        .await
        .expect_err("parameterized row insert rejected");

    assert_eq!(error.code, "0A000");
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 ORDER BY id").await,
        Vec::<i32>::new()
    );
}

#[tokio::test]
async fn non_literal_row_insert_is_rejected_before_execution() {
    let (gateway, _handles) = MultiRangeTenant::start(row_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED")
        .await
        .expect("create");

    let error = session
        .simple_query("INSERT INTO t150 VALUES (10 + 10, 7)")
        .await
        .expect_err("non-literal row insert rejected");

    assert_eq!(error.code, "0A000");
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 ORDER BY id").await,
        Vec::<i32>::new()
    );
}

#[tokio::test]
async fn sharded_broad_update_delete_remain_fail_clear() {
    let (gateway, _handles) = MultiRangeTenant::start(row_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t150 VALUES (20, 10)")
        .await
        .expect("insert first row");
    session
        .simple_query("INSERT INTO t150 VALUES (120, 20)")
        .await
        .expect("insert second row");

    let update_error = session
        .simple_query("UPDATE t150 SET value = 99")
        .await
        .expect_err("broad update rejected");
    let delete_error = session
        .simple_query("DELETE FROM t150")
        .await
        .expect_err("broad delete rejected");

    assert_eq!(update_error.code, "0A000");
    assert_eq!(delete_error.code, "0A000");
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 ORDER BY id").await,
        vec![10, 20]
    );
}

#[tokio::test]
async fn hash_sharded_multi_row_insert_spanning_ranges_commits_atomically() {
    let (gateway, handles) = MultiRangeTenant::start(hash_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");
    let spec =
        HashShardSpec::new(TableId::new(150), vec!["id".into()], 16, None).expect("hash spec");
    let (first_id, second_id) = cross_hash_range_ids(&handles.range_map(), &spec);

    session
        .simple_query(&format!(
            "INSERT INTO t150 VALUES ({first_id}, 10), ({second_id}, 20)"
        ))
        .await
        .expect("cross-range hash insert");
    assert_eq!(
        select_values(&mut session, "SELECT value FROM t150 ORDER BY value").await,
        vec![10, 20]
    );
}

#[tokio::test]
async fn parameterized_hash_insert_is_rejected_before_binding() {
    let (gateway, _handles) = MultiRangeTenant::start(hash_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");
    let params = [text_param("42")];

    let error = session
        .extended_query_v2("INSERT INTO t150 VALUES ($1, 7)", &params)
        .await
        .expect_err("parameterized insert rejected");

    assert_eq!(error.code, "0A000");
}

#[tokio::test]
async fn parameterized_hash_select_is_rejected_before_binding() {
    let (gateway, _handles) = MultiRangeTenant::start(hash_split_tenant_config()).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");
    let params = [text_param("42")];

    let error = session
        .extended_query_v2("SELECT value FROM t150 WHERE id = $1", &params)
        .await
        .expect_err("parameterized select rejected");

    assert_eq!(error.code, "0A000");
}

#[tokio::test]
async fn sharded_scan_fails_when_required_range_is_not_hosted() {
    let config = hash_split_tenant_config()
        .with_hosted_ranges(vec![RangeId::COORDINATOR, RangeId::new(1)])
        .expect("hosted ranges");
    let (gateway, _handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t150 (id int4, value int4) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");

    let error = session
        .simple_query("SELECT value FROM t150 ORDER BY id")
        .await
        .expect_err("partial scan rejected");

    assert_eq!(error.code, "0A000");
}

fn same_hash_range_ids(
    range_map: &crabka_gres_ranges::RangeMap,
    spec: &HashShardSpec,
) -> (i32, i32, RangeId) {
    for first_id in 0_i32..100 {
        let first_range = hash_range(range_map, spec, first_id);
        for second_id in first_id + 1..100 {
            if hash_range(range_map, spec, second_id) == first_range {
                return (first_id, second_id, first_range);
            }
        }
    }
    panic!("expected two ids in one hash range")
}

fn cross_hash_range_ids(
    range_map: &crabka_gres_ranges::RangeMap,
    spec: &HashShardSpec,
) -> (i32, i32) {
    let first_id = 0_i32;
    let first_range = hash_range(range_map, spec, first_id);
    for second_id in 1_i32..100 {
        if hash_range(range_map, spec, second_id) != first_range {
            return (first_id, second_id);
        }
    }
    panic!("expected ids in different hash ranges")
}

fn hash_range(range_map: &crabka_gres_ranges::RangeMap, spec: &HashShardSpec, id: i32) -> RangeId {
    range_map
        .route_hash_equality(spec, id.to_be_bytes())
        .expect("route")
        .range_id
}

fn text_param(value: &str) -> BoundParam {
    BoundParam {
        type_oid: None,
        format: 0,
        value: Some(value.as_bytes().to_vec().into()),
    }
}

async fn select_values(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    sql: &str,
) -> Vec<i32> {
    let results = session.simple_query(sql).await.expect(sql);
    let [QueryResult::Rows { rows, .. }] = results.as_slice() else {
        panic!("expected rows")
    };
    rows.iter()
        .map(|row| {
            let cell = row[0].as_ref().expect("cell");
            std::str::from_utf8(&cell.text)
                .expect("utf8")
                .parse::<i32>()
                .expect("i32")
        })
        .collect()
}

async fn select_scalar(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    sql: &str,
) -> String {
    let results = session.simple_query(sql).await.expect(sql);
    let [QueryResult::Rows { rows, .. }] = results.as_slice() else {
        panic!("expected rows")
    };
    let [row] = rows.as_slice() else {
        panic!("expected one row")
    };
    let cell = row[0].as_ref().expect("non-null cell");
    std::str::from_utf8(&cell.text).expect("utf8").to_string()
}
mod support;

use support::ExtendedQueryV2 as _;
