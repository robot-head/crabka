use crabka_gres_ranges::{MultiRangeTenant, MultiRangeTenantConfig, TenantName};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

#[tokio::test]
async fn durable_multirange_reopens_range_local_state() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_durable").expect("tenant"),
        "0,100,200",
    )
    .expect("config")
    .with_data_dir(temp_dir.path().to_path_buf());

    {
        let (gateway, _handles) = MultiRangeTenant::start(config.clone()).expect("tenant");
        let mut session = gateway.connect();
        session
            .simple_query("CREATE TABLE t150 (id int4)")
            .await
            .expect("create");
        session
            .simple_query("INSERT INTO t150 VALUES (41)")
            .await
            .expect("insert");
    }

    let (gateway, _handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    let rows = session
        .simple_query("SELECT id FROM t150")
        .await
        .expect("select after restart");

    let [QueryResult::Rows { rows, .. }] = &rows[..] else {
        panic!("expected rows");
    };
    assert_eq!(rows[0][0].as_ref().expect("cell").text, "41");
}
