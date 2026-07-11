mod harness;

use harness::{TwoComputeHarness, process::ProcessHarness};

#[tokio::test]
async fn two_range_computes_accept_forwarded_dml_on_hosted_ranges() {
    let computes = TwoComputeHarness::start("tenant_multiprocess");
    computes.create_table_on_all_computes("t150").await;
    computes.create_table_on_all_computes("t250").await;

    computes.forwarded_insert(150, 10).await;
    computes.forwarded_insert(250, 20).await;

    assert_eq!(computes.count_rows(150).await, 1);
    assert_eq!(computes.count_rows(250).await, 1);
}

#[tokio::test]
async fn real_range_process_recovers_durable_forwarded_rows_after_kill() {
    let mut computes = ProcessHarness::start("tenant-process-recovery").await;
    computes
        .create_table_on_all("CREATE TABLE t150 (id int4)")
        .await;
    let gateway = computes.sql(0).await;
    gateway
        .simple_query("INSERT INTO t150 VALUES (7)")
        .await
        .expect("forward insert to r1");

    let old_pid = computes.pid(1);
    computes.kill_and_restart(1).await;
    assert_ne!(computes.pid(1), old_pid, "r1 must be a new OS process");

    let recovered_gateway = computes.sql(0).await;
    let rows = recovered_gateway
        .query("SELECT id FROM t150", &[])
        .await
        .expect("read recovered remote-owned row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 7);
}
