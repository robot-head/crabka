mod harness;

use assert2::assert;
use harness::{TwoComputeHarness, process::ProcessHarness};

#[tokio::test]
async fn combined_r0_r1_source_starts_with_local_activation_receipt_authority() {
    let system = ProcessHarness::start_all_on_zero("tenant-combined-receipt-authority").await;
    system
        .sql(0)
        .await
        .simple_query("SELECT 1")
        .await
        .expect("combined source SQL ready");
    assert_ne!(system.endpoints()[0].1, system.endpoints()[1].1);
    system.shutdown().await;
}

#[tokio::test]
async fn concurrent_process_harnesses_publish_distinct_ports_and_shutdown_cleanly() {
    let (first, second) = tokio::join!(
        ProcessHarness::start("tenant-concurrent-harness-a"),
        ProcessHarness::start("tenant-concurrent-harness-b"),
    );
    let first_endpoints = first.endpoints();
    let second_endpoints = second.endpoints();
    assert!(
        first_endpoints
            .iter()
            .all(|endpoint| endpoint.0 != 0 && endpoint.1 != 0)
    );
    assert!(
        second_endpoints
            .iter()
            .all(|endpoint| endpoint.0 != 0 && endpoint.1 != 0)
    );
    for first in first_endpoints {
        for second in second_endpoints {
            assert_ne!(first.0, second.0);
            assert_ne!(first.1, second.1);
        }
    }
    first
        .sql(0)
        .await
        .simple_query("SELECT 1")
        .await
        .expect("first ready");
    second
        .sql(1)
        .await
        .simple_query("SELECT 1")
        .await
        .expect("second ready");
    tokio::join!(first.shutdown(), second.shutdown());
}

#[tokio::test]
async fn range_zero_lease_serializes_explicit_transactions_across_compute_gateways_and_expires() {
    let computes = ProcessHarness::start("tenant-real-explicit-gate").await;
    let r0 = computes.sql(0).await;
    let r1 = computes.sql(1).await;

    r0.simple_query("BEGIN").await.expect("r0 begin owns lease");
    let waiting = tokio::spawn(async move {
        let result = r1.simple_query("BEGIN").await;
        (r1, result)
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !waiting.is_finished(),
        "direct r1 BEGIN must wait for r0 owner"
    );
    r0.simple_query("COMMIT").await.expect("release r0 lease");
    let (r1, result) = waiting.await.expect("r1 waiter task");
    result.expect("r1 begins after release");
    r1.simple_query("ROLLBACK").await.expect("release r1 lease");

    let abandoned = computes.sql(0).await;
    abandoned
        .simple_query("BEGIN")
        .await
        .expect("idle owner begins");
    let recovered = computes.sql(1).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(7),
        recovered.simple_query("BEGIN"),
    )
    .await
    .expect("bounded lease recovery")
    .expect("new owner after lease expiry");
    recovered
        .simple_query("ROLLBACK")
        .await
        .expect("release lease");
    drop(abandoned);
    computes.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_range_computes_accept_forwarded_dml_on_hosted_ranges() {
    let computes = TwoComputeHarness::start("tenant_multiprocess").await;
    // Each CREATE TABLE is issued once, through the non-r0 right compute: it
    // forwards to the left-hosted range-0 owner and barriers cluster-wide.
    computes.create_table("CREATE TABLE t150 (id int4)").await;
    computes.create_table("CREATE TABLE t250 (id int4)").await;

    // Unsharded tables with locally-hosted writes: t150 lives on the left
    // compute's r1, t250 on the right compute's r2.
    computes.forwarded_insert(150, 10).await;
    computes.forwarded_insert(250, 20).await;

    assert!(computes.count_rows(150).await == 1);
    assert!(computes.count_rows(250).await == 1);
    // Cross-compute visibility: each side reads the row the other side hosts.
    assert!(computes.count_rows_via_peer(150).await == 1);
    assert!(computes.count_rows_via_peer(250).await == 1);
}

#[tokio::test]
async fn real_range_process_recovers_durable_forwarded_rows_after_kill() {
    let mut computes = ProcessHarness::start("tenant-process-recovery").await;
    computes.create_table("CREATE TABLE t150 (id int4)").await;
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
    computes.shutdown().await;
}
