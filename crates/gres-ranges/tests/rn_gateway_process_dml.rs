//! Live-process end-to-end for any-node DML coordination.
//!
//! This test uses real `crabka-gres` binaries over a real broker. Node 0 hosts
//! r0. Node 1 hosts only r1 and carries a range-0 follower catalog replica that
//! truly lags. Every statement drives through node 1's SQL front door, so
//! classification and planning run against the follower replica, timestamps mint
//! over the remote TSO wire path, and cross-range writes forward to the r0
//! owner. That last path failed with `0A000` before any-node coordination.
//!
//! The harness boundaries are `0:0,50:10`. The interval-sharded table `t50`
//! splits at rowid 10, so r0 owns the ids below 10 and r1 owns 10 and above. The
//! ordinary table `t40` lives wholly on r0, which node 1 does not host.

mod harness;

use assert2::assert;
use harness::process::ProcessHarness;

#[tokio::test]
async fn non_r0_compute_coordinates_sharded_and_ordinary_dml() {
    let system = ProcessHarness::start("tenant-rn-gateway-dml").await;
    // DDL through node 1 forwards to the r0 owner and barriers cluster-wide,
    // so the follower catalog is current when create_table returns: no
    // propagation wait is needed before the first write.
    system
        .create_table("CREATE TABLE t50 (id int4) SHARDED")
        .await;
    system.create_table("CREATE TABLE t40 (id int4)").await;

    let coordinator = system.sql(1).await;
    let observer = system.sql(0).await;

    // Autocommit scatter insert: ids 1 and 5 land in r0-owned key space,
    // ids 10 and 15 in the r1-owned key space at and above the boundary.
    coordinator
        .simple_query("INSERT INTO t50 VALUES (1), (5), (10), (15)")
        .await
        .expect("autocommit sharded insert through the non-r0 compute");
    assert!(table_ids(&coordinator, "t50").await == vec![1, 5, 10, 15]);
    assert!(table_ids(&observer, "t50").await == vec![1, 5, 10, 15]);

    // Explicit cross-range transaction committed from node 1: one write per
    // side of the boundary, resolved through the r0-owned oracle and GTM.
    coordinator
        .simple_query("BEGIN")
        .await
        .expect("begin the explicit transaction on the non-r0 compute");
    coordinator
        .simple_query("INSERT INTO t50 VALUES (2)")
        .await
        .expect("r0-owned write inside the explicit transaction");
    coordinator
        .simple_query("INSERT INTO t50 VALUES (16)")
        .await
        .expect("r1-owned write inside the explicit transaction");
    coordinator
        .simple_query("COMMIT")
        .await
        .expect("commit the cross-range transaction from the non-r0 compute");
    let committed = vec![1, 2, 5, 10, 15, 16];
    assert!(table_ids(&coordinator, "t50").await == committed);
    assert!(table_ids(&observer, "t50").await == committed);

    // A rolled-back scatter write leaves both sides of the boundary
    // untouched.
    coordinator
        .simple_query("BEGIN")
        .await
        .expect("begin the transaction that will roll back");
    coordinator
        .simple_query("INSERT INTO t50 VALUES (3), (25)")
        .await
        .expect("scatter write that will be rolled back");
    coordinator
        .simple_query("ROLLBACK")
        .await
        .expect("rollback the scatter write from the non-r0 compute");
    assert!(table_ids(&coordinator, "t50").await == committed);
    assert!(table_ids(&observer, "t50").await == committed);

    // Ordinary DML for a table whose range node 1 does not host: t40 lives
    // wholly on r0, so the write must forward and still read back from both
    // front doors.
    coordinator
        .simple_query("INSERT INTO t40 VALUES (7)")
        .await
        .expect("ordinary insert into an r0-owned table through the non-r0 compute");
    assert!(table_ids(&coordinator, "t40").await == vec![7]);
    assert!(table_ids(&observer, "t40").await == vec![7]);

    system.shutdown().await;
}

/// All `id` values in `table`, sorted, so the scatter-gather order never
/// matters.
async fn table_ids(client: &tokio_postgres::Client, table: &str) -> Vec<i32> {
    let rows = client
        .query(&format!("SELECT id FROM {table}"), &[])
        .await
        .unwrap_or_else(|error| panic!("read {table} ids: {error}"));
    let mut ids = rows
        .iter()
        .map(|row| row.get::<_, i32>(0))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}
