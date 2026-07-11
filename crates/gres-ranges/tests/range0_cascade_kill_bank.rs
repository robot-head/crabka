mod harness;

use crabka_gres_ranges::RangeId;
use harness::{SystemHarness, TableAccount, process::ProcessHarness};

#[tokio::test]
async fn range0_cascade_kill_bank_fences_coordinator_before_recovery() {
    let mut system = SystemHarness::start("tenant_range0_cascade_bank");
    system.initialize_bank(100).await;

    system.kill_writer(RangeId::new(0));
    system.kill_writer(RangeId::new(1));
    assert!(
        !system
            .transfer(TableAccount::LEFT, TableAccount::RIGHT, 5)
            .await
    );
    system.fence_and_recover(RangeId::new(0));
    system.fence_and_recover(RangeId::new(1));
    assert!(
        !system
            .transfer(TableAccount::RIGHT, TableAccount::LEFT, 5)
            .await
    );

    assert_eq!(system.bank_total().await, 200);
}

#[tokio::test]
async fn real_cascade_kill_recovers_both_writers_and_accepts_new_transfer() {
    let mut system = ProcessHarness::start("tenant-real-cascade-bank").await;
    system
        .create_table_on_all(
            "CREATE TABLE bank50 (id int4, balance int4); \
             CREATE TABLE bank150 (id int4, balance int4)",
        )
        .await;
    system
        .sql(0)
        .await
        .simple_query(
            "INSERT INTO bank50 VALUES (1, 100); \
             INSERT INTO bank150 VALUES (1, 100)",
        )
        .await
        .expect("seed bank");
    let old_r0 = system.pid(0);
    let old_r1 = system.pid(1);

    system.kill(0).await;
    system.kill(1).await;
    assert!(system.try_sql(0).await.is_none());
    assert!(system.try_sql(1).await.is_none());

    system.restart(0).await;
    system.restart(1).await;
    assert_ne!(system.pid(0), old_r0);
    assert_ne!(system.pid(1), old_r1);

    let client = system.sql(0).await;
    client.simple_query("BEGIN").await.expect("begin transfer");
    client
        .simple_query("UPDATE bank50 SET balance = 95 WHERE id = 1")
        .await
        .expect("debit r0 account");
    client
        .simple_query("UPDATE bank150 SET balance = 105 WHERE id = 1")
        .await
        .expect("credit r1 account");
    if let Err(error) = client.simple_query("COMMIT").await {
        panic!(
            "commit transfer: {error}; r0 log:\n{}\nr1 log:\n{}",
            system.log(0),
            system.log(1)
        );
    }

    let left: i32 = client
        .query_one("SELECT balance FROM bank50 WHERE id = 1", &[])
        .await
        .expect("read r0 account")
        .get(0);
    let right: i32 = client
        .query_one("SELECT balance FROM bank150 WHERE id = 1", &[])
        .await
        .expect("read r1 account")
        .get(0);
    assert_eq!((left, right, left + right), (95, 105, 200));
}
