mod harness;

use crabka_gres_ranges::RangeId;
use harness::{FaultEvent, SystemHarness, TableAccount, process::ProcessHarness};

#[tokio::test]
async fn participant_kill_bank_aborts_blocked_transfer_and_recovery_preserves_total() {
    let mut system = SystemHarness::start("tenant_participant_kill_bank");
    system.initialize_bank(100).await;

    system.kill_writer(RangeId::new(1));
    assert!(
        !system
            .transfer(TableAccount::LEFT, TableAccount::RIGHT, 11)
            .await
    );
    system.fence_and_recover(RangeId::new(1));
    assert!(
        !system
            .transfer(TableAccount::LEFT, TableAccount::RIGHT, 11)
            .await
    );

    assert_eq!(system.bank_total().await, 200);
    assert_eq!(
        system.fault_log(),
        &[
            FaultEvent::WriterKilled(RangeId::new(1)),
            FaultEvent::FenceAndPrologue(RangeId::new(1)),
        ]
    );
}

#[tokio::test]
async fn real_participant_kill_aborts_partial_bank_transfer_and_preserves_total() {
    let mut system = ProcessHarness::start("tenant-real-participant-bank").await;
    system
        .create_table(
            "CREATE TABLE bank50 (id int4, balance int4); \
             CREATE TABLE bank150 (id int4, balance int4)",
        )
        .await;
    let setup = system.sql(0).await;
    setup
        .simple_query(
            "INSERT INTO bank50 VALUES (1, 100); \
             INSERT INTO bank150 VALUES (1, 100)",
        )
        .await
        .expect("seed bank accounts");

    let transaction = system.sql(0).await;
    transaction
        .simple_query("BEGIN")
        .await
        .expect("begin transfer");
    transaction
        .simple_query("UPDATE bank50 SET balance = 89 WHERE id = 1")
        .await
        .expect("debit coordinator account");
    system.kill(1).await;
    assert!(
        transaction
            .simple_query("UPDATE bank150 SET balance = 111 WHERE id = 1")
            .await
            .is_err(),
        "participant mutation must fail after its process is killed"
    );
    transaction
        .simple_query("ROLLBACK")
        .await
        .expect("rollback failed transfer");
    system.restart(1).await;

    let recovered = system.sql(0).await;
    let left: i32 = recovered
        .query_one("SELECT balance FROM bank50 WHERE id = 1", &[])
        .await
        .expect("read coordinator balance")
        .get(0);
    let right: i32 = recovered
        .query_one("SELECT balance FROM bank150 WHERE id = 1", &[])
        .await
        .expect("read participant balance")
        .get(0);
    assert_eq!(left + right, 200);
}
