mod harness;

use crabka_gres_ranges::RangeId;
use harness::{FaultEvent, SystemHarness, TableAccount, process::ProcessHarness};

#[tokio::test]
async fn range0_writer_kill_drain_is_fence_plus_prologue_before_serving() {
    let mut system = SystemHarness::start("tenant_range0_drain");
    system.initialize_bank(100).await;

    system.kill_writer(RangeId::new(0));
    assert!(
        !system
            .transfer(TableAccount::LEFT, TableAccount::RIGHT, 13)
            .await
    );
    system.fence_and_recover(RangeId::new(0));
    assert!(
        !system
            .transfer(TableAccount::LEFT, TableAccount::RIGHT, 13)
            .await
    );

    assert_eq!(system.bank_total().await, 200);
    assert_eq!(
        system.fault_log(),
        &[
            FaultEvent::WriterKilled(RangeId::new(0)),
            FaultEvent::FenceAndPrologue(RangeId::new(0)),
        ]
    );
}

#[tokio::test]
async fn real_range0_kill_fences_old_session_and_recovers_before_serving() {
    let mut system = ProcessHarness::start("tenant-real-range0-drain").await;
    system
        .create_table_on_all(
            "CREATE TABLE bank50 (id int4, balance int4); \
             CREATE TABLE bank150 (id int4, balance int4)",
        )
        .await;
    let old_session = system.sql(0).await;
    old_session
        .simple_query(
            "INSERT INTO bank50 VALUES (1, 100); \
             INSERT INTO bank150 VALUES (1, 100)",
        )
        .await
        .expect("seed bank");
    let old_r0 = system.pid(0);
    let participant = system.pid(1);

    system.kill(0).await;
    assert!(system.try_sql(0).await.is_none());
    assert!(old_session.simple_query("SELECT 1").await.is_err());
    assert_eq!(
        system.pid(1),
        participant,
        "participant stays live during drain"
    );

    system.restart(0).await;
    assert_ne!(system.pid(0), old_r0, "r0 must be a recovered OS process");
    assert_eq!(system.pid(1), participant);

    let client = system.sql(0).await;
    client
        .simple_query("BEGIN")
        .await
        .expect("begin after prologue");
    client
        .simple_query("UPDATE bank50 SET balance = 87 WHERE id = 1")
        .await
        .expect("debit after prologue");
    client
        .simple_query("UPDATE bank150 SET balance = 113 WHERE id = 1")
        .await
        .expect("credit after prologue");
    client
        .simple_query("COMMIT")
        .await
        .expect("commit after prologue");

    let left: i32 = client
        .query_one("SELECT balance FROM bank50 WHERE id = 1", &[])
        .await
        .expect("read left")
        .get(0);
    let right: i32 = client
        .query_one("SELECT balance FROM bank150 WHERE id = 1", &[])
        .await
        .expect("read right")
        .get(0);
    assert_eq!((left, right, left + right), (87, 113, 200));
}
