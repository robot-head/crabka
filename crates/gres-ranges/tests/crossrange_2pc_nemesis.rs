mod harness;

use crabka_gres_ranges::RangeId;
use harness::{SystemHarness, TableAccount, process::ProcessHarness};

#[tokio::test]
async fn crossrange_2pc_nemesis_commits_only_after_all_killed_writers_recover() {
    let mut system = SystemHarness::start("tenant_crossrange_2pc_nemesis");
    system.initialize_bank(100).await;

    assert!(
        !system
            .transfer(TableAccount::LEFT, TableAccount::RIGHT, 4)
            .await
    );
    system.kill_writer(RangeId::new(1));
    assert!(
        !system
            .transfer(TableAccount::RIGHT, TableAccount::LEFT, 9)
            .await
    );
    system.fence_and_recover(RangeId::new(1));
    assert!(
        !system
            .transfer(TableAccount::RIGHT, TableAccount::LEFT, 9)
            .await
    );

    assert_eq!(system.bank_total().await, 200);
}

#[tokio::test]
async fn real_range_partition_aborts_transfer_and_heal_restores_2pc() {
    let system = ProcessHarness::start("tenant-real-partition-bank").await;
    system
        .create_table(
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

    let participant_pid = system.pid(1);
    system.partition(1).await;
    assert_eq!(system.pid(1), participant_pid);
    let blocked = system.sql(0).await;
    blocked
        .simple_query("BEGIN")
        .await
        .expect("begin blocked transfer");
    blocked
        .simple_query("UPDATE bank50 SET balance = 93 WHERE id = 1")
        .await
        .expect("tentative local debit");
    assert!(
        blocked
            .simple_query("UPDATE bank150 SET balance = 107 WHERE id = 1")
            .await
            .is_err(),
        "partitioned participant must be unreachable"
    );
    blocked
        .simple_query("ROLLBACK")
        .await
        .expect("rollback partitioned transfer");

    system.heal(1).await;
    assert_eq!(system.pid(1), participant_pid);
    let client = system.sql(0).await;
    client
        .simple_query("BEGIN")
        .await
        .expect("begin healed transfer");
    client
        .simple_query("UPDATE bank50 SET balance = 95 WHERE id = 1")
        .await
        .expect("debit after heal");
    client
        .simple_query("UPDATE bank150 SET balance = 105 WHERE id = 1")
        .await
        .expect("credit after heal");
    client
        .simple_query("COMMIT")
        .await
        .expect("commit after heal");

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
    assert_eq!((left, right, left + right), (95, 105, 200));
}
