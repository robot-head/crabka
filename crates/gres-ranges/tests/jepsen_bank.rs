mod harness;

use std::{num::NonZeroU64, sync::Arc};

use crabka_gres_ranges::{MemoryTsoHorizon, TsoError, TsoOracle};
use crabka_pgkv::MemKv;
use harness::{SystemHarness, TableAccount, process::ProcessHarness};

#[tokio::test]
async fn jepsen_bank_deterministic_transfers_preserve_total_balance() {
    let mut system = SystemHarness::start("tenant_bank");
    system.initialize_bank(100).await;

    assert!(
        !system
            .transfer(TableAccount::LEFT, TableAccount::RIGHT, 7)
            .await
    );
    assert!(
        !system
            .transfer(TableAccount::RIGHT, TableAccount::LEFT, 3)
            .await
    );

    assert_eq!(system.bank_total().await, 200);
}

#[tokio::test]
async fn real_process_bank_history_preserves_exact_balances_across_writer_kill() {
    let mut system = ProcessHarness::start("tenant-real-jepsen-bank").await;
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

    let debit_seven = real_transfer(system.sql(0).await, true, 7);
    let credit_three = real_transfer(system.sql(0).await, false, 3);
    let debit_five = real_transfer(system.sql(0).await, true, 5);
    let credit_eleven = real_transfer(system.sql(0).await, false, 11);
    let concurrent_results = tokio::join!(debit_seven, credit_three, debit_five, credit_eleven);
    for result in [
        concurrent_results.0,
        concurrent_results.1,
        concurrent_results.2,
        concurrent_results.3,
    ] {
        result.expect("concurrent transfer");
    }

    system.kill(1).await;
    assert!(real_transfer(system.sql(0).await, true, 13).await.is_err());
    system.restart(1).await;

    let post_recovery_debit = real_transfer(system.sql(0).await, true, 2);
    let post_recovery_credit = real_transfer(system.sql(0).await, false, 4);
    let (post_recovery_debit, post_recovery_credit) =
        tokio::join!(post_recovery_debit, post_recovery_credit);
    post_recovery_debit.expect("post-recovery debit");
    post_recovery_credit.expect("post-recovery credit");

    let client = system.sql(0).await;
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
    assert_eq!((left, right, left + right), (104, 96, 200));
}

async fn real_transfer(
    client: tokio_postgres::Client,
    from_left: bool,
    amount: i32,
) -> Result<(), tokio_postgres::Error> {
    client.simple_query("BEGIN").await?;
    let transfer = async {
        let left: i32 = client
            .query_one("SELECT balance FROM bank50 WHERE id = 1", &[])
            .await?
            .get(0);
        let right: i32 = client
            .query_one("SELECT balance FROM bank150 WHERE id = 1", &[])
            .await?
            .get(0);
        let (new_left, new_right) = if from_left {
            (left - amount, right + amount)
        } else {
            (left + amount, right - amount)
        };
        client
            .simple_query(&format!(
                "UPDATE bank50 SET balance = {new_left} WHERE id = 1"
            ))
            .await?;
        client
            .simple_query(&format!(
                "UPDATE bank150 SET balance = {new_right} WHERE id = 1"
            ))
            .await?;
        Ok::<(), tokio_postgres::Error>(())
    }
    .await;
    match transfer {
        Ok(()) => client.simple_query("COMMIT").await.map(|_| ()),
        Err(error) => {
            let _ = client.simple_query("ROLLBACK").await;
            Err(error)
        }
    }
}

#[tokio::test]
async fn sharded_timestamp_bank_ledger_survives_writer_kills_and_tso_fences() {
    let mut system = SystemHarness::start("tenant_ts_bank_kill_fence");
    system.initialize_sharded_bank_ledger(100).await;

    assert!(
        !system
            .append_bank_transfer(TableAccount::LEFT, TableAccount::RIGHT, 7)
            .await
    );
    system.kill_writer(TableAccount::RIGHT.range_id());
    assert!(
        !system
            .append_bank_transfer(TableAccount::LEFT, TableAccount::RIGHT, 5)
            .await
    );
    system.fence_and_recover(TableAccount::RIGHT.range_id());

    let store = Arc::new(MemKv::default());
    let horizon = MemoryTsoHorizon::new(store, 1);
    let old_oracle =
        TsoOracle::recover(horizon.clone(), horizon.clone(), 1, nonzero(1), 0).expect("old oracle");
    let before_fence = old_oracle
        .grant(nonzero(1))
        .await
        .expect("grant before fence");
    horizon.set_live_epoch(2).await;
    assert!(matches!(
        old_oracle
            .grant(nonzero(1))
            .await
            .expect_err("fenced oracle"),
        TsoError::FencedEpoch { epoch: 1 }
    ));
    let new_oracle = TsoOracle::recover(
        horizon.clone(),
        horizon.clone(),
        2,
        nonzero(4),
        horizon.load_max_ts().expect("horizon"),
    )
    .expect("new oracle");
    let after_fence = new_oracle.grant(nonzero(1)).await.expect("successor grant");
    assert!(before_fence.last_ts().expect("last") < after_fence.first_ts);

    assert!(
        !system
            .append_bank_transfer(TableAccount::RIGHT, TableAccount::LEFT, 3)
            .await
    );
    assert_eq!(system.sharded_bank_ledger_total().await, 200);
}

fn nonzero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("test value is non-zero")
}
