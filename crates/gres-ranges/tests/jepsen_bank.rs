mod harness;

use std::{num::NonZeroU64, sync::Arc};

use crabka_gres_ranges::{MemoryTsoHorizon, TsoError, TsoOracle};
use crabka_pgkv::MemKv;
use harness::{SystemHarness, TableAccount};

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
        TsoOracle::recover(horizon.clone(), horizon.clone(), 1, nonzero(4), 0).expect("old oracle");
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
