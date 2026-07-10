mod harness;

use crabka_gres_ranges::RangeId;
use harness::{SystemHarness, TableAccount};

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
