mod harness;

use crabka_gres_ranges::RangeId;
use harness::{FaultEvent, SystemHarness, TableAccount};

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
