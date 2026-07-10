mod harness;

use crabka_gres_ranges::RangeId;
use harness::{FaultEvent, SystemHarness, TableAccount};

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
