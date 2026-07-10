mod harness;

use crabka_gres_ranges::RangeId;
use harness::{SystemHarness, TableAccount};

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
