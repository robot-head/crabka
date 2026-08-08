//! Integration test for the KIP-113 log-dir admin RPCs.
//!
//! The test drives `AlterReplicaLogDirs` and `DescribeLogDirs` through
//! `AdminClient`. This runs the typed admin wrappers in
//! `crates/client-admin/src/log_dirs.rs` end-to-end against a live broker.

use std::{collections::BTreeMap, time::Duration};

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::AdminClient;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_log_dirs_alter_then_describe_converges() {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    cfg.extra_log_dirs = vec![extra.path().to_path_buf()];
    let handle = Broker::start(cfg).await.expect("broker start");
    let bootstrap = handle.listen_addr().to_string();

    let mut admin = AdminClient::connect(&[bootstrap]).await.expect("connect");

    // Create a 2-partition topic so KIP-113 placement spreads them
    // across both configured log.dirs.
    admin
        .create_topics(
            &[crabka_client_admin::CreateTopicSpec {
                name: "t".to_string(),
                partitions: 2,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            crabka_units::secs(5),
        )
        .await
        .expect("create_topics");

    // Wait until both partitions are physically materialized on disk.
    handle.wait_until_partition_present("t", 0).await;
    handle.wait_until_partition_present("t", 1).await;

    // Initial DescribeLogDirs reports both dirs, no future logs.
    let initial = admin.describe_log_dirs(None).await.expect("describe");
    assert2::assert!(initial.len() == 2);
    for d in &initial {
        assert2::assert!(d.error.is_none());
        for t in &d.topics {
            for p in &t.partitions {
                assert2::assert!(!p.is_future_key);
                assert2::assert!(p.partition_size >= 0);
            }
        }
    }

    // Move both partitions into `extra`. AlterReplicaLogDirs picks the
    // last entry per (topic, partition) on the wire if listed twice —
    // we list each only once.
    let mut assignments: BTreeMap<String, Vec<(String, Vec<i32>)>> = BTreeMap::new();
    assignments.insert(
        extra.path().to_string_lossy().to_string(),
        vec![("t".to_string(), vec![0, 1])],
    );
    let outcomes = admin
        .alter_replica_log_dirs(&assignments)
        .await
        .expect("alter");
    assert2::assert!(outcomes.len() == 2);
    for o in &outcomes {
        assert2::assert!(o.error.is_none());
    }

    // Poll DescribeLogDirs through the admin client until both
    // partitions live in `extra` with `is_future_key=false`.
    let target_canon = std::fs::canonicalize(extra.path()).unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let resp = admin.describe_log_dirs(None).await.expect("describe poll");
            let mut current_in_target: Vec<i32> = Vec::new();
            let mut any_future = false;
            for d in &resp {
                let d_canon = std::fs::canonicalize(&d.log_dir)
                    .unwrap_or_else(|_| std::path::PathBuf::from(&d.log_dir));
                if d_canon != target_canon {
                    continue;
                }
                for t in &d.topics {
                    if t.name != "t" {
                        continue;
                    }
                    for p in &t.partitions {
                        if p.is_future_key {
                            any_future = true;
                        } else {
                            current_in_target.push(p.partition_index);
                        }
                    }
                }
            }
            current_in_target.sort_unstable();
            current_in_target.dedup();
            if !any_future && current_in_target == vec![0, 1] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("AlterReplicaLogDirs move completed within 30s");

    // Filtered describe — request only topic "t" — should still see
    // both partitions in the target dir.
    let mut filter: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    filter.insert("t".to_string(), vec![]); // empty = all partitions
    let filtered = admin
        .describe_log_dirs(Some(&filter))
        .await
        .expect("filtered describe");
    let mut filtered_in_target: Vec<i32> = Vec::new();
    for d in &filtered {
        let d_canon = std::fs::canonicalize(&d.log_dir)
            .unwrap_or_else(|_| std::path::PathBuf::from(&d.log_dir));
        if d_canon != target_canon {
            continue;
        }
        for t in &d.topics {
            for p in &t.partitions {
                filtered_in_target.push(p.partition_index);
            }
        }
    }
    filtered_in_target.sort_unstable();
    assert2::assert!(filtered_in_target == vec![0, 1]);

    handle.shutdown().await;
}
