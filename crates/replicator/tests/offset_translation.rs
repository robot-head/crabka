//! Integration test: consumer-offset translation never skips un-replicated data.
//!
//! Proves that a translated (downstream) checkpoint offset never points past
//! the last record that has actually landed on the target cluster.

mod common;

use std::{collections::BTreeMap, time::Duration};

use crabka_replicator::{
    config::{ClusterConfig, Delivery, FlowConfig, NamingPolicy, ReplicatorConfig, Selectors},
    ids::{CommittedOffset, DownstreamOffset, PartitionIndex, UpstreamOffset},
    mm2::{Checkpoint, OffsetSync},
    offset_sync_store::OffsetSyncStore,
    selector::Selector,
    supervisor::FlowSupervisor,
    tasks::checkpoint::{CheckpointParams, run_once},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn offset_translation_never_skips_unreplicated_data() {
    // ------------------------------------------------------------------ setup
    let source = common::start_broker().await;
    let target = common::start_broker().await;

    // Create the source topic and produce 50 records.
    common::create_topic(&source.bootstrap, "orders", 1).await;
    for i in 0..50_u32 {
        let key = format!("k{i}");
        let val = format!("v{i}");
        common::produce(&source.bootstrap, "orders", key.as_bytes(), val.as_bytes()).await;
    }

    // ------------------------------------------ start the replication flow
    let mut clusters = BTreeMap::new();
    clusters.insert(
        "us-east".to_string(),
        ClusterConfig {
            bootstrap: source.bootstrap.clone(),
            region: "us".into(),
            zones: vec!["us".into()],
        },
    );
    clusters.insert(
        "eu-west".to_string(),
        ClusterConfig {
            bootstrap: target.bootstrap.clone(),
            region: "eu".into(),
            zones: vec!["eu".into()],
        },
    );
    let config = ReplicatorConfig {
        clusters,
        flows: vec![FlowConfig {
            from: "us-east".into(),
            to: "eu-west".into(),
            topics: Selectors {
                include: vec!["orders".into()],
                exclude: vec![],
            },
            groups: Selectors::default(),
            naming: NamingPolicy::Default,
            delivery: Delivery::AtLeastOnce,
        }],
        policies: vec![],
    };

    let sup = FlowSupervisor::run(config).await.expect("supervisor start");

    // Wait until all 50 records land in the target's replicated topic.
    common::await_count(
        &target.bootstrap,
        "us-east.orders",
        50,
        Duration::from_secs(30),
    )
    .await;

    // ------------------------------------------ commit consumer group on source
    // Group "analytics" reads and commits all 50 records on the source.
    common::consume_and_commit(&source.bootstrap, "analytics", "orders").await;

    // ------------------------------------------ build OffsetSyncStore
    // The sink wrote offset-sync records to `mm2-offset-syncs.us-east.internal`
    // on the TARGET cluster.  Drain that topic and feed every valid record into
    // the store.
    let sync_records = crabka_replicator::admin_util::read_all(
        &target.bootstrap,
        &OffsetSync::topic_name("us-east"),
        None,
    )
    .await
    .expect("read offset-syncs topic");

    println!(
        "[offset_translation] offset-sync records on target: {}",
        sync_records.len()
    );

    let mut store = OffsetSyncStore::default();
    for (k, v) in &sync_records {
        if let (Some(k), Some(v)) = (k.as_deref(), v.as_deref()) {
            match OffsetSync::from_bytes(k, v) {
                Ok(os) => {
                    println!(
                        "[offset_translation] ingest OffsetSync: topic={} part={} up={} down={}",
                        os.topic, os.partition, os.upstream, os.downstream
                    );
                    store.ingest(os);
                }
                Err(e) => eprintln!("[offset_translation] skip malformed offset-sync: {e}"),
            }
        }
    }

    // ------------------------------------------ run checkpoint translation pass
    run_once(
        &CheckpointParams {
            source_bootstrap: source.bootstrap.clone(),
            target_bootstrap: target.bootstrap.clone(),
            source_alias: "us-east".into(),
            naming: crabka_replicator::config::NamingPolicy::Default,
            group_selector: Selector::compile(&["analytics".into()], &[]).unwrap(),
            security: None,
        },
        &store,
    )
    .await
    .expect("run_once checkpoint");

    // ------------------------------------------ read back the checkpoint
    // Drain `us-east.checkpoints.internal` on the target and find the entry
    // for (analytics, orders, 0).
    let checkpoint_records = crabka_replicator::admin_util::read_all(
        &target.bootstrap,
        &Checkpoint::topic_name("us-east"),
        None,
    )
    .await
    .expect("read checkpoints topic");

    println!(
        "[offset_translation] checkpoint records on target: {}",
        checkpoint_records.len()
    );

    let checkpoint = checkpoint_records
        .into_iter()
        .filter_map(|(k, v)| {
            if let (Some(k), Some(v)) = (k, v) {
                Checkpoint::from_bytes(&k, &v).ok()
            } else {
                None
            }
        })
        // MM2 parity: the checkpoint stores the RENAMED (remote) topic that a
        // failed-over consumer reads on the target — `us-east.orders`, not `orders`.
        .filter(|c| {
            c.group == "analytics"
                && c.topic == "us-east.orders"
                && c.partition == PartitionIndex(0)
        })
        .last()
        .expect("no checkpoint found for (analytics, us-east.orders, 0)");

    // ------------------------------------------ count replicated records
    #[allow(clippy::cast_possible_wrap)]
    let replicated_count = common::count(&target.bootstrap, "us-east.orders").await as i64;

    println!(
        "[offset_translation] upstream={} downstream={} replicated={}",
        checkpoint.upstream, checkpoint.downstream, replicated_count
    );

    // ------------------------------------------ assertions: NEVER-SKIP property

    // a) The upstream offset equals the committed source offset (50).
    assert_eq!(
        checkpoint.upstream,
        UpstreamOffset(50),
        "upstream committed offset must be 50 (all records consumed); got {}",
        checkpoint.upstream
    );

    // b) The downstream offset is non-negative and does not exceed what has
    //    actually been replicated — this is the core never-skip invariant.
    assert!(
        checkpoint.downstream >= DownstreamOffset(0),
        "downstream offset must be >= 0; got {}",
        checkpoint.downstream
    );
    assert!(
        checkpoint.downstream <= DownstreamOffset(replicated_count),
        "NEVER-SKIP VIOLATED: downstream={} exceeds replicated_count={}",
        checkpoint.downstream,
        replicated_count
    );

    // c) The store's translate() output is consistent with what was written.
    let translated = store.translate("orders", PartitionIndex(0), CommittedOffset(50));
    assert_eq!(
        translated,
        Some(checkpoint.downstream),
        "store.translate(\"orders\", 0, 50) = {:?} but checkpoint.downstream = {}",
        translated,
        checkpoint.downstream
    );

    println!(
        "[offset_translation] PASS — upstream={} downstream={} replicated={} (never-skip held)",
        checkpoint.upstream, checkpoint.downstream, replicated_count
    );

    // ------------------------------------------ teardown
    sup.shutdown().await;
}
