//! Slice-48o integration: `KafkaMetadataEventLog` manual per-partition
//! fetch consumer honors a partition subset and a non-zero start offset.
//!
//! The test boots a bare loopback broker with no tiered-storage backend.
//! It constructs the `KafkaMetadataEventLog` directly, not through
//! `Broker::start`'s RLMM bootstrap. It publishes across three partitions,
//! then subscribes to a subset from a non-zero offset. It asserts that the
//! reworked consumer delivers exactly the assigned records, and never the
//! unassigned partition.

use assert2::assert;
mod support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_remote_storage_topic::{
    kafka_log::{KafkaMetadataEventLog, KafkaMetadataLogConfig},
    log::{MetadataEventLog, PartitionStart},
};
use futures_util::StreamExt;
use tempfile::TempDir;

/// Boot a bare loopback broker with the pinned-port pattern from
/// `tiered_storage_topic_rlmm.rs::start_broker_with_topic_rlmm`. This
/// helper drops the tiered-storage backend and the `remote_log_metadata`
/// bootstrap, because the test wires `KafkaMetadataEventLog` by hand. It
/// returns the handle plus the log tempdir, which the caller keeps alive.
async fn start_bare_broker() -> (BrokerHandle, TempDir) {
    support::init_tracing();

    // Pin a loopback port so the metadata-log bootstrap address is
    // knowable before the listener binds.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let listen = client_addrs[0];

    let log_dir = TempDir::new().expect("log tempdir");

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.controller_listen_addr = controller_addrs[0];
    cfg.controller_quorum_voters =
        vec![(crabka_broker::NodeId(1), controller_addrs[0].to_string())];

    let data_listener = client_listeners.into_iter().next().unwrap();
    let controller_listener = controller_listeners.into_iter().next().unwrap();
    let broker = Broker::start_with_listeners(cfg, Some(controller_listener), Some(data_listener))
        .await
        .expect("broker start");
    (broker, log_dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_subset_from_nonzero_offset_yields_exact_records() {
    let (broker, _log_dir) = start_bare_broker().await;
    let bootstrap = format!("127.0.0.1:{}", broker.listen_addr().port());

    let mut cfg = KafkaMetadataLogConfig::new(bootstrap);
    cfg.num_partitions = 3;
    cfg.replication = 1;
    let log = KafkaMetadataEventLog::start(cfg).await.expect("log start");
    let pc = log.partition_count();
    assert!(pc >= 3);

    // Publish: partition 0 -> a,b,c ; partition 1 -> x,y ; partition 2 -> z
    for v in [b"a".as_slice(), b"b", b"c"] {
        log.publish(0, Bytes::copy_from_slice(v)).await.unwrap();
    }
    for v in [b"x".as_slice(), b"y"] {
        log.publish(1, Bytes::copy_from_slice(v)).await.unwrap();
    }
    log.publish(2, Bytes::from_static(b"z")).await.unwrap();

    // Subscribe to partitions {0 from offset 1, 1 from offset 0}; not 2.
    let (mut stream, _handle) = log.subscribe(vec![
        PartitionStart {
            partition: 0,
            start_offset: 1,
        },
        PartitionStart {
            partition: 1,
            start_offset: 0,
        },
    ]);

    // Expect exactly: (0,1,b), (0,2,c), (1,0,x), (1,1,y). Collect with a
    // deadline; assert no partition-2 record ever arrives.
    let mut got: Vec<(i32, i64, Vec<u8>)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while got.len() < 4 {
        let next = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
        match next {
            Ok(Some(r)) => {
                assert!(r.partition != 2, "partition 2 was not assigned");
                got.push((r.partition, r.offset, r.payload.to_vec()));
            }
            Ok(None) => break,
            Err(_) => {} // timeout tick; keep waiting until deadline
        }
        assert!(
            Instant::now() <= deadline,
            "did not receive 4 records in 15s: {got:?}"
        );
    }
    got.sort();
    assert!(
        got == vec![
            (0, 1, b"b".to_vec()),
            (0, 2, b"c".to_vec()),
            (1, 0, b"x".to_vec()),
            (1, 1, b"y".to_vec()),
        ]
    );

    // Brief grace: ensure no stray partition-2 record sneaks in.
    if let Ok(Some(extra)) = tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
        assert!(extra.partition != 2, "partition 2 leaked: {extra:?}");
    }

    log.shutdown().await;
    broker.shutdown().await;
}
