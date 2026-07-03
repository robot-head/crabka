//! Slice-48f broker integration: the topic-backed
//! [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager)
//! (configured via `[remote_storage.kafka_metadata]`) wired against a
//! single broker's own loopback listener.
//!
//! `Broker::start` boots on the fail-closed `NotReadyRlmm` behind a
//! `SwappableRlmm`, then a retry-until-success task dials the broker's own
//! advertised listener, provisions `__remote_log_metadata`, starts the
//! `TopicBasedRemoteLogMetadataManager`, and swaps it in. These tests
//! exercise that path end-to-end with the `Local` tiered-storage backend:
//!
//! * [`topic_rlmm_activates_against_loopback`] — the bootstrap completes:
//!   the `tiered_storage_rlmm_topic_backed` gauge flips to 1 and the
//!   `__remote_log_metadata` topic exists on the broker.
//! * [`topic_rlmm_copy_then_fetch_round_trip`] — a sealed segment is
//!   tiered (proving the RLM copy task's `CopySegment*` events round-trip
//!   through `__remote_log_metadata` over the loopback) and the records
//!   read back at offset 0.

#![allow(clippy::pedantic, clippy::manual_assert)]

use assert2::assert;
mod support;

use std::time::{Duration, Instant};

use crabka_broker::{
    Broker, BrokerConfig, BrokerHandle, KafkaRlmmConfig, RemoteStorageBackend, RlmmKind,
};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{
    CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use tempfile::TempDir;

const METADATA_TOPIC: &str = "__remote_log_metadata";

/// Boot a single broker with the `Local` tiered-storage backend and the
/// topic-backed RLMM pointed at its own loopback listener. Returns the
/// handle plus the log + remote tempdirs (kept alive by the caller).
async fn start_broker_with_topic_rlmm() -> (BrokerHandle, TempDir, TempDir) {
    support::init_tracing();

    // Pin a loopback port so the RLMM bootstrap can dial the broker's own
    // listener: `KafkaRlmmConfig::bootstrap` is resolved before the
    // listener binds, so an ephemeral `:0` wouldn't be knowable in time.
    // Held listeners eliminate the bind-and-drop TOCTOU race under parallel
    // nextest (`AddrInUse` flakes).
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let listen = client_addrs[0];

    let log_dir = TempDir::new().expect("log tempdir");
    let remote_dir = TempDir::new().expect("remote tempdir");

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.controller_listen_addr = controller_addrs[0];
    cfg.controller_quorum_voters = vec![(1, controller_addrs[0].to_string())];
    cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: remote_dir.path().to_path_buf(),
    });
    cfg.remote_log_manager_interval = Duration::from_secs(1);
    cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        bootstrap: format!("127.0.0.1:{}", listen.port()),
        num_partitions: 1,
        replication: 1,
        snapshot_interval: Duration::from_hours(1),
        snapshot_dir: log_dir.path().join("remote-log-metadata"),
        security: None,
    });

    let data_listener = client_listeners.into_iter().next().unwrap();
    let controller_listener = controller_listeners.into_iter().next().unwrap();
    let broker = Broker::start_with_listeners(cfg, Some(controller_listener), Some(data_listener))
        .await
        .expect("broker start");
    (broker, log_dir, remote_dir)
}

async fn build_client(broker: &BrokerHandle) -> Client {
    build_client_secured(broker, None).await
}

/// Build a test client, optionally negotiating TLS/SASL. `None` is the
/// plaintext path used by the loopback tests; `Some(..)` authenticates
/// against a SASL listener.
async fn build_client_secured(
    broker: &BrokerHandle,
    security: Option<crabka_client_core::security::ClientSecurity>,
) -> Client {
    Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("tiered-topic-rlmm-test")
        .maybe_security(security)
        .build()
        .await
        .expect("client build")
}

/// Poll the `tiered_storage_rlmm_topic_backed` gauge until the slice-48f
/// bootstrap swaps the topic-backed manager in.
async fn await_activation(broker: &BrokerHandle) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if broker.rlmm_topic_backed_active_for_test() {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "topic-backed RLMM never activated within 30s"
        );
        tokio::task::yield_now().await;
    }
}

/// The bootstrap completes against the loopback listener: the activation
/// gauge flips and the `__remote_log_metadata` topic is provisioned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_rlmm_activates_against_loopback() {
    let (broker, _log_dir, _remote_dir) = start_broker_with_topic_rlmm().await;

    await_activation(&broker).await;
    assert!(
        broker.has_partition(METADATA_TOPIC, 0).await,
        "__remote_log_metadata-0 should be hosted after bootstrap"
    );

    broker.shutdown().await;
}

/// Produce enough to seal several segments, wait for the RLM copy task to
/// tier one through the topic-backed RLMM (which publishes `CopySegment*`
/// events to `__remote_log_metadata` over the loopback and consumes them
/// back to update its cache), then read the records back at offset 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn topic_rlmm_copy_then_fetch_round_trip() {
    const TOPIC: &str = "tiered-topic-rlmm-itest";

    let (broker, _log_dir, remote_dir) = start_broker_with_topic_rlmm().await;
    await_activation(&broker).await;

    let client = build_client(&broker).await;
    copy_then_fetch_round_trip(&broker, &client, remote_dir.path(), TOPIC).await;
    broker.shutdown().await;
}

/// Shared copy→metadata→read body: create a tiered topic, wait for the
/// config to propagate, produce enough to seal segments, wait for the RLM
/// copy task to tier one through the topic-backed RLMM, then read offset 0
/// back. Used by both the plaintext loopback test and the SASL_PLAINTEXT
/// variant; the only difference is how `client` was built.
#[allow(clippy::too_many_lines)]
async fn copy_then_fetch_round_trip(
    broker: &BrokerHandle,
    client: &Client,
    remote_dir: &std::path::Path,
    topic: &str,
) {
    // Tiny `segment.bytes` so a modest produce seals several segments;
    // `local.retention.bytes=1` evicts every copied segment from local
    // disk so the read-back must consult the remote tier.
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs: vec![
                    CreatableTopicConfig {
                        name: "remote.storage.enable".into(),
                        value: Some("true".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "segment.bytes".into(),
                        value: Some("1024".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "local.retention.bytes".into(),
                        value: Some("1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.bytes".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                    // `produce_records_for_test` stamps no record timestamp, so
                    // sealed segments carry max_timestamp_ms=0; the default 7-day
                    // `retention.ms` would then immediately evict every tiered
                    // segment (`now - 0 > 7d`). Disable time retention so the
                    // copied segments survive for the read-back.
                    CreatableTopicConfig {
                        name: "retention.ms".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {:?}",
        resp.topics[0].error_message
    );

    // Wait for the tiered config to flow from the metadata image through
    // the supervisor's reconcile loop into the partition's `LogConfig`.
    // Without this gate the first batches land in a default-config log
    // (1 GiB segments, tiering off) and never roll or copy.
    let cfg_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(cfg) = broker.partition_log_config_for_test(topic, 0)
            && cfg.remote_storage_enable
            && cfg.segment_bytes == 1024
            && cfg.local_retention_bytes == Some(1)
        {
            break;
        }
        assert!(
            Instant::now() <= cfg_deadline,
            "tiered-storage topic config never propagated within 10s; saw {:?}",
            broker.partition_log_config_for_test(topic, 0)
        );
        tokio::task::yield_now().await;
    }

    // Single-record batches (~85 bytes each) roll the 1 KiB segment every
    // ~12 records, so 80 records seal several segments for the copy task.
    broker
        .produce_records_for_test(topic, 0, 80)
        .await
        .expect("produce records");

    // Wait for at least one segment to land in the remote tier. The
    // `LocalTieredStorage` layout writes each copied segment's bytes to a
    // file named `log`; its presence proves the RLM copy task's
    // `CopySegment*` events round-tripped through `__remote_log_metadata`.
    let copy_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if count_remote_log_files(remote_dir) >= 1 {
            break;
        }
        assert!(
            Instant::now() <= copy_deadline,
            "no segment tiered to remote storage within 30s"
        );
        tokio::task::yield_now().await;
    }

    // Read offset 0 back. Whether it is served from a still-local segment
    // or (after eviction) the remote tier, a successful read exercises the
    // full path with the topic-backed RLMM active. Retry to absorb the
    // local-retention eviction race.
    let topic_id = topic_id_for(client, topic).await;
    let fetch_deadline = Instant::now() + Duration::from_secs(30);
    let value = loop {
        let r = client
            .send(FetchRequest {
                max_wait_ms: 500,
                min_bytes: 1,
                topics: vec![FetchTopic {
                    topic: topic.into(),
                    topic_id,
                    partitions: vec![FetchPartition {
                        partition: 0,
                        fetch_offset: 0,
                        partition_max_bytes: 1_048_576,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Fetch");
        if let Some(batches) = r
            .responses
            .first()
            .and_then(|t| t.partitions.first())
            .and_then(|p| p.records.as_ref())
            .and_then(|recs| recs.as_v2())
            && let Some(first) = batches.first().and_then(|b| b.records.first())
        {
            break first.value.clone();
        }
        assert!(
            Instant::now() <= fetch_deadline,
            "offset 0 never returned records within 30s"
        );
        tokio::task::yield_now().await;
    };

    assert!(
        value.as_deref() == Some(b"test-record-0".as_slice()),
        "offset 0 should read back the first produced record"
    );
}

async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// Count files named `log` anywhere under `root` — the
/// `LocalTieredStorage` segment-bytes object for each copied segment.
fn count_remote_log_files(root: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, count: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, count);
            } else if path.file_name().and_then(|n| n.to_str()) == Some("log") {
                *count += 1;
            }
        }
    }
    let mut count = 0;
    walk(root, &mut count);
    count
}

/// Boot a single broker whose only (and inter-broker) listener is
/// SASL_PLAINTEXT/PLAIN, with the topic-backed RLMM pointed at it. The
/// RLMM authenticates as the inter-broker PLAIN principal.
async fn start_sasl_broker_with_topic_rlmm() -> (BrokerHandle, TempDir, TempDir) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism};

    support::init_tracing();
    // Held listeners eliminate the bind-and-drop TOCTOU race. The data
    // listener matches `spec.bind_addr == listen` in `start_with_listeners`
    // even for the custom SASL_PLAINTEXT ListenerSpec, so both can be passed.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let listen = client_addrs[0];
    let log_dir = TempDir::new().expect("log tempdir");
    let remote_dir = TempDir::new().expect("remote tempdir");

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.controller_listen_addr = controller_addrs[0];
    cfg.controller_quorum_voters = vec![(1, controller_addrs[0].to_string())];
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: listen,
        advertised: format!("127.0.0.1:{}", listen.port()),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("rlmm".to_string(), "rlmm-secret".to_string());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: "rlmm".to_string(),
        password: "rlmm-secret".to_string(),
    });
    cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: remote_dir.path().to_path_buf(),
    });
    cfg.remote_log_manager_interval = Duration::from_secs(1);
    cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        // The broker overrides bootstrap + security from the inter-broker
        // listener; the operator value here is the same loopback addr.
        bootstrap: format!("127.0.0.1:{}", listen.port()),
        num_partitions: 1,
        replication: 1,
        snapshot_interval: Duration::from_hours(1),
        snapshot_dir: log_dir.path().join("remote-log-metadata"),
        security: None,
    });

    let data_listener = client_listeners.into_iter().next().unwrap();
    let controller_listener = controller_listeners.into_iter().next().unwrap();
    let broker = Broker::start_with_listeners(cfg, Some(controller_listener), Some(data_listener))
        .await
        .expect("broker start");
    (broker, log_dir, remote_dir)
}

/// While the topic-backed RLMM has not yet activated (bootstrap points at a
/// dead port so the retry loop never succeeds), the RLM copy task must not
/// tier any segment — `add_remote_log_segment_metadata` is called first,
/// and a `NotReady` error causes the copy task to skip the segment entirely.
/// This proves the fail-closed guarantee: no orphaned objects accumulate in
/// the remote store while the RLMM is unavailable.
///
/// The topic config and produce volume mirror
/// [`topic_rlmm_copy_then_fetch_round_trip`] exactly, so "0 tiered objects"
/// is genuinely discriminating: the analogous loopback test tiers ≥ 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_task_skips_tiering_while_rlmm_not_ready() {
    const TOPIC: &str = "tiered-not-ready-itest";

    support::init_tracing();

    // Held listeners eliminate the bind-and-drop TOCTOU race under parallel
    // nextest. The RLMM bootstrap here is a dead port (127.0.0.1:1) so the
    // data-plane port itself is never dialled by the RLMM — but we still hold
    // both ports race-free.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let listen = client_addrs[0];

    let log_dir = TempDir::new().expect("log tempdir");
    let remote_dir = TempDir::new().expect("remote tempdir");

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.controller_listen_addr = controller_addrs[0];
    cfg.controller_quorum_voters = vec![(1, controller_addrs[0].to_string())];
    cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: remote_dir.path().to_path_buf(),
    });
    // Fast ticks so the copy task gets plenty of chances to (not) tier.
    cfg.remote_log_manager_interval = Duration::from_millis(200);
    // Dead port: the retry loop can never dial the bootstrap; the SwappableRlmm
    // stays on the NotReadyRlmm stub for the entire test.
    cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        bootstrap: "127.0.0.1:1".into(),
        num_partitions: 1,
        replication: 1,
        snapshot_interval: Duration::from_hours(1),
        snapshot_dir: log_dir.path().join("rlmm-snap"),
        security: None,
    });

    let data_listener = client_listeners.into_iter().next().unwrap();
    let controller_listener = controller_listeners.into_iter().next().unwrap();
    let broker = Broker::start_with_listeners(cfg, Some(controller_listener), Some(data_listener))
        .await
        .expect("broker starts");
    let client = build_client(&broker).await;

    // Create the same tiered topic config as the loopback round-trip test.
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs: vec![
                    CreatableTopicConfig {
                        name: "remote.storage.enable".into(),
                        value: Some("true".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "segment.bytes".into(),
                        value: Some("1024".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "local.retention.bytes".into(),
                        value: Some("1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.bytes".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.ms".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {:?}",
        resp.topics[0].error_message
    );

    // Wait for the tiered config to propagate into the partition's LogConfig
    // (same gate as the loopback round-trip test).
    let cfg_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(pcfg) = broker.partition_log_config_for_test(TOPIC, 0)
            && pcfg.remote_storage_enable
            && pcfg.segment_bytes == 1024
            && pcfg.local_retention_bytes == Some(1)
        {
            break;
        }
        assert!(
            Instant::now() <= cfg_deadline,
            "tiered-storage topic config never propagated within 10s; saw {:?}",
            broker.partition_log_config_for_test(TOPIC, 0)
        );
        tokio::task::yield_now().await;
    }

    // Same 80 records as the loopback round-trip — enough to seal several
    // 1 KiB segments and give the copy task ample segments to try to tier.
    broker
        .produce_records_for_test(TOPIC, 0, 80)
        .await
        .expect("produce records");

    // Several copy-task ticks (200 ms interval × ~10 ticks).
    // real-time wait (not a progress poll): settle then assert ZERO tiered objects — proving absence while the RLMM stays NotReady, so there is no positive condition to poll.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The RLMM is still NotReady, so add_remote_log_segment_metadata returns
    // NotReady and the copy task must have skipped every segment.
    let tiered = count_remote_log_files(remote_dir.path());
    assert!(
        tiered == 0,
        "expected no tiered objects while RLMM not ready, found {tiered}"
    );

    broker.shutdown().await;
}

/// The full copy→metadata→read round-trip, but the broker's only listener
/// is SASL_PLAINTEXT/PLAIN. The RLMM's internal metadata client must
/// authenticate as the inter-broker PLAIN principal to bootstrap the
/// topic, publish/consume CopySegment events, and serve the read-back —
/// proving the secured metadata client works end-to-end. The test's own
/// client authenticates with the same credentials.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_rlmm_sasl_loopback_copy_then_fetch_round_trip() {
    use crabka_client_core::security::{ClientSecurity, SaslCredentials};
    use crabka_security::ListenerProtocol;

    const TOPIC: &str = "tiered-topic-rlmm-sasl-itest";
    let (broker, _log_dir, remote_dir) = start_sasl_broker_with_topic_rlmm().await;
    await_activation(&broker).await;

    let security = ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: "rlmm".into(),
            password: "rlmm-secret".into(),
        }),
        sasl_host: None,
    };
    let client = build_client_secured(&broker, Some(security)).await;
    copy_then_fetch_round_trip(&broker, &client, remote_dir.path(), TOPIC).await;
    broker.shutdown().await;
}
