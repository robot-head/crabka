//! In-process broker: a streams member joins, converges to an assignment, and
//! leaves cleanly. Requires `streams.version` finalized + source topic created.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_client_streams::{NodeHandle, StreamsEvent, StreamsMembership, Topology};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

async fn finalize_streams_version(client: &Client) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "streams.version".into(),
                max_version_level: 1,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert2::assert!(resp.error_code == 0);
}

async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert2::assert!(resp.topics[0].error_code == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_joins_converges_and_leaves() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;
    create_topic(&admin, "streams-input", 2).await;

    let mut topo = Topology::new();
    let src: NodeHandle<bytes::Bytes, bytes::Bytes> = topo.add_source("src", ["streams-input"]);
    topo.add_sink("snk", "streams-output", [&src]);
    let built = topo.build("streams-app").unwrap();

    let mut membership = StreamsMembership::builder()
        .bootstrap(&bootstrap)
        .group_id("streams-app")
        .topology(std::sync::Arc::new(built))
        .rebalance_timeout(Duration::from_secs(30))
        .build()
        .await
        .expect("join");

    let assigned = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match membership.next_event().await.expect("event") {
                StreamsEvent::Assigned(a) => {
                    let active_parts: usize = a.active.iter().map(|t| t.partitions.len()).sum();
                    if active_parts >= 2 {
                        break a;
                    }
                }
                StreamsEvent::NotReady(_) | StreamsEvent::Fenced => {}
            }
        }
    })
    .await
    .expect("converged to an assignment");

    assert2::assert!(assigned.active[0].subtopology_id == "0");
    let topics: Vec<&str> = assigned.active[0]
        .source_topic_partitions
        .iter()
        .map(|tp| tp.topic.as_str())
        .collect();
    assert2::assert!(topics.iter().all(|t| *t == "streams-input"));

    membership.close().await.expect("close");
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_source_topic_reports_not_ready() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;
    // Deliberately do NOT create "streams-missing".

    let mut topo = Topology::new();
    let src: NodeHandle<bytes::Bytes, bytes::Bytes> = topo.add_source("src", ["streams-missing"]);
    topo.add_sink("snk", "out", [&src]);
    let built = topo.build("streams-missing-app").unwrap();

    let mut membership = StreamsMembership::builder()
        .bootstrap(&bootstrap)
        .group_id("streams-missing-app")
        .topology(std::sync::Arc::new(built))
        .build()
        .await
        .expect("join");

    let saw_not_ready = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let StreamsEvent::NotReady(statuses) = membership.next_event().await.expect("event")
                && !statuses.is_empty()
            {
                break true;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert2::assert!(saw_not_ready);

    membership.close().await.expect("close");
    broker.shutdown().await;
}
