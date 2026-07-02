//! Integration coverage for the native `Consumer`'s **proactive** KIP-320
//! position validation (`crates/client-consumer/src/validate.rs`).
//!
//! ## The path under test (proactive — distinct from the reactive paths)
//!
//! `Consumer::poll` runs a validate pass at the very TOP, before fetching:
//!
//!   1. [`refresh_leader_epochs`] reads `Metadata` and flags a partition
//!      `awaiting_validation` when the metadata leader epoch ADVANCES past the
//!      epoch the consumer last consumed at (`offset_epoch >= 0`).
//!   2. [`validate_positions`] issues an `OffsetForLeaderEpoch` (OFLE,
//!      `api_key` 23) RPC to the leader for each flagged partition and runs
//!      `position::classify`. A `Truncated` outcome resets the partition; under
//!      `auto.offset.reset = None` it surfaces `ConsumerError::LogTruncation`.
//!      Crucially, an `awaiting_validation` partition is EXCLUDED from the
//!      Fetch — so detection here cannot have come from a fetch response.
//!
//! This is fundamentally different from the **reactive** truncation paths,
//! which the consumer's own `tests/integration.rs` already covers:
//!   * in-band `diverging_epoch` on a Fetch response, and
//!   * `OFFSET_OUT_OF_RANGE` (code 1) on a Fetch response.
//!
//! Both reactive paths are driven by a Fetch *response* and issue NO OFLE RPC.
//!
//! ## Proving it was PROACTIVE, two independent ways
//!
//! 1. The partition is flagged `awaiting_validation` by the metadata
//!    leader-epoch advance, so the poll EXCLUDES it from the Fetch entirely.
//!    The `LogTruncation` therefore cannot originate from a `diverging_epoch`
//!    or `OFFSET_OUT_OF_RANGE` fetch response — there is no fetch for this
//!    partition this round. The error's `safe_offset` equals the OFLE-derived
//!    epoch boundary (not a fetch `log_start_offset`).
//! 2. A broker-side OFLE request counter
//!    ([`BrokerHandle::offset_for_leader_epoch_count_for_test`]) increments
//!    during the truncating poll — direct evidence the validate pass issued the
//!    OFLE RPC. The reactive paths would leave it unchanged.
//!
//! ## Inducing a genuine post-handoff divergence deterministically
//!
//! `classify` only returns `Truncated` if the leader's epoch-`e0` end offset is
//! BELOW the consumer's position. A clean handoff (new leader has all the data)
//! yields `Valid`. So we engineer real divergence on a single-broker cluster:
//!
//!   1. Produce `N = 4` records at the partition's natural leader epoch 0.
//!   2. Consumer (group, `auto.offset.reset = None`) consumes all 4 → its
//!      `offset_epoch = 0`, `next_offset = 4`, observed metadata `leader_epoch
//!      = 0`.
//!   3. Truncate the leader's local log to offset 2 (`test_truncate_local_log`):
//!      the epoch-0 boundary on the leader is now 2 — BELOW the consumer's
//!      position 4. This is the "new leader is missing records the consumer
//!      already saw" divergence the task requires.
//!   4. Advance the metadata image's leader epoch to 1 by submitting a
//!      `PartitionRecord` with `leader_epoch + 1` (same leader/replicas/ISR).
//!      This is exactly the mechanism `tests/elect_leaders.rs` uses to advance a
//!      partition's epoch via the controller; here it stands in for the
//!      leadership handoff that bumps the epoch.
//!
//! On the next poll: `refresh_leader_epochs` sees epoch 1 > `offset_epoch` 0 →
//! flags `awaiting_validation`; `validate_positions` issues OFLE → the handler
//! answers `end_offset = 2, error_code = 0` → `classify(offset=4, offset_epoch=0,
//! leader_epoch=1, leader_end_offset=2)` → `Truncated { safe_offset: 2 }` →
//! `None` policy surfaces `LogTruncation { fetch_offset: 4, safe_offset: 2 }`.
//!
//! Windows-gated like the other broker integration tests (openraft's
//! `debug_assert!` races on the hosted Windows scheduler).

#![allow(clippy::too_many_lines)]

use std::time::{Duration, Instant};

use assert2::{assert, check};
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerError};
use crabka_client_core::Client;
use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};
use tempfile::TempDir;

async fn topic_id_for(client: &Client, name: &str) -> crabka_protocol::primitives::uuid::Uuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// Produce each value as its OWN single-record batch (one batch per offset),
/// retrying the `UNKNOWN_TOPIC_OR_PARTITION` (3) metadata-apply race. Separate
/// batches keep offsets dense AND individually truncatable — `truncate_to`
/// operates on whole batch boundaries, so a single multi-record batch could not
/// be split at a mid-batch offset.
async fn produce(client: &Client, topic: &str, values: &[&str]) {
    let topic_id = topic_id_for(client, topic).await;
    for v in values {
        let mut batch = RecordBatch {
            last_offset_delta: 0,
            ..RecordBatch::default()
        };
        batch.records.push(Record {
            offset_delta: 0,
            value: Some(Bytes::from((*v).to_string())),
            ..Default::default()
        });
        let mut produced = false;
        for attempt in 1..=5 {
            let resp = client
                .send(ProduceRequest {
                    acks: 1,
                    timeout_ms: 5_000,
                    topic_data: vec![TopicProduceData {
                        name: topic.into(),
                        topic_id,
                        partition_data: vec![PartitionProduceData {
                            index: 0,
                            records: Some(batch.clone().into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .await
                .expect("produce");
            let err = resp.responses[0].partition_responses[0].error_code;
            if err == 0 {
                produced = true;
                break;
            }
            if err == 3 && attempt < 5 {
                // intentional: bounded RPC-response retry on the
                // UNKNOWN_TOPIC_OR_PARTITION (3) metadata-apply race. This is a
                // client-only helper with no broker handle, so it polls the
                // produce response's error_code rather than a broker awaiter;
                // back off briefly and re-send.
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            panic!("produce failed after {attempt} attempt(s): code {err}");
        }
        assert!(produced, "produce of {v} did not succeed");
    }
}

async fn create_topic(client: &Client, name: &str) {
    let cr = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(cr.topics[0].error_code == 0, "create_topic failed: {cr:?}");
}

/// PROACTIVE KIP-320 truncation detection.
///
/// See the module docs for the full rationale. The discriminating assertions:
///   * `poll()` returns `ConsumerError::LogTruncation` with `fetch_offset = 4`
///     (the consumer's pre-divergence position) and `safe_offset = 2` (the
///     OFLE-derived epoch-0 boundary — NOT a fetch `log_start_offset`);
///   * the broker's OFLE request counter strictly increased across the
///     truncating poll, proving the validate pass issued the RPC.
///
/// The honesty check (verified manually, reverted): no-op'ing either
/// `validate_positions` or the `awaiting_validation` flagging in
/// `refresh_leader_epochs` makes this test FAIL (no truncation surfaced /
/// counter unchanged), so it is genuinely discriminating.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_proactively_validates_and_surfaces_truncation() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let topic = "proactive-trunc";

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();
    create_topic(&producer, topic).await;
    // 4 records at the natural leader epoch 0 → offsets 0..=3, LEO 4,
    // epoch checkpoint `0 -> 0`.
    produce(&producer, topic, &["v0", "v1", "v2", "v3"]).await;

    // The partition's natural leader epoch is 0 on a fresh single-broker topic.
    broker.wait_until_partition_present(topic, 0).await;
    let pr0 = broker
        .partition_record_for_test(topic, 0)
        .expect("partition record must be present after wait_until_partition_present");
    assert!(
        pr0.leader_epoch == 0,
        "fresh topic should start at leader_epoch 0, got {}",
        pr0.leader_epoch
    );

    // Seed the group's committed position. A fresh `auto.offset.reset = None`
    // consumer starts at the log-end sentinel and would read nothing, so we
    // first run an `Earliest` seed consumer in the SAME group that consumes all
    // 4 records (at epoch 0) and commits. The committed `(offset = 4, epoch =
    // 0)` is what the `None` consumer below inherits: `next_offset = 4`,
    // `offset_epoch = 0`. This commit MUST happen before the divergence is
    // induced, while the records still exist.
    {
        let mut seed = Consumer::builder()
            .bootstrap(&bootstrap)
            .client_id("seed")
            .group_id("proactive-grp")
            .session_timeout(Duration::from_secs(30))
            .rebalance_timeout(Duration::from_secs(2))
            .heartbeat_interval(Duration::from_secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe([topic.to_string()])
            .build()
            .await
            .unwrap();
        let mut epochs: Vec<i32> = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline && epochs.len() < 4 {
            for r in seed
                .poll(Duration::from_millis(300))
                .await
                .expect("seed consume must not error")
            {
                epochs.push(r.leader_epoch);
            }
        }
        assert!(
            epochs == vec![0, 0, 0, 0],
            "seed consumer must read all 4 records at epoch 0 first, got {epochs:?}"
        );
        // Commit the consumed position (offset 4, epoch 0) for the group.
        seed.commit_sync().await.unwrap();
        seed.close().await.unwrap();
    }

    // ── Induce the post-handoff divergence ────────────────────────────────
    // (a) Truncate the leader's local log to offset 2. Its epoch-0 boundary is
    //     now 2 — BELOW the consumer's position 4. The leader is genuinely
    //     missing records the consumer already consumed.
    broker
        .test_truncate_local_log(topic, 0, 2)
        .await
        .expect("truncate leader log");
    let leo = broker
        .local_log_end_offset(topic, 0)
        .await
        .expect("leader LEO");
    assert!(
        leo == 2,
        "leader LEO should be 2 after truncation, got {leo}"
    );

    // (b) Advance the metadata image's leader epoch to 1 (same leader/replicas/
    //     ISR). This is the metadata event the consumer's refresh_leader_epochs
    //     keys on to flag awaiting_validation.
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_string(),
        partition: 0,
        leader: pr0.leader,
        replicas: pr0.replicas.clone(),
        isr: pr0.isr.clone(),
        leader_epoch: pr0.leader_epoch + 1,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    });
    broker
        .submit_metadata_record_for_test(forged)
        .await
        .expect("advance metadata leader epoch");
    broker
        .wait_for_image(|img| img.partition(topic, 0).is_some_and(|p| p.leader_epoch >= 1))
        .await;

    // The `None`-policy consumer under test. It inherits the group's committed
    // position (offset 4, epoch 0): `next_offset = 4`, `offset_epoch = 0`. With
    // `None` and no truncation it would simply park at the log-end and deliver
    // nothing — so any surfaced `LogTruncation` is the proactive validation
    // result, not a fetch-driven reset.
    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("c")
        .group_id("proactive-grp")
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
        .auto_offset_reset(AutoOffsetReset::None)
        .subscribe([topic.to_string()])
        .build()
        .await
        .unwrap();

    // Wait for the coordinator to publish the assignment. This consumer uses the
    // classic JoinGroup/SyncGroup protocol. We first gate on the broker having
    // processed the JoinGroup (member count → 1), then spin-yield until the
    // client-side coordinator task completes SyncGroup + prime_offsets and sets
    // the assignment. Polling before prime_offsets finishes would let
    // `refresh_leader_epochs` see `offset_epoch = -1` and skip the validation
    // flag — so this gate makes the proactive trigger deterministic.
    broker
        .wait_until_classic_group_member_count("proactive-grp", 1)
        .await;
    // Spin without sleeping until the SyncGroup response and prime_offsets have
    // propagated back to the client coordinator (a few async hops after the
    // broker-side member-count gate fires).
    let settle = Instant::now() + Duration::from_secs(10);
    loop {
        if !consumer.assignment().await.is_empty() {
            break;
        }
        assert!(
            Instant::now() < settle,
            "consumer assignment did not propagate within 10s after member-count gate"
        );
        // intentional: bounded re-check tick for a client-side coordinator RPC
        // poll. The SyncGroup response + prime_offsets propagate to the client's
        // `assignment()` a few async hops after the broker-side member-count gate
        // (`wait_until_classic_group_member_count`) fires; that client-side state
        // has no metadata-image or metric signal to await on. The brief sleep
        // avoids a tight `yield_now` busy-spin on loaded runners.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Snapshot the OFLE counter immediately before the truncating poll.
    let ofle_before = broker.offset_for_leader_epoch_count_for_test();

    // ── The proactive validate pass fires on the next poll ────────────────
    // refresh_leader_epochs: metadata epoch 1 > offset_epoch 0 → awaiting_validation.
    // The partition is therefore EXCLUDED from the Fetch this round, so the
    // truncation below cannot have come from a fetch response.
    // validate_positions: OFLE → end_offset 2 → classify → Truncated{safe:2}.
    // None policy → Err(LogTruncation{fetch_offset:4, safe_offset:2}).
    //
    // Tolerate empty/timeout polls (assignment churn), but the first non-empty
    // outcome must be the proactive truncation error.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut got = None;
    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(300)).await {
            Ok(recs) => {
                assert!(
                    recs.is_empty(),
                    "an awaiting_validation partition must not be fetched / deliver records; got {recs:?}"
                );
            }
            Err(e) => {
                got = Some(e);
                break;
            }
        }
    }

    let err = got.expect("proactive validation must surface a truncation error within 15s");
    match err {
        ConsumerError::LogTruncation {
            topic: t,
            partition,
            fetch_offset,
            safe_offset,
        } => {
            check!(t == topic, "wrong topic in LogTruncation: {t}");
            check!(
                partition == 0,
                "wrong partition in LogTruncation: {partition}"
            );
            // fetch_offset = the consumer's pre-divergence position (4).
            check!(
                fetch_offset == 4,
                "fetch_offset must be the consumed position 4, got {fetch_offset}"
            );
            // safe_offset = the OFLE-derived epoch-0 boundary (2). A reactive
            // OFFSET_OUT_OF_RANGE reset would instead carry a fetch
            // log_start_offset; here the partition was never fetched.
            check!(
                safe_offset == 2,
                "safe_offset must be the OFLE epoch-0 boundary 2, got {safe_offset}"
            );
        }
        other => panic!("expected proactive LogTruncation, got {other:?}"),
    }

    // Discriminator: the validate pass must have issued at least one OFLE RPC.
    // The reactive diverging_epoch / OFFSET_OUT_OF_RANGE paths issue none.
    let ofle_after = broker.offset_for_leader_epoch_count_for_test();
    assert!(
        ofle_after > ofle_before,
        "proactive validation must issue an OffsetForLeaderEpoch RPC \
         (before={ofle_before}, after={ofle_after})"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}
