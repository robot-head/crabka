//! Byte-exactness proof: Crabka's [`MetadataEvent`] codec produces and
//! consumes the *same* bytes as the real JVM `RemoteLogMetadataSerde`.
//!
//! The golden vectors in `tests/fixtures/rlmm_golden.json` were captured from
//! `mirror.gcr.io/apache/kafka:4.0.0`'s
//! `org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde`
//! by `scripts/capture-rlmm-golden.sh` (which compiles + runs
//! `scripts/capture-rlmm/Capture.java`). The fixture is the committed source of
//! truth; the script documents how to reproduce it.
//!
//! For every case we assert BOTH directions:
//!   1. `event.encode() == golden`   — Crabka encodes byte-identically to the JVM.
//!   2. `MetadataEvent::decode(golden) == event` — Crabka decodes JVM bytes.
//!
//! The FIXED constants below MUST match `Capture.java` exactly:
//!   topicId   = `Uuid::from_u128(0xCA)`
//!   topic     = "orders", partition = 7
//!   segmentId = `Uuid::from_u128(0xFE)`
//!   startOffset=0 endOffset=99 brokerId=42 maxTimestampMs=100
//!   eventTimestampMs=123 (add cases) segmentSizeInBytes=4096
//!   segmentLeaderEpochs = {0->0, 1->50}
//!   customMetadata (with-custom case) = [1,2,3,4]

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write as _,
};

use crabka_ids::LeaderEpoch;
use crabka_remote_storage::{
    CustomMetadata, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemoteLogSegmentState, RemotePartitionDeleteMetadata, RemotePartitionDeleteState,
    TopicIdPartition,
};
use crabka_remote_storage_topic::MetadataEvent;
use uuid::Uuid;

/// Load and hex-decode one named golden vector from the committed fixture.
fn golden(name: &str) -> Vec<u8> {
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rlmm_golden.json"
    ))
    .expect("read rlmm_golden.json");
    let map: HashMap<String, String> = serde_json::from_str(&raw).expect("parse rlmm_golden.json");
    let hex = map
        .get(name)
        .unwrap_or_else(|| panic!("no golden case {name}"));
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
        .collect()
}

fn topic_id_partition() -> TopicIdPartition {
    TopicIdPartition::new(Uuid::from_u128(0xCA), "orders", 7)
}

fn segment_id() -> RemoteLogSegmentId {
    RemoteLogSegmentId::new(topic_id_partition(), Uuid::from_u128(0xFE))
}

fn epochs() -> BTreeMap<LeaderEpoch, i64> {
    BTreeMap::from([(LeaderEpoch(0), 0), (LeaderEpoch(1), 50)])
}

/// Base add-segment metadata shared by the three add cases:
/// `COPY_SEGMENT_STARTED`, eventTimestampMs=123, no custom, `txnIdxEmpty`=false.
fn base_add() -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        segment_id(),
        0,    // start_offset
        99,   // end_offset
        100,  // max_timestamp_ms
        42,   // broker_id
        123,  // event_timestamp_ms
        4096, // segment_size_in_bytes
        RemoteLogSegmentState::CopySegmentStarted,
        epochs(),
    )
    .expect("valid RemoteLogSegmentMetadata")
}

/// Assert Crabka round-trips byte-identically against a JVM golden vector.
fn assert_byte_exact(case: &str, event: &MetadataEvent) {
    let want = golden(case);
    let got = event.encode();
    assert2::assert!(got.as_ref() == want.as_slice());
    let decoded = MetadataEvent::decode(&want)
        .unwrap_or_else(|e| panic!("case `{case}`: Crabka failed to decode JVM bytes: {e}"));
    assert2::assert!(&decoded == event);
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[test]
fn metadata_events_match_jvm_golden_bytes() {
    let update = RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: segment_id(),
        event_timestamp_ms: 456,
        custom_metadata: None,
        state: RemoteLogSegmentState::CopySegmentFinished,
        broker_id: 42,
    };
    let delete = RemotePartitionDeleteMetadata {
        topic_id_partition: topic_id_partition(),
        state: RemotePartitionDeleteState::DeletePartitionMarked,
        event_timestamp_ms: 789,
        broker_id: 42,
    };
    for (name, event) in [
        (
            "add_with_custom",
            MetadataEvent::AddSegment(
                base_add().with_custom_metadata(CustomMetadata(vec![1, 2, 3, 4])),
            ),
        ),
        ("add_no_custom", MetadataEvent::AddSegment(base_add())),
        // JVM-captured via the RemoteLogSegmentMetadata(..., boolean) constructor
        // present in mirror.gcr.io/apache/kafka:4.0.0; same as add_no_custom but
        // txnIdxEmpty=true.
        (
            "add_txn_empty",
            MetadataEvent::AddSegment(base_add().with_txn_index_empty(true)),
        ),
        ("update_finish", MetadataEvent::UpdateSegment(update)),
        (
            "partition_delete_marked",
            MetadataEvent::PartitionDelete(delete),
        ),
    ] {
        assert_byte_exact(name, &event);
    }
}
