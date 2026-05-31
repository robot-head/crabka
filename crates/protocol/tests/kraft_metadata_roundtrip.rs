//! Byte-identity: decode every record/batch in a real `apache/kafka:4.0.0`
//! metadata log + bootstrap.checkpoint through the generated record types + the
//! `KraftMetadataRecord` envelope, re-encode, and assert the bytes are
//! unchanged. This is the primary validator for the KIP-631 record layer: it
//! proves byte-exact compatibility against genuine JVM-produced bytes with no
//! timestamp nondeterminism (input bytes are preserved).
//!
//! Fixtures captured from a freshly-formatted JVM node (see
//! docs/superpowers/specs/2026-05-30-kraft-wire-findings.md):
//! - `bootstrap_checkpoint.bin`: `SnapshotHeader` + 3 `FeatureLevelRecord`s + `SnapshotFooter`
//! - `startup_log.bin`: offsets 0..=8 (`LeaderChange`, bootstrap txn, registrations)
//! - `topic_log.bin`: through offset 31 (adds `TopicRecord` + 2 `PartitionRecord`s)

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::records::RecordBatch;
use crabka_protocol::records::metadata::record::KraftMetadataRecord;

/// Walk every batch in `log`; assert each re-encodes byte-identically, and that
/// every non-control record value round-trips through `KraftMetadataRecord`.
fn assert_log_roundtrips(log: &[u8]) {
    let mut pos = 0usize;
    while pos < log.len() {
        let mut cur: &[u8] = &log[pos..];
        let before = cur.len();
        let batch = RecordBatch::decode(&mut cur).expect("batch decodes");
        let consumed = before - cur.len();
        let batch_bytes = &log[pos..pos + consumed];

        // The whole batch must re-encode byte-identically (validates the record
        // batch layer + that record values were read verbatim).
        let mut re = BytesMut::new();
        batch.encode(&mut re).expect("batch re-encodes");
        assert!(
            re.as_ref() == batch_bytes,
            "batch at base_offset {} not byte-identical (len {} vs {})",
            batch.base_offset,
            re.len(),
            batch_bytes.len()
        );

        // Control batches (LeaderChange / Snapshot*) are not value-enveloped;
        // their byte-identity is covered by the whole-batch assertion above.
        if !batch.attributes.is_control_batch() {
            for rec in &batch.records {
                if let Some(value) = &rec.value {
                    let (decoded, version) = KraftMetadataRecord::decode_value(value)
                        .expect("metadata record value decodes");
                    let reencoded = decoded
                        .encode_value(version)
                        .expect("metadata record value re-encodes");
                    assert!(
                        reencoded.as_ref() == value.as_ref(),
                        "record value (apiKey {}) not byte-identical",
                        decoded.api_key()
                    );
                }
            }
        }
        pos += consumed;
    }
    assert!(pos == log.len(), "trailing bytes after final batch");
}

#[test]
fn bootstrap_checkpoint_roundtrips() {
    assert_log_roundtrips(include_bytes!("fixtures/bootstrap_checkpoint.bin"));
}

#[test]
fn startup_log_roundtrips() {
    assert_log_roundtrips(include_bytes!("fixtures/startup_log.bin"));
}

#[test]
fn topic_log_roundtrips() {
    assert_log_roundtrips(include_bytes!("fixtures/topic_log.bin"));
}

/// A real `apache/kafka:4.0.0` metadata log after `kafka-configs --alter` on a
/// topic and a client quota — exercises `ConfigRecord` (apiKey 4) and
/// `ClientQuotaRecord` (apiKey 14), the new Slice-3d-1 dispatch variants, against
/// genuine JVM bytes (alongside the common Topic/Partition/RegisterBroker/… set).
#[test]
fn config_quota_log_roundtrips() {
    assert_log_roundtrips(include_bytes!("fixtures/config_quota_log.bin"));
}
