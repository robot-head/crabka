//! Byte-identity: decode every record/batch in real `mirror.gcr.io/apache/kafka:4.0.0`
//! metadata logs + bootstrap.checkpoint through the generated record types +
//! the `KraftMetadataRecord` envelope, re-encode, and assert the bytes are
//! unchanged.

use std::path::Path;

use bytes::BytesMut;
use crabka_protocol::records::{RecordBatch, metadata::record::KraftMetadataRecord};

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
        assert2::assert!(re.as_ref() == batch_bytes);

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
                    assert2::assert!(reencoded.as_ref() == value.as_ref());
                }
            }
        }
        pos += consumed;
    }
    assert2::assert!(pos == log.len());
}

fn metadata_log_roundtrips(path: &Path) -> datatest_stable::Result<()> {
    let bytes = std::fs::read(path)?;
    assert_log_roundtrips(&bytes);
    Ok(())
}

datatest_stable::harness! {
    { test = metadata_log_roundtrips, root = "tests/fixtures", pattern = r"^[^/]+\.bin$" },
}
