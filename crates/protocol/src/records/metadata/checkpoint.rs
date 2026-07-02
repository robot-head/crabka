//! Builds a `KRaft` `bootstrap.checkpoint`: a `SnapshotHeader` control batch, a
//! data batch of `FeatureLevelRecord`s, and a `SnapshotFooter` control batch —
//! matching what `kafka-storage format` writes (minus per-run timestamps).

use bytes::{BufMut, Bytes, BytesMut};

use crate::Encode;
use crate::owned::feature_level_record::FeatureLevelRecord;
use crate::owned::snapshot_footer_record::SnapshotFooterRecord;
use crate::owned::snapshot_header_record::SnapshotHeaderRecord;
use crate::records::metadata::control::{
    ControlRecordType, control_record_key, encode_control_batch,
};
use crate::records::metadata::record::KraftMetadataRecord;
use crate::records::{Record, RecordBatch};

/// `FeatureLevelRecord`s are always written at apiVersion 0.
const FEATURE_LEVEL_API_VERSION: i16 = 0;

/// Build a `bootstrap.checkpoint` from an ordered list of `(feature_name, level)`.
///
/// # Panics
/// Panics only if a record fails to encode, which cannot happen for these
/// well-formed feature/control records.
#[must_use]
pub fn build_bootstrap_checkpoint(features: &[(&str, i16)]) -> Bytes {
    let mut out = BytesMut::new();

    // (1) SnapshotHeader control batch at offset 0.
    let header = SnapshotHeaderRecord::default();
    let mut header_body = BytesMut::new();
    header
        .encode(&mut header_body, 0)
        .expect("snapshot header encodes");
    out.put_slice(&encode_control_batch(
        0,
        control_record_key(ControlRecordType::SnapshotHeader),
        header_body.freeze(),
    ));

    // (2) Data batch of FeatureLevelRecords at offset 1.
    let records: Vec<Record> = features
        .iter()
        .enumerate()
        .map(|(i, (name, level))| {
            let rec = KraftMetadataRecord::FeatureLevel(FeatureLevelRecord {
                name: (*name).to_string(),
                feature_level: *level,
                ..Default::default()
            });
            Record {
                offset_delta: i32::try_from(i).expect("few features"),
                value: Some(
                    rec.encode_value(FEATURE_LEVEL_API_VERSION)
                        .expect("feature record encodes"),
                ),
                ..Default::default()
            }
        })
        .collect();
    let data = RecordBatch {
        base_offset: 1,
        last_offset_delta: i32::try_from(features.len().saturating_sub(1)).unwrap_or(0),
        records,
        ..Default::default()
    };
    data.encode(&mut out).expect("feature data batch encodes");

    // (3) SnapshotFooter control batch.
    let footer_offset = 1 + i64::try_from(features.len()).expect("few features");
    let footer = SnapshotFooterRecord::default();
    let mut footer_body = BytesMut::new();
    footer
        .encode(&mut footer_body, 0)
        .expect("snapshot footer encodes");
    out.put_slice(&encode_control_batch(
        footer_offset,
        control_record_key(ControlRecordType::SnapshotFooter),
        footer_body.freeze(),
    ));

    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Decode;
    use crate::records::RecordBatch;
    use assert2::{assert, check};

    #[test]
    fn bootstrap_checkpoint_has_header_features_footer() {
        let bytes = build_bootstrap_checkpoint(&[
            ("metadata.version", 25),
            ("group.version", 1),
            ("transaction.version", 2),
        ]);
        // Walk batches: control header (offset 0), data batch (offsets 1..=3),
        // control footer.
        let mut cur: &[u8] = &bytes;
        let header = RecordBatch::decode(&mut cur).expect("header batch");
        check!(header.base_offset == 0);
        check!(header.attributes.is_control_batch());
        assert!(header.records.len() == 1);
        let header_value = header.records[0].value.as_ref().expect("header value");
        let mut header_cur = &header_value[..];
        let header_record =
            SnapshotHeaderRecord::decode(&mut header_cur, 0).expect("snapshot header");
        let expected_header = SnapshotHeaderRecord {
            version: 0,
            last_contained_log_timestamp: 0,
            unknown_tagged_fields: crate::UnknownTaggedFields(vec![]),
        };
        assert!(header_record == expected_header);
        assert!(header_cur.is_empty());

        let data = RecordBatch::decode(&mut cur).expect("data batch");
        check!(data.base_offset == 1);
        check!(data.last_offset_delta == 2);
        check!(!data.attributes.is_control_batch());
        assert!(data.records.len() == 3);
        let expected = [
            ("metadata.version", 25),
            ("group.version", 1),
            ("transaction.version", 2),
        ];
        for (i, (record, (name, level))) in data.records.iter().zip(expected).enumerate() {
            assert!(record.offset_delta == i32::try_from(i).expect("test index fits"));
            let value = record.value.as_ref().expect("feature value");
            let (decoded, version) =
                KraftMetadataRecord::decode_value(value).expect("feature record");
            assert!(version == FEATURE_LEVEL_API_VERSION);
            let KraftMetadataRecord::FeatureLevel(feature) = decoded else {
                panic!("expected feature level record");
            };
            assert!(feature.name == name);
            assert!(feature.feature_level == level);
        }

        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        check!(footer.base_offset == 4);
        check!(footer.attributes.is_control_batch());
        assert!(footer.records.len() == 1);
        let footer_value = footer.records[0].value.as_ref().expect("footer value");
        let mut footer_cur = &footer_value[..];
        let footer_record =
            SnapshotFooterRecord::decode(&mut footer_cur, 0).expect("snapshot footer");
        let expected_footer = SnapshotFooterRecord {
            version: 0,
            unknown_tagged_fields: crate::UnknownTaggedFields(vec![]),
        };
        assert!(footer_record == expected_footer);
        check!(footer_cur.is_empty());
        check!(cur.is_empty());
    }
}
