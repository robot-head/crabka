//! Deterministic structural compaction over immutable delta layers.

use std::collections::{BTreeMap, btree_map::Entry};

use crabka_object_store::{ObjectOps, ObjectStoreError};
use object_store::path::Path as ObjectPath;
use thiserror::Error;

use crate::{
    ContainerError, LayerDesc, LayerKind, LayerMap, LayerMapError, LayerReader, LayerWriteEntry,
    TimelinePath, write_layer,
};

/// Result from one compaction attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionReport {
    /// Delta layers removed from the active map.
    pub removed: Vec<LayerDesc>,
    /// Replacement delta layers added to the active map.
    pub added: Vec<LayerDesc>,
    /// Number of entries written into replacement layers.
    pub entry_count: usize,
}

impl CompactionReport {
    /// Returns true when no compaction work was necessary.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.added.is_empty()
    }
}

/// Merges all delta layers currently visible in `map` into one deterministic delta layer.
pub async fn compact_l0(
    ops: &dyn ObjectOps,
    map: &mut LayerMap,
) -> Result<CompactionReport, CompactError> {
    let candidate_layers = map
        .layers()
        .iter()
        .filter(|layer| layer.kind == LayerKind::Delta)
        .cloned()
        .collect::<Vec<_>>();
    if candidate_layers.len() < 2 {
        return Ok(CompactionReport {
            removed: Vec::new(),
            added: Vec::new(),
            entry_count: 0,
        });
    }

    let timeline = candidate_layers
        .first()
        .map(|layer| layer.timeline.clone())
        .ok_or(CompactError::NoCandidateTimeline)?;
    let entries = merged_entries(ops, &candidate_layers).await?;
    if entries.is_empty() {
        return Ok(CompactionReport {
            removed: Vec::new(),
            added: Vec::new(),
            entry_count: 0,
        });
    }

    let new_layer = write_layer(ops, &timeline, LayerKind::Delta, &entries).await?;
    let new_layer_object_name = new_layer.object_name();
    map.replace_layers(&candidate_layers, [new_layer.clone()])?;
    for old_layer in &candidate_layers {
        let old_layer_object_name = old_layer.object_name();
        if old_layer_object_name == new_layer_object_name {
            continue;
        }

        ops.delete(&ObjectPath::from(old_layer_object_name)).await?;
    }

    Ok(CompactionReport {
        removed: candidate_layers,
        added: vec![new_layer],
        entry_count: entries.len(),
    })
}

async fn merged_entries(
    ops: &dyn ObjectOps,
    layers: &[LayerDesc],
) -> Result<Vec<LayerWriteEntry>, CompactError> {
    let mut by_key_lsn = BTreeMap::new();
    for layer in layers {
        let reader = LayerReader::open(ops, layer).await?;
        for (key, lsn, value) in reader.entries(ops).await? {
            if let Entry::Vacant(slot) = by_key_lsn.entry((key, lsn)) {
                slot.insert(value);
            }
        }
    }

    Ok(by_key_lsn
        .into_iter()
        .map(|((key, lsn), value)| (key, lsn, value))
        .collect())
}

/// Errors returned by structural compaction.
#[derive(Debug, Error)]
pub enum CompactError {
    /// Compaction was asked to proceed without a candidate timeline.
    #[error("no candidate timeline was available for compaction")]
    NoCandidateTimeline,
    /// Layer map update failed.
    #[error(transparent)]
    LayerMap(#[from] LayerMapError),
    /// Layer container read/write failed.
    #[error(transparent)]
    Container(#[from] ContainerError),
    /// Object store operation failed.
    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),
}

#[allow(dead_code)]
fn _timeline_of(layer: &LayerDesc) -> &TimelinePath {
    &layer.timeline
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use bytes::Bytes;
    use crabka_object_store::ObjectStoreClient;
    use crabka_postgres_wal::Lsn;
    use object_store::memory::InMemory;

    use super::*;
    use crate::{OpenLayer, PAGE_SIZE, PageKey, TenantId, TimelineId, Value};

    fn ops() -> ObjectStoreClient {
        ObjectStoreClient::new(Arc::new(InMemory::new()))
    }

    fn timeline() -> TimelinePath {
        TimelinePath::new(
            TenantId::parse("tenant").expect("tenant id is valid"),
            TimelineId::parse("timeline").expect("timeline id is valid"),
        )
    }

    fn key(block_number: u32) -> PageKey {
        PageKey::new(1663, 5, 16_384, 0, block_number)
    }

    fn image(byte: u8) -> Value {
        Value::image(Bytes::from(vec![byte; PAGE_SIZE])).expect("test image is one page")
    }

    fn wal(bytes: &'static [u8]) -> Value {
        Value::Wal {
            will_init: false,
            rec: Bytes::from_static(bytes),
        }
    }

    #[tokio::test]
    async fn compaction_preserves_reconstruct_data_and_reduces_layers() {
        let ops = ops();
        let mut map = LayerMap::new(timeline());
        for (lsn, value) in [(10, image(b'a')), (20, wal(b"r1")), (30, wal(b"r2"))] {
            let mut open = OpenLayer::builder(timeline())
                .with_disk_consistent_lsn(Lsn(lsn - 1))
                .build();
            open.put_value(key(0), Lsn(lsn), value).unwrap();
            let flushed = open.flush(&ops, Lsn(lsn)).await.unwrap().unwrap();
            map.insert(flushed.desc).unwrap();
        }
        let before = map
            .get_reconstruct_data(&ops, key(0), Lsn(30))
            .await
            .unwrap();

        let report = compact_l0(&ops, &mut map).await.unwrap();
        let after = map
            .get_reconstruct_data(&ops, key(0), Lsn(30))
            .await
            .unwrap();

        assert!(before == after);
        assert!(report.removed.len() == 3);
        assert!(report.added.len() == 1);
        assert!(map.layers().len() == 1);
    }

    #[tokio::test]
    async fn compaction_keeps_newest_duplicate_key_lsn_value() {
        let ops = ops();
        let timeline = timeline();
        let mut map = LayerMap::new(timeline.clone());
        let duplicate_key = key(0);
        let filler_key = key(1);
        let older_only_key = key(2);
        let stale = write_layer(
            &ops,
            &timeline,
            LayerKind::Delta,
            &[
                (
                    duplicate_key,
                    Lsn(10),
                    Value::Wal {
                        will_init: true,
                        rec: Bytes::from_static(b"stale"),
                    },
                ),
                (older_only_key, Lsn(10), wal(b"older-only")),
            ],
        )
        .await
        .unwrap();
        let newest = write_layer(
            &ops,
            &timeline,
            LayerKind::Delta,
            &[
                (
                    duplicate_key,
                    Lsn(10),
                    Value::Wal {
                        will_init: true,
                        rec: Bytes::from_static(b"newest"),
                    },
                ),
                (filler_key, Lsn(20), wal(b"filler")),
            ],
        )
        .await
        .unwrap();
        map.insert(stale).unwrap();
        map.insert(newest).unwrap();

        let before = map
            .get_reconstruct_data(&ops, duplicate_key, Lsn(10))
            .await
            .unwrap();
        let report = compact_l0(&ops, &mut map).await.unwrap();
        let after = map
            .get_reconstruct_data(&ops, duplicate_key, Lsn(10))
            .await
            .unwrap();

        assert!(before == after);
        assert!(after.deltas == vec![(Lsn(10), Bytes::from_static(b"newest"))]);
        assert!(report.entry_count == 3);
    }

    #[tokio::test]
    async fn compaction_does_not_delete_replacement_when_name_matches_old_layer() {
        let ops = ops();
        let timeline = timeline();
        let mut map = LayerMap::new(timeline.clone());
        let wide = write_layer(
            &ops,
            &timeline,
            LayerKind::Delta,
            &[
                (key(0), Lsn(10), wal(b"wide-start")),
                (key(2), Lsn(20), wal(b"wide-end")),
            ],
        )
        .await
        .unwrap();
        let narrower_overlap = write_layer(
            &ops,
            &timeline,
            LayerKind::Delta,
            &[(
                key(1),
                Lsn(15),
                Value::Wal {
                    will_init: true,
                    rec: Bytes::from_static(b"overlap"),
                },
            )],
        )
        .await
        .unwrap();
        map.insert(wide.clone()).unwrap();
        map.insert(narrower_overlap).unwrap();

        let report = compact_l0(&ops, &mut map).await.unwrap();
        let replacement = report.added.first().expect("replacement layer is written");
        let replacement_object = ObjectPath::from(replacement.object_name());
        let reconstructed = map
            .get_reconstruct_data(&ops, key(1), Lsn(15))
            .await
            .unwrap();

        assert!(replacement.object_name() == wide.object_name());
        assert!(ops.head(&replacement_object).await.is_ok());
        assert!(reconstructed.deltas == vec![(Lsn(15), Bytes::from_static(b"overlap"))]);
    }
}
