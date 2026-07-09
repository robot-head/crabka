//! Mutable ingest buffer for one timeline before it is sealed as immutable layers.

use std::collections::{BTreeMap, btree_map::Entry};

use bytes::Bytes;
use crabka_object_store::{ObjectOps, ObjectStoreError};
use crabka_postgres_wal::{Lsn, Sharded};
use thiserror::Error;

use crate::{
    ContainerError, LayerDesc, LayerKind, LayerWriteEntry, PAGE_SIZE, PageKey, TimelinePath, Value,
    write_layer,
};

/// Metadata-lane WAL payload retained beside page-shard data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenMetaRecord {
    /// Record start LSN.
    pub lsn: Lsn,
    /// Resource manager identifier.
    pub rmid: u8,
    /// Verbatim metadata bytes available from the decoded record.
    pub bytes: Bytes,
}

/// Result of accepting an ingest item into the open layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    /// The item was older than, or equal to, the durable boundary and was ignored.
    IgnoredDurable,
    /// The item was a duplicate of an already buffered item.
    Duplicate,
    /// The item was buffered.
    Buffered,
}

/// Descriptor returned after a non-empty flush.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushedLayer {
    /// Newly-written immutable layer descriptor.
    pub desc: LayerDesc,
    /// Durable LSN after the flush.
    pub disk_consistent_lsn: Lsn,
    /// Metadata records drained by the same flush.
    pub meta: Vec<OpenMetaRecord>,
}

/// Builder for an [`OpenLayer`].
#[derive(Debug, Clone)]
pub struct OpenLayerBuilder {
    timeline: TimelinePath,
    disk_consistent_lsn: Lsn,
}

impl OpenLayerBuilder {
    /// Builds an ingest buffer for `timeline` starting with no durable WAL.
    #[must_use]
    pub const fn new(timeline: TimelinePath) -> Self {
        Self {
            timeline,
            disk_consistent_lsn: Lsn(0),
        }
    }

    /// Sets the durable boundary loaded from disk before ingest starts.
    #[must_use]
    pub const fn with_disk_consistent_lsn(mut self, disk_consistent_lsn: Lsn) -> Self {
        self.disk_consistent_lsn = disk_consistent_lsn;
        self
    }

    /// Finishes the builder.
    #[must_use]
    pub fn build(self) -> OpenLayer {
        OpenLayer {
            timeline: self.timeline,
            disk_consistent_lsn: self.disk_consistent_lsn,
            entries: BTreeMap::new(),
            newest_lsn_by_key: BTreeMap::new(),
            meta: Vec::new(),
        }
    }
}

/// Single-writer open layer for page and metadata WAL ingest.
#[derive(Debug, Clone)]
pub struct OpenLayer {
    timeline: TimelinePath,
    disk_consistent_lsn: Lsn,
    entries: BTreeMap<(PageKey, Lsn), Value>,
    newest_lsn_by_key: BTreeMap<PageKey, Lsn>,
    meta: Vec<OpenMetaRecord>,
}

impl OpenLayer {
    /// Starts a builder for `timeline`.
    #[must_use]
    pub const fn builder(timeline: TimelinePath) -> OpenLayerBuilder {
        OpenLayerBuilder::new(timeline)
    }

    /// Returns the durable LSN boundary.
    #[must_use]
    pub const fn disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn
    }

    /// Returns buffered metadata records that have not yet been flushed.
    #[must_use]
    pub fn meta_records(&self) -> &[OpenMetaRecord] {
        &self.meta
    }

    /// Buffers a parsed page value for `key` and `lsn`.
    pub fn put_value(
        &mut self,
        key: PageKey,
        lsn: Lsn,
        value: Value,
    ) -> Result<IngestOutcome, OpenLayerError> {
        if lsn <= self.disk_consistent_lsn {
            return Ok(IngestOutcome::IgnoredDurable);
        }

        if let Some(newest_lsn) = self.newest_lsn_by_key.get(&key)
            && lsn < *newest_lsn
        {
            return Err(OpenLayerError::NonMonotonicLsn {
                key,
                previous: *newest_lsn,
                current: lsn,
            });
        }

        match self.entries.entry((key, lsn)) {
            Entry::Vacant(slot) => {
                slot.insert(value);
                self.newest_lsn_by_key.insert(key, lsn);
                Ok(IngestOutcome::Buffered)
            }
            Entry::Occupied(slot) if slot.get() == &value => Ok(IngestOutcome::Duplicate),
            Entry::Occupied(_) => Err(OpenLayerError::ConflictingValue { key, lsn }),
        }
    }

    /// Buffers one routed WAL item from `crabka-postgres-wal`.
    pub fn ingest_sharded(&mut self, item: Sharded) -> Result<IngestOutcome, OpenLayerError> {
        match item {
            Sharded::Page {
                key,
                lsn,
                blk_idx,
                rec,
            } => {
                let block = rec
                    .blocks
                    .get(blk_idx)
                    .ok_or(OpenLayerError::MissingBlockRef { blk_idx })?;
                let key = page_key_from_wal(key);
                let value = if let Some(image) = &block.image {
                    Value::image(Bytes::copy_from_slice(image.as_ref()))?
                } else {
                    Value::Wal {
                        will_init: block.will_init(),
                        rec: Bytes::copy_from_slice(&block.data),
                    }
                };
                self.put_value(key, lsn, value)
            }
            Sharded::Meta { rmid, lsn, rec } => {
                self.put_meta(lsn, rmid, Bytes::copy_from_slice(&rec.main_data))
            }
        }
    }

    /// Buffers a metadata-lane record.
    pub fn put_meta(
        &mut self,
        lsn: Lsn,
        rmid: u8,
        bytes: Bytes,
    ) -> Result<IngestOutcome, OpenLayerError> {
        if lsn <= self.disk_consistent_lsn {
            return Ok(IngestOutcome::IgnoredDurable);
        }
        if self
            .meta
            .iter()
            .any(|record| record.lsn == lsn && record.rmid == rmid && record.bytes == bytes)
        {
            return Ok(IngestOutcome::Duplicate);
        }

        self.meta.push(OpenMetaRecord { lsn, rmid, bytes });
        Ok(IngestOutcome::Buffered)
    }

    /// Flushes buffered entries up to `upto` into one L0 delta layer.
    pub async fn flush(
        &mut self,
        ops: &dyn ObjectOps,
        upto: Lsn,
    ) -> Result<Option<FlushedLayer>, OpenLayerError> {
        if upto < self.disk_consistent_lsn {
            return Err(OpenLayerError::FlushBelowDiskConsistent {
                disk_consistent_lsn: self.disk_consistent_lsn,
                upto,
            });
        }

        let entries = self.entries_to_flush(upto);
        if entries.is_empty() {
            return Ok(None);
        }

        let meta = self.meta_records_to_flush(upto);
        let desc = write_layer(ops, &self.timeline, LayerKind::Delta, &entries).await?;
        self.drop_flushed_entries(&entries);
        self.drop_flushed_meta(upto);
        self.disk_consistent_lsn = upto;
        Ok(Some(FlushedLayer {
            desc,
            disk_consistent_lsn: upto,
            meta,
        }))
    }

    fn entries_to_flush(&self, upto: Lsn) -> Vec<LayerWriteEntry> {
        self.entries
            .iter()
            .filter(|((_, lsn), _)| *lsn <= upto)
            .map(|((key, lsn), value)| (*key, *lsn, value.clone()))
            .collect()
    }

    fn meta_records_to_flush(&self, upto: Lsn) -> Vec<OpenMetaRecord> {
        self.meta
            .iter()
            .filter(|record| record.lsn <= upto)
            .cloned()
            .collect()
    }

    fn drop_flushed_meta(&mut self, upto: Lsn) {
        let mut keep = Vec::new();
        for record in self.meta.drain(..) {
            if record.lsn <= upto {
                continue;
            }
            keep.push(record);
        }
        self.meta = keep;
    }

    fn drop_flushed_entries(&mut self, flushed: &[LayerWriteEntry]) {
        for (key, lsn, _) in flushed {
            self.entries.remove(&(*key, *lsn));
        }
        self.rebuild_newest_lsn_by_key();
    }

    fn rebuild_newest_lsn_by_key(&mut self) {
        self.newest_lsn_by_key.clear();
        for (key, lsn) in self.entries.keys() {
            self.newest_lsn_by_key.insert(*key, *lsn);
        }
    }
}

/// Errors returned by open-layer ingest and flush.
#[derive(Debug, Error)]
pub enum OpenLayerError {
    /// Page values for a key must arrive in non-decreasing LSN order.
    #[error("non-monotonic LSN for key {key}: previous buffered {previous}, current {current}")]
    NonMonotonicLsn {
        key: PageKey,
        previous: Lsn,
        current: Lsn,
    },
    /// Same key/LSN was supplied with different bytes.
    #[error("conflicting value for key {key} at LSN {lsn}")]
    ConflictingValue { key: PageKey, lsn: Lsn },
    /// Flush cannot move the durable boundary backwards.
    #[error("flush target {upto} is below disk-consistent LSN {disk_consistent_lsn}")]
    FlushBelowDiskConsistent { disk_consistent_lsn: Lsn, upto: Lsn },
    /// A routed block index did not exist in the decoded record.
    #[error("routed block index {blk_idx} is missing from decoded WAL record")]
    MissingBlockRef { blk_idx: usize },
    /// Full-page images must be exactly one page.
    #[error(transparent)]
    Value(#[from] crate::value::ValueError),
    /// Layer container writing failed.
    #[error(transparent)]
    Container(#[from] ContainerError),
    /// Object store operation failed.
    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),
}

fn page_key_from_wal(key: crabka_postgres_wal::PageKey) -> PageKey {
    let rel = key.0;
    PageKey::new(rel.spc_oid, rel.db_oid, rel.rel_number, rel.fork, key.1)
}

#[allow(dead_code)]
const _: () = assert!(PAGE_SIZE == 8 * 1024);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_object_store::{ObjectOps as _, ObjectStoreClient};
    use object_store::memory::InMemory;

    use super::*;
    use crate::{LayerMap, TenantId, TimelineId};

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

    fn wal(bytes: &'static [u8]) -> Value {
        Value::Wal {
            will_init: false,
            rec: Bytes::from_static(bytes),
        }
    }

    #[tokio::test]
    async fn flush_produces_l0_and_advances_disk_consistent_lsn() {
        let ops = ops();
        let mut open = OpenLayer::builder(timeline()).build();
        open.put_value(key(0), Lsn(10), wal(b"r1")).unwrap();
        open.put_value(key(0), Lsn(20), wal(b"r2")).unwrap();

        let flushed = open.flush(&ops, Lsn(20)).await.unwrap().unwrap();
        let objects = ops
            .list(Some(&object_store::path::Path::from(timeline().prefix())))
            .await
            .unwrap();

        assert!(flushed.desc.lsn_end == Lsn(20));
        assert!(open.disk_consistent_lsn() == Lsn(20));
        assert!(objects.len() == 1);
    }

    #[tokio::test]
    async fn reingest_below_disk_consistent_lsn_is_a_noop() {
        let ops = ops();
        let mut open = OpenLayer::builder(timeline()).build();
        open.put_value(key(0), Lsn(10), wal(b"r1")).unwrap();
        open.put_value(key(0), Lsn(20), wal(b"r2")).unwrap();
        open.flush(&ops, Lsn(20)).await.unwrap().unwrap();
        let before = ops.list(None).await.unwrap();

        assert!(
            open.put_value(key(0), Lsn(10), wal(b"r1")).unwrap() == IngestOutcome::IgnoredDurable
        );
        assert!(
            open.put_value(key(0), Lsn(20), wal(b"r2")).unwrap() == IngestOutcome::IgnoredDurable
        );
        assert!(open.flush(&ops, Lsn(20)).await.unwrap().is_none());

        assert!(ops.list(None).await.unwrap() == before);
    }

    #[tokio::test]
    async fn metadata_only_flush_retains_meta_without_advancing_disk_consistent_lsn() {
        let ops = ops();
        let meta_bytes = Bytes::from_static(b"meta-only");
        let expected_meta = OpenMetaRecord {
            lsn: Lsn(10),
            rmid: 42,
            bytes: meta_bytes.clone(),
        };
        let mut open = OpenLayer::builder(timeline()).build();
        open.put_meta(Lsn(10), 42, meta_bytes).unwrap();

        let metadata_only_flush = open.flush(&ops, Lsn(10)).await.unwrap();

        assert!(metadata_only_flush.is_none());
        assert!(open.disk_consistent_lsn() == Lsn(0));
        assert!(open.meta_records() == std::slice::from_ref(&expected_meta));

        let accepted_after_metadata_only_flush = open
            .put_value(
                key(0),
                Lsn(10),
                Value::Wal {
                    will_init: true,
                    rec: Bytes::from_static(b"r1"),
                },
            )
            .unwrap();
        let flushed = open.flush(&ops, Lsn(10)).await.unwrap().unwrap();
        let mut map = LayerMap::new(timeline());
        map.insert(flushed.desc).unwrap();
        let reconstructed = map
            .get_reconstruct_data(&ops, key(0), Lsn(10))
            .await
            .unwrap();

        assert!(accepted_after_metadata_only_flush == IngestOutcome::Buffered);
        assert!(flushed.meta == vec![expected_meta]);
        assert!(open.meta_records().is_empty());
        assert!(open.disk_consistent_lsn() == Lsn(10));
        assert!(reconstructed.deltas == vec![(Lsn(10), Bytes::from_static(b"r1"))]);
    }

    #[test]
    fn rejects_non_monotonic_lsn_for_buffered_key() {
        let mut open = OpenLayer::builder(timeline()).build();
        open.put_value(key(0), Lsn(20), wal(b"r2")).unwrap();

        let err = open.put_value(key(0), Lsn(10), wal(b"r1")).unwrap_err();

        assert!(matches!(err, OpenLayerError::NonMonotonicLsn { .. }));
    }

    #[test]
    fn meta_records_are_retained_verbatim() {
        let mut open = OpenLayer::builder(timeline()).build();
        let bytes = Bytes::from_static(b"meta-bytes");

        open.put_meta(Lsn(10), 42, bytes.clone()).unwrap();

        assert!(
            open.meta_records()
                == &[OpenMetaRecord {
                    lsn: Lsn(10),
                    rmid: 42,
                    bytes
                }]
        );
    }

    #[tokio::test]
    async fn flushed_layer_reconstructs_through_layer_map() {
        let ops = ops();
        let mut open = OpenLayer::builder(timeline()).build();
        open.put_value(
            key(0),
            Lsn(10),
            Value::Wal {
                will_init: true,
                rec: Bytes::from_static(b"init"),
            },
        )
        .unwrap();
        open.put_value(key(0), Lsn(20), wal(b"r2")).unwrap();
        let flushed = open.flush(&ops, Lsn(20)).await.unwrap().unwrap();
        let mut map = LayerMap::new(timeline());
        map.insert(flushed.desc).unwrap();

        let rd = map
            .get_reconstruct_data(&ops, key(0), Lsn(20))
            .await
            .unwrap();

        assert!(rd.base.is_none());
        assert!(
            rd.deltas
                == vec![
                    (Lsn(10), Bytes::from_static(b"init")),
                    (Lsn(20), Bytes::from_static(b"r2"))
                ]
        );
    }
}
