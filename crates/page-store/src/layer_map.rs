//! Rebuildable immutable layer map and page reconstruction planning.

use std::{cmp::Ordering, collections::BTreeMap};

use bytes::Bytes;
use crabka_object_store::{ObjectOps, ObjectStoreError};
use crabka_postgres_wal::Lsn;
use object_store::path::Path as ObjectPath;
use thiserror::Error;

use crate::{ContainerError, LayerDesc, LayerKind, LayerReader, PageKey, TimelinePath, Value};

/// Values needed by a future redo implementation to reconstruct one page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconstructData {
    /// Newest full-page image at or below the requested LSN, if history reaches one.
    pub base: Option<(Lsn, Bytes)>,
    /// WAL records to apply in oldest-first order.
    pub deltas: Vec<(Lsn, Bytes)>,
}

/// In-memory map of immutable layers for one timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerMap {
    timeline: TimelinePath,
    layers: Vec<LayerDesc>,
}

impl LayerMap {
    /// Builds an empty layer map for `timeline`.
    #[must_use]
    pub const fn new(timeline: TimelinePath) -> Self {
        Self {
            timeline,
            layers: Vec::new(),
        }
    }

    /// Rebuilds the map by listing immutable layer containers below the timeline prefix.
    pub async fn rebuild(
        ops: &dyn ObjectOps,
        timeline: TimelinePath,
    ) -> Result<Self, LayerMapError> {
        let prefix = ObjectPath::from(timeline.prefix());
        let mut map = Self::new(timeline);
        for meta in ops.list(Some(&prefix)).await? {
            let object_name = meta.location.as_ref();
            let Some(desc) = parse_layer_object_name(object_name, &map.timeline)? else {
                continue;
            };
            map.insert(desc)?;
        }

        Ok(map)
    }

    /// Adds a layer descriptor and keeps deterministic newest-first query order.
    pub fn insert(&mut self, desc: LayerDesc) -> Result<(), LayerMapError> {
        if desc.timeline != self.timeline {
            return Err(LayerMapError::WrongTimeline {
                expected: self.timeline.prefix(),
                actual: desc.timeline.prefix(),
            });
        }

        self.layers.push(desc);
        self.layers.sort_by(compare_layer_descs);
        Ok(())
    }

    /// Removes any descriptor with an object name matching `desc`.
    pub fn remove(&mut self, desc: &LayerDesc) {
        let object_name = desc.object_name();
        self.layers
            .retain(|layer| layer.object_name() != object_name);
    }

    /// Replaces old descriptors with new descriptors as one deterministic map update.
    pub fn replace_layers(
        &mut self,
        old_layers: &[LayerDesc],
        new_layers: impl IntoIterator<Item = LayerDesc>,
    ) -> Result<(), LayerMapError> {
        for old_layer in old_layers {
            self.remove(old_layer);
        }
        for new_layer in new_layers {
            self.insert(new_layer)?;
        }
        Ok(())
    }

    /// Returns layer descriptors in deterministic newest-first query order.
    #[must_use]
    pub fn layers(&self) -> &[LayerDesc] {
        &self.layers
    }

    /// Returns this map's timeline namespace.
    #[must_use]
    pub const fn timeline(&self) -> &TimelinePath {
        &self.timeline
    }

    /// Finds the best layer containing both `key` and `lsn`.
    #[must_use]
    pub fn best_layer(&self, key: PageKey, lsn: Lsn) -> Option<&LayerDesc> {
        self.layers
            .iter()
            .find(|layer| layer.contains_key(key) && layer.contains_lsn(lsn))
    }

    /// Returns image/WAL records needed to reconstruct `key` at `target_lsn`.
    pub async fn get_reconstruct_data(
        &self,
        ops: &dyn ObjectOps,
        key: PageKey,
        target_lsn: Lsn,
    ) -> Result<ReconstructData, LayerMapError> {
        let mut records = BTreeMap::new();
        for desc in self.intersecting_layers(key, target_lsn) {
            let reader = LayerReader::open(ops, desc).await?;
            for (_, lsn, value) in reader.entries_for_key(ops, key).await? {
                if lsn > target_lsn || records.contains_key(&lsn) {
                    continue;
                }
                records.insert(lsn, value);
            }
        }

        reconstruct_from_records(records, key, target_lsn)
    }

    fn intersecting_layers(
        &self,
        key: PageKey,
        target_lsn: Lsn,
    ) -> impl Iterator<Item = &LayerDesc> {
        self.layers
            .iter()
            .filter(move |layer| layer.contains_key(key) && layer.lsn_start <= target_lsn)
    }
}

/// Errors returned while rebuilding layer maps or planning reconstruction.
#[derive(Debug, Error)]
pub enum LayerMapError {
    /// The requested page history is not available in the layer set.
    #[error("history for key {key} at LSN {lsn} is trimmed or missing")]
    HistoryTrimmed {
        /// Page key requested.
        key: PageKey,
        /// Target LSN requested.
        lsn: Lsn,
    },
    /// A layer descriptor belongs to a different timeline.
    #[error("layer belongs to timeline {actual}, expected {expected}")]
    WrongTimeline {
        /// Expected timeline prefix.
        expected: String,
        /// Actual timeline prefix.
        actual: String,
    },
    /// A listed object looked like a layer but did not parse as one.
    #[error("invalid layer object name `{object}`: {reason}")]
    InvalidObjectName {
        /// Object name.
        object: String,
        /// Human-readable reason.
        reason: &'static str,
    },
    /// Layer descriptor invariants failed.
    #[error(transparent)]
    Layer(#[from] crate::LayerError),
    /// Layer container read failed.
    #[error(transparent)]
    Container(#[from] ContainerError),
    /// Object store operation failed.
    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),
}

fn reconstruct_from_records(
    records: BTreeMap<Lsn, Value>,
    key: PageKey,
    target_lsn: Lsn,
) -> Result<ReconstructData, LayerMapError> {
    let mut deltas = Vec::new();
    for (lsn, value) in records.into_iter().rev() {
        match value {
            Value::Image(page) => {
                deltas.reverse();
                return Ok(ReconstructData {
                    base: Some((lsn, page)),
                    deltas,
                });
            }
            Value::Wal { will_init, rec } => {
                deltas.push((lsn, rec));
                if will_init {
                    deltas.reverse();
                    return Ok(ReconstructData { base: None, deltas });
                }
            }
        }
    }

    Err(LayerMapError::HistoryTrimmed {
        key,
        lsn: target_lsn,
    })
}

fn parse_layer_object_name(
    object_name: &str,
    timeline: &TimelinePath,
) -> Result<Option<LayerDesc>, LayerMapError> {
    let prefix = timeline.prefix();
    let Some(file_name) = object_name.strip_prefix(&format!("{prefix}/")) else {
        return Ok(None);
    };
    if file_name.ends_with(".manifest.json") {
        return Ok(None);
    }

    let Some((body, extension)) = file_name.rsplit_once('.') else {
        return Ok(None);
    };
    let kind = match extension {
        "image" => LayerKind::Image,
        "delta" => LayerKind::Delta,
        _ => return Ok(None),
    };

    let Some((key_range, lsn_range)) = body.split_once("__") else {
        return Err(invalid_object_name(
            object_name,
            "missing key/LSN separator",
        ));
    };
    let Some((key_start, key_end)) = key_range.split_once('-') else {
        return Err(invalid_object_name(
            object_name,
            "missing key range separator",
        ));
    };
    let Some((lsn_start, lsn_end)) = lsn_range.split_once('-') else {
        return Err(invalid_object_name(
            object_name,
            "missing LSN range separator",
        ));
    };

    Ok(Some(LayerDesc::new(
        timeline.clone(),
        kind,
        parse_key_hex(key_start)
            .ok_or_else(|| invalid_object_name(object_name, "invalid start key"))?,
        parse_key_hex(key_end)
            .ok_or_else(|| invalid_object_name(object_name, "invalid end key"))?,
        parse_lsn_hex(lsn_start)
            .ok_or_else(|| invalid_object_name(object_name, "invalid start LSN"))?,
        parse_lsn_hex(lsn_end)
            .ok_or_else(|| invalid_object_name(object_name, "invalid end LSN"))?,
    )?))
}

fn parse_key_hex(raw: &str) -> Option<PageKey> {
    if raw.len() != 34 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }

    Some(PageKey::new(
        u32::from_str_radix(&raw[0..8], 16).ok()?,
        u32::from_str_radix(&raw[8..16], 16).ok()?,
        u32::from_str_radix(&raw[16..24], 16).ok()?,
        u8::from_str_radix(&raw[24..26], 16).ok()?,
        u32::from_str_radix(&raw[26..34], 16).ok()?,
    ))
}

fn parse_lsn_hex(raw: &str) -> Option<Lsn> {
    if raw.len() != 16 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }

    Some(Lsn(u64::from_str_radix(raw, 16).ok()?))
}

fn invalid_object_name(object: &str, reason: &'static str) -> LayerMapError {
    LayerMapError::InvalidObjectName {
        object: object.to_owned(),
        reason,
    }
}

fn compare_layer_descs(left: &LayerDesc, right: &LayerDesc) -> Ordering {
    right
        .lsn_end
        .cmp(&left.lsn_end)
        .then_with(|| right.lsn_start.cmp(&left.lsn_start))
        .then_with(|| left.key_start.cmp(&right.key_start))
        .then_with(|| left.key_end.cmp(&right.key_end))
        .then_with(|| left.kind.cmp(&right.kind))
        .then_with(|| left.timeline.cmp(&right.timeline))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use bytes::Bytes;
    use crabka_object_store::ObjectStoreClient;
    use object_store::memory::InMemory;

    use super::*;
    use crate::{PAGE_SIZE, TenantId, TimelineId, write_layer};

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

    fn page(byte: u8) -> Bytes {
        Bytes::from(vec![byte; PAGE_SIZE])
    }

    fn image(byte: u8) -> Value {
        Value::image(page(byte)).expect("test image is one page")
    }

    fn wal(will_init: bool, rec: &'static [u8]) -> Value {
        Value::Wal {
            will_init,
            rec: Bytes::from_static(rec),
        }
    }

    fn desc(kind: LayerKind, key_start: PageKey, key_end: PageKey) -> LayerDesc {
        LayerDesc::new(timeline(), kind, key_start, key_end, Lsn(10), Lsn(30))
            .expect("test descriptor ranges are valid")
    }

    async fn write_and_insert(
        ops: &dyn ObjectOps,
        map: &mut LayerMap,
        kind: LayerKind,
        entries: &[crate::LayerWriteEntry],
    ) {
        let desc = write_layer(ops, &timeline(), kind, entries)
            .await
            .expect("layer writes");
        map.insert(desc).expect("descriptor matches map timeline");
    }

    #[tokio::test]
    async fn reconstruct_stops_at_image_base() {
        let ops = ops();
        let mut map = LayerMap::new(timeline());
        write_and_insert(
            &ops,
            &mut map,
            LayerKind::Delta,
            &[(key(0), Lsn(10), image(b'a'))],
        )
        .await;
        write_and_insert(
            &ops,
            &mut map,
            LayerKind::Delta,
            &[(key(0), Lsn(20), wal(false, b"r1"))],
        )
        .await;
        write_and_insert(
            &ops,
            &mut map,
            LayerKind::Delta,
            &[(key(0), Lsn(30), wal(false, b"r2"))],
        )
        .await;

        let rd = map
            .get_reconstruct_data(&ops, key(0), Lsn(25))
            .await
            .expect("history reaches image base");

        assert!(rd.base == Some((Lsn(10), page(b'a'))));
        assert!(rd.deltas == vec![(Lsn(20), Bytes::from_static(b"r1"))]);
    }

    #[tokio::test]
    async fn will_init_terminates_with_no_base() {
        let ops = ops();
        let mut map = LayerMap::new(timeline());
        write_and_insert(
            &ops,
            &mut map,
            LayerKind::Delta,
            &[(key(0), Lsn(20), wal(true, b"init"))],
        )
        .await;
        write_and_insert(
            &ops,
            &mut map,
            LayerKind::Delta,
            &[(key(0), Lsn(30), wal(false, b"r2"))],
        )
        .await;

        let rd = map
            .get_reconstruct_data(&ops, key(0), Lsn(30))
            .await
            .expect("history reaches will_init record");

        assert!(rd.base.is_none());
        assert!(
            rd.deltas
                == vec![
                    (Lsn(20), Bytes::from_static(b"init")),
                    (Lsn(30), Bytes::from_static(b"r2"))
                ]
        );
    }

    #[tokio::test]
    async fn overlapping_layers_choose_deterministic_newest_records() {
        let ops = ops();
        let mut map = LayerMap::new(timeline());
        write_and_insert(
            &ops,
            &mut map,
            LayerKind::Delta,
            &[
                (key(0), Lsn(10), image(b'a')),
                (key(0), Lsn(20), wal(false, b"old")),
            ],
        )
        .await;
        write_and_insert(
            &ops,
            &mut map,
            LayerKind::Delta,
            &[(key(0), Lsn(20), wal(false, b"new"))],
        )
        .await;

        let rd = map
            .get_reconstruct_data(&ops, key(0), Lsn(20))
            .await
            .expect("overlapping history is reconstructable");

        assert!(rd.base == Some((Lsn(10), page(b'a'))));
        assert!(rd.deltas == vec![(Lsn(20), Bytes::from_static(b"new"))]);
    }

    #[tokio::test]
    async fn best_layer_selection_is_deterministic() {
        let mut map = LayerMap::new(timeline());
        let narrow = desc(LayerKind::Delta, key(0), key(1));
        let wide = desc(LayerKind::Delta, key(0), key(9));
        map.insert(wide).expect("descriptor matches map timeline");
        map.insert(narrow.clone())
            .expect("descriptor matches map timeline");

        let best = map.best_layer(key(0), Lsn(10));

        assert!(best == Some(&narrow));
    }

    #[tokio::test]
    async fn missing_history_is_trimmed_error() {
        let ops = ops();
        let mut map = LayerMap::new(timeline());
        write_and_insert(
            &ops,
            &mut map,
            LayerKind::Delta,
            &[(key(0), Lsn(20), wal(false, b"r1"))],
        )
        .await;

        let err = map
            .get_reconstruct_data(&ops, key(0), Lsn(20))
            .await
            .unwrap_err();

        assert!(matches!(err, LayerMapError::HistoryTrimmed { .. }));
    }

    #[tokio::test]
    async fn below_oldest_history_is_trimmed_error() {
        let ops = ops();
        let mut map = LayerMap::new(timeline());
        write_and_insert(
            &ops,
            &mut map,
            LayerKind::Delta,
            &[(key(0), Lsn(20), wal(true, b"init"))],
        )
        .await;

        let err = map
            .get_reconstruct_data(&ops, key(0), Lsn(5))
            .await
            .unwrap_err();

        assert!(matches!(err, LayerMapError::HistoryTrimmed { .. }));
    }

    #[tokio::test]
    async fn rebuild_from_container_files_matches_registered_map() {
        let ops = ops();
        let mut registered = LayerMap::new(timeline());
        write_and_insert(
            &ops,
            &mut registered,
            LayerKind::Image,
            &[(key(0), Lsn(10), image(b'a'))],
        )
        .await;
        write_and_insert(
            &ops,
            &mut registered,
            LayerKind::Delta,
            &[(key(0), Lsn(20), wal(false, b"r1"))],
        )
        .await;

        let rebuilt = LayerMap::rebuild(&ops, timeline())
            .await
            .expect("map rebuilds from object listing");
        let from_registered = registered
            .get_reconstruct_data(&ops, key(0), Lsn(20))
            .await
            .expect("registered map reconstructs");
        let from_rebuilt = rebuilt
            .get_reconstruct_data(&ops, key(0), Lsn(20))
            .await
            .expect("rebuilt map reconstructs");

        assert!(rebuilt.layers() == registered.layers());
        assert!(from_rebuilt == from_registered);
    }
}
