//! Immutable layer descriptors, manifests, and layer indexes.

use std::{
    cmp::Ordering,
    fs, io,
    io::Write as _,
    path::{Component, Path, PathBuf},
};

use crabka_postgres_wal::Lsn;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{PageKey, TimelinePath};

/// Immutable layer kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayerKind {
    /// Full image layer.
    Image,
    /// WAL delta layer.
    Delta,
}

impl LayerKind {
    fn extension(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Delta => "delta",
        }
    }
}

/// LSN-aware immutable layer metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayerDesc {
    /// Timeline namespace that owns the layer.
    pub timeline: TimelinePath,
    /// Layer kind.
    pub kind: LayerKind,
    /// Inclusive first key covered by this layer.
    pub key_start: PageKey,
    /// Inclusive last key covered by this layer.
    pub key_end: PageKey,
    /// Inclusive first LSN covered by this layer.
    #[serde(with = "lsn_serde")]
    pub lsn_start: Lsn,
    /// Inclusive last LSN covered by this layer.
    #[serde(with = "lsn_serde")]
    pub lsn_end: Lsn,
}

impl LayerDesc {
    /// Builds a layer descriptor after checking range invariants.
    pub fn new(
        timeline: TimelinePath,
        kind: LayerKind,
        key_start: PageKey,
        key_end: PageKey,
        lsn_start: Lsn,
        lsn_end: Lsn,
    ) -> Result<Self, LayerError> {
        if key_start > key_end {
            return Err(LayerError::InvalidKeyRange { key_start, key_end });
        }

        if lsn_start > lsn_end {
            return Err(LayerError::InvalidLsnRange { lsn_start, lsn_end });
        }

        Ok(Self {
            timeline,
            kind,
            key_start,
            key_end,
            lsn_start,
            lsn_end,
        })
    }

    /// Returns true when this layer covers `key`.
    #[must_use]
    pub fn contains_key(&self, key: PageKey) -> bool {
        self.key_start <= key && key <= self.key_end
    }

    /// Returns true when this layer covers `lsn`.
    #[must_use]
    pub fn contains_lsn(&self, lsn: Lsn) -> bool {
        self.lsn_start <= lsn && lsn <= self.lsn_end
    }

    /// Encodes this descriptor as a deterministic layer object name.
    #[must_use]
    pub fn object_name(&self) -> String {
        format!(
            "{}/{}-{}__{:016x}-{:016x}.{}",
            self.timeline.prefix(),
            self.key_start,
            self.key_end,
            self.lsn_start.value(),
            self.lsn_end.value(),
            self.kind.extension()
        )
    }
}

/// On-disk descriptor manifest for one immutable layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayerManifest {
    /// Manifest schema version.
    pub version: u16,
    /// Layer descriptor.
    pub desc: LayerDesc,
}

impl LayerManifest {
    /// Current manifest version.
    pub const VERSION: u16 = 1;

    /// Builds a manifest for `desc`.
    #[must_use]
    pub const fn new(desc: LayerDesc) -> Self {
        Self {
            version: Self::VERSION,
            desc,
        }
    }

    /// Serializes this manifest deterministically.
    pub fn to_bytes(&self) -> Result<Vec<u8>, LayerError> {
        let mut bytes = serde_json::to_vec_pretty(self)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Parses a manifest and rejects unsupported versions.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, LayerError> {
        let manifest: Self = serde_json::from_slice(bytes)?;
        if manifest.version != Self::VERSION {
            return Err(LayerError::UnsupportedManifestVersion(manifest.version));
        }

        Ok(manifest)
    }
}

/// In-memory index over immutable layer descriptors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LayerIndex {
    layers: Vec<LayerDesc>,
}

impl LayerIndex {
    /// Builds an empty index.
    #[must_use]
    pub const fn new() -> Self {
        Self { layers: Vec::new() }
    }

    /// Adds a layer descriptor and keeps query order deterministic.
    pub fn insert(&mut self, desc: LayerDesc) {
        self.layers.push(desc);
        self.layers.sort_by(compare_layer_descs);
    }

    /// Returns all descriptors in deterministic newest-first query order.
    #[must_use]
    pub fn layers(&self) -> &[LayerDesc] {
        &self.layers
    }

    /// Finds the best layer containing both `key` and `lsn`.
    #[must_use]
    pub fn best_layer(&self, key: PageKey, lsn: Lsn) -> Option<&LayerDesc> {
        self.layers
            .iter()
            .find(|layer| layer.contains_key(key) && layer.contains_lsn(lsn))
    }
}

/// Directory-backed manifest index used by tests and local development.
#[derive(Debug, Clone)]
pub struct DirectoryLayerIndex {
    root: PathBuf,
}

impl DirectoryLayerIndex {
    /// Builds a directory-backed index rooted at `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Writes a layer manifest exactly once and returns its relative manifest path.
    pub fn write_manifest(&self, manifest: &LayerManifest) -> Result<PathBuf, LayerError> {
        let relative_path = manifest_path(&manifest.desc);
        let path = manifest_write_path(&self.root, &relative_path)?;
        let Some(parent) = path.parent() else {
            return Err(LayerError::MissingManifestParent(relative_path));
        };
        fs::create_dir_all(parent)?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(&manifest.to_bytes()?)?;
        Ok(relative_path)
    }

    /// Rebuilds an in-memory index from all manifests under the directory root.
    pub fn rebuild(&self) -> Result<LayerIndex, LayerError> {
        let mut index = LayerIndex::new();
        if !self.root.exists() {
            return Ok(index);
        }

        for path in manifest_files(&self.root)? {
            let bytes = fs::read(path)?;
            index.insert(LayerManifest::from_bytes(&bytes)?.desc);
        }
        Ok(index)
    }
}

/// Errors returned by layer descriptor and manifest operations.
#[derive(Debug, Error)]
pub enum LayerError {
    /// Key ranges must be ordered.
    #[error("layer key range is inverted: {key_start} > {key_end}")]
    InvalidKeyRange {
        /// Inclusive range start.
        key_start: PageKey,
        /// Inclusive range end.
        key_end: PageKey,
    },
    /// LSN ranges must be ordered.
    #[error("layer LSN range is inverted: {lsn_start} > {lsn_end}")]
    InvalidLsnRange {
        /// Inclusive range start.
        lsn_start: Lsn,
        /// Inclusive range end.
        lsn_end: Lsn,
    },
    /// Manifest version is not supported by this greenfield crate revision.
    #[error("unsupported layer manifest version {0}")]
    UnsupportedManifestVersion(u16),
    /// A relative manifest path unexpectedly had no parent component.
    #[error("manifest path has no parent directory: {0:?}")]
    MissingManifestParent(PathBuf),
    /// Manifest paths must stay under the directory index root.
    #[error("manifest path escapes directory index root: {relative_path:?}")]
    InvalidManifestPath {
        /// Rejected relative manifest path.
        relative_path: PathBuf,
    },
    /// Filesystem I/O failed.
    #[error("layer manifest I/O failed")]
    Io(#[from] io::Error),
    /// JSON serialization failed.
    #[error("layer manifest JSON failed")]
    Json(#[from] serde_json::Error),
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

fn manifest_path(desc: &LayerDesc) -> PathBuf {
    PathBuf::from(format!("{}.manifest.json", desc.object_name()))
}

fn manifest_write_path(root: &Path, relative_path: &Path) -> Result<PathBuf, LayerError> {
    if !is_safe_relative_manifest_path(relative_path) {
        return Err(LayerError::InvalidManifestPath {
            relative_path: relative_path.to_path_buf(),
        });
    }

    let path = root.join(relative_path);
    if !path.starts_with(root) {
        return Err(LayerError::InvalidManifestPath {
            relative_path: relative_path.to_path_buf(),
        });
    }

    Ok(path)
}

fn is_safe_relative_manifest_path(relative_path: &Path) -> bool {
    let Some(relative_path_text) = relative_path.to_str() else {
        return false;
    };
    if relative_path_text.contains('\\') {
        return false;
    }
    if !relative_path_text
        .split('/')
        .all(is_safe_manifest_path_str_component)
    {
        return false;
    }

    let mut has_component = false;
    for component in relative_path.components() {
        let Component::Normal(component) = component else {
            return false;
        };
        if !is_safe_manifest_path_component(component) {
            return false;
        }
        has_component = true;
    }

    has_component
}

fn is_safe_manifest_path_component(component: &std::ffi::OsStr) -> bool {
    let Some(component) = component.to_str() else {
        return false;
    };

    is_safe_manifest_path_str_component(component)
}

fn is_safe_manifest_path_str_component(component: &str) -> bool {
    if component == "." || component == ".." {
        return false;
    }

    !component.is_empty()
        && component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn manifest_files(root: &Path) -> Result<Vec<PathBuf>, LayerError> {
    let mut paths = Vec::new();
    collect_manifest_files(root, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_manifest_files(dir: &Path, paths: &mut Vec<PathBuf>) -> Result<(), LayerError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_manifest_files(&path, paths)?;
            continue;
        }

        if path.extension().and_then(std::ffi::OsStr::to_str) == Some("json") {
            paths.push(path);
        }
    }
    Ok(())
}

mod lsn_serde {
    use crabka_postgres_wal::Lsn;
    use serde::{Deserialize, Deserializer, Serializer};

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(super) fn serialize<S>(lsn: &Lsn, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(lsn.value())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Lsn, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Lsn(u64::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{TenantId, TimelineId};

    fn timeline() -> TimelinePath {
        TimelinePath::new(
            TenantId::parse("tenant").expect("test tenant id is valid"),
            TimelineId::parse("timeline").expect("test timeline id is valid"),
        )
    }

    fn key(block_number: u32) -> PageKey {
        PageKey::new(1663, 5, 16_384, 0, block_number)
    }

    fn desc(
        kind: LayerKind,
        key_start: PageKey,
        key_end: PageKey,
        lsn_start: u64,
        lsn_end: u64,
    ) -> LayerDesc {
        LayerDesc::new(
            timeline(),
            kind,
            key_start,
            key_end,
            Lsn(lsn_start),
            Lsn(lsn_end),
        )
        .expect("test descriptor ranges are valid")
    }

    #[test]
    fn layer_descriptors_reject_inverted_ranges() {
        assert!(
            LayerDesc::new(timeline(), LayerKind::Delta, key(2), key(1), Lsn(1), Lsn(2)).is_err()
        );
        assert!(
            LayerDesc::new(timeline(), LayerKind::Delta, key(1), key(2), Lsn(2), Lsn(1)).is_err()
        );
    }

    #[test]
    fn object_name_uses_fixed_width_sortable_terms() {
        let layer = desc(LayerKind::Delta, key(0), key(128), 0x0100_0000, 0x0140_0000);

        assert!(
            layer.object_name()
                == "pg/tenant/timeline/0000067f00000005000040000000000000-0000067f00000005000040000000000080__0000000001000000-0000000001400000.delta"
        );
    }

    #[test]
    fn best_layer_selects_newest_overlapping_lsn() {
        let probe_key = key(7);
        let mut index = LayerIndex::new();
        index.insert(desc(LayerKind::Delta, key(0), key(9), 10, 30));
        index.insert(desc(LayerKind::Delta, key(0), key(9), 20, 40));
        index.insert(desc(LayerKind::Image, key(0), key(9), 15, 25));

        let best = index.best_layer(probe_key, Lsn(22));

        assert!(
            best.map(|layer| (layer.kind, layer.lsn_start, layer.lsn_end))
                == Some((LayerKind::Delta, Lsn(20), Lsn(40)))
        );
    }

    #[test]
    fn best_layer_returns_none_for_missing_key() {
        let mut index = LayerIndex::new();
        index.insert(desc(LayerKind::Delta, key(0), key(9), 10, 30));

        assert!(index.best_layer(key(10), Lsn(20)).is_none());
    }

    #[test]
    fn manifest_roundtrip_is_deterministic() {
        let manifest = LayerManifest::new(desc(LayerKind::Image, key(0), key(0), 10, 10));
        let first = manifest.to_bytes().expect("manifest serializes");
        let second = manifest
            .to_bytes()
            .expect("manifest serializes deterministically");
        let parsed = LayerManifest::from_bytes(&first).expect("manifest parses");

        assert!(first == second);
        assert!(parsed == manifest);
    }

    #[test]
    fn bad_manifest_input_returns_errors_without_panicking() {
        assert!(LayerManifest::from_bytes(b"not json").is_err());
        assert!(LayerManifest::from_bytes(br#"{"version":99,"desc":null}"#).is_err());
    }

    #[test]
    fn directory_index_rebuilds_written_immutable_manifests() {
        let root = tempfile::tempdir().expect("temp dir is available");
        let dir_index = DirectoryLayerIndex::new(root.path());
        let manifest = LayerManifest::new(desc(LayerKind::Delta, key(0), key(4), 10, 20));

        let relative_path = dir_index
            .write_manifest(&manifest)
            .expect("manifest writes once");
        let rebuilt = dir_index.rebuild().expect("manifest index rebuilds");

        assert!(relative_path.to_string_lossy().ends_with(".manifest.json"));
        assert!(root.path().join(&relative_path).starts_with(root.path()));
        assert!(root.path().join(&relative_path).exists());
        assert!(rebuilt.best_layer(key(2), Lsn(15)) == Some(&manifest.desc));
        assert!(dir_index.write_manifest(&manifest).is_err());
    }

    #[test]
    fn directory_manifest_write_path_rejects_traversal_components() {
        let root = tempfile::tempdir().expect("temp dir is available");

        for relative_path in [
            Path::new("../escape.manifest.json"),
            Path::new("pg/../escape.manifest.json"),
            Path::new("pg/tenant/./timeline/layer.manifest.json"),
            Path::new("pg/tenant/a\\b/layer.manifest.json"),
            Path::new("pg/tenant/with space/layer.manifest.json"),
        ] {
            assert!(manifest_write_path(root.path(), relative_path).is_err());
        }
    }

    #[test]
    fn directory_index_write_manifest_rejects_traversal_ids() {
        let root = tempfile::tempdir().expect("temp dir is available");
        let dir_index = DirectoryLayerIndex::new(root.path());
        let traversal_timeline = TimelinePath::new(
            TenantId::from_raw_for_test(".."),
            TimelineId::from_raw_for_test("timeline"),
        );
        let traversal_desc = LayerDesc::new(
            traversal_timeline,
            LayerKind::Delta,
            key(0),
            key(4),
            Lsn(10),
            Lsn(20),
        )
        .expect("test descriptor ranges are valid");
        let manifest = LayerManifest::new(traversal_desc);

        assert!(dir_index.write_manifest(&manifest).is_err());
        assert!(!root.path().join("timeline").exists());
    }

    #[test]
    fn directory_manifest_write_path_keeps_valid_manifests_under_root() {
        let root = tempfile::tempdir().expect("temp dir is available");
        let relative_path = Path::new(
            "pg/tenant/timeline/0000067f00000005000040000000000000-0000067f00000005000040000000000080__0000000001000000-0000000001400000.delta.manifest.json",
        );

        let path = manifest_write_path(root.path(), relative_path).expect("manifest path is safe");

        assert!(path.starts_with(root.path()));
        assert!(path == root.path().join(relative_path));
    }
}
