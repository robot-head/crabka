//! Checkpoint manifest JSON format and validation.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::codec::{CheckpointPart, sha256_hex};
use crate::error::SubstrateError;

/// Current checkpoint manifest format version.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Versioned JSON checkpoint manifest written after every part is durable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest wire format version. Must be [`MANIFEST_FORMAT_VERSION`].
    pub format_version: u32,
    /// Tenant whose KV state was checkpointed.
    pub tenant: String,
    /// WAL offset covered through. Replay resumes at `covered_offset + 1`.
    pub covered_offset: i64,
    /// Next engine journal sequence at the snapshot instant.
    pub journal_seq: u64,
    /// Fenced writer epoch that produced the checkpoint.
    pub producer_epoch: i16,
    /// WAL generation containing `covered_offset`.
    pub wal_generation: u64,
    /// Part entries in key order.
    pub parts: Vec<PartEntry>,
    /// Total key/value pairs across all parts.
    pub total_pairs: u64,
    /// Total encoded checkpoint bytes across manifest and parts.
    pub total_bytes: u64,
}

impl Manifest {
    /// Construct a version-1 manifest from already computed part entries.
    #[must_use]
    pub fn new(
        tenant: String,
        covered_offset: i64,
        journal_seq: u64,
        producer_epoch: i16,
        wal_generation: u64,
        parts: Vec<PartEntry>,
    ) -> Self {
        let total_pairs = parts.iter().map(|part| part.pairs).sum();
        let total_part_bytes = parts.iter().map(|part| part.encoded_bytes).sum();
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            tenant,
            covered_offset,
            journal_seq,
            producer_epoch,
            wal_generation,
            parts,
            total_pairs,
            total_bytes: total_part_bytes,
        }
    }

    /// Serialize the manifest as JSON bytes.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn encode(&self) -> Result<Vec<u8>, SubstrateError> {
        self.validate_shape()?;
        serde_json::to_vec(self).map_err(|error| SubstrateError::Checkpoint(error.to_string()))
    }

    /// Decode JSON bytes and validate intrinsic manifest invariants.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn decode(bytes: &[u8]) -> Result<Self, SubstrateError> {
        let manifest = serde_json::from_slice::<Self>(bytes)
            .map_err(|error| SubstrateError::Checkpoint(error.to_string()))?;
        manifest.validate_shape()?;
        Ok(manifest)
    }

    /// Validate this manifest for the expected tenant/generation and visible parts.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn validate(
        &self,
        validation: &ManifestValidation<'_>,
    ) -> Result<Vec<CheckpointPart>, SubstrateError> {
        self.validate_shape()?;
        if self.tenant != validation.tenant {
            return Err(SubstrateError::Checkpoint(format!(
                "checkpoint tenant mismatch: manifest {}, expected {}",
                self.tenant, validation.tenant,
            )));
        }
        if self.wal_generation != validation.wal_generation {
            return Err(SubstrateError::Checkpoint(format!(
                "checkpoint generation mismatch: manifest {}, expected {}",
                self.wal_generation, validation.wal_generation,
            )));
        }
        if let Some(log_start) = validation.log_start {
            let replay_start = self.covered_offset.checked_add(1).ok_or_else(|| {
                SubstrateError::Checkpoint("checkpoint covered offset overflow".into())
            })?;
            if log_start > replay_start {
                return Err(SubstrateError::TornTruncation {
                    log_start,
                    newest_manifest: self.covered_offset,
                });
            }
        }

        self.decode_and_verify_parts(validation.parts_by_name)
    }

    fn validate_shape(&self) -> Result<(), SubstrateError> {
        if self.format_version != MANIFEST_FORMAT_VERSION {
            return Err(SubstrateError::Checkpoint(format!(
                "unknown checkpoint manifest format version {}",
                self.format_version,
            )));
        }
        if self.tenant.is_empty() {
            return Err(SubstrateError::Checkpoint(
                "checkpoint tenant must not be empty".into(),
            ));
        }
        if self.covered_offset < 0 {
            return Err(SubstrateError::Checkpoint(
                "checkpoint covered_offset must be non-negative".into(),
            ));
        }

        let mut expected_pairs = 0_u64;
        let mut previous_name: Option<&str> = None;
        for part in &self.parts {
            part.validate_shape()?;
            if previous_name.is_some_and(|name| name >= part.name.as_str()) {
                return Err(SubstrateError::Checkpoint(
                    "checkpoint parts must be in ascending name order".into(),
                ));
            }
            previous_name = Some(&part.name);
            expected_pairs = expected_pairs.checked_add(part.pairs).ok_or_else(|| {
                SubstrateError::Checkpoint("checkpoint total_pairs overflow".into())
            })?;
        }

        if expected_pairs != self.total_pairs {
            return Err(SubstrateError::Checkpoint(format!(
                "checkpoint total_pairs mismatch: manifest {}, entries {}",
                self.total_pairs, expected_pairs,
            )));
        }
        let expected_bytes = self.parts.iter().try_fold(0_u64, |total, part| {
            total
                .checked_add(part.encoded_bytes)
                .ok_or_else(|| SubstrateError::Checkpoint("checkpoint total_bytes overflow".into()))
        })?;
        if expected_bytes > self.total_bytes {
            return Err(SubstrateError::Checkpoint(format!(
                "checkpoint total_bytes mismatch: manifest {}, entries {}",
                self.total_bytes, expected_bytes,
            )));
        }
        Ok(())
    }

    fn decode_and_verify_parts(
        &self,
        parts_by_name: &BTreeMap<String, Vec<u8>>,
    ) -> Result<Vec<CheckpointPart>, SubstrateError> {
        let mut decoded_parts = Vec::with_capacity(self.parts.len());
        let mut observed_pairs = 0_u64;
        let mut previous_last_key: Option<Vec<u8>> = None;

        for entry in &self.parts {
            let Some(bytes) = parts_by_name.get(&entry.name) else {
                return Err(SubstrateError::Checkpoint(format!(
                    "checkpoint missing part {}",
                    entry.name,
                )));
            };
            let digest = sha256_hex(bytes);
            if digest != entry.sha256_hex {
                return Err(SubstrateError::ChecksumMismatch {
                    part: entry.name.clone(),
                });
            }

            let part = CheckpointPart::decode(bytes)?;
            let pair_count = u64::try_from(part.pairs.len())
                .map_err(|_| SubstrateError::Checkpoint("part pair count exceeds u64".into()))?;
            if pair_count != entry.pairs {
                return Err(SubstrateError::Checkpoint(format!(
                    "checkpoint part {} pair count mismatch: manifest {}, decoded {}",
                    entry.name, entry.pairs, pair_count,
                )));
            }
            verify_part_key_order(previous_last_key.as_deref(), &part)?;
            previous_last_key = part.pairs.last().map(|(key, _)| key.clone());
            observed_pairs = observed_pairs.checked_add(pair_count).ok_or_else(|| {
                SubstrateError::Checkpoint("checkpoint observed pair count overflow".into())
            })?;
            decoded_parts.push(part);
        }

        if observed_pairs != self.total_pairs {
            return Err(SubstrateError::Checkpoint(format!(
                "checkpoint decoded total_pairs mismatch: manifest {}, decoded {}",
                self.total_pairs, observed_pairs,
            )));
        }
        Ok(decoded_parts)
    }
}

/// One manifest entry for a checkpoint part object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartEntry {
    /// Object name or key used to fetch the part.
    pub name: String,
    /// Number of key/value pairs in the part.
    pub pairs: u64,
    /// Lowercase SHA-256 digest of the encoded part bytes.
    pub sha256_hex: String,
    /// Encoded part object size in bytes.
    pub encoded_bytes: u64,
}

impl PartEntry {
    /// Create an entry from a part object name and encoded part bytes.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn from_encoded_part(name: String, bytes: &[u8]) -> Result<Self, SubstrateError> {
        let part = CheckpointPart::decode(bytes)?;
        Ok(Self {
            name,
            pairs: u64::try_from(part.pairs.len())
                .map_err(|_| SubstrateError::Checkpoint("part pair count exceeds u64".into()))?,
            sha256_hex: sha256_hex(bytes),
            encoded_bytes: u64::try_from(bytes.len())
                .map_err(|_| SubstrateError::Checkpoint("part bytes exceed u64".into()))?,
        })
    }

    fn validate_shape(&self) -> Result<(), SubstrateError> {
        if self.name.is_empty() {
            return Err(SubstrateError::Checkpoint(
                "checkpoint part name must not be empty".into(),
            ));
        }
        if !is_lowercase_sha256_hex(&self.sha256_hex) {
            return Err(SubstrateError::Checkpoint(format!(
                "checkpoint part {} has invalid lowercase sha256_hex",
                self.name,
            )));
        }
        if self.encoded_bytes == 0 {
            return Err(SubstrateError::Checkpoint(format!(
                "checkpoint part {} has zero encoded_bytes",
                self.name,
            )));
        }
        Ok(())
    }
}

fn is_lowercase_sha256_hex(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// External visibility context for manifest-last checkpoint validation.
pub struct ManifestValidation<'a> {
    /// Tenant expected by the recovery/checkpointer caller.
    pub tenant: &'a str,
    /// WAL generation expected by the caller.
    pub wal_generation: u64,
    /// Optional Kafka log start offset for torn-truncation detection.
    pub log_start: Option<i64>,
    /// Visible part bytes, keyed by manifest entry name.
    pub parts_by_name: &'a BTreeMap<String, Vec<u8>>,
}

fn verify_part_key_order(
    previous_last_key: Option<&[u8]>,
    part: &CheckpointPart,
) -> Result<(), SubstrateError> {
    let mut last_key = previous_last_key;
    for (key, _) in &part.pairs {
        if last_key.is_some_and(|previous| previous >= key.as_slice()) {
            return Err(SubstrateError::Checkpoint(
                "checkpoint parts must contain strictly ascending keys".into(),
            ));
        }
        last_key = Some(key);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::checkpoint::{PartPayload, ckpt_dir, ckpt_dir_for_range, manifest_key, part_key};

    #[test]
    fn manifest_json_round_trips() {
        let (manifest, _) = manifest_with_parts(vec![(b"a".to_vec(), b"one".to_vec())]);

        let decoded = Manifest::decode(&manifest.encode().expect("encode")).expect("decode");

        assert!(decoded == manifest);
    }

    #[test]
    fn unknown_manifest_version_is_named_error() {
        let (mut manifest, _) = manifest_with_parts(Vec::new());
        manifest.format_version = 99;

        let error = manifest.encode().expect_err("version rejected");

        assert!(format!("{error}").contains("unknown checkpoint manifest format version 99"));
    }

    #[test]
    fn manifest_last_visibility_requires_manifest() {
        let (_, parts) = manifest_with_parts(vec![(b"a".to_vec(), b"one".to_vec())]);

        assert!(visible_checkpoint(None, &parts).is_none());
    }

    #[test]
    fn manifest_last_visibility_accepts_manifest_after_parts() {
        let (manifest, parts) = manifest_with_parts(vec![(b"a".to_vec(), b"one".to_vec())]);
        let manifest_bytes = manifest.encode().expect("encode");

        let visible = visible_checkpoint(Some(&manifest_bytes), &parts).expect("manifest visible");

        assert!(visible.total_pairs == 1);
    }

    #[test]
    fn rejects_missing_part() {
        let (manifest, parts) = manifest_with_parts(vec![(b"a".to_vec(), b"one".to_vec())]);
        let empty_parts = BTreeMap::new();

        assert!(manifest.validate(&validation(&empty_parts)).is_err());
        assert!(manifest.validate(&validation(&parts)).is_ok());
    }

    #[test]
    fn rejects_bad_digest() {
        let (manifest, mut parts) = manifest_with_parts(vec![(b"a".to_vec(), b"one".to_vec())]);
        if let Some(byte) = parts.values_mut().next().expect("part").last_mut() {
            *byte ^= 1;
        }

        assert!(matches!(
            manifest.validate(&validation(&parts)),
            Err(SubstrateError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn rejects_pair_count_mismatch() {
        let (mut manifest, parts) = manifest_with_parts(vec![(b"a".to_vec(), b"one".to_vec())]);
        manifest.parts[0].pairs = 2;
        manifest.total_pairs = 2;

        assert!(manifest.validate(&validation(&parts)).is_err());
    }

    #[test]
    fn rejects_tenant_generation_and_torn_offsets() {
        let (manifest, parts) = manifest_with_parts(vec![(b"a".to_vec(), b"one".to_vec())]);

        let tenant_error = manifest.validate(&ManifestValidation {
            tenant: "other",
            wal_generation: 7,
            log_start: None,
            parts_by_name: &parts,
        });
        let generation_error = manifest.validate(&ManifestValidation {
            tenant: "tenant-a",
            wal_generation: 8,
            log_start: None,
            parts_by_name: &parts,
        });
        let torn_error = manifest.validate(&ManifestValidation {
            tenant: "tenant-a",
            wal_generation: 7,
            log_start: Some(12),
            parts_by_name: &parts,
        });

        assert!(tenant_error.is_err());
        assert!(generation_error.is_err());
        assert!(matches!(
            torn_error,
            Err(SubstrateError::TornTruncation { .. })
        ));
    }

    #[test]
    fn rejects_non_ascending_keys_across_parts() {
        let part_a = CheckpointPart::new(vec![(b"b".to_vec(), b"one".to_vec())]).encode();
        let part_b = CheckpointPart::new(vec![(b"a".to_vec(), b"two".to_vec())]).encode();
        let mut parts = BTreeMap::new();
        parts.insert("part-00000".to_string(), part_a.clone());
        parts.insert("part-00001".to_string(), part_b.clone());
        let manifest = Manifest::new(
            "tenant-a".to_string(),
            10,
            2,
            3,
            7,
            vec![
                PartEntry::from_encoded_part("part-00000".to_string(), &part_a).expect("entry"),
                PartEntry::from_encoded_part("part-00001".to_string(), &part_b).expect("entry"),
            ],
        );

        assert!(manifest.validate(&validation(&parts)).is_err());
    }

    #[test]
    fn key_layout_is_zero_padded_and_manifest_last() {
        let dir = ckpt_dir("tenant-a", 2, 42, 3);

        assert!(dir == "gres/tenant-a/ckpt/0000000002-00000000000000000042-00003/");
        assert!(part_key(&dir, 9) == format!("{dir}part-00009"));
        assert!(manifest_key(&dir) == format!("{dir}MANIFEST"));
    }

    #[test]
    fn range_key_layout_uses_range_checkpoint_prefix() {
        let tenant = crabka_gres_ranges::TenantName::parse("tenant-a").expect("tenant");
        let dir = ckpt_dir_for_range(&tenant, crabka_gres_ranges::RangeId::new(4), 2, 42, 3);

        assert!(dir == "gres/tenant-a/r4/ckpt/0000000002-00000000000000000042-00003/");
        assert!(part_key(&dir, 9) == format!("{dir}part-00009"));
        assert!(manifest_key(&dir) == format!("{dir}MANIFEST"));
    }

    fn visible_checkpoint(
        manifest_bytes: Option<&[u8]>,
        parts: &BTreeMap<String, Vec<u8>>,
    ) -> Option<Manifest> {
        let manifest = Manifest::decode(manifest_bytes?).ok()?;
        manifest.validate(&validation(parts)).ok()?;
        Some(manifest)
    }

    fn validation(parts_by_name: &BTreeMap<String, Vec<u8>>) -> ManifestValidation<'_> {
        ManifestValidation {
            tenant: "tenant-a",
            wal_generation: 7,
            log_start: Some(11),
            parts_by_name,
        }
    }

    fn manifest_with_parts(pairs: Vec<PartPayload>) -> (Manifest, BTreeMap<String, Vec<u8>>) {
        let part = CheckpointPart::new(pairs);
        let part_bytes = part.encode();
        let mut parts = BTreeMap::new();
        parts.insert("part-00000".to_string(), part_bytes.clone());
        let manifest = Manifest::new(
            "tenant-a".to_string(),
            10,
            2,
            3,
            7,
            vec![
                PartEntry::from_encoded_part("part-00000".to_string(), &part_bytes).expect("entry"),
            ],
        );
        (manifest, parts)
    }
}
