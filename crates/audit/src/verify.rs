//! Offline verification of an audit partition's hash-chain + signed checkpoints.
//!
//! Reads `<dir>/*.log` segment files directly (NO recovery/truncation, so tail
//! corruption is visible) and recomputes the chain with the same primitives the
//! writer used, validating each checkpoint signature against a trusted key.

use std::collections::HashMap;
use std::path::Path;

use crabka_protocol::records::{Record, RecordBatch};

use crate::chain::{GENESIS_HEAD, chain_hash, from_hex32};
use crate::checkpoint::{Checkpoint, EVENT_CLASS_CHECKPOINT};
use crate::sink::{AuditError, HEADER_PREV_HASH, HEADER_SEQ};

/// Trusted public keys, keyed by `key_id`.
#[derive(Debug, Default)]
pub struct TrustedKeys {
    keys: HashMap<String, Vec<u8>>,
}

impl TrustedKeys {
    #[must_use]
    pub fn single(key_id: String, public_key: Vec<u8>) -> Self {
        let mut keys = HashMap::new();
        keys.insert(key_id, public_key);
        Self { keys }
    }

    #[must_use]
    pub fn get(&self, key_id: &str) -> Option<&[u8]> {
        self.keys.get(key_id).map(Vec::as_slice)
    }
}

/// First detected break in the chain or signatures.
#[derive(Debug, Clone)]
pub struct VerifyBreak {
    pub offset: i64,
    pub seq: Option<u64>,
    pub reason: String,
}

/// Result of verifying a partition.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub records: u64,
    pub checkpoints: u64,
    pub ok: bool,
    pub first_break: Option<VerifyBreak>,
}

fn header<'a>(record: &'a Record, key: &str) -> Option<&'a [u8]> {
    record
        .headers
        .iter()
        .find(|h| h.key == key)
        .and_then(|h| h.value.as_deref())
}

fn broke(
    records: u64,
    checkpoints: u64,
    offset: i64,
    seq: Option<u64>,
    reason: &str,
) -> VerifyReport {
    VerifyReport {
        records,
        checkpoints,
        ok: false,
        first_break: Some(VerifyBreak {
            offset,
            seq,
            reason: reason.to_string(),
        }),
    }
}

/// Mutable per-record walk state threaded through helpers.
struct WalkState {
    head: [u8; 32],
    expected_seq: u64,
    records: u64,
    checkpoints: u64,
}

impl WalkState {
    fn new() -> Self {
        Self {
            head: GENESIS_HEAD,
            expected_seq: 0,
            records: 0,
            checkpoints: 0,
        }
    }
}

/// Validate a checkpoint record against the running chain and trusted keys.
/// Returns `Err(VerifyReport)` on the first detected break.
fn check_checkpoint(
    rec: &Record,
    offset: i64,
    state: &mut WalkState,
    trusted: &TrustedKeys,
) -> Result<(), VerifyReport> {
    state.checkpoints += 1;
    let value = rec.value.as_deref().unwrap_or_default();
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(value) else {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "checkpoint value is not JSON",
        ));
    };
    let Some(cp) = Checkpoint::from_value(&json) else {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "malformed checkpoint",
        ));
    };
    let Some(pubkey) = trusted.get(&cp.key_id) else {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            &format!("no trusted key for key_id '{}'", cp.key_id),
        ));
    };
    if !cp.verify(pubkey) {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "checkpoint signature invalid",
        ));
    }
    if cp.chain_head != state.head {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "checkpoint chain_head does not match recomputed chain",
        ));
    }
    if cp.seq_high != state.expected_seq.saturating_sub(1) {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "checkpoint seq_high does not match record count",
        ));
    }
    Ok(())
}

/// Validate a chained data record and advance the chain head.
/// Returns `Err(VerifyReport)` on the first detected break.
fn check_chained(rec: &Record, offset: i64, state: &mut WalkState) -> Result<(), VerifyReport> {
    let value = rec.value.as_deref().unwrap_or_default();
    let seq = header(rec, HEADER_SEQ)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<u64>().ok());
    let prev = header(rec, HEADER_PREV_HASH)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(from_hex32);
    let (Some(seq), Some(prev)) = (seq, prev) else {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            seq,
            "missing/invalid chain headers",
        ));
    };
    if seq != state.expected_seq {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            Some(seq),
            &format!("seq gap: expected {}, found {seq}", state.expected_seq),
        ));
    }
    if prev != state.head {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            Some(seq),
            "prev_hash does not match recomputed chain head",
        ));
    }
    state.head = chain_hash(&state.head, seq, value);
    state.expected_seq += 1;
    state.records += 1;
    Ok(())
}

/// Verify the audit partition under `dir`.
///
/// Reads all `*.log` segment files in base-offset (filename) order, decodes
/// each `RecordBatch` directly (not via `Log::open` — that path runs
/// recovery/truncation which would silently mask tail corruption), recomputes
/// the hash-chain, and validates every checkpoint signature against `trusted`.
pub fn verify_partition_dir(dir: &Path, trusted: &TrustedKeys) -> Result<VerifyReport, AuditError> {
    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| AuditError::Sink(format!("read dir {}: {e}", dir.display())))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    segments.sort();

    let mut state = WalkState::new();

    for seg in segments {
        let bytes = std::fs::read(&seg)
            .map_err(|e| AuditError::Sink(format!("read segment {}: {e}", seg.display())))?;
        let mut cur: &[u8] = &bytes;
        while !cur.is_empty() {
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                break; // undecodable tail: stop this segment (visible truncation)
            };
            for rec in &batch.records {
                let offset = batch.base_offset + i64::from(rec.offset_delta);
                let class = header(rec, "event_class").unwrap_or_default();
                let result = if class == EVENT_CLASS_CHECKPOINT.as_bytes() {
                    check_checkpoint(rec, offset, &mut state, trusted)
                } else {
                    check_chained(rec, offset, &mut state)
                };
                if let Err(report) = result {
                    return Ok(report);
                }
            }
        }
    }

    Ok(VerifyReport {
        records: state.records,
        checkpoints: state.checkpoints,
        ok: true,
        first_break: None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use bytes::Bytes;
    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::{Record, RecordBatch, RecordHeader};

    use super::*;
    use crate::chain::ChainState;
    use crate::checkpoint::Checkpoint;
    use crate::signing::FileEd25519Signer;
    use crate::sink::AuditRecord;

    fn signer() -> (Arc<FileEd25519Signer>, Vec<u8>) {
        use ring::signature::{Ed25519KeyPair, KeyPair};
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let pubkey = kp.public_key().as_ref().to_vec();
        (
            Arc::new(FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "k1".into()).unwrap()),
            pubkey,
        )
    }

    fn audit_record_to_batch(rec: &AuditRecord, base_offset: i64) -> RecordBatch {
        let headers = rec
            .headers
            .iter()
            .map(|(k, v)| RecordHeader {
                key: k.clone(),
                value: Some(Bytes::from(v.clone())),
            })
            .collect();
        let mut batch = RecordBatch {
            base_offset,
            last_offset_delta: 0,
            ..RecordBatch::default()
        };
        batch.records.push(Record {
            offset_delta: 0,
            value: Some(Bytes::from(rec.value.clone())),
            headers,
            ..Default::default()
        });
        batch
    }

    /// Build a valid chained+checkpointed partition on disk; return pubkey.
    fn build_partition(tmp: &std::path::Path) -> Vec<u8> {
        let (s, pubkey) = signer();
        let mut log = Log::open(tmp, LogConfig::default()).unwrap();
        let mut chain = ChainState::new();
        let mut offset = 0i64;
        for i in 0..3u8 {
            let mut rec = AuditRecord {
                class: crate::event::AuditEventClass::ApplicationLifecycle,
                value: format!("{{\"i\":{i}}}").into_bytes(),
                headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
            };
            let (seq, prev) = chain.extend(&rec.value);
            rec.push_chain_headers(seq, &prev);
            let mut b = audit_record_to_batch(&rec, offset);
            log.append(&mut b).unwrap();
            offset += 1;
        }
        // checkpoint over the chain head
        let cp = Checkpoint::signed(s.as_ref(), chain.next_seq() - 1, &chain.head(), 123);
        let mut b = audit_record_to_batch(&cp.to_record(), offset);
        log.append(&mut b).unwrap();
        pubkey
    }

    #[test]
    fn valid_partition_verifies_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let pubkey = build_partition(tmp.path());
        let trusted = TrustedKeys::single("k1".into(), pubkey);
        let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
        check!(report.ok);
        check!(report.records == 3);
        check!(report.checkpoints == 1);
        check!(report.first_break.is_none());
    }

    #[test]
    fn wrong_trusted_key_fails_at_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let _pubkey = build_partition(tmp.path());
        let (_other, other_pub) = signer();
        let trusted = TrustedKeys::single("k1".into(), other_pub);
        let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
        check!(!report.ok);
        let b = report.first_break.unwrap();
        check!(b.reason.contains("signature"));
    }
}
