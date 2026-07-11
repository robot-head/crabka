//! Offline verification of an audit partition's hash-chain + signed checkpoints.
//!
//! Reads `<dir>/*.log` segment files directly (NO recovery/truncation, so tail
//! corruption is visible) and recomputes the chain with the same primitives the
//! writer used, validating each checkpoint signature against a trusted key.

use std::{collections::HashMap, path::Path};

use crabka_protocol::records::{Record, RecordBatch};

use crate::{
    chain::{GENESIS_HEAD, chain_hash, from_hex32},
    checkpoint::{Checkpoint, EVENT_CLASS_CHECKPOINT},
    ids::{CheckpointCount, RecordCount, Seq},
    sink::{AuditError, HEADER_PREV_HASH, HEADER_SEQ},
};

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
    pub seq: Option<Seq>,
    pub reason: String,
}

/// Result of verifying a partition.
///
/// `unanchored_records` is only meaningful when `ok` is `true`. It counts
/// records whose seq is greater than the highest seq covered by the last valid
/// signed checkpoint. When `ok` is `false` this field is 0 (the walk stopped
/// at the break before a reliable count could be established).
#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub records: RecordCount,
    pub checkpoints: CheckpointCount,
    pub ok: bool,
    pub first_break: Option<VerifyBreak>,
    /// Number of records that are NOT covered by a signed checkpoint (i.e. the
    /// unsigned tail). Zero means the chain is fully attested. Only meaningful
    /// when `ok` is `true`.
    pub unanchored_records: RecordCount,
}

fn header<'a>(record: &'a Record, key: &str) -> Option<&'a [u8]> {
    record
        .headers
        .iter()
        .find(|h| h.key == key)
        .and_then(|h| h.value.as_deref())
}

fn broke(
    records: RecordCount,
    checkpoints: CheckpointCount,
    offset: i64,
    seq: Option<Seq>,
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
        // unanchored_records is 0 when ok=false; the count is not meaningful
        // after a break since the walk stopped early.
        unanchored_records: RecordCount(0),
    }
}

/// Mutable per-record walk state threaded through helpers.
struct WalkState {
    head: [u8; 32],
    expected_seq: Seq,
    records: RecordCount,
    checkpoints: CheckpointCount,
    /// The `seq_high` of the most-recently validated checkpoint, if any.
    last_checkpoint_seq_high: Option<Seq>,
}

impl WalkState {
    fn new() -> Self {
        Self {
            head: GENESIS_HEAD,
            expected_seq: Seq(0),
            records: RecordCount(0),
            checkpoints: CheckpointCount(0),
            last_checkpoint_seq_high: None,
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
    state.checkpoints.0 += 1;
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
    if cp.seq_high != Seq(state.expected_seq.0.saturating_sub(1)) {
        return Err(broke(
            state.records,
            state.checkpoints,
            offset,
            None,
            "checkpoint seq_high does not match record count",
        ));
    }
    state.last_checkpoint_seq_high = Some(cp.seq_high);
    Ok(())
}

/// Validate a chained data record and advance the chain head.
/// Returns `Err(VerifyReport)` on the first detected break.
fn check_chained(rec: &Record, offset: i64, state: &mut WalkState) -> Result<(), VerifyReport> {
    let value = rec.value.as_deref().unwrap_or_default();
    let seq = header(rec, HEADER_SEQ)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Seq);
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
    state.head = chain_hash(&state.head, seq.0, value);
    state.expected_seq.0 += 1;
    state.records.0 += 1;
    Ok(())
}

/// Verify the audit partition under `dir`.
///
/// Reads all `*.log` segment files in base-offset (filename) order, decodes
/// each `RecordBatch` directly (not via `Log::open` — that path runs
/// recovery/truncation which would silently mask tail corruption), recomputes
/// the hash-chain, and validates every checkpoint signature against `trusted`.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(dir = %dir.display(), records = tracing::field::Empty, checkpoints = tracing::field::Empty, ok = tracing::field::Empty),
    err
)]
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
                    let span = tracing::Span::current();
                    span.record("records", report.records.0);
                    span.record("checkpoints", report.checkpoints.0);
                    span.record("ok", report.ok);
                    return Ok(report);
                }
            }
        }
    }

    let unanchored_records = match state.last_checkpoint_seq_high {
        Some(seq_high) => RecordCount(state.records.0.saturating_sub(seq_high.0 + 1)),
        None => state.records,
    };

    let span = tracing::Span::current();
    span.record("records", state.records.0);
    span.record("checkpoints", state.checkpoints.0);
    span.record("ok", true);
    Ok(VerifyReport {
        records: state.records,
        checkpoints: state.checkpoints,
        ok: true,
        first_break: None,
        unanchored_records,
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
    use crate::{
        chain::ChainState, checkpoint::Checkpoint, ids::EpochMs, signing::FileEd25519Signer,
        sink::AuditRecord,
    };

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
        let cp = Checkpoint::signed(
            s.as_ref(),
            Seq(chain.next_seq() - 1),
            &chain.head(),
            EpochMs(123),
        );
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
        // build_partition writes 3 records (seq 0..2) + 1 checkpoint (seq_high=2),
        // so all records are covered.
        check!(
            (
                report.ok,
                report.records.0,
                report.checkpoints.0,
                report.first_break.is_none(),
                report.unanchored_records.0,
            ) == (true, 3, 1, true, 0)
        );
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

    // ── Fix 1 tests: unanchored_records field ─────────────────────────────────

    /// Partition with a trailing tail of 2 records beyond the last checkpoint.
    #[test]
    fn unanchored_tail_records_are_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let (s, pubkey) = signer();
        let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
        let mut chain = ChainState::new();
        let mut offset = 0i64;

        // 3 records + checkpoint (seq_high=2)
        for i in 0..3u8 {
            let mut rec = crate::sink::AuditRecord {
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
        let cp = Checkpoint::signed(
            s.as_ref(),
            Seq(chain.next_seq() - 1),
            &chain.head(),
            EpochMs(100),
        );
        let mut b = audit_record_to_batch(&cp.to_record(), offset);
        log.append(&mut b).unwrap();
        offset += 1;

        // 2 more records WITHOUT a trailing checkpoint
        for i in 3..5u8 {
            let mut rec = crate::sink::AuditRecord {
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

        let trusted = TrustedKeys::single("k1".into(), pubkey);
        let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
        check!(
            (
                report.ok,
                report.checkpoints.0,
                report.records.0,
                report.unanchored_records.0,
            ) == (true, 1, 5, 2)
        );
    }

    /// Chain-only partition (no signing key, no checkpoints) — all records are unanchored.
    #[test]
    fn chain_only_partition_all_records_unanchored() {
        let tmp = tempfile::tempdir().unwrap();
        let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
        let mut chain = ChainState::new();

        for (offset, i) in (0..3u8).enumerate() {
            let mut rec = crate::sink::AuditRecord {
                class: crate::event::AuditEventClass::ApplicationLifecycle,
                value: format!("{{\"i\":{i}}}").into_bytes(),
                headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
            };
            let (seq, prev) = chain.extend(&rec.value);
            rec.push_chain_headers(seq, &prev);
            let mut b = audit_record_to_batch(&rec, i64::try_from(offset).unwrap());
            log.append(&mut b).unwrap();
        }

        // No trusted key needed — no checkpoints present
        let trusted = TrustedKeys::default();
        let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
        check!(
            (
                report.ok,
                report.checkpoints.0,
                report.records.0,
                report.unanchored_records.0,
            ) == (true, 0, 3, 3)
        );
    }

    // ── Fix 2 tests: direct tamper-detection (chain-inconsistent fixtures) ────

    /// Dropped record creates a seq gap that the verifier detects as a break.
    #[test]
    fn dropped_record_detected_as_seq_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
        let mut chain = ChainState::new();

        // Build 3 records into memory first, then write only [0] and [2] (skip [1]).
        let mut records: Vec<crate::sink::AuditRecord> = (0..3u8)
            .map(|i| crate::sink::AuditRecord {
                class: crate::event::AuditEventClass::ApplicationLifecycle,
                value: format!("{{\"i\":{i}}}").into_bytes(),
                headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
            })
            .collect();

        for rec in &mut records {
            let (seq, prev) = chain.extend(&rec.value);
            rec.push_chain_headers(seq, &prev);
        }

        // Write records[0] (seq=0) then records[2] (seq=2) — skip records[1]
        let mut b = audit_record_to_batch(&records[0], 0);
        log.append(&mut b).unwrap();
        let mut b = audit_record_to_batch(&records[2], 1);
        log.append(&mut b).unwrap();

        let trusted = TrustedKeys::default();
        let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
        check!(!report.ok, "dropped record must be detected as tamper");
        let reason = &report.first_break.unwrap().reason;
        check!(
            reason.contains("seq"),
            "reason should mention seq gap, got: {reason}"
        );
    }

    /// A record stamped with the wrong `prev_hash` is detected as a chain break.
    #[test]
    fn wrong_prev_hash_detected() {
        let tmp = tempfile::tempdir().unwrap();
        let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
        let mut chain = ChainState::new();

        // Record 0: correct chain
        let mut rec0 = crate::sink::AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: b"{\"i\":0}".to_vec(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        };
        let (seq0, prev0) = chain.extend(&rec0.value);
        rec0.push_chain_headers(seq0, &prev0);
        let mut b = audit_record_to_batch(&rec0, 0);
        log.append(&mut b).unwrap();

        // Record 1: stamped with GENESIS_HEAD as prev (wrong — should be head after rec0)
        let mut rec1 = crate::sink::AuditRecord {
            class: crate::event::AuditEventClass::ApplicationLifecycle,
            value: b"{\"i\":1}".to_vec(),
            headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
        };
        // Advance chain to get the correct seq, but use GENESIS_HEAD as wrong prev
        let (seq1, _correct_prev) = chain.extend(&rec1.value);
        rec1.push_chain_headers(seq1, &GENESIS_HEAD); // wrong prev
        let mut b = audit_record_to_batch(&rec1, 1);
        log.append(&mut b).unwrap();

        let trusted = TrustedKeys::default();
        let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
        check!(!report.ok, "wrong prev_hash must be detected as tamper");
        let reason = &report.first_break.unwrap().reason;
        check!(
            reason.contains("prev_hash"),
            "reason should mention prev_hash, got: {reason}"
        );
    }

    /// A checkpoint signed over the original chain head does not match after
    /// records are replaced with different values (re-stamped chain head differs).
    #[test]
    fn stale_checkpoint_chain_head_mismatch_detected() {
        let tmp_orig = tempfile::tempdir().unwrap();
        let tmp_tampered = tempfile::tempdir().unwrap();

        let (s, pubkey) = signer();

        // Build original partition: 2 records + checkpoint
        let mut orig_chain = ChainState::new();
        let mut orig_records: Vec<crate::sink::AuditRecord> = (0..2u8)
            .map(|i| crate::sink::AuditRecord {
                class: crate::event::AuditEventClass::ApplicationLifecycle,
                value: format!("{{\"i\":{i}}}").into_bytes(),
                headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
            })
            .collect();
        for rec in &mut orig_records {
            let (seq, prev) = orig_chain.extend(&rec.value);
            rec.push_chain_headers(seq, &prev);
        }
        let orig_cp = Checkpoint::signed(
            s.as_ref(),
            Seq(orig_chain.next_seq() - 1),
            &orig_chain.head(),
            EpochMs(42),
        );

        // Build tampered partition: same structure but different values → different chain head
        // but reuse the OLD checkpoint (signed over the original head)
        let mut tampered_chain = ChainState::new();
        let mut tampered_records: Vec<crate::sink::AuditRecord> = (0..2u8)
            .map(|i| crate::sink::AuditRecord {
                class: crate::event::AuditEventClass::ApplicationLifecycle,
                value: format!("{{\"i\":{},\"tampered\":true}}", i + 10).into_bytes(),
                headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
            })
            .collect();
        for rec in &mut tampered_records {
            let (seq, prev) = tampered_chain.extend(&rec.value);
            rec.push_chain_headers(seq, &prev);
        }

        let mut log = Log::open(tmp_tampered.path(), LogConfig::default()).unwrap();
        let mut offset = 0i64;
        for rec in &tampered_records {
            let mut b = audit_record_to_batch(rec, offset);
            log.append(&mut b).unwrap();
            offset += 1;
        }
        // Reuse the OLD checkpoint (signed over original chain head — won't match tampered head)
        let mut b = audit_record_to_batch(&orig_cp.to_record(), offset);
        log.append(&mut b).unwrap();

        let _ = tmp_orig; // keep alive

        let trusted = TrustedKeys::single("k1".into(), pubkey);
        let report = verify_partition_dir(tmp_tampered.path(), &trusted).unwrap();
        check!(
            !report.ok,
            "stale checkpoint over wrong chain_head must be detected"
        );
        let reason = &report.first_break.unwrap().reason;
        check!(
            reason.contains("chain_head"),
            "reason should mention chain_head, got: {reason}"
        );
    }
}
