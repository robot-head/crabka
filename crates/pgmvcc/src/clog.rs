//! Commit-status log — Postgres' `pg_xact`. Maps each transaction id to its
//! final outcome; the authority on whether a writer committed. An ABSENT entry
//! means the xid recorded no outcome: it is in-progress while the transaction
//! runs, and aborted-equivalent after a crash (it is then in no live snapshot).

use crabka_pgkv::{Kv, KvError, WriteOp};

/// A transaction's recorded outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XidStatus {
    InProgress,
    Committed,
    Aborted,
    Prepared(u64),
}

const S_IN_PROGRESS: u8 = 0;
const S_COMMITTED: u8 = 1;
const S_ABORTED: u8 = 2;
const S_PREPARED: u8 = 3;

/// Read an xid's status. An absent entry is treated as `InProgress`
/// (aborted-equivalent once the xid is in no live snapshot — see recovery).
pub fn get(kv: &dyn Kv, xid: u64) -> Result<XidStatus, KvError> {
    decode(
        &kv.get(&crabka_pgkv::key::clog_key(xid))?
            .unwrap_or_default(),
    )
}

/// Decode a clog entry's bytes. An EMPTY slice (an absent key, via
/// `kv.get(...)?.unwrap_or_default()`) is `InProgress` — preserving `get`'s
/// absent-key semantics. A non-empty value decodes its status byte.
pub fn decode(value: &[u8]) -> Result<XidStatus, KvError> {
    match value.first() {
        None | Some(&S_IN_PROGRESS) => Ok(XidStatus::InProgress),
        Some(&S_COMMITTED) => Ok(XidStatus::Committed),
        Some(&S_ABORTED) => Ok(XidStatus::Aborted),
        Some(&S_PREPARED) => {
            let g: [u8; 8] = value
                .get(1..9)
                .ok_or_else(|| KvError::CorruptRow("prepared clog missing global xid".into()))?
                .try_into()
                .expect("slice 1..9 is 8 bytes");
            Ok(XidStatus::Prepared(u64::from_be_bytes(g)))
        }
        _ => Err(KvError::CorruptRow("bad clog status byte".into())),
    }
}

/// A write-batch op recording an xid's final status.
#[must_use]
pub fn put_op(xid: u64, status: XidStatus) -> WriteOp {
    let value = match status {
        XidStatus::InProgress => vec![S_IN_PROGRESS],
        XidStatus::Committed => vec![S_COMMITTED],
        XidStatus::Aborted => vec![S_ABORTED],
        XidStatus::Prepared(g) => {
            let mut v = Vec::with_capacity(9);
            v.push(S_PREPARED);
            v.extend_from_slice(&g.to_be_bytes());
            v
        }
    };
    WriteOp::Put {
        key: crabka_pgkv::key::clog_key(xid),
        value,
    }
}

/// True iff `value` (a clog entry's bytes) encodes a TERMINAL decision
/// (Committed/Aborted) — the statuses the write-once global decision must keep.
/// `Prepared`/`InProgress`/empty are non-terminal.
#[must_use]
pub fn is_terminal(value: &[u8]) -> bool {
    matches!(value.first(), Some(&S_COMMITTED | &S_ABORTED))
}

#[cfg(test)]
mod tests {
    use crabka_pgkv::MemKv;

    use super::*;

    #[test]
    fn absent_entry_is_in_progress() {
        let kv = MemKv::new();
        assert_eq!(get(&kv, 7).expect("get"), XidStatus::InProgress);
    }

    #[test]
    fn committed_and_aborted_roundtrip() {
        let kv = MemKv::new();
        kv.write_batch(&[put_op(7, XidStatus::Committed)])
            .expect("put");
        kv.write_batch(&[put_op(8, XidStatus::Aborted)])
            .expect("put");
        assert_eq!(get(&kv, 7).expect("get"), XidStatus::Committed);
        assert_eq!(get(&kv, 8).expect("get"), XidStatus::Aborted);
    }

    #[test]
    fn corrupt_status_byte_errors() {
        let kv = MemKv::new();
        kv.write_batch(&[crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::clog_key(9),
            value: vec![99],
        }])
        .expect("put");
        assert!(get(&kv, 9).is_err());
    }

    #[test]
    fn prepared_carries_global_xid_roundtrip() {
        let kv = MemKv::new();
        kv.write_batch(&[put_op(
            7,
            XidStatus::Prepared(crate::xid::GLOBAL_XID_BASE + 3),
        )])
        .expect("put");
        assert_eq!(
            get(&kv, 7).expect("get"),
            XidStatus::Prepared(crate::xid::GLOBAL_XID_BASE + 3)
        );
    }

    #[test]
    fn truncated_prepared_value_errors_not_panics() {
        let kv = MemKv::new();
        kv.write_batch(&[crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::clog_key(9),
            value: vec![3],
        }])
        .expect("put");
        assert!(get(&kv, 9).is_err());
    }

    #[test]
    fn is_terminal_only_for_committed_and_aborted() {
        // Derive each status's on-disk bytes from put_op so this tracks the real
        // encoding rather than hardcoding the status bytes.
        let bytes = |status| match put_op(1, status) {
            crabka_pgkv::WriteOp::Put { value, .. }
            | crabka_pgkv::WriteOp::ConditionalPut { value, .. } => value,
            crabka_pgkv::WriteOp::Delete { .. } => {
                unreachable!("put_op must record a clog status")
            }
        };
        assert!(is_terminal(&bytes(XidStatus::Committed)));
        assert!(is_terminal(&bytes(XidStatus::Aborted)));
        assert!(!is_terminal(&bytes(XidStatus::InProgress)));
        assert!(!is_terminal(&bytes(XidStatus::Prepared(
            crate::xid::GLOBAL_XID_BASE + 1
        ))));
        assert!(!is_terminal(&[])); // an absent clog entry is non-terminal
    }
}
