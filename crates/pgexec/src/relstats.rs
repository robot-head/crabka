//! Stored `pg_class` relation statistics: `reltuples` and `relhassubclass`.
//!
//! Both columns are *stored* in `PostgreSQL`, not derived, and the regression
//! corpus observes the difference. `expected/vacuum.out` drops a partition and
//! then reads the parent's row **before** re-running `ANALYZE`, and expects the
//! pre-drop count and a `relhassubclass` that is still true. A `relhassubclass`
//! computed from the inheritance links at projection time answers false there,
//! and a `reltuples` computed the same way answers zero. So the two facts live
//! in the catalog, are written when something makes them true, and are only
//! corrected when `ANALYZE` looks.
//!
//! The two facts have different lifecycles, so they are two keys rather than
//! one record:
//!
//! * `relhassubclass` is a latch. Attaching a child sets it — `INHERITS`,
//!   `PARTITION OF`, `ATTACH PARTITION` — and nothing but `ANALYZE` clears it.
//!   Presence of the key *is* the flag, which is what lets
//!   [`crate::inheritance::attach_ops`] and [`crate::partition::attach_ops`]
//!   emit it without first reading what the other column holds.
//! * `reltuples` is an estimate that `ANALYZE` overwrites.
//!
//! Keys carry the relation name, as the inheritance, partition and tablespace
//! keyspaces beside them do. A relation renamed out from under these keys loses
//! them, exactly as it loses its inheritance links today.

use std::collections::BTreeMap;

use crabka_pgcatalog::RelationName;
use crabka_pgkv::{Kv, WriteOp, key::push_key_part};

use crate::error::ExecError;

const SUBCLASS_PREFIX: &[u8] = b"\0\0\0\0catalog_relstats/subclass/";
const TUPLES_PREFIX: &[u8] = b"\0\0\0\0catalog_relstats/tuples/";

/// What `pg_class.reltuples` reports for a relation no `ANALYZE` has looked at:
/// `PostgreSQL`'s "unknown", which is a negative count rather than a null.
pub(crate) const UNKNOWN_TUPLES: f32 = -1.0;

/// Every stored statistic for one relation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RelStats {
    /// `pg_class.reltuples`.
    pub(crate) reltuples: f32,
    /// `pg_class.relhassubclass`.
    pub(crate) has_subclass: bool,
}

impl Default for RelStats {
    fn default() -> Self {
        Self {
            reltuples: UNKNOWN_TUPLES,
            has_subclass: false,
        }
    }
}

fn relation_key(prefix: &[u8], relation: &RelationName) -> Vec<u8> {
    let mut key = prefix.to_vec();
    push_key_part(&mut key, &relation.schema);
    push_key_part(&mut key, &relation.name);
    key
}

/// The op that latches `relhassubclass` on a relation that just gained a child.
pub(crate) fn set_has_subclass_op(parent: &RelationName) -> WriteOp {
    WriteOp::Put {
        key: relation_key(SUBCLASS_PREFIX, parent),
        value: Vec::new(),
    }
}

/// The op `ANALYZE` writes once it has seen that a relation has no children
/// left. Deleting the key is what makes the next projection report false.
pub(crate) fn clear_has_subclass_op(parent: &RelationName) -> WriteOp {
    WriteOp::Delete {
        key: relation_key(SUBCLASS_PREFIX, parent),
    }
}

/// The op `ANALYZE` writes to record the row count it just measured.
pub(crate) fn set_reltuples_op(relation: &RelationName, reltuples: f32) -> WriteOp {
    WriteOp::Put {
        key: relation_key(TUPLES_PREFIX, relation),
        value: reltuples.to_be_bytes().to_vec(),
    }
}

/// Forget everything stored about a relation that is going away.
pub(crate) fn drop_metadata_ops(relation: &RelationName) -> Vec<WriteOp> {
    vec![
        clear_has_subclass_op(relation),
        WriteOp::Delete {
            key: relation_key(TUPLES_PREFIX, relation),
        },
    ]
}

/// Every relation's statistics, in two scans rather than two reads per row.
///
/// `pg_class` is projected in full for every catalog query that touches it, so
/// the per-relation cost of this has to stay off the row loop.
pub(crate) fn all(kv: &dyn Kv) -> Result<BTreeMap<RelationName, RelStats>, ExecError> {
    let mut stats: BTreeMap<RelationName, RelStats> = BTreeMap::new();
    for (key, _) in kv.scan_prefix(SUBCLASS_PREFIX).map_err(ExecError::Kv)? {
        let relation = relation_from_key(SUBCLASS_PREFIX, &key)?;
        stats.entry(relation).or_default().has_subclass = true;
    }
    for (key, value) in kv.scan_prefix(TUPLES_PREFIX).map_err(ExecError::Kv)? {
        let relation = relation_from_key(TUPLES_PREFIX, &key)?;
        let bytes: [u8; 4] = value
            .as_slice()
            .try_into()
            .map_err(|_| corrupt("stored reltuples is not four bytes"))?;
        stats.entry(relation).or_default().reltuples = f32::from_be_bytes(bytes);
    }
    Ok(stats)
}

/// One relation's statistics, for the callers that hold a single name.
pub(crate) fn of(kv: &dyn Kv, relation: &RelationName) -> Result<RelStats, ExecError> {
    let has_subclass = kv
        .get(&relation_key(SUBCLASS_PREFIX, relation))
        .map_err(ExecError::Kv)?
        .is_some();
    let reltuples = match kv
        .get(&relation_key(TUPLES_PREFIX, relation))
        .map_err(ExecError::Kv)?
    {
        Some(value) => {
            let bytes: [u8; 4] = value
                .as_slice()
                .try_into()
                .map_err(|_| corrupt("stored reltuples is not four bytes"))?;
            f32::from_be_bytes(bytes)
        }
        None => UNKNOWN_TUPLES,
    };
    Ok(RelStats {
        reltuples,
        has_subclass,
    })
}

fn relation_from_key(prefix: &[u8], key: &[u8]) -> Result<RelationName, ExecError> {
    let mut suffix = key
        .get(prefix.len()..)
        .ok_or_else(|| corrupt("relation statistics key is shorter than its prefix"))?;
    let schema = key_part(&mut suffix)?;
    let name = key_part(&mut suffix)?;
    if !suffix.is_empty() {
        return Err(corrupt("trailing relation statistics key bytes"));
    }
    Ok(RelationName::new(schema, name))
}

fn key_part(cur: &mut &[u8]) -> Result<String, ExecError> {
    if cur.len() < 4 {
        return Err(corrupt("truncated relation statistics key"));
    }
    let (head, tail) = cur.split_at(4);
    let len = usize::try_from(u32::from_be_bytes(head.try_into().expect("four bytes")))
        .map_err(|_| corrupt("relation statistics key name length exceeds usize"))?;
    if tail.len() < len {
        return Err(corrupt("truncated relation statistics key"));
    }
    let (name, rest) = tail.split_at(len);
    *cur = rest;
    String::from_utf8(name.to_vec()).map_err(|_| corrupt("relation statistics name is not UTF-8"))
}

fn corrupt(message: &str) -> ExecError {
    ExecError::Kv(crabka_pgkv::KvError::CorruptRow(message.into()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::RelationName;
    use crabka_pgkv::{Kv, MemKv};

    use super::{
        RelStats, UNKNOWN_TUPLES, all, clear_has_subclass_op, drop_metadata_ops, of,
        set_has_subclass_op, set_reltuples_op,
    };

    fn relation(schema: &str, name: &str) -> RelationName {
        RelationName::new(schema.to_string(), name.to_string())
    }

    fn apply(kv: &MemKv, ops: Vec<crabka_pgkv::WriteOp>) {
        kv.write_batch(&ops).expect("write batch");
    }

    /// A stored estimate compared bit for bit. A round-trip through the
    /// keyspace has to hand back exactly the float it was given, which is a
    /// stronger claim than numeric equality and the one this store owes.
    fn same_bits(left: f32, right: f32) -> bool {
        left.to_bits() == right.to_bits()
    }

    #[test]
    fn an_untouched_relation_reads_as_unknown_and_childless() {
        let kv = MemKv::new();
        assert!(of(&kv, &relation("public", "t")).expect("read") == RelStats::default());
        assert!(same_bits(RelStats::default().reltuples, UNKNOWN_TUPLES));
        assert!(all(&kv).expect("scan").is_empty());
    }

    #[test]
    fn the_subclass_latch_survives_a_reltuples_write_and_the_other_way_round() {
        let kv = MemKv::new();
        let parent = relation("public", "p");
        apply(&kv, vec![set_has_subclass_op(&parent)]);
        apply(&kv, vec![set_reltuples_op(&parent, 7.0)]);
        assert!(
            of(&kv, &parent).expect("read")
                == RelStats {
                    reltuples: 7.0,
                    has_subclass: true,
                }
        );
        apply(&kv, vec![clear_has_subclass_op(&parent)]);
        assert!(
            of(&kv, &parent).expect("read")
                == RelStats {
                    reltuples: 7.0,
                    has_subclass: false,
                }
        );
    }

    #[test]
    fn a_scan_reports_every_relation_the_single_read_would() {
        let kv = MemKv::new();
        let latched = relation("public", "p");
        let counted = relation("pg_temp_3", "t");
        apply(
            &kv,
            vec![
                set_has_subclass_op(&latched),
                set_reltuples_op(&counted, -0.5),
            ],
        );
        let scanned = all(&kv).expect("scan");
        assert!(scanned.len() == 2);
        for name in [&latched, &counted] {
            assert!(scanned.get(name).copied() == Some(of(&kv, name).expect("read")));
        }
    }

    #[test]
    fn two_relations_whose_key_parts_concatenate_alike_stay_apart() {
        // "a"."bc" and "ab"."c" produce the same bytes under naive
        // concatenation; the length prefixes are what keep them distinct.
        let kv = MemKv::new();
        let left = relation("a", "bc");
        let right = relation("ab", "c");
        apply(
            &kv,
            vec![set_reltuples_op(&left, 1.0), set_reltuples_op(&right, 2.0)],
        );
        assert!(same_bits(of(&kv, &left).expect("read").reltuples, 1.0));
        assert!(same_bits(of(&kv, &right).expect("read").reltuples, 2.0));
        assert!(all(&kv).expect("scan").len() == 2);
    }

    #[test]
    fn dropping_a_relation_forgets_both_halves() {
        let kv = MemKv::new();
        let name = relation("public", "gone");
        apply(
            &kv,
            vec![set_has_subclass_op(&name), set_reltuples_op(&name, 3.0)],
        );
        apply(&kv, drop_metadata_ops(&name));
        assert!(of(&kv, &name).expect("read") == RelStats::default());
        assert!(all(&kv).expect("scan").is_empty());
    }
}
