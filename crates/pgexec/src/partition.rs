//! Declarative partitioning: catalog metadata, bound validation, overlap
//! detection, and row routing.
//!
//! A partitioned parent is a catalog relation that owns no rows: every row
//! lives in exactly one leaf partition, chosen by comparing the row's partition
//! key against each leaf's stored bound. This module is the single place that
//! decides which leaf a row belongs to, so the `INSERT` routing path, the
//! per-leaf constraint check, and `ATTACH PARTITION` validation cannot disagree
//! about it.
//!
//! # Relationship to native sharding
//!
//! The chapter design assigns declarative partitioning to the G-8/G-9c sharding
//! machinery. That mapping is *not* what this module does, and the reason is
//! the program's correctness-over-coverage rule: sharding routes on a hash of a
//! single column into a power-of-two bucket count, which cannot express a
//! `LIST` bound, a `RANGE` bound, a `DEFAULT` partition, or `PostgreSQL`'s
//! `MODULUS`/`REMAINDER` hash bucketing — and a sharded relation additionally
//! has a narrower mutation surface (no `PRIMARY KEY`/`UNIQUE`). Routing a
//! partitioned table through it would answer with the wrong rows for every
//! shape but one. Partitions are therefore ordinary relations linked by catalog
//! metadata, and `SHARDED` and `PARTITION BY` are mutually exclusive
//! ([`reject_sharded_partitioned`]).

pub(crate) mod hash;

use std::cmp::Ordering;

use crabka_pgcatalog::RelationName;
use crabka_pgkv::{
    Kv, WriteOp,
    key::{key_parts, push_key_part},
};
use crabka_pgtypes::{ColumnType, Datum};

use crate::error::ExecError;

/// System-key prefix for a partitioned parent's key definition.
///
/// The three partition families sit *beside* the relation catalog rather than
/// under it: a scan of `catalog/` answers "every stored relation", and a
/// partition record living under that prefix would be handed to the relation
/// decoder as if it were one.
const SCHEME_PREFIX: &[u8] = b"\0\0\0\0catalog_partition/scheme/";
/// System-key prefix for a leaf's parent link and bound.
const CHILD_PREFIX: &[u8] = b"\0\0\0\0catalog_partition/child/";
/// System-key prefix for the parent → child index.
const CHILDREN_PREFIX: &[u8] = b"\0\0\0\0catalog_partition/children/";

const SCHEME_VERSION: u8 = 1;
const BOUND_VERSION: u8 = 1;

/// `PARTITION BY` strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Strategy {
    Range,
    List,
    Hash,
}

impl Strategy {
    /// The word `PostgreSQL` prints in its diagnostics and stores in
    /// `pg_partitioned_table.partstrat`'s expansion.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Strategy::Range => "range",
            Strategy::List => "list",
            Strategy::Hash => "hash",
        }
    }

    /// `pg_partitioned_table.partstrat`.
    pub(crate) fn code(self) -> &'static str {
        match self {
            Strategy::Range => "r",
            Strategy::List => "l",
            Strategy::Hash => "h",
        }
    }

    fn tag(self) -> u8 {
        match self {
            Strategy::Range => 0,
            Strategy::List => 1,
            Strategy::Hash => 2,
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Strategy::Range),
            1 => Some(Strategy::List),
            2 => Some(Strategy::Hash),
            _ => None,
        }
    }
}

/// One partition-key column of a partitioned parent.
///
/// Only plain column references are stored: an expression key is refused at
/// `CREATE TABLE` time (see [`key_columns`]), because routing a row through an
/// arbitrary expression would need the expression's result type to coerce the
/// stored bounds against, and getting that wrong routes rows to the wrong leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyColumn {
    /// Zero-based ordinal of the column in the parent's column list.
    pub ordinal: usize,
    pub name: String,
}

/// A partitioned parent's key definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scheme {
    pub strategy: Strategy,
    pub keys: Vec<KeyColumn>,
}

/// One value of a range partition's `FROM`/`TO` tuple.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RangeDatum {
    MinValue,
    Value(Datum),
    MaxValue,
}

/// A leaf partition's stored bound, with every value already coerced to its
/// partition-key column's type.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Bound {
    Default,
    List(Vec<Datum>),
    Range {
        from: Vec<RangeDatum>,
        to: Vec<RangeDatum>,
    },
    Hash {
        modulus: i64,
        remainder: i64,
    },
}

/// A leaf partition: its relation name and its bound.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Partition {
    pub name: RelationName,
    pub bound: Bound,
}

// ── Catalog keys ─────────────────────────────────────────────────────────────

/// Append a relation to a partition key as two length-prefixed parts.
///
/// A relation is never flattened into one string here for the reason
/// [`RelationName`] gives: `PostgreSQL` lets `"a.b"` in `public` and `b` in
/// schema `a` be different relations, so a partition of one must not be found
/// under the other. The length prefixes also make the parent → child index
/// recoverable — [`partitions_of`] reads the child back out of the key suffix
/// exactly, instead of splitting on a byte the names were assumed not to hold.
fn push_relation(key: &mut Vec<u8>, relation: &RelationName) {
    push_key_part(key, &relation.schema);
    push_key_part(key, &relation.name);
}

fn scheme_key(parent: &RelationName) -> Vec<u8> {
    let mut key = SCHEME_PREFIX.to_vec();
    push_relation(&mut key, parent);
    key
}

fn child_key(child: &RelationName) -> Vec<u8> {
    let mut key = CHILD_PREFIX.to_vec();
    push_relation(&mut key, child);
    key
}

fn children_prefix(parent: &RelationName) -> Vec<u8> {
    let mut key = CHILDREN_PREFIX.to_vec();
    push_relation(&mut key, parent);
    key
}

fn children_key(parent: &RelationName, child: &RelationName) -> Vec<u8> {
    let mut key = children_prefix(parent);
    push_relation(&mut key, child);
    key
}

/// Recover the relation a key suffix written by [`push_relation`] names.
///
/// A suffix that is not exactly two length-prefixed parts belongs to no
/// relation, so it is rejected structurally rather than guessed at.
fn relation_from_key_suffix(suffix: &[u8]) -> Result<RelationName, ExecError> {
    match key_parts(suffix, 2).as_deref() {
        Some([schema, name]) => Ok(RelationName::new(*schema, *name)),
        _ => Err(corrupt("partition index key does not name a relation")),
    }
}

// ── Serialization ────────────────────────────────────────────────────────────

fn write_str(out: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).expect("a catalog name fits in u32 bytes");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], ExecError> {
    if cur.len() < n {
        return Err(corrupt("partition metadata is truncated"));
    }
    let (head, rest) = cur.split_at(n);
    *cur = rest;
    Ok(head)
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, ExecError> {
    Ok(take_n(cur, 1)?[0])
}

fn take_u32(cur: &mut &[u8]) -> Result<u32, ExecError> {
    Ok(u32::from_be_bytes(
        take_n(cur, 4)?.try_into().expect("four bytes"),
    ))
}

fn take_i64(cur: &mut &[u8]) -> Result<i64, ExecError> {
    Ok(i64::from_be_bytes(
        take_n(cur, 8)?.try_into().expect("eight bytes"),
    ))
}

fn read_string(cur: &mut &[u8]) -> Result<String, ExecError> {
    let len = usize::try_from(take_u32(cur)?).expect("u32 fits usize on supported targets");
    let bytes = take_n(cur, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| corrupt("partition metadata name is not UTF-8"))
}

fn corrupt(message: &str) -> ExecError {
    ExecError::Kv(crabka_pgkv::KvError::CorruptRow(message.to_string()))
}

fn serialize_scheme(scheme: &Scheme) -> Vec<u8> {
    let mut out = vec![SCHEME_VERSION, scheme.strategy.tag()];
    let count = u32::try_from(scheme.keys.len()).expect("a partition key has few columns");
    out.extend_from_slice(&count.to_be_bytes());
    for key in &scheme.keys {
        let ordinal = u32::try_from(key.ordinal).expect("a column ordinal fits in u32");
        out.extend_from_slice(&ordinal.to_be_bytes());
        write_str(&mut out, &key.name);
    }
    out
}

fn deserialize_scheme(bytes: &[u8]) -> Result<Scheme, ExecError> {
    let mut cur = bytes;
    if take_u8(&mut cur)? != SCHEME_VERSION {
        return Err(corrupt("unsupported partition scheme version"));
    }
    let strategy = Strategy::from_tag(take_u8(&mut cur)?)
        .ok_or_else(|| corrupt("unknown partition strategy tag"))?;
    let count = usize::try_from(take_u32(&mut cur)?).expect("u32 fits usize on supported targets");
    let mut keys = Vec::with_capacity(count.min(32));
    for _ in 0..count {
        let ordinal =
            usize::try_from(take_u32(&mut cur)?).expect("u32 fits usize on supported targets");
        keys.push(KeyColumn {
            ordinal,
            name: read_string(&mut cur)?,
        });
    }
    Ok(Scheme { strategy, keys })
}

/// Datum lists ride the storage row encoding, which covers the whole `Datum`
/// space — including the date/time types a range partition is usually keyed on.
fn write_datums(out: &mut Vec<u8>, values: &[Datum]) {
    let encoded = crabka_pgkv::rowenc::encode_row(values);
    let len = u32::try_from(encoded.len()).expect("an encoded bound fits in u32 bytes");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&encoded);
}

fn read_datums(cur: &mut &[u8]) -> Result<Vec<Datum>, ExecError> {
    let len = usize::try_from(take_u32(cur)?).expect("u32 fits usize on supported targets");
    let bytes = take_n(cur, len)?;
    crabka_pgkv::rowenc::decode_row(bytes).map_err(ExecError::Kv)
}

/// A range tuple is stored as its infinity tags plus the row encoding of the
/// finite values, so `MINVALUE`/`MAXVALUE` need no placeholder datum.
fn write_range_side(out: &mut Vec<u8>, side: &[RangeDatum]) {
    let count = u32::try_from(side.len()).expect("a range bound has few columns");
    out.extend_from_slice(&count.to_be_bytes());
    let mut finite = Vec::new();
    for value in side {
        match value {
            RangeDatum::MinValue => out.push(0),
            RangeDatum::Value(datum) => {
                out.push(1);
                finite.push(datum.clone());
            }
            RangeDatum::MaxValue => out.push(2),
        }
    }
    write_datums(out, &finite);
}

fn read_range_side(cur: &mut &[u8]) -> Result<Vec<RangeDatum>, ExecError> {
    let count = usize::try_from(take_u32(cur)?).expect("u32 fits usize on supported targets");
    let mut tags = Vec::with_capacity(count.min(32));
    for _ in 0..count {
        tags.push(take_u8(cur)?);
    }
    let finite = read_datums(cur)?;
    let mut finite = finite.into_iter();
    tags.into_iter()
        .map(|tag| match tag {
            0 => Ok(RangeDatum::MinValue),
            1 => finite
                .next()
                .map(RangeDatum::Value)
                .ok_or_else(|| corrupt("range bound is missing a finite value")),
            2 => Ok(RangeDatum::MaxValue),
            _ => Err(corrupt("unknown range bound tag")),
        })
        .collect()
}

fn serialize_child(parent: &RelationName, bound: &Bound) -> Vec<u8> {
    let mut out = vec![BOUND_VERSION];
    write_str(&mut out, &parent.schema);
    write_str(&mut out, &parent.name);
    match bound {
        Bound::Default => out.push(0),
        Bound::List(values) => {
            out.push(1);
            write_datums(&mut out, values);
        }
        Bound::Range { from, to } => {
            out.push(2);
            write_range_side(&mut out, from);
            write_range_side(&mut out, to);
        }
        Bound::Hash { modulus, remainder } => {
            out.push(3);
            out.extend_from_slice(&modulus.to_be_bytes());
            out.extend_from_slice(&remainder.to_be_bytes());
        }
    }
    out
}

fn deserialize_child(bytes: &[u8]) -> Result<(RelationName, Bound), ExecError> {
    let mut cur = bytes;
    if take_u8(&mut cur)? != BOUND_VERSION {
        return Err(corrupt("unsupported partition bound version"));
    }
    let parent = RelationName::new(read_string(&mut cur)?, read_string(&mut cur)?);
    let bound = match take_u8(&mut cur)? {
        0 => Bound::Default,
        1 => Bound::List(read_datums(&mut cur)?),
        2 => Bound::Range {
            from: read_range_side(&mut cur)?,
            to: read_range_side(&mut cur)?,
        },
        3 => Bound::Hash {
            modulus: take_i64(&mut cur)?,
            remainder: take_i64(&mut cur)?,
        },
        _ => return Err(corrupt("unknown partition bound tag")),
    };
    Ok((parent, bound))
}

// ── Catalog reads ────────────────────────────────────────────────────────────

/// The partition key of `name`, or `None` when `name` is not a partitioned
/// parent.
pub(crate) fn scheme_of(kv: &dyn Kv, name: &RelationName) -> Result<Option<Scheme>, ExecError> {
    match kv.get(&scheme_key(name)).map_err(ExecError::Kv)? {
        Some(bytes) => Ok(Some(deserialize_scheme(&bytes)?)),
        None => Ok(None),
    }
}

/// True when `name` is a partitioned parent.
pub(crate) fn is_partitioned(kv: &dyn Kv, name: &RelationName) -> Result<bool, ExecError> {
    Ok(kv.get(&scheme_key(name)).map_err(ExecError::Kv)?.is_some())
}

/// `(parent, bound)` when `name` is a partition, `None` otherwise.
pub(crate) fn parent_of(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<Option<(RelationName, Bound)>, ExecError> {
    match kv.get(&child_key(name)).map_err(ExecError::Kv)? {
        Some(bytes) => Ok(Some(deserialize_child(&bytes)?)),
        None => Ok(None),
    }
}

/// Every direct partition of `parent`, in catalog-name order.
pub(crate) fn partitions_of(
    kv: &dyn Kv,
    parent: &RelationName,
) -> Result<Vec<Partition>, ExecError> {
    let prefix = children_prefix(parent);
    let mut names = kv
        .scan_prefix(&prefix)
        .map_err(ExecError::Kv)?
        .into_iter()
        .map(|(key, _)| relation_from_key_suffix(&key[prefix.len()..]))
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    names
        .into_iter()
        .map(|name| {
            let (_, bound) = parent_of(kv, &name)?
                .ok_or_else(|| corrupt("partition index names a relation with no bound"))?;
            Ok(Partition { name, bound })
        })
        .collect()
}

/// Every partition of `parent`, and every partition of those, depth first — the
/// set of relations that actually store `parent`'s rows plus the intermediate
/// parents in between.
pub(crate) fn descendants(
    kv: &dyn Kv,
    parent: &RelationName,
) -> Result<Vec<RelationName>, ExecError> {
    let mut out = Vec::new();
    // A visited set, not just a queue: a cycle in the partition metadata would
    // otherwise spin this walk forever while pushing to `out`, which burns a
    // core and allocates until the process is killed. `ATTACH PARTITION`
    // rejects the cycles it can see, but this walk is on the DROP path and must
    // terminate on any metadata it is handed, however that metadata got there.
    let mut seen: std::collections::HashSet<RelationName> =
        std::iter::once(parent.clone()).collect();
    let mut queue: Vec<RelationName> = partitions_of(kv, parent)?
        .into_iter()
        .map(|partition| partition.name)
        .collect();
    while let Some(name) = queue.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        queue.extend(
            partitions_of(kv, &name)?
                .into_iter()
                .map(|partition| partition.name),
        );
        out.push(name);
    }
    out.sort();
    Ok(out)
}

/// The relations that actually hold `parent`'s rows: every descendant that is
/// not itself a partitioned parent, in catalog-name order.
pub(crate) fn leaves_of(
    kv: &dyn Kv,
    parent: &RelationName,
) -> Result<Vec<RelationName>, ExecError> {
    let mut leaves = Vec::new();
    for name in descendants(kv, parent)? {
        if !is_partitioned(kv, &name)? {
            leaves.push(name);
        }
    }
    Ok(leaves)
}

// ── Catalog writes ───────────────────────────────────────────────────────────

/// Write ops recording `parent` as partitioned by `scheme`.
pub(crate) fn put_scheme_ops(parent: &RelationName, scheme: &Scheme) -> Vec<WriteOp> {
    vec![WriteOp::Put {
        key: scheme_key(parent),
        value: serialize_scheme(scheme),
    }]
}

/// Write ops attaching `child` to `parent` with `bound`.
pub(crate) fn attach_ops(
    parent: &RelationName,
    child: &RelationName,
    bound: &Bound,
) -> Vec<WriteOp> {
    vec![
        WriteOp::Put {
            key: child_key(child),
            value: serialize_child(parent, bound),
        },
        WriteOp::Put {
            key: children_key(parent, child),
            value: Vec::new(),
        },
    ]
}

/// Write ops detaching `child` from `parent`.
pub(crate) fn detach_ops(parent: &RelationName, child: &RelationName) -> Vec<WriteOp> {
    vec![
        WriteOp::Delete {
            key: child_key(child),
        },
        WriteOp::Delete {
            key: children_key(parent, child),
        },
    ]
}

/// Write ops removing `name`'s own partition metadata — its key definition if
/// it is a parent, and its parent link if it is a partition.
pub(crate) fn drop_metadata_ops(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<Vec<WriteOp>, ExecError> {
    let mut ops = vec![WriteOp::Delete {
        key: scheme_key(name),
    }];
    if let Some((parent, _)) = parent_of(kv, name)? {
        ops.extend(detach_ops(&parent, name));
    }
    Ok(ops)
}

// ── Routing ──────────────────────────────────────────────────────────────────

/// Extract a row's partition key values.
fn key_values(scheme: &Scheme, row: &[Datum]) -> Result<Vec<Datum>, ExecError> {
    scheme
        .keys
        .iter()
        .map(|key| {
            row.get(key.ordinal)
                .cloned()
                .ok_or_else(|| corrupt("partition key column ordinal is past the end of the row"))
        })
        .collect()
}

/// Compare two partition-key datums. `None` means the comparison had no answer
/// — either operand was NULL, or the two types do not compare — and every
/// caller treats that as "does not belong here" rather than guessing.
fn compare(left: &Datum, right: &Datum) -> Option<Ordering> {
    crabka_pgtypes::ops::compare(left, right).ok().flatten()
}

/// Does `key` fall inside `bound`? `None` means "not decidable", which is
/// treated as "no" by [`route`] and reported as a routing failure rather than
/// guessed at.
fn contains(scheme: &Scheme, bound: &Bound, key: &[Datum]) -> Result<bool, ExecError> {
    match bound {
        // The default partition takes whatever no other partition took; `route`
        // never asks it directly.
        Bound::Default => Ok(false),
        Bound::List(values) => {
            let Some(value) = key.first() else {
                return Ok(false);
            };
            for candidate in values {
                // A list bound stores NULL as a bound value of its own, and NULL
                // matches only that bound — `IS NOT DISTINCT FROM`, not `=`.
                let matches = match (value, candidate) {
                    (Datum::Null, Datum::Null) => true,
                    (Datum::Null, _) | (_, Datum::Null) => false,
                    _ => compare(value, candidate) == Some(Ordering::Equal),
                };
                if matches {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Bound::Range { from, to } => {
            // PostgreSQL treats a NULL partition-key value as unroutable in a
            // range-partitioned table: it belongs to the DEFAULT partition, or
            // to none.
            if key.iter().any(|value| matches!(value, Datum::Null)) {
                return Ok(false);
            }
            Ok(compare_range_tuple(key, from)? != Ordering::Less
                && compare_range_tuple(key, to)? == Ordering::Less)
        }
        Bound::Hash { modulus, remainder } => {
            let modulus = u64::try_from(*modulus)
                .map_err(|_| corrupt("stored hash partition modulus is negative"))?;
            let remainder = u64::try_from(*remainder)
                .map_err(|_| corrupt("stored hash partition remainder is negative"))?;
            let _ = scheme;
            Ok(self::hash::partition_hash(key)? % modulus == remainder)
        }
    }
}

/// Compare a row's key tuple against one side of a range bound, using
/// `PostgreSQL`'s rule that the first differing column decides and an infinite
/// bound decides immediately.
fn compare_range_tuple(key: &[Datum], bound: &[RangeDatum]) -> Result<Ordering, ExecError> {
    for (index, limit) in bound.iter().enumerate() {
        match limit {
            RangeDatum::MinValue => return Ok(Ordering::Greater),
            RangeDatum::MaxValue => return Ok(Ordering::Less),
            RangeDatum::Value(value) => {
                let Some(actual) = key.get(index) else {
                    return Err(corrupt("range bound is wider than the partition key"));
                };
                let ordering = compare(actual, value).ok_or_else(|| {
                    corrupt("partition key value is not comparable with its stored bound")
                })?;
                if ordering != Ordering::Equal {
                    return Ok(ordering);
                }
            }
        }
    }
    Ok(Ordering::Equal)
}

/// The partition of `parent` that `row` belongs to, or `None` when no partition
/// accepts it.
///
/// `partitions` is the full direct-partition list; the default partition is
/// only chosen once every other bound has declined the row, exactly as
/// `PostgreSQL` does.
pub(crate) fn route<'a>(
    scheme: &Scheme,
    partitions: &'a [Partition],
    row: &[Datum],
) -> Result<Option<&'a Partition>, ExecError> {
    let key = key_values(scheme, row)?;
    for partition in partitions {
        if contains(scheme, &partition.bound, &key)? {
            return Ok(Some(partition));
        }
    }
    Ok(partitions
        .iter()
        .find(|partition| matches!(partition.bound, Bound::Default)))
}

/// Does `row` satisfy `bound` under `scheme`? This is `PostgreSQL`'s implicit
/// per-partition `CHECK`, applied when a row is written straight into a leaf.
///
/// A `DEFAULT` partition accepts a row exactly when no sibling accepts it, so
/// the sibling bounds have to be supplied too.
pub(crate) fn satisfies(
    scheme: &Scheme,
    bound: &Bound,
    siblings: &[Partition],
    row: &[Datum],
) -> Result<bool, ExecError> {
    let key = key_values(scheme, row)?;
    if matches!(bound, Bound::Default) {
        for sibling in siblings {
            if contains(scheme, &sibling.bound, &key)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    contains(scheme, bound, &key)
}

// ── Bound validation ─────────────────────────────────────────────────────────

/// Reject a bound whose spelling does not match the parent's strategy, using
/// `PostgreSQL`'s 42P16 wording.
pub(crate) fn check_bound_shape(strategy: Strategy, bound: &Bound) -> Result<(), ExecError> {
    let matches = matches!(
        (strategy, bound),
        (_, Bound::Default)
            | (Strategy::List, Bound::List(_))
            | (Strategy::Range, Bound::Range { .. })
            | (Strategy::Hash, Bound::Hash { .. })
    );
    if !matches {
        return Err(ExecError::InvalidTableDefinition(format!(
            "invalid bound specification for a {} partition",
            strategy.name()
        )));
    }
    if strategy == Strategy::Hash && matches!(bound, Bound::Default) {
        return Err(ExecError::InvalidTableDefinition(
            "a hash-partitioned table may not have a default partition".into(),
        ));
    }
    Ok(())
}

/// Reject a range bound whose lower limit is not below its upper limit.
///
/// `PostgreSQL` names the partition with `RelationGetRelationName`, so the
/// message carries the bare name even for a partition outside `public`.
pub(crate) fn check_range_not_empty(
    partition: &RelationName,
    bound: &Bound,
) -> Result<(), ExecError> {
    let Bound::Range { from, to } = bound else {
        return Ok(());
    };
    if range_side_ordering(from, to)? == Ordering::Less {
        return Ok(());
    }
    Err(ExecError::InvalidObjectDefinition(format!(
        "empty range bound specified for partition \"{}\"",
        partition.name
    )))
}

/// Order one range tuple against another, comparing column by column with
/// `MINVALUE`/`MAXVALUE` as the extremes.
fn range_side_ordering(left: &[RangeDatum], right: &[RangeDatum]) -> Result<Ordering, ExecError> {
    for (left, right) in left.iter().zip(right.iter()) {
        let ordering = match (left, right) {
            (RangeDatum::MinValue, RangeDatum::MinValue)
            | (RangeDatum::MaxValue, RangeDatum::MaxValue) => Ordering::Equal,
            (RangeDatum::MinValue, _) | (_, RangeDatum::MaxValue) => Ordering::Less,
            (RangeDatum::MaxValue, _) | (_, RangeDatum::MinValue) => Ordering::Greater,
            (RangeDatum::Value(left), RangeDatum::Value(right)) => compare(left, right)
                .ok_or_else(|| corrupt("range bound values are not comparable"))?,
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

/// Reject a bound that overlaps an existing sibling, or a second `DEFAULT`,
/// using `PostgreSQL`'s 42P17 wording — which names both partitions with
/// `RelationGetRelationName`, so neither carries its schema.
pub(crate) fn check_no_overlap(
    strategy: Strategy,
    partition: &RelationName,
    bound: &Bound,
    siblings: &[Partition],
) -> Result<(), ExecError> {
    let partition = &partition.name;
    for sibling in siblings {
        if let Some(reason) = overlap_reason(strategy, bound, &sibling.bound)? {
            return Err(match reason {
                Overlap::Default => ExecError::InvalidObjectDefinition(format!(
                    "partition \"{partition}\" conflicts with existing default partition \"{}\"",
                    sibling.name.name
                )),
                Overlap::Bounds => ExecError::InvalidObjectDefinition(format!(
                    "partition \"{partition}\" would overlap partition \"{}\"",
                    sibling.name.name
                )),
                Overlap::HashModulus => ExecError::InvalidObjectDefinition(
                    "every hash partition modulus must be a factor of the next larger modulus"
                        .into(),
                ),
            });
        }
    }
    Ok(())
}

enum Overlap {
    Default,
    Bounds,
    HashModulus,
}

fn overlap_reason(
    strategy: Strategy,
    new: &Bound,
    existing: &Bound,
) -> Result<Option<Overlap>, ExecError> {
    match (new, existing) {
        (Bound::Default, Bound::Default) => Ok(Some(Overlap::Default)),
        (Bound::Default, _) | (_, Bound::Default) => Ok(None),
        (Bound::List(new), Bound::List(existing)) => {
            for value in new {
                for other in existing {
                    let same = match (value, other) {
                        (Datum::Null, Datum::Null) => true,
                        (Datum::Null, _) | (_, Datum::Null) => false,
                        _ => compare(value, other) == Some(Ordering::Equal),
                    };
                    if same {
                        return Ok(Some(Overlap::Bounds));
                    }
                }
            }
            Ok(None)
        }
        (
            Bound::Range { from, to },
            Bound::Range {
                from: other_from,
                to: other_to,
            },
        ) => {
            let disjoint = range_side_ordering(to, other_from)? != Ordering::Greater
                || range_side_ordering(other_to, from)? != Ordering::Greater;
            Ok((!disjoint).then_some(Overlap::Bounds))
        }
        (
            Bound::Hash { modulus, remainder },
            Bound::Hash {
                modulus: other_modulus,
                remainder: other_remainder,
            },
        ) => {
            // PostgreSQL requires the moduli to form a divisibility chain before
            // it will even ask about overlap, and reports the chain break first.
            let (larger, smaller) = if modulus >= other_modulus {
                (*modulus, *other_modulus)
            } else {
                (*other_modulus, *modulus)
            };
            if smaller == 0 || larger % smaller != 0 {
                return Ok(Some(Overlap::HashModulus));
            }
            let overlaps = remainder.rem_euclid(smaller) == other_remainder.rem_euclid(smaller);
            Ok(overlaps.then_some(Overlap::Bounds))
        }
        // A shape mismatch is caught by `check_bound_shape` before this runs.
        _ => {
            let _ = strategy;
            Ok(None)
        }
    }
}

/// Reject a hash bound whose modulus/remainder pair `PostgreSQL` rejects.
pub(crate) fn check_hash_bound(bound: &Bound) -> Result<(), ExecError> {
    let Bound::Hash { modulus, remainder } = bound else {
        return Ok(());
    };
    if *modulus <= 0 {
        return Err(ExecError::InvalidTableDefinition(
            "modulus for hash partition must be an integer value greater than zero".into(),
        ));
    }
    if *remainder >= *modulus {
        return Err(ExecError::InvalidTableDefinition(
            "remainder for hash partition must be less than modulus".into(),
        ));
    }
    Ok(())
}

// ── Definition-time rules ────────────────────────────────────────────────────

/// `SHARDED` and `PARTITION BY` both claim ownership of how a relation's rows
/// are distributed, and the two routing rules cannot both hold. Declaring both
/// is refused rather than silently letting one win.
pub(crate) fn reject_sharded_partitioned() -> ExecError {
    ExecError::Unsupported(
        "a SHARDED table cannot also be partitioned: sharding and declarative partitioning are \
         two different row-placement rules, and only one can decide where a row lives"
            .into(),
    )
}

/// Resolve a `PARTITION BY` key list against the parent's columns.
///
/// Every rule `PostgreSQL` enforces at parse-analysis time is applied here so
/// the SQLSTATEs match: a missing column is 42703, a system column or a
/// constant expression is 42P17, and `LIST` with more than one column is 42P17.
pub(crate) fn key_columns(
    strategy: Strategy,
    keys: &[crabka_pgparser::ast::PartitionKeyElem],
    columns: &[crabka_pgcatalog::Column],
) -> Result<Vec<KeyColumn>, ExecError> {
    if strategy == Strategy::List && keys.len() > 1 {
        return Err(ExecError::InvalidObjectDefinition(
            "cannot use \"list\" partition strategy with more than one column".into(),
        ));
    }
    keys.iter()
        .map(|key| {
            let Some(name) = key.column.as_deref() else {
                return Err(expression_key_error(&key.text));
            };
            if is_system_column(name) {
                return Err(ExecError::InvalidObjectDefinition(format!(
                    "cannot use system column \"{name}\" in partition key"
                )));
            }
            let ordinal = columns
                .iter()
                .position(|column| column.name == name)
                .ok_or_else(|| ExecError::UndefinedPartitionKeyColumn(name.to_string()))?;
            Ok(KeyColumn {
                ordinal,
                name: name.to_string(),
            })
        })
        .collect()
}

/// `PostgreSQL`'s system columns, which may not appear in a partition key.
fn is_system_column(name: &str) -> bool {
    matches!(
        name,
        "xmin" | "xmax" | "cmin" | "cmax" | "ctid" | "tableoid"
    )
}

/// The refusal for an expression partition key.
///
/// A constant expression is `PostgreSQL`'s own 42P17; every other expression is
/// a documented 0A000 rather than an approximation, because routing through an
/// expression needs the expression's result type to coerce the stored bounds
/// against and a wrong coercion puts rows in the wrong leaf.
fn expression_key_error(text: &str) -> ExecError {
    ExecError::Unsupported(format!(
        "expression partition keys are not supported: PARTITION BY … ({text}) needs the \
         expression's result type to coerce partition bounds against, and routing a row through \
         the wrong type would place it in the wrong partition"
    ))
}

/// Type of the `ordinal`-th column, for coercing a written bound value.
pub(crate) fn key_column_type(
    columns: &[crabka_pgcatalog::Column],
    key: &KeyColumn,
) -> Result<ColumnType, ExecError> {
    columns
        .get(key.ordinal)
        .map(|column| column.ty)
        .ok_or_else(|| corrupt("partition key names a column the relation does not have"))
}

#[cfg(test)]
mod tests {

    /// A cycle in the partition metadata must not make the tree walk diverge.
    ///
    /// `ATTACH PARTITION` refuses the cycles it can see, but this walk is on the
    /// `DROP` path and has to terminate on whatever metadata it is handed. When
    /// it did not, a single `DROP TABLE` spun a core and allocated until the
    /// process was killed — which read as 10,135 corpus statements failing to
    /// connect rather than as one bad statement.
    #[test]
    fn a_cycle_in_the_partition_tree_does_not_diverge() {
        use assert2::assert;

        let kv = crabka_pgkv::MemKv::default();
        // Hand-write a two-node cycle: each table is recorded as the other's
        // partition, which `ATTACH PARTITION` rejects but a direct catalog write
        // could still produce.
        let bound = Bound::Hash {
            modulus: 1,
            remainder: 0,
        };
        for (parent, child) in [("a", "b"), ("b", "a")] {
            for op in attach_ops(
                &RelationName::public(parent),
                &RelationName::public(child),
                &bound,
            ) {
                if let WriteOp::Put { key, value } = op {
                    kv.put(key, value).expect("put");
                }
            }
        }

        let found = descendants(&kv, &RelationName::public("a")).expect("the walk terminates");

        assert!(found == vec![RelationName::public("b")], "got {found:?}");
    }
    use assert2::assert;
    use crabka_pgtypes::Datum;

    use super::*;

    fn hash_bound() -> Bound {
        Bound::Hash {
            modulus: 1,
            remainder: 0,
        }
    }

    fn write(kv: &crabka_pgkv::MemKv, ops: Vec<WriteOp>) {
        kv.write_batch(&ops).expect("write");
    }

    /// The relation catalog is read by scanning its prefix, so a partition
    /// record stored under it would be handed to the relation decoder — and
    /// `DROP SCHEMA … CASCADE`, which lists a schema's contents that way, would
    /// try to drop it as a relation.
    #[test]
    fn partition_metadata_is_stored_outside_the_relation_catalog() {
        let parent = RelationName::new("sch", "p");
        let child = RelationName::new("sch", "c");
        let catalog = crabka_pgkv::key::catalog_prefix();
        for key in [
            scheme_key(&parent),
            child_key(&child),
            children_key(&parent, &child),
        ] {
            assert!(!key.starts_with(&catalog), "{key:?}");
        }
    }

    /// Both halves of every partition key are length-prefixed, so one parent's
    /// child index cannot be read as another's — even when one relation's name
    /// begins with the other's, which a plain concatenation would confuse.
    #[test]
    fn the_children_index_returns_one_parents_partitions_only() {
        let kv = crabka_pgkv::MemKv::default();
        let parent = RelationName::new("sch", "p");
        let neighbour = RelationName::new("sch", "p2");
        // A dot in the name is not a qualifier: this leaf is `c.1` in `sch`.
        let child = RelationName::new("sch", "c.1");
        write(&kv, attach_ops(&parent, &child, &hash_bound()));
        write(
            &kv,
            attach_ops(&neighbour, &RelationName::new("sch", "n"), &hash_bound()),
        );

        assert!(
            partitions_of(&kv, &parent).expect("scan")
                == vec![Partition {
                    name: child.clone(),
                    bound: hash_bound(),
                }]
        );
        assert!(parent_of(&kv, &child).expect("link") == Some((parent, hash_bound())));
        // A relation in another schema of the same name is a different leaf.
        assert!(
            parent_of(&kv, &RelationName::new("other", "c.1"))
                .expect("link")
                .is_none()
        );
    }

    fn list_scheme() -> Scheme {
        Scheme {
            strategy: Strategy::List,
            keys: vec![KeyColumn {
                ordinal: 0,
                name: "a".into(),
            }],
        }
    }

    fn range_scheme() -> Scheme {
        Scheme {
            strategy: Strategy::Range,
            keys: vec![KeyColumn {
                ordinal: 0,
                name: "a".into(),
            }],
        }
    }

    fn value(n: i32) -> RangeDatum {
        RangeDatum::Value(Datum::Int4(n))
    }

    #[test]
    fn list_routing_prefers_a_matching_bound_over_the_default() {
        let scheme = list_scheme();
        let partitions = vec![
            Partition {
                name: RelationName::public("p_default"),
                bound: Bound::Default,
            },
            Partition {
                name: RelationName::public("p_one"),
                bound: Bound::List(vec![Datum::Int4(1), Datum::Int4(2)]),
            },
        ];
        for (input, expected) in [
            (Datum::Int4(1), Some("p_one")),
            (Datum::Int4(2), Some("p_one")),
            (Datum::Int4(9), Some("p_default")),
            (Datum::Null, Some("p_default")),
        ] {
            let routed =
                route(&scheme, &partitions, std::slice::from_ref(&input)).expect("routing decides");
            assert!(routed.map(|partition| partition.name.name.as_str()) == expected);
        }
    }

    #[test]
    fn a_list_bound_of_null_takes_the_null_key() {
        let scheme = list_scheme();
        let partitions = vec![Partition {
            name: RelationName::public("p_null"),
            bound: Bound::List(vec![Datum::Null]),
        }];
        let routed = route(&scheme, &partitions, &[Datum::Null]).expect("routing decides");
        assert!(routed.map(|partition| partition.name.name.as_str()) == Some("p_null"));
        let routed = route(&scheme, &partitions, &[Datum::Int4(1)]).expect("routing decides");
        assert!(routed.is_none());
    }

    #[test]
    fn range_routing_is_lower_inclusive_and_upper_exclusive() {
        let scheme = range_scheme();
        let partitions = vec![
            Partition {
                name: RelationName::public("p_low"),
                bound: Bound::Range {
                    from: vec![RangeDatum::MinValue],
                    to: vec![value(10)],
                },
            },
            Partition {
                name: RelationName::public("p_high"),
                bound: Bound::Range {
                    from: vec![value(10)],
                    to: vec![RangeDatum::MaxValue],
                },
            },
        ];
        for (input, expected) in [
            (i32::MIN, Some("p_low")),
            (9, Some("p_low")),
            (10, Some("p_high")),
            (i32::MAX, Some("p_high")),
        ] {
            let routed =
                route(&scheme, &partitions, &[Datum::Int4(input)]).expect("routing decides");
            assert!(routed.map(|partition| partition.name.name.as_str()) == expected);
        }
        // A NULL key belongs to no range partition.
        let routed = route(&scheme, &partitions, &[Datum::Null]).expect("routing decides");
        assert!(routed.is_none());
    }

    #[test]
    fn overlapping_and_empty_range_bounds_are_refused() {
        let existing = vec![Partition {
            name: RelationName::public("p_one"),
            bound: Bound::Range {
                from: vec![value(10)],
                to: vec![value(20)],
            },
        }];
        let overlapping = Bound::Range {
            from: vec![value(15)],
            to: vec![value(25)],
        };
        let adjacent = Bound::Range {
            from: vec![value(20)],
            to: vec![value(25)],
        };
        assert!(
            check_no_overlap(
                Strategy::Range,
                &RelationName::public("p_new"),
                &overlapping,
                &existing
            )
            .is_err()
        );
        assert!(
            check_no_overlap(
                Strategy::Range,
                &RelationName::public("p_new"),
                &adjacent,
                &existing
            )
            .is_ok()
        );
        let empty = Bound::Range {
            from: vec![value(1)],
            to: vec![value(1)],
        };
        assert!(check_range_not_empty(&RelationName::public("p_new"), &empty).is_err());
        assert!(check_range_not_empty(&RelationName::public("p_new"), &adjacent).is_ok());
    }

    #[test]
    fn a_second_default_partition_conflicts_with_the_first() {
        let existing = vec![Partition {
            name: RelationName::public("p_default"),
            bound: Bound::Default,
        }];
        assert!(
            check_no_overlap(
                Strategy::List,
                &RelationName::public("p_other"),
                &Bound::Default,
                &existing
            )
            .is_err()
        );
        assert!(
            check_no_overlap(
                Strategy::List,
                &RelationName::public("p_other"),
                &Bound::List(vec![Datum::Int4(1)]),
                &existing,
            )
            .is_ok()
        );
    }

    #[test]
    fn hash_bounds_require_a_divisibility_chain_and_a_free_remainder() {
        let existing = vec![Partition {
            name: RelationName::public("p_zero"),
            bound: Bound::Hash {
                modulus: 4,
                remainder: 0,
            },
        }];
        for (modulus, remainder, ok) in [
            (4, 1, true),
            (8, 1, true),
            (8, 4, false),
            (6, 1, false),
            (4, 0, false),
        ] {
            let bound = Bound::Hash { modulus, remainder };
            let checked = check_no_overlap(
                Strategy::Hash,
                &RelationName::public("p_new"),
                &bound,
                &existing,
            );
            assert!(
                checked.is_ok() == ok,
                "modulus {modulus} remainder {remainder}"
            );
        }
    }

    #[test]
    fn hash_bound_values_follow_postgres_range_checks() {
        for (modulus, remainder, ok) in [
            (4, 0, true),
            (4, 3, true),
            (4, 4, false),
            (0, 0, false),
            (1, 0, true),
        ] {
            let checked = check_hash_bound(&Bound::Hash { modulus, remainder });
            assert!(
                checked.is_ok() == ok,
                "modulus {modulus} remainder {remainder}"
            );
        }
    }

    #[test]
    fn bound_shape_must_match_the_parents_strategy() {
        let list = Bound::List(vec![Datum::Int4(1)]);
        let range = Bound::Range {
            from: vec![value(1)],
            to: vec![value(2)],
        };
        let hash = Bound::Hash {
            modulus: 4,
            remainder: 0,
        };
        for (strategy, bound, ok) in [
            (Strategy::List, &list, true),
            (Strategy::List, &range, false),
            (Strategy::List, &hash, false),
            (Strategy::Range, &range, true),
            (Strategy::Range, &list, false),
            (Strategy::Hash, &hash, true),
            (Strategy::Hash, &list, false),
            (Strategy::List, &Bound::Default, true),
            (Strategy::Range, &Bound::Default, true),
            (Strategy::Hash, &Bound::Default, false),
        ] {
            let checked = check_bound_shape(strategy, bound);
            assert!(checked.is_ok() == ok, "{strategy:?} {bound:?}");
        }
    }

    #[test]
    fn a_default_partition_accepts_exactly_what_its_siblings_decline() {
        let scheme = list_scheme();
        let siblings = vec![Partition {
            name: RelationName::public("p_one"),
            bound: Bound::List(vec![Datum::Int4(1)]),
        }];
        assert!(
            satisfies(&scheme, &Bound::Default, &siblings, &[Datum::Int4(9)])
                .expect("default check decides")
        );
        assert!(
            !satisfies(&scheme, &Bound::Default, &siblings, &[Datum::Int4(1)])
                .expect("default check decides")
        );
    }

    #[test]
    fn scheme_and_bound_metadata_round_trip_through_their_encodings() {
        let scheme = Scheme {
            strategy: Strategy::Range,
            keys: vec![
                KeyColumn {
                    ordinal: 0,
                    name: "a".into(),
                },
                KeyColumn {
                    ordinal: 2,
                    name: "c".into(),
                },
            ],
        };
        let encoded = serialize_scheme(&scheme);
        assert!(deserialize_scheme(&encoded).expect("scheme decodes") == scheme);

        for bound in [
            Bound::Default,
            Bound::List(vec![Datum::Int4(1), Datum::Null, Datum::Text("x".into())]),
            Bound::Range {
                from: vec![RangeDatum::MinValue, value(3)],
                to: vec![value(7), RangeDatum::MaxValue],
            },
            Bound::Hash {
                modulus: 8,
                remainder: 3,
            },
        ] {
            let parent = RelationName::new("sch", "parent");
            let encoded = serialize_child(&parent, &bound);
            let decoded = deserialize_child(&encoded).expect("bound decodes");
            assert!(decoded == (parent, bound));
        }
    }
}
