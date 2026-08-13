//! Declarative partitioning: catalog metadata, bound validation, overlap
//! detection, and row routing.
//!
//! A partitioned parent is a catalog relation that owns no rows. Every row
//! lives in exactly one leaf partition. A comparison of the row's partition key
//! against each leaf's stored bound picks that leaf. This module is the single
//! place that decides which leaf a row belongs to, so the `INSERT` routing
//! path, the per-leaf constraint check, and `ATTACH PARTITION` validation
//! cannot disagree about it.
//!
//! # Relationship to native sharding
//!
//! The chapter design assigns declarative partitioning to the G-8/G-9c sharding
//! machinery. That mapping is *not* what this module does, because of the
//! program's correctness-over-coverage rule. Sharding routes on a hash of a
//! single column into a power-of-two bucket count. That cannot express a
//! `LIST` bound, a `RANGE` bound, a `DEFAULT` partition, or `PostgreSQL`'s
//! `MODULUS`/`REMAINDER` hash bucketing. A sharded relation also has a narrower
//! mutation surface, with no `PRIMARY KEY` and no `UNIQUE`.
//!
//! A partitioned table routed through the sharding machinery would answer with
//! the wrong rows for every shape but one. Partitions are therefore ordinary
//! relations linked by catalog metadata, and `SHARDED` and `PARTITION BY` are
//! mutually exclusive. See [`reject_sharded_partitioned`].

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
/// The three partition families sit *beside* the relation catalog and not
/// under it. A scan of `catalog/` answers "every stored relation", and a
/// partition record under that prefix would go to the relation decoder as if
/// it were a relation.
const SCHEME_PREFIX: &[u8] = b"\0\0\0\0catalog_partition/scheme/";
/// System-key prefix for a leaf's parent link and bound.
const CHILD_PREFIX: &[u8] = b"\0\0\0\0catalog_partition/child/";
/// System-key prefix for the parent → child index.
const CHILDREN_PREFIX: &[u8] = b"\0\0\0\0catalog_partition/children/";

/// Version 2 dropped the column ordinal each key used to carry beside its name.
/// A position is resolved from the live column list at every use instead. See
/// [`Scheme`]. A version-1 record is refused rather than read, because its
/// leading ordinal would decode as the length of a name.
const SCHEME_VERSION: u8 = 2;
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

/// A partitioned parent's key definition: the strategy, and the parent's own
/// name for each key column.
///
/// A key holds only plain column references. An expression key is refused at
/// `CREATE TABLE` time. See [`key_columns`]. A row routed through an arbitrary
/// expression would need the expression's result type to coerce the stored
/// bounds against, and a wrong result type routes rows to the wrong leaf.
///
/// # Why a name and not a position
///
/// A key column's *position* in the parent's column list is not stored, and is
/// resolved from the live column list at every use. See [`key_ordinals`].
///
/// `PostgreSQL` can store `pg_partitioned_table.partattrs` as attribute numbers
/// because an attnum is stable for the life of the relation: `DROP COLUMN`
/// leaves the attribute in place and sets `attisdropped`. Crabka instead
/// *compacts* the column list and every stored row, so a position is only
/// meaningful against one particular version of the schema. A stored position
/// silently decays into a pointer at the neighbouring column the moment
/// anything earlier is dropped — and a partition key that reads the wrong
/// column routes rows into the wrong leaf without an error. A name cannot decay
/// that way: `RENAME COLUMN` rewrites it, and `DROP COLUMN` refuses to remove a
/// column a key names at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scheme {
    pub strategy: Strategy,
    /// Key columns, in the order `PARTITION BY` wrote them.
    pub keys: Vec<String>,
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
/// recoverable. [`partitions_of`] reads the child back out of the key suffix
/// exactly, and does not split on a byte the names were assumed not to hold.
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
/// relation, so this function rejects it structurally and does not guess.
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
        write_str(&mut out, key);
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
        keys.push(read_string(&mut cur)?);
    }
    Ok(Scheme { strategy, keys })
}

/// Datum lists ride the storage row encoding, which covers the whole `Datum`
/// space. This includes the date/time types a range partition is usually keyed
/// on.
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

// ── Deparsing ────────────────────────────────────────────────────────────────

/// One bound value as `pg_get_expr(pg_class.relpartbound, …)` prints it.
///
/// A partition bound is *not* rendered like a stored default: PostgreSQL prints
/// the bare literal, without the `::type` annotation
/// [`crate::viewdef::const_text`] adds, and a NULL list bound as the word `NULL`
/// rather than `NULL::text`. The two renderings must therefore stay separate,
/// however similar they look.
fn bound_datum_text(value: &Datum) -> String {
    match value {
        Datum::Null => "NULL".to_string(),
        Datum::Bool(flag) => (if *flag { "true" } else { "false" }).to_string(),
        Datum::Int2(n) => n.to_string(),
        Datum::Int4(n) => n.to_string(),
        Datum::Int8(n) => n.to_string(),
        Datum::Float4(n) => n.to_string(),
        Datum::Float8(n) => n.to_string(),
        Datum::Numeric(n) => n.to_string(),
        other => format!(
            "'{}'",
            crate::func::text_render(other, &jiff::tz::TimeZone::UTC).replace('\'', "''")
        ),
    }
}

fn range_side_text(side: &[RangeDatum]) -> String {
    side.iter()
        .map(|value| match value {
            RangeDatum::MinValue => "MINVALUE".to_string(),
            RangeDatum::MaxValue => "MAXVALUE".to_string(),
            RangeDatum::Value(datum) => bound_datum_text(datum),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A stored bound as `pg_class.relpartbound` reports it — the very clause the
/// partition was declared with, which is what psql echoes after `Partition of:`
/// and in the `\d+` partition list.
///
/// The hash form spells its two keywords in lower case: that is PostgreSQL's
/// own output, and it does not match the upper-case spelling the grammar
/// accepts.
pub(crate) fn bound_text(bound: &Bound) -> String {
    match bound {
        Bound::Default => "DEFAULT".to_string(),
        Bound::List(values) => format!(
            "FOR VALUES IN ({})",
            values
                .iter()
                .map(bound_datum_text)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Bound::Range { from, to } => format!(
            "FOR VALUES FROM ({}) TO ({})",
            range_side_text(from),
            range_side_text(to)
        ),
        Bound::Hash { modulus, remainder } => {
            format!("FOR VALUES WITH (modulus {modulus}, remainder {remainder})")
        }
    }
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

/// Every partition of `parent`, and every partition of those, depth first. This
/// is the set of relations that actually store `parent`'s rows, plus the
/// intermediate parents between them.
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

/// Write ops that record `parent` as partitioned by `scheme`.
pub(crate) fn put_scheme_ops(parent: &RelationName, scheme: &Scheme) -> Vec<WriteOp> {
    vec![WriteOp::Put {
        key: scheme_key(parent),
        value: serialize_scheme(scheme),
    }]
}

/// Write ops that attach `child` to `parent` with `bound`.
///
/// The third op is the `pg_class.relhassubclass` latch. `PostgreSQL` sets it
/// here — on `PARTITION OF` and on `ATTACH PARTITION` alike — and
/// [`detach_ops`] deliberately does not clear it: the flag stays true until an
/// `ANALYZE` looks and finds no children, and the regression corpus reads the
/// parent in exactly that window.
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
        crate::relstats::set_has_subclass_op(parent),
    ]
}

/// Write ops that detach `child` from `parent`.
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

/// Move every partition link that names `from` onto `to`, so a rename leaves
/// the partitioning scheme, the bounds and their enforcement exactly as they
/// were — which is what `PostgreSQL` does.
///
/// All three families key on the relation name, and each one lost loses a
/// different thing. The key definition *is* the fact that the relation is a
/// partitioned parent, so leaving it behind turns the parent into an ordinary
/// heap: `relkind` drops from `p` to `r`, `pg_partitioned_table` empties, the
/// rows under the partitions stop being reachable through it, and it starts
/// accepting rows of its own that belong in no partition. The parent link
/// carries the bound, so leaving it behind takes bound enforcement off the leaf.
/// The parent → child index is what a read of the parent walks, so a leaf
/// renamed out of it makes the parent unreadable.
///
/// The two records that move keep their bytes: a key definition names columns,
/// and a leaf's record names *its* parent, neither of which a rename of the
/// relation itself changes. Only a leaf whose parent is the relation being
/// renamed is re-encoded.
///
/// Deletes are emitted before the puts that replace them, so the batch is
/// correct even where the two collide — which needs the relation to be its own
/// partition, a cycle `ATTACH PARTITION` refuses.
pub(crate) fn rename_ops(
    kv: &dyn Kv,
    from: &RelationName,
    to: &RelationName,
) -> Result<Vec<WriteOp>, ExecError> {
    let mut ops = Vec::new();
    if let Some(scheme) = kv.get(&scheme_key(from)).map_err(ExecError::Kv)? {
        ops.push(WriteOp::Delete {
            key: scheme_key(from),
        });
        ops.push(WriteOp::Put {
            key: scheme_key(to),
            value: scheme,
        });
    }
    if let Some(record) = kv.get(&child_key(from)).map_err(ExecError::Kv)? {
        let (parent, _) = deserialize_child(&record)?;
        ops.push(WriteOp::Delete {
            key: child_key(from),
        });
        ops.push(WriteOp::Delete {
            key: children_key(&parent, from),
        });
        ops.push(WriteOp::Put {
            key: child_key(to),
            value: record,
        });
        ops.push(WriteOp::Put {
            key: children_key(&parent, to),
            value: Vec::new(),
        });
    }
    for partition in partitions_of(kv, from)? {
        ops.push(WriteOp::Delete {
            key: children_key(from, &partition.name),
        });
        ops.push(WriteOp::Put {
            key: children_key(to, &partition.name),
            value: Vec::new(),
        });
        ops.push(WriteOp::Put {
            key: child_key(&partition.name),
            value: serialize_child(to, &partition.bound),
        });
    }
    Ok(ops)
}

/// Write ops that remove `name`'s own partition metadata: its key definition
/// if it is a parent, and its parent link if it is a partition.
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

/// Where each key column sits in `columns`, which must be the *parent's* column
/// list in the parent's own order. A caller holding a leaf's row permutes it
/// into parent order first; see `exec::column_mapping`.
///
/// A name that resolves to nothing is genuine catalog corruption rather than a
/// user error: `DROP COLUMN` refuses to remove a column a partition key names,
/// and `RENAME COLUMN` rewrites the key alongside the column.
pub(crate) fn key_ordinals(
    scheme: &Scheme,
    columns: &[crabka_pgcatalog::Column],
) -> Result<Vec<usize>, ExecError> {
    scheme
        .keys
        .iter()
        .map(|key| {
            columns
                .iter()
                .position(|column| column.name == *key)
                .ok_or_else(|| corrupt("partition key names a column the relation does not have"))
        })
        .collect()
}

/// Extract a row's partition key values. `columns` describes `row`.
fn key_values(
    scheme: &Scheme,
    columns: &[crabka_pgcatalog::Column],
    row: &[Datum],
) -> Result<Vec<Datum>, ExecError> {
    key_ordinals(scheme, columns)?
        .into_iter()
        .map(|ordinal| {
            row.get(ordinal)
                .cloned()
                .ok_or_else(|| corrupt("partition key column ordinal is past the end of the row"))
        })
        .collect()
}

/// One value of a diagnostic row or key description, in `PostgreSQL`'s
/// `maxfieldlen` form: the type's *output* text — no quotes around a string and
/// no `::type` annotation, unlike [`bound_datum_text`], which spells a literal —
/// cut to 64 bytes on a character boundary with a trailing `...` when it is
/// longer, and the bare word `null` for a NULL.
///
/// Shared with [`crate::rls::describe_row`], which renders whole rows by the
/// same rule: `ExecBuildSlotValueDescription` and
/// `ExecBuildSlotPartitionKeyDescription` format a field identically, and two
/// copies of the rule could disagree about a cut.
pub(crate) fn field_text(value: &Datum, ctx: &crate::clock::EvalCtx) -> String {
    /// `PostgreSQL`'s `maxfieldlen`.
    const MAX_FIELD: usize = 64;
    match value {
        Datum::Null => "null".to_string(),
        other => {
            let text = String::from_utf8_lossy(&crabka_pgtypes::encoding::encode_text(
                other,
                &ctx.time_zone,
            ))
            .into_owned();
            if text.len() <= MAX_FIELD {
                return text;
            }
            // `pg_mbcliplen`: the longest prefix inside the budget that does
            // not split a character, so a multi-byte character straddling the
            // limit stops the cut short of it rather than at it.
            let cut = (0..=MAX_FIELD)
                .rev()
                .find(|end| text.is_char_boundary(*end))
                .unwrap_or(0);
            format!("{}...", &text[..cut])
        }
    }
}

/// `PostgreSQL`'s `ExecBuildSlotPartitionKeyDescription` — the `(key) = (values)`
/// body of the `DETAIL` line that follows a routing failure.
///
/// The key is spelled as `pg_get_partkeydef_columns` spells it, so a column
/// name is quoted only when it has to be — unlike the whole-row description in
/// [`crate::rls::describe_row`], whose column list upstream writes unquoted.
/// The values are the row's key columns alone, not the whole row.
///
/// Only the caller decides whether this may be shown at all. Building it is
/// unconditional; disclosing it is not. See `exec::may_describe_key`.
pub(crate) fn key_description(
    scheme: &Scheme,
    columns: &[crabka_pgcatalog::Column],
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<String, ExecError> {
    let names = scheme
        .keys
        .iter()
        .map(|key| crate::catalog_fn::quote_identifier(key))
        .collect::<Vec<_>>()
        .join(", ");
    let values = key_values(scheme, columns, row)?
        .iter()
        .map(|value| field_text(value, ctx))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("({names}) = ({values})"))
}

/// Compare two partition-key datums. `None` means the comparison had no answer,
/// because either operand was NULL or the two types do not compare. Every
/// caller treats that as "does not belong here" and does not guess.
fn compare(left: &Datum, right: &Datum) -> Option<Ordering> {
    crabka_pgtypes::ops::compare(left, right).ok().flatten()
}

/// Does `key` fall inside `bound`? `None` means "not decidable". [`route`]
/// treats that as "no" and reports a routing failure. It does not guess.
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

/// Compare a row's key tuple against one side of a range bound, with
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
/// `partitions` is the full direct-partition list. This function chooses the
/// default partition only after every other bound has declined the row,
/// exactly as `PostgreSQL` does. `columns` describes `row`, which is in the
/// parent's own column order.
pub(crate) fn route<'a>(
    scheme: &Scheme,
    columns: &[crabka_pgcatalog::Column],
    partitions: &'a [Partition],
    row: &[Datum],
) -> Result<Option<&'a Partition>, ExecError> {
    let key = key_values(scheme, columns, row)?;
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
/// per-partition `CHECK`, applied when a row goes straight into a leaf.
///
/// A `DEFAULT` partition accepts a row exactly when no sibling accepts it, so
/// the caller must supply the sibling bounds too.
pub(crate) fn satisfies(
    scheme: &Scheme,
    columns: &[crabka_pgcatalog::Column],
    bound: &Bound,
    siblings: &[Partition],
    row: &[Datum],
) -> Result<bool, ExecError> {
    let key = key_values(scheme, columns, row)?;
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

/// Reject a bound whose spelling does not match the parent's strategy, with
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

/// Order one range tuple against another, column by column, with
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
/// with `PostgreSQL`'s 42P17 wording. That wording names both partitions with
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
/// are distributed, and the two routing rules cannot both hold. This function
/// refuses a declaration of both, and does not silently let one win.
pub(crate) fn reject_sharded_partitioned() -> ExecError {
    ExecError::Unsupported(
        "a SHARDED table cannot also be partitioned: sharding and declarative partitioning are \
         two different row-placement rules, and only one can decide where a row lives"
            .into(),
    )
}

/// Resolve a `PARTITION BY` key list against the parent's columns.
///
/// This function applies every rule `PostgreSQL` enforces at parse-analysis
/// time, so the SQLSTATEs match. A missing column is 42703. A system column or
/// a constant expression is 42P17. `LIST` with more than one column is 42P17.
pub(crate) fn key_columns(
    strategy: Strategy,
    keys: &[crabka_pgparser::ast::PartitionKeyElem],
    columns: &[crabka_pgcatalog::Column],
) -> Result<Vec<String>, ExecError> {
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
            if crate::scope::is_system_column(name) {
                return Err(ExecError::InvalidObjectDefinition(format!(
                    "cannot use system column \"{name}\" in partition key"
                )));
            }
            if !columns.iter().any(|column| column.name == name) {
                return Err(ExecError::UndefinedPartitionKeyColumn(name.to_string()));
            }
            Ok(name.to_string())
        })
        .collect()
}

/// The system columns a generation expression may not read.
///
/// `tableoid` is the one exception `PostgreSQL` makes: it is fixed for the life
/// of the row, so a generated column may depend on it.
pub(crate) fn is_generation_forbidden_system_column(name: &str) -> bool {
    crate::scope::is_system_column(name) && name != crate::scope::TABLEOID_COLUMN
}

/// The refusal for an expression partition key.
///
/// A constant expression is `PostgreSQL`'s own 42P17. Every other expression is
/// a documented 0A000 and not an approximation. A route through an expression
/// needs the expression's result type to coerce the stored bounds against, and
/// a wrong coercion puts rows in the wrong leaf.
fn expression_key_error(text: &str) -> ExecError {
    ExecError::Unsupported(format!(
        "expression partition keys are not supported: PARTITION BY … ({text}) needs the \
         expression's result type to coerce partition bounds against, and routing a row through \
         the wrong type would place it in the wrong partition"
    ))
}

/// Type of the key column named `key`, for the coercion of a written bound
/// value.
pub(crate) fn key_column_type(
    columns: &[crabka_pgcatalog::Column],
    key: &str,
) -> Result<ColumnType, ExecError> {
    columns
        .iter()
        .find(|column| column.name == key)
        .map(|column| column.ty)
        .ok_or_else(|| corrupt("partition key names a column the relation does not have"))
}

#[cfg(test)]
mod tests {

    /// A cycle in the partition metadata must not make the tree walk diverge.
    ///
    /// `ATTACH PARTITION` refuses the cycles it can see, but this walk is on the
    /// `DROP` path and must terminate on whatever metadata it is handed. When it
    /// did not, a single `DROP TABLE` spun a core and allocated until the
    /// process was killed. That read as 10,135 corpus statements that failed to
    /// connect, and not as one bad statement.
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

    /// A scan of its prefix reads the relation catalog, so a partition record
    /// stored under it would go to the relation decoder. `DROP SCHEMA … CASCADE`
    /// lists a schema's contents that way, so it would try to drop the record as
    /// a relation.
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
    /// child index cannot be read as another's. This holds even when one
    /// relation's name begins with the other's, which a plain concatenation
    /// would confuse.
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

    /// Whether any partition key or value still spells `name`. A rename that
    /// leaves one behind either strands the metadata or hands it to whatever
    /// takes the old name next.
    fn anything_still_names(kv: &crabka_pgkv::MemKv, name: &str) -> bool {
        let mut needle = Vec::new();
        push_key_part(&mut needle, name);
        [SCHEME_PREFIX, CHILD_PREFIX, CHILDREN_PREFIX]
            .into_iter()
            .any(|prefix| {
                kv.scan_prefix(prefix)
                    .expect("scan")
                    .into_iter()
                    .any(|(key, value)| {
                        [key, value]
                            .iter()
                            .any(|bytes| bytes.windows(needle.len()).any(|w| w == needle))
                    })
            })
    }

    /// Renaming the parent turned it back into an ordinary heap: the key
    /// definition stayed under the old name, `relkind` fell from `p` to `r`,
    /// the leaf's rows stopped being reachable through it, and it began
    /// accepting rows that belong in no partition.
    #[test]
    fn a_renamed_parent_keeps_its_scheme_and_its_partitions() {
        let kv = crabka_pgkv::MemKv::default();
        let parent = RelationName::new("sch", "p");
        let renamed = RelationName::new("sch", "p_renamed");
        let child = RelationName::new("sch", "c");
        write(&kv, put_scheme_ops(&parent, &list_scheme()));
        write(&kv, attach_ops(&parent, &child, &hash_bound()));

        write(&kv, rename_ops(&kv, &parent, &renamed).expect("ops"));

        assert!(scheme_of(&kv, &renamed).expect("scheme") == Some(list_scheme()));
        assert!(!is_partitioned(&kv, &parent).expect("read"));
        assert!(
            partitions_of(&kv, &renamed).expect("scan")
                == vec![Partition {
                    name: child.clone(),
                    bound: hash_bound(),
                }]
        );
        assert!(parent_of(&kv, &child).expect("link") == Some((renamed, hash_bound())));
        assert!(!anything_still_names(&kv, "p"));
    }

    /// Renaming the leaf left the parent naming a relation nothing carried, so
    /// every read and every write of the parent failed outright.
    #[test]
    fn a_renamed_leaf_is_still_its_parents_partition() {
        let kv = crabka_pgkv::MemKv::default();
        let parent = RelationName::new("sch", "p");
        let child = RelationName::new("sch", "c");
        let renamed = RelationName::new("sch", "c_renamed");
        write(&kv, put_scheme_ops(&parent, &list_scheme()));
        write(&kv, attach_ops(&parent, &child, &hash_bound()));

        write(&kv, rename_ops(&kv, &child, &renamed).expect("ops"));

        assert!(
            partitions_of(&kv, &parent).expect("scan")
                == vec![Partition {
                    name: renamed.clone(),
                    bound: hash_bound(),
                }]
        );
        assert!(parent_of(&kv, &renamed).expect("link") == Some((parent, hash_bound())));
        assert!(parent_of(&kv, &child).expect("link").is_none());
        assert!(!anything_still_names(&kv, "c"));
    }

    /// An intermediate parent is both ends of the rename at once, and the walk
    /// below it has to survive.
    #[test]
    fn renaming_a_sub_partitioned_level_keeps_the_tree_whole() {
        let kv = crabka_pgkv::MemKv::default();
        let top = RelationName::new("sch", "top");
        let mid = RelationName::new("sch", "mid");
        let renamed = RelationName::new("sch", "middle");
        let leaf = RelationName::new("sch", "leaf");
        write(&kv, put_scheme_ops(&top, &list_scheme()));
        write(&kv, put_scheme_ops(&mid, &list_scheme()));
        write(&kv, attach_ops(&top, &mid, &hash_bound()));
        write(&kv, attach_ops(&mid, &leaf, &hash_bound()));

        write(&kv, rename_ops(&kv, &mid, &renamed).expect("ops"));

        assert!(descendants(&kv, &top).expect("walk") == vec![leaf.clone(), renamed.clone()]);
        assert!(leaves_of(&kv, &top).expect("leaves") == vec![leaf]);
        assert!(scheme_of(&kv, &renamed).expect("scheme") == Some(list_scheme()));
        assert!(!anything_still_names(&kv, "mid"));
    }

    #[test]
    fn renaming_an_unpartitioned_relation_writes_nothing() {
        let kv = crabka_pgkv::MemKv::default();
        let plain = RelationName::new("sch", "plain");
        write(
            &kv,
            attach_ops(
                &RelationName::new("sch", "p"),
                &RelationName::new("sch", "c"),
                &hash_bound(),
            ),
        );
        assert!(
            rename_ops(&kv, &plain, &RelationName::new("sch", "plain2")).expect("ops") == vec![]
        );
    }

    fn list_scheme() -> Scheme {
        Scheme {
            strategy: Strategy::List,
            keys: vec!["a".into()],
        }
    }

    fn range_scheme() -> Scheme {
        Scheme {
            strategy: Strategy::Range,
            keys: vec!["a".into()],
        }
    }

    /// The one-column relation the schemes above are keyed on.
    fn keyed_columns() -> Vec<crabka_pgcatalog::Column> {
        vec![crabka_pgcatalog::Column::new("a", ColumnType::Int4)]
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
            let routed = route(
                &scheme,
                &keyed_columns(),
                &partitions,
                std::slice::from_ref(&input),
            )
            .expect("routing decides");
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
        let routed =
            route(&scheme, &keyed_columns(), &partitions, &[Datum::Null]).expect("routing decides");
        assert!(routed.map(|partition| partition.name.name.as_str()) == Some("p_null"));
        let routed = route(&scheme, &keyed_columns(), &partitions, &[Datum::Int4(1)])
            .expect("routing decides");
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
            let routed = route(
                &scheme,
                &keyed_columns(),
                &partitions,
                &[Datum::Int4(input)],
            )
            .expect("routing decides");
            assert!(routed.map(|partition| partition.name.name.as_str()) == expected);
        }
        // A NULL key belongs to no range partition.
        let routed =
            route(&scheme, &keyed_columns(), &partitions, &[Datum::Null]).expect("routing decides");
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
            satisfies(
                &scheme,
                &keyed_columns(),
                &Bound::Default,
                &siblings,
                &[Datum::Int4(9)]
            )
            .expect("default check decides")
        );
        assert!(
            !satisfies(
                &scheme,
                &keyed_columns(),
                &Bound::Default,
                &siblings,
                &[Datum::Int4(1)]
            )
            .expect("default check decides")
        );
    }

    #[test]
    fn scheme_and_bound_metadata_round_trip_through_their_encodings() {
        let scheme = Scheme {
            strategy: Strategy::Range,
            keys: vec!["a".into(), "c".into()],
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
