//! Key construction: `/<table_id>/<index_id>/<rowid>` and, for hash-sharded
//! primary rows, `/<table_id>/<index_id>/<bucket>/<rowid>`. The primary "index"
//! is id 1; secondary indexes get higher ids under the same table prefix.

use crate::{
    KvError,
    keyenc::{put_u32, put_u64, take_u32, take_u64},
};

/// The primary storage index for a table's rows.
pub const INDEX_PRIMARY: u32 = 1;

/// Reserved table id for system metadata (catalog, sequences, global meta).
pub const SYSTEM_TABLE_ID: u32 = 0;

/// Parsed storage-key class. This is deliberately structural: callers must not
/// infer a key's meaning from byte offsets themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyClass {
    /// An ordinary table primary-row key without an MVCC version suffix.
    PrimaryRow { table_id: u32, rowid: u64 },
    /// An ordinary table primary-row MVCC version key.
    PrimaryVersion {
        table_id: u32,
        rowid: u64,
        version: u64,
    },
    /// A hash-sharded primary-row key, unsupported by local table transfer.
    HashPrimaryRow {
        table_id: u32,
        bucket: u32,
        rowid: u64,
    },
    /// A hash-sharded primary-row version key, unsupported by local transfer.
    HashPrimaryVersion {
        table_id: u32,
        bucket: u32,
        rowid: u64,
        version: u64,
    },
    /// A local secondary-index key.
    SecondaryIndex {
        table_id: u32,
        storage_index_id: u32,
    },
    /// A table-local rowid sequence key.
    Sequence { table_id: u32 },
    /// A transaction commit-status-log key.
    Clog { xid: u64 },
    /// A cross-node `NOTIFY` record key. WAL-only: it is dropped by every apply
    /// site and must never reach the store.
    Notify,
    /// A known system key that is not transferable table state.
    System,
    /// A malformed or currently unrecognised key.
    Unknown,
}

/// Classify a storage key without accepting trailing bytes as a valid key.
///
/// This is the common boundary for transfer and recovery code. In particular,
/// hash rows and secondary indexes are not silently mistaken for ordinary
/// primary versions.
#[must_use]
pub fn classify_key(bytes: &[u8]) -> KeyClass {
    if let Some(xid) = clog_xid_of(bytes) {
        return KeyClass::Clog { xid };
    }
    if let Some(table_id) = sequence_table_id_of(bytes) {
        return KeyClass::Sequence { table_id };
    }
    // Must precede the `table_id == 0` catch-all below, which would otherwise
    // report notify keys as ordinary (persistable) system state.
    if is_notify_key(bytes) {
        return KeyClass::Notify;
    }

    let Some((table_id, index_id)) = key_header(bytes) else {
        return KeyClass::Unknown;
    };
    if table_id == SYSTEM_TABLE_ID {
        return KeyClass::System;
    }
    if index_id != INDEX_PRIMARY {
        return KeyClass::SecondaryIndex {
            table_id,
            storage_index_id: index_id,
        };
    }

    match bytes.len() {
        16 => KeyClass::PrimaryRow {
            table_id,
            rowid: u64_component(&bytes[8..16]),
        },
        24 => KeyClass::PrimaryVersion {
            table_id,
            rowid: u64_component(&bytes[8..16]),
            version: u64_component(&bytes[16..24]),
        },
        20 => KeyClass::HashPrimaryRow {
            table_id,
            bucket: u32_component(&bytes[8..12]),
            rowid: u64_component(&bytes[12..20]),
        },
        28 => KeyClass::HashPrimaryVersion {
            table_id,
            bucket: u32_component(&bytes[8..12]),
            rowid: u64_component(&bytes[12..20]),
            version: u64_component(&bytes[20..28]),
        },
        _ => KeyClass::Unknown,
    }
}

/// Extract an ordinary table MVCC version identity from `key`.
#[must_use]
pub fn primary_version_of(key: &[u8]) -> Option<(u32, u64, u64)> {
    match classify_key(key) {
        KeyClass::PrimaryVersion {
            table_id,
            rowid,
            version,
        } => Some((table_id, rowid, version)),
        _ => None,
    }
}

/// Extract a table id from an exact per-table sequence key.
#[must_use]
pub fn sequence_table_id_of(key: &[u8]) -> Option<u32> {
    let prefix = seq_prefix();
    if key.len() != prefix.len() + 4 || !key.starts_with(&prefix) {
        return None;
    }
    Some(u32_component(&key[prefix.len()..]))
}

fn key_header(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 8 {
        return None;
    }
    Some((u32_component(&bytes[..4]), u32_component(&bytes[4..8])))
}

fn u32_component(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("key component has fixed width"))
}

fn u64_component(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("key component has fixed width"))
}

/// Bytes shared by every row of a table's primary index.
#[must_use]
pub fn table_prefix(table_id: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(8);
    put_u32(&mut k, table_id);
    put_u32(&mut k, INDEX_PRIMARY);
    k
}

/// Full key for one row: table prefix followed by the order-preserving rowid.
#[must_use]
pub fn row_key(table_id: u32, rowid: u64) -> Vec<u8> {
    let mut k = table_prefix(table_id);
    put_u64(&mut k, rowid);
    k
}

/// Bytes shared by every row in one hash bucket of a table's primary index.
#[must_use]
pub fn hash_bucket_prefix(table_id: u32, bucket: u32) -> Vec<u8> {
    let mut k = table_prefix(table_id);
    put_u32(&mut k, bucket);
    k
}

/// Full key for one hash-sharded row: table prefix, bucket, then rowid.
#[must_use]
pub fn hash_row_key(table_id: u32, bucket: u32, rowid: u64) -> Vec<u8> {
    let mut k = hash_bucket_prefix(table_id, bucket);
    put_u64(&mut k, rowid);
    k
}

/// Bytes shared by every entry in one local secondary index.
///
/// # Panics
///
/// Panics when `index_id` cannot be shifted into the secondary-index storage
/// namespace without overflowing `u32`.
#[must_use]
pub fn secondary_index_prefix(table_id: u32, index_id: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(8);
    put_u32(&mut k, table_id);
    let storage_index_id = index_id
        .checked_add(INDEX_PRIMARY)
        .expect("secondary index id exceeds u32 storage id range");
    put_u32(&mut k, storage_index_id);
    k
}

/// Prefix for a local secondary-index equality probe over an encoded key tuple.
///
/// Index lookups (and unique-constraint enforcement) are equality **by these
/// bytes**, so the values are first put through
/// [`crabka_pgtypes::canonicalize_row_for_key`]: two values that compare equal
/// — `numeric` `1.0` and `1.00`, `-0.0` and `0.0`, `interval` `1 mon` and
/// `30 days`, the same inside `jsonb` or an array — must produce the same key.
/// Canonicalization is confined to this key path; `encode_row` on a stored row
/// keeps whatever the value was written as.
///
/// # Panics
///
/// Panics when the encoded tuple exceeds 4 GiB or `index_id` overflows the
/// secondary-index storage namespace.
#[must_use]
pub fn secondary_index_entry_prefix(
    table_id: u32,
    index_id: u32,
    indexed_values: &[crabka_pgtypes::Datum],
) -> Vec<u8> {
    let canonical = crabka_pgtypes::canonicalize_row_for_key(indexed_values);
    let encoded_values = crate::rowenc::encode_row(&canonical);
    let mut k = secondary_index_prefix(table_id, index_id);
    let encoded_len = u32::try_from(encoded_values.len()).expect("index key exceeds 4 GiB");
    k.extend_from_slice(&encoded_len.to_be_bytes());
    k.extend_from_slice(&encoded_values);
    k
}

/// Full local secondary-index entry key.
#[must_use]
pub fn secondary_index_entry_key(
    table_id: u32,
    index_id: u32,
    indexed_values: &[crabka_pgtypes::Datum],
    rowid: u64,
) -> Vec<u8> {
    let mut k = secondary_index_entry_prefix(table_id, index_id, indexed_values);
    put_u64(&mut k, rowid);
    k
}

/// Recover the base rowid from a local secondary-index entry key.
///
/// # Errors
///
/// Returns [`KvError::CorruptRow`] when the key is truncated or does not belong
/// to the requested secondary index.
pub fn secondary_index_rowid_of(table_id: u32, index_id: u32, key: &[u8]) -> Result<u64, KvError> {
    let prefix = secondary_index_prefix(table_id, index_id);
    if !key.starts_with(&prefix) {
        return Err(KvError::CorruptRow(
            "key does not belong to this secondary index".into(),
        ));
    }
    if key.len() < prefix.len() + 4 + 8 {
        return Err(KvError::CorruptRow("secondary index key too short".into()));
    }

    let mut len_bytes = [0_u8; 4];
    len_bytes.copy_from_slice(&key[prefix.len()..prefix.len() + 4]);
    let value_len = usize::try_from(u32::from_be_bytes(len_bytes)).map_err(|_| {
        KvError::CorruptRow("secondary index value length does not fit usize".into())
    })?;
    let rowid_offset = prefix.len() + 4 + value_len;
    if key.len() != rowid_offset + 8 {
        return Err(KvError::CorruptRow(
            "secondary index key has trailing bytes".into(),
        ));
    }

    let mut rowid_bytes = &key[rowid_offset..];
    take_u64(&mut rowid_bytes)
}

/// Deterministically map value bytes to a power-of-two bucket count.
///
/// # Panics
///
/// Panics only if a valid bucket index cannot be represented as `u32`.
#[must_use]
pub fn hash_bucket(value: &[u8], bucket_count: u32) -> Option<u32> {
    if bucket_count == 0 || !bucket_count.is_power_of_two() {
        return None;
    }

    Some(u32::try_from(fnv1a64(value) & u64::from(bucket_count - 1)).expect("bucket fits u32"))
}

fn system_prefix(tag: &str) -> Vec<u8> {
    let mut k = Vec::new();
    put_u32(&mut k, SYSTEM_TABLE_ID);
    k.extend_from_slice(tag.as_bytes());
    k.push(b'/');
    k
}

/// Durable range-control receipt key under system table zero.
#[must_use]
pub fn range_control_receipt_key(tenant: &str, receipt: &str) -> Vec<u8> {
    let mut key = range_control_receipt_prefix(tenant);
    key.extend_from_slice(receipt.as_bytes());
    key
}

/// Prefix containing every durable control receipt for one tenant.
#[must_use]
pub fn range_control_receipt_prefix(tenant: &str) -> Vec<u8> {
    let mut key = system_prefix("range_control_receipt");
    key.extend_from_slice(tenant.as_bytes());
    key.push(b'/');
    key
}

/// Durable topology-activation receipt key under system table zero.
#[must_use]
pub fn topology_activation_receipt_key(tenant: &str, operation_id: &str) -> Vec<u8> {
    let mut key = topology_activation_receipt_prefix(tenant);
    key.extend_from_slice(operation_id.as_bytes());
    key
}

/// Prefix containing every incomplete or completed topology activation receipt.
#[must_use]
pub fn topology_activation_receipt_prefix(tenant: &str) -> Vec<u8> {
    let mut key = system_prefix("topology_activation_receipt");
    key.extend_from_slice(tenant.as_bytes());
    key.push(b'/');
    key
}

/// Append one length-prefixed part to a catalog key.
///
/// Every catalog key that names a relation is built from `(schema, name)` this
/// way rather than from a dotted string, because the two are not
/// interchangeable: `PostgreSQL` lets a relation called `a.b` in `public` and a
/// relation called `b` in schema `a` coexist with distinct contents, which no
/// flattened `schema.relation` string can represent.
///
/// The prefix is a big-endian `u32` byte length rather than a separator byte.
/// A separator would work — an identifier holds no NUL — but `\0` already
/// separates sub-families inside the catalog prefix, and a length prefix means
/// a scan recovers a part exactly instead of guessing at it. That guessing is
/// what let `CREATE TABLE "a/b"` store a relation the catalog projections
/// could never see.
///
/// # Panics
///
/// Panics when `part` exceeds 4 GiB, which no identifier does.
pub fn push_key_part(key: &mut Vec<u8>, part: &str) {
    put_u32(
        key,
        u32::try_from(part.len()).expect("catalog key part exceeds 4 GiB"),
    );
    key.extend_from_slice(part.as_bytes());
}

/// Split a key suffix written by [`push_key_part`] into exactly `count` parts.
///
/// Returns `None` for a suffix that is truncated, carries trailing bytes, or
/// holds a part that is not UTF-8 — so a scan over a family prefix rejects a
/// neighbouring sub-family's keys structurally rather than by filtering for a
/// byte the names were assumed not to contain.
#[must_use]
pub fn key_parts(suffix: &[u8], count: usize) -> Option<Vec<&str>> {
    let mut rest = suffix;
    let mut parts = Vec::with_capacity(count);
    for _ in 0..count {
        let len = usize::try_from(take_u32(&mut rest).ok()?).ok()?;
        let (part, tail) = rest.split_at_checked(len)?;
        parts.push(std::str::from_utf8(part).ok()?);
        rest = tail;
    }
    rest.is_empty().then_some(parts)
}

/// Bytes shared by every relation-keyed entry in one catalog sub-family.
fn relation_family_key(family: &str, schema: &str, name: &str) -> Vec<u8> {
    let mut k = system_prefix(family);
    push_key_part(&mut k, schema);
    push_key_part(&mut k, name);
    k
}

/// Key for a table's stored schema: `/0/catalog/<schema><name>`, each part
/// length-prefixed.
#[must_use]
pub fn catalog_key(schema: &str, name: &str) -> Vec<u8> {
    relation_family_key("catalog", schema, name)
}

/// Prefix covering every stored table schema.
#[must_use]
pub fn catalog_prefix() -> Vec<u8> {
    system_prefix("catalog")
}

/// Key for a table's optional sharding strategy:
/// `/0/catalog_sharding/<schema><name>`.
#[must_use]
pub fn catalog_sharding_key(schema: &str, name: &str) -> Vec<u8> {
    relation_family_key("catalog_sharding", schema, name)
}

/// Key for the id-keyed index over stored tables: `/0/catalog_by_id/<id>`.
///
/// A table's identity is its id — row keys, lock identities and foreign-key
/// referents all key on it, and it survives a rename or a move between schemas
/// where the name does not. Range RPCs therefore ship an id, and the receiving
/// node needs the relation the id names without a full catalog scan.
#[must_use]
pub fn catalog_by_id_key(table_id: u32) -> Vec<u8> {
    let mut k = system_prefix("catalog_by_id");
    put_u32(&mut k, table_id);
    k
}

/// Key for a table's next-rowid sequence: `/0/seq/<table_id>`.
#[must_use]
pub fn seq_key(table_id: u32) -> Vec<u8> {
    let mut k = seq_prefix();
    put_u32(&mut k, table_id);
    k
}

/// Prefix of every per-table sequence-allocator key (`/0/seq/<table_id>`).
#[must_use]
pub fn seq_prefix() -> Vec<u8> {
    system_prefix("seq")
}

/// Key for the global next-table-id counter: `/0/meta/next_table_id`.
#[must_use]
pub fn meta_next_table_id_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"next_table_id");
    k
}

/// Key for a user-defined type stored in the catalog: `/0/usertype/<name>`.
#[must_use]
pub fn user_type_key(name: &str) -> Vec<u8> {
    let mut k = user_type_prefix();
    k.extend_from_slice(name.as_bytes());
    k
}

/// Shared prefix for every user-defined type, for the hydration scan.
#[must_use]
pub fn user_type_prefix() -> Vec<u8> {
    system_prefix("usertype")
}

/// Key for the global next-user-type-oid counter: `/0/meta/next_type_oid`.
#[must_use]
pub fn meta_next_type_oid_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"next_type_oid");
    k
}

/// Key for a foreign-data wrapper stored in the catalog: `/0/fdw/<name>`.
#[must_use]
pub fn fdw_key(name: &str) -> Vec<u8> {
    let mut k = system_prefix("fdw");
    k.extend_from_slice(name.as_bytes());
    k
}

/// Key for a foreign server stored in the catalog: `/0/fsrv/<name>`.
#[must_use]
pub fn server_key(name: &str) -> Vec<u8> {
    let mut k = system_prefix("fsrv");
    k.extend_from_slice(name.as_bytes());
    k
}

/// Key for a user mapping stored in the catalog: `/0/umap/<user>\0<server>`.
#[must_use]
pub fn user_mapping_key(user: &str, server: &str) -> Vec<u8> {
    let mut k = system_prefix("umap");
    k.extend_from_slice(user.as_bytes());
    k.push(0);
    k.extend_from_slice(server.as_bytes());
    k
}

/// Shared prefix for all foreign-server entries (for listing / IMPORT scans).
#[must_use]
pub fn server_prefix() -> Vec<u8> {
    system_prefix("fsrv")
}

/// Key for the replicated range-descriptor blob: `/0/meta/range_map`.
#[must_use]
pub fn meta_range_map_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"range_map");
    k
}

/// Key for the global next-transaction-id counter: `/0/meta/next_xid`.
#[must_use]
pub fn next_xid_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"next_xid");
    k
}

/// Key for the GTM's monotonic global-xid counter: `/0/meta/next_global_xid`.
/// Lives in range 0's store, disjoint from the per-range `next_xid` key.
#[must_use]
pub fn meta_next_global_xid_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"next_global_xid");
    k
}

/// Key for a DATA range's recovery-scan watermark: the smallest local xid `Li`
/// at/after which the leadership-rise recovery scan must still look. Lives in the
/// `meta` namespace (disjoint from the `/0/clog/` prefix, so a clog scan never
/// returns it). Stored per-range in that range's own store. Value = `Li` big-endian.
#[must_use]
pub fn clog_scan_lo_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"clog_scan_lo");
    k
}

/// Exclusive upper bound for a scan over the whole `/0/clog/` keyspace: the clog
/// prefix with its trailing byte incremented (the prefix's successor).
///
/// # Panics
///
/// Panics if the internal clog prefix invariant unexpectedly produces an empty
/// prefix.
#[must_use]
pub fn clog_scan_end() -> Vec<u8> {
    let mut p = clog_prefix();
    let last = p.last_mut().expect("clog prefix is non-empty");
    *last += 1;
    p
}

/// Key for a transaction's commit-status-log entry: `/0/clog/<xid>`.
#[must_use]
pub fn clog_key(xid: u64) -> Vec<u8> {
    let mut k = system_prefix("clog");
    crate::keyenc::put_u64(&mut k, xid);
    k
}

/// The shared prefix of every `/0/clog/<xid>` entry.
#[must_use]
pub fn clog_prefix() -> Vec<u8> {
    system_prefix("clog")
}

/// Decode the xid from a `/0/clog/<xid>` key, or `None` if `key` is not a clog key.
#[must_use]
pub fn clog_xid_of(key: &[u8]) -> Option<u64> {
    let prefix = clog_prefix();
    if key.len() != prefix.len() + 8 || key[..prefix.len()] != prefix[..] {
        return None;
    }
    let mut rest = &key[prefix.len()..];
    crate::keyenc::take_u64(&mut rest).ok()
}

/// The shared prefix of every cross-node `NOTIFY` record: `/0/notify/`.
///
/// Records under this prefix live in the WAL only. Range-0 checkpoints snapshot
/// the entire KV and the WAL topic never expires by time, so a notify record
/// that reached the store would become permanent catalog state; every apply
/// site drops them instead (see [`crate::is_notify_op`]).
#[must_use]
pub fn notify_prefix() -> Vec<u8> {
    system_prefix("notify")
}

/// Key for one cross-node `NOTIFY` record: `/0/notify/<seq>`.
///
/// `seq` is written big-endian so records sort — and are therefore observed —
/// in arrival order. One WAL frame may carry several records, each with its own
/// `seq`.
#[must_use]
pub fn notify_key(seq: u64) -> Vec<u8> {
    let mut k = notify_prefix();
    put_u64(&mut k, seq);
    k
}

/// True for any key in the `/0/notify/` namespace.
///
/// Deliberately a prefix test rather than an exact-shape test: this predicate
/// guards the store, so even a malformed key under the notify namespace must be
/// rejected.
#[must_use]
pub fn is_notify_key(key: &[u8]) -> bool {
    key.starts_with(&notify_prefix())
}

/// Recover `(table_id, rowid)` from a primary-index row/version key.
#[must_use]
pub fn table_rowid_of(key: &[u8]) -> Option<(u32, u64)> {
    let mut cur = key;
    let t = take_u32(&mut cur).ok()?;
    let idx = take_u32(&mut cur).ok()?;
    if t == SYSTEM_TABLE_ID || idx != INDEX_PRIMARY {
        return None;
    }
    let rowid = take_u64(&mut cur).ok()?;
    Some((t, rowid))
}

/// Recover `(table_id, bucket, rowid)` from a hash-sharded primary row/version key.
#[must_use]
pub fn table_bucket_rowid_of(key: &[u8]) -> Option<(u32, u32, u64)> {
    let mut cur = key;
    let t = take_u32(&mut cur).ok()?;
    let idx = take_u32(&mut cur).ok()?;
    if t == SYSTEM_TABLE_ID || idx != INDEX_PRIMARY {
        return None;
    }
    let bucket = take_u32(&mut cur).ok()?;
    let rowid = take_u64(&mut cur).ok()?;
    Some((t, bucket, rowid))
}

/// Recover the rowid from a key known to belong to `table_id`.
///
/// # Errors
///
/// Returns [`KvError::CorruptRow`] when the key is truncated or does not belong
/// to `table_id`'s primary index.
pub fn rowid_of(table_id: u32, key: &[u8]) -> Result<u64, KvError> {
    let mut cur = key;
    let t = take_u32(&mut cur)?;
    let idx = take_u32(&mut cur)?;
    if t != table_id || idx != INDEX_PRIMARY {
        return Err(KvError::CorruptRow(
            "key does not belong to this table index".into(),
        ));
    }
    take_u64(&mut cur)
}

/// Recover `(bucket, rowid)` from a hash-sharded key known to belong to `table_id`.
///
/// # Errors
///
/// Returns [`KvError::CorruptRow`] when the key is truncated or does not belong
/// to `table_id`'s primary index.
pub fn bucket_rowid_of(table_id: u32, key: &[u8]) -> Result<(u32, u64), KvError> {
    let mut cur = key;
    let t = take_u32(&mut cur)?;
    let idx = take_u32(&mut cur)?;
    if t != table_id || idx != INDEX_PRIMARY {
        return Err(KvError::CorruptRow(
            "key does not belong to this table index".into(),
        ));
    }
    let bucket = take_u32(&mut cur)?;
    let rowid = take_u64(&mut cur)?;
    Ok((bucket, rowid))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn table_zero_prefix() -> Vec<u8> {
        let mut k = Vec::new();
        crate::keyenc::put_u32(&mut k, 0);
        k
    }

    /// Index probes are equality by raw key bytes, so values that compare equal
    /// MUST build the same key — otherwise a unique index would admit two rows
    /// `PostgreSQL` considers duplicates.
    #[test]
    fn equal_index_values_build_identical_keys() {
        use assert2::assert;
        use crabka_pgtypes::{ArrayValue, Datum, ElemType};

        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("numeric"));
        let json = |s: &str| Datum::Jsonb(crabka_pgtypes::jsonb::parse(s).expect("jsonb"));
        let iv = |s: &str| {
            Datum::Interval(crabka_pgtypes::datetime::parse_interval(s).expect("interval"))
        };
        let pairs: &[(Datum, Datum)] = &[
            // Plain numeric scale.
            (num("1.0"), num("1.00")),
            (num("-0.0"), num("0")),
            // float negative zero.
            (Datum::Float8(-0.0), Datum::Float8(0.0)),
            // interval field spelling: equality is the 30-day-month / 24-hour-day
            // estimate, so these are one value stored three ways.
            (iv("1 mon"), iv("30 days")),
            (iv("1 day"), iv("24 hours")),
            (iv("1 year 1 day"), iv("12 mons 24 hours")),
            (iv("1 mon -1 hour"), iv("29 days 23:00:00")),
            (iv("-1 mon"), iv("-30 days")),
            // jsonb key order and number scale.
            (json(r#"{"b":2,"a":1}"#), json(r#"{"a":1,"b":2}"#)),
            (json(r#"{"a":1.0}"#), json(r#"{"a":1.00}"#)),
            // Array elements, recursively.
            (
                Datum::Array(ArrayValue::new(ElemType::Numeric, vec![num("2.50")])),
                Datum::Array(ArrayValue::new(ElemType::Numeric, vec![num("2.5")])),
            ),
            (
                Datum::Array(ArrayValue::new(ElemType::Interval, vec![iv("1 mon")])),
                Datum::Array(ArrayValue::new(ElemType::Interval, vec![iv("30 days")])),
            ),
        ];
        for (left, right) in pairs {
            assert!(left == right, "{left:?} and {right:?} are equal values");
            assert!(
                secondary_index_entry_prefix(7, 1, std::slice::from_ref(left))
                    == secondary_index_entry_prefix(7, 1, std::slice::from_ref(right)),
                "index key for {left:?}"
            );
        }
        // Values that differ still build different keys.
        assert!(
            secondary_index_entry_prefix(7, 1, &[num("1.0")])
                != secondary_index_entry_prefix(7, 1, &[num("1.1")])
        );
        assert!(
            secondary_index_entry_prefix(7, 1, &[json(r#"{"a":1}"#)])
                != secondary_index_entry_prefix(7, 1, &[json(r#"{"a":2}"#)])
        );
        assert!(
            secondary_index_entry_prefix(7, 1, &[iv("1 mon")])
                != secondary_index_entry_prefix(7, 1, &[iv("31 days")])
        );
    }

    /// Key canonicalization must not leak into row storage: a stored `interval`
    /// keeps the months/days/micros it was given, so `1 mon` renders `1 mon`
    /// (not the `30 days` its index key folds to).
    #[test]
    fn canonicalization_is_key_only_and_never_rewrites_stored_rows() {
        use assert2::assert;
        use crabka_pgtypes::Datum;

        let parse = |s: &str| crabka_pgtypes::datetime::parse_interval(s).expect("interval");
        // Fields + rendering of a decoded interval, or `None` for any other datum.
        let stored = |d: &Datum| match d {
            Datum::Interval(i) => Some((
                i.months,
                i.days,
                i.micros,
                crabka_pgtypes::datetime::interval_to_text(*i),
            )),
            _ => None,
        };
        for (spelling, rendered) in [
            ("1 mon", "1 mon"),
            ("30 days", "30 days"),
            ("720 hours", "720:00:00"),
            ("1 mon -1 hour", "1 mon -01:00:00"),
            ("-1 mon", "-1 mons"),
        ] {
            let original = parse(spelling);
            let row = crate::rowenc::encode_row(&[Datum::Interval(original)]);
            let decoded = crate::rowenc::decode_row(&row).expect("row decodes");
            assert!(
                stored(&decoded[0])
                    == Some((
                        original.months,
                        original.days,
                        original.micros,
                        rendered.to_string(),
                    )),
                "stored form of {spelling}"
            );
        }
        // `1 mon` and `30 days` share an index key but NOT a stored row.
        assert!(
            crate::rowenc::encode_row(&[Datum::Interval(parse("1 mon"))])
                != crate::rowenc::encode_row(&[Datum::Interval(parse("30 days"))])
        );
    }

    #[test]
    fn row_keys_sort_by_rowid_within_a_table() {
        let k1 = row_key(7, 1);
        let k2 = row_key(7, 2);
        let k10 = row_key(7, 10);
        assert!(k1 < k2 && k2 < k10, "rowid order must be byte order");
        assert!(k1.starts_with(&table_prefix(7)));
    }

    #[test]
    fn hash_row_keys_sort_by_bucket_then_rowid() {
        let b0_r9 = hash_row_key(7, 0, 9);
        let b1_r1 = hash_row_key(7, 1, 1);
        let b1_r2 = hash_row_key(7, 1, 2);

        assert!(b0_r9 < b1_r1 && b1_r1 < b1_r2);
        assert!(b1_r1.starts_with(&hash_bucket_prefix(7, 1)));
    }

    #[test]
    fn hash_bucket_is_deterministic_and_rejects_invalid_counts() {
        assert_eq!(hash_bucket(b"alpha", 16), hash_bucket(b"alpha", 16));
        assert!(hash_bucket(b"alpha", 0).is_none());
        assert!(hash_bucket(b"alpha", 3).is_none());
    }

    #[test]
    fn bucket_rowid_roundtrips_from_a_hash_key() {
        let k = hash_row_key(7, 3, 42);

        assert_eq!(bucket_rowid_of(7, &k).expect("bucket rowid"), (3, 42));
        assert_eq!(table_bucket_rowid_of(&k), Some((7, 3, 42)));
    }

    #[test]
    fn different_tables_do_not_share_a_prefix() {
        assert!(!row_key(8, 1).starts_with(&table_prefix(7)));
    }

    #[test]
    fn rowid_roundtrips_from_a_key() {
        let k = row_key(7, 42);
        assert_eq!(rowid_of(7, &k).expect("rowid"), 42);
    }

    #[test]
    fn rowid_of_rejects_wrong_table() {
        let k = row_key(7, 42);
        assert!(rowid_of(8, &k).is_err(), "wrong table id must be rejected");
    }

    #[test]
    fn system_keys_are_distinct_and_under_table_zero() {
        let cat = catalog_key("public", "users");
        let seq = seq_key(7);
        let meta = meta_next_table_id_key();
        let zero = table_zero_prefix();
        assert!(cat.starts_with(&zero));
        assert!(seq.starts_with(&zero));
        assert!(meta.starts_with(&zero));
        assert_ne!(cat, seq);
        assert_ne!(seq, meta);
        assert_ne!(catalog_key("public", "a"), catalog_key("public", "b"));
        assert_ne!(seq_key(7), seq_key(8));
    }

    /// A relation key is built from two parts, not from a dotted string, so
    /// every way of splitting the same characters keys a different relation.
    /// `PostgreSQL` lets all of these coexist with distinct contents.
    #[test]
    fn a_relation_key_keeps_the_schema_and_the_name_apart() {
        let cases = [
            ("public", "a.b"),
            ("a", "b"),
            ("a.b", ""),
            ("", "a.b"),
            ("public", "ab"),
        ];
        for (i, (schema, name)) in cases.iter().enumerate() {
            for (j, (other_schema, other_name)) in cases.iter().enumerate() {
                let same = catalog_key(schema, name) == catalog_key(other_schema, other_name);
                assert!(
                    same == (i == j),
                    "{schema}.{name} vs {other_schema}.{other_name}"
                );
            }
        }
        // The two sub-families never collide for the same relation either.
        assert!(catalog_key("a", "b") != catalog_sharding_key("a", "b"));
    }

    /// Every part a relation name can hold survives the round trip, including
    /// the separator bytes a delimited encoding would have to escape. A name
    /// holding a `/` used to be storable but invisible to every catalog scan.
    #[test]
    fn key_parts_recovers_exactly_what_push_key_part_wrote() {
        for parts in [
            vec!["public", "t"],
            vec!["a", "b"],
            vec!["public", "a.b"],
            vec!["s1", "a/b"],
            vec!["", ""],
            vec!["schema\0with\0nuls", "n\"a'm/e.\\"],
            vec!["один", "🦀"],
        ] {
            let mut key = Vec::new();
            for part in &parts {
                push_key_part(&mut key, part);
            }
            assert!(key_parts(&key, parts.len()) == Some(parts.clone()));
        }
    }

    /// A suffix that is truncated, over-long, or not UTF-8 is rejected rather
    /// than guessed at — a scan over one family prefix must reject a
    /// neighbouring family's keys structurally.
    #[test]
    fn key_parts_rejects_a_suffix_it_did_not_write() {
        let mut key = Vec::new();
        push_key_part(&mut key, "public");
        push_key_part(&mut key, "t");

        assert!(key_parts(&key, 1).is_none(), "trailing bytes");
        assert!(key_parts(&key, 3).is_none(), "truncated");
        assert!(key_parts(&key[..key.len() - 1], 2).is_none(), "short part");
        assert!(key_parts(&key[1..], 2).is_none(), "misaligned");

        let mut invalid = Vec::new();
        push_key_part(&mut invalid, "ok");
        crate::keyenc::put_u32(&mut invalid, 1);
        invalid.push(0xff);
        assert!(key_parts(&invalid, 2).is_none(), "not utf-8");
    }

    #[test]
    fn sequence_prefix_matches_sequence_keys() {
        let prefix = seq_prefix();

        assert!(!prefix.is_empty(), "seq_prefix must be non-empty");
        assert!(seq_key(0).starts_with(&prefix));
        assert!(seq_key(u32::MAX).starts_with(&prefix));
        assert!(!clog_key(0).starts_with(&prefix));
    }

    #[test]
    fn clog_prefix_matches_clog_keys() {
        let prefix = clog_prefix();

        assert!(!prefix.is_empty(), "clog_prefix must be non-empty");
        assert!(clog_key(0).starts_with(&prefix));
        assert!(clog_key(u64::MAX).starts_with(&prefix));
        assert!(!seq_key(0).starts_with(&prefix));
    }

    #[test]
    fn meta_next_global_xid_key_is_distinct_from_all_other_meta_keys() {
        let gxid = meta_next_global_xid_key();
        assert_ne!(gxid, next_xid_key(), "distinct from next_xid");
        assert_ne!(
            gxid,
            meta_next_table_id_key(),
            "distinct from next_table_id"
        );
        assert_ne!(gxid, meta_range_map_key(), "distinct from range_map");
        assert!(
            gxid.starts_with(&table_zero_prefix()),
            "under table-zero prefix"
        );
    }

    #[test]
    fn system_keys_do_not_collide_with_user_rows() {
        assert!(!catalog_key("public", "t").starts_with(&table_prefix(1)));
        assert!(!seq_key(1).starts_with(&table_prefix(1)));
    }

    #[test]
    fn meta_range_map_key_is_under_table_zero_meta() {
        let k = meta_range_map_key();
        assert!(
            k.starts_with(&table_zero_prefix()),
            "range map key is under table 0"
        );
        assert_ne!(k, meta_next_table_id_key(), "distinct from next_table_id");
        assert_ne!(k, next_xid_key(), "distinct from next_xid");
    }

    #[test]
    fn xid_and_clog_keys_are_under_table_zero_and_distinct() {
        let zero = table_zero_prefix();
        assert!(next_xid_key().starts_with(&zero));
        assert!(clog_key(5).starts_with(&zero));
        assert_ne!(clog_key(5), clog_key(6));
        assert_ne!(next_xid_key(), meta_next_table_id_key());
        assert!(clog_key(5) < clog_key(6));
    }

    #[test]
    fn clog_scan_end_is_above_every_clog_key() {
        assert!(clog_scan_end() > clog_key(u64::MAX));
        assert!(!clog_scan_lo_key().starts_with(&clog_prefix()));
        assert!(clog_key(0) >= clog_prefix() && clog_key(0) < clog_scan_end());
        let lo = clog_scan_lo_key();
        assert!(lo < clog_prefix() || lo >= clog_scan_end());
    }

    #[test]
    fn clog_scan_lo_key_is_a_distinct_table_zero_meta_key() {
        let lo = clog_scan_lo_key();
        assert!(
            lo.starts_with(&table_zero_prefix()),
            "under the table-zero system prefix"
        );
        assert!(!lo.starts_with(&clog_prefix()));
        assert_ne!(lo, next_xid_key());
        assert_ne!(lo, meta_next_table_id_key());
        assert_ne!(lo, meta_next_global_xid_key());
        assert_ne!(lo, meta_range_map_key());
        assert_ne!(lo, clog_key(0));
    }

    #[test]
    fn clog_xid_of_roundtrips_only_clog_keys() {
        assert_eq!(clog_xid_of(&clog_key(42)), Some(42));
        assert_eq!(clog_xid_of(&clog_key(0)), Some(0));
        assert_eq!(clog_xid_of(&clog_key(u64::MAX)), Some(u64::MAX));
        assert_eq!(clog_xid_of(&next_xid_key()), None);
        let mut wrong = clog_key(42);
        wrong[0] ^= 0xFF;
        assert_eq!(clog_xid_of(&wrong), None);
        assert_eq!(clog_xid_of(&clog_prefix()), None);
    }

    #[test]
    fn classification_requires_exact_key_shapes() {
        let mut version = row_key(7, 42);
        put_u64(&mut version, 9);

        assert_eq!(
            classify_key(&version),
            KeyClass::PrimaryVersion {
                table_id: 7,
                rowid: 42,
                version: 9,
            }
        );
        assert_eq!(primary_version_of(&version), Some((7, 42, 9)));
        assert_eq!(
            classify_key(&hash_row_key(7, 1, 42)),
            KeyClass::HashPrimaryRow {
                table_id: 7,
                bucket: 1,
                rowid: 42
            }
        );
        assert_eq!(
            classify_key(&secondary_index_prefix(7, 1)),
            KeyClass::SecondaryIndex {
                table_id: 7,
                storage_index_id: 2,
            }
        );
        version.push(0);
        assert_eq!(classify_key(&version), KeyClass::Unknown);
    }

    #[test]
    fn classification_extracts_only_exact_sequence_keys() {
        assert_eq!(sequence_table_id_of(&seq_key(7)), Some(7));
        let mut trailing = seq_key(7);
        trailing.push(0);
        assert_eq!(sequence_table_id_of(&trailing), None);
        assert_eq!(
            classify_key(&seq_key(7)),
            KeyClass::Sequence { table_id: 7 }
        );
    }

    #[test]
    fn fdw_server_umap_keys_are_non_empty_and_distinct() {
        let fdw_a = fdw_key("a");
        let fdw_b = fdw_key("b");
        let srv_a = server_key("a");
        let umap = user_mapping_key("alice", "s");
        let cat = catalog_key("public", "a");

        assert!(!fdw_a.is_empty());
        assert!(!srv_a.is_empty());
        assert!(!umap.is_empty());
        assert_ne!(fdw_a, fdw_b, "fdw_key includes the name");
        assert_ne!(srv_a, server_key("b"), "server_key includes the name");
        assert_ne!(
            user_mapping_key("alice", "s"),
            user_mapping_key("bob", "s"),
            "user_mapping_key includes the user"
        );
        assert_ne!(
            user_mapping_key("alice", "s1"),
            user_mapping_key("alice", "s2"),
            "user_mapping_key includes the server"
        );
        assert_ne!(fdw_a, srv_a, "fdw and server keys are distinct");
        assert_ne!(fdw_a, cat, "fdw and catalog keys are distinct");
        assert_ne!(srv_a, cat, "server and catalog keys are distinct");
        assert_ne!(umap, srv_a, "umap and server keys are distinct");
    }

    #[test]
    fn server_prefix_is_a_prefix_of_server_key() {
        let prefix = server_prefix();
        assert!(!prefix.is_empty(), "server_prefix must be non-empty");
        assert!(server_key("kafka").starts_with(&prefix));
        assert!(server_key("pg").starts_with(&prefix));
        assert!(!fdw_key("x").starts_with(&prefix));
        assert!(!catalog_key("public", "t").starts_with(&prefix));
    }

    #[test]
    fn notify_keys_are_an_isolated_ordered_namespace() {
        let prefix = notify_prefix();

        assert!(notify_key(0).starts_with(&prefix));
        assert!(notify_key(u64::MAX).starts_with(&prefix));
        assert!(notify_key(1) < notify_key(2) && notify_key(2) < notify_key(10));
        assert!(notify_key(3).starts_with(&table_zero_prefix()));
        // No other namespace overlaps the notify prefix in either direction.
        for other in [
            clog_key(3),
            seq_key(3),
            catalog_key("public", "notify"),
            next_xid_key(),
            meta_range_map_key(),
            row_key(7, 3),
            secondary_index_prefix(7, 1),
        ] {
            assert!(!is_notify_key(&other), "{other:?}");
            assert!(!other.starts_with(&prefix), "{other:?}");
            assert!(!notify_key(3).starts_with(&other), "{other:?}");
        }
    }

    #[test]
    fn is_notify_key_covers_the_whole_namespace() {
        use assert2::assert;

        let mut malformed = notify_prefix();
        malformed.extend_from_slice(b"garbage");

        assert!(is_notify_key(&notify_key(7)));
        assert!(is_notify_key(&notify_prefix()));
        // A truncated or over-long key under the prefix is still a notify key:
        // the predicate guards the store, so it must not admit junk.
        assert!(is_notify_key(&malformed));
        let mut long = notify_key(7);
        long.push(0);
        assert!(is_notify_key(&long));
        assert!(!is_notify_key(&notify_prefix()[..4]));
        assert!(!is_notify_key(b""));
    }

    #[test]
    fn classification_reports_notify_keys_and_leaves_other_shapes_alone() {
        use assert2::assert;

        let mut version = row_key(7, 42);
        put_u64(&mut version, 9);
        let mut malformed_notify = notify_prefix();
        malformed_notify.extend_from_slice(b"garbage");

        let cases: &[(&str, Vec<u8>, KeyClass)] = &[
            ("notify record", notify_key(7), KeyClass::Notify),
            ("notify prefix", notify_prefix(), KeyClass::Notify),
            ("malformed notify", malformed_notify, KeyClass::Notify),
            (
                "primary row",
                row_key(7, 42),
                KeyClass::PrimaryRow {
                    table_id: 7,
                    rowid: 42,
                },
            ),
            (
                "primary version",
                version,
                KeyClass::PrimaryVersion {
                    table_id: 7,
                    rowid: 42,
                    version: 9,
                },
            ),
            (
                "hash primary row",
                hash_row_key(7, 1, 42),
                KeyClass::HashPrimaryRow {
                    table_id: 7,
                    bucket: 1,
                    rowid: 42,
                },
            ),
            (
                "secondary index",
                secondary_index_prefix(7, 1),
                KeyClass::SecondaryIndex {
                    table_id: 7,
                    storage_index_id: 2,
                },
            ),
            ("sequence", seq_key(7), KeyClass::Sequence { table_id: 7 }),
            ("clog", clog_key(7), KeyClass::Clog { xid: 7 }),
            ("catalog", catalog_key("public", "users"), KeyClass::System),
            ("meta range map", meta_range_map_key(), KeyClass::System),
            ("next xid", next_xid_key(), KeyClass::System),
            ("server", server_key("kafka"), KeyClass::System),
            ("short", b"abc".to_vec(), KeyClass::Unknown),
        ];

        for (name, key, class) in cases {
            assert!(classify_key(key) == *class, "{name}");
        }
    }

    #[test]
    fn topology_activation_receipts_have_an_isolated_tenant_namespace() {
        let prefix = topology_activation_receipt_prefix("tenant-a");
        let key = topology_activation_receipt_key("tenant-a", "split-7");

        assert!(key.starts_with(&prefix));
        assert_ne!(prefix, range_control_receipt_prefix("tenant-a"));
        assert_ne!(key, topology_activation_receipt_key("tenant-b", "split-7"));
    }
}
