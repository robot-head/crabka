//! Durable extended-statistics object metadata.
//!
//! This is the catalog half of `CREATE STATISTICS`. The object definition and
//! its `ANALYZE`-derived payload share one lifecycle: renames preserve both,
//! while dropping the object or its table removes both atomically.

use crabka_pgkv::{Kv, KvError, WriteOp, key::push_key_part};

use crate::{CatalogError, CommentObject, RelationName, TableId, set_comment_op};

/// The first OID handed to an extended-statistics object.
pub const STATISTICS_OID_BASE: u32 = 180_000;
const PREFIX: &[u8] = b"\0\0\0\0catalog_statistics/";
const NEXT_OID_KEY: &[u8] = b"\0\0\0\0meta/next_statistics_oid";
const VERSION: u8 = 2;
const PREVIOUS_VERSION: u8 = 1;

/// The derived `pg_statistic_ext_data` payload for one statistics object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatisticsData {
    pub inherited: bool,
    /// PostgreSQL's serialized `pg_ndistinct` map.
    pub ndistinct: Option<String>,
    /// PostgreSQL's serialized functional-dependency data.
    pub dependencies: Option<String>,
    /// PostgreSQL's serialized multi-column MCV list.
    pub mcv: Option<String>,
}

/// One `pg_statistic_ext` object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statistics {
    pub oid: u32,
    pub name: RelationName,
    pub table_id: TableId,
    pub owner: String,
    /// `stxstattarget`; `-1` means the server default.
    pub target: i16,
    /// One-based ordinary attribute numbers. Expressions are represented by
    /// zero in `pg_statistic_ext.stxkeys` and their text lives in `expressions`.
    pub keys: Vec<i16>,
    /// PostgreSQL's single-letter `stxkind` values (`d`, `f`, `m`, `e`).
    pub kinds: Vec<String>,
    /// Deparsed expressions in definition order.
    pub expressions: Vec<String>,
    /// Absent until `ANALYZE` has derived the payload, or when its target is zero.
    pub data: Option<StatisticsData>,
}

fn key(name: &RelationName) -> Vec<u8> {
    let mut out = PREFIX.to_vec();
    push_key_part(&mut out, &name.schema);
    push_key_part(&mut out, &name.name);
    out
}

/// The next OID that [`create_ops`] will allocate.
pub fn next_oid(kv: &dyn Kv) -> Result<u32, CatalogError> {
    match kv.get(NEXT_OID_KEY)? {
        Some(value) => value
            .as_slice()
            .try_into()
            .map(u32::from_be_bytes)
            .map_err(|_| KvError::CorruptRow("statistics oid counter is not u32".into()).into()),
        None => Ok(STATISTICS_OID_BASE),
    }
}

/// Look up one object by its schema-qualified name.
pub fn get(kv: &dyn Kv, name: &RelationName) -> Result<Option<Statistics>, CatalogError> {
    kv.get(&key(name))?.map(|value| decode(&value)).transpose()
}

/// Every object in name order.
pub fn list(kv: &dyn Kv) -> Result<Vec<Statistics>, CatalogError> {
    let mut objects = kv
        .scan_prefix(PREFIX)?
        .into_iter()
        .map(|(_, value)| decode(&value))
        .collect::<Result<Vec<_>, _>>()?;
    objects.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(objects)
}

/// Build atomic creation and OID-counter writes.
pub fn create_ops(kv: &dyn Kv, object: &Statistics) -> Result<Vec<WriteOp>, CatalogError> {
    if get(kv, &object.name)?.is_some() {
        return Err(CatalogError::DuplicateObject(object.name.to_string()));
    }
    let oid = next_oid(kv)?;
    let next = oid
        .checked_add(1)
        .ok_or_else(|| KvError::CorruptRow("statistics oid counter overflow".into()))?;
    let stored = Statistics {
        oid,
        ..object.clone()
    };
    Ok(vec![
        WriteOp::Put {
            key: NEXT_OID_KEY.to_vec(),
            value: next.to_be_bytes().to_vec(),
        },
        put_op(&stored),
    ])
}

/// Replace a stored definition without changing its OID or identity.
#[must_use]
pub fn put_op(object: &Statistics) -> WriteOp {
    WriteOp::Put {
        key: key(&object.name),
        value: encode(object),
    }
}

/// Drop one object.
pub fn drop_ops(kv: &dyn Kv, name: &RelationName) -> Result<Vec<WriteOp>, CatalogError> {
    let object = get(kv, name)?.ok_or_else(|| CatalogError::UndefinedObject(name.to_string()))?;
    let oid = object.oid.to_string();
    Ok(vec![
        WriteOp::Delete { key: key(name) },
        set_comment_op("statistics", CommentObject::Named(&oid), None),
    ])
}

/// Rename an object, preserving its OID and every definition field.
pub fn rename_ops(
    kv: &dyn Kv,
    old: &RelationName,
    new: &RelationName,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut object = get(kv, old)?.ok_or_else(|| CatalogError::UndefinedObject(old.to_string()))?;
    if get(kv, new)?.is_some() {
        return Err(CatalogError::DuplicateObject(new.to_string()));
    }
    object.name.clone_from(new);
    Ok(vec![WriteOp::Delete { key: key(old) }, put_op(&object)])
}

/// Delete every object tied to a departed table.
pub fn drop_for_table_ops(kv: &dyn Kv, table_id: TableId) -> Result<Vec<WriteOp>, CatalogError> {
    Ok(list(kv)?
        .into_iter()
        .filter(|object| object.table_id == table_id)
        .flat_map(|object| {
            let oid = object.oid.to_string();
            [
                WriteOp::Delete {
                    key: key(&object.name),
                },
                set_comment_op("statistics", CommentObject::Named(&oid), None),
            ]
        })
        .collect())
}

fn encode(object: &Statistics) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(VERSION);
    out.extend_from_slice(&object.oid.to_be_bytes());
    out.extend_from_slice(&object.table_id.to_be_bytes());
    out.extend_from_slice(&object.target.to_be_bytes());
    push_string(&mut out, &object.name.schema);
    push_string(&mut out, &object.name.name);
    push_string(&mut out, &object.owner);
    let key_count = u16::try_from(object.keys.len()).expect("statistics key count exceeds u16");
    out.extend_from_slice(&key_count.to_be_bytes());
    for key in &object.keys {
        out.extend_from_slice(&key.to_be_bytes());
    }
    push_strings(&mut out, &object.kinds);
    push_strings(&mut out, &object.expressions);
    match &object.data {
        None => out.push(0),
        Some(data) => {
            out.push(1);
            out.push(u8::from(data.inherited));
            push_optional_string(&mut out, data.ndistinct.as_deref());
            push_optional_string(&mut out, data.dependencies.as_deref());
            push_optional_string(&mut out, data.mcv.as_deref());
        }
    }
    out
}

fn decode(value: &[u8]) -> Result<Statistics, CatalogError> {
    let mut input = value;
    let version = take_u8(&mut input)?;
    if version != PREVIOUS_VERSION && version != VERSION {
        return Err(corrupt("unknown statistics record version"));
    }
    let oid = take_u32(&mut input)?;
    let table_id = take_u32(&mut input)?;
    let target = take_i16(&mut input)?;
    let name = RelationName::new(take_string(&mut input)?, take_string(&mut input)?);
    let owner = take_string(&mut input)?;
    let key_count = usize::from(take_u16(&mut input)?);
    let mut keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        keys.push(take_i16(&mut input)?);
    }
    let kinds = take_strings(&mut input)?;
    let expressions = take_strings(&mut input)?;
    let data = if version == PREVIOUS_VERSION {
        None
    } else {
        match take_u8(&mut input)? {
            0 => None,
            1 => Some(StatisticsData {
                inherited: take_u8(&mut input)? != 0,
                ndistinct: take_optional_string(&mut input)?,
                dependencies: take_optional_string(&mut input)?,
                mcv: take_optional_string(&mut input)?,
            }),
            _ => return Err(corrupt("invalid statistics data presence")),
        }
    };
    if !input.is_empty() {
        return Err(corrupt("trailing statistics record bytes"));
    }
    Ok(Statistics {
        oid,
        name,
        table_id,
        owner,
        target,
        keys,
        kinds,
        expressions,
        data,
    })
}

fn push_string(out: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).expect("statistics string exceeds u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn push_strings(out: &mut Vec<u8>, values: &[String]) {
    let len = u16::try_from(values.len()).expect("statistics list exceeds u16");
    out.extend_from_slice(&len.to_be_bytes());
    for value in values {
        push_string(out, value);
    }
}

fn push_optional_string(out: &mut Vec<u8>, value: Option<&str>) {
    out.push(u8::from(value.is_some()));
    if let Some(value) = value {
        push_string(out, value);
    }
}

fn take_u8(input: &mut &[u8]) -> Result<u8, CatalogError> {
    let (&value, rest) = input
        .split_first()
        .ok_or_else(|| corrupt("truncated statistics record"))?;
    *input = rest;
    Ok(value)
}

fn take<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], CatalogError> {
    let (bytes, rest) = input
        .split_first_chunk::<N>()
        .ok_or_else(|| corrupt("truncated statistics record"))?;
    *input = rest;
    Ok(*bytes)
}

fn take_u16(input: &mut &[u8]) -> Result<u16, CatalogError> {
    Ok(u16::from_be_bytes(take(input)?))
}

fn take_i16(input: &mut &[u8]) -> Result<i16, CatalogError> {
    Ok(i16::from_be_bytes(take(input)?))
}

fn take_u32(input: &mut &[u8]) -> Result<u32, CatalogError> {
    Ok(u32::from_be_bytes(take(input)?))
}

fn take_string(input: &mut &[u8]) -> Result<String, CatalogError> {
    let len = usize::try_from(take_u32(input)?).expect("u32 fits usize");
    let (bytes, rest) = input
        .split_at_checked(len)
        .ok_or_else(|| corrupt("truncated statistics string"))?;
    *input = rest;
    String::from_utf8(bytes.to_vec()).map_err(|_| corrupt("statistics string is not UTF-8"))
}

fn take_optional_string(input: &mut &[u8]) -> Result<Option<String>, CatalogError> {
    match take_u8(input)? {
        0 => Ok(None),
        1 => take_string(input).map(Some),
        _ => Err(corrupt("invalid optional statistics string presence")),
    }
}

fn take_strings(input: &mut &[u8]) -> Result<Vec<String>, CatalogError> {
    let count = usize::from(take_u16(input)?);
    (0..count).map(|_| take_string(input)).collect()
}

fn corrupt(message: &'static str) -> CatalogError {
    CatalogError::Storage(KvError::CorruptRow(message.into()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv};

    use super::{
        STATISTICS_OID_BASE, Statistics, StatisticsData, create_ops, drop_for_table_ops, drop_ops,
        get, list, next_oid, rename_ops,
    };
    use crate::{CommentObject, RelationName, get_comment, set_comment_op};

    fn object(name: &str, table_id: u32) -> Statistics {
        Statistics {
            oid: 0,
            name: RelationName::public(name),
            table_id,
            owner: "alice".into(),
            target: -1,
            keys: vec![1, 0, 2],
            kinds: vec!["d".into(), "m".into()],
            expressions: vec!["(b + 1)".into()],
            data: None,
        }
    }

    #[test]
    fn durable_statistics_allocate_rename_and_follow_table_drop() {
        let kv = MemKv::new();
        assert!(next_oid(&kv).expect("next oid") == STATISTICS_OID_BASE);
        kv.write_batch(&create_ops(&kv, &object("s", 42)).expect("create"))
            .expect("write");
        let stored = get(&kv, &RelationName::public("s"))
            .expect("get")
            .expect("present");
        assert!(stored.oid == STATISTICS_OID_BASE);
        assert!(stored.keys == [1, 0, 2]);
        assert!(stored.expressions == ["(b + 1)"]);

        let renamed = RelationName::new("audit", "s2");
        kv.write_batch(&rename_ops(&kv, &RelationName::public("s"), &renamed).expect("rename"))
            .expect("write");
        assert!(get(&kv, &RelationName::public("s")).expect("get").is_none());
        assert!(get(&kv, &renamed).expect("get").expect("renamed").oid == STATISTICS_OID_BASE);

        kv.write_batch(&drop_for_table_ops(&kv, 42).expect("drop table"))
            .expect("write");
        assert!(list(&kv).expect("list").is_empty());
    }

    #[test]
    fn derived_data_round_trips_with_the_definition() {
        let kv = MemKv::new();
        let mut record = object("s", 42);
        record.data = Some(StatisticsData {
            inherited: false,
            ndistinct: Some(r#"{"1, 2": 3}"#.into()),
            dependencies: None,
            mcv: Some("mcv".into()),
        });
        kv.write_batch(&create_ops(&kv, &record).expect("create"))
            .expect("write");
        assert!(
            get(&kv, &RelationName::public("s"))
                .expect("get")
                .expect("present")
                .data
                == record.data
        );
    }

    #[test]
    fn dropping_statistics_removes_its_comment() {
        let kv = MemKv::new();
        let name = RelationName::public("s");
        kv.write_batch(&create_ops(&kv, &object("s", 42)).expect("create"))
            .expect("write");
        let oid = get(&kv, &name)
            .expect("get")
            .expect("present")
            .oid
            .to_string();
        kv.write_batch(&[set_comment_op(
            "statistics",
            CommentObject::Named(&oid),
            Some("comment"),
        )])
        .expect("comment");
        kv.write_batch(&drop_ops(&kv, &name).expect("drop"))
            .expect("write drop");
        assert!(
            get_comment(&kv, "statistics", CommentObject::Named(&oid))
                .expect("comment")
                .is_none()
        );
    }
}
