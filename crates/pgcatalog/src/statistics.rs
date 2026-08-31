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
const VERSION: u8 = 5;

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
    /// Scalar `pg_statistic` fields for each expression in definition order.
    pub expression_stats: Vec<ExpressionStats>,
}

/// The scalar statistics `pg_stats_ext_exprs` exposes for one expression.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpressionStats {
    pub null_frac: Option<String>,
    pub avg_width: Option<i32>,
    pub n_distinct: Option<String>,
    pub most_common_vals: Option<String>,
    pub most_common_freqs: Option<String>,
}

/// One item in PostgreSQL's multi-column most-common-values list.
///
/// Frequencies stay as their shortest round-trippable float strings. The
/// durable catalog has no floating-point field encoding, and the SQL boundary
/// parses them back to `float8`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McvItem {
    pub values: Vec<Option<String>>,
    pub frequency: String,
    pub base_frequency: String,
}

/// Encode a multi-column MCV list for `pg_statistic_ext_data.stxdmcv`.
///
/// The physical PostgreSQL type is opaque. This self-delimiting text encoding
/// keeps Gres's text-backed catalog surface opaque too, while allowing the
/// built-in `pg_mcv_list_items` implementation to recover exact values.
#[must_use]
pub fn encode_mcv(items: &[McvItem]) -> String {
    let mut out = format!("mcv1{:08x}", items.len());
    for item in items {
        push_mcv_string(&mut out, &item.frequency);
        push_mcv_string(&mut out, &item.base_frequency);
        out.push_str(&format!("{:08x}", item.values.len()));
        for value in &item.values {
            match value {
                None => out.push('n'),
                Some(value) => {
                    out.push('s');
                    push_mcv_string(&mut out, value);
                }
            }
        }
    }
    out
}

/// Decode one payload created by [`encode_mcv`].
#[must_use]
pub fn decode_mcv(value: &str) -> Option<Vec<McvItem>> {
    let mut input = value.as_bytes();
    let (prefix, rest) = input.split_at_checked(4)?;
    if prefix != b"mcv1" {
        return None;
    }
    input = rest;
    let item_count = take_mcv_hex(&mut input)?;
    let mut items = Vec::with_capacity(item_count);
    for _ in 0..item_count {
        let frequency = take_mcv_string(&mut input)?;
        let base_frequency = take_mcv_string(&mut input)?;
        let value_count = take_mcv_hex(&mut input)?;
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            let (tag, rest) = input.split_first()?;
            input = rest;
            values.push(match tag {
                b'n' => None,
                b's' => Some(take_mcv_string(&mut input)?),
                _ => return None,
            });
        }
        items.push(McvItem {
            values,
            frequency,
            base_frequency,
        });
    }
    input.is_empty().then_some(items)
}

fn push_mcv_string(out: &mut String, value: &str) {
    out.push_str(&format!("{:08x}", value.len()));
    out.push_str(value);
}

fn take_mcv_hex(input: &mut &[u8]) -> Option<usize> {
    let (digits, rest) = input.split_at_checked(8)?;
    *input = rest;
    usize::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok()
}

fn take_mcv_string(input: &mut &[u8]) -> Option<String> {
    let len = take_mcv_hex(input)?;
    let (bytes, rest) = input.split_at_checked(len)?;
    *input = rest;
    String::from_utf8(bytes.to_vec()).ok()
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
    /// The `stxdinherit = false` payload, absent until `ANALYZE` derives it.
    pub data: Option<StatisticsData>,
    /// The `stxdinherit = true` payload for an inherited or partitioned scan.
    pub inherited_data: Option<StatisticsData>,
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
    push_data(&mut out, &object.data);
    push_data(&mut out, &object.inherited_data);
    out
}

fn push_data(out: &mut Vec<u8>, data: &Option<StatisticsData>) {
    match data {
        None => out.push(0),
        Some(data) => {
            out.push(1);
            out.push(u8::from(data.inherited));
            push_optional_string(out, data.ndistinct.as_deref());
            push_optional_string(out, data.dependencies.as_deref());
            push_optional_string(out, data.mcv.as_deref());
            push_expression_stats(out, &data.expression_stats);
        }
    }
}

fn decode(value: &[u8]) -> Result<Statistics, CatalogError> {
    let mut input = value;
    let version = take_u8(&mut input)?;
    if version != VERSION {
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
    let data = take_data(&mut input)?;
    let inherited_data = take_data(&mut input)?;
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
        inherited_data,
    })
}

fn take_data(input: &mut &[u8]) -> Result<Option<StatisticsData>, CatalogError> {
    match take_u8(input)? {
        0 => Ok(None),
        1 => Ok(Some(StatisticsData {
            inherited: take_u8(input)? != 0,
            ndistinct: take_optional_string(input)?,
            dependencies: take_optional_string(input)?,
            mcv: take_optional_string(input)?,
            expression_stats: take_expression_stats(input)?,
        })),
        _ => return Err(corrupt("invalid statistics data presence")),
    }
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

fn push_expression_stats(out: &mut Vec<u8>, stats: &[ExpressionStats]) {
    let len = u16::try_from(stats.len()).expect("expression statistics count exceeds u16");
    out.extend_from_slice(&len.to_be_bytes());
    for stat in stats {
        push_optional_string(out, stat.null_frac.as_deref());
        match stat.avg_width {
            None => out.push(0),
            Some(width) => {
                out.push(1);
                out.extend_from_slice(&width.to_be_bytes());
            }
        }
        push_optional_string(out, stat.n_distinct.as_deref());
        push_optional_string(out, stat.most_common_vals.as_deref());
        push_optional_string(out, stat.most_common_freqs.as_deref());
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

fn take_expression_stats(input: &mut &[u8]) -> Result<Vec<ExpressionStats>, CatalogError> {
    let count = usize::from(take_u16(input)?);
    (0..count)
        .map(|_| {
            let null_frac = take_optional_string(input)?;
            let avg_width = match take_u8(input)? {
                0 => None,
                1 => Some(i32::from_be_bytes(take(input)?)),
                _ => return Err(corrupt("invalid expression statistics width presence")),
            };
            Ok(ExpressionStats {
                null_frac,
                avg_width,
                n_distinct: take_optional_string(input)?,
                most_common_vals: take_optional_string(input)?,
                most_common_freqs: take_optional_string(input)?,
            })
        })
        .collect()
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
        ExpressionStats, McvItem, STATISTICS_OID_BASE, Statistics, StatisticsData, create_ops,
        decode, decode_mcv, drop_for_table_ops, drop_ops, encode, encode_mcv, get, list, next_oid,
        rename_ops,
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
            inherited_data: None,
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
            expression_stats: vec![ExpressionStats {
                null_frac: Some("0.25".into()),
                avg_width: Some(12),
                n_distinct: Some("3".into()),
                most_common_vals: Some("{x}".into()),
                most_common_freqs: Some("{1}".into()),
            }],
        });
        record.inherited_data = Some(StatisticsData {
            inherited: true,
            ndistinct: Some(r#"{"1, 2": 4}"#.into()),
            dependencies: Some(r#"{"1 => 2": 1.000000}"#.into()),
            mcv: None,
            expression_stats: Vec::new(),
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
        assert!(
            get(&kv, &RelationName::public("s"))
                .expect("get")
                .expect("present")
                .inherited_data
                == record.inherited_data
        );
        let mut older = encode(&record);
        older[0] -= 1;
        assert!(decode(&older).is_err());
    }

    #[test]
    fn mcv_payload_round_trips_nulls_and_delimiters() {
        let items = vec![McvItem {
            values: vec![Some("comma, brace{} and \\ slash".into()), None],
            frequency: "0.6666666666666666".into(),
            base_frequency: "0.5".into(),
        }];
        assert!(decode_mcv(&encode_mcv(&items)) == Some(items));
        assert!(decode_mcv("mcv1not-a-list").is_none());
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
