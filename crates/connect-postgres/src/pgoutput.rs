use std::collections::BTreeMap;

use crate::{
    PgLsn, PostgresConnectError,
    ids::{CommitLsn, EndLsn, RelationId, TransactionId},
    model::{
        ColumnSchema, ColumnValue, EntityDifference, EntityKey, Operation, ScalarValue, TableSchema,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationEvent {
    pub relation_id: RelationId,
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowTupleKind {
    Full,
    Key,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowEventKind {
    Insert,
    Update {
        old: Vec<ColumnValue>,
        old_tuple_kind: RowTupleKind,
    },
    Delete {
        tuple_kind: RowTupleKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowEvent {
    pub relation_id: RelationId,
    pub lsn: PgLsn,
    pub commit_lsn: Option<PgLsn>,
    pub txid: Option<TransactionId>,
    pub commit_timestamp_ms: Option<i64>,
    pub kind: RowEventKind,
    pub values: Vec<ColumnValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedMessage {
    Begin {
        final_lsn: PgLsn,
        xid: TransactionId,
    },
    Commit {
        commit_lsn: CommitLsn,
        end_lsn: EndLsn,
        commit_timestamp_ms: i64,
    },
    Relation(RelationEvent),
    Row(RowEvent),
    Keepalive,
}

pub fn decode_pgoutput_message(
    bytes: &[u8],
    lsn: PgLsn,
    txid: Option<TransactionId>,
) -> Result<DecodedMessage, PostgresConnectError> {
    let mut reader = PgOutputReader::new(bytes);
    let tag = reader.read_u8("message tag")?;

    let message = match tag {
        b'B' => {
            let final_lsn = PgLsn(reader.read_u64("begin final_lsn")?);
            let _commit_time = reader.read_i64("begin commit_time")?;
            let xid = TransactionId(i64::from(reader.read_u32("begin xid")?));
            DecodedMessage::Begin { final_lsn, xid }
        }
        b'C' => {
            let _flags = reader.read_u8("commit flags")?;
            let commit_lsn = CommitLsn(PgLsn(reader.read_u64("commit commit_lsn")?));
            let end_lsn = EndLsn(PgLsn(reader.read_u64("commit end_lsn")?));
            let commit_timestamp_ms =
                postgres_timestamp_micros_to_unix_ms(reader.read_i64("commit commit_time")?);
            DecodedMessage::Commit {
                commit_lsn,
                end_lsn,
                commit_timestamp_ms,
            }
        }
        b'R' => DecodedMessage::Relation(decode_relation(&mut reader)?),
        b'I' => DecodedMessage::Row(decode_insert(&mut reader, lsn, txid)?),
        b'U' => DecodedMessage::Row(decode_update(&mut reader, lsn, txid)?),
        b'D' => DecodedMessage::Row(decode_delete(&mut reader, lsn, txid)?),
        tag => {
            return Err(PostgresConnectError::Backend(format!(
                "unsupported pgoutput message tag {:?}",
                char::from(tag)
            )));
        }
    };

    reader.finish()?;
    Ok(message)
}

fn decode_relation(reader: &mut PgOutputReader<'_>) -> Result<RelationEvent, PostgresConnectError> {
    let relation_id = RelationId(reader.read_u32("relation id")?);
    let schema = reader.read_cstr("relation namespace")?;
    let table = reader.read_cstr("relation name")?;
    let _replica_identity = reader.read_u8("relation replica identity")?;
    let column_count = reader.read_count("relation column count")?;
    let mut columns = Vec::with_capacity(column_count);

    for index in 0..column_count {
        let flags = reader.read_u8("relation column flags")?;
        let name = reader.read_cstr("relation column name")?;
        let type_oid = reader.read_i32("relation column type oid")?;
        let _atttypmod = reader.read_i32("relation column atttypmod")?;
        columns.push(ColumnSchema {
            name,
            type_name: type_name(type_oid).to_owned(),
            key: flags & 1 == 1,
        });

        if columns[index].name.is_empty() {
            return Err(PostgresConnectError::Backend(format!(
                "invalid pgoutput relation column {index}: empty name"
            )));
        }
    }

    Ok(RelationEvent {
        relation_id,
        schema,
        table,
        columns,
    })
}

fn decode_insert(
    reader: &mut PgOutputReader<'_>,
    lsn: PgLsn,
    txid: Option<TransactionId>,
) -> Result<RowEvent, PostgresConnectError> {
    let relation_id = RelationId(reader.read_u32("insert relation id")?);
    reader.expect_u8(b'N', "insert tuple tag N")?;
    let values = decode_tuple(reader)?;

    Ok(RowEvent {
        relation_id,
        lsn,
        commit_lsn: None,
        txid,
        commit_timestamp_ms: None,
        kind: RowEventKind::Insert,
        values,
    })
}

fn decode_update(
    reader: &mut PgOutputReader<'_>,
    lsn: PgLsn,
    txid: Option<TransactionId>,
) -> Result<RowEvent, PostgresConnectError> {
    let relation_id = RelationId(reader.read_u32("update relation id")?);
    let tag = reader.read_u8("update tuple tag")?;
    let (old, old_tuple_kind, new_tag) = match tag {
        b'K' | b'O' => (
            decode_tuple(reader)?,
            tuple_kind(tag),
            reader.read_u8("update new tuple tag")?,
        ),
        b'N' => (Vec::new(), RowTupleKind::Full, b'N'),
        tag => {
            return Err(PostgresConnectError::Backend(format!(
                "invalid pgoutput update tuple tag {:?}",
                char::from(tag)
            )));
        }
    };

    if new_tag != b'N' {
        return Err(PostgresConnectError::Backend(format!(
            "invalid pgoutput update new tuple tag {:?}",
            char::from(new_tag)
        )));
    }

    let values = decode_tuple(reader)?;
    Ok(RowEvent {
        relation_id,
        lsn,
        commit_lsn: None,
        txid,
        commit_timestamp_ms: None,
        kind: RowEventKind::Update {
            old,
            old_tuple_kind,
        },
        values,
    })
}

fn decode_delete(
    reader: &mut PgOutputReader<'_>,
    lsn: PgLsn,
    txid: Option<TransactionId>,
) -> Result<RowEvent, PostgresConnectError> {
    let relation_id = RelationId(reader.read_u32("delete relation id")?);
    let tag = reader.read_u8("delete tuple tag")?;
    if !matches!(tag, b'K' | b'O') {
        return Err(PostgresConnectError::Backend(format!(
            "invalid pgoutput delete tuple tag {:?}",
            char::from(tag)
        )));
    }
    let values = decode_tuple(reader)?;

    Ok(RowEvent {
        relation_id,
        lsn,
        commit_lsn: None,
        txid,
        commit_timestamp_ms: None,
        kind: RowEventKind::Delete {
            tuple_kind: tuple_kind(tag),
        },
        values,
    })
}

fn tuple_kind(tag: u8) -> RowTupleKind {
    if tag == b'K' {
        RowTupleKind::Key
    } else {
        RowTupleKind::Full
    }
}

fn decode_tuple(reader: &mut PgOutputReader<'_>) -> Result<Vec<ColumnValue>, PostgresConnectError> {
    let value_count = reader.read_count("tuple value count")?;
    let mut values = Vec::with_capacity(value_count);

    for index in 0..value_count {
        let value_tag = reader.read_u8("tuple value tag")?;
        let value = match value_tag {
            b'n' => ScalarValue::Null,
            b'u' => ScalarValue::UnchangedToast,
            b't' => {
                let bytes = reader.read_len_bytes("text tuple value")?;
                let value = std::str::from_utf8(bytes).map_err(|error| {
                    PostgresConnectError::Backend(format!(
                        "invalid utf8 in pgoutput text tuple value: {error}"
                    ))
                })?;
                ScalarValue::Text(value.to_owned())
            }
            b'b' => ScalarValue::Bytes(reader.read_len_bytes("binary tuple value")?.to_vec()),
            tag => {
                return Err(PostgresConnectError::Backend(format!(
                    "unsupported pgoutput tuple value tag {:?}",
                    char::from(tag)
                )));
            }
        };

        values.push(ColumnValue {
            name: format!("col{index}"),
            value,
        });
    }

    Ok(values)
}

fn type_name(type_oid: i32) -> &'static str {
    match type_oid {
        16 => "bool",
        17 => "bytea",
        20 => "int8",
        21 => "int2",
        23 => "int4",
        25 => "text",
        700 => "float4",
        701 => "float8",
        1043 => "varchar",
        1700 => "numeric",
        _ => "unknown",
    }
}

fn postgres_timestamp_micros_to_unix_ms(value: i64) -> i64 {
    const POSTGRES_EPOCH_UNIX_MS: i64 = 946_684_800_000;
    POSTGRES_EPOCH_UNIX_MS + value.div_euclid(1_000)
}

struct PgOutputReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> PgOutputReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn finish(&self) -> Result<(), PostgresConnectError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(PostgresConnectError::Backend(format!(
                "invalid pgoutput message: {} trailing bytes",
                self.bytes.len() - self.position
            )))
        }
    }

    fn read_u8(&mut self, field: &str) -> Result<u8, PostgresConnectError> {
        let bytes = self.read_exact(1, field)?;
        Ok(bytes[0])
    }

    fn expect_u8(&mut self, expected: u8, field: &str) -> Result<(), PostgresConnectError> {
        let actual = self.read_u8(field)?;
        if actual == expected {
            Ok(())
        } else {
            Err(PostgresConnectError::Backend(format!(
                "invalid pgoutput {field}: expected {:?}, got {:?}",
                char::from(expected),
                char::from(actual)
            )))
        }
    }

    fn read_i16(&mut self, field: &str) -> Result<i16, PostgresConnectError> {
        let bytes = self.read_exact(2, field)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_count(&mut self, field: &str) -> Result<usize, PostgresConnectError> {
        let count = self.read_i16(field)?;
        usize::try_from(count).map_err(|_| {
            PostgresConnectError::Backend(format!("invalid pgoutput {field}: negative count"))
        })
    }

    fn read_i32(&mut self, field: &str) -> Result<i32, PostgresConnectError> {
        let bytes = self.read_exact(4, field)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u32(&mut self, field: &str) -> Result<u32, PostgresConnectError> {
        let bytes = self.read_exact(4, field)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self, field: &str) -> Result<i64, PostgresConnectError> {
        let bytes = self.read_exact(8, field)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_u64(&mut self, field: &str) -> Result<u64, PostgresConnectError> {
        let bytes = self.read_exact(8, field)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_len_bytes(&mut self, field: &str) -> Result<&'a [u8], PostgresConnectError> {
        let length = self.read_i32(field)?;
        let length = usize::try_from(length).map_err(|_| {
            PostgresConnectError::Backend(format!("invalid pgoutput {field}: negative length"))
        })?;
        self.read_exact(length, field)
    }

    fn read_cstr(&mut self, field: &str) -> Result<String, PostgresConnectError> {
        let remaining = &self.bytes[self.position..];
        let Some(length) = remaining.iter().position(|byte| *byte == 0) else {
            return Err(PostgresConnectError::Backend(format!(
                "truncated pgoutput {field}: missing null terminator"
            )));
        };
        let bytes = &remaining[..length];
        self.position += length + 1;
        let value = std::str::from_utf8(bytes).map_err(|error| {
            PostgresConnectError::Backend(format!("invalid utf8 in pgoutput {field}: {error}"))
        })?;
        Ok(value.to_owned())
    }

    fn read_exact(&mut self, length: usize, field: &str) -> Result<&'a [u8], PostgresConnectError> {
        let end = self.position.checked_add(length).ok_or_else(|| {
            PostgresConnectError::Backend(format!("invalid pgoutput {field}: length overflow"))
        })?;
        if end > self.bytes.len() {
            return Err(PostgresConnectError::Backend(format!(
                "truncated pgoutput {field}: needed {length} bytes, have {}",
                self.bytes.len().saturating_sub(self.position)
            )));
        }

        let bytes = &self.bytes[self.position..end];
        self.position = end;
        Ok(bytes)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RelationCache {
    relations: BTreeMap<RelationId, TableSchema>,
}

impl RelationCache {
    pub fn apply_relation(&mut self, event: RelationEvent) {
        self.relations.insert(
            event.relation_id,
            TableSchema {
                schema: event.schema,
                table: event.table,
                columns: event.columns,
            },
        );
    }

    pub fn translate(&self, event: RowEvent) -> Result<EntityDifference, PostgresConnectError> {
        let schema = self.relations.get(&event.relation_id).ok_or_else(|| {
            PostgresConnectError::Backend(format!(
                "missing relation metadata for relation id {}",
                event.relation_id
            ))
        })?;
        let table = format!("{}.{}", schema.schema, schema.table);
        let lsn = event.commit_lsn.unwrap_or(event.lsn);

        let (op, before, after, key_columns) = match event.kind {
            RowEventKind::Insert => {
                let values =
                    normalize_values(schema, event.values, RowTupleKind::Full, "row values")?;
                let key_columns = extract_key_columns(schema, &values)?;
                (Operation::Insert, Vec::new(), values, key_columns)
            }
            RowEventKind::Update {
                old,
                old_tuple_kind,
            } => {
                let values =
                    normalize_values(schema, event.values, RowTupleKind::Full, "row values")?;
                let key_columns = extract_key_columns(schema, &values)?;
                let old = if old.is_empty() {
                    old
                } else {
                    normalize_values(schema, old, old_tuple_kind, "old row values")?
                };
                if has_any_key_column(schema, &old) {
                    let old_key_columns = extract_key_columns(schema, &old)?;
                    if old_key_columns != key_columns {
                        return Err(PostgresConnectError::Backend(
                            "key-changing updates are not supported".to_owned(),
                        ));
                    }
                }

                (Operation::Update, old, values, key_columns)
            }
            RowEventKind::Delete { tuple_kind } => {
                let values = normalize_values(schema, event.values, tuple_kind, "row values")?;
                let key_columns = extract_key_columns(schema, &values)?;
                (Operation::Delete, values, Vec::new(), key_columns)
            }
        };
        let key = EntityKey {
            table: table.clone(),
            columns: key_columns,
        };

        Ok(EntityDifference {
            table,
            key,
            op,
            before,
            after,
            lsn,
            txid: event.txid,
            commit_timestamp_ms: event.commit_timestamp_ms,
            schema: schema.clone(),
        })
    }
}

fn normalize_values(
    schema: &TableSchema,
    values: Vec<ColumnValue>,
    tuple_kind: RowTupleKind,
    label: &str,
) -> Result<Vec<ColumnValue>, PostgresConnectError> {
    let expected_columns: Vec<&ColumnSchema> = match tuple_kind {
        RowTupleKind::Full => schema.columns.iter().collect(),
        RowTupleKind::Key => schema.columns.iter().filter(|column| column.key).collect(),
    };

    if values.len() != expected_columns.len() {
        return Err(PostgresConnectError::Backend(format!(
            "column count mismatch for {}.{} {label}: decoded {} values but expected {} columns",
            schema.schema,
            schema.table,
            values.len(),
            expected_columns.len()
        )));
    }

    if values.iter().all(|value| {
        expected_columns
            .iter()
            .any(|column| column.name == value.name)
    }) {
        return values
            .into_iter()
            .map(|value| {
                let column = expected_columns
                    .iter()
                    .find(|column| column.name == value.name)
                    .expect("validated column name should be present");
                Ok(ColumnValue {
                    name: value.name,
                    value: coerce_value(value.value, &column.type_name)?,
                })
            })
            .collect();
    }

    values
        .into_iter()
        .zip(expected_columns)
        .map(|(value, column)| {
            Ok(ColumnValue {
                name: column.name.clone(),
                value: coerce_value(value.value, &column.type_name)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

fn coerce_value(value: ScalarValue, type_name: &str) -> Result<ScalarValue, PostgresConnectError> {
    let ScalarValue::Text(text) = value else {
        return Ok(value);
    };

    match type_name {
        "bool" => coerce_bool(&text),
        "int2" | "int4" | "int8" => text.parse::<i64>().map(ScalarValue::Int).map_err(|error| {
            PostgresConnectError::Backend(format!(
                "invalid {type_name} pgoutput text value {text:?}: {error}"
            ))
        }),
        "float4" | "float8" | "numeric" => Ok(ScalarValue::Float(text)),
        "bytea" => Ok(ScalarValue::Bytes(coerce_bytea(&text)?)),
        _ => Ok(ScalarValue::Text(text)),
    }
}

fn coerce_bool(text: &str) -> Result<ScalarValue, PostgresConnectError> {
    match text {
        "t" | "true" | "1" => Ok(ScalarValue::Bool(true)),
        "f" | "false" | "0" => Ok(ScalarValue::Bool(false)),
        _ => Err(PostgresConnectError::Backend(format!(
            "invalid bool pgoutput text value {text:?}"
        ))),
    }
}

fn coerce_bytea(text: &str) -> Result<Vec<u8>, PostgresConnectError> {
    let Some(hex) = text.strip_prefix("\\x") else {
        return Ok(text.as_bytes().to_vec());
    };
    if hex.len() % 2 != 0 {
        return Err(PostgresConnectError::Backend(format!(
            "invalid bytea pgoutput text value {text:?}: odd hex length"
        )));
    }

    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|chunk| {
            let pair = std::str::from_utf8(chunk).map_err(|error| {
                PostgresConnectError::Backend(format!(
                    "invalid bytea pgoutput text value {text:?}: {error}"
                ))
            })?;
            u8::from_str_radix(pair, 16).map_err(|error| {
                PostgresConnectError::Backend(format!(
                    "invalid bytea pgoutput text value {text:?}: {error}"
                ))
            })
        })
        .collect()
}

fn extract_key_columns(
    schema: &TableSchema,
    values: &[ColumnValue],
) -> Result<Vec<ColumnValue>, PostgresConnectError> {
    schema
        .columns
        .iter()
        .filter(|column| column.key)
        .map(|column| {
            values
                .iter()
                .find(|value| value.name == column.name)
                .cloned()
                .ok_or_else(|| {
                    PostgresConnectError::Backend(format!(
                        "missing key column {:?} for {}.{}",
                        column.name, schema.schema, schema.table
                    ))
                })
        })
        .collect()
}

fn has_any_key_column(schema: &TableSchema, values: &[ColumnValue]) -> bool {
    schema
        .columns
        .iter()
        .filter(|column| column.key)
        .any(|column| values.iter().any(|value| value.name == column.name))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{
        RelationCache, RelationEvent, RowEvent, RowEventKind, RowTupleKind, coerce_bytea,
        coerce_value,
    };
    use crate::{
        PgLsn, PostgresConnectError,
        ids::{RelationId, TransactionId},
        model::{ColumnSchema, ColumnValue, Operation, ScalarValue, TableSchema},
    };

    fn orders_relation(type_name: &str) -> RelationEvent {
        RelationEvent {
            relation_id: RelationId(7),
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_owned(),
                    type_name: "int8".to_owned(),
                    key: true,
                },
                ColumnSchema {
                    name: "status".to_owned(),
                    type_name: type_name.to_owned(),
                    key: false,
                },
            ],
        }
    }

    fn id(value: i64) -> ColumnValue {
        ColumnValue {
            name: "id".to_owned(),
            value: ScalarValue::Int(value),
        }
    }

    fn status(value: &str) -> ColumnValue {
        ColumnValue {
            name: "status".to_owned(),
            value: ScalarValue::Text(value.to_owned()),
        }
    }

    fn tenant_id(value: i64) -> ColumnValue {
        ColumnValue {
            name: "tenant_id".to_owned(),
            value: ScalarValue::Int(value),
        }
    }

    fn composite_key_orders_relation() -> RelationEvent {
        RelationEvent {
            relation_id: RelationId(7),
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            columns: vec![
                ColumnSchema {
                    name: "tenant_id".to_owned(),
                    type_name: "int8".to_owned(),
                    key: true,
                },
                ColumnSchema {
                    name: "id".to_owned(),
                    type_name: "int8".to_owned(),
                    key: true,
                },
                ColumnSchema {
                    name: "status".to_owned(),
                    type_name: "text".to_owned(),
                    key: false,
                },
            ],
        }
    }

    #[test]
    fn insert_translates_to_entity_difference_with_key() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let values = vec![id(42), status("paid")];
        let difference = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x16_b374_d848),
                commit_lsn: None,
                txid: Some(TransactionId(99)),
                commit_timestamp_ms: Some(1_700_000_000_000),
                kind: RowEventKind::Insert,
                values: values.clone(),
            })
            .expect("relation should translate");

        check!(difference.table == "public.orders");
        check!(difference.key.table == "public.orders");
        check!(difference.key.columns == vec![id(42)]);
        check!(difference.op == Operation::Insert);
        check!(difference.before == Vec::new());
        check!(difference.after == values);
        check!(difference.lsn == PgLsn(0x16_b374_d848));
        check!(difference.txid == Some(TransactionId(99)));
        check!(difference.commit_timestamp_ms == Some(1_700_000_000_000));
        check!(difference.schema.table == "orders");
        check!(difference.schema.columns[0].key);
    }

    #[test]
    fn delete_translates_to_before_only_difference() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let values = vec![id(42), status("cancelled")];
        let difference = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x2a),
                commit_lsn: None,
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Delete {
                    tuple_kind: RowTupleKind::Full,
                },
                values: values.clone(),
            })
            .expect("relation should translate");

        check!(difference.table == "public.orders");
        check!(difference.key.columns == vec![id(42)]);
        check!(difference.op == Operation::Delete);
        check!(difference.before == values);
        check!(difference.after == Vec::new());
    }

    #[test]
    fn relation_refresh_changes_table_schema() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));
        cache.apply_relation(orders_relation("varchar"));

        let difference = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x2b),
                commit_lsn: None,
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Insert,
                values: vec![id(7), status("new")],
            })
            .expect("relation should translate");

        check!(difference.schema.columns[1].type_name == "varchar");
    }

    #[test]
    fn composite_key_output_follows_schema_order_even_if_row_values_are_out_of_order() {
        let mut cache = RelationCache::default();
        cache.apply_relation(composite_key_orders_relation());

        let difference = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x2c),
                commit_lsn: None,
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Insert,
                values: vec![status("new"), id(7), tenant_id(3)],
            })
            .expect("relation should translate");

        check!(difference.key.columns == vec![tenant_id(3), id(7)]);
    }

    #[test]
    fn missing_key_column_is_an_error() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let result = cache.translate(RowEvent {
            relation_id: RelationId(7),
            lsn: PgLsn(0x2d),
            commit_lsn: None,
            txid: None,
            commit_timestamp_ms: None,
            kind: RowEventKind::Insert,
            values: vec![status("new"), status("paid")],
        });

        let error = result.expect_err("missing key should fail");
        match error {
            PostgresConnectError::Backend(message) => {
                check!(message.contains("missing key column"));
                check!(message.contains("id"));
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }

    #[test]
    fn non_key_update_translates_with_before_after_and_stable_key() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let old = vec![id(42), status("new")];
        let new = vec![id(42), status("paid")];
        let difference = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x2e),
                commit_lsn: None,
                txid: Some(TransactionId(100)),
                commit_timestamp_ms: Some(1_700_000_000_001),
                kind: RowEventKind::Update {
                    old: old.clone(),
                    old_tuple_kind: RowTupleKind::Full,
                },
                values: new.clone(),
            })
            .expect("relation should translate");

        check!(difference.op == Operation::Update);
        check!(difference.before == old);
        check!(difference.after == new);
        check!(difference.key.columns == vec![id(42)]);
    }

    #[test]
    fn key_changing_update_is_an_error() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let result = cache.translate(RowEvent {
            relation_id: RelationId(7),
            lsn: PgLsn(0x2f),
            commit_lsn: None,
            txid: None,
            commit_timestamp_ms: None,
            kind: RowEventKind::Update {
                old: vec![id(41), status("new")],
                old_tuple_kind: RowTupleKind::Full,
            },
            values: vec![id(42), status("paid")],
        });

        let error = result.expect_err("key-changing update should fail");
        match error {
            PostgresConnectError::Backend(message) => {
                check!(message == "key-changing updates are not supported");
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }

    #[test]
    fn decoded_placeholder_values_bind_to_schema_names_before_key_extraction() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let difference = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x30),
                commit_lsn: None,
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Insert,
                values: vec![
                    ColumnValue {
                        name: "col0".to_owned(),
                        value: ScalarValue::Text("42".to_owned()),
                    },
                    ColumnValue {
                        name: "col1".to_owned(),
                        value: ScalarValue::Text("paid".to_owned()),
                    },
                ],
            })
            .expect("decoded placeholders should bind to relation schema");

        check!(difference.key.columns[0].name == "id");
        check!(difference.key.columns[0].value == ScalarValue::Int(42));
        check!(difference.after[1].name == "status");
    }

    #[test]
    fn decoded_placeholder_count_mismatch_is_an_error() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let error = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x31),
                commit_lsn: None,
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Insert,
                values: vec![ColumnValue {
                    name: "col0".to_owned(),
                    value: ScalarValue::Text("42".to_owned()),
                }],
            })
            .expect_err("decoded placeholder count should match relation schema");

        match error {
            PostgresConnectError::Backend(message) => {
                check!(message.contains("column count mismatch"));
                check!(message.contains("public.orders"));
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }

    #[test]
    fn named_partial_full_row_values_are_a_count_mismatch() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let error = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x34),
                commit_lsn: None,
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Insert,
                values: vec![id(42)],
            })
            .expect_err("partial full row should fail count validation");

        match error {
            PostgresConnectError::Backend(message) => {
                check!(message.contains("column count mismatch"));
                check!(message.contains("public.orders"));
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }

    #[test]
    fn pgoutput_text_scalars_are_coerced_to_declared_column_types() {
        check!(
            coerce_value(ScalarValue::Text("t".to_owned()), "bool").expect("true bool")
                == ScalarValue::Bool(true)
        );
        check!(
            coerce_value(ScalarValue::Text("f".to_owned()), "bool").expect("false bool")
                == ScalarValue::Bool(false)
        );
        check!(
            coerce_value(ScalarValue::Text("12.50".to_owned()), "numeric").expect("numeric")
                == ScalarValue::Float("12.50".to_owned())
        );
        check!(
            coerce_value(ScalarValue::Text("\\xDEad".to_owned()), "bytea").expect("bytea")
                == ScalarValue::Bytes(vec![0xde, 0xad])
        );
    }

    #[test]
    fn invalid_bool_text_is_rejected() {
        let error = coerce_value(ScalarValue::Text("yes".to_owned()), "bool")
            .expect_err("invalid bool should fail");

        match error {
            PostgresConnectError::Backend(message) => {
                check!(message.contains("invalid bool pgoutput text value"));
                check!(message.contains("yes"));
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }

    #[test]
    fn bytea_text_supports_escape_and_raw_forms() {
        check!(coerce_bytea("\\x0001ff").expect("hex bytea") == vec![0, 1, 255]);
        check!(coerce_bytea("raw").expect("raw bytea") == b"raw".to_vec());
    }

    #[test]
    fn bytea_hex_rejects_odd_length_and_invalid_pairs() {
        let odd = coerce_bytea("\\x0").expect_err("odd-length bytea should fail");
        let invalid = coerce_bytea("\\xzz").expect_err("invalid bytea should fail");

        match odd {
            PostgresConnectError::Backend(message) => {
                check!(message.contains("odd hex length"));
            }
            error => panic!("expected backend error, got {error:?}"),
        }
        match invalid {
            PostgresConnectError::Backend(message) => {
                check!(message.contains("invalid bytea pgoutput text value"));
                check!(message.contains("zz"));
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }

    #[test]
    fn update_without_old_tuple_does_not_trigger_key_change_check() {
        let mut cache = RelationCache::default();
        cache.apply_relation(orders_relation("text"));

        let difference = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x35),
                commit_lsn: None,
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Update {
                    old: Vec::new(),
                    old_tuple_kind: RowTupleKind::Full,
                },
                values: vec![id(42), status("paid")],
            })
            .expect("missing old tuple should translate");

        check!(difference.key.columns == vec![id(42)]);
        check!(difference.before == Vec::new());
    }

    #[test]
    fn key_column_presence_ignores_non_key_columns() {
        let schema = TableSchema {
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            columns: orders_relation("text").columns,
        };

        check!(super::has_any_key_column(&schema, &[id(42)]));
        check!(!super::has_any_key_column(&schema, &[status("paid")]));
        check!(!super::has_any_key_column(&schema, &[]));
    }

    #[test]
    fn type_oids_map_to_supported_scalar_names() {
        for (oid, name) in [
            (16, "bool"),
            (17, "bytea"),
            (20, "int8"),
            (21, "int2"),
            (23, "int4"),
            (25, "text"),
            (700, "float4"),
            (701, "float8"),
            (1043, "varchar"),
            (1700, "numeric"),
            (999_999, "unknown"),
        ] {
            check!(super::type_name(oid) == name);
        }
    }
}

#[cfg(test)]
mod decode_tests {
    use assert2::check;

    use super::{DecodedMessage, RowEventKind, RowTupleKind, decode_pgoutput_message};
    use crate::{
        PgLsn, PostgresConnectError,
        ids::{CommitLsn, EndLsn, RelationId, TransactionId},
        model::{ColumnSchema, ScalarValue},
    };

    fn put_i16(bytes: &mut Vec<u8>, value: i16) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_i32(bytes: &mut Vec<u8>, value: i32) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u32(bytes: &mut Vec<u8>, value: u32) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_i64(bytes: &mut Vec<u8>, value: i64) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_cstr(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0);
    }

    fn put_text_value(bytes: &mut Vec<u8>, value: &str) {
        bytes.push(b't');
        let length = i32::try_from(value.len()).expect("test value length should fit i32");
        put_i32(bytes, length);
        bytes.extend_from_slice(value.as_bytes());
    }

    fn orders_relation_message() -> super::RelationEvent {
        super::RelationEvent {
            relation_id: RelationId(7),
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_owned(),
                    type_name: "int8".to_owned(),
                    key: true,
                },
                ColumnSchema {
                    name: "status".to_owned(),
                    type_name: "text".to_owned(),
                    key: false,
                },
            ],
        }
    }

    #[test]
    fn decodes_relation_message() {
        let mut bytes = vec![b'R'];
        put_i32(&mut bytes, 7);
        put_cstr(&mut bytes, "public");
        put_cstr(&mut bytes, "orders");
        bytes.push(b'd');
        put_i16(&mut bytes, 2);
        bytes.push(1);
        put_cstr(&mut bytes, "id");
        put_i32(&mut bytes, 20);
        put_i32(&mut bytes, -1);
        bytes.push(0);
        put_cstr(&mut bytes, "status");
        put_i32(&mut bytes, 25);
        put_i32(&mut bytes, -1);

        let decoded = decode_pgoutput_message(&bytes, PgLsn(0x2a), Some(TransactionId(99)))
            .expect("relation message should decode");

        let DecodedMessage::Relation(relation) = decoded else {
            panic!("expected relation message");
        };
        check!(relation.relation_id == RelationId(7));
        check!(relation.schema == "public");
        check!(relation.table == "orders");
        check!(relation.columns.len() == 2);
        check!(relation.columns[0].name == "id");
        check!(relation.columns[0].type_name == "int8");
        check!(relation.columns[0].key);
        check!(relation.columns[1].name == "status");
        check!(relation.columns[1].type_name == "text");
        check!(!relation.columns[1].key);
    }

    #[test]
    fn decodes_insert_message_to_row_event() {
        let mut bytes = vec![b'I'];
        put_i32(&mut bytes, 7);
        bytes.push(b'N');
        put_i16(&mut bytes, 2);
        put_text_value(&mut bytes, "42");
        put_text_value(&mut bytes, "paid");

        let decoded = decode_pgoutput_message(&bytes, PgLsn(0x2a), Some(TransactionId(99)))
            .expect("insert message should decode");

        let DecodedMessage::Row(row) = decoded else {
            panic!("expected row message");
        };
        check!(row.relation_id == RelationId(7));
        check!(row.lsn == PgLsn(0x2a));
        check!(row.txid == Some(TransactionId(99)));
        check!(row.commit_timestamp_ms == None);
        check!(row.kind == RowEventKind::Insert);
        check!(row.values.len() == 2);
        check!(row.values[0].name == "col0");
        check!(row.values[0].value == ScalarValue::Text("42".to_owned()));
    }

    #[test]
    fn decoded_relation_and_insert_translate_with_schema_column_names() {
        let mut relation_bytes = vec![b'R'];
        put_i32(&mut relation_bytes, 7);
        put_cstr(&mut relation_bytes, "public");
        put_cstr(&mut relation_bytes, "orders");
        relation_bytes.push(b'd');
        put_i16(&mut relation_bytes, 2);
        relation_bytes.push(1);
        put_cstr(&mut relation_bytes, "id");
        put_i32(&mut relation_bytes, 20);
        put_i32(&mut relation_bytes, -1);
        relation_bytes.push(0);
        put_cstr(&mut relation_bytes, "status");
        put_i32(&mut relation_bytes, 25);
        put_i32(&mut relation_bytes, -1);

        let mut cache = super::RelationCache::default();
        let DecodedMessage::Relation(relation) =
            decode_pgoutput_message(&relation_bytes, PgLsn(0), None)
                .expect("relation should decode")
        else {
            panic!("expected relation");
        };
        cache.apply_relation(relation);

        let mut insert_bytes = vec![b'I'];
        put_i32(&mut insert_bytes, 7);
        insert_bytes.push(b'N');
        put_i16(&mut insert_bytes, 2);
        put_text_value(&mut insert_bytes, "42");
        put_text_value(&mut insert_bytes, "paid");

        let DecodedMessage::Row(row) =
            decode_pgoutput_message(&insert_bytes, PgLsn(0x32), Some(TransactionId(101)))
                .expect("insert should decode")
        else {
            panic!("expected row");
        };
        let difference = cache
            .translate(row)
            .expect("decoded insert should translate");

        check!(difference.key.columns[0].name == "id");
        check!(difference.key.columns[0].value == ScalarValue::Int(42));
        check!(difference.after[1].name == "status");
    }

    #[test]
    fn decoded_int8_text_value_translates_to_int_scalar_for_key() {
        let mut cache = super::RelationCache::default();
        cache.apply_relation(orders_relation_message());

        let mut insert_bytes = vec![b'I'];
        put_i32(&mut insert_bytes, 7);
        insert_bytes.push(b'N');
        put_i16(&mut insert_bytes, 2);
        put_text_value(&mut insert_bytes, "42");
        put_text_value(&mut insert_bytes, "paid");

        let DecodedMessage::Row(row) =
            decode_pgoutput_message(&insert_bytes, PgLsn(0x32), Some(TransactionId(101)))
                .expect("insert should decode")
        else {
            panic!("expected row");
        };
        let difference = cache
            .translate(row)
            .expect("decoded insert should translate");

        check!(difference.key.columns[0].name == "id");
        check!(difference.key.columns[0].value == ScalarValue::Int(42));
    }

    #[test]
    fn decoded_delete_key_tuple_translates_with_key_column_name() {
        let mut cache = super::RelationCache::default();
        cache.apply_relation(orders_relation_message());

        let mut delete_bytes = vec![b'D'];
        put_i32(&mut delete_bytes, 7);
        delete_bytes.push(b'K');
        put_i16(&mut delete_bytes, 1);
        put_text_value(&mut delete_bytes, "42");

        let DecodedMessage::Row(row) =
            decode_pgoutput_message(&delete_bytes, PgLsn(0x35), Some(TransactionId(102)))
                .expect("delete should decode")
        else {
            panic!("expected row");
        };
        let difference = cache
            .translate(row)
            .expect("decoded delete should translate");

        check!(difference.key.columns[0].name == "id");
        check!(difference.before[0].name == "id");
        check!(difference.before[0].value == ScalarValue::Int(42));
    }

    #[test]
    fn decoded_update_key_old_tuple_translates_for_non_key_update() {
        let mut cache = super::RelationCache::default();
        cache.apply_relation(orders_relation_message());

        let mut update_bytes = vec![b'U'];
        put_i32(&mut update_bytes, 7);
        update_bytes.push(b'K');
        put_i16(&mut update_bytes, 1);
        put_text_value(&mut update_bytes, "42");
        update_bytes.push(b'N');
        put_i16(&mut update_bytes, 2);
        put_text_value(&mut update_bytes, "42");
        put_text_value(&mut update_bytes, "paid");

        let DecodedMessage::Row(row) =
            decode_pgoutput_message(&update_bytes, PgLsn(0x36), Some(TransactionId(103)))
                .expect("update should decode")
        else {
            panic!("expected row");
        };
        let difference = cache
            .translate(row)
            .expect("decoded update should translate");

        check!(difference.before[0].name == "id");
        check!(difference.before[0].value == ScalarValue::Int(42));
        check!(difference.after[1].name == "status");
        check!(difference.after[1].value == ScalarValue::Text("paid".to_owned()));
    }

    #[test]
    fn decodes_delete_message_to_row_event() {
        let mut bytes = vec![b'D'];
        put_i32(&mut bytes, 7);
        bytes.push(b'K');
        put_i16(&mut bytes, 1);
        put_text_value(&mut bytes, "42");

        let decoded = decode_pgoutput_message(&bytes, PgLsn(0x2b), None)
            .expect("delete message should decode");

        let DecodedMessage::Row(row) = decoded else {
            panic!("expected row message");
        };
        check!(row.relation_id == RelationId(7));
        check!(row.lsn == PgLsn(0x2b));
        check!(row.txid == None);
        check!(
            row.kind
                == RowEventKind::Delete {
                    tuple_kind: RowTupleKind::Key,
                }
        );
        check!(row.values.len() == 1);
        check!(row.values[0].value == ScalarValue::Text("42".to_owned()));
    }

    #[test]
    fn decodes_update_message_to_row_event() {
        let mut bytes = vec![b'U'];
        put_i32(&mut bytes, 7);
        bytes.push(b'K');
        put_i16(&mut bytes, 1);
        put_text_value(&mut bytes, "41");
        bytes.push(b'N');
        put_i16(&mut bytes, 2);
        put_text_value(&mut bytes, "42");
        put_text_value(&mut bytes, "paid");

        let decoded = decode_pgoutput_message(&bytes, PgLsn(0x2c), Some(TransactionId(100)))
            .expect("update message should decode");

        let DecodedMessage::Row(row) = decoded else {
            panic!("expected row message");
        };
        check!(row.relation_id == RelationId(7));
        let RowEventKind::Update {
            old,
            old_tuple_kind,
        } = row.kind
        else {
            panic!("expected update event");
        };
        check!(old_tuple_kind == RowTupleKind::Key);
        check!(old.len() == 1);
        check!(old[0].value == ScalarValue::Text("41".to_owned()));
        check!(row.values.len() == 2);
        check!(row.values[0].value == ScalarValue::Text("42".to_owned()));
    }

    #[test]
    fn decodes_unchanged_toast_as_distinct_scalar_value() {
        let mut bytes = vec![b'U'];
        put_i32(&mut bytes, 7);
        bytes.push(b'N');
        put_i16(&mut bytes, 2);
        put_text_value(&mut bytes, "42");
        bytes.push(b'u');

        let decoded = decode_pgoutput_message(&bytes, PgLsn(0x33), Some(TransactionId(100)))
            .expect("update message should decode");

        let DecodedMessage::Row(row) = decoded else {
            panic!("expected row message");
        };
        check!(row.values[1].value == ScalarValue::UnchangedToast);
    }

    #[test]
    fn malformed_message_returns_backend_error() {
        let error = decode_pgoutput_message(&[b'R', 0, 0], PgLsn(0), None)
            .expect_err("truncated message should fail");

        match error {
            PostgresConnectError::Backend(message) => {
                check!(message.contains("truncated"));
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }

    #[test]
    fn trailing_message_bytes_are_rejected() {
        let mut bytes = vec![b'I'];
        put_i32(&mut bytes, 7);
        bytes.push(b'N');
        put_i16(&mut bytes, 2);
        put_text_value(&mut bytes, "42");
        put_text_value(&mut bytes, "paid");
        bytes.push(0xff);

        let error = decode_pgoutput_message(&bytes, PgLsn(0), None)
            .expect_err("trailing bytes should fail");

        match error {
            PostgresConnectError::Backend(message) => {
                check!(message.contains("trailing bytes"));
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }

    #[test]
    fn decodes_begin_and_commit_messages() {
        let mut begin = vec![b'B'];
        put_i64(&mut begin, 0x0102_0304);
        put_i64(&mut begin, 1_700_000_000);
        put_i32(&mut begin, 123);

        let decoded = decode_pgoutput_message(&begin, PgLsn(0), None).expect("begin should decode");
        check!(
            decoded
                == DecodedMessage::Begin {
                    final_lsn: PgLsn(0x0102_0304),
                    xid: TransactionId(123),
                }
        );

        let mut commit = vec![b'C', 0];
        put_i64(&mut commit, 0x0102_0305);
        put_i64(&mut commit, 0x0102_0306);
        put_i64(&mut commit, 1_000);

        let decoded =
            decode_pgoutput_message(&commit, PgLsn(0), None).expect("commit should decode");
        check!(
            decoded
                == DecodedMessage::Commit {
                    commit_lsn: CommitLsn(PgLsn(0x0102_0305)),
                    end_lsn: EndLsn(PgLsn(0x0102_0306)),
                    commit_timestamp_ms: 946_684_800_001,
                }
        );
    }

    #[test]
    fn decodes_begin_xid_as_unsigned_u32() {
        let mut begin = vec![b'B'];
        put_i64(&mut begin, 0x0102_0304);
        put_i64(&mut begin, 1_700_000_000);
        put_u32(&mut begin, 0x8000_0000);

        let decoded = decode_pgoutput_message(&begin, PgLsn(0), None).expect("begin should decode");
        check!(
            decoded
                == DecodedMessage::Begin {
                    final_lsn: PgLsn(0x0102_0304),
                    xid: TransactionId(2_147_483_648),
                }
        );
    }
}
