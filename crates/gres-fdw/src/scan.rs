//! Row assembly: turns a `Vec<RawRecord>` into `Vec<Vec<Datum>>` in the
//! foreign table's column order.
//!
//! The five envelope columns `_partition`, `_offset`, `_timestamp`, `_key`,
//! and `_headers` come first. The decoded value columns
//! (`table.columns[5..]`) follow, exactly as
//! [`crabka_pgcatalog::create_foreign_table`] lays them out.

use std::{fmt::Write as _, sync::Arc};

use crabka_pgcatalog::Table;
use crabka_pgtypes::Datum;
use crabka_schema_serde::SchemaCache;

use crate::{
    config::ConnProfile,
    decode::{FdwDecodePolicy, decode_value_with_policy},
    error::KafkaFdwError,
    source::RawRecord,
    types::project,
};

/// Number of envelope columns that [`crabka_pgcatalog::create_foreign_table`]
/// prepends to every foreign table. The value columns start at this index.
const ENVELOPE_COLS: usize = 5;

/// Assembles decoded rows for a foreign-table scan.
///
/// For each [`RawRecord`], the function emits a row of `table.columns.len()`
/// datums:
/// 1. `_partition` → [`Datum::Int4`]
/// 2. `_offset`    → [`Datum::Int8`]
/// 3. `_timestamp` → [`Datum::Timestamptz`] (from `timestamp_ms`)
/// 4. `_key`       → [`Datum::Bytea`] or [`Datum::Null`]
/// 5. `_headers`   → [`Datum::Text`] holding the headers as a JSON string
/// 6. the value columns. [`decode_value_with_policy`] decodes them, and
///    [`project`] projects them onto `table.columns[5..]`. A `None` or empty
///    value gives all value columns as [`Datum::Null`].
///
/// # Errors
/// Propagates [`KafkaFdwError`] from the value decode: wire-format, schema
/// registry, or Avro/JSON parse failures.
#[cfg(test)]
pub async fn assemble_rows(
    table: &Table,
    raw_records: &[RawRecord],
    profile: &ConnProfile,
    cache: &Arc<SchemaCache>,
) -> Result<Vec<Vec<Datum>>, KafkaFdwError> {
    assemble_rows_with_policy(
        table,
        raw_records,
        profile,
        cache,
        FdwDecodePolicy::default(),
    )
    .await
}

/// Assembles rows under an explicit cold-cache schema resolution policy.
///
/// # Errors
/// Propagates value decode and schema resolution failures.
pub async fn assemble_rows_with_policy(
    table: &Table,
    raw_records: &[RawRecord],
    profile: &ConnProfile,
    cache: &Arc<SchemaCache>,
    policy: FdwDecodePolicy,
) -> Result<Vec<Vec<Datum>>, KafkaFdwError> {
    let policy = policy.validate().map_err(KafkaFdwError::Config)?;
    let value_columns = &table.columns[ENVELOPE_COLS.min(table.columns.len())..];

    let mut rows = Vec::with_capacity(raw_records.len());
    for raw in raw_records {
        let mut row = Vec::with_capacity(table.columns.len());

        // ── envelope ──────────────────────────────────────────────────────
        row.push(Datum::Int4(raw.partition));
        row.push(Datum::Int8(raw.offset));
        let ts = jiff::Timestamp::from_millisecond(raw.timestamp_ms).map_err(|e| {
            KafkaFdwError::Other(format!("timestamp {} out of range: {e}", raw.timestamp_ms))
        })?;
        row.push(Datum::Timestamptz(ts));
        row.push(match &raw.key {
            Some(bytes) => Datum::Bytea(bytes.clone()),
            None => Datum::Null,
        });
        row.push(Datum::Text(headers_to_json(&raw.headers)));

        // ── value columns ─────────────────────────────────────────────────
        match raw.value.as_deref() {
            Some(bytes) if !bytes.is_empty() => {
                let (decoded, avro_schema) = decode_value_with_policy(
                    cache,
                    profile.value_format,
                    &profile.topic,
                    bytes,
                    policy,
                )
                .await?;
                row.extend(project(&decoded, value_columns, avro_schema.as_ref()));
            }
            // Null / empty value (a tombstone, or no payload) → all value
            // columns null.
            _ => row.extend(value_columns.iter().map(|_| Datum::Null)),
        }

        rows.push(row);
    }

    Ok(rows)
}

/// Serialises record headers as a JSON object string for the `_headers` text
/// column.
///
/// Header values are bytes. An absent value becomes JSON `null`. A present
/// value becomes a `\x`-prefixed lowercase-hex string, which mirrors
/// `PostgreSQL`'s `bytea` text output, so the column round-trips losslessly
/// through text.
///
/// Kafka permits duplicate header keys, so this function writes the JSON
/// object by hand instead of a collect into a map. Empty headers keep the FDW
/// representation `{}`.
fn headers_to_json(headers: &[(String, Option<Vec<u8>>)]) -> String {
    if headers.is_empty() {
        return "{}".to_string();
    }

    let mut sorted_headers: Vec<_> = headers.iter().collect();
    sorted_headers.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));

    let mut json = String::from("{");
    for (index, (key, value)) in sorted_headers.into_iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        let escaped_key = serde_json::to_string(key).expect("serializing a string cannot fail");
        json.push_str(&escaped_key);
        json.push(':');
        match value {
            Some(bytes) => {
                let escaped_value = serde_json::to_string(&to_hex_text(bytes))
                    .expect("serializing a string cannot fail");
                json.push_str(&escaped_value);
            }
            None => json.push_str("null"),
        }
    }
    json.push('}');
    json
}

fn to_hex_text(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(2 + bytes.len() * 2);
    text.push_str("\\x");
    for byte in bytes {
        write!(&mut text, "{byte:02x}").expect("writing to String cannot fail");
    }
    text
}

#[cfg(test)]
mod tests {
    use crabka_pgcatalog::{Column, ForeignTableMeta};
    use crabka_pgtypes::ColumnType;

    use super::*;
    use crate::decode::Wire;

    /// Builds a foreign `Table` with the five envelope columns and a single
    /// `bytea` value column, the raw-format projection target.
    fn raw_value_table() -> Table {
        Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: crabka_pgcatalog::RelationName::public("events"),
            columns: vec![
                Column::new("_partition", ColumnType::Int4),
                Column::new("_offset", ColumnType::Int8),
                Column::new("_timestamp", ColumnType::Timestamptz),
                Column::new("_key", ColumnType::Bytea),
                Column::new("_headers", ColumnType::Text),
                Column::new("value", ColumnType::Bytea),
            ],
            sharded: false,
            sharding: None,
            foreign: Some(ForeignTableMeta {
                server: "s".into(),
                options: vec![("topic".into(), "events".into())],
            }),
            checks: Vec::new(),
        }
    }

    fn raw_profile() -> ConnProfile {
        ConnProfile {
            bootstrap: vec!["b:9092".into()],
            registry_url: String::new(),
            security: None,
            topic: "events".into(),
            value_format: Wire::Raw,
            key_format: Wire::Raw,
        }
    }

    /// The signature needs a `SchemaCache`, but the raw path never uses it
    /// and makes no registry access.
    fn dummy_cache() -> Arc<SchemaCache> {
        SchemaCache::new(
            crabka_schema_serde::RegistryClient::new("http://unused"),
            crabka_schema_serde::CacheConfig::default(),
        )
    }

    #[tokio::test]
    async fn assemble_rows_builds_envelope_and_raw_value() {
        let table = raw_value_table();
        let profile = raw_profile();
        let cache = dummy_cache();

        let records = vec![
            RawRecord {
                partition: 3,
                offset: 42,
                timestamp_ms: 1_600_000_000_000,
                key: Some(b"k1".to_vec()),
                value: Some(b"payload-one".to_vec()),
                headers: vec![
                    ("z".into(), Some(vec![0x00, 0xff])),
                    ("dup".into(), Some(b"one".to_vec())),
                    ("dup".into(), None),
                ],
            },
            RawRecord {
                partition: 0,
                offset: 7,
                timestamp_ms: 0,
                key: None,
                value: Some(b"payload-two".to_vec()),
                headers: Vec::new(),
            },
        ];

        let assembled_rows = assemble_rows(&table, &records, &profile, &cache)
            .await
            .expect("assemble_rows");
        assert_eq!(assembled_rows.len(), 2);

        // ── row 0 ─────────────────────────────────────────────────────────
        let r0 = &assembled_rows[0];
        assert_eq!(r0.len(), 6, "5 envelope + 1 value column");
        assert_eq!(r0[0], Datum::Int4(3), "_partition");
        assert_eq!(r0[1], Datum::Int8(42), "_offset");
        assert_eq!(
            r0[2],
            Datum::Timestamptz(
                jiff::Timestamp::from_millisecond(1_600_000_000_000).expect("ts in range")
            ),
            "_timestamp"
        );
        assert_eq!(r0[3], Datum::Bytea(b"k1".to_vec()), "_key");
        assert_eq!(
            r0[4],
            Datum::Text("{\"dup\":\"\\\\x6f6e65\",\"dup\":null,\"z\":\"\\\\x00ff\"}".to_string()),
            "_headers JSON preserves duplicate keys, nulls, and binary values"
        );
        assert_eq!(
            r0[5],
            Datum::Bytea(b"payload-one".to_vec()),
            "raw value column is the verbatim payload bytea"
        );

        // ── row 1 ─────────────────────────────────────────────────────────
        let r1 = &assembled_rows[1];
        assert_eq!(r1[0], Datum::Int4(0), "_partition");
        assert_eq!(r1[1], Datum::Int8(7), "_offset");
        assert_eq!(r1[3], Datum::Null, "_key is Null when absent");
        assert_eq!(r1[4], Datum::Text("{}".to_string()), "empty headers → {{}}");
        assert_eq!(r1[5], Datum::Bytea(b"payload-two".to_vec()));
    }

    #[tokio::test]
    async fn assemble_rows_null_value_yields_null_value_columns() {
        let table = raw_value_table();
        let profile = raw_profile();
        let cache = dummy_cache();

        let records = vec![RawRecord {
            partition: 1,
            offset: 100,
            timestamp_ms: 5,
            key: Some(b"tombstone".to_vec()),
            value: None,
            headers: Vec::new(),
        }];

        let assembled_rows = assemble_rows(&table, &records, &profile, &cache)
            .await
            .expect("assemble_rows");
        assert_eq!(assembled_rows.len(), 1);
        let r = &assembled_rows[0];
        // Envelope still present.
        assert_eq!(r[0], Datum::Int4(1));
        assert_eq!(r[3], Datum::Bytea(b"tombstone".to_vec()));
        // The single value column is Null for a None value.
        assert_eq!(r[5], Datum::Null, "None value → value columns are Null");
    }
}
