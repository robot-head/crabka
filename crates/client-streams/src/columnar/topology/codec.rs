//! `BatchCodec`: bridges a per-partition batch of Kafka records ↔ a polars `DataFrame`.

use ::polars::prelude::*;
use bytes::Bytes;

use crate::{columnar::serde::polars::PolarsIpcSerde, processor::serde::Serde};

/// Reserved metadata column names carried on every assembled `DataFrame` so the
/// sink codec can faithfully reconstruct Kafka records and the runtime can commit
/// offsets. Payload columns may not use these names.
pub const COL_KEY: &str = "__key";
pub const COL_TIMESTAMP: &str = "__timestamp";
pub const COL_PARTITION: &str = "__partition";
pub const COL_OFFSET: &str = "__offset";

/// All reserved column names, in `DataFrame` column order.
pub const RESERVED_COLUMNS: [&str; 4] = [COL_KEY, COL_TIMESTAMP, COL_PARTITION, COL_OFFSET];

/// One consumed Kafka record handed to a `BatchCodec::decode`.
#[derive(Debug, Clone)]
pub struct ConsumedRecord {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub timestamp: i64,
    pub partition: i32,
    pub offset: i64,
}

/// One record a `BatchCodec::encode` asks the runtime to produce.
#[derive(Debug, Clone)]
pub struct ProduceRecord {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub timestamp: i64,
}

/// Failure assembling/decomposing a batch.
#[derive(Debug, thiserror::Error)]
#[error("batch codec error: {0}")]
pub struct BatchError(pub String);

/// Bridges a per-partition batch of records ↔ a polars `DataFrame`.
pub trait BatchCodec: Send + Sync + 'static {
    /// Assemble consumed records (in offset order) into one `DataFrame`, including
    /// the reserved metadata columns.
    fn decode(&self, records: &[ConsumedRecord]) -> Result<DataFrame, BatchError>;
    /// Decompose an output `DataFrame` into produce records.
    fn encode(&self, df: &DataFrame) -> Result<Vec<ProduceRecord>, BatchError>;
}

/// Returns `Err` if `df_columns` contains a name that collides with a reserved
/// metadata column. Shared by codecs (Tasks 6–7) and the topology builder (Task 9).
pub fn reject_reserved_payload_columns(df_columns: &[&str]) -> Result<(), BatchError> {
    for name in df_columns {
        if RESERVED_COLUMNS.contains(name) {
            return Err(BatchError(format!(
                "payload column `{name}` collides with a reserved metadata column"
            )));
        }
    }
    Ok(())
}

/// `BatchCodec` where each record value is itself an Arrow-IPC `DataFrame`.
///
/// `decode` vstacks the per-record frames (attaching the reserved metadata
/// columns); `encode` writes the result as IPC record(s), splitting if the
/// encoded size would exceed `max_record_bytes`.
#[derive(Debug, Clone)]
pub struct BlobCodec {
    /// Soft cap on one produced record's encoded size; the frame is row-chunked
    /// to stay under it. Defaults to ~900 KiB (under Kafka's default 1 MiB
    /// `max.request.size`, leaving headroom for record headers/framing).
    pub max_record_bytes: usize,
}

impl Default for BlobCodec {
    fn default() -> Self {
        Self {
            max_record_bytes: 900 * 1024,
        }
    }
}

impl BatchCodec for BlobCodec {
    fn decode(&self, records: &[ConsumedRecord]) -> Result<DataFrame, BatchError> {
        let mut acc: Option<DataFrame> = None;
        for (i, rec) in records.iter().enumerate() {
            let frame = PolarsIpcSerde
                .deserialize("", &rec.value)
                .map_err(|e| BatchError(format!("decode record {i}: {e}")))?;
            // Reject payloads whose own columns collide with the reserved metadata
            // names we are about to attach — otherwise `with_meta_columns` would
            // silently overwrite the payload's data.
            let cols: Vec<&str> = frame
                .get_column_names()
                .iter()
                .map(|s| s.as_str())
                .collect();
            reject_reserved_payload_columns(&cols)
                .map_err(|e| BatchError(format!("decode record {i}: {e}")))?;
            let frame = with_meta_columns(frame, rec)?;
            acc = Some(match acc {
                None => frame,
                Some(a) => a.vstack(&frame).map_err(|e| BatchError(e.to_string()))?,
            });
        }
        acc.ok_or_else(|| BatchError("decode called with empty record batch".into()))
    }

    fn encode(&self, df: &DataFrame) -> Result<Vec<ProduceRecord>, BatchError> {
        let payload = drop_reserved_columns(df);
        let ts = last_timestamp(df).unwrap_or(0);
        let mut out = Vec::new();
        for chunk in chunk_by_size(&payload, self.max_record_bytes) {
            let value = PolarsIpcSerde.serialize("", &chunk);
            out.push(ProduceRecord {
                key: None,
                value,
                timestamp: ts,
            });
        }
        Ok(out)
    }
}

/// Add the four reserved metadata columns (broadcast to every row of `frame`).
fn with_meta_columns(frame: DataFrame, rec: &ConsumedRecord) -> Result<DataFrame, BatchError> {
    let n = frame.height();
    let mut df = frame;
    // `Vec<Option<Vec<u8>>>` maps to a Binary column via polars' `NamedFrom` impl.
    let key_vals: Vec<Option<Vec<u8>>> = vec![rec.key.as_ref().map(|k| k.to_vec()); n];
    df.with_column(Column::new(COL_KEY.into(), key_vals))
        .map_err(|e| BatchError(e.to_string()))?;
    df.with_column(Column::new(COL_TIMESTAMP.into(), vec![rec.timestamp; n]))
        .map_err(|e| BatchError(e.to_string()))?;
    df.with_column(Column::new(COL_PARTITION.into(), vec![rec.partition; n]))
        .map_err(|e| BatchError(e.to_string()))?;
    df.with_column(Column::new(COL_OFFSET.into(), vec![rec.offset; n]))
        .map_err(|e| BatchError(e.to_string()))?;
    Ok(df)
}

/// Drop the reserved metadata columns, leaving only payload columns. Tolerates a
/// frame that never carried them (`drop_many` ignores absent names).
fn drop_reserved_columns(df: &DataFrame) -> DataFrame {
    df.drop_many(RESERVED_COLUMNS.iter().map(|s| PlSmallStr::from_str(s)))
}

fn last_timestamp(df: &DataFrame) -> Option<i64> {
    let col = df.column(COL_TIMESTAMP).ok()?;
    col.i64().ok()?.get(df.height().saturating_sub(1))
}

/// Split a frame into row-slices whose IPC-encoded size stays under `cap`.
fn chunk_by_size(df: &DataFrame, cap: usize) -> Vec<DataFrame> {
    if PolarsIpcSerde.serialize("", df).len() <= cap || df.height() <= 1 {
        return vec![df.clone()];
    }
    let mid = df.height() / 2;
    let mut out = chunk_by_size(&df.slice(0, mid), cap);
    // `mid` is at most `df.height() / 2`, which fits in i64 on any real frame.
    #[allow(
        clippy::cast_possible_wrap,
        reason = "row count cannot exceed i64::MAX"
    )]
    let mid_i64 = mid as i64;
    out.extend(chunk_by_size(&df.slice(mid_i64, df.height() - mid), cap));
    out
}

use std::marker::PhantomData;

use crate::{columnar::topology::row_bridge::RowBridge, processor::serde::SerdeAssociate};

/// `BatchCodec` over ordinary row records: deserialize each `(key, value)` with
/// the inner serdes, assemble payload columns via a [`RowBridge`], and attach the
/// reserved metadata columns. `encode` reverses it (one record per row).
pub struct RowCodec<K, V, KS, VS, B> {
    #[allow(dead_code, reason = "retained for future typed-key reconstruction")]
    key_serde: KS,
    value_serde: VS,
    bridge: B,
    _kv: PhantomData<fn() -> (K, V)>,
}

impl<K, V, KS, VS, B> RowCodec<K, V, KS, VS, B> {
    /// Construct a `RowCodec` from its key/value serdes and a row bridge.
    pub fn new(key_serde: KS, value_serde: VS, bridge: B) -> Self {
        Self {
            key_serde,
            value_serde,
            bridge,
            _kv: PhantomData,
        }
    }
}

impl<K, V, KS, VS, B> BatchCodec for RowCodec<K, V, KS, VS, B>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    KS: Serde<K> + SerdeAssociate<Target = K>,
    VS: Serde<V> + SerdeAssociate<Target = V>,
    B: RowBridge<V>,
{
    fn decode(&self, records: &[ConsumedRecord]) -> Result<DataFrame, BatchError> {
        let mut values = Vec::with_capacity(records.len());
        for (i, rec) in records.iter().enumerate() {
            values.push(
                self.value_serde
                    .deserialize("", &rec.value)
                    .map_err(|e| BatchError(format!("row {i} value: {e}")))?,
            );
        }
        let payload = self.bridge.rows_to_frame(&values)?;
        let names: Vec<&str> = payload
            .get_column_names()
            .iter()
            .map(|s| s.as_str())
            .collect();
        reject_reserved_payload_columns(&names)?;

        let mut df = payload;
        let key_vals: Vec<Option<Vec<u8>>> = records
            .iter()
            .map(|r| r.key.as_ref().map(|k| k.to_vec()))
            .collect();
        df.with_column(Column::new(COL_KEY.into(), key_vals))
            .map_err(|e| BatchError(e.to_string()))?;
        df.with_column(Column::new(
            COL_TIMESTAMP.into(),
            records.iter().map(|r| r.timestamp).collect::<Vec<i64>>(),
        ))
        .map_err(|e| BatchError(e.to_string()))?;
        df.with_column(Column::new(
            COL_PARTITION.into(),
            records.iter().map(|r| r.partition).collect::<Vec<i32>>(),
        ))
        .map_err(|e| BatchError(e.to_string()))?;
        df.with_column(Column::new(
            COL_OFFSET.into(),
            records.iter().map(|r| r.offset).collect::<Vec<i64>>(),
        ))
        .map_err(|e| BatchError(e.to_string()))?;
        Ok(df)
    }

    fn encode(&self, df: &DataFrame) -> Result<Vec<ProduceRecord>, BatchError> {
        let payload = drop_reserved_columns(df);
        let rows: Vec<V> = self.bridge.frame_to_rows(&payload)?;
        let keys = df.column(COL_KEY).ok();
        let ts = df.column(COL_TIMESTAMP).ok();
        let mut out = Vec::with_capacity(rows.len());
        for (i, v) in rows.iter().enumerate() {
            let value = self.value_serde.serialize("", v);
            let key = keys
                .and_then(|c| c.binary().ok())
                .and_then(|c| c.get(i))
                .map(Bytes::copy_from_slice);
            let timestamp = ts
                .and_then(|c| c.i64().ok())
                .and_then(|c| c.get(i))
                .unwrap_or(0);
            out.push(ProduceRecord {
                key,
                value,
                timestamp,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn reserved_columns_are_distinct_and_underscored() {
        let actual = RESERVED_COLUMNS.map(|column| (column, column.starts_with("__")));
        check!(
            actual
                == [
                    (COL_KEY, true),
                    (COL_TIMESTAMP, true),
                    (COL_PARTITION, true),
                    (COL_OFFSET, true)
                ]
        );
    }

    #[test]
    fn reject_reserved_payload_columns_flags_collision() {
        check!(reject_reserved_payload_columns(&["id", "total"]).is_ok());
        check!(reject_reserved_payload_columns(&["id", "__key"]).is_err());
    }

    use crate::{columnar::serde::polars::PolarsIpcSerde, processor::serde::Serde};

    fn ipc_bytes(df: &DataFrame) -> Bytes {
        PolarsIpcSerde.serialize("t", df)
    }

    #[test]
    fn blob_codec_vstacks_records_then_round_trips() {
        let codec = BlobCodec::default();
        let a = df!("v" => [1_i64, 2]).unwrap();
        let b = df!("v" => [3_i64]).unwrap();
        let records = vec![
            ConsumedRecord {
                key: None,
                value: ipc_bytes(&a),
                timestamp: 10,
                partition: 0,
                offset: 5,
            },
            ConsumedRecord {
                key: None,
                value: ipc_bytes(&b),
                timestamp: 11,
                partition: 0,
                offset: 6,
            },
        ];
        let df = codec.decode(&records).unwrap();
        check!((df.height(), df.column(COL_PARTITION).is_ok()) == (3, true));

        let out = codec.encode(&df).unwrap();
        let back = PolarsIpcSerde.deserialize("t", &out[0].value).unwrap();
        check!((out.len(), back.height()) == (1, 3));
    }

    #[test]
    fn blob_codec_rejects_payload_with_reserved_column() {
        // A blob payload that already carries `__key` must be rejected rather than
        // silently overwritten by the attached metadata column.
        let codec = BlobCodec::default();
        let bad = df!("__key" => [1_i64, 2]).unwrap();
        let records = vec![ConsumedRecord {
            key: None,
            value: ipc_bytes(&bad),
            timestamp: 0,
            partition: 0,
            offset: 0,
        }];
        check!(codec.decode(&records).is_err());
    }

    #[test]
    fn blob_codec_rejects_empty_batch() {
        let codec = BlobCodec::default();
        check!(codec.decode(&[]).is_err());
    }

    use std::marker::PhantomData;

    use crate::{columnar::topology::row_bridge::JsonRowBridge, processor::serde::StringSerde};

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
    struct Txn {
        user: String,
        amount: i64,
    }

    struct JsonValueSerde<T>(PhantomData<fn() -> T>);
    impl<T> Default for JsonValueSerde<T> {
        fn default() -> Self {
            Self(PhantomData)
        }
    }
    impl<T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static>
        crate::processor::serde::Serde<T> for JsonValueSerde<T>
    {
        fn serialize(&self, _t: &str, v: &T) -> Bytes {
            Bytes::from(serde_json::to_vec(v).unwrap())
        }
        fn deserialize(
            &self,
            _t: &str,
            b: &[u8],
        ) -> Result<T, crate::processor::serde::SerdeError> {
            serde_json::from_slice(b)
                .map_err(|e| crate::processor::serde::SerdeError(e.to_string()))
        }
    }
    impl<T: Send + Sync + 'static> crate::processor::serde::SerdeAssociate for JsonValueSerde<T> {
        type Target = T;
    }

    #[test]
    fn row_codec_assembles_and_explodes() {
        let codec = RowCodec::<String, Txn, _, _, _>::new(
            StringSerde,
            JsonValueSerde::<Txn>::default(),
            JsonRowBridge,
        );
        let recs = vec![
            ConsumedRecord {
                key: Some(Bytes::from_static(b"a")),
                value: Bytes::from(
                    serde_json::to_vec(&Txn {
                        user: "a".into(),
                        amount: 5,
                    })
                    .unwrap(),
                ),
                timestamp: 1,
                partition: 0,
                offset: 0,
            },
            ConsumedRecord {
                key: Some(Bytes::from_static(b"b")),
                value: Bytes::from(
                    serde_json::to_vec(&Txn {
                        user: "b".into(),
                        amount: 7,
                    })
                    .unwrap(),
                ),
                timestamp: 2,
                partition: 0,
                offset: 1,
            },
        ];
        let df = codec.decode(&recs).unwrap();
        check!(
            (
                df.height(),
                df.column("amount").is_ok(),
                df.column(COL_KEY).is_ok()
            ) == (2, true, true)
        );

        let out = codec.encode(&df).unwrap();
        let actual = out
            .iter()
            .map(|record| {
                (
                    record.key.as_deref(),
                    serde_json::from_slice::<Txn>(&record.value).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        check!(
            actual
                == vec![
                    (
                        Some(b"a".as_ref()),
                        Txn {
                            user: "a".into(),
                            amount: 5
                        }
                    ),
                    (
                        Some(b"b".as_ref()),
                        Txn {
                            user: "b".into(),
                            amount: 7
                        }
                    ),
                ]
        );
    }

    use proptest::prelude::*;
    proptest! {
        #[test]
        fn row_codec_round_trip_preserves_rows(users in proptest::collection::vec("[a-z]{1,4}", 1..8)) {
            let codec = RowCodec::<String, Txn, _, _, _>::new(
                StringSerde, JsonValueSerde::<Txn>::default(), JsonRowBridge,
            );
            let recs: Vec<ConsumedRecord> = users.iter().enumerate().map(|(i, u)| ConsumedRecord {
                key: Some(Bytes::from(u.clone().into_bytes())),
                value: Bytes::from(serde_json::to_vec(&Txn { user: u.clone(), amount: i64::try_from(i).unwrap() }).unwrap()),
                timestamp: i64::try_from(i).unwrap(), partition: 0, offset: i64::try_from(i).unwrap(),
            }).collect();
            let df = codec.decode(&recs).unwrap();
            let out = codec.encode(&df).unwrap();
            let actual = out
                .iter()
                .map(|record| {
                    (
                        record.key.as_deref(),
                        serde_json::from_slice::<Txn>(&record.value).unwrap(),
                        record.timestamp,
                    )
                })
                .collect::<Vec<_>>();
            let expected = recs
                .iter()
                .map(|record| {
                    (
                        record.key.as_deref(),
                        serde_json::from_slice::<Txn>(&record.value).unwrap(),
                        record.timestamp,
                    )
                })
                .collect::<Vec<_>>();
            prop_assert_eq!(actual, expected);
        }
    }
}
