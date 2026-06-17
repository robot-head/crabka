use std::collections::VecDeque;

use async_trait::async_trait;
use bytes::Bytes;
use crabka_connect::{ConnectError, ConnectRecord, Source, SourceOffset};

use crate::model::Operation;
use crate::pgoutput::{RelationCache, RelationEvent, RowEvent};
use crate::schema::PostgresProtoEncoder;
use crate::{PgLsn, PostgresSourceConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalEvent {
    Relation(RelationEvent),
    Row(RowEvent),
}

#[derive(Debug, Clone)]
pub struct PostgresWalSource {
    config: PostgresSourceConfig,
    database_name: String,
    relation_cache: RelationCache,
    encoder: PostgresProtoEncoder,
    pending: VecDeque<LogicalEvent>,
    checkpoint: Option<PgLsn>,
}

impl PostgresWalSource {
    pub fn scripted(
        config: PostgresSourceConfig,
        database_name: impl Into<String>,
        events: impl IntoIterator<Item = LogicalEvent>,
    ) -> Result<Self, ConnectError> {
        Ok(Self {
            config,
            database_name: database_name.into(),
            relation_cache: RelationCache::default(),
            encoder: PostgresProtoEncoder::new()?,
            pending: events.into_iter().collect(),
            checkpoint: None,
        })
    }
}

#[async_trait]
impl Source<Bytes, Bytes> for PostgresWalSource {
    async fn poll(&mut self) -> Result<Option<ConnectRecord<Bytes, Bytes>>, ConnectError> {
        while let Some(event) = self.pending.pop_front() {
            match event {
                LogicalEvent::Relation(relation) => {
                    self.relation_cache.apply_relation(relation);
                }
                LogicalEvent::Row(row) => {
                    let diff = self.relation_cache.translate(row)?;
                    let key = self.encoder.encode_key(&diff.key)?;
                    let value = if diff.op == Operation::Delete {
                        None
                    } else {
                        Some(self.encoder.encode_value(&diff)?)
                    };
                    self.checkpoint = Some(diff.lsn);

                    let mut record = ConnectRecord::new(Some(key), value)
                        .with_header("crabka.pg.table", Some(Bytes::from(diff.table.clone())))
                        .with_header("crabka.pg.lsn", Some(Bytes::from(diff.lsn.to_string())))
                        .with_header(
                            "crabka.pg.operation",
                            Some(Bytes::from_static(operation_header(diff.op).as_bytes())),
                        );
                    if let Some(commit_timestamp_ms) = diff.commit_timestamp_ms {
                        record = record.with_timestamp(commit_timestamp_ms);
                    }

                    return Ok(Some(record));
                }
            }
        }

        Ok(None)
    }

    fn checkpoint(&self) -> Option<SourceOffset> {
        self.checkpoint
            .map(|lsn| lsn.to_source_offset(&self.database_name, &self.config.slot_name))
    }

    async fn seek(&mut self, offset: SourceOffset) -> Result<(), ConnectError> {
        self.checkpoint = Some(PgLsn::from_source_offset(&offset, &self.config.slot_name)?);
        Ok(())
    }
}

fn operation_header(operation: Operation) -> &'static str {
    match operation {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_connect::{SecretString, Source as _};
    use crabka_schema_serde::wire::MAGIC;

    use super::{LogicalEvent, PostgresWalSource};
    use crate::model::{ColumnSchema, ColumnValue, ScalarValue};
    use crate::pgoutput::{RelationEvent, RowEvent, RowEventKind};
    use crate::{PgLsn, PostgresSourceConfig};

    fn header_value(
        record: &crabka_connect::ConnectRecord<bytes::Bytes, bytes::Bytes>,
        key: &str,
    ) -> bytes::Bytes {
        record
            .headers
            .iter()
            .find(|header| header.key == key)
            .and_then(|header| header.value.clone())
            .unwrap_or_else(|| panic!("missing header {key}"))
    }

    fn config(slot_name: &str) -> PostgresSourceConfig {
        PostgresSourceConfig {
            database_url: SecretString::new("postgres://localhost/app"),
            slot_name: slot_name.to_owned(),
            publication_name: "crabka_connect".to_owned(),
            schema: "public".to_owned(),
            table_names: vec!["orders".to_owned()],
            max_messages_per_poll: 1000,
        }
    }

    fn orders_relation() -> RelationEvent {
        RelationEvent {
            relation_id: 7,
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

    fn insert_event(lsn: PgLsn) -> RowEvent {
        RowEvent {
            relation_id: 7,
            lsn,
            txid: Some(99),
            commit_timestamp_ms: Some(1_700_000_000_000),
            kind: RowEventKind::Insert,
            values: vec![id(42), status("paid")],
        }
    }

    fn delete_event(lsn: PgLsn) -> RowEvent {
        RowEvent {
            relation_id: 7,
            lsn,
            txid: None,
            commit_timestamp_ms: None,
            kind: RowEventKind::Delete,
            values: vec![id(42), status("cancelled")],
        }
    }

    #[tokio::test]
    async fn insert_poll_emits_framed_record_and_checkpoint() {
        let mut source = PostgresWalSource::scripted(
            config("slot_a"),
            "app",
            [
                LogicalEvent::Relation(orders_relation()),
                LogicalEvent::Row(insert_event(PgLsn(0x2a))),
            ],
        )
        .expect("source builds");

        let record = source
            .poll()
            .await
            .expect("poll succeeds")
            .expect("row emits");

        check!(record.key.is_some());
        check!(record.value.is_some());
        check!(record.key.as_ref().expect("key")[0] == MAGIC);
        check!(record.value.as_ref().expect("value")[0] == MAGIC);
        check!(record.timestamp == Some(1_700_000_000_000));
        check!(header_value(&record, "crabka.pg.table").as_ref() == b"public.orders");
        check!(header_value(&record, "crabka.pg.lsn").as_ref() == b"0/2A");
        check!(header_value(&record, "crabka.pg.operation").as_ref() == b"insert");
        check!(source.checkpoint() == Some(PgLsn(0x2a).to_source_offset("app", "slot_a")));
    }

    #[tokio::test]
    async fn delete_poll_emits_tombstone() {
        let mut source = PostgresWalSource::scripted(
            config("slot_a"),
            "app",
            [
                LogicalEvent::Relation(orders_relation()),
                LogicalEvent::Row(delete_event(PgLsn(0x2b))),
            ],
        )
        .expect("source builds");

        let record = source
            .poll()
            .await
            .expect("poll succeeds")
            .expect("row emits");

        check!(record.key.is_some());
        check!(record.value.is_none());
        check!(record.timestamp.is_none());
        check!(header_value(&record, "crabka.pg.table").as_ref() == b"public.orders");
        check!(header_value(&record, "crabka.pg.lsn").as_ref() == b"0/2B");
        check!(header_value(&record, "crabka.pg.operation").as_ref() == b"delete");
    }

    #[tokio::test]
    async fn seek_restores_lsn_checkpoint() {
        let mut source = PostgresWalSource::scripted(config("slot_a"), "app", []).unwrap();
        let offset = PgLsn(0x2a).to_source_offset("app", "slot_a");

        source.seek(offset).await.expect("seek succeeds");

        check!(source.checkpoint() == Some(PgLsn(0x2a).to_source_offset("app", "slot_a")));
    }

    #[tokio::test]
    async fn relation_only_poll_returns_none_without_checkpoint() {
        let mut source = PostgresWalSource::scripted(
            config("slot_a"),
            "app",
            [LogicalEvent::Relation(orders_relation())],
        )
        .expect("source builds");

        check!(source.poll().await.expect("poll succeeds").is_none());
        check!(source.checkpoint().is_none());
    }
}
