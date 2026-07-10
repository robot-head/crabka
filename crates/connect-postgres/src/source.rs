use std::collections::{HashSet, VecDeque};

use async_trait::async_trait;
use bytes::Bytes;
use crabka_connect::{ConnectError, ConnectRecord, OffsetValue, Source, SourceOffset};

use crate::{
    PgLsn, PostgresSourceConfig,
    catalog::{PgCatalog, TokioPgCatalog},
    ids::{CommitLsn, EndLsn, TransactionId},
    model::Operation,
    pgoutput::{DecodedMessage, RelationCache, RelationEvent, RowEvent, decode_pgoutput_message},
    schema::PostgresProtoEncoder,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalEvent {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransactionState {
    xid: TransactionId,
}

#[derive(Debug)]
pub struct PostgresWalSource {
    config: PostgresSourceConfig,
    database_name: String,
    catalog: Option<Box<dyn PgCatalog>>,
    relation_cache: RelationCache,
    encoder: PostgresProtoEncoder,
    pending: VecDeque<LogicalEvent>,
    transaction: Option<TransactionState>,
    transaction_rows: Vec<RowEvent>,
    checkpoint: Option<PgLsn>,
    resume_lsn: Option<PgLsn>,
}

impl PostgresWalSource {
    fn build(
        config: PostgresSourceConfig,
        database_name: String,
        catalog: Option<Box<dyn PgCatalog>>,
        pending: VecDeque<LogicalEvent>,
    ) -> Result<Self, ConnectError> {
        Ok(Self {
            config,
            database_name,
            catalog,
            relation_cache: RelationCache::default(),
            encoder: PostgresProtoEncoder::new()?,
            pending,
            transaction: None,
            transaction_rows: Vec::new(),
            checkpoint: None,
            resume_lsn: None,
        })
    }

    pub fn scripted(
        config: PostgresSourceConfig,
        database_name: impl Into<String>,
        events: impl IntoIterator<Item = LogicalEvent>,
    ) -> Result<Self, ConnectError> {
        Self::build(
            config,
            database_name.into(),
            None,
            events.into_iter().collect(),
        )
    }

    // cargo-mutants: real DB connection; not exercised under unit tests.
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(slot = %config.slot_name, publication = %config.publication_name, schema = %config.schema),
        err,
    )]
    pub async fn connect(config: PostgresSourceConfig) -> Result<Self, ConnectError> {
        let catalog = TokioPgCatalog::connect(config.database_url.expose_secret()).await?;
        let database_name = initialize(&catalog, &config).await?;
        Self::build(
            config,
            database_name,
            Some(Box::new(catalog)),
            VecDeque::new(),
        )
    }

    #[cfg(test)]
    fn with_catalog(
        config: PostgresSourceConfig,
        database_name: impl Into<String>,
        catalog: Box<dyn PgCatalog>,
    ) -> Result<Self, ConnectError> {
        Self::build(config, database_name.into(), Some(catalog), VecDeque::new())
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.pending.len()
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(slot = %self.config.slot_name, changes = tracing::field::Empty),
        err,
    )]
    async fn fill_pending_from_slot(&mut self) -> Result<(), ConnectError> {
        let Some(catalog) = &self.catalog else {
            return Ok(());
        };

        // This live seam intentionally peeks instead of getting changes. Peeking
        // avoids advancing the Postgres replication slot before downstream sink
        // durability is known; acknowledge advances the slot only after the
        // runtime has committed the sink and saved the checkpoint.
        let changes = catalog
            .peek_changes(
                &self.config.slot_name,
                self.config.max_messages_per_poll,
                &self.config.publication_name,
            )
            .await?;

        tracing::Span::current().record("changes", changes.len());
        for change in changes {
            let lsn = change.lsn.parse::<PgLsn>()?;

            self.apply_decoded_message(decode_pgoutput_message(&change.data, lsn, None)?);
        }

        Ok(())
    }

    fn apply_decoded_message(&mut self, message: DecodedMessage) {
        match message {
            DecodedMessage::Begin { final_lsn, xid } => {
                self.apply_logical_event(LogicalEvent::Begin { final_lsn, xid });
            }
            DecodedMessage::Commit {
                commit_lsn,
                end_lsn,
                commit_timestamp_ms,
            } => self.apply_logical_event(LogicalEvent::Commit {
                commit_lsn,
                end_lsn,
                commit_timestamp_ms,
            }),
            DecodedMessage::Relation(relation) => {
                self.apply_logical_event(LogicalEvent::Relation(relation));
            }
            DecodedMessage::Row(row) => self.apply_logical_event(LogicalEvent::Row(row)),
            DecodedMessage::Keepalive => {}
        }
    }

    fn apply_logical_event(&mut self, event: LogicalEvent) {
        match event {
            LogicalEvent::Begin { final_lsn: _, xid } => {
                self.transaction = Some(TransactionState { xid });
                self.transaction_rows.clear();
            }
            LogicalEvent::Commit {
                commit_lsn: _,
                end_lsn,
                commit_timestamp_ms,
            } => {
                self.commit_transaction(end_lsn.0, commit_timestamp_ms);
            }
            LogicalEvent::Relation(relation) => {
                self.pending.push_back(LogicalEvent::Relation(relation));
            }
            LogicalEvent::Row(row) => {
                self.enqueue_row(row);
            }
        }
    }

    fn enqueue_row(&mut self, row: RowEvent) {
        if self.should_skip_row(&row) {
            return;
        }

        if self.transaction.is_some() {
            if !self.transaction_rows.contains(&row) {
                self.transaction_rows.push(row);
            }
        } else {
            self.pending.push_back(LogicalEvent::Row(row));
        }
    }

    fn commit_transaction(&mut self, end_lsn: PgLsn, commit_timestamp_ms: i64) {
        let Some(transaction) = self.transaction.take() else {
            return;
        };
        if self.should_skip_lsn(end_lsn) {
            self.transaction_rows.clear();
            return;
        }

        for mut row in self.transaction_rows.drain(..) {
            row.commit_lsn = Some(end_lsn);
            row.txid = Some(transaction.xid);
            row.commit_timestamp_ms = Some(commit_timestamp_ms);
            self.pending.push_back(LogicalEvent::Row(row));
        }
    }

    fn should_skip_row(&self, row: &RowEvent) -> bool {
        let checkpoint_lsn = row.commit_lsn.unwrap_or(row.lsn);
        if self.should_skip_resume_lsn(checkpoint_lsn) {
            return true;
        }
        if row.commit_lsn.is_some() {
            return false;
        }

        self.should_skip_checkpoint_lsn(row.lsn)
    }

    fn should_skip_lsn(&self, lsn: PgLsn) -> bool {
        self.should_skip_resume_lsn(lsn) || self.should_skip_checkpoint_lsn(lsn)
    }

    fn should_skip_resume_lsn(&self, lsn: PgLsn) -> bool {
        self.resume_lsn.is_some_and(|resume_lsn| lsn <= resume_lsn)
    }

    fn should_skip_checkpoint_lsn(&self, lsn: PgLsn) -> bool {
        // Because the live path peeks, Postgres can return rows that were
        // already emitted and checkpointed in this process. Skip them here so
        // duplicate peek results do not re-emit before the runtime calls
        // acknowledge, which advances the replication slot after checkpoint
        // persistence.
        self.checkpoint
            .is_some_and(|checkpoint_lsn| lsn <= checkpoint_lsn)
    }
}

#[async_trait]
impl Source<Bytes, Bytes> for PostgresWalSource {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(slot = %self.config.slot_name, table = tracing::field::Empty, op = tracing::field::Empty),
        err,
    )]
    async fn poll(&mut self) -> Result<Option<ConnectRecord<Bytes, Bytes>>, ConnectError> {
        if self.pending.is_empty() {
            self.fill_pending_from_slot().await?;
        }

        while let Some(event) = self.pending.front().cloned() {
            match event {
                LogicalEvent::Begin { .. } | LogicalEvent::Commit { .. } => {
                    self.pending.pop_front();
                    self.apply_logical_event(event);
                }
                LogicalEvent::Relation(relation) => {
                    self.pending.pop_front();
                    self.relation_cache.apply_relation(relation);
                }
                LogicalEvent::Row(row) => {
                    if self.transaction.is_some() && row.commit_lsn.is_none() {
                        self.pending.pop_front();
                        self.apply_logical_event(LogicalEvent::Row(row));
                        continue;
                    }

                    if self.should_skip_row(&row) {
                        self.pending.pop_front();
                        continue;
                    }

                    let diff = self.relation_cache.translate(row)?;
                    let span = tracing::Span::current();
                    span.record("table", diff.table.as_str());
                    span.record("op", operation_header(diff.op));
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

                    self.pending.pop_front();
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

    #[tracing::instrument(level = "debug", skip_all, fields(slot = %self.config.slot_name), err)]
    async fn seek(&mut self, offset: SourceOffset) -> Result<(), ConnectError> {
        validate_database(&offset, &self.database_name)?;
        let lsn = PgLsn::from_source_offset(&offset, &self.config.slot_name)?;
        self.checkpoint = Some(lsn);
        self.resume_lsn = Some(lsn);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(slot = %self.config.slot_name), err)]
    async fn acknowledge(&mut self, offset: &SourceOffset) -> Result<(), ConnectError> {
        validate_database(offset, &self.database_name)?;
        let lsn = PgLsn::from_source_offset(offset, &self.config.slot_name)?;

        if let Some(catalog) = &self.catalog {
            let lsn_text = lsn.to_string();
            catalog
                .advance_slot(&self.config.slot_name, &lsn_text)
                .await?;
        }

        self.resume_lsn = Some(lsn);
        Ok(())
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn create_publication_sql(publication: &str, schema: &str, tables: &[String]) -> String {
    let table_list = tables
        .iter()
        .map(|table| format!("{}.{}", quote_ident(schema), quote_ident(table)))
        .collect::<Vec<_>>()
        .join(", ");
    let create_sql = format!(
        "CREATE PUBLICATION {} FOR TABLE {} WITH (publish = 'insert, update, delete')",
        quote_ident(publication),
        table_list
    );

    format!(
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = {}) THEN EXECUTE {}; END IF; END $$",
        sql_string(publication),
        sql_string(&create_sql)
    )
}

pub(crate) fn peek_binary_changes_sql(slot: &str, max_messages: u32, publication: &str) -> String {
    format!(
        "SELECT lsn::text AS lsn, data FROM pg_logical_slot_peek_binary_changes({}, NULL, {}, 'proto_version', '1', 'publication_names', {})",
        sql_string(slot),
        max_messages,
        sql_string(publication)
    )
}

pub(crate) fn publication_tables_sql() -> &'static str {
    "SELECT tablename FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2"
}

pub(crate) fn publication_settings_sql() -> &'static str {
    "SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1"
}

pub(crate) fn replication_slot_sql() -> &'static str {
    "SELECT slot_name, plugin, slot_type, database FROM pg_replication_slots WHERE slot_name = $1"
}

pub(crate) fn create_logical_slot_sql() -> &'static str {
    "SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')"
}

pub(crate) fn advance_slot_sql() -> &'static str {
    "SELECT pg_replication_slot_advance($1, $2::pg_lsn)"
}

/// Run the one-time connection setup against `catalog`: resolve the database
/// name, create + validate the publication (when tables are configured), and
/// ensure the replication slot. Split out of [`PostgresWalSource::connect`] so
/// the orchestration is unit-testable against a [`PgCatalog`] mock.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(slot = %config.slot_name, publication = %config.publication_name, tables = config.table_names.len()),
    err,
)]
async fn initialize(
    catalog: &dyn PgCatalog,
    config: &PostgresSourceConfig,
) -> Result<String, ConnectError> {
    let database_name = catalog.current_database().await?;

    if !config.table_names.is_empty() {
        catalog
            .ensure_publication(&create_publication_sql(
                &config.publication_name,
                &config.schema,
                &config.table_names,
            ))
            .await?;
        validate_publication_tables(
            catalog,
            &config.publication_name,
            &config.schema,
            &config.table_names,
        )
        .await?;
        validate_publication_settings(catalog, &config.publication_name).await?;
    }

    ensure_slot(catalog, &config.slot_name, &database_name).await?;

    Ok(database_name)
}

async fn validate_publication_tables(
    catalog: &dyn PgCatalog,
    publication: &str,
    schema: &str,
    tables: &[String],
) -> Result<(), ConnectError> {
    let published_tables = catalog.published_tables(publication, schema).await?;
    let missing_tables = missing_publication_tables(tables, published_tables);

    if missing_tables.is_empty() {
        Ok(())
    } else {
        Err(ConnectError::Backend(format!(
            "publication {publication:?} does not cover configured tables: {}",
            missing_tables.join(", ")
        )))
    }
}

fn missing_publication_tables(
    tables: &[String],
    published_tables: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let published_tables = published_tables.into_iter().collect::<HashSet<_>>();
    tables
        .iter()
        .filter(|table| !published_tables.contains(*table))
        .cloned()
        .collect()
}

async fn validate_publication_settings(
    catalog: &dyn PgCatalog,
    publication: &str,
) -> Result<(), ConnectError> {
    let Some(flags) = catalog.publication_settings(publication).await? else {
        return Ok(());
    };

    if publication_settings_are_compatible(flags) {
        Ok(())
    } else {
        Err(ConnectError::Backend(format!(
            "publication {publication:?} must publish insert, update, and delete, and must not publish truncate"
        )))
    }
}

fn publication_settings_are_compatible([insert, update, delete, truncate]: [bool; 4]) -> bool {
    insert && update && delete && !truncate
}

async fn ensure_slot(
    catalog: &dyn PgCatalog,
    slot_name: &str,
    database_name: &str,
) -> Result<(), ConnectError> {
    let Some(slot) = catalog.replication_slot(slot_name).await? else {
        catalog.create_logical_slot(slot_name).await?;
        return Ok(());
    };

    validate_slot_metadata(
        slot_name,
        slot.plugin.as_deref(),
        &slot.slot_type,
        slot.database.as_deref(),
        database_name,
    )
}

fn validate_slot_metadata(
    slot_name: &str,
    plugin: Option<&str>,
    slot_type: &str,
    database: Option<&str>,
    database_name: &str,
) -> Result<(), ConnectError> {
    let mut mismatches = Vec::new();

    if plugin != Some("pgoutput") {
        mismatches.push(format!("plugin is {plugin:?}"));
    }
    if slot_type != "logical" {
        mismatches.push(format!("slot_type is {slot_type:?}"));
    }
    if database != Some(database_name) {
        mismatches.push(format!("database is {database:?}"));
    }

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(ConnectError::Backend(format!(
            "replication slot {slot_name:?} is not compatible: {}",
            mismatches.join(", ")
        )))
    }
}

fn validate_database(offset: &SourceOffset, expected_database: &str) -> Result<(), ConnectError> {
    match offset.partition.get("database") {
        Some(OffsetValue::String(database)) if database == expected_database => Ok(()),
        Some(OffsetValue::String(database)) => Err(ConnectError::Offset(format!(
            "source offset database {database:?} does not match expected database {expected_database:?}"
        ))),
        _ => Err(ConnectError::Offset(
            "source offset missing string database".to_owned(),
        )),
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
mod sql_tests {
    use assert2::check;

    use super::{
        advance_slot_sql, create_logical_slot_sql, create_publication_sql,
        missing_publication_tables, peek_binary_changes_sql, publication_settings_are_compatible,
        publication_settings_sql, publication_tables_sql, replication_slot_sql,
        validate_slot_metadata,
    };

    #[test]
    fn create_logical_slot_sql_uses_pgoutput_plugin() {
        check!(
            create_logical_slot_sql()
                == "SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')"
        );
    }

    #[test]
    fn publication_sql_quotes_identifiers() {
        let sql = create_publication_sql(
            "pub\"name",
            "sch\"ema",
            &["orders".to_owned(), "line\"items".to_owned()],
        );

        check!(sql.contains("CREATE PUBLICATION \"pub\"\"name\""));
        check!(
            sql.contains("FOR TABLE \"sch\"\"ema\".\"orders\", \"sch\"\"ema\".\"line\"\"items\"")
        );
        check!(sql.contains("WHERE pubname = 'pub\"name'"));
    }

    #[test]
    fn peek_binary_changes_sql_escapes_string_literals_without_advancing_slot() {
        let sql = peek_binary_changes_sql("slot'name", 25, "pub'name");

        check!(
            sql == "SELECT lsn::text AS lsn, data FROM pg_logical_slot_peek_binary_changes('slot''name', NULL, 25, 'proto_version', '1', 'publication_names', 'pub''name')"
        );
    }

    #[test]
    fn publication_sql_excludes_truncate_events() {
        let sql = create_publication_sql("pub", "public", &["orders".to_owned()]);

        check!(sql.contains("WITH (publish = ''insert, update, delete'')"));
    }

    #[test]
    fn validation_sql_cases() {
        for (_name, actual, expected) in [
            (
                "publication_tables",
                publication_tables_sql(),
                "SELECT tablename FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2",
            ),
            (
                "publication_settings",
                publication_settings_sql(),
                "SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1",
            ),
            (
                "replication_slot",
                replication_slot_sql(),
                "SELECT slot_name, plugin, slot_type, database FROM pg_replication_slots WHERE slot_name = $1",
            ),
            (
                "advance_slot",
                advance_slot_sql(),
                "SELECT pg_replication_slot_advance($1, $2::pg_lsn)",
            ),
        ] {
            assert2::assert!(actual == expected);
        }
    }

    #[test]
    fn publication_table_validation_reports_only_missing_configured_tables() {
        let missing = missing_publication_tables(
            &["orders".to_owned(), "accounts".to_owned()],
            ["orders".to_owned(), "ignored".to_owned()],
        );

        check!(missing == vec!["accounts".to_owned()]);
    }

    #[test]
    fn publication_settings_require_insert_update_delete_without_truncate() {
        for (name, flags, expected) in [
            ("required-flags", [true, true, true, false], true),
            ("missing-insert", [false, true, true, false], false),
            ("missing-update", [true, false, true, false], false),
            ("missing-delete", [true, true, false, false], false),
            ("includes-truncate", [true, true, true, true], false),
        ] {
            check!(
                publication_settings_are_compatible(flags) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn slot_metadata_accepts_pgoutput_logical_slot_for_current_database() {
        validate_slot_metadata("slot_a", Some("pgoutput"), "logical", Some("app"), "app")
            .expect("matching slot should validate");
    }

    #[test]
    fn slot_metadata_reports_all_incompatible_fields() {
        let error =
            validate_slot_metadata("slot_a", Some("test_decoding"), "physical", None, "app")
                .expect_err("incompatible slot should fail");

        match error {
            crabka_connect::ConnectError::Backend(message) => {
                check!(
                    (
                        message.contains("replication slot \"slot_a\" is not compatible"),
                        message.contains("plugin is Some(\"test_decoding\")"),
                        message.contains("slot_type is \"physical\""),
                        message.contains("database is None"),
                    ) == (true, true, true, true)
                );
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;
    use crabka_connect::{SecretString, Source as _};
    use crabka_schema_serde::wire::MAGIC;

    use super::{LogicalEvent, PostgresWalSource, validate_database};
    use crate::{
        PgLsn, PostgresSourceConfig,
        ids::{CommitLsn, EndLsn, RelationId, TransactionId},
        model::{ColumnSchema, ColumnValue, ScalarValue},
        pgoutput::{RelationEvent, RowEvent, RowEventKind, RowTupleKind},
    };

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
            relation_id: RelationId(7),
            lsn,
            commit_lsn: None,
            txid: Some(TransactionId(99)),
            commit_timestamp_ms: Some(1_700_000_000_000),
            kind: RowEventKind::Insert,
            values: vec![id(42), status("paid")],
        }
    }

    fn delete_event(lsn: PgLsn) -> RowEvent {
        RowEvent {
            relation_id: RelationId(7),
            lsn,
            commit_lsn: None,
            txid: None,
            commit_timestamp_ms: None,
            kind: RowEventKind::Delete {
                tuple_kind: RowTupleKind::Full,
            },
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

        check!(
            (
                record.key.as_ref().map(|key| key[0]),
                record.value.as_ref().map(|value| value[0]),
                record.timestamp,
                header_value(&record, "crabka.pg.table"),
                header_value(&record, "crabka.pg.lsn"),
                header_value(&record, "crabka.pg.operation"),
                source.checkpoint(),
            ) == (
                Some(MAGIC),
                Some(MAGIC),
                Some(1_700_000_000_000),
                Bytes::from_static(b"public.orders"),
                Bytes::from_static(b"0/2A"),
                Bytes::from_static(b"insert"),
                Some(PgLsn(0x2a).to_source_offset("app", "slot_a")),
            )
        );
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

        check!(
            (
                record.key.is_some(),
                record.value.is_some(),
                record.timestamp,
                header_value(&record, "crabka.pg.table"),
                header_value(&record, "crabka.pg.lsn"),
                header_value(&record, "crabka.pg.operation"),
            ) == (
                true,
                false,
                None,
                Bytes::from_static(b"public.orders"),
                Bytes::from_static(b"0/2B"),
                Bytes::from_static(b"delete"),
            )
        );
    }

    #[tokio::test]
    async fn seek_restores_lsn_checkpoint() {
        let mut source = PostgresWalSource::scripted(config("slot_a"), "app", []).unwrap();
        let offset = PgLsn(0x2a).to_source_offset("app", "slot_a");

        source.seek(offset).await.expect("seek succeeds");

        check!(source.checkpoint() == Some(PgLsn(0x2a).to_source_offset("app", "slot_a")));
    }

    #[test]
    fn skip_lsn_checks_resume_and_checkpoint_offsets_independently() {
        let mut source = PostgresWalSource::scripted(config("slot_a"), "app", []).unwrap();
        source.resume_lsn = Some(PgLsn(0x20));

        check!(source.should_skip_lsn(PgLsn(0x20)));
        check!(!source.should_skip_lsn(PgLsn(0x21)));

        source.resume_lsn = None;
        source.checkpoint = Some(PgLsn(0x30));

        check!(source.should_skip_lsn(PgLsn(0x30)));
        check!(!source.should_skip_lsn(PgLsn(0x31)));

        source.resume_lsn = Some(PgLsn(0x20));
        source.checkpoint = Some(PgLsn(0x30));

        check!(source.should_skip_lsn(PgLsn(0x20)));
        check!(source.should_skip_lsn(PgLsn(0x30)));
        check!(!source.should_skip_lsn(PgLsn(0x31)));
    }

    #[tokio::test]
    async fn seek_past_first_row_poll_emits_only_later_row_without_regressing_checkpoint() {
        let mut source = PostgresWalSource::scripted(
            config("slot_a"),
            "app",
            [
                LogicalEvent::Relation(orders_relation()),
                LogicalEvent::Row(insert_event(PgLsn(0x2a))),
                LogicalEvent::Row(insert_event(PgLsn(0x2b))),
            ],
        )
        .expect("source builds");

        source
            .seek(PgLsn(0x2a).to_source_offset("app", "slot_a"))
            .await
            .expect("seek succeeds");

        let record = source
            .poll()
            .await
            .expect("poll succeeds")
            .expect("later row emits");

        check!(header_value(&record, "crabka.pg.lsn").as_ref() == b"0/2B");
        check!(source.checkpoint() == Some(PgLsn(0x2b).to_source_offset("app", "slot_a")));
        check!(source.poll().await.expect("poll succeeds").is_none());
    }

    #[tokio::test]
    async fn checkpointed_duplicate_row_is_skipped_without_regressing_checkpoint() {
        let mut source = PostgresWalSource::scripted(
            config("slot_a"),
            "app",
            [
                LogicalEvent::Relation(orders_relation()),
                LogicalEvent::Row(insert_event(PgLsn(0x2a))),
                LogicalEvent::Row(insert_event(PgLsn(0x2a))),
            ],
        )
        .expect("source builds");

        let record = source
            .poll()
            .await
            .expect("poll succeeds")
            .expect("first row emits");

        check!(header_value(&record, "crabka.pg.lsn").as_ref() == b"0/2A");
        check!(source.poll().await.expect("poll succeeds").is_none());
        check!(source.checkpoint() == Some(PgLsn(0x2a).to_source_offset("app", "slot_a")));
    }

    #[tokio::test]
    async fn translation_failure_does_not_drop_pending_row() {
        let mut source = PostgresWalSource::scripted(
            config("slot_a"),
            "app",
            [LogicalEvent::Row(insert_event(PgLsn(0x2a)))],
        )
        .expect("source builds");

        check!(source.poll().await.is_err());

        check!(source.pending_len() == 1);
        check!(source.checkpoint().is_none());
    }

    #[tokio::test]
    async fn seek_rejects_offset_for_different_database() {
        let mut source = PostgresWalSource::scripted(config("slot_a"), "app", []).unwrap();
        let offset = PgLsn(0x2a).to_source_offset("other_app", "slot_a");

        let error = source.seek(offset).await.expect_err("database mismatch");

        check!(matches!(error, crabka_connect::ConnectError::Offset(_)));
        check!(source.checkpoint().is_none());
    }

    #[test]
    fn validate_database_rejects_missing_or_non_string_database_partition() {
        let missing = crabka_connect::SourceOffset::default();
        let mut non_string = crabka_connect::SourceOffset::default();
        non_string
            .partition
            .0
            .insert("database".to_owned(), crabka_connect::OffsetValue::Long(7));

        for (_name, offset) in [("missing", missing), ("non_string", non_string)] {
            assert2::assert!(matches!(
                validate_database(&offset, "app"),
                Err(crabka_connect::ConnectError::Offset(_))
            ));
        }
    }

    #[test]
    fn validate_database_reports_database_mismatch() {
        let offset = PgLsn(0x2a).to_source_offset("other_app", "slot_a");

        let error = validate_database(&offset, "app").expect_err("database mismatch should fail");

        match error {
            crabka_connect::ConnectError::Offset(message) => {
                check!(message.contains("does not match expected database"));
                check!(message.contains("other_app"));
                check!(message.contains("app"));
            }
            error => panic!("expected offset error, got {error:?}"),
        }
    }

    #[tokio::test]
    async fn scripted_acknowledge_accepts_matching_offset_and_rejects_database_mismatch() {
        let mut source = PostgresWalSource::scripted(config("slot_a"), "app", []).unwrap();

        source
            .acknowledge(&PgLsn(0x2a).to_source_offset("app", "slot_a"))
            .await
            .expect("matching offset acknowledged");

        let error = source
            .acknowledge(&PgLsn(0x2b).to_source_offset("other_app", "slot_a"))
            .await
            .expect_err("database mismatch rejected");

        check!(matches!(error, crabka_connect::ConnectError::Offset(_)));
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

    #[tokio::test]
    async fn transaction_commit_metadata_is_applied_to_all_rows() {
        let mut second = insert_event(PgLsn(0x2b));
        second.values = vec![id(43), status("pending")];
        let mut source = PostgresWalSource::scripted(
            config("slot_a"),
            "app",
            [
                LogicalEvent::Relation(orders_relation()),
                LogicalEvent::Begin {
                    final_lsn: PgLsn(0x40),
                    xid: TransactionId(123),
                },
                LogicalEvent::Row(insert_event(PgLsn(0x2a))),
                LogicalEvent::Row(second),
                LogicalEvent::Commit {
                    commit_lsn: CommitLsn(PgLsn(0x41)),
                    end_lsn: EndLsn(PgLsn(0x42)),
                    commit_timestamp_ms: 1_700_000_000_123,
                },
            ],
        )
        .expect("source builds");

        let first = source
            .poll()
            .await
            .expect("first poll succeeds")
            .expect("first row emits");
        let second = source
            .poll()
            .await
            .expect("second poll succeeds")
            .expect("second row emits");

        check!(
            (
                header_value(&first, "crabka.pg.lsn"),
                header_value(&second, "crabka.pg.lsn"),
                first.timestamp,
                second.timestamp,
                source.checkpoint(),
            ) == (
                Bytes::from_static(b"0/42"),
                Bytes::from_static(b"0/42"),
                Some(1_700_000_000_123),
                Some(1_700_000_000_123),
                Some(PgLsn(0x42).to_source_offset("app", "slot_a")),
            )
        );
        check!(source.poll().await.expect("poll succeeds").is_none());
    }

    #[test]
    fn decoded_transaction_messages_stage_rows_with_commit_metadata() {
        let mut source =
            PostgresWalSource::scripted(config("slot_a"), "app", []).expect("source builds");
        let mut row = insert_event(PgLsn(0x2a));
        row.txid = None;
        row.commit_timestamp_ms = None;

        source.apply_decoded_message(crate::pgoutput::DecodedMessage::Begin {
            final_lsn: PgLsn(0x40),
            xid: TransactionId(123),
        });
        source.apply_decoded_message(crate::pgoutput::DecodedMessage::Row(row));
        source.apply_decoded_message(crate::pgoutput::DecodedMessage::Commit {
            commit_lsn: CommitLsn(PgLsn(0x41)),
            end_lsn: EndLsn(PgLsn(0x42)),
            commit_timestamp_ms: 1_700_000_000_123,
        });

        let Some(LogicalEvent::Row(row)) = source.pending.pop_front() else {
            panic!("committed row should be pending");
        };
        let mut expected = insert_event(PgLsn(0x2a));
        expected.commit_lsn = Some(PgLsn(0x42));
        expected.txid = Some(TransactionId(123));
        expected.commit_timestamp_ms = Some(1_700_000_000_123);
        check!(row == expected);
    }
}

/// Mock-driven coverage for the connection-setup decision logic. The `PgCatalog`
/// seam lets these run entirely offline: every excluded-by-necessity live query
/// is replaced by a `mockall` expectation, so the validation/orchestration
/// branches carry real mutation signal without a running `PostgreSQL`.
#[cfg(test)]
mod catalog_tests {
    use assert2::check;
    use crabka_connect::{ConnectError, SecretString, Source as _};

    use super::{
        PostgresWalSource, ensure_slot, initialize, validate_publication_settings,
        validate_publication_tables,
    };
    use crate::{
        PgLsn, PostgresSourceConfig,
        catalog::{MockPgCatalog, SlotChange, SlotMetadata},
    };

    fn config_with_tables(tables: Vec<String>) -> PostgresSourceConfig {
        PostgresSourceConfig {
            database_url: SecretString::new("postgres://localhost/app"),
            slot_name: "slot_a".to_owned(),
            publication_name: "crabka_connect".to_owned(),
            schema: "public".to_owned(),
            table_names: tables,
            max_messages_per_poll: 1000,
        }
    }

    fn valid_slot() -> SlotMetadata {
        SlotMetadata {
            plugin: Some("pgoutput".to_owned()),
            slot_type: "logical".to_owned(),
            database: Some("app".to_owned()),
        }
    }

    /// A minimal valid pgoutput `Begin` frame (tag, then the final-LSN,
    /// commit-time, and xid fields); decoding it stages a transaction without
    /// needing real WAL bytes.
    fn begin_frame() -> Vec<u8> {
        let mut data = vec![b'B'];
        data.extend_from_slice(&0u64.to_be_bytes()); // final_lsn
        data.extend_from_slice(&0i64.to_be_bytes()); // commit_time
        data.extend_from_slice(&0u32.to_be_bytes()); // xid
        data
    }

    #[tokio::test]
    async fn validate_publication_tables_accepts_full_coverage_and_reports_gaps() {
        let mut catalog = MockPgCatalog::new();
        catalog
            .expect_published_tables()
            .returning(|_, _| Ok(vec!["orders".to_owned()]));

        validate_publication_tables(&catalog, "crabka_connect", "public", &["orders".to_owned()])
            .await
            .expect("full coverage validates");

        let mut missing = MockPgCatalog::new();
        missing
            .expect_published_tables()
            .returning(|_, _| Ok(Vec::new()));

        let error = validate_publication_tables(
            &missing,
            "crabka_connect",
            "public",
            &["orders".to_owned()],
        )
        .await
        .expect_err("uncovered table fails");
        match error {
            ConnectError::Backend(message) => {
                check!(message.contains("does not cover configured tables"));
                check!(message.contains("orders"));
            }
            error => panic!("expected backend error, got {error:?}"),
        }
    }

    #[tokio::test]
    async fn validate_publication_settings_requires_insert_update_delete_without_truncate() {
        let mut compatible = MockPgCatalog::new();
        compatible
            .expect_publication_settings()
            .returning(|_| Ok(Some([true, true, true, false])));
        validate_publication_settings(&compatible, "crabka_connect")
            .await
            .expect("compatible flags validate");

        let mut truncating = MockPgCatalog::new();
        truncating
            .expect_publication_settings()
            .returning(|_| Ok(Some([true, true, true, true])));
        let error = validate_publication_settings(&truncating, "crabka_connect")
            .await
            .expect_err("publishing truncate fails");
        check!(matches!(error, ConnectError::Backend(_)));

        // An absent publication row is tolerated (the create path handles it).
        let mut absent = MockPgCatalog::new();
        absent.expect_publication_settings().returning(|_| Ok(None));
        validate_publication_settings(&absent, "crabka_connect")
            .await
            .expect("missing publication row is tolerated");
    }

    #[tokio::test]
    async fn ensure_slot_creates_when_absent_and_validates_when_present() {
        let mut absent = MockPgCatalog::new();
        absent.expect_replication_slot().returning(|_| Ok(None));
        absent
            .expect_create_logical_slot()
            .times(1)
            .returning(|_| Ok(()));
        ensure_slot(&absent, "slot_a", "app")
            .await
            .expect("absent slot is created");

        // A present, compatible slot must not be recreated.
        let mut present = MockPgCatalog::new();
        present
            .expect_replication_slot()
            .returning(|_| Ok(Some(valid_slot())));
        ensure_slot(&present, "slot_a", "app")
            .await
            .expect("compatible slot validates");

        let mut mismatched = MockPgCatalog::new();
        mismatched.expect_replication_slot().returning(|_| {
            Ok(Some(SlotMetadata {
                plugin: Some("test_decoding".to_owned()),
                slot_type: "physical".to_owned(),
                database: Some("other".to_owned()),
            }))
        });
        let error = ensure_slot(&mismatched, "slot_a", "app")
            .await
            .expect_err("incompatible slot fails");
        check!(matches!(error, ConnectError::Backend(_)));
    }

    #[tokio::test]
    async fn initialize_runs_publication_setup_only_when_tables_configured() {
        let mut with_tables = MockPgCatalog::new();
        with_tables
            .expect_current_database()
            .returning(|| Ok("app".to_owned()));
        with_tables
            .expect_ensure_publication()
            .times(1)
            .returning(|_| Ok(()));
        with_tables
            .expect_published_tables()
            .returning(|_, _| Ok(vec!["orders".to_owned()]));
        with_tables
            .expect_publication_settings()
            .returning(|_| Ok(Some([true, true, true, false])));
        with_tables
            .expect_replication_slot()
            .returning(|_| Ok(Some(valid_slot())));

        let database = initialize(&with_tables, &config_with_tables(vec!["orders".to_owned()]))
            .await
            .expect("initialize succeeds");
        check!(database == "app");

        // With no tables configured, the publication path is skipped entirely
        // (no `ensure_publication` expectation set — calling it would panic).
        let mut no_tables = MockPgCatalog::new();
        no_tables
            .expect_current_database()
            .returning(|| Ok("app".to_owned()));
        no_tables
            .expect_replication_slot()
            .returning(|_| Ok(Some(valid_slot())));
        initialize(&no_tables, &config_with_tables(Vec::new()))
            .await
            .expect("initialize without tables succeeds");
    }

    #[tokio::test]
    async fn fill_pending_applies_peeked_changes_and_propagates_decode_input() {
        let mut catalog = MockPgCatalog::new();
        catalog.expect_peek_changes().returning(|_, _, _| {
            Ok(vec![SlotChange {
                lsn: "0/2A".to_owned(),
                data: begin_frame(),
            }])
        });

        let mut source = PostgresWalSource::with_catalog(
            config_with_tables(Vec::new()),
            "app",
            Box::new(catalog),
        )
        .expect("source builds");
        source
            .fill_pending_from_slot()
            .await
            .expect("fill succeeds");
        // The Begin frame opened a transaction — proof the peeked change was
        // decoded and applied rather than dropped.
        check!(source.transaction.is_some());
    }

    #[tokio::test]
    async fn fill_pending_surfaces_unparsable_lsn() {
        let mut catalog = MockPgCatalog::new();
        catalog.expect_peek_changes().returning(|_, _, _| {
            Ok(vec![SlotChange {
                lsn: "not-a-lsn".to_owned(),
                data: begin_frame(),
            }])
        });

        let mut source = PostgresWalSource::with_catalog(
            config_with_tables(Vec::new()),
            "app",
            Box::new(catalog),
        )
        .expect("source builds");
        check!(source.fill_pending_from_slot().await.is_err());
    }

    #[tokio::test]
    async fn fill_pending_without_catalog_is_a_noop() {
        let mut source =
            PostgresWalSource::scripted(config_with_tables(Vec::new()), "app", []).unwrap();
        source
            .fill_pending_from_slot()
            .await
            .expect("noop succeeds");
        check!(source.pending_len() == 0);
    }

    #[tokio::test]
    async fn acknowledge_advances_slot_through_catalog() {
        let mut catalog = MockPgCatalog::new();
        catalog
            .expect_advance_slot()
            .times(1)
            .returning(|_, _| Ok(()));

        let mut source = PostgresWalSource::with_catalog(
            config_with_tables(Vec::new()),
            "app",
            Box::new(catalog),
        )
        .expect("source builds");
        source
            .acknowledge(&PgLsn(0x2a).to_source_offset("app", "slot_a"))
            .await
            .expect("acknowledge advances the slot");
    }
}
