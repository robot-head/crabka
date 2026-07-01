//! Postgres catalog + replication-slot operations the WAL source depends on.
//!
//! The trait exists to give [`crate::source`]'s connection-setup *logic*
//! (publication/slot validation, slot orchestration, peek-result application) a
//! mockable seam: `tokio_postgres::Row` is opaque and cannot be constructed in
//! a test, so each method returns already-extracted plain data. The live
//! row-extraction shim lives in [`TokioPgCatalog`] and is the only part that
//! needs a running `PostgreSQL` — every decision is delegated back to the
//! unit-tested pure helpers in `source.rs`.

use async_trait::async_trait;
use crabka_connect::ConnectError;
use tokio_postgres::{Client, NoTls};

use crate::source::{
    advance_slot_sql, create_logical_slot_sql, peek_binary_changes_sql, publication_settings_sql,
    publication_tables_sql, replication_slot_sql,
};

/// Replication-slot metadata extracted from `pg_replication_slots`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotMetadata {
    pub plugin: Option<String>,
    pub slot_type: String,
    pub database: Option<String>,
}

/// A single peeked logical-decoding change (`lsn` text + raw pgoutput bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotChange {
    pub lsn: String,
    pub data: Vec<u8>,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait PgCatalog: Send + Sync + std::fmt::Debug {
    /// `SELECT current_database()`.
    async fn current_database(&self) -> Result<String, ConnectError>;

    /// Create the publication if it does not already exist (idempotent DDL).
    async fn ensure_publication(&self, ddl: &str) -> Result<(), ConnectError>;

    /// Table names currently covered by `publication` in `schema`.
    async fn published_tables(
        &self,
        publication: &str,
        schema: &str,
    ) -> Result<Vec<String>, ConnectError>;

    /// `[pubinsert, pubupdate, pubdelete, pubtruncate]` for `publication`, or
    /// `None` when the publication row is absent.
    async fn publication_settings(
        &self,
        publication: &str,
    ) -> Result<Option<[bool; 4]>, ConnectError>;

    /// Metadata for `slot_name`, or `None` when the slot does not exist yet.
    async fn replication_slot(&self, slot_name: &str)
    -> Result<Option<SlotMetadata>, ConnectError>;

    /// Create a `pgoutput` logical replication slot named `slot_name`.
    async fn create_logical_slot(&self, slot_name: &str) -> Result<(), ConnectError>;

    /// Peek up to `max_messages` binary changes without advancing the slot.
    async fn peek_changes(
        &self,
        slot_name: &str,
        max_messages: u32,
        publication: &str,
    ) -> Result<Vec<SlotChange>, ConnectError>;

    /// Advance `slot_name` to `lsn_text` (called after checkpoint durability).
    async fn advance_slot(&self, slot_name: &str, lsn_text: &str) -> Result<(), ConnectError>;
}

/// Live `tokio_postgres`-backed [`PgCatalog`]. Every method runs a query and
/// extracts opaque `Row` fields; the only un-mockable code in the connector.
#[derive(Debug)]
pub(crate) struct TokioPgCatalog {
    client: Client,
}

impl TokioPgCatalog {
    /// Dial `database_url`, spawn the connection driver task, and return the
    /// catalog. The spawned task owns the protocol connection for the lifetime
    /// of the client.
    #[tracing::instrument(level = "info", skip_all, err)]
    pub(crate) async fn connect(database_url: &str) -> Result<Self, ConnectError> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls)
            .await
            .map_err(|error| backend(&error))?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "postgres source connection task failed");
            }
        });

        Ok(Self { client })
    }
}

fn backend(error: &tokio_postgres::Error) -> ConnectError {
    ConnectError::Backend(error.to_string())
}

#[async_trait]
impl PgCatalog for TokioPgCatalog {
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn current_database(&self) -> Result<String, ConnectError> {
        self.client
            .query_one("SELECT current_database()", &[])
            .await
            .map(|row| row.get(0))
            .map_err(|error| backend(&error))
    }

    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn ensure_publication(&self, ddl: &str) -> Result<(), ConnectError> {
        self.client
            .batch_execute(ddl)
            .await
            .map_err(|error| backend(&error))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(publication = %publication, schema = %schema), err)]
    async fn published_tables(
        &self,
        publication: &str,
        schema: &str,
    ) -> Result<Vec<String>, ConnectError> {
        let rows = self
            .client
            .query(publication_tables_sql(), &[&publication, &schema])
            .await
            .map_err(|error| backend(&error))?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<_, String>("tablename"))
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(publication = %publication), err)]
    async fn publication_settings(
        &self,
        publication: &str,
    ) -> Result<Option<[bool; 4]>, ConnectError> {
        let Some(row) = self
            .client
            .query_opt(publication_settings_sql(), &[&publication])
            .await
            .map_err(|error| backend(&error))?
        else {
            return Ok(None);
        };

        Ok(Some([
            row.get("pubinsert"),
            row.get("pubupdate"),
            row.get("pubdelete"),
            row.get("pubtruncate"),
        ]))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(slot = %slot_name), err)]
    async fn replication_slot(
        &self,
        slot_name: &str,
    ) -> Result<Option<SlotMetadata>, ConnectError> {
        let Some(row) = self
            .client
            .query_opt(replication_slot_sql(), &[&slot_name])
            .await
            .map_err(|error| backend(&error))?
        else {
            return Ok(None);
        };

        Ok(Some(SlotMetadata {
            plugin: row.get("plugin"),
            slot_type: row.get("slot_type"),
            database: row.get("database"),
        }))
    }

    #[tracing::instrument(level = "info", skip_all, fields(slot = %slot_name), err)]
    async fn create_logical_slot(&self, slot_name: &str) -> Result<(), ConnectError> {
        self.client
            .query(create_logical_slot_sql(), &[&slot_name])
            .await
            .map_err(|error| backend(&error))?;
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(slot = %slot_name, max_messages, changes = tracing::field::Empty),
        err,
    )]
    async fn peek_changes(
        &self,
        slot_name: &str,
        max_messages: u32,
        publication: &str,
    ) -> Result<Vec<SlotChange>, ConnectError> {
        let rows = self
            .client
            .query(
                &peek_binary_changes_sql(slot_name, max_messages, publication),
                &[],
            )
            .await
            .map_err(|error| backend(&error))?;
        tracing::Span::current().record("changes", rows.len());
        Ok(rows
            .into_iter()
            .map(|row| SlotChange {
                lsn: row.get("lsn"),
                data: row.get("data"),
            })
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(slot = %slot_name, lsn = %lsn_text), err)]
    async fn advance_slot(&self, slot_name: &str, lsn_text: &str) -> Result<(), ConnectError> {
        self.client
            .execute(advance_slot_sql(), &[&slot_name, &lsn_text])
            .await
            .map_err(|error| backend(&error))?;
        Ok(())
    }
}
