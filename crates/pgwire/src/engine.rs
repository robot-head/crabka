//! Engine seam: types the wire layer exchanges with the query engine.

use std::future::Future;

use bytes::Bytes;

use crate::error::PgError;

/// Type OIDs from `pg_type.dat`. The stub needs only these two; the real
/// catalog crate will own the full set.
pub mod oids {
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
}

/// A single value, pre-encoded in both wire formats.
///
/// SP2 NOTE: pre-computing both encodings is fine for the stub but doubles
/// encoding work for a real engine; the wire layer knows the negotiated
/// format at Bind time and could request only one. Revisit this seam when
/// the real engine lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    pub text: Bytes,
    pub binary: Bytes,
}

/// One bound extended-query parameter in its client-supplied wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundParam {
    pub type_oid: Option<u32>,
    /// 0 = text, 1 = binary.
    pub format: i16,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResult {
    Rows {
        fields: Vec<FieldDescription>,
        rows: Vec<Vec<Option<Cell>>>,
        tag: String,
    },
    /// Statement with no result set (e.g. SET); tag like "INSERT 0 1".
    Command { tag: String },
    /// Empty query string → `EmptyQueryResponse`.
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyInResponse {
    /// 0 = text, 1 = binary.
    pub overall_format: i16,
    /// One format code per target column.
    pub column_formats: Vec<i16>,
}

pub use crate::messages::backend::TxStatus;

/// A database engine: a factory for per-connection sessions. Shared across all
/// connections (`Send + Sync`); each connection gets its own [`Session`].
///
/// SP1 ships only `StubEngine`; the real engine implements this same trait.
pub trait Engine: Send + Sync + 'static {
    type Session: Session;

    /// Create a fresh per-connection session. Called once per connection.
    fn connect(&self) -> Self::Session;
}

/// A per-connection session. Owns transaction state; not shared between
/// connections. `simple_query`/`describe` take `&mut self` because they mutate
/// transaction state.
///
/// Cancellation: the wire layer may DROP an in-flight `simple_query` future
/// (`tokio::select!`). Session implementations must be drop-safe mid-execution;
/// the real engine needs transaction cleanup on drop.
pub trait Session: Send {
    /// Execute the full text of a simple-protocol Query message (may contain
    /// multiple statements — splitting is the engine's job).
    fn simple_query(
        &mut self,
        sql: &str,
    ) -> impl Future<Output = Result<Vec<QueryResult>, PgError>> + Send;

    /// Execute one extended-protocol portal with already-bound parameters.
    fn extended_query(
        &mut self,
        sql: &str,
        params: &[BoundParam],
    ) -> impl Future<Output = Result<Vec<QueryResult>, PgError>> + Send {
        let _ = params;
        self.simple_query(sql)
    }

    /// Row description for a statement without executing it (extended-protocol
    /// Describe). Empty vec = statement returns no rows.
    fn describe(
        &mut self,
        sql: &str,
    ) -> impl Future<Output = Result<Vec<FieldDescription>, PgError>> + Send;

    /// Row description for a parsed statement, including client-supplied
    /// parameter type OIDs when the engine cannot infer them from SQL alone.
    fn describe_prepared(
        &mut self,
        sql: &str,
        param_types: &[u32],
    ) -> impl Future<Output = Result<(Vec<FieldDescription>, Vec<u32>), PgError>> + Send {
        async move {
            self.describe(sql)
                .await
                .map(|fields| (fields, param_types.to_vec()))
        }
    }

    /// Return `Some` when `sql` is a supported simple-query COPY FROM STDIN
    /// command. The wire layer enters copy-in mode and later calls `copy_in` with
    /// all received `CopyData` frames. Non-COPY SQL returns `None`.
    fn begin_copy_in(
        &mut self,
        sql: &str,
    ) -> impl Future<Output = Result<Option<CopyInResponse>, PgError>> + Send {
        let _ = sql;
        async { Ok(None) }
    }

    /// Finish a COPY FROM STDIN after the client sends `CopyDone`.
    fn copy_in(
        &mut self,
        sql: &str,
        data: Vec<Bytes>,
    ) -> impl Future<Output = Result<QueryResult, PgError>> + Send {
        let _ = (sql, data);
        async {
            Err(PgError::error(
                crate::error::sqlstate::FEATURE_NOT_SUPPORTED,
                "COPY FROM STDIN is not supported by this engine",
            ))
        }
    }

    /// Mark the current statement as failed after a protocol-side error.
    ///
    /// COPY FROM STDIN can fail because the client sends `CopyFail`, before the
    /// engine sees `copy_in`. Engines with explicit transaction state must abort
    /// the open transaction block here while leaving autocommit sessions usable.
    fn mark_statement_failed(&mut self) {}

    /// The transaction status reported to the client in `ReadyForQuery`.
    fn tx_status(&self) -> TxStatus;
}

/// One column in a `RowDescription`. Field order matches the wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: u32,
    pub column_id: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    /// 0 = text, 1 = binary.
    pub format: i16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stub::StubEngine;

    #[tokio::test]
    async fn stub_answers_select_1() {
        let engine = StubEngine::new();
        let mut s = engine.connect();
        let results = s.simple_query("SELECT 1").await.expect("ok");
        let [QueryResult::Rows { fields, rows, tag }] = &results[..] else {
            panic!("expected one Rows result, got {results:?}");
        };
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "?column?");
        assert_eq!(fields[0].type_oid, oids::INT4);
        assert_eq!(tag, "SELECT 1");
        assert_eq!(rows.len(), 1);
        let cell = rows[0][0].as_ref().expect("not null");
        assert_eq!(&cell.text[..], b"1");
        assert_eq!(&cell.binary[..], &1i32.to_be_bytes());
    }

    #[tokio::test]
    async fn stub_answers_version_case_insensitively() {
        let engine = StubEngine::new();
        let mut s = engine.connect();
        let results = s.simple_query("select VERSION()").await.expect("ok");
        let [QueryResult::Rows { fields, rows, tag }] = &results[..] else {
            panic!("expected Rows");
        };
        assert_eq!(fields[0].type_oid, oids::TEXT);
        let text = std::str::from_utf8(&rows[0][0].as_ref().expect("not null").text).expect("utf8");
        assert!(
            text.starts_with("PostgreSQL 18"),
            "clients parse this prefix: {text}"
        );
        assert_eq!(tag, "SELECT 1");
    }

    #[tokio::test]
    async fn stub_rejects_unknown_sql_with_feature_not_supported() {
        let engine = StubEngine::new();
        let mut s = engine.connect();
        let err = s
            .simple_query("SELECT * FROM t")
            .await
            .expect_err("must fail");
        assert_eq!(err.code, crate::error::sqlstate::FEATURE_NOT_SUPPORTED);
    }

    #[tokio::test]
    async fn stub_handles_empty_query() {
        let engine = StubEngine::new();
        let mut s = engine.connect();
        let results = s.simple_query("   ").await.expect("ok");
        assert_eq!(results, vec![QueryResult::Empty]);
    }

    #[tokio::test]
    async fn stub_extended_query_echoes_bound_text_parameter() {
        let engine = StubEngine::new();
        let mut s = engine.connect();
        let params = [BoundParam {
            type_oid: Some(oids::TEXT),
            format: 0,
            value: Some(Bytes::from_static(b"hello")),
        }];
        let results = s
            .extended_query("SELECT $1", &params)
            .await
            .expect("parameterized query works");
        let [QueryResult::Rows { fields, rows, tag }] = &results[..] else {
            panic!("expected one Rows result, got {results:?}");
        };
        assert_eq!(fields[0].type_oid, oids::TEXT);
        assert_eq!(tag, "SELECT 1");
        assert_eq!(rows[0][0].as_ref().expect("not null").text, "hello");
    }

    #[tokio::test]
    async fn stub_extended_query_preserves_null_and_binary_format() {
        let engine = StubEngine::new();
        let mut s = engine.connect();
        let params = [BoundParam {
            type_oid: Some(oids::TEXT),
            format: 1,
            value: None,
        }];
        let results = s
            .extended_query("SELECT $1", &params)
            .await
            .expect("null parameter works");
        let [QueryResult::Rows { rows, .. }] = &results[..] else {
            panic!("expected one Rows result, got {results:?}");
        };
        assert_eq!(rows[0][0], None);
    }

    #[tokio::test]
    async fn stub_describe_returns_fields_without_executing() {
        let engine = StubEngine::new();
        let mut s = engine.connect();
        let described = s.describe("SELECT 1").await.expect("ok");
        assert_eq!(described.len(), 1);
        assert_eq!(described[0].type_oid, oids::INT4);
    }

    #[tokio::test]
    async fn stub_pg_sleep_zero_completes_with_one_row() {
        let engine = StubEngine::new();
        let mut s = engine.connect();
        let results = s.simple_query("SELECT pg_sleep(0)").await.expect("ok");
        let [QueryResult::Rows { fields, rows, tag }] = &results[..] else {
            panic!("expected Rows");
        };
        assert_eq!(fields[0].name, "pg_sleep");
        assert_eq!(rows.len(), 1);
        assert_eq!(tag, "SELECT 1");
    }
}
