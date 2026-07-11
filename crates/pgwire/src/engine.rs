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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedDescription {
    pub parameter_types: Vec<u32>,
    pub fields: Vec<FieldDescription>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortalDescription {
    pub fields: Vec<FieldDescription>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseTarget<'a> {
    Statement(&'a str),
    Portal(&'a str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOutResponse {
    pub overall_format: i16,
    pub column_formats: Vec<i16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub process_id: i32,
    pub channel: String,
    pub payload: String,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteOutcome {
    /// Values are already encoded in the portal's negotiated result formats.
    Rows {
        rows: Vec<Vec<Option<Bytes>>>,
        completion: Option<String>,
    },
    CommandComplete {
        tag: String,
    },
    EmptyQuery,
    CopyIn {
        response: CopyInResponse,
    },
    CopyOut {
        response: CopyOutResponse,
    },
    Notification {
        notification: Notification,
    },
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

    fn parse(
        &mut self,
        name: &str,
        sql: &str,
        parameter_types: &[u32],
    ) -> impl Future<Output = Result<PreparedDescription, PgError>> + Send;
    fn bind(
        &mut self,
        portal: &str,
        statement: &str,
        params: &[BoundParam],
        result_formats: &[i16],
    ) -> impl Future<Output = Result<PortalDescription, PgError>> + Send;
    fn describe_statement(
        &mut self,
        name: &str,
    ) -> impl Future<Output = Result<PreparedDescription, PgError>> + Send;
    fn describe_portal(
        &mut self,
        name: &str,
    ) -> impl Future<Output = Result<PortalDescription, PgError>> + Send;
    fn execute(
        &mut self,
        portal: &str,
        max_rows: u32,
    ) -> impl Future<Output = Result<ExecuteOutcome, PgError>> + Send;
    fn close(
        &mut self,
        target: CloseTarget<'_>,
    ) -> impl Future<Output = Result<(), PgError>> + Send;
    fn sync(&mut self) -> impl Future<Output = Result<(), PgError>> + Send;

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

    struct RecordingSession;

    impl Session for RecordingSession {
        async fn simple_query(&mut self, _: &str) -> Result<Vec<QueryResult>, PgError> {
            Ok(vec![])
        }
        async fn parse(
            &mut self,
            _: &str,
            _: &str,
            _: &[u32],
        ) -> Result<PreparedDescription, PgError> {
            todo!()
        }
        async fn bind(
            &mut self,
            _: &str,
            _: &str,
            _: &[BoundParam],
            _: &[i16],
        ) -> Result<PortalDescription, PgError> {
            todo!()
        }
        async fn describe_statement(&mut self, _: &str) -> Result<PreparedDescription, PgError> {
            todo!()
        }
        async fn describe_portal(&mut self, _: &str) -> Result<PortalDescription, PgError> {
            todo!()
        }
        async fn execute(&mut self, _: &str, _: u32) -> Result<ExecuteOutcome, PgError> {
            todo!()
        }
        async fn close(&mut self, _: CloseTarget<'_>) -> Result<(), PgError> {
            Ok(())
        }
        async fn sync(&mut self) -> Result<(), PgError> {
            Ok(())
        }
        fn tx_status(&self) -> TxStatus {
            TxStatus::Idle
        }
    }

    fn assert_native_session_is_send<T: Session + Send>() {}

    #[test]
    fn session_v2_supports_native_async_implementations() {
        assert_native_session_is_send::<RecordingSession>();
    }

    #[tokio::test]
    async fn engine_owns_replacement_close_and_sync_lifetimes() {
        let mut session = StubEngine::new().connect();
        session
            .parse("", "SELECT 1", &[])
            .await
            .expect("unnamed parse");
        session.bind("old", "", &[], &[]).await.expect("old portal");
        session
            .parse("", "SELECT version()", &[])
            .await
            .expect("replace unnamed statement");
        let ExecuteOutcome::Rows { completion, .. } = session
            .execute("old", 0)
            .await
            .expect("bound portal remains independent")
        else {
            panic!("rows")
        };
        assert_eq!(completion.as_deref(), Some("SELECT 1"));
        session
            .bind("", "", &[], &[])
            .await
            .expect("unnamed portal");
        session
            .bind("", "", &[], &[])
            .await
            .expect("replace unnamed portal");
        session
            .close(CloseTarget::Statement(""))
            .await
            .expect("close statement");
        session
            .execute("", 0)
            .await
            .expect("closing statement preserves portal");
        session
            .parse("survivor", "SELECT 1", &[])
            .await
            .expect("named parse");
        session.sync().await.expect("sync");
        assert_eq!(
            session
                .execute("", 0)
                .await
                .expect_err("sync removes portals")
                .code,
            crate::error::sqlstate::INVALID_CURSOR_NAME
        );
        session
            .bind("after-sync", "survivor", &[], &[])
            .await
            .expect("prepared survives sync");
        session
            .close(CloseTarget::Portal("missing"))
            .await
            .expect("nonexistent close succeeds");
    }

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
        let prepared = s
            .parse("s", "SELECT $1", &[oids::TEXT])
            .await
            .expect("parse");
        assert_eq!(prepared.fields[0].type_oid, oids::TEXT);
        s.bind("p", "s", &params, &[0]).await.expect("bind");
        let ExecuteOutcome::Rows { rows, completion } = s.execute("p", 0).await.expect("execute")
        else {
            panic!("rows")
        };
        assert_eq!(completion.as_deref(), Some("SELECT 1"));
        assert_eq!(rows[0][0].as_deref(), Some(&b"hello"[..]));
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
        s.parse("s", "SELECT $1", &[oids::TEXT])
            .await
            .expect("parse");
        s.bind("p", "s", &params, &[1]).await.expect("bind");
        let ExecuteOutcome::Rows { rows, .. } = s.execute("p", 0).await.expect("execute") else {
            panic!("rows")
        };
        assert_eq!(rows[0][0], None);
    }

    #[tokio::test]
    async fn stub_describe_returns_fields_without_executing() {
        let engine = StubEngine::new();
        let mut s = engine.connect();
        let described = s.parse("s", "SELECT 1", &[]).await.expect("ok");
        assert_eq!(described.fields.len(), 1);
        assert_eq!(described.fields[0].type_oid, oids::INT4);
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
