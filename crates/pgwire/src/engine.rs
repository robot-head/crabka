//! Engine seam: types the wire layer exchanges with the query engine.

use std::future::Future;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::PgError;

/// Type OIDs from `pg_type.dat`. The stub needs only these two. The real
/// catalog crate will own the full set.
pub mod oids {
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
}

/// A single value, pre-encoded in both wire formats.
///
/// SP2 NOTE: both encodings computed in advance are fine for the stub, but
/// they double the encoding work for a real engine. The wire layer knows the
/// negotiated format at Bind time and could request only one. Revisit this
/// seam when the real engine lands.
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
    /// Statement with no result set, for example SET. The tag looks like
    /// "INSERT 0 1".
    Command { tag: String },
    /// Empty query string → `EmptyQueryResponse`.
    Empty,
}

/// A bounded fragment of a simple-query result. Row pages carry the description
/// only on the first page and the command tag only on the final page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultPage {
    Rows {
        result_index: usize,
        fields: Option<Vec<FieldDescription>>,
        rows: Vec<Vec<Option<Cell>>>,
        tag: Option<String>,
    },
    Command {
        result_index: usize,
        tag: String,
    },
    Empty {
        result_index: usize,
    },
}

/// Backpressured consumer for bounded simple-query result pages.
#[async_trait::async_trait]
pub trait ResultSink: Send {
    async fn send(&mut self, page: ResultPage) -> Result<(), PgError>;

    /// Deliver a non-error `PostgreSQL` diagnostic. Existing sinks may ignore
    /// notices. Wire-facing sinks can override this to emit `NoticeResponse`.
    async fn send_notice(&mut self, notice: PgError) -> Result<(), PgError> {
        debug_assert!(notice.severity.is_notice());
        Ok(())
    }
}

/// Compatibility sink used by callers that still require `Vec<QueryResult>`.
#[derive(Debug, Default)]
pub struct CollectingResultSink {
    pages: Vec<ResultPage>,
}

impl CollectingResultSink {
    #[must_use]
    pub fn pages(&self) -> &[ResultPage] {
        &self.pages
    }

    /// Assemble collected pages into complete query results.
    ///
    /// # Errors
    ///
    /// Returns an error when pages are missing, duplicated, or arrive out of
    /// result order.
    pub fn finish(self) -> Result<Vec<QueryResult>, PgError> {
        let mut results = Vec::new();
        for page in self.pages {
            match page {
                ResultPage::Command { result_index, tag } => {
                    if result_index != results.len() {
                        return Err(malformed_result_pages(result_index, results.len()));
                    }
                    results.push(QueryResult::Command { tag });
                }
                ResultPage::Empty { result_index } => {
                    if result_index != results.len() {
                        return Err(malformed_result_pages(result_index, results.len()));
                    }
                    results.push(QueryResult::Empty);
                }
                ResultPage::Rows {
                    result_index,
                    fields,
                    mut rows,
                    tag,
                } => {
                    if result_index == results.len() {
                        let fields = fields.ok_or_else(|| {
                            PgError::protocol("first row page is missing field descriptions")
                        })?;
                        results.push(QueryResult::Rows {
                            fields,
                            rows: Vec::new(),
                            tag: String::new(),
                        });
                    } else if result_index.checked_add(1) != Some(results.len()) {
                        return Err(malformed_result_pages(result_index, results.len()));
                    } else if fields.is_some() {
                        return Err(PgError::protocol(
                            "continuation row page repeated field descriptions",
                        ));
                    }
                    let Some(QueryResult::Rows {
                        rows: all,
                        tag: final_tag,
                        ..
                    }) = results.get_mut(result_index)
                    else {
                        return Err(PgError::protocol("result page changed result kind"));
                    };
                    if !final_tag.is_empty() {
                        return Err(PgError::protocol(
                            "row page followed a completed row result",
                        ));
                    }
                    all.append(&mut rows);
                    if let Some(tag) = tag {
                        *final_tag = tag;
                    }
                }
            }
        }
        if results
            .iter()
            .any(|result| matches!(result, QueryResult::Rows { tag, .. } if tag.is_empty()))
        {
            return Err(PgError::protocol("row result is missing a completion tag"));
        }
        Ok(results)
    }
}

fn malformed_result_pages(actual: usize, expected: usize) -> PgError {
    PgError::protocol(format!(
        "result page index {actual} does not match expected index {expected}"
    ))
}

#[async_trait::async_trait]
impl ResultSink for CollectingResultSink {
    async fn send(&mut self, page: ResultPage) -> Result<(), PgError> {
        self.pages.push(page);
        Ok(())
    }
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

/// One `NOTIFY` delivered asynchronously to a listening connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    /// Process id of the notifying backend, as announced in `BackendKeyData`.
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

/// A database engine: a factory for per-connection sessions.
///
/// All connections share one engine, so it is `Send + Sync`. Each connection
/// gets its own [`Session`].
///
/// SP1 ships only `StubEngine`. The real engine implements this same trait.
pub trait Engine: Send + Sync + 'static {
    type Session: Session;

    /// Create a fresh per-connection session. The wire layer calls this once
    /// per connection.
    fn connect(&self) -> Self::Session;

    /// Create a fresh per-connection session that knows its backend process id.
    ///
    /// `pid` is the same value the wire layer announces in `BackendKeyData`.
    /// An engine with LISTEN/NOTIFY can therefore stamp it on the
    /// notifications it publishes, and self-notifications arrive with the
    /// listener's own pid, as in Postgres. The default ignores the pid and
    /// delegates to [`Engine::connect`].
    fn connect_with_pid(&self, _pid: i32) -> Self::Session {
        self.connect()
    }
}

/// A per-connection session.
///
/// A session owns transaction state and is not shared between connections.
/// `simple_query` and `describe` take `&mut self` because they mutate
/// transaction state.
///
/// Cancellation: the wire layer may DROP an in-flight query future with
/// `tokio::select!`, then await [`Session::cancel_current_query`] before it
/// reports `ReadyForQuery`. Implementations must make that pair drop-safe.
pub trait Session: Send {
    /// Complete engine-side connection startup before the wire layer reports
    /// `ReadyForQuery`. An error return rejects the connection.
    fn startup(&mut self) -> impl Future<Output = Result<(), PgError>> + Send {
        async { Ok(()) }
    }

    /// Release whatever the session owns that outlives the connection but not
    /// the session. The wire layer calls this once when the message loop ends,
    /// however it ends.
    ///
    /// This is not `Drop`. The work is a durable write, so the caller must
    /// await it. A session whose connection was severed still reaches here,
    /// because the loop carries its outcome past the call rather than returns
    /// it through the call.
    ///
    /// The default does nothing, which is right for a session that owns no
    /// such state.
    fn terminate(&mut self) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Execute the full text of a simple-protocol Query message. The text can
    /// contain several statements, and the engine must split them.
    fn simple_query(
        &mut self,
        sql: &str,
    ) -> impl Future<Output = Result<Vec<QueryResult>, PgError>> + Send;

    /// Execute simple-query text into a backpressured bounded sink.
    fn simple_query_into<S: ResultSink>(
        &mut self,
        sql: &str,
        page_rows: usize,
        sink: &mut S,
    ) -> impl Future<Output = Result<(), PgError>> + Send {
        async move {
            if page_rows == 0 {
                return Err(PgError::protocol(
                    "result page size must be greater than zero",
                ));
            }
            for (result_index, result) in self.simple_query(sql).await?.into_iter().enumerate() {
                match result {
                    QueryResult::Rows { fields, rows, tag } => {
                        if rows.is_empty() {
                            sink.send(ResultPage::Rows {
                                result_index,
                                fields: Some(fields),
                                rows,
                                tag: Some(tag),
                            })
                            .await?;
                            continue;
                        }
                        let mut fields = Some(fields);
                        let chunks = rows.len().div_ceil(page_rows);
                        for (index, rows) in rows.chunks(page_rows).enumerate() {
                            sink.send(ResultPage::Rows {
                                result_index,
                                fields: fields.take(),
                                rows: rows.to_vec(),
                                tag: (index + 1 == chunks).then(|| tag.clone()),
                            })
                            .await?;
                        }
                    }
                    QueryResult::Command { tag } => {
                        sink.send(ResultPage::Command { result_index, tag }).await?;
                    }
                    QueryResult::Empty => sink.send(ResultPage::Empty { result_index }).await?,
                }
            }
            Ok(())
        }
    }

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
    /// command. The wire layer then enters copy-in mode and later calls
    /// `copy_in` with all received `CopyData` frames. Non-COPY SQL returns
    /// `None`.
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

    /// Finish an extended-protocol COPY FROM STDIN after `CopyDone`. `portal`
    /// is the portal whose Execute returned [`ExecuteOutcome::CopyIn`].
    /// Engines that return that outcome must implement this method to apply
    /// the buffered `CopyData` frames.
    fn copy_in_portal(
        &mut self,
        portal: &str,
        data: Vec<Bytes>,
    ) -> impl Future<Output = Result<QueryResult, PgError>> + Send {
        let _ = (portal, data);
        async {
            Err(PgError::error(
                crate::error::sqlstate::FEATURE_NOT_SUPPORTED,
                "COPY FROM STDIN is not supported by this engine",
            ))
        }
    }

    /// Hand the wire layer this session's asynchronous notification stream.
    ///
    /// The wire layer calls this exactly once, immediately after it creates
    /// the session. The wire loop owns the receiver, not the session, so it
    /// can push a notification to the client while the connection waits for
    /// the next frontend message. Engines without LISTEN/NOTIFY keep the
    /// default `None` and never see an asynchronous message on the wire.
    fn take_notifications(&mut self) -> Option<mpsc::Receiver<Notification>> {
        None
    }

    /// Hand the wire layer this session's non-error diagnostic stream.
    ///
    /// The default keeps existing engines source-compatible and silent.
    fn take_notices(&mut self) -> Option<mpsc::Receiver<PgError>> {
        None
    }

    /// Mark the current statement as failed after a protocol-side error.
    ///
    /// COPY FROM STDIN can fail because the client sends `CopyFail` before the
    /// engine sees `copy_in`. Engines with explicit transaction state must
    /// abort the open transaction block here and must leave autocommit
    /// sessions usable.
    fn mark_statement_failed(&mut self) {}

    /// Finish engine-side cancellation after the wire layer drops an in-flight
    /// query future. Implementations with detached workers must stop and join
    /// them here before releasing transaction resources.
    fn cancel_current_query(&mut self) -> impl Future<Output = ()> + Send {
        async move { self.mark_statement_failed() }
    }

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

    #[tokio::test]
    async fn collecting_sink_rejects_skipped_result_index() {
        let mut sink = CollectingResultSink::default();
        sink.send(ResultPage::Empty { result_index: 1 })
            .await
            .expect("collect page");

        let error = sink.finish().expect_err("skipped index is malformed");
        assert_eq!(error.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }

    #[tokio::test]
    async fn collecting_sink_rejects_row_page_after_command() {
        let mut sink = CollectingResultSink::default();
        sink.send(ResultPage::Command {
            result_index: 0,
            tag: "UPDATE 1".into(),
        })
        .await
        .expect("collect command");
        sink.send(ResultPage::Rows {
            result_index: 0,
            fields: None,
            rows: Vec::new(),
            tag: Some("SELECT 0".into()),
        })
        .await
        .expect("collect malformed continuation");

        let error = sink
            .finish()
            .expect_err("a result cannot change kind between pages");
        assert_eq!(error.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }

    #[tokio::test]
    async fn default_notice_seams_keep_existing_sinks_and_sessions_compatible() {
        let mut sink = CollectingResultSink::default();
        sink.send_notice(PgError::notice("hello"))
            .await
            .expect("default notice sink");
        assert2::assert!(sink.pages().is_empty());

        let mut session = RecordingSession;
        assert2::assert!(session.take_notices().is_none());
    }
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
