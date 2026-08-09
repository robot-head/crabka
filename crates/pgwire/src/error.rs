//! Protocol-level diagnostic carrying a SQLSTATE, mapped to `ErrorResponse` or
//! `NoticeResponse` according to its severity.

/// SQLSTATE codes used by the wire layer. Values must match real Postgres,
/// because the conformance harness diffs error codes against the oracle.
pub mod sqlstate {
    pub const PROTOCOL_VIOLATION: &str = "08P01";
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const INVALID_PASSWORD: &str = "28P01";
    pub const INVALID_AUTHORIZATION_SPECIFICATION: &str = "28000";
    pub const QUERY_CANCELED: &str = "57014";
    pub const INVALID_SQL_STATEMENT_NAME: &str = "26000";
    pub const INVALID_CURSOR_NAME: &str = "34000";
    pub const DUPLICATE_PREPARED_STATEMENT: &str = "42P05";
    pub const DUPLICATE_CURSOR: &str = "42P03";
    pub const UNDEFINED_PARAMETER: &str = "42P02";
    pub const IN_FAILED_SQL_TRANSACTION: &str = "25P02";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Developer-level diagnostic; does not abort the query.
    Debug,
    /// Server log message delivered to the client; does not abort the query.
    Log,
    /// Informational message; does not abort the query.
    Info,
    /// Ordinary client notice; does not abort the query.
    Notice,
    /// Warning; does not abort the query.
    Warning,
    /// Aborts the current query or transaction. The session continues.
    Error,
    /// Aborts the session. The server closes the connection after it sends
    /// the message.
    Fatal,
}

impl Severity {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Debug => "DEBUG",
            Severity::Log => "LOG",
            Severity::Info => "INFO",
            Severity::Notice => "NOTICE",
            Severity::Warning => "WARNING",
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
        }
    }

    /// Whether this severity is sent as a `NoticeResponse` rather than an
    /// `ErrorResponse`.
    #[must_use]
    pub const fn is_notice(self) -> bool {
        matches!(
            self,
            Self::Debug | Self::Log | Self::Info | Self::Notice | Self::Warning
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message} ({code})", severity.as_str())]
pub struct PgError {
    pub severity: Severity,
    /// Five-character SQLSTATE.
    pub code: String,
    pub message: String,
    /// Optional `PostgreSQL` diagnostic fields, allocated only when populated.
    pub diagnostics: Option<Box<DiagnosticFields>>,
}

/// Optional fields shared by `PostgreSQL` `ErrorResponse` and `NoticeResponse`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiagnosticFields {
    pub detail: Option<String>,
    pub hint: Option<String>,
    /// `PostgreSQL`'s `W` diagnostic field (often rendered as `CONTEXT`).
    pub context: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub column: Option<String>,
    pub datatype: Option<String>,
    pub constraint: Option<String>,
}

impl PgError {
    fn new(severity: Severity, code: &str, message: impl Into<String>) -> Self {
        Self {
            severity,
            code: code.into(),
            message: message.into(),
            diagnostics: None,
        }
    }

    fn diagnostics_mut(&mut self) -> &mut DiagnosticFields {
        self.diagnostics
            .get_or_insert_with(|| Box::new(DiagnosticFields::default()))
    }

    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Self::new(Severity::Error, code, message)
    }

    pub fn fatal(code: &str, message: impl Into<String>) -> Self {
        Self::new(Severity::Fatal, code, message)
    }

    pub fn debug(message: impl Into<String>) -> Self {
        Self::new(Severity::Debug, "00000", message)
    }

    pub fn log(message: impl Into<String>) -> Self {
        Self::new(Severity::Log, "00000", message)
    }

    pub fn info(message: impl Into<String>) -> Self {
        Self::new(Severity::Info, "00000", message)
    }

    pub fn notice(message: impl Into<String>) -> Self {
        Self::new(Severity::Notice, "00000", message)
    }

    pub fn warning(message: impl Into<String>) -> Self {
        Self::new(Severity::Warning, "01000", message)
    }

    /// Override the SQLSTATE attached to a diagnostic.
    #[must_use]
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = code.into();
        self
    }

    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.diagnostics_mut().detail = Some(detail.into());
        self
    }

    #[must_use]
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.diagnostics_mut().hint = Some(hint.into());
        self
    }

    #[must_use]
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.diagnostics_mut().context = Some(context.into());
        self
    }

    #[must_use]
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.diagnostics_mut().schema = Some(schema.into());
        self
    }

    #[must_use]
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.diagnostics_mut().table = Some(table.into());
        self
    }

    #[must_use]
    pub fn with_column(mut self, column: impl Into<String>) -> Self {
        self.diagnostics_mut().column = Some(column.into());
        self
    }

    #[must_use]
    pub fn with_datatype(mut self, datatype: impl Into<String>) -> Self {
        self.diagnostics_mut().datatype = Some(datatype.into());
        self
    }

    #[must_use]
    pub fn with_constraint(mut self, constraint: impl Into<String>) -> Self {
        self.diagnostics_mut().constraint = Some(constraint.into());
        self
    }

    /// Malformed bytes on the wire. Always fatal, per Postgres behavior.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::fatal(sqlstate::PROTOCOL_VIOLATION, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_error_has_fatal_severity_and_08p01() {
        let e = PgError::protocol("bad frame");
        assert_eq!(e.severity, Severity::Fatal);
        assert_eq!(e.code, sqlstate::PROTOCOL_VIOLATION);
        assert_eq!(e.message, "bad frame");
    }

    #[test]
    fn error_constructor_keeps_code() {
        let e = PgError::error(sqlstate::SYNTAX_ERROR, "oops");
        assert_eq!(e.severity, Severity::Error);
        assert_eq!(e.code, "42601");
        assert_eq!(e.message, "oops");
    }

    #[test]
    fn with_detail_and_with_hint_chain_and_leave_other_fields_alone() {
        let e = PgError::error("23503", "violates fk")
            .with_detail(r#"Key (p_id)=(1) is not present in table "p"."#)
            .with_hint("use CASCADE");
        let fields = e.diagnostics.as_deref().expect("diagnostic fields");
        assert_eq!(
            fields.detail.as_deref(),
            Some(r#"Key (p_id)=(1) is not present in table "p"."#)
        );
        assert_eq!(fields.hint.as_deref(), Some("use CASCADE"));
    }

    #[test]
    fn with_detail_and_with_hint_replace_a_previous_value() {
        let e = PgError::error(sqlstate::SYNTAX_ERROR, "oops")
            .with_detail("first")
            .with_hint("first hint")
            .with_detail("second")
            .with_hint("second hint");
        let fields = e.diagnostics.as_deref().expect("diagnostic fields");
        assert_eq!(fields.detail.as_deref(), Some("second"));
        assert_eq!(fields.hint.as_deref(), Some("second hint"));
    }

    #[test]
    fn display_ignores_detail_and_hint() {
        let bare = PgError::error(sqlstate::SYNTAX_ERROR, "oops");
        let decorated = bare.clone().with_detail("d").with_hint("h");
        assert_eq!(bare.to_string(), "ERROR: oops (42601)");
        assert_eq!(decorated.to_string(), "ERROR: oops (42601)");
    }

    #[test]
    fn notice_constructors_and_fields_are_structured() {
        let notice = PgError::warning("careful")
            .with_code("01004")
            .with_detail("value was shortened")
            .with_hint("use a wider column")
            .with_context("PL/pgSQL function f() line 2")
            .with_schema("public")
            .with_table("things")
            .with_column("name")
            .with_datatype("text")
            .with_constraint("things_name_check");

        assert2::assert!(notice.severity == Severity::Warning);
        assert2::assert!(notice.severity.is_notice());
        assert2::assert!(notice.code == "01004");
        let fields = notice.diagnostics.as_deref().expect("diagnostic fields");
        assert2::assert!(fields.detail.as_deref() == Some("value was shortened"));
        assert2::assert!(fields.hint.as_deref() == Some("use a wider column"));
        assert2::assert!(fields.context.as_deref() == Some("PL/pgSQL function f() line 2"));
        assert2::assert!(fields.schema.as_deref() == Some("public"));
        assert2::assert!(fields.table.as_deref() == Some("things"));
        assert2::assert!(fields.column.as_deref() == Some("name"));
        assert2::assert!(fields.datatype.as_deref() == Some("text"));
        assert2::assert!(fields.constraint.as_deref() == Some("things_name_check"));
        assert2::assert!(!Severity::Error.is_notice());
    }
}
