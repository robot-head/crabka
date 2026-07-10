//! Protocol-level error type carrying a SQLSTATE, mapped to `ErrorResponse`.

/// SQLSTATE codes used by the wire layer. Values must match real Postgres —
/// the conformance harness diffs error codes against the oracle.
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
    /// Aborts the current query/transaction; session continues.
    Error,
    /// Aborts the session; connection is closed after sending.
    Fatal,
}

impl Severity {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message} ({code})", severity.as_str())]
pub struct PgError {
    pub severity: Severity,
    /// Five-character SQLSTATE.
    pub code: String,
    pub message: String,
}

impl PgError {
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn fatal(code: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Fatal,
            code: code.into(),
            message: message.into(),
        }
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
}
