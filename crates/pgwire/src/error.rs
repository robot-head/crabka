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
    /// Postgres `DETAIL`: secondary text naming the values that provoked the
    /// error, where the primary message names only the constraint — a foreign
    /// key violation reports `Key (p_id)=(1) is not present in table "p".`
    /// here. Deliberately absent from `Display`, which stays a one-liner.
    pub detail: Option<String>,
    /// Postgres `HINT`: a suggested fix, such as `TRUNCATE`'s advice to use
    /// `CASCADE` when a foreign key blocks it.
    pub hint: Option<String>,
}

impl PgError {
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            code: code.into(),
            message: message.into(),
            detail: None,
            hint: None,
        }
    }

    pub fn fatal(code: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Fatal,
            code: code.into(),
            message: message.into(),
            detail: None,
            hint: None,
        }
    }

    /// Malformed bytes on the wire. Always fatal, per Postgres behavior.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::fatal(sqlstate::PROTOCOL_VIOLATION, message)
    }

    /// Attach the `DETAIL` field, replacing any previously set detail.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Attach the `HINT` field, replacing any previously set hint.
    #[must_use]
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn protocol_error_has_fatal_severity_and_08p01() {
        let e = PgError::protocol("bad frame");
        assert!(
            e == PgError {
                severity: Severity::Fatal,
                code: sqlstate::PROTOCOL_VIOLATION.into(),
                message: "bad frame".into(),
                detail: None,
                hint: None,
            }
        );
    }

    #[test]
    fn error_constructor_keeps_code() {
        let e = PgError::error(sqlstate::SYNTAX_ERROR, "oops");
        assert!(
            e == PgError {
                severity: Severity::Error,
                code: "42601".into(),
                message: "oops".into(),
                detail: None,
                hint: None,
            }
        );
    }

    #[test]
    fn with_detail_and_with_hint_chain_and_leave_other_fields_alone() {
        let e = PgError::error("23503", "violates fk")
            .with_detail(r#"Key (p_id)=(1) is not present in table "p"."#)
            .with_hint("use CASCADE");
        assert!(
            e == PgError {
                severity: Severity::Error,
                code: "23503".into(),
                message: "violates fk".into(),
                detail: Some(r#"Key (p_id)=(1) is not present in table "p"."#.into()),
                hint: Some("use CASCADE".into()),
            }
        );
    }

    #[test]
    fn with_detail_and_with_hint_replace_a_previous_value() {
        let e = PgError::error(sqlstate::SYNTAX_ERROR, "oops")
            .with_detail("first")
            .with_hint("first hint")
            .with_detail("second")
            .with_hint("second hint");
        assert!(e.detail.as_deref() == Some("second"));
        assert!(e.hint.as_deref() == Some("second hint"));
    }

    #[test]
    fn display_ignores_detail_and_hint() {
        let bare = PgError::error(sqlstate::SYNTAX_ERROR, "oops");
        let decorated = bare.clone().with_detail("d").with_hint("h");
        assert!(bare.to_string() == "ERROR: oops (42601)");
        assert!(decorated.to_string() == "ERROR: oops (42601)");
    }
}
