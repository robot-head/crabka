//! Parse/lex errors.
//!
//! Most parse/lex errors map to SQLSTATE 42601 (`syntax_error`) and carry the
//! byte offset where the parser found the problem. A too-deep query, one whose
//! nesting would overflow the parser/evaluator stack, instead maps to 54001
//! (`statement_too_complex` / "stack depth limit exceeded"), the same as
//! `PostgreSQL`.

/// A parse/lex error.
///
/// `message` is the full text to display, because the `#[error]` format is only
/// `"{message}"`. So `new` writes the `42601` "syntax error at position
/// N: …" frame into the message itself. A `54001` depth error, built by
/// `too_deep`, keeps its own PostgreSQL-faithful text unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ParseError {
    pub message: String,
    pub position: usize,
    /// The SQLSTATE this error maps to. Defaults to `"42601"` (`syntax_error`);
    /// `too_deep` sets it to `"54001"` (`statement_too_complex`).
    sqlstate: &'static str,
    /// `PostgreSQL`'s DETAIL line, where the error has one. A rejected
    /// reloption is the case that needs it: the message names the value and
    /// the option, and the range it had to fall in is on the line below.
    detail: Option<String>,
    /// `PostgreSQL`'s HINT line, for the errors that offer a remedy.
    hint: Option<&'static str>,
}

impl ParseError {
    pub fn new(message: impl Into<String>, position: usize) -> Self {
        Self {
            message: format!("syntax error at position {position}: {}", message.into()),
            position,
            sqlstate: "42601",
            detail: None,
            hint: None,
        }
    }

    pub(crate) fn new_sqlstate(
        sqlstate: &'static str,
        message: impl Into<String>,
        position: usize,
    ) -> Self {
        Self {
            message: message.into(),
            position,
            sqlstate,
            detail: None,
            hint: None,
        }
    }

    /// The rejection [`crate::reloptions::validate`] built, placed at
    /// `position`.
    pub(crate) fn from_reloption(
        error: crate::reloptions::RelOptionError,
        position: usize,
    ) -> Self {
        Self {
            message: error.message,
            position,
            sqlstate: error.sqlstate,
            detail: error.detail,
            hint: error.hint,
        }
    }

    #[must_use]
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    #[must_use]
    pub fn hint(&self) -> Option<&'static str> {
        self.hint
    }

    /// Builds a recursion-depth-limit error.
    ///
    /// Use it when the statement nests more deeply than the parser's
    /// `MAX_DEPTH` allows. The error maps to SQLSTATE `54001`
    /// (`statement_too_complex`) with `PostgreSQL`'s "stack depth limit
    /// exceeded" message. So a maliciously deep query returns a clean error. It
    /// does not overflow the stack and it does not abort the server process.
    #[must_use]
    pub fn too_deep(position: usize) -> Self {
        Self {
            message: "stack depth limit exceeded".to_string(),
            position,
            sqlstate: "54001",
            detail: None,
            hint: None,
        }
    }

    #[must_use]
    pub fn sqlstate(&self) -> &'static str {
        self.sqlstate
    }
}
