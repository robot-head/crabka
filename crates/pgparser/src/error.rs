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
    /// Whether `position` is the one `PostgreSQL` puts in the `P` field of this
    /// same error, which is what makes psql draw a `LINE`/caret pair under the
    /// statement. See [`ParseError::reported_position`].
    reports_position: bool,
}

impl ParseError {
    pub fn new(message: impl Into<String>, position: usize) -> Self {
        Self {
            message: format!("syntax error at position {position}: {}", message.into()),
            position,
            sqlstate: "42601",
            detail: None,
            hint: None,
            reports_position: false,
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
            reports_position: false,
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
            reports_position: false,
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
            reports_position: false,
        }
    }

    #[must_use]
    pub fn sqlstate(&self) -> &'static str {
        self.sqlstate
    }

    /// Declare that `PostgreSQL` reports a cursor position for this error, at
    /// the same offset.
    ///
    /// `PostgreSQL`'s grammar attaches `scanner_errposition` to every syntax
    /// error it raises, and psql turns that `P` field into the `LINE n: …` echo
    /// with a caret under the offending token. Crabka's parser knows an offset
    /// for every error it builds, but the offset is only *`PostgreSQL`'s* offset
    /// where crabka rejects the same text `PostgreSQL` rejects. Most of what
    /// this parser refuses, `PostgreSQL` accepts — grammar crabka has not
    /// implemented — and dressing those refusals in a caret would spell a
    /// coverage gap as though it were the user's typing mistake. Measured over
    /// the 231-file `pg_regress` corpus, marking every `syntax error at or near`
    /// would add 624 output lines against 40 that upstream also carries.
    ///
    /// So the marker is opt-in, one grammar rule at a time, and belongs only on
    /// a rule that rejects exactly what `PostgreSQL`'s rejects.
    #[must_use]
    pub(crate) fn reporting_position(mut self) -> Self {
        self.reports_position = true;
        self
    }

    /// The one-based **character** position `PostgreSQL` would put in the `P`
    /// field, for an error whose rule declared one with
    /// [`ParseError::reporting_position`].
    ///
    /// `source` is the query text [`Self::position`] indexes as a byte offset;
    /// the wire field counts characters, so the two differ once a statement
    /// holds any multi-byte text before the error.
    #[must_use]
    pub fn reported_position(&self, source: &str) -> Option<usize> {
        if !self.reports_position {
            return None;
        }
        let prefix = source.get(..self.position)?;
        Some(prefix.chars().count() + 1)
    }
}
