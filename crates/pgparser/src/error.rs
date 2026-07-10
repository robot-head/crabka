//! Parse/lex errors. Most map to SQLSTATE 42601 (`syntax_error`) and carry the
//! byte offset where the problem was detected. A too-deep query — one whose
//! nesting would overflow the parser/evaluator stack — instead maps to 54001
//! (`statement_too_complex` / "stack depth limit exceeded"), matching
//! `PostgreSQL`.

/// A parse/lex error. `message` is the full, ready-to-display text (the
/// `#[error]` format is just `"{message}"`), so the `42601` "syntax error at
/// position N: …" framing is baked in by `new`, while a `54001` depth error
/// (built by `too_deep`) renders its own PostgreSQL-faithful text verbatim.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ParseError {
    pub message: String,
    pub position: usize,
    /// The SQLSTATE this error maps to. Defaults to `"42601"` (`syntax_error`);
    /// `too_deep` sets it to `"54001"` (`statement_too_complex`).
    sqlstate: &'static str,
}

impl ParseError {
    pub fn new(message: impl Into<String>, position: usize) -> Self {
        Self {
            message: format!("syntax error at position {position}: {}", message.into()),
            position,
            sqlstate: "42601",
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
        }
    }

    /// A recursion-depth-limit error: the statement nests more deeply than the
    /// parser's `MAX_DEPTH` allows. Maps to SQLSTATE `54001`
    /// (`statement_too_complex`) with `PostgreSQL`'s "stack depth limit exceeded"
    /// message, so a maliciously deep query returns a clean error instead of
    /// overflowing the stack and aborting the server process.
    #[must_use]
    pub fn too_deep(position: usize) -> Self {
        Self {
            message: "stack depth limit exceeded".to_string(),
            position,
            sqlstate: "54001",
        }
    }

    #[must_use]
    pub fn sqlstate(&self) -> &'static str {
        self.sqlstate
    }
}
