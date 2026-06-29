use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid regex `{pattern}`: {source}")]
    InvalidRegex {
        pattern: String,
        source: regex::Error,
    },
    #[error("{message} at byte {position}")]
    Syntax { message: String, position: usize },
}
