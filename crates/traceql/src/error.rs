//! `TraceQL` error categories.

/// Errors that the `TraceQL` engine raises.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TraceqlError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("plan error: {0}")]
    Plan(String),
    #[error("execution error: {0}")]
    Exec(String),
    #[error("store error: {0}")]
    Store(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
}

/// Internal convenience alias.
pub type Result<T> = std::result::Result<T, TraceqlError>;

impl From<datafusion::error::DataFusionError> for TraceqlError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::Exec(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn datafusion_error_maps_to_exec() {
        let dfe = datafusion::error::DataFusionError::Plan("boom".into());
        let te: TraceqlError = dfe.into();
        assert!(matches!(te, TraceqlError::Exec(_)));
    }

    #[test]
    fn display_includes_category() {
        let e = TraceqlError::Unsupported("negated structural op".into());
        assert!(format!("{e}").contains("unsupported"));
    }
}
