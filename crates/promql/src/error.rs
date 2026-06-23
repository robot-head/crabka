//! The crate's error type.

/// Errors raised by the `PromQL` engine.
#[derive(Debug, thiserror::Error)]
pub enum PromqlError {
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
pub type Result<T> = std::result::Result<T, PromqlError>;

impl From<datafusion::error::DataFusionError> for PromqlError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        Self::Exec(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn datafusion_error_maps_to_exec() {
        let dfe = datafusion::error::DataFusionError::Plan("boom".into());
        let pe: PromqlError = dfe.into();
        assert!(matches!(pe, PromqlError::Exec(_)));
    }

    #[test]
    fn display_includes_category() {
        let e = PromqlError::Unsupported("histogram_quantile".into());
        assert!(format!("{e}").contains("unsupported"));
    }
}
