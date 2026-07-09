//! Public SDK error taxonomy.

/// Error taxonomy shared by the app SDK contract.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum CrabkaError {
    /// Transport or endpoint reachability failure.
    #[error("transport: {0}")]
    Transport(String),
    /// Authentication failed or credentials were absent.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    /// Caller supplied an invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Target resource was not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// Server-side failure outside the narrower classes.
    #[error("server error: {0}")]
    ServerError(String),
    /// SDK module is intentionally gated on later work.
    #[error("{module} is unimplemented; gated on {gated_on}")]
    Unimplemented {
        /// SDK module name.
        module: &'static str,
        /// Plan or spec slug gating the module.
        gated_on: &'static str,
    },
}

impl CrabkaError {
    /// Convert a Connect code string into the SDK taxonomy.
    #[must_use]
    pub fn from_connect_code(code: &str, message: impl Into<String>) -> Self {
        let message = message.into();
        match code {
            "not_found" => Self::NotFound(message),
            "invalid_argument" | "failed_precondition" | "out_of_range" => {
                Self::InvalidArgument(message)
            }
            "unauthenticated" => Self::Unauthenticated(message),
            "unavailable" | "deadline_exceeded" => Self::Transport(message),
            _ => Self::ServerError(message),
        }
    }
}

impl From<crate::connect_client::ConnectClientError> for CrabkaError {
    fn from(value: crate::connect_client::ConnectClientError) -> Self {
        match value {
            crate::connect_client::ConnectClientError::Connect { code, message } => {
                Self::from_connect_code(&code, message)
            }
            other => Self::Transport(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_codes_map_to_taxonomy() {
        assert!(matches!(
            CrabkaError::from_connect_code("not_found", "x"),
            CrabkaError::NotFound(_)
        ));
        assert!(matches!(
            CrabkaError::from_connect_code("unavailable", "x"),
            CrabkaError::Transport(_)
        ));
        assert!(matches!(
            CrabkaError::from_connect_code("invalid_argument", "x"),
            CrabkaError::InvalidArgument(_)
        ));
    }
}
