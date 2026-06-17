use std::error::Error;

/// Result alias for connector configuration work.
pub type ConfigResult<T> = Result<T, ConfigError>;

/// Errors raised while validating or resolving connector configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The raw config contains a key not declared by `ConfigDef`.
    #[error("unknown config key `{key}`")]
    UnknownKey { key: String },

    /// A required key is absent and has no default.
    #[error("missing required config key `{key}`")]
    MissingRequired { key: String },

    /// A value did not match the field's declared type.
    #[error("config key `{key}` expected {expected}")]
    WrongType { key: String, expected: &'static str },

    /// A default value declared by the config definition is invalid.
    #[error("invalid default for config key `{key}`: {reason}")]
    InvalidDefault { key: String, reason: String },

    /// A secret field did not contain a supported reference object.
    #[error("invalid secret reference for config key `{key}`: {reason}")]
    InvalidSecretRef { key: String, reason: String },

    /// A resolver failed to fetch the referenced secret.
    #[error("failed to resolve secret for config key `{key}`: {source}")]
    SecretResolution {
        key: String,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
}
