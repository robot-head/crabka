//! Connector configuration definitions, validation, and secret resolution.

mod def;
mod error;
mod resolved;
mod secret;

pub use def::{ConfigDef, ConfigKey, ConfigKind, RawConfig};
pub use error::{ConfigError, ConfigResult};
pub use resolved::ResolvedConfig;
pub use secret::{EnvSecretResolver, ResolveOptions, SecretRef, SecretResolver, SecretString};

/// Typed connector configuration produced from a resolved config map.
pub trait ConnectorConfig: Sized {
    /// Return this connector's configuration definition.
    fn config_def() -> ConfigDef;

    /// Build the typed config from validated, resolved values.
    fn from_resolved(config: &ResolvedConfig) -> ConfigResult<Self>;
}
