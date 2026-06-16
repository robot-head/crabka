//! Connector configuration definitions, validation, and secret resolution.

mod def;
mod error;
mod resolved;
mod secret;

pub use def::{ConfigDef, ConfigKey, ConfigKind, RawConfig};
pub use error::{ConfigError, ConfigResult};
pub use resolved::ResolvedConfig;
pub use secret::{
    EnvSecretResolver, ResolveOptions, SecretRef, SecretResolutionError, SecretResolver,
    SecretString,
};

/// Typed connector configuration produced from a resolved config map.
pub trait ConnectorConfig: Sized {
    /// Return this connector's configuration definition.
    fn config_def() -> ConfigDef;

    /// Build the typed config from validated, resolved values.
    fn from_resolved(config: &ResolvedConfig) -> ConfigResult<Self>;
}

/// Converts one resolved config key into a concrete Rust field type.
pub trait FromResolvedValue: Sized {
    /// The `ConfigDef` kind for this Rust type.
    const KIND: ConfigKind;

    /// Read `key` from `config`.
    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self>;
}

impl FromResolvedValue for String {
    const KIND: ConfigKind = ConfigKind::String;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        config.get_string(key)
    }
}

impl FromResolvedValue for bool {
    const KIND: ConfigKind = ConfigKind::Bool;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        config.get_bool(key)
    }
}

impl FromResolvedValue for i64 {
    const KIND: ConfigKind = ConfigKind::Integer;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        config.get_i64(key)
    }
}

impl FromResolvedValue for u64 {
    const KIND: ConfigKind = ConfigKind::Integer;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        config.get_u64(key)
    }
}

impl FromResolvedValue for f64 {
    const KIND: ConfigKind = ConfigKind::Float;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        config.get_f64(key)
    }
}

impl FromResolvedValue for Vec<String> {
    const KIND: ConfigKind = ConfigKind::StringList;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        config.get_string_list(key)
    }
}

impl FromResolvedValue for serde_json::Value {
    const KIND: ConfigKind = ConfigKind::Json;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        config.get_json(key)
    }
}

impl FromResolvedValue for SecretString {
    const KIND: ConfigKind = ConfigKind::Secret;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        config.get_secret(key)
    }
}

macro_rules! impl_signed_integer_config {
    ($($ty:ty),* $(,)?) => {
        $(
            impl FromResolvedValue for $ty {
                const KIND: ConfigKind = ConfigKind::Integer;

                fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
                    let value = config.get_i64(key)?;
                    <$ty>::try_from(value).map_err(|_| ConfigError::WrongType {
                        key: key.into(),
                        expected: "integer in range",
                    })
                }
            }
        )*
    };
}

macro_rules! impl_unsigned_integer_config {
    ($($ty:ty),* $(,)?) => {
        $(
            impl FromResolvedValue for $ty {
                const KIND: ConfigKind = ConfigKind::Integer;

                fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
                    let value = config.get_u64(key)?;
                    <$ty>::try_from(value).map_err(|_| ConfigError::WrongType {
                        key: key.into(),
                        expected: "unsigned integer in range",
                    })
                }
            }
        )*
    };
}

impl_signed_integer_config!(i8, i16, i32, isize);
impl_unsigned_integer_config!(u8, u16, u32, usize);

impl FromResolvedValue for f32 {
    const KIND: ConfigKind = ConfigKind::Float;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        Ok(config.get_f64(key)? as f32)
    }
}
