//! Connector configuration definitions, validation, and secret resolution.
//!
//! ```no_run
//! # #[cfg(not(feature = "derive"))]
//! # fn main() {}
//!
//! # #[cfg(feature = "derive")]
//! # mod example {
//! use crabka_connect::{
//!     ConfigDef, ConnectorConfig, EnvSecretResolver, SecretString,
//! };
//! use serde_json::json;
//!
//! #[derive(ConnectorConfig)]
//! struct ExampleConfig {
//!     #[config(required)]
//!     database_url: String,
//!     #[config(secret)]
//!     password: SecretString,
//!     #[config(default = "public")]
//!     schema: String,
//! }
//!
//! # async fn build() -> crabka_connect::ConfigResult<ExampleConfig> {
//! let raw: serde_json::Map<String, serde_json::Value> = serde_json::Map::from_iter([
//!     ("database_url".to_string(), json!("postgres://localhost/app")),
//!     (
//!         "password".to_string(),
//!         json!({ "from": "env", "name": "POSTGRES_PASSWORD" }),
//!     ),
//! ]);
//! let def: ConfigDef = ExampleConfig::config_def();
//! let resolved = def.resolve(raw, &EnvSecretResolver).await?;
//! let config: ExampleConfig = ConnectorConfig::from_resolved(&resolved)?;
//! Ok(config)
//! # }
//! # }
//! # #[cfg(feature = "derive")]
//! # fn main() {}
//! ```

use std::time::Duration;

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
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    fn from_resolved(config: &ResolvedConfig) -> ConfigResult<Self>;
}

/// Converts one resolved config key into a concrete Rust field type.
pub trait FromResolvedValue: Sized {
    /// The `ConfigDef` kind for this Rust type.
    const KIND: ConfigKind;

    /// Read `key` from `config`.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
    const KIND: ConfigKind = ConfigKind::UnsignedInteger;

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
                        expected: concat!("integer in range for ", stringify!($ty)),
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
                const KIND: ConfigKind = ConfigKind::UnsignedInteger;

                fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
                    let value = config.get_u64(key)?;
                    <$ty>::try_from(value).map_err(|_| ConfigError::WrongType {
                        key: key.into(),
                        expected: concat!("unsigned integer in range for ", stringify!($ty)),
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
        let value = config.get_f64(key)?;
        if !value.is_finite() || value < f64::from(f32::MIN) || value > f64::from(f32::MAX) {
            return Err(ConfigError::WrongType {
                key: key.into(),
                expected: "float in range for f32",
            });
        }
        value
            .to_string()
            .parse::<f32>()
            .map_err(|_| ConfigError::WrongType {
                key: key.into(),
                expected: "float in range for f32",
            })
    }
}

impl<T: FromResolvedValue> FromResolvedValue for Option<T> {
    const KIND: ConfigKind = T::KIND;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        if config.contains_key(key) {
            T::from_resolved_value(config, key).map(Some)
        } else {
            Ok(None)
        }
    }
}

impl FromResolvedValue for Duration {
    const KIND: ConfigKind = ConfigKind::DurationMillis;

    fn from_resolved_value(config: &ResolvedConfig, key: &str) -> ConfigResult<Self> {
        let value = config.get_i64(key)?;
        let millis = u64::try_from(value).map_err(|_| ConfigError::WrongType {
            key: key.into(),
            expected: "non-negative duration milliseconds",
        })?;
        Ok(Self::from_millis(millis))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Number, Value};

    use super::*;

    fn resolved_float(value: f64) -> ResolvedConfig {
        let mut config = ResolvedConfig::default();
        config.insert_plain(
            "ratio",
            Value::Number(Number::from_f64(value).expect("finite test float")),
        );
        config
    }

    #[test]
    fn f32_extraction_accepts_inclusive_boundaries() {
        for (_name, value, expected) in [
            ("minimum", f64::from(f32::MIN), f32::MIN),
            ("maximum", f64::from(f32::MAX), f32::MAX),
        ] {
            let config = resolved_float(value);
            assert2::assert!(
                f32::from_resolved_value(&config, "ratio")
                    .unwrap()
                    .to_bits()
                    == expected.to_bits()
            );
        }
    }

    #[test]
    fn f32_extraction_rejects_values_beyond_boundaries() {
        let below_min = resolved_float(f64::from(f32::MIN) * 2.0);
        let above_max = resolved_float(f64::from(f32::MAX) * 2.0);

        for (_name, config) in [("below_min", &below_min), ("above_max", &above_max)] {
            assert2::assert!(matches!(
                f32::from_resolved_value(config, "ratio"),
                Err(ConfigError::WrongType {
                    key,
                    expected: "float in range for f32",
                }) if key == "ratio"
            ));
        }
    }
}
