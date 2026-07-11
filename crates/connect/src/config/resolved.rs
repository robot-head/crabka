use std::{collections::BTreeMap, fmt};

use serde_json::Value;

use super::{
    error::{ConfigError, ConfigResult},
    secret::SecretString,
};

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum ResolvedValue {
    Plain(Value),
    Secret(SecretString),
}

/// Validated connector configuration with secret-aware formatting.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct ResolvedConfig {
    values: BTreeMap<String, ResolvedValue>,
}

impl ResolvedConfig {
    pub(crate) fn insert_plain(&mut self, key: impl Into<String>, value: Value) {
        self.values.insert(key.into(), ResolvedValue::Plain(value));
    }

    pub(crate) fn insert_secret(&mut self, key: impl Into<String>, value: SecretString) {
        self.values.insert(key.into(), ResolvedValue::Secret(value));
    }

    /// Return whether a key is present.
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    /// Read a string field.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn get_string(&self, key: &str) -> ConfigResult<String> {
        match self.values.get(key) {
            Some(ResolvedValue::Plain(Value::String(value))) => Ok(value.clone()),
            Some(ResolvedValue::Secret(_)) => Err(ConfigError::WrongType {
                key: key.into(),
                expected: "non-secret string",
            }),
            _ => Err(ConfigError::WrongType {
                key: key.into(),
                expected: "string",
            }),
        }
    }

    /// Read a boolean field.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn get_bool(&self, key: &str) -> ConfigResult<bool> {
        match self.values.get(key) {
            Some(ResolvedValue::Plain(Value::Bool(value))) => Ok(*value),
            _ => Err(ConfigError::WrongType {
                key: key.into(),
                expected: "bool",
            }),
        }
    }

    /// Read a signed integer field.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn get_i64(&self, key: &str) -> ConfigResult<i64> {
        match self.values.get(key) {
            Some(ResolvedValue::Plain(Value::Number(value))) => {
                value.as_i64().ok_or_else(|| ConfigError::WrongType {
                    key: key.into(),
                    expected: "integer",
                })
            }
            _ => Err(ConfigError::WrongType {
                key: key.into(),
                expected: "integer",
            }),
        }
    }

    /// Read an unsigned integer field.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn get_u64(&self, key: &str) -> ConfigResult<u64> {
        match self.values.get(key) {
            Some(ResolvedValue::Plain(Value::Number(value))) => {
                value.as_u64().ok_or_else(|| ConfigError::WrongType {
                    key: key.into(),
                    expected: "unsigned integer",
                })
            }
            _ => Err(ConfigError::WrongType {
                key: key.into(),
                expected: "unsigned integer",
            }),
        }
    }

    /// Read a floating-point field.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn get_f64(&self, key: &str) -> ConfigResult<f64> {
        match self.values.get(key) {
            Some(ResolvedValue::Plain(Value::Number(value))) => {
                value.as_f64().ok_or_else(|| ConfigError::WrongType {
                    key: key.into(),
                    expected: "float",
                })
            }
            _ => Err(ConfigError::WrongType {
                key: key.into(),
                expected: "float",
            }),
        }
    }

    /// Read a string-list field.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn get_string_list(&self, key: &str) -> ConfigResult<Vec<String>> {
        match self.values.get(key) {
            Some(ResolvedValue::Plain(Value::Array(values))) => values
                .iter()
                .map(|value| match value {
                    Value::String(s) => Ok(s.clone()),
                    _ => Err(ConfigError::WrongType {
                        key: key.into(),
                        expected: "string list",
                    }),
                })
                .collect(),
            _ => Err(ConfigError::WrongType {
                key: key.into(),
                expected: "string list",
            }),
        }
    }

    /// Read a JSON value field.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn get_json(&self, key: &str) -> ConfigResult<Value> {
        match self.values.get(key) {
            Some(ResolvedValue::Plain(value)) => Ok(value.clone()),
            Some(ResolvedValue::Secret(_)) => Err(ConfigError::WrongType {
                key: key.into(),
                expected: "json value",
            }),
            None => Err(ConfigError::MissingRequired { key: key.into() }),
        }
    }

    /// Read a secret field.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn get_secret(&self, key: &str) -> ConfigResult<SecretString> {
        match self.values.get(key) {
            Some(ResolvedValue::Secret(value)) => Ok(value.clone()),
            _ => Err(ConfigError::WrongType {
                key: key.into(),
                expected: "secret",
            }),
        }
    }
}

impl fmt::Debug for ResolvedConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (key, value) in &self.values {
            match value {
                ResolvedValue::Plain(value) => {
                    map.entry(key, value);
                }
                ResolvedValue::Secret(_) => {
                    map.entry(key, &"[REDACTED]");
                }
            }
        }
        map.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_getter_rejects_secret_values_as_non_secret_string() {
        let mut config = ResolvedConfig::default();
        config.insert_secret("password", SecretString::new("literal-secret"));

        let err = config.get_string("password").unwrap_err();

        assert2::assert!(
            matches!(err, ConfigError::WrongType { key, expected: "non-secret string" } if key == "password")
        );
    }
}
