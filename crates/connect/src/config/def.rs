use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde_json::{Map, Value};

use super::{
    error::{ConfigError, ConfigResult},
    resolved::ResolvedConfig,
    secret::{ResolveOptions, SecretRef, SecretResolver, SecretString},
};

/// Incoming connector configuration as a JSON object.
pub type RawConfig = Map<String, Value>;

/// Supported logical connector configuration kinds.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConfigKind {
    String,
    Bool,
    Integer,
    UnsignedInteger,
    Float,
    DurationMillis,
    DurationMs,
    StringList,
    Json,
    Secret,
}

impl ConfigKind {
    /// Return a human-readable type expectation for diagnostics.
    #[must_use]
    pub fn expected(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Bool => "bool",
            Self::Integer => "integer",
            Self::UnsignedInteger => "unsigned integer",
            Self::Float => "float",
            Self::DurationMillis | Self::DurationMs => "duration milliseconds",
            Self::StringList => "string list",
            Self::Json => "json value",
            Self::Secret => "secret reference",
        }
    }
}

/// One connector configuration field definition.
#[non_exhaustive]
#[derive(Clone, Eq, PartialEq)]
pub struct ConfigKey {
    pub name: String,
    pub kind: ConfigKind,
    pub required: bool,
    pub default: Option<Value>,
    pub description: Option<String>,
}

/// ConfigDef-style connector configuration schema.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct ConfigDef {
    name: String,
    keys: BTreeMap<String, ConfigKey>,
}

impl fmt::Debug for ConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted_default = if self.kind == ConfigKind::Secret && self.default.is_some() {
            Some(Value::String("<redacted>".to_owned()))
        } else {
            self.default.clone()
        };

        f.debug_struct("ConfigKey")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("required", &self.required)
            .field("default", &redacted_default)
            .field("description", &self.description)
            .finish()
    }
}

impl fmt::Debug for ConfigDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigDef")
            .field("name", &self.name)
            .field("keys", &self.keys)
            .finish()
    }
}

impl ConfigDef {
    /// Create an empty configuration definition for a connector.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            keys: BTreeMap::new(),
        }
    }

    /// Return the connector definition name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Iterate over declared config keys in stable name order.
    pub fn keys(&self) -> impl Iterator<Item = &ConfigKey> {
        self.keys.values()
    }

    /// Add a required configuration key.
    #[must_use]
    pub fn required(mut self, name: impl Into<String>, kind: ConfigKind) -> Self {
        let name = name.into();
        self.define_key(ConfigKey {
            name,
            kind,
            required: true,
            default: None,
            description: None,
        });
        self
    }

    /// Add an optional configuration key.
    #[must_use]
    pub fn optional(mut self, name: impl Into<String>, kind: ConfigKind) -> Self {
        let name = name.into();
        self.define_key(ConfigKey {
            name,
            kind,
            required: false,
            default: None,
            description: None,
        });
        self
    }

    /// Add an optional configuration key with a default value.
    #[must_use]
    pub fn default(
        mut self,
        name: impl Into<String>,
        kind: ConfigKind,
        default: impl Into<Value>,
    ) -> Self {
        let name = name.into();
        assert!(
            kind != ConfigKind::Secret,
            "secret connector config key `{name}` cannot have a default"
        );
        self.define_key(ConfigKey {
            name,
            kind,
            required: false,
            default: Some(default.into()),
            description: None,
        });
        self
    }

    /// Add a required secret configuration key.
    #[must_use]
    pub fn secret(self, name: impl Into<String>) -> Self {
        self.required(name, ConfigKind::Secret)
    }

    fn define_key(&mut self, key: ConfigKey) {
        let name = key.name.clone();
        assert!(
            self.keys.insert(name.clone(), key).is_none(),
            "duplicate connector config key `{name}`"
        );
    }

    /// Validate raw configuration and resolve secret references.
    pub async fn resolve(
        &self,
        raw: RawConfig,
        resolver: &dyn SecretResolver,
    ) -> ConfigResult<ResolvedConfig> {
        self.resolve_with_options(raw, resolver, ResolveOptions::default())
            .await
    }

    /// Validate raw configuration and resolve secret references with options.
    pub async fn resolve_with_options(
        &self,
        raw: RawConfig,
        resolver: &dyn SecretResolver,
        options: ResolveOptions,
    ) -> ConfigResult<ResolvedConfig> {
        self.reject_unknown_keys(&raw)?;
        self.validate_defaults(options)?;

        let mut resolved_config = ResolvedConfig::default();
        for key in self.keys.values() {
            let (value, is_default) = if let Some(value) = raw.get(&key.name).cloned() {
                (Some(value), false)
            } else {
                (key.default.clone(), true)
            };
            let Some(value) = value else {
                if key.required {
                    return Err(ConfigError::MissingRequired {
                        key: key.name.clone(),
                    });
                }
                continue;
            };

            if key.kind == ConfigKind::Secret {
                let secret = resolve_secret_value(&key.name, value, resolver, options).await?;
                resolved_config.insert_secret(key.name.clone(), secret);
            } else if let Err(err) = validate_kind(&key.name, key.kind, &value) {
                if is_default {
                    return Err(ConfigError::InvalidDefault {
                        key: key.name.clone(),
                        reason: err.to_string(),
                    });
                }
                return Err(err);
            } else {
                resolved_config.insert_plain(key.name.clone(), value);
            }
        }

        Ok(resolved_config)
    }

    fn reject_unknown_keys(&self, raw: &RawConfig) -> ConfigResult<()> {
        let known = self.keys.keys().collect::<BTreeSet<_>>();
        for key in raw.keys() {
            if !known.contains(key) {
                return Err(ConfigError::UnknownKey { key: key.clone() });
            }
        }
        Ok(())
    }

    fn validate_defaults(&self, options: ResolveOptions) -> ConfigResult<()> {
        for key in self.keys.values() {
            let Some(default) = &key.default else {
                continue;
            };
            let result = if key.kind == ConfigKind::Secret {
                validate_secret_default(default, options)
            } else {
                validate_kind(&key.name, key.kind, default).map_err(|err| err.to_string())
            };
            if let Err(err) = result {
                return Err(ConfigError::InvalidDefault {
                    key: key.name.clone(),
                    reason: err,
                });
            }
        }
        Ok(())
    }
}

fn validate_kind(key: &str, kind: ConfigKind, value: &Value) -> ConfigResult<()> {
    let valid = match kind {
        ConfigKind::String => value.is_string(),
        ConfigKind::Bool => value.is_boolean(),
        ConfigKind::Integer => value.as_i64().is_some(),
        ConfigKind::DurationMillis | ConfigKind::DurationMs => {
            value.as_i64().is_some_and(|millis| millis >= 0)
        }
        ConfigKind::UnsignedInteger => value.as_u64().is_some(),
        ConfigKind::Float => value.as_f64().is_some(),
        ConfigKind::StringList => value
            .as_array()
            .is_some_and(|items| items.iter().all(Value::is_string)),
        ConfigKind::Json => true,
        ConfigKind::Secret => unreachable!("secret kind validated separately"),
    };

    if valid {
        Ok(())
    } else {
        Err(ConfigError::WrongType {
            key: key.into(),
            expected: kind.expected(),
        })
    }
}

fn validate_secret_default(value: &Value, options: ResolveOptions) -> Result<(), String> {
    if value.is_string() {
        if options.allow_literal_secrets {
            return Ok(());
        }
        return Err("literal secret strings are disabled".into());
    }

    serde_json::from_value::<SecretRef>(value.clone())
        .map(|_| ())
        .map_err(|source| source.to_string())
}

async fn resolve_secret_value(
    key: &str,
    value: Value,
    resolver: &dyn SecretResolver,
    options: ResolveOptions,
) -> ConfigResult<SecretString> {
    if let Some(literal) = value.as_str() {
        if options.allow_literal_secrets {
            return Ok(SecretString::new(literal));
        }
        return Err(ConfigError::InvalidSecretRef {
            key: key.into(),
            reason: "literal secret strings are disabled".into(),
        });
    }

    let secret_ref: SecretRef =
        serde_json::from_value(value).map_err(|source| ConfigError::InvalidSecretRef {
            key: key.into(),
            reason: source.to_string(),
        })?;

    resolver
        .resolve(&secret_ref)
        .await
        .map_err(|source| ConfigError::SecretResolution {
            key: key.into(),
            source: Box::new(source),
        })
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use async_trait::async_trait;
    use serde_json::{Value, json};

    use super::*;
    use crate::config::{ConfigError, EnvSecretResolver, ResolveOptions, SecretResolutionError};

    fn raw(entries: impl IntoIterator<Item = (&'static str, Value)>) -> RawConfig {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    #[tokio::test]
    async fn resolve_applies_defaults_and_validates_required_fields() {
        let def = ConfigDef::new("demo")
            .required("database_url", ConfigKind::String)
            .default("schema", ConfigKind::String, "public");
        let raw = raw([("database_url", json!("postgres://localhost/app"))]);

        let resolved = def.resolve(raw, &EnvSecretResolver).await.unwrap();

        assert_eq!(
            resolved.get_string("database_url").unwrap(),
            "postgres://localhost/app".to_string()
        );
        assert_eq!(resolved.get_string("schema").unwrap(), "public".to_string());
        assert!(!resolved.contains_key("missing_optional"));
    }

    #[tokio::test]
    async fn resolve_rejects_unknown_keys() {
        let def = ConfigDef::new("demo").required("database_url", ConfigKind::String);
        let raw = raw([
            ("database_url", json!("postgres://localhost/app")),
            ("extra", json!(true)),
        ]);

        let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

        assert!(matches!(err, ConfigError::UnknownKey { key } if key == "extra"));
    }

    #[tokio::test]
    async fn resolve_rejects_missing_required_keys() {
        let def = ConfigDef::new("demo").required("database_url", ConfigKind::String);

        let err = def
            .resolve(RawConfig::new(), &EnvSecretResolver)
            .await
            .unwrap_err();

        assert!(matches!(err, ConfigError::MissingRequired { key } if key == "database_url"));
    }

    #[tokio::test]
    async fn resolve_rejects_wrong_types() {
        let def = ConfigDef::new("demo").required("database_url", ConfigKind::String);
        let raw = raw([("database_url", json!(42))]);

        let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

        assert!(
            matches!(err, ConfigError::WrongType { key, expected: "string" } if key == "database_url")
        );
    }

    #[tokio::test]
    async fn defaults_are_validated() {
        let def = ConfigDef::new("demo").default("topics", ConfigKind::StringList, json!(["a", 7]));

        let err = def
            .resolve(RawConfig::new(), &EnvSecretResolver)
            .await
            .unwrap_err();

        assert!(matches!(err, ConfigError::InvalidDefault { key, .. } if key == "topics"));
    }

    #[tokio::test]
    async fn defaults_are_validated_even_when_raw_value_is_supplied() {
        let def = ConfigDef::new("demo").default("topics", ConfigKind::StringList, json!(["a", 7]));
        let raw = raw([("topics", json!(["user"]))]);

        let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

        assert!(matches!(err, ConfigError::InvalidDefault { key, .. } if key == "topics"));
    }

    #[tokio::test]
    async fn integer_kind_rejects_values_outside_i64_range() {
        let def = ConfigDef::new("demo").required("limit", ConfigKind::Integer);
        let raw = raw([("limit", json!(i64::MAX as u64 + 1))]);

        let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

        assert!(
            matches!(err, ConfigError::WrongType { key, expected: "integer" } if key == "limit")
        );
    }

    #[tokio::test]
    async fn unsigned_integer_kind_accepts_u64_max() {
        let def = ConfigDef::new("demo").required("limit", ConfigKind::UnsignedInteger);
        let raw = raw([("limit", json!(u64::MAX))]);

        let resolved = def.resolve(raw, &EnvSecretResolver).await.unwrap();

        assert_eq!(resolved.get_u64("limit").unwrap(), u64::MAX);
    }

    #[tokio::test]
    async fn unsigned_integer_kind_rejects_negative_values() {
        let def = ConfigDef::new("demo").required("limit", ConfigKind::UnsignedInteger);
        let raw = raw([("limit", json!(-1))]);

        let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

        assert!(
            matches!(err, ConfigError::WrongType { key, expected: "unsigned integer" } if key == "limit")
        );
    }

    #[tokio::test]
    async fn duration_kinds_reject_negative_milliseconds() {
        for kind in [ConfigKind::DurationMillis, ConfigKind::DurationMs] {
            let def = ConfigDef::new("demo").required("timeout", kind);
            let raw = raw([("timeout", json!(-1))]);

            let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

            assert!(
                matches!(err, ConfigError::WrongType { key, expected: "duration milliseconds" } if key == "timeout")
            );
        }
    }

    #[tokio::test]
    async fn typed_getters_return_resolved_values() {
        let def = ConfigDef::new("demo")
            .required("name", ConfigKind::String)
            .required("enabled", ConfigKind::Bool)
            .required("limit", ConfigKind::Integer)
            .required("unsigned_limit", ConfigKind::UnsignedInteger)
            .required("timeout_ms", ConfigKind::DurationMillis)
            .required("ratio", ConfigKind::Float)
            .required("topics", ConfigKind::StringList)
            .required("metadata", ConfigKind::Json)
            .secret("password");
        let raw = raw([
            ("name", json!("source-a")),
            ("enabled", json!(true)),
            ("limit", json!(42)),
            ("unsigned_limit", json!(u64::MAX)),
            ("timeout_ms", json!(2500)),
            ("ratio", json!(0.75)),
            ("topics", json!(["alpha", "beta"])),
            ("metadata", json!({"mode": "snapshot"})),
            ("password", json!("literal-secret")),
        ]);

        let resolved = def
            .resolve_with_options(
                raw,
                &EnvSecretResolver,
                ResolveOptions {
                    allow_literal_secrets: true,
                },
            )
            .await
            .unwrap();

        assert_eq!(resolved.get_string("name").unwrap(), "source-a".to_string());
        assert!(resolved.get_bool("enabled").unwrap());
        assert_eq!(resolved.get_i64("limit").unwrap(), 42);
        assert_eq!(resolved.get_u64("unsigned_limit").unwrap(), u64::MAX);
        assert_eq!(resolved.get_u64("timeout_ms").unwrap(), 2500);
        assert_eq!(
            resolved.get_string_list("topics").unwrap(),
            vec!["alpha".to_string(), "beta".to_string()]
        );
        assert_eq!(
            resolved.get_json("metadata").unwrap(),
            json!({"mode": "snapshot"})
        );
        assert_eq!(
            resolved.get_secret("password").unwrap().expose_secret(),
            "literal-secret"
        );
        assert!((resolved.get_f64("ratio").unwrap() - 0.75).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn duration_ms_spelling_remains_supported() {
        let def = ConfigDef::new("demo").required("timeout_ms", ConfigKind::DurationMs);
        let raw = raw([("timeout_ms", json!(2500))]);

        let resolved = def.resolve(raw, &EnvSecretResolver).await.unwrap();

        assert_eq!(resolved.get_u64("timeout_ms").unwrap(), 2500);
        assert_eq!(
            ConfigKind::DurationMs.expected(),
            ConfigKind::DurationMillis.expected()
        );
    }

    #[test]
    fn config_def_reports_its_name() {
        let def = ConfigDef::new("postgres-source");

        assert_eq!(def.name(), "postgres-source");
    }

    #[test]
    fn config_def_debug_includes_name_and_redacts_secret_defaults() {
        let key = ConfigKey {
            name: "password".to_string(),
            kind: ConfigKind::Secret,
            required: false,
            default: Some(json!("literal-secret")),
            description: Some("database password".to_string()),
        };
        let def = ConfigDef {
            name: "demo".to_string(),
            keys: BTreeMap::from_iter([("password".to_string(), key)]),
        };

        let debug = format!("{def:?}");

        check!(
            (
                debug.contains("ConfigDef"),
                debug.contains("demo"),
                debug.contains("password"),
                debug.contains("<redacted>"),
                debug.contains("literal-secret"),
            ) == (true, true, true, true, false)
        );
    }

    #[test]
    #[should_panic(expected = "duplicate connector config key `database_url`")]
    fn duplicate_config_def_keys_panic_at_definition_time() {
        let _ = ConfigDef::new("demo")
            .required("database_url", ConfigKind::String)
            .optional("database_url", ConfigKind::String);
    }

    #[test]
    #[should_panic(expected = "secret connector config key `password` cannot have a default")]
    fn secret_defaults_panic_at_definition_time() {
        let _ = ConfigDef::new("demo").default("password", ConfigKind::Secret, "literal-secret");
    }

    #[test]
    fn config_key_debug_secret_and_non_secret_cases() {
        let cases = [
            (
                "secret default",
                ConfigKey {
                    name: "password".to_string(),
                    kind: ConfigKind::Secret,
                    required: false,
                    default: Some(json!("literal-secret")),
                    description: None,
                },
                (false, true, false),
            ),
            (
                "non-secret default",
                ConfigKey {
                    name: "schema".to_string(),
                    kind: ConfigKind::String,
                    required: false,
                    default: Some(json!("public")),
                    description: None,
                },
                (true, false, false),
            ),
        ];

        for (name, key, expected) in cases {
            let debug = format!("{key:?}");
            let actual = (
                debug.contains("public"),
                debug.contains("<redacted>"),
                debug.contains("literal-secret"),
            );
            assert_eq!(actual, expected, "config key debug case {name}");
        }
    }

    #[test]
    fn secret_default_validation_rejects_literals_without_opt_in() {
        let err = validate_secret_default(&json!("literal-secret"), ResolveOptions::default())
            .expect_err("literal secret defaults require explicit opt in");

        assert_eq!(err, "literal secret strings are disabled");
    }

    #[tokio::test]
    async fn secret_literals_are_rejected_by_default() {
        let def = ConfigDef::new("demo").secret("password");
        let raw = raw([("password", json!("secret"))]);

        let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

        assert!(matches!(err, ConfigError::InvalidSecretRef { key, .. } if key == "password"));
    }

    #[tokio::test]
    async fn secret_literals_can_be_allowed_for_local_use_and_debug_is_redacted() {
        let def = ConfigDef::new("demo").secret("password");
        let raw = raw([("password", json!("secret"))]);

        let resolved = def
            .resolve_with_options(
                raw,
                &EnvSecretResolver,
                ResolveOptions {
                    allow_literal_secrets: true,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            resolved.get_secret("password").unwrap().expose_secret(),
            "secret"
        );
        assert!(!format!("{resolved:?}").contains("secret"));
        assert!(format!("{resolved:?}").contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn structured_secret_refs_resolve_through_provider() {
        struct RecordingResolver;

        #[async_trait]
        impl SecretResolver for RecordingResolver {
            async fn resolve(
                &self,
                secret_ref: &SecretRef,
            ) -> Result<SecretString, SecretResolutionError> {
                assert_eq!(
                    secret_ref,
                    &SecretRef::Env {
                        name: "POSTGRES_PASSWORD".into()
                    }
                );
                Ok(SecretString::new("resolved-password"))
            }
        }

        let def = ConfigDef::new("demo").secret("password");
        let raw = raw([(
            "password",
            json!({"from": "env", "name": "POSTGRES_PASSWORD"}),
        )]);

        let resolved = def.resolve(raw, &RecordingResolver).await.unwrap();

        assert_eq!(
            resolved.get_secret("password").unwrap().expose_secret(),
            "resolved-password"
        );
    }

    #[tokio::test]
    async fn env_resolver_failure_reports_config_field_key() {
        let def = ConfigDef::new("demo").secret("password");
        let raw = raw([(
            "password",
            json!({"from": "env", "name": "CRABKA_CONNECT_TEST_MISSING_PASSWORD"}),
        )]);

        let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

        let ConfigError::SecretResolution { key, .. } = err else {
            panic!("expected secret resolution failure for the config field")
        };
        assert_eq!(key, "password");
    }

    #[tokio::test]
    async fn secret_ref_unknown_fields_report_invalid_secret_ref_for_config_field() {
        let def = ConfigDef::new("demo").secret("password");
        let raw = raw([(
            "password",
            json!({"from": "env", "name": "POSTGRES_PASSWORD", "extra": true}),
        )]);

        let err = def.resolve(raw, &EnvSecretResolver).await.unwrap_err();

        assert!(matches!(err, ConfigError::InvalidSecretRef { ref key, .. } if key == "password"));
        assert!(err.to_string().contains("unknown field"));
    }
}
