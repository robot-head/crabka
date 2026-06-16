use std::env;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::error::{ConfigError, ConfigResult};

/// Owned secret text. Formatting is always redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap a secret value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the secret text for use by connector implementations.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString([REDACTED])")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// A reference to secret material held by an external provider.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "camelCase")]
pub enum SecretRef {
    /// Resolve from an environment variable.
    Env { name: String },
    /// Resolve from a Kubernetes Secret key.
    KubernetesSecret { name: String, key: String },
    /// Resolve from a Vault path/key pair.
    Vault { path: String, key: String },
}

/// Options controlling config resolution.
#[derive(Debug, Clone, Copy, Default)]
pub struct ResolveOptions {
    /// Permit direct string values for secret fields. Intended for tests and
    /// local development only.
    pub allow_literal_secrets: bool,
}

/// Resolves secret references into redacted secret values.
#[async_trait]
pub trait SecretResolver: Send + Sync {
    /// Resolve one secret reference.
    async fn resolve(&self, secret_ref: &SecretRef) -> ConfigResult<SecretString>;
}

/// Secret resolver backed by process environment variables.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvSecretResolver;

#[async_trait]
impl SecretResolver for EnvSecretResolver {
    async fn resolve(&self, secret_ref: &SecretRef) -> ConfigResult<SecretString> {
        match secret_ref {
            SecretRef::Env { name } => env::var(name).map(SecretString::new).map_err(|source| {
                ConfigError::SecretResolution {
                    key: name.clone(),
                    source: Box::new(source),
                }
            }),
            SecretRef::KubernetesSecret { .. } | SecretRef::Vault { .. } => {
                Err(ConfigError::InvalidSecretRef {
                    key: "secret".into(),
                    reason: "resolver only supports env references".into(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_string_redacts_debug_and_display() {
        let secret = SecretString::new("super-secret");

        assert_eq!(format!("{secret:?}"), "SecretString([REDACTED])");
        assert_eq!(secret.to_string(), "[REDACTED]");
        assert_eq!(secret.expose_secret(), "super-secret");
    }

    #[test]
    fn secret_ref_parses_env_contract() {
        let value = serde_json::json!({
            "from": "env",
            "name": "POSTGRES_PASSWORD"
        });

        let parsed: SecretRef = serde_json::from_value(value).unwrap();

        assert_eq!(
            parsed,
            SecretRef::Env {
                name: "POSTGRES_PASSWORD".into()
            }
        );
    }
}
