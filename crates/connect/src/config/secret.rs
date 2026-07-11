use std::{env, error::Error, fmt};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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
#[serde(tag = "from", rename_all = "camelCase", deny_unknown_fields)]
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

/// Errors raised by a secret provider while resolving a reference.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SecretResolutionError {
    /// The reference names a provider this resolver does not support.
    #[error("unsupported secret reference provider `{provider}`")]
    UnsupportedReference { provider: &'static str },

    /// An environment-variable lookup failed.
    #[error("failed to resolve environment variable `{name}`: {source}")]
    EnvVar {
        name: String,
        #[source]
        source: env::VarError,
    },

    /// A provider-specific resolver failure.
    #[error("secret provider failed: {message}")]
    Provider {
        message: String,
        #[source]
        source: Option<Box<dyn Error + Send + Sync>>,
    },
}

/// Resolves secret references into redacted secret values.
#[async_trait]
pub trait SecretResolver: Send + Sync {
    /// Resolve one secret reference.
    async fn resolve(&self, secret_ref: &SecretRef) -> Result<SecretString, SecretResolutionError>;
}

/// Secret resolver backed by process environment variables.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvSecretResolver;

#[async_trait]
impl SecretResolver for EnvSecretResolver {
    async fn resolve(&self, secret_ref: &SecretRef) -> Result<SecretString, SecretResolutionError> {
        match secret_ref {
            SecretRef::Env { name } => env::var(name).map(SecretString::new).map_err(|source| {
                SecretResolutionError::EnvVar {
                    name: name.clone(),
                    source,
                }
            }),
            SecretRef::KubernetesSecret { .. } => {
                Err(SecretResolutionError::UnsupportedReference {
                    provider: "kubernetesSecret",
                })
            }
            SecretRef::Vault { .. } => {
                Err(SecretResolutionError::UnsupportedReference { provider: "vault" })
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

        assert2::assert!(format!("{secret:?}") == "SecretString([REDACTED])".to_string());
        assert2::assert!(secret.to_string() == "[REDACTED]".to_string());
        assert2::assert!(secret.expose_secret() == "super-secret");
    }

    #[test]
    fn secret_ref_parses_env_contract() {
        let value = serde_json::json!({
            "from": "env",
            "name": "POSTGRES_PASSWORD"
        });

        let parsed: SecretRef = serde_json::from_value(value).unwrap();

        assert2::assert!(
            parsed
                == SecretRef::Env {
                    name: "POSTGRES_PASSWORD".into()
                }
        );
    }

    #[test]
    fn secret_ref_rejects_unknown_fields() {
        let value = serde_json::json!({
            "from": "env",
            "name": "POSTGRES_PASSWORD",
            "extra": true
        });

        let err = serde_json::from_value::<SecretRef>(value).unwrap_err();

        assert2::assert!(err.to_string().contains("unknown field"));
    }

    #[tokio::test]
    async fn env_resolver_reports_missing_env_as_provider_error() {
        let resolver = EnvSecretResolver;
        let err = resolver
            .resolve(&SecretRef::Env {
                name: "CRABKA_CONNECT_TEST_MISSING_SECRET".into(),
            })
            .await
            .unwrap_err();

        assert2::assert!(matches!(
            err,
            SecretResolutionError::EnvVar {
                name,
                source: env::VarError::NotPresent,
            } if name == "CRABKA_CONNECT_TEST_MISSING_SECRET"
        ));
    }

    #[tokio::test]
    async fn env_resolver_reports_valid_non_env_refs_as_unsupported() {
        let resolver = EnvSecretResolver;
        let err = resolver
            .resolve(&SecretRef::Vault {
                path: "secret/data/connect/pg".into(),
                key: "password".into(),
            })
            .await
            .unwrap_err();

        assert2::assert!(matches!(
            err,
            SecretResolutionError::UnsupportedReference { provider: "vault" }
        ));
    }

    #[test]
    fn provider_error_carries_message_and_optional_source() {
        let err = SecretResolutionError::Provider {
            message: "vault token expired".into(),
            source: Some(Box::new(env::VarError::NotPresent)),
        };

        assert2::assert!(err.to_string() == "secret provider failed: vault token expired");
        assert2::assert!(std::error::Error::source(&err).is_some());
    }
}
