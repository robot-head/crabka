/// Per-listener TLS and SASL resolution helpers.
///
/// The broker supports a single top-level `tls_config` and a global list of
/// `enabled_sasl_mechanisms`, but individual listeners can override both via
/// `ListenerSpec::tls_config` and `ListenerSpec::sasl_mechanisms`. These
/// helpers centralise the "per-listener wins, fall back to broker-wide"
/// resolution so the accept loop doesn't need to inline the logic.
use crabka_security::{SaslMechanism, TlsConfig};

use crate::config::ListenerSpec;
use crate::error::BrokerError;

/// Return the `TlsConfig` to use for `spec`.
///
/// Preference order:
/// 1. `spec.tls_config` (per-listener override)
/// 2. `top_level` (broker-wide `BrokerConfig::tls_config`)
///
/// Returns an error when neither is set, because a TLS-protocol listener
/// cannot proceed without certificate material.
pub(crate) fn resolve_tls_for_listener<'a>(
    spec: &'a ListenerSpec,
    top_level: Option<&'a TlsConfig>,
) -> Result<&'a TlsConfig, BrokerError> {
    if let Some(per_listener) = &spec.tls_config {
        return Ok(per_listener);
    }
    top_level.ok_or_else(|| {
        BrokerError::Tls(format!(
            "listener '{}' requires TLS but no tls_config (per-listener or top-level) is set",
            spec.name
        ))
    })
}

/// Return the SASL mechanisms enabled for `spec`.
///
/// Preference order:
/// 1. `spec.sasl_mechanisms` (per-listener override)
/// 2. `broker_default` (broker-wide `BrokerConfig::enabled_sasl_mechanisms`)
pub(crate) fn resolve_sasl_mechanisms_for_listener<'a>(
    spec: &'a ListenerSpec,
    broker_default: &'a [SaslMechanism],
) -> &'a [SaslMechanism] {
    spec.sasl_mechanisms.as_deref().unwrap_or(broker_default)
}

#[cfg(test)]
mod per_listener_config_tests {
    use super::*;
    use crabka_security::{ClientAuthMode, ListenerProtocol};
    use std::path::PathBuf;

    fn test_listener_spec(
        protocol: ListenerProtocol,
        tls: Option<TlsConfig>,
        sasl: Option<Vec<SaslMechanism>>,
    ) -> ListenerSpec {
        ListenerSpec {
            name: "test".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "localhost:9094".into(),
            protocol,
            tls_config: tls,
            sasl_mechanisms: sasl,
        }
    }

    fn dummy_tls(cert: &str, key: &str) -> TlsConfig {
        TlsConfig {
            cert_chain_path: PathBuf::from(cert),
            private_key_path: PathBuf::from(key),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        }
    }

    #[test]
    fn per_listener_tls_config_overrides_top_level() {
        let per_listener_tls = dummy_tls("/per-listener.crt", "/per-listener.key");
        let top_level = dummy_tls("/top-level.crt", "/top-level.key");
        let spec = test_listener_spec(ListenerProtocol::Ssl, Some(per_listener_tls.clone()), None);
        let resolved = resolve_tls_for_listener(&spec, Some(&top_level));
        assert_eq!(
            resolved.unwrap().cert_chain_path,
            per_listener_tls.cert_chain_path
        );
    }

    #[test]
    fn per_listener_tls_falls_back_to_top_level_when_absent() {
        let top_level = dummy_tls("/top-level.crt", "/top-level.key");
        let spec = test_listener_spec(ListenerProtocol::Ssl, None, None);
        let resolved = resolve_tls_for_listener(&spec, Some(&top_level));
        assert_eq!(resolved.unwrap().cert_chain_path, top_level.cert_chain_path);
    }

    #[test]
    fn tls_listener_without_any_config_errors() {
        let spec = test_listener_spec(ListenerProtocol::Ssl, None, None);
        let resolved = resolve_tls_for_listener(&spec, None);
        assert!(resolved.is_err());
    }

    #[test]
    fn per_listener_sasl_mechanisms_override_broker_default() {
        let per_listener = vec![SaslMechanism::ScramSha512];
        let broker_default = vec![SaslMechanism::Plain, SaslMechanism::ScramSha256];
        let spec = test_listener_spec(ListenerProtocol::SaslSsl, None, Some(per_listener.clone()));
        let resolved = resolve_sasl_mechanisms_for_listener(&spec, &broker_default);
        assert_eq!(resolved, &per_listener);
    }

    #[test]
    fn per_listener_sasl_falls_back_to_broker_default_when_absent() {
        let broker_default = vec![SaslMechanism::ScramSha512];
        let spec = test_listener_spec(ListenerProtocol::SaslSsl, None, None);
        let resolved = resolve_sasl_mechanisms_for_listener(&spec, &broker_default);
        assert_eq!(resolved, &broker_default);
    }
}
