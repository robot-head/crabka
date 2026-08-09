//! Per-listener SASL resolution helper.
use crabka_security::SaslMechanism;

use crate::config::ListenerSpec;

/// Returns the SASL mechanisms enabled for `spec`.
///
/// The preference order is:
/// 1. `spec.sasl_mechanisms`, the per-listener override
/// 2. `broker_default`, the broker-wide
///    `BrokerConfig::enabled_sasl_mechanisms`
pub(crate) fn resolve_sasl_mechanisms_for_listener<'a>(
    spec: &'a ListenerSpec,
    broker_default: &'a [SaslMechanism],
) -> &'a [SaslMechanism] {
    spec.sasl_mechanisms.as_deref().unwrap_or(broker_default)
}

#[cfg(test)]
mod per_listener_config_tests {
    use assert2::assert;
    use crabka_security::ListenerProtocol;

    use super::*;

    fn test_listener_spec(
        protocol: ListenerProtocol,
        sasl: Option<Vec<SaslMechanism>>,
    ) -> ListenerSpec {
        ListenerSpec {
            name: "test".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "localhost:9094".into(),
            protocol,
            tls_config: None,
            sasl_mechanisms: sasl,
        }
    }

    #[test]
    fn per_listener_sasl_mechanisms_override_broker_default() {
        let per_listener = vec![SaslMechanism::ScramSha512];
        let broker_default = vec![SaslMechanism::Plain, SaslMechanism::ScramSha256];
        let spec = test_listener_spec(ListenerProtocol::SaslSsl, Some(per_listener.clone()));
        let resolved = resolve_sasl_mechanisms_for_listener(&spec, &broker_default);
        assert!(resolved == &per_listener);
    }

    #[test]
    fn per_listener_sasl_falls_back_to_broker_default_when_absent() {
        let broker_default = vec![SaslMechanism::ScramSha512];
        let spec = test_listener_spec(ListenerProtocol::SaslSsl, None);
        let resolved = resolve_sasl_mechanisms_for_listener(&spec, &broker_default);
        assert!(resolved == &broker_default);
    }
}
