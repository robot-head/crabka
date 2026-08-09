//! Schema for `Kafka.spec.clusterCa` and `Kafka.spec.clientsCa`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ids::{CertGeneration, KeyGeneration};

/// Declarative configuration for one CA, in the Strimzi shape.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CertificateAuthority {
    /// When `true`, which is the default, the operator generates and
    /// renews this CA. When `false`, the cluster admin must create the CA
    /// `Secret` pair first, and the operator refuses to overwrite them.
    /// The admin renews a BYO CA. The `CronJob` skips a BYO CA and emits
    /// an Event when the CA comes near its expiry.
    #[serde(default = "default_generate")]
    pub generate_certificate_authority: bool,

    /// Cert validity in days. Default 365.
    #[serde(default = "default_validity_days")]
    pub validity_days: u32,

    /// Window in days before `notAfter` in which the renewal `CronJob`
    /// reissues the leaf certs. Default 30.
    #[serde(default = "default_renewal_days")]
    pub renewal_days: u32,
}

#[must_use]
const fn default_generate() -> bool {
    true
}
#[must_use]
const fn default_validity_days() -> u32 {
    365
}
#[must_use]
const fn default_renewal_days() -> u32 {
    30
}

impl Default for CertificateAuthority {
    fn default() -> Self {
        Self {
            generate_certificate_authority: default_generate(),
            validity_days: default_validity_days(),
            renewal_days: default_renewal_days(),
        }
    }
}

/// Status surface for one CA.
///
/// The reconciler fills it in from the parsed CA cert and the CRD spec.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct CertificateAuthorityStatus {
    /// RFC3339 `notAfter` of the current CA cert, which is the signing
    /// cert.
    pub not_after: String,
    /// `true` when the operator generated this CA, that is, when
    /// `generateCertificateAuthority == true`. `false` for a BYO CA.
    pub generated: bool,
    /// Monotonic generation of the active signing cert. It increments on
    /// a same-key renewal and on a key promotion.
    #[serde(default)]
    pub cert_generation: CertGeneration,
    /// Monotonic generation of the active signing key. It increments only
    /// on a key replacement.
    #[serde(default)]
    pub key_generation: KeyGeneration,
    /// Staged key-replacement phase. One of `idle`, `key-replace-trust`,
    /// and `key-replace-promote`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation_phase: Option<String>,
    /// Number of CA certs in the trust bundle now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust_anchors: Option<usize>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn defaults_match_strimzi() {
        let d = CertificateAuthority::default();
        assert!(
            d == CertificateAuthority {
                generate_certificate_authority: true,
                validity_days: 365,
                renewal_days: 30,
            }
        );
    }

    #[test]
    fn deserialize_empty_object_uses_defaults() {
        let v: CertificateAuthority = serde_json::from_value(serde_json::json!({})).expect("parse");
        assert!(v == CertificateAuthority::default());
    }

    #[test]
    fn byo_round_trips() {
        let v: CertificateAuthority = serde_json::from_value(serde_json::json!({
            "generateCertificateAuthority": false,
            "validityDays": 90,
            "renewalDays": 7,
        }))
        .expect("parse");
        assert!(
            v == CertificateAuthority {
                generate_certificate_authority: false,
                validity_days: 90,
                renewal_days: 7,
            }
        );
    }
}
