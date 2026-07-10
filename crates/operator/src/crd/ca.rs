//! `Kafka.spec.clusterCa` + `Kafka.spec.clientsCa` schema.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ids::{CertGeneration, KeyGeneration};

/// Per-CA declarative config. Strimzi-shaped.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CertificateAuthority {
    /// When `true` (default), the operator generates and renews this CA.
    /// When `false`, the operator expects the CA `Secret` pair to be
    /// pre-created by the cluster admin and refuses to overwrite them.
    /// Renewal of BYO CAs is the admin's responsibility; the `CronJob`
    /// skips them and emits an Event when they're nearing expiry.
    #[serde(default = "default_generate")]
    pub generate_certificate_authority: bool,

    /// Cert validity in days. Default 365.
    #[serde(default = "default_validity_days")]
    pub validity_days: u32,

    /// Window before `notAfter` in which the renewal `CronJob` will
    /// reissue leaf certs. Default 30.
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

/// Status surface for a single CA. Populated by the reconciler from the
/// parsed CA cert + the CRD spec.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct CertificateAuthorityStatus {
    /// RFC3339 `notAfter` of the current (signing) CA cert.
    pub not_after: String,
    /// `true` when the operator generated this CA (i.e.
    /// `generateCertificateAuthority == true`); `false` for BYO.
    pub generated: bool,
    /// Monotonic generation of the active signing cert (bumped on
    /// same-key renewal and on key promotion).
    #[serde(default)]
    pub cert_generation: CertGeneration,
    /// Monotonic generation of the active signing key (bumped only on
    /// key replacement).
    #[serde(default)]
    pub key_generation: KeyGeneration,
    /// Staged key-replacement phase
    /// (`idle` | `key-replace-trust` | `key-replace-promote`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation_phase: Option<String>,
    /// Number of CA certs currently in the trust bundle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust_anchors: Option<usize>,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn defaults_match_strimzi() {
        let d = CertificateAuthority::default();
        assert2::assert!(
            d == CertificateAuthority {
                generate_certificate_authority: true,
                validity_days: 365,
                renewal_days: 30,
            }
        );
    }

    #[test]
    fn certificate_authority_deserialization_cases() {
        for (_name, json, expected) in [
            (
                "empty object uses defaults",
                serde_json::json!({}),
                CertificateAuthority::default(),
            ),
            (
                "bring-your-own CA",
                serde_json::json!({
                    "generateCertificateAuthority": false,
                    "validityDays": 90,
                    "renewalDays": 7,
                }),
                CertificateAuthority {
                    generate_certificate_authority: false,
                    validity_days: 90,
                    renewal_days: 7,
                },
            ),
        ] {
            assert2::assert!(
                serde_json::from_value::<CertificateAuthority>(json).expect("parse") == expected
            );
        }
    }
}
