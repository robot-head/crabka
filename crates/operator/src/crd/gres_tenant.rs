//! `GresTenant` CRD.
//!
//! A `GresTenant` represents one tenant provisioned under a `Gres` fleet. The
//! reconciler provisions registry records and compute workloads.

use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{SecretKeyRef, TenantDefaults};

/// Gres tenant specification.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "GresTenant",
    plural = "grestenants",
    singular = "grestenant",
    shortname = "gt",
    namespaced,
    status = "GresTenantStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct GresTenantSpec {
    /// Name of the `Gres` fleet this tenant belongs to.
    pub gres: String,

    /// Compute container image override. When absent, the operator uses its
    /// global Gres image override or its compiled default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub image: Option<String>,

    /// SQL user exposed through `PgDog` and enforced by the tenant compute.
    pub user: String,

    /// Secret key that holds the plaintext tenant password. The reconciler
    /// hashes it to a verifier. The reconciler never copies the password to
    /// status.
    pub password_secret_ref: SecretKeyRef,

    /// Suspends the tenant when true. The operator treats unset as active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspended: Option<bool>,

    /// CPU and memory resource requests and limits for the tenant compute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// Optional ordered range layout. An omitted layout gives one open-ended
    /// range r0.
    ///
    /// The operator places each entry on a distinct range compute.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<GresTenantRangeSpec>,

    /// Tenant-specific overrides for fleet defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overrides: Option<TenantDefaults>,
}

/// One row-key range in a tenant layout.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GresTenantRangeSpec {
    /// Range identifier used by compute placement and WAL topics.
    pub range_id: u32,
    /// Exclusive `(table_id, bucket?, rowid)` upper bound. Unset marks the final range.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_key: Option<GresTenantRangeKey>,
}

/// Boundary between two Gres tenant ranges.
#[derive(
    Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(rename_all = "camelCase")]
pub struct GresTenantRangeKey {
    /// Table identifier at the boundary.
    pub table_id: u64,
    /// Hash bucket at the boundary. It is absent for non-hash tables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<u32>,
    /// Row identifier at the boundary.
    pub rowid: u64,
}

/// Observed tenant state.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GresTenantStatus {
    /// Kubernetes-style condition list.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last successfully-reconciled spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Whether the tenant is ready to accept connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,

    /// Tenant WAL topic name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_topic: Option<String>,

    /// Registry record version last written for this tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_version: Option<u64>,

    /// Lifecycle phase last observed by the operator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_phase: Option<String>,

    /// Bounded compatibility window for `PgDog` 0.1.47 after a resumed compute
    /// becomes Active. The fleet reconciler removes the temporary backend
    /// credential after this Unix-millisecond deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pgdog_credential_grace_until_unix_ms: Option<u64>,

    /// Last checkpoint offset observed by the controller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_offset: Option<u64>,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = GresTenant::crd();
        check!(crd.spec.group == "crabka.io");
        check!(crd.spec.names.kind == "GresTenant");
        check!(crd.spec.names.plural == "grestenants");
        check!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"gt".to_string())),
            "expected shortname `gt`",
        );
        check!(crd.spec.versions.len() == 1);
        check!(crd.spec.versions[0].name == "v1alpha1");
    }

    #[test]
    fn tenant_scram_iterations_schema_matches_broker_bounds() {
        let crd = serde_json::to_value(GresTenant::crd()).unwrap();
        let iterations = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["overrides"]["properties"]["scramIterations"];
        assert!(iterations["minimum"].as_f64() == Some(4_096.0));
        assert!(iterations["maximum"].as_f64() == Some(16_384.0));
    }

    #[test]
    fn spec_round_trips_through_json() {
        let spec = GresTenantSpec {
            gres: "analytics".into(),
            image: Some("example.test/gres:v2".into()),
            user: "alice".into(),
            password_secret_ref: SecretKeyRef {
                name: "alice-db-password".into(),
                key: "password".into(),
            },
            suspended: Some(false),
            resources: None,
            ranges: vec![GresTenantRangeSpec {
                range_id: 0,
                end_key: None,
            }],
            overrides: Some(TenantDefaults {
                wal_replication: Some(3),
                scram_iterations: Some(12_288),
                checkpoint_frames: None,
                checkpoint_size: Some(crabka_units::bytes(134_217_728)),
                suspend_max_checkpoint_size: None,
                idle_seconds: None,
            }),
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(
            json.contains("\"image\":\"example.test/gres:v2\""),
            "got: {json}"
        );
        assert!(json.contains("\"passwordSecretRef\""), "got: {json}");
        assert!(
            json.contains("\"checkpointSize\":\"128MiB\""),
            "got: {json}"
        );
        assert!(json.contains("\"scramIterations\":12288"), "got: {json}");
        let back: GresTenantSpec = serde_json::from_str(&json).unwrap();
        assert!(back == spec);
    }

    #[test]
    fn image_schema_requires_a_nonempty_value() {
        let crd = serde_json::to_value(GresTenant::crd()).expect("serialize GresTenant CRD");
        let image = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["image"];
        assert!(image["minLength"].as_u64() == Some(1), "got: {image}");
    }

    #[test]
    fn status_round_trips() {
        let status = GresTenantStatus {
            conditions: vec![],
            observed_generation: Some(7),
            ready: Some(true),
            wal_topic: Some("__gres_wal.alice".into()),
            registry_version: Some(3),
            lifecycle_phase: Some("active".into()),
            pgdog_credential_grace_until_unix_ms: Some(123),
            last_checkpoint_offset: Some(42),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"registryVersion\":3"), "got: {json}");
        let back: GresTenantStatus = serde_json::from_str(&json).unwrap();
        assert!(back == status);
    }
}
