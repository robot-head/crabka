//! Lifecycle and rotation of the cluster CA and the clients CA.
//!
//! This module owns:
//! - the per-cluster `cluster CA` Secret pair, which is a private key and
//!   a public cert,
//! - the per-cluster `clients CA` Secret pair, which `user_tls.rs` held
//!   before,
//! - the per-cluster broker-keystore Secret `<cluster>-kafka-brokers`,
//! - `reconcile_ca`, the per-CA create, reuse, and **rotate** entry point
//!   that the `Kafka` reconciler calls on each pass. It also owns the pure
//!   rotation state machine `plan_ca_rotation` and the trust-bundle
//!   helpers,
//! - the pure `renew_if_expiring` predicate, which `reconcile_ca` and the
//!   `ca-renewal-check` `CronJob` subcommand both use,
//! - the `run_renewal_check` entry point for the `CronJob`.

use std::{collections::BTreeMap, net::IpAddr};

use crabka_security::ca::{
    CaMaterial, SubjectAltName, generate_clients_ca, generate_cluster_ca, issue_broker_cert,
};
use crabka_units::{
    Time,
    convert::TimeExt as _,
    days,
    uom::{num_traits::ToPrimitive as _, si::time::day},
};
use k8s_openapi::{
    ByteString, api::core::v1::Secret, apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    Resource, ResourceExt as _,
    api::{Api, Patch, PatchParams},
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    controller::common::{FIELD_MANAGER, ReconcileError, owner_ref, read_pem_key},
    crd::{CertificateAuthority, Kafka},
    ids::{CertGeneration, KeyGeneration},
};

pub(crate) const CLUSTER_CA_KEY_SUFFIX: &str = "-cluster-ca";
pub(crate) const CLUSTER_CA_CERT_SUFFIX: &str = "-cluster-ca-cert";
pub(crate) const CLIENTS_CA_KEY_SUFFIX: &str = "-clients-ca";
pub(crate) const CLIENTS_CA_CERT_SUFFIX: &str = "-clients-ca-cert";
pub(crate) const BROKER_KEYSTORE_SUFFIX: &str = "-kafka-brokers";

// Rotation bookkeeping.
/// Annotation on the cert Secret that tracks the monotonic generation of
/// the *active signing cert*. It increments on a same-key renewal and on a
/// key promotion.
pub(crate) const ANN_CERT_GENERATION: &str = "crabka.io/ca-cert-generation";
/// Annotation on the key Secret that tracks the monotonic generation of
/// the *active signing key*. It increments only when the operator replaces
/// the key.
pub(crate) const ANN_KEY_GENERATION: &str = "crabka.io/ca-key-generation";
/// Annotation on the cert Secret that records the staged key-replacement
/// phase.
pub(crate) const ANN_ROTATION_PHASE: &str = "crabka.io/ca-rotation-phase";
/// Secret-data keys for the staged new key and the staged new cert during
/// `key-replace-trust`.
const NEXT_KEY: &str = "ca.key.next";
const NEXT_CERT: &str = "ca.crt.next";

/// `Kafka` CR annotation: force a same-key CA cert renewal on the next reconcile.
pub const ANN_FORCE_RENEW: &str = "crabka.io/force-renew-ca";
/// `Kafka` CR annotation: force a staged CA key replacement on the next reconcile.
pub const ANN_FORCE_REPLACE_KEY: &str = "crabka.io/force-replace-ca-key";
/// `Kafka` CR annotation that the `CronJob` sets.
///
/// It means that a CA cert is inside `renewalDays`, so the reconciler should
/// run a same-key renewal. The value is an RFC3339 timestamp.
pub const ANN_RENEW_AFTER: &str = "crabka.io/ca-renew-after";

#[must_use]
pub(crate) fn cluster_ca_key_name(cluster: &str) -> String {
    format!("{cluster}{CLUSTER_CA_KEY_SUFFIX}")
}
#[must_use]
pub(crate) fn cluster_ca_cert_name(cluster: &str) -> String {
    format!("{cluster}{CLUSTER_CA_CERT_SUFFIX}")
}
#[must_use]
pub(crate) fn clients_ca_key_name(cluster: &str) -> String {
    format!("{cluster}{CLIENTS_CA_KEY_SUFFIX}")
}
#[must_use]
pub(crate) fn clients_ca_cert_name(cluster: &str) -> String {
    format!("{cluster}{CLIENTS_CA_CERT_SUFFIX}")
}
#[must_use]
pub(crate) fn broker_keystore_name(cluster: &str) -> String {
    format!("{cluster}{BROKER_KEYSTORE_SUFFIX}")
}

// ---------------------------------------------------------------------------
// Trust-bundle helpers (pure)
// ---------------------------------------------------------------------------

const BEGIN_CERT: &str = "-----BEGIN CERTIFICATE-----";
const END_CERT: &str = "-----END CERTIFICATE-----";

/// Splits a PEM bundle into single normalized certificate blocks.
///
/// Each block ends in one newline. This function ignores content that is
/// not a certificate.
#[must_use]
pub(crate) fn split_pem_certs(bundle: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = bundle;
    while let Some(b) = rest.find(BEGIN_CERT) {
        let after = &rest[b..];
        let Some(e) = after.find(END_CERT) else { break };
        let end = e + END_CERT.len();
        out.push(format!("{}\n", after[..end].trim()));
        rest = &after[end..];
    }
    out
}

/// Concatenates the cert blocks into one bundle PEM.
#[must_use]
pub(crate) fn join_bundle(blocks: &[String]) -> String {
    blocks.concat()
}

/// Removes the blocks that are byte duplicates and keeps the order of the
/// first occurrence.
#[must_use]
pub(crate) fn dedup_blocks(blocks: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    blocks
        .iter()
        .filter(|b| seen.insert((*b).clone()))
        .cloned()
        .collect()
}

/// Removes the expired blocks, which are the blocks with
/// `notAfter <= now`.
///
/// This function NEVER removes the first block, which is the signing
/// block. It also keeps a block that it cannot parse, as a defensive
/// measure.
#[must_use]
pub(crate) fn prune_expired(blocks: &[String], now: OffsetDateTime) -> Vec<String> {
    blocks
        .iter()
        .enumerate()
        .filter(|(i, b)| *i == 0 || cert_not_after(b).map_or(true, |na| na > now))
        .map(|(_, b)| b.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Rotation state machine (pure)
// ---------------------------------------------------------------------------

/// Staged key-replacement phase.
///
/// The operator stores it in the `crabka.io/ca-rotation-phase` annotation
/// of the cert Secret. An absent annotation means `Idle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumString)]
pub(crate) enum CaPhase {
    #[strum(serialize = "idle")]
    Idle,
    /// The operator generated a new CA and added its cert to the trust
    /// bundle for trust only. The old key still signs. A roll distributes
    /// the larger trust set.
    #[strum(serialize = "key-replace-trust")]
    KeyReplaceTrust,
    /// The operator promoted the new key to signer and reissued the
    /// leafs. The old cert stays in the bundle, so peers with open
    /// connections still validate. A roll applies the new leafs.
    #[strum(serialize = "key-replace-promote")]
    KeyReplacePromote,
}

impl CaPhase {
    pub(crate) fn as_str(self) -> &'static str {
        self.into()
    }
    fn parse(s: &str) -> Self {
        s.parse().unwrap_or(Self::Idle)
    }
}

/// CA material and rotation bookkeeping, rebuilt from the Secret pair.
#[derive(Debug, Clone)]
pub(crate) struct CaState {
    /// Trust bundle, signing cert first.
    pub bundle: Vec<String>,
    /// Active signing key. It pairs with `bundle[0]`.
    pub key_pem: String,
    /// Staged new key during `KeyReplaceTrust`.
    pub pending_key_pem: Option<String>,
    /// Staged new cert during `KeyReplaceTrust`.
    pub pending_cert_pem: Option<String>,
    pub cert_generation: CertGeneration,
    pub key_generation: KeyGeneration,
    pub phase: CaPhase,
}

/// Per-reconcile inputs to [`plan_ca_rotation`].
// distinct rotation triggers/state, not a state enum
pub(crate) struct RotationInputs {
    /// `generateCertificateAuthority`. The operator never rotates a BYO
    /// CA.
    pub generate: bool,
    pub validity: Time,
    pub renewal: Time,
    /// `crabka.io/force-renew-ca` present on the `Kafka` CR.
    pub force: ForceRotation,
    /// Every pool carries the desired config-hash AND is Ready, so the
    /// roll of the previous rotation step has finished. The planner reads
    /// this field only for the staged phases.
    pub rollout_converged: bool,
    pub now: OffsetDateTime,
    pub which: WhichCa,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForceRotation {
    None,
    RenewCertificate,
    ReplaceKey,
    RenewAndReplaceKey,
}

impl ForceRotation {
    fn renews_certificate(self) -> bool {
        matches!(self, Self::RenewCertificate | Self::RenewAndReplaceKey)
    }

    fn replaces_key(self) -> bool {
        matches!(self, Self::ReplaceKey | Self::RenewAndReplaceKey)
    }
}

/// The reason why the operator could not do a forced rotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefuseReason {
    /// `generateCertificateAuthority=false`. The operator never changes a
    /// BYO CA.
    Byo,
    /// The clients CA does not support key replacement.
    ClientsCaKeyReplace,
}

/// What the reconciler should do to a CA in this pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CaRotationPlan {
    NoOp,
    /// Sign the cert again with the existing key. This causes no
    /// disruption.
    RenewCertSameKey,
    /// Generate a new key and cert. Add the new cert to the bundle for
    /// trust only.
    StartKeyReplace,
    /// Promote the staged key to signer and reissue the leafs.
    PromoteNewKey,
    /// Remove the superseded and the expired trust anchors from the
    /// bundle.
    PruneOldTrust,
    /// A user asked for a forced rotation, but the operator cannot do
    /// it.
    Refuse(RefuseReason),
}

/// Makes the pure rotation decision. See the decision table in the
/// CA-rotation design.
pub(crate) fn plan_ca_rotation(state: &CaState, inp: &RotationInputs) -> CaRotationPlan {
    // BYO: never rotate. A forced request is refused (and the annotation is
    // stripped by the caller, with a Warning Event).
    if !inp.generate {
        return if inp.force.renews_certificate() || inp.force.replaces_key() {
            CaRotationPlan::Refuse(RefuseReason::Byo)
        } else {
            CaRotationPlan::NoOp
        };
    }

    match state.phase {
        // Mid key-replacement: advance only once the prior roll has converged.
        CaPhase::KeyReplaceTrust => {
            if inp.rollout_converged {
                CaRotationPlan::PromoteNewKey
            } else {
                CaRotationPlan::NoOp
            }
        }
        CaPhase::KeyReplacePromote => {
            if inp.rollout_converged {
                CaRotationPlan::PruneOldTrust
            } else {
                CaRotationPlan::NoOp
            }
        }
        CaPhase::Idle => {
            if inp.force.replaces_key() {
                return match inp.which {
                    WhichCa::Cluster => CaRotationPlan::StartKeyReplace,
                    WhichCa::Clients => CaRotationPlan::Refuse(RefuseReason::ClientsCaKeyReplace),
                };
            }
            let signing = state.bundle.first().map_or("", String::as_str);
            let renew_due = inp.force.renews_certificate()
                || renew_if_expiring(signing, inp.renewal, inp.now).unwrap_or(false);
            if renew_due {
                return CaRotationPlan::RenewCertSameKey;
            }
            // Routine prune of expired trust anchors that linger after a past
            // same-key renewal.
            let has_expired = state
                .bundle
                .iter()
                .skip(1)
                .any(|b| cert_not_after(b).is_ok_and(|na| na <= inp.now));
            if has_expired {
                CaRotationPlan::PruneOldTrust
            } else {
                CaRotationPlan::NoOp
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WhichCa {
    Cluster,
    Clients,
}

impl WhichCa {
    pub(crate) fn cn_suffix(self) -> &'static str {
        match self {
            Self::Cluster => "-cluster-ca",
            Self::Clients => "-clients-ca",
        }
    }
    pub(crate) fn condition_name(self) -> &'static str {
        match self {
            Self::Cluster => "ClusterCaReady",
            Self::Clients => "ClientsCaReady",
        }
    }
}

const SECRET_TYPE_CA_KEY: &str = "ca-key";
const SECRET_TYPE_CA_CERT: &str = "ca-cert";
const SECRET_TYPE_BROKER_KEYSTORE: &str = "broker-keystore";

pub(crate) fn cert_not_after(pem: &str) -> Result<OffsetDateTime, ReconcileError> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    use x509_parser::prelude::{FromDer, X509Certificate};
    let der = CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .ok_or_else(|| ReconcileError::CertParse("no PEM block".into()))?
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    OffsetDateTime::from_unix_timestamp(cert.validity().not_after.timestamp())
        .map_err(|e| ReconcileError::CertParse(e.to_string()))
}

/// Result of one reconcile pass over one CA, either the cluster CA or the
/// clients CA.
#[derive(Debug, Clone)]
pub(crate) struct CaReconcileOutcome {
    /// Active signing cert, which is `bundle[0]`, and the active key.
    /// Leaf issuance uses them.
    pub signing_material: CaMaterial,
    /// Full trust bundle PEM. The broker truststore and the config hash
    /// use it.
    pub trust_bundle_pem: String,
    /// RFC3339 `notAfter` of the signing cert.
    pub not_after: String,
    pub generated: bool,
    pub cert_generation: CertGeneration,
    pub key_generation: KeyGeneration,
    pub phase: CaPhase,
    pub trust_anchors: usize,
    /// Cluster CA only. The operator must reissue every broker leaf with
    /// the new key.
    pub force_reissue_leafs: bool,
    /// `CaRotation` condition surface.
    pub rotation_in_progress: bool,
    pub rotation_reason: &'static str,
    pub rotation_message: String,
    /// The operator refused a forced rotation. The caller emits a Warning
    /// Event.
    pub refused: Option<RefuseReason>,
}

/// Reconciles one CA.
///
/// This function creates the CA when it is missing, reuses and rotates an
/// existing CA, or reports `ByoCaMissing`. It makes exactly the create-path
/// I/O: `GET key`, `GET cert`, and on a create also `PATCH key` and
/// `PATCH cert`. It adds one more cert or key `PATCH` only when a rotation
/// step runs.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(cluster = %kafka.name_any(), which = ?which, force_renew, force_replace_key, rollout_converged),
    err,
)]
pub(crate) async fn reconcile_ca(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    which: WhichCa,
    force_renew: bool,
    force_replace_key: bool,
    rollout_converged: bool,
    now: OffsetDateTime,
) -> Result<CaReconcileOutcome, ReconcileError> {
    let cluster = kafka.name_any();
    let spec = match which {
        WhichCa::Cluster => kafka.spec.cluster_ca.clone().unwrap_or_default(),
        WhichCa::Clients => kafka.spec.clients_ca.clone().unwrap_or_default(),
    };
    let (key_name, cert_name) = match which {
        WhichCa::Cluster => (
            cluster_ca_key_name(&cluster),
            cluster_ca_cert_name(&cluster),
        ),
        WhichCa::Clients => (
            clients_ca_key_name(&cluster),
            clients_ca_cert_name(&cluster),
        ),
    };
    let cn = format!("{cluster}{}", which.cn_suffix());

    let existing_key = secret_api.get_opt(&key_name).await?;
    let existing_cert = secret_api.get_opt(&cert_name).await?;

    // Reuse + rotate path: both Secrets present with readable material.
    if let (Some(k), Some(c)) = (&existing_key, &existing_cert)
        && let (Some(key_pem), Some(bundle_pem)) =
            (read_pem_key(k, "ca.key"), read_pem_key(c, "ca.crt"))
    {
        let state = CaState {
            bundle: split_pem_certs(&bundle_pem),
            key_pem,
            pending_key_pem: read_pem_key(k, NEXT_KEY),
            pending_cert_pem: read_pem_key(k, NEXT_CERT),
            cert_generation: CertGeneration(read_generation(c, ANN_CERT_GENERATION)),
            key_generation: KeyGeneration(read_generation(k, ANN_KEY_GENERATION)),
            phase: read_phase(c),
        };
        let inp = RotationInputs {
            generate: spec.generate_certificate_authority,
            validity: days(spec.validity_days),
            renewal: days(spec.renewal_days),
            force: match (force_renew, force_replace_key) {
                (false, false) => ForceRotation::None,
                (true, false) => ForceRotation::RenewCertificate,
                (false, true) => ForceRotation::ReplaceKey,
                (true, true) => ForceRotation::RenewAndReplaceKey,
            },
            rollout_converged,
            now,
            which,
        };
        let plan = plan_ca_rotation(&state, &inp);
        return apply_ca_rotation(
            (secret_api, kafka, which),
            (&key_name, &cert_name, &cn),
            &state,
            plan,
            &inp,
            &bundle_pem,
        )
        .await;
    }

    // BYO CA whose Secret pair is absent.
    if !spec.generate_certificate_authority {
        return Err(ReconcileError::ByoCaMissing {
            which: which.condition_name().into(),
        });
    }

    // Create a fresh single-cert CA (generation 0, idle). Byte-identical to the
    // create path apart from the added generation annotation.
    let material = match which {
        WhichCa::Cluster => generate_cluster_ca(&cn, spec.validity_days)?,
        WhichCa::Clients => generate_clients_ca(&cn, spec.validity_days)?,
    };
    patch_secret(
        secret_api,
        kafka,
        &key_name,
        SECRET_TYPE_CA_KEY,
        [("ca.key".to_string(), material.key_pem.clone())].into(),
        [(ANN_KEY_GENERATION.to_string(), "0".to_string())].into(),
    )
    .await?;
    patch_secret(
        secret_api,
        kafka,
        &cert_name,
        SECRET_TYPE_CA_CERT,
        [("ca.crt".to_string(), material.cert_pem.clone())].into(),
        [(ANN_CERT_GENERATION.to_string(), "0".to_string())].into(),
    )
    .await?;

    let not_after = cert_not_after(&material.cert_pem)?
        .format(&Rfc3339)
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    Ok(CaReconcileOutcome {
        signing_material: CaMaterial {
            cert_pem: material.cert_pem.clone(),
            key_pem: material.key_pem,
        },
        trust_bundle_pem: material.cert_pem,
        not_after,
        generated: true,
        cert_generation: CertGeneration(0),
        key_generation: KeyGeneration(0),
        phase: CaPhase::Idle,
        trust_anchors: 1,
        force_reissue_leafs: false,
        rotation_in_progress: false,
        rotation_reason: "Idle",
        rotation_message: "no rotation in progress".into(),
        refused: None,
    })
}

async fn patch_secret(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    name: &str,
    secret_type_label: &str,
    data: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
) -> Result<(), ReconcileError> {
    let secret = render_ca_secret(kafka, name, secret_type_label, data, annotations)?;
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    secret_api
        .patch(name, &params, &Patch::Apply(&secret))
        .await?;
    Ok(())
}

/// Executes a [`CaRotationPlan`] against the live Secrets and builds the
/// outcome.
///
/// For `NoOp` and `Refuse`, the trust bundle is the raw stored PEM. The
/// config hash of the path without a rotation therefore stays
/// byte-identical. A plan that rotates gives a newly joined bundle.
async fn apply_ca_rotation(
    owner: (&Api<Secret>, &Kafka, WhichCa),
    names: (&str, &str, &str),
    state: &CaState,
    plan: CaRotationPlan,
    inp: &RotationInputs,
    raw_bundle_pem: &str,
) -> Result<CaReconcileOutcome, ReconcileError> {
    let (secret_api, kafka, which) = owner;
    let (key_name, cert_name, cn) = names;
    let now = inp.now;
    // Defaults carried over from the current state for plans that don't change a field.
    let mut bundle = state.bundle.clone();
    let mut key_pem = state.key_pem.clone();
    let mut cert_gen = state.cert_generation;
    let mut key_gen = state.key_generation;
    let mut phase = state.phase;
    let mut force_reissue = false;
    let mut raw_override: Option<String> = Some(raw_bundle_pem.to_string());
    let mut refused: Option<RefuseReason> = None;

    match plan {
        CaRotationPlan::NoOp => {}
        CaRotationPlan::Refuse(reason) => refused = Some(reason),
        CaRotationPlan::RenewCertSameKey => {
            let new_cert = match which {
                WhichCa::Cluster => {
                    crabka_security::ca::renew_cluster_ca(&key_pem, cn, whole_days(inp.validity))?
                }
                WhichCa::Clients => {
                    crabka_security::ca::renew_clients_ca(&key_pem, cn, whole_days(inp.validity))?
                }
            };
            let mut blocks = vec![normalize_block(&new_cert)];
            blocks.extend(prune_expired(&state.bundle, now));
            bundle = dedup_blocks(&blocks);
            cert_gen += CertGeneration(1);
            phase = CaPhase::Idle;
            patch_cert_bundle(secret_api, kafka, cert_name, &bundle, cert_gen, phase).await?;
            raw_override = None;
        }
        CaRotationPlan::StartKeyReplace => {
            let new = generate_cluster_ca(cn, whole_days(inp.validity))?;
            // Old signing cert stays first (still signing with the old key); the
            // new cert is appended as trust-only.
            let mut blocks = prune_expired(&state.bundle, now);
            blocks.push(normalize_block(&new.cert_pem));
            bundle = dedup_blocks(&blocks);
            phase = CaPhase::KeyReplaceTrust;
            // Stage the new key+cert in the key Secret alongside the active key.
            patch_secret(
                secret_api,
                kafka,
                key_name,
                SECRET_TYPE_CA_KEY,
                [
                    ("ca.key".to_string(), key_pem.clone()),
                    (NEXT_KEY.to_string(), new.key_pem),
                    (NEXT_CERT.to_string(), new.cert_pem),
                ]
                .into(),
                [(ANN_KEY_GENERATION.to_string(), key_gen.to_string())].into(),
            )
            .await?;
            patch_cert_bundle(secret_api, kafka, cert_name, &bundle, cert_gen, phase).await?;
            raw_override = None;
        }
        CaRotationPlan::PromoteNewKey => {
            let new_key = state
                .pending_key_pem
                .clone()
                .ok_or_else(|| ReconcileError::CertParse("promote without staged key".into()))?;
            let new_cert =
                normalize_block(state.pending_cert_pem.as_deref().ok_or_else(|| {
                    ReconcileError::CertParse("promote without staged cert".into())
                })?);
            // New cert to the front (new signer); keep the old certs as trust-only.
            let remaining: Vec<String> = state
                .bundle
                .iter()
                .filter(|b| **b != new_cert)
                .cloned()
                .collect();
            let mut blocks = vec![new_cert];
            blocks.extend(remaining);
            bundle = prune_expired(&dedup_blocks(&blocks), now);
            key_pem = new_key.clone();
            cert_gen += CertGeneration(1);
            key_gen += KeyGeneration(1);
            phase = CaPhase::KeyReplacePromote;
            force_reissue = matches!(which, WhichCa::Cluster);
            // Promote the key + drop the staged material.
            patch_secret(
                secret_api,
                kafka,
                key_name,
                SECRET_TYPE_CA_KEY,
                [("ca.key".to_string(), new_key)].into(),
                [(ANN_KEY_GENERATION.to_string(), key_gen.to_string())].into(),
            )
            .await?;
            patch_cert_bundle(secret_api, kafka, cert_name, &bundle, cert_gen, phase).await?;
            raw_override = None;
        }
        CaRotationPlan::PruneOldTrust => {
            bundle = if state.phase == CaPhase::KeyReplacePromote {
                // Key replacement complete: keep only the new signing cert.
                state.bundle.first().cloned().into_iter().collect()
            } else {
                prune_expired(&state.bundle, now)
            };
            phase = CaPhase::Idle;
            patch_cert_bundle(secret_api, kafka, cert_name, &bundle, cert_gen, phase).await?;
            raw_override = None;
        }
    }

    let signing_cert_pem = bundle.first().cloned().unwrap_or_default();
    let trust_bundle_pem = raw_override.unwrap_or_else(|| join_bundle(&bundle));
    let not_after = cert_not_after(&signing_cert_pem)?
        .format(&Rfc3339)
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    let (in_progress, reason, message) = rotation_condition(plan, phase, refused);

    Ok(CaReconcileOutcome {
        signing_material: CaMaterial {
            cert_pem: signing_cert_pem,
            key_pem,
        },
        trust_bundle_pem,
        not_after,
        generated: inp.generate,
        cert_generation: cert_gen,
        key_generation: key_gen,
        phase,
        trust_anchors: bundle.len(),
        force_reissue_leafs: force_reissue,
        rotation_in_progress: in_progress,
        rotation_reason: reason,
        rotation_message: message,
        refused,
    })
}

/// Normalizes one cert PEM to a block with one trailing newline.
///
/// The result matches the bundle blocks that [`split_pem_certs`]
/// produces.
fn normalize_block(cert_pem: &str) -> String {
    split_pem_certs(cert_pem)
        .into_iter()
        .next()
        .unwrap_or_else(|| format!("{}\n", cert_pem.trim()))
}

async fn patch_cert_bundle(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    cert_name: &str,
    bundle: &[String],
    cert_gen: CertGeneration,
    phase: CaPhase,
) -> Result<(), ReconcileError> {
    patch_secret(
        secret_api,
        kafka,
        cert_name,
        SECRET_TYPE_CA_CERT,
        [("ca.crt".to_string(), join_bundle(bundle))].into(),
        [
            (ANN_CERT_GENERATION.to_string(), cert_gen.to_string()),
            (ANN_ROTATION_PHASE.to_string(), phase.as_str().to_string()),
        ]
        .into(),
    )
    .await
}

/// Maps an executed plan and the phase that follows it to the
/// `CaRotation` condition surface.
fn rotation_condition(
    plan: CaRotationPlan,
    phase: CaPhase,
    refused: Option<RefuseReason>,
) -> (bool, &'static str, String) {
    if let Some(reason) = refused {
        return match reason {
            RefuseReason::Byo => (
                false,
                "ByoCaImmutable",
                "forced rotation ignored: BYO CA (generateCertificateAuthority=false)".into(),
            ),
            RefuseReason::ClientsCaKeyReplace => (
                false,
                "ClientsCaKeyReplaceUnsupported",
                "clients-CA key replacement is not supported in this release".into(),
            ),
        };
    }
    match plan {
        CaRotationPlan::RenewCertSameKey => (
            true,
            "RenewingCert",
            "re-signing the CA cert (same key)".into(),
        ),
        CaRotationPlan::StartKeyReplace => (
            true,
            "DistributingTrust",
            "new CA generated; rolling to distribute the trust bundle".into(),
        ),
        CaRotationPlan::PromoteNewKey => (
            true,
            "PromotingKey",
            "promoting the new CA key and reissuing broker certs".into(),
        ),
        CaRotationPlan::NoOp => match phase {
            CaPhase::KeyReplaceTrust => (
                true,
                "DistributingTrust",
                "waiting for the trust-bundle roll to converge".into(),
            ),
            CaPhase::KeyReplacePromote => (
                true,
                "PromotingKey",
                "waiting for the new-key roll to converge".into(),
            ),
            CaPhase::Idle => (false, "Idle", "no rotation in progress".into()),
        },
        CaRotationPlan::PruneOldTrust => (
            false,
            "Idle",
            "rotation complete; pruned old trust anchors".into(),
        ),
        CaRotationPlan::Refuse(_) => unreachable!("handled above"),
    }
}

fn render_ca_secret(
    kafka: &Kafka,
    name: &str,
    secret_type_label: &str,
    data: BTreeMap<String, String>,
    extra_annotations: BTreeMap<String, String>,
) -> Result<Secret, ReconcileError> {
    let cluster = kafka.name_any();
    let mut labels = BTreeMap::new();
    labels.insert("crabka.io/secret-type".into(), secret_type_label.into());
    labels.insert("crabka.io/cluster".into(), cluster);
    let mut annotations = BTreeMap::new();
    annotations.insert("crabka.io/strictly-operator-managed".into(), "true".into());
    annotations.extend(extra_annotations);
    let data: BTreeMap<String, ByteString> = data
        .into_iter()
        .map(|(k, v)| (k, ByteString(v.into_bytes())))
        .collect();
    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: kafka.meta().namespace.clone(),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: Some(vec![owner_ref::<Kafka>(kafka)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    })
}

/// Reads a monotonic generation annotation from a Secret.
///
/// The result is `0` when the annotation is absent and when it does not
/// parse.
fn read_generation(secret: &Secret, ann: &str) -> u64 {
    secret
        .meta()
        .annotations
        .as_ref()
        .and_then(|a| a.get(ann))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Reads the rotation phase from the cert Secret. The result is `Idle`
/// when the annotation is absent.
fn read_phase(cert_secret: &Secret) -> CaPhase {
    cert_secret
        .meta()
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANN_ROTATION_PHASE))
        .map_or(CaPhase::Idle, |v| CaPhase::parse(v))
}

// ---------------------------------------------------------------------------
// Broker keystore
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct BrokerKeystoreStatus {
    pub issued: Vec<i32>,
    pub reused: Vec<i32>,
    pub pruned: Vec<i32>,
}

/// Per-broker cert request.
///
/// The caller supplies the CN and the SAN list. They must match the names
/// that peer brokers dial, which are the real pod FQDNs from the
/// `StatefulSet` name `{cluster}-{pool_name}` and the ordinal.
#[derive(Debug, Clone)]
pub(crate) struct BrokerCertRequest {
    pub broker_id: i32,
    pub cn: String,
    pub sans: Vec<SubjectAltName>,
    /// Extra SANs for external listeners, for example `NodePort` node
    /// addresses and `LoadBalancer` IPs. The list is empty when no
    /// external TLS listener is configured.
    pub extra_sans: Vec<SubjectAltName>,
}

pub(crate) async fn ensure_broker_keystore(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    requests: &[BrokerCertRequest],
    cluster_ca: &CaMaterial,
    force_reissue: bool,
) -> Result<BrokerKeystoreStatus, ReconcileError> {
    let cluster = kafka.name_any();
    let namespace = kafka.meta().namespace.clone().unwrap_or_default();
    let name = broker_keystore_name(&cluster);

    let validity = days(
        kafka
            .spec
            .cluster_ca
            .as_ref()
            .map_or(365, |c| c.validity_days),
    );

    let existing = secret_api.get_opt(&name).await?;
    let mut data: BTreeMap<String, ByteString> = existing
        .as_ref()
        .and_then(|s| s.data.clone())
        .unwrap_or_default();

    let mut issued = Vec::new();
    let mut reused = Vec::new();

    for req in requests {
        let id = req.broker_id;
        let crt_key = format!("{id}.crt");
        let key_key = format!("{id}.key");
        let digest_key = format!("{id}.sans-digest");

        let requested_digest = compute_san_digest(&req.sans, &req.extra_sans);

        let has_cert = data.contains_key(&crt_key) && data.contains_key(&key_key);
        let stored_digest = data.get(&digest_key).and_then(|b| {
            std::str::from_utf8(&b.0)
                .ok()
                .map(std::borrow::ToOwned::to_owned)
        });

        let needs_reissue = force_reissue
            || !has_cert
            || stored_digest.is_none()
            || stored_digest.as_deref() != Some(&requested_digest);

        if !needs_reissue {
            reused.push(id);
            continue;
        }
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            &req.cn,
            &req.sans,
            &req.extra_sans,
            whole_days(validity),
        )?;
        data.insert(crt_key, ByteString(leaf.cert_pem.into_bytes()));
        data.insert(key_key, ByteString(leaf.key_pem.into_bytes()));
        data.insert(digest_key, ByteString(requested_digest.into_bytes()));
        issued.push(id);
    }

    let want_keys: std::collections::HashSet<String> = requests
        .iter()
        .flat_map(|req| {
            let id = req.broker_id;
            [
                format!("{id}.crt"),
                format!("{id}.key"),
                format!("{id}.sans-digest"),
            ]
        })
        .collect();
    let mut pruned_ids = std::collections::BTreeSet::new();
    data.retain(|k, _| {
        if want_keys.contains(k) {
            true
        } else if let Some((id_str, _)) = k.split_once('.')
            && let Ok(id) = id_str.parse::<i32>()
        {
            pruned_ids.insert(id);
            false
        } else {
            true
        }
    });
    let pruned: Vec<i32> = pruned_ids.into_iter().collect();

    let mut labels = BTreeMap::new();
    labels.insert(
        "crabka.io/secret-type".into(),
        SECRET_TYPE_BROKER_KEYSTORE.into(),
    );
    labels.insert("crabka.io/cluster".into(), cluster.clone());

    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref::<Kafka>(kafka)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    };

    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    secret_api
        .patch(&name, &params, &Patch::Apply(&secret))
        .await?;

    Ok(BrokerKeystoreStatus {
        issued,
        reused,
        pruned,
    })
}

// ---------------------------------------------------------------------------
// SAN-list digest
// ---------------------------------------------------------------------------

/// SHA-256 digest of the SAN list in canonical form, sorted and without
/// duplicates.
///
/// The operator compares this digest against the cert in the Secret. A
/// different digest means that the SAN list of the broker changed, and the
/// operator then issues a new cert.
#[must_use]
pub fn compute_san_digest(base_sans: &[SubjectAltName], extras: &[SubjectAltName]) -> String {
    use std::fmt::Write as _;

    use sha2::{Digest, Sha256};
    let mut all: Vec<&SubjectAltName> = base_sans.iter().chain(extras.iter()).collect();
    all.sort();
    all.dedup();
    let mut h = Sha256::new();
    for s in all {
        match s {
            SubjectAltName::Dns(d) => {
                h.update(b"DNS:");
                h.update(d.as_bytes());
            }
            SubjectAltName::Ip(ip) => {
                h.update(b"IP:");
                h.update(ip.to_string().as_bytes());
            }
        }
        h.update(b"\n");
    }
    let result = h.finalize();
    result.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

// ---------------------------------------------------------------------------
// Renewal predicate
// ---------------------------------------------------------------------------

/// Converts a span from the `time` crate into a [`Time`] extent.
///
/// The conversion keeps the sign. A `notAfter` that is already before
/// `now` therefore stays negative, and it still compares as inside the
/// renewal window.
fn span_as_time(span: time::Duration) -> Time {
    Time::from_secs_f64(span.as_seconds_f64())
}

/// Converts a certificate lifetime into whole days, which is the unit that
/// `crabka_security::ca` uses.
///
/// The CRD carries these values as `u32` days, so the round-trip is exact
/// for every configured value. A negative extent becomes zero, and a very
/// large one saturates.
fn whole_days(extent: Time) -> u32 {
    let value = extent.get::<day>().round();
    if value <= 0.0 {
        0
    } else {
        value.to_u32().unwrap_or(u32::MAX)
    }
}

/// Reports whether `cert_pem` is inside its renewal window, that is,
/// whether it expires within `renewal` of `now`.
///
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub fn renew_if_expiring(
    cert_pem: &str,
    renewal: Time,
    now: OffsetDateTime,
) -> Result<bool, ReconcileError> {
    let not_after = cert_not_after(cert_pem)?;
    Ok(span_as_time(not_after - now) <= renewal)
}

// ---------------------------------------------------------------------------
// CronJob entrypoint: run_renewal_check
// ---------------------------------------------------------------------------

use k8s_openapi::api::core::v1::Event;
use kube::api::{ListParams, PostParams};

#[tracing::instrument(level = "info", skip_all, fields(namespace = ?namespace), err)]
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn run_renewal_check(
    client: kube::Client,
    namespace: Option<&str>,
) -> Result<(), ReconcileError> {
    let kafkas: Api<Kafka> = if let Some(ns) = namespace {
        Api::namespaced(client.clone(), ns)
    } else {
        Api::all(client.clone())
    };
    let list = kafkas.list(&ListParams::default()).await?;
    for kafka in list {
        if let Err(e) = renew_one(&client, &kafka).await {
            tracing::error!(
                cluster = %kafka.name_any(),
                error = %e,
                "ca-renewal-check: cluster failed"
            );
        }
    }
    Ok(())
}

#[tracing::instrument(level = "debug", skip_all, fields(cluster = %kafka.name_any()), err)]
async fn renew_one(client: &kube::Client, kafka: &Kafka) -> Result<(), ReconcileError> {
    let ns = kafka.meta().namespace.clone().unwrap_or_default();
    let cluster = kafka.name_any();
    let secret_api: Api<Secret> = Api::namespaced(client.clone(), &ns);
    let now = OffsetDateTime::now_utc();

    let cluster_ca = read_existing_ca(&secret_api, &cluster, WhichCa::Cluster).await?;
    let clients_ca = read_existing_ca(&secret_api, &cluster, WhichCa::Clients).await?;

    let cluster_ca_spec = kafka.spec.cluster_ca.clone().unwrap_or_default();
    let clients_ca_spec = kafka.spec.clients_ca.clone().unwrap_or_default();

    flag_ca_if_expiring(
        client,
        kafka,
        &cluster_ca.cert_pem,
        &cluster_ca_spec,
        WhichCa::Cluster,
        now,
    )
    .await?;
    flag_ca_if_expiring(
        client,
        kafka,
        &clients_ca.cert_pem,
        &clients_ca_spec,
        WhichCa::Clients,
        now,
    )
    .await?;

    renew_broker_leafs(
        client,
        kafka,
        &cluster_ca,
        days(cluster_ca_spec.renewal_days),
        days(cluster_ca_spec.validity_days),
        now,
    )
    .await?;
    Ok(())
}

async fn read_existing_ca(
    secret_api: &Api<Secret>,
    cluster: &str,
    which: WhichCa,
) -> Result<CaMaterial, ReconcileError> {
    let (key_name, cert_name) = match which {
        WhichCa::Cluster => (cluster_ca_key_name(cluster), cluster_ca_cert_name(cluster)),
        WhichCa::Clients => (clients_ca_key_name(cluster), clients_ca_cert_name(cluster)),
    };
    let key_secret =
        secret_api
            .get_opt(&key_name)
            .await?
            .ok_or_else(|| ReconcileError::CaSecretMissing {
                name: key_name.clone(),
            })?;
    let cert_secret =
        secret_api
            .get_opt(&cert_name)
            .await?
            .ok_or_else(|| ReconcileError::CaSecretMissing {
                name: cert_name.clone(),
            })?;
    let key_pem = read_pem_key(&key_secret, "ca.key")
        .ok_or_else(|| ReconcileError::CertParse(format!("{key_name} ca.key unreadable")))?;
    let cert_pem = read_pem_key(&cert_secret, "ca.crt")
        .ok_or_else(|| ReconcileError::CertParse(format!("{cert_name} ca.crt unreadable")))?;
    Ok(CaMaterial { cert_pem, key_pem })
}

async fn flag_ca_if_expiring(
    client: &kube::Client,
    kafka: &Kafka,
    ca_cert_pem: &str,
    spec: &CertificateAuthority,
    which: WhichCa,
    now: OffsetDateTime,
) -> Result<(), ReconcileError> {
    if !renew_if_expiring(ca_cert_pem, days(spec.renewal_days), now)? {
        return Ok(());
    }
    let ns = kafka.meta().namespace.clone().unwrap_or_default();
    if spec.generate_certificate_authority {
        // Operator-managed CA within renewalDays: nudge the reconciler to run a
        // same-key renewal. The reconciler owns the actual rotation
        // (it has the rollout machinery); the CronJob only stamps a one-shot
        // annotation. Idempotent — skip if already stamped so repeated CronJob
        // runs don't churn the CR.
        let already = kafka
            .meta()
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key(ANN_RENEW_AFTER));
        if already {
            return Ok(());
        }
        let kafka_api: Api<Kafka> = Api::namespaced(client.clone(), &ns);
        let stamp = now
            .format(&Rfc3339)
            .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
        let patch = serde_json::json!({
            "metadata": { "annotations": { ANN_RENEW_AFTER: stamp } }
        });
        kafka_api
            .patch(
                &kafka.name_any(),
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await?;
        emit_event(
            client,
            &ns,
            kafka,
            EventDetails {
                type_: "Normal",
                reason: "CaRenewalScheduled",
                message: &format!(
                    "CA {} is within renewalDays; scheduled a same-key renewal on the next reconcile",
                    which.condition_name()
                ),
                generate_name: "crabka-ca-renewal-",
                action: "RenewalCheck",
                reporting_component: "crabka-operator/ca-renewal-check",
            },
        )
        .await?;
    } else {
        // BYO CA: event only, no status condition (spec: BYO emits only the Event).
        emit_event(
            client,
            &ns,
            kafka,
            EventDetails {
                type_: "Warning",
                reason: "ByoCaExpiringSoon",
                message: &format!(
                    "CA {} is expiring within renewalDays; \
                     rotation is the cluster admin's responsibility (BYO)",
                    which.condition_name()
                ),
                generate_name: "crabka-ca-renewal-",
                action: "RenewalCheck",
                reporting_component: "crabka-operator/ca-renewal-check",
            },
        )
        .await?;
    }
    Ok(())
}

/// Extracts the CN from the subject and the SAN list from the SAN
/// extension of an existing broker leaf cert PEM.
///
/// `renew_broker_leafs` uses this function, so that the renewal `CronJob`
/// keeps the exact identity that the reconciler issued. To build that
/// identity again from nothing would be fragile, because the `CronJob`
/// cannot read the pool names and the ordinals.
fn read_existing_cn_and_sans(
    cert_pem: &str,
) -> Result<(String, Vec<SubjectAltName>), ReconcileError> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    use x509_parser::{
        extensions::GeneralName,
        prelude::{FromDer, X509Certificate},
    };

    let der = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .next()
        .ok_or_else(|| ReconcileError::CertParse("no PEM block in broker cert".into()))?
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?;

    // Extract CN from subject.
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|attr| attr.as_str().ok())
        .ok_or_else(|| ReconcileError::CertParse("broker cert has no CN in subject".into()))?
        .to_string();

    // Extract SANs from the SubjectAltName extension.
    let sans: Vec<SubjectAltName> = cert
        .subject_alternative_name()
        .map_err(|e| ReconcileError::CertParse(e.to_string()))?
        .map(|san_ext| {
            san_ext
                .value
                .general_names
                .iter()
                .filter_map(|gn| match gn {
                    GeneralName::DNSName(s) => Some(SubjectAltName::Dns(s.to_string())),
                    GeneralName::IPAddress(bytes) => {
                        // x509_parser gives raw bytes: 4 bytes = IPv4, 16 = IPv6.
                        let bytes: &[u8] = bytes;
                        match bytes.len() {
                            4 => {
                                let arr: [u8; 4] = bytes.try_into().ok()?;
                                Some(SubjectAltName::Ip(IpAddr::V4(arr.into())))
                            }
                            16 => {
                                let arr: [u8; 16] = bytes.try_into().ok()?;
                                Some(SubjectAltName::Ip(IpAddr::V6(arr.into())))
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();

    Ok((cn, sans))
}

async fn renew_broker_leafs(
    client: &kube::Client,
    kafka: &Kafka,
    cluster_ca: &CaMaterial,
    renewal: Time,
    validity: Time,
    now: OffsetDateTime,
) -> Result<(), ReconcileError> {
    let ns = kafka.meta().namespace.clone().unwrap_or_default();
    let cluster = kafka.name_any();
    let secret_api: Api<Secret> = Api::namespaced(client.clone(), &ns);
    let name = broker_keystore_name(&cluster);
    let Some(mut secret) = secret_api.get_opt(&name).await? else {
        return Ok(());
    };
    let Some(mut data) = secret.data.take() else {
        return Ok(());
    };

    let mut renewed_ids = Vec::new();
    let crt_keys: Vec<String> = data
        .keys()
        .filter(|k| {
            std::path::Path::new(k.as_str())
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("crt"))
        })
        .cloned()
        .collect();
    for crt_key in crt_keys {
        let Some((id_str, _)) = crt_key.split_once('.') else {
            continue;
        };
        let Ok(id) = id_str.parse::<i32>() else {
            continue;
        };
        let Some(cert_bytes) = data.get(&crt_key) else {
            continue;
        };
        let Ok(cert_pem) = std::str::from_utf8(&cert_bytes.0) else {
            continue;
        };
        if !renew_if_expiring(cert_pem, renewal, now)? {
            continue;
        }
        let (cn, sans) = match read_existing_cn_and_sans(cert_pem) {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(
                    cluster = %cluster,
                    broker_id = id,
                    error = %e,
                    "ca-renewal-check: could not parse CN/SANs from existing broker cert; skipping renewal"
                );
                continue;
            }
        };
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            &cn,
            &sans,
            &[],
            whole_days(validity),
        )?;
        data.insert(crt_key.clone(), ByteString(leaf.cert_pem.into_bytes()));
        data.insert(format!("{id}.key"), ByteString(leaf.key_pem.into_bytes()));
        let digest = compute_san_digest(&sans, &[]);
        data.insert(format!("{id}.sans-digest"), ByteString(digest.into_bytes()));
        renewed_ids.push(id);
    }
    if renewed_ids.is_empty() {
        return Ok(());
    }
    secret.data = Some(data);
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    secret_api
        .patch(&name, &params, &Patch::Apply(&secret))
        .await?;

    for id in renewed_ids {
        emit_event(
            client,
            &ns,
            kafka,
            EventDetails {
                type_: "Normal",
                reason: "BrokerCertRenewed",
                message: &format!("broker={id} reissued by ca-renewal-check"),
                generate_name: "crabka-ca-renewal-",
                action: "RenewalCheck",
                reporting_component: "crabka-operator/ca-renewal-check",
            },
        )
        .await?;
    }
    Ok(())
}

pub(crate) struct EventDetails<'a> {
    pub type_: &'a str,
    pub reason: &'a str,
    pub message: &'a str,
    pub generate_name: &'a str,
    pub action: &'a str,
    pub reporting_component: &'a str,
}

pub(crate) async fn emit_event(
    client: &kube::Client,
    namespace: &str,
    kafka: &Kafka,
    details: EventDetails<'_>,
) -> Result<(), ReconcileError> {
    use k8s_openapi::{apimachinery::pkg::apis::meta::v1::MicroTime, jiff::Timestamp};

    let EventDetails {
        type_,
        reason,
        message,
        generate_name,
        action,
        reporting_component,
    } = details;
    let now = Timestamp::now();
    let event = Event {
        metadata: ObjectMeta {
            generate_name: Some(generate_name.into()),
            namespace: Some(namespace.into()),
            ..Default::default()
        },
        type_: Some(type_.into()),
        reason: Some(reason.into()),
        message: Some(message.into()),
        involved_object: k8s_openapi::api::core::v1::ObjectReference {
            api_version: Some("crabka.io/v1alpha1".into()),
            kind: Some("Kafka".into()),
            name: Some(kafka.name_any()),
            namespace: Some(namespace.into()),
            uid: kafka.meta().uid.clone(),
            ..Default::default()
        },
        event_time: Some(MicroTime(now)),
        action: Some(action.into()),
        reporting_component: Some(reporting_component.into()),
        reporting_instance: Some(
            std::env::var("POD_NAME").unwrap_or_else(|_| "crabka-operator-renewal".into()),
        ),
        ..Default::default()
    };
    let api: Api<Event> = Api::namespaced(client.clone(), namespace);
    api.create(&PostParams::default(), &event).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_security::ca::{generate_clients_ca, generate_cluster_ca, issue_user_cert};

    use super::*;

    /// A CA that the operator generates with `validity_days = 30` must
    /// have a `notAfter` inside [29, 31] days of now. The range allows one
    /// second of clock skew in CI.
    #[test]
    fn ca_validity_days_is_honored() {
        use rustls::pki_types::{CertificateDer, pem::PemObject};
        use x509_parser::prelude::{FromDer, X509Certificate};

        let ca = generate_cluster_ca("test-cluster-ca", 30).expect("CA");
        let der = CertificateDer::pem_slice_iter(ca.cert_pem.as_bytes())
            .next()
            .expect("PEM block")
            .expect("valid PEM");
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("valid DER");
        let not_after = OffsetDateTime::from_unix_timestamp(cert.validity().not_after.timestamp())
            .expect("valid timestamp");
        let now = OffsetDateTime::now_utc();
        let days_remaining = (not_after - now).whole_days();
        assert!(
            (29..=31).contains(&days_remaining),
            "expected ~30 days remaining, got {days_remaining}"
        );
    }

    #[test]
    fn renews_when_within_window() {
        let ca = generate_clients_ca("c1", 365).expect("CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 5).expect("leaf");
        let now = OffsetDateTime::now_utc();
        assert!(renew_if_expiring(&user.cert_pem, days(30), now).expect("predicate"));
    }

    #[test]
    fn does_not_renew_when_comfortably_in_future() {
        let ca = generate_clients_ca("c1", 365).expect("CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 365).expect("leaf");
        let now = OffsetDateTime::now_utc();
        assert!(!renew_if_expiring(&user.cert_pem, days(30), now).expect("predicate"));
    }

    #[test]
    fn renews_when_already_past() {
        let ca = generate_clients_ca("c1", 365).expect("CA");
        let user = issue_user_cert(&ca.cert_pem, &ca.key_pem, "alice", 1).expect("leaf");
        let now = OffsetDateTime::now_utc() + time::Duration::days(10);
        assert!(renew_if_expiring(&user.cert_pem, days(30), now).expect("predicate"));
    }
}

#[cfg(test)]
mod reissue_tests {
    use assert2::assert;
    use crabka_security::ca::SubjectAltName;

    use super::compute_san_digest;

    #[test]
    fn san_digest_changes_when_extras_differ() {
        let base = vec![SubjectAltName::Dns("internal.svc".into())];
        let no_extras = compute_san_digest(&base, &[]);
        let with_extras =
            compute_san_digest(&base, &[SubjectAltName::Dns("broker-0.example.com".into())]);
        assert!(no_extras != with_extras);
    }

    #[test]
    fn san_digest_stable_for_same_inputs_in_different_order() {
        let a = vec![
            SubjectAltName::Dns("a.example.com".into()),
            SubjectAltName::Dns("b.example.com".into()),
        ];
        let b = vec![
            SubjectAltName::Dns("b.example.com".into()),
            SubjectAltName::Dns("a.example.com".into()),
        ];
        assert!(compute_san_digest(&a, &[]) == compute_san_digest(&b, &[]));
    }

    #[test]
    fn san_digest_dedupes_overlap_between_base_and_extras() {
        let base = vec![SubjectAltName::Dns("internal.svc".into())];
        let extras = vec![SubjectAltName::Dns("internal.svc".into())];
        let single = compute_san_digest(&base, &[]);
        let with_dup_extra = compute_san_digest(&base, &extras);
        assert!(
            single == with_dup_extra,
            "duplicate extras should not change digest"
        );
    }
}

#[cfg(test)]
mod san_tests {
    use assert2::assert;
    use crabka_security::ca::{SubjectAltName, generate_cluster_ca, issue_broker_cert};
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    use x509_parser::{
        extensions::GeneralName,
        prelude::{FromDer, X509Certificate},
    };

    fn parse_cert_sans(cert_pem: &str) -> Vec<String> {
        let der = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
            .next()
            .expect("PEM block")
            .expect("valid PEM");
        let (_, cert) = X509Certificate::from_der(der.as_ref()).expect("valid DER");
        cert.subject_alternative_name()
            .expect("SAN parse")
            .map(|san_ext| {
                san_ext
                    .value
                    .general_names
                    .iter()
                    .map(|gn| match gn {
                        GeneralName::DNSName(s) => format!("DNS:{s}"),
                        GeneralName::IPAddress(bytes) => {
                            let bytes: &[u8] = bytes;
                            match bytes.len() {
                                4 => {
                                    let arr: [u8; 4] = bytes.try_into().expect("4 bytes");
                                    format!("IP:{}", std::net::IpAddr::V4(arr.into()))
                                }
                                16 => {
                                    let arr: [u8; 16] = bytes.try_into().expect("16 bytes");
                                    format!("IP:{}", std::net::IpAddr::V6(arr.into()))
                                }
                                _ => "IP:unknown".to_string(),
                            }
                        }
                        other => format!("{other:?}"),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn issue_broker_cert_includes_extra_sans_in_leaf() {
        let cluster_ca = generate_cluster_ca("test-san-ca", 365).expect("test CA");
        let extra = vec![
            SubjectAltName::Dns("broker-0.example.com".into()),
            SubjectAltName::Ip("203.0.113.10".parse().unwrap()),
        ];
        let internal_sans = vec![SubjectAltName::Dns("internal.svc".into())];
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            "broker-0",
            &internal_sans,
            &extra,
            365,
        )
        .unwrap();
        let parsed_sans = parse_cert_sans(&leaf.cert_pem);
        for want in [
            "DNS:internal.svc",
            "DNS:broker-0.example.com",
            "IP:203.0.113.10",
        ] {
            assert!(parsed_sans.iter().any(|s| s == want), "missing {want:?}");
        }
    }

    #[test]
    fn issue_broker_cert_empty_extra_sans_yields_base_sans_only() {
        let cluster_ca = generate_cluster_ca("test-san-ca", 365).expect("test CA");
        let internal_sans = vec![SubjectAltName::Dns("internal.svc".into())];
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            "broker-0",
            &internal_sans,
            &[],
            365,
        )
        .unwrap();
        let parsed = parse_cert_sans(&leaf.cert_pem);
        assert!(parsed.len() == 1);
        assert!(parsed[0] == "DNS:internal.svc");
    }

    // Round-trip: issue a leaf with a known CN and mixed DNS/IP SANs, then pull
    // them back out with `read_existing_cn_and_sans`. Pins the actual parsed CN
    // and SAN list so a body-stub mutant (Ok(("", vec![])) / Ok(("xyzzy", vec![])))
    // is caught — the CN must equal the issued name and the SANs must be preserved.
    #[test]
    fn read_existing_cn_and_sans_round_trips_cn_and_sans() {
        use super::read_existing_cn_and_sans;

        let cluster_ca = generate_cluster_ca("test-san-ca", 365).expect("test CA");
        let internal_sans = vec![SubjectAltName::Dns("internal.svc".into())];
        let extra = vec![
            SubjectAltName::Dns("broker-0.example.com".into()),
            SubjectAltName::Ip("203.0.113.10".parse().unwrap()),
        ];
        let leaf = issue_broker_cert(
            &cluster_ca.cert_pem,
            &cluster_ca.key_pem,
            "broker-0",
            &internal_sans,
            &extra,
            365,
        )
        .unwrap();

        let (cn, sans) = read_existing_cn_and_sans(&leaf.cert_pem).expect("parse leaf");
        assert!(cn == "broker-0");
        for want in [
            SubjectAltName::Dns("internal.svc".into()),
            SubjectAltName::Dns("broker-0.example.com".into()),
            SubjectAltName::Ip("203.0.113.10".parse().unwrap()),
        ] {
            assert!(sans.contains(&want), "missing {want:?} in {sans:?}");
        }
    }
}

#[cfg(test)]
mod rotation_tests {
    use assert2::assert;
    use crabka_security::ca::{generate_cluster_ca, renew_cluster_ca};

    use super::*;

    fn ca_cert(cn: &str, days: u32) -> String {
        generate_cluster_ca(cn, days).expect("CA").cert_pem
    }

    fn state(bundle: Vec<String>, key: &str, phase: CaPhase) -> CaState {
        CaState {
            bundle,
            key_pem: key.to_string(),
            pending_key_pem: None,
            pending_cert_pem: None,
            cert_generation: CertGeneration(0),
            key_generation: KeyGeneration(0),
            phase,
        }
    }

    fn inputs(generate: bool, which: WhichCa) -> RotationInputs {
        RotationInputs {
            generate,
            validity: days(365),
            renewal: days(30),
            force: ForceRotation::None,
            rollout_converged: false,
            now: OffsetDateTime::now_utc(),
            which,
        }
    }

    // --- bundle helpers -----------------------------------------------------

    #[test]
    fn split_join_round_trip_two_certs() {
        let a = ca_cert("a", 365);
        let b = ca_cert("b", 365);
        let bundle = format!("{a}{b}");
        let blocks = split_pem_certs(&bundle);
        assert!(blocks.len() == 2);
        assert!(blocks[0].contains("BEGIN CERTIFICATE"));
        // join is the concatenation of normalized blocks; re-splitting is stable.
        let rejoined = join_bundle(&blocks);
        assert!(split_pem_certs(&rejoined).len() == 2);
    }

    #[test]
    fn dedup_blocks_removes_duplicates_keeps_order() {
        let a = normalize_block(&ca_cert("a", 365));
        let b = normalize_block(&ca_cert("b", 365));
        let out = dedup_blocks(&[a.clone(), b.clone(), a.clone()]);
        assert!(out == vec![a, b]);
    }

    #[test]
    fn prune_expired_keeps_signing_block_even_if_expired() {
        let signing = normalize_block(&ca_cert("sign", 10));
        let trust = normalize_block(&ca_cert("trust", 10));
        // now is 100 days out → both certs expired, but the signing block (index
        // 0) is never dropped.
        let now = OffsetDateTime::now_utc() + time::Duration::days(100);
        let out = prune_expired(&[signing.clone(), trust], now);
        assert!(out == vec![signing]);
    }

    #[test]
    fn prune_expired_drops_only_expired_trust_anchor() {
        let signing = normalize_block(&ca_cert("sign", 365));
        let fresh = normalize_block(&ca_cert("fresh", 365));
        let stale = normalize_block(&ca_cert("stale", 20));
        let now = OffsetDateTime::now_utc() + time::Duration::days(100);
        let out = prune_expired(&[signing.clone(), fresh.clone(), stale], now);
        assert!(out == vec![signing, fresh]);
    }

    // --- planner: BYO -------------------------------------------------------

    #[test]
    fn byo_never_rotates() {
        let s = state(
            vec![normalize_block(&ca_cert("c1-cluster-ca", 365))],
            "k",
            CaPhase::Idle,
        );
        assert!(plan_ca_rotation(&s, &inputs(false, WhichCa::Cluster)) == CaRotationPlan::NoOp);
    }

    #[test]
    fn byo_force_is_refused() {
        let s = state(
            vec![normalize_block(&ca_cert("c1-cluster-ca", 365))],
            "k",
            CaPhase::Idle,
        );
        let mut inp = inputs(false, WhichCa::Cluster);
        inp.force = ForceRotation::ReplaceKey;
        assert!(plan_ca_rotation(&s, &inp) == CaRotationPlan::Refuse(RefuseReason::Byo));
        let mut inp2 = inputs(false, WhichCa::Cluster);
        inp2.force = ForceRotation::RenewCertificate;
        assert!(plan_ca_rotation(&s, &inp2) == CaRotationPlan::Refuse(RefuseReason::Byo));
    }

    // --- planner: idle ------------------------------------------------------

    #[test]
    fn idle_not_due_is_noop() {
        let s = state(
            vec![normalize_block(&ca_cert("c1-cluster-ca", 365))],
            "k",
            CaPhase::Idle,
        );
        assert!(plan_ca_rotation(&s, &inputs(true, WhichCa::Cluster)) == CaRotationPlan::NoOp);
    }

    #[test]
    fn idle_within_renewal_window_renews_same_key() {
        // Signing cert with 20 days left, renewalDays=30 → due.
        let s = state(
            vec![normalize_block(&ca_cert("c1-cluster-ca", 20))],
            "k",
            CaPhase::Idle,
        );
        assert!(
            plan_ca_rotation(&s, &inputs(true, WhichCa::Cluster))
                == CaRotationPlan::RenewCertSameKey
        );
    }

    #[test]
    fn idle_force_renew_renews_even_when_not_due() {
        let s = state(
            vec![normalize_block(&ca_cert("c1-cluster-ca", 365))],
            "k",
            CaPhase::Idle,
        );
        let mut inp = inputs(true, WhichCa::Cluster);
        inp.force = ForceRotation::RenewCertificate;
        assert!(plan_ca_rotation(&s, &inp) == CaRotationPlan::RenewCertSameKey);
    }

    #[test]
    fn idle_force_replace_starts_key_replace_on_cluster_ca() {
        let s = state(
            vec![normalize_block(&ca_cert("c1-cluster-ca", 365))],
            "k",
            CaPhase::Idle,
        );
        let mut inp = inputs(true, WhichCa::Cluster);
        inp.force = ForceRotation::ReplaceKey;
        assert!(plan_ca_rotation(&s, &inp) == CaRotationPlan::StartKeyReplace);
    }

    #[test]
    fn idle_force_replace_refused_on_clients_ca() {
        let s = state(
            vec![normalize_block(&ca_cert("c1-clients-ca", 365))],
            "k",
            CaPhase::Idle,
        );
        let mut inp = inputs(true, WhichCa::Clients);
        inp.force = ForceRotation::ReplaceKey;
        assert!(
            plan_ca_rotation(&s, &inp) == CaRotationPlan::Refuse(RefuseReason::ClientsCaKeyReplace)
        );
    }

    #[test]
    fn idle_with_expired_trust_anchor_prunes() {
        let signing = normalize_block(&ca_cert("c1-cluster-ca", 365));
        let stale = normalize_block(&ca_cert("old", 50));
        let s = state(vec![signing, stale], "k", CaPhase::Idle);
        let mut inp = inputs(true, WhichCa::Cluster);
        // 100 days out: signing still valid (not due), trust anchor expired.
        inp.now = OffsetDateTime::now_utc() + time::Duration::days(100);
        assert!(plan_ca_rotation(&s, &inp) == CaRotationPlan::PruneOldTrust);
    }

    // --- planner: staged phases --------------------------------------------

    #[test]
    fn trust_phase_waits_until_converged() {
        let s = state(
            vec![normalize_block(&ca_cert("c1-cluster-ca", 365))],
            "k",
            CaPhase::KeyReplaceTrust,
        );
        let mut inp = inputs(true, WhichCa::Cluster);
        inp.rollout_converged = false;
        assert!(plan_ca_rotation(&s, &inp) == CaRotationPlan::NoOp);
        inp.rollout_converged = true;
        assert!(plan_ca_rotation(&s, &inp) == CaRotationPlan::PromoteNewKey);
    }

    #[test]
    fn promote_phase_waits_then_prunes() {
        let s = state(
            vec![normalize_block(&ca_cert("c1-cluster-ca", 365))],
            "k",
            CaPhase::KeyReplacePromote,
        );
        let mut inp = inputs(true, WhichCa::Cluster);
        inp.rollout_converged = false;
        assert!(plan_ca_rotation(&s, &inp) == CaRotationPlan::NoOp);
        inp.rollout_converged = true;
        assert!(plan_ca_rotation(&s, &inp) == CaRotationPlan::PruneOldTrust);
    }

    #[test]
    fn staged_phase_ignores_force_until_complete() {
        // A force-replace mid-flight does not restart the machine.
        let s = state(
            vec![normalize_block(&ca_cert("c1-cluster-ca", 365))],
            "k",
            CaPhase::KeyReplaceTrust,
        );
        let mut inp = inputs(true, WhichCa::Cluster);
        inp.force = ForceRotation::ReplaceKey;
        inp.rollout_converged = false;
        assert!(plan_ca_rotation(&s, &inp) == CaRotationPlan::NoOp);
    }

    // --- same-key renewal preserves leaf chaining ---------------------------

    #[test]
    fn renew_same_key_keeps_leaf_chaining() {
        use rustls::pki_types::{CertificateDer, pem::PemObject};
        use x509_parser::prelude::{FromDer, X509Certificate};

        let ca = generate_cluster_ca("c1-cluster-ca", 20).expect("CA");
        let leaf = crabka_security::ca::issue_broker_cert(
            &ca.cert_pem,
            &ca.key_pem,
            "c1-broker-0",
            &[crabka_security::ca::SubjectAltName::Dns(
                "c1-broker-0".into(),
            )],
            &[],
            20,
        )
        .expect("leaf");
        // Renew the CA cert reusing the key (what RenewCertSameKey does).
        let renewed = renew_cluster_ca(&ca.key_pem, "c1-cluster-ca", 365).expect("renew");

        let leaf_der = CertificateDer::pem_slice_iter(leaf.cert_pem.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        let (_, leaf_x509) = X509Certificate::from_der(leaf_der.as_ref()).unwrap();
        let ca_der = CertificateDer::pem_slice_iter(renewed.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        let (_, renewed_ca) = X509Certificate::from_der(ca_der.as_ref()).unwrap();
        leaf_x509
            .verify_signature(Some(renewed_ca.public_key()))
            .expect("existing leaf must still chain to the renewed CA");
    }
}
