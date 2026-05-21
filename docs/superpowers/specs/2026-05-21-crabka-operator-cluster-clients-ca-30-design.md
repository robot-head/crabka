# Crabka Operator — Slice 30: Cluster CA + clients CA generation (design)

**Date:** 2026-05-21
**Status:** Plan-ready
**Scope:** Operator owns the full PKI lifecycle for a Crabka cluster — two
CAs (cluster + clients), per-broker server/client certs signed by the
cluster CA, Strimzi-shaped declarative validity/renewal config with BYO
toggle, a renewal CronJob shipped in the Helm chart, and inter-broker
mTLS turned on by default for the controller listener.
**Depends on:** slice 25 (TOML config-file delivery + listeners reconcile),
slice 29 (broker mTLS client-auth on listeners), slice 33 (cert hot-reload),
slice 37 (clients-CA lazy bootstrap — replaced by this slice).
**Replaces:** slice 37's `controller/user_tls.rs::ensure_clients_ca`
lazy-bootstrap path. Per CLAUDE.md the project is greenfield and
undeployed, so the bootstrap helper is deleted outright with no
migration shim.

## Goal

Land the operator-side PKI that turns Crabka's existing TLS/mTLS broker
support into a declarative, hands-off configuration. The single visible
behavior change after this slice: `kubectl apply` of a stock `Kafka` CR
produces a cluster whose controller listener runs mTLS by default using
operator-issued certs. Per-user TLS certs (slice 37) continue to work
unchanged — the clients-CA bootstrap moves from a lazy
"first-TLS-user-triggers-it" path to an explicit, declarative one.

## Decisions captured during brainstorm

1. **Scope** — All-in-one. CA lifecycle + per-broker keystore + inter-broker
   mTLS rollout + renewal CronJob all in one PR. Larger than recent operator
   slices but the pieces are tightly coupled and shipping them split would
   leave the cluster in a half-wired state between PRs.
2. **Renewal driver** — Strimzi-style CronJob, not reconciler. The
   reconciler creates Secrets when missing and re-renders broker keystore
   entries when broker count changes; renewal of valid-but-aging certs is
   the CronJob's job. Separation of concerns: "create" lives in the
   controller, "renew" lives in a scheduled side-process.
3. **BYO** — In scope. `generateCertificateAuthority: false` makes the
   operator validate that CA Secrets are user-supplied and never overwrite
   or renew them. CronJob skips BYO CAs and emits an Event noting that
   renewal is the cluster admin's responsibility.
4. **CA-itself expiry** — Disruptive in slice 30. When a CA is within
   `renewalDays` of `notAfter`, the operator sets a
   `CaRotationRequired=True` status condition + Kubernetes Event but
   does **not** auto-rotate. Slice 34 owns the rotation procedure
   (multi-generation trust bundle, two-phase coordinated roll). Slice 30
   keeps the trust bundle single-cert.
5. **Inter-broker mTLS** — Always on once cluster CA exists. No
   `Kafka.spec.kafka.interBroker.tls` toggle. Crabka's greenfield posture
   means the default flips to secure-by-default; CLAUDE.md explicitly
   says no compat shims for old plaintext-mesh deployments.
6. **Data-plane listener TLS** — Out of scope. `Kafka.spec.listeners[].tls`
   stays gated until slice 31 ("Listener auth wiring"). Slice 30 only
   touches the controller listener.

## Architecture

### CRD shape (`Kafka.spec.clusterCa`, `Kafka.spec.clientsCa`)

Two new top-level fields on `KafkaSpec`, peer to `listeners`,
`networkPolicy`, `metricsConfig`. Both wrap the same
`CertificateAuthority` struct.

```yaml
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata: { name: my-cluster }
spec:
  clusterCa:
    generateCertificateAuthority: true   # default true
    validityDays: 365                    # default 365
    renewalDays: 30                      # default 30
  clientsCa:
    generateCertificateAuthority: true
    validityDays: 365
    renewalDays: 30
```

Both fields are optional; when absent the operator applies the same defaults
as if a fully-defaulted `CertificateAuthority` had been written explicitly.
Inter-broker mTLS still turns on — the cluster CA is generated either way.

Rust types (in `crates/operator/src/crd/ca.rs`, new module):

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CertificateAuthority {
    #[serde(default = "default_generate")]
    pub generate_certificate_authority: bool,
    #[serde(default = "default_validity_days")]
    pub validity_days: u32,
    #[serde(default = "default_renewal_days")]
    pub renewal_days: u32,
}

fn default_generate() -> bool { true }
fn default_validity_days() -> u32 { 365 }
fn default_renewal_days() -> u32 { 30 }

impl Default for CertificateAuthority { /* delegates to the defaults above */ }
```

`KafkaSpec` gains `pub cluster_ca: Option<CertificateAuthority>` and
`pub clients_ca: Option<CertificateAuthority>` (both
`#[serde(default, skip_serializing_if = "Option::is_none")]`).

### Secret layout (matches Strimzi)

| Secret name                       | Contents                                  | Mounted by                                  |
|-----------------------------------|-------------------------------------------|---------------------------------------------|
| `<cluster>-cluster-ca`            | `ca.key` (PEM)                            | Operator + CronJob only                     |
| `<cluster>-cluster-ca-cert`       | `ca.crt` (PEM)                            | Every broker pod, every KafkaUser TLS Secret consumer |
| `<cluster>-clients-ca`            | `ca.key` (PEM)                            | Operator + CronJob only                     |
| `<cluster>-clients-ca-cert`       | `ca.crt` (PEM)                            | Already projected into per-user Secrets by slice 37; this slice promotes that path |
| `<cluster>-kafka-brokers`         | `<id>.crt` + `<id>.key` per replica (PEM) | Every broker pod (broker picks its own entries by node id) |

The two `*-ca` private-key Secrets carry the
`crabka.io/secret-type=ca-key` label and an explicit
`metadata.annotations["crabka.io/strictly-operator-managed"]="true"`
to make accidental hand-edits obvious in audit logs. RBAC for non-operator
ServiceAccounts denies read access to these two; the public `-ca-cert`
Secrets are world-readable within the cluster (`labels:` matches the
broker `ServiceAccount`'s implicit reach).

**Why one shared `*-kafka-brokers` Secret rather than per-broker?** StatefulSet
PodSpecs can't template Secret names by pod ordinal, so per-broker would
require either an init-container that copies its own entries out of a
shared Secret (Strimzi's choice) or N separate StatefulSets. Single
Secret + broker-picks-by-id is simpler. Tradeoff: every broker pod sees
every other broker's private key. Accepted: the broker mesh is one
trust boundary already (slice 13's super-user model, slice 12's
inter-broker SASL).

### `crates/security/src/ca.rs` — new pure helpers

Existing `generate_clients_ca` + `issue_user_cert` stay. Two additions:

```rust
/// Self-signed *cluster* CA. Same shape as `generate_clients_ca` (ECDSA
/// P-256, CA:TRUE, keyCertSign + cRLSign). Separate function so the
/// subject DN can carry `OU=cluster` (clients CA carries `OU=clients`),
/// making the two distinguishable in audit logs and cert chains without
/// extra metadata.
pub fn generate_cluster_ca(cn: &str, validity_days: u32) -> Result<CaMaterial, CaError>;

/// Sign a broker leaf cert: server cert + client cert in one (EKU =
/// serverAuth + clientAuth, KU = digitalSignature + keyEncipherment).
/// SANs include every DNS name + IP the broker presents on the
/// inter-broker mesh: the headless service FQDN, the per-pod FQDN, the
/// per-pod hostname-only form, and `127.0.0.1`. Caller supplies the SAN
/// list.
pub fn issue_broker_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    cn: &str,
    sans: &[SubjectAltName],
    validity_days: u32,
) -> Result<BrokerCert, CaError>;

pub enum SubjectAltName {
    Dns(String),
    Ip(IpAddr),
}

pub struct BrokerCert {
    pub cert_pem: String,
    pub key_pem: String,
    pub not_after: String,
}
```

No async, no I/O. Reusable from both the reconciler and the CronJob
subcommand.

### `crates/operator/src/controller/cluster_ca.rs` — new module

Owns the controller-side CA logic:

- `ensure_cluster_ca(secret_api, kafka) -> Result<CaMaterial>` —
  get-or-create the cluster CA Secret pair. Mirrors slice 37's
  `ensure_clients_ca` but for the cluster CA. When
  `spec.clusterCa.generateCertificateAuthority == false`, validates the
  pre-existing Secret pair and errors with a structured
  `ReconcileError::ByoCaMissing { which: ClusterCa | ClientsCa }` if not
  found.
- `ensure_clients_ca(secret_api, kafka) -> Result<CaMaterial>` — replaces
  the slice-37 helper. Same semantics, same Secret names, but now
  honors `spec.clientsCa.generateCertificateAuthority`.
- `ensure_broker_keystore(secret_api, kafka, replicas, cluster_ca) -> Result<BrokerKeystoreStatus>` —
  creates or updates the per-cluster `<cluster>-kafka-brokers` Secret.
  Idempotent: existing entries whose `notAfter` is more than
  `renewalDays` away are reused; missing entries get issued; entries
  for retired broker ids get pruned. Returns
  `{ replicas_issued: Vec<i32>, replicas_reused: Vec<i32>, oldest_not_after: String }`
  for status.

The reconciler calls them in order in `controller/kafka.rs` (see the
fuller pipeline below in "Reconcile pipeline"). The constraints are:

- cluster + clients CAs first (the keystore step signs against the
  cluster CA);
- broker keystore Secret before the StatefulSet (the StatefulSet
  template mounts the Secret);
- ConfigMap before the StatefulSet (same reason, but the ConfigMap and
  keystore step are unordered relative to each other — ConfigMap
  references *mount paths*, which are static, not Secret contents).

### `crates/operator/src/controller/user_tls.rs` — diff

`ensure_clients_ca` (the slice-37 helper) **is deleted**. Its single
caller in `controller/user.rs` switches to the new
`controller::cluster_ca::ensure_clients_ca` API which has the same
signature but additionally honors the BYO toggle.

Per CLAUDE.md no compat shim, no deprecation path. The function moves
modules and gains one new behavior (BYO) in a single edit.

### Broker config-file additions

Slice 25a's TOML config-file already supports `controller_listener_protocol`
+ `tls_config`. The operator-rendered file for slice 30 looks like:

```toml
node_id = 0
inter_broker_listener_name = "internal"

controller_listener_protocol = "Ssl"

[tls_config]
cert_path        = "/etc/crabka/broker-tls/0.crt"
key_path         = "/etc/crabka/broker-tls/0.key"
client_ca_path   = "/etc/crabka/cluster-ca/ca.crt"
client_auth      = "Required"

[[listeners]]
name = "internal"
port = 9092
protocol = "Plaintext"   # still — data-plane TLS is slice 31
```

Per-broker file: the `cert_path` / `key_path` references `<id>.crt` and
`<id>.key` matching the broker's `--node-id`. The operator emits one
config-file per broker into the existing per-broker key of the
`<cluster>-broker-config` ConfigMap; no schema change beyond the new
fields.

### StatefulSet template additions

Three new volume mounts on every broker container:

```yaml
spec.template.spec.containers[0].volumeMounts:
  - name: cluster-ca-cert
    mountPath: /etc/crabka/cluster-ca
    readOnly: true
  - name: broker-tls
    mountPath: /etc/crabka/broker-tls
    readOnly: true
  - name: clients-ca-cert
    mountPath: /etc/crabka/clients-ca
    readOnly: true   # mounted but not consumed by the broker in slice 30; reserved for slice 31

spec.template.spec.volumes:
  - name: cluster-ca-cert
    secret:
      secretName: <cluster>-cluster-ca-cert
  - name: broker-tls
    secret:
      secretName: <cluster>-kafka-brokers
      defaultMode: 0o400
  - name: clients-ca-cert
    secret:
      secretName: <cluster>-clients-ca-cert
```

The broker's `--config-file` argument is unchanged from slice 25a; only
the file's contents grow.

### Slice-21 config-hash inclusion

`combined_config_hash` (in `crates/operator/src/controller/common.rs`)
grows a fourth segment: the cluster CA cert PEM. Renewing the cluster
CA (or rotating from one CA to a new one in BYO mode) forces a cluster
roll. Per-broker leaf cert renewal does **not** change the hash and so
does not trigger a roll — slice 33's cert hot-reload picks up the new
file from the mounted Secret. The data-plane segment from slice 25 and
the metrics segment from slice 40 stay unchanged.

Tests: an explicit assertion that `combined_config_hash` is **stable**
under broker-keystore Secret changes (so leaf renewal doesn't cascade
into a roll) and **unstable** under cluster-CA-cert Secret changes.

### CronJob: `crabka-operator ca-renewal-check`

New CLI subcommand in `crates/operator/src/main.rs`:

```text
crabka-operator ca-renewal-check [--namespace <ns>]
```

Without `--namespace`: cluster-scoped, requires `ClusterRole`. With
`--namespace`: namespace-scoped, requires `Role`. Default in the Helm
chart: cluster-scoped, matching the operator Deployment's reach.

Behavior:

1. List Kafka CRs in scope.
2. For each CR, read both CA Secrets + the broker keystore Secret.
3. For each leaf cert (broker `<id>.crt` entries):
   - Parse `notAfter`.
   - If `notAfter - now <= renewalDays`: reissue (sign with the still-valid
     CA), patch the broker keystore Secret with the new entry. Emit
     `Normal` Event `BrokerCertRenewed broker=<id>`.
4. For each CA cert:
   - If `notAfter - now <= renewalDays` **and** `generateCertificateAuthority`:
     set status condition `CaRotationRequired=True` on the Kafka CR,
     emit `Warning` Event `CaRotationRequired which=<cluster|clients>`,
     do **not** rotate. Slice 34 owns rotation.
   - If `notAfter - now <= renewalDays` **and not** `generateCertificateAuthority`:
     emit `Warning` Event `ByoCaExpiringSoon which=<cluster|clients>`, do
     nothing else.
5. Exit 0 unless something fatal happened (e.g. kube-apiserver unreachable).

The subcommand is idempotent and safe to re-run. The Helm chart ships
the CronJob with `schedule: "0 2 * * *"` (daily, 02:00 UTC) and
`startingDeadlineSeconds: 600`. The CronJob pod uses the same operator
image and a dedicated `ServiceAccount` (`crabka-operator-renewal`) with
narrower RBAC than the main operator: read on Kafka CRs, read+patch on
Secrets in the same namespaces, create on Events. No write access on
`statefulsets`, no leader-Lease.

The renewal logic — `renew_if_expiring(now, cert_pem, ca, renewal_days)` —
lives in `crates/operator/src/controller/cluster_ca.rs` as a pure
function and is unit-tested with fixed `now` values. Both the reconciler
and the CronJob subcommand call it; the reconciler calls it only on
**creation** (initial generation), not on subsequent reconciles, so
renewal stays in the CronJob lane.

### Helm chart additions (`charts/crabka-operator/templates/`)

- `cronjob-ca-renewal.yaml` (new) — `kind: CronJob`, schedule from
  `values.yaml`, `imagePullPolicy` matches the Deployment.
- `serviceaccount-renewal.yaml` (new) — separate SA for the CronJob.
- `clusterrole-renewal.yaml` (new) — narrower RBAC (read CRs, RW Secrets,
  write Events).
- `clusterrolebinding-renewal.yaml` (new) — binds the renewal SA.
- `values.yaml` (edit) — `caRenewal.schedule`, `caRenewal.enabled`
  (default true), `caRenewal.startingDeadlineSeconds`.

The existing operator `ClusterRole` also grows Secret write permission
on the new naming pattern (already implied since slice 37 patches user
TLS Secrets, but the `*-cluster-ca` Secrets are new resource names; the
existing `*` resource name match covers it). No new permissions on the
main operator role.

## Reconcile pipeline (delta from slice 25 / 37)

`controller/kafka.rs::reconcile_kafka`:

```text
1.  validate_spec_listeners (slice 25)
2.  ensure_cluster_ca   ← NEW: get-or-create CA Secret pair
3.  ensure_clients_ca   ← MOVED from user_tls.rs, now BYO-aware
4.  ensure_broker_keystore   ← NEW: get-or-create per-cluster broker keystore
5.  ensure_configmap (renders broker config-file with TLS paths)
6.  ensure_service (existing)
7.  ensure_statefulset (template gains the three volume mounts above)
8.  reconcile listeners (slice 25)
9.  reconcile network_policy (slice 23)
10. reconcile metrics (slice 40)
11. update_status
```

The order matters: CA before keystore (keystore signs against CA),
keystore and ConfigMap before StatefulSet (StatefulSet mounts both).
Keystore and ConfigMap are unordered relative to each other — the
ConfigMap references static mount paths, not Secret contents, so it
doesn't observe the keystore Secret's existence.

The reconciler creates Secrets but **never overwrites them after
initial creation** (apart from broker-id additions when `replicas`
grows). Renewal of valid certs is the CronJob's exclusive job. This
is enforced by: when a Secret exists, the reconciler reads its
contents and reuses them; only if a key is missing does the
reconciler issue.

## Status surfacing

New fields on `KafkaStatus`:

```yaml
status:
  clusterCa:
    notAfter: "2027-05-21T00:00:00Z"
    generated: true                # vs BYO
  clientsCa:
    notAfter: "2027-05-21T00:00:00Z"
    generated: true
  conditions:
    - type: ClusterCaReady          # True when cluster CA Secret pair exists and is parseable
    - type: ClientsCaReady
    - type: CaRotationRequired      # True when CronJob has flagged a CA as expiring
```

The existing `Ready` condition gains a precondition: `ClusterCaReady`
and `ClientsCaReady` must both be True before `Ready` flips True.

## Out of scope

- **Data-plane listener TLS** — `Kafka.spec.listeners[].tls=true` still
  rejected at validation time. Slice 31 unblocks it.
- **Non-disruptive CA rotation** — Slice 34. This slice keeps the trust
  bundle single-cert; rotating the CA requires a coordinated cluster
  roll which slice 34 will orchestrate.
- **PKCS#12 / JKS bundles in broker keystore Secret** — PEM-only.
  Add `.p12` keys to the Secret when a JVM-client consumer asks
  (likely a slice-30 follow-up after a real-world ask).
- **`MaintenanceTimeWindows`** — Strimzi field for gating disruptive
  ops to specific hours. Slice 34 territory.
- **CRL / OCSP** — Not Strimzi parity. Future security work, no slice
  number yet.
- **CA private-key encryption at rest** — Kubernetes Secrets are
  base64'd, not encrypted. EncryptionConfiguration is a cluster-level
  feature; we don't add a Crabka layer on top.

## Testing strategy

### Unit tests (per file)

- `crates/security/src/ca.rs`: existing tests stay; add 4 new tests for
  `generate_cluster_ca` (subject DN carries `OU=cluster`, CA:TRUE,
  validity span) and `issue_broker_cert` (EKU = serverAuth+clientAuth,
  SANs round-trip via x509-parser, signature verifies against the CA).
- `crates/operator/src/crd/ca.rs`: schema-gen and defaults round-trip.
- `crates/operator/src/controller/cluster_ca.rs`:
  - `renew_if_expiring` returns reissue=true when `notAfter - now < renewal_days`,
    reissue=false when comfortably in the future, and reissue=true when
    `notAfter` is already past.
  - `ensure_broker_keystore` returns `issued=[0,1,2]` on first call and
    `reused=[0,1,2]` on second call against a stable kube fixture.
  - `ensure_broker_keystore` returns `issued=[3]` + `reused=[0,1,2]`
    when `replicas` grows from 3 to 4.
- `crates/operator/src/controller/common.rs`: `combined_config_hash`
  changes when cluster CA cert PEM changes; does not change when the
  broker keystore changes.

### Integration tests (no-Docker)

`crates/operator/tests/reconcile_ca.rs` (new):

1. **Default (operator-managed) flow.** Apply a `Kafka` CR with no
   `clusterCa` / `clientsCa` fields. Run the reconciler. Assert: both
   CA private-key Secrets exist, both CA cert Secrets exist, broker
   keystore Secret has `<id>.crt` + `<id>.key` entries for every replica.
   Parse the broker cert via x509-parser, assert subject DN is
   `CN=<cluster>-kafka-<id>`, EKU includes serverAuth + clientAuth, the
   chain verifies against the cluster CA.
2. **Replica scale-up issues new entries.** Apply with replicas=3, then
   patch replicas=5. Assert keystore Secret grows by exactly two entries
   (`3.crt`, `3.key`, `4.crt`, `4.key`) and pre-existing entries are
   byte-identical (reuse, not reissue).
3. **Replica scale-down prunes entries.** Patch replicas=5 down to 3.
   Assert keystore Secret loses `3.*` and `4.*` entries.
4. **BYO mode validates pre-existing Secrets.** Pre-create
   `<cluster>-cluster-ca` + `<cluster>-cluster-ca-cert` with user-provided
   ECDSA P-256 material. Apply Kafka CR with
   `clusterCa.generateCertificateAuthority=false`. Assert: CA Secrets
   are byte-identical to what was pre-created (operator did not
   overwrite), broker keystore Secret still gets issued (operator signed
   leafs against the user's CA), reconciler returns Ok.
5. **BYO mode without pre-existing Secrets errors gracefully.** Apply
   Kafka CR with `generateCertificateAuthority=false` and no
   pre-existing CA Secrets. Assert: reconciler returns a `ByoCaMissing`
   ReconcileError, status condition `ClusterCaReady=False reason=ByoCaMissing`,
   no Secrets created.
6. **Reconciler does not renew valid leaf certs.** Pre-create a
   keystore Secret with leaf certs whose `notAfter` is 60 days out
   (within validity but past `renewalDays`). Reconcile. Assert keystore
   Secret is unchanged byte-for-byte.
7. **Config-hash propagates cluster CA cert changes.** Compute
   `combined_config_hash` for spec X with CA-A; reissue cluster CA
   (different key); compute hash again. Assert hashes differ.

`crates/operator/tests/reconcile_inter_broker_mtls.rs` (new):

1. **Default cluster boots with mTLS controller listener.** Apply
   `Kafka` CR. Assert: rendered `<cluster>-broker-config` ConfigMap
   contains `controller_listener_protocol = "Ssl"` and a `[tls_config]`
   table with all three paths. StatefulSet template has the three
   expected `volumeMounts` + `volumes`. Render is stable across two
   consecutive reconciles (idempotency).
2. **`Kafka.spec.listeners[].tls=true` is still rejected.** Apply a
   listener with `tls: true`; assert `ListenersValid=False reason=TlsNotYetSupported`.
   Slice 30 doesn't change this behavior, but the assertion guards
   against drift while we wire CAs.

`crates/operator/tests/ca_renewal_cronjob.rs` (new):

1. **Subcommand reissues leafs within renewal window.** Construct a
   kube fixture with a keystore Secret whose `<0>.crt` has
   `notAfter = now + 20 days` (under default 30-day renewal). Run the
   subcommand. Assert: keystore Secret PATCHed, new `<0>.crt` has
   `notAfter > now + 364 days`, other entries unchanged byte-identical,
   one `BrokerCertRenewed` Event emitted.
2. **Subcommand flags expiring CA without rotating.** Construct a
   fixture with a cluster CA whose `notAfter = now + 25 days`. Run
   subcommand. Assert: cluster CA Secret unchanged, status condition
   `CaRotationRequired=True reason=ClusterCaExpiringSoon` on the Kafka
   CR, one `Warning` Event emitted.
3. **Subcommand on BYO CA emits a warning, no rotation.** Fixture with
   `generateCertificateAuthority=false`, CA expiring within window.
   Assert: Secret unchanged, `Warning` Event `ByoCaExpiringSoon`
   emitted, no `CaRotationRequired` condition (the responsibility is
   the admin's, not the operator's — slice 34's rotation path doesn't
   apply to BYO either).

### JVM acceptance tests

**None.** Inter-broker mTLS doesn't surface to the JVM admin CLI; the
existing slice-12b JVM SASL+TLS inter-broker test already covers the
broker-side wire behavior under operator-issued certs would look
identical to. Adding a JVM test for the *operator* rendering of broker
configs duplicates what the integration tests already do.

### kind e2e

The existing slice-17 kind smoke test gets a single new assertion: after
the default `Kafka` CR Ready, `kubectl get secret <cluster>-cluster-ca-cert`
+ `<cluster>-clients-ca-cert` both exist and `kubectl exec broker-0 --
ls /etc/crabka/cluster-ca` returns `ca.crt`. No new e2e file.

## Risks and mitigations

| Risk                                                                                  | Mitigation                                                                                                                  |
|---------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| Broker pods can't reach kube-apiserver to mount Secrets on first start (chicken/egg)  | Operator creates Secrets *before* StatefulSet (reconcile step ordering above). kube-apiserver outage during cluster bring-up surfaces as `StatefulSet Pending`, same as today. |
| Renewal CronJob misses a window because the CronJob pod was unschedulable             | `startingDeadlineSeconds: 600`, daily schedule means worst-case lag is ~24h before the next attempt. Default `renewalDays: 30` against a 365-day cert means the CronJob has to miss ~30 consecutive daily attempts before a leaf actually expires. If it does expire, brokers fail TLS handshake and the existing `Ready` condition goes False — alertable via the standard Kubernetes mechanisms. |
| BYO CA cert is invalid (expired, wrong EKU, etc.)                                     | `ensure_cluster_ca` parses the user-supplied PEM via x509-parser at reconcile time. Parse failure → structured `ReconcileError::ByoCaMalformed { which, reason }` → status condition. |
| Per-broker private keys all visible inside every pod                                  | Documented tradeoff (above). Per-broker Secret would require init-container choreography; deferred until we have a real principle of least privilege story for the broker mesh. |
| `combined_config_hash` collision causes false-negative drift detection                | sha256 truncated to 8 bytes is 2^64 space. Negligible. (Same risk class as the existing slice-21 hash.) |

## Migration / compatibility

None. Slice 37's `controller/user_tls.rs::ensure_clients_ca` is deleted
in this PR. KafkaUser TLS Secrets created by slice 37 reference
`<cluster>-clients-ca-cert` by name; that Secret name is preserved
verbatim in slice 30, so existing per-user Secrets keep working without
changes (the `ca.crt` they carry just got generated through a different
code path).

## Implementation order (will become the plan)

1. `crates/security/src/ca.rs` — add `generate_cluster_ca` +
   `issue_broker_cert` + `SubjectAltName`. Tests.
2. `crates/operator/src/crd/ca.rs` — new module with
   `CertificateAuthority` + defaults. Field additions on `KafkaSpec`.
   CRD regeneration check in CI.
3. `crates/operator/src/controller/cluster_ca.rs` — `ensure_cluster_ca`,
   `ensure_clients_ca` (moved from user_tls), `ensure_broker_keystore`,
   `renew_if_expiring` pure helper. Tests against a fake `kube::Client`.
4. `crates/operator/src/controller/user_tls.rs` — delete
   `ensure_clients_ca`, update caller to use the new module.
5. `crates/operator/src/controller/common.rs` —
   `combined_config_hash` grows a fourth segment. Tests.
6. `crates/operator/src/controller/kafka.rs` — reconcile pipeline:
   call the three new `ensure_*` helpers, render TLS-paths into the
   broker config-file, add the three volume mounts to the StatefulSet
   template, update status. Tests.
7. `crates/operator/src/main.rs` — add `CaRenewalCheck` subcommand,
   wire to `controller/cluster_ca::run_renewal_check`. Tests.
8. `charts/crabka-operator/templates/` — CronJob, ServiceAccount,
   ClusterRole, ClusterRoleBinding for renewal. Helm-lint test in CI.
9. Integration tests: `reconcile_ca.rs`, `reconcile_inter_broker_mtls.rs`,
   `ca_renewal_cronjob.rs`.
10. kind e2e amendment in the existing smoke test.
11. STATUS.md entry.

Each step is one logical commit on the slice-30 branch.
