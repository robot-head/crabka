# Gateway P9 — Operator deployment + full gateway↔broker mTLS — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Tasks run in parallel batches (disjoint file sets); steps use `- [ ]`.

**Goal:** Ship a `KafkaGrpcGateway` Custom Resource that the crabka-operator reconciles into a Deployment + Service + operator-issued TLS, AND make the gateway speak mTLS to the broker (so it works against a TLS-secured Kafka).

**Architecture:** Three crates. (1) `client-core`: add a client *identity* (mTLS cert+key) to `TlsConnectorConfig` (today server-auth only). (2) `crabka-grpc-gateway`: add `--broker-tls-*` flags → build a `ClientSecurity{protocol:Ssl, tls:…}` → thread `.security(…)` into every Kafka client it builds. (3) `crabka-operator`: a rich `KafkaGrpcGateway` CRD + a controller that renders Deployment + Service + a config Secret (the two gateway TOML files, with HMAC secrets resolved from user `secretRef`s), issues the gateway's **serving** cert from the cluster CA, and delegates the gateway's **client** cert + Kafka ACLs to a child `KafkaUser` CR. The gateway is the operator's first Deployment-owning controller.

**Tech Stack:** kube-rs `CustomResource`, `bon` builders, `rcgen` via `crabka_security::ca`, rustls 0.23, axum, the operator's FIFO mock-client reconcile harness.

**Stacked on:** P8 (#419, branch `claude/gateway-p8`). Branch `claude/gateway-p9`.

---

## Design

### Trust topology (the load-bearing detail)
The broker has two CAs (survey-confirmed): **cluster CA** (`<kafka>-cluster-ca-cert`, key `ca.crt`) signs broker **serving** certs; **clients CA** (`<kafka>-clients-ca-cert`, key `ca.crt`) is the broker's data-plane `client_ca_path` — it verifies **client** certs. Therefore:

- Gateway **serving** cert (its own Connect/webhook/metrics TLS) → signed by **cluster CA** (so peer gateways + clients that trust the cluster CA accept it). Issued by the controller via `crabka_security::ca::issue_broker_cert` (SANs = Service DNS).
- Gateway **client** cert (mTLS to broker) → signed by **clients CA** (so the broker's `client_ca_path` accepts it). Obtained by the controller **creating a child `KafkaUser`** (`authentication: tls`, CN = gateway name) and letting the existing KafkaUser reconciler issue `user.crt/user.key/ca.crt` + provision ACLs.
- Gateway verifies the **broker's** serving cert using **cluster CA** trust roots.
- Gateway verifies inbound mTLS clients using **clients CA** (`--tls-client-ca`); verifies peer-gateway serving certs (forwarding) using **cluster CA** (`--tls-trust-roots`).

> ⚠ Signing the gateway's broker-bound client cert with the cluster CA (instead of the clients CA) is the #1 integration failure mode. The child-KafkaUser approach sidesteps it: KafkaUser certs are clients-CA-signed by construction.

### Deployment mount set (what the controller produces)
| Mount | Source Secret | Keys | Gateway flag(s) |
|---|---|---|---|
| `/etc/crabka-gw/serving/` | `<gw>-serving` (operator-issued, cluster-CA) | `tls.crt`,`tls.key` | `--tls-cert`,`--tls-key` |
| `/etc/crabka-gw/broker-client/` | `<gw>-broker` (the child KafkaUser's Secret) | `user.crt`,`user.key` | `--broker-tls-cert`,`--broker-tls-key` |
| `/etc/crabka-gw/cluster-ca/` | `<kafka>-cluster-ca-cert` | `ca.crt` | `--broker-tls-ca`, `--tls-trust-roots` |
| `/etc/crabka-gw/clients-ca/` | `<kafka>-clients-ca-cert` | `ca.crt` | `--tls-client-ca` |
| `/etc/crabka-gw/config/` | `<gw>-config` (operator-rendered) | `webhooks.toml`,`outbound.toml` | `--webhooks-config`,`--outbound-webhooks-config` |

Bootstrap = the **TLS internal listener** bootstrap from `Kafka.status.listeners` (NOT a plaintext listener). `--broker-tls-server-name` = the broker SNI matching its serving-cert SAN (the headless-svc DNS). `advertised-addr` = `$(POD_IP):9500` via the downward API (Deployment-friendly; pod IPs are in-cluster routable for gateway→gateway forwarding).

### CRD shape (rich; `crd/grpc_gateway.rs`)
```rust
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(group="crabka.io", version="v1alpha1", kind="KafkaGrpcGateway",
       plural="kafkagrpcgateways", singular="kafkagrpcgateway", shortname="kgg",
       namespaced, status="KafkaGrpcGatewayStatus", derive="PartialEq")]
#[serde(rename_all = "camelCase")]
pub struct KafkaGrpcGatewaySpec {
    // Parent Kafka is discovered from the `crabka.io/cluster` LABEL (not a spec field) — KafkaTopic/KafkaUser convention.
    #[serde(default, skip_serializing_if="Option::is_none")] pub replicas: Option<i32>,        // default 1
    #[serde(default, skip_serializing_if="Option::is_none")] pub image: Option<String>,        // else operator --default-gateway-image
    #[serde(default, skip_serializing_if="Option::is_none")] pub resources: Option<k8s_openapi::api::core::v1::ResourceRequirements>,
    #[serde(default, skip_serializing_if="Option::is_none")] pub dedup: Option<DedupSpec>,
    #[serde(default, skip_serializing_if="Option::is_none")] pub tls: Option<GatewayTlsSpec>,
    #[serde(default, skip_serializing_if="Option::is_none")] pub authz: Option<GatewayAuthzSpec>,
    #[serde(default, skip_serializing_if="Vec::is_empty")]   pub webhooks: Vec<InboundWebhookSpec>,
    #[serde(default, skip_serializing_if="Vec::is_empty")]   pub outbound_subscriptions: Vec<OutboundSubscriptionSpec>,
    #[serde(default, skip_serializing_if="Option::is_none")] pub telemetry: Option<TelemetrySpec>,
}
// DedupSpec { topic?, partitions? (u32, default 8), window_ms? (i64), txn_id_prefix? }
// GatewayTlsSpec { client_auth? ("disabled"|"optional"|"required", default "required"), validity_days? (u32, default 365) }
//   — mode is implicitly "operator-issued" (the chosen design); no other mode modelled.
// GatewayAuthzSpec { mode ("off"|"simple", default "simple"), super_users: Vec<String>, acl_refresh_secs? (u64),
//                    bearer? GatewayBearerSpec { mode ("off"|"unsecured"), principal_claim? } }
// InboundWebhookSpec { name, target_topic, principal?, signature_header?, signature_encoding?, signature_prefix?,
//                      timestamp_header?, timestamp_tolerance_secs?, idempotency_source?, key_source?, max_body_bytes?,
//                      secret_ref?: SecretKeyRef }     // HMAC secret via Secret, NOT inline
// OutboundSubscriptionSpec { name, source_topics: Vec<String>, target_url, dead_letter_topic?, max_attempts?,
//                      base_backoff_ms?, max_backoff_ms?, request_timeout_ms?, filter?, headers?: BTreeMap<String,String>,
//                      signing_secret_ref?: SecretKeyRef }
// AllowedTargetSpec { scheme, host }  + the controller derives allowed_targets from each subscription's target_url host (or an explicit list field `allowed_targets: Vec<AllowedTargetSpec>`).
// SecretKeyRef { name: String, key: String }  — resolved by the controller from a same-namespace Secret.
// TelemetrySpec { otlp_endpoint?, otlp_protocol? ("grpc"|"http"), sample_ratio? (f64) }
```
**Status:** `KafkaGrpcGatewayStatus { conditions: Vec<crate::crd::KafkaCondition>, observed_generation: Option<i64>, ready_replicas: Option<i32> }`. Conditions: `Ready`, `KafkaVersionValid` (gate, copied from parent), `CertReady`, `Degraded`.

**Schemars gotcha:** any tagged-enum field with a shared `type` discriminator needs `#[schemars(schema_with=…)]`. The structs above are plain optionals — none needed — but if you model `tls.mode` as an enum later, apply it.

### Config rendering (CRD → gateway inputs)
- **Simple scalars → Deployment env** (every gateway flag has a `CRABKA_GATEWAY_*` env; Survey A list): bootstrap, client-id (`$(POD_NAME)`), listen-addr, advertised-addr (`$(POD_IP):9500`), dedup.*, authz.*, bearer.*, the mounted-file PATHS, telemetry envs, `RUST_LOG`.
- **Two TOML files → `<gw>-config` Secret** (Secret, not ConfigMap — they embed resolved HMAC secrets): `webhooks.toml` (from `spec.webhooks`, resolving each `secret_ref` → the secret value), `outbound.toml` (from `spec.outbound_subscriptions`, resolving `signing_secret_ref` + `headers`; `allowed_targets` derived). Serialize with the `toml` crate to the exact schema in `crates/grpc-gateway/src/{webhook_config.rs,outbound_config.rs}`.
- **Certs → Secrets** (serving issued by controller; client via child KafkaUser; CA bundles referenced).

### Controller flow (`controller/grpc_gateway.rs::reconcile`)
1. Parse `crabka.io/cluster` label → fetch parent `Kafka` (requeue 30s if absent).
2. **Version gate** — reuse the pool's `version_gate` logic: if parent `KafkaVersionValid != True` and no finalized `metadataVersion`, set `Ready=False reason=WaitingForVersionValidation`, requeue 30s.
3. Ensure the **child `KafkaUser`** (`<gw>-broker`, label `crabka.io/cluster`, `authentication: tls`, broad ALLOW ACLs on Topic:*/Group:*/TransactionalId:*/Cluster, owner-ref → this CR) via SSA. Read back its issued Secret; if not yet present, set `CertReady=False`, requeue 15s.
4. **Issue the serving cert** (cluster CA via `issue_broker_cert`, SANs = `<gw>.<ns>.svc`, `<gw>.<ns>.svc.cluster.local`, `<gw>`) → SSA the `<gw>-serving` Opaque Secret (`tls.crt`/`tls.key`), owner-ref → CR. Renew when expiring (reuse `is_cert_expiring_soon`).
5. **Render** the `<gw>-config` Secret (two TOML files, secretRefs resolved) → SSA, owner-ref → CR.
6. **Apply** the Deployment (env + the 5 volume mounts + probes on `/health*` + container port 9500) and the Service (ClusterIP, port 9500) via SSA `Patch::Apply` (field manager `crabka-operator`), owner-refs → CR.
7. Read back the Deployment; set `Ready` from `readyReplicas == replicas`, set `observedGeneration`, `readyReplicas`; `patch_status` (`Patch::Merge`). Requeue 30s.

`error_policy` → requeue 15s. Owner refs via `common::owner_ref::<KafkaGrpcGateway>`; SSA via `common::apply_object`; status via `common::patch_status`; labels via a new `gateway_labels()` (app `crabka-grpc-gateway`, instance = kafka name, managed-by `crabka-operator`).

### Deltas checklist
- `crd/mod.rs`: `pub mod grpc_gateway;` + re-export the types.
- `gen_crds.rs`: `write_one::<KafkaGrpcGateway>` + extend the existence test → regenerate `deploy/crds/crabka.io_kafkagrpcgateways.yaml` (CI `codegen-check` drift-gates it).
- `run.rs`: `tokio::spawn` + `select!` arm for `controller::grpc_gateway::run`.
- `charts/crabka-operator/templates/clusterrole.yaml`: add `kafkagrpcgateways`(+`/status`) and **`apps/deployments`** (NOT currently present — real gap); `kafkausers`, `services`, `secrets` verbs already exist.
- `charts/crabka-operator/values.yaml`: `gatewayImage:` stanza; operator deployment template threads `--default-gateway-image`.
- `config.rs`: `default_gateway_image: String` field + `--default-gateway-image` arg (mirror `default_broker_image`); a `DEFAULT_GATEWAY_IMAGE` const = `concat!("ghcr.io/robot-head/crabka-grpc-gateway:", env!("CARGO_PKG_VERSION"))`.
- `tests/reconcile_gateway.rs` + `tests/shared/mod.rs` fakes.

---

## Batches

- **Batch A (parallel — disjoint crates):** T1 client-core mTLS ∥ T2 operator CRD types+gen.
- **Batch B (parallel — disjoint crates):** T3 gateway broker-TLS wiring (needs T1) ∥ T4 operator controller (needs T2).
- **Batch C (parallel — disjoint file sets):** T5 operator run+helm+config wiring (needs T4) ∥ T6 operator reconcile tests (needs T4).
- Final review + PR.

---

## Task 1: client-core — mTLS client identity in `TlsConnectorConfig`

**Files:** Modify `crates/client-core/src/security.rs` (+ its tests).

The builders (`Producer`/`Consumer`/`AdminClient`) already accept `.security(ClientSecurity)`; `ClientSecurity.tls: Option<TlsConnectorConfig>`. The only gap: `TlsConnectorConfig::build()` ends in `.with_no_client_auth()` — no client cert. Add an optional client identity.

- [ ] **Step 1:** Extend the struct:
```rust
pub struct TlsConnectorConfig {
    pub trust_roots_pem: Option<PathBuf>,
    pub server_name: String,
    /// Client identity for mTLS: (cert chain PEM, private key PEM). `None` → no client auth (one-way TLS).
    pub client_identity: Option<(PathBuf, PathBuf)>,
}
```
- [ ] **Step 2:** In `build()`, after assembling `roots`, branch:
```rust
let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
let cfg = match &self.client_identity {
    Some((cert_pem, key_pem)) => {
        let certs: Vec<_> = rustls::pki_types::CertificateDer::pem_file_iter(cert_pem)
            .map_err(|e| format!("client cert load {}: {e}", cert_pem.display()))?
            .collect::<Result<_,_>>().map_err(|e| format!("client cert parse: {e}"))?;
        let key = rustls::pki_types::PrivateKeyDer::from_pem_file(key_pem)
            .map_err(|e| format!("client key load {}: {e}", key_pem.display()))?;
        builder.with_client_auth_cert(certs, key).map_err(|e| format!("client auth cert: {e}"))?
    }
    None => builder.with_no_client_auth(),
};
Ok(Arc::new(cfg))
```
- [ ] **Step 3:** Update the 4 existing `TlsConnectorConfig { … }` literals in this file's tests to add `client_identity: None`. Add a test `tls_connector_config_builds_with_client_identity` that points at a generated/temp cert+key (or asserts the `None` path unchanged + a `Some` path returns a config — generate an ephemeral self-signed pair via `rcgen` in the test, write to `tempfile`, assert `build()` is Ok). Keep `crabka_security` available; `rcgen` may need adding as a dev-dep — check `client-core/Cargo.toml` first; if absent, prefer asserting the load-error path on a bogus path instead of adding a dev-dep.
- [ ] **Step 4:** Grep the workspace for other `TlsConnectorConfig {` constructors (inter-broker dialer, broker, tests) and add `client_identity: None` so the workspace compiles. Run `cargo build --workspace` to find them all.
- [ ] **Gates:** `cargo test -p crabka-client-core`; `cargo clippy -p crabka-client-core --all-targets -- -D warnings`; `cargo build --workspace` (the new field compiles everywhere). Commit `feat(client-core): mTLS client identity in TlsConnectorConfig`.

---

## Task 2: operator — `KafkaGrpcGateway` CRD types + generation

**Files:** Create `crates/operator/src/crd/grpc_gateway.rs`; modify `crates/operator/src/crd/mod.rs`, `crates/operator/src/gen_crds.rs`; regenerate `deploy/crds/crabka.io_kafkagrpcgateways.yaml`. **Types only — no controller (Task 4).** Must compile green.

- [ ] **Step 1:** Write `crd/grpc_gateway.rs` with the full `KafkaGrpcGatewaySpec` + sub-structs + `KafkaGrpcGatewayStatus` exactly per the Design §"CRD shape" (mirror `crd/topic.rs`/`crd/user.rs` conventions: `#[serde(rename_all="camelCase")]`, `Option`+`skip_serializing_if`, `derive="PartialEq"`, `KafkaCondition` from `crate::crd`). Use `k8s_openapi::api::core::v1::ResourceRequirements` for `resources` (confirm it derives `JsonSchema` under the enabled `k8s-openapi` features; if not, model a minimal `{ limits?, requests?: BTreeMap<String,String> }`).
- [ ] **Step 2:** `crd/mod.rs`: `pub mod grpc_gateway;` + `pub use grpc_gateway::{KafkaGrpcGateway, KafkaGrpcGatewaySpec, KafkaGrpcGatewayStatus};`.
- [ ] **Step 3:** `gen_crds.rs::write_all`: add `write_one::<KafkaGrpcGateway>(out_dir)?;`. Extend the unit test's expected-files list with `crabka.io_kafkagrpcgateways.yaml`.
- [ ] **Step 4:** Regenerate: `cargo run -p crabka-operator -- gen-crds deploy/crds` (or `./tools/regen-crds.sh`). Confirm `deploy/crds/crabka.io_kafkagrpcgateways.yaml` appears and `git diff --quiet deploy/crds` is clean after committing it.
- [ ] **Gates:** `cargo test -p crabka-operator --lib`; `cargo clippy -p crabka-operator --all-targets -- -D warnings`; `cargo fmt --check`; `git status` shows the new CRD YAML staged. Commit `feat(operator): KafkaGrpcGateway CRD types + generated manifest`.

---

## Task 3: gateway — `--broker-tls-*` flags + thread `.security()` into all Kafka clients

**Files:** Modify `crates/grpc-gateway/src/bin/gateway.rs`, `crates/grpc-gateway/src/config.rs`, and every site that builds a `Producer`/`Consumer`/`AdminClient` (`core/produce.rs`, `dedup/store.rs`, `dedup/mod.rs`/txn, `membership.rs`, `outbound.rs`, the ACL-refresh in `authz/`). **Needs Task 1.**

- [ ] **Step 1:** Add `clap` args (each with `env`): `--broker-tls-ca` (`CRABKA_GATEWAY_BROKER_TLS_CA`, `Option<PathBuf>` — trust roots verifying the broker), `--broker-tls-cert` (`…_BROKER_TLS_CERT`, client cert), `--broker-tls-key` (`…_BROKER_TLS_KEY`, client key), `--broker-tls-server-name` (`…_BROKER_TLS_SERVER_NAME`, `Option<String>` SNI). TLS-to-broker is enabled iff `--broker-tls-cert` AND `--broker-tls-key` are set (and `--broker-tls-server-name` provided); cert/key one-without-the-other is an error (mirror the serving-side rule).
- [ ] **Step 2:** Build `Option<ClientSecurity>` once in `main`:
```rust
let broker_security = match (&args.broker_tls_cert, &args.broker_tls_key) {
    (Some(c), Some(k)) => Some(crabka_client_core::security::ClientSecurity {
        protocol: crabka_security::ListenerProtocol::Ssl,
        tls: Some(crabka_client_core::security::TlsConnectorConfig {
            trust_roots_pem: args.broker_tls_ca.clone(),
            server_name: args.broker_tls_server_name.clone().ok_or_else(|| anyhow!("broker-tls-server-name required with broker TLS"))?,
            client_identity: Some((c.clone(), k.clone())),
        }),
        sasl: None, sasl_host: None,
    }),
    (None, None) => None,
    _ => anyhow::bail!("--broker-tls-cert and --broker-tls-key must be set together"),
};
```
Store on `GatewayConfig` (`pub broker_security: Option<ClientSecurity>`).
- [ ] **Step 3:** Thread it into EVERY client builder via `.maybe_security(config.broker_security.clone())` (bon's optional setter). Find all sites: `grep -rnE "Producer::builder|Consumer::builder|AdminClient::(builder|connect)" crates/grpc-gateway/src`. Each `Producer::builder()…`, `Consumer::builder()…`, and the admin connect must get the security. For `AdminClient::connect(brokers)` (ACL refresh) check its signature — if it takes no security, switch to its builder/`with_security` form, or extend the call. Confirm `DedupStore::write_claim`'s producer too.
- [ ] **Step 4:** Unit test in `gateway.rs` (or `config.rs`): a helper `build_broker_security(cert, key, ca, sni)` returns `Some(ClientSecurity{protocol: Ssl, tls: Some(..)})` when cert+key present, `None` when both absent, and errors on cert-without-key. Assert the protocol/identity. (Don't connect — just assert construction, like `producer_builder_accepts_security`.)
- [ ] **Gates:** `cargo test -p crabka-grpc-gateway`; `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`; `cargo fmt --check`. Commit `feat(gateway): mTLS to the broker via --broker-tls-* (ClientSecurity on all Kafka clients)`.

---

## Task 4: operator — `KafkaGrpcGateway` controller

**Files:** Create `crates/operator/src/controller/grpc_gateway.rs`; modify `crates/operator/src/controller/mod.rs` (`pub mod grpc_gateway;`). **Needs Task 2.** Reuse `controller/common.rs` helpers + `crabka_security::ca` + the version-gate pattern from `kafka_node_pool.rs`.

- [ ] **Step 1: render helpers (pure fns, unit-testable):**
  - `fn deployment(gw, parent, image, bootstrap, sni) -> Deployment` — `serde_json::from_value(json!({...}))`: replicas, `gateway_labels`, owner-ref, pod template with the gateway container (image, `args`/`env` per Design §"Config rendering": `$(POD_NAME)`/`$(POD_IP)` via `fieldRef`), the 5 volumes+mounts, `/healthz`+`/readyz` probes, containerPort 9500.
  - `fn service(gw, parent) -> Service` — ClusterIP, port 9500, selector = `gateway_labels`, owner-ref.
  - `fn config_secret(gw) -> Result<Secret>` — render `webhooks.toml` + `outbound.toml` from the spec using the `toml` crate into the exact `crates/grpc-gateway/src/{webhook_config.rs,outbound_config.rs}` schemas. (SecretRef *resolution* happens in reconcile, passed in as resolved values.)
  - `fn child_kafkauser(gw, parent_name) -> KafkaUser` — `authentication: tls`, CN = gw name, broad ALLOW ACLs (Topic:*/Group:*/TransactionalId:*/Cluster, all ops), label `crabka.io/cluster`, owner-ref → the gateway CR.
  - `fn gateway_labels(kafka, gw) -> BTreeMap<…>` (mirror `common::common_labels`, app = `crabka-grpc-gateway`).
- [ ] **Step 2: serving cert** — `fn ensure_serving_cert(secret_api, gw, ns, cluster_ca) -> Result<()>`: load cluster CA (`ca.crt`/`ca.key` from `<kafka>-cluster-ca`/`-cluster-ca-cert`), call `crabka_security::ca::issue_broker_cert(ca_cert, ca_key, &gw_name, &base_sans, &[], validity)` with `base_sans` = the Service DNS names; SSA the `<gw>-serving` Opaque Secret (`tls.crt`/`tls.key`), owner-ref → gw. Re-issue when `is_cert_expiring_soon`.
- [ ] **Step 3: `reconcile(gw, ctx)`** orchestrating Design §"Controller flow" steps 1–7: label→parent, version gate (set `KafkaVersionValid`/`Ready` conditions on block), SSA child KafkaUser + read back its Secret (`<gw>-broker`; if absent set `CertReady=False`, requeue 15s), `ensure_serving_cert`, resolve `secret_ref`s from same-ns Secrets + render+SSA `<gw>-config`, SSA Deployment + Service, read-back + `patch_status`. Requeue 30s. `error_policy` requeue 15s.
- [ ] **Step 4: `run(ctx)`** — `Controller::new(Api::<KafkaGrpcGateway>::all, …).owns(deployments).owns(services).owns(secrets).owns(kafkausers).watches(kafkas, …).run(reconcile, error_policy, Arc::new(ctx))`. Mirror `kafka_node_pool::run`.
- [ ] **Step 5:** unit tests for the pure render helpers (Deployment has the 5 mounts + the right env; Service selector matches; `config_secret` produces parseable TOML that round-trips through the gateway's `WebhooksFile`/`OutboundFile` — add `crabka-grpc-gateway` as a `dev-dependency` of the operator ONLY if needed for the round-trip, else assert the TOML string contains the expected keys; child KafkaUser has TLS auth + the ACLs).
- [ ] **Gates:** `cargo test -p crabka-operator --lib`; `cargo clippy -p crabka-operator --all-targets -- -D warnings`; `cargo fmt --check`. Commit `feat(operator): KafkaGrpcGateway controller (Deployment+Service+config+serving cert+child KafkaUser)`.

---

## Task 5: operator — run wiring + helm RBAC + values + config

**Files:** Modify `crates/operator/src/run.rs`, `crates/operator/src/config.rs`, `charts/crabka-operator/templates/clusterrole.yaml`, `charts/crabka-operator/values.yaml`, `charts/crabka-operator/templates/deployment.yaml`. **Needs Task 4.** (Disjoint from Task 6's `tests/`.)

- [ ] **Step 1:** `run.rs`: add the `tokio::spawn { controller::grpc_gateway::run(ctx.clone()).await }` block + the matching `tokio::select!` arm (mirror the pool/topic handles). Don't drop the handle.
- [ ] **Step 2:** `config.rs`: add `default_gateway_image: String` + `--default-gateway-image` (`env` mirror of `--default-broker-image`); a `DEFAULT_GATEWAY_IMAGE` const = `concat!("ghcr.io/robot-head/crabka-grpc-gateway:", env!("CARGO_PKG_VERSION"))` as the default. The controller (Task 4) reads `ctx.config.default_gateway_image` when `spec.image` is `None`.
- [ ] **Step 3:** `clusterrole.yaml`: add a rule for `apiGroups:["crabka.io"] resources:["kafkagrpcgateways","kafkagrpcgateways/status"]` (full verbs) and `apiGroups:["apps"] resources:["deployments"]` (full verbs — **currently missing**). Verify `kafkausers`, `services`, `secrets`, `configmaps` rules already exist (they do — don't duplicate).
- [ ] **Step 4:** `values.yaml`: add `gatewayImage: { repository: ghcr.io/robot-head/crabka-grpc-gateway, tag: "", pullPolicy: IfNotPresent }` (empty tag → chart appVersion). `templates/deployment.yaml`: pass `--default-gateway-image={{ .Values.gatewayImage.repository }}:{{ .Values.gatewayImage.tag | default .Chart.AppVersion }}` to the operator container args (mirror how `--default-broker-image` is passed; grep the template).
- [ ] **Gates:** `cargo build -p crabka-operator`; `cargo clippy -p crabka-operator --all-targets -- -D warnings`; `helm lint charts/crabka-operator` (if helm available; else `helm template` or skip with a note); `cargo fmt --check`. Commit `feat(operator): wire KafkaGrpcGateway controller (run + RBAC + gateway image config)`.

---

## Task 6: operator — `reconcile_gateway.rs` mock-harness tests

**Files:** Create `crates/operator/tests/reconcile_gateway.rs`; modify `crates/operator/tests/shared/mod.rs` (add `fake_deployment_body`, `fake_gateway_body`, reuse `fake_parent_kafka_body`). **Needs Task 4.** (Disjoint from Task 5.)

- [ ] **Step 1:** `shared/mod.rs`: add minimal JSON-body builders `fake_deployment_body(name, ns, ready_replicas)` (`apps/v1` Deployment with `status.readyReplicas`) and `fake_gateway_body(name, ns)` (a `KafkaGrpcGateway` with the `crabka.io/cluster` label + a uid). Reuse existing `fake_parent_kafka_body` (has `KafkaVersionValid=True` + `metadataVersion`) and `<kafka>-cluster-ca`/`-cluster-ca-cert` secret fakes (add minimal CA secret bodies if not present — generate a throwaway CA PEM via `rcgen` in a `OnceLock`, or embed a fixed test CA PEM).
- [ ] **Step 2:** `reconcile_gateway.rs` happy path — drive `controller::grpc_gateway::reconcile` once with a FIFO `MockRule` sequence: GET parent Kafka → GET/PATCH child KafkaUser → GET its Secret → GET cluster-ca secrets → PATCH `<gw>-serving` Secret → PATCH `<gw>-config` Secret → GET/PATCH Deployment → PATCH Service → PATCH `…/status`. Assert the reconcile returns `Action::requeue(30s)` and (by capturing the PATCH bodies via the mock) that the Deployment has the 5 mounts + the broker-TLS env, and the child KafkaUser has TLS auth. Keep `path_substr`s precise (substring match).
- [ ] **Step 3:** version-gate test — parent Kafka with `KafkaVersionValid=False` (or no `metadataVersion`) ⇒ the reconcile patches `Ready=False reason=WaitingForVersionValidation` and does NOT create the Deployment (assert no Deployment PATCH rule is consumed). 
- [ ] **Step 4:** Add `reconcile_gateway` to the operator's llvm-cov `--test` list in `.github/workflows/ci.yml` (per project memory — a new `tests/<x>.rs` not listed reports 0% patch). Grep ci.yml for the operator's existing `--test reconcile_*` enumeration and append.
- [ ] **Gates:** `cargo test -p crabka-operator --test reconcile_gateway`; then `cargo test -p crabka-operator`; clippy `--all-targets -D warnings`; fmt. Commit `test(operator): KafkaGrpcGateway reconcile (happy path + version gate)`.

---

## Final review + finish
Adversarial review focus: (1) **trust-root correctness** — serving cert = cluster CA, broker-client cert = clients CA (child KafkaUser), gateway trust-roots/client-ca wired to the right bundle; the gateway can actually mTLS-handshake the broker. (2) **No secrets in the CRD/etcd** — HMAC keys come via `secretRef`, resolved only at render time into the managed `<gw>-config` Secret. (3) **Config fidelity** — the rendered TOML round-trips through the gateway's `WebhooksFile`/`OutboundFile`; every gateway flag the operator sets is real. (4) **Version gate** adopted; no Deployment before parent Kafka is Ready. (5) **RBAC** — `deployments` rule added (was missing); CRD + status verbs present. (6) **CRD drift** — `deploy/crds/*.yaml` regenerated + committed; gen_crds test updated. (7) **client-core** mTLS path is sound (`with_client_auth_cert`) and the new field didn't break other constructors. (8) **broker NEVER modified.** Then push + PR stacked on #419 (rebase `--onto origin/main` when the parents merge).

## Self-review notes (author)
- **Broker untouched**: P9 touches client-core, gateway, operator, charts only.
- **Greenfield**: no compat shims; the CRD is new, `deploy/crds` regenerated wholesale.
- **Child-KafkaUser** delegation is the key simplification — reuses the entire clients-CA cert + ACL machinery instead of duplicating `issue_user_cert` + ACL provisioning in the gateway controller; the controller only adds serving-cert issuance.
- **Deferred** (noted, not in this slice): kind-based operator-e2e for the gateway; HPA/autoscaling; pod scheduling (nodeSelector/affinity) on the CRD; SASL-to-broker (only mTLS modelled); bearer JWKS/signed mode (gateway only has off/unsecured today).
- **Scale**: 6 tasks / 3 batches; the largest single PR of the gateway program (multi-crate). Each batch's file sets are disjoint per CLAUDE.md.
