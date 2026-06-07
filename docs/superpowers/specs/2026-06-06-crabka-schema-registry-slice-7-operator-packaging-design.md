# Crabka Schema Registry — Slice 7: Operator CRD + Packaging (design)

- **Status:** Approved (brainstorm); ready for an implementation plan.
- **Scope:** Make the standalone `crabka-schema-registry` service deployable on
  Kubernetes: a `SchemaRegistry` operator CRD + reconciler, a melange/apko
  container image, a Helm chart + generated CRD manifest, docs, and the README
  capability flip (`❌ → ✅`). This is the final slice of the 7-slice roadmap.
- **Depends on:** slices 1–6 (the SR crate, its HA election + write-forwarding,
  and its security CLI). Stacked on `claude/schema-registry-slice-6` (#423);
  rebase onto `main` when #423 merges.
- **Validation oracle:** unlike the wire/compat slices there is no Confluent
  byte-exactness to match here — the oracles are (a) the existing operator
  reconcile patterns (mirror the `Kafka` / `KafkaTopic` CRDs) and (b) a real
  kind-cluster e2e that deploys a `SchemaRegistry` and does a schema round-trip.

---

## 1. Background

`crabka-schema-registry` is a standalone Confluent-compatible registry: a Kafka
*client* of the Crabka broker whose state lives in the `_schemas` topic. Slices
1–6 made it functional (3-format registry + compat, deletes/modes/lookups,
references, HA via the broker's `"sr"` Kafka-group election + write-forwarding,
and security: REST authn/authz/TLS + SR↔broker client auth). It is configured
entirely via **CLI flags / env** (slice 6's `crates/schema-registry/src/cli.rs`
`SecurityCliInput` → `SecurityConfig`); there is no config-file loader.

The operator (`crates/operator/`) already manages the broker via a `Kafka` CRD
(+ `KafkaNodePool`, `KafkaTopic`, `KafkaUser`, `KafkaRebalance`) with a kube-rs
reconciler. No `SchemaRegistry` CRD exists yet. Packaging is **melange + apko**
(Wolfi), not Dockerfiles; the operator + broker ship as OCI images built in CI.
The grpc-gateway (the most recent standalone service) got *neither* a CRD nor a
chart, so this slice is the first new service to receive the full treatment.

## 2. Goals / non-goals

**Goals**
- A `SchemaRegistry` CRD (`crabka.io/v1alpha1`) with first-class typed fields for
  the SR config surface that the binary actually supports today.
- A reconciler that renders a `Deployment` + `Service` + `ConfigMap` + `Secret`
  from a `SchemaRegistry` CR, associated with a managed `Kafka` via the
  `crabka.io/cluster` label (operator-native), gated on the Kafka being Ready.
- A melange/apko image for `crabka-schema-registry`, built in CI.
- A standalone Helm chart + generated CRD manifest + operator RBAC.
- Docs + README capability flip.
- Validation: mock-client reconcile unit tests + a kind e2e round-trip.

**Non-goals (YAGNI / deferred)**
- **JWKS-backed Bearer auth.** SR's `--bearer` supports `off|unsecured` only
  today. The CRD models `bearer.mode: unsecured` (+ `principalClaim`); a
  production JWKS mode needs the (already-present) `crabka-security` JWKS
  validator wired into SR's CLI — a small follow-up, deliberately **out of scope**
  to keep this slice "operator + packaging." (Flagged for review — see §11.)
- **Operator-minted serving certificates.** Slice 7 references a user/cert-manager
  Secret for the HTTPS cert (`tls.secretName`). Minting from the cluster CA
  (as the `Kafka` CRD does) is a noted future extension.
- **A `Deployment`-managed autoscaler / PodDisruptionBudget / NetworkPolicy.**
- **No back-compat shims** (greenfield project rule): new CRD, just define it.

## 3. Architecture & components

Five units, each independently reviewable:

| Unit | Files | Responsibility |
|---|---|---|
| CRD | `crates/operator/src/crd/schema_registry.rs` (+ `crd/mod.rs` re-exports, `gen_crds.rs` registration) | The `SchemaRegistry` Spec/Status types + generated CRD YAML. |
| Reconciler | `crates/operator/src/controller/schema_registry.rs` (+ `controller/mod.rs`; child-resource renderers near `controller/common.rs`) | Watch `SchemaRegistry`+`Kafka`; render+SSA the child resources; patch status. |
| Packaging | `packaging/melange/crabka-schema-registry.yaml`, `packaging/apko/crabka-schema-registry.yaml`, `.github/workflows/operator-e2e.yml` (build-images loop) | Build the OCI image in CI. |
| Helm + manifests | `charts/crabka-schema-registry/**`, `deploy/crds/crabka.io_schemaregistries.yaml`, `charts/crabka-operator/templates/clusterrole.yaml` (RBAC) | Non-operator install path + CRD manifest + operator RBAC. |
| Docs | `website/` (or `docs/`) deploy page, `README.md` capability table | Document deployment; flip `❌ → ✅`. |

**Workload = `Deployment` (not `StatefulSet`).** SR is stateless — all registry
state is in `_schemas` and leader election runs through the broker's `"sr"`
group (slice 5). N replicas all join the group; one is elected primary; the rest
forward mutating requests to the primary's advertised URL. No PVC.

### Key decisions (alternatives considered)
- **Deployment vs StatefulSet** → Deployment (stateless). *StatefulSet/PVC
  rejected — no local persistent state.*
- **Config delivery: CRD → container args/env (+ Secret)** vs adding a
  config-file loader to SR → args/env (reuses slice-6's existing CLI surface).
  *Config-file loader rejected — scope creep into the SR crate.*
- **Kafka association: `crabka.io/cluster` label** (mirrors `KafkaTopic`,
  `controller/topic.rs`) vs a `spec.kafkaRef` field → label, for consistency
  with existing CRDs. A `spec.bootstrapServers` override covers external Kafka.
- **TLS serving cert: referenced Secret** vs operator-minted → Secret (simplest;
  cert-manager-friendly). Minting deferred.

## 4. The `SchemaRegistry` CRD

`crabka.io/v1alpha1`, kind `SchemaRegistry`, plural `schemaregistries`,
shortname `sr`, namespaced, with a status subresource and printer columns
(Replicas, Ready, Age). Conditions reuse the shared `crate::crd::KafkaCondition`.

```rust
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistrySpec {
    /// Stateless replicas; all join the election group. Default 1.
    pub replicas: i32,                                  // default 1
    /// Image; defaults to the operator's --default-schema-registry-image flag.
    pub image: Option<String>,

    // ---- Kafka linkage --------------------------------------------------
    /// Override for an external/unmanaged Kafka. When unset, bootstrap + the
    /// SR↔broker client security are derived from the Kafka named by the
    /// `crabka.io/cluster` label. The override path targets PLAINTEXT/
    /// unauthenticated external brokers in slice 7; secured external brokers
    /// (explicit client-credential CRD fields) are a future enhancement — the
    /// managed-`Kafka` (label) path is the secured one.
    pub bootstrap_servers: Option<String>,
    pub schemas_topic: Option<String>,                 // default "_schemas"
    pub schemas_topic_replication_factor: Option<i32>, // default = cluster default
    pub group_id: Option<String>,                      // default "schema-registry"

    // ---- Server-side security (maps to slice-6 SR CLI) ------------------
    pub tls: Option<SchemaRegistryTls>,                // None = plain HTTP
    pub authentication: Option<SchemaRegistryAuthn>,
    pub authorization: Option<SchemaRegistryAuthz>,

    // ---- Pod knobs (YAGNI-minimal) -------------------------------------
    pub resources: Option<k8s_openapi::api::core::v1::ResourceRequirements>,
    pub template: Option<PodTemplateOverrides>,        // labels/annotations/nodeSelector/tolerations
}

pub struct SchemaRegistryTls {
    pub secret_name: String,                           // tls.crt + tls.key
    pub client_auth: Option<TlsClientAuth>,            // Disabled | Optional | Required  (default Disabled)
    pub client_ca_secret_name: Option<String>,         // ca.crt to verify client certs
}
pub enum TlsClientAuth { Disabled, Optional, Required } // serde rename_all = camelCase

pub struct SchemaRegistryAuthn {
    pub require_auth: bool,                             // reject Anonymous with 401
    pub realm: Option<String>,                          // WWW-Authenticate realm
    pub basic: Option<BasicAuthn>,
    pub bearer: Option<BearerAuthn>,
}
pub struct BasicAuthn {
    pub users_secret_name: String,                     // htpasswd-style user:cred entries
    pub realm: Option<String>,
}
pub struct BearerAuthn {
    pub mode: BearerMode,                              // Unsecured (only mode SR supports today)
    pub principal_claim: Option<String>,
}
pub enum BearerMode { Unsecured }                      // Jwks deferred (see §2 non-goals)

pub struct SchemaRegistryAuthz {
    pub enabled: bool,
    pub super_users: Vec<String>,
    pub acl_refresh_seconds: Option<i64>,              // default 30
}

pub struct SchemaRegistryStatus {
    pub conditions: Vec<crate::crd::KafkaCondition>,   // KafkaReady, Available, Ready
    pub observed_generation: Option<i64>,
    pub replicas: Option<i32>,
    pub ready_replicas: Option<i32>,
    pub url: Option<String>,                           // in-cluster REST URL
}
```

Field-shape conventions match the existing CRDs: every optional field is
`#[serde(default, skip_serializing_if = "Option::is_none")]`; enums use
`#[serde(rename_all = "camelCase")]`.

**Secrets rule:** credentials never appear inline — `basic.usersSecretName`,
`tls.secretName`, `tls.clientCaSecretName` reference `Secret`s; the reconciler
mounts them. Non-secret knobs are inline typed fields.

**This is the full typed security surface SR supports today** (require-auth,
realm, basic, bearer-unsecured, TLS + client-auth, authz super-users + refresh).
SR↔broker *client* security (SASL/mTLS) is **derived** from the managed Kafka's
internal listener — not a CRD field (see §5).

## 5. The reconciler

`run/reconcile/error_policy` mirroring `controller/topic.rs`:

- **Watch:** `Controller::new(Api::<SchemaRegistry>::all)` `.watches(Api::<Kafka>)`
  so a cluster status change re-runs dependents. `error_policy` → `Action::requeue(15s)`.
- **Association + bootstrap:** require `metadata.labels["crabka.io/cluster"]`
  (else set a `Ready=False, reason=MissingClusterLabel` condition, no children);
  `kafka_api.get_opt(cluster)`; derive bootstrap via the internal listener
  (reuse `internal_listener_bootstrap`); if not Ready → `KafkaReady=False`,
  requeue. `spec.bootstrapServers`, when set, overrides (external Kafka path).
- **SR↔broker client security:** derive from the Kafka's internal-listener auth
  (the same security the operator's own admin client uses for that cluster) and
  provision the matching credentials into the SR pod — i.e. set
  `--kafka-security-protocol/--kafka-sasl-*` + mount a client-cert/CA Secret as
  needed. (Implementation detail resolved in the plan by reading how
  `admin_client_for` / the broker internal listener authenticate; for a
  PLAINTEXT internal listener this is a no-op.)
- **Children (all `OwnerReference`'d to the CR, SSA-applied via `apply_object`
  with `field_manager = crabka-operator`):**
  - **`ConfigMap`** — non-secret config rendered to container **args/env**:
    bootstrap, schemas topic + RF, group id, TLS client-auth mode, authz
    enabled/super-users/refresh, bearer mode + principal-claim, require-auth,
    realm. (SR has no config file; the reconciler maps CRD → CLI flags.)
  - **`Secret`** (or env-from referenced Secrets) — basic-auth users, SR↔broker
    SASL credentials. Referenced TLS/clientCA/usersSecret Secrets are mounted,
    not copied.
  - **`Deployment`** — `replicas`; the SR image; args/env from the ConfigMap +
    `--advertised-url=$(SCHEME)://$(POD_NAME).<headless-svc>.<ns>.svc.cluster.local:<port>`
    (forwarding, slice 5; `POD_NAME` via the downward API); TLS cert volume from
    `tls.secretName`; `securityContext` nonroot 65532 (matching apko);
    readiness/liveness probe on the REST port (`GET /` returns `{}`, slice 1);
    `spec.resources`/`template` overrides.
  - **`Service`** — a **headless** `Service` (`clusterIP: None`) so each pod is
    addressable at a stable DNS name for write-forwarding, plus a normal
    `ClusterIP` Service as the client entry point. The REST port is the SR
    default (Confluent `8081`; confirmed against the SR binary in the plan).
- **Status:** patch `KafkaReady`, `Available` (≥1 ready replica from the
  Deployment status), `Ready` (rollup), `replicas`/`readyReplicas`, and the
  in-cluster `url`. Use the shared `condition(...)` + `patch_status` helpers and
  set `observedGeneration`.

## 6. Packaging

Clone the broker recipes:
- `packaging/melange/crabka-schema-registry.yaml` — pinned Rust toolchain,
  `cargo build --release --bin crabka-schema-registry -p crabka-schema-registry`,
  install to `/usr/bin/crabka-schema-registry`.
- `packaging/apko/crabka-schema-registry.yaml` — Wolfi base + ca-certificates +
  tzdata, `entrypoint: /usr/bin/crabka-schema-registry`, run-as nonroot 65532,
  `cmd: run`, x86_64.
- `.github/workflows/operator-e2e.yml` — add `crabka-schema-registry` to the
  `build-images` recipe loop (apk + OCI tarball uploaded for the e2e job).

## 7. Helm chart + manifests

- `charts/crabka-schema-registry/` — standalone chart for non-operator installs
  pointing at an external/managed bootstrap: `Chart.yaml`, `values.yaml`
  (image, replicas, resources, bootstrap, tls/auth secret refs, securityContext),
  `templates/{deployment.yaml,service.yaml,serviceaccount.yaml,_helpers.tpl}`.
- `deploy/crds/crabka.io_schemaregistries.yaml` — generated by `gen_crds`
  (applied before the chart, matching the operator's CRD-apply step).
- `charts/crabka-operator/templates/clusterrole.yaml` — add
  `schemaregistries` (+ `/status`, `/finalizers`) to the operator ClusterRole.
- Operator CLI: add `--default-schema-registry-image` (mirroring
  `--default-broker-image`) so the reconciler has a default image.

## 8. Docs + README

- A deploy page (under `website/`/docs, following the docs-site pattern) with a
  `SchemaRegistry` CR example + the Helm install path.
- Flip the README capability table `| Schema Registry | ❌ |` → `✅`.
- The `crabka-docgen` CRD reference auto-generates the SchemaRegistry CRD docs if
  it enumerates CRDs (verify in the plan; extend if needed).

## 9. Testing

- **Reconcile unit tests** — `crates/operator/tests/reconcile_schema_registry.rs`
  using the existing mock-client FIFO harness (`tests/shared/`):
  - CR + `crabka.io/cluster` label + a Ready `Kafka` → asserts the rendered
    `Deployment` (replicas, image, args, advertised-url, TLS mount), `Service`
    (headless + ClusterIP), `ConfigMap`, `Secret`, owner-refs, and the
    `Ready/Available/KafkaReady` conditions.
  - Not-Ready (or missing) Kafka → gate: `KafkaReady=False`, no/again-requeued.
  - Missing `crabka.io/cluster` label → `Ready=False, MissingClusterLabel`.
  - Full-typed-security CR (tls + basic + authz + bearer) → asserts every field
    renders to the correct arg / mounted secret key.
- **CRD schema test** — assert `gen_crds` emits a `SchemaRegistry` CRD that
  round-trips (kind/group/version/printer-columns present).
- **Kind e2e** (`operator-e2e.yml`, CI-only) — build the SR image; apply a
  `Kafka` + a labeled `SchemaRegistry`; wait for `Ready`; **register a schema and
  fetch it back** over the deployed `Service` (a real REST round-trip). This is
  the proof behind the README `✅`.

## 10. Phasing (one PR, batched)

| Batch | Tasks | Files (non-overlapping within a batch) |
|---|---|---|
| 1 | Packaging | `packaging/melange/*`, `packaging/apko/*`, `operator-e2e.yml` build-images |
| 2 | CRD + gen + schema test | `crd/schema_registry.rs`, `crd/mod.rs`, `gen_crds.rs`, generated `deploy/crds/*.yaml`, CRD schema test |
| 3 | Reconciler + unit tests | `controller/schema_registry.rs`, `controller/mod.rs`, `controller/common.rs` (renderers), `tests/reconcile_schema_registry.rs`, operator `--default-schema-registry-image` flag |
| 4 | Helm + RBAC | `charts/crabka-schema-registry/**`, `charts/crabka-operator/templates/clusterrole.yaml` |
| 5 | e2e + docs + README | `operator-e2e.yml` e2e steps, docs page, `README.md` |

Batches 1 and 2 don't touch overlapping files → parallelizable. Batch 3 depends
on 2 (the CRD types). Batches 4–5 depend on 3.

## 11. Open items for review

1. **JWKS Bearer** — modeled as deferred (CRD has `bearer.mode: unsecured` only,
   since that's all SR's CLI supports). Option to pull a small JWKS-wiring task
   into slice 7 (the validator already exists in `crabka-security`) if you want
   the CRD's bearer to be production-grade now. **Default: defer.**
2. **TLS serving cert** — referenced Secret (cert-manager/user-supplied), not
   operator-minted. **Default: Secret ref.**
3. **REST port** — assumed Confluent default `8081`; the plan confirms against
   the SR binary's actual listen port.
