# Crabka Operator — Strimzi-equivalent Roadmap (design)

**Date:** 2026-05-15
**Status:** Roadmap design, ready for first-slice plan
**Scope:** Long-horizon roadmap toward full Strimzi feature parity, expressed as numbered slices in Crabka's existing style. Identifies the first slice to drive into an implementation plan.

## Goal

Deliver a Rust Kubernetes operator that brings Crabka to feature-parity with [Strimzi](https://github.com/strimzi/strimzi-kafka-operator) for managing Kafka clusters on Kubernetes. This document is the long-form roadmap; each slice in it is a single PR sized comparably to slices 1–16 in the existing Crabka history.

## Decisions captured during brainstorm

1. **Scope:** Roadmap-level document covering full Strimzi parity, decomposed into ordered slices grouped into phases. First slice picked at the end of the doc.
2. **CRD compatibility:** Strimzi-*shaped* CRDs (same kinds, same field structure where it applies) under our own API group `crabka.io`. Not drop-in compatible with `kafka.strimzi.io/v1beta2`. A migration tool is a Phase 12 slice, not the primary surface.
3. **Crabka-core gaps:** The roadmap drives Crabka-core slices too. Operator slices and core slices are interleaved by dependency.
4. **Time horizon:** Single long roadmap to full parity, no MVP gate; first slice picked at the end.
5. **Decomposition style:** Feature-driven numbered slices in Crabka's existing rhythm (`Slice N: <feature>`). Each slice is one cohesive end-to-end PR.

## Architecture

### Process shape

One operator binary, multiple controllers composed in-process. Strimzi's split into separate Cluster / Topic / User operator binaries was driven by JVM memory and crash-isolation concerns that do not apply to Rust. Single `Deployment`, one image, namespace- or cluster-scoped via config flag `WATCH_NAMESPACES`.

### Crate layout

- New workspace member `crates/operator` producing the `crabka-operator` binary.
- Internal modules per CRD: `controller_kafka`, `controller_kafkanodepool`, `controller_kafkatopic`, `controller_kafkauser`, `controller_kafkarebalance`, `controller_kafkaconnect`, `controller_kafkaconnector`, `controller_kafkamirrormaker2`, `controller_kafkabridge`.
- Each module is a `kube-rs` `Controller` with its own reflector + work-queue + reconcile fn. They share a `kube::Client` but no in-memory state beyond kube-rs's caches.
- Same Cargo workspace as the broker. Same release cycle. Same `cargo` test invocation runs operator tests alongside core tests.

### Naming & API surface

- Crate / binary / container image: `crabka-operator`
- Container images for workloads: `crabka/broker`, `crabka/connect`, `crabka/operator`
- API group: `crabka.io`
- Schema versions evolve `v1alpha1` → `v1beta1` → `v1` as features stabilize
- CRD kinds: `Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaUser`, `KafkaRebalance`, `KafkaConnect`, `KafkaConnector`, `KafkaMirrorMaker2`, `KafkaBridge`
- CRD YAMLs ship in `deploy/crds/`. Operator does **not** self-install CRDs (matches Strimzi).

### Deployment artifacts

- Helm chart at `charts/crabka-operator/` for the operator itself.
- Cluster deployment happens via the `Kafka` CRD, not through a separate cluster chart.
- OLM bundle is a follow-up Phase 12 slice, not Phase 1.

### Reconciliation model

- Per-CRD `kube-rs` `Controller`. Reflector + work-queue + reconcile fn.
- Coordination via labels and owner references only — no shared in-memory state.
- Stateless operator: all truth in the K8s API. Restart-safe.
- `Lease`-based leader election for HA replicas.
- Workloads under a `Kafka` CR use upstream `StatefulSet` rather than re-implementing Strimzi's `StrimziPodSet`. Modern `StatefulSet` features (1.27+) close the gap that motivated `StrimziPodSet` originally.

### Rust dependencies

`kube`, `k8s-openapi`, `schemars`, `tokio`, `tracing`, `serde`, `prometheus-client`. No exotic picks; same ecosystem the broker already uses.

## Phase / slice breakdown

Current Crabka head is at Slice 16c. Operator work begins at Slice 17. Each slice below is one PR.

### Phase 1 — Operator foundation

| Slice | Title | Summary |
|------:|-------|---------|
| 17 | Operator runtime scaffold + Helm chart | kube-rs `Controller` plumbing, Lease leader election, healthz, RBAC, namespace-watch config, tracing, `/metrics` endpoint, kind-based CI smoke test. Ships a placeholder `Kafka` CRD that just logs reconciles — no workload yet. |

### Phase 2 — Cluster CRD core

| Slice | Title | Summary |
|------:|-------|---------|
| 18 | `Kafka` CRD minimal | KRaft mixed-mode cluster, ephemeral storage, one internal PLAINTEXT listener, headless `Service`, `ConfigMap` + cluster-ID `Secret` + `StatefulSet`, status subresource. |
| 19 | `KafkaNodePool` CRD | Controller-only / broker-only / mixed pools, one StatefulSet per pool, `Kafka.spec` references pools. |
| 20 | Pod templates | Affinity, tolerations, labels, annotations, resources on `Kafka` and `KafkaNodePool`. Cross-cutting field surface reused by every later workload CRD. |
| 21 | Rolling restart on config drift | Config-hash annotation, one-at-a-time restart waiting for readiness + ISR recovery. |
| 22 | **Crabka core:** `ControlledShutdown` handler | Operator can drain a broker before restart (KIP-baseline RPC). Unblocks slice 21's full safety. |
| 23 | Network policies | `Kafka.spec.kafka.networkPolicy` generates `NetworkPolicy` for broker/controller traffic. |

### Phase 3 — Day-2 cluster ops

| Slice | Title | Summary |
|------:|-------|---------|
| 24 | Persistent storage | PVC templates, `storageClass`, retain-vs-delete on cluster delete. |
| 25 | External listener — NodePort | Per-broker bootstrap services, advertised-listener computation. |
| 26 | External listener — LoadBalancer | Cloud-provider LB per broker + bootstrap LB. |
| 27 | External listener — Ingress / Route | SNI per broker on Ingress; OpenShift `Route`. |
| 28 | Version upgrades | Pinned `inter.broker.protocol.version`-style flag, ordered rolling upgrade, downgrade-window enforcement. |

### Phase 4 — Security & certificate management

| Slice | Title | Summary |
|------:|-------|---------|
| 29 | **Crabka core:** mTLS client authentication on listeners | Currently absent. Unblocks slices 30 and 37. |
| 30 | Cluster CA + clients CA generation | Operator-managed CA Secrets, keystore Secrets, renewal CronJob. Inter-broker mTLS using cluster CA. |
| 31 | Listener auth wiring (TLS + SCRAM-SHA-512) | Surface existing Crabka auth as CRD listener config. |
| 32 | **Crabka core:** SASL/SCRAM-SHA-256 | Port from SHA-512 path. |
| 33 | **Crabka core:** Certificate hot-reload | Swap server certs without restart. Required for non-disruptive CA rotation. |
| 34 | CA rotation orchestration | Coordinated cluster roll using slice 33's hot-reload. |

### Phase 5 — Topic + User

| Slice | Title | Summary |
|------:|-------|---------|
| 35 | `KafkaTopic` CRD | Unidirectional reconciliation (CreateTopics, AlterConfigs, CreatePartitions, DeleteTopics) via Crabka admin client. |
| 36 | `KafkaUser` — SCRAM-SHA-512 + ACLs | Both already present in Crabka. Operator generates user-credential `Secret`, manages ACLs. |
| 37 | `KafkaUser` — mTLS | Per-user cert from clients CA, exposed as `Secret`. Depends on slice 29. |
| 38 | `KafkaUser` — client quotas | Wire `AlterClientQuotas` (already in Crabka) from `KafkaUser.spec.quotas`. |

### Phase 6 — Observability

| Slice | Title | Summary |
|------:|-------|---------|
| 39 | **Crabka core:** Prometheus metrics exporter | Surface JMX-equivalent metrics named to match upstream Kafka. |
| 40 | `Kafka.spec.metricsConfig` | `ServiceMonitor` / `PodMonitor` generation + scrape config. |
| 41 | Configurable logging | `Kafka.spec.logging` → `tracing` env filter via `ConfigMap`. |
| 42 | **Crabka core:** OTLP distributed tracing | Optional but charted on roadmap. Surfaces via a follow-up CRD-config slice. |

### Phase 7 — Rebalance & reassignment

| Slice | Title | Summary |
|------:|-------|---------|
| 43 | **Crabka core:** Native rebalancer service | Goal-seeking partition placement built on existing KIP-455 + KIP-73 primitives. REST API: propose / dry-run / execute / status. Anomaly detection deferred. |
| 44 | `KafkaRebalance` CRD | Operator drives the rebalancer service. |

### Phase 8 — Storage gaps

| Slice | Title | Summary |
|------:|-------|---------|
| 45 | **Crabka core:** JBOD / multi-log-dir + KIP-113 | Per-partition log-dir placement, log-dir reassignment. |
| 46 | JBOD in `Kafka.spec.storage` | Multi-PVC per pod, per-broker log-dir balance. |
| 47 | **Crabka core:** Log compaction | `cleanup.policy=compact`, cleaner thread, tombstone retention. Exposed through existing `KafkaTopic`, no extra operator slice. |
| 48 | **Crabka core:** Tiered storage (KIP-405) | Large; likely splits into sub-slices when planned. An operator-surfacing follow-up slice (number assigned at plan time) lands after the core work. |

### Phase 9 — Auth & authorization extensions

| Slice | Title | Summary |
|------:|-------|---------|
| 49 | **Crabka core:** SASL/OAUTHBEARER | |
| 50 | `KafkaUser` OAuth + listener OAuth config | |
| 51 | **Crabka core:** Delegation tokens | Surfaced by a follow-up `KafkaUser` field. |
| 52 | **Crabka core:** SASL/GSSAPI (optional) | Only if user demand emerges. |
| 53 | Authorization plugin: OPA bridge | |
| 54 | Authorization plugin: Keycloak | |

### Phase 10 — Ecosystem: Connect

| Slice | Title | Summary |
|------:|-------|---------|
| 55 | **Crabka core:** Kafka Connect equivalent — runtime | Distributed worker: REST API, connector lifecycle, task assignment, JSON/Avro/Protobuf converters. |
| 56 | `KafkaConnect` CRD | Operator deploys Connect worker `Deployment`s. |
| 57 | `KafkaConnect.spec.build` | Declarative plugin list, image build via Kaniko or BuildConfig. |
| 58 | `KafkaConnector` CRD | Operator submits connector configs to Connect REST API. |

### Phase 11 — Ecosystem: MirrorMaker2 + Bridge

| Slice | Title | Summary |
|------:|-------|---------|
| 59 | **Crabka core:** MirrorMaker2 (on Connect) | |
| 60 | `KafkaMirrorMaker2` CRD | |
| 61 | **Crabka core:** REST bridge (HTTP→Kafka proxy) | |
| 62 | `KafkaBridge` CRD | |

### Phase 12 — Parity tail

| Slice | Title | Summary |
|------:|-------|---------|
| 63 | **Crabka core:** Static membership (KIP-345) + `KafkaUser`/`KafkaTopic` follow-ups | |
| 64 | **Crabka core:** KIP-848 next-gen consumer group protocol | |
| 65 | **Crabka core:** KIP-841 force-elect / unclean-recovery toggle + `Kafka` CRD field | |
| 66 | **Crabka core:** IPv6 ACL host filter + `KafkaUser` ACL acceptance | |
| 67 | **Crabka core:** Broker-side recompression | |
| 68 | Optional: Schema Registry equivalent + CRD | Whether to take on a Schema Registry is debatable; left as optional. |

## Crabka-core dependency map

The operator features intertwine with Crabka-core slices. The table below makes each operator → core dependency explicit so each slice is fully deliverable when its turn comes.

| Core slice | Capability | Unblocks operator slice(s) |
|-----------:|------------|----------------------------|
| 22 | `ControlledShutdown` request handler | 21 (rolling restart with graceful drain) |
| 29 | mTLS client authentication | 30 (cluster CA + inter-broker mTLS), 37 (`KafkaUser` mTLS) |
| 32 | SASL/SCRAM-SHA-256 | follow-up to 31 (listener SCRAM-256), follow-up to 36 (KafkaUser SCRAM-256) |
| 33 | Certificate hot-reload | 34 (non-disruptive CA rotation) |
| 39 | Prometheus metrics exporter | 40 (`Kafka.spec.metricsConfig`) |
| 42 | OTLP tracing | 41-follow-up |
| 43 | Native rebalancer service | 44 (`KafkaRebalance`) |
| 45 | JBOD / multi-log-dir | 46 (`Kafka.spec.storage` JBOD) |
| 47 | Log compaction | exposed via existing `KafkaTopic`; no extra slice |
| 48 | Tiered storage | `Kafka.spec.storage.tieredStorage` operator-surfacing follow-up slice |
| 49 | SASL/OAUTHBEARER | 50 (`KafkaUser` OAuth) |
| 51 | Delegation tokens | future `KafkaUser` field |
| 52 | SASL/GSSAPI | future `KafkaUser` Kerberos field |
| 55 | Connect runtime | 56–58 |
| 59 | MirrorMaker2 | 60 |
| 61 | REST bridge | 62 |
| 63 | Static membership | future `KafkaUser`/`KafkaTopic` fields |
| 64 | KIP-848 | none direct |
| 65 | KIP-841 | `Kafka` CRD field exposure |
| 66 | IPv6 ACL host filter | `KafkaUser` ACL acceptance |
| 67 | Broker-side recompression | exposed via existing topic config |

**Pure operator work** (no Crabka-core dependency): all of Phase 1, Phase 2 except slice 22, Phase 3, slices 30 + 31 of Phase 4, slices 35 + 36 + 38 of Phase 5.

**Sequencing implication:** slices 17–21, 23–28, 30, 31, 35, 36, 38 (sixteen operator slices) ship before any operator-driven Crabka-core work begins. Slice 22 (`ControlledShutdown`) is a small core slice that slots in early for slice 21's safety.

## First slice — Slice 17

The implementation plan that follows this design covers Slice 17 only.

### Goal

Land an operator binary in the workspace that runs in a kind cluster, watches a placeholder `Kafka` CRD, logs reconciles, and is packaged as a Helm chart. No broker workload yet — the controller's reconcile fn is a stub that updates status and returns. Subsequent slices fill in the workload.

### Deliverables

- New workspace member `crates/operator` producing the `crabka-operator` binary.
- Library modules: `controller`, `context` (shared `kube::Client` + config), `leader_election`, `health`, `telemetry` (tracing + `/metrics`).
- Placeholder CRD: `Kafka` (`crabka.io/v1alpha1`) — minimal schema (`spec.kafkaVersion`, `status.conditions: []`). Generated via `kube-rs` `CustomResource` derive + `schemars`.
- CRD YAML at `deploy/crds/crabka.io_kafkas.yaml`, regenerated by a `cargo xtask gen-crds` task. CI fails on drift, mirroring the protocol-codegen pattern.
- Helm chart at `charts/crabka-operator/` with `Deployment`, `ServiceAccount`, `ClusterRole` + `Role` + bindings, `Service` for `/metrics`, optional `ServiceMonitor` template. Values cover image, watch-namespaces, replica count, resources.
- Multi-stage `Dockerfile.operator` producing a minimal distroless-cc image.
- Tests:
  - Unit: reconciler stub against a faked `kube::Client` via `tower::ServiceExt::mock_service`.
  - Integration: kind-cluster CI job that installs the chart, applies a `Kafka` resource, asserts the operator sets `status.conditions[0].type == "Ready"`.
  - Helm: `helm lint` + `helm template` in CI.

### Out of scope for Slice 17

Any broker workload reconciliation (StatefulSet, Service, ConfigMap) — that's Slice 18. Any other CRD. Multi-namespace watching beyond a flag. Admission webhooks. CRD conversion webhooks.

### Open questions to resolve in the implementation plan

- Exact `kube` and `k8s-openapi` minor versions (latest stable at plan time).
- Whether to use `kube::runtime::Controller` directly or layer a higher-level abstraction. Default: raw `Controller`, no abstraction yet.
- Whether the Helm chart lives in-repo or in a separate `charts` repo. Default: in-repo under `charts/`, published to GitHub Pages via a release workflow in a later slice.

### Acceptance criteria

1. `cargo build -p crabka-operator` produces the binary; `cargo test -p crabka-operator` passes.
2. `helm lint charts/crabka-operator` passes; `helm template` renders valid manifests.
3. CI kind job: applies CRD YAML, installs chart, applies a stub `Kafka` resource, waits for `status.conditions[?type=="Ready"].status == "True"`, passes.
4. Operator restarts cleanly with two replicas (no leader-election split-brain on rolling restart).
5. `/metrics` serves Prometheus format; `/healthz` and `/readyz` return 200.

## Testing & validation strategy (cross-cutting)

Three layers, mirroring the broker's existing test taxonomy:

1. **Unit tests** in each controller. Fake `kube::Client` via `tower::ServiceExt::mock_service`. Drive reconcile fn against canned inputs; assert intent (which K8s objects would be created/patched). Fast, deterministic, no cluster.
2. **Envtest-style integration** using a real K8s API server but no nodes — either `kube`'s test harness or a vendored `etcd` + `kube-apiserver` binary pair. Tests CRD schema validation, controller wiring, owner references, watch behavior. No actual broker pods.
3. **kind-cluster end-to-end** in CI for each slice's golden path. Real pods, real broker, JVM admin tool assertions where applicable — same pattern as the existing `kafka-topics` / `kafka-configs` differential acceptance tests. One e2e per slice minimum.

**CRD schema validation** is generated from Rust types via `schemars`. `cargo xtask gen-crds` regenerates `deploy/crds/*.yaml`; CI fails on drift.

**Upgrade testing** lives at the slice level for every slice that changes CRD schema or reconciler behavior in a user-visible way: install previous release chart, apply CRs, install new release chart, assert no spurious restarts and CRs still reconcile.

## Non-goals

- **ZooKeeper-mode anything.** Crabka is KRaft-only; the operator refuses a `Kafka` CR specifying ZK and does not carry Strimzi's ZK lineage in CRD fields.
- **Drop-in YAML compatibility with `kafka.strimzi.io/v1beta2`.** Strimzi-shaped, not Strimzi-compatible. A migration tool (`crabka migrate strimzi-to-crabka manifest.yaml`) is a Phase 12 slice.
- **Strimzi's `StrimziPodSet`.** Upstream `StatefulSet` only.
- **Embedding Cruise Control.** Rebalancer is Crabka-native (slice 43), not a CC fork.
- **Embedding the JVM JMX exporter.** Metrics come from Crabka's own exporter (slice 39).
- **Separate operator-only repo.** Same Cargo workspace, same release cycle.
- **Webhook-based field defaulting / conversion in Phase 1.** Defaults are computed in the reconciler. Webhooks may appear once schema versions diverge post-`v1alpha1`.
- **Multi-cluster federation** (one operator managing Kafka clusters across multiple K8s clusters). Single-cluster operator only.
