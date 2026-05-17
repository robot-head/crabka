# Crabka Operator — listener trilogy (slices 25a / 25 / 27) — design

**Date:** 2026-05-17
**Status:** Approved, ready for implementation plan
**Scope:** Adds the `Kafka.spec.listeners` schema and operator support for internal, NodePort, and LoadBalancer external listeners; lays the schema (but defers the reconcile) for Ingress and OpenShift Route. Includes the Crabka-core broker change needed to consume the listener config (slice 25a).

## Goal

Make Crabka clusters reachable from outside the Kubernetes cluster with the same operator-driven UX Strimzi users expect, expressed through a Strimzi-shaped `Kafka.spec.listeners` schema. Land the schema and external listener types most users actually need (NodePort + LoadBalancer) in plaintext; defer TLS/auth wiring and Ingress/Route reconcile to Phase 4 / a follow-up slice.

## Decisions captured during brainstorm

1. **Single spec, multiple slices.** This document covers all three roadmap slices in the listener family. Implementation lands in two operator slices (25, 27) plus one Crabka-core slice (25a). Slice 27 is described here but its implementation plan is deferred until Phase 4 lands TLS.
2. **Broker config delivery: TOML file.** The operator writes a per-broker TOML file into a ConfigMap; the broker reads it via a new `--config-file` flag and `serde + toml`. Chosen over Kafka-style `server.properties` for parser simplicity (no `=`/escape/comment-rules code), Rust ergonomics, and because the broker config file is *internal* — not part of any Kafka wire-protocol compatibility constraint.
3. **Plaintext-only this chunk.** No TLS, no SASL/SCRAM, no inter-broker mTLS in 25/25a. `tls: true` on the schema is rejected at reconcile with a status condition. The Phase 4 slices (30/31) wire authentication later.
4. **Slice 27 schema landed early; reconcile deferred.** `type: ingress | route` is accepted by the CRD schema in slice 25 so users see a forward-stable surface, but the reconciler rejects those types with `ListenersValid=False reason=IngressDeferred` until slice 27 implements them.
5. **NodePort + LoadBalancer combined into one operator slice (25).** Their object topologies are structurally identical (one per-broker Service + one bootstrap Service; only `type` and advertised-host derivation differ). Bundling avoids a duplicate scaffold PR.
6. **Reconciler-driven advertised computation.** The operator watches `Service` and `Node`, computes advertised host:port per (broker, listener), writes the ConfigMap, and relies on slice 21's config-hash rolling-restart to roll on changes. No init container queries the K8s API.
7. **Flat CRD shape.** `listeners` lives at `Kafka.spec.listeners`, not nested under `spec.kafka`. Internally consistent with slice 19's already-flat `kafkaVersion` and `config` fields. Phase 12 migration tool can rewrite during Strimzi import.

## Architecture

### Slice split

| # | Title | Crate | Approx size |
|---|-------|-------|------------|
| 25a | Broker `--config-file` (TOML) + multi-listener wiring | `crabka-broker` | ~0.8x |
| 25 | Operator: `Kafka.spec.listeners` schema, internal/nodeport/loadbalancer reconcile, per-broker Services, advertised-listener computation, ConfigMap rewrite | `crabka-operator` | ~1.5x |
| 27 | Operator: Ingress (SNI) + OpenShift Route reconcile | `crabka-operator` | ~1x (deferred — plan written after Phase 4) |

Slice 25 depends on slice 25a (must land first). Slice 27 depends on Phase 4 slices 30/31.

### CRD schema (slice 25)

Added to `KafkaSpec`:

```rust
#[serde(default, skip_serializing_if = "Vec::is_empty")]
pub listeners: Vec<Listener>,

#[serde(default, skip_serializing_if = "Option::is_none")]
pub inter_broker_listener_name: Option<String>,
```

```rust
#[derive(CustomResource, Deserialize, Serialize, JsonSchema, …)]
pub struct Listener {
    /// Unique within the cluster. Alphanumeric + `-`, ≤25 chars. Used as
    /// the Kafka listener name; surfaces in `bootstrap.servers`-style URLs.
    pub name: String,

    /// Container port the broker binds. Unique within the cluster.
    pub port: i32,

    /// Listener type. `internal` is in-cluster ClusterIP/headless;
    /// `nodeport`/`loadbalancer` create external Services; `ingress`/`route`
    /// are accepted by the schema in slice 25 but rejected at reconcile
    /// until slice 27.
    pub r#type: ListenerType,

    /// Must be `false` in this chunk; rejected at reconcile when `true`.
    /// Phase 4 (slices 30/31) lifts this.
    #[serde(default)]
    pub tls: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<ListenerConfiguration>,
}

pub enum ListenerType { Internal, Nodeport, Loadbalancer, Ingress, Route }

pub struct ListenerConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<BootstrapConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<BrokerOverride>,
}

pub struct BootstrapConfig {
    pub node_port: Option<i32>,           // nodeport
    pub load_balancer_ip: Option<String>, // loadbalancer
    pub host: Option<String>,             // ingress/route (slice 27)
    pub annotations: Option<BTreeMap<String, String>>, // bootstrap Service annotations
    pub labels: Option<BTreeMap<String, String>>,
}

pub struct BrokerOverride {
    pub broker: i32,                       // matches the broker's node id
    pub advertised_host: Option<String>,
    pub advertised_port: Option<i32>,
    pub node_port: Option<i32>,            // nodeport: pin a specific nodePort
    pub load_balancer_ip: Option<String>,  // loadbalancer: pin LB IP per broker
    pub host: Option<String>,              // ingress/route (slice 27)
}
```

#### Default behavior

If `spec.listeners` is empty (or absent), the operator synthesizes a single internal listener equivalent to slice 19's hardcoded default:

```yaml
- name: PLAIN
  port: 9092
  type: internal
  tls: false
```

This is critical for the slice-25 upgrade test: an existing slice-19/20/21/24 cluster must roll *zero* pods on operator upgrade. The synthesized listener must produce a byte-identical TOML to what the new code would emit, so the slice-21 config-hash is unchanged.

#### Validation (status condition `ListenersValid`)

Rejected at reconcile (no Services or ConfigMap rendered while invalid):

- duplicate `listener.name` across the list
- duplicate `listener.port` across the list
- `tls: true` on any listener
- `type: ingress` or `type: route` on any listener
- duplicate `broker` ids in `configuration.brokers`
- `inter_broker_listener_name` references a missing or non-internal listener

When `spec.listeners` is non-empty and `inter_broker_listener_name` is `None`, the operator picks the first listener whose `type == internal`. If no internal listener exists, validation fails (`reason=NoInternalListener`).

### Broker config file (slice 25a)

New CLI flag on `crabka-broker`:

```
crabka-broker --config-file=/path/to/broker.toml [--broker-id N]
```

Mutually exclusive with `--listen-addr` / `--advertised-listener`. CLI flags that *don't* overlap with the file (e.g. `--broker-id`, `--metrics-listen-addr`) still apply and override file values where both are set.

File format — `serde + toml`:

```toml
broker_id = 0                       # optional; CLI --broker-id wins if both
log_dir = "/var/lib/crabka/data"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-controller-0.demo-broker-headless.default.svc.cluster.local:9092"
protocol = "plaintext"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "10.0.1.5:32100"
protocol = "plaintext"

[server_properties]
"log.retention.hours" = "24"
```

- `[[listeners]]` deserializes into `BrokerConfig::listeners: Vec<ListenerSpec>` (already supported by the broker library).
- `[server_properties]` is accepted but unused by the broker in this slice. It is the carrier for `Kafka.spec.config` — future Crabka-core slices may grow recognition of specific Kafka-style keys here and map them onto typed `BrokerConfig` fields. Unknown keys are not an error.
- `protocol = "plaintext" | "ssl" | "sasl_plaintext" | "sasl_ssl"`. This chunk emits only `plaintext`; the broker library already supports the others.

### ConfigMap layout (slice 25)

One ConfigMap per cluster: `<cluster>-broker-config` (same name as today — already non-`.properties`-suffixed).

Keys: one complete TOML per broker. Each broker's file is self-contained (cluster-wide content + that broker's `advertised` values). Per-broker duplication of cluster-wide content is cheap (a few KB × N brokers, well under the 1 MiB ConfigMap limit for realistic cluster sizes).

```
broker-0.toml
broker-1.toml
broker-2.toml
```

Mounted as a directory at `/etc/crabka/config/` on each pod. The init container picks the right file:

```sh
cp /etc/crabka/config/broker-${NODE_ID}.toml /run/crabka/broker.toml
```

Broker `MAIN_SCRIPT` becomes:

```sh
exec /usr/bin/crabka-broker \
  --config-file=/run/crabka/broker.toml \
  --broker-id="$(cat /var/lib/crabka/data/.node-id)"
```

(`--broker-id` continues to come from the persisted node-id file; the TOML's `broker_id` field is informational/redundant.)

### Per-broker Service rendering (slice 25)

For each `nodeport` or `loadbalancer` listener:

#### Per-broker Services

For every broker `b` across all `KafkaNodePool`s of the cluster, render:

```
Name:       <cluster>-<listener-name>-<b>
Namespace:  same as Kafka resource
OwnerRef:   Kafka (cluster-level)
Type:       NodePort | LoadBalancer
Selector:   statefulset.kubernetes.io/pod-name = <cluster>-<pool>-<ordinal>
Ports:
  - name:        <listener-name>
    port:        listener.port
    targetPort:  listener.port
    nodePort:    configuration.brokers[b].nodePort
                 ?? K8s-allocated   (nodeport only)
LoadBalancerIP:  configuration.brokers[b].loadBalancerIP   (loadbalancer only)
```

The pod-name label `statefulset.kubernetes.io/pod-name` is set automatically by the StatefulSet controller and is a built-in Kubernetes label as of K8s 1.28 ([reference](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#pod-name-label)). Slice 25 therefore requires K8s 1.28+ — added to the operator's Helm chart `kubeVersion` constraint. Pre-1.28 clusters fail Helm install with a clear message.

The pool/ordinal corresponding to broker id `b` is computed by walking the cluster's `KafkaNodePool`s in name order and assigning `nodeIdStart + ordinal` per pool. Under the slice-20 single-replica constraint (`replicas` is pinned `min=1, max=1`), this reduces to `broker_id = nodeIdStart` per pool. The algorithm generalizes cleanly when multi-replica pools land in a later slice.

#### Bootstrap Service

One per external listener:

```
Name:       <cluster>-<listener-name>-bootstrap
Namespace:  same as Kafka resource
OwnerRef:   Kafka
Type:       NodePort | LoadBalancer
Selector:   app.kubernetes.io/name=crabka, app.kubernetes.io/instance=<cluster>
Ports:
  - name:        <listener-name>
    port:        listener.port
    targetPort:  listener.port
    nodePort:    configuration.bootstrap.nodePort ?? K8s-allocated
LoadBalancerIP:  configuration.bootstrap.loadBalancerIP   (loadbalancer only)
```

The bootstrap selector targets all broker pods of the cluster (any pool), so a client can connect through any healthy broker and discover the rest via metadata.

### Advertised-listener computation (slice 25)

For each (listener, broker) pair, the operator computes `advertised = host:port`:

| Type | Host | Port |
|------|------|------|
| `internal` | `<pod-name>.<headless-svc>.<namespace>.svc.cluster.local` (static template per pod) | `listener.port` |
| `nodeport` | `brokers[b].advertisedHost` ?? `Node[pod-b's nodeName].status.addresses[ExternalIP]` ?? `…[InternalIP]` | `brokers[b].advertisedPort` ?? `brokers[b].nodePort` ?? `Service[<cluster>-<listener>-<b>].spec.ports[0].nodePort` |
| `loadbalancer` | `brokers[b].advertisedHost` ?? `Service[<cluster>-<listener>-<b>].status.loadBalancer.ingress[0].hostname` ?? `.ip` | `listener.port` |

If any required dynamic value is missing (the Node for a pod hasn't been assigned yet, an LB hasn't been provisioned yet), the operator does **not** write a partial TOML. It sets:

```yaml
status:
  conditions:
    - type: ListenersReady
      status: False
      reason: PendingExternalAddresses
      message: "LB for listener 'external' not yet provisioned"
```

and requeues with exponential backoff. Pods stay in their existing state; first-time cold-start pods sit unscheduled-but-not-failed until the ConfigMap appears.

### Cold-start ordering

1. Operator creates per-broker and bootstrap Services.
2. K8s allocates `NodePort`s immediately; LB provisioning is async via the cloud controller.
3. Operator watches both for status; once all dynamic values resolved, renders the ConfigMap.
4. StatefulSet pods are reconciled — on cold start they come up only after the ConfigMap exists, since the volume mount requires the named ConfigMap to be present (the kubelet will retry the pod sandbox until the ConfigMap appears).
5. Slice 21's existing rolling-restart machinery handles subsequent updates.

### Slice 21 hash integration

Slice 21 today hashes `serialize_broker_properties(spec)` — i.e. only the user-visible `spec.config`. In slice 25, the hash function grows to:

```
hash( serialize_broker_properties(spec)
      || "\x1F"                                   // unit separator
      || canonical_listener_intent(spec) )
```

`canonical_listener_intent(spec)` returns the empty string when `spec.listeners` is empty (or absent), and a deterministic serialization of the user-supplied listeners + computed advertised values otherwise.

**Upgrade-zero-restart property:** when a slice-24 cluster has no `spec.listeners` set (the only valid pre-slice-25 state), `canonical_listener_intent` is empty, so the resulting hash is identical to slice-24's hash for the same `spec.config`. No roll. Once the user adds an actual listener, the hash changes once (intended roll) and any subsequent advertised-address change (Node IP, LB hostname) flows through the same mechanism.

This is *coarse* for NodePort with frequent reschedules — a single broker's Node-IP change rolls the whole pool. For LB-typed listeners (stable cloud-assigned hostnames in practice) it's a non-issue. Documented as known coarseness.

A future refinement could maintain per-broker config-hashes for per-pod targeting, but that would require either per-pod annotations that StatefulSet doesn't natively support, or moving away from StatefulSet for finer-grained control. Out of scope here.

### Watches and RBAC (slice 25)

**Watches added:**
- `Node` (cluster-scoped) — informer used to look up a pod's node's external IP for the NodePort case.
- `Service` (already watched) — but reconcile now reacts to `status.loadBalancer.ingress` changes, not just metadata.

**RBAC additions:**
```
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "list", "watch"]
```

`services` permissions are already in the ClusterRole (the existing headless Service work).

### Reconcile sequencing (`controller/kafka.rs`)

1. **Validate** `spec.listeners` → set `ListenersValid` condition. If invalid, requeue without rendering Services or ConfigMap. Existing objects are not deleted (let user fix the spec without disrupting a running cluster).
2. **Render per-broker + bootstrap Services** for each `nodeport`/`loadbalancer` listener; SSA-apply. Existing Services with stale per-broker names are garbage-collected via owner-ref + ownership labels.
3. **Read back** `Service.status` and `Node.status` for all dynamic values needed.
4. **If any value missing** → set `ListenersReady=False reason=PendingExternalAddresses message=…`, requeue.
5. **Render ConfigMap** with N `broker-i.toml` keys; SSA-apply.
6. **Set `ListenersReady=True`** and the populated `status.listeners[]` block.
7. Fall through to `KafkaNodePool` reconcile — slice 21 picks up the config-hash change and rolls if needed.

## Status reporting (slice 25)

Added to `KafkaStatus`:

```rust
pub struct KafkaStatus {
    // ... existing fields ...

    /// Per-listener address information. Surfaced for each entry in
    /// `spec.listeners` once `ListenersReady=True`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<ListenerStatus>,
}

pub struct ListenerStatus {
    pub name: String,
    pub r#type: ListenerType,
    pub bootstrap_servers: String,             // "host:port"
    pub addresses: Vec<ListenerAddress>,
}

pub struct ListenerAddress {
    pub host: String,
    pub port: i32,
}
```

Conditions on `KafkaStatus`:

- `Ready` — overall reconcile success (existing).
- `ListenersValid` — schema-level validation of `spec.listeners`.
- `ListenersReady` — all dynamic external addresses resolved and ConfigMap written.

`bootstrap_servers` semantics by type:
- `internal`: the headless Service FQDN, `host:listener.port`.
- `nodeport`: the bootstrap Service's `nodePort` paired with… *something*. K8s NodePort doesn't have a canonical external host; the operator picks the first ready Node's ExternalIP (or InternalIP fallback) as a hint. Documented in the status condition's message: "bootstrap host is one of N node IPs; clients should be configured with all of them or with an external DNS that round-robins."
- `loadbalancer`: the bootstrap Service's resolved LB hostname/IP, `host:listener.port`.

## Test strategy

### Slice 25a (broker)

- **Unit:** TOML deserialization round-trip against golden fixtures; error cases:
  - duplicate `listener.name` in `[[listeners]]`
  - two listeners sharing `bind_addr`
  - `protocol = "ssl"` without TLS keystore (existing `BrokerConfig::validate()`)
  - unknown top-level table → reject with helpful error citing the key
- **CLI conflict:** `crabka-broker --config-file FOO --listen-addr BAR` exits non-zero with a clear message.
- **CLI smoke (existing `cli_smoke.rs` extended):** boot a broker with a single-listener TOML config-file; produce a `Metadata` request; assert the advertised listener matches the file's value.

### Slice 25 (operator)

- **Unit reconcile tests** (fake `kube::Client` via `tower::ServiceExt::mock_service`):
  - Empty `spec.listeners` synthesizes internal-only default. **Critical:** the resulting `config-hash` must equal the slice-24 hash for the same `spec.config` (so subsequent reconciles don't re-roll after the one-time upgrade template change). Unit test asserts the hash equality directly; the "no second roll" property is verified end-to-end in the upgrade e2e.
  - Validation failure paths (duplicate names, `tls: true`, `type: ingress|route`, duplicate broker ids, missing internal listener for inter-broker) — each sets `ListenersValid=False` with the expected `reason`; no Services or ConfigMap rendered.
  - NodePort listener with fully-populated Node IPs → asserts: 1 bootstrap Service + N per-broker Services with correct selectors, correct `nodePort`s, correct owner-refs; ConfigMap has N keys with expected `advertised` values per broker.
  - LoadBalancer listener with one broker's LB still pending → `ListenersReady=False reason=PendingExternalAddresses`; ConfigMap not written; existing Services unchanged.
  - Override paths: `configuration.brokers[i].advertisedHost` wins over Node-derived IP; `configuration.brokers[i].advertisedPort` wins over allocated `nodePort`.
- **Kind e2e:**
  - **NodePort:** deploy a 3-broker single-pool cluster with one `internal` + one `nodeport` listener. From a pod with `hostNetwork: true` (or from the kind host directly), connect via the bootstrap nodePort using `crabka-cli` / `kcat`; produce 100 messages; consume them; assert byte-equality. Assert `Kafka.status.listeners[name=external].bootstrapServers` is populated and resolves.
  - **LoadBalancer:** same as above, with [MetalLB](https://metallb.io) preinstalled in the kind cluster to provide a real LB controller. Connect via the LB's external IP.
- **Upgrade test:** install slice-24 operator chart + a `Kafka` resource with `spec.config` set; upgrade to slice-25 chart; assert:
  - `Kafka.status.listeners` populated with the synthesized internal-default
  - `crabka.io/config-hash` annotation on the pool's StatefulSet **unchanged** (synthesized default produces empty `canonical_listener_intent`, hash stable)
  - one-time pod roll **does** happen: the pod template changes (MAIN_SCRIPT switches to `--config-file`, new ConfigMap volume mount). This is a single rolling restart driven by the StatefulSet itself reacting to template change, **not** by slice 21's config-hash annotation. The roll is graceful (one pod at a time, via the slice-22 ControlledShutdown handler).
  - producing/consuming a message succeeds before, during (best-effort), and after the upgrade
  - no second roll occurs once the upgrade completes (subsequent reconciles converge)

### Slice 27

Test plan deferred until Phase 4 lands TLS. Sketch: SNI passthrough verified by connecting to `<broker-N>.kafka.example.com` and asserting routing to the matching broker pod.

## Out of scope

- TLS, SASL, SCRAM, mTLS on listeners (Phase 4: slices 30, 31).
- Cluster CA / clients CA generation (slice 30).
- Slice 27 (Ingress + Route) **implementation** — schema is here, reconcile is deferred.
- Network policies (skipped slice 23 — orthogonal, separate chunk).
- IPv4/IPv6 dual-stack listener selection (Strimzi has `ipFamilies` / `ipFamilyPolicy`; not yet).
- ExternalName / headless service-only "external" surface (Strimzi's `cluster-ip` listener type).
- Per-listener `maxConnections` / `maxConnectionCreationRate` (KIP-612 / 599 surface on listeners — Crabka has cluster-wide quotas via slice 16 but no per-listener bounds yet).
- Per-pod config-hash for selective rolling restart on per-broker address changes.
- Bootstrap address selection heuristics for NodePort beyond "first ready Node's ExternalIP".

## Open questions to resolve in the implementation plan

- Exact `toml` crate version (latest stable at plan time).
- Whether to add a `serde(deny_unknown_fields)` on the top-level `FileConfig` or accept unknown-and-warn (current preference: deny on top-level structs, accept-unknown inside `[server_properties]`).
- Bootstrap-host-for-NodePort: pick first ready Node, all Nodes, or require user override via `configuration.bootstrap.host`? (Lean: first ready Node, and document the limitation.)
- Whether `inter_broker_listener_name` field on `KafkaSpec` belongs in this slice or in the Phase 4 auth-wiring slice when inter-broker mTLS arrives. (Lean: in this slice, defaulted to "first internal listener", documented as overridable later.)

## Acceptance criteria

### Slice 25a
1. `cargo build -p crabka-broker` produces a binary that accepts `--config-file`.
2. `cargo test -p crabka-broker --test cli_smoke` covers config-file boot.
3. TOML parser unit tests cover all error cases listed above.

### Slice 25
1. `cargo build -p crabka-operator` clean.
2. `cargo test -p crabka-operator` passes all reconcile-unit tests above.
3. CI kind job: NodePort e2e and LoadBalancer e2e both pass.
4. Slice-24-to-25 upgrade e2e: one-time graceful rolling restart on upgrade (pod template change); `crabka.io/config-hash` annotation unchanged for empty `spec.listeners`; no second roll afterward.
5. CRD-drift CI job: `cargo xtask gen-crds` produces no diff; `helm lint charts/crabka-operator` passes.
