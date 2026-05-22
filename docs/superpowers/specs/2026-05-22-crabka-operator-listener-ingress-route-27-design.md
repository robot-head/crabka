# Crabka Operator — Slice 27: Ingress / Route external listeners — design

**Date:** 2026-05-22
**Status:** Approved, implemented
**Scope:** Implements the `type: ingress` and `type: route` external listener
reconcile that slice 25 deferred. Builds on Phase 4 (slices 30/31), which
landed listener TLS, so SNI-passthrough routing now has the per-broker server
certificates it requires.

## Goal

Make Crabka clusters reachable from outside the Kubernetes cluster over a single
HTTPS port (443) using SNI-based routing — `Ingress` (with TLS passthrough) on
vanilla Kubernetes, and `Route` (with `passthrough` termination) on OpenShift.
This completes the external-listener family (NodePort + LoadBalancer landed in
slice 25/26).

## Why Ingress/Route needs TLS

Kafka's protocol is not HTTP: a client connects to a bootstrap address, receives
a `Metadata` response listing every broker's advertised `host:port`, then opens a
direct TCP connection to each broker. To multiplex many brokers behind one
ingress IP on port 443, the ingress controller must route by **SNI** — it
inspects the TLS `ClientHello` server-name and forwards the *raw* TLS byte
stream to the matching broker Service. The controller does **not** terminate
TLS; the broker does. SNI routing therefore requires `tls: true`, and each
broker needs a distinct externally-resolvable hostname whose name is carried in
both the client's SNI and the broker's server certificate SAN set.

Consequently:

- `type: ingress` and `type: route` **require** `tls: true`. Validation rejects
  otherwise (`ListenerIngressRequiresTls`).
- Every broker (and the bootstrap) needs an explicit hostname. Slice 27 takes
  these from `configuration.bootstrap.host` and
  `configuration.brokers[].host` (both already in the slice-25 schema).
- The advertised port is always **443** — the port the ingress controller /
  OpenShift router listens on — unless `configuration.brokers[].advertisedPort`
  overrides it.

## Schema additions

`ListenerConfiguration` gains one Strimzi-shaped field:

```rust
/// Ingress only: `spec.ingressClassName` on the generated Ingress objects.
#[serde(default, skip_serializing_if = "Option::is_none", rename = "class")]
pub ingress_class: Option<String>,
```

The `host` fields on `BootstrapConfig` / `BrokerOverride` were already present
(landed forward-stable in slice 25). No other schema change.

## Validation (status condition `ListenersValid`)

New rejections, in addition to slice 25/31's existing set (the slice-25
`IngressDeferred` / `RouteDeferred` placeholders are deleted):

- `ListenerIngressRequiresTls` — `ingress` or `route` listener with `tls:false`.
- `ListenerIngressBootstrapHostMissing` — `ingress` or `route` listener whose
  `configuration.bootstrap.host` is unset (no way to derive a bootstrap
  hostname; the operator does not invent one).

Per-broker host absence is surfaced at advertised-address time (broker ids
aren't known until pools are enumerated) as `ListenersReady=False` with reason
`PendingExternalAddresses` and an `IngressBrokerHostMissing` message — the same
not-ready channel NodePort/LoadBalancer use for unresolved external addresses.

## Objects rendered

For each `ingress` / `route` listener the operator renders, owner-ref'd to the
`Kafka`:

1. **Backend ClusterIP Services** — the ingress/route backends.
   - Per-broker `<cluster>-<listener>-<broker>` with the built-in
     `statefulset.kubernetes.io/pod-name` selector (pins to one broker pod).
   - Bootstrap `<cluster>-<listener>-bootstrap` selecting all broker pods.
   - These reuse `render_broker_service` / `render_bootstrap_service`, which now
     emit `type: ClusterIP` for ingress/route (vs `NodePort` / `LoadBalancer`).

2. **Ingress** (`networking.k8s.io/v1`), per-broker + bootstrap:
   - `spec.ingressClassName` from `configuration.class` when set.
   - `nginx.ingress.kubernetes.io/ssl-passthrough: "true"` annotation (the
     de-facto passthrough switch; harmless on controllers that ignore it).
   - `spec.tls[].hosts: [host]` (no `secretName` — passthrough, the broker owns
     the cert).
   - One rule: `host` → backend Service `name`/`port.number = listener.port`,
     `path: /`, `pathType: Prefix`.

3. **Route** (`route.openshift.io/v1`), per-broker + bootstrap, applied as a
   `DynamicObject` (OpenShift CRD, not in `k8s-openapi`) via the shared
   `apply_dynamic` helper:
   - `spec.host: <host>`.
   - `spec.port.targetPort: listener.port`.
   - `spec.tls.termination: passthrough`.
   - `spec.to: { kind: Service, name: <service>, weight: 100 }`.

Bootstrap-Service annotations/labels from `configuration.bootstrap` continue to
flow onto the bootstrap ClusterIP Service.

## Advertised-address computation

`compute_advertised` gains an ingress/route arm:

| Source (first match wins) | Host | Port |
|---|---|---|
| `brokers[b].advertisedHost` | override | — |
| `brokers[b].host` | the ingress/route hostname | — |
| (else) | → `IngressBrokerHostMissing` error | — |
| `brokers[b].advertisedPort` | — | override |
| (else) | — | `443` |

Ingress and Route resolve identically (both terminate at the controller on 443).
Bootstrap status (`status.listeners[].bootstrapServers`) uses
`configuration.bootstrap.host:443`.

## Certificate SANs

`compute_extra_sans` gains an ingress/route arm so the per-broker server cert is
valid for the SNI hostname clients present: it adds, as DNS SANs, this broker's
`host` / `advertisedHost` plus the listener's `bootstrap.host`. Because all
hostnames are config-supplied (deterministic), there is no "not ready" SAN
gating for ingress/route (unlike the LoadBalancer ingress-pending path).

## Reconcile wiring (`controller/kafka.rs`)

- `apply_external_services` now also handles ingress/route: it applies the
  ClusterIP Services then the Ingress (typed) / Route (dynamic) objects.
- The node/pod read inside `read_external_state` is gated on the presence of a
  NodePort/LoadBalancer listener — ingress/route advertised hosts come from
  config and need neither, so an ingress-only cluster issues no Node/Pod LISTs.
- All listener-intent changes already flow through slice-21's config-hash →
  ordered rolling restart (the rendered TOML — with `host:443` advertised
  values — is in the hash).

## RBAC

ClusterRole gains:

```yaml
- apiGroups: ["networking.k8s.io"]
  resources: ["ingresses"]
  verbs: ["get","list","watch","create","update","patch","delete"]
- apiGroups: ["route.openshift.io"]
  resources: ["routes"]
  verbs: ["get","list","watch","create","update","patch","delete"]
```

## Out of scope / deferred

- **OpenShift-assigned Route hosts.** Slice 27 requires an explicit `host` for
  route listeners (symmetric with ingress). Reading the OpenShift-assigned host
  back from `Route.status.ingress[].host` and re-reconciling is a follow-up;
  it needs a live OpenShift API to validate and adds a read-back/requeue loop.
- BYO server cert per listener (`brokerCertChainAndKey`).
- Ingress controllers other than the SNI-passthrough model (e.g. HTTP/2 or
  GRPC-terminating controllers) — Kafka-over-Ingress fundamentally needs raw
  TLS passthrough.
- Per-listener `maxConnections` / connection-rate limits.

## Acceptance criteria

1. `cargo build -p crabka-operator` clean; `cargo test -p crabka-operator`
   passes the new unit + integration tests.
2. Validation: ingress/route without `tls` → `ListenersValid=False reason=
   ListenerIngressRequiresTls`; ingress/route without bootstrap host →
   `ListenerIngressBootstrapHostMissing`.
3. An ingress listener renders ClusterIP backends + Ingress objects, a
   ConfigMap whose `advertised` is `<host>:443`, and `ListenersReady=True`.
4. A route listener renders the dynamic `route.openshift.io/v1` objects.
5. CRD-drift: `cargo run -p crabka-operator -- gen-crds deploy/crds` produces
   only the `class` addition; `helm lint charts/crabka-operator` passes.
