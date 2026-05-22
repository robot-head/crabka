# Slice 27 — Ingress / Route external listeners — implementation plan

**Design:** [`2026-05-22-crabka-operator-listener-ingress-route-27-design.md`](../specs/2026-05-22-crabka-operator-listener-ingress-route-27-design.md)

Single cohesive PR in `crates/operator` (+ chart RBAC + CRD regen). The work is
concentrated in `controller/listeners.rs` and `controller/kafka.rs`, which edit
the same call graph, so it is implemented as one unit rather than parallel
batches.

## Tasks

1. **Schema** (`crd/listener.rs`): add `ListenerConfiguration.ingress_class`
   (`#[serde(rename = "class")]`). `host` fields already exist.
2. **Validation** (`controller/listeners.rs`): replace `IngressDeferred` /
   `RouteDeferred` with `ListenerIngressRequiresTls` +
   `ListenerIngressBootstrapHostMissing`; ingress/route require `tls: true` and
   `configuration.bootstrap.host`.
3. **Backend Services**: `render_broker_service` / `render_bootstrap_service`
   emit `ClusterIP` for ingress/route (panic only on `internal`).
4. **Ingress rendering**: `render_broker_ingress` / `render_bootstrap_ingress`
   (typed `networking.k8s.io/v1`) — SNI passthrough annotation, ingress class,
   `tls[].hosts`, host→backend rule. Host helpers `ingress_broker_host` /
   `ingress_bootstrap_host`.
5. **Route rendering**: `render_broker_route` / `render_bootstrap_route` (JSON
   body) — `passthrough` termination, `to` → Service. Applied via new shared
   `common::apply_dynamic`.
6. **Advertised**: `compute_advertised` ingress/route arm — config host, port
   443 (override-aware); `AdvertisedError::IngressBrokerHostMissing`.
7. **SANs**: `compute_extra_sans` adds config ingress/route hostnames.
8. **Status**: `resolve_bootstrap_servers` ingress/route arm (`host:443`).
9. **Reconcile** (`controller/kafka.rs`): `apply_external_services` applies
   ClusterIP backends + Ingress/Route objects; gate Node/Pod LIST on
   NodePort/LoadBalancer presence; `has_external` = any non-internal listener.
10. **RBAC**: ClusterRole `ingresses` + `routes`.
11. **Tests**: unit (validation, render, advertised, SAN) + integration
    (`reconcile_listener_ingress.rs`).
12. **CRD regen**: `cargo run -p crabka-operator -- gen-crds deploy/crds`.

## Acceptance

`cargo test -p crabka-operator` green; clippy + fmt clean; CRD diff is only the
`class` field; chart still lints.
