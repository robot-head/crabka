# Gres G-4: Front door — design

**Date:** 2026-07-09
**Status:** Approved
**Type:** Slice design. The fourth slice of [Chapter Gres](2026-07-09-crabka-gres-chapter-design.md): tenants become a product surface — provisioned, routed, and authenticated — with PgDog as the co-deployed pgwire front door and Crabka owning the control plane.

## Context — what the tree and PgDog actually hold

1. **PgDog (v0.1.x, AGPL-3.0) fits the role as-is.** `[[databases]]` entries mapping a client-facing database name to a backend host/port are first-class (sharding is opt-in and unused here); config hot-reloads via SIGHUP or the `RELOAD` command on its admin database (only `host`/`port`/`workers` need restarts), with maintenance mode (`MAINTENANCE ON` → `RELOAD` → `MAINTENANCE OFF`) for atomic swaps that stall rather than error clients; **passthrough auth** (`passthrough_auth = "enabled"`, TLS required) forwards client credentials to the backend instead of terminating auth against `users.toml`; TLS exists on both legs including mTLS; there is an official container image (`ghcr.io/pgdogdev/pgdog`), an official Helm chart, and an OpenMetrics endpoint — but **no operator and no scale-to-zero** (a down backend errors after bounded connect/checkout timeouts; nothing queues new connections while a backend cold-starts). That last fact is G-5's central problem; this slice only ensures the registry carries the state G-5 needs.
2. **The KRaft metadata path is broker-internal; the compacted-topic registry is the ordinary-wire precedent.** Cluster metadata records ride Crabka-private API keys on the controller listener — writing tenant records there would mean broker changes, which the chapter forbids. The schema-registry's `_schemas` store is the proven pattern for a durable cluster-scoped registry over the ordinary Kafka wire: a compacted single-partition topic, one elected writer, produce-then-await-apply read-your-writes, and a tailing reader publishing a watch.
3. **The operator has the exact scaffolds this slice needs.** One controller per CRD kind (`crabka.io/v1alpha1`), per-instance Deployment precedents (`KafkaGrpcGateway`, `SchemaRegistry`) including rendered-config-Secrets for a third-party process, an **aggregation** precedent for building one config file from many logical entries under a single server-side-apply owner (no partial patches), the `crabka.io/cluster` label convention, a documented new-kind checklist (`crd/`, `gen_crds.rs`, `controller/`, `run.rs`, mock-harness tests), and a KafkaUser→`AlterUserScramCredentials` precedent for "CRD is intent; the wire is the runtime registry".
4. **SCRAM primitives are shared, with one gap.** KIP-554 is fully implemented broker-side; `crabka-security` exposes mechanism-parameterized SCRAM building blocks (`hash_scram_password`, `derive_keys_from_salted`, `ScramCredential`, client/server exchanges). The vendored `crabka-pgwire` carries its own SCRAM-SHA-256 verifier; what neither has is a `pg_authid`-style verifier-string codec (`SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>`) — the small shared piece this slice adds so verifiers can travel through the registry.

## Design Goals

- **Tenant lifecycle as data:** creating, describing, suspending, and deleting a tenant are records in one durable registry, driven identically from Kubernetes (CRDs) and from a CLI — the operator is a consumer of the control plane, not its owner.
- **One credential store:** a tenant's SCRAM verifier lives in the registry and is enforced by the tenant's own compute; no second copy in PgDog config to drift.
- **Stock PgDog:** co-deployed, unmodified (AGPL hygiene), driven entirely through rendered configuration and its admin commands.
- **Gate (chapter):** N tenants served through one PgDog endpoint with per-tenant isolation and per-tenant auth.

## Non-goals

- **Idle suspend / spawn-on-demand / cold-start SLO** — G-5 (this slice lands the `suspended` registry state and the config-re-render machinery G-5 will drive).
- **Multi-region, tenant quotas/limits, billing hooks** — later chapters.
- **PgDog sharding features** — one tenant, one backend; PgDog is pooling + routing + TLS here.
- **Forking or patching PgDog** — any missing behavior is solved in Crabka's control plane or documented as a limitation.

## Architecture Overview

```
                          kubectl / CI                         psql / drivers
                               │                                    │ TLS + SCRAM
                        Gres / GresTenant CRs                     PgDog  (official image; Deployment,
                               │                                    │     config Secret, admin RELOAD)
                     crabka-operator controllers                    │ passthrough auth
            ┌─ gres_tenant.rs: per tenant ──────────────┐          │
            │    ensure __gres_wal.<t> topic            │   ┌──────┴──────┐
            │    write registry record                  │   │  computes   │  crabka-gres Deployments
            │    render compute Deployment              │   │  (1/tenant) │  (KafkaGrpcGateway shape)
            └─ gres.rs: the fleet ──────────────────────┘   └──────▲──────┘
                 aggregate all tenants → pgdog.toml +              │ boot: tail registry → own entry
                 users skeleton → Secret → RELOAD                  │ (verifier, bucket, thresholds)
                               │                                   │
                    crabka-gres-control (lib) ─── ordinary wire ───┤
                     __gres_tenants registry                       │
                     (compacted, 1 partition,                      │
                      _schemas pattern)                            │
                               ▲                                   │
              crabka gres CLI ─┘  (create-tenant / suspend / resume / list / render-pgdog)
```

## Key Design Decisions

### The registry is a compacted topic, and everything else is a view of it

`__gres_tenants` (compacted, 1 partition, RF configurable) holds one keyed record per tenant: name, state (`active`/`suspended`), SQL user + SCRAM verifier (never a password), WAL-topic settings, bucket prefix, checkpoint thresholds, sizing hints. Writes go through `crabka-gres-control`, a small library implementing the `_schemas` idiom (ensure topic, produce keyed JSON records, await the reader's applied offset for read-your-writes; tombstone = deletion); readers tail and fold. The alternatives fell to the grounding: KRaft records need broker changes; CRD-only leaves non-k8s deployments and the computes themselves without a registry. Computes read their own entry at boot over the connection they already have — the Deployment env stays minimal (bootstrap, tenant name) and secrets never pass through pod specs.

### CRDs are intent; the controllers reconcile intent into the registry and workloads

Two kinds, `Gres` (the fleet) and `GresTenant` (labeled to its fleet), following the operator's documented new-kind checklist. The `GresTenant` controller: ensure the WAL topic, upsert the registry record (verifier from a referenced Secret, hashed via `crabka-security` + the new codec), server-side-apply the per-tenant compute Deployment/Service (the `KafkaGrpcGateway` shape), surface readiness + last-checkpoint in status. The `Gres` controller owns the fleet pieces: the PgDog Deployment (official image, pinned tag), its Service, and the **aggregated** `pgdog.toml`/`users.toml` Secret rendered from all tenants of the fleet — the one-SSA-owner aggregation pattern; per-tenant partial patches of shared config were rejected as ownerless. After the Secret propagates, the controller issues `RELOAD` over PgDog's admin database (it is ordinary Postgres wire; `tokio-postgres` is already a workspace dep), using maintenance mode when a change spans multiple PgDog replicas.

### Auth is passthrough; the verifier lives with the compute

PgDog runs with `passthrough_auth = "enabled"` (TLS on the client leg, per its requirement): the client's SCRAM exchange terminates at the tenant compute, whose `crabka-pgwire` verifier is loaded from the registry entry. This keeps exactly one credential store and makes tenant isolation structural — a connection routed to the wrong backend fails auth because the verifier is wrong. `users.toml`-terminated auth remains a documented dev-mode. The new `pg_authid` verifier codec (`SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>`) lands next to `crabka-security`'s SCRAM primitives and is shared by the control plane (building verifiers at provision time) and `crabka-pgwire` (consuming them), with round-trip tests against verifiers produced by real PostgreSQL.

### The CLI drives the same control plane

`crabka gres create-tenant|describe|suspend|resume|delete|list` subcommands in `crabka-cli` call `crabka-gres-control` directly, plus `crabka gres render-pgdog` emitting the same `pgdog.toml`/`users.toml` the operator renders — so a non-Kubernetes deployment (compose, bare metal) gets the full product with hand-run PgDog. Renderers are one shared implementation in `crabka-gres-control`, serialized through typed config structs so the emitted files round-trip PgDog's loader in tests.

## Integration

- **New crate `crates/gres-control`** (`crabka-gres-control`, `publish = false`): registry client (writer + tailing reader), tenant record schema (versioned), PgDog config renderers, the verifier codec (or the codec lands in `crabka-security` — decided at plan time by where the deps point most cleanly).
- **`crates/operator`:** `Gres` + `GresTenant` CRDs, two controllers, `gen_crds.sh` regen, sample YAML, mock-harness reconcile tests.
- **`crates/cli`:** the `gres` subcommand family.
- **`crates/gres`:** boot-time registry read (own entry → SessionConfig with the tenant's verifier, bucket prefix, thresholds); flags become overrides for dev.
- **PgDog:** consumed as the official image, version-pinned in operator defaults; never vendored, never patched.

## Kafka / wire compliance

The registry rides ordinary produce/fetch on a compacted topic (the `_schemas` pattern); provisioning uses stock CreateTopics. Nothing touches broker behavior. PgDog's admin database is Postgres wire, driven with a stock client.

## Testing

- **Registry:** unit + property tests for record round-trips and fold semantics (create/update/suspend/tombstone orderings); read-your-writes pinned against an in-process broker.
- **Renderers:** golden `pgdog.toml`/`users.toml` outputs; loader round-trip (parse what we render with PgDog's own config shapes where feasible — otherwise a schema-pinned golden corpus with a documented upgrade check against the pinned PgDog version).
- **Verifier codec:** round-trips against verifiers generated by real PostgreSQL (`CREATE ROLE … PASSWORD` → `pg_authid` fixture) and against `crabka-pgwire`'s SCRAM path.
- **Operator:** mock-harness reconcile tests per the house pattern (exact request sequences for topic-ensure, registry write, Deployment/Secret SSA, RELOAD call), fleet aggregation across N tenants, suspend re-render.
- **The gate (e2e, CI):** in-CI broker + two provisioned tenants + real PgDog container from the pinned official image → psql through PgDog to both tenants with per-tenant SCRAM over TLS; wrong-tenant credentials fail; kill one compute and confirm the other tenant is unaffected.

## Risks

- **PgDog version drift:** its config schema is young (v0.1.x) — the image tag is pinned, renderers are golden-tested, and bumping the pin is a deliberate change with the e2e leg as the gate. Removal-at-runtime semantics are undocumented upstream; the e2e covers our suspend/remove path empirically.
- **Passthrough-auth maturity:** flagged as newer PgDog surface; the dev-mode `users.toml` path is the documented fallback if a blocking defect appears, at the cost of a second credential store (sync owned by the `Gres` controller if ever needed).
- **Registry writer concurrency:** CLI and operator can both write; last-writer-wins per key is acceptable for v1 (records are whole-tenant snapshots with a bumped version field; the operator re-reconciles), documented explicitly.
- **AGPL boundary:** stock image, config-only interaction, admin commands over the wire — no PgDog code in the repo, recorded in NOTICE-adjacent docs.

## Resolved decisions

- Registry: `__gres_tenants` compacted topic via `crabka-gres-control`; CRDs are intent; computes self-configure from the registry at boot.
- Operator: `Gres` (fleet + PgDog + aggregated config Secret + RELOAD) and `GresTenant` (topic + registry + compute Deployment) controllers.
- Auth: PgDog passthrough over TLS; verifier in the registry; `pg_authid` verifier codec shared between control plane and pgwire; `users.toml` as dev fallback.
- CLI: `crabka gres` subcommands over the same library, including `render-pgdog`.
- Gate: mock reconcile suites + real-PgDog e2e with two tenants, per-tenant SCRAM, isolation on compute failure.
