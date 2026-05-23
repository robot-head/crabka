# Slice 41 — Operator: Configurable logging (`Kafka.spec.logging`) (design)

**Date:** 2026-05-23
**Status:** Implemented
**Phase:** 6 (Observability). Continues 39 (metrics exporter) + 40 (`metricsConfig`).

## Goal

Surface broker log configuration as a Strimzi-shaped `Kafka.spec.logging`
field. The operator resolves it to a `tracing` `RUST_LOG` env-filter, delivers
it to brokers via the existing broker `ConfigMap`, and rolls the cluster on
change through the slice-21 config hash. Pure operator work — no Crabka-core
dependency. The broker already reads `RUST_LOG` via
`EnvFilter::try_from_default_env()` (`crates/broker/src/bin/broker.rs`), so no
broker change is needed.

## Why via the broker ConfigMap

Strimzi mounts a `log4j.properties` `ConfigMap` into the broker. Crabka's
broker has no log4j; it takes a single `RUST_LOG` env-filter directive string.
The cleanest mapping that keeps one source of truth and reuses existing
machinery:

- The operator computes the resolved filter once (inline composition or
  external read happen only in the `Kafka` reconciler), renders it into the
  cluster's broker `ConfigMap` under a `rust.log` key, and points each broker
  pod's `RUST_LOG` env at that key via `configMapKeyRef` (`optional: true`).
- The filter string is folded into `combined_config_hash`, so a *value* change
  rolls the cluster via slice 21 (the broker only re-reads `RUST_LOG` at
  startup — a live `ConfigMap` edit alone would not take effect).

## CRD shape (Strimzi-shaped)

```yaml
# inline
logging:
  type: inline          # default
  loggers:
    root: info          # `root` (case-insensitive) -> bare global level
    crabka_broker: debug
    crabka_raft: warn
# external
logging:
  type: external
  valueFrom:
    configMapKeyRef:
      name: my-log-cm
      key: rust.log
```

- `loggers` keys are **tracing targets** (Rust module paths, e.g.
  `crabka_broker`), not log4j logger names. `root` sets the env-filter global
  default. Levels are `trace|debug|info|warn|error|off` (case-insensitive;
  `warning`→`warn`, `fatal`→`error`, `none`→`off`).
- Inline composition is pure + deterministic (directives sorted), so the hash
  is stable across reconciles regardless of map iteration order. Example:
  `{root: info, crabka_broker: debug}` → `crabka_broker=debug,info`.
- `external` reads the referenced `ConfigMap` key verbatim (one extra GET,
  only on the external path). RBAC already grants `configmaps` get.

## Resolution + conditions

`controller::logging::resolve_logging` returns `LoggingOutcome`:

- `Disabled` — `spec.logging` unset → `LoggingReady=False reason=Disabled`
  (mirrors slice-40 `MetricsReady`); no `rust.log` key, no `RUST_LOG` env.
- `Resolved(filter)` — `LoggingReady=True reason=Available`; `rust.log` rendered,
  `RUST_LOG` env wired, filter in the config hash.
- `Invalid(err)` — user error (bad level, blank key, missing external
  ConfigMap/key, missing `valueFrom`) → `LoggingReady=False` with a specific
  reason (`InvalidLogLevel`, `LoggingConfigMapNotFound`, …). The operator
  surfaces the condition and leaves the broker on its built-in default filter;
  it does **not** spin (a transient API error during the external GET, by
  contrast, propagates and requeues).

## Pod-template gating (upgrade stability)

The `RUST_LOG` env entry is gated on `spec.logging.is_some()` — identical to the
slice-40 metrics-port/flag gating. A logging-unset cluster gets a byte-identical
pod template (no spurious roll). With `optional: true` on the `configMapKeyRef`,
a pod stays bootable if the key is briefly absent (e.g. external resolution
pending), falling back to the broker default.

## Config-hash interaction

`combined_config_hash` gains a sixth segment (the resolved filter, empty when
unset). The slice-24 empty-hash collapse is preserved: with listeners,
metricsConfig, CA cert, metadata pin, **and** logging all absent, the hash still
collapses to `config_hash(config_part)`.

## Out of scope (deferred)

- Per-`KafkaNodePool` logging override (Strimzi keeps logging on the cluster
  spec; the node-pool reads `parent.spec.logging`).
- Live log-level hot-reload without a restart (broker reads `RUST_LOG` only at
  startup; would need a broker-core control surface — a future core slice).
- Mapping log4j logger names to tracing targets (loggers keys *are* tracing
  targets).
- OTLP / structured-logging knobs (slice 42 territory).

## Tests

- Unit (`crd::logging`, `controller::logging`, `kafka_node_pool`): CRD
  round-trips; inline composition (sorting, `root`, level canonicalization,
  empty/blank/invalid rejection); outcome→condition mapping; `RUST_LOG` env
  presence/absence + `configMapKeyRef` shape.
- Hash (`controller::common`): logging segment changes the hash; absent logging
  preserves the slice-24 collapse (existing tests, now 4-arg).
- Integration (`tests/reconcile_kafka.rs`): inline renders `rust.log` +
  `LoggingReady=True`; unset omits it (`Disabled`); external reads the user
  ConfigMap; missing external ConfigMap surfaces
  `LoggingReady=False reason=LoggingConfigMapNotFound` and renders no key.
- kind-e2e: patch `demo` with inline logging, assert `rust.log` rendered +
  `LoggingReady=True` + the broker STS pod template wires `RUST_LOG` to the
  `configMapKeyRef`, then reset.
