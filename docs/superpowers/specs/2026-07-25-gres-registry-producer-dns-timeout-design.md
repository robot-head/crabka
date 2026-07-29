# Gres Registry Producer DNS Timeout Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

Expose the existing client DNS deadline through the shared Gres registry
policy and every process that constructs the registry producer.

## Goals

The producer inside `crabka-gres-control::Registry` must not inherit an
untunable DNS deadline. Kubernetes users configure it through the existing
Kafka-owned Gres registry CRD policy. Standalone processes configure it with a
command-line argument backed by an environment variable.

When absent, behavior remains unchanged: the deadline is
`ClientDnsTimeout::default()` (10 seconds).

## Architecture

`RegistryPolicy` gains one `ClientDnsTimeout` field, its default, an accessor,
and a consuming override method. This reuses the existing validated client
type and avoids a registry-specific timeout type or a second producer policy.

`Registry::connect_with_policy` forwards the policy value to the sole registry
`Producer::builder()` call with `.dns_timeout(...)`. The policy remains the
single shared input for operator-internal control, Gres compute, the activator,
the Gres CLI, and the load-test harness.

The standalone surface is:

- `--registry-producer-dns-timeout-ms`
- `CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS`

It is added to the existing registry options in `crabka-gres`, `crabka gres`,
`crabka-gres-activator`, and `crabka-gres-loadtest`. Load-test child processes
receive the effective value through the existing registry-policy argument
renderer.

The Kafka CRD adds optional
`spec.gresRegistry.producerDnsTimeoutMs`. The same effective policy is used
directly by operator control clients and rendered into Gres compute and
activator deployments.

## Validation and Precedence

The value is a positive whole number of milliseconds representable as `u64`.
CLI inputs use the existing `PositiveMillis` parser. The CRD schema declares a
minimum of one. Both inputs construct the existing refined
`ClientDnsTimeout`, so invalid values fail before DNS or socket I/O.

Precedence is:

1. command-line argument;
2. environment variable;
3. `ClientDnsTimeout::default()`.

CRD conversion errors name `spec.gresRegistry.producerDnsTimeoutMs`.

## Data Flow

The Kubernetes path is:

`Kafka.spec.gresRegistry.producerDnsTimeoutMs`
→ `GresRegistrySpec::policy`
→ `RegistryPolicy`
→ operator control client or rendered
`--registry-producer-dns-timeout-ms`
→ process `RegistryOptions`
→ `Registry::connect_with_policy`
→ `Producer::builder().dns_timeout`
→ `Client::builder().dns_timeout`
→ `ConnectionOptions::dns_timeout`.

Standalone processes enter at their `RegistryOptions`.

## Testing

Focused tests prove:

- registry policy defaulting and a distinct typed override;
- every standalone registry-options surface preserves environment/CLI
  precedence and rejects zero;
- the load-test child renderer emits the effective value;
- CRD defaulting, override, error text, and schema minimum;
- operator control caching distinguishes policies with different DNS values;
- compute and activator deployments render the value exactly once; and
- two fresh generations of all nine CRDs match each other and `deploy/crds`.

Affected package tests, strict Clippy, formatting, diff checks, CLI help, and
the runtime-value audit must pass before publication.

## Scope

This slice covers the sole producer owned by `Registry`. It does not configure
the registry reader's raw `ToSocketAddrs` resolution, registry admin
connections, WAL producers, or unrelated producers. Those remain separate
configuration owners because they use different runtime paths.
