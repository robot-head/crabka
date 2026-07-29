# Gres Registry Reader/Admin DNS Timeout Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

Expose one validated DNS deadline for the Gres registry reader and admin
paths. Keep the registry producer deadline separate and unchanged.

## Goals

Registry refresh, the background reader, topic creation, and metadata refresh
must not perform unbounded or hardcoded DNS lookup. Kubernetes users configure
one shared reader/admin deadline through the existing Kafka-owned Gres registry
CRD policy. Standalone processes configure it with a command-line argument
backed by an environment variable.

When absent, behavior remains unchanged: the deadline is
`ClientDnsTimeout::default()` (10 seconds).

## Architecture

`RegistryPolicy` gains one `reader_admin_dns_timeout: ClientDnsTimeout` field,
its default, an accessor, and a consuming override method. This reuses the
existing validated client type; it adds no registry-specific timeout type or
dependency.

The standalone surface is:

- `--registry-reader-admin-dns-timeout-ms`
- `CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS`

It is added to the existing registry options in `crabka-gres`, `crabka gres`,
`crabka-gres-activator`, and `crabka-gres-loadtest`. Load-test child processes
receive the effective value through the existing registry-policy argument
renderer.

The Kafka CRD adds optional
`spec.gresRegistry.readerAdminDnsTimeoutMs`. The same effective
`RegistryPolicy` is used directly by operator control clients and rendered
into Gres compute and activator deployments.

`AdminClient::connect_one` currently carries
`ConnectionOptions::dns_timeout` but does not apply it to
`tokio::net::lookup_host`. The shared connection function will bound that
lookup with the carried timeout, so bootstrap and controller reconnects obey
the same policy. A narrow public `connect_with_dns_timeout` entry point will
reuse the existing admin defaults while replacing only the DNS timeout.

Registry refresh and the background reader will replace synchronous
`ToSocketAddrs` resolution with one async, timeout-bounded local resolver. Both
paths consume `RegistryPolicy::reader_admin_dns_timeout`; no new renderer or
reader policy is introduced.

## Validation and Precedence

The value is a positive whole number of milliseconds representable as `u64`.
CLI inputs use the existing `PositiveMillis` parser. The CRD schema declares a
minimum of one. Both inputs construct the existing refined
`ClientDnsTimeout`, so invalid values fail before DNS or socket I/O.

Precedence is:

1. command-line argument;
2. environment variable;
3. `ClientDnsTimeout::default()`.

CRD conversion errors name
`spec.gresRegistry.readerAdminDnsTimeoutMs`.

## Data Flow

The Kubernetes path is:

`Kafka.spec.gresRegistry.readerAdminDnsTimeoutMs`
→ `GresRegistrySpec::policy`
→ `RegistryPolicy`
→ operator control client or rendered
`--registry-reader-admin-dns-timeout-ms`
→ process `RegistryOptions`
→ `Registry::connect_with_policy`
→ registry refresh, background reader, and registry admin calls.

Admin calls continue through `AdminClient`, whose bootstrap and controller
reconnect lookups consume `ConnectionOptions::dns_timeout`. Direct registry
reader connections use the same effective `ClientDnsTimeout` in their local
bounded resolver.

Standalone processes enter at their `RegistryOptions`.

## Failure Behavior

DNS errors and timeouts remain ordinary connection failures. Registry reader
failures retain the existing retry backoff. Registry refresh and admin
operations retain their existing error propagation. This slice changes only
how long DNS lookup may wait.

The reader and admin share one value because they resolve the same registry
bootstrap endpoints and do not have a useful independent tuning boundary.
The producer remains separate because it uses client-core's producer-owned
connection pool and already has an explicit public setting.

## Testing

Focused tests prove:

- `AdminClient` DNS lookup stops at the `ConnectionOptions` deadline;
- registry policy defaulting, a distinct typed override, and zero rejection;
- registry refresh, background reader, and admin connections consume the
  distinctive timeout;
- every standalone registry-options surface preserves environment/CLI
  precedence and rejects zero;
- the load-test child renderer emits the effective value once;
- CRD defaulting, override, exact error text, and schema minimum;
- operator control caching distinguishes policies with different reader/admin
  DNS values;
- compute and activator deployments render the value exactly once; and
- two fresh generations of all nine CRDs match each other and `deploy/crds`.

Affected package tests, strict Clippy, formatting, diff checks, CLI help, and
the runtime-value audit must pass before publication.

## Scope

This slice covers DNS resolution used by the Gres registry reader, registry
refresh, registry topic creation, and registry metadata refresh. It fixes the
shared `AdminClient` lookup boundary so existing and future callers that pass
`ConnectionOptions::dns_timeout` receive the documented behavior.

It does not combine the producer and reader/admin settings, change retry
backoff, configure connection or request timeouts, or alter unrelated raw DNS
sites. The repository-wide hardcoded operational-value audit remains open.
