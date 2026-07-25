# Gres WAL Producer DNS Timeout Design

Expose the generic client DNS deadline through the WAL producer's existing
Gres configuration path.

## Design Goals

The Gres WAL producer must not inherit an untunable DNS deadline while its
other operational timeouts are configurable. Operators must be able to set the
deadline through the Gres CRD or, for standalone and Kubernetes overrides,
through a command-line argument backed by an environment variable.

The existing 10-second client DNS default and all producer retry, TCP-connect,
request, and transaction behavior remain unchanged when the setting is absent.

## Architecture

`crabka-client-producer` adds one `dns_timeout: Duration` builder input,
defaulting to `crabka_client_core::DEFAULT_CLIENT_DNS_TIMEOUT`. The producer
validates it with the existing `ClientDnsTimeout` refined type before starting
network I/O and forwards its duration to `Client::builder().dns_timeout(...)`.
No producer-specific DNS type or resolver abstraction is added.

Gres stores the validated `ClientDnsTimeout` separately from
`ProducerRetryPolicy`, because DNS resolution is independent of protocol
request and retry timing. `LiveRecoveryConfig` carries that value to the WAL
producer builder.

The standalone surface adds:

- `--wal-producer-dns-timeout-ms`
- `CRABKA_GRES_WAL_PRODUCER_DNS_TIMEOUT_MS`

The Gres CRD adds optional
`spec.compute.walProducerDnsTimeoutMs`. The operator resolves the CRD value to
the same validated type and emits the command-line argument into every Gres
compute deployment.

## Validation and Precedence

The value is a positive whole number of milliseconds representable as `u64`.
The CLI uses the existing positive-millisecond parser. The CRD schema declares
a minimum of one, while its `u64` field supplies the upper representation
bound. Both paths construct `ClientDnsTimeout`, keeping the client boundary as
the final invariant check.

Invalid producer builder input returns `ProducerError::InvalidConfig` before
DNS or socket I/O. Invalid CLI input is rejected by Clap. Invalid CRD input is
rejected during effective-policy construction with the field name
`spec.compute.walProducerDnsTimeoutMs`.

Precedence is:

1. command-line argument;
2. environment variable;
3. `ClientDnsTimeout::default()` (10 seconds).

As with the existing WAL producer settings, configuring this option without
`--substrate-bootstrap` is invalid.

## Data Flow

The configured value follows one explicit path:

`GresTenant.spec.compute.walProducerDnsTimeoutMs`
→ operator effective compute policy
→ `--wal-producer-dns-timeout-ms`
→ Gres `ServeArgs`
→ `SubstrateRuntimeConfig`
→ `LiveRecoveryConfig`
→ `Producer::builder().dns_timeout(...)`
→ `Client::builder().dns_timeout(...)`
→ `ConnectionOptions::dns_timeout`.

Standalone deployments enter the same path at `ServeArgs`, using either the
environment variable or command-line argument.

## Testing

Focused tests prove:

- producer builder defaults to the client DNS default and forwards an override;
- invalid producer DNS durations fail before resolution;
- Gres uses the default and preserves environment/CLI precedence;
- the option is rejected without substrate mode;
- CRD defaulting, override, validation, and schema minimum are exact;
- the operator emits the effective value exactly once in single- and
  multi-range deployments;
- all checked-in CRDs regenerate without drift.

Affected package tests, strict Clippy, formatting, diff checks, and the
runtime-value audit must pass before publication.

## Scope

This slice covers the Gres WAL producer end to end. Consumer, streams, admin,
and other producer deployments remain separate configuration owners and will
be traced independently rather than receiving speculative unused settings.
