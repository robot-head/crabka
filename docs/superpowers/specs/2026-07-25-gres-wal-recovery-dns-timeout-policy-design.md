# Gres WAL Recovery DNS Timeout Policy

Bound raw committed-WAL hostname resolution with validated standalone and fleet configuration.

## Design Goals

Raw committed-WAL replay and committed-end sampling must not wait indefinitely for DNS before their configured TCP connection timeout begins.

DNS resolution is an independently tunable operation because cluster DNS latency and TCP connection latency have different failure modes. The existing WAL recovery connect timeout retains its documented meaning: it bounds TCP establishment only.

## Configuration Surface

Standalone Gres accepts `--wal-recovery-dns-timeout-ms`, backed by `CRABKA_GRES_WAL_RECOVERY_DNS_TIMEOUT_MS`.

The optional value is a positive whole number of milliseconds and requires `--substrate-bootstrap`. Its default is 10,000 milliseconds. Zero and malformed values fail during CLI or environment parsing before network I/O.

The Gres fleet CRD exposes the same policy as:

```yaml
spec:
  compute:
    walRecoveryDnsTimeoutMs: 10000
```

The optional field has a schema minimum of one. The operator resolves the default through the same validated boundary and renders one exact argument pair for every single-range and multi-range compute Deployment.

## Architecture Overview

`crabka-gres-substrate` owns `DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS` and stores the validated duration in the existing `RecoveryReadPolicy`. A source-compatible `with_dns_timeout` builder validates positive milliseconds with `refined_type`; no new policy object or dependency is introduced.

`LiveRecoveryConfig` already carries `RecoveryReadPolicy` through every raw WAL path. The existing `open_wal_connection` boundary applies the configured deadline to `tokio::net::lookup_host` before selecting the first returned address and opening the broker connection.

Gres extends its existing WAL recovery policy assembly with the CLI/environment value. The operator extends `GresComputeSpec::effective_policy` and the shared compute argument renderer, so the same effective value reaches all managed compute layouts.

## Key Design Decisions

### DNS Has Its Own Deadline

Reusing `walRecoveryConnectTimeoutMs` would be shorter code, but it would change that setting from a TCP deadline into a mixed DNS/TCP policy and contradict its accepted design. Moving the deadline into generic client-core would affect unrelated clients and is outside this owner.

An independent DNS deadline keeps failures diagnosable and lets operators tune slow cluster DNS without weakening TCP failure detection.

### Only the Existing Raw WAL Lookup Is in Scope

This slice bounds the lookup in `open_wal_connection`, which serves normal committed-WAL replay and committed-end sampling. Admin, registry, producer, broker-pool, and other client-library lookups remain separate owners because they have distinct retry and connection lifecycles.

Bootstrap ordering, first-address selection, security, request timeouts, fetch limits, and retry counts do not change. They are not additional DNS timeout settings.

### Timeout Errors Stay at the Lookup Boundary

DNS resolver errors and empty results retain their existing field-specific messages. Deadline expiry returns an unavailable error naming the bootstrap address and configured timeout. TCP connection failures continue to use the existing connection error path.

## Testing

A narrow async lookup helper accepts the real `lookup_host` future in production and a pending future in tests. Paused-time coverage proves that the exact configured duration ends a stalled lookup without relying on external DNS.

Substrate tests pin the default, positive validation, configured accessor, successful resolution, resolver failure, empty results, and exact timeout behavior.

Gres tests cover default, environment input, CLI-over-environment precedence, zero rejection, inert local-mode rejection, hostile environment isolation, help output, and propagation through the shared recovery configuration.

Operator tests cover serialization, schema minimum, default and override validation, and exact single-range and multi-range arguments. Two fresh nine-file CRD generations must match each other and the checked-in manifests.

Full affected all-target tests, strict Clippy, formatting, `git diff --check`, and focused configuration-audit searches must pass before publication.

## Deferred Owners

Other DNS lookups remain visible in the repository audit and must be handled through their own existing configuration owners rather than a speculative global resolver abstraction.
