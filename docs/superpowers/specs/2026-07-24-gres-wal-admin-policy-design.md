# Gres WAL Admin Policy

## Goal

Replace fixed WAL topic replication and admin-operation timing with one
validated policy exposed through Gres CLI/environment and the fleet CRD.

## Scope

The policy owns:

- WAL topic replication factor: 1
- WAL topic ensure timeout: 30,000 ms
- admin connection timeout: 5,000 ms
- admin request timeout: 30,000 ms

WAL partition count remains one because range WAL ordering depends on a single
partition. `cleanup.policy=delete` and `retention.ms=-1` remain durability
invariants. DNS, producer, checkpoint deletion, registry, and generic-client
policy are separate owners.

## Configuration Surface

Standalone Gres accepts four optional positive settings:

- `--wal-topic-replication-factor`
  / `CRABKA_GRES_WAL_TOPIC_REPLICATION_FACTOR`
- `--wal-topic-ensure-timeout-ms`
  / `CRABKA_GRES_WAL_TOPIC_ENSURE_TIMEOUT_MS`
- `--wal-admin-connect-timeout-ms`
  / `CRABKA_GRES_WAL_ADMIN_CONNECT_TIMEOUT_MS`
- `--wal-admin-request-timeout-ms`
  / `CRABKA_GRES_WAL_ADMIN_REQUEST_TIMEOUT_MS`

Explicit settings require substrate mode and fail before listener or network
I/O otherwise.

The optional fleet fields under `spec.compute` are:

```yaml
walTopicReplicationFactor: 1
walTopicEnsureTimeoutMs: 30000
walAdminConnectTimeoutMs: 5000
walAdminRequestTimeoutMs: 30000
```

Every schema minimum is one. The operator renders all four effective values
for every substrate compute.

## Ownership and Data Flow

`crabka-gres-substrate::WalAdminPolicy` owns the four compiled defaults and
validates raw values with `refined_type`. It stores private protocol values and
`Duration`s with typed accessors.

`LiveRecoveryConfig` defaults the policy and offers `with_wal_admin_policy`.
Gres resolves optional parser values once in `SubstrateRuntimeConfig` and
applies both recovery policies through the existing single constructor helper.

`AdminClient` gains an options-based connection entry point. It stores the
complete `ConnectionOptions` template so controller and bootstrap reconnects
reuse the configured client id, security, connect timeout, and request
timeout. Existing `connect` and `connect_secured` callers retain their current
5-second/30-second defaults.

The existing public WAL topic ensure helpers retain default behavior.
Policy-aware variants pass replication factor and ensure timeout through the
narrow `TopicAdmin` seam. Live recovery uses those variants; tests and
external callers remain source-compatible where practical.

## Errors

- Zero values fail CLI/environment parsing, programmatic policy construction,
  CRD schema validation, and operator effective-policy validation.
- Explicit local-mode settings fail before I/O.
- No ordering relationship is imposed between connection, request, and topic
  operation timeouts because they guard independent operations.

## Tests and Verification

- Client-admin tests prove custom options reach initial dialing and every
  reconnect path while existing constructors preserve defaults.
- Substrate tests pin defaults, validation, topic request replication/timeout,
  configured admin options, and `LiveRecoveryConfig` replacement.
- Gres tests extend the hostile-environment and precedence matrix from six
  recovery variables to ten and prove shared-helper propagation.
- Operator tests pin schema, defaults, validation paths, and exact
  single-/multi-range arguments.
- All nine generated CRDs, full affected tests, strict Clippy, help output,
  formatting, and diff checks must pass.

## Deferred Values

DNS lookup, producer construction and transaction initialization, checkpoint
deletion, registry, and generic client defaults remain separate policy owners.
