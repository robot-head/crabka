# Client Core DNS Timeout Policy Design

Bound every generic Kafka client DNS lookup with one validated per-lookup deadline.

## Design Goals

Initial bootstrap resolution, bootstrap re-resolution, and advertised-broker resolution must not wait indefinitely on the system resolver. DNS remains distinct from TCP connection establishment and request handling because each phase can stall independently.

The policy must preserve existing client behavior: an individual lookup failure does not prevent trying other bootstrap entries, advertised brokers that cannot resolve remain absent from the pool, and bootstrap returns `Disconnected` only when no entry resolves.

## Architecture Overview

`crabka-client-core` owns one positive `ClientDnsTimeout` value with a 10-second default. The existing client builder accepts a raw `Duration`, validates it before the first lookup, and stores the typed value in `ConnectionOptions`.

Bootstrap parsing applies the deadline independently to each non-empty `host:port` entry. `Client::reconnect_bootstrap` reuses the same stored policy. `BrokerPool` copies the policy from `ConnectionOptions` and applies it independently to each hostname learned from metadata.

The existing 30-second TCP-connect and request defaults become named public constants while this policy is introduced. Their behavior does not change.

## Key Design Decisions

### One DNS Policy Covers Both Lookup Sites

Bootstrap and advertised-broker resolution use the same resolver and have the same operational failure mode. Separate knobs would add configuration and propagation work without a demonstrated need.

The timeout applies per lookup rather than to the whole bootstrap list or metadata refresh. This preserves ordered fallback: one slow hostname cannot block forever, while later entries still receive a full attempt.

### DNS Does Not Reuse the TCP Connect Timeout

DNS resolution and TCP connection establishment are separate phases. Reusing the connect timeout would make one setting control two unrelated operations and would make resolver latency consume an implicit portion of connection policy.

### Validation Lives at the Client Boundary

`ClientDnsTimeout` accepts only positive, whole-millisecond durations representable as `u64` milliseconds. It uses `refined_type` for the positive invariant. Invalid builder input returns `ClientError::InvalidConfig` before DNS or socket I/O.

`ConnectionOptions` stores the validated type, so direct option construction cannot pass an invalid DNS deadline into the pool. No resolver trait, global setting, or new dependency beyond the workspace's existing `refined_type` dependency is introduced.

### Existing Failure Semantics Remain

Resolver errors and deadline expiry are logged and skipped in bootstrap resolution. Advertised-broker resolution continues to skip an unresolved broker. No new runtime DNS-timeout error variant is required because the existing APIs deliberately collapse exhausted bootstrap resolution to `Disconnected` and make metadata refresh best-effort.

## Integration

This slice exposes the policy through `Client::builder()` and `ConnectionOptions`, which are the generic ownership boundaries for `crabka-client-core`. Higher-level producer, consumer, streams, admin, and service deployment surfaces remain separate propagation owners because each constructs and sometimes clones clients differently.

The next slice will carry this typed client policy through those higher-level builders and then expose it through the CLI/environment or CRD surface that owns each deployed component. This separation avoids changing unrelated public builders before the generic behavior is tested and stable.

## Testing

A private future seam around `tokio::time::timeout` permits paused-time tests with a permanently pending lookup. Tests pin the default, replacement, zero/fractional rejection, exact deadline, successful and failed resolution behavior, bootstrap fallback, reconnect reuse, and advertised-broker timeout behavior without relying on external DNS.

Affected `crabka-client-core` tests, strict Clippy, formatting, and the runtime-value scanner must pass before the slice is published.
