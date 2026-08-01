# Rebalancer Runtime Policy Design

## Goal

Replace the standalone rebalancer's remaining production timing, retry,
bounded-memory, and state-topic policy literals with validated CLI/environment
configuration while preserving existing behavior.

## Configuration boundary

Add one `RebalancerRuntimePolicy` in `crabka-rebalancer` and flatten matching
options into the existing binary CLI. Every option has a
`CRABKA_REBALANCER_*` environment variable and a matching Helm value because
the rebalancer is deployed by its standalone chart, not by a workload CRD.

| Setting | Default |
|---|---:|
| recovery load poll interval | `100ms` |
| executor shutdown drain timeout | `10s` |
| ingester shutdown join timeout | `5s` |
| scraper HTTP timeout | `5s` |
| cancellation drain timeout | `5s` |
| cancellation poll interval | `25ms` |
| detector snapshot-history capacity | `10` |
| state-topic creation timeout | `10s` |
| state-topic loader poll interval | `100ms` |
| quiet polls before declaring state loaded | `5` |
| state-topic fetch maximum | `1MiB` |
| state produce retry attempts | `50` |
| state produce retry backoff | `200ms` |
| state produce timeout | `10s` |
| state-topic minimum cleanable dirty ratio | `1%` |
| state-topic segment interval | `1m` |

Times, byte limits, and ratios remain dimensioned UOM values. Positive counts
use a `refined_type`-validated newtype. Validation requires the cancellation
poll interval to be below its drain timeout, Kafka millisecond fields to be
exact and representable, the fetch maximum to fit Kafka's signed `i32`, and
the dirty ratio to be strictly between zero and one.

## Runtime ownership

The binary resolves one policy before external I/O. It passes relevant values
to the recovery and shutdown loops, `Scraper`, `AppState`, `StateTopic`,
`StateTopicLoader`, and `topic_admin::ensure_topic`. Existing public
constructors remain default-preserving wrappers for tests and downstream
callers.

The state-topic producer stores the policy it needs beside the client and
topic name, avoiding new parameters on every executor persistence call. The
loader similarly owns its polling and fetch values. The cancellation handler
reads its two values from `AppState`.

## Fixed values

The state topic remains compacted, single-partition, and keyed by `in_flight`:
those define its storage protocol rather than deployment tuning. Kafka error
codes, record versions, `acks=all`, partition zero, health paths, and service
ports remain fixed compatibility values. Test-only deadlines remain test
controls.

## Verification

Tests cover defaults, scalar and relational validation, CLI-over-environment
precedence, runtime-owner propagation, topic creation configs, and Helm
rendering. Closure runs rebalancer all-target tests, chart tests, workspace
check, strict Clippy, nightly formatting, and diff hygiene.
