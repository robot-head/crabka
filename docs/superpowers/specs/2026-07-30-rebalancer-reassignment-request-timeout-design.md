# Rebalancer Reassignment Request Timeout

## Scope

Expose the Kafka broker-side timeout carried by
`AlterPartitionReassignmentsRequest`. The rebalancer currently inherits the
generated protocol default of 60 seconds for both submit and cancel requests.
Preserve that default while making the deployment policy explicit.

This slice does not change the executor deadline, reassignment polling cadence,
client connection timeout, or client request deadline.

## Validated Policy

`crabka-rebalancer` owns a `ReassignmentRequestTimeout` newtype. It accepts a
UOM `Time` and stores the validated whole-millisecond value required by the
Kafka protocol. Construction rejects non-finite, zero, negative, fractional
millisecond, and values greater than `i32::MAX` milliseconds. Validation uses
`refined_type`; the default remains 60 seconds.

`LiveClient::new` remains source-compatible and uses the default timeout.
`LiveClient::with_reassignment_request_timeout` accepts the explicit policy
used by the binary.

## Request Construction

Submit and cancel request builders receive `ReassignmentRequestTimeout` and
set `AlterPartitionReassignmentsRequest.timeout_ms` explicitly. They no longer
rely on the generated request's `Default` implementation for deployment
policy. Topic grouping, replica lists, replication-factor changes, tagged
fields, and response handling remain unchanged.

## Deployment Configuration

The standalone binary exposes:

```text
--reassignment-request-timeout
CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT
```

The value uses human UOM syntax such as `60s` and is parsed as positive
`Time`. The validated newtype performs the Kafka whole-millisecond and `i32`
range checks before client construction.

The Helm chart exposes:

```text
reassignmentRequestTimeout: 60s
```

and renders it to the environment variable above.

No CRD field is added. `KafkaRebalance` owns per-proposal goals and throttle,
whereas this timeout is transport policy for the standalone rebalancer daemon.

## Verification

- newtype tests cover the default, a custom timeout, zero, fractional
  milliseconds, and `i32` overflow;
- submit and cancel request tests prove the configured timeout is framed;
- binary tests prove the default, CLI override, environment override, and
  invalid-value rejection;
- Helm unit tests prove the default and overridden value are rendered;
- focused tests, Helm lint/unit tests, workspace strict Clippy, nightly
  formatting, and `git diff --check` pass.
