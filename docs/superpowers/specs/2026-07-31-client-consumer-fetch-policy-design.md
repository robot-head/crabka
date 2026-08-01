# Client Consumer Fetch Policy Design

## Goal

Expose the classic Consumer's complete fetch byte policy through its production
demo owner while replacing the remaining hardcoded `FetchRequest.min_bytes`.
Preserve the existing defaults:

- minimum response size: `1B`;
- total response maximum: `50MiB`; and
- per-partition response maximum: `1MiB`.

## Library API

Reuse the existing validated types:

- `crabka_client_core::FetchMinBytes`;
- `ConsumerFetchMaxBytes`; and
- `ConsumerFetchPartitionMaxBytes`.

Add `fetch_min: ByteSize` to `Consumer::builder()`, defaulting to `1B`.
Validation occurs before startup retry or network I/O. Store the validated
minimum with the existing total and per-partition byte budgets, then write it
to every `FetchRequest.min_bytes`.

All three values must be positive, finite, whole-byte UOM quantities fitting
Kafka's signed `i32` fields. The minimum must not exceed the total maximum.
Do not require the per-partition maximum to be less than the total maximum:
Kafka permits the first oversized record batch to exceed the aggregate fetch
budget, and the two settings have distinct semantics.

Do not add another policy struct or new dependency. The existing semantic
newtypes already enforce the required boundaries.

## Demo Configuration

The observability demo Consume role exposes:

| CLI | Environment | Default |
|---|---|---:|
| `--consumer-fetch-min` | `CRABKA_DEMO_CONSUMER_FETCH_MIN` | `1B` |
| `--consumer-fetch-max` | `CRABKA_DEMO_CONSUMER_FETCH_MAX` | `50MiB` |
| `--consumer-fetch-partition-max` | `CRABKA_DEMO_CONSUMER_FETCH_PARTITION_MAX` | `1MiB` |

Use direct `ByteSize` UOM parsing. Keep fields optional so explicit CLI or
environment inputs can be rejected on Produce and Stream roles. Absence selects
the typed library defaults. Resolve and validate the complete policy before
telemetry initialization or external I/O, then pass the three values directly
to `Consumer::builder()`.

Only `demo-consume` receives the three Compose variables. No CRD is added
because the operator does not own the standalone demo Consumer.

## Errors and Compatibility

Reject zero, negative, fractional-byte, non-finite, and `i32`-overflow values.
Reject a minimum greater than the total maximum. Error messages identify the
invalid setting or relationship. CLI values retain Clap's precedence over
environment values.

Existing callers remain source-compatible because all builder additions have
defaults. Fetch request versions, polling timeout behavior, oversized first
batch handling, isolation level, and per-partition budgeting remain unchanged.

## Verification

Tests cover:

- exact library defaults and custom request propagation;
- invalid UOM values and minimum-above-maximum rejection;
- demo defaults and explicit overrides;
- CLI-over-environment precedence;
- rejection on Produce and Stream roles before external I/O; and
- Compose defaults and Consume-only ownership.

Run affected all-target tests, strict workspace Clippy, workspace check,
nightly formatting, and diff hygiene. Update `docs/configuration-audit.md` only
after implementation passes those gates. Do not run `cargo clean`; it remains
reserved for completion of the repository-wide goal.
