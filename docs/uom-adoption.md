# Dimensioned Values: `crabka-units` Adoption

`crabka-units` wraps [`uom`](https://docs.rs/uom) so a size, a rate, or a timeout
carries its dimension in the type. This is the sibling of
[`docs/newtype-safety-rollout.md`](newtype-safety-rollout.md): newtypes give
*identifiers* distinct types, quantities give *magnitudes* distinct dimensions.

## The vocabulary

| Alias | `uom` quantity | Base unit | Use for |
| --- | --- | --- | --- |
| `ByteSize` | `Information` | byte | message/segment/buffer sizes, quota balances |
| `ByteRate` | `InformationRate` | byte/s | producer and replication quotas, measured throughput |
| `Time` | `Time` | second | timeouts, intervals, retention windows, latencies |
| `Frequency` | `Frequency` | hertz | records/s, requests/s |
| `Ratio` | `Ratio` | — | fill factors, sampling probabilities, percentages |

All five store `f64` in base units, which is what lets `uom` combine them:
`ByteSize / Time` is a `ByteRate`, checked by the compiler.

```rust
use crabka_units::prelude::*;

let quota: ByteRate = mebibytes_per_sec(10);
let backlog: ByteSize = mebibytes(50);
let drain: Time = quota.time_to_transfer(backlog); // 5s
```

## What converts, and what does not

**Convert** a value that is a magnitude held in a bare number: `max_bytes: i32`,
`session_timeout_ms: i32`, `throttle_bytes_per_sec: i64`, `buffer_size: usize`,
`interval_secs: u64`.

**Leave alone**:

- **The generated Kafka codec** (`crates/protocol/generated`). It must stay
  byte-exact; convert at the hand-written boundary instead.
- **Instants.** An offset, a leader epoch, or an epoch-milliseconds timestamp is a
  coordinate, not a magnitude — those stay `crabka-ids` newtypes. `Time` is an
  *extent*: a difference between instants, never an instant.
- **Counts of things.** A partition count, a replica count, a retry budget, a
  record count. Dimensionless integers are already unambiguous.
- **Atomics and verified kernels.** `AtomicU64` cannot hold a quantity, and the
  Creusot-verified arithmetic in `crabka-throttle` translates only over integers.
  Keep the raw representation and convert in the accessors.

## The seams

Every conversion goes through `crabka_units::convert`, so the rounding and
saturation rules live in one place. Conversions in are exact; conversions out
round to nearest and saturate.

```rust
use crabka_units::prelude::*;

// Wire in / wire out.
let timeout = Time::from_millis(i64::from(request.session_timeout_ms));
response.throttle_time_ms = throttle.millis_i32();

// tokio, std, qubit-clock.
tokio::time::sleep(interval.to_std()).await;
let elapsed = observed_duration.as_time();

// Kafka's `-1` "unlimited" sentinel becomes an absence.
let retention: Option<ByteSize> = wire::opt_size_from_bytes_i64(raw);
```

## Config and JSON

A config struct holds quantities and reads the form an operator writes:

```rust
#[derive(Serialize, Deserialize)]
struct TopicConfig {
    #[serde(with = "crabka_units::serde_units::human::byte_size")]
    segment_size: ByteSize,            // "512MiB"
    #[serde(with = "crabka_units::serde_units::numeric::millis_i64")]
    retention: Time,                   // 604800000
}
```

Use `human::*` for operator-facing YAML and `numeric::*` where the encoding must
mirror a Kafka wire field or an existing JSON API. `human::*` rejects a bare
number: guessing whether `30` means seconds or milliseconds is the failure the
type exists to prevent.

For logs and diagnostics, `Human::human()` renders the operator form:
`tracing::info!(size = %limit.human(), "…")` prints `512MiB`.

## Naming after conversion

Drop the unit from the name once the type carries it: `session_timeout_ms: i32`
becomes `session_timeout: Time`, `fetch_max_bytes: i32` becomes `fetch_max:
ByteSize`. A field still named `_ms` that no longer holds milliseconds is worse
than either. Keep the suffix only where the name mirrors a Kafka config key or
wire field exactly, and the value is still the raw integer.
