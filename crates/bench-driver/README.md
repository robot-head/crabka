# crabka-bench-driver

[![Crates.io](https://img.shields.io/crates/v/crabka-bench-driver.svg)](https://crates.io/crates/crabka-bench-driver)
[![Docs.rs](https://docs.rs/crabka-bench-driver/badge.svg)](https://docs.rs/crabka-bench-driver)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Load driver + report aggregator for the Crabka vs Strimzi benchmark harness.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-bench-driver
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Run one benchmark scenario against a reachable Kafka-compatible cluster and write the JSON report.

Every size, duration, and rate in the scenario carries its unit — `512B`, `5ms`, `20000/s`. A bare number is rejected rather than guessed at, so a scenario cannot silently mean milliseconds where it meant seconds:

```bash
cat > /tmp/smoke.yaml <<'YAML'
name: smoke-produce-consume
mode_tag: ci
msg_size: 512B
partitions: 6
producers: 1
consumers: 1
mode:
  kind: saturate
acks: leader
linger: 5ms
batch_size: 16KiB
warmup: 5s
duration: 30s
YAML

crabka-bench-driver \
  --scenario /tmp/smoke.yaml \
  --bootstrap localhost:9092 \
  --stack crabka \
  --topic bench-topic \
  --broker-count 1 \
  --out /tmp/crabka-run.json
```

Paced runs replace `mode: {kind: saturate}` with an explicit event rate:

```yaml
mode:
  kind: fixed_rate
  rate: 20000/s
```

The `RunOutput` JSON the driver writes encodes its *measurements* as exact integers instead — latencies in nanoseconds, sizes in bytes — so the report aggregator compares and plots them without rounding.

## Documentation

API documentation is published on [docs.rs/crabka-bench-driver](https://docs.rs/crabka-bench-driver). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
