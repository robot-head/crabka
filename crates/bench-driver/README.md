# crabka-bench-driver

[![Crates.io](https://img.shields.io/crates/v/crabka-bench-driver.svg)](https://crates.io/crates/crabka-bench-driver)
[![Docs.rs](https://docs.rs/crabka-bench-driver/badge.svg)](https://docs.rs/crabka-bench-driver)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Load driver + report aggregator for the Crabka vs Strimzi benchmark harness.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-bench-driver = "0.3.2"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Run one benchmark scenario against a reachable Kafka-compatible cluster and write the JSON report:

```bash
cat > /tmp/smoke.yaml <<'YAML'
name: smoke-produce-consume
producer:
  records: 10000
  valueBytes: 512
consumer:
  expectedRecords: 10000
YAML

crabka-bench-driver \
  --scenario /tmp/smoke.yaml \
  --bootstrap localhost:9092 \
  --stack crabka \
  --topic bench-topic \
  --broker-count 1 \
  --out /tmp/crabka-run.json
```

## Documentation

API documentation is published on [docs.rs/crabka-bench-driver](https://docs.rs/crabka-bench-driver). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
