# Crabka

[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://codspeed.io/robot-head/crabka?utm_source=badge)

A Rust reimplementation of [Apache Kafka](https://kafka.apache.org), distributed under the
Apache License 2.0 as a derivative work.

This repository hosts the [`crabka-protocol`](crates/protocol) crate. Other components
(broker, clients, KRaft, etc.) will arrive in their own crates over time. See the design
spec for the full roadmap.

## Status

Pre-1.0, pre-alpha. No production use.

## Published crates

- [`crabka-compression`](https://crates.io/crates/crabka-compression) — Kafka wire-protocol compression codecs ([docs](https://docs.rs/crabka-compression)).
- [`crabka-protocol`](https://crates.io/crates/crabka-protocol) — Apache Kafka wire-protocol codec ([docs](https://docs.rs/crabka-protocol)).

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
