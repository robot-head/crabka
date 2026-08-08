# crabka-ids

Canonical newtypes for Crabka's cross-crate Kafka identifiers.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.
It defines the shared `Offset`, `PartitionIndex`, … newtypes. The same identifier type then flows through the log, broker, consensus, replication, and observability crates, and no crate mints its own raw-integer copy. It is a zero-IO, WASM-buildable leaf crate. The generated wire codec stays raw and converts at the boundary with `From`/`Into`. See [`docs/newtype-safety-rollout.md`](../../docs/newtype-safety-rollout.md).

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
