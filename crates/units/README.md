# crabka-units

Dimensioned quantities for Crabka: byte counts, byte rates, durations, frequencies, and ratios.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.
It wraps [`uom`](https://docs.rs/uom) in a small vocabulary — `ByteSize`, `ByteRate`, `Time`, `Frequency`, `Ratio` — so a configured size, quota, or timeout carries its dimension in the type and the compiler rejects a millisecond passed where bytes were meant. Cross-dimension arithmetic (`ByteSize / Time == ByteRate`) is checked rather than commented. It is a zero-IO, WASM-buildable leaf crate; the generated Kafka wire codec stays raw integers and converts at the boundary through the extension traits in [`convert`](src/convert.rs).

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
