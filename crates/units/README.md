# crabka-units

Dimensioned quantities for Crabka: byte counts, byte rates, durations, frequencies, and ratios.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

It wraps [`uom`](https://docs.rs/uom) in a small vocabulary: `ByteSize`, `ByteRate`, `Time`, `Frequency`, and `Ratio`. A configured size, quota, or timeout carries its dimension in the type, and the compiler rejects a millisecond where the code needs bytes. The compiler also checks cross-dimension arithmetic such as `ByteSize / Time == ByteRate`. The types enforce that rule, so no comment has to describe it.

This is a zero-IO, WASM-buildable leaf crate. The generated Kafka wire codec keeps raw integers and converts at the boundary through the extension traits in [`convert`](src/convert.rs).

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
