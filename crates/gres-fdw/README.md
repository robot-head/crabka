# crabka-gres-fdw

Foreign-data wrapper that exposes Kafka topics as SQL foreign tables inside the
Crabka Gres engine.

This crate is the Crabka port of the donor `kafka_fdw` crate from
`crabgresql@93f3d17168d056a28b4abe60af3b489d4bf62f1d`. It keeps the donor FDW
contract: resolve catalog FDW options into Kafka client profiles, import topic
schemas from Schema Registry, scan bounded Kafka offsets, and project raw,
Avro, JSON, and Protobuf values into Crabka PostgreSQL datums.

The in-process broker/pgwire round-trip test is retained behind the
`roundtrip` feature so normal package checks exercise deterministic unit
behavior without starting the full Kafka stack.
