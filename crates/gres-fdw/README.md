# crabka-gres-fdw

Foreign-data wrapper that exposes Kafka topics as SQL foreign tables inside the
Crabka Gres engine.

This crate is the Crabka port of the donor `kafka_fdw` crate from
`crabgresql@93f3d17168d056a28b4abe60af3b489d4bf62f1d`. It keeps the donor FDW
contract: resolve catalog FDW options into Kafka client profiles, import topic
schemas from Schema Registry, scan bounded Kafka offsets, and project raw,
Avro, JSON, and Protobuf values into Crabka PostgreSQL datums.

The `roundtrip` feature holds the in-process broker/pgwire round-trip test.
Normal package checks then test deterministic unit behavior and do not start
the full Kafka stack.
