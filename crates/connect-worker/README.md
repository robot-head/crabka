# crabka-connect-worker

`crabka-connect-worker` runs one `PostgreSQL` logical-decoding source connector
per process and writes its change records to Kafka. It uses an idempotent
producer with `acks=all` and persists the acknowledged `PostgreSQL` LSN in the
compacted `__crabka_connect_offsets` topic, keyed by connector ID.

Delivery is at least once: a crash after Kafka acknowledges data but before the
checkpoint record is durable can replay records, but the worker never advances
the `PostgreSQL` slot before both barriers complete.

The HTTP listener exposes `/live`, `/ready`, and `/metrics`.
