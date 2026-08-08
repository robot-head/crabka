# crabka-connect-worker

`crabka-connect-worker` runs one `PostgreSQL` logical-decoding source connector
per process and writes its change records to Kafka. It uses an idempotent
producer with `acks=all` and persists the acknowledged `PostgreSQL` LSN in the
compacted `__crabka_connect_offsets` topic, keyed by connector and source
identity.
The key also hashes the `PostgreSQL` URL and slot, so changing the source cannot
reuse an incompatible checkpoint from an earlier deployment.

Auto-created data and checkpoint topics use the configured replication factor;
the managed operator derives it from the cluster size, capped at three.

Delivery is at least once: a crash after Kafka acknowledges data but before the
checkpoint record is durable can replay records, but the worker never advances
the `PostgreSQL` slot before both barriers complete.

The HTTP listener exposes `/live`, `/ready`, and `/metrics`.
