# Managed Postgres CDC

`KafkaConnector` runs one operator-managed PostgreSQL logical-decoding worker
and writes its change records to Crabka topics. The initial implementation
supports PostgreSQL sources only.

## Prerequisites

- Install the Crabka operator and the `KafkaConnector` CRD.
- In the connector namespace, provide a ready `Kafka` named by the
  `crabka.io/cluster` label. It must expose an internal TLS listener with TLS
  client authentication; the operator creates the connector's `KafkaUser` and
  mounts its credentials.
- Configure PostgreSQL with logical decoding (`wal_level=logical`). The URL
  Secret must identify a user allowed to read the selected tables and manage or
  use the publication and logical replication slot.
- Give captured tables stable keys. Set an appropriate PostgreSQL replica
  identity when update or delete events must include enough key data.

The connector creates the configured publication and `pgoutput` logical slot
when they are absent, so its PostgreSQL user needs those privileges. If they
already exist, the publication must cover every configured table, publish
insert/update/delete but not truncate, and the slot must be a logical
`pgoutput` slot for the same database. Table names in the example are relative
to `spec.schema`.

## Apply and observe

Copy [the example manifest](examples/kafka-connector-postgres.yaml), replace the
namespace, cluster name, and database URL, then apply it:

```bash
kubectl apply -f docs/examples/kafka-connector-postgres.yaml
kubectl -n streaming get kafkaconnector orders-cdc -w
```

The operator reports `Ready`, `Paused`, and `Failed` conditions and creates a
Deployment with the same name as the connector. Inspect progress with:

```bash
kubectl -n streaming describe kafkaconnector orders-cdc
kubectl -n streaming get deployment orders-cdc
kubectl -n streaming logs deployment/orders-cdc
```

Invalid configuration and missing Secrets appear in status instead of starting
a worker. The database Secret must be in the connector's namespace.

## Pause and resume

Pausing retains the durable checkpoint and scales the worker to zero:

```bash
kubectl -n streaming patch kafkaconnector orders-cdc --type=merge \
  -p '{"spec":{"paused":true}}'
```

Set `paused` back to `false` to create one worker replica and resume from the
saved LSN:

```bash
kubectl -n streaming patch kafkaconnector orders-cdc --type=merge \
  -p '{"spec":{"paused":false}}'
```

## Topics and delivery

The source names each record after its schema-qualified relation. The worker
prepends `spec.topicPrefix` with a dot; the default prefix is `db`. For example,
`public.orders` is written to `db.public.orders`. Kafka chooses the partition
from the encoded row key. Deletes are keyed tombstones, and records retain the
PostgreSQL table, operation, and LSN headers.

Delivery is at least once. The worker first obtains Kafka acknowledgement for
the data, then durably stores the LSN in the compacted
`__crabka_connect_offsets` topic, and only then advances the PostgreSQL slot. A
crash after the data acknowledgement but before the checkpoint is durable can
replay records after restart. Consumers must tolerate duplicates; keys and the
`crabka.pg.lsn` header can support idempotent processing. This contract avoids
advancing the source beyond durably acknowledged Kafka data, but it is not
exactly-once source delivery.

## Health and metrics

The worker listens on port `8080` inside its pod:

- `/live` returns success while the worker process is live.
- `/ready` returns success while the connector runtime can process records.
- `/metrics` exposes OpenMetrics counters and live/ready gauges.

Kubernetes uses `/live` and `/ready` for its probes. For direct inspection:

```bash
kubectl -n streaming port-forward deployment/orders-cdc 8080:8080
curl http://127.0.0.1:8080/metrics
```

## Current scope

This release does not provide initial database snapshots, exactly-once source
delivery, arbitrary source or sink plugins, JVM connector loading, the Kafka
Connect distributed-worker protocol or REST API, multiple tasks per connector,
or more than one worker replica per connector.
