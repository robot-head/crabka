+++
title = "Schema Registry Deployment"
description = "Deploy Crabka's Confluent-compatible Schema Registry: a REST service that stores schemas in the compacted _schemas topic and enforces compatibility checks."
weight = 20
template = "docs/page.html"

[extra]
mermaid = true
+++

Crabka includes a Confluent Schema Registry-compatible REST service. It runs as
a separate Kafka client, stores state in the compacted `_schemas` topic, and can
be deployed by the operator or as a standalone Helm release.

Use it when producers and consumers exchange structured records and need schema
IDs, compatibility checks, and existing Confluent serializer/tooling
compatibility.

When producers and consumers exchange structured records, they need to agree on
the shape of the data. A schema registry is the shared source of truth for those
shapes. Producers register a schema and stamp each record with its id; consumers
fetch the schema by id to deserialize. Crucially, the registry **checks
compatibility** before accepting a new schema version — so a producer can't roll
out a change that would break the consumers reading from a topic. That is how you
evolve a data format (add a field, widen a type) without a flag-day coordination
across every team.

The registry:

- supports **Avro, Protobuf, and JSON Schema**, each with configurable
  compatibility checking (backward, forward, full, and their transitive
  variants);
- stores every schema in a compacted **`_schemas` topic** on the broker — the
  topic *is* the database, so the registry is stateless and any replica can be
  rebuilt by replaying it;
- exposes a **Confluent-compatible REST API**, so existing Confluent
  serializers, `kafka-avro-console-*` tools, and tooling work unmodified.

{% mermaid() %}
flowchart LR
  Producer[Producer] -->|register / get schema| REST[Registry REST API]
  Consumer[Consumer] -->|get schema by id| REST
  REST <-->|read / write| Topic[_schemas topic]
  Topic --> Broker[(Broker)]
{% end %}

The rest of this page covers deploying the registry.

## Operator-managed (recommended)

Apply a `SchemaRegistry` next to a managed `Kafka`. The `crabka.io/cluster`
label binds it to the cluster; bootstrap is derived from the internal listener.

```yaml
apiVersion: crabka.io/v1alpha1
kind: SchemaRegistry
metadata:
  name: sr
  labels:
    crabka.io/cluster: demo
spec:
  replicas: 3
  schemasTopicReplicationFactor: 3
```

The operator creates a Deployment, a ClusterIP Service
(`sr-sr.<ns>.svc.cluster.local:8081`), and a headless Service for write
forwarding. The generated [SchemaRegistry CRD reference](/docs/reference/operator/schemaregistry/)
lists every field.

## Standalone (Helm, external broker)

```bash
helm install sr charts/crabka-schema-registry \
  --set bootstrapServers=my-broker:9092
```

## Security

`spec.tls`, `spec.authentication` (Basic / unsecured Bearer), and
`spec.authorization` (Kafka-ACL super-users) map to mounted Secrets + SR flags.
Credentials are always referenced Secrets, never inline.

## Next steps

- Build schema-aware stream processors with
  [Streams and Data Formats](/docs/develop/streams/).
- Look up all Schema Registry fields in the
  [SchemaRegistry CRD reference](/docs/reference/operator/schemaregistry/).
