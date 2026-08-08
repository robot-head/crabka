+++
title = "Schema Registry Deployment"
description = "Deploy Crabka's Confluent-compatible Schema Registry: a REST service that stores schemas in the compacted _schemas topic and enforces compatibility checks."
weight = 20
template = "docs/page.html"

[extra]
mermaid = true
+++

Crabka includes a Confluent Schema Registry-compatible REST service. It runs as
a separate Kafka client and stores state in the compacted `_schemas` topic. You
can deploy it with the operator or as a standalone Helm release.

Use it when producers and consumers exchange structured records. It gives schema
IDs, compatibility checks, and compatibility with existing Confluent serializers
and tools.

Producers and consumers must agree on the shape of the data. A schema registry
is the shared source of truth for those shapes. Producers register a schema and
stamp each record with its id. Consumers get the schema by id and deserialize
the record. The registry **checks compatibility** before it accepts a new schema
version, so a producer cannot release a change that breaks the consumers of a
topic. This lets you change a data format, for example to add a field or to
widen a type, without one coordinated switchover across every team.

The registry has these properties:

- It supports **Avro, Protobuf, and JSON Schema**. Each format has configurable
  compatibility checks: backward, forward, full, and the transitive variants.
- It stores every schema in a compacted **`_schemas` topic** on the broker. The
  topic *is* the database, so the registry is stateless. You can rebuild any
  replica when you replay the topic.
- It exposes a **Confluent-compatible REST API**. Existing Confluent
  serializers, `kafka-avro-console-*` tools, and other tools work unmodified.

{% mermaid() %}
flowchart LR
  Producer[Producer] -->|register / get schema| REST[Registry REST API]
  Consumer[Consumer] -->|get schema by id| REST
  REST <-->|read / write| Topic[_schemas topic]
  Topic --> Broker[(Broker)]
{% end %}

The next sections show how to deploy the registry.

## Operator-managed (recommended)

Apply a `SchemaRegistry` next to a managed `Kafka`. The `crabka.io/cluster`
label binds it to the cluster. The registry gets its bootstrap address from the
internal listener.

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

The operator creates a Deployment, a ClusterIP Service at
`sr-sr.<ns>.svc.cluster.local:8081`, and a headless Service that forwards
writes. The generated [SchemaRegistry CRD reference](/docs/reference/operator/schemaregistry/)
lists every field.

## Standalone (Helm, external broker)

```bash
helm install sr charts/crabka-schema-registry \
  --set bootstrapServers=my-broker:9092
```

## Security

Three fields map to mounted Secrets and SR flags: `spec.tls`,
`spec.authentication` for Basic or unsecured Bearer, and `spec.authorization`
for Kafka-ACL super-users. Credentials are always referenced Secrets. Never
write them inline.

## Next steps

- Build schema-aware stream processors with
  [Streams and Data Formats](/docs/develop/streams/).
- Look up all Schema Registry fields in the
  [SchemaRegistry CRD reference](/docs/reference/operator/schemaregistry/).
