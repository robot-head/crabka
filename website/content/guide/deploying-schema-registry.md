+++
title = "Deploying Schema Registry"
weight = 30
template = "docs/page.html"
+++

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

The operator creates a Deployment + a ClusterIP Service
(`sr-sr.<ns>.svc.cluster.local:8081`) + a headless Service for write
forwarding. See the [SchemaRegistry CRD reference](/reference/operator/schemaregistry/).

## Standalone (Helm, external broker)

```bash
helm install sr charts/crabka-schema-registry \
  --set bootstrapServers=my-broker:9092
```

## Security

`spec.tls`, `spec.authentication` (Basic / unsecured Bearer), and
`spec.authorization` (Kafka-ACL super-users) map to mounted Secrets + SR flags.
Credentials are always referenced Secrets, never inline.
