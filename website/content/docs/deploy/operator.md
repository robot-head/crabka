+++
title = "Operator Deployment"
description = "Run a Crabka cluster on Kubernetes with the operator: declarative Kafka and KafkaNodePool resources manage broker config, StatefulSets, certificates, and rolls."
weight = 10
template = "docs/page.html"

[extra]
mermaid = true
+++

The Crabka Operator turns declarative Kubernetes resources into a running
Kafka-compatible cluster. It controls the operational lifecycle. It formats the
data directories and manages broker configuration, Services, StatefulSets,
Secrets, certificates, topic and user reconciliation, broker rolls, and
rebalances.

Use the operator for real clusters. Use the local
[Quickstart](/docs/start-here/quickstart/) only for development and evaluation.

## How the operator works

Crabka has its own operator. It is not a Strimzi plugin or fork. One operator
process can watch the namespaces that you grant it and reconcile every Crabka
resource it finds.

A `Kafka` resource owns the cluster. One or more `KafkaNodePool` resources supply
brokers. Each node pool becomes a StatefulSet, and the total broker count is the
sum of the pool replica counts. To scale the cluster, edit `replicas` or add
another pool.

## Custom Resources

| CRD | What it declares |
| --- | --- |
| `Kafka` | A broker cluster: version, cluster-wide config, the owning resource for Services/ConfigMap/Secrets/CA |
| `KafkaNodePool` | A StatefulSet of brokers (roles, replicas, storage, resources) bound to a cluster |
| `KafkaTopic` | A topic and its configuration |
| `KafkaUser` | SCRAM / mTLS credentials, ACLs, and quotas for a principal |
| `KafkaRebalance` | A Cruise-Control-equivalent partition rebalance request |
| `SchemaRegistry` | A Confluent-compatible schema registry bound to a cluster |
| `KafkaGrpcGateway` | A gRPC / Connect-RPC gateway in front of the cluster |

The docs build generates the exact fields into the
[Operator CRD reference](/docs/reference/operator/).

## Architecture

{% mermaid() %}
flowchart TD
  Operator[Operator process] -->|watches| CRDs[Kafka / NodePool / Topic / User / Rebalance / SchemaRegistry]
  CRDs --> Recon[Per-resource reconcilers]
  Recon --> Pools[KafkaNodePool reconciler]
  Pools --> STS[StatefulSets — one broker per pod]
  Recon --> Cluster[Kafka reconciler]
  Cluster --> Svc[Cluster Service]
  Cluster --> CM[ConfigMap]
  Cluster --> Sec[Secrets + CA]
{% end %}

## Prerequisites

- A Kubernetes cluster running version `1.28` or higher.
- [Helm 3](https://helm.sh/) installed locally.
- `kubectl` configured to point to your cluster.

## 1. Install Prometheus CRDs

The operator uses the Prometheus Operator's `PodMonitor` and `ServiceMonitor`
CRDs to scrape metrics. If your cluster does not have them, apply them:

```bash
PROM_OP_TAG=v0.79.2
kubectl apply -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OP_TAG}/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml"
kubectl apply -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OP_TAG}/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml"
```

## 2. Install Crabka Custom Resource Definitions (CRDs)

Apply the Crabka CRDs from this repository:

```bash
kubectl apply -f deploy/crds/
```

## 3. Install the operator

Install the operator into its own namespace with the chart in this repository:

```bash
helm install operator charts/crabka-operator \
  --namespace crabka-operator \
  --create-namespace
```

For the published chart repository and provenance verification, see the
[Helm chart documentation](https://github.com/robot-head/crabka/blob/main/charts/README.md).

### Private registries

To use operator or broker images from a private registry, create a pull secret:

```bash
kubectl create secret docker-registry my-registry-secret \
  --docker-server=ghcr.io \
  --docker-username=<username> \
  --docker-password=<token> \
  --namespace=crabka-operator
```

Then reference it from the chart values:

```bash
helm upgrade operator charts/crabka-operator \
  --namespace crabka-operator \
  --set image.tag=0.3.7 \
  --set brokerImage.tag=0.3.7 \
  --set imagePullSecrets[0].name=my-registry-secret
```

## 4. Create a cluster

Apply a `Kafka` resource and a matching `KafkaNodePool`:

```yaml
# kafka-cluster.yaml
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata:
  name: demo
  namespace: default
spec:
  kafkaVersion: "0.3.7"
  config:
    log.retention.hours: "24"
---
apiVersion: crabka.io/v1alpha1
kind: KafkaNodePool
metadata:
  name: brokers
  namespace: default
  labels:
    crabka.io/cluster: demo
spec:
  roles: [Controller, Broker]
  replicas: 1
  nodeIdStart: 0
  storage:
    type: PersistentClaim
    size: 1Gi
    deleteClaim: true
  resources:
    requests:
      cpu: "500m"
      memory: 512Mi
    limits:
      cpu: "2000m"
      memory: 2Gi
```

Apply the manifest:

```bash
kubectl apply -f kafka-cluster.yaml
```

Wait for the broker pod to become ready:

```bash
kubectl rollout status statefulset/demo-brokers --timeout=300s
```

## 5. Use the cluster

Connect standard Kafka clients and tools to the bootstrap Service that the
operator creates. The exact Service names depend on the listener configuration
in the `Kafka` resource.

Next:

- Create topics and users with the generated
  [KafkaTopic](/docs/reference/operator/kafkatopic/) and
  [KafkaUser](/docs/reference/operator/kafkauser/) resources.
- Add Schema Registry with
  [Schema Registry Deployment](/docs/deploy/schema-registry/).
- Look up broker config keys in
  [Server Configuration](/docs/reference/broker/server-config/).
