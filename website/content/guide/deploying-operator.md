+++
title = "Deploying the Operator"
weight = 25
template = "docs/page.html"

[extra]
mermaid = true
+++

## What the operator is and why you'd want it

The Crabka Operator is a single Kubernetes controller process that turns a
handful of declarative Custom Resources into a running, self-healing Kafka
cluster. You describe the cluster you want in YAML; the operator formats data
directories, generates and rotates the CA and TLS material, writes ConfigMaps
and Secrets, creates the Services clients connect through, and rolls brokers
when their spec changes. You never run `crabka format` or hand-edit broker
config — the operator owns that lifecycle.

It is its own project, purpose-built for Crabka. It is not a Strimzi plugin or
a fork: it speaks directly to Crabka's native KRaft broker and knows about
Crabka-specific concerns like the rebalancer and the schema registry.

### One operator, many clusters, dynamic size

A single operator process watches the whole set of namespaces it is granted and
reconciles every Crabka resource it finds. A cluster's size is **dynamic**: a
`Kafka` resource owns one or more `KafkaNodePool`s, each pool is a StatefulSet
of brokers (one broker per pod) labeled `crabka.io/cluster=<name>`, and the
cluster's broker count is simply the sum of replica counts across its pools.
Scale by editing a pool's `replicas` or adding another pool — the operator
reconciles the StatefulSets to match.

### The CRDs at a glance

| CRD | What it declares |
| --- | --- |
| `Kafka` | A broker cluster: version, cluster-wide config, the owning resource for Services/ConfigMap/Secrets/CA |
| `KafkaNodePool` | A StatefulSet of brokers (roles, replicas, storage, resources) bound to a cluster |
| `KafkaTopic` | A topic and its configuration |
| `KafkaUser` | SCRAM / mTLS credentials, ACLs, and quotas for a principal |
| `KafkaRebalance` | A Cruise-Control-equivalent partition rebalance request |
| `SchemaRegistry` | A Confluent-compatible schema registry bound to a cluster |
| `KafkaGrpcGateway` | A gRPC / Connect-RPC gateway in front of the cluster |

### Operator architecture

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

The rest of this page walks through actually deploying the operator and a first
cluster.

## Deploying

This guide walks you through deploying the Crabka Kubernetes Operator to a
Kubernetes cluster and then creating a Kafka-compatible broker cluster.

## Prerequisites

- A Kubernetes cluster running version `1.28` or higher.
- [Helm 3](https://helm.sh/) installed locally.
- `kubectl` configured to point to your cluster.

## 1. Install Prometheus CRDs

The operator relies on the Prometheus Operator's `PodMonitor` and `ServiceMonitor` CRDs for metrics scraping. If they are not already installed, apply them:

```bash
PROM_OP_TAG=v0.79.2
kubectl apply -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OP_TAG}/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml"
kubectl apply -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OP_TAG}/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml"
```

## 2. Install Crabka Custom Resource Definitions (CRDs)

The operator manages Crabka clusters through Custom Resources. Apply the CRDs
from the `deploy/crds` directory:

```bash
kubectl apply -f deploy/crds/
```

This registers the following CRDs in your cluster:
- `Kafka` (broker cluster config)
- `KafkaNodePool` (nodes/storage/resource pools)
- `KafkaTopic` (topic management)
- `KafkaUser` (SASL/mTLS user credentials)
- `KafkaRebalance` (Cruise-Control-like partition rebalancing)
- `SchemaRegistry` (Schema Registry service)
- `KafkaGrpcGateway` (gRPC / Connect-RPC gateway)

## 3. Deploy the Operator with Helm

Install the operator into its own namespace using the Helm chart provided in `charts/crabka-operator`:

```bash
helm install operator charts/crabka-operator \
  --namespace crabka-operator \
  --create-namespace
```

### Private Registries

If your operator or broker container images are hosted in a private registry (such as GitHub Container Registry), you can configure image pull secrets.

1. Create a docker-registry secret:
   ```bash
   kubectl create secret docker-registry my-registry-secret \
     --docker-server=ghcr.io \
     --docker-username=<username> \
     --docker-password=<token> \
     --namespace=crabka-operator
   ```

2. Upgrade the Helm chart specifying the image tag and pull secret:
   ```bash
   helm upgrade operator charts/crabka-operator \
     --namespace crabka-operator \
     --set image.tag=0.3.6 \
     --set brokerImage.tag=0.3.6 \
     --set imagePullSecrets[0].name=my-registry-secret
   ```

## 4. Run a Kafka-compatible Broker

Once the operator is running, deploy a single-broker cluster by applying a
`Kafka` resource and a corresponding `KafkaNodePool` resource:

```yaml
# kafka-cluster.yaml
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata:
  name: demo
  namespace: default
spec:
  kafkaVersion: "0.3.6"
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
