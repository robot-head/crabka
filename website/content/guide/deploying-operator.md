+++
title = "Deploying the Operator"
weight = 25
template = "docs/page.html"
+++

This guide walks you through deploying the Crabka Kubernetes Operator to a Kubernetes cluster and deploying a Kafka-compatible broker cluster.

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

The operator manages Crabka clusters through Custom Resources. Apply the CRDs from the `deploy/crds` directory:

```bash
kubectl apply -f deploy/crds/
```

This registers the following CRDs in your cluster:
- `Kafka` (broker cluster config)
- `KafkaNodePool` (nodes/storage/resource pools)
- `KafkaTopic` (topic management)
- `KafkaUser` (SASL/mTLS user credentials)
- `KafkaRebalance` ( Cruise-Control-like partition rebalancing)
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

Once the operator is running, deploy a single-broker cluster by applying a `Kafka` resource and a corresponding `KafkaNodePool` resource:

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
