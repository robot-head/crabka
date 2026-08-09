# Reproduce the Crabka-vs-Strimzi benchmark on GKE

This is the end-to-end recipe for the published [**Crabka vs Strimzi**](https://robot-head.github.io/crabka/benchmarks/crabka-vs-strimzi/)
Kubernetes benchmark. Provision a GKE cluster with Terraform. Install both
operators and Prometheus. Drive each scenario through the in-cluster
`crabka-bench-driver` Job. Then aggregate the per-run JSON into a report.

The setup has two three-broker clusters. The Crabka operator manages one, and
Strimzi manages the other. They start **one at a time** on the same node pool
with byte-for-byte identical pod resources. The same Rust load driver drives
both over the Kafka wire protocol.

## What this provisions

This module provisions a single-zone GKE **Standard** cluster that matches the
published run:

| | |
|---|---|
| `beefy-pool` | `e2-standard-4` × `broker_pool_node_count` (default 6, 4 vCPU / 16 GiB). One broker per node. 6 nodes size the `6broker-rf3` high-partition matrix. |
| `default-pool` | `e2-standard-2` × 3 (2 vCPU / 8 GiB). Runs system pods, both operators, Prometheus, and the driver Job. |
| Image | `COS_CONTAINERD`. It supplies the `tls` kernel module that Linux kTLS needs. |
| Storage | GCE PD CSI driver → the `premium-rwo` (pd-ssd) StorageClass that the broker CRs request (200 GiB PVC per broker) |
| Layout | A broker requests 2-4 vCPU, so it runs alone on a 4-vCPU `beefy-pool` node. The lighter workloads run on `default-pool`. |

This configuration matches the live `test-crabka-cluster` in
`robot-head/us-central1-b`. After you import that cluster and its two node pools
into local state, `tofu plan` or `terraform plan` reports **no drift**. This
holds if `broker_pool_node_count` matches the live `beefy-pool` size. The
default is 6 for the 6-broker matrix, so set it to 3 to match a 3-node pool.
`premium-rwo` is a built-in GKE StorageClass when the PD CSI driver is enabled,
and this module enables it. You do not have to apply anything more for storage.

## Prerequisites

- `gcloud` authenticated against a project with the Kubernetes Engine and
  Compute APIs enabled, and quota for 3 `e2-standard-4` nodes, 3
  `e2-standard-2` nodes, and 3 × 200 GiB pd-ssd.
- `terraform` ≥ 1.5, `kubectl`, `helm`, [`just`](https://github.com/casey/just).
- A container registry that the cluster can pull from, such as Artifact Registry
  or GHCR, for the `crabka-bench-driver` image. Use `melange` and `apko` to build
  the image, or use a published image.
- A Rust toolchain to run the report aggregator (`crabka-bench-report`).

## 1. Provision the cluster

```bash
cd bench/terraform/gke
cp terraform.tfvars.example terraform.tfvars   # set `project` (at minimum)
terraform init
terraform apply

# point kubectl at the new cluster:
eval "$(terraform output -raw get_credentials_command)"
kubectl get nodes        # expect 6 Ready nodes (3 e2-standard-4 + 3 e2-standard-2)
```

## 2. Make the images pullable

The operator and broker images default to `ghcr.io/robot-head/crabka-*`. Override
the tags with the release that you want to test. You must build the driver image
and push it to a registry that the cluster can pull from. Do this from the repo
root:

```bash
# Build the bench-driver OCI image (melange + apko):
just -f bench/justfile build-driver-image

# Tag + push it to your registry, e.g. Artifact Registry:
REG=us-central1-docker.pkg.dev/$PROJECT/bench
docker tag crabka-bench-driver:e2e "$REG/crabka-bench-driver:e2e"
docker push "$REG/crabka-bench-driver:e2e"
export BENCH_DRIVER_IMAGE="$REG/crabka-bench-driver:e2e"

# Pin the operator + broker image tags to the release under test:
export CRABKA_OPERATOR_IMAGE_TAG=v0.3.6
export CRABKA_BROKER_IMAGE_TAG=v0.3.6
```

## 3. Install both operators + Prometheus

```bash
just -f bench/justfile install-all
# = install-strimzi + install-crabka + install-prometheus
```

This installs the Crabka operator with Helm, the Strimzi cluster operator that
watches `default`, and a minimal Prometheus in the `monitoring` namespace.
Prometheus scrapes cAdvisor, the broker `/metrics` endpoint, and the Strimzi JMX
exporter.

## 4. Run the scenario matrix

Each run applies the stack's Kafka CR, waits for Ready, applies the topic,
starts the driver Job, and writes `bench/results/<stack>-<scenario>-<topology>.json`.
The published table uses the **3-broker / RF=3** topology:

```bash
for scenario in small-msg-saturate fan-out mixed-acks large-msg; do
  bench/scripts/run-scenario.sh crabka "$scenario" 3broker-rf3
  bench/scripts/run-scenario.sh kafka  "$scenario" 3broker-rf3
done

# Zero-copy TLS (kTLS) data path — add a 4th `tls` arg:
bench/scripts/run-scenario.sh crabka small-msg-saturate 3broker-rf3 tls
bench/scripts/run-scenario.sh crabka large-msg         3broker-rf3 tls
```

`run-scenario.sh STACK SCENARIO TOPOLOGY [tls]` takes `STACK` as `crabka|kafka`.
The scenarios are in [`bench/scenarios/`](../../scenarios). The topology is
`1broker-rf1` or `3broker-rf3`. The brokers run one cluster at a time, so the two
stacks never contend.

## 5. Aggregate the results

```bash
just -f bench/justfile bench-report     # → bench/results/SUMMARY.md
```

`bench-report` runs `crabka-bench-report` over `bench/results/*.json`. It renders
the side-by-side table with a `ratio` column. The table shows throughput, cgroup
working-set memory, msgs/CPU-core, operator startup, and failover.

## 6. Tear down

```bash
just -f bench/justfile bench-clean      # delete operators, Prometheus, CRs
# deletion_protection defaults to true (matches the live cluster); flip it off:
cd bench/terraform/gke && terraform destroy -var deletion_protection=false
```

## Validate against an existing cluster (no drift)

The defaults here match the live `test-crabka-cluster`. To confirm that the
configuration still matches a running cluster, import it into local state and run
`plan`. A clean plan means zero drift. The state stays local, and git ignores it:

```bash
cd bench/terraform/gke
terraform init
terraform import google_container_cluster.bench   PROJECT/us-central1-b/test-crabka-cluster
terraform import google_container_node_pool.default PROJECT/us-central1-b/test-crabka-cluster/default-pool
terraform import google_container_node_pool.beefy   PROJECT/us-central1-b/test-crabka-cluster/beefy-pool
terraform plan          # → "No changes. Your infrastructure matches the configuration."
```

With OpenTofu, use `tofu` in place of `terraform`. If you authenticate with
`gcloud auth login` and not with application-default credentials, export
`GOOGLE_OAUTH_ACCESS_TOKEN=$(gcloud auth print-access-token)` first.

## Notes

- **Single sample per cell.** Shared-cloud infrastructure has large run-to-run
  variance. The inter-stack **ratio** is therefore the reliable comparison, not
  one absolute number. Run a scenario again to measure the spread.
- **Resources are identical across stacks.** Both broker CRs request 2-4 vCPU /
  6-12 GiB and a 200 GiB `premium-rwo` PVC. Only the operator is different. See
  the CRs under [`bench/manifests/crabka/`](../../manifests/crabka) and
  [`bench/manifests/strimzi/`](../../manifests/strimzi).
- **kTLS.** On COS, the broker logs `Linux kTLS supported: TLS fetch connections
  will use kernel-offloaded sendfile` at startup. The TLS runs exercise that
  zero-copy path. If you use a node image without the `tls` module, the broker
  changes to userspace TLS automatically. The wire bytes are identical, but the
  throughput is lower.
- For a quick local smoke of the same harness without GKE, use the KinD path:
  `just -f bench/justfile bench-ci`.
