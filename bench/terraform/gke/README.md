# Reproduce the Crabka-vs-Strimzi benchmark on GKE

End-to-end recipe for the published [**Crabka vs Strimzi**](https://robot-head.github.io/crabka/benchmarks/crabka-vs-strimzi/)
Kubernetes benchmark: provision a GKE cluster with Terraform, install both
operators + Prometheus, drive each scenario through the in-cluster
`crabka-bench-driver` Job, and aggregate the per-run JSON into a report.

The cluster is two three-broker clusters — one managed by the Crabka operator,
one by Strimzi — brought up **one at a time** on the same node pool with
byte-for-byte identical pod resources, driven by the same Rust load driver over
the Kafka wire protocol.

## What this provisions

A single-zone GKE **Standard** cluster sized to match the published run:

| | |
|---|---|
| `beefy-pool` | `e2-standard-4` × `broker_pool_node_count` (default 6, 4 vCPU / 16 GiB) — one broker per node; 6 sizes the `6broker-rf3` high-partition matrix |
| `default-pool` | `e2-standard-2` × 3 (2 vCPU / 8 GiB) — system pods, both operators, Prometheus, the driver Job |
| Image | `COS_CONTAINERD` — ships the `tls` kernel module Linux kTLS needs |
| Storage | GCE PD CSI driver → the `premium-rwo` (pd-ssd) StorageClass the broker CRs request (200 GiB PVC per broker) |
| Layout | a broker requests 2-4 vCPU, so it lands on a 4-vCPU `beefy-pool` node by itself; the lighter workloads pack onto `default-pool` |

This mirrors the live `test-crabka-cluster` in `robot-head/us-central1-b`:
`tofu plan` (or `terraform plan`) reports **no drift** after importing that
cluster + its two node pools into local state, provided `broker_pool_node_count`
is set to the live `beefy-pool` size (the default is now 6 for the 6-broker
matrix — set it to 3 to match a 3-node pool). `premium-rwo` is a built-in GKE
StorageClass once the PD CSI driver is enabled (this module enables it), so
there is nothing extra to apply for storage.

## Prerequisites

- `gcloud` authenticated against a project with the Kubernetes Engine + Compute
  APIs enabled, and quota for 3 `e2-standard-4` + 3 `e2-standard-2` nodes and
  3 × 200 GiB pd-ssd.
- `terraform` ≥ 1.5, `kubectl`, `helm`, [`just`](https://github.com/casey/just).
- A container registry the cluster can pull from (Artifact Registry or GHCR) for
  the `crabka-bench-driver` image; `melange` + `apko` to build it (or reuse a
  published image).
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

The operator/broker images default to `ghcr.io/robot-head/crabka-*`; override the
tags to a release you want to test. The driver image must be built and pushed to
a registry the cluster can pull from. From the repo root:

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

This installs the Crabka operator (Helm), the Strimzi cluster operator (watching
`default`), and a minimal Prometheus (cAdvisor + broker `/metrics` + the Strimzi
JMX exporter) in the `monitoring` namespace.

## 4. Run the scenario matrix

Each run applies the stack's Kafka CR, waits for Ready, applies the topic,
launches the driver Job, and writes `bench/results/<stack>-<scenario>-<topology>.json`.
The published table is the **3-broker / RF=3** topology:

```bash
for scenario in small-msg-saturate fan-out mixed-acks large-msg; do
  bench/scripts/run-scenario.sh crabka "$scenario" 3broker-rf3
  bench/scripts/run-scenario.sh kafka  "$scenario" 3broker-rf3
done

# Zero-copy TLS (kTLS) data path — add a 4th `tls` arg:
bench/scripts/run-scenario.sh crabka small-msg-saturate 3broker-rf3 tls
bench/scripts/run-scenario.sh crabka large-msg         3broker-rf3 tls
```

`run-scenario.sh STACK SCENARIO TOPOLOGY [tls]` — `STACK` is `crabka|kafka`,
scenarios live in [`bench/scenarios/`](../../scenarios), topology is
`1broker-rf1` or `3broker-rf3`. Brokers run one cluster at a time, so the two
stacks never contend.

## 5. Aggregate the results

```bash
just -f bench/justfile bench-report     # → bench/results/SUMMARY.md
```

`bench-report` runs `crabka-bench-report` over `bench/results/*.json` and renders
the side-by-side table (throughput, cgroup working-set memory, msgs/CPU-core,
operator startup, failover) with a `ratio` column.

## 6. Tear down

```bash
just -f bench/justfile bench-clean      # delete operators, Prometheus, CRs
# deletion_protection defaults to true (matches the live cluster); flip it off:
cd bench/terraform/gke && terraform destroy -var deletion_protection=false
```

## Validate against an existing cluster (no drift)

The defaults here mirror the live `test-crabka-cluster`. To confirm the config
still matches a running cluster, import it into local state and `plan` — a clean
plan means zero drift. State stays local (and is git-ignored):

```bash
cd bench/terraform/gke
terraform init
terraform import google_container_cluster.bench   PROJECT/us-central1-b/test-crabka-cluster
terraform import google_container_node_pool.default PROJECT/us-central1-b/test-crabka-cluster/default-pool
terraform import google_container_node_pool.beefy   PROJECT/us-central1-b/test-crabka-cluster/beefy-pool
terraform plan          # → "No changes. Your infrastructure matches the configuration."
```

(With OpenTofu, substitute `tofu` for `terraform`. If you authenticate with
`gcloud auth login` rather than application-default credentials, export
`GOOGLE_OAUTH_ACCESS_TOKEN=$(gcloud auth print-access-token)` first.)

## Notes

- **Single sample per cell.** Shared-cloud infrastructure has meaningful
  run-to-run variance, so the inter-stack **ratio** is the reliable comparison,
  not any one absolute number. Re-run a scenario to gauge the spread.
- **Resources are identical across stacks.** Both broker CRs request 2-4 vCPU /
  6-12 GiB and a 200 GiB `premium-rwo` PVC; only the operator differs. See the
  CRs under [`bench/manifests/crabka/`](../../manifests/crabka) and
  [`bench/manifests/strimzi/`](../../manifests/strimzi).
- **kTLS.** On COS the broker logs `Linux kTLS supported: TLS fetch connections
  will use kernel-offloaded sendfile` at startup; the TLS runs exercise that
  zero-copy path. If a node image without the `tls` module is used, the broker
  transparently falls back to userspace TLS (identical wire bytes, lower
  throughput).
- For a quick local smoke of the same harness without GKE, use the KinD path:
  `just -f bench/justfile bench-ci`.
