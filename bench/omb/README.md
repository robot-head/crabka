# OpenMessaging Benchmark — Crabka vs Kafka on GCP

A Terraform + Ansible harness that runs the upstream
[openmessaging-benchmark](https://github.com/openmessaging/openmessaging-benchmark)
suite against both **Apache Kafka (KRaft)** and **Crabka**, on the same
GCP infrastructure, using the same OMB workload definitions.

It mirrors the layout of OMB's canonical
`driver-kafka/deploy/ssd-deployment` (Terraform on AWS + Ansible),
adapted for GCP. The OMB source is fetched at run time at a pinned
commit; nothing from upstream is vendored.

> Companion to the existing in-house `bench/` harness, which uses a
> custom Rust driver on KinD/Kubernetes. OMB is the industry-standard
> third-party comparison; the in-house driver gives finer per-stack
> observability. Run whichever fits your question.

## Why a separate broker count for Crabka and Kafka?

Crabka's bare-VM bring-up only supports **single-broker, RF=1** today.
Multi-broker quorums need the
in-process [`Broker::change_membership`](../../crates/broker/src/broker.rs)
dance, which the operator currently drives on Kubernetes. There's no
admin RPC for it yet. So:

- `kafka_broker_count` — defaults to **3** (canonical OMB topology).
- `crabka_broker_count` — defaults to **1**, max **1**. The Ansible
  playbook errors if you set it higher.

Report comparisons therefore have to qualify replication-factor
parity. The `producer_acks` / `replication_factor` knobs in the
driver YAML are forced to `1` for both stacks when
`crabka_broker_count == 1`, so the comparison is apples-to-apples
*at RF=1*. Run the existing KinD harness if you need RF=3 numbers —
that path uses the Crabka operator.

## Topology produced

```
              ┌─────────────────────────┐
              │  client VMs (4 by dflt) │  ← OMB benchmark-worker + coordinator
              └────────────┬────────────┘
                           │ bootstrap.servers
            ┌──────────────┼──────────────┐
            ▼                             ▼
   ┌────────────────┐            ┌────────────────┐
   │ Kafka brokers  │            │ Crabka brokers │
   │  (KRaft, ×3)   │            │   (KRaft, ×1)  │
   └────────────────┘            └────────────────┘
```

Both stacks run side-by-side on disjoint VMs so they can be benchmarked
in succession without re-provisioning. The OMB client pool talks to
whichever bootstrap address the driver YAML points at.

## Prerequisites (on your laptop)

| tool      | tested version | install                                   |
|-----------|----------------|-------------------------------------------|
| terraform | 1.7+           | https://developer.hashicorp.com/terraform |
| ansible   | 2.16+          | `pipx install ansible-core`               |
| gcloud    | latest         | https://cloud.google.com/sdk/docs/install |
| jq        | 1.6+           | distro package                            |
| just      | 1.30+          | optional, for the recipe shorthand        |

Authenticate `gcloud` with a project that has Compute Engine API
enabled. The Terraform module uses
`gcloud auth application-default login` credentials.

## Cost warning

The defaults (3× `n2-standard-16` for Kafka, 1× for Crabka, 4×
`n2-standard-32` clients, 3× 375 GB local SSD per broker) run roughly
**~$30/hour** in `us-central1`. The full default workload sweep takes
about 90 minutes. Always `bench-omb-down` when finished — Terraform
deletes the VMs and persistent disks, but local SSDs are deleted on
stop automatically.

## Quick start

```bash
# 0. From repo root. Configure your GCP project + region:
cp bench/omb/terraform/gcp/terraform.tfvars.example \
   bench/omb/terraform/gcp/terraform.tfvars
$EDITOR bench/omb/terraform/gcp/terraform.tfvars

# 1. Fetch OMB at the pinned commit (writes to .omb/).
bench/omb/scripts/fetch-omb.sh

# 2. Provision VMs on GCP.
bench/omb/scripts/tf-apply.sh

# 3. Install Kafka + Crabka + OMB clients (one playbook per stack).
bench/omb/scripts/deploy-stack.sh kafka
bench/omb/scripts/deploy-stack.sh crabka

# 4. Run a workload against one stack, then the other.
bench/omb/scripts/run-workload.sh kafka  1-topic-16-partitions-1kb
bench/omb/scripts/run-workload.sh crabka 1-topic-16-partitions-1kb

# 5. Pull results to your laptop.
bench/omb/scripts/fetch-results.sh

# 6. Tear it all down.
bench/omb/scripts/tf-destroy.sh
```

Or via `just`:

```bash
just -f bench/omb/justfile up                                 # 1+2+3
just -f bench/omb/justfile run kafka  1-topic-16-partitions-1kb
just -f bench/omb/justfile run crabka 1-topic-16-partitions-1kb
just -f bench/omb/justfile sweep                              # default workloads × both stacks
just -f bench/omb/justfile down
```

## Workloads

A few starter workloads live in `bench/omb/workloads/`. The full
upstream set is at `.omb/workloads/` after `fetch-omb.sh` runs — you
can pass any name (without `.yaml`) to `run-workload.sh`; it'll resolve
from `bench/omb/workloads/` first, then `.omb/workloads/`.

## Driver YAML

`bench/omb/drivers/kafka-omb.yaml` is OMB's stock Kafka driver, with
the `bootstrap.servers` line left as a placeholder. The deploy
playbook patches it per stack on each client VM. Because Crabka speaks
the Kafka wire protocol, the **same** driver YAML works for both
stacks — only the bootstrap address differs.

## Results

Each invocation of `run-workload.sh` produces a timestamped JSON
result file plus an HDR histogram under
`bench/omb/results/<stack>/`. The aggregator
script (`scripts/aggregate-results.sh`) renders a side-by-side
Markdown table from those files.

## Honest gaps

- **RF=1 only.** See above.
- **No tiered storage.** OMB doesn't probe S3/GCS offloading.
- **No SASL / TLS.** Driver YAML is plaintext to match the OMB AWS
  reference. Crabka and Kafka both support SASL/SSL; wiring it into
  OMB's driver YAML is straightforward but out of scope here.
- **GCP `us-central1` default.** Change `region` / `zone` in tfvars.
- **OMB pinned to one commit** (`5b1fa70`, May 2025). Bump
  `.pinned-omb-commit` and re-run `fetch-omb.sh` to refresh.

## Files

```
bench/omb/
├── README.md                       ← this file
├── .pinned-omb-commit              ← OMB upstream SHA
├── justfile
├── drivers/kafka-omb.yaml          ← single OMB driver for both stacks
├── workloads/                      ← starter workloads
├── scripts/
│   ├── common.sh
│   ├── fetch-omb.sh
│   ├── tf-apply.sh
│   ├── tf-destroy.sh
│   ├── deploy-stack.sh
│   ├── run-workload.sh
│   ├── fetch-results.sh
│   └── aggregate-results.sh
├── terraform/gcp/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
└── ansible/
    ├── ansible.cfg
    ├── inventory.py                ← reads `terraform output -json`
    ├── group_vars/all.yaml
    ├── deploy-kafka.yaml
    ├── deploy-crabka.yaml
    ├── deploy-client.yaml
    └── templates/
        ├── kafka-server.properties.j2
        ├── kafka.service.j2
        ├── crabka-broker.toml.j2
        ├── crabka-broker.service.j2
        ├── benchmark-worker.service.j2
        └── workers.yaml.j2
```
