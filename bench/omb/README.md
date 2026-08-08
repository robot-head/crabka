# OpenMessaging Benchmark — Crabka vs Kafka on GCP

This Terraform and Ansible harness runs the upstream
[openmessaging-benchmark](https://github.com/openmessaging/openmessaging-benchmark)
suite against both **Apache Kafka (KRaft)** and **Crabka**. Both stacks
run on the same GCP infrastructure with the same OMB workload definitions.

The harness follows the layout of OMB's canonical
`driver-kafka/deploy/ssd-deployment`, which uses Terraform on AWS with
Ansible, and adapts it for GCP. The harness fetches the OMB source at run
time at a pinned commit. The repository vendors nothing from upstream.

> This harness is a companion to the in-house `bench/` harness, which uses
> a custom Rust driver on KinD and Kubernetes. OMB is the industry-standard
> third-party comparison. The in-house driver gives finer per-stack
> observability. Run the one that fits your question.

## Why a separate broker count for Crabka and Kafka?

Crabka's bare-VM bring-up supports only **single-broker, RF=1** today.
Multi-broker quorums need the
in-process [`Broker::change_membership`](../../crates/broker/src/broker.rs)
sequence, which the operator drives on Kubernetes today. There is no
admin RPC for it yet. So:

- `kafka_broker_count` defaults to **3**, the canonical OMB topology.
- `crabka_broker_count` defaults to **1**, with a maximum of **1**. The
  Ansible playbook gives an error if you set it higher.

Report comparisons must therefore qualify replication-factor parity. The
harness forces the `producer_acks` and `replication_factor` settings in
the driver YAML to `1` for both stacks when `crabka_broker_count == 1`,
so the comparison is direct *at RF=1*. Run the KinD harness if you need
RF=3 numbers, because that path uses the Crabka operator.

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

Both stacks run side-by-side on disjoint VMs, so you can benchmark them
in succession without new provisioning. The OMB client pool talks to the
bootstrap address that the driver YAML points at.

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

The defaults cost about **$30/hour** in `us-central1`. Those defaults are
3× `n2-standard-16` for Kafka, 1× for Crabka, 4× `n2-standard-32`
clients, and 3× 375 GB local SSD per broker. The full default workload
sweep takes about 90 minutes. Always run `bench-omb-down` when you
finish. Terraform deletes the VMs and the persistent disks, and GCP
deletes the local SSDs automatically on stop.

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

Some starter workloads are in `bench/omb/workloads/`. The full upstream
set is in `.omb/workloads/` after `fetch-omb.sh` runs. You can give any
workload name to `run-workload.sh` without the `.yaml` suffix. The script
looks in `bench/omb/workloads/` first, then in `.omb/workloads/`.

## Driver YAML

`bench/omb/drivers/kafka-omb.yaml` is OMB's stock Kafka driver. The
`bootstrap.servers` line is a placeholder. The deploy playbook patches it
per stack on each client VM. Crabka speaks the Kafka wire protocol, so
the **same** driver YAML works for both stacks. Only the bootstrap
address is different.

## Results

Each run of `run-workload.sh` writes a timestamped JSON result file and
an HDR histogram under `bench/omb/results/<stack>/`. The aggregator
script `scripts/aggregate-results.sh` renders a side-by-side Markdown
table from those files.

## Honest gaps

- **RF=1 only.** See above.
- **No tiered storage.** OMB does not test S3 or GCS offload.
- **No SASL / TLS.** The driver YAML is plaintext, to match the OMB AWS
  reference. Crabka and Kafka both support SASL/SSL. It is simple to add
  SASL/SSL to OMB's driver YAML, but it is out of scope here.
- **GCP `us-central1` default.** Change `region` / `zone` in tfvars.
- **OMB pinned to one commit**, `5b1fa70` from May 2025. To refresh it,
  change `.pinned-omb-commit` and run `fetch-omb.sh` again.

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
