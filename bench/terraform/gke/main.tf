// GKE provisioner for the Crabka-vs-Strimzi Kubernetes benchmark.
//
// Brings up the GKE Standard cluster the `bench/` harness (Crabka + Strimzi
// operators, Prometheus, the `crabka-bench-driver` Job) runs against. Two
// node pools on COS (so Linux kTLS's `tls` module is present), with the GCE
// PD CSI driver enabled so the brokers' `premium-rwo` (pd-ssd) PVCs provision:
//
//   - `default-pool` (e2-standard-2 × 3) — system pods, both operators,
//     Prometheus, and the driver Job;
//   - `beefy-pool`   (e2-standard-4 × 3) — the 3 brokers, one per node (a
//     broker requests 2-4 vCPU, so it gets a 4-vCPU node to itself).
//
// This mirrors the live `test-crabka-cluster` in `robot-head/us-central1-b`;
// `tofu plan` against that cluster (after importing it into local state)
// reports no drift. The bare-VM OpenMessaging-Benchmark path lives separately
// under `bench/omb/terraform/gcp/`.

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# ── Cluster ──────────────────────────────────────────────────────────────────
// Zonal (single-zone) Standard cluster. The auto-created default node pool is
// removed and replaced by the two managed pools below so node shape and count
// are explicit.

resource "google_container_cluster" "bench" {
  name     = var.cluster_name
  location = var.zone

  remove_default_node_pool = true
  initial_node_count       = 1

  release_channel {
    channel = var.release_channel
  }

  # `premium-rwo` (pd-ssd) is provisioned by the GCE PD CSI driver. Enabled by
  # default on GKE Standard, but pinned here so the benchmark's PVCs are
  # guaranteed to bind on a fresh project.
  addons_config {
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }

  # On by default to match the live cluster. Set `-var deletion_protection=false`
  # before `terraform destroy` to tear an ephemeral benchmark cluster down.
  deletion_protection = var.deletion_protection

  # `initial_node_count` sizes the throwaway pool that `remove_default_node_pool`
  # deletes at create time; the real pools are the separate resources below.
  # Neither is reconstructable from an imported cluster (count reads back 0, the
  # flag is create-time-only), so ignore both to keep `plan` clean against the
  # live cluster while still driving a from-scratch `apply`.
  lifecycle {
    ignore_changes = [initial_node_count, remove_default_node_pool]
  }
}

# ── default-pool ─────────────────────────────────────────────────────────────
// System pods, both operators, Prometheus, and the driver Job land here.

resource "google_container_node_pool" "default" {
  name       = "default-pool"
  cluster    = google_container_cluster.bench.name
  location   = var.zone
  node_count = var.default_pool_node_count

  node_config {
    machine_type = var.default_pool_machine_type
    # COS_CONTAINERD carries the `tls` kernel module Linux kTLS needs.
    image_type   = "COS_CONTAINERD"
    disk_size_gb = var.node_disk_gb
    disk_type    = "pd-balanced"
    oauth_scopes = var.oauth_scopes
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# ── beefy-pool ───────────────────────────────────────────────────────────────
// The 3 brokers, one per node. e2-standard-4 (4 vCPU) holds one broker
// (2-4 vCPU request/limit) plus the node's system daemons.

resource "google_container_node_pool" "beefy" {
  name       = "beefy-pool"
  cluster    = google_container_cluster.bench.name
  location   = var.zone
  node_count = var.broker_pool_node_count

  node_config {
    machine_type = var.broker_pool_machine_type
    image_type   = "COS_CONTAINERD"
    disk_size_gb = var.node_disk_gb
    disk_type    = "pd-balanced"
    oauth_scopes = var.oauth_scopes
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}
