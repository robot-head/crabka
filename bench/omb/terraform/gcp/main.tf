// GCP provisioner for the OMB Crabka-vs-Kafka harness.
//
// Topology (defaults):
//   - 1 VPC + 1 subnet in `var.region`
//   - 3 Kafka broker VMs   (KRaft, combined controller+broker)
//   - 1 Crabka broker VM   (single-node quorum; see README caveat)
//   - 4 OMB client VMs     (coordinator on the first, workers on all 4)
//
// Each broker VM gets `var.local_ssd_count` × 375 GB NVMe local SSDs.
// Network is wide-open inside the VPC and SSH-from-anywhere on 22 (the
// latter mirrors OMB's AWS reference; tighten via `source_ranges` in
// production runs).

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  suffix = random_id.suffix.hex

  base_labels = merge(var.labels, {
    "omb-run" = local.suffix
  })

  ssh_public_key = file(pathexpand(var.public_key_path))

  // Single block of metadata so every VM picks up the same SSH key.
  ssh_metadata = {
    "ssh-keys"               = "${var.ssh_user}:${local.ssh_public_key}"
    "block-project-ssh-keys" = "TRUE"
  }
}

# ── Network ────────────────────────────────────────────────────────────────

resource "google_compute_network" "vpc" {
  name                    = "${var.name_prefix}-vpc-${local.suffix}"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.name_prefix}-subnet-${local.suffix}"
  ip_cidr_range = "10.20.0.0/20"
  region        = var.region
  network       = google_compute_network.vpc.id
}

resource "google_compute_firewall" "allow_ssh" {
  name    = "${var.name_prefix}-allow-ssh-${local.suffix}"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["${var.name_prefix}-${local.suffix}"]
}

resource "google_compute_firewall" "allow_internal" {
  name    = "${var.name_prefix}-allow-internal-${local.suffix}"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = [google_compute_subnetwork.subnet.ip_cidr_range]
  target_tags   = ["${var.name_prefix}-${local.suffix}"]
}

# ── Broker template ────────────────────────────────────────────────────────

// Local-SSD-attached VM. Used for both Kafka and Crabka brokers — same
// shape, different label.

resource "google_compute_instance" "kafka_broker" {
  count        = var.num_instances["kafka_broker"]
  name         = "${var.name_prefix}-kafka-${count.index}-${local.suffix}"
  machine_type = var.machine_types["kafka_broker"]
  zone         = var.zone
  tags         = ["${var.name_prefix}-${local.suffix}", "kafka-broker"]

  labels = merge(local.base_labels, {
    "role"  = "kafka-broker"
    "index" = tostring(count.index)
  })

  boot_disk {
    initialize_params {
      image = var.image
      size  = var.broker_boot_disk_gb
      type  = "pd-balanced"
    }
  }

  dynamic "scratch_disk" {
    for_each = range(var.local_ssd_count)
    content {
      interface = "NVME"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.self_link
    access_config {} // ephemeral public IP for SSH from your laptop
  }

  metadata = local.ssh_metadata
}

resource "google_compute_instance" "crabka_broker" {
  count        = var.num_instances["crabka_broker"]
  name         = "${var.name_prefix}-crabka-${count.index}-${local.suffix}"
  machine_type = var.machine_types["crabka_broker"]
  zone         = var.zone
  tags         = ["${var.name_prefix}-${local.suffix}", "crabka-broker"]

  labels = merge(local.base_labels, {
    "role"  = "crabka-broker"
    "index" = tostring(count.index)
  })

  boot_disk {
    initialize_params {
      image = var.image
      size  = var.broker_boot_disk_gb
      type  = "pd-balanced"
    }
  }

  dynamic "scratch_disk" {
    for_each = range(var.local_ssd_count)
    content {
      interface = "NVME"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.self_link
    access_config {}
  }

  metadata = local.ssh_metadata
}

# ── Client template ────────────────────────────────────────────────────────

resource "google_compute_instance" "client" {
  count        = var.num_instances["client"]
  name         = "${var.name_prefix}-client-${count.index}-${local.suffix}"
  machine_type = var.machine_types["client"]
  zone         = var.zone
  tags         = ["${var.name_prefix}-${local.suffix}", "omb-client"]

  labels = merge(local.base_labels, {
    "role"  = "omb-client"
    "index" = tostring(count.index)
  })

  boot_disk {
    initialize_params {
      image = var.image
      size  = var.client_boot_disk_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.self_link
    access_config {}
  }

  metadata = local.ssh_metadata
}
