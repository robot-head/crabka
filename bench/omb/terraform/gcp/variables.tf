// GCP-side configuration for the OMB Crabka-vs-Kafka harness.
// Mirrors the variable surface of OMB's AWS reference deployment at
// driver-kafka/deploy/ssd-deployment/, adapted for GCE.

variable "project" {
  description = "GCP project ID that will own the resources."
  type        = string
}

variable "region" {
  description = "GCP region. Pick one with N2 + local-SSD support."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Single zone for all benchmark VMs (placement-group equivalent)."
  type        = string
  default     = "us-central1-a"
}

variable "public_key_path" {
  description = "SSH public key to install on the VMs. The matching private key must be loaded into your local ssh-agent for Ansible."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "ssh_user" {
  description = "Linux user injected into instance metadata. Ansible will SSH as this user."
  type        = string
  default     = "omb"
}

variable "image" {
  description = "Boot image. Defaults to the Ubuntu 22.04 LTS family — the OMB AWS reference uses RHEL-8, but the Ansible playbooks here are Ubuntu/Debian friendly."
  type        = string
  default     = "ubuntu-os-cloud/ubuntu-2204-lts"
}

variable "machine_types" {
  description = "GCE machine types for each VM class. Picked to roughly match the OMB AWS reference (i3en.6xlarge / m5n.8xlarge)."
  type        = map(string)
  default = {
    kafka_broker  = "n2-standard-16"
    crabka_broker = "n2-standard-16"
    client        = "n2-standard-32"
  }
}

variable "num_instances" {
  description = "Per-class VM counts. crabka_broker is hard-capped at 1 today — see bench/omb/README.md."
  type        = map(number)
  default = {
    kafka_broker  = 3
    crabka_broker = 1
    client        = 4
  }
  validation {
    condition     = var.num_instances["crabka_broker"] <= 1
    error_message = "Crabka bare-VM bring-up is single-broker only today. Lower crabka_broker to 1, or use bench/justfile for the K8s/operator path."
  }
}

variable "local_ssd_count" {
  description = "Number of 375 GB NVMe local SSDs attached per broker. Brokers stripe their log dirs across these."
  type        = number
  default     = 2
}

variable "client_boot_disk_gb" {
  description = "Boot disk size on client VMs (OMB worker JVM + result files)."
  type        = number
  default     = 100
}

variable "broker_boot_disk_gb" {
  description = "Boot disk size on broker VMs (only OS + binaries; data lives on local SSD)."
  type        = number
  default     = 50
}

variable "name_prefix" {
  description = "Prefix applied to all VM/network names. Useful for shared projects."
  type        = string
  default     = "omb"
}

variable "labels" {
  description = "Extra labels applied to every created resource."
  type        = map(string)
  default = {
    workload = "openmessaging-benchmark"
  }
}
