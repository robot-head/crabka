// Configuration for the GKE benchmark cluster. Only `project` is required;
// every other variable defaults to the shape of the live `test-crabka-cluster`.

variable "project" {
  description = "GCP project ID that will own the cluster (the published run used `robot-head`)."
  type        = string
}

variable "region" {
  description = "GCP region for the provider. The cluster itself is zonal — see `zone`."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Single zone for the zonal cluster + node pools."
  type        = string
  default     = "us-central1-b"
}

variable "cluster_name" {
  description = "GKE cluster name."
  type        = string
  default     = "test-crabka-cluster"
}

variable "broker_pool_machine_type" {
  description = "Machine type for the broker pool. e2-standard-4 = 4 vCPU / 16 GiB — one broker (2-4 vCPU req/limit) per node."
  type        = string
  default     = "e2-standard-4"
}

variable "broker_pool_node_count" {
  description = "Nodes in the broker pool. Brokers `podAntiAffinity` one-per-node, so this must be >= the broker count of the largest topology you run: 3 for `3broker-rf3`, 6 for the `6broker-rf3` high-partition matrix."
  type        = number
  default     = 6
}

variable "default_pool_machine_type" {
  description = "Machine type for the default pool that hosts the operators, Prometheus, and the driver Job."
  type        = string
  default     = "e2-standard-2"
}

variable "default_pool_node_count" {
  description = "Nodes in the default pool."
  type        = number
  default     = 3
}

variable "node_disk_gb" {
  description = "Node boot disk (GiB). Broker DATA lives on separate 200 GiB `premium-rwo` PVCs, so this only holds the OS + container images."
  type        = number
  default     = 100
}

variable "release_channel" {
  description = "GKE release channel. REGULAR tracks a recent stable version that ships COS (kTLS `tls` module) and the pd-ssd CSI driver."
  type        = string
  default     = "REGULAR"
}

variable "deletion_protection" {
  description = "GKE deletion-protection guard. Defaults to true (matches the live cluster); set false before `terraform destroy` to dispose of an ephemeral benchmark cluster."
  type        = bool
  default     = true
}

variable "oauth_scopes" {
  description = "Node OAuth scopes. Defaults to the standard GKE node scopes (enough to pull images + write Cloud Logging/Monitoring)."
  type        = list(string)
  default = [
    "https://www.googleapis.com/auth/devstorage.read_only",
    "https://www.googleapis.com/auth/logging.write",
    "https://www.googleapis.com/auth/monitoring",
    "https://www.googleapis.com/auth/service.management.readonly",
    "https://www.googleapis.com/auth/servicecontrol",
    "https://www.googleapis.com/auth/trace.append",
  ]
}
