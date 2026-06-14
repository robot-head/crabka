output "cluster_name" {
  description = "Provisioned GKE cluster name."
  value       = google_container_cluster.bench.name
}

output "location" {
  description = "Cluster zone."
  value       = google_container_cluster.bench.location
}

output "endpoint" {
  description = "Kubernetes API server endpoint."
  value       = google_container_cluster.bench.endpoint
  sensitive   = true
}

output "get_credentials_command" {
  description = "Run this to point kubectl at the cluster."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.bench.name} --zone ${google_container_cluster.bench.location} --project ${var.project}"
}
