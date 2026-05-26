// Outputs consumed by `bench/omb/ansible/inventory.py` via
// `terraform output -json`. Keep the shape stable — the inventory
// script keys off these names.

output "ssh_user" {
  value = var.ssh_user
}

output "kafka_brokers" {
  description = "Kafka KRaft brokers (public + private IPs)."
  value = [
    for vm in google_compute_instance.kafka_broker : {
      name       = vm.name
      public_ip  = vm.network_interface[0].access_config[0].nat_ip
      private_ip = vm.network_interface[0].network_ip
    }
  ]
}

output "crabka_brokers" {
  description = "Crabka brokers (public + private IPs). Capped at 1 today."
  value = [
    for vm in google_compute_instance.crabka_broker : {
      name       = vm.name
      public_ip  = vm.network_interface[0].access_config[0].nat_ip
      private_ip = vm.network_interface[0].network_ip
    }
  ]
}

output "clients" {
  description = "OMB benchmark-worker VMs. The first one also runs the coordinator (`bin/benchmark`) when a workload is launched."
  value = [
    for vm in google_compute_instance.client : {
      name       = vm.name
      public_ip  = vm.network_interface[0].access_config[0].nat_ip
      private_ip = vm.network_interface[0].network_ip
    }
  ]
}

output "kafka_bootstrap_servers" {
  description = "PLAINTEXT bootstrap.servers for the Kafka cluster (private IPs, port 9092)."
  value = join(",", [
    for vm in google_compute_instance.kafka_broker :
    "${vm.network_interface[0].network_ip}:9092"
  ])
}

output "crabka_bootstrap_servers" {
  description = "PLAINTEXT bootstrap.servers for the Crabka cluster (private IPs, port 9092)."
  value = join(",", [
    for vm in google_compute_instance.crabka_broker :
    "${vm.network_interface[0].network_ip}:9092"
  ])
}
