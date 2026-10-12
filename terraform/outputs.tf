output "kafka_bootstrap" {
  value       = "${google_compute_address.kafka.address}:9094"
  description = "Put this in KAFKA_BOOTSTRAP for the detector."
}

output "ssh_command" {
  value = "ssh -i ~/.ssh/frequency-detector ubuntu@${google_compute_address.kafka.address}"
}
