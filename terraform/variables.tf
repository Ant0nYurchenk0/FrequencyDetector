variable "project_id" {
  type        = string
  description = "GCP project ID (not the display name)."
}

variable "region" {
  type    = string
  default = "europe-west3"
}

variable "zone" {
  type    = string
  default = "europe-west3-a"
}

variable "my_ip_cidr" {
  type        = string
  description = "Your public IP in CIDR form, e.g. 203.0.113.7/32. Only this address can reach the server."
}

variable "kafka_username" {
  type    = string
  default = "detector"
}

variable "kafka_password" {
  type        = string
  sensitive   = true
  description = "Password clients use to connect to Kafka (SASL/PLAIN)."
}

variable "public_key_path" {
  type    = string
  default = "~/.ssh/frequency-detector.pub"
}

variable "machine_type" {
  type    = string
  default = "e2-small"
}
