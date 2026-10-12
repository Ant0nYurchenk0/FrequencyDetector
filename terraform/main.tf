terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# The Compute Engine API is off in new projects.
resource "google_project_service" "compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

# SSH (22) reachable from your IP only.
resource "google_compute_firewall" "ssh" {
  name    = "frequency-detector-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = [var.my_ip_cidr]
  target_tags   = ["kafka"]

  depends_on = [google_project_service.compute]
}

# Kafka (9094) open to the whole internet, because Databricks cluster IPs change.
# Only the SASL password (sent unencrypted) protects it: demo use only, tighten before real data.
resource "google_compute_firewall" "kafka" {
  name    = "frequency-detector-kafka"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["9094"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["kafka"]

  depends_on = [google_project_service.compute]
}

# Fixed public address, so Kafka's advertised address survives instance restarts.
resource "google_compute_address" "kafka" {
  name       = "frequency-detector-kafka"
  depends_on = [google_project_service.compute]
}

resource "google_compute_instance" "kafka" {
  name         = "frequency-detector-kafka"
  machine_type = var.machine_type
  tags         = ["kafka"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
      size  = 20
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.kafka.address
    }
  }

  # Uploads only the PUBLIC half of your key; the private key never leaves your machine.
  metadata = {
    ssh-keys = "ubuntu:${trimspace(file(pathexpand(var.public_key_path)))}"
  }

  # Runs at every boot, so the script is written to be repeatable.
  metadata_startup_script = templatefile("${path.module}/user_data.sh.tftpl", {
    public_ip = google_compute_address.kafka.address
    username  = var.kafka_username
    password  = var.kafka_password
  })
}
