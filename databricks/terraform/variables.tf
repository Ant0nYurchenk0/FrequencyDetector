variable "databricks_profile" {
  type        = string
  default     = "frequency-detector"
  description = "Profile name used with `databricks auth login --profile`."
}

variable "catalog" {
  type        = string
  description = "Existing Unity Catalog catalog you can create schemas in (see `databricks catalogs list`)."
}

variable "schema" {
  type    = string
  default = "frequency_detector"
}

variable "kafka_bootstrap" {
  type        = string
  description = "Output of `terraform -chdir=terraform output -raw kafka_bootstrap`, e.g. 34.1.2.3:9094."
}

variable "kafka_username" {
  type    = string
  default = "detector"
}

variable "kafka_password" {
  type        = string
  sensitive   = true
  description = "Same value as kafka_password in terraform/terraform.tfvars."
}

variable "ingest_keep_fraction" {
  type        = number
  default     = 0.5
  description = "Probability that the ingestion job stores a scan (emulates a processing stage)."
}

variable "scan_retention_hours" {
  type        = number
  default     = 24
  description = "Scans older than this (by Kafka timestamp) are deleted by the cleanup job."
}

variable "ingest_cron" {
  type        = string
  default     = "0 0 0/6 * * ?"
  description = "Quartz cron for the ingest job, in UTC. Default: every 6 hours."
}

variable "cleanup_cron" {
  type        = string
  default     = "0 0 3 * * ?"
  description = "Quartz cron for the cleanup job, in UTC. Default: daily at 03:00."
}

variable "train_cron" {
  type        = string
  default     = "0 0 4 * * ?"
  description = "Quartz cron for the train job, in UTC. Default: daily at 04:00, after cleanup."
}

variable "schedules_paused" {
  type        = bool
  default     = false
  description = "Pause all three job schedules (e.g. while Databricks usage limits are reached). Manual runs still work."
}

variable "train_max_samples" {
  type        = number
  default     = 100000
  description = "At most this many scans are sampled from spectrum_scans for one training run."
}

variable "serverless_environment_version" {
  type        = string
  default     = "3"
  description = "Serverless environment version: fixes the Python version and preinstalled packages of every job."
}

variable "ingest_node_type" {
  type        = string
  default     = "n2-standard-4"
  description = "GCP machine type for the ingest cluster. Under 4 cores the Spark driver often fails to start in time."
}

variable "github_repo" {
  type        = string
  default     = "Ant0nYurchenk0/FrequencyDetector"
  description = "Repository whose Release model workflow is triggered when a new champion is promoted."
}

variable "github_token" {
  type        = string
  sensitive   = true
  description = "Fine-grained GitHub token for github_repo with Contents: read and write (sends repository_dispatch)."
}
