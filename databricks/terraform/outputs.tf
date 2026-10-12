output "ingest_job_id" {
  value       = databricks_job.ingest_scans.id
  description = "Start a run with: databricks jobs run-now <id> --profile <profile>"
}

output "ingest_job_url" {
  value = databricks_job.ingest_scans.url
}

output "spectrum_scans_table" {
  value = "${databricks_schema.this.catalog_name}.${databricks_schema.this.name}.spectrum_scans"
}

output "cleanup_job_id" {
  value       = databricks_job.cleanup_scans.id
  description = "Runs daily; start one now with: databricks jobs run-now <id> --profile <profile>"
}

output "train_job_id" {
  value       = databricks_job.train_model.id
  description = "Runs daily; start one now from the Jobs UI or with databricks jobs run-now."
}
