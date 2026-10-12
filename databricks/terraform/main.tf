terraform {
  required_version = ">= 1.5"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.60, < 2.0"
    }
  }
}

# Uses the login saved by `databricks auth login --profile <name>`; no tokens in this repo.
provider "databricks" {
  profile = var.databricks_profile
}

data "databricks_current_user" "me" {}

data "databricks_spark_version" "lts" {
  long_term_support = true
}

# --- Storage: a Unity Catalog schema for the Delta tables, plus volumes for streaming checkpoints and ONNX models.

resource "databricks_schema" "this" {
  catalog_name = var.catalog
  name         = var.schema
  comment      = "RF emitter detection: spectrum scans and models."
}

resource "databricks_volume" "checkpoints" {
  catalog_name = databricks_schema.this.catalog_name
  schema_name  = databricks_schema.this.name
  name         = "checkpoints"
  volume_type  = "MANAGED"
}

resource "databricks_volume" "models" {
  catalog_name = databricks_schema.this.catalog_name
  schema_name  = databricks_schema.this.name
  name         = "models"
  volume_type  = "MANAGED"
}

resource "databricks_mlflow_experiment" "training" {
  name = "${data.databricks_current_user.me.home}/frequency-detector-training"
}

# --- Secrets: Kafka credentials and the GitHub token, read by the job scripts with dbutils.secrets.

resource "databricks_secret_scope" "kafka" {
  name = "frequency-detector"
}

resource "databricks_secret" "kafka_bootstrap" {
  scope        = databricks_secret_scope.kafka.name
  key          = "kafka-bootstrap"
  string_value = var.kafka_bootstrap
}

resource "databricks_secret" "kafka_username" {
  scope        = databricks_secret_scope.kafka.name
  key          = "kafka-username"
  string_value = var.kafka_username
}

resource "databricks_secret" "kafka_password" {
  scope        = databricks_secret_scope.kafka.name
  key          = "kafka-password"
  string_value = var.kafka_password
}

# --- Code and compute: cleanup and train run on serverless compute. Ingest runs on its own cluster,
# because serverless cannot reach the Kafka VM (the workspace network policy blocks arbitrary internet IPs).

locals {
  schedule_pause_status = var.schedules_paused ? "PAUSED" : "UNPAUSED"
}

resource "databricks_workspace_file" "ingest_scans" {
  source = "${path.module}/../jobs/ingest_scans.py"
  path   = "${data.databricks_current_user.me.home}/frequency-detector/ingest_scans.py"
}

resource "databricks_job" "ingest_scans" {
  name = "frequency-detector-ingest-scans"

  # Each run reads everything new in Kafka, then stops. A run still going when the next is due is skipped.
  schedule {
    quartz_cron_expression = var.ingest_cron
    timezone_id            = "UTC"
    pause_status           = local.schedule_pause_status
  }

  job_cluster {
    job_cluster_key = "ingest"
    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.ingest_node_type
      num_workers        = 0
      data_security_mode = "SINGLE_USER"
      spark_conf = {
        "spark.databricks.cluster.profile" = "singleNode"
        "spark.master"                     = "local[*]"
      }
      custom_tags = {
        "ResourceClass" = "SingleNode"
      }
    }
  }

  task {
    task_key        = "ingest"
    job_cluster_key = "ingest"
    spark_python_task {
      python_file = databricks_workspace_file.ingest_scans.path
      source      = "WORKSPACE"
      parameters = [
        "--catalog", databricks_schema.this.catalog_name,
        "--schema", databricks_schema.this.name,
        "--secret-scope", databricks_secret_scope.kafka.name,
        "--keep-fraction", tostring(var.ingest_keep_fraction),
      ]
    }
  }

  depends_on = [
    databricks_volume.checkpoints,
    databricks_secret.kafka_bootstrap,
    databricks_secret.kafka_username,
    databricks_secret.kafka_password,
  ]
}

resource "databricks_workspace_file" "cleanup_scans" {
  source = "${path.module}/../jobs/cleanup_scans.py"
  path   = "${data.databricks_current_user.me.home}/frequency-detector/cleanup_scans.py"
}

resource "databricks_job" "cleanup_scans" {
  name = "frequency-detector-cleanup-scans"

  schedule {
    quartz_cron_expression = var.cleanup_cron
    timezone_id            = "UTC"
    pause_status           = local.schedule_pause_status
  }

  performance_target = "PERFORMANCE_OPTIMIZED"

  environment {
    environment_key = "default"
    spec {
      environment_version = var.serverless_environment_version
    }
  }

  task {
    task_key        = "cleanup"
    environment_key = "default"
    spark_python_task {
      python_file = databricks_workspace_file.cleanup_scans.path
      source      = "WORKSPACE"
      parameters = [
        "--catalog", databricks_schema.this.catalog_name,
        "--schema", databricks_schema.this.name,
        "--retention-hours", tostring(var.scan_retention_hours),
      ]
    }
  }
}

resource "databricks_workspace_file" "train_model" {
  source = "${path.module}/../jobs/train_model.py"
  path   = "${data.databricks_current_user.me.home}/frequency-detector/train_model.py"
}

resource "databricks_job" "train_model" {
  name = "frequency-detector-train-model"

  schedule {
    quartz_cron_expression = var.train_cron
    timezone_id            = "UTC"
    pause_status           = local.schedule_pause_status
  }

  performance_target = "PERFORMANCE_OPTIMIZED"

  # Serverless has no ML runtime, so the ML libraries are installed here.
  environment {
    environment_key = "default"
    spec {
      environment_version = var.serverless_environment_version
      dependencies = [
        "mlflow>=2.16",
        "scikit-learn>=1.3",
        "xgboost>=2.0,<3", # onnxmltools' XGBoost converter targets the 2.x model format
        "onnxmltools>=1.12",
        "onnxruntime>=1.17",
      ]
    }
  }

  task {
    task_key        = "train"
    environment_key = "default"
    spark_python_task {
      python_file = databricks_workspace_file.train_model.path
      source      = "WORKSPACE"
      parameters = [
        "--catalog", databricks_schema.this.catalog_name,
        "--schema", databricks_schema.this.name,
        "--experiment", databricks_mlflow_experiment.training.name,
        "--max-samples", tostring(var.train_max_samples),
      ]
    }
  }

  # Sends the "model-promoted" POST to GitHub, which starts the Release model workflow.
  # Does nothing when train did not promote a new champion.
  task {
    task_key        = "notify_release"
    environment_key = "notify"
    depends_on {
      task_key = "train"
    }
    spark_python_task {
      python_file = databricks_workspace_file.notify_release.path
      source      = "WORKSPACE"
      parameters = [
        "--repo", var.github_repo,
        "--model-name", "${databricks_schema.this.catalog_name}.${databricks_schema.this.name}.emitter_detector",
        "--secret-scope", databricks_secret_scope.kafka.name,
      ]
    }
  }

  # The notify task needs no extra packages, so it gets its own (faster to start) environment.
  environment {
    environment_key = "notify"
    spec {
      environment_version = var.serverless_environment_version
    }
  }

  depends_on = [databricks_volume.models]
}

resource "databricks_workspace_file" "notify_release" {
  source = "${path.module}/../jobs/notify_release.py"
  path   = "${data.databricks_current_user.me.home}/frequency-detector/notify_release.py"
}

# Fine-grained GitHub token (this repo only, Contents: read and write) used by notify_release.
resource "databricks_secret" "github_token" {
  scope        = databricks_secret_scope.kafka.name
  key          = "github-token"
  string_value = var.github_token
}

# --- Access control: every workspace user may view the jobs and their runs; only you manage them.

resource "databricks_permissions" "ingest_scans" {
  job_id = databricks_job.ingest_scans.id

  access_control {
    group_name       = "users"
    permission_level = "CAN_VIEW"
  }
}

resource "databricks_permissions" "cleanup_scans" {
  job_id = databricks_job.cleanup_scans.id

  access_control {
    group_name       = "users"
    permission_level = "CAN_VIEW"
  }
}

resource "databricks_permissions" "train_model" {
  job_id = databricks_job.train_model.id

  access_control {
    group_name       = "users"
    permission_level = "CAN_VIEW"
  }
}
