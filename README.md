# RF Emitter Detection Platform — Implementation Work Breakdown

## Project objective

Build an end-to-end ML engineering platform that simulates RF observations, continuously processes data, trains and evaluates a gradient boosting classifier, validates an ONNX artifact **before deployment**, and serves predictions through Kubernetes.

### Scope and architecture

```text
RF emitter simulator → Detector simulator → Kafka
                                           ├→ Spark Structured Streaming / Databricks
                                           │    → Delta: spectrum_scans
                                           │    → Sampling job → Delta: training_samples
                                           │                         → Airflow / Databricks training
                                           │                         → Gradient boosting / MLflow
                                           │                         → Champion/challenger comparison
                                           │                         → ONNX export, validation, benchmarks
                                           │                         → Champion promotion → Kubernetes / FastAPI
                                           └→ Live inference consumer ───────────────────→ Kubernetes / FastAPI
                                                                                           → detection results
```

Terraform provisions infrastructure; Python/PySpark implement simulation and processing; Airflow orchestrates training and evaluation; MLflow manages model artifacts and lifecycle.

## 1. Prerequisites

### Development environment

- Python 3.11+ and `uv` or Poetry.
- Docker and Docker Compose.
- Git and GitHub, with GitHub Actions for CI/CD.
- Terraform CLI and `kubectl`.
- An AWS account (proposed cloud provider) and appropriate IAM permissions.
- A Databricks workspace with access to jobs, Delta tables, and MLflow.
- A local or cloud Kubernetes cluster.

### Technology choices

| Area                   | Technology                                |
| ---------------------- | ----------------------------------------- |
| Synthetic RF signals   | Python, NumPy                             |
| Message streaming      | Apache Kafka                              |
| Stream processing      | Spark Structured Streaming                |
| Data platform          | Databricks                                |
| Data storage           | Delta Lake                                |
| Model                  | scikit-learn `GradientBoostingClassifier` |
| Orchestration          | Apache Airflow                            |
| Experiments / registry | MLflow                                    |
| Model export / runtime | `skl2onnx`, ONNX Runtime                  |
| Serving                | FastAPI, Docker, Kubernetes               |
| Infrastructure         | Terraform                                 |
| CI/CD                  | GitHub Actions                            |

Start with local services wherever feasible, then provision managed/cloud infrastructure using Terraform.

## 2. Implementation tasks and acceptance tests

### Phase 1 — Simulation

**Outcome:** Reproducible continuous stream of synthetic RF measurements.

#### Task 1.1 — Implement RF emitter simulator

- Generate a recognizable nonlinear spectral signature with multiple peaks.
- Support ON/OFF periods, configurable signal strength, and frequency drift.

**Test:** With a fixed random seed, generated signals are reproducible. In the OFF state, emitter contribution is exactly zero.

#### Task 1.2 — Implement RF detector simulator

- Measure 100 fixed frequency bins in each spectrum scan.
- Add background noise and emitter contribution when active.
- Produce 10 scans per second with timestamp, detector ID, scan ID, power array, and separately held ground-truth label.

**Test:** Approximately 600 scans are generated over a 60-second run. ON scans statistically show the expected pattern; OFF scans contain background only. No label is passed as an inference feature.

#### Task 1.3 — Publish scans to Kafka

- Create an `rf-spectrum-scans` topic and a separate protected ground-truth label feed/topic.
- Publish schema-validated messages with stable scan IDs.

**Test:** Kafka consumers can read approximately 600 valid, uniquely identified scans after one minute and join scans to their ground truth by ID.

### Phase 2 — Streaming and storage

**Outcome:** Reliable ingestion and bounded storage in two Delta tables.

#### Task 2.1 — Provision Databricks resources using Terraform

- Configure Databricks jobs/compute, storage, access controls, and required workspace resources.
- Initially use an existing Databricks workspace to reduce setup complexity.

**Test:** `terraform apply` succeeds; a subsequent `terraform plan` shows no unintended resource changes.

#### Task 2.2 — Implement Spark streaming ingestion

- Consume the Kafka topic, parse and validate messages, and persist scans in `spectrum_scans`.
- Configure streaming checkpointing and idempotent record handling.

**Test:** Publishing 1,000 distinct scans results in 1,000 unique records. Restarting streaming processing does not duplicate committed records.

#### Task 2.3 — Build curated training-data sampling

- Sample representative scans across ON/OFF states, signal strengths, and noise conditions.
- Persist selected examples in `training_samples`, subject to a configurable size limit.
- Save immutable dataset snapshots or sufficient version references for reproducible training.

**Test:** After ingesting 200,000 scans, the curated active dataset remains within its configured cap, covers both classes and different noise conditions, and an earlier snapshot can be reproduced.

#### Task 2.4 — Implement data retention

- Expire raw `spectrum_scans` after a configurable period (e.g., seven days).
- Configure Kafka retention and Delta maintenance/VACUUM with safe retention windows.
- Preserve any data needed by immutable model-training snapshots.

**Test:** Seed expired and current records; cleanup deletes only expired active records. Physical cleanup can reclaim unreferenced files once the safety period elapses without breaking retained snapshots.

### Phase 3 — Machine learning

**Outcome:** Reproducible baseline classifier and learning curves.

#### Task 3.1 — Prepare training and evaluation datasets

- Convert a scan into exactly 100 consistently ordered numerical features.
- Join labels using scan IDs.
- Make train, validation, and test partitions using independent simulation sessions to avoid leakage.

**Test:** Every example contains 100 features and a valid binary label. No scan ID or simulation session overlaps splits.

#### Task 3.2 — Train baseline gradient boosting classifier

- Train a configurable scikit-learn `GradientBoostingClassifier` on a manageable curated dataset.
- Serialize and load the fitted model.

**Test:** Training completes, produces class probabilities, and a reloaded model reproduces predictions.

#### Task 3.3 — Evaluate model quality

- Compute precision, recall, F1, confusion matrix, and false-positive rate.
- Choose a prediction threshold on validation data; hold the final test set separate.

**Test:** Metrics match a manually prepared known confusion matrix; changing thresholds produces sensible precision/recall trade-offs.

#### Task 3.4 — Measure learning as data grows

- Train comparable models with 100, 1,000, 10,000, and 50,000 training samples.
- Keep the held-out evaluation dataset and model configuration fixed.
- Plot performance against training size.

**Test:** A reproducible report contains precision, recall, and F1 learning curves, including any plateau or degradation; improvement is measured rather than assumed.

### Phase 4 — ML lifecycle and orchestration

**Outcome:** Automated, auditable challenger evaluation and training runs.

#### Task 4.1 — Integrate MLflow

- Log parameters, dataset version, source revision, metrics, and model artifacts.
- Register trained models as `EmitterDetector` candidates.

**Test:** A model can be retrieved from a recorded run and reproduces the stored evaluation predictions.

#### Task 4.2 — Implement champion/challenger comparison

- Compare candidate and champion using a fixed validation protocol.
- Define promotion rules, including minimum detection quality and a maximum false-positive rate.
- Record every accept/reject decision and supporting metrics.

**Test:** A deliberately worse challenger is rejected; a better qualified one passes. Decisions can be audited in MLflow.

#### Task 4.3 — Orchestrate training with Airflow

- Create a DAG covering dataset validation, training, evaluation, registration, and candidate eligibility.
- Ensure tasks are retry-safe and idempotent.

**Test:** A manual run completes end-to-end; injected training failure prevents downstream promotion, and retrying does not duplicate releases.

### Phase 5 — ONNX conversion and optimization

**Outcome:** An edge-compatible model artifact validated **before** production deployment.

#### Task 5.1 — Export candidate model to ONNX

- Convert `GradientBoostingClassifier` using `skl2onnx`.
- Define the input schema of 100 ordered float features.

**Test:** ONNX Runtime loads the artifact and produces class predictions and probabilities for test scans.

#### Task 5.2 — Benchmark native vs ONNX inference

- Measure artifact size, startup time, p50/p95 prediction latency, and memory use in comparable environments.
- Try supported runtime optimizations where appropriate.

**Test:** Store repeatable comparison results, reporting gains **or** regressions rather than assuming ONNX is faster.

#### Task 5.3 — Validate prediction equivalence

- Compare sklearn and ONNX probabilities on representative scans, including cases near the classification threshold.
- Require a defined numerical tolerance and verify decisions on non-boundary cases.

**Test:** Maximum probability deviation meets tolerance; non-boundary detection decisions agree. Failure blocks promotion/deployment.

### Phase 6 — Kubernetes inference

**Outcome:** Live inference API serving the validated ONNX champion.

#### Task 6.1 — Implement FastAPI inference service

- Load approved ONNX artifact at startup.
- Expose `/predict`, `/health`, `/ready`, and `/model-info`.
- Validate 100-feature input schema and include active model version in responses.

**Test:** Valid requests return probability, ON/OFF prediction, and model version; malformed requests fail validation; readiness fails if model loading fails.

#### Task 6.2 — Package serving service in Docker

- Pin dependencies and package an immutable approved artifact.
- Ensure the container does not require files from the development host.

**Test:** A clean container runs independently and answers prediction requests.

#### Task 6.3 — Deploy inference service to Kubernetes

- Provision cluster infrastructure via Terraform; manage application releases through Kubernetes manifests/CI-CD.
- Configure a Service, readiness/liveness probes, resource limits, three replicas, and rolling deployment.

**Test:** Three replicas respond; deleting a Pod leads to replacement; a rolling update preserves ready serving replicas and passes continuous smoke requests.

#### Task 6.4 — Implement live inference consumer

- Read new scans from Kafka, call the Kubernetes inference API, and publish results to a predictions topic.
- Correlate results to scan IDs and handle API failures/retries.

**Test:** Publish 1,000 new scans and verify correlated prediction outputs, with no silent loss and successful retry during a temporary API outage.

### Phase 7 — Release automation and operations

**Outcome:** Safe model upgrades, observability, and reproducible delivery.

#### Task 7.1 — Automate champion release

- Deploy only candidates that pass champion comparison and ONNX validation.
- Use immutable model versions and promote the MLflow `champion` alias only after successful rollout verification.
- Provide rollback to the last working version.

**Test:** A qualified challenger is deployed automatically. The active version is inspectable; failed deployment retains or restores the previous working model.

#### Task 7.2 — Add monitoring

- Measure ingestion rates, Kafka lag, Spark errors, training status, inference latency/errors, deployed version, and detection metrics when ground truth is available.

**Test:** Stop a consumer and observe rising lag; take the endpoint offline and observe errors; deploy a new version and confirm monitoring reflects the change.

#### Task 7.3 — Add CI/CD and documentation

- Implement unit/integration tests, formatting/linting, Terraform validation, image builds, and deployment workflows.
- Document startup, teardown, recovery, and end-to-end demonstration steps.

**Test:** A clean checkout passes the automated checks and reproduces the deployment following the README using configured credentials.

## 3. Milestones

- [ ] **M1 — Simulation works locally:** Tasks 1.1–1.3.
- [ ] **M2 — Kafka data reaches Delta Lake:** Tasks 2.1–2.2.
- [ ] **M3 — Storage and curation are bounded:** Tasks 2.3–2.4.
- [ ] **M4 — Baseline and learning curves work:** Tasks 3.1–3.4.
- [ ] **M5 — Automated training and comparison work:** Tasks 4.1–4.3.
- [ ] **M6 — ONNX model is validated:** Tasks 5.1–5.3.
- [ ] **M7 — Kubernetes serves live predictions:** Tasks 6.1–6.4.
- [ ] **M8 — Releases and monitoring are automated:** Tasks 7.1–7.3.

## 4. End-to-end acceptance test

1. Start emitter and detector simulators.
2. Confirm Kafka continuously receives valid scans.
3. Confirm Spark stores scans in `spectrum_scans`.
4. Confirm the curated `training_samples` table follows size/retention policy.
5. Trigger Airflow and verify a challenger is trained and tracked in MLflow.
6. Demonstrate champion/challenger accept/reject logic with detection metrics.
7. Confirm ONNX export, equivalence tests, and benchmark reports complete before release.
8. Confirm Kubernetes is serving the new validated champion and the live consumer produces predictions.
9. Inject a service failure and verify retry or rollback behavior.
10. Ingest additional labelled data and retrain to demonstrate empirically how detection performance evolves.

## 5. Engineering guardrails

- Keep the **fixed test dataset** independent of training and model selection. Use validation data for promotion decisions.
- Keep ground-truth labels out of live inference inputs.
- Preserve a stable ordering for all 100 spectrum features.
- Use Spark for high-volume ingest and curation; use sklearn on appropriately sized training datasets.
- Benchmark ONNX rather than assuming conversion improves speed or size.
- Use Terraform for stable infrastructure, not individual model-version promotions.
- Preserve model artifacts, dataset versions, source revisions, and metrics for reproducibility.
