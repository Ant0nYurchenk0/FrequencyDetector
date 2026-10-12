"""Databricks job: train an XGBoost emitter detector on spectrum_scans and promote it if it beats the champion.

1. Sample up to --max-samples scans and split them randomly into train and validation sets.
2. Train XGBoost on the 100 power values to predict emitter_on; log params, metrics and model to MLflow
   and register a new version of the Unity Catalog model.
3. Score the current champion (MLflow alias "champion") on the same validation set. The new model is
   promoted only if its F1 is strictly higher, or if there is no champion yet.
4. On promotion: convert to ONNX, check ONNX and XGBoost probabilities agree, then move the alias and
   save the .onnx file to the models volume for deployment.
5. On promotion, publish the version as the task value "promoted_version"; the job's notify_release task
   reads it and asks GitHub to deploy that version.
"""
import argparse
import os
import shutil

import mlflow
import numpy as np
import onnxruntime as ort
from databricks.sdk.runtime import dbutils
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from onnxmltools import convert_xgboost
from onnxmltools.convert.common.data_types import FloatTensorType
from pyspark.sql import SparkSession
from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

NUM_CHANNELS = 100
CHAMPION_ALIAS = "champion"
ONNX_TOLERANCE = 1e-4  # max allowed |p_xgboost - p_onnx|


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--max-samples", type=int, default=100_000)
    parser.add_argument("--validation-fraction", type=float, default=0.2)
    parser.add_argument("--n-estimators", type=int, default=100)
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--learning-rate", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def load_data(spark, table, max_samples, seed):
    """Returns X (n x 100 float32), y (0/1) and the Delta version the sample was read from."""
    version = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]
    df = spark.sql(f"SELECT power, emitter_on FROM {table} VERSION AS OF {version}")
    total = df.count()
    if total == 0:
        raise RuntimeError(f"{table} is empty: run the ingestion job first")
    if total > max_samples:
        df = df.sample(fraction=max_samples / total, seed=seed)
    pdf = df.toPandas()
    X = np.array(pdf["power"].tolist(), dtype=np.float32)
    y = pdf["emitter_on"].astype(int).to_numpy()
    return X, y, version


def evaluate(y_true, y_pred):
    tn, fp, fn, tp = (int(v) for v in confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel())
    return {
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1": f1_score(y_true, y_pred, zero_division=0),
        "false_positive_rate": fp / (fp + tn) if fp + tn else 0.0,
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
    }


def champion_f1(client, model_name, X_val, y_val):
    """F1 of the current champion on this validation set, or None if there is no champion yet."""
    try:
        client.get_model_version_by_alias(model_name, CHAMPION_ALIAS)
    except MlflowException:
        return None
    champion = mlflow.xgboost.load_model(f"models:/{model_name}@{CHAMPION_ALIAS}")
    return f1_score(y_val, champion.predict(X_val), zero_division=0)


def export_onnx(model, X_val, path):
    """Converts to ONNX and fails if its probabilities drift from XGBoost's beyond ONNX_TOLERANCE."""
    onnx_model = convert_xgboost(model, initial_types=[("power", FloatTensorType([None, NUM_CHANNELS]))])
    with open(path, "wb") as f:
        f.write(onnx_model.SerializeToString())

    session = ort.InferenceSession(path, providers=["CPUExecutionProvider"])
    _, onnx_proba = session.run(None, {"power": X_val})
    max_diff = float(np.max(np.abs(np.asarray(onnx_proba)[:, 1] - model.predict_proba(X_val)[:, 1])))
    if max_diff > ONNX_TOLERANCE:
        raise RuntimeError(f"ONNX probabilities differ from XGBoost by {max_diff} (> {ONNX_TOLERANCE})")
    return max_diff


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    table = f"{args.catalog}.{args.schema}.spectrum_scans"
    model_name = f"{args.catalog}.{args.schema}.emitter_detector"
    models_dir = f"/Volumes/{args.catalog}/{args.schema}/models"

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(args.experiment)
    client = MlflowClient()

    X, y, data_version = load_data(spark, table, args.max_samples, args.seed)
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=args.validation_fraction, random_state=args.seed, stratify=y)

    with mlflow.start_run() as run:
        mlflow.log_params({
            "table": table,
            "data_version": data_version,
            "train_size": len(y_train),
            "validation_size": len(y_val),
            "n_estimators": args.n_estimators,
            "max_depth": args.max_depth,
            "learning_rate": args.learning_rate,
            "seed": args.seed,
        })

        model = XGBClassifier(n_estimators=args.n_estimators, max_depth=args.max_depth,
                              learning_rate=args.learning_rate, random_state=args.seed)
        model.fit(X_train, y_train)

        metrics = evaluate(y_val, model.predict(X_val))
        mlflow.log_metrics(metrics)
        print(f"challenger: {metrics}")

        model_info = mlflow.xgboost.log_model(
            model, "model", signature=infer_signature(X_val[:5], model.predict_proba(X_val[:5])))
        version = mlflow.register_model(model_info.model_uri, model_name).version

        best_f1 = champion_f1(client, model_name, X_val, y_val)
        accepted = best_f1 is None or metrics["f1"] > best_f1
        decision = "accepted" if accepted else "rejected"
        if best_f1 is not None:
            mlflow.log_metric("champion_f1", best_f1)
        mlflow.set_tag("decision", decision)
        client.set_model_version_tag(model_name, version, "decision", decision)
        print(f"version {version}: {decision} (challenger f1={metrics['f1']:.4f}, champion f1={best_f1})")
        if not accepted:
            return

        onnx_path = f"/tmp/emitter_detector_v{version}.onnx"
        max_diff = export_onnx(model, X_val, onnx_path)
        mlflow.log_metric("onnx_max_probability_diff", max_diff)
        mlflow.log_artifact(onnx_path, "onnx")

        client.set_registered_model_alias(model_name, CHAMPION_ALIAS, version)
        os.makedirs(models_dir, exist_ok=True)
        shutil.copy(onnx_path, f"{models_dir}/emitter_detector_v{version}.onnx")
        shutil.copy(onnx_path, f"{models_dir}/champion.onnx")
        print(f"version {version} is the new champion; ONNX saved to {models_dir} (run {run.info.run_id})")
        dbutils.jobs.taskValues.set(key="promoted_version", value=int(version))


if __name__ == "__main__":
    main()
