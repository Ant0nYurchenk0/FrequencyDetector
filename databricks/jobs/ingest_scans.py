"""Databricks job: Kafka topic rf-spectrum-scans -> Delta table spectrum_scans.

Each Kafka message holds a JSON array of scans. To emulate a processing stage, each scan is kept
with probability --keep-fraction. The Delta sink plus checkpoint commits every micro-batch exactly
once, so restarting the job never duplicates rows.
Each run reads everything new in Kafka, then stops (serverless supports only this "available now" mode).
Kafka connection settings are read from the Databricks secret scope named by --secret-scope.
"""
import argparse
import socket

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

NUM_CHANNELS = 100
TOPIC = "rf-spectrum-scans"

MESSAGE_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("session_id", T.StringType()),
    T.StructField("scan_id", T.LongType()),
    T.StructField("power", T.ArrayType(T.DoubleType())),
    T.StructField("emitter_on", T.BooleanType()),
]))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--keep-fraction", type=float, default=0.5)
    return parser.parse_args()


def check_kafka_reachable(bootstrap):
    """Fail fast with a readable message instead of Kafka's endless reconnect loop."""
    host, port = bootstrap.rsplit(":", 1)
    try:
        socket.create_connection((host, int(port)), timeout=10).close()
    except OSError as e:
        raise RuntimeError(
            f"Cannot open TCP connection to Kafka at {bootstrap}: {e}. "
            "Check the VM is running, the firewall allows 9094, and the kafka-bootstrap secret is right."
        ) from e


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    bootstrap = dbutils.secrets.get(args.secret_scope, "kafka-bootstrap")
    username = dbutils.secrets.get(args.secret_scope, "kafka-username")
    password = dbutils.secrets.get(args.secret_scope, "kafka-password")
    check_kafka_reachable(bootstrap)

    table = f"{args.catalog}.{args.schema}.spectrum_scans"
    checkpoint = f"/Volumes/{args.catalog}/{args.schema}/checkpoints/spectrum_scans"

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        # Databricks ships a relocated Kafka client: the "kafkashaded." prefix is required.
        .option("kafka.sasl.jaas.config",
                "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{username}" password="{password}";')
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        # Kafka retention may delete offsets the checkpoint still points at; skip ahead instead of failing.
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )

    scans = (
        raw.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.explode(F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA)).alias("scan"),
        )
        .select("scan.*", "kafka_timestamp")
        .filter(F.col("session_id").isNotNull()
                & F.col("scan_id").isNotNull()
                & F.col("emitter_on").isNotNull()
                & (F.size("power") == NUM_CHANNELS))
        .filter(F.rand() < args.keep_fraction)
        .withColumn("ingested_at", F.current_timestamp())
    )

    (scans.writeStream.format("delta")
     .option("checkpointLocation", checkpoint)
     .trigger(availableNow=True)
     .toTable(table)
     .awaitTermination())


if __name__ == "__main__":
    main()
