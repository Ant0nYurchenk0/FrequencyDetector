"""Databricks job: delete scans older than --retention-hours from spectrum_scans.

DELETE removes the rows from the table; VACUUM then deletes the data files no longer referenced,
once they are older than Delta's default safety window (7 days).
"""
import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--retention-hours", type=int, default=24)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    table = f"{args.catalog}.{args.schema}.spectrum_scans"

    if not spark.catalog.tableExists(table):
        print(f"{table}: does not exist yet, nothing to clean")
        return

    deleted = spark.sql(f"""
        DELETE FROM {table}
        WHERE kafka_timestamp < current_timestamp() - INTERVAL {args.retention_hours} HOURS
    """).first()[0]
    print(f"{table}: deleted {deleted} rows older than {args.retention_hours} hours")

    spark.sql(f"VACUUM {table}")


if __name__ == "__main__":
    main()
