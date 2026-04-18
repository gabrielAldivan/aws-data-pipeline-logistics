"""
BRONZE LAYER — Glue ETL Job: Raw CSV → Bronze Parquet

Responsibilities:
  - Read raw freight CSV files from S3 landing zone
  - Zero business transformations (raw preservation)
  - Add pipeline metadata columns (_ingestion_timestamp, _source_file, _run_id)
  - Write partitioned Parquet to Bronze S3 bucket
  - Register table in Glue Data Catalog

Runs on: AWS Glue 4.0 (Spark 3.3 + Python 3)
Local test: python glue_jobs/bronze_ingest.py (mock args via env vars)
"""
import sys
import os
import uuid
from datetime import datetime, timezone

# ── Glue context (no-op locally) ──────────────────────────────────────────────
try:
    from awsglue.transforms import *  # noqa: F401, F403
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "RAW_BUCKET",
        "BRONZE_BUCKET",
        "GLUE_DATABASE",
    ])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    IS_GLUE = True

except ImportError:
    # Local development — fall back to plain PySpark
    IS_GLUE = False
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("BronzeIngest-local").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    args = {
        "RAW_BUCKET":    os.environ.get("RAW_BUCKET", "local"),
        "BRONZE_BUCKET": os.environ.get("BRONZE_BUCKET", "local"),
        "GLUE_DATABASE": os.environ.get("GLUE_DATABASE", "logistics_db"),
    }

from pyspark.sql import functions as F  # noqa: E402

# ── Config ────────────────────────────────────────────────────────────────────
RAW_PATH    = f"s3://{args['RAW_BUCKET']}/freight/" if IS_GLUE else "data/raw/"
BRONZE_PATH = f"s3://{args['BRONZE_BUCKET']}/freight/" if IS_GLUE else "data/bronze/freight/"
RUN_ID      = str(uuid.uuid4())
INGESTION_TS = datetime.now(timezone.utc).isoformat()

EXPECTED_COLUMNS = [
    "trip_id", "origin_station", "destination_station", "cargo_type",
    "cargo_weight_tons", "departure_time", "arrival_time", "train_id",
    "operator", "freight_value_brl", "fuel_cost_brl", "delay_minutes",
]


def validate_schema(df):
    """Warn on missing expected columns — never drop rows at Bronze."""
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        print(f"[BRONZE][WARN] Missing expected columns: {missing}")
    return df


def add_metadata(df):
    """Add pipeline lineage columns — Bronze is the source of truth for traceability."""
    return (
        df
        .withColumn("_ingestion_timestamp", F.lit(INGESTION_TS))
        .withColumn("_pipeline_run_id", F.lit(RUN_ID))
        .withColumn("_source_path", F.input_file_name())
        .withColumn("_ingestion_date", F.current_date())
    )


def run():
    print(f"[BRONZE] Reading raw CSVs from: {RAW_PATH}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(RAW_PATH)
    )

    row_count = df.count()
    print(f"[BRONZE] Rows read: {row_count:,}")

    df = validate_schema(df)
    df = add_metadata(df)

    print(f"[BRONZE] Writing to: {BRONZE_PATH}")
    (
        df.write
        .mode("append")                        # append — Bronze is immutable/additive
        .partitionBy("_ingestion_date")
        .parquet(BRONZE_PATH)
    )

    print(f"[BRONZE] Done. Run ID: {RUN_ID} | Rows ingested: {row_count:,}")

    if IS_GLUE:
        job.commit()


if __name__ == "__main__":
    run()
