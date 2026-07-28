"""
SILVER LAYER — Glue ETL Job: Bronze Parquet → Silver Parquet

Responsibilities:
  - Rename columns to snake_case
  - Cast to correct types (timestamps, doubles, integers)
  - Derive business columns (trip_duration_hours, revenue_per_ton, trip_date)
  - Apply data quality rules (quarantine invalid rows — never silently drop)
  - Deduplicate by natural key
  - Write partitioned Parquet to Silver S3 bucket
  - Register table in Glue Data Catalog

Runs on: AWS Glue 4.0 (Spark 3.3 + Python 3)
"""

import sys
import os
from dataclasses import dataclass
from typing import List

try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "BRONZE_BUCKET",
            "SILVER_BUCKET",
            "GLUE_DATABASE",
        ],
    )
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    IS_GLUE = True

except ImportError:
    IS_GLUE = False
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("SilverTransform-local")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    args = {
        "BRONZE_BUCKET": os.environ.get("BRONZE_BUCKET", "local"),
        "SILVER_BUCKET": os.environ.get("SILVER_BUCKET", "local"),
        "GLUE_DATABASE": os.environ.get("GLUE_DATABASE", "logistics_db"),
    }

from pyspark.sql import functions as F, DataFrame  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    IntegerType,
    TimestampType,
    StringType,
)

# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_PATH = (
    f"s3://{args['BRONZE_BUCKET']}/freight/" if IS_GLUE else "data/bronze/freight/"
)
SILVER_PATH = (
    f"s3://{args['SILVER_BUCKET']}/freight/" if IS_GLUE else "data/silver/freight/"
)
QUARANTINE_PATH = (
    f"s3://{args['SILVER_BUCKET']}/quarantine/freight/"
    if IS_GLUE
    else "data/quarantine/freight/"
)


# ── Data Quality Framework ────────────────────────────────────────────────────


@dataclass
class DQRule:
    name: str
    condition: str  # Spark SQL expression — True = pass
    severity: str  # "error" → quarantine | "warning" → flag and keep
    description: str = ""


DQ_RULES: List[DQRule] = [
    DQRule(
        "positive_weight",
        "cargo_weight_tons > 0",
        "error",
        "Cargo weight must be positive",
    ),
    DQRule(
        "non_negative_value",
        "freight_value_brl >= 0",
        "error",
        "Freight value cannot be negative",
    ),
    DQRule(
        "non_negative_delay",
        "delay_minutes >= 0",
        "error",
        "Delay in minutes cannot be negative",
    ),
    DQRule(
        "valid_departure",
        "departure_time IS NOT NULL",
        "error",
        "Departure time is required",
    ),
    DQRule(
        "valid_arrival", "arrival_time IS NOT NULL", "error", "Arrival time is required"
    ),
    DQRule(
        "time_sequence",
        "arrival_time > departure_time",
        "error",
        "Arrival must be after departure",
    ),
    DQRule(
        "reasonable_weight",
        "cargo_weight_tons < 15000",
        "warning",
        "Single trip > 15,000 tons is suspicious",
    ),
    DQRule(
        "known_cargo_type",
        "cargo_type IN ('GRAIN','FUEL','IRON_ORE','CONTAINER','SUGAR','SOYBEAN','OTHER')",
        "warning",
        "Unrecognised cargo type",
    ),
]


def apply_dq_rules(df: DataFrame) -> tuple:
    """
    Separate rows into clean (all ERROR rules pass) and quarantine (any ERROR failed).
    WARNING rules add a flag column but never quarantine.
    Returns (df_clean, df_quarantine).
    """
    error_rules = [r for r in DQ_RULES if r.severity == "error"]
    warning_rules = [r for r in DQ_RULES if r.severity == "warning"]

    # Evaluate all ERROR rules
    for rule in error_rules:
        df = df.withColumn(f"_dq_{rule.name}", F.expr(rule.condition))

    error_cols = [f"_dq_{r.name}" for r in error_rules]
    all_pass_expr = " AND ".join(error_cols)

    df_clean = df.filter(F.expr(all_pass_expr))
    df_fail = df.filter(~F.expr(all_pass_expr))

    # Add failure reason string to quarantine
    reason_expr = " || ', ' || ".join(
        [
            f"CASE WHEN NOT {col} THEN '{r.name}' ELSE '' END"
            for col, r in zip(error_cols, error_rules)
        ]
    )
    df_quarantine = df_fail.withColumn("_failure_reason", F.expr(reason_expr))

    # Evaluate WARNING rules on clean data (flag only)
    for rule in warning_rules:
        df_clean = df_clean.withColumn(f"_warn_{rule.name}", ~F.expr(rule.condition))

    # Drop internal DQ columns before writing
    dq_cols = [c for c in df_clean.columns if c.startswith("_dq_")]
    df_clean = df_clean.drop(*dq_cols)

    return df_clean, df_quarantine


# ── Transformations ───────────────────────────────────────────────────────────


def cast_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("departure_time", F.col("departure_time").cast(TimestampType()))
        .withColumn("arrival_time", F.col("arrival_time").cast(TimestampType()))
        .withColumn("cargo_weight_tons", F.col("cargo_weight_tons").cast(DoubleType()))
        .withColumn("freight_value_brl", F.col("freight_value_brl").cast(DoubleType()))
        .withColumn("fuel_cost_brl", F.col("fuel_cost_brl").cast(DoubleType()))
        .withColumn("delay_minutes", F.col("delay_minutes").cast(IntegerType()))
        .withColumn(
            "cargo_type", F.upper(F.trim(F.col("cargo_type"))).cast(StringType())
        )
        .withColumn(
            "origin_station",
            F.upper(F.trim(F.col("origin_station"))).cast(StringType()),
        )
        .withColumn(
            "destination_station",
            F.upper(F.trim(F.col("destination_station"))).cast(StringType()),
        )
    )


def derive_columns(df: DataFrame) -> DataFrame:
    return (
        df
        # Core temporal
        .withColumn("trip_date", F.to_date("departure_time"))
        .withColumn("departure_hour", F.hour("departure_time"))
        .withColumn(
            "trip_duration_hours",
            F.round(
                (F.unix_timestamp("arrival_time") - F.unix_timestamp("departure_time"))
                / 3600,
                2,
            ),
        )
        # Business KPIs
        .withColumn(
            "revenue_per_ton",
            F.when(
                F.col("cargo_weight_tons") > 0,
                F.round(F.col("freight_value_brl") / F.col("cargo_weight_tons"), 2),
            ).otherwise(None),
        )
        .withColumn(
            "margin_brl",
            F.round(F.col("freight_value_brl") - F.col("fuel_cost_brl"), 2),
        )
        .withColumn("is_delayed", (F.col("delay_minutes") > 30).cast("boolean"))
    )


def dedup(df: DataFrame) -> DataFrame:
    """Deduplicate on natural key — trip_id + departure_time."""
    before = df.count()
    df = df.dropDuplicates(["trip_id", "departure_time"])
    removed = before - df.count()
    if removed > 0:
        print(f"[SILVER] Deduplication: removed {removed:,} duplicate rows")
    return df


def run():
    print(f"[SILVER] Reading from Bronze: {BRONZE_PATH}")
    df = spark.read.parquet(BRONZE_PATH)
    print(f"[SILVER] Bronze rows: {df.count():,}")

    df = cast_types(df)
    df = df.filter(
        F.col("departure_time").isNotNull() & F.col("arrival_time").isNotNull()
    )
    df = derive_columns(df)
    df = dedup(df)

    print("[SILVER] Applying data quality rules...")
    df_clean, df_quarantine = apply_dq_rules(df)

    quarantine_count = df_quarantine.count()
    clean_count = df_clean.count()
    print(f"[SILVER] Clean rows: {clean_count:,} | Quarantined: {quarantine_count:,}")

    if quarantine_count > 0:
        (
            df_quarantine.write.mode("append")
            .partitionBy("trip_date")
            .parquet(QUARANTINE_PATH)
        )
        print(f"[SILVER] Quarantine written to: {QUARANTINE_PATH}")

    # Drop Bronze metadata before writing Silver
    meta_cols = [
        c for c in df_clean.columns if c.startswith("_") and not c.startswith("_warn")
    ]
    df_silver = df_clean.drop(*meta_cols)

    print(f"[SILVER] Writing to: {SILVER_PATH}")
    (df_silver.write.mode("overwrite").partitionBy("trip_date").parquet(SILVER_PATH))

    print(f"[SILVER] Done. Rows written: {clean_count:,}")

    if IS_GLUE:
        job.commit()


if __name__ == "__main__":
    run()
