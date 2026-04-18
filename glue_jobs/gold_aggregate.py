"""
GOLD LAYER — Glue ETL Job: Silver → Gold (business-ready data products)

Produces four domain-ready tables optimised for BI consumption:
  1. gold_daily_kpis         — one row per day (revenue, volume, delays)
  2. gold_route_performance  — one row per origin→destination per day
  3. gold_cargo_mix          — revenue breakdown by cargo type per day
  4. gold_operator_ranking   — operator SLA performance

All tables land in S3 Gold bucket, queryable via Athena.
Gold → Redshift COPY is triggered by Step Functions after this job.

Runs on: AWS Glue 4.0 (Spark 3.3 + Python 3)
"""
import sys
import os

try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "SILVER_BUCKET",
        "GOLD_BUCKET",
        "GLUE_DATABASE",
        "REDSHIFT_IAM_ROLE",
    ])
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
        SparkSession.builder
        .appName("GoldAggregate-local")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    args = {
        "SILVER_BUCKET":   os.environ.get("SILVER_BUCKET", "local"),
        "GOLD_BUCKET":     os.environ.get("GOLD_BUCKET", "local"),
        "GLUE_DATABASE":   os.environ.get("GLUE_DATABASE", "logistics_db"),
        "REDSHIFT_IAM_ROLE": "",
    }

from pyspark.sql import functions as F, DataFrame, Window  # noqa: E402

SILVER_PATH = f"s3://{args['SILVER_BUCKET']}/freight/" if IS_GLUE else "data/silver/freight/"
GOLD_PATH   = f"s3://{args['GOLD_BUCKET']}/"           if IS_GLUE else "data/gold/"


# ── Gold data products ────────────────────────────────────────────────────────

def daily_kpis(df: DataFrame) -> DataFrame:
    """
    Grain: one row per trip_date.
    Primary dashboard table — revenue, volume, SLA metrics.
    """
    return (
        df.groupBy("trip_date")
        .agg(
            F.count("*")                          .alias("total_trips"),
            F.sum("cargo_weight_tons")            .alias("total_cargo_tons"),
            F.round(F.sum("freight_value_brl"), 2).alias("total_revenue_brl"),
            F.round(F.avg("freight_value_brl"), 2).alias("avg_revenue_per_trip"),
            F.round(F.sum("margin_brl"), 2)       .alias("total_margin_brl"),
            F.round(F.avg("trip_duration_hours"), 2).alias("avg_duration_hours"),
            F.round(F.avg("delay_minutes"), 1)    .alias("avg_delay_min"),
            F.sum(F.col("is_delayed").cast("int")).alias("delayed_trips"),
            F.countDistinct("origin_station")     .alias("unique_origins"),
            F.countDistinct("train_id")           .alias("unique_trains"),
        )
        .withColumn("on_time_rate_pct",
                    F.round(
                        (1 - F.col("delayed_trips") / F.col("total_trips")) * 100, 2
                    ))
        .withColumn("margin_rate_pct",
                    F.round(F.col("total_margin_brl") / F.col("total_revenue_brl") * 100, 2))
        .orderBy("trip_date")
    )


def route_performance(df: DataFrame) -> DataFrame:
    """
    Grain: one row per origin_station + destination_station + trip_date.
    Revenue rank per day helps identify highest-value corridors.
    """
    window = Window.partitionBy("trip_date").orderBy(F.col("total_revenue_brl").desc())
    return (
        df.groupBy("origin_station", "destination_station", "trip_date")
        .agg(
            F.count("*")                           .alias("total_trips"),
            F.round(F.sum("cargo_weight_tons"), 0) .alias("total_tons"),
            F.round(F.sum("freight_value_brl"), 2) .alias("total_revenue_brl"),
            F.round(F.avg("revenue_per_ton"), 2)   .alias("avg_revenue_per_ton"),
            F.round(F.avg("delay_minutes"), 1)     .alias("avg_delay_min"),
            F.round(F.avg("trip_duration_hours"), 2).alias("avg_duration_hours"),
        )
        .withColumn("revenue_rank_daily", F.rank().over(window))
        .orderBy("trip_date", "revenue_rank_daily")
    )


def cargo_mix(df: DataFrame) -> DataFrame:
    """
    Grain: one row per cargo_type + trip_date.
    Shows revenue composition and which commodities drive the P&L.
    """
    total_rev_window = Window.partitionBy("trip_date")
    return (
        df.groupBy("cargo_type", "trip_date")
        .agg(
            F.count("*")                            .alias("total_trips"),
            F.round(F.sum("cargo_weight_tons"), 0)  .alias("total_tons"),
            F.round(F.sum("freight_value_brl"), 2)  .alias("total_revenue_brl"),
            F.round(F.avg("freight_value_brl"), 2)  .alias("avg_revenue_per_trip"),
            F.round(F.avg("revenue_per_ton"), 2)    .alias("avg_revenue_per_ton"),
            F.round(F.avg("delay_minutes"), 1)      .alias("avg_delay_min"),
        )
        .withColumn("revenue_share_pct",
                    F.round(
                        F.col("total_revenue_brl")
                        / F.sum("total_revenue_brl").over(total_rev_window) * 100,
                        2,
                    ))
        .orderBy("trip_date", F.col("total_revenue_brl").desc())
    )


def operator_ranking(df: DataFrame) -> DataFrame:
    """
    Grain: one row per operator + trip_date.
    SLA: on-time rate, average delay, revenue contribution.
    """
    return (
        df.groupBy("operator", "trip_date")
        .agg(
            F.count("*")                            .alias("total_trips"),
            F.round(F.sum("freight_value_brl"), 2)  .alias("total_revenue_brl"),
            F.round(F.avg("delay_minutes"), 1)      .alias("avg_delay_min"),
            F.sum(F.col("is_delayed").cast("int"))  .alias("delayed_trips"),
            F.round(F.avg("trip_duration_hours"), 2).alias("avg_duration_hours"),
        )
        .withColumn("on_time_rate_pct",
                    F.round(
                        (1 - F.col("delayed_trips") / F.col("total_trips")) * 100, 2
                    ))
        .orderBy("trip_date", F.col("total_revenue_brl").desc())
    )


# ── Write helper ──────────────────────────────────────────────────────────────

def write_gold(df: DataFrame, table_name: str, partition_col: str = "trip_date"):
    path = f"{GOLD_PATH}{table_name}/"
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)
    count = df.count()
    print(f"[GOLD] {table_name}: {count:,} rows → {path}")
    return count


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    print(f"[GOLD] Reading Silver from: {SILVER_PATH}")
    df = spark.read.parquet(SILVER_PATH)
    total = df.count()
    print(f"[GOLD] Silver rows available: {total:,}")

    df.cache()   # reused across 4 aggregations

    write_gold(daily_kpis(df),        "gold_daily_kpis")
    write_gold(route_performance(df), "gold_route_performance")
    write_gold(cargo_mix(df),         "gold_cargo_mix")
    write_gold(operator_ranking(df),  "gold_operator_ranking")

    df.unpersist()

    print("\n[GOLD] All data products written. Pipeline complete.")
    print("[GOLD] Next: Step Functions will run the Gold Glue Crawler + Redshift COPY.")

    if IS_GLUE:
        job.commit()


if __name__ == "__main__":
    run()
