"""
EMR PySpark Job — Heavy batch processing

Use cases that exceed Glue's capacity and warrant EMR:
  1. Historical backfill: reprocessing multi-year freight archives
  2. Delay prediction scoring: run XGBoost model over full Silver dataset
  3. Freight demand clustering: K-Means over all origin-destination pairs

Submit via AWS CLI:
  aws emr add-steps \
    --cluster-id j-XXXXXXX \
    --steps Type=Spark,Name="FreightScoring",\
      Args=[--deploy-mode,cluster,--master,yarn,\
            s3://bucket/emr_jobs/heavy_processing.py,\
            --silver-path,s3://bucket/freight/,\
            --output-path,s3://bucket/scored/,\
            --mode,score]

Or via Terraform EMR step resource / Step Functions EMR integration.
"""

import argparse
import os
import sys

from pyspark.sql import SparkSession, functions as F, DataFrame, Window
from pyspark.sql.types import DoubleType

# ── SparkSession (provided by YARN on EMR, or local for testing) ──────────────


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    # On EMR, master is set by YARN — only override locally
    if os.environ.get("SPARK_LOCAL", "false").lower() == "true":
        builder = builder.master("local[*]")
    return builder.getOrCreate()


# ── Mode 1: Historical backfill ───────────────────────────────────────────────


def historical_backfill(spark: SparkSession, silver_path: str, gold_path: str):
    """
    Reprocess all Silver partitions and rewrite Gold tables from scratch.
    Used after schema migrations or bug fixes in Silver transformations.
    Runs on EMR because it involves full-table scans (100M+ rows).
    """
    print("[BACKFILL] Reading full Silver history...")
    df = spark.read.parquet(silver_path)
    total = df.count()
    print(f"[BACKFILL] Total Silver rows: {total:,}")

    # Window for percentile calculations (requires full dataset — EMR justified)
    pct_window = Window.partitionBy("cargo_type").orderBy("freight_value_brl")
    df = df.withColumn("revenue_percentile", F.percent_rank().over(pct_window))

    # Route-level metrics with full historical context
    route_w = Window.partitionBy("origin_station", "destination_station")
    df_routes = df.withColumn(
        "route_avg_revenue_historical",
        F.round(F.avg("freight_value_brl").over(route_w), 2),
    ).withColumn("route_trip_count_historical", F.count("*").over(route_w))

    output_path = f"{gold_path}gold_historical_enriched/"
    (df_routes.write.mode("overwrite").partitionBy("trip_date").parquet(output_path))
    print(f"[BACKFILL] Done. Written to: {output_path}")


# ── Mode 2: Delay prediction scoring ─────────────────────────────────────────


def delay_scoring(spark: SparkSession, silver_path: str, output_path: str):
    """
    Batch ML scoring: predict delay probability for each trip using
    a pre-trained XGBoost model loaded from S3.

    Feature engineering mirrors the model training pipeline.
    Results land in Gold so analysts can query via Athena.
    """
    try:
        import mlflow
        import mlflow.xgboost

        model_uri = os.environ.get("MODEL_URI", "")
        if model_uri:
            model = mlflow.xgboost.load_model(model_uri)
            print(f"[SCORING] Model loaded from MLflow: {model_uri}")
        else:
            raise ValueError("MODEL_URI not set")
    except Exception as exc:
        print(
            f"[SCORING][WARN] Could not load MLflow model ({exc}). Using heuristic fallback."
        )
        model = None

    df = spark.read.parquet(silver_path)
    print(f"[SCORING] Rows to score: {df.count():,}")

    # Feature engineering (must match training pipeline)
    df_feat = (
        df.withColumn("hour_sin", F.sin(2 * 3.14159 * F.col("departure_hour") / 24))
        .withColumn("hour_cos", F.cos(2 * 3.14159 * F.col("departure_hour") / 24))
        .withColumn("log_weight", F.log1p("cargo_weight_tons"))
        .withColumn("log_revenue", F.log1p("freight_value_brl"))
    )

    if model is not None:
        # Vectorise features → score via pandas UDF for scalability
        feature_cols = [
            "cargo_weight_tons",
            "freight_value_brl",
            "fuel_cost_brl",
            "trip_duration_hours",
            "hour_sin",
            "hour_cos",
            "log_weight",
            "log_revenue",
        ]
        import pandas as pd
        from pyspark.sql.functions import pandas_udf

        @pandas_udf(DoubleType())
        def predict_delay_proba(*cols) -> pd.Series:
            X = pd.concat(list(cols), axis=1)
            X.columns = feature_cols
            return pd.Series(model.predict_proba(X)[:, 1])

        df_scored = df_feat.withColumn(
            "delay_probability", predict_delay_proba(*[F.col(c) for c in feature_cols])
        )
    else:
        # Heuristic fallback: longer + heavier = higher delay risk
        df_scored = df_feat.withColumn(
            "delay_probability",
            F.least(
                F.lit(1.0),
                F.round(
                    (F.col("cargo_weight_tons") / 10000 * 0.3)
                    + (F.col("trip_duration_hours") / 48 * 0.4)
                    + F.lit(0.1),
                    4,
                ),
            ).cast(DoubleType()),
        )

    (
        df_scored.select(
            "trip_id",
            "trip_date",
            "origin_station",
            "destination_station",
            "cargo_type",
            "delay_probability",
        )
        .write.mode("overwrite")
        .partitionBy("trip_date")
        .parquet(output_path)
    )
    print(f"[SCORING] Scored trips written to: {output_path}")


# ── Mode 3: Freight demand clustering ─────────────────────────────────────────


def demand_clustering(
    spark: SparkSession, silver_path: str, output_path: str, k: int = 6
):
    """
    K-Means clustering on origin-destination pairs.
    Segments routes into demand tiers for pricing strategy.
    """
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator

    print(f"[CLUSTER] Running K-Means (k={k}) on route features...")
    df = spark.read.parquet(silver_path)

    # Aggregate to route level
    df_routes = (
        df.groupBy("origin_station", "destination_station")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("cargo_weight_tons").alias("avg_weight"),
            F.avg("freight_value_brl").alias("avg_revenue"),
            F.avg("delay_minutes").alias("avg_delay"),
            F.avg("trip_duration_hours").alias("avg_duration"),
            F.countDistinct("cargo_type").alias("cargo_diversity"),
        )
        .filter(F.col("trip_count") >= 10)  # only routes with enough history
    )

    feature_cols = [
        "trip_count",
        "avg_weight",
        "avg_revenue",
        "avg_delay",
        "avg_duration",
        "cargo_diversity",
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features", withStd=True, withMean=True
    )
    kmeans = KMeans(
        featuresCol="features", predictionCol="cluster", k=k, seed=42, maxIter=100
    )

    df_assembled = assembler.transform(df_routes)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    km_model = kmeans.fit(df_scaled)

    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")
    silhouette = evaluator.evaluate(km_model.transform(df_scaled))
    print(f"[CLUSTER] Silhouette Score: {silhouette:.4f}")
    print(f"[CLUSTER] Cluster sizes:\n{km_model.summary.clusterSizes}")

    df_result = km_model.transform(df_scaled).drop("features_raw", "features")
    df_result.write.mode("overwrite").parquet(output_path)
    print(f"[CLUSTER] Route clusters written to: {output_path}")


# ── CLI entrypoint ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="EMR heavy processing job")
    parser.add_argument(
        "--mode", choices=["backfill", "score", "cluster"], required=True
    )
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--gold-path", default="")
    parser.add_argument(
        "--k", type=int, default=6, help="K-Means clusters (mode=cluster only)"
    )
    args = parser.parse_args()

    spark = get_spark(f"EMR-{args.mode.title()}")

    if args.mode == "backfill":
        historical_backfill(spark, args.silver_path, args.gold_path or args.output_path)
    elif args.mode == "score":
        delay_scoring(spark, args.silver_path, args.output_path)
    elif args.mode == "cluster":
        demand_clustering(spark, args.silver_path, args.output_path, k=args.k)

    spark.stop()
    print("[EMR] Job complete.")


if __name__ == "__main__":
    main()
