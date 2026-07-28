"""
Unit tests for Glue ETL jobs — run locally with plain PySpark.

Sets SPARK_LOCAL=true so get_spark() uses local[*] instead of YARN.
Tests validate transformation logic independently of AWS infrastructure.
"""

import os
import sys

import pytest

os.environ["SPARK_LOCAL"] = "true"
os.environ["BRONZE_BUCKET"] = "local"
os.environ["SILVER_BUCKET"] = "local"
os.environ["GOLD_BUCKET"] = "local"
os.environ["GLUE_DATABASE"] = "test_db"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Spark fixture ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("test-glue-jobs")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Helpers ───────────────────────────────────────────────────────────────────


def make_silver_df(spark, n: int = 100):
    """Synthetic Silver-style freight DataFrame for testing Gold aggregations."""
    from datetime import date, datetime
    import random

    random.seed(42)

    cargo_types = ["GRAIN", "FUEL", "IRON_ORE", "CONTAINER", "SOYBEAN"]
    operators = ["RUMO", "VLI", "FCA", "MRS"]
    stations = ["SANTOS", "SAO_PAULO", "CAMPINAS", "CURITIBA", "GOIANIA"]

    rows = []
    for i in range(n):
        rows.append(
            {
                "trip_id": f"TRIP-{i:04d}",
                "origin_station": random.choice(stations),
                "destination_station": random.choice(stations),
                "cargo_type": random.choice(cargo_types),
                "cargo_weight_tons": round(random.uniform(500, 10000), 2),
                "freight_value_brl": round(random.uniform(5000, 200000), 2),
                "fuel_cost_brl": round(random.uniform(1000, 50000), 2),
                "delay_minutes": random.choice([0] * 7 + [30, 90, 180]),
                "trip_duration_hours": round(random.uniform(2, 48), 2),
                "revenue_per_ton": round(random.uniform(10, 150), 2),
                "margin_brl": round(random.uniform(1000, 80000), 2),
                "is_delayed": random.random() < 0.28,
                "departure_hour": random.randint(0, 23),
                "operator": random.choice(operators),
                "trip_date": date(2024, random.randint(1, 6), random.randint(1, 28)),
            }
        )
    return spark.createDataFrame(rows)


# ── Silver transform tests ────────────────────────────────────────────────────


class TestSilverTransformations:
    def test_cast_types_returns_correct_timestamp_type(self, spark):
        from pyspark.sql.types import TimestampType
        from glue_jobs.silver_transform import cast_types

        df = spark.createDataFrame(
            [
                {
                    "departure_time": "2024-03-15 08:30:00",
                    "arrival_time": "2024-03-15 16:45:00",
                    "cargo_weight_tons": "5000.5",
                    "freight_value_brl": "75000.00",
                    "fuel_cost_brl": "12000.00",
                    "delay_minutes": "45",
                    "cargo_type": " grain ",
                    "origin_station": " santos ",
                    "destination_station": " sao paulo ",
                }
            ]
        )
        result = cast_types(df)
        assert dict(result.dtypes)["departure_time"] == "timestamp"
        assert dict(result.dtypes)["cargo_weight_tons"] == "double"

    def test_cast_types_uppercases_string_cols(self, spark):
        from glue_jobs.silver_transform import cast_types

        df = spark.createDataFrame(
            [
                {
                    "departure_time": "2024-01-01 00:00:00",
                    "arrival_time": "2024-01-01 06:00:00",
                    "cargo_weight_tons": "1000",
                    "freight_value_brl": "50000",
                    "fuel_cost_brl": "5000",
                    "delay_minutes": "0",
                    "cargo_type": "grain",
                    "origin_station": " santos ",
                    "destination_station": " campinas",
                }
            ]
        )
        result = cast_types(df).collect()[0]
        assert result["cargo_type"] == "GRAIN"
        assert result["origin_station"] == "SANTOS"

    def test_derive_columns_adds_trip_date(self, spark):
        from glue_jobs.silver_transform import derive_columns
        from pyspark.sql import functions as F
        from pyspark.sql.types import TimestampType

        df = (
            spark.createDataFrame(
                [
                    {
                        "departure_time": "2024-03-15 08:00:00",
                        "arrival_time": "2024-03-15 20:00:00",
                        "cargo_weight_tons": 5000.0,
                        "freight_value_brl": 100000.0,
                        "fuel_cost_brl": 15000.0,
                        "delay_minutes": 30,
                    }
                ]
            )
            .withColumn("departure_time", F.col("departure_time").cast(TimestampType()))
            .withColumn("arrival_time", F.col("arrival_time").cast(TimestampType()))
        )

        result = derive_columns(df)
        assert "trip_date" in result.columns
        assert "trip_duration_hours" in result.columns
        assert "is_delayed" in result.columns
        assert "margin_brl" in result.columns

    def test_derive_columns_duration_is_positive(self, spark):
        from glue_jobs.silver_transform import derive_columns
        from pyspark.sql import functions as F
        from pyspark.sql.types import TimestampType

        df = (
            spark.createDataFrame(
                [
                    {
                        "departure_time": "2024-03-15 06:00:00",
                        "arrival_time": "2024-03-15 18:00:00",
                        "cargo_weight_tons": 3000.0,
                        "freight_value_brl": 60000.0,
                        "fuel_cost_brl": 10000.0,
                        "delay_minutes": 0,
                    }
                ]
            )
            .withColumn("departure_time", F.col("departure_time").cast(TimestampType()))
            .withColumn("arrival_time", F.col("arrival_time").cast(TimestampType()))
        )

        row = derive_columns(df).collect()[0]
        assert row["trip_duration_hours"] == pytest.approx(12.0, abs=0.1)

    def test_dq_rules_quarantine_negative_weight(self, spark):
        from glue_jobs.silver_transform import apply_dq_rules
        from pyspark.sql import functions as F
        from pyspark.sql.types import TimestampType

        rows = [
            {
                "trip_id": "OK-1",
                "cargo_weight_tons": 1000.0,
                "freight_value_brl": 50000.0,
                "delay_minutes": 0,
                "departure_time": "2024-01-01 06:00:00",
                "arrival_time": "2024-01-01 12:00:00",
            },
            {
                "trip_id": "BAD-1",
                "cargo_weight_tons": -1.0,
                "freight_value_brl": 50000.0,
                "delay_minutes": 0,
                "departure_time": "2024-01-01 06:00:00",
                "arrival_time": "2024-01-01 12:00:00",
            },
        ]
        df = (
            spark.createDataFrame(rows)
            .withColumn("departure_time", F.col("departure_time").cast(TimestampType()))
            .withColumn("arrival_time", F.col("arrival_time").cast(TimestampType()))
        )

        df_clean, df_quarantine = apply_dq_rules(df)
        assert df_clean.count() == 1
        assert df_quarantine.count() == 1
        assert df_clean.collect()[0]["trip_id"] == "OK-1"

    def test_dedup_removes_duplicates(self, spark):
        from glue_jobs.silver_transform import dedup

        rows = [
            {"trip_id": "T1", "departure_time": "2024-01-01 08:00:00"},
            {"trip_id": "T1", "departure_time": "2024-01-01 08:00:00"},  # duplicate
            {"trip_id": "T2", "departure_time": "2024-01-02 08:00:00"},
        ]
        df = spark.createDataFrame(rows)
        result = dedup(df)
        assert result.count() == 2


# ── Gold aggregate tests ──────────────────────────────────────────────────────


class TestGoldAggregations:
    def test_daily_kpis_grain_is_one_row_per_date(self, spark):
        from glue_jobs.gold_aggregate import daily_kpis

        df = make_silver_df(spark, 100)
        result = daily_kpis(df)
        # Should have one row per trip_date
        assert result.count() == result.select("trip_date").distinct().count()

    def test_daily_kpis_on_time_rate_between_0_and_100(self, spark):
        from glue_jobs.gold_aggregate import daily_kpis

        df = make_silver_df(spark, 100)
        result = daily_kpis(df)
        rates = [row["on_time_rate_pct"] for row in result.collect()]
        for rate in rates:
            assert 0.0 <= rate <= 100.0, f"on_time_rate_pct={rate} out of range"

    def test_cargo_mix_revenue_shares_sum_to_100_per_date(self, spark):
        from glue_jobs.gold_aggregate import cargo_mix
        from pyspark.sql import functions as F

        df = make_silver_df(spark, 200)
        result = cargo_mix(df)
        sums = (
            result.groupBy("trip_date")
            .agg(F.round(F.sum("revenue_share_pct"), 0).alias("total_pct"))
            .collect()
        )
        for row in sums:
            assert (
                abs(row["total_pct"] - 100.0) < 2.0
            ), f"Revenue shares don't sum to ~100% on {row['trip_date']}"

    def test_route_performance_has_revenue_rank(self, spark):
        from glue_jobs.gold_aggregate import route_performance

        df = make_silver_df(spark, 100)
        result = route_performance(df)
        assert "revenue_rank_daily" in result.columns
        ranks = [row["revenue_rank_daily"] for row in result.collect()]
        assert all(r >= 1 for r in ranks)

    def test_operator_ranking_columns_present(self, spark):
        from glue_jobs.gold_aggregate import operator_ranking

        df = make_silver_df(spark, 100)
        result = operator_ranking(df)
        expected = {
            "operator",
            "trip_date",
            "total_trips",
            "total_revenue_brl",
            "on_time_rate_pct",
        }
        assert expected.issubset(set(result.columns))
