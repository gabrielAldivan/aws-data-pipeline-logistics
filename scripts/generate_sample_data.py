"""
Sample data generator — Brazilian railway freight dataset

Produces realistic synthetic freight trip records for local pipeline testing.
Mirrors the data schema that would come from real operational systems (e.g.,
TOTVS TMS, SAP TM) ingested into the S3 raw zone.

Usage:
  python scripts/generate_sample_data.py               # 50k rows → data/raw/
  python scripts/generate_sample_data.py --rows 200000 --output-dir s3://my-bucket/freight/
"""
import argparse
import os
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────

STATIONS = [
    "SANTOS", "SAO_PAULO", "CAMPINAS", "RIBEIRAO_PRETO", "UBERLANDIA",
    "GOIANIA", "BRASILIA", "BELO_HORIZONTE", "CURITIBA", "PARANAGUA",
    "PORTO_ALEGRE", "CAMPO_GRANDE", "RONDONOPOLIS", "CUIABA", "MANAUS",
]

CARGO_TYPES = ["GRAIN", "FUEL", "IRON_ORE", "CONTAINER", "SUGAR", "SOYBEAN", "OTHER"]
CARGO_WEIGHTS = {
    "GRAIN":     (2000, 8000),   # tons
    "FUEL":      (1000, 5000),
    "IRON_ORE":  (5000, 12000),
    "CONTAINER": (500,  3000),
    "SUGAR":     (1500, 6000),
    "SOYBEAN":   (2000, 9000),
    "OTHER":     (100,  2000),
}

OPERATORS = ["RUMO", "VLI", "FCA", "MRS", "VALE_LOG"]
TRAIN_PREFIX = "BR"


def make_trip(rng: np.random.Generator, trip_date: datetime) -> dict:
    origin, destination = rng.choice(STATIONS, size=2, replace=False)
    cargo_type = rng.choice(CARGO_TYPES, p=[0.25, 0.15, 0.20, 0.10, 0.10, 0.15, 0.05])
    w_min, w_max = CARGO_WEIGHTS[cargo_type]
    weight = round(rng.uniform(w_min, w_max), 2)

    # Distance proxy: fixed table of key corridors, else ~500-2000 km
    dist_km = rng.uniform(300, 2500)
    base_speed_kmh = rng.uniform(50, 80)
    duration_h = dist_km / base_speed_kmh

    departure = trip_date + timedelta(hours=rng.uniform(0, 23))
    arrival   = departure + timedelta(hours=duration_h)

    # Delay: ~30% of trips delayed, heavy-tail distribution
    is_delayed = rng.random() < 0.28
    delay_min  = int(rng.exponential(45)) if is_delayed else 0

    # Freight value: weight × rate/ton (varies by cargo type)
    rate_brl_per_ton = {
        "GRAIN": 35, "FUEL": 50, "IRON_ORE": 28,
        "CONTAINER": 120, "SUGAR": 40, "SOYBEAN": 38, "OTHER": 60,
    }[cargo_type]
    # Apply distance + noise multiplier
    freight_value = round(weight * rate_brl_per_ton * (dist_km / 1000) * rng.uniform(0.8, 1.2), 2)
    fuel_cost = round(dist_km * rng.uniform(8, 14), 2)

    return {
        "trip_id":              str(uuid.uuid4()),
        "origin_station":       origin,
        "destination_station":  destination,
        "cargo_type":           cargo_type,
        "cargo_weight_tons":    weight,
        "departure_time":       departure.strftime("%Y-%m-%d %H:%M:%S"),
        "arrival_time":         arrival.strftime("%Y-%m-%d %H:%M:%S"),
        "train_id":             f"{TRAIN_PREFIX}-{rng.integers(1000, 9999)}",
        "operator":             rng.choice(OPERATORS, p=[0.40, 0.20, 0.15, 0.15, 0.10]),
        "freight_value_brl":    freight_value,
        "fuel_cost_brl":        fuel_cost,
        "delay_minutes":        delay_min,
    }


def generate(n_rows: int = 50_000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    # Spread trips across last 180 days
    base_date = datetime(2024, 1, 1)
    records = []
    for _ in range(n_rows):
        day_offset = rng.integers(0, 180)
        trip_date  = base_date + timedelta(days=int(day_offset))
        records.append(make_trip(rng, trip_date))

    df = pd.DataFrame(records)

    # Inject ~2% dirty rows for DQ demo (negative weights, missing times)
    n_dirty = max(1, int(n_rows * 0.02))
    dirty_idx = rng.choice(df.index, size=n_dirty, replace=False)
    df.loc[dirty_idx[:n_dirty // 3], "cargo_weight_tons"] = -1.0
    df.loc[dirty_idx[n_dirty // 3: 2 * n_dirty // 3], "freight_value_brl"] = -100.0
    df.loc[dirty_idx[2 * n_dirty // 3:], "departure_time"] = None

    return df.sample(frac=1, random_state=seed).reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows",       type=int, default=50_000)
    parser.add_argument("--seed",       type=int, default=42)
    parser.add_argument("--output-dir", default="data/raw/freight/")
    args = parser.parse_args()

    print(f"Generating {args.rows:,} freight trip records (seed={args.seed})...")
    df = generate(n_rows=args.rows, seed=args.seed)

    # Split into monthly files to simulate partitioned landing zone
    df["_month"] = pd.to_datetime(df["departure_time"]).dt.to_period("M").astype(str)
    for month, group in df.groupby("_month"):
        out_df = group.drop(columns=["_month"])
        if args.output_dir.startswith("s3://"):
            # Upload to S3
            import boto3
            import io
            s3 = boto3.client("s3")
            parts = args.output_dir.replace("s3://", "").split("/", 1)
            bucket, prefix = parts[0], parts[1]
            key = f"{prefix}year={month[:4]}/month={month[5:]}/freight_{month}.csv"
            buf = io.BytesIO()
            out_df.to_csv(buf, index=False)
            buf.seek(0)
            s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
            print(f"  Uploaded s3://{bucket}/{key} ({len(out_df):,} rows)")
        else:
            month_dir = os.path.join(args.output_dir, f"year={month[:4]}", f"month={month[5:]}")
            os.makedirs(month_dir, exist_ok=True)
            path = os.path.join(month_dir, f"freight_{month}.csv")
            out_df.to_csv(path, index=False)
            print(f"  Saved {path} ({len(out_df):,} rows)")

    print(f"\nDone. Total rows: {len(df):,}")
    print(f"Dirty rows injected: {int(len(df) * 0.02):,} (~2% for DQ demo)")


if __name__ == "__main__":
    main()
