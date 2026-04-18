-- ─────────────────────────────────────────────────────────────────────────────
-- Redshift COPY — Load Gold S3 Parquet into Redshift Serverless
--
-- Called by Step Functions after GoldAggregate Glue job completes.
-- TRUNCATE + COPY pattern: full refresh of each Gold table.
-- For incremental loads (append-only), remove TRUNCATE and use
-- COPY with MANIFEST to load only new partitions.
--
-- Variables replaced at runtime by Step Functions / Lambda:
--   :gold_bucket    → S3 bucket name (from Terraform output)
--   :iam_role_arn   → Redshift IAM role ARN (from Terraform output)
--   :load_date      → YYYY-MM-DD of the partition being loaded
-- ─────────────────────────────────────────────────────────────────────────────

SET search_path TO logistics;

-- ── Load daily_kpis ───────────────────────────────────────────────────────────
TRUNCATE TABLE logistics.daily_kpis;

COPY logistics.daily_kpis (
    trip_date, total_trips, total_cargo_tons, total_revenue_brl,
    avg_revenue_per_trip, total_margin_brl, avg_duration_hours,
    avg_delay_min, delayed_trips, unique_origins, unique_trains,
    on_time_rate_pct, margin_rate_pct
)
FROM 's3://:gold_bucket/gold_daily_kpis/'
IAM_ROLE ':iam_role_arn'
FORMAT AS PARQUET
SERIALIZETOJSON;

-- ── Load route_performance ────────────────────────────────────────────────────
TRUNCATE TABLE logistics.route_performance;

COPY logistics.route_performance (
    origin_station, destination_station, trip_date,
    total_trips, total_tons, total_revenue_brl,
    avg_revenue_per_ton, avg_delay_min, avg_duration_hours, revenue_rank_daily
)
FROM 's3://:gold_bucket/gold_route_performance/'
IAM_ROLE ':iam_role_arn'
FORMAT AS PARQUET
SERIALIZETOJSON;

-- ── Load cargo_mix ────────────────────────────────────────────────────────────
TRUNCATE TABLE logistics.cargo_mix;

COPY logistics.cargo_mix (
    cargo_type, trip_date, total_trips, total_tons, total_revenue_brl,
    avg_revenue_per_trip, avg_revenue_per_ton, avg_delay_min, revenue_share_pct
)
FROM 's3://:gold_bucket/gold_cargo_mix/'
IAM_ROLE ':iam_role_arn'
FORMAT AS PARQUET
SERIALIZETOJSON;

-- ── Load operator_ranking ─────────────────────────────────────────────────────
TRUNCATE TABLE logistics.operator_ranking;

COPY logistics.operator_ranking (
    operator, trip_date, total_trips, total_revenue_brl,
    avg_delay_min, delayed_trips, avg_duration_hours, on_time_rate_pct
)
FROM 's3://:gold_bucket/gold_operator_ranking/'
IAM_ROLE ':iam_role_arn'
FORMAT AS PARQUET
SERIALIZETOJSON;

-- ── Post-load validation ──────────────────────────────────────────────────────
-- Run after COPY to verify row counts and surface COPY errors
SELECT 'daily_kpis'       AS table_name, COUNT(*) AS row_count FROM logistics.daily_kpis
UNION ALL
SELECT 'route_performance',               COUNT(*) FROM logistics.route_performance
UNION ALL
SELECT 'cargo_mix',                       COUNT(*) FROM logistics.cargo_mix
UNION ALL
SELECT 'operator_ranking',                COUNT(*) FROM logistics.operator_ranking;

-- Check for COPY errors (empty = success)
SELECT * FROM STL_LOAD_ERRORS ORDER BY starttime DESC LIMIT 20;

-- Vacuum + analyze after full load (Redshift best practice)
VACUUM logistics.daily_kpis;
VACUUM logistics.route_performance;
VACUUM logistics.cargo_mix;
VACUUM logistics.operator_ranking;

ANALYZE logistics.daily_kpis;
ANALYZE logistics.route_performance;
ANALYZE logistics.cargo_mix;
ANALYZE logistics.operator_ranking;
