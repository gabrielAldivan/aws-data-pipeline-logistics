-- ─────────────────────────────────────────────────────────────────────────────
-- Redshift Serverless — Logistics Data Warehouse Schema
--
-- Gold S3 Parquet → Redshift COPY → BI dashboards (Power BI / QuickSight)
--
-- Design decisions:
--   DISTSTYLE KEY on trip_date for time-based analytical queries
--   SORTKEY on trip_date for range scans (dashboard last-30-days filters)
--   ENCODE ZSTD for most varchar columns (high compression, good for analytics)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS logistics
    AUTHORIZATION admin;

SET search_path TO logistics;

-- ── Daily KPIs ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS logistics.daily_kpis (
    trip_date              DATE         NOT NULL  ENCODE DELTA32K,
    total_trips            INTEGER      NOT NULL  ENCODE AZ64,
    total_cargo_tons       DECIMAL(15,2)          ENCODE AZ64,
    total_revenue_brl      DECIMAL(18,2)          ENCODE AZ64,
    avg_revenue_per_trip   DECIMAL(12,2)          ENCODE AZ64,
    total_margin_brl       DECIMAL(18,2)          ENCODE AZ64,
    avg_duration_hours     DECIMAL(8,2)           ENCODE AZ64,
    avg_delay_min          DECIMAL(8,1)           ENCODE AZ64,
    delayed_trips          INTEGER                ENCODE AZ64,
    unique_origins         INTEGER                ENCODE AZ64,
    unique_trains          INTEGER                ENCODE AZ64,
    on_time_rate_pct       DECIMAL(5,2)           ENCODE AZ64,
    margin_rate_pct        DECIMAL(5,2)           ENCODE AZ64,
    loaded_at              TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY DISTKEY (trip_date)
SORTKEY (trip_date);

-- ── Route Performance ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS logistics.route_performance (
    origin_station         VARCHAR(100) NOT NULL  ENCODE ZSTD,
    destination_station    VARCHAR(100) NOT NULL  ENCODE ZSTD,
    trip_date              DATE         NOT NULL  ENCODE DELTA32K,
    total_trips            INTEGER                ENCODE AZ64,
    total_tons             DECIMAL(15,0)          ENCODE AZ64,
    total_revenue_brl      DECIMAL(18,2)          ENCODE AZ64,
    avg_revenue_per_ton    DECIMAL(10,2)          ENCODE AZ64,
    avg_delay_min          DECIMAL(8,1)           ENCODE AZ64,
    avg_duration_hours     DECIMAL(8,2)           ENCODE AZ64,
    revenue_rank_daily     INTEGER                ENCODE AZ64,
    loaded_at              TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY DISTKEY (trip_date)
COMPOUND SORTKEY (trip_date, origin_station, destination_station);

-- ── Cargo Mix ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS logistics.cargo_mix (
    cargo_type             VARCHAR(50)  NOT NULL  ENCODE ZSTD,
    trip_date              DATE         NOT NULL  ENCODE DELTA32K,
    total_trips            INTEGER                ENCODE AZ64,
    total_tons             DECIMAL(15,0)          ENCODE AZ64,
    total_revenue_brl      DECIMAL(18,2)          ENCODE AZ64,
    avg_revenue_per_trip   DECIMAL(12,2)          ENCODE AZ64,
    avg_revenue_per_ton    DECIMAL(10,2)          ENCODE AZ64,
    avg_delay_min          DECIMAL(8,1)           ENCODE AZ64,
    revenue_share_pct      DECIMAL(5,2)           ENCODE AZ64,
    loaded_at              TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY DISTKEY (trip_date)
COMPOUND SORTKEY (trip_date, cargo_type);

-- ── Operator Ranking ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS logistics.operator_ranking (
    operator               VARCHAR(100) NOT NULL  ENCODE ZSTD,
    trip_date              DATE         NOT NULL  ENCODE DELTA32K,
    total_trips            INTEGER                ENCODE AZ64,
    total_revenue_brl      DECIMAL(18,2)          ENCODE AZ64,
    avg_delay_min          DECIMAL(8,1)           ENCODE AZ64,
    delayed_trips          INTEGER                ENCODE AZ64,
    avg_duration_hours     DECIMAL(8,2)           ENCODE AZ64,
    on_time_rate_pct       DECIMAL(5,2)           ENCODE AZ64,
    loaded_at              TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY DISTKEY (trip_date)
COMPOUND SORTKEY (trip_date, operator);
