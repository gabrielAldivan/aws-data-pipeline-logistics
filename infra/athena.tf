# ─────────────────────────────────────────────────────────────────────────────
# AWS Athena — Serverless SQL over S3
#
# Athena queries the Glue Data Catalog tables directly on S3.
# Gold tables → ad-hoc analysis, dashboards (QuickSight / Power BI)
# Silver tables → data quality audits
# Bronze tables → lineage / raw record investigation
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "main" {
  name        = "${local.name_prefix}-wg"
  description = "Workgroup for logistics pipeline analytical queries"

  configuration {
    # Hard limit: cancel queries that scan more than threshold (cost control)
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = {
    CostCenter = "data-engineering"
  }
}

# Named queries — reusable SQL saved inside Athena console
resource "aws_athena_named_query" "daily_kpis" {
  name      = "daily-freight-kpis"
  workgroup = aws_athena_workgroup.main.id
  database  = aws_glue_catalog_database.main.name
  description = "Daily freight KPIs from the Gold layer"

  query = <<-SQL
    SELECT
        trip_date,
        COUNT(*)                          AS total_trips,
        SUM(cargo_weight_tons)            AS total_cargo_tons,
        ROUND(SUM(freight_value_brl), 2)  AS total_revenue_brl,
        ROUND(AVG(freight_value_brl), 2)  AS avg_revenue_per_trip,
        ROUND(AVG(delay_minutes), 1)      AS avg_delay_min,
        SUM(CASE WHEN delay_minutes > 60 THEN 1 ELSE 0 END) AS trips_delayed_over_1h
    FROM "${aws_glue_catalog_database.main.name}"."gold_daily_kpis"
    WHERE trip_date >= DATE_ADD('day', -30, CURRENT_DATE)
    GROUP BY trip_date
    ORDER BY trip_date DESC
  SQL
}

resource "aws_athena_named_query" "cargo_type_ranking" {
  name      = "cargo-type-revenue-ranking"
  workgroup = aws_athena_workgroup.main.id
  database  = aws_glue_catalog_database.main.name
  description = "Revenue breakdown by cargo category (last 90 days)"

  query = <<-SQL
    SELECT
        cargo_type,
        COUNT(*)                          AS total_trips,
        ROUND(SUM(cargo_weight_tons), 0)  AS total_tons,
        ROUND(SUM(freight_value_brl), 2)  AS total_revenue_brl,
        ROUND(AVG(freight_value_brl / NULLIF(cargo_weight_tons, 0)), 2) AS revenue_per_ton,
        ROUND(AVG(delay_minutes), 1)      AS avg_delay_min
    FROM "${aws_glue_catalog_database.main.name}"."silver_freight"
    WHERE trip_date >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY cargo_type
    ORDER BY total_revenue_brl DESC
  SQL
}

resource "aws_athena_named_query" "route_performance" {
  name      = "top-routes-by-volume"
  workgroup = aws_athena_workgroup.main.id
  database  = aws_glue_catalog_database.main.name
  description = "Top 20 origin-destination routes by cargo volume"

  query = <<-SQL
    SELECT
        origin_station,
        destination_station,
        COUNT(*)                         AS total_trips,
        ROUND(SUM(cargo_weight_tons), 0) AS total_tons,
        ROUND(AVG(delay_minutes), 1)     AS avg_delay_min,
        ROUND(SUM(freight_value_brl), 2) AS total_revenue_brl
    FROM "${aws_glue_catalog_database.main.name}"."silver_freight"
    WHERE trip_date >= DATE_ADD('day', -30, CURRENT_DATE)
    GROUP BY origin_station, destination_station
    ORDER BY total_tons DESC
    LIMIT 20
  SQL
}

resource "aws_athena_named_query" "dq_quarantine_audit" {
  name      = "dq-quarantine-audit"
  workgroup = aws_athena_workgroup.main.id
  database  = aws_glue_catalog_database.main.name
  description = "Data quality quarantine audit — rows rejected by Silver DQ rules"

  query = <<-SQL
    SELECT
        failure_reason,
        COUNT(*) AS rejected_rows,
        MIN(ingestion_date) AS first_seen,
        MAX(ingestion_date) AS last_seen
    FROM "${aws_glue_catalog_database.main.name}"."quarantine_silver"
    GROUP BY failure_reason
    ORDER BY rejected_rows DESC
  SQL
}
