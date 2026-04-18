# ─────────────────────────────────────────────────────────────────────────────
# AWS Glue — Data Catalog + ETL Jobs + Crawlers
#
# Architecture:
#   Raw CSV (S3) → Glue Job: Bronze → Glue Job: Silver → Glue Job: Gold
#   Glue Crawlers keep the Data Catalog tables in sync after each write,
#   so Athena always queries up-to-date schemas.
# ─────────────────────────────────────────────────────────────────────────────

# ── Data Catalog ──────────────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "main" {
  name        = replace("${local.name_prefix}_db", "-", "_")
  description = "Glue Data Catalog for the logistics freight pipeline"
}

# ── ETL Jobs ──────────────────────────────────────────────────────────────────

resource "aws_glue_job" "bronze_ingest" {
  name         = "${local.name_prefix}-bronze-ingest"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers

  command {
    name            = "glueetl"
    python_version  = var.glue_python_version
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/bronze_ingest.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.scripts.bucket}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.bucket}/tmp/"
    "--RAW_BUCKET"                       = aws_s3_bucket.raw.bucket
    "--BRONZE_BUCKET"                    = aws_s3_bucket.bronze.bucket
    "--GLUE_DATABASE"                    = aws_glue_catalog_database.main.name
  }

  execution_property {
    max_concurrent_runs = 3
  }

  tags = {
    Layer = "bronze"
  }
}

resource "aws_glue_job" "silver_transform" {
  name         = "${local.name_prefix}-silver-transform"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers

  command {
    name            = "glueetl"
    python_version  = var.glue_python_version
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/silver_transform.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.scripts.bucket}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.bucket}/tmp/"
    "--BRONZE_BUCKET"                    = aws_s3_bucket.bronze.bucket
    "--SILVER_BUCKET"                    = aws_s3_bucket.silver.bucket
    "--GLUE_DATABASE"                    = aws_glue_catalog_database.main.name
  }

  execution_property {
    max_concurrent_runs = 1   # Silver transforms are stateful (dedup) — run sequentially
  }

  tags = {
    Layer = "silver"
  }
}

resource "aws_glue_job" "gold_aggregate" {
  name         = "${local.name_prefix}-gold-aggregate"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers

  command {
    name            = "glueetl"
    python_version  = var.glue_python_version
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/gold_aggregate.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.scripts.bucket}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.bucket}/tmp/"
    "--SILVER_BUCKET"                    = aws_s3_bucket.silver.bucket
    "--GOLD_BUCKET"                      = aws_s3_bucket.gold.bucket
    "--GLUE_DATABASE"                    = aws_glue_catalog_database.main.name
    "--REDSHIFT_IAM_ROLE"                = aws_iam_role.redshift.arn
  }

  tags = {
    Layer = "gold"
  }
}

# ── Crawlers (keep Glue Data Catalog tables in sync) ─────────────────────────

resource "aws_glue_crawler" "bronze" {
  name          = "${local.name_prefix}-crawler-bronze"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.bronze.bucket}/freight/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = { Layer = "bronze" }
}

resource "aws_glue_crawler" "silver" {
  name          = "${local.name_prefix}-crawler-silver"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.silver.bucket}/freight/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  tags = { Layer = "silver" }
}

resource "aws_glue_crawler" "gold" {
  name          = "${local.name_prefix}-crawler-gold"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.gold.bucket}/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  tags = { Layer = "gold" }
}

# ── Glue Job Triggers (scheduled daily at 02:00 UTC) ─────────────────────────
# Note: event-driven trigger is handled by Lambda + Step Functions.
# This scheduled trigger acts as a daily catch-up / full refresh.

resource "aws_glue_trigger" "daily_pipeline" {
  name     = "${local.name_prefix}-daily-trigger"
  type     = "SCHEDULED"
  schedule = "cron(0 2 * * ? *)"   # 02:00 UTC = 23:00 BRT

  actions {
    job_name = aws_glue_job.bronze_ingest.name
  }

  tags = { Schedule = "daily" }
}
