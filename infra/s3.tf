# ─────────────────────────────────────────────────────────────────────────────
# S3 — Data Lake buckets (Raw → Bronze → Silver → Gold)
# Follows the medallion / multi-hop architecture.
# Each layer is a separate bucket so IAM policies can enforce write isolation
# (e.g., only the Silver Glue job can write to the silver bucket).
# ─────────────────────────────────────────────────────────────────────────────

locals {
  buckets = {
    raw     = "${local.name_prefix}-raw"
    bronze  = "${local.name_prefix}-bronze"
    silver  = "${local.name_prefix}-silver"
    gold    = "${local.name_prefix}-gold"
    scripts = "${local.name_prefix}-scripts"
    athena  = "${local.name_prefix}-athena-results"
  }
}

# ── Raw (landing zone) ────────────────────────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket        = local.buckets.raw
  force_destroy = var.s3_force_destroy
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = var.s3_versioning_enabled ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "expire-raw-after-90-days"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 90 }
  }
}

# Block public access on all data lake buckets
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Bronze ────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "bronze" {
  bucket        = local.buckets.bronze
  force_destroy = var.s3_force_destroy
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = var.s3_versioning_enabled ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Silver ────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "silver" {
  bucket        = local.buckets.silver
  force_destroy = var.s3_force_destroy
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = var.s3_versioning_enabled ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Gold ──────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "gold" {
  bucket        = local.buckets.gold
  force_destroy = var.s3_force_destroy
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = var.s3_versioning_enabled ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Scripts (Glue jobs + EMR bootstraps) ─────────────────────────────────────
resource "aws_s3_bucket" "scripts" {
  bucket        = local.buckets.scripts
  force_destroy = var.s3_force_destroy
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket                  = aws_s3_bucket.scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload Glue job scripts to S3 on terraform apply
resource "aws_s3_object" "glue_bronze" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/bronze_ingest.py"
  source = "${path.module}/../glue_jobs/bronze_ingest.py"
  etag   = filemd5("${path.module}/../glue_jobs/bronze_ingest.py")
}

resource "aws_s3_object" "glue_silver" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/silver_transform.py"
  source = "${path.module}/../glue_jobs/silver_transform.py"
  etag   = filemd5("${path.module}/../glue_jobs/silver_transform.py")
}

resource "aws_s3_object" "glue_gold" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/gold_aggregate.py"
  source = "${path.module}/../glue_jobs/gold_aggregate.py"
  etag   = filemd5("${path.module}/../glue_jobs/gold_aggregate.py")
}

# ── Athena results ────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "athena_results" {
  bucket        = local.buckets.athena
  force_destroy = var.s3_force_destroy
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 30 }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 event notification → Lambda (triggers pipeline on new raw files)
resource "aws_s3_bucket_notification" "raw_trigger" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "freight/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}
