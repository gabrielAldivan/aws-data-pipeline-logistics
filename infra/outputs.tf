output "s3_bucket_raw" {
  description = "S3 bucket for raw ingestion (landing zone)"
  value       = aws_s3_bucket.raw.bucket
}

output "s3_bucket_bronze" {
  description = "S3 bucket for Bronze layer (immutable raw Parquet)"
  value       = aws_s3_bucket.bronze.bucket
}

output "s3_bucket_silver" {
  description = "S3 bucket for Silver layer (cleaned, typed)"
  value       = aws_s3_bucket.silver.bucket
}

output "s3_bucket_gold" {
  description = "S3 bucket for Gold layer (business aggregations)"
  value       = aws_s3_bucket.gold.bucket
}

output "s3_bucket_scripts" {
  description = "S3 bucket hosting Glue job scripts and EMR bootstraps"
  value       = aws_s3_bucket.scripts.bucket
}

output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.main.name
}

output "glue_role_arn" {
  description = "IAM role ARN used by Glue jobs"
  value       = aws_iam_role.glue.arn
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.main.name
}

output "athena_results_bucket" {
  description = "S3 bucket for Athena query results"
  value       = aws_s3_bucket.athena_results.bucket
}

output "redshift_workgroup_endpoint" {
  description = "Redshift Serverless endpoint for SQL clients"
  value       = aws_redshiftserverless_workgroup.main.endpoint
}

output "redshift_namespace_id" {
  description = "Redshift Serverless namespace ID"
  value       = aws_redshiftserverless_namespace.main.id
}

output "step_functions_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "lambda_trigger_arn" {
  description = "Lambda function ARN that triggers the pipeline on S3 events"
  value       = aws_lambda_function.trigger.arn
}

output "emr_cluster_id" {
  description = "EMR cluster ID for heavy PySpark workloads"
  value       = aws_emr_cluster.main.id
}
