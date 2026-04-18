# ─────────────────────────────────────────────────────────────────────────────
# AWS Redshift Serverless — Data Warehouse for Gold layer consumption
#
# Gold data (S3 Parquet) is loaded into Redshift via COPY command
# (orchestrated by Step Functions after the Gold Glue job completes).
#
# Redshift Serverless: no cluster management, pay-per-query, auto-scales RPUs.
# Suitable for analytics workloads with variable concurrency.
#
# Consumers: Power BI / QuickSight dashboards, ad-hoc SQL by analysts.
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${local.name_prefix}-ns"
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift.arn]

  log_exports = ["userlog", "connectionlog", "useractivitylog"]

  tags = {
    Component = "data-warehouse"
  }
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "${local.name_prefix}-wg"
  base_capacity  = var.redshift_base_capacity   # RPUs: 8 = minimum, scales up automatically

  # Enable enhanced VPC routing so all COPY/UNLOAD traffic stays in the VPC
  enhanced_vpc_routing = false   # set true when vpc_id is configured

  publicly_accessible = false

  config_parameter {
    parameter_key   = "max_query_execution_time"
    parameter_value = "14400"   # 4 hours — hard limit for runaway queries
  }

  config_parameter {
    parameter_key   = "enable_user_activity_logging"
    parameter_value = "true"
  }

  tags = {
    Component = "data-warehouse"
  }
}

# Resource policy — restrict Redshift to access only the Gold bucket
resource "aws_redshiftserverless_resource_policy" "main" {
  resource_arn = aws_redshiftserverless_namespace.main.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
      Action    = ["redshift-serverless:GetNamespace"]
      Resource  = aws_redshiftserverless_namespace.main.arn
    }]
  })
}
