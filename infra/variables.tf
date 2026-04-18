variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name — used as prefix for all resource names"
  type        = string
  default     = "logistics-pipeline"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod"
  }
}

# ── S3 ────────────────────────────────────────────────────────────────────────

variable "s3_force_destroy" {
  description = "Allow Terraform to destroy non-empty buckets (true only in dev)"
  type        = bool
  default     = true
}

variable "s3_versioning_enabled" {
  description = "Enable versioning on data lake buckets"
  type        = bool
  default     = true
}

# ── Glue ─────────────────────────────────────────────────────────────────────

variable "glue_python_version" {
  description = "Python version for Glue ETL jobs"
  type        = string
  default     = "3"
}

variable "glue_worker_type" {
  description = "Glue worker type: Standard, G.1X, G.2X, G.4X, G.8X"
  type        = string
  default     = "G.1X"
}

variable "glue_num_workers" {
  description = "Number of Glue workers for ETL jobs"
  type        = number
  default     = 5
}

# ── Athena ────────────────────────────────────────────────────────────────────

variable "athena_bytes_scanned_cutoff" {
  description = "Cancel Athena queries that scan more than this many bytes (cost guard)"
  type        = number
  default     = 10737418240   # 10 GB
}

# ── Redshift Serverless ───────────────────────────────────────────────────────

variable "redshift_db_name" {
  description = "Redshift database name"
  type        = string
  default     = "logistics_dw"
}

variable "redshift_admin_user" {
  description = "Redshift admin username"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "redshift_admin_password" {
  description = "Redshift admin password — provide via TF_VAR_redshift_admin_password or tfvars"
  type        = string
  sensitive   = true
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base capacity in RPUs (8–512, multiples of 8)"
  type        = number
  default     = 8
}

# ── EMR ───────────────────────────────────────────────────────────────────────

variable "emr_release" {
  description = "EMR release label"
  type        = string
  default     = "emr-6.15.0"
}

variable "emr_master_instance_type" {
  description = "EC2 instance type for EMR master node"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "EC2 instance type for EMR core nodes"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core nodes"
  type        = number
  default     = 2
}

variable "emr_key_pair" {
  description = "EC2 key pair name for SSH access to EMR nodes (optional)"
  type        = string
  default     = ""
}

# ── Networking ────────────────────────────────────────────────────────────────

variable "vpc_id" {
  description = "VPC ID for EMR and Redshift (leave empty to use default VPC)"
  type        = string
  default     = ""
}

variable "subnet_id" {
  description = "Subnet ID for EMR master and Redshift (must be in vpc_id)"
  type        = string
  default     = ""
}
