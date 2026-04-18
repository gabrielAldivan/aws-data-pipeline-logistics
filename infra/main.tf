terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Production: replace with S3 remote backend
  # backend "s3" {
  #   bucket         = "my-terraform-state-prod"
  #   key            = "logistics-pipeline/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "terraform-lock"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = "data-engineering"
    }
  }
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}
