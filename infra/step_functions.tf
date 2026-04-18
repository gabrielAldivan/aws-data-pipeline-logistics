# ─────────────────────────────────────────────────────────────────────────────
# AWS Step Functions — Pipeline orchestration
#
# DAG:
#   BronzeIngest (Glue)
#     └→ SilverTransform (Glue)
#          └→ GoldAggregate (Glue)
#               ├→ RunGoldCrawler (Glue Crawler)
#               └→ [future] CopyToRedshift (Lambda)
#
# Each Glue step uses .sync integration (Step Functions waits for completion
# before advancing), with error catching and retry logic.
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name_prefix}-pipeline"
  role_arn = aws_iam_role.step_functions.arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment = "Logistics freight data pipeline: Raw S3 → Bronze → Silver → Gold → Redshift"
    StartAt = "BronzeIngest"

    States = {

      BronzeIngest = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_ingest.name
          Arguments = {
            "--SOURCE_KEY.$" = "$.key"
            "--SOURCE_BUCKET.$" = "$.bucket"
          }
        }
        Retry = [{
          ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "SilverTransform"
      }

      SilverTransform = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver_transform.name
        }
        Retry = [{
          ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
          IntervalSeconds = 60
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "GoldAggregate"
      }

      GoldAggregate = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.gold_aggregate.name
        }
        Retry = [{
          ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
          IntervalSeconds = 60
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
        Next = "UpdateCatalog"
      }

      UpdateCatalog = {
        Type     = "Task"
        Comment  = "Run Gold crawler to update Glue Data Catalog tables for Athena"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.gold.name
        }
        Catch = [{
          ErrorEquals = ["Glue.CrawlerRunningException"]
          Next        = "PipelineSucceeded"   # crawler already running — that's fine
          ResultPath  = "$.crawlerError"
        }]
        Next = "PipelineSucceeded"
      }

      PipelineSucceeded = {
        Type = "Succeed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Cause = "One or more pipeline steps failed. Check CloudWatch Logs for details."
      }
    }
  })

  logging_configuration {
    level                  = "ERROR"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
  }

  tags = {
    Component = "orchestration"
  }
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/states/${local.name_prefix}-pipeline"
  retention_in_days = 30
}
