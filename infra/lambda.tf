# ─────────────────────────────────────────────────────────────────────────────
# AWS Lambda — Event-driven pipeline trigger
#
# Listens to S3 PutObject events on the raw bucket.
# On each new freight CSV, starts a Step Functions execution
# with the file metadata as input — enabling parallel per-file processing.
# ─────────────────────────────────────────────────────────────────────────────

data "archive_file" "lambda_trigger" {
  type        = "zip"
  source_file = "${path.module}/../lambda_/trigger_pipeline.py"
  output_path = "${path.module}/../lambda_/trigger_pipeline.zip"
}

resource "aws_lambda_function" "trigger" {
  function_name    = "${local.name_prefix}-trigger"
  filename         = data.archive_file.lambda_trigger.output_path
  source_code_hash = data.archive_file.lambda_trigger.output_base64sha256
  handler          = "trigger_pipeline.handler"
  runtime          = "python3.11"
  role             = aws_iam_role.lambda.arn
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      STATE_MACHINE_ARN = aws_sfn_state_machine.pipeline.arn
      ENVIRONMENT       = var.environment
    }
  }

  tags = {
    Component = "event-trigger"
  }
}

# Allow S3 to invoke the Lambda function
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

# CloudWatch log group for Lambda — 14-day retention
resource "aws_cloudwatch_log_group" "lambda_trigger" {
  name              = "/aws/lambda/${aws_lambda_function.trigger.function_name}"
  retention_in_days = 14
}
