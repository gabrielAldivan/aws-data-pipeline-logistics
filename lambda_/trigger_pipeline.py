"""
Lambda — Event-driven pipeline trigger

Invoked by S3 PutObject events on the raw bucket.
Starts a Step Functions execution for each new freight CSV,
passing source metadata as input to the state machine.

Handles:
  - Concurrent files (multiple S3 records in one event)
  - Idempotency: deduplicates by S3 ETag to avoid double-processing
  - DLQ-friendly: exceptions propagate so Lambda retries work correctly
"""

import json
import os
import logging
import boto3
from datetime import datetime, timezone
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sfn = boto3.client("stepfunctions")
s3 = boto3.client("s3")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")


def _build_execution_name(bucket: str, key: str, etag: str) -> str:
    """
    Execution names must be unique per state machine.
    Use ETag (content hash) for idempotency — same file won't start a new execution.
    """
    safe_etag = etag.strip('"').replace("-", "")[:20]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{ENVIRONMENT}-{ts}-{safe_etag}"


def _get_object_metadata(bucket: str, key: str) -> dict:
    """Fetch S3 object metadata (size, etag) for the execution input."""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        return {
            "size_bytes": head["ContentLength"],
            "etag": head["ETag"].strip('"'),
            "last_modified": head["LastModified"].isoformat(),
        }
    except Exception as exc:
        logger.warning(
            "Could not fetch object metadata for s3://%s/%s: %s", bucket, key, exc
        )
        return {}


def handler(event, context):
    """
    Process S3 event records — one Step Functions execution per CSV file.

    Event shape (S3 notification):
      {"Records": [{"s3": {"bucket": {"name": "..."}, "object": {"key": "..."}}}]}
    """
    executions_started = 0
    errors = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        # Only process freight CSV files in the expected prefix
        if not key.startswith("freight/") or not key.endswith(".csv"):
            logger.info("Skipping non-freight file: s3://%s/%s", bucket, key)
            continue

        metadata = _get_object_metadata(bucket, key)
        etag = metadata.get("etag", context.aws_request_id[:20])

        execution_input = json.dumps(
            {
                "bucket": bucket,
                "key": key,
                "etag": etag,
                "size_bytes": metadata.get("size_bytes", 0),
                "triggered_at": datetime.now(timezone.utc).isoformat(),
                "environment": ENVIRONMENT,
            }
        )

        execution_name = _build_execution_name(bucket, key, etag)

        try:
            resp = sfn.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_name,
                input=execution_input,
            )
            executions_started += 1
            logger.info(
                "Started execution %s for s3://%s/%s → ARN: %s",
                execution_name,
                bucket,
                key,
                resp["executionArn"],
            )
        except sfn.exceptions.ExecutionAlreadyExists:
            # Idempotent — same ETag = same execution name = already running
            logger.info("Execution already exists for etag %s — skipping", etag)
        except Exception as exc:
            logger.error(
                "Failed to start execution for s3://%s/%s: %s", bucket, key, exc
            )
            errors.append({"key": key, "error": str(exc)})

    if errors:
        # Raise so Lambda retries and DLQ captures persistent failures
        raise RuntimeError(
            f"Failed to trigger pipeline for {len(errors)} file(s): {errors}"
        )

    return {
        "statusCode": 200,
        "executions_started": executions_started,
    }
