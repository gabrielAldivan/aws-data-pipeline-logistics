"""
Unit tests for Lambda trigger function.
Uses moto to mock AWS services (S3 + Step Functions) — no real AWS calls.
"""

import json
import os

import boto3
import pytest

os.environ["STATE_MACHINE_ARN"] = (
    "arn:aws:states:us-east-1:123456789012:stateMachine:test"
)
os.environ["ENVIRONMENT"] = "test"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

try:
    from moto import mock_stepfunctions, mock_s3, mock_iam

    MOTO_AVAILABLE = True
except ImportError:
    MOTO_AVAILABLE = False

pytestmark = pytest.mark.skipif(not MOTO_AVAILABLE, reason="moto not installed")


def _make_s3_event(bucket: str, key: str) -> dict:
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key, "size": 1024},
                }
            }
        ]
    }


class FakeContext:
    aws_request_id = "fake-request-id-1234"


@mock_stepfunctions
@mock_s3
@mock_iam
def test_handler_starts_execution_for_freight_csv():
    from lambda_.trigger_pipeline import handler

    # Create a real moto state machine
    iam = boto3.client("iam", region_name="us-east-1")
    role = iam.create_role(
        RoleName="test-sfn-role",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )
    sfn = boto3.client("stepfunctions", region_name="us-east-1")
    sm = sfn.create_state_machine(
        name="test",
        definition=json.dumps(
            {
                "Comment": "test",
                "StartAt": "Done",
                "States": {"Done": {"Type": "Succeed"}},
            }
        ),
        roleArn=role["Role"]["Arn"],
    )
    os.environ["STATE_MACHINE_ARN"] = sm["stateMachineArn"]

    # Create S3 bucket + object
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-raw-bucket")
    s3.put_object(
        Bucket="test-raw-bucket", Key="freight/2024/file.csv", Body=b"col1,col2\n1,2"
    )

    event = _make_s3_event("test-raw-bucket", "freight/2024/file.csv")
    result = handler(event, FakeContext())
    assert result["statusCode"] == 200
    assert result["executions_started"] == 1


@mock_stepfunctions
@mock_s3
def test_handler_skips_non_freight_files():
    from lambda_.trigger_pipeline import handler

    event = _make_s3_event("test-raw-bucket", "other-prefix/file.csv")
    result = handler(event, FakeContext())
    assert result["executions_started"] == 0


@mock_stepfunctions
@mock_s3
def test_handler_skips_non_csv_files():
    from lambda_.trigger_pipeline import handler

    event = _make_s3_event("test-raw-bucket", "freight/file.parquet")
    result = handler(event, FakeContext())
    assert result["executions_started"] == 0
