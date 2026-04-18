# ─────────────────────────────────────────────────────────────────────────────
# AWS EMR — Heavy PySpark workloads
#
# Used for computationally intensive jobs that exceed Glue's capacity:
#   - Historical backfill (multi-year freight data)
#   - ML batch scoring (predicting cargo delay probability at scale)
#   - Complex window functions across full dataset
#
# Glue handles daily incremental ETL; EMR handles heavy lifting.
# Cluster auto-terminates after step completion (cost optimization).
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_emr_cluster" "main" {
  name          = "${local.name_prefix}-emr"
  release_label = var.emr_release
  applications  = ["Spark", "Hadoop", "Hive"]

  # Auto-terminate after all steps complete
  keep_job_flow_alive_when_no_steps = false
  termination_protection            = false   # set true for long-running clusters

  service_role     = aws_iam_role.emr_service.arn
  autoscaling_role = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/EMR_AutoScaling_DefaultRole"

  ec2_attributes {
    instance_profile = aws_iam_instance_profile.emr_ec2.arn
    # Uncomment when VPC is configured:
    # subnet_id                         = var.subnet_id
    # emr_managed_master_security_group = aws_security_group.emr_master.id
    # emr_managed_slave_security_group  = aws_security_group.emr_core.id
    key_name = var.emr_key_pair != "" ? var.emr_key_pair : null
  }

  master_instance_group {
    instance_type = var.emr_master_instance_type

    ebs_config {
      size                 = 32
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  core_instance_group {
    instance_type  = var.emr_core_instance_type
    instance_count = var.emr_core_instance_count

    ebs_config {
      size                 = 64
      type                 = "gp3"
      volumes_per_instance = 1
    }

    # Auto-scaling: scale out when YARN memory > 80%, scale in when < 30%
    autoscaling_policy = jsonencode({
      Constraints = {
        MinCapacity = 2
        MaxCapacity = 10
      }
      Rules = [
        {
          Name   = "ScaleOut"
          Action = { SimpleScalingPolicyConfiguration = { AdjustmentType = "CHANGE_IN_CAPACITY", ScalingAdjustment = 2, CoolDown = 300 } }
          Trigger = { CloudWatchAlarmDefinition = {
            ComparisonOperator = "LESS_THAN"
            EvaluationPeriods  = 1
            MetricName         = "YARNMemoryAvailablePercentage"
            Namespace          = "AWS/ElasticMapReduce"
            Period             = 300
            Statistic          = "AVERAGE"
            Threshold          = 20
          }}
        },
        {
          Name   = "ScaleIn"
          Action = { SimpleScalingPolicyConfiguration = { AdjustmentType = "CHANGE_IN_CAPACITY", ScalingAdjustment = -1, CoolDown = 300 } }
          Trigger = { CloudWatchAlarmDefinition = {
            ComparisonOperator = "GREATER_THAN"
            EvaluationPeriods  = 3
            MetricName         = "YARNMemoryAvailablePercentage"
            Namespace          = "AWS/ElasticMapReduce"
            Period             = 300
            Statistic          = "AVERAGE"
            Threshold          = 75
          }}
        },
      ]
    })
  }

  # Spark configuration tuned for freight analytics workload
  configurations_json = jsonencode([
    {
      Classification = "spark"
      Properties     = { maximizeResourceAllocation = "true" }
    },
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.shuffle.partitions"              = "200"
        "spark.sql.adaptive.enabled"               = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
        "spark.sql.parquet.compression.codec"      = "snappy"
        "spark.serializer"                         = "org.apache.spark.serializer.KryoSerializer"
        "spark.sql.extensions"                     = "io.delta.sql.DeltaSparkSessionExtension"
        "spark.sql.catalog.spark_catalog"          = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      }
    },
    {
      Classification = "spark-env"
      Configurations = [{
        Classification = "export"
        Properties     = { PYSPARK_PYTHON = "/usr/bin/python3" }
      }]
    },
  ])

  log_uri = "s3://${aws_s3_bucket.scripts.bucket}/emr-logs/"

  # Bootstrap: install Python packages on all nodes
  bootstrap_action {
    name = "install-python-deps"
    path = "s3://${aws_s3_bucket.scripts.bucket}/bootstrap/install_deps.sh"
  }

  tags = {
    Component = "heavy-processing"
  }
}
