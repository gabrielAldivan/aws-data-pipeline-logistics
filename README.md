# AWS Data Pipeline — Logistics

End-to-end data pipeline for a logistics operator built entirely on AWS managed services. Covers ingestion through analytics-ready aggregations, with infrastructure-as-code, distributed processing, and event-driven orchestration.

## Architecture

```
S3 (raw CSV)
    │
    ▼  S3 event notification
AWS Lambda ──► Step Functions state machine
                    │
                    ├─ AWS Glue Job: Bronze ingest    (PySpark, append)
                    ├─ AWS Glue Job: Silver transform  (DQ rules, dedup, derive)
                    ├─ AWS Glue Job: Gold aggregate    (KPIs, routes, cargo mix)
                    └─ Glue Crawlers → Data Catalog
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
              AWS Athena           Redshift Serverless
          (ad-hoc queries)        (BI / reporting layer)
                    │
              AWS EMR (optional)
          (historical backfill, ML clustering, batch scoring)
```

## Services used

| Service | Role |
|---|---|
| **S3** | Raw / Bronze / Silver / Gold / scripts storage |
| **AWS Glue** | ETL jobs (PySpark), crawlers, Data Catalog |
| **AWS Athena** | Cost-controlled ad-hoc SQL (10 GB scan limit per query) |
| **Redshift Serverless** | Analytical serving layer — DISTKEY/SORTKEY/ENCODE optimised |
| **AWS EMR** | Historical backfill, MLlib KMeans clustering, pandas_udf batch scoring |
| **AWS Lambda** | Event-driven S3→Step Functions trigger, idempotent by ETag |
| **Step Functions** | DAG orchestration with retry, backoff, and failure catch |
| **Terraform** | All infrastructure provisioned as code |

## Project structure

```
aws-data-pipeline-logistics/
├── infra/                    # Terraform — all AWS resources
│   ├── main.tf               # Provider, backend, default tags
│   ├── variables.tf          # 15+ typed variables with validation
│   ├── outputs.tf            # Endpoints, ARNs, bucket names
│   ├── s3.tf                 # 5 buckets — SSE, versioning, lifecycle
│   ├── iam.tf                # Least-privilege roles per service
│   ├── glue.tf               # Jobs, crawlers, daily trigger
│   ├── athena.tf             # Workgroup + 4 named queries
│   ├── redshift.tf           # Serverless namespace + workgroup
│   ├── emr.tf                # Cluster 6.15.0, YARN autoscaling
│   ├── lambda.tf             # Trigger function + CloudWatch logs
│   └── step_functions.tf     # State machine JSON definition
├── glue_jobs/
│   ├── bronze_ingest.py      # Metadata injection, append mode
│   ├── silver_transform.py   # DQ rules, quarantine, dedup, derived cols
│   └── gold_aggregate.py     # Daily KPIs, route ranking, cargo mix
├── emr_jobs/
│   └── heavy_processing.py   # Backfill, delay scoring, demand clustering
├── lambda/
│   └── trigger_pipeline.py   # Idempotent trigger by S3 ETag
├── redshift/
│   ├── schema.sql            # Tables with DISTKEY / SORTKEY / ENCODE
│   └── load_from_s3.sql      # COPY Parquet + VACUUM + ANALYZE
├── scripts/
│   └── generate_sample_data.py  # 500k synthetic freight rows
└── tests/
    ├── test_glue_jobs.py     # PySpark unit tests with moto for DQ/agg logic
    └── test_lambda.py        # moto mocks for S3 + Step Functions
```

## Key engineering decisions

**Medallion architecture on S3**
Raw → Bronze (metadata enrichment) → Silver (validated, typed, deduplicated) → Gold (aggregated KPIs). Each layer is Parquet-partitioned by date and registered in the Glue Data Catalog for schema governance.

**Data Quality framework (Silver layer)**
`DQRule` dataclass defines per-column rules (`NOT_NULL`, `RANGE`, `POSITIVE`). Rows failing `ERROR`-severity rules are quarantined to a separate S3 prefix instead of being silently dropped. `WARNING` rows are flagged with a column and continue downstream. Quarantine rate is auditable via Athena.

**Idempotent Lambda trigger**
The Lambda builds the Step Functions execution name from the S3 object ETag. Re-uploading the same file is a no-op (`ExecutionAlreadyExists` silently swallowed). A different file version triggers a new execution. DLQ-friendly: any unexpected error raises so the event retries.

**Least-privilege IAM**
Each service has its own role. Glue Bronze can only write to the Bronze bucket. Glue Gold can only read Silver and write Gold. Redshift can only read Gold. Lambda can only start Step Functions executions. No wildcard resource ARNs.

**Cost controls on Athena**
Workgroup sets `bytes_scanned_cutoff_per_query = 10 GB`. All named queries use partition filters and `SELECT` specific columns — no `SELECT *` on full tables.

**EMR autoscaling**
Scale-out rule: add 1 node when `YARNMemoryAvailablePercentage < 20`. Scale-in rule: remove 1 node when `YARNMemoryAvailablePercentage > 75`. Prevents idle cluster cost while handling burst loads.

## Infrastructure deployment

```bash
cd infra
terraform init
terraform plan -var="environment=dev" -var="project_name=logistics-pipeline"
terraform apply
```

## Running Glue jobs locally (without AWS)

The jobs use a try/except pattern to import `awsglue` and fall back to plain PySpark when running outside AWS. This makes local development and CI testing possible without mocking the Glue runtime.

```bash
pip install pyspark pytest moto boto3
pytest tests/ -v
```

## Athena named queries

| Query | Description |
|---|---|
| `daily_kpis` | Revenue, weight, trip count, on-time rate per day |
| `cargo_type_ranking` | Revenue share and trip count by cargo type |
| `route_performance` | Delay rate and avg revenue per route |
| `dq_quarantine_audit` | Quarantined row count and rate by date |
